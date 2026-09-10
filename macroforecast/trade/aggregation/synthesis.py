"""Multi-level synthesis runner: one metric table, three (score, rank) pairs.

The estimators of the module score **one** ``(n, d)`` matrix; the pipeline needs
a score for every cell ``reporter x product`` of a context
``(freq, flow, indicators, TIME_PERIOD)``, at the three comparison levels of
D-09: ``by_product`` (one group per product, countries ordered),
``by_reporter`` (one group per country, products ordered) and ``global`` (every
cell of the context). Methods with endogenous weights are fitted **on each
group**.

:func:`run_synthesis` is a pure ``DataFrame -> DataFrames + report`` function
(D-17): no DuckLake connection, no S3, no side effect beyond the optional
tracker, so it becomes a Kedro node without a rewrite. It returns the long
score table of S-2.4 (one row per cell and per method, including the consensus
pseudo-methods), the fit diagnostics of S-2.5.c in the long format of S-2.6,
and a :class:`SynthesisReport`.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field, replace
import logging
import time
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
import warnings
import zlib
# Modules de manipulation de données
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.pipeline import Pipeline
# Modules du package
from ...tracking import NULL_TRACKER, RunTracker, flatten_metrics
from .base import AggregationConfig
from .diagnostics import borda_rank, copeland_rank, kemeny_rank
from .methods import (
    DRAW_PARAMETERS,
    METHOD_REGISTRY,
    MethodSpec,
    SEED_PARAMETERS,
    build_method,
    method_metrics,
    registry_entry,
)
from .weights import AutoWeightingReport

# Initialisation du logger
logger = logging.getLogger(__name__)

# Niveaux de comparaison (D-09) : règle de constitution des groupes d'un contexte
LEVELS: Tuple[str, ...] = ("by_product", "by_reporter", "global")

# Sentinelle de groupe absent dans les tables longues (S-2.6, jamais NULL)
GROUP_SENTINEL = "ALL"
# Sentinelle d'objet absent (`item_a` / `item_b`) dans les tables longues
ITEM_SENTINEL = ""

# Préfixe des pseudo-méthodes de consensus dans la colonne `method` (S-2.4)
CONSENSUS_PREFIX = "consensus_"

# Famille de diagnostics émise par ce runner (S-2.5.c)
FIT_FAMILY = "fit"

# Colonnes de la table longue de diagnostics (S-2.6)
DIAGNOSTIC_COLUMNS: Tuple[str, ...] = (
    "level",
    "reporter",
    "product",
    "family",
    "statistic",
    "item_a",
    "item_b",
    "value",
    "n",
)

# Consensus admissibles et fonction de classement associée (`top_n` en second)
_CONSENSUS_FUNCTIONS = {
    "borda": lambda scores, top_n: borda_rank(scores),
    "copeland": lambda scores, top_n: copeland_rank(scores, top_n=top_n),
    "kemeny": lambda scores, top_n: kemeny_rank(scores, top_n=top_n),
}


# ──────────────────────────────────────────────────────────────────────
# Configuration (S-1.2) et liste de méthodes par défaut (S-2.2)
# ──────────────────────────────────────────────────────────────────────

# Liste de méthodes par défaut, identique au bloc `methods` de S-2.2
DEFAULT_METHODS: Tuple[MethodSpec, ...] = (
    MethodSpec(
        name="pareto",
        kind="pareto",
        params={
            "epsilon": 0.1,
            "epsilon_scale": "mad",
            "reduce": {"threshold": 0.3},
            "layers": True,
        },
    ),
    MethodSpec(
        name="pareto_global",
        kind="pareto",
        levels=("global",),
        params={"epsilon": 0.1, "reduce": {"threshold": 0.3}, "layers": False},
    ),
    MethodSpec(name="rank_mean", kind="rank_mean"),
    MethodSpec(
        name="entropy_sum",
        kind="weighted",
        params={"weighting": "entropy", "aggregation": "weighted_sum"},
    ),
    MethodSpec(
        name="critic_sum",
        kind="weighted",
        params={
            "weighting": "critic",
            "aggregation": "weighted_sum",
            "weighting_params": {"method": "spearman", "scale": "mad"},
        },
    ),
    MethodSpec(
        name="auto_sum",
        kind="weighted",
        params={"weighting": "auto", "aggregation": "weighted_sum"},
    ),
    MethodSpec(
        name="auto_geo",
        kind="weighted",
        params={
            "weighting": "auto",
            "aggregation": "geometric_mean",
            "aggregation_params": {"epsilon": 1e-3},
        },
    ),
    MethodSpec(name="mpi", kind="mpi"),
    MethodSpec(
        name="topsis_critic",
        kind="topsis",
        params={"weighting": "critic", "robust": True},
    ),
    MethodSpec(
        name="vikor_critic", kind="vikor", params={"weighting": "critic", "v": 0.5}
    ),
    MethodSpec(
        name="bod", kind="bod", params={"rho": 4.0, "restriction": "assurance_region"}
    ),
    MethodSpec(
        name="whitened",
        kind="whitened_projection",
        params={"covariance_estimator": "mcd"},
    ),
    MethodSpec(
        name="kantorovich",
        kind="kantorovich",
        metrics=("HHI", "CDI2", "CDI3", "EXPORT_HHI"),
        params={
            "epsilon": 0.1,
            "n_target": 4096,
            "fit_sample_size": 20_000,
            "alpha": 0.05,
            "theta0_degrees": 60.0,
            "conformal": "split",
            "alert": "projected",
        },
    ),
    MethodSpec(name="smaa", kind="smaa", params={"aggregation": "weighted_sum"}),
    MethodSpec(name="cone_quantile", kind="cone_quantile"),
)


# Paramètres de la synthèse multiniveau
@dataclass(frozen=True)
class SynthesisConfig:
    """Parameters of the multi-level synthesis (S-1.2).

    Attributes:
        context_columns: Keys defining a comparison universe; two contexts are
            never compared with one another.
        reporter_col: Name of the reporting-country column.
        product_col: Name of the product column.
        metric_columns: Metric columns available to the methods.
        polarities: ``(metric, +/-1)`` pairs; every unlisted metric has a
            positive polarity.
        levels: Levels the synthesis is run at, a subset of :data:`LEVELS`.
        methods: Configured methods; defaults to :data:`DEFAULT_METHODS`.
        normalization: Default normalisation scheme, applied to every method
            admitting it (D-13).
        winsorize_quantile: Upper quantile of the winsorisation; ``None``
            disables it (M-08, D-12).
        min_group_size: Floor applied on top of the per-method default of
            table S-1.4.
        rank_ties: Tie-handling of ``scipy.stats.rankdata``.
        consensus: Consensus rules turned into pseudo-methods
            (``borda``, ``copeland``, ``kemeny``).
        consensus_top_n: Borda preselection the Copeland and Kemeny consensus
            are solved on (I-17).
        smaa_n_draws: Number of Dirichlet draws shared by SMAA and the cone
            quantile inside a group.
        smaa_k: Depth of the SMAA confidence factor.
        bootstrap_methods: Names of the methods a rank interval is bootstrapped
            for (D-08).
        bootstrap_n: Number of bootstrap draws.
        bootstrap_ci: Width of the reported rank interval.
        bootstrap_levels: Levels the bootstrap runs at; the global level is
            excluded by default (D-11).
        random_state: Seed the per-group seeds are derived from.
        ot_dimension_limit: Dimension above which the Kantorovitch score is
            skipped (D-07).
        artifact_top_n: Number of rows kept in the ``top_*`` artifacts.

    Examples:
        >>> config = SynthesisConfig(metric_columns=("HHI", "CDI2"))
        >>> config.levels
        ('by_product', 'by_reporter', 'global')
    """
    context_columns: Tuple[str, ...] = ("freq", "flow", "indicators", "TIME_PERIOD")
    reporter_col: str = "reporter"
    product_col: str = "product"
    metric_columns: Tuple[str, ...] = ("HHI", "CDI2", "CDI3")
    polarities: Tuple[Tuple[str, int], ...] = ()
    levels: Tuple[str, ...] = LEVELS
    methods: Tuple[MethodSpec, ...] = DEFAULT_METHODS
    normalization: str = "minmax"
    winsorize_quantile: Optional[float] = None
    min_group_size: int = 30
    rank_ties: str = "average"
    consensus: Tuple[str, ...] = ("borda", "copeland")
    consensus_top_n: int = 100
    smaa_n_draws: int = 2_000
    smaa_k: int = 50
    bootstrap_methods: Tuple[str, ...] = ()
    bootstrap_n: int = 50
    bootstrap_ci: float = 0.9
    bootstrap_levels: Tuple[str, ...] = ("by_product", "by_reporter")
    random_state: int = 0
    ot_dimension_limit: int = 8
    artifact_top_n: int = 50


# ──────────────────────────────────────────────────────────────────────
# Rapport d'exécution (S-2.7)
# ──────────────────────────────────────────────────────────────────────

# Résumé d'un niveau de comparaison
@dataclass
class LevelReport:
    """Summary of one comparison level.

    Attributes:
        n_groups: Number of groups met at this level, every context together.
        n_cells_scored: Number of ``(cell, method)`` pairs actually scored.
        n_methods_skipped: Number of ``(group, method)`` pairs skipped, for any
            reason (group too small, missing dependency, dimension limit).
        median_group_size: Median number of rows per group.
        seconds: Cumulated fit-and-predict time, per method name.
    """
    n_groups: int = 0
    n_cells_scored: int = 0
    n_methods_skipped: int = 0
    median_group_size: float = float("nan")
    seconds: Dict[str, float] = field(default_factory=dict)


# Résumé d'une exécution de synthèse
@dataclass
class SynthesisReport:
    """Summary of a synthesis run (S-2.7).

    Attributes:
        n_contexts: Number of contexts scored.
        n_cells: Number of cells (rows of the input table).
        created: Whether the result schema was created; set by the calling
            script, the runner performing no I/O (D-17).
        levels: One :class:`LevelReport` per level, keyed by level name.

    Examples:
        >>> report = SynthesisReport(n_contexts=2, n_cells=120)
        >>> report.to_metrics()["synthesis.n_contexts"]
        2.0
    """
    n_contexts: int = 0
    n_cells: int = 0
    created: bool = False
    levels: Dict[str, LevelReport] = field(default_factory=dict)

    # Mise en forme des métriques (la seule à connaître les contraintes MLflow)
    def to_metrics(self, prefix: str = "synthesis") -> Dict[str, float]:
        """Flatten every numeric field into a dotted metric mapping.

        Produces ``…n_contexts``, ``…created`` and, per level,
        ``…by_reporter.n_groups``, ``…by_reporter.median_group_size`` and
        ``…by_reporter.seconds_{method}`` (S-2.7). ``NaN`` and infinite values
        are dropped, MLflow rejecting them.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> report = SynthesisReport(n_contexts=1)
            >>> report.levels["global"] = LevelReport(n_groups=1, seconds={"mpi": 0.5})
            >>> report.to_metrics()["synthesis.global.seconds_mpi"]
            0.5
        """
        metrics: Dict[str, float] = {}
        metrics.update(
            flatten_metrics(
                {
                    "n_contexts": self.n_contexts,
                    "n_cells": self.n_cells,
                    "created": self.created,
                },
                prefix=prefix,
            )
        )
        for level, level_report in self.levels.items():
            level_prefix = f"{prefix}.{level}"
            metrics.update(
                flatten_metrics(
                    {
                        "n_groups": level_report.n_groups,
                        "n_cells_scored": level_report.n_cells_scored,
                        "n_methods_skipped": level_report.n_methods_skipped,
                        "median_group_size": level_report.median_group_size,
                    },
                    prefix=level_prefix,
                )
            )
            # Temps par méthode : nomenclature `seconds_{méthode}` de S-2.7
            metrics.update(
                flatten_metrics(
                    {f"seconds_{name}": value
                     for name, value in level_report.seconds.items()},
                    prefix=level_prefix,
                )
            )
        return metrics


# ──────────────────────────────────────────────────────────────────────
# Groupes et colonnes
# ──────────────────────────────────────────────────────────────────────

# Fonction de description des clés de groupement d'un niveau
def group_keys(level: str, config: SynthesisConfig) -> Tuple[str, ...]:
    """Return the grouping columns of a level, inside a context.

    Args:
        level: One of :data:`LEVELS`.
        config: Synthesis configuration.

    Returns:
        ``(product_col,)`` for ``by_product``, ``(reporter_col,)`` for
        ``by_reporter``, and the empty tuple for ``global`` (a single group).

    Raises:
        ValueError: If ``level`` is not one of :data:`LEVELS`.

    Examples:
        >>> group_keys("by_product", SynthesisConfig())
        ('product',)
        >>> group_keys("global", SynthesisConfig())
        ()
    """
    if level not in LEVELS:
        raise ValueError(f"Unknown level {level!r}. Available: {list(LEVELS)}.")
    if level == "by_product":
        return (config.product_col,)
    if level == "by_reporter":
        return (config.reporter_col,)
    return ()


# Générateur des groupes d'un niveau à l'intérieur d'un contexte
def iter_groups(
    df_context: pd.DataFrame, config: SynthesisConfig, level: str
) -> Iterator[Tuple[Any, np.ndarray]]:
    """Iterate over the groups of a level inside one context.

    Args:
        df_context: Rows of a single context.
        config: Synthesis configuration.
        level: One of :data:`LEVELS`.

    Yields:
        Tuples ``(group_value, positions)``, ``group_value`` being the value of
        the grouping key (:data:`GROUP_SENTINEL` at the global level) and
        ``positions`` the **positional** indices of the group rows inside
        ``df_context``.

    Raises:
        ValueError: If ``level`` is not one of :data:`LEVELS`.

    Examples:
        >>> df = pd.DataFrame({"reporter": ["FR", "FR", "DE"], "product": list("aba")})
        >>> [(value, positions.tolist())
        ...  for value, positions in iter_groups(df, SynthesisConfig(), "by_reporter")]
        [('FR', [0, 1]), ('DE', [2])]
    """
    keys = group_keys(level, config)
    if not keys:
        yield GROUP_SENTINEL, np.arange(len(df_context))
        return
    # `indices` renvoie directement les positions, sans copie des lignes
    for value, positions in df_context.groupby(
        list(keys), sort=False, observed=True
    ).indices.items():
        yield value, np.asarray(positions)


# Fonction de description des colonnes de la table des scores
def score_columns(config: SynthesisConfig) -> Tuple[str, ...]:
    """Return the columns of the score table, in the order of S-2.4.

    Args:
        config: Synthesis configuration.

    Returns:
        Context keys, cell keys, ``method``, then the ``score_``, ``rank_``,
        ``n_``, ``alert_`` and ``rank_low_`` / ``rank_high_`` blocks, one entry
        per level of :data:`LEVELS` — the schema is fixed, whatever the levels
        the run covers, so the target table never migrates (D-01).

    Examples:
        >>> score_columns(SynthesisConfig())[:7]
        ('freq', 'flow', 'indicators', 'TIME_PERIOD', 'reporter', 'product', 'method')
    """
    bounds: List[str] = []
    for level in LEVELS:
        bounds.extend((f"rank_low_{level}", f"rank_high_{level}"))
    return (
        *config.context_columns,
        config.reporter_col,
        config.product_col,
        "method",
        *(f"score_{level}" for level in LEVELS),
        *(f"rank_{level}" for level in LEVELS),
        *(f"n_{level}" for level in LEVELS),
        *(f"alert_{level}" for level in LEVELS),
        *bounds,
    )


# ──────────────────────────────────────────────────────────────────────
# Utilitaires internes
# ──────────────────────────────────────────────────────────────────────

# Fonction de dérivation de la graine d'un groupe
def _group_seed(
    config: SynthesisConfig, context: Tuple[Any, ...], level: str, group: Any
) -> int:
    """Derive the deterministic seed of one group.

    The seed drives the Dirichlet draws of SMAA and of the cone quantile — the
    two methods share it, and therefore share their draws (A-01) — and the
    bootstrap resampling. Deriving it from the group identity rather than
    reusing ``config.random_state`` everywhere keeps the runs reproducible
    while leaving the draws of two groups independent.

    Args:
        config: Synthesis configuration carrying ``random_state``.
        context: Values of the context keys.
        level: Level of the group.
        group: Value of the grouping key.

    Returns:
        A seed in ``[0, 2**32)``.

    Examples:
        >>> seed = _group_seed(SynthesisConfig(), ("A", 1), "by_product", "85411000")
        >>> seed == _group_seed(SynthesisConfig(), ("A", 1), "by_product", "85411000")
        True
    """
    payload = repr((config.random_state, context, level, group)).encode("utf-8")
    return int(zlib.crc32(payload))


# Fonction d'injection de l'aléa et des tailles de tirage d'un groupe
def _seeded_spec(spec: MethodSpec, config: SynthesisConfig, seed: int) -> MethodSpec:
    """Return a copy of ``spec`` whose random parameters are those of the group.

    Args:
        spec: Configured method.
        config: Synthesis configuration (draw counts).
        seed: Seed of the group.

    Returns:
        The same spec when the method takes no random parameter, a copy with
        ``params`` completed otherwise.
    """
    if spec.kind not in SEED_PARAMETERS and spec.kind not in DRAW_PARAMETERS:
        return spec
    params: Dict[str, Any] = dict(spec.params)
    # La graine du groupe prime : c'est elle qui fait partager les tirages
    if spec.kind in SEED_PARAMETERS:
        params[SEED_PARAMETERS[spec.kind]] = seed
    if spec.kind in DRAW_PARAMETERS:
        params.setdefault(DRAW_PARAMETERS[spec.kind], config.smaa_n_draws)
    if spec.kind == "smaa":
        params.setdefault("k", config.smaa_k)
    return replace(spec, params=params)


# Fonction d'extraction des alertes d'un pipeline ajusté
def _alert_values(pipeline: Pipeline, X: np.ndarray) -> Optional[np.ndarray]:
    """Compute the alert flags of a fitted pipeline, when it defines any.

    Args:
        pipeline: Fitted pipeline whose last step may expose ``alert``.
        X: Raw metric matrix of the scored rows.

    Returns:
        Boolean array of shape ``(n,)``, or ``None`` when the method defines
        no alert.
    """
    estimator = pipeline[-1]
    if not hasattr(estimator, "alert"):
        return None
    return np.asarray(estimator.alert(pipeline[:-1].transform(X)), dtype=bool)


# Fonction de création des tampons de résultat d'une méthode
def _new_buffers(n_rows: int) -> Dict[str, np.ndarray]:
    """Allocate the per-method result buffers of one context.

    Args:
        n_rows: Number of cells in the context.

    Returns:
        Mapping of column name to an array of ``n_rows`` entries, filled with
        ``NaN`` (``None`` for the alert columns).
    """
    buffers: Dict[str, np.ndarray] = {}
    for level in LEVELS:
        for prefix in ("score", "rank", "n", "rank_low", "rank_high"):
            buffers[f"{prefix}_{level}"] = np.full(n_rows, np.nan)
        buffers[f"alert_{level}"] = np.full(n_rows, None, dtype=object)
    return buffers


# ──────────────────────────────────────────────────────────────────────
# Ajustement d'une méthode sur un groupe
# ──────────────────────────────────────────────────────────────────────

# Contexte d'exécution d'un groupe, transmis aux fonctions d'ajustement
@dataclass
class _GroupContext:
    """Everything one group needs, gathered to keep signatures short.

    Attributes:
        level: Level of the group.
        positions: Positional indices of the group rows inside the context.
        seed: Deterministic seed of the group.
        base: Identifying fields of a diagnostic row (context, level, group).
        n_rows: Number of rows in the group.
    """
    level: str
    positions: np.ndarray
    seed: int
    base: Dict[str, Any]
    n_rows: int


# Fonction d'émission d'une ligne de diagnostic d'ajustement
def _diagnostic_row(
    group: _GroupContext,
    statistic: str,
    value: float,
    *,
    item_a: str = ITEM_SENTINEL,
    item_b: str = ITEM_SENTINEL,
    n: Optional[int] = None,
) -> Dict[str, Any]:
    """Build one long-format diagnostic row of the ``fit`` family (S-2.6).

    Args:
        group: Execution context of the group.
        statistic: Name of the statistic.
        value: Numeric value.
        item_a: Method the statistic refers to, ``''`` when none.
        item_b: Metric the statistic refers to, ``''`` when none.
        n: Group size; the number of rows of the group by default.

    Returns:
        A mapping ready to be appended to the diagnostic accumulator.
    """
    return {
        **group.base,
        "family": FIT_FAMILY,
        "statistic": statistic,
        "item_a": item_a,
        "item_b": item_b,
        "value": float(value),
        "n": int(group.n_rows if n is None else n),
    }


# Fonction d'émission des diagnostics d'ajustement d'un estimateur
def _fit_diagnostics(
    estimator: Any,
    spec: MethodSpec,
    metrics: Sequence[str],
    group: _GroupContext,
    X_complete: np.ndarray,
    pipeline: Pipeline,
    n_scored: int,
) -> List[Dict[str, Any]]:
    """Collect the ``fit``-family diagnostics of one fitted method (S-2.5.c).

    Args:
        estimator: Last step of the fitted pipeline.
        spec: Configured method.
        metrics: Metric columns the method was fitted on.
        group: Execution context of the group.
        X_complete: Raw matrix of the scored rows.
        pipeline: The fitted pipeline (its preprocessing feeds the OT report).
        n_scored: Number of scored rows.

    Returns:
        The diagnostic rows: weights per metric, ``auto`` selection report and
        optimal-transport convergence, calibration and orientation.
    """
    rows: List[Dict[str, Any]] = []

    # Poids partagés : une ligne par métrique (S-2.5.c). Les pondérations
    # individualisées du BoD sont une matrice (n, d) et n'y figurent pas (D-04)
    weights = getattr(estimator, "weights_", None)
    if weights is not None and np.ndim(weights) == 1:
        for name, value in zip(metrics, np.asarray(weights, dtype=float)):
            rows.append(
                _diagnostic_row(
                    group, "weight", value, item_a=spec.name, item_b=name, n=n_scored
                )
            )

    # Méta-sélection de la pondération (A-05) : schéma retenu et ses diagnostics
    report = getattr(estimator, "weighting_report_", None)
    if isinstance(report, AutoWeightingReport):
        rows.append(
            _diagnostic_row(
                group,
                f"auto_selected__{report.selected}",
                1.0,
                item_a=spec.name,
                n=n_scored,
            )
        )
        for statistic, value in (
            ("kmo", report.kmo),
            ("axis1_share", report.axis1_share),
        ):
            rows.append(
                _diagnostic_row(
                    group, statistic, value, item_a=spec.name, n=n_scored
                )
            )

    # Transport optimal : convergence, uniformité, seuil et sensibilité de u*
    if hasattr(estimator, "fit_report"):
        ot_report = estimator.fit_report(pipeline[:-1].transform(X_complete))
        for statistic, value in (
            ("ot_converged", float(ot_report.converged)),
            ("ot_ks_p", ot_report.uniformity_ks_p_value),
            ("ot_alert_threshold", ot_report.alert_threshold),
            ("ot_alert_share", ot_report.alert_share),
            ("ot_direction_cos_q90_q99", ot_report.direction_cos),
        ):
            rows.append(
                _diagnostic_row(
                    group, statistic, value, item_a=spec.name, n=n_scored
                )
            )
    return rows


# Fonction de calcul des bornes bootstrap d'une méthode sur un groupe (D-08)
def _bootstrap_bounds(
    spec: MethodSpec,
    config: SynthesisConfig,
    metrics: Sequence[str],
    X_complete: np.ndarray,
    seed: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """Bootstrap the rank interval of one method on one group (D-08).

    Args:
        spec: Configured method, already carrying the group seed.
        config: Synthesis configuration (draws and interval width).
        metrics: Metric columns of the method.
        X_complete: Raw matrix of the scored rows.
        seed: Seed of the resampling generator.

    Returns:
        Tuple ``(rank_low, rank_high)``, both of shape ``(n_scored,)``.
    """
    # Import local : `diagnostics` importe `base`, pas de cycle mais un coût
    from .diagnostics import bootstrap_rank_stability

    identifiers = np.arange(X_complete.shape[0])
    df_group = pd.DataFrame(X_complete, columns=list(metrics))
    df_group["__cell__"] = identifiers
    aggregation_config = AggregationConfig(
        id_columns=("__cell__",), metric_columns=tuple(metrics)
    )
    result = bootstrap_rank_stability(
        df_group,
        aggregation_config,
        lambda _config: build_method(spec, config),
        n_boot=config.bootstrap_n,
        ci=config.bootstrap_ci,
        random_state=seed,
    )
    return result["rank_low"].to_numpy(), result["rank_high"].to_numpy()


# ──────────────────────────────────────────────────────────────────────
# Runner de synthèse (S-1.6)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'exécution de la synthèse multiniveau
def run_synthesis(
    df_metrics: pd.DataFrame,
    config: SynthesisConfig,
    *,
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, SynthesisReport]:
    """Score every cell of every context, at every configured level.

    The loop runs context -> level -> group -> method. Inside a context, the
    three levels write into the **same** per-method buffers at disjoint
    positions, so a single frame is built per context and a single
    concatenation closes the run.

    Per method and per group (D-15), only the rows complete over the metrics
    of the method are fitted and scored; the others carry ``NaN`` on both the
    score and the rank (D-16). A method whose group holds fewer scored rows
    than its ``min_group_size`` is skipped with a ``skipped_min_group_size``
    diagnostic, and so is the Kantorovitch score when ``jax`` is missing
    (``skipped_missing_dependency``) or when the dimension exceeds
    ``ot_dimension_limit`` (``skipped_dimension_limit``).

    Args:
        df_metrics: Cells by metrics, already filtered on the contexts of
            interest; must carry the context columns, the reporter and product
            columns and the metric columns.
        config: Synthesis configuration.
        tracker: Experiment tracker receiving the artifacts; the null tracker
            by default, so the runner stays side-effect free.
        log_artifacts: Whether to send the artifacts of S-2.7 to the tracker.

    Returns:
        Tuple ``(df_scores, df_fit_diagnostics, report)``: the long score table
        of S-2.4, the ``fit``-family diagnostics of S-2.5.c in the long format
        of S-2.6, and the :class:`SynthesisReport`.

    Raises:
        KeyError: If a configured column is absent from ``df_metrics``.
        ValueError: If a configured level or method is unknown, or if a
            method asks for an unavailable normalisation.

    Examples:
        >>> df = pd.DataFrame({
        ...     "freq": ["A"] * 4, "flow": [1] * 4,
        ...     "indicators": ["V"] * 4, "TIME_PERIOD": ["2024"] * 4,
        ...     "reporter": ["FR", "FR", "DE", "DE"],
        ...     "product": ["a", "b", "a", "b"],
        ...     "HHI": [0.8, 0.2, 0.5, 0.4], "CDI2": [0.7, 0.3, 0.6, 0.1],
        ... })
        >>> config = SynthesisConfig(
        ...     metric_columns=("HHI", "CDI2"), levels=("global",),
        ...     methods=(MethodSpec(name="mpi", kind="mpi"),), consensus=(),
        ... )
        >>> df_scores, df_fit, report = run_synthesis(df, config)
        >>> int(df_scores["n_global"].iloc[0]), report.n_contexts
        (4, 1)
    """
    _check_columns(df_metrics, config)
    # Vérification immédiate des niveaux et des consensus : une faute de frappe
    # ne doit pas attendre le premier groupe pour être signalée
    for level in config.levels:
        group_keys(level, config)
    unknown = [rule for rule in config.consensus if rule not in _CONSENSUS_FUNCTIONS]
    if unknown:
        raise ValueError(
            f"Unknown consensus rule(s) {unknown}. "
            f"Available: {sorted(_CONSENSUS_FUNCTIONS)}."
        )

    report = SynthesisReport(n_cells=int(len(df_metrics)))
    report.levels = {level: LevelReport() for level in config.levels}
    group_sizes: Dict[str, List[int]] = {level: [] for level in config.levels}

    score_frames: List[pd.DataFrame] = []
    diagnostic_rows: List[Dict[str, Any]] = []
    # Avertissement de dépendance absente émis une seule fois par exécution
    warned_dependencies: set = set()

    context_columns = list(config.context_columns)
    for context_value, df_context in df_metrics.groupby(
        context_columns, sort=False, observed=True
    ):
        context = context_value if isinstance(context_value, tuple) else (context_value,)
        report.n_contexts += 1
        n_rows = len(df_context)
        method_names = [spec.name for spec in config.methods] + [
            f"{CONSENSUS_PREFIX}{rule}" for rule in config.consensus
        ]
        buffers = {name: _new_buffers(n_rows) for name in method_names}
        # Matrices brutes mises en cache par jeu de métriques (une par méthode au plus)
        matrices: Dict[Tuple[str, ...], np.ndarray] = {}

        for level in config.levels:
            for group_value, positions in iter_groups(df_context, config, level):
                group = _GroupContext(
                    level=level,
                    positions=positions,
                    seed=_group_seed(config, context, level, group_value),
                    base=_diagnostic_base(context, config, level, group_value),
                    n_rows=len(positions),
                )
                report.levels[level].n_groups += 1
                group_sizes[level].append(len(positions))

                scores_of_group: Dict[str, np.ndarray] = {}
                for spec in config.methods:
                    _score_method(
                        spec=spec,
                        config=config,
                        df_context=df_context,
                        matrices=matrices,
                        group=group,
                        buffers=buffers[spec.name],
                        scores_of_group=scores_of_group,
                        diagnostic_rows=diagnostic_rows,
                        level_report=report.levels[level],
                        warned_dependencies=warned_dependencies,
                    )
                _score_consensus(config, group, buffers, scores_of_group)

        score_frames.append(_context_frame(df_context, config, context, buffers))
        # Logging
        logger.debug(
            f"Contexte {context} synthétisé : {n_rows} cellules, "
            f"{len(method_names)} méthodes."
        )

    for level, sizes in group_sizes.items():
        if sizes:
            report.levels[level].median_group_size = float(np.median(sizes))

    df_scores = _concat_scores(score_frames, config)
    df_fit_diagnostics = _diagnostics_frame(diagnostic_rows, config)

    if log_artifacts:
        log_synthesis_artifacts(tracker, df_scores, df_fit_diagnostics, config)
    return df_scores, df_fit_diagnostics, report


# Fonction de vérification des colonnes attendues
def _check_columns(df_metrics: pd.DataFrame, config: SynthesisConfig) -> None:
    """Check that every configured column is present.

    Args:
        df_metrics: Input table.
        config: Synthesis configuration.

    Raises:
        KeyError: If a context, cell or metric column is absent.
    """
    expected = (
        *config.context_columns,
        config.reporter_col,
        config.product_col,
        *config.metric_columns,
    )
    missing = [column for column in expected if column not in df_metrics.columns]
    if missing:
        raise KeyError(
            f"Column(s) {missing} are absent from the metric table. "
            f"Available columns: {list(df_metrics.columns)}."
        )


# Fonction de construction des champs identifiants d'une ligne de diagnostic
def _diagnostic_base(
    context: Tuple[Any, ...], config: SynthesisConfig, level: str, group_value: Any
) -> Dict[str, Any]:
    """Build the identifying fields of the diagnostic rows of one group.

    Args:
        context: Values of the context keys.
        config: Synthesis configuration.
        level: Level of the group.
        group_value: Value of the grouping key.

    Returns:
        Mapping of the context columns plus ``level``, ``reporter`` and
        ``product``, the latter two carrying :data:`GROUP_SENTINEL` when they
        do not identify the group (S-2.6: never ``NULL``).
    """
    base: Dict[str, Any] = dict(zip(config.context_columns, context))
    base["level"] = level
    base[config.reporter_col] = (
        group_value if level == "by_reporter" else GROUP_SENTINEL
    )
    base[config.product_col] = (
        group_value if level == "by_product" else GROUP_SENTINEL
    )
    return base


# Fonction d'ajustement et de notation d'une méthode sur un groupe
def _score_method(
    *,
    spec: MethodSpec,
    config: SynthesisConfig,
    df_context: pd.DataFrame,
    matrices: Dict[Tuple[str, ...], np.ndarray],
    group: _GroupContext,
    buffers: Dict[str, np.ndarray],
    scores_of_group: Dict[str, np.ndarray],
    diagnostic_rows: List[Dict[str, Any]],
    level_report: LevelReport,
    warned_dependencies: set,
) -> None:
    """Fit one method on one group and write its scores into the buffers.

    Args:
        spec: Configured method.
        config: Synthesis configuration.
        df_context: Rows of the current context.
        matrices: Cache of raw metric matrices, keyed by metric tuple.
        group: Execution context of the group.
        buffers: Result buffers of this method, for the whole context.
        scores_of_group: Accumulator of the group scores, feeding the
            consensus.
        diagnostic_rows: Accumulator of the ``fit``-family diagnostics.
        level_report: Level summary, updated in place.
        warned_dependencies: Kinds an optional-dependency warning has already
            been issued for.
    """
    level = group.level
    # Niveaux d'application de la méthode : rien n'est écrit ailleurs
    if spec.levels is not None and level not in spec.levels:
        return

    entry = registry_entry(spec.kind)
    metrics = method_metrics(spec, config)
    if metrics not in matrices:
        matrices[metrics] = df_context.loc[:, list(metrics)].to_numpy(dtype=float)
    X_group = matrices[metrics][group.positions]

    # Lignes complètes sur les métriques de la méthode (D-15)
    complete = np.isfinite(X_group).all(axis=1)
    n_scored = int(complete.sum())
    n_incomplete = int(group.n_rows - n_scored)
    if n_incomplete:
        diagnostic_rows.append(
            _diagnostic_row(
                group,
                "n_skipped_incomplete",
                float(n_incomplete),
                item_a=spec.name,
                n=n_scored,
            )
        )

    positions_scored = group.positions[complete]
    # Taille minimale : surcharge de la méthode, sinon défaut de la méthode
    # (S-1.4) relevé au plancher global de la configuration
    minimum = (
        spec.min_group_size
        if spec.min_group_size is not None
        else max(entry.min_group_size, config.min_group_size)
    )

    # Méthode sautée : `n` renseigné à la taille du groupe, score et rang NaN
    def skip(statistic: str) -> None:
        """Record a skipped method and its diagnostic line.

        Args:
            statistic: Name of the skip diagnostic.
        """
        buffers[f"n_{level}"][group.positions] = float(group.n_rows)
        level_report.n_methods_skipped += 1
        diagnostic_rows.append(
            _diagnostic_row(group, statistic, 1.0, item_a=spec.name)
        )

    if n_scored < minimum:
        skip("skipped_min_group_size")
        return
    if spec.kind == "kantorovich" and len(metrics) > config.ot_dimension_limit:
        skip("skipped_dimension_limit")
        return

    X_complete = X_group[complete]
    seeded = _seeded_spec(spec, config, group.seed)
    started = time.perf_counter()
    try:
        pipeline = build_method(seeded, config)
        scores = np.asarray(pipeline.fit(X_complete).predict(X_complete), dtype=float)
    except ImportError as error:
        # Dépendance optionnelle absente (jax / ott-jax) : saut documenté
        if spec.kind not in warned_dependencies:
            warned_dependencies.add(spec.kind)
            warnings.warn(
                f"Method kind {spec.kind!r} skipped: {error}", RuntimeWarning
            )
        skip("skipped_missing_dependency")
        return
    elapsed = time.perf_counter() - started
    level_report.seconds[spec.name] = (
        level_report.seconds.get(spec.name, 0.0) + elapsed
    )
    level_report.n_cells_scored += n_scored

    # Rangs : 1 = plus vulnérable, ex aequo moyennés, NaN propagés (D-16)
    ranks = stats.rankdata(-scores, method=config.rank_ties)
    buffers[f"score_{level}"][positions_scored] = scores
    buffers[f"rank_{level}"][positions_scored] = ranks
    buffers[f"n_{level}"][group.positions] = float(n_scored)

    if entry.supports_alert:
        alerts = _alert_values(pipeline, X_complete)
        if alerts is not None:
            buffers[f"alert_{level}"][positions_scored] = alerts

    # Score du groupe, aligné sur les positions du groupe, pour le consensus
    group_scores = np.full(group.n_rows, np.nan)
    group_scores[complete] = scores
    scores_of_group[spec.name] = group_scores

    diagnostic_rows.extend(
        _fit_diagnostics(
            pipeline[-1], spec, metrics, group, X_complete, pipeline, n_scored
        )
    )

    # Intervalle de rang par bootstrap (D-08), méthodes à état ajusté seulement
    if spec.name in config.bootstrap_methods and level in config.bootstrap_levels:
        if not entry.has_fitted_state:
            diagnostic_rows.append(
                _diagnostic_row(
                    group, "skipped_bootstrap_stateless", 1.0, item_a=spec.name
                )
            )
        else:
            low, high = _bootstrap_bounds(
                seeded, config, metrics, X_complete, group.seed
            )
            buffers[f"rank_low_{level}"][positions_scored] = low
            buffers[f"rank_high_{level}"][positions_scored] = high


# Fonction de calcul des pseudo-méthodes de consensus d'un groupe
def _score_consensus(
    config: SynthesisConfig,
    group: _GroupContext,
    buffers: Dict[str, Dict[str, np.ndarray]],
    scores_of_group: Mapping[str, np.ndarray],
) -> None:
    """Compute the consensus rankings of one group (S-2.4).

    The consensus runs on the methods actually scored in the group, restricted
    to the rows every one of them scored; its score is the opposite of its
    rank, so that the convention "higher = more vulnerable" holds for the
    pseudo-methods too (D-16).

    Args:
        config: Synthesis configuration (rules and preselection size).
        group: Execution context of the group.
        buffers: Result buffers of every method of the context.
        scores_of_group: Score vectors of the group, keyed by method name.
    """
    if not config.consensus or len(scores_of_group) < 2:
        return
    stacked = np.column_stack(list(scores_of_group.values()))
    complete = np.isfinite(stacked).all(axis=1)
    if complete.sum() < 2:
        return
    common = {
        name: values[complete] for name, values in scores_of_group.items()
    }
    positions = group.positions[complete]
    level = group.level
    for rule in config.consensus:
        if rule not in _CONSENSUS_FUNCTIONS:
            raise ValueError(
                f"Unknown consensus rule {rule!r}. "
                f"Available: {sorted(_CONSENSUS_FUNCTIONS)}."
            )
        ranks = _CONSENSUS_FUNCTIONS[rule](common, config.consensus_top_n)
        method_buffers = buffers[f"{CONSENSUS_PREFIX}{rule}"]
        method_buffers[f"rank_{level}"][positions] = ranks
        method_buffers[f"score_{level}"][positions] = -np.asarray(ranks, dtype=float)
        method_buffers[f"n_{level}"][group.positions] = float(complete.sum())


# Fonction d'assemblage du cadre long d'un contexte
def _context_frame(
    df_context: pd.DataFrame,
    config: SynthesisConfig,
    context: Tuple[Any, ...],
    buffers: Mapping[str, Dict[str, np.ndarray]],
) -> pd.DataFrame:
    """Assemble the long frame of one context, one block per method.

    Args:
        df_context: Rows of the context.
        config: Synthesis configuration.
        context: Values of the context keys.
        buffers: Result buffers, keyed by method name.

    Returns:
        A frame of ``len(df_context) * len(buffers)`` rows carrying the columns
        of :func:`score_columns`.
    """
    identity = {
        column: np.repeat(value, len(df_context))
        for column, value in zip(config.context_columns, context)
    }
    identity[config.reporter_col] = df_context[config.reporter_col].to_numpy()
    identity[config.product_col] = df_context[config.product_col].to_numpy()

    frames: List[pd.DataFrame] = []
    for method_name, method_buffers in buffers.items():
        frame = pd.DataFrame(
            {
                **identity,
                "method": method_name,
                **{column: values.copy() for column, values in method_buffers.items()},
            }
        )
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


# Fonction d'assemblage et de typage de la table des scores
def _concat_scores(
    frames: Sequence[pd.DataFrame], config: SynthesisConfig
) -> pd.DataFrame:
    """Concatenate the per-context frames and enforce the S-2.4 schema.

    Args:
        frames: One frame per context.
        config: Synthesis configuration.

    Returns:
        The score table, columns ordered as in S-2.4, ``n_*`` as nullable
        integers and ``alert_*`` as nullable booleans.
    """
    columns = list(score_columns(config))
    if not frames:
        return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})
    df_scores = pd.concat(frames, ignore_index=True)[columns]
    for level in LEVELS:
        df_scores[f"n_{level}"] = df_scores[f"n_{level}"].astype("Int64")
        df_scores[f"alert_{level}"] = df_scores[f"alert_{level}"].astype("boolean")
    return df_scores


# Fonction d'assemblage de la table longue des diagnostics d'ajustement
def _diagnostics_frame(
    rows: Sequence[Mapping[str, Any]], config: SynthesisConfig
) -> pd.DataFrame:
    """Build the ``fit``-family diagnostic table (S-2.6).

    Args:
        rows: Accumulated diagnostic rows.
        config: Synthesis configuration.

    Returns:
        A frame carrying the context columns then the columns of
        :data:`DIAGNOSTIC_COLUMNS`, with the sentinels of S-2.6 in place of
        every missing group or item.
    """
    columns = [*config.context_columns, *DIAGNOSTIC_COLUMNS]
    # Les colonnes de groupe portent les noms configurés, pas les noms canoniques
    columns = [
        config.reporter_col if column == "reporter" else column for column in columns
    ]
    columns = [
        config.product_col if column == "product" else column for column in columns
    ]
    if not rows:
        return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})
    df_diagnostics = pd.DataFrame(list(rows))
    return df_diagnostics[columns]


# ──────────────────────────────────────────────────────────────────────
# Artefacts (S-2.7)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'envoi des artefacts de synthèse au suivi d'expérience
def log_synthesis_artifacts(
    tracker: RunTracker,
    df_scores: pd.DataFrame,
    df_fit_diagnostics: pd.DataFrame,
    config: SynthesisConfig,
) -> None:
    """Send the artifacts of S-2.7 to the tracker.

    Four families: the ``artifact_top_n`` most vulnerable cells per level and
    per method, the fitted weights, the ``auto`` selection reports and the
    optimal-transport report, each per level.

    Args:
        tracker: Tracker receiving the tables; the null tracker discards them.
        df_scores: Score table produced by :func:`run_synthesis`.
        df_fit_diagnostics: Diagnostic table produced by :func:`run_synthesis`.
        config: Synthesis configuration.
    """
    for level in config.levels:
        rank_column = f"rank_{level}"
        for method_name, frame in df_scores.groupby("method", sort=False):
            ranked = frame.dropna(subset=[rank_column])
            if ranked.empty:
                continue
            top = ranked.nsmallest(config.artifact_top_n, rank_column)
            tracker.log_table(top, f"synthesis/top_{level}_{method_name}.csv")

        if df_fit_diagnostics.empty:
            continue
        at_level = df_fit_diagnostics[df_fit_diagnostics["level"] == level]
        statistics = at_level["statistic"]
        for name, mask in (
            ("weights", statistics == "weight"),
            ("auto_selection", statistics.str.startswith("auto_selected__")),
            ("ot_report", statistics.str.startswith("ot_")),
        ):
            selection = at_level[mask]
            if not selection.empty:
                tracker.log_table(selection, f"synthesis/{name}_{level}.csv")
