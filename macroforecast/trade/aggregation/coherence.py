"""Coherence runner: how much do the metrics, and the syntheses, agree?

:func:`~macroforecast.trade.aggregation.synthesis.run_synthesis` produces one
score and one rank per cell and per method (S-2.4); it never says whether those
methods rank the cells alike, nor whether the input metrics measure the same
thing. This module answers both questions and writes the answers into the long
diagnostic table of S-2.6, in a schema of its own (D-02), at the group
granularity: ``context x level x group x family x statistic x item_a x item_b``.

Two families are emitted. ``metrics`` (S-2.5.a) measures the coherence of the
vulnerability metrics with one another: rank correlations, factorial structure
(KMO, Bartlett, share of the first axis) and the share of the Pareto front,
compared with its expected value under independence. ``methods`` (S-2.5.b)
measures the coherence of the syntheses: Kendall's ``tau_b`` and Vigna's
weighted ``tau``, rank-biased overlap, top-``k`` overlap, the concordance ``W``,
the strict and tie dominance-violation rates (M-10), where each method places
the front members, the share of disputed cells, the width of the bootstrap rank
intervals, the leave-one-metric-out sensitivity, the score-metric ``tau``, the
ellipticity diagnostic and the method clusters.

:func:`run_coherence` is a pure ``DataFrames -> DataFrame + report`` function
(D-17): no DuckLake connection, no S3, no side effect beyond the optional
tracker, so it becomes a Kedro node without a rewrite.

Not to be confused with
:class:`~macroforecast.trade.aggregation.diagnostics.CoherenceReport`, which
summarises **one** group in memory; :class:`CoherenceRunReport` summarises a
whole run and only carries the aggregates MLflow needs (S-2.7).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field, replace
from itertools import combinations
import logging
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import warnings
# Modules de manipulation de données
import numpy as np
import pandas as pd
from scipy import stats
from scipy.cluster.hierarchy import fcluster
# Modules du package
from ...tracking import NULL_TRACKER, RunTracker, flatten_metrics
from .diagnostics import (
    cluster_methods,
    dominance_violation_rate,
    front_rank_summary,
    kendall_tau_b_matrix,
    kendall_w,
    rank_biased_overlap,
    topk_overlap,
    weighted_tau_matrix,
)
from .methods import build_method, method_metrics, registry_entry
from .pareto import pareto_front_sweep
from .preprocessing import compute_correlation_diagnostics, spearman_correlation_matrix
from .synthesis import (
    DIAGNOSTIC_COLUMNS,
    ITEM_SENTINEL,
    SynthesisConfig,
    _diagnostic_base as diagnostic_base,
    iter_groups,
)

# Initialisation du logger
logger = logging.getLogger(__name__)

# Familles de diagnostics émises par ce runner (S-2.5.a et S-2.5.b)
METRICS_FAMILY = "metrics"
METHODS_FAMILY = "methods"
COHERENCE_FAMILIES: Tuple[str, ...] = (METRICS_FAMILY, METHODS_FAMILY)

# Seuil de coupure de la CAH sur `1 - tau_b`, exprimé en distance (S-2.5.b)
CLUSTER_THRESHOLD = 0.2

# Nombre de lignes au-delà duquel le coût du leave-one-metric-out est signalé
LOMO_WARN_ROWS = 20_000

# Nombre minimal de lignes complètes exigé par les statistiques de corrélation
MIN_ROWS_CORRELATION = 3

# Seuil du facteur de confiance SMAA au-delà duquel une cellule est comptée
SMAA_CONFIDENCE_THRESHOLD = 0.5

# Statistiques d'ellipticité, lues dans les diagnostics d'ajustement ou recalculées
ELLIPTICITY_STATISTICS: Mapping[str, str] = {
    "tau_proj": "ellipticity_tau_proj",
    "tau_rank": "ellipticity_tau_rank",
}


# ──────────────────────────────────────────────────────────────────────
# Configuration (S-1.2)
# ──────────────────────────────────────────────────────────────────────

# Paramètres du script de cohérence
@dataclass(frozen=True)
class CoherenceConfig:
    """Parameters of the coherence run (S-1.2).

    Attributes:
        topk_depths: Depths ``k`` at which the top-``k`` overlaps are reported;
            the second entry drives the leave-one-metric-out overlap.
        rbo_p: Persistence of the rank-biased overlap; the expected rank
            examined is ``1 / (1 - rbo_p)``.
        dispute_fraction: A cell is disputed when its rank spread across the
            methods exceeds ``dispute_fraction * n``.
        lomo: Whether to refit the methods of ``lomo_methods`` without each
            metric in turn.
        lomo_methods: Names of the methods the leave-one-metric-out applies to;
            the cost is ``d`` refits per method and per group.
        metric_pairs: Whether to emit the pairwise ``spearman`` and
            ``kendall_tau_b`` of the ``metrics`` family.

    Examples:
        >>> CoherenceConfig().topk_depths
        (10, 50, 100)
    """
    topk_depths: Tuple[int, ...] = (10, 50, 100)
    rbo_p: float = 0.98
    dispute_fraction: float = 0.01
    lomo: bool = False
    lomo_methods: Tuple[str, ...] = ()
    metric_pairs: bool = True


# ──────────────────────────────────────────────────────────────────────
# Rapport d'exécution (S-2.7)
# ──────────────────────────────────────────────────────────────────────

# Résumé de la cohérence d'un niveau de comparaison
@dataclass
class CoherenceLevelReport:
    """Summary of the coherence of one comparison level.

    Every field but ``n_groups`` is a median over the groups of the level: the
    groups of a level are of very unequal size (a product traded by three
    countries and one traded by two hundred), so a median states the typical
    group where a mean would state the largest one.

    Attributes:
        n_groups: Number of groups met at this level, every context together.
        kendall_w: Median concordance of the methods over the groups.
        disputed_share: Median share of disputed cells.
        mean_abs_rho: Median mean absolute Spearman correlation of the metrics.
        violation_strict: Median strict dominance-violation rate, per method.
    """
    n_groups: int = 0
    kendall_w: float = float("nan")
    disputed_share: float = float("nan")
    mean_abs_rho: float = float("nan")
    violation_strict: Dict[str, float] = field(default_factory=dict)


# Résumé d'une exécution de cohérence
@dataclass
class CoherenceRunReport:
    """Summary of a coherence run (S-2.7).

    Attributes:
        n_contexts: Number of contexts analysed.
        n_groups: Total number of groups analysed, every level together.
        created: Whether the result schema was created; set by the calling
            script, the runner performing no I/O (D-17).
        levels: One :class:`CoherenceLevelReport` per level, keyed by level.

    Examples:
        >>> report = CoherenceRunReport(n_contexts=2, n_groups=40)
        >>> report.to_metrics()["coherence.n_groups"]
        40.0
    """
    n_contexts: int = 0
    n_groups: int = 0
    created: bool = False
    levels: Dict[str, CoherenceLevelReport] = field(default_factory=dict)

    # Mise en forme des métriques (la seule à connaître les contraintes MLflow)
    def to_metrics(self, prefix: str = "coherence") -> Dict[str, float]:
        """Flatten every numeric field into a dotted metric mapping.

        Produces ``...n_contexts``, ``...n_groups``, ``...created`` and, per
        level, ``...global.kendall_w``, ``...global.disputed_share``,
        ``...global.mean_abs_rho`` and ``...global.violation_strict_{method}``
        (S-2.7). ``NaN`` and infinite values are dropped, MLflow rejecting them.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> report = CoherenceRunReport()
            >>> report.levels["global"] = CoherenceLevelReport(
            ...     n_groups=1, kendall_w=0.8, violation_strict={"mpi": 0.02}
            ... )
            >>> report.to_metrics()["coherence.global.violation_strict_mpi"]
            0.02
        """
        metrics: Dict[str, float] = {}
        metrics.update(
            flatten_metrics(
                {
                    "n_contexts": self.n_contexts,
                    "n_groups": self.n_groups,
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
                        "kendall_w": level_report.kendall_w,
                        "disputed_share": level_report.disputed_share,
                        "mean_abs_rho": level_report.mean_abs_rho,
                    },
                    prefix=level_prefix,
                )
            )
            # Taux de violation par méthode : nomenclature `violation_strict_{méthode}`
            metrics.update(
                flatten_metrics(
                    {
                        f"violation_strict_{name}": value
                        for name, value in level_report.violation_strict.items()
                    },
                    prefix=level_prefix,
                )
            )
        return metrics


# ──────────────────────────────────────────────────────────────────────
# Données d'un groupe, partagées par les deux familles
# ──────────────────────────────────────────────────────────────────────

# Tout ce dont un groupe a besoin, extrait et orienté une seule fois
@dataclass
class _GroupData:
    """Everything the two families of one group share.

    The oriented matrix and the Pareto front are the two expensive objects; the
    ``metrics`` family needs them (front share, correlations) and so does the
    ``methods`` family (violation rates, front ranks), so both are built once
    per group and handed to both.

    Attributes:
        base: Identifying fields of a diagnostic row (context, level, group).
        level: Level of the group.
        positions: Positional indices of the group rows inside the context.
        X: Oriented matrix of the complete rows, shape ``(n_complete, d)``.
        complete: Boolean mask of the complete rows, shape ``(n_rows,)``.
        n_rows: Number of rows in the group.
        n_complete: Number of rows complete over every metric.
        scores: Score vector per method, aligned on the rows of ``X``.
    """
    base: Dict[str, Any]
    level: str
    positions: np.ndarray
    X: np.ndarray
    complete: np.ndarray
    n_rows: int
    n_complete: int
    scores: Dict[str, np.ndarray] = field(default_factory=dict)
    _front: Optional[np.ndarray] = None

    # Front de Pareto exact, mémoïsé : partagé par les deux familles
    @property
    def front(self) -> np.ndarray:
        """Return the exact Pareto front of the oriented matrix.

        Computed on first access by
        :func:`~macroforecast.trade.aggregation.pareto.pareto_front_sweep`
        (linear in memory, D-11) and cached: a group whose methods were all
        skipped never pays for it.

        Returns:
            Boolean mask of shape ``(n_complete,)``.
        """
        if self._front is None:
            self._front = (
                pareto_front_sweep(self.X)
                if self.n_complete
                else np.zeros(0, dtype=bool)
            )
        return self._front


# Fonction de construction du vecteur de polarité de la synthèse
def _polarities(config: SynthesisConfig) -> np.ndarray:
    """Build the ``+/-1`` polarity vector aligned on ``config.metric_columns``.

    Args:
        config: Synthesis configuration carrying ``polarities`` as pairs.

    Returns:
        Integer array of shape ``(d,)``, ``+1`` for every unlisted metric.

    Examples:
        >>> config = SynthesisConfig(
        ...     metric_columns=("HHI", "CDI2"), polarities=(("CDI2", -1),)
        ... )
        >>> _polarities(config).tolist()
        [1, -1]
    """
    polarities = dict(config.polarities or ())
    return np.array([polarities.get(name, 1) for name in config.metric_columns])


# ──────────────────────────────────────────────────────────────────────
# Émission des lignes du format long (S-2.6)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'émission d'une ligne de diagnostic de cohérence
def _row(
    data: _GroupData,
    family: str,
    statistic: str,
    value: float,
    *,
    item_a: str = ITEM_SENTINEL,
    item_b: str = ITEM_SENTINEL,
    n: Optional[int] = None,
    always: bool = False,
) -> Optional[Dict[str, Any]]:
    """Build one long-format diagnostic row, or ``None`` when undefined.

    A statistic whose value is not finite (``tau`` on a constant column, KMO on
    a singular correlation matrix, a violation rate over an empty dominance
    relation) emits **no row**: the table then carries only usable numbers and
    :meth:`CoherenceRunReport.to_metrics` is free of ``NaN`` by construction.
    ``n_rows`` and ``n_complete``, which document the volumetry of a group even
    when every statistic was skipped, pass ``always=True`` to bypass the guard.

    Args:
        data: Group the statistic was computed on.
        family: ``'metrics'`` or ``'methods'``.
        statistic: Name of the statistic.
        value: Numeric value.
        item_a: Method or metric the statistic refers to, ``''`` when none.
        item_b: Second method or metric, ``''`` when none.
        n: Group size; the number of complete rows by default.
        always: Whether to emit the row even for a non-finite value.

    Returns:
        A mapping ready to be appended to the accumulator, or ``None``.
    """
    numeric = float(value)
    if not always and not math.isfinite(numeric):
        return None
    return {
        **data.base,
        "family": family,
        "statistic": statistic,
        "item_a": item_a,
        "item_b": item_b,
        "value": numeric,
        "n": int(data.n_complete if n is None else n),
    }


# Fonction d'ajout d'une ligne à l'accumulateur, les indéfinies écartées
def _append(rows: List[Dict[str, Any]], row: Optional[Dict[str, Any]]) -> None:
    """Append a row to the accumulator when it is defined.

    Args:
        rows: Accumulator of diagnostic rows.
        row: Output of :func:`_row`, possibly ``None``.
    """
    if row is not None:
        rows.append(row)


# ──────────────────────────────────────────────────────────────────────
# Famille `metrics` (S-2.5.a)
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul de la part attendue du front sous indépendance
def pareto_front_share_expected(n: int, d: int) -> float:
    """Return the expected share of the Pareto front under independence.

    For ``n`` points drawn independently with continuous, mutually independent
    coordinates, the expected number of maxima is asymptotically
    ``(ln n)^(d-1) / (d-1)!``, hence a share of
    ``(ln n)^(d-1) / ((d-1)! * n)`` (S-2.5.a). Read next to the observed
    ``pareto_front_share``: a much larger observed share signals metrics that
    disagree far more than chance, a much smaller one signals redundancy.

    ``(d-1)!`` goes through :func:`math.lgamma` so the ratio stays stable for
    every dimension the pipeline admits.

    Args:
        n: Number of rows of the group.
        d: Number of metrics.

    Returns:
        The expected share in ``[0, 1]``, ``NaN`` when ``n < 2`` or ``d < 1``.

    Examples:
        >>> round(pareto_front_share_expected(100, 2), 4)
        0.0461
        >>> pareto_front_share_expected(1, 3)
        nan
    """
    if n < 2 or d < 1:
        return float("nan")
    if d == 1:
        return 1.0 / n
    log_share = (d - 1) * math.log(math.log(n)) - math.lgamma(d) - math.log(n)
    return float(math.exp(log_share))


# Fonction de calcul des statistiques de cohérence des métriques
def _metrics_family(
    data: _GroupData, metrics: Sequence[str], config: CoherenceConfig
) -> List[Dict[str, Any]]:
    """Compute the ``metrics`` statistics of one group (S-2.5.a).

    Every statistic carries its own degeneracy guard: pairwise correlations
    need three complete rows, the factorial diagnostics need more rows than
    metrics, and the whole family needs at least two metrics. ``n_rows`` and
    ``n_complete`` are emitted unconditionally, so a group whose statistics
    were all skipped still appears in the table and documents the skip.

    Args:
        data: Group data carrying the oriented matrix and the front.
        metrics: Metric column names, in the column order of ``data.X``.
        config: Coherence configuration.

    Returns:
        The diagnostic rows of the ``metrics`` family.
    """
    rows: List[Dict[str, Any]] = []
    # Volumétrie : les deux seules lignes émises quoi qu'il arrive
    _append(rows, _row(data, METRICS_FAMILY, "n_rows", data.n_rows, always=True))
    _append(
        rows, _row(data, METRICS_FAMILY, "n_complete", data.n_complete, always=True)
    )

    n, d = data.n_complete, len(metrics)
    if n == 0 or d < 2:
        return rows

    # Corrélations de rang : trois lignes au moins pour un tau interprétable
    if n >= MIN_ROWS_CORRELATION:
        rho = spearman_correlation_matrix(data.X)
        if config.metric_pairs:
            for first, second in combinations(range(d), 2):
                # Ordre alphabétique des noms : chaque paire émise une seule fois
                item_a, item_b = sorted((metrics[first], metrics[second]))
                _append(
                    rows,
                    _row(
                        data,
                        METRICS_FAMILY,
                        "spearman",
                        rho[first, second],
                        item_a=item_a,
                        item_b=item_b,
                    ),
                )
                tau, _ = stats.kendalltau(data.X[:, first], data.X[:, second])
                _append(
                    rows,
                    _row(
                        data,
                        METRICS_FAMILY,
                        "kendall_tau_b",
                        tau,
                        item_a=item_a,
                        item_b=item_b,
                    ),
                )
        # Redondance moyenne : moyenne des |rho| hors diagonale (S-2.5.a)
        off_diagonal = ~np.eye(d, dtype=bool)
        _append(
            rows,
            _row(
                data,
                METRICS_FAMILY,
                "mean_abs_rho",
                float(np.mean(np.abs(rho[off_diagonal]))),
            ),
        )
        # Concordance des `d` classements induits par les métriques
        _append(
            rows,
            _row(
                data,
                METRICS_FAMILY,
                "kendall_w",
                kendall_w({name: data.X[:, index] for index, name in enumerate(metrics)}),
            ),
        )
        # Part de variance du premier axe, sur la corrélation de `Z` (A-05)
        _append(
            rows, _row(data, METRICS_FAMILY, "axis1_share", _axis1_share(data.X))
        )

    # Structure factorielle : matrice de corrélation singulière si `n <= d`
    if n > d:
        diagnostics = compute_correlation_diagnostics(data.X)
        _append(rows, _row(data, METRICS_FAMILY, "kmo", diagnostics.kmo))
        for index, name in enumerate(metrics):
            _append(
                rows,
                _row(
                    data,
                    METRICS_FAMILY,
                    "kmo",
                    diagnostics.kmo_per_variable[index],
                    item_a=name,
                ),
            )
        _append(
            rows,
            _row(data, METRICS_FAMILY, "bartlett_p", diagnostics.bartlett_p_value),
        )

    # Front de Pareto : part observée, puis part attendue sous indépendance
    _append(
        rows,
        _row(
            data,
            METRICS_FAMILY,
            "pareto_front_share",
            float(data.front.mean()),
        ),
    )
    _append(
        rows,
        _row(
            data,
            METRICS_FAMILY,
            "pareto_front_share_expected",
            pareto_front_share_expected(n, d),
        ),
    )
    return rows


# Fonction de calcul de la part de variance du premier axe principal
def _axis1_share(X: np.ndarray) -> float:
    """Return ``lambda_1 / d``, the variance share of the first principal axis.

    Computed on the correlation matrix of the standardised matrix ``Z``, the
    definition :func:`~macroforecast.trade.aggregation.weights.auto_weights`
    uses, so that the ``metrics`` family and the ``fit`` family report the same
    number on the same group.

    Args:
        X: Oriented matrix of shape ``(n, d)``.

    Returns:
        The share in ``[1/d, 1]``, ``NaN`` when a column is constant.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]])
        >>> round(_axis1_share(X), 3)
        1.0
    """
    d = X.shape[1]
    with np.errstate(invalid="ignore", divide="ignore"):
        correlation = np.corrcoef(X, rowvar=False)
    if not np.all(np.isfinite(correlation)):
        return float("nan")
    eigenvalues = np.linalg.eigvalsh(np.atleast_2d(correlation))
    return float(eigenvalues[-1] / d)


# ──────────────────────────────────────────────────────────────────────
# Famille `methods` (S-2.5.b)
# ──────────────────────────────────────────────────────────────────────

# Fonction de conversion d'un score en classement d'indices
def _ranking(scores: np.ndarray) -> List[int]:
    """Return the row indices ordered from most to least vulnerable.

    Args:
        scores: Score vector, higher meaning more vulnerable (D-10).

    Returns:
        Row indices, the most vulnerable first.

    Examples:
        >>> import numpy as np
        >>> [int(index) for index in _ranking(np.array([0.1, 0.9, 0.5]))]
        [1, 2, 0]
    """
    return list(np.argsort(-np.asarray(scores, dtype=float), kind="stable"))


# Fonction de calcul des statistiques de cohérence des synthèses
def _methods_family(
    data: _GroupData,
    metrics: Sequence[str],
    synthesis_config: SynthesisConfig,
    config: CoherenceConfig,
    bounds: Mapping[str, Tuple[np.ndarray, np.ndarray]],
    ellipticity: Mapping[Tuple[str, str], float],
    state: "_RunState",
) -> List[Dict[str, Any]]:
    """Compute the ``methods`` statistics of one group (S-2.5.b).

    Args:
        data: Group data carrying the oriented matrix, the front and the
            scores of every method that scored the group.
        metrics: Metric column names, in the column order of ``data.X``.
        synthesis_config: Configuration the scores were produced with.
        config: Coherence configuration.
        bounds: Bootstrap rank bounds per method, restricted to the group.
        ellipticity: Ellipticity statistics read from the fit diagnostics,
            keyed by ``(method, statistic)``.
        state: Mutable run state, carrying the level accumulators and the
            optional-dependency warnings already issued.

    Returns:
        The diagnostic rows of the ``methods`` family.
    """
    rows: List[Dict[str, Any]] = []
    names = sorted(data.scores)
    if not names:
        return rows

    # Statistiques par méthode : dominance, front, intervalle de rang
    for name in names:
        # Restriction aux lignes que *cette* méthode a notées : les
        # pseudo-méthodes de consensus ne couvrent que les cellules notées par
        # toutes les méthodes, et un `NaN` laissé dans le vecteur ferait
        # silencieusement disparaître les paires dominantes qui le portent
        scored = np.isfinite(data.scores[name])
        scores = data.scores[name][scored]
        X_scored = data.X[scored]
        front = data.front[scored] if scored.all() else pareto_front_sweep(X_scored)
        n_scored = int(scored.sum())
        violations = dominance_violation_rate(
            X_scored, scores, random_state=synthesis_config.random_state
        )
        _append(
            rows,
            _row(
                data,
                METHODS_FAMILY,
                "violation_strict",
                violations.strict,
                item_a=name,
                n=n_scored,
            ),
        )
        _append(
            rows,
            _row(
                data,
                METHODS_FAMILY,
                "violation_tie",
                violations.tie,
                item_a=name,
                n=n_scored,
            ),
        )
        state.record_violation(data.level, name, violations.strict)

        median_rank, max_rank = front_rank_summary(front, scores)
        _append(
            rows,
            _row(
                data,
                METHODS_FAMILY,
                "front_median_rank",
                median_rank,
                item_a=name,
                n=n_scored,
            ),
        )
        _append(
            rows,
            _row(
                data,
                METHODS_FAMILY,
                "front_max_rank",
                max_rank,
                item_a=name,
                n=n_scored,
            ),
        )

        # Largeur médiane de l'intervalle de rang bootstrap (D-08, Q4)
        if name in bounds:
            low, high = bounds[name]
            widths = high - low
            if np.isfinite(widths).any():
                _append(
                    rows,
                    _row(
                        data,
                        METHODS_FAMILY,
                        "rank_interval_width_median",
                        float(np.nanmedian(widths)),
                        item_a=name,
                    ),
                )

        # Sensibilité du score à chaque métrique prise isolément (Q5)
        if n_scored >= MIN_ROWS_CORRELATION:
            for index, metric in enumerate(metrics):
                tau, _ = stats.kendalltau(scores, X_scored[:, index])
                _append(
                    rows,
                    _row(
                        data,
                        METHODS_FAMILY,
                        "score_metric_tau",
                        tau,
                        item_a=name,
                        item_b=metric,
                        n=n_scored,
                    ),
                )

    # Facteur de confiance SMAA : part des cellules au-dessus du seuil
    rows.extend(_smaa_rows(data, synthesis_config))
    # Diagnostic d'ellipticité : lecture, sinon recalcul (M-06, D-07)
    rows.extend(
        _ellipticity_rows(data, synthesis_config, ellipticity, state)
    )

    if len(names) < 2:
        return rows

    # Comparaisons deux à deux : une seule matrice de scores communs
    stacked = np.column_stack([data.scores[name] for name in names])
    common = np.isfinite(stacked).all(axis=1)
    if common.sum() < 2:
        return rows
    scores_by_method = {
        name: stacked[common, index] for index, name in enumerate(names)
    }
    n_common = int(common.sum())

    tau_matrix = kendall_tau_b_matrix(scores_by_method)
    weighted = weighted_tau_matrix(scores_by_method)
    rankings = {name: _ranking(values) for name, values in scores_by_method.items()}
    for item_a, item_b in combinations(names, 2):
        # `names` est trié : `item_a < item_b` par construction (P08)
        for statistic, matrix in (
            ("kendall_tau_b", tau_matrix),
            ("weighted_tau", weighted),
        ):
            _append(
                rows,
                _row(
                    data,
                    METHODS_FAMILY,
                    statistic,
                    matrix.loc[item_a, item_b],
                    item_a=item_a,
                    item_b=item_b,
                    n=n_common,
                ),
            )
        _append(
            rows,
            _row(
                data,
                METHODS_FAMILY,
                "rbo",
                rank_biased_overlap(rankings[item_a], rankings[item_b], p=config.rbo_p),
                item_a=item_a,
                item_b=item_b,
                n=n_common,
            ),
        )
        for depth in config.topk_depths:
            # Une profondeur plus grande que le groupe ne compare rien
            if depth > n_common:
                continue
            _append(
                rows,
                _row(
                    data,
                    METHODS_FAMILY,
                    f"topk_overlap_{depth}",
                    topk_overlap(rankings[item_a], rankings[item_b], depth),
                    item_a=item_a,
                    item_b=item_b,
                    n=n_common,
                ),
            )

    # Concordance de l'ensemble des méthodes, puis part de cellules litigieuses
    concordance = kendall_w(scores_by_method)
    _append(
        rows, _row(data, METHODS_FAMILY, "kendall_w", concordance, n=n_common)
    )
    state.record_level(data.level, "kendall_w", concordance)

    ranks = np.column_stack(
        [stats.rankdata(-values, method="average") for values in scores_by_method.values()]
    )
    # Seuil de litige : au moins un rang d'écart, comme `compute_coherence_report`
    threshold = max(1, int(round(config.dispute_fraction * n_common)))
    spread = ranks.max(axis=1) - ranks.min(axis=1)
    disputed = float((spread > threshold).mean())
    _append(
        rows, _row(data, METHODS_FAMILY, "disputed_share", disputed, n=n_common)
    )
    state.record_level(data.level, "disputed_share", disputed)

    # Groupes de méthodes : CAH sur `1 - tau_b`, coupure en distance. Une
    # méthode à score constant sur le groupe donne un `tau` indéfini et sort
    # de la classification plutôt que de rendre la matrice inutilisable
    finite = tau_matrix.columns[np.isfinite(tau_matrix.to_numpy()).all(axis=0)]
    if len(finite) >= 2:
        labels = fcluster(
            cluster_methods(tau_matrix.loc[finite, finite]),
            t=CLUSTER_THRESHOLD,
            criterion="distance",
        )
        for name, label in zip(finite, labels):
            _append(
                rows,
                _row(
                    data,
                    METHODS_FAMILY,
                    "cluster_id",
                    float(label),
                    item_a=str(name),
                    n=n_common,
                ),
            )
    return rows


# Fonction d'émission de la part de cellules confiantes au sens SMAA
def _smaa_rows(
    data: _GroupData, synthesis_config: SynthesisConfig
) -> List[Dict[str, Any]]:
    """Emit ``smaa_confidence_top{k}_share`` for every SMAA method (S-2.5.b).

    The SMAA score is the confidence factor ``p_i``, the share of weight draws
    placing the cell in the top ``k``; the share of cells whose ``p_i`` reaches
    ``0.5`` says how much of the ranking survives an agnostic weighting.

    Args:
        data: Group data carrying the scores.
        synthesis_config: Configuration the scores were produced with, whose
            ``smaa_k`` names the depth.

    Returns:
        One row per configured SMAA method, ``item_a`` empty when a single one
        is configured and the method name otherwise.
    """
    smaa_names = [
        spec.name
        for spec in synthesis_config.methods
        if spec.kind == "smaa" and spec.name in data.scores
    ]
    statistic = f"smaa_confidence_top{synthesis_config.smaa_k}_share"
    rows: List[Dict[str, Any]] = []
    for name in smaa_names:
        scores = data.scores[name]
        scores = scores[np.isfinite(scores)]
        if not scores.size:
            continue
        share = float((scores >= SMAA_CONFIDENCE_THRESHOLD).mean())
        # `item_a` vide dans le cas nominal (S-2.5.b), nommé s'il y a ambiguïté
        item_a = ITEM_SENTINEL if len(smaa_names) == 1 else name
        _append(rows, _row(data, METHODS_FAMILY, statistic, share, item_a=item_a))
    return rows


# Fonction d'émission du diagnostic d'ellipticité
def _ellipticity_rows(
    data: _GroupData,
    synthesis_config: SynthesisConfig,
    ellipticity: Mapping[Tuple[str, str], float],
    state: "_RunState",
) -> List[Dict[str, Any]]:
    """Emit ``ellipticity_tau_proj`` / ``ellipticity_tau_rank`` (M-06, D-07).

    The two ``tau`` say whether the transport-based score added anything to its
    linear counterpart on this group; they are a diagnostic, never a gate
    (D-07). They are read from the fit diagnostics when the synthesis run
    provided them, and recomputed otherwise — which needs the optional
    ``jax`` / ``ott`` stack and a group at least as large as the Kantorovitch
    ``min_group_size``.

    Args:
        data: Group data carrying the oriented matrix.
        synthesis_config: Configuration the scores were produced with.
        ellipticity: Statistics read from the fit diagnostics, keyed by
            ``(method, statistic)``.
        state: Mutable run state, carrying the dependency warnings already
            issued.

    Returns:
        The ellipticity rows, empty when no Kantorovitch method is configured
        or when the optional dependency is missing.
    """
    rows: List[Dict[str, Any]] = []
    for spec in synthesis_config.methods:
        if spec.kind != "kantorovich":
            continue
        # Lecture prioritaire : le score a déjà été ajusté par la synthèse
        read = {
            statistic: ellipticity[(spec.name, statistic)]
            for statistic in ELLIPTICITY_STATISTICS.values()
            if (spec.name, statistic) in ellipticity
        }
        if read:
            for statistic, value in read.items():
                _append(
                    rows, _row(data, METHODS_FAMILY, statistic, value, item_a=spec.name)
                )
            continue

        minimum = (
            spec.min_group_size
            if spec.min_group_size is not None
            else registry_entry(spec.kind).min_group_size
        )
        if data.n_complete < minimum:
            continue
        metrics = method_metrics(spec, synthesis_config)
        columns = [synthesis_config.metric_columns.index(name) for name in metrics]
        # Matrice brute : le pipeline réapplique lui-même l'orientation
        X_raw = data.X[:, columns] * _polarities(synthesis_config)[columns]
        try:
            # Import local : `ellipticity_screen` tire la pile `jax` / `ott`
            from .optimal_transport import ellipticity_screen

            pipeline = build_method(spec, synthesis_config)
            pipeline.fit(X_raw)
            screen = ellipticity_screen(
                pipeline[:-1].transform(X_raw), pipeline[-1]
            )
        except ImportError as error:
            if spec.kind not in state.warned_dependencies:
                state.warned_dependencies.add(spec.kind)
                warnings.warn(
                    f"Ellipticity diagnostic skipped for {spec.name!r}: {error}",
                    RuntimeWarning,
                )
            continue
        for key, statistic in ELLIPTICITY_STATISTICS.items():
            _append(
                rows,
                _row(data, METHODS_FAMILY, statistic, screen[key], item_a=spec.name),
            )
    return rows


# ──────────────────────────────────────────────────────────────────────
# Leave-one-metric-out (S-2.5.b, Q5)
# ──────────────────────────────────────────────────────────────────────

# Fonction de ré-ajustement des méthodes privées d'une métrique
def _lomo_family(
    data: _GroupData,
    synthesis_config: SynthesisConfig,
    config: CoherenceConfig,
    state: "_RunState",
) -> List[Dict[str, Any]]:
    """Refit the ``lomo_methods`` without each metric in turn (Q5).

    The cost is one refit per method, per metric and per group, so the
    perimeter is held by ``config.lomo_methods`` rather than by a global
    switch, and a group above :data:`LOMO_WARN_ROWS` rows draws a warning.

    Args:
        data: Group data carrying the oriented matrix and the scores.
        synthesis_config: Configuration the scores were produced with.
        config: Coherence configuration.
        state: Mutable run state, carrying the warnings already issued.

    Returns:
        The ``lomo_tau`` and ``lomo_topk_overlap`` rows of the group.
    """
    rows: List[Dict[str, Any]] = []
    if not config.lomo or not config.lomo_methods:
        return rows
    if data.n_complete < MIN_ROWS_CORRELATION:
        return rows
    if data.n_complete > LOMO_WARN_ROWS and not state.warned_lomo:
        state.warned_lomo = True
        warnings.warn(
            f"Leave-one-metric-out on a group of {data.n_complete} rows at level "
            f"{data.level!r}: one refit per method and per metric.",
            RuntimeWarning,
        )
    # Profondeur du recouvrement : deuxième entrée de `topk_depths` (P08)
    depth = (
        config.topk_depths[1]
        if len(config.topk_depths) > 1
        else config.topk_depths[-1]
    )
    signs = _polarities(synthesis_config)

    for spec in synthesis_config.methods:
        if spec.name not in config.lomo_methods or spec.name not in data.scores:
            continue
        metrics = method_metrics(spec, synthesis_config)
        if len(metrics) < 2:
            continue
        scored = np.isfinite(data.scores[spec.name])
        if int(scored.sum()) < MIN_ROWS_CORRELATION:
            continue
        reference = data.scores[spec.name][scored]
        reference_ranking = _ranking(reference)
        for dropped in metrics:
            kept = tuple(name for name in metrics if name != dropped)
            columns = [synthesis_config.metric_columns.index(name) for name in kept]
            X_raw = data.X[scored][:, columns] * signs[columns]
            try:
                pipeline = build_method(replace(spec, metrics=kept), synthesis_config)
                scores = np.asarray(
                    pipeline.fit(X_raw).predict(X_raw), dtype=float
                )
            except (ImportError, ValueError) as error:
                logger.debug(
                    f"LOMO ignoré pour {spec.name} sans {dropped} : {error}"
                )
                continue
            tau, _ = stats.kendalltau(reference, scores)
            _append(
                rows,
                _row(
                    data,
                    METHODS_FAMILY,
                    "lomo_tau",
                    tau,
                    item_a=spec.name,
                    item_b=dropped,
                ),
            )
            if depth <= int(scored.sum()):
                _append(
                    rows,
                    _row(
                        data,
                        METHODS_FAMILY,
                        "lomo_topk_overlap",
                        topk_overlap(reference_ranking, _ranking(scores), depth),
                        item_a=spec.name,
                        item_b=dropped,
                    ),
                )
    return rows


# ──────────────────────────────────────────────────────────────────────
# État mutable d'une exécution
# ──────────────────────────────────────────────────────────────────────

# Accumulateurs par niveau et avertissements d'une exécution
@dataclass
class _RunState:
    """Mutable state of one run, gathered to keep signatures short.

    Attributes:
        level_values: Per level, the values collected for each aggregated
            statistic, later reduced to their median.
        level_violations: Per level and per method, the strict violation rates.
        warned_dependencies: Method kinds an optional-dependency warning has
            already been issued for.
        warned_lomo: Whether the leave-one-metric-out cost warning was issued.
    """
    level_values: Dict[str, Dict[str, List[float]]] = field(default_factory=dict)
    level_violations: Dict[str, Dict[str, List[float]]] = field(default_factory=dict)
    warned_dependencies: set = field(default_factory=set)
    warned_lomo: bool = False

    # Enregistrement d'une valeur agrégée par niveau
    def record_level(self, level: str, statistic: str, value: float) -> None:
        """Record one value of an aggregated statistic.

        Args:
            level: Level of the group.
            statistic: Name of the statistic.
            value: Value; non-finite values are ignored.
        """
        if not math.isfinite(value):
            return
        self.level_values.setdefault(level, {}).setdefault(statistic, []).append(
            float(value)
        )

    # Enregistrement d'un taux de violation stricte par méthode
    def record_violation(self, level: str, method: str, value: float) -> None:
        """Record one strict violation rate.

        Args:
            level: Level of the group.
            method: Method name.
            value: Rate; non-finite values are ignored.
        """
        if not math.isfinite(value):
            return
        self.level_violations.setdefault(level, {}).setdefault(method, []).append(
            float(value)
        )


# ──────────────────────────────────────────────────────────────────────
# Runner de cohérence (S-1.7)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'exécution de l'analyse de cohérence
def run_coherence(
    df_metrics: pd.DataFrame,
    df_scores: pd.DataFrame,
    synthesis_config: SynthesisConfig,
    config: CoherenceConfig,
    *,
    df_fit_diagnostics: Optional[pd.DataFrame] = None,
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
) -> Tuple[pd.DataFrame, CoherenceRunReport]:
    """Measure the coherence of the metrics and of the syntheses (S-1.7).

    The loop runs context -> level -> group, the same groups
    :func:`~macroforecast.trade.aggregation.synthesis.run_synthesis` scored.
    Each group is extracted **once**: one oriented matrix and one Pareto front
    serve both the ``metrics`` family (S-2.5.a) and the ``methods`` family
    (S-2.5.b). The scores are read from ``df_scores`` through a single pivot
    per context and per level, so no join runs inside the group loop.

    A statistic whose value is not finite emits no row (a ``tau`` on a constant
    column, a KMO on a singular matrix, a violation rate over an empty
    dominance relation); ``n_rows`` and ``n_complete`` are always emitted, so
    every group appears in the table even when its statistics were all skipped.

    Args:
        df_metrics: Cells by metrics, the table the synthesis was run on.
        df_scores: Long score table of S-2.4.
        synthesis_config: Configuration the scores were produced with.
        config: Coherence configuration.
        df_fit_diagnostics: ``fit``-family diagnostics of the synthesis run;
            read for the ellipticity statistics, which are recomputed when it
            is absent and the optional transport stack is installed.
        tracker: Experiment tracker receiving the artifacts; the null tracker
            by default, so the runner stays side-effect free.
        log_artifacts: Whether to send the artifacts of S-2.7 to the tracker.

    Returns:
        Tuple ``(df_diagnostics, report)``: the long diagnostic table of S-2.6,
        families ``metrics`` and ``methods``, and the
        :class:`CoherenceRunReport`.

    Raises:
        KeyError: If a configured column is absent from either table.
        AssertionError: If the primary key of S-2.6 is not unique.

    Examples:
        >>> from macroforecast.trade.aggregation.methods import MethodSpec
        >>> from macroforecast.trade.aggregation.synthesis import run_synthesis
        >>> df = pd.DataFrame({
        ...     "freq": ["A"] * 4, "flow": [1] * 4,
        ...     "indicators": ["V"] * 4, "TIME_PERIOD": ["2024"] * 4,
        ...     "reporter": ["FR", "FR", "DE", "DE"],
        ...     "product": ["a", "b", "a", "b"],
        ...     "HHI": [0.8, 0.2, 0.5, 0.4], "CDI2": [0.7, 0.3, 0.6, 0.1],
        ... })
        >>> synthesis_config = SynthesisConfig(
        ...     metric_columns=("HHI", "CDI2"), levels=("global",),
        ...     methods=(MethodSpec(name="mpi", kind="mpi"),), consensus=(),
        ...     min_group_size=3,
        ... )
        >>> df_scores, df_fit, _ = run_synthesis(df, synthesis_config)
        >>> df_diagnostics, report = run_coherence(
        ...     df, df_scores, synthesis_config, CoherenceConfig()
        ... )
        >>> report.n_groups
        1
        >>> int(df_diagnostics.loc[
        ...     df_diagnostics["statistic"] == "n_complete", "value"].iloc[0])
        4
    """
    _check_inputs(df_metrics, df_scores, synthesis_config)
    ellipticity = _ellipticity_index(df_fit_diagnostics)
    signs = _polarities(synthesis_config)
    metric_columns = list(synthesis_config.metric_columns)

    report = CoherenceRunReport()
    report.levels = {level: CoherenceLevelReport() for level in synthesis_config.levels}
    state = _RunState()
    rows: List[Dict[str, Any]] = []

    context_columns = list(synthesis_config.context_columns)
    scores_by_context = df_scores.groupby(context_columns, sort=False, observed=True)
    for context_value, df_context in df_metrics.groupby(
        context_columns, sort=False, observed=True
    ):
        context = context_value if isinstance(context_value, tuple) else (context_value,)
        report.n_contexts += 1
        # Matrice orientée du contexte : une extraction et une orientation
        X_context = df_context.loc[:, metric_columns].to_numpy(dtype=float) * signs
        complete_context = np.isfinite(X_context).all(axis=1)
        try:
            df_scores_context = scores_by_context.get_group(context_value)
        except KeyError:
            logger.debug(f"Contexte {context} absent de la table des scores.")
            df_scores_context = df_scores.iloc[:0]

        for level in synthesis_config.levels:
            # Pivot unique du niveau : méthodes en colonnes, cellules en lignes
            wide = _wide_frames(
                df_scores_context, df_context, synthesis_config, level
            )
            for group_value, positions in iter_groups(df_context, synthesis_config, level):
                complete = complete_context[positions]
                data = _GroupData(
                    base=diagnostic_base(
                        context, synthesis_config, level, group_value
                    ),
                    level=level,
                    positions=positions,
                    X=X_context[positions][complete],
                    complete=complete,
                    n_rows=int(len(positions)),
                    n_complete=int(complete.sum()),
                )
                data.scores = _group_scores(wide["score"], positions, complete)
                bounds = _group_bounds(wide, positions, complete, data.scores)

                report.n_groups += 1
                report.levels[level].n_groups += 1
                metric_rows = _metrics_family(data, metric_columns, config)
                rows.extend(metric_rows)
                for row in metric_rows:
                    if row["statistic"] == "mean_abs_rho":
                        state.record_level(level, "mean_abs_rho", row["value"])
                rows.extend(
                    _methods_family(
                        data,
                        metric_columns,
                        synthesis_config,
                        config,
                        bounds,
                        ellipticity,
                        state,
                    )
                )
                rows.extend(_lomo_family(data, synthesis_config, config, state))
        # Logging
        logger.debug(
            f"Contexte {context} analysé : {len(df_context)} cellules, "
            f"{report.n_groups} groupes cumulés."
        )

    _reduce_levels(report, state)
    df_diagnostics = _diagnostics_frame(rows, synthesis_config)
    _assert_primary_key(df_diagnostics, synthesis_config)

    if log_artifacts:
        log_coherence_artifacts(tracker, df_diagnostics, synthesis_config)
    return df_diagnostics, report


# Fonction de vérification des colonnes attendues
def _check_inputs(
    df_metrics: pd.DataFrame, df_scores: pd.DataFrame, config: SynthesisConfig
) -> None:
    """Check that both tables carry every configured column.

    Args:
        df_metrics: Metric table (S-2.3).
        df_scores: Score table (S-2.4).
        config: Synthesis configuration.

    Raises:
        KeyError: If a context, cell, metric or score column is absent.
    """
    expected_metrics = (
        *config.context_columns,
        config.reporter_col,
        config.product_col,
        *config.metric_columns,
    )
    missing = [column for column in expected_metrics if column not in df_metrics.columns]
    if missing:
        raise KeyError(
            f"Column(s) {missing} are absent from the metric table. "
            f"Available columns: {list(df_metrics.columns)}."
        )
    expected_scores = (
        *config.context_columns,
        config.reporter_col,
        config.product_col,
        "method",
        *(f"score_{level}" for level in config.levels),
    )
    missing = [column for column in expected_scores if column not in df_scores.columns]
    if missing:
        raise KeyError(
            f"Column(s) {missing} are absent from the score table. "
            f"Available columns: {list(df_scores.columns)}."
        )


# Fonction d'indexation des statistiques d'ellipticité des diagnostics d'ajustement
def _ellipticity_index(
    df_fit_diagnostics: Optional[pd.DataFrame],
) -> Dict[Tuple[str, str], float]:
    """Index the ellipticity statistics of the fit diagnostics.

    Args:
        df_fit_diagnostics: ``fit``-family table, possibly ``None``.

    Returns:
        Mapping of ``(method, statistic)`` to its value, empty when the table
        is absent or carries no ellipticity row.
    """
    if df_fit_diagnostics is None or df_fit_diagnostics.empty:
        return {}
    wanted = list(ELLIPTICITY_STATISTICS.values())
    selection = df_fit_diagnostics[df_fit_diagnostics["statistic"].isin(wanted)]
    return {
        (str(row.item_a), str(row.statistic)): float(row.value)
        for row in selection.itertuples()
    }


# Fonction de pivot de la table des scores d'un contexte et d'un niveau
def _wide_frames(
    df_scores_context: pd.DataFrame,
    df_context: pd.DataFrame,
    config: SynthesisConfig,
    level: str,
) -> Dict[str, pd.DataFrame]:
    """Pivot the score table of one context into method columns.

    Done once per context and per level: afterwards a group is a positional
    slice of a ``numpy`` array, with no join and no ``groupby`` inside the
    group loop.

    Args:
        df_scores_context: Score rows of the context.
        df_context: Metric rows of the context, whose order the pivot follows.
        config: Synthesis configuration.
        level: Level whose columns are read.

    Returns:
        Mapping of ``'score'``, ``'rank_low'`` and ``'rank_high'`` to a frame
        indexed like ``df_context`` and columned by method name; empty frames
        when the level carries no such column.
    """
    cells = pd.MultiIndex.from_arrays(
        [df_context[config.reporter_col], df_context[config.product_col]]
    )
    frames: Dict[str, pd.DataFrame] = {}
    for prefix in ("score", "rank_low", "rank_high"):
        column = f"{prefix}_{level}"
        if column not in df_scores_context.columns or df_scores_context.empty:
            frames[prefix] = pd.DataFrame(index=range(len(df_context)))
            continue
        pivoted = (
            df_scores_context.set_index(
                [config.reporter_col, config.product_col, "method"]
            )[column]
            .unstack("method")
            .reindex(cells)
            .reset_index(drop=True)
        )
        frames[prefix] = pivoted.astype(float)
    return frames


# Fonction d'extraction des scores d'un groupe, méthode par méthode
def _group_scores(
    wide: pd.DataFrame, positions: np.ndarray, complete: np.ndarray
) -> Dict[str, np.ndarray]:
    """Extract the score vector of every method that scored the group.

    Args:
        wide: Pivoted score frame of the level.
        positions: Positional indices of the group rows.
        complete: Mask of the complete rows inside the group.

    Returns:
        Mapping of method name to its score vector, aligned on the complete
        rows; a method is kept when at least two of its scores are finite.
    """
    if wide.empty or not len(wide.columns):
        return {}
    block = wide.to_numpy(dtype=float)[positions][complete]
    scores: Dict[str, np.ndarray] = {}
    for index, name in enumerate(wide.columns):
        values = block[:, index]
        if np.isfinite(values).sum() >= 2:
            scores[str(name)] = values
    return scores


# Fonction d'extraction des bornes bootstrap d'un groupe
def _group_bounds(
    wide: Mapping[str, pd.DataFrame],
    positions: np.ndarray,
    complete: np.ndarray,
    scores: Mapping[str, np.ndarray],
) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
    """Extract the bootstrap rank bounds of every method of the group.

    Args:
        wide: Pivoted frames of the level.
        positions: Positional indices of the group rows.
        complete: Mask of the complete rows inside the group.
        scores: Methods that scored the group.

    Returns:
        Mapping of method name to ``(rank_low, rank_high)``, restricted to the
        methods carrying at least one finite bound.
    """
    low_frame, high_frame = wide["rank_low"], wide["rank_high"]
    if low_frame.empty or not len(low_frame.columns):
        return {}
    low_block = low_frame.to_numpy(dtype=float)[positions][complete]
    high_block = high_frame.to_numpy(dtype=float)[positions][complete]
    columns = list(low_frame.columns)
    bounds: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    for name in scores:
        if name not in columns:
            continue
        index = columns.index(name)
        low, high = low_block[:, index], high_block[:, index]
        if np.isfinite(low).any() and np.isfinite(high).any():
            bounds[name] = (low, high)
    return bounds


# Fonction de réduction des accumulateurs de niveau en médianes
def _reduce_levels(report: CoherenceRunReport, state: _RunState) -> None:
    """Reduce the per-level accumulators to the medians of S-2.7.

    Args:
        report: Report whose level summaries are filled in place.
        state: Run state carrying the collected values.
    """
    for level, level_report in report.levels.items():
        values = state.level_values.get(level, {})
        for statistic in ("kendall_w", "disputed_share", "mean_abs_rho"):
            collected = values.get(statistic, [])
            if collected:
                setattr(level_report, statistic, float(np.median(collected)))
        level_report.violation_strict = {
            method: float(np.median(rates))
            for method, rates in state.level_violations.get(level, {}).items()
        }


# Fonction d'assemblage de la table longue des diagnostics de cohérence
def _diagnostics_frame(
    rows: Sequence[Mapping[str, Any]], config: SynthesisConfig
) -> pd.DataFrame:
    """Build the ``metrics`` and ``methods`` diagnostic table (S-2.6).

    Args:
        rows: Accumulated diagnostic rows.
        config: Synthesis configuration.

    Returns:
        A frame carrying the context columns then the columns of
        :data:`~macroforecast.trade.aggregation.synthesis.DIAGNOSTIC_COLUMNS`,
        ``value`` as a float and ``n`` as a nullable integer.
    """
    columns = _diagnostic_columns(config)
    if not rows:
        return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})
    df_diagnostics = pd.DataFrame(list(rows))[columns]
    df_diagnostics["value"] = df_diagnostics["value"].astype(float)
    df_diagnostics["n"] = df_diagnostics["n"].astype("Int64")
    return df_diagnostics


# Fonction de description des colonnes de la table longue
def _diagnostic_columns(config: SynthesisConfig) -> List[str]:
    """Return the columns of the diagnostic table, in the order of S-2.6.

    Args:
        config: Synthesis configuration, whose ``reporter_col`` and
            ``product_col`` replace the canonical group column names.

    Returns:
        The ordered column names.

    Examples:
        >>> _diagnostic_columns(SynthesisConfig())[:6]
        ['freq', 'flow', 'indicators', 'TIME_PERIOD', 'level', 'reporter']
    """
    renaming = {"reporter": config.reporter_col, "product": config.product_col}
    return [
        *config.context_columns,
        *(renaming.get(column, column) for column in DIAGNOSTIC_COLUMNS),
    ]


# Fonction de vérification de l'unicité de la clé primaire (S-2.6)
def _assert_primary_key(
    df_diagnostics: pd.DataFrame, config: SynthesisConfig
) -> None:
    """Check that no two rows share the primary key of S-2.6.

    Args:
        df_diagnostics: Assembled diagnostic table.
        config: Synthesis configuration.

    Raises:
        AssertionError: If the key is not unique, naming the first duplicates.
    """
    if df_diagnostics.empty:
        return
    key = [
        column for column in _diagnostic_columns(config) if column not in ("value", "n")
    ]
    duplicated = df_diagnostics.duplicated(subset=key)
    n_duplicated = int(duplicated.sum())
    assert n_duplicated == 0, (
        f"The primary key of S-2.6 is not unique: {n_duplicated} duplicated row(s). "
        f"First occurrences:\n{df_diagnostics.loc[duplicated, key].head()}"
    )


# ──────────────────────────────────────────────────────────────────────
# Artefacts (S-2.7)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'envoi des artefacts de cohérence au suivi d'expérience
def log_coherence_artifacts(
    tracker: RunTracker, df_diagnostics: pd.DataFrame, config: SynthesisConfig
) -> None:
    """Send the artifacts of S-2.7 to the tracker.

    Two families: the square ``tau_b`` matrix of the methods, per level, for
    the **first** context only — the matrix of every context would reproduce
    the table it comes from — and the leave-one-metric-out table, per level.

    Args:
        tracker: Tracker receiving the tables; the null tracker discards them.
        df_diagnostics: Diagnostic table produced by :func:`run_coherence`.
        config: Synthesis configuration.
    """
    if df_diagnostics.empty:
        return
    context_columns = list(config.context_columns)
    first = df_diagnostics.iloc[0][context_columns]
    is_first_context = (df_diagnostics[context_columns] == first).all(axis=1)
    label = "_".join(str(value) for value in first)

    for level, at_level in df_diagnostics.groupby("level", sort=False):
        pairs = at_level[
            is_first_context.reindex(at_level.index, fill_value=False)
            & (at_level["family"] == METHODS_FAMILY)
            & (at_level["statistic"] == "kendall_tau_b")
        ]
        if not pairs.empty:
            tracker.log_table(pairs, f"coherence/tau_matrix_{level}_{label}.csv")
        lomo = at_level[
            at_level["statistic"].isin(("lomo_tau", "lomo_topk_overlap"))
        ]
        if not lomo.empty:
            tracker.log_table(lomo, f"coherence/lomo_{level}.csv")
