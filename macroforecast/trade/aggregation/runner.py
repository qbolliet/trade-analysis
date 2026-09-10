"""Orchestration : from a wide metric table to a compared, audited score.

Two entry points:

* :func:`run_aggregation` — apply a caller-chosen set of named methods
  (``sklearn.pipeline.Pipeline`` or
  :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`
  instances) to the same metric matrix and assemble the coherence dashboard
  (:mod:`~macroforecast.trade.aggregation.diagnostics`) comparing them —
  the generic building block.
* :func:`recommended_workflow` — the priority-ordered sequence of §7 of the
  methodological note: the Pareto front and dominance count first (free, no
  assumption), SMAA on a weighted sum, two or three contrasted scores with
  the coherence dashboard, a bootstrap of rank stability, and the oriented
  Kantorovitch score last, whenever the group is large enough and
  low-dimensional enough for its estimation to make sense — its ellipticity
  diagnostic being reported rather than used as a gate (D-07).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple
# Modules de manipulation de données
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
# Modules du package
from .base import AggregationConfig, attach_scores, polarity_vector, split_frame
from .diagnostics import (
    CoherenceReport,
    SmaaResult,
    bootstrap_rank_stability,
    compute_coherence_report,
    smaa_rank_acceptability,
)
from .estimators import WeightedAggregator
from .functions import weighted_sum_score
from .pareto import dominance_count, pareto_front
from .preprocessing import PolarityOrienter, Winsorizer, make_normalizer


# ──────────────────────────────────────────────────────────────────────
# Rapport composite
# ──────────────────────────────────────────────────────────────────────

# Rapport d'exécution d'une agrégation
@dataclass
class AggregationReport:
    """Summary of an aggregation run.

    Attributes:
        n_products: Number of rows (products) scored.
        methods: Names of the methods compared.
        coherence: Comparison-and-robustness dashboard (§6 of the note).
        pareto_front_size: Number of products on the Pareto front.
        bootstrap: Rank-stability intervals
            (:func:`~macroforecast.trade.aggregation.diagnostics.bootstrap_rank_stability`),
            ``None`` when not requested.
        smaa: SMAA rank-acceptability result, ``None`` when not requested.
        ellipticity_tau_proj: Kendall τ_b between the oriented Kantorovitch
            score and its linear counterpart, the whitened projection;
            ``None`` when the optimal-transport step was skipped.
        ellipticity_tau_rank: Kendall τ_b between the center-outward rank and
            the Mahalanobis distance to the robust centre; ``None`` when the
            optimal-transport step was skipped. Both are reported for
            interpretation, neither gates the score (D-07).
    """
    n_products: int = 0
    methods: List[str] = field(default_factory=list)
    coherence: Optional[CoherenceReport] = None
    pareto_front_size: int = 0
    bootstrap: Optional[pd.DataFrame] = None
    smaa: Optional[SmaaResult] = None
    ellipticity_tau_proj: Optional[float] = None
    ellipticity_tau_rank: Optional[float] = None

    # Mise en forme des indicateurs numériques (style VulnerabilityReport.to_metrics)
    def to_metrics(self, prefix: str = "aggregation") -> Dict[str, float]:
        """Flatten the numeric diagnostics into a dotted metric mapping.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> report = AggregationReport(n_products=10, pareto_front_size=3)
            >>> report.to_metrics()["aggregation.pareto_front_size"]
            3.0
        """
        metrics: Dict[str, float] = {
            f"{prefix}.n_products": float(self.n_products),
            f"{prefix}.pareto_front_size": float(self.pareto_front_size),
        }
        if self.coherence is not None:
            metrics.update(self.coherence.to_metrics(prefix=f"{prefix}.coherence"))
        for name in ("ellipticity_tau_proj", "ellipticity_tau_rank"):
            value = getattr(self, name)
            if value is not None:
                metrics[f"{prefix}.{name}"] = float(value)
        return metrics


# ──────────────────────────────────────────────────────────────────────
# Construction de pipelines par défaut
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction d'un pipeline de prétraitement + agrégation standard
def default_pipeline(
    config: AggregationConfig,
    *,
    normalization: str = "minmax",
    winsorize_quantile: float = 0.99,
    weighting: str = "entropy",
    aggregation: str = "weighted_sum",
    weighting_params: Optional[Dict[str, Any]] = None,
    aggregation_params: Optional[Dict[str, Any]] = None,
) -> Pipeline:
    """Build the standard orientation → winsorisation → scaling → aggregation pipeline.

    A real ``sklearn.pipeline.Pipeline``: every configuration knob is a
    string or a number a caller can source from a YAML file, following the
    project's convention of leaving methodological choices to configuration
    rather than to hardcoded code paths.

    ``"minmax"`` is the default rather than ``"robust"`` (median/MAD, which
    can be negative) because it is the one scheme every default weighting and
    aggregation choice here tolerates: entropy and CRITIC weighting are only
    valid on non-negative, min-max-normalised data (§4.1's warning in the
    note), and the geometric mean needs non-negative values too. Passing
    ``normalization="robust"`` is still fine for a Mahalanobis- or
    PCA-only pipeline, which place no such constraint on their input.

    Args:
        config: Column conventions (``polarities`` sizes
            :class:`~macroforecast.trade.aggregation.preprocessing.PolarityOrienter`).
        normalization: Name forwarded to
            :func:`~macroforecast.trade.aggregation.preprocessing.make_normalizer`.
        winsorize_quantile: Upper quantile capped by
            :class:`~macroforecast.trade.aggregation.preprocessing.Winsorizer`.
        weighting: Name forwarded to
            :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`.
        aggregation: Name forwarded to
            :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`.
        weighting_params: Extra keyword arguments for the weighting scheme.
        aggregation_params: Extra keyword arguments for the aggregation
            function.

    Returns:
        An unfitted ``sklearn.pipeline.Pipeline``.

    Examples:
        >>> config = AggregationConfig(id_columns=("id",), metric_columns=("HHI", "CDI2"))
        >>> pipeline = default_pipeline(config, weighting="critic")
        >>> [name for name, _ in pipeline.steps]
        ['orient', 'winsorize', 'scale', 'aggregate']
    """
    return Pipeline(
        [
            ("orient", PolarityOrienter(polarity_vector(config))),
            ("winsorize", Winsorizer(quantile=winsorize_quantile)),
            ("scale", make_normalizer(normalization)),
            (
                "aggregate",
                WeightedAggregator(
                    weighting=weighting,
                    aggregation=aggregation,
                    weighting_params=weighting_params,
                    aggregation_params=aggregation_params,
                ),
            ),
        ]
    )


# ──────────────────────────────────────────────────────────────────────
# Orchestration générique
# ──────────────────────────────────────────────────────────────────────

# Fonction d'application et de comparaison d'un ensemble de méthodes
def run_aggregation(
    df_data: pd.DataFrame,
    config: AggregationConfig,
    methods: Mapping[str, Any],
    *,
    dispute_threshold: int = 50,
) -> Tuple[pd.DataFrame, AggregationReport]:
    """Apply every named method to the same metric matrix and compare them.

    Args:
        df_data: Wide metric table (``config.id_columns`` +
            ``config.metric_columns``).
        config: Column conventions.
        methods: Mapping of method name to an unfitted estimator exposing
            ``fit(X).predict(X)`` — typically built with
            :func:`default_pipeline`, or any ``sklearn.pipeline.Pipeline`` /
            :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`.
        dispute_threshold: Rank-spread above which a product is flagged as
            disputed in the coherence report.

    Returns:
        Tuple ``(df_scores, report)``: a wide table (one column per method,
        indexed like ``config.id_columns``) and the :class:`AggregationReport`.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     "id": ["a", "b", "c"], "HHI": [0.8, 0.3, 0.5], "CDI2": [0.6, 0.4, 0.5],
        ... })
        >>> config = AggregationConfig(id_columns=("id",), metric_columns=("HHI", "CDI2"))
        >>> methods = {
        ...     "sum": default_pipeline(config, weighting="equal"),
        ...     "geo": default_pipeline(config, weighting="equal", aggregation="geometric_mean"),
        ... }
        >>> df_scores, report = run_aggregation(df, config, methods)
        >>> sorted(df_scores.columns)
        ['geo', 'sum']
        >>> report.n_products
        3
    """
    X, index = split_frame(df_data, config)
    # I-01 : dominance et cohérence exigent la matrice orientée (haut = vulnérable) ;
    # les estimateurs, eux, reçoivent X brut et s'orientent via leur propre étape
    X_oriented = PolarityOrienter(polarity_vector(config)).fit_transform(X)

    scores: Dict[str, pd.Series] = {}
    for name, estimator in methods.items():
        values = estimator.fit(X).predict(X)
        scores[name] = attach_scores(values, index, name)
    df_scores = pd.concat(scores.values(), axis=1)

    coherence = compute_coherence_report(
        scores, X_oriented, dispute_threshold=dispute_threshold
    )
    report = AggregationReport(
        n_products=len(index),
        methods=list(methods),
        coherence=coherence,
        pareto_front_size=int(pareto_front(X_oriented).sum()),
    )
    return df_scores, report


# ──────────────────────────────────────────────────────────────────────
# Démarche recommandée (§7 de la note)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'exécution de la démarche recommandée par ordre de priorité
def recommended_workflow(
    df_data: pd.DataFrame,
    config: AggregationConfig,
    *,
    normalization: str = "minmax",
    winsorize_quantile: float = 0.99,
    smaa_n_draws: int = 10_000,
    smaa_k: int = 50,
    bootstrap_n: int = 200,
    dispute_threshold: int = 50,
    consider_optimal_transport: bool = True,
    ot_dimension_limit: int = 6,
    ot_min_group_size: int = 500,
    random_state: Optional[int] = None,
) -> Tuple[pd.DataFrame, AggregationReport]:
    """Run the note's priority-ordered workflow (§7): decreasing marginal value.

    1. Pareto front and dominance count — zero cost, zero assumption.
    2. SMAA on a weighted sum with entropy weights — answers the impossibility
       of ranking criteria with probabilised statements rather than a single
       fragile ranking.
    3. Two or three contrasted scores (CRITIC-weighted sum, geometric mean,
       TOPSIS) plus the coherence dashboard.
    4. Bootstrap of rank stability.
    5. The oriented Kantorovitch score, as soon as the group is large enough
       (``n >= ot_min_group_size``) and low-dimensional enough
       (``d <= ot_dimension_limit``) — the ellipticity diagnostic is
       *reported*, not a gate (D-07). Skipped silently (both
       ``report.ellipticity_tau_*`` stay ``None``) when the optional
       ``jax``/``ott-jax`` dependency is not installed.

    Args:
        df_data: Wide metric table.
        config: Column conventions.
        normalization: Scheme forwarded to :func:`default_pipeline`.
        winsorize_quantile: Quantile forwarded to :func:`default_pipeline`.
        smaa_n_draws: Number of Dirichlet weight draws for step 2.
        smaa_k: Rank depth of the SMAA confidence factor.
        bootstrap_n: Number of bootstrap draws for step 4.
        dispute_threshold: Rank-spread above which a product is flagged as
            disputed.
        consider_optimal_transport: Whether to attempt step 5 at all.
        ot_dimension_limit: Maximum ``d`` for which optimal transport is
            attempted — the map degrades with the dimension (M-17).
        ot_min_group_size: Minimum number of rows below which the transport is
            not estimated (D-11).
        random_state: Seed shared by the SMAA and bootstrap draws.

    Returns:
        Tuple ``(df_scores, report)``, the same shape as :func:`run_aggregation`,
        with ``report.bootstrap`` and ``report.smaa`` populated.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     "id": ["a", "b", "c", "d"],
        ...     "HHI": [0.9, 0.2, 0.5, 0.6],
        ...     "CDI2": [0.7, 0.3, 0.4, 0.5],
        ... })
        >>> config = AggregationConfig(id_columns=("id",), metric_columns=("HHI", "CDI2"))
        >>> df_scores, report = recommended_workflow(
        ...     df, config, smaa_n_draws=50, bootstrap_n=20, random_state=0)
        >>> "dominance_count" in df_scores.columns
        True
        >>> report.smaa is not None
        True
    """
    X, index = split_frame(df_data, config)
    # I-01 : orientation unique, préalable à tout calcul de dominance
    X_oriented = PolarityOrienter(polarity_vector(config)).fit_transform(X)

    # Etape 1 : front de Pareto et comptage de dominance, sans coût ni hypothèse
    front_mask = pareto_front(X_oriented)
    dominance = dominance_count(X_oriented)

    # Etape 2 : SMAA sur une somme pondérée, poids entropiques comme pivot
    preprocessing = Pipeline(
        [
            ("orient", PolarityOrienter(polarity_vector(config))),
            ("winsorize", Winsorizer(quantile=winsorize_quantile)),
            ("scale", make_normalizer(normalization)),
        ]
    )
    X_scaled = preprocessing.fit_transform(X)
    smaa = smaa_rank_acceptability(
        X_scaled,
        weighted_sum_score,
        k=smaa_k,
        n_draws=smaa_n_draws,
        random_state=random_state,
    )

    # Etape 3 : deux à trois scores contrastés, plus le tableau de bord de cohérence
    methods = {
        "critic_weighted_sum": default_pipeline(
            config,
            normalization=normalization,
            winsorize_quantile=winsorize_quantile,
            weighting="critic",
            aggregation="weighted_sum",
        ),
        "geometric_mean": default_pipeline(
            config,
            normalization=normalization,
            winsorize_quantile=winsorize_quantile,
            weighting="critic",
            aggregation="geometric_mean",
        ),
        "topsis": default_pipeline(
            config,
            normalization=normalization,
            winsorize_quantile=winsorize_quantile,
            weighting="critic",
            aggregation="topsis",
        ),
    }
    df_scores, report = run_aggregation(
        df_data, config, methods, dispute_threshold=dispute_threshold
    )
    df_scores.insert(0, "dominance_count", pd.Series(dominance, index=index))
    report.pareto_front_size = int(front_mask.sum())
    report.smaa = smaa

    # Etape 4 : bootstrap de la stabilité des rangs, sur la somme pondérée CRITIC
    report.bootstrap = bootstrap_rank_stability(
        df_data,
        config,
        lambda cfg: default_pipeline(cfg, normalization=normalization, weighting="critic"),
        n_boot=bootstrap_n,
        random_state=random_state,
    )

    # Etape 5 : transport optimal, dès que la taille et la dimension du groupe le
    # permettent (D-07 : l'ellipticité est un diagnostic) — dépendance optionnelle
    n, d = X.shape
    if consider_optimal_transport and d <= ot_dimension_limit and n >= ot_min_group_size:
        try:
            from .optimal_transport import OrientedKantorovichScorer, ellipticity_screen

            scorer = OrientedKantorovichScorer(seed=random_state or 0).fit(X_scaled)
            df_scores["oriented_kantorovich"] = pd.Series(
                scorer.predict(X_scaled), index=index
            )
            report.methods.append("oriented_kantorovich")
            screen = ellipticity_screen(X_scaled, scorer)
            report.ellipticity_tau_proj = screen["tau_proj"]
            report.ellipticity_tau_rank = screen["tau_rank"]
        except ImportError:
            # Dépendance optionnelle absente : étape silencieusement ignorée
            pass

    return df_scores, report
