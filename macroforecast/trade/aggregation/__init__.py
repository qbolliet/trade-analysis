# Importation des éléments d'intérêt du sous-module
# Conventions de colonnes
from .base import (
    AggregationConfig,
    attach_scores,
    polarity_vector,
    split_frame,
)
# Prétraitement : polarité, winsorisation, normalisation, diagnostics de corrélation
from .preprocessing import (
    CorrelationDiagnostics,
    GaussianQuantileScaler,
    MedianMadScaler,
    NORMALIZER_REGISTRY,
    PolarityOrienter,
    RankScaler,
    Winsorizer,
    bartlett_sphericity,
    compute_correlation_diagnostics,
    kmo_statistic,
    make_normalizer,
    spearman_correlation_matrix,
)
# Dominance de Pareto
from .pareto import (
    dominance_count,
    epsilon_pareto_front,
    non_dominated_sort,
    normalized_dominance_depth,
    pareto_dominance_matrix,
    pareto_front,
)
# Pondérations endogènes
from .weights import (
    PcaWeightingReport,
    benefit_of_doubt_weights,
    critic_weights,
    dirichlet_weights,
    entropy_weights,
    pca_weights,
)
# Fonctions d'agrégation
from .functions import (
    geometric_mean_score,
    mahalanobis_score,
    mpi_score,
    topsis_score,
    weighted_sum_score,
)
# Estimateurs sklearn (pondération + agrégation)
from .estimators import (
    AGGREGATION_REGISTRY,
    WEIGHTING_REGISTRY,
    DominanceCountScorer,
    WeightedAggregator,
)
# Protocole de comparaison/robustesse et classements consensus
from .diagnostics import (
    CoherenceReport,
    SmaaResult,
    bootstrap_rank_stability,
    borda_rank,
    cluster_methods,
    compute_coherence_report,
    copeland_rank,
    dominance_violation_rate,
    kemeny_rank,
    kendall_tau_b_matrix,
    kendall_w,
    leave_one_metric_out,
    rank_biased_overlap,
    smaa_rank_acceptability,
    topk_overlap,
)
# Transport optimal (dépendance optionnelle jax/ott-jax, import paresseux)
from .optimal_transport import (
    OrientedKantorovichReport,
    OrientedKantorovichScorer,
    ellipticity_screen,
    spherical_uniform_grid,
)
# Orchestration
from .runner import (
    AggregationReport,
    default_pipeline,
    recommended_workflow,
    run_aggregation,
)

# Réexport des éléments d'intérêt du sous-module
__all__ = [
    # Base
    "AggregationConfig",
    "attach_scores",
    "polarity_vector",
    "split_frame",
    # Prétraitement
    "CorrelationDiagnostics",
    "GaussianQuantileScaler",
    "MedianMadScaler",
    "NORMALIZER_REGISTRY",
    "PolarityOrienter",
    "RankScaler",
    "Winsorizer",
    "bartlett_sphericity",
    "compute_correlation_diagnostics",
    "kmo_statistic",
    "make_normalizer",
    "spearman_correlation_matrix",
    # Pareto
    "dominance_count",
    "epsilon_pareto_front",
    "non_dominated_sort",
    "normalized_dominance_depth",
    "pareto_dominance_matrix",
    "pareto_front",
    # Pondérations
    "PcaWeightingReport",
    "benefit_of_doubt_weights",
    "critic_weights",
    "dirichlet_weights",
    "entropy_weights",
    "pca_weights",
    # Fonctions d'agrégation
    "geometric_mean_score",
    "mahalanobis_score",
    "mpi_score",
    "topsis_score",
    "weighted_sum_score",
    # Estimateurs
    "AGGREGATION_REGISTRY",
    "WEIGHTING_REGISTRY",
    "DominanceCountScorer",
    "WeightedAggregator",
    # Diagnostics / comparaison / consensus
    "CoherenceReport",
    "SmaaResult",
    "bootstrap_rank_stability",
    "borda_rank",
    "cluster_methods",
    "compute_coherence_report",
    "copeland_rank",
    "dominance_violation_rate",
    "kemeny_rank",
    "kendall_tau_b_matrix",
    "kendall_w",
    "leave_one_metric_out",
    "rank_biased_overlap",
    "smaa_rank_acceptability",
    "topk_overlap",
    # Transport optimal
    "OrientedKantorovichReport",
    "OrientedKantorovichScorer",
    "ellipticity_screen",
    "spherical_uniform_grid",
    # Orchestration
    "AggregationReport",
    "default_pipeline",
    "recommended_workflow",
    "run_aggregation",
]
