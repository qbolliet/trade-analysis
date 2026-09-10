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
    MetricReducer,
    ParetoScorer,
    dominance_count,
    dominance_count_chunked,
    epsilon_pareto_front,
    epsilon_pareto_set,
    non_dominated_sort,
    non_dominated_sort_sweep,
    normalized_dominance_depth,
    pareto_dominance_matrix,
    pareto_front,
    pareto_front_sweep,
)
# Pondérations endogènes
from .weights import (
    AutoWeightingReport,
    PcaWeightingReport,
    auto_weights,
    benefit_of_doubt_weights,
    critic_weights,
    dirichlet_weights,
    entropy_weights,
    pca_weights,
)
# Fonctions d'agrégation
from .functions import (
    cone_quantile_bounds,
    cone_quantile_score,
    geometric_mean_score,
    mahalanobis_score,
    mpi_score,
    rank_mean_score,
    topsis_score,
    vikor_score,
    weighted_sum_score,
    whitened_projection_score,
)
# Estimateurs sklearn (pondération + agrégation)
from .estimators import (
    AGGREGATION_REGISTRY,
    WEIGHTING_REGISTRY,
    BenefitOfDoubtScorer,
    ConeQuantileScorer,
    DominanceCountScorer,
    PcaProjectionScorer,
    SmaaScorer,
    WeightedAggregator,
)
# Protocole de comparaison/robustesse et classements consensus
from .diagnostics import (
    CoherenceReport,
    SmaaResult,
    ViolationRates,
    bootstrap_rank_stability,
    borda_rank,
    cluster_methods,
    compute_coherence_report,
    copeland_rank,
    dominance_violation_rate,
    front_rank_summary,
    kemeny_rank,
    kendall_tau_b_matrix,
    kendall_w,
    leave_one_metric_out,
    rank_biased_overlap,
    smaa_rank_acceptability,
    topk_overlap,
    weighted_tau_matrix,
)
# Transport optimal (dépendance optionnelle jax/ott-jax, import paresseux)
from .optimal_transport import (
    OrientedKantorovichReport,
    OrientedKantorovichScorer,
    ellipticity_screen,
    spherical_uniform_grid,
)
# Registre déclaratif des méthodes (configuration -> pipeline sklearn)
from .methods import (
    METHOD_REGISTRY,
    MethodRegistryEntry,
    MethodSpec,
    build_method,
    method_metrics,
    method_spec_from_mapping,
    registry_entry,
    resolve_normalization,
)
# Synthèse multiniveau (contexte x niveau x groupe x méthode)
from .synthesis import (
    DEFAULT_METHODS,
    LEVELS,
    LevelReport,
    SynthesisConfig,
    SynthesisReport,
    group_keys,
    iter_groups,
    log_synthesis_artifacts,
    run_synthesis,
    score_columns,
)
# Cohérence des métriques et des synthèses (contexte x niveau x groupe)
from .coherence import (
    COHERENCE_FAMILIES,
    CoherenceConfig,
    CoherenceLevelReport,
    CoherenceRunReport,
    log_coherence_artifacts,
    pareto_front_share_expected,
    run_coherence,
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
    "MetricReducer",
    "ParetoScorer",
    "dominance_count",
    "dominance_count_chunked",
    "epsilon_pareto_front",
    "epsilon_pareto_set",
    "non_dominated_sort",
    "non_dominated_sort_sweep",
    "normalized_dominance_depth",
    "pareto_dominance_matrix",
    "pareto_front",
    "pareto_front_sweep",
    # Pondérations
    "AutoWeightingReport",
    "PcaWeightingReport",
    "auto_weights",
    "benefit_of_doubt_weights",
    "critic_weights",
    "dirichlet_weights",
    "entropy_weights",
    "pca_weights",
    # Fonctions d'agrégation
    "cone_quantile_bounds",
    "cone_quantile_score",
    "geometric_mean_score",
    "mahalanobis_score",
    "mpi_score",
    "rank_mean_score",
    "topsis_score",
    "vikor_score",
    "weighted_sum_score",
    "whitened_projection_score",
    # Estimateurs
    "AGGREGATION_REGISTRY",
    "WEIGHTING_REGISTRY",
    "BenefitOfDoubtScorer",
    "ConeQuantileScorer",
    "DominanceCountScorer",
    "PcaProjectionScorer",
    "SmaaScorer",
    "WeightedAggregator",
    # Diagnostics / comparaison / consensus
    "CoherenceReport",
    "SmaaResult",
    "ViolationRates",
    "bootstrap_rank_stability",
    "borda_rank",
    "cluster_methods",
    "compute_coherence_report",
    "copeland_rank",
    "dominance_violation_rate",
    "front_rank_summary",
    "kemeny_rank",
    "kendall_tau_b_matrix",
    "kendall_w",
    "leave_one_metric_out",
    "rank_biased_overlap",
    "smaa_rank_acceptability",
    "topk_overlap",
    "weighted_tau_matrix",
    # Transport optimal
    "OrientedKantorovichReport",
    "OrientedKantorovichScorer",
    "ellipticity_screen",
    "spherical_uniform_grid",
    # Méthodes déclaratives
    "METHOD_REGISTRY",
    "MethodRegistryEntry",
    "MethodSpec",
    "build_method",
    "method_metrics",
    "method_spec_from_mapping",
    "registry_entry",
    "resolve_normalization",
    # Synthèse multiniveau
    "DEFAULT_METHODS",
    "LEVELS",
    "LevelReport",
    "SynthesisConfig",
    "SynthesisReport",
    "group_keys",
    "iter_groups",
    "log_synthesis_artifacts",
    "run_synthesis",
    "score_columns",
    # Cohérence
    "COHERENCE_FAMILIES",
    "CoherenceConfig",
    "CoherenceLevelReport",
    "CoherenceRunReport",
    "log_coherence_artifacts",
    "pareto_front_share_expected",
    "run_coherence",
    # Orchestration
    "AggregationReport",
    "default_pipeline",
    "recommended_workflow",
    "run_aggregation",
]
