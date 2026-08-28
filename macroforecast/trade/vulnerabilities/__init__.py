# Importation des éléments d'intérêt du sous-module
# API normalisée
from .base import (
    VulnerabilityConfig,
    DEFAULT_CONFIG,
    VulnerabilityMetric,
    NetworkVulnerabilityConfig,
    DEFAULT_NETWORK_CONFIG,
    NetworkVulnerabilityMetric,
    ScoreConfig,
    individual_partner_expr,
)
# Diagnostics et rapports structurés
from .diagnostics import (
    VulnerabilityReport,
    NetworkVulnerabilityReport,
    InputReport,
    CoverageReport,
    AggregateQualityReport,
    GraphQualityReport,
    ScoreDistributionReport,
    DriftReport,
    compute_input_report,
    compute_coverage_report,
    compute_quality_report,
    compute_distribution_reports,
    compute_drift_report,
    compute_shares_frame,
    missing_aggregate_cells,
    unscored_cells,
    metric_alert_threshold,
    append_alert_flags,
    log_vulnerability_artifacts,
    log_network_vulnerability_artifacts,
)
# Primitives de graphe (métriques de réseau)
from .graph import (
    GraphReport,
    GRAPH_FEATURES,
    compute_graph_features,
    diameter,
    weighted_clustering,
)
# Métriques partenaires
from .metrics import (
    HerfindahlHirschmanIndex,
    ConcentrationDependencyIndex2,
    ConcentrationDependencyIndex3,
    DEFAULT_METRIC_CLASSES,
    default_metrics,
)
# Métriques de réseau
from .network_metrics import (
    WeightedOutdegreeCentralityRisk,
    WeightedClusteringCoefficient,
    NetworkDiameter,
    WorldExportConcentration,
    SinglePointOfFailureRisk,
    SinglePointOfFailureDecile,
    DEFAULT_NETWORK_METRIC_CLASSES,
    default_network_metrics,
)
# Runner
from .runner import (
    compute_vulnerabilities,
    read_previous_result,
    run_vulnerabilities,
    compute_network_vulnerabilities,
    read_previous_network_result,
    run_network_vulnerabilities,
)

# Réexport des éléments d'intérêt du sous-module
__all__ = [
    # Base
    "VulnerabilityConfig",
    "DEFAULT_CONFIG",
    "VulnerabilityMetric",
    "NetworkVulnerabilityConfig",
    "DEFAULT_NETWORK_CONFIG",
    "NetworkVulnerabilityMetric",
    "ScoreConfig",
    "individual_partner_expr",
    # Diagnostics et rapports structurés
    "VulnerabilityReport",
    "NetworkVulnerabilityReport",
    "InputReport",
    "CoverageReport",
    "AggregateQualityReport",
    "GraphQualityReport",
    "ScoreDistributionReport",
    "DriftReport",
    "compute_input_report",
    "compute_coverage_report",
    "compute_quality_report",
    "compute_distribution_reports",
    "compute_drift_report",
    "compute_shares_frame",
    "missing_aggregate_cells",
    "unscored_cells",
    "metric_alert_threshold",
    "append_alert_flags",
    "log_vulnerability_artifacts",
    "log_network_vulnerability_artifacts",
    # Primitives de graphe
    "GraphReport",
    "GRAPH_FEATURES",
    "compute_graph_features",
    "diameter",
    "weighted_clustering",
    # Métriques partenaires
    "HerfindahlHirschmanIndex",
    "ConcentrationDependencyIndex2",
    "ConcentrationDependencyIndex3",
    "DEFAULT_METRIC_CLASSES",
    "default_metrics",
    # Métriques de réseau
    "WeightedOutdegreeCentralityRisk",
    "WeightedClusteringCoefficient",
    "NetworkDiameter",
    "WorldExportConcentration",
    "SinglePointOfFailureRisk",
    "SinglePointOfFailureDecile",
    "DEFAULT_NETWORK_METRIC_CLASSES",
    "default_network_metrics",
    # Runner
    "compute_vulnerabilities",
    "read_previous_result",
    "run_vulnerabilities",
    "compute_network_vulnerabilities",
    "read_previous_network_result",
    "run_network_vulnerabilities",
]
