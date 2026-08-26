# Importation des éléments d'intérêt du sous-module
# API normalisée
from .base import (
    VulnerabilityConfig,
    DEFAULT_CONFIG,
    VulnerabilityMetric,
    individual_partner_expr,
)
# Diagnostics et rapports structurés
from .diagnostics import (
    VulnerabilityReport,
    InputReport,
    CoverageReport,
    AggregateQualityReport,
    ScoreDistributionReport,
    DriftReport,
    compute_input_report,
    compute_coverage_report,
    compute_quality_report,
    compute_distribution_reports,
    compute_drift_report,
    compute_shares_frame,
    missing_aggregate_cells,
    log_vulnerability_artifacts,
)
# Métriques
from .metrics import (
    HerfindahlHirschmanIndex,
    ConcentrationDependencyIndex2,
    ConcentrationDependencyIndex3,
    DEFAULT_METRIC_CLASSES,
    default_metrics,
)
# Runner
from .runner import (
    compute_vulnerabilities,
    read_previous_result,
    run_vulnerabilities,
)

# Réexport des éléments d'intérêt du sous-module
__all__ = [
    # Base
    "VulnerabilityConfig",
    "DEFAULT_CONFIG",
    "VulnerabilityMetric",
    "individual_partner_expr",
    # Diagnostics et rapports structurés
    "VulnerabilityReport",
    "InputReport",
    "CoverageReport",
    "AggregateQualityReport",
    "ScoreDistributionReport",
    "DriftReport",
    "compute_input_report",
    "compute_coverage_report",
    "compute_quality_report",
    "compute_distribution_reports",
    "compute_drift_report",
    "compute_shares_frame",
    "missing_aggregate_cells",
    "log_vulnerability_artifacts",
    # Métriques
    "HerfindahlHirschmanIndex",
    "ConcentrationDependencyIndex2",
    "ConcentrationDependencyIndex3",
    "DEFAULT_METRIC_CLASSES",
    "default_metrics",
    # Runner
    "compute_vulnerabilities",
    "read_previous_result",
    "run_vulnerabilities",
]
