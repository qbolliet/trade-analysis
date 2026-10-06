# Importation des éléments d'intérêt du sous-module
# Configuration
from .baci import (
    ComtradeSchema,
    BaciConfig,
    DEFAULT_CONFIG,
    BACI_FINGERPRINT_EXCLUDED,
)
# Préparation des données
from .baci import (
    build_gravity_data,
    build_mirror_flows,
    infer_import_valuation_regime,
    world_median_unit_values,
    world_median_unit_values_sql,
    required_columns,
)
# Étapes du redressement
from .baci import (
    TonnageConverter,
    CifGravityModel,
    Fobizer,
    ReportingQualityModel,
    QualityResult,
    GravityFit,
    MirrorReconciler,
    AreaNesReallocator,
)
# Rapports d'étape (principe P3 : les diagnostics sont des données)
from .baci import (
    TonnageReport,
    GravityReport,
    FobisationReport,
    MirrorReport,
    QualityReport,
    NesReport,
)
# Ré-agrégation
from .aggregation import (
    aggregate_measures,
)
# Harmonisation des nomenclatures HS
from .classification import (
    HsHarmonizer,
    HsHarmonizationReport,
    build_conversion_map,
    conversion_preimage,
    harmonize_partner_flows,
    merge_harmonization_reports,
    resolve_vintage,
)
# Orchestration
from .baci import (
    BaciReport,
    run_baci,
)
# Orchestration par passes sur des tranches (mémoire bornée)
from .baci import (
    BaciPassIO,
    BaciPassState,
    ChunkKey,
    FREIGHT_QUANTILES,
    InMemoryPassIO,
    run_baci_passes,
)
# Accumulateurs de statistiques suffisantes
from .streaming import (
    AbsorbedWLSAccumulator,
    CookFilter,
    WelfordGroupStats,
    WeightedLeastSquaresAccumulator,
    WlsSolution,
)

# Réexport des éléments d'intérêt du sous-module
__all__ = [
    # Configuration
    "ComtradeSchema",
    "BaciConfig",
    "DEFAULT_CONFIG",
    "BACI_FINGERPRINT_EXCLUDED",
    # Préparation des données
    "build_gravity_data",
    "build_mirror_flows",
    "infer_import_valuation_regime",
    "world_median_unit_values",
    "world_median_unit_values_sql",
    "required_columns",
    # Étapes du redressement
    "TonnageConverter",
    "CifGravityModel",
    "Fobizer",
    "ReportingQualityModel",
    "QualityResult",
    "GravityFit",
    "MirrorReconciler",
    "AreaNesReallocator",
    # Rapports d'étape
    "TonnageReport",
    "GravityReport",
    "FobisationReport",
    "MirrorReport",
    "QualityReport",
    "NesReport",
    # Ré-agrégation partagée
    "aggregate_measures",
    # Harmonisation des nomenclatures HS
    "HsHarmonizer",
    "HsHarmonizationReport",
    "build_conversion_map",
    "conversion_preimage",
    "harmonize_partner_flows",
    "merge_harmonization_reports",
    "resolve_vintage",
    # Orchestration
    "BaciReport",
    "run_baci",
    # Orchestration par passes
    "BaciPassIO",
    "BaciPassState",
    "ChunkKey",
    "FREIGHT_QUANTILES",
    "InMemoryPassIO",
    "run_baci_passes",
    # Accumulateurs de statistiques suffisantes
    "AbsorbedWLSAccumulator",
    "CookFilter",
    "WelfordGroupStats",
    "WeightedLeastSquaresAccumulator",
    "WlsSolution",
]
