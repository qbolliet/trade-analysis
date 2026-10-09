"""Factories of the typed configurations of the pipeline steps.

Every factory turns a parameter block (a plain mapping, as read by Kedro or by
the scripts) into the frozen dataclass of a methodology, or into the identity
of a DuckLake table, with the same generic rule: a key naming a field overrides
its default, an unknown key is dropped with a warning, YAML lists become the
tuples the frozen dataclasses expect.

No environment variable and no YAML path are read here, except the CPU count
fallback of :func:`incremental_settings` when no explicit ``n_jobs`` is given.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, fields, replace
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Modules du package
from kedro_pipeline.config import active_targets
from kedro_pipeline.io.ducklake import DuckLakeLocation
from kedro_pipeline.parallel import resolve_n_jobs
from macroforecast.trade.aggregation import (
    CoherenceConfig,
    SynthesisConfig,
    method_spec_from_mapping,
)
from macroforecast.trade.processing import DEFAULT_CONFIG as DEFAULT_BACI_CONFIG
from macroforecast.trade.processing import BaciConfig, ComtradeSchema
from macroforecast.trade.vulnerabilities import (
    DEFAULT_CONFIG,
    DEFAULT_NETWORK_CONFIG,
    NetworkVulnerabilityConfig,
    VulnerabilityConfig,
    flow_code_map,
    parse_flows,
)

# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé YAML portant le backend de calcul narwhals (hors configurations méthodologiques)
_BACKEND_KEY = "BACKEND"
# Clé YAML des sens de flux calculés (racine du bloc)
_FLOWS_KEY = "FLOWS"
# Clé YAML portant les conventions de schéma des sources BACI (sous-section de PARAMETERS)
_SCHEMA_KEY = "SCHEMA"
# Colonnes de la grille partenaires portant le flux et le millésime SH de
# rattachement — faits de schéma source, clés de contexte de la synthèse
_FLOW_COLUMN = "flow"
_VINTAGE_COLUMN = "hs_vintage"
# Colonne temporelle de la grille partenaires (clé de contexte)
_PERIOD_COLUMN = "TIME_PERIOD"
# Valeurs admises de `SYNTHESIS.VINTAGES`
VINTAGE_MODES: Tuple[str, ...] = ("in_force", "all")


# ──────────────────────────────────────────────────────────────────────
# Vulnérabilités partenaires et réseau
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction de la configuration méthodologique des vulnérabilités
def vulnerability_config_from_params(params: Optional[Dict]) -> VulnerabilityConfig:
    """Build a ``VulnerabilityConfig`` from the YAML ``PARAMETERS`` section.

    Generic construction : every key matching a ``VulnerabilityConfig``
    field name overrides the dataclass default; unknown keys are ignored with a
    warning. YAML lists are coerced to the tuple types the frozen dataclass
    expects, nested pairs included (``metric_alert_thresholds``). The ``BACKEND``
    key is skipped: it drives the narwhals execution backend, not the
    methodology.

    Args:
        params: The ``PARAMETERS`` mapping of the ``vulnerabilities`` parameters
            (or ``None``, meaning the default Comext conventions).

    Returns:
        A ``VulnerabilityConfig`` reflecting the configured overrides.
    """
    # Aucune surcharge : configuration par défaut
    if not params:
        return DEFAULT_CONFIG

    # Surcharge générique champ à champ, avec coercition listes → tuples
    valid = {field.name for field in fields(VulnerabilityConfig)}
    overrides: Dict[str, Any] = {}
    for key, value in params.items():
        # Backend d'exécution : lu à part, pas un paramètre méthodologique
        if key == _BACKEND_KEY:
            continue
        if key not in valid:
            # Logging
            logger.warning(f"Paramètre de vulnérabilité inconnu ignoré : {key}")
            continue
        default = getattr(DEFAULT_CONFIG, key)
        if isinstance(default, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(item) if isinstance(item, (list, tuple)) else item
                for item in value
            )
        overrides[key] = value

    return replace(DEFAULT_CONFIG, **overrides)

# Fonction de lecture des sens de flux calculés
def load_flows(block: Optional[Mapping[str, Any]]) -> Tuple[str, ...]:
    """Read the flow directions to compute from a configuration block.

    Args:
        block: Mapping holding a ``FLOWS`` key (the root of
            the ``vulnerabilities`` parameters, or its ``NETWORK_VULNERABILITIES``
            block). An absent key keeps the import direction alone, the
            behaviour of the metrics before the export directions existed.

    Returns:
        The validated directions, in configuration order.

    Raises:
        ValueError: If ``FLOWS`` is empty or names an unknown direction.

    Examples:
        >>> load_flows({"FLOWS": ["import", "export"]})
        ('import', 'export')
        >>> load_flows({})
        ('import',)
    """
    return parse_flows((block or {}).get(_FLOWS_KEY, ["import"]))



# Fonction de construction de la configuration méthodologique des métriques de réseau
def network_config_from_params(params: Optional[Dict]) -> NetworkVulnerabilityConfig:
    """Build a ``NetworkVulnerabilityConfig`` from the YAML ``PARAMETERS`` section.

    Generic construction, twin of ``vulnerability_config_from_params``: every key
    matching a ``NetworkVulnerabilityConfig`` field name overrides the dataclass
    default; unknown keys are ignored with a warning. YAML lists are coerced to
    the tuple types the frozen dataclass expects, nested pairs included
    (``metric_alert_thresholds``). The ``BACKEND`` key is skipped: it drives the
    narwhals execution backend, not the methodology.

    Args:
        params: The ``NETWORK_VULNERABILITIES.PARAMETERS`` mapping of
            the ``vulnerabilities`` parameters (or ``None``, meaning the default
            BACI conventions).

    Returns:
        A ``NetworkVulnerabilityConfig`` reflecting the configured overrides.
    """
    # Aucune surcharge : configuration par défaut
    if not params:
        return DEFAULT_NETWORK_CONFIG

    # Surcharge générique champ à champ, avec coercition listes → tuples
    valid = {field.name for field in fields(NetworkVulnerabilityConfig)}
    overrides: Dict[str, Any] = {}
    for key, value in params.items():
        # Backend d'exécution : lu à part, pas un paramètre méthodologique
        if key == _BACKEND_KEY:
            continue
        if key not in valid:
            # Logging
            logger.warning(f"Paramètre de vulnérabilité réseau inconnu ignoré : {key}")
            continue
        default = getattr(DEFAULT_NETWORK_CONFIG, key)
        if isinstance(default, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(item) if isinstance(item, (list, tuple)) else item
                for item in value
            )
        overrides[key] = value

    return replace(DEFAULT_NETWORK_CONFIG, **overrides)


# ──────────────────────────────────────────────────────────────────────
# BACI
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction des conventions de schéma des sources
def comtrade_schema_from_params(params: Optional[Dict]) -> ComtradeSchema:
    """Build a ``ComtradeSchema`` from the YAML ``PARAMETERS.SCHEMA`` sub-section.

    Generic construction: every key matching a ``ComtradeSchema`` field name
    overrides the dataclass default; unknown keys are ignored with a warning.

    Args:
        params: The ``PARAMETERS.SCHEMA`` mapping of the ``baci`` parameters (or
            ``None``, meaning the default COMTRADE/CEPII conventions).

    Returns:
        A ``ComtradeSchema`` reflecting the configured overrides.
    """
    # Aucune surcharge : conventions de schéma par défaut
    if not params:
        return DEFAULT_BACI_CONFIG.schema

    # Surcharge générique champ à champ (noms de colonnes et codes de flux)
    valid = {f.name for f in fields(ComtradeSchema)}
    overrides: Dict[str, object] = {}
    for key, value in params.items():
        if key not in valid:
            logger.warning("Champ de schéma BACI inconnu ignoré : %s", key)
            continue
        overrides[key] = value

    return replace(DEFAULT_BACI_CONFIG.schema, **overrides)

# Fonction de construction de la configuration méthodologique BACI
def baci_config_from_params(params: Optional[Dict]) -> BaciConfig:
    """Build a ``BaciConfig`` from the YAML ``parameters`` section.

    Generic construction: every key matching a ``BaciConfig`` field name
    overrides the dataclass default; unknown keys are ignored with a warning.
    YAML lists are coerced to the tuple types expected by the frozen dataclass
    (including nested pairs such as ``excluded_pairs``). The nested ``SCHEMA``
    sub-section carries the source column conventions and is delegated to
    :func:`comtrade_schema_from_params`.

    Args:
        params: The ``parameters`` mapping of the ``baci`` parameters (or ``None``).

    Returns:
        A ``BaciConfig`` reflecting the configured overrides.
    """
    # Aucune surcharge : configuration par défaut
    if not params:
        return DEFAULT_BACI_CONFIG

    # Conventions de schéma issues de la sous-section dédiée
    schema = comtrade_schema_from_params(params.get(_SCHEMA_KEY))

    # Surcharge générique champ à champ, avec coercition listes → tuples
    valid = {f.name for f in fields(BaciConfig)} - {"schema"}
    overrides: Dict[str, object] = {}
    for key, value in params.items():
        # Sous-section de schéma déjà traitée
        if key == _SCHEMA_KEY:
            continue
        if key not in valid:
            logger.warning("Paramètre BACI inconnu ignoré : %s", key)
            continue
        default = getattr(DEFAULT_BACI_CONFIG, key)
        if isinstance(default, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(v) if isinstance(v, (list, tuple)) else v for v in value
            )
        overrides[key] = value

    return replace(DEFAULT_BACI_CONFIG, schema=schema, **overrides)

# Fonction de résolution des premières années des millésimes cibles
def resolve_target_start_years(
    targets: Mapping[str, Mapping[str, Any]],
    runtime_config: Mapping[str, Any],
) -> Dict[str, int]:
    """Resolve the first year of each BACI target vintage.

    A vintage starts the year it enters into force, never before the first year of the
    Comtrade analysis.

    An explicit ``START_YEAR`` is kept; a null one resolves to
    ``max(runtime.NOMENCLATURES.HS[vintage], runtime.ANALYSIS_START_YEAR.comtrade)``.

    Args:
        targets: ``CLASSIFICATIONS.TARGETS`` of the ``baci`` parameters
            (null entries, disabled vintages, are skipped).
        runtime_config: Parsed ``runtime`` mapping.

    Returns:
        Mapping ``vintage -> first year`` of the enabled targets.

    Raises:
        KeyError: If a null ``START_YEAR`` vintage is absent from
            ``runtime.NOMENCLATURES.HS``.

    Examples:
        >>> resolve_target_start_years(
        ...     {"HS1992": {"START_YEAR": None}, "HS2017": {"START_YEAR": 2017}},
        ...     {"NOMENCLATURES": {"HS": {"HS1992": 1988}},
        ...      "ANALYSIS_START_YEAR": {"comtrade": 1994}},
        ... )
        {'HS1992': 1994, 'HS2017': 2017}
    """
    analysis_start = int(runtime_config["ANALYSIS_START_YEAR"]["comtrade"])
    in_force = runtime_config["NOMENCLATURES"]["HS"]
    return {
        label: (
            int(cfg["START_YEAR"])
            if cfg.get("START_YEAR") is not None
            else max(int(in_force[label]), analysis_start)
        )
        for label, cfg in active_targets(targets).items()
    }


# ──────────────────────────────────────────────────────────────────────
# Synthèse et cohérence
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction de la configuration méthodologique de la synthèse
def synthesis_config_from_params(params: Optional[Mapping[str, Any]]) -> SynthesisConfig:
    """Build a ``SynthesisConfig`` from the YAML ``PARAMETERS`` section.

    Generic construction, twin of ``network_config_from_params`` /
    ``vulnerability_config_from_params`` of the sibling scripts: every key
    matching a ``SynthesisConfig`` field name overrides the dataclass default;
    unknown keys are dropped with a warning. YAML lists are coerced to the tuple
    types the frozen dataclass expects, nested pairs included (``polarities``).
    The ``methods`` list is routed through
    :func:`~macroforecast.trade.aggregation.method_spec_from_mapping`, one
    ``MethodSpec`` per entry.

    Args:
        params: The ``SYNTHESIS.PARAMETERS`` mapping of
            ``config/synthesis.yaml`` (or ``None``, meaning the defaults of
            :class:`~macroforecast.trade.aggregation.SynthesisConfig`).

    Returns:
        A ``SynthesisConfig`` reflecting the configured overrides.

    Examples:
        >>> config = synthesis_config_from_params({"levels": ["global"]})
        >>> config.levels
        ('global',)
    """
    # Aucune surcharge : configuration par défaut
    default = SynthesisConfig()
    if not params:
        return default

    # Surcharge générique champ à champ
    valid = {field.name for field in fields(SynthesisConfig)}
    overrides: Dict[str, Any] = {}
    for key, value in params.items():
        if key not in valid:
            # Logging
            logger.warning(f"Paramètre de synthèse inconnu ignoré : {key}")
            continue
        # Liste de méthodes : une spécification gelée par entrée
        if key == "methods":
            overrides[key] = tuple(
                method_spec_from_mapping(entry) for entry in value
            )
            continue
        # Coercition listes → tuples pour les champs tuple du dataclass gelé,
        # paires imbriquées comprises (polarities)
        current = getattr(default, key)
        if isinstance(current, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(item) if isinstance(item, (list, tuple)) else item
                for item in value
            )
        overrides[key] = value

    return replace(default, **overrides)

# Fonction de validation du contexte de comparaison au regard des sens synthétisés
def validate_flow_context(config: SynthesisConfig, flows: Sequence[str]) -> None:
    """Check that import and export cells can never be compared together.

    The synthesis compares cells within a context only (``context_columns``):
    with more than one direction, the flow column must be part of the context,
    otherwise an importer's and an exporter's scores would be ranked against
    each other. The polarities need no such care: a more concentrated cell is
    a more vulnerable one in both directions (suppliers at the import, outlets
    at the export), for the partner and the network metrics alike.

    Args:
        config: Synthesis configuration.
        flows: Directions synthesised.

    Raises:
        ValueError: If more than one direction is synthesised and the flow
            column is not a context column.

    Examples:
        >>> validate_flow_context(SynthesisConfig(context_columns=("flow",)), ["import", "export"])
        >>> validate_flow_context(SynthesisConfig(context_columns=("freq",)), ["import"])
    """
    if len(flows) > 1 and _FLOW_COLUMN not in config.context_columns:
        raise ValueError(
            f"FLOWS={list(flows)} requires '{_FLOW_COLUMN}' in context_columns "
            f"(got {list(config.context_columns)}): import and export cells must "
            "never be compared together."
        )

# Fonction de lecture des sens synthétisés et de leurs codes
def load_synthesis_flows(
    synthesis_block: Mapping[str, Any],
    vulnerability_config: Mapping[str, Any],
    config: SynthesisConfig,
) -> Tuple[Tuple[str, ...], List[int]]:
    """Read the synthesised directions and translate them into flow codes.

    Args:
        synthesis_block: ``SYNTHESIS`` block of ``config/synthesis.yaml``
            (``FLOWS`` key; the import direction alone when absent).
        vulnerability_config: Whole ``config/vulnerabilities.yaml``: the codes
            are those of the partner ``PARAMETERS`` (``import_flow`` /
            ``export_flow``), never literals of this script.
        config: Synthesis configuration (context columns checked).

    Returns:
        ``(flows, flow_codes)``.

    Raises:
        ValueError: If ``FLOWS`` is invalid, or names several directions while
            the flow column is not a context column.

    Examples:
        >>> load_synthesis_flows({"FLOWS": ["import", "export"]}, {}, SynthesisConfig())
        (('import', 'export'), [1, 2])
    """
    flows = load_flows(synthesis_block)
    validate_flow_context(config, flows)
    codes = flow_code_map(
        vulnerability_config_from_params(vulnerability_config.get("PARAMETERS"))
    )
    return flows, [codes[flow] for flow in flows]

# Fonction de lecture du périmètre de nomenclature synthétisé
def load_synthesis_vintages(
    synthesis_block: Mapping[str, Any], config: SynthesisConfig
) -> str:
    """Read which partner rows the synthesis scores, by nomenclature.

    ``"in_force"`` keeps the rows of the nomenclature in force (codes as
    declared, the dashboard's yearly view); ``"all"`` also scores the
    historical rows (flows of later years converted into an older HS vintage),
    which then requires the HS vintage among the context columns: a row in
    force and a historical row of the same period must never be ranked
    together.

    Args:
        synthesis_block: ``SYNTHESIS`` block (``VINTAGES`` key, ``"in_force"``
            when absent).
        config: Synthesis configuration (context columns checked).

    Returns:
        ``"in_force"`` or ``"all"``.

    Raises:
        ValueError: If the value is unknown, or ``"all"`` without the HS
            vintage in the context columns.

    Examples:
        >>> load_synthesis_vintages({}, SynthesisConfig())
        'in_force'
        >>> load_synthesis_vintages({"VINTAGES": "all"},
        ...                         SynthesisConfig(context_columns=("hs_vintage", "flow")))
        'all'
    """
    vintages = synthesis_block.get("VINTAGES", "in_force")
    if vintages not in VINTAGE_MODES:
        raise ValueError(f"SYNTHESIS.VINTAGES must be one of {VINTAGE_MODES}, got {vintages!r}")
    if vintages == "all" and _VINTAGE_COLUMN not in config.context_columns:
        raise ValueError(
            f"SYNTHESIS.VINTAGES='all' requires '{_VINTAGE_COLUMN}' in context_columns "
            f"(got {list(config.context_columns)}): rows of two vintages must never be "
            "compared together."
        )
    return vintages

# Réglages de l'exécution incrémentale d'une étape par contexte
@dataclass(frozen=True)
class IncrementalSettings:
    """Run settings of a per-context step (synthesis or coherence).

    Attributes:
        recent_periods: Number of most recent periods recomputed when the
            partner metrics have new data (the incremental download only
            brings back the last observations); ``None`` treats every period
            as recent.
        max_contexts: Catch-up budget: maximum number of contexts computed in
            one run, the most recent periods first, the others left to the
            next runs; ``None`` (nominal regime) computes every stale context.
        write_batch_contexts: Number of contexts read, written and recorded
            together.
        min_interval_days: Minimum interval between two runs under
            ``--cadence-check``.
        period_column: Context column holding the period.
        n_jobs: Number of worker processes computing the contexts.
        sequential_methods: Methods computed in the parent process once the
            parallel phase is over (memory-hungry ones such as the optimal
            transport score), for the same contexts.

    Examples:
        >>> IncrementalSettings().max_contexts is None
        True
    """
    recent_periods: Optional[int] = None
    max_contexts: Optional[int] = None
    write_batch_contexts: int = 50
    min_interval_days: float = 6.0
    period_column: str = _PERIOD_COLUMN
    n_jobs: int = 1
    sequential_methods: Tuple[str, ...] = ()

# Fonction de lecture de la profondeur de l'incrémental Eurostat
def default_recent_periods(eurostat_config: Optional[Mapping[str, Any]]) -> Optional[int]:
    """Number of periods the incremental partner download brings back.

    Args:
        eurostat_config: Parsed Eurostat download configuration, or ``None``.

    Returns:
        The largest ``N_LAST_OBSERVATIONS`` found in it, or ``None`` when none
        is set (every period then counts as recent, the conservative choice).

    Examples:
        >>> default_recent_periods({"DOWNLOADS": {"DS": {"N_LAST_OBSERVATIONS": 10}}})
        10
        >>> default_recent_periods(None) is None
        True
    """
    found: List[int] = []

    # Parcours récursif : la clé vit sous le bloc de chaque dataflow
    def walk(node: Any) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                if key == "N_LAST_OBSERVATIONS" and value is not None:
                    found.append(int(value))
                else:
                    walk(value)

    walk(eurostat_config or {})
    return max(found) if found else None

# Fonction de lecture des réglages incrémentaux d'un bloc de configuration
def incremental_settings(
    block: Mapping[str, Any],
    eurostat_config: Optional[Mapping[str, Any]] = None,
    *,
    n_jobs: Optional[int] = None,
) -> IncrementalSettings:
    """Read the incremental settings of a ``SYNTHESIS`` or ``COHERENCE`` block.

    Args:
        block: Configuration block (keys ``RECENT_PERIODS``,
            ``MAX_CONTEXTS_PER_RUN``, ``WRITE_BATCH_CONTEXTS``,
            ``CADENCE.MIN_INTERVAL_DAYS``, ``N_JOBS`` and
            ``SEQUENTIAL_METHODS``). ``N_JOBS`` null resolves to the CPU of the
            pod (see :func:`~kedro_pipeline.parallel.resolve_n_jobs`) unless
            ``n_jobs`` is given.
        eurostat_config: Eurostat download configuration, the default of
            ``RECENT_PERIODS``.
        n_jobs: Number of worker processes resolved by the caller; the step
            functions always give it, so that they never read the environment.

    Returns:
        The settings.

    Raises:
        ValueError: If a count is not a positive integer.

    Examples:
        >>> incremental_settings({"MAX_CONTEXTS_PER_RUN": 2, "RECENT_PERIODS": 3}).max_contexts
        2
    """
    recent = block.get("RECENT_PERIODS")
    if recent is None:
        recent = default_recent_periods(eurostat_config)
    budget = block.get("MAX_CONTEXTS_PER_RUN")
    batch = block.get("WRITE_BATCH_CONTEXTS") or IncrementalSettings.write_batch_contexts
    interval = (block.get("CADENCE") or {}).get("MIN_INTERVAL_DAYS")
    for name, value in (("RECENT_PERIODS", recent), ("MAX_CONTEXTS_PER_RUN", budget),
                        ("WRITE_BATCH_CONTEXTS", batch)):
        if value is not None and int(value) < 1:
            raise ValueError(f"{name} must be a positive integer, got {value!r}")
    return IncrementalSettings(
        recent_periods=int(recent) if recent is not None else None,
        max_contexts=int(budget) if budget is not None else None,
        write_batch_contexts=int(batch),
        min_interval_days=(
            float(interval) if interval is not None else IncrementalSettings.min_interval_days
        ),
        n_jobs=int(n_jobs) if n_jobs is not None else resolve_n_jobs(block.get("N_JOBS")),
        sequential_methods=tuple(block.get("SEQUENTIAL_METHODS") or ()),
    )



# Fonction de construction de la configuration méthodologique de la cohérence
def coherence_config_from_params(
    params: Optional[Mapping[str, Any]],
) -> CoherenceConfig:
    """Build a ``CoherenceConfig`` from the YAML ``PARAMETERS`` section.

    Generic construction, twin of ``synthesis_config_from_params``: every key
    matching a ``CoherenceConfig`` field name overrides the frozen dataclass
    default; unknown keys are dropped with a warning. YAML lists are coerced to
    the tuple types the dataclass expects (``topk_depths``, ``lomo_methods``).

    Args:
        params: The ``COHERENCE.PARAMETERS`` mapping of
            the ``synthesis`` parameters (or ``None``, meaning the defaults of
            :class:`~macroforecast.trade.aggregation.CoherenceConfig`).

    Returns:
        A ``CoherenceConfig`` reflecting the configured overrides.

    Examples:
        >>> coherence_config_from_params(None) == CoherenceConfig()
        True
        >>> coherence_config_from_params({"topk_depths": [5, 25]}).topk_depths
        (5, 25)
    """
    # Aucune surcharge : configuration par défaut
    default = CoherenceConfig()
    if not params:
        return default

    # Surcharge générique champ à champ
    valid = {field.name for field in fields(CoherenceConfig)}
    overrides: Dict[str, Any] = {}
    for key, value in params.items():
        if key not in valid:
            # Logging
            logger.warning(f"Paramètre de cohérence inconnu ignoré : {key}")
            continue
        # Coercition listes → tuples pour les champs tuple du dataclass gelé
        current = getattr(default, key)
        if isinstance(current, tuple) and isinstance(value, (list, tuple)):
            value = tuple(value)
        overrides[key] = value

    return replace(default, **overrides)


# ──────────────────────────────────────────────────────────────────────
# Identité des tables DuckLake lues ou écrites par les étapes
# ──────────────────────────────────────────────────────────────────────

# Fonction de nom de schéma d'un dataflow (convention de statflows)
def schema_name(name: str) -> str:
    """Return the DuckLake schema of a dataflow or result name (``statflows`` rule).

    Args:
        name: Dataflow identifier or configured result schema.

    Returns:
        The schema name.

    Examples:
        >>> schema_name("DS-045409")
        'DS_045409'
    """
    from statflows.core.download import _schema_name

    return _schema_name(name)


# Fonction de localisation de la table téléchargée d'une source
def download_location(config: Mapping[str, Any], *, schema: Optional[str] = None) -> DuckLakeLocation:
    """Locate the fact table downloaded for a source (``eurostat`` or ``comtrade`` block).

    Args:
        config: Download parameter block (``DATAFLOW``, ``DOWNLOADS``).
        schema: Schema the connection is positioned on; the dataflow schema
            by default (BACI result schemas live in the same catalog).

    Returns:
        The location.
    """
    dataflow = config["DATAFLOW"]
    downloads = config["DOWNLOADS"]
    return DuckLakeLocation(
        dbname=downloads["DBNAME"],
        catalog_alias=downloads["CATALOG_ALIAS"],
        schema=schema or schema_name(dataflow),
        bucket=downloads[dataflow]["BUCKET"],
        data_path=downloads[dataflow]["PATHS"]["DATA_PATH"],
    )


# Fonction de localisation d'une table du catalogue des vulnérabilités
def vulnerabilities_location(
    vulnerability_config: Mapping[str, Any], *, schema: str, bucket: Optional[str], data_path: str
) -> DuckLakeLocation:
    """Locate a table of the shared ``vulnerabilities`` catalog.

    Args:
        vulnerability_config: The ``vulnerabilities`` parameter block (catalog
            identity: ``VULNERABILITIES.DBNAME`` / ``CATALOG_ALIAS``).
        schema: Schema of the table.
        bucket: S3 bucket holding its data, ``None`` for a local path.
        data_path: Data path of the table.

    Returns:
        The location.
    """
    catalog = vulnerability_config["VULNERABILITIES"]
    return DuckLakeLocation(
        dbname=catalog["DBNAME"],
        catalog_alias=catalog["CATALOG_ALIAS"],
        schema=schema,
        bucket=bucket,
        data_path=data_path,
    )


# Fonction des schémas BACI des millésimes configurés
def baci_target_schemas(baci_config: Mapping[str, Any]) -> Dict[str, str]:
    """Return the BACI result schema of every enabled target vintage.

    Args:
        baci_config: The ``baci`` parameter block (``CLASSIFICATIONS.TARGETS``;
            null entries, disabled vintages, are skipped).

    Returns:
        Mapping ``vintage -> schema``, in configuration order.

    Examples:
        >>> baci_target_schemas({"CLASSIFICATIONS": {"TARGETS": {
        ...     "HS2017": {"RESULT_SCHEMA": "baci_hs2017"}, "HS2022": None}}})
        {'HS2017': 'baci_hs2017'}
    """
    return {
        label: schema_name(target["RESULT_SCHEMA"])
        for label, target in active_targets(baci_config["CLASSIFICATIONS"]["TARGETS"]).items()
    }
