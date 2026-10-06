"""Script de calcul/mise à jour des indicateurs de vulnérabilité commerciale.

Recalcule les indicateurs (HHI, CDI2, CDI3 — cf. `macroforecast.trade.vulnerabilities`)
par unité de fraîcheur `classification x reporter x produit`, pour chaque sens de
flux de `FLOWS` (racine de `config/vulnerabilities.yaml` : `import`, `export`). La classification
est, pour toutes les unités, le millésime SH le plus récent de
`runtime.NOMENCLATURES.HS` (une paire couvre toutes les périodes). Une unité est
recalculée, avec toutes ses métriques, quand :

- elle n'a jamais été calculée ;
- elle entre dans le périmètre d'un forçage ponctuel (`runtime.FORCE_*` ou
  variables d'environnement `FORCE_*`). `FORCE_METRICS=HHI` suffit à recalculer
  HHI partout ;
- sa série a été retéléchargée depuis le dernier calcul (registre
  `LAST_DOWNLOAD_PATH` du téléchargement, lu en lecture seule) ;
- l'empreinte méthodologique d'une métrique a changé ou manque (métrique
  ajoutée, paramètre modifié, ou empreinte invalidée après la correction d'une
  formule : `scripts/invalidate_freshness.py --step partners --metrics HHI`).

Les empreintes sont tenues par métrique ET par sens (`HHI/import`,
`HHI/export`) : ajouter `export` à `FLOWS` ne recalcule que les lignes export,
les lignes import de la table résultat restant intactes (upsert par clé, le flux
faisant partie de la clé). Les métriques MLflow sont préfixées par sens
(`partners/import/...`, `partners/export/...`).

Le registre de fraîcheur est fragmenté (`STATE.PATH_TEMPLATE`, un fichier par
classification x reporter) et n'est écrit qu'après succès du calcul et de
l'écriture. L'ancien registre global (`PATHS.LAST_COMPUTATION_PATH`) n'est plus
qu'une source de migration (`STATE.ADOPT_LEGACY_FINGERPRINTS`).

Source et résultat sont adressés par deux connecteurs DuckLake distincts, dont
les connexions sont ouvertes ici et passées à `run_vulnerabilities` : le runner
ne suppose rien du backend de catalogue et ne gère pas le cycle de vie des
connexions.

Prévu comme étape Argo s'exécutant après le téléchargement (dépendance directe,
mais couplage faible : ce script ne lit que le registre JSON produit par le
téléchargement, il ne dépend d'aucun état en mémoire de ce dernier).

Le suivi d'exécution MLflow est piloté par le bloc `MLFLOW` de
`config/vulnerabilities.yaml` : sans `TRACKING_URI` (ou sans serveur joignable),
`get_tracker` retourne un objet nul et l'exécution est strictement inchangée. La
relecture du résultat précédent, qui alimente les diagnostics de dérive, est
faite ici — jamais par le module de calcul.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from collections import Counter
from dataclasses import dataclass, fields, replace
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Set, Tuple
import yaml

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    build_connector,
    compute_write_options,
    pg_credentials_from_env,
    s3_credentials_from_env,
)
# Vue du registre de téléchargement (lecture indépendante de son format physique)
from kedro_pipeline.io.registry_views import DownloadRegistryView
# Registres de fraîcheur v2 (fragments, empreintes, forçage)
from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    LegacySource,
    RegistryEntry,
    Unit,
    UnitPlan,
    adopt_legacy_flag,
    fingerprint,
    legacy_entry,
    plan_metrics,
    qualifiers_to_compute,
    units_to_compute,
    utc_now,
)
# Millésime SH en vigueur une année donnée
from kedro_pipeline.config import vintage_in_force
# Paramètres d'exécution partagés (même lecteur que le téléchargement)
from scripts.download_comtrade import load_runtime_config
# Module d'utilitaires de téléchargement
from statflows.core.download import _parse_iso, _schema_name

# Module de suivi d'exécution (MLflow optionnel, objet nul par défaut)
from macroforecast.tracking import NULL_TRACKER, CapturingTracker, get_tracker
from macroforecast.tracking.figures import (
    key_figures_partner_vulnerabilities,
    sections_partner_vulnerabilities,
)
from macroforecast.tracking.report import Units
from scripts._run_report import RunScope, flow_run_metrics, guarded_run, run_name
# Module de calcul des indicateurs
from macroforecast.trade.vulnerabilities import (
    DEFAULT_CONFIG,
    DEFAULT_METRIC_CLASSES,
    VulnerabilityConfig,
    VulnerabilityMetric,
    VulnerabilityReport,
    flow_code_map,
    parse_flows,
)
from macroforecast.trade.vulnerabilities.runner import (
    read_previous_result,
    run_vulnerabilities,
)

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé racine du registre JSON des dates de dernier calcul
_REGISTRY_ROOT = "VULNERABILITIES"
# Clé YAML portant le backend de calcul narwhals (hors VulnerabilityConfig)
_BACKEND_KEY = "BACKEND"
# Clé YAML des sens de flux calculés (racine du fichier)
_FLOWS_KEY = "FLOWS"
# Préfixe des métriques MLflow de l'étape (suivi du sens : partners/import/...)
_METRICS_FAMILY = "partners"


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

# Fonction de chargement de la configuration (fichier dédié à eurostat)
def load_eurostat_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load configuration from file.
    
    Args:
        config_path: Path to config file. If None, uses default location
                     or CONFIG_PATH environment variable.
    
    Returns:
        dict: Configuration dictionary
    """
    # Détermination du chemin de configuration
    if config_path is None:
        # Priorité 1 : variable d'environnement (pour flexibilité Kubernetes)
        config_path = os.environ.get('EUROSTAT_CONFIG_PATH', 'config/datasets/eurostat.yaml')
    
    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de chargement de la configuration (fichier dédié au calcul des vulnérabilités)
def load_vulnerability_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the vulnerability-computation configuration from file.

    Deliberately a separate config file from the download step's
    ``eurostat.yaml`` (own env var, own default path).

    Args:
        config_path: Path to config file. If None, uses default location
                     or VULNERABILITIES_CONFIG_PATH environment variable.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        config_path = os.environ.get(
            "VULNERABILITIES_CONFIG_PATH", "config/vulnerabilities.yaml"
        )
    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


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
        params: The ``PARAMETERS`` mapping of ``config/vulnerabilities.yaml``
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
            ``config/vulnerabilities.yaml``, or its ``NETWORK_VULNERABILITIES``
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


# ──────────────────────────────────────────────────────────────────────
# Registre de téléchargement (lecture seule) : dates par reporter x produit
# ──────────────────────────────────────────────────────────────────────

# Fonction d'indexation des dates de dernier téléchargement par couple reporter x produit
def load_last_download_dates(
    last_download_path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Dict[Tuple[str, str], datetime]:
    """Read the download registry and index last-download dates by (reporter, product).

    The registry is read through ``DownloadRegistryView`` (``statflows``
    ``iter_registry_entries``), which hides its physical layout (single file or
    fragments) and explodes multi-product queries into pairs.

    Args:
        last_download_path: Path to the ``LAST_DOWNLOAD_PATH`` registry
            (cf. ``eurostat.yaml`` / ``SDMXDownloader``).
        loader: ``Loader`` instance. Kept for compatibility: the view reads the
            registry with its own loader.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        Mapping ``(reporter, product) -> last_download`` (UTC-aware datetime).
    """
    return DownloadRegistryView(last_download_path, bucket).pairs_last_download()


# ──────────────────────────────────────────────────────────────────────
# Registre de calcul des vulnérabilités (lecture/écriture)
# ──────────────────────────────────────────────────────────────────────

# Fonction de lecture des dates de dernier calcul par couple reporter x produit
def load_last_computation_dates(
    last_computation_path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Dict[Tuple[str, str], datetime]:
    """Read the vulnerability-computation registry (empty if it does not exist yet).

    Args:
        last_computation_path: Path to the vulnerability-computation registry.
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        Mapping ``(reporter, product) -> last_computed`` (UTC-aware datetime).
    """
    # Importation des données
    data = loader.load(last_computation_path, bucket=bucket, missing_ok=True) or {}
    # Extraction du registre
    registry = data.get(_REGISTRY_ROOT, {})
    # Initialisation du dictionnaire associant au tuple reporter X produit la date du dernier calcul de vulnérabilité
    dates: Dict[Tuple[str, str], datetime] = {}
    # Parcours des entrées du registre
    for entry in registry.values():
        # Extraction du reporter et du produit
        reporter = entry.get("reporter")
        product = entry.get("product")
        # Extraction de la date de dernier téléchargement
        last_computed = _parse_iso(entry.get("last_computed"))
        # Association de la date de dernier téléchargement au couple
        if reporter and product and last_computed is not None:
            dates[(reporter, product)] = last_computed
    return dates


# Fonction de fusion et de sauvegarde des dates de calcul mises à jour
def save_last_computation_dates(
    last_computation_path: Path,
    dates: Dict[Tuple[str, str], datetime],
    loader: Loader,
    saver: Saver,
    bucket: Optional[str],
) -> None:
    """Merge new (reporter, product) computation dates into the registry and persist it.

    Args:
        last_computation_path: Path to the vulnerability-computation registry.
        dates: Pairs and their new ``last_computed`` instant. Merged into the
            existing registry; every other entry is preserved untouched.
        loader: ``Loader`` instance (to load the existing registry before merging).
        saver: ``Saver`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
    """
    # Fusion avec le registre existant : seules les entrées recalculées bougent
    data = loader.load(last_computation_path, bucket=bucket, missing_ok=True) or {}
    registry = data.get(_REGISTRY_ROOT, {})

    # Mise à jour des dates
    for (reporter, product), when in dates.items():
        registry[f"{reporter}|{product}"] = {
            "reporter": reporter,
            "product": product,
            "last_computed": when.isoformat(),
        }

    # Ecriture du fichier json mis à jour
    saver.save(
        last_computation_path,
        {_REGISTRY_ROOT: registry},
        bucket=bucket,
        indent=2,
        ensure_ascii=False,
    )
    # Logging
    logger.info(
        f"{len(dates)} date(s) de calcul mise(s) à jour dans '{last_computation_path}'"
    )


# ──────────────────────────────────────────────────────────────────────
# Détermination des couples à (re)calculer
# ──────────────────────────────────────────────────────────────────────

# Fonction de sélection des couples dont le score de vulnérabilité est périmé
def pairs_to_recompute(
    last_download: Dict[Tuple[str, str], datetime],
    last_computed: Dict[Tuple[str, str], datetime],
) -> Set[Tuple[str, str]]:
    """Select (reporter, product) pairs whose vulnerability score is stale.

    A pair is selected when it was never scored, or when it was last scored
    before its most recent download.

    Args:
        last_download: Last-download date per pair (source registry).
        last_computed: Last-computation date per pair (vulnerability registry).

    Returns:
        Set of pairs to (re)compute.
    """
    return {
        pair
        for pair, downloaded_at in last_download.items()
        if pair not in last_computed or last_computed[pair] < downloaded_at
    }


# ──────────────────────────────────────────────────────────────────────
# Registre de fraîcheur v2 : unités, empreintes, décision et écriture
# ──────────────────────────────────────────────────────────────────────

# Nom de l'étape (forçage FORCE_STEPS, champ « step » des fragments)
STEP = "partners"


# Fonction de détermination de la classification des unités partenaires
def partner_classification(nomenclatures: Mapping[str, int]) -> str:
    """Return the classification label of every partner unit.

    A (reporter, product) pair covers every period of the source, so no single
    period can give it a classification. Until the result table is keyed by
    classification and period, every unit is labelled with the **most recent
    HS vintage** of the configured nomenclatures, eight-digit Combined
    Nomenclature codes included: the label is stable from one year to the
    next, so it never triggers a recomputation by itself; it only changes when
    a new HS vintage is added to the configuration.

    Args:
        nomenclatures: Mapping HS vintage -> entry-into-force year
            (``runtime.NOMENCLATURES.HS``).

    Returns:
        The most recent vintage label.

    Raises:
        ValueError: If the mapping is empty.

    Examples:
        >>> partner_classification({"HS2017": 2017, "HS2022": 2022, "HS1992": 1988})
        'HS2022'
    """
    # Vérification des arguments
    if not nomenclatures:
        raise ValueError("runtime.NOMENCLATURES.HS is empty")
    return vintage_in_force(max(int(year) for year in nomenclatures.values()), nomenclatures)


# Fonction de construction des unités partenaires et de leur watermark amont
def partner_units(
    last_download: Mapping[Tuple[str, str], datetime],
    classification: str,
) -> Dict[Unit, datetime]:
    """Turn the last-download dates of the pairs into freshness units.

    Args:
        last_download: Last-download date per (reporter, product) pair, as read
            from the download registry.
        classification: Classification label of the units
            (:func:`partner_classification`).

    Returns:
        Mapping ``Unit(classification, reporter, product) -> last_download``.

    Examples:
        >>> from datetime import timezone
        >>> when = datetime(2026, 9, 16, tzinfo=timezone.utc)
        >>> units = partner_units({("FR", "280530"): when}, "HS2022")
        >>> [unit.key for unit in units]
        ['HS2022|FR|280530']
    """
    return {
        Unit.of(classification=classification, reporter=reporter, product=product): when
        for (reporter, product), when in last_download.items()
    }


# Fonction de calcul des empreintes demandées des métriques partenaires
def partner_requested(
    config: VulnerabilityConfig,
    flows: Sequence[str] = ("import",),
    metric_classes: Sequence[type] = DEFAULT_METRIC_CLASSES,
) -> Dict[str, str]:
    """Current methodological fingerprint of every partner metric and direction.

    Each fingerprint digests the metric name, the configuration fields that
    shape the written values (diagnostic-only fields excluded) and the flow
    direction. It is keyed by ``"<metric>/<flow>"``: two instances of a metric
    share a column name, and adding a direction must only make that direction
    stale. Adding a metric class or changing such a parameter makes every
    existing unit stale; a formula fix is signalled by invalidating the
    recorded fingerprints (``scripts/invalidate_freshness.py``).

    Args:
        config: Methodological configuration of the partner metrics.
        flows: Directions computed by the step.
        metric_classes: Metric classes computed by the step (each in the
            directions it supports).

    Returns:
        Mapping ``"<metric>/<flow>" -> fingerprint``.

    Examples:
        >>> sorted(partner_requested(VulnerabilityConfig()))
        ['CDI2/import', 'CDI3/import', 'HHI/import']
        >>> len(partner_requested(VulnerabilityConfig(), ("import", "export")))
        6
    """
    metrics = [
        cls(config, flow=flow)
        for flow in flows
        for cls in metric_classes
        if flow in cls.supported_flows
    ]
    return {
        metric.fingerprint_key: fingerprint(metric.name, metric.fingerprint_params())
        for metric in metrics
    }


# Fonction de lecture du registre v1 des partenaires en entrées héritées
def _legacy_partner_parser(classification: str):
    """Build the parser of the version-1 partners registry.

    Args:
        classification: Classification label given to the legacy units.

    Returns:
        Function turning ``{"VULNERABILITIES": {"FR|280530": {...}}}`` into
        legacy entries.
    """
    def parse(data: Mapping[str, Any]) -> Iterator[RegistryEntry]:
        for item in (data.get(_REGISTRY_ROOT) or {}).values():
            if not isinstance(item, Mapping) or not item.get("reporter") or not item.get("product"):
                continue
            unit = Unit.of(
                classification=classification,
                reporter=item["reporter"],
                product=item["product"],
            )
            yield legacy_entry(unit, item.get("last_computed"))

    return parse


# Fonction de construction du registre de fraîcheur des partenaires
def partner_registry(
    block: Mapping[str, Any],
    classification: str,
    *,
    loader: Optional[Loader] = None,
    saver: Optional[Saver] = None,
) -> FreshnessRegistry:
    """Build the partners freshness registry from its configuration block.

    One fragment per classification x reporter
    (``STATE.PATH_TEMPLATE``), with the version-1 registry
    (``PATHS.LAST_COMPUTATION_PATH``) read as a fallback for the migration.
    Also used read-only by the synthesis to follow its upstream.

    Args:
        block: Dataflow block of ``VULNERABILITIES`` in
            ``config/vulnerabilities.yaml`` (``BUCKET``, ``PATHS``, ``STATE``).
        classification: Classification label of the units.
        loader: JSON loader (a fresh one by default).
        saver: JSON saver (a fresh one by default).

    Returns:
        The registry.

    Raises:
        KeyError: If the block has no ``STATE.PATH_TEMPLATE``.
    """
    bucket = block.get("BUCKET")
    legacy_path = (block.get("PATHS") or {}).get("LAST_COMPUTATION_PATH")
    return FreshnessRegistry(
        block["STATE"]["PATH_TEMPLATE"],
        bucket,
        STEP,
        shard_of=lambda unit: f"{unit.get('classification')}/{unit.get('reporter')}",
        legacy=(
            LegacySource(legacy_path, bucket, _legacy_partner_parser(classification))
            if legacy_path
            else None
        ),
        loader=loader,
        saver=saver,
    )


# Fonction de décision des unités partenaires à (re)calculer
def plan_partner_units(
    registry: FreshnessRegistry,
    units: Mapping[Unit, datetime],
    requested: Mapping[str, str],
    force: ForceSpec,
    *,
    adopt_legacy_fingerprints: bool = False,
) -> Dict[Unit, UnitPlan]:
    """Decide which partner units to (re)compute.

    A unit is planned when it was never computed, when it is forced, when its
    series was downloaded again since, or when a metric fingerprint changed.
    Every metric of a planned direction is recomputed (the result table is wide
    and upserted by whole rows); the directions recomputed are those named by
    the plans (see :func:`kedro_pipeline.io.freshness.qualifiers_to_compute`).

    Args:
        registry: Partners freshness registry.
        units: Candidate units and their last-download date.
        requested: Current metric fingerprints (:func:`partner_requested`).
        force: One-off forcing.
        adopt_legacy_fingerprints: Deployment migration flag (see
            :func:`kedro_pipeline.io.freshness.units_to_compute`).

    Returns:
        Mapping ``unit -> plan`` for the stale units.
    """
    return units_to_compute(
        units, registry, units, requested, force,
        step=STEP, adopt_legacy_fingerprints=adopt_legacy_fingerprints,
    )


# Fonction d'inscription des unités calculées dans le registre
def record_computed_units(
    registry: FreshnessRegistry,
    plans: Mapping[Unit, UnitPlan],
    watermarks: Mapping[Unit, Optional[datetime]],
    requested: Mapping[str, str],
    computed_at: datetime,
) -> None:
    """Record the units just computed (in memory until ``registry.save()``).

    The recorded fingerprints are those of **every** current metric, since a
    planned unit is recomputed as a whole; the recorded reason is the plan's,
    which the downstream steps read to cascade a methodological or forced
    recomputation.

    Args:
        registry: Freshness registry.
        plans: Plans of the computed units.
        watermarks: Upstream instant of each unit, taken into account.
        requested: Current fingerprints.
        computed_at: Instant captured before the computation started.
    """
    for unit, plan in plans.items():
        registry.upsert(
            RegistryEntry(
                unit=unit,
                last_computed=computed_at,
                upstream_watermark=watermarks.get(unit),
                fingerprints=dict(requested),
                reason=plan.reason,
            )
        )


# Classe de résultat de l'étape partenaires
@dataclass
class PartnerStepResult:
    """Outcome of :func:`run_partner_step`.

    Args:
        plans: Units computed and their plan (empty when nothing was stale).
        report: Report of the computation, ``None`` when nothing was computed.
        written: Registry fragments written.
    """

    plans: Dict[Unit, UnitPlan]
    report: Optional[VulnerabilityReport]
    written: List[str]


# Fonction de calcul des unités planifiées et de mise à jour du registre
def compute_partner_units(
    source_conn: Any,
    result_conn: Any,
    *,
    plans: Mapping[Unit, UnitPlan],
    registry: FreshnessRegistry,
    units: Mapping[Unit, datetime],
    requested: Mapping[str, str],
    force: ForceSpec,
    source_catalog_alias: str,
    source_schema: str,
    result_catalog_alias: str,
    result_schema: str,
    config: VulnerabilityConfig,
    flows: Sequence[str] = ("import",),
    metrics: Optional[Sequence[VulnerabilityMetric]] = None,
    backend: str = "pandas",
    tracker: Any = NULL_TRACKER,
    log_artifacts: bool = True,
    measure_drift: bool = True,
    write_options: Optional[Mapping[str, Any]] = None,
    now: Optional[datetime] = None,
) -> PartnerStepResult:
    """Compute the planned partner units, write them, then record them.

    The registry is written only after the computation and the table write
    succeeded: a date is never moved forward wrongly. The computation instant
    is captured before reading the source, so an upstream update landing
    during the run is never missed.

    Only the directions named by the plans are computed, in a single pass over
    the union of the planned units: after ``export`` is added to ``flows``,
    nothing but the export rows is computed and written. When some units are
    stale for another reason (new data), the other units of the pass recompute
    that direction too, with identical values — the price of a single pass.

    Args:
        source_conn: Open connection on the source catalog (owned by the caller).
        result_conn: Open connection on the result catalog (owned by the caller).
        plans: Units to compute (:func:`plan_partner_units`).
        registry: Partners freshness registry.
        units: Candidate units and their last-download date.
        requested: Current metric fingerprints.
        force: One-off forcing (``forced`` tag of the run).
        source_catalog_alias: Alias of the source catalog.
        source_schema: Source schema (Comext fact table).
        result_catalog_alias: Alias of the result catalog.
        result_schema: Result schema.
        config: Methodological configuration.
        flows: Every configured direction (``FLOWS``), in output order.
        metrics: Metric instances (the default registry when ``None``).
        backend: Narwhals computation backend.
        tracker: Run tracker (metrics ``freshness/*`` and tag ``forced``).
        log_artifacts: Whether to log the business artifacts.
        measure_drift: Whether to re-read the previous result for the drift
            diagnostics.
        write_options: Options forwarded to the table write.
        now: Computation instant (current UTC instant by default).

    Returns:
        The step result.
    """
    computed_at = now or utc_now()
    pairs = {(unit.get("reporter"), unit.get("product")) for unit in plans}
    # Sens à recalculer : ceux que nomment les plans (empreintes qualifiées)
    computed_flows = qualifiers_to_compute(plans, flows)
    codes = flow_code_map(config)

    # Fraîcheur : métriques de décision et tag de forçage
    tracker.log_metrics(plan_metrics(plans, n_candidates=len(units)))
    if force.forces_step(STEP, requested):
        tracker.set_tags({"forced": force.describe()})

    # Résultat précédent du périmètre : lecture par le script, jamais par le runner
    df_previous = (
        read_previous_result(
            result_conn, result_catalog_alias, result_schema,
            reporters_products=sorted(pairs),
            flow_codes=[codes[flow] for flow in computed_flows],
            config=config,
        )
        if measure_drift
        else None
    )
    report = run_vulnerabilities(
        source_conn,
        source_catalog_alias=source_catalog_alias,
        source_schema=source_schema,
        result_schema=result_schema,
        result_conn=result_conn,
        result_catalog_alias=result_catalog_alias,
        reporters_products=pairs,
        metrics=metrics,
        config=config,
        flows=computed_flows,
        backend=backend,
        tracker=tracker,
        log_artifacts=log_artifacts,
        df_previous=df_previous,
        write_options=write_options,
    )

    # Inscription des unités calculées, puis écriture des seuls fragments modifiés
    record_computed_units(registry, plans, units, requested, computed_at)
    written = registry.save()
    return PartnerStepResult(dict(plans), report, written)


# Fonction d'exécution complète de l'étape partenaires sur des connexions ouvertes
def run_partner_step(
    source_conn: Any,
    result_conn: Any,
    *,
    registry: FreshnessRegistry,
    units: Mapping[Unit, datetime],
    requested: Mapping[str, str],
    force: ForceSpec,
    adopt_legacy_fingerprints: bool = False,
    **compute_kwargs: Any,
) -> PartnerStepResult:
    """Decide, compute and record the partner units on already-open connections.

    Isolates the core of :func:`main` from connection setup and configuration
    loading, so that it runs on any pair of connections (tests included).

    Args:
        source_conn: Open connection on the source catalog.
        result_conn: Open connection on the result catalog.
        registry: Partners freshness registry.
        units: Candidate units and their last-download date.
        requested: Current metric fingerprints.
        force: One-off forcing.
        adopt_legacy_fingerprints: Deployment migration flag.
        **compute_kwargs: Remaining arguments of :func:`compute_partner_units`
            (schemas, aliases, configuration, metrics, tracker…).

    Returns:
        The step result; ``plans`` is empty when nothing was stale (migrated
        legacy entries are still written).
    """
    plans = plan_partner_units(
        registry, units, requested, force, adopt_legacy_fingerprints=adopt_legacy_fingerprints
    )
    if not plans:
        # Rien à recalculer : seules d'éventuelles entrées v1 adoptées sont écrites
        return PartnerStepResult({}, None, registry.save())
    return compute_partner_units(
        source_conn, result_conn, plans=plans, registry=registry, units=units,
        requested=requested, force=force, **compute_kwargs,
    )


# ──────────────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────

# Nœud du rapport de run (clé de config/tracking.yaml)
NODE = "compute_partner_vulnerabilities"


# Fonction principale de calcul des vulnérabilités
def main() -> None:
    """CLI entry point for the incremental vulnerability computation script."""
    # Chargement de la configuration dédiée aux données eurostat qui servent de source au calcul des vulnérabilités
    eurostat_config = load_eurostat_config()
    # Chargement de la configuration dédiée au calcul des vulnérabilités
    vulnerability_config = load_vulnerability_config()
    # Paramètres d'exécution partagés : nomenclatures et forçage ponctuel
    runtime_config = load_runtime_config()

    # Construction des paramètres méthodologiques (seuils, conventions de colonnes)
    parameters = vulnerability_config.get("PARAMETERS") or {}
    vulnerability_parameters = vulnerability_config_from_params(parameters)
    # Sens de flux calculés (racine du fichier)
    flows = load_flows(vulnerability_config)
    # Backend de calcul narwhals, lu à part (pas un paramètre méthodologique)
    backend = parameters.get(_BACKEND_KEY, "pandas")

    # Options de suivi d'exécution (MLflow optionnel)
    mlflow_config = vulnerability_config.get("MLFLOW") or {}
    log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))
    measure_drift = bool(mlflow_config.get("DRIFT", True))

    # Initialisation du Dataflow sur lequel sont calculées les métriques de vulnérabilité
    DATAFLOW = eurostat_config["DATAFLOW"]
    block = vulnerability_config["VULNERABILITIES"][DATAFLOW]

    # Unités candidates : couples reporter x produit du registre de téléchargement
    # (lecture seule), rattachés au millésime SH le plus récent
    last_download = load_last_download_dates(
        last_download_path=Path(eurostat_config["DOWNLOADS"][DATAFLOW]["PATHS"]["LAST_DOWNLOAD_PATH"]),
        loader=Loader(),
        bucket=eurostat_config["DOWNLOADS"][DATAFLOW]["BUCKET"]
    )
    classification = partner_classification(runtime_config["NOMENCLATURES"]["HS"])
    units = partner_units(last_download, classification)

    # Registre de fraîcheur fragmenté, empreintes courantes et forçage ponctuel
    registry = partner_registry(block, classification)
    requested = partner_requested(vulnerability_parameters, flows)
    force = ForceSpec.from_runtime(runtime_config)
    plans = plan_partner_units(
        registry, units, requested, force,
        adopt_legacy_fingerprints=adopt_legacy_flag(block.get("STATE")),
    )

    # Logging
    reasons = Counter(plan.reason for plan in plans.values())
    logger.info(
        f"{len(plans)} unité(s) reporter x produit à recalculer sur {len(units)} "
        f"({dict(reasons)})"
    )

    # Sortie anticipée : rien à recalculer (entrées v1 adoptées écrites malgré tout)
    if not plans:
        registry.save()
        logger.info("Nothing to recompute, stop.")
        return

    # Construction du suivi d'exécution : sans URI (ou sans MLflow installé,
    # ou serveur injoignable), get_tracker retourne un tracker inerte et
    # l'exécution est strictement inchangée
    tracker = CapturingTracker(
        get_tracker(
            tracking_uri=mlflow_config.get("TRACKING_URI"),
            experiment=mlflow_config.get("EXPERIMENT", "trade-03-vulnerabilities"),
            run_name=run_name(f"vulnerabilities-{datetime.now():%Y%m%d-%H%M}", NODE),
        )
    )
    scope = RunScope(NODE)

    with tracker, guarded_run(scope, tracker):
        # Identifiants du catalogue et du stockage (lus une fois dans l'environnement)
        pg_credentials = pg_credentials_from_env()
        s3_credentials = s3_credentials_from_env()

        # Connecteur DuckLake aux données sources
        source_connector = build_connector(
            DuckLakeLocation(
                dbname=eurostat_config["DOWNLOADS"]["DBNAME"],
                catalog_alias=eurostat_config["DOWNLOADS"]["CATALOG_ALIAS"],
                schema=_schema_name(DATAFLOW),
                bucket=eurostat_config["DOWNLOADS"][DATAFLOW]["BUCKET"],
                data_path=eurostat_config["DOWNLOADS"][DATAFLOW]["PATHS"]["DATA_PATH"],
            ),
            pg=pg_credentials,
            s3=s3_credentials,
        )

        # Connecteur DuckLake résultat : catalogue Postgres positionné sur le schéma résultat des vulnérabilités.
        result_schema = _schema_name(block["RESULT_SCHEMA"])
        result_connector = build_connector(
            DuckLakeLocation(
                dbname=vulnerability_config["VULNERABILITIES"]["DBNAME"],
                catalog_alias=vulnerability_config["VULNERABILITIES"]["CATALOG_ALIAS"],
                schema=result_schema,
                bucket=block["BUCKET"],
                data_path=block["PATHS"]["DATA_PATH"],
            ),
            pg=pg_credentials,
            s3=s3_credentials,
        )

        # Ouverture des connexions : leur cycle de vie appartient au script, le
        # runner ne les ouvre ni ne les ferme (cf. `run_vulnerabilities`)
        source_conn = source_connector.connect()
        try:
            result_conn = result_connector.connect()
            try:
                # Calcul des unités planifiées, upsert, puis registre (après succès)
                result = compute_partner_units(
                    source_conn,
                    result_conn,
                    plans=plans,
                    registry=registry,
                    units=units,
                    requested=requested,
                    force=force,
                    source_catalog_alias=source_connector.catalog_alias,
                    source_schema=_schema_name(DATAFLOW),
                    result_catalog_alias=result_connector.catalog_alias,
                    result_schema=result_schema,
                    config=vulnerability_parameters,
                    flows=flows,
                    backend=backend,
                    tracker=tracker,
                    log_artifacts=log_artifacts,
                    measure_drift=measure_drift,
                    write_options=compute_write_options(
                        f"{NODE} {len(plans)} couples reporter x produit"
                    ),
                )
                report = result.report

                # Envoi des métriques, préfixées par sens (le rapport connaît sa
                # mise en forme, le préfixe appartient à l'appelant). Les
                # paramètres sont journalisés par le runner lui-même ; seuls
                # les tags propres au script restent ici.
                tracker.log_metrics(flow_run_metrics(report, _METRICS_FAMILY))
                tracker.set_tags(
                    {
                        "dataflow": DATAFLOW,
                        "result_schema": result_schema,
                        "created": str(report.created),
                        "n_pairs": str(len(plans)),
                        "flows": ",".join(report.flows),
                    }
                )

                # Rapport de run : contrôles, chiffres clés, sections, publiés avant toute
                # sortie en erreur
                scope.step = "rapport de run"
                run_report = scope.build(
                    metrics=tracker.metrics,
                    units=Units(
                        planned=len(plans),
                        succeeded=len(plans),
                        planned_label=f"{len(plans)} couples reporter × produit",
                    ),
                    key_figures=key_figures_partner_vulnerabilities,
                    sections=lambda m: sections_partner_vulnerabilities(m, tracker.tables),
                )
                scope.publish(tracker, run_report)
            finally:
                result_conn.close()
        finally:
            source_conn.close()

    # Logging
    logger.info(
        f"Vulnerability computation complete : {report} ; "
        f"{len(result.written)} fragment(s) de registre écrit(s)"
    )


# Exécution du script principal
if __name__ == "__main__":
    main()
