"""Partner-metrics step: concentration of the trading partners of each reporter.

Computes the partner vulnerability metrics (HHI, CDI2, CDI3 — see
``macroforecast.trade.vulnerabilities``) for every flow direction of ``FLOWS``
(root of the ``vulnerabilities`` parameters: ``import``, ``export``) and writes
them into a single table whose key carries the nomenclature
(``classification``). Two families of rows live in it:

- the rows **in force** (``in_force = true``): every code as declared in Comext
  (HS2, HS4, HS6, CN8), ``classification`` being the HS vintage in force in the
  year of the period (``CN<year>`` for an eight-digit code);
- the **historical** rows (``in_force = false``): for every older HS vintage
  requested (``VINTAGES``), the HS6 flows of the later years converted into that
  vintage by the UNSD correspondence tables shared with BACI (exact sum of the
  merged codes, a recent code being assigned whole to the older code its table
  designates when it overlaps several), then the same metrics. A product thus
  reads over a long series in a fixed nomenclature.

Both families go through the same function (:func:`prepare_vintage_flows`): the
rows in force are the identity conversion. Every row also carries ``hs_vintage``
(HS vintage the synthesis joins the BACI network of) and ``is_provisional``
(profile flag: restricted product scope).

Freshness (fragmented registry ``STATE.PATH_TEMPLATE``, one file per
classification x reporter, written after the computation and the write
succeeded):

- unit in force: (most recent HS vintage of the referential, reporter, product),
  one unit covering every period of the downloaded pair; its upstream is the
  last download of the pair;
- historical unit: (vintage, reporter, HS6 target code); its sources are the
  recent codes converting into it (preimage of the correspondence tables), its
  upstream the most recent of their downloads. It waits while a source was
  never downloaded (``freshness/units_waiting_sources``), a partial sum being
  wrong. Its fingerprint adds the checksum of the correspondence tables: a
  corrected table recomputes the rows.

A unit is recomputed, with all its metrics, when it was never computed, when a
forcing covers it, when its upstream was downloaded again since, or when a
fingerprint changed or is missing (metric added, parameter changed, fingerprint
invalidated). Fingerprints are kept per metric AND per direction
(``HHI/import``): adding ``export`` only recomputes the export rows.

:func:`run_partner_vulnerabilities` runs one pass for the rows in force, then
one per historical vintage, each in its own tracked run (tags
``classification`` and ``in_force``, metrics prefixed by direction
``partners/import/...``). The failure of a pass does not stop the next ones; the
step fails at the end if one failed. A result table created before the
``classification`` key is refused: it must be recreated first.

No environment variable and no YAML path are read here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime
from functools import partial
import logging
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, Iterator, List, Mapping, Optional, Sequence, Set, Tuple

# Modules de manipulation de données
import narwhals as nw
import pandas as pd

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Poignée d'écriture des tables DuckLake
from kedro_pipeline.io.ducklake import DuckLakeTable, attached_catalog_alias
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
# Référentiel des millésimes SH (fonctions pures et macros SQL équivalentes)
from kedro_pipeline.config import (
    classification_of,
    first_historical_year,
    nomenclature_macros_sql,
    product_code,
    requested_vintages,
    vintage_in_force,
)
# Fabriques de configuration (ré-exportées pour les étapes aval)
from kedro_pipeline.steps._config import (  # noqa: F401
    _BACKEND_KEY,
    _FLOWS_KEY,
    load_flows,
    schema_name,
    vulnerability_config_from_params,
)
# Cache des tables de passage UNSD, partagé avec BACI
from kedro_pipeline.steps.baci import (
    concordances_checksum,
    downward_pairs,
    prepare_concordances,
)
# Résultat d'étape et runs d'unités
from kedro_pipeline.steps.result import (
    StepResult,
    UnitRuns,
    capturing,
    failure_message,
    shared_runs,
)
# Module d'utilitaires de téléchargement
from statflows.core.download import _parse_iso

# Module de suivi d'exécution (objet nul par défaut)
from kedro_pipeline.io.tracking import flow_run_metrics
from macroforecast.tracking import NULL_TRACKER
# Module de calcul des indicateurs
from macroforecast.trade.vulnerabilities import (
    DEFAULT_METRIC_CLASSES,
    VulnerabilityConfig,
    VulnerabilityMetric,
    VulnerabilityReport,
    flow_code_map,
)
from macroforecast.trade.vulnerabilities.runner import (
    read_previous_result,
    read_source_flows,
    run_vulnerabilities_on_frame,
)
# Conversion des flux vers un millésime antérieur (mêmes règles que BACI)
from macroforecast.trade.processing import (
    build_conversion_map,
    conversion_preimage,
    harmonize_partner_flows,
)
# Existence et nom de la table de faits DuckLake
from statflows.storage.ducklake.tables import FACT_TABLE, fact_table_exists

# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé racine du registre JSON des dates de dernier calcul
_REGISTRY_ROOT = "VULNERABILITIES"

# Préfixe des métriques MLflow de l'étape (suivi du sens : partners/import/...)
_METRICS_FAMILY = "partners"

# Clés YAML des millésimes historiques (racine du fichier) et du drapeau de
# périmètre provisoire (bloc du dataflow)
_VINTAGES_KEY = "VINTAGES"

_ON_UNMAPPED_KEY = "VINTAGES_ON_UNMAPPED"

_PROVISIONAL_KEY = "IS_PROVISIONAL"

# Colonnes de nomenclature de la table résultat (faits de schéma, lus par la
# synthèse et la couche de service)
CLASSIFICATION_COL = "classification"

HS_VINTAGE_COL = "hs_vintage"

IN_FORCE_COL = "in_force"

PROVISIONAL_COL = "is_provisional"

# Longueur des codes SH6, seuls codes que les tables de passage convertissent
_HS6_LENGTH = 6

# Nom de l'empreinte des tables de passage d'une unité historique
CONCORDANCE_FINGERPRINT = "concordance"



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
            (cf. the ``eurostat`` parameters / ``SDMXDownloader``).
        loader: ``Loader`` instance. Kept for compatibility: the view reads the
            registry with its own loader.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        Mapping ``(reporter, product) -> last_download`` (UTC-aware datetime).
    """
    return DownloadRegistryView(last_download_path, bucket).pairs_last_download()

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

# Nom de l'étape (forçage FORCE_STEPS, champ « step » des fragments)
STEP = "partners"

# Fonction de détermination de la classification des unités partenaires
def partner_classification(nomenclatures: Mapping[str, int]) -> str:
    """Return the classification label of every unit of rows in force.

    A unit in force is a downloaded (reporter, product) pair and covers every
    period of the source, whose rows carry several classifications (the
    vintage in force each year, ``CN<year>`` for eight-digit codes): no single
    one can name it. It is labelled with the **most recent HS vintage** of the
    referential: stable from one year to the next (a new year never triggers a
    recomputation by itself), and never equal to the label of a historical
    unit, which is always an older vintage. It only changes when a new HS
    vintage is added to the referential, which recomputes every row in force.

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
            the ``vulnerabilities`` parameters (``BUCKET``, ``PATHS``, ``STATE``).
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

# Fonction d'extension de la clé des cellules par la classification
def nomenclature_config(config: VulnerabilityConfig) -> VulnerabilityConfig:
    """Return the configuration whose cell key starts with the classification.

    The same product code designates different goods in two vintages: the
    classification is part of the cell, of the grid the metrics group by and
    of the primary key of the result table. The methodological fingerprints
    are computed on the configuration *before* this extension, so adding the
    column never makes a unit stale by itself.

    Args:
        config: Methodological configuration of the partner metrics.

    Returns:
        The configuration with ``classification`` prepended to
        ``key_columns`` (unchanged if already present).

    Examples:
        >>> nomenclature_config(VulnerabilityConfig()).key_columns[:2]
        ('classification', 'freq')
    """
    if CLASSIFICATION_COL in config.key_columns:
        return config
    return replace(config, key_columns=(CLASSIFICATION_COL, *config.key_columns))

# Fonction d'extraction de l'année d'une colonne de périodes
def _period_years(periods: pd.Series) -> pd.Series:
    """Return the year of every period (``"2019"``, ``"2019-03"``…) as integers."""
    return periods.astype(str).str[:4].astype(int)

# Fonction de préparation des flux d'une passe (en vigueur ou historique)
def prepare_vintage_flows(
    df_flows: pd.DataFrame,
    *,
    target_vintage: Optional[str],
    nomenclatures: Mapping[str, int],
    concordances: Mapping[Tuple[str, str], pd.DataFrame],
    config: VulnerabilityConfig,
    on_unmapped: str = "drop",
) -> pd.DataFrame:
    """Convert partner flows into one vintage and stamp their classification.

    The single path of both kinds of rows. Each row is declared in the HS
    vintage in force at its period; the rows are grouped by that source
    vintage and converted into ``target_vintage``
    (:func:`~macroforecast.trade.processing.harmonize_partner_flows`):

    - rows in force (``target_vintage=None``): each group is converted into
      its own vintage, i.e. left untouched, and the classification is the one
      in force (``CN<year>`` for an eight-digit code);
    - historical rows (``target_vintage="HS2017"``…): six-digit flows of later
      years, converted into the older vintage, which becomes their
      classification.

    Args:
        df_flows: Partner flows read from the source (grid keys, partner and
            value columns).
        target_vintage: Vintage to convert into, ``None`` for the rows in
            force.
        nomenclatures: Mapping vintage label -> entry-into-force year.
        concordances: Correspondence tables ``(source, target) -> table``;
            unused for the rows in force.
        config: Column conventions (base configuration, without the
            classification key).
        on_unmapped: Policy for a code absent from a correspondence table
            (``"raise"``, ``"drop"``, ``"keep"``).

    Returns:
        The flows, converted, with a ``classification`` column.

    Raises:
        ValueError: If a period precedes the first vintage, or a conversion
            fails under ``on_unmapped="raise"``.

    Examples:
        >>> hs = {"HS2017": 2017, "HS2022": 2022}
        >>> flows = pd.DataFrame({"freq": "A", "reporter": "FR", "product": ["854110", "85411000"],
        ...                       "flow": 1, "indicators": "VALUE_IN_EUROS", "TIME_PERIOD": "2019",
        ...                       "partner": "CN", "OBS_VALUE": 1.0})
        >>> prepare_vintage_flows(flows, target_vintage=None, nomenclatures=hs, concordances={},
        ...                       config=VulnerabilityConfig())["classification"].tolist()
        ['HS2017', 'CN2019']
    """
    # Colonnes d'identification hors produit et hors classification
    key_columns = [
        column
        for column in [*config.key_columns, config.partner_col]
        if column not in (CLASSIFICATION_COL, config.product_col)
    ]
    columns = [*key_columns, config.product_col, config.value_col]
    if df_flows.empty:
        return df_flows.loc[:, columns].assign(**{CLASSIFICATION_COL: pd.Series(dtype="object")})

    # Millésime source de chaque ligne : celui en vigueur l'année de sa période
    years = _period_years(df_flows[config.period_col])
    sources = years.map({year: vintage_in_force(year, nomenclatures) for year in years.unique()})

    parts: List[pd.DataFrame] = []
    for source, df_part in df_flows.groupby(sources, sort=True):
        converted = harmonize_partner_flows(
            df_part,
            source_vintage=source,
            target_vintage=target_vintage or source,
            concordances=concordances,
            key_columns=key_columns,
            measure_columns=[config.value_col],
            product_col=config.product_col,
            period_col=config.period_col,
            on_unmapped=on_unmapped,
        )
        if target_vintage is None:
            # Classification en vigueur, évaluée une fois par couple distinct
            pairs = pd.MultiIndex.from_arrays(
                [converted[config.product_col], converted[config.period_col]]
            )
            labels = {
                pair: classification_of(pair[0], int(str(pair[1])[:4]), nomenclatures)
                for pair in pairs.unique()
            }
            converted[CLASSIFICATION_COL] = pairs.map(labels).to_numpy()
        else:
            converted[CLASSIFICATION_COL] = target_vintage
        parts.append(converted)
    return pd.concat(parts, ignore_index=True)

# Fonction d'ajout des colonnes descriptives de nomenclature
def annotate_nomenclature(
    result: nw.DataFrame,
    *,
    target_vintage: Optional[str],
    nomenclatures: Mapping[str, int],
    is_provisional: bool,
    period_col: str = "TIME_PERIOD",
) -> nw.DataFrame:
    """Add ``hs_vintage``, ``in_force`` and ``is_provisional`` to the scores.

    ``hs_vintage`` is the HS vintage a row is attached to — the one the
    synthesis joins the network metrics of: the vintage in force at the
    period for the rows in force (eight-digit codes included, whose first six
    digits are HS codes of that vintage), the target vintage for the
    historical rows.

    Args:
        result: Scores of one pass.
        target_vintage: Target vintage of the pass, ``None`` for the rows in
            force.
        nomenclatures: Mapping vintage label -> entry-into-force year.
        is_provisional: Whether the profile computes a restricted product
            perimeter.
        period_col: Period column.

    Returns:
        The scores with the three descriptive columns.

    Examples:
        >>> frame = nw.from_native(pd.DataFrame({"TIME_PERIOD": ["2019", "2023"]}), eager_only=True)
        >>> out = annotate_nomenclature(frame, target_vintage=None,
        ...                             nomenclatures={"HS2017": 2017, "HS2022": 2022},
        ...                             is_provisional=False)
        >>> out.to_native()[["hs_vintage", "in_force"]].values.tolist()
        [['HS2017', True], ['HS2022', True]]
    """
    if target_vintage is not None:
        hs_vintage: Any = nw.lit(target_vintage)
    else:
        years = result.get_column(period_col).cast(nw.String).str.slice(0, 4)
        mapping = {year: vintage_in_force(int(year), nomenclatures) for year in years.unique().to_list()}
        hs_vintage = years.replace_strict(mapping, return_dtype=nw.String)
    return result.with_columns(
        hs_vintage.alias(HS_VINTAGE_COL),
        nw.lit(target_vintage is None).alias(IN_FORCE_COL),
        nw.lit(bool(is_provisional)).alias(PROVISIONAL_COL),
    )

# Fonction de construction des dictionnaires de conversion vers chaque millésime
def historical_conversions(
    concordances: Mapping[Tuple[str, str], pd.DataFrame],
    vintages: Sequence[str],
    nomenclatures: Mapping[str, int],
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Build, per historical vintage, the conversion map of every later vintage.

    Args:
        concordances: Correspondence tables ``(source, target) -> table``.
        vintages: Historical vintages requested.
        nomenclatures: Mapping vintage label -> entry-into-force year.

    Returns:
        Mapping ``target vintage -> {source vintage -> {source code -> target
        code}}``.

    Raises:
        ValueError: If a pair is missing and no chain of tables connects it.
    """
    return {
        target: {
            source: build_conversion_map(concordances, source, target)
            for source, pair_target in downward_pairs([target], nomenclatures)
            if pair_target == target
        }
        for target in vintages
    }

# Classe des unités historiques d'un millésime
@dataclass(frozen=True)
class HistoricalUnits:
    """Freshness units of the historical rows of one vintage.

    Attributes:
        vintage: Target vintage of the units.
        watermarks: Units whose sources were all downloaded, and the most
            recent download among their sources.
        sources: Source codes of every unit of ``watermarks`` (the recent
            codes converted into its target code).
        waiting: Units with at least one source never downloaded, and the
            missing sources: a partial sum would be wrong, so they wait.
    """

    vintage: str
    watermarks: Dict[Unit, datetime] = field(default_factory=dict)
    sources: Dict[Unit, FrozenSet[str]] = field(default_factory=dict)
    waiting: Dict[Unit, FrozenSet[str]] = field(default_factory=dict)

# Fonction de construction des unités historiques d'un millésime
def historical_units(
    last_download: Mapping[Tuple[str, str], datetime],
    conversions: Mapping[str, Mapping[str, str]],
    vintage: str,
) -> HistoricalUnits:
    """Turn the downloaded pairs into the historical units of one vintage.

    A historical unit is a reporter and a six-digit code of ``vintage``. Its
    sources are the codes of every later vintage that convert into it (the
    preimage of the correspondence tables); its upstream instant is the most
    recent download among them. A unit exists as soon as one of its sources
    was downloaded for the reporter, and waits until all of them were.

    Args:
        last_download: Last-download date per (reporter, product) pair.
        conversions: Conversion maps into ``vintage``, keyed by source vintage
            (:func:`historical_conversions`).
        vintage: Target vintage.

    Returns:
        The units, their sources, and the units waiting for a source.

    Examples:
        >>> from datetime import timezone
        >>> t1, t2 = datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc)
        >>> units = historical_units({("FR", "010121"): t1, ("FR", "010129"): t2},
        ...                          {"HS2022": {"010121": "010121", "010129": "010121"}}, "HS2017")
        >>> [(unit.key, when == t2) for unit, when in units.watermarks.items()]
        [('HS2017|FR|010121', True)]
    """
    # Préimage de chaque code cible, tous millésimes sources confondus
    preimage: Dict[str, Set[str]] = {}
    for conversion_map in conversions.values():
        for target, sources in conversion_preimage(conversion_map).items():
            preimage.setdefault(target, set()).update(sources)
    targets_of: Dict[str, Set[str]] = {}
    for target, sources in preimage.items():
        for source in sources:
            targets_of.setdefault(source, set()).add(target)

    # Codes SH6 téléchargés par reporter (zéro initial restitué)
    downloaded: Dict[str, Dict[str, datetime]] = {}
    for (reporter, product), when in last_download.items():
        code = product_code(product)
        if len(code) != _HS6_LENGTH:
            continue
        codes = downloaded.setdefault(reporter, {})
        if code not in codes or when > codes[code]:
            codes[code] = when

    units = HistoricalUnits(vintage)
    for reporter, codes in sorted(downloaded.items()):
        targets = {target for code in codes for target in targets_of.get(code, ())}
        for target in sorted(targets):
            unit = Unit.of(classification=vintage, reporter=reporter, product=target)
            sources = frozenset(preimage[target])
            missing = sources - codes.keys()
            if missing:
                units.waiting[unit] = frozenset(missing)
                continue
            units.watermarks[unit] = max(codes[code] for code in sources)
            units.sources[unit] = sources
    return units

# Fonction de calcul des empreintes d'une passe historique
def historical_requested(
    requested: Mapping[str, str],
    concordances: Mapping[Tuple[str, str], pd.DataFrame],
    vintage: str,
    on_unmapped: str,
) -> Dict[str, str]:
    """Fingerprints of the historical rows of one vintage.

    Those of the metrics, plus one digest of the correspondence tables into
    ``vintage`` and of the policy applied to unmapped codes: a table corrected
    by UNSD, or a policy change, makes every historical unit of the vintage
    stale.

    Args:
        requested: Fingerprints of the metrics (:func:`partner_requested`).
        concordances: Correspondence tables ``(source, target) -> table``.
        vintage: Target vintage.
        on_unmapped: Policy for codes absent from a table.

    Returns:
        ``requested`` plus the ``concordance`` fingerprint.

    Examples:
        >>> sorted(historical_requested({"HHI/import": "x"}, {}, "HS2017", "drop"))
        ['HHI/import', 'concordance']
    """
    tables = {pair: table for pair, table in concordances.items() if pair[1] == vintage}
    return {
        **dict(requested),
        CONCORDANCE_FINGERPRINT: fingerprint(
            CONCORDANCE_FINGERPRINT,
            {"checksum": concordances_checksum(tables), "on_unmapped": on_unmapped},
        ),
    }

# Fonction de construction du prédicat de lecture des flux SH6 historiques
def historical_source_predicate(config: VulnerabilityConfig, first_year: int) -> str:
    """SQL predicate selecting the six-digit flows converted into a vintage.

    Relies on the ``product_code`` session macro (leading zero of the codes
    stored as integers restored before counting the digits).

    Args:
        config: Column conventions.
        first_year: First year converted (entry of the next vintage).

    Returns:
        The SQL predicate.

    Examples:
        >>> print(historical_source_predicate(VulnerabilityConfig(), 2022))
        length(product_code("product")) = 6 AND CAST(substr(CAST("TIME_PERIOD" AS VARCHAR), 1, 4) AS INTEGER) >= 2022
    """
    return (
        f'length(product_code("{config.product_col}")) = {_HS6_LENGTH} AND '
        f'CAST(substr(CAST("{config.period_col}" AS VARCHAR), 1, 4) AS INTEGER) >= {int(first_year)}'
    )

# Fonction de vérification de la clé de la table résultat
def ensure_nomenclature_key(conn: Any, catalog_alias: str, schema: str) -> None:
    """Refuse to write into a result table created before the classification key.

    The upsert matches rows on the primary key of the existing table: on a
    table keyed without ``classification``, a historical row would overwrite
    the row in force of the same cell. Such a table must first be recreated
    with the new key.

    Args:
        conn: Open connection on the result catalog.
        catalog_alias: Alias of the result catalog.
        schema: Result schema.

    Raises:
        RuntimeError: If the table exists without a ``classification`` column.
    """
    if not fact_table_exists(conn, catalog_alias, schema):
        return
    columns = {
        row[0]
        for row in conn.execute(
            f'DESCRIBE "{catalog_alias}"."{schema}"."{FACT_TABLE}"'
        ).fetchall()
    }
    if CLASSIFICATION_COL not in columns:
        raise RuntimeError(
            f"Table '{schema}' has no '{CLASSIFICATION_COL}' column: it was created "
            "before the nomenclature key. Recreate it with "
            "tools/migrate_indicators_key.py before running this step."
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
    nomenclatures: Mapping[str, int],
    flows: Sequence[str] = ("import",),
    target_vintage: Optional[str] = None,
    sources: Optional[Mapping[Unit, FrozenSet[str]]] = None,
    concordances: Optional[Mapping[Tuple[str, str], pd.DataFrame]] = None,
    on_unmapped: str = "drop",
    is_provisional: bool = False,
    n_waiting: Optional[int] = None,
    metrics: Optional[Sequence[VulnerabilityMetric]] = None,
    backend: str = "pandas",
    tracker: Any = NULL_TRACKER,
    log_artifacts: bool = True,
    measure_drift: bool = True,
    run_id: Optional[str] = None,
    commit_message: Optional[str] = None,
    now: Optional[datetime] = None,
) -> PartnerStepResult:
    """Compute the planned units of one pass, write them, then record them.

    A pass is either the rows in force (``target_vintage=None``: the planned
    pairs are read as declared, at every level) or the historical rows of one
    vintage (the six-digit sources of the planned target codes are read for
    the years following the vintage, then converted). Both go through
    :func:`prepare_vintage_flows`, then the same computation and write.

    The registry is written only after the computation and the table write
    succeeded: a date is never moved forward wrongly. The computation instant
    is captured before reading the source, so an upstream update landing
    during the run is never missed. Only the directions named by the plans
    are computed, in a single pass over the union of the planned units.

    Args:
        source_conn: Open connection on the source catalog (owned by the caller).
        result_conn: Open connection on the result catalog (owned by the caller).
        plans: Units to compute (:func:`plan_partner_units`).
        registry: Partners freshness registry.
        units: Candidate units of the pass and their upstream instant.
        requested: Current fingerprints of the pass.
        force: One-off forcing (``forced`` tag of the run).
        source_catalog_alias: Alias of the source catalog.
        source_schema: Source schema (Comext fact table).
        result_catalog_alias: Alias of the result catalog.
        result_schema: Result schema.
        config: Methodological configuration (without the classification key).
        nomenclatures: Mapping vintage label -> entry-into-force year.
        flows: Every configured direction (``FLOWS``), in output order.
        target_vintage: Historical vintage of the pass, ``None`` for the rows
            in force.
        sources: Source codes of every historical unit
            (:attr:`HistoricalUnits.sources`); required with
            ``target_vintage``.
        concordances: Correspondence tables; required with ``target_vintage``.
        on_unmapped: Policy for codes absent from a correspondence table.
        is_provisional: Value of the ``is_provisional`` column.
        n_waiting: Units waiting for a source, logged as
            ``freshness/units_waiting_sources`` when given.
        metrics: Metric instances, built on the configuration extended by
            :func:`nomenclature_config` (the default registry when ``None``).
        backend: Narwhals computation backend.
        tracker: Run tracker (metrics ``freshness/*`` and tag ``forced``).
        log_artifacts: Whether to log the business artifacts.
        measure_drift: Whether to re-read the previous result of the pass for
            the drift diagnostics.
        run_id: Run identifier recorded on the DuckLake snapshot of the write
            (Argo workflow id), ``None`` outside Argo.
        commit_message: Commit message recorded on the snapshot. The write
            goes through :class:`kedro_pipeline.io.ducklake.DuckLakeTable`:
            the columns of a new metric are added on the fly, the compaction
            is left to the maintenance pass and the table is partitioned by
            classification at its creation.
        now: Computation instant (current UTC instant by default).

    Returns:
        The step result.

    Raises:
        ValueError: If a historical pass lacks its sources or tables.
        RuntimeError: If the result table predates the classification key.
    """
    computed_at = now or utc_now()
    pairs = sorted({(unit.get("reporter"), unit.get("product")) for unit in plans})
    # Sens à recalculer : ceux que nomment les plans (empreintes qualifiées)
    computed_flows = qualifiers_to_compute(plans, flows)
    codes = flow_code_map(config)
    keyed = nomenclature_config(config)

    # Fraîcheur : métriques de décision et tag de forçage
    freshness = plan_metrics(plans, n_candidates=len(units))
    if n_waiting is not None:
        freshness["freshness/units_waiting_sources"] = float(n_waiting)
    tracker.log_metrics(freshness)
    if force.forces_step(STEP, requested):
        tracker.set_tags({"forced": force.describe()})

    # Garde : jamais d'upsert sur une table indexée sans classification
    ensure_nomenclature_key(result_conn, result_catalog_alias, result_schema)

    # Lecture des flux de la passe ; les macros de session restituent le zéro
    # initial des codes stockés en entiers
    for statement in nomenclature_macros_sql(nomenclatures):
        source_conn.execute(statement)
    if target_vintage is None:
        df_flows = read_source_flows(
            source_conn, source_catalog_alias, source_schema,
            config=config, reporters_products=pairs,
        )
        pass_predicate = f'"{IN_FORCE_COL}"'
    else:
        if sources is None or concordances is None:
            raise ValueError("A historical pass requires its sources and correspondence tables")
        source_pairs = sorted(
            {(unit.get("reporter"), code) for unit in plans for code in sources[unit]}
        )
        df_flows = read_source_flows(
            source_conn, source_catalog_alias, source_schema,
            config=config, reporters_products=source_pairs,
            where=historical_source_predicate(
                config, first_historical_year(target_vintage, nomenclatures)
            ),
        )
        pass_predicate = (
            f"\"{CLASSIFICATION_COL}\" = '{target_vintage}' AND NOT \"{IN_FORCE_COL}\""
        )

    # Conversion (identité pour les lignes en vigueur) et classification
    df_flows = prepare_vintage_flows(
        df_flows,
        target_vintage=target_vintage,
        nomenclatures=nomenclatures,
        concordances=concordances or {},
        config=config,
        on_unmapped=on_unmapped,
    )
    if target_vintage is not None:
        # Codes cibles non planifiés : une source lue pour un code planifié peut,
        # une autre année, se convertir vers un autre code dont les autres
        # sources n'ont pas été lues ; sa somme serait partielle
        planned = set(pairs)
        cells = zip(df_flows[config.reporter_col], df_flows[config.product_col].map(product_code))
        df_flows = df_flows[[cell in planned for cell in cells]].reset_index(drop=True)

    # Résultat précédent de la passe : lecture par l'étape, jamais par le runner
    df_previous = (
        read_previous_result(
            result_conn, result_catalog_alias, result_schema,
            reporters_products=pairs,
            flow_codes=[codes[flow] for flow in computed_flows],
            config=config,
            where=pass_predicate,
        )
        if measure_drift
        else None
    )
    # Écrivain de la table résultat : évolution de schéma native, partition par
    # classification posée à la création, traçabilité du snapshot
    writer = DuckLakeTable(result_conn, result_catalog_alias, result_schema).writer(
        build_options={"partition_by": [CLASSIFICATION_COL]},
        run_id=run_id,
        commit_message=commit_message,
    )
    report = run_vulnerabilities_on_frame(
        df_flows,
        result_conn=result_conn,
        result_catalog_alias=result_catalog_alias,
        result_schema=result_schema,
        metrics=metrics,
        config=keyed,
        flows=computed_flows,
        flow_codes=codes,
        backend=backend,
        tracker=tracker,
        log_artifacts=log_artifacts,
        df_previous=df_previous,
        writer=writer,
        annotate=partial(
            annotate_nomenclature,
            target_vintage=target_vintage,
            nomenclatures=nomenclatures,
            is_provisional=is_provisional,
            period_col=config.period_col,
        ),
        params={
            "source_schema": source_schema,
            "classification": target_vintage or "in_force",
            "n_reporter_product_pairs": len(pairs),
        },
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

# Classe décrivant une passe planifiée (lignes en vigueur ou d'un millésime)
@dataclass
class PartnerPass:
    """One pass of the partner step: the rows in force, or one historical vintage.

    Args:
        label: Classification label of the pass (tag of its run).
        target_vintage: Historical vintage, ``None`` for the rows in force.
        units: Candidate units and their upstream instant.
        requested: Current fingerprints of the pass.
        plans: Units to compute.
        sources: Source codes of the historical units.
        n_waiting: Historical units waiting for a source.
    """

    label: str
    target_vintage: Optional[str]
    units: Dict[Unit, datetime]
    requested: Dict[str, str]
    plans: Dict[Unit, UnitPlan]
    sources: Optional[Dict[Unit, FrozenSet[str]]] = None
    n_waiting: Optional[int] = None



# ──────────────────────────────────────────────────────────────────────
# Fonctions d'étape : passes planifiées et calcul, un run par passe
# ──────────────────────────────────────────────────────────────────────

# Nœud du rapport de run (clé des contrôles des paramètres tracking)
NODE = "compute_partner_vulnerabilities"


# Fonction de chargement des tables de passage des millésimes historiques
def load_partner_concordances(
    vintages: Sequence[str],
    nomenclatures: Mapping[str, int],
    baci_config: Mapping[str, Any],
    *,
    client_factory: Optional[Callable[[], Any]] = None,
    loader: Any = None,
    saver: Any = None,
) -> Dict[Tuple[str, str], pd.DataFrame]:
    """Load the correspondence tables into every historical vintage requested.

    Same Parquet cache as the BACI step (``CLASSIFICATIONS.CONCORDANCE_PATH``
    of the ``baci`` parameters); a missing pair is downloaded from UNSD and
    cached.

    Args:
        vintages: Historical vintages requested.
        nomenclatures: Mapping vintage label -> entry-into-force year.
        baci_config: The ``baci`` parameter block.
        client_factory: Builds the UNSD client (``statflows.UNSDClient`` by default).
        loader: Table loader of the cache (a fresh one by default).
        saver: Table saver of the cache (a fresh one by default).

    Returns:
        Mapping ``(source, target) -> table``.
    """
    from macroforecast.storage import Loader as TableLoader, Saver as TableSaver

    if client_factory is None:
        from statflows import UNSDClient as client_factory
    classifications = baci_config["CLASSIFICATIONS"]
    return prepare_concordances(
        downward_pairs(vintages, nomenclatures),
        client_factory=client_factory,
        loader=loader if loader is not None else TableLoader(),
        saver=saver if saver is not None else TableSaver(),
        concordance_path=classifications["CONCORDANCE_PATH"],
        bucket=baci_config.get("BUCKET"),
        force_refresh=bool(classifications.get("FORCE_REFRESH", False)),
    )


# Plan des passes de l'étape partenaires
@dataclass
class PartnerPlan:
    """Passes planned by :func:`plan_partner_passes` and their shared inputs.

    Attributes:
        passes: Pass of the rows in force, then one per historical vintage.
        concordances: Correspondence tables of the historical passes.
        config: Methodological configuration (without the classification key).
        flows: Every configured direction, in output order.
    """

    passes: List["PartnerPass"]
    concordances: Dict[Tuple[str, str], pd.DataFrame]
    config: VulnerabilityConfig
    flows: Tuple[str, ...]


# Fonction de planification des passes (aucune lecture de table)
def plan_partner_passes(
    state: FreshnessRegistry,
    download_registry: Any,
    *,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    dataflow: str,
    concordances_loader: Optional[Callable[..., Dict[Tuple[str, str], pd.DataFrame]]] = None,
    force: Optional[ForceSpec] = None,
    adopt_legacy_fingerprints: Optional[bool] = None,
) -> PartnerPlan:
    """Plan the pass of the rows in force and one pass per historical vintage.

    Args:
        state: Partners freshness registry.
        download_registry: View of the Comext download registry
            (``pairs_last_download()``).
        params: The ``vulnerabilities`` parameters (``PARAMETERS``, ``FLOWS``,
            ``VINTAGES``, ``VINTAGES_ON_UNMAPPED``, ``VULNERABILITIES``).
        runtime: The ``runtime`` parameters (``NOMENCLATURES.HS``).
        dataflow: Source dataflow (block of ``VULNERABILITIES``).
        concordances_loader: Called as ``loader(vintages, nomenclatures)`` to
            load the correspondence tables of the historical vintages; required
            when ``VINTAGES`` requests some.
        force: One-off forcing; read from ``runtime`` alone when ``None``.
        adopt_legacy_fingerprints: Deployment migration flag (rows in force);
            read from ``STATE`` alone when ``None``.

    Returns:
        The planned passes.

    Raises:
        ValueError: If historical vintages are requested without a loader of
            the correspondence tables.
    """
    nomenclatures = runtime["NOMENCLATURES"]["HS"]
    config = vulnerability_config_from_params(params.get("PARAMETERS") or {})
    flows = load_flows(params)
    vintages = requested_vintages(params.get(_VINTAGES_KEY, []), nomenclatures)
    on_unmapped = params.get(_ON_UNMAPPED_KEY, "drop")
    block = params["VULNERABILITIES"][dataflow]
    force = force if force is not None else ForceSpec.from_runtime(runtime, environ={})
    adopt = (
        adopt_legacy_fingerprints
        if adopt_legacy_fingerprints is not None
        else adopt_legacy_flag(block.get("STATE"), environ={})
    )

    # Couples reporter x produit du registre de téléchargement (lecture seule)
    last_download = download_registry.pairs_last_download()
    classification = partner_classification(nomenclatures)
    requested = partner_requested(config, flows)

    # Passe des lignes en vigueur : unités du registre de téléchargement
    units = partner_units(last_download, classification)
    passes = [
        PartnerPass(
            label=classification,
            target_vintage=None,
            units=units,
            requested=requested,
            plans=plan_partner_units(state, units, requested, force, adopt_legacy_fingerprints=adopt),
        )
    ]

    # Passes historiques : unités par préimage des tables de passage
    concordances: Dict[Tuple[str, str], pd.DataFrame] = {}
    if vintages:
        if concordances_loader is None:
            raise ValueError("VINTAGES requests historical vintages: a correspondence-table loader is required")
        concordances = concordances_loader(vintages, nomenclatures)
        conversions = historical_conversions(concordances, vintages, nomenclatures)
        for vintage in vintages:
            historical = historical_units(last_download, conversions[vintage], vintage)
            vintage_requested = historical_requested(requested, concordances, vintage, on_unmapped)
            passes.append(
                PartnerPass(
                    label=vintage,
                    target_vintage=vintage,
                    units=historical.watermarks,
                    requested=vintage_requested,
                    plans=plan_partner_units(state, historical.watermarks, vintage_requested, force),
                    sources=historical.sources,
                    n_waiting=len(historical.waiting),
                )
            )
            # Logging
            if historical.waiting:
                logger.info(
                    f"{vintage} : {len(historical.waiting)} unité(s) en attente d'une source "
                    f"jamais téléchargée (ex. {next(iter(historical.waiting)).key})"
                )

    # Logging
    for partner_pass in passes:
        reasons = Counter(plan.reason for plan in partner_pass.plans.values())
        logger.info(
            f"{partner_pass.label} ({'en vigueur' if partner_pass.target_vintage is None else 'historique'}) : "
            f"{len(partner_pass.plans)} unité(s) à recalculer sur {len(partner_pass.units)} ({dict(reasons)})"
        )
    return PartnerPlan(passes=passes, concordances=concordances, config=config, flows=flows)


# Fonction d'étape : métriques partenaires, une passe par nomenclature
def run_partner_vulnerabilities(
    source: Any,
    result: Any,
    state: FreshnessRegistry,
    download_registry: Any,
    *,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    dataflow: str,
    runs: Optional[UnitRuns] = None,
    concordances_loader: Optional[Callable[..., Dict[Tuple[str, str], pd.DataFrame]]] = None,
    force: Optional[ForceSpec] = None,
    adopt_legacy_fingerprints: Optional[bool] = None,
    run_id: Optional[str] = None,
) -> StepResult:
    """Plan, compute and record the partner metrics, one tracked run per pass.

    The source and result connections are opened once, only when a pass has
    stale units, and shared by every pass. The failure of a pass crosses its
    own run (failed, with its reduced description), is logged, and does not
    stop the next passes.

    Args:
        source: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            Comext fact table.
        result: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            partner metrics (``indicators``).
        state: Partners freshness registry.
        download_registry: View of the Comext download registry.
        params: The ``vulnerabilities`` parameters (``TRACKING``,
            ``PARAMETERS.BACKEND`` read here too).
        runtime: The ``runtime`` parameters.
        dataflow: Source dataflow (block of ``VULNERABILITIES``).
        runs: Factory of the run of each pass (tags ``classification`` and
            ``in_force``); every pass in a null run when ``None``.
        concordances_loader: See :func:`plan_partner_passes`.
        force: One-off forcing; read from ``runtime`` alone when ``None``.
        adopt_legacy_fingerprints: Deployment migration flag.
        run_id: Run identifier recorded on the DuckLake snapshots.

    Returns:
        The step result: one unit per pass with stale units, ``children``
        holding the result of each successful pass (its ``outputs`` ``plans``,
        ``report`` and ``written``); ``failures`` raised as
        ``RuntimeError("Passe(s) partenaires en échec : [...]")``.
    """
    runs = runs if runs is not None else shared_runs()
    plan = plan_partner_passes(
        state, download_registry, params=params, runtime=runtime, dataflow=dataflow,
        concordances_loader=concordances_loader, force=force,
        adopt_legacy_fingerprints=adopt_legacy_fingerprints,
    )
    force = force if force is not None else ForceSpec.from_runtime(runtime, environ={})
    todo = [partner_pass for partner_pass in plan.passes if partner_pass.plans]

    # Rien à recalculer : seules d'éventuelles entrées v1 adoptées sont écrites
    if not todo:
        state.save()
        logger.info("Nothing to recompute, stop.")
        return StepResult(step="partners", reportable=False)

    parameters = params.get("PARAMETERS") or {}
    tracking = params.get("TRACKING") or {}
    block = params["VULNERABILITIES"][dataflow]
    compute_kwargs = dict(
        registry=state,
        force=force,
        source_schema=source.schema,
        result_schema=result.schema,
        config=plan.config,
        nomenclatures=runtime["NOMENCLATURES"]["HS"],
        flows=plan.flows,
        concordances=plan.concordances,
        on_unmapped=params.get(_ON_UNMAPPED_KEY, "drop"),
        is_provisional=bool(block.get(_PROVISIONAL_KEY, False)),
        # Backend de calcul narwhals, lu à part (pas un paramètre méthodologique)
        backend=parameters.get(_BACKEND_KEY, "pandas"),
        log_artifacts=bool(tracking.get("LOG_ARTIFACTS", True)),
        measure_drift=bool(tracking.get("DRIFT", True)),
        run_id=run_id,
    )

    # Connexions ouvertes une fois pour toutes les passes
    children: Dict[str, StepResult] = {}
    failed: Dict[str, BaseException] = {}
    with source.connect() as source_conn, result.connect() as result_conn:
        for partner_pass in todo:
            try:
                children[partner_pass.label] = _run_partner_pass(
                    partner_pass, runs,
                    source_conn=source_conn,
                    result_conn=result_conn,
                    source_catalog_alias=attached_catalog_alias(source),
                    result_catalog_alias=attached_catalog_alias(result),
                    dataflow=dataflow,
                    **compute_kwargs,
                )
            except Exception as exc:  # une passe en échec n'emporte pas les autres
                logger.exception(f"Passe {partner_pass.label} en échec : {exc}")
                failed[partner_pass.label] = exc

    return StepResult(
        step="partners",
        n_units_planned=len(todo),
        n_units_succeeded=len(children),
        failures={label: failure_message(exc) for label, exc in failed.items()},
        children=children,
        reportable=False,
        failure_exception=(
            RuntimeError(f"Passe(s) partenaires en échec : {sorted(failed)}") if failed else None
        ),
    )


# Fonction d'exécution d'une passe dans son run
def _run_partner_pass(
    partner_pass: PartnerPass,
    runs: UnitRuns,
    *,
    dataflow: str,
    **compute_kwargs: Any,
) -> StepResult:
    """Run one pass inside its own tracked run and publish its report there.

    Args:
        partner_pass: The planned pass.
        runs: Factory of the run of the pass.
        dataflow: Source dataflow (tag of the run).
        **compute_kwargs: Remaining arguments of :func:`compute_partner_units`.

    Returns:
        The result of the pass.
    """
    in_force = partner_pass.target_vintage is None
    plans = partner_pass.plans
    tags = {"classification": partner_pass.label, "in_force": str(in_force).lower()}
    with runs(partner_pass.label, tags) as run:
        tracker = capturing(run.tracker)
        computed = compute_partner_units(
            plans=plans,
            units=partner_pass.units,
            requested=partner_pass.requested,
            target_vintage=partner_pass.target_vintage,
            sources=partner_pass.sources,
            n_waiting=partner_pass.n_waiting,
            tracker=tracker,
            commit_message=f"{NODE} {partner_pass.label} {len(plans)} unités",
            **compute_kwargs,
        )
        report = computed.report

        # Métriques préfixées par sens ; mêmes noms pour toutes les passes, la
        # classification étant portée par les tags du run
        tracker.log_metrics(flow_run_metrics(report, _METRICS_FAMILY))
        tracker.set_tags(
            {
                "dataflow": dataflow,
                "result_schema": compute_kwargs["result_schema"],
                "created": str(report.created),
                "n_pairs": str(len(plans)),
                "flows": ",".join(report.flows),
            }
        )
        outcome = StepResult(
            step="partners",
            n_units_planned=len(plans),
            n_units_succeeded=len(plans),
            metrics=dict(tracker.metrics),
            artifacts=dict(tracker.tables),
            tags=dict(tracker.tags),
            units_label=f"{len(plans)} unités {partner_pass.label} × reporter × produit",
            outputs={"plans": computed.plans, "report": report, "written": computed.written},
        )
        # Rapport de run publié avant toute sortie en erreur
        run.publish(outcome)

    # Logging
    logger.info(
        f"Passe {partner_pass.label} terminée : {report} ; "
        f"{len(computed.written)} fragment(s) de registre écrit(s)"
    )
    return outcome
