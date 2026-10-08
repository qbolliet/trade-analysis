"""Script de redressement BACI multi-millésimes des flux de commerce international.

Variante de ``scripts/process_baci.py`` qui exécute la méthodologie BACI une
fois par millésime de nomenclature HS configuré (typiquement HS2022, HS2017,
HS2012) plutôt qu'une seule fois sur la nomenclature courante. Pour chaque
millésime cible ``V`` :

1. les déclarations COMTRADE d'année ``>= START_YEAR[V]`` sont sélectionnées ;
2. toutes les nomenclatures présentes dans cette tranche sont harmonisées vers
   ``V`` par ``macroforecast.trade.processing.classification.HsHarmonizer`` ;
3. la méthodologie BACI (``run_baci``) est appliquée à la tranche harmonisée ;
4. le résultat est écrit dans un schéma DuckLake dédié au millésime
   (``baci_hs2022``, ``baci_hs2017``, …) du même catalogue.

D'où la propriété visée : HS2022 ne porte que les années 2022 et suivantes,
tandis que HS2017 porte 2017 et suivantes, les données postérieures à 2022
étant reversées vers HS2017 par les tables de passage UNSD. Un schéma par
millésime plutôt qu'une colonne de millésime dans une table unique : les
millésimes se recouvrent (une même année figure dans plusieurs cibles), et les
mélanger inviterait au double compte.

Comme ``process_baci.py``, ce script assume tout l'I/O — chargement de la
configuration YAML, construction de la ``BaciConfig``, lecture des fichiers
Excel CEPII, lecture de la table de faits COMTRADE et écriture des résultats —
tandis que le package (``macroforecast.trade.processing``) ne contient que la
méthodologie. Les tables de correspondance HS sont téléchargées via
``UNSDClient`` puis mises en cache par ``kedro_pipeline.steps.baci.prepare_concordances``
(Parquet + registre JSON, cache partagé avec l'étape partenaires) :
elles sont invariantes une fois publiées, l'absence de fichier en cache est
donc le seul déclencheur de téléchargement (pas de vérification de fraîcheur
distante), sauf ``FORCE_REFRESH`` explicite en configuration.

L'échec d'un millésime n'interrompt pas les autres : chaque échec est capturé
et journalisé individuellement, et le script ne sort en erreur qu'en fin de
parcours si au moins un millésime a échoué.

Fraîcheur : un registre fragmenté (``STATE.PATH_TEMPLATE``, un fichier par
millésime), l'unité étant le millésime entier puisque ses paramètres sont
estimés sur toutes ses années. Une passe sur un millésime est lancée s'il n'a
jamais été calculé, si la passe précédente est interrompue (années écrites
différentes du périmètre), si une nouvelle année complète entre dans son
périmètre (``REFRESH.ON_NEW_COMPLETE_YEAR``), si l'empreinte méthodologique
change, en cas de forçage, ou si l'amont Comtrade a été révisé et que le dernier
calcul date d'au moins ``REFRESH.MIN_INTERVAL_DAYS`` jours. Une entrée
« démarrée » (identifiant de passe, années du périmètre, aucune année écrite)
précède toute écriture et sert de point de reprise ; l'entrée terminée n'est
écrite qu'après succès. Son ``last_computed`` est la seule chose que
``scripts/compute_network_vulnerabilities.py`` lit de ce script : le couplage
reste faible, aucun état en mémoire n'étant partagé. L'ancien registre
``PATHS.LAST_PROCESSING_PATH`` n'est plus qu'une source de migration.

Périmètre et exécution :

- **porte de complétude** : le registre de téléchargement Comtrade
  (``LAST_DOWNLOAD_PATH``) est confronté à la liste des requêtes PLANIFIÉES,
  reconstruite par la même fonction que le script de téléchargement
  (``scripts.download_comtrade.plan_queries``) ; seules les années dont la part
  de lots téléchargés au moins une fois atteint ``COMPLETENESS.MIN_SHARE`` sont
  redressées (``kedro_pipeline.steps.baci.eligible_years``) ;
- **traitement par passes à mémoire bornée** : la table de faits Comtrade n'est
  jamais lue en entier. Chaque millésime est redressé par
  ``macroforecast.trade.processing.run_baci_passes`` sur des tranches annuelles
  (découpées en blocs de chapitres SH au-delà de ``PASSES.MAX_ROWS_PER_CHUNK``),
  les flux miroirs intermédiaires étant écrits en Parquet de travail sous
  ``PASSES.WORK_PATH/<millésime>/<fit_id>/`` (supprimés en fin de passe réussie
  sauf ``PASSES.KEEP_WORK_FILES``, réutilisés par la reprise d'une passe
  interrompue de même ``fit_id``). Les estimations mises en commun sur toutes les
  années du millésime (taux de conversion, médianes des valeurs unitaires,
  gravité et distance de Cook, ANOVA de qualité et covariance robuste) sont
  exprimées par des statistiques suffisantes : le résultat est identique au
  traitement monobloc ;
- **écriture année par année** : chaque année est réécrite dans une transaction
  DuckLake (suppression de ses lignes puis insertion, colonne ``fit_id``), puis
  ajoutée aux ``years_written`` du registre (point de reprise) ;
- **restriction à certains millésimes** : ``BACI_TARGETS`` (variable
  d'environnement) ou ``--targets HS2017,HS1992`` (ligne de commande) ne traite
  que les millésimes listés — un pod par millésime en production ;
- **étiquette ``is_provisional``** : vraie quand le périmètre produit planifié
  n'est qu'un sous-ensemble strict du périmètre HS6 complet (profil ``demo``).

Configuration lue : blocs ``baci``, ``comtrade`` et ``runtime`` des paramètres
Kedro (``config/<env>/parameters_*.yml``), environnement choisi par ``KEDRO_ENV``
(``local`` par défaut, ``demo`` pour le périmètre de démonstration).
"""
# Importation des modules
# Modules de base
import argparse
import logging
import os
import sys
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence

# Modules de manipulation de données
import duckdb
import pandas as pd

# Configuration du projet (paramètres Kedro, expériences MLflow, cibles actives)
from kedro_pipeline.config import (
    active_targets,
    experiment_name,
    load_parameters,
    read_config_file,
)
# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
)
from kedro_pipeline.io.registry_views import DownloadRegistryView, ProductsKey
# Registres de fraîcheur v2 (fragments, empreintes, forçage, cadence)
from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    LegacySource,
    NewDataPredicate,
    RegistryEntry,
    Unit,
    UnitPlan,
    adopt_legacy_flag,
    fingerprint,
    format_instant,
    legacy_entry,
    plan_metrics,
    units_to_compute,
    upstream_is_newer,
)
from kedro_pipeline.steps.reference import publish_hs_reference
# Étape BACI : cache des tables de correspondance UNSD (partagé avec l'étape
# partenaires), porte de complétude, entrées / sorties du traitement par passes
from kedro_pipeline.steps.baci import (
    DuckDBPassIO,
    DuckLakeYearWriter,
    chapter_links,
    classifications_by_year,
    completeness_by_year,
    concordance_pairs,
    eligible_years,
    peak_memory_mb,
    prepare_concordances,
    work_root,
)
# Planification des requêtes Comtrade : même liste que le téléchargement
from scripts.download_comtrade import (
    fetch_dimension_codelists,
    load_runtime_config,
    plan_queries,
    _SUBSCRIPTION_KEY_ENV,
)
from statflows import ComtradeClient
from statflows.core.factory import filter_codes
# Modules de chargement/sauvegarde de données (xls/parquet, puis json)
from macroforecast.storage import Loader as TableLoader, Saver as TableSaver
# Helpers DuckLake partagés (création puis upsert de la table de faits)
from statflows.storage.ducklake.tables import FACT_TABLE as _FACT_TABLE
from statflows.storage.json import Loader as JsonLoader, Saver as JsonSaver
from statflows.core.download import _schema_name

# Module client des tables de correspondance de nomenclatures UNSD
from statflows import UNSDClient

# Module d'implémentation du traitement BACI
from macroforecast.trade.processing import run_baci_passes
from macroforecast.trade.processing import BaciConfig, ComtradeSchema, DEFAULT_CONFIG, BaciReport
from macroforecast.trade.processing import BACI_FINGERPRINT_EXCLUDED
from macroforecast.trade.methodology import methodology_params
from macroforecast.trade.processing import HsHarmonizer
# Module de suivi d'exécution (MLflow optionnel) et rapport de run
from macroforecast.tracking import CapturingTracker, get_tracker, rekey_metrics
from macroforecast.tracking.figures import key_figures_baci, sections_baci
from macroforecast.tracking.report import Units
from scripts._run_report import RunScope, guarded_run, run_name


# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé YAML portant les conventions de schéma des sources (sous-section de PARAMETERS)
_SCHEMA_KEY = "SCHEMA"

# Clé racine du registre JSON des dates de dernier traitement BACI, lu par
# scripts/compute_network_vulnerabilities.py pour ne recalculer que les
# millésimes réécrits depuis son dernier passage
_PROCESSING_ROOT = "BACI"


# ──────────────────────────────────────────────────────────────────────
# Plomberie du script (chargement config, lecture de la table de faits). Seule
# dépendance croisée : la planification des requêtes Comtrade, importée de
# scripts/download_comtrade.py pour que la porte de complétude raisonne sur
# exactement les mêmes lots que le téléchargement.
# ──────────────────────────────────────────────────────────────────────

# Fonction de chargement de la configuration associée à la base comtrade
def load_comtrade_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the UN Comtrade configuration (source catalog of BACI).

    Args:
        config_path: Explicit YAML file (``comtrade`` root key or historical
            format without it). If None, the ``comtrade`` block of the Kedro
            parameters of the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary.
    """
    if config_path is None:
        return load_parameters()["comtrade"]
    return read_config_file(config_path, "comtrade")


# Fonction de chargement de la configuration
def load_baci_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the BACI configuration.

    Args:
        config_path: Explicit YAML file (``baci`` root key or historical format
            without it). If None, the ``baci`` block of the Kedro parameters of
            the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary.
    """
    if config_path is None:
        return load_parameters()["baci"]
    return read_config_file(config_path, "baci")


# Fonction de lecture de la table de faits COMTRADE (schéma source du catalogue partagé)
def _read_comtrade_fact_table(
    conn: duckdb.DuckDBPyConnection,
    source_schema: str,
    columns: Sequence[str],
    period_col: str,
    years: Sequence[int],
    period_start: Optional[int] = None,
    period_end: Optional[int] = None,
) -> pd.DataFrame:
    """Read selected columns of the COMTRADE fact table for some years, read-only.

    No longer used by :func:`main` (the passes read one year at a time, see
    ``kedro_pipeline.steps.baci.read_comtrade_year``); kept for ad-hoc reads of a
    few years (tests, notebooks).

    Source and result live in two schemas of the same DuckLake catalog (cf.
    module docstring), so a plain schema-qualified ``SELECT`` on the shared
    connection is enough — no separate ``ATTACH`` is required. The year filter
    is pushed down to SQL with bound parameters (identifiers — schema and
    column names from the configuration — are quoted, never values).

    Args:
        conn: Open DuckLake connection (result-schema-bound connector).
        source_schema: Schema holding the COMTRADE ``fact_table``.
        columns: Columns to project.
        period_col: Period column (year, or ``YYYYMM``, as text or integer).
        years: Years to read (e.g. the years passing the completeness gate).
        period_start: Lower bound (included), ``None`` for none.
        period_end: Upper bound (included), ``None`` for none.

    Returns:
        A pandas DataFrame of the projected, year-filtered fact table.

    Examples:
        >>> df = _read_comtrade_fact_table(
        ...     conn, "C_A_HS", ["period", "primaryValue"], "period", [2022, 2023],
        ...     period_start=2017,
        ... )  # doctest: +SKIP
    """
    # Construction de la clause de projection
    col_list = ", ".join(f'"{c}"' for c in columns)
    # Expression de l'année (les 4 premiers caractères de la période)
    year_expr = f'CAST(substr(CAST("{period_col}" AS VARCHAR), 1, 4) AS INTEGER)'
    return conn.execute(
        f'SELECT {col_list} FROM "{source_schema}".{_FACT_TABLE} '
        f"WHERE list_contains(?::INTEGER[], {year_expr}) "
        f"AND (?::INTEGER IS NULL OR {year_expr} >= ?::INTEGER) "
        f"AND (?::INTEGER IS NULL OR {year_expr} <= ?::INTEGER)",
        [
            [int(y) for y in years],
            period_start, period_start,
            period_end, period_end,
        ],
    ).df()


# ──────────────────────────────────────────────────────────────────────
# Porte de complétude et périmètre (PS-14.1) : logique pure, testable sur un
# registre fictif ; seul main() lit le registre et appelle l'API
# ──────────────────────────────────────────────────────────────────────

# Les fonctions de la porte de complétude (completeness_by_year, eligible_years)
# vivent dans kedro_pipeline.steps.baci, importé ci-dessus


# Fonction de résolution des premières années des millésimes cibles
def resolve_target_start_years(
    targets: Mapping[str, Mapping[str, Any]],
    runtime_config: Mapping[str, Any],
) -> Dict[str, int]:
    """Resolve the first year of each BACI target vintage (PD-08).

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


# Fonction de détection d'un périmètre produit restreint
def is_provisional_scope(
    available_products: Iterable[str],
    planned_products: Iterable[str],
    full_product_regex: str,
    exclude: Optional[Sequence[str]] = None,
) -> bool:
    """Tell whether the planned products are a strict subset of the full BACI scope.

    A BACI computed on a product subset is not the full BACI (reporter quality
    is estimated on every product): it is labelled provisional (PD-06, point 3).

    Args:
        available_products: Product codelist of the source.
        planned_products: Products actually planned for download.
        full_product_regex: Pattern of the full product scope (HS6).
        exclude: Codes excluded from the full scope (same deny-list as the
            download filters).

    Returns:
        ``True`` when some code of the full scope is not planned.

    Examples:
        >>> is_provisional_scope(["010121", "854140"], ["854140"], r"^\\d{6}$")
        True
        >>> is_provisional_scope(["010121", "854140", "01"], ["010121", "854140"], r"^\\d{6}$")
        False
    """
    full_scope = set(
        filter_codes(available_products, include_regex=full_product_regex, exclude=exclude)
    )
    return not full_scope.issubset(set(map(str, planned_products)))


# Fonction de calcul de la part minimale sur une plage d'années
def _share_min(shares: Mapping[int, float], start: int, end: Optional[int]) -> float:
    """Minimum download share over the planned years of ``[start, end]`` (0 if none)."""
    values = [s for y, s in shares.items() if y >= start and (end is None or y <= end)]
    return float(min(values)) if values else 0.0


# Fonction de construction des métriques de couverture d'un millésime
def coverage_metrics(
    years_eligible: Sequence[int],
    shares: Mapping[int, float],
    start: int,
    end: Optional[int],
) -> Dict[str, float]:
    """Coverage metrics of one vintage: eligible years and minimum download share.

    Args:
        years_eligible: Years passing the completeness gate.
        shares: Download share of the planned batches, by year.
        start: First year of the vintage.
        end: Optional last year of the perimeter.

    Returns:
        ``coverage/years_eligible`` and ``coverage/share_min``.

    Examples:
        >>> coverage_metrics([2019, 2020, 2021], {2019: 1.0, 2020: 0.9, 2021: 1.0}, 2020, None)
        {'coverage/years_eligible': 2.0, 'coverage/share_min': 0.9}
    """
    return {
        "coverage/years_eligible": float(sum(y >= start for y in years_eligible)),
        "coverage/share_min": _share_min(shares, start, end),
    }


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
        return DEFAULT_CONFIG.schema

    # Surcharge générique champ à champ (noms de colonnes et codes de flux)
    valid = {f.name for f in fields(ComtradeSchema)}
    overrides: Dict[str, object] = {}
    for key, value in params.items():
        if key not in valid:
            logger.warning("Champ de schéma BACI inconnu ignoré : %s", key)
            continue
        overrides[key] = value

    return replace(DEFAULT_CONFIG.schema, **overrides)


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
        return DEFAULT_CONFIG

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
        default = getattr(DEFAULT_CONFIG, key)
        if isinstance(default, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(v) if isinstance(v, (list, tuple)) else v for v in value
            )
        overrides[key] = value

    return replace(DEFAULT_CONFIG, schema=schema, **overrides)


# ──────────────────────────────────────────────────────────────────────
# Registre de fraîcheur v2 : un fragment par millésime, cadence de réestimation
# ──────────────────────────────────────────────────────────────────────

# Nom de l'étape (forçage FORCE_STEPS, champ « step » des fragments) et nom de
# l'empreinte unique du millésime (toutes les étapes BACI sont couplées)
STEP = "baci"


# Fonction de construction de l'unité de fraîcheur d'un millésime
def baci_unit(vintage: str) -> Unit:
    """Freshness unit of a BACI vintage (the whole vintage, all its years).

    Args:
        vintage: HS vintage label (``"HS2017"``).

    Returns:
        ``Unit(vintage=...)``.

    Examples:
        >>> baci_unit("HS2017").key
        'HS2017'
    """
    return Unit.of(vintage=vintage)


# Fonction de calcul de l'empreinte méthodologique du redressement
def baci_requested(config: BaciConfig) -> Dict[str, str]:
    """Current methodological fingerprint of the BACI reconstruction.

    A single fingerprint per vintage: the BACI steps are coupled (the gravity
    fit uses the converted tonnes, the reconciliation the reporting-quality
    sigmas…), so a vintage is always re-estimated as a whole. A fix in the
    implementation of any step is signalled by invalidating the recorded
    fingerprints (``scripts/invalidate_freshness.py --step baci``).

    Args:
        config: Methodological configuration of the reconstruction.

    Returns:
        ``{"baci": fingerprint}``.

    Examples:
        >>> list(baci_requested(DEFAULT_CONFIG))
        ['baci']
    """
    params = methodology_params(config, BACI_FINGERPRINT_EXCLUDED)
    return {STEP: fingerprint(STEP, params)}


# Fonction de lecture du registre v1 des traitements BACI en entrées héritées
def _parse_legacy_baci(data: Mapping[str, Any]) -> Iterator[RegistryEntry]:
    """Turn the version-1 processing registry (``{"BACI": {schema: {...}}}``) into legacy entries.

    Args:
        data: Version-1 document.

    Yields:
        One legacy entry per vintage recorded.
    """
    for schema, item in (data.get(_PROCESSING_ROOT) or {}).items():
        if not isinstance(item, Mapping) or not item.get("vintage"):
            continue
        yield legacy_entry(
            baci_unit(item["vintage"]),
            item.get("last_processed"),
            result_schema=item.get("result_schema", schema),
            n_rows=item.get("n_rows"),
        )


# Fonction de construction du registre de fraîcheur BACI
def baci_registry(
    baci_config: Mapping[str, Any],
    *,
    loader: Optional[JsonLoader] = None,
    saver: Optional[JsonSaver] = None,
) -> FreshnessRegistry:
    """Build the BACI freshness registry (one fragment per vintage).

    Also read by the network step: the ``last_computed`` of a vintage is the
    upstream watermark of its network metrics.

    Args:
        baci_config: The ``baci`` parameter block (``BUCKET``, ``STATE``,
            ``PATHS.LAST_PROCESSING_PATH`` read as the version-1 fallback).
        loader: JSON loader (a fresh one by default).
        saver: JSON saver (a fresh one by default).

    Returns:
        The registry.

    Raises:
        KeyError: If the configuration has no ``STATE.PATH_TEMPLATE``.
    """
    bucket = baci_config.get("BUCKET")
    legacy_path = (baci_config.get("PATHS") or {}).get("LAST_PROCESSING_PATH")
    return FreshnessRegistry(
        baci_config["STATE"]["PATH_TEMPLATE"],
        bucket,
        STEP,
        shard_of=lambda unit: unit.get("vintage"),
        legacy=LegacySource(legacy_path, bucket, _parse_legacy_baci) if legacy_path else None,
        loader=loader,
        saver=saver,
    )


# Fonction de calcul des périmètres temporels des millésimes
def vintage_scopes(
    years_eligible: Sequence[int],
    start_years: Mapping[str, int],
) -> Dict[str, List[int]]:
    """Years of each vintage passing the completeness gate.

    Args:
        years_eligible: Complete years (:func:`eligible_years`).
        start_years: First year of each vintage
            (:func:`resolve_target_start_years`).

    Returns:
        Mapping ``vintage -> sorted years``, vintages without any eligible
        year left out.

    Examples:
        >>> vintage_scopes([2016, 2017, 2022], {"HS2022": 2022, "HS2017": 2017})
        {'HS2022': [2022], 'HS2017': [2017, 2022]}
    """
    scopes = {
        label: sorted(int(y) for y in years_eligible if int(y) >= int(start))
        for label, start in start_years.items()
    }
    return {label: years for label, years in scopes.items() if years}


# Fonction de calcul du watermark amont d'un millésime
def vintage_watermark(
    batches: Mapping[int, Mapping[ProductsKey, datetime]],
    years: Iterable[int],
) -> Optional[datetime]:
    """Most recent download of the Comtrade batches of the years of a vintage.

    Args:
        batches: Downloaded batches by year
            (:meth:`DownloadRegistryView.batches_by_year`).
        years: Years of the vintage scope.

    Returns:
        The latest ``last_download``, or ``None`` when no batch is known.

    Examples:
        >>> from datetime import timezone
        >>> t1, t2 = datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc)
        >>> vintage_watermark({2022: {("a",): t1}, 2023: {("a",): t2}}, [2022]) == t1
        True
    """
    dates = [when for year in years for when in (batches.get(int(year)) or {}).values()]
    return max(dates) if dates else None


# Fonction de calcul de l'identifiant d'une passe d'estimation
def compute_fit_id(
    vintage: str,
    scope: Sequence[int],
    watermark: Optional[datetime],
    requested: Mapping[str, str],
) -> str:
    """Identifier of a BACI estimation pass.

    Two passes on the same vintage, scope, upstream watermark and methodology
    share their identifier, which makes the rewrite of an interrupted pass
    idempotent.

    Args:
        vintage: Vintage label.
        scope: Years of the pass.
        watermark: Upstream watermark of the pass.
        requested: Methodological fingerprint (:func:`baci_requested`).

    Returns:
        A 16-hex identifier.

    Examples:
        >>> compute_fit_id("HS2017", [2017], None, {"baci": "x"}) == compute_fit_id("HS2017", [2017], None, {"baci": "x"})
        True
    """
    return fingerprint(
        "fit",
        {
            "vintage": vintage,
            "scope": sorted(int(y) for y in scope),
            "watermark": format_instant(watermark),
            "fingerprints": dict(requested),
        },
    )


# Fonction de test de complétude de l'écriture d'une passe
def pass_is_complete(entry: RegistryEntry) -> bool:
    """Whether every year of the recorded pass was written.

    A version-1 entry (no ``years_scope``) is deemed complete: version 1 only
    recorded fully written vintages.

    Args:
        entry: Registry entry of a vintage.

    Returns:
        ``True`` when ``years_written`` covers ``years_scope``.

    Examples:
        >>> unit = baci_unit("HS2017")
        >>> pass_is_complete(RegistryEntry(unit, extra={"years_scope": [2017], "years_written": []}))
        False
    """
    scope = entry.extra.get("years_scope")
    if scope is None:
        return True
    return sorted(entry.extra.get("years_written") or []) == sorted(scope)


# Fabrique du prédicat de fraîcheur des millésimes BACI
def baci_is_new_data(
    scopes: Mapping[Unit, Sequence[int]],
    refresh: Optional[Mapping[str, Any]],
    now: datetime,
) -> NewDataPredicate:
    """Build the ``new_data`` rule of the BACI vintages (re-estimation cadence).

    A pass on a computed vintage is due when:

    - the previous pass was interrupted (``years_written`` differs from
      ``years_scope``): it is resumed from the start of the vintage;
    - a new complete year enters its scope, when ``ON_NEW_COMPLETE_YEAR`` is
      true (immediate pass);
    - its scope changed otherwise, or the Comtrade upstream was revised (more
      recent watermark), **and** the last computation is at least
      ``MIN_INTERVAL_DAYS`` old: revisions trigger at most one pass per
      interval, so that seven vintages are not re-estimated every day during
      the catch-up.

    Never-computed, forced and fingerprint-changed vintages are handled by
    :func:`kedro_pipeline.io.freshness.units_to_compute` itself.

    Args:
        scopes: Current scope of each vintage unit.
        refresh: ``REFRESH`` block of the ``baci`` parameters
            (``MIN_INTERVAL_DAYS``, default 7; ``ON_NEW_COMPLETE_YEAR``,
            default true).
        now: Decision instant.

    Returns:
        The predicate ``(unit, entry, watermark) -> bool``.
    """
    refresh = refresh or {}
    min_interval = timedelta(days=float(refresh.get("MIN_INTERVAL_DAYS", 7)))
    on_new_year = bool(refresh.get("ON_NEW_COMPLETE_YEAR", True))

    def rule(unit: Unit, entry: RegistryEntry, watermark: Optional[datetime]) -> bool:
        # Reprise d'une passe interrompue (table mixte entre deux ajustements)
        if not pass_is_complete(entry):
            return True
        scope = {int(y) for y in scopes.get(unit, ())}
        recorded = entry.extra.get("years_scope")
        recorded_scope = {int(y) for y in recorded} if recorded is not None else None
        # Nouvelle année complète : passe immédiate si la configuration le demande
        if recorded_scope is not None and on_new_year and scope - recorded_scope:
            return True
        # Autres changements soumis à l'intervalle minimal entre deux passes
        elapsed = entry.last_computed is not None and now - entry.last_computed >= min_interval
        if not elapsed:
            return False
        if recorded_scope is not None and scope != recorded_scope:
            return True
        return upstream_is_newer(unit, entry, watermark)

    return rule


# Fonction de décision des millésimes à redresser
def plan_baci_vintages(
    registry: FreshnessRegistry,
    scopes: Mapping[str, Sequence[int]],
    watermarks: Mapping[str, Optional[datetime]],
    requested: Mapping[str, str],
    force: ForceSpec,
    refresh: Optional[Mapping[str, Any]],
    now: datetime,
    *,
    adopt_legacy_fingerprints: bool = False,
) -> Dict[Unit, UnitPlan]:
    """Decide which BACI vintages to re-estimate.

    Args:
        registry: BACI freshness registry.
        scopes: Eligible years of each vintage (:func:`vintage_scopes`).
        watermarks: Upstream watermark of each vintage
            (:func:`vintage_watermark`).
        requested: Current methodological fingerprint (:func:`baci_requested`).
        force: One-off forcing (step ``baci``; the ``VINTAGES`` filter
            applies, ``PERIODS`` does not since a vintage is always
            re-estimated as a whole).
        refresh: ``REFRESH`` block of the ``baci`` parameters.
        now: Decision instant.
        adopt_legacy_fingerprints: Deployment migration flag.

    Returns:
        Mapping ``unit -> plan`` for the vintages to re-estimate.
    """
    units = {baci_unit(label): years for label, years in scopes.items()}
    upstream = {baci_unit(label): watermarks.get(label) for label in scopes}
    return units_to_compute(
        units,
        registry,
        upstream,
        requested,
        force,
        step=STEP,
        is_new_data=baci_is_new_data(units, refresh, now),
        adopt_legacy_fingerprints=adopt_legacy_fingerprints,
    )


# Fonction de construction de l'entrée d'une passe démarrée (point de reprise)
def started_entry(
    previous: Optional[RegistryEntry],
    unit: Unit,
    plan: UnitPlan,
    fit_id: str,
    scope: Sequence[int],
) -> RegistryEntry:
    """Entry recorded before writing a vintage: the resume point of the pass.

    The previous ``last_computed`` is kept, so the network step does not
    recompute on a vintage whose rewrite has not completed, while
    ``years_written=[]`` makes an interrupted pass detectable.

    Args:
        previous: Current entry of the vintage, if any.
        unit: Vintage unit.
        plan: Plan of the pass.
        fit_id: Identifier of the pass (:func:`compute_fit_id`).
        scope: Years of the pass.

    Returns:
        The entry to upsert before writing.
    """
    return RegistryEntry(
        unit=unit,
        last_computed=previous.last_computed if previous else None,
        upstream_watermark=previous.upstream_watermark if previous else None,
        fingerprints=dict(previous.fingerprints) if previous else {},
        reason=plan.reason,
        extra={
            **(previous.extra if previous else {}),
            "fit_id": fit_id,
            "years_scope": sorted(int(y) for y in scope),
            "years_written": [],
        },
    )


# Fonction de construction de l'entrée d'une passe terminée
def completed_entry(
    unit: Unit,
    plan: UnitPlan,
    fit_id: str,
    scope: Sequence[int],
    processed_at: datetime,
    watermark: Optional[datetime],
    requested: Mapping[str, str],
    **counters: Any,
) -> RegistryEntry:
    """Entry recorded once every year of the vintage was written.

    Args:
        unit: Vintage unit.
        plan: Plan of the pass (its reason cascades downstream).
        fit_id: Identifier of the pass.
        scope: Years of the pass (all written).
        processed_at: Instant captured before the pass started.
        watermark: Upstream watermark taken into account.
        requested: Current methodological fingerprint.
        **counters: Extra fields (``n_rows``, ``result_schema``,
            ``is_provisional``…).

    Returns:
        The entry to upsert after the write.
    """
    years = sorted(int(y) for y in scope)
    return RegistryEntry(
        unit=unit,
        last_computed=processed_at,
        upstream_watermark=watermark,
        fingerprints=dict(requested),
        reason=plan.reason,
        extra={"fit_id": fit_id, "years_scope": years, "years_written": years, **counters},
    )


# Variable d'environnement restreignant les millésimes traités
TARGETS_ENV = "BACI_TARGETS"


# Fonction de lecture des millésimes demandés (ligne de commande ou environnement)
def requested_targets(
    argv: Optional[Sequence[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[List[str]]:
    """Read the vintages this run is restricted to, if any.

    ``--targets HS2017,HS1992`` takes precedence over the ``BACI_TARGETS``
    environment variable; both hold comma-separated labels. Unknown arguments
    are ignored (the script also runs under test runners).

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` when ``None``).
        environ: Environment mapping (``os.environ`` when ``None``).

    Returns:
        The requested labels, or ``None`` when no restriction is given.

    Examples:
        >>> requested_targets(["--targets", "HS2017, HS1992"], {})
        ['HS2017', 'HS1992']
        >>> requested_targets([], {"BACI_TARGETS": "HS2022"})
        ['HS2022']
        >>> requested_targets([], {}) is None
        True
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--targets", default=None)
    args, _ = parser.parse_known_args(list(sys.argv[1:] if argv is None else argv))
    env = os.environ if environ is None else environ
    raw = args.targets if args.targets is not None else env.get(TARGETS_ENV)
    if raw is None or not str(raw).strip():
        return None
    return [label.strip() for label in str(raw).split(",") if label.strip()]


# Fonction de restriction des cibles configurées aux millésimes demandés
def select_targets(
    targets: Mapping[str, Mapping[str, Any]],
    requested: Optional[Sequence[str]],
) -> Dict[str, Mapping[str, Any]]:
    """Restrict the configured targets to the requested vintages.

    Args:
        targets: ``CLASSIFICATIONS.TARGETS`` of the ``baci`` parameters; a
            null entry (vintage disabled by an environment) is never selected.
        requested: Requested labels (``None``: every enabled target).

    Returns:
        The selected targets, in configuration order.

    Raises:
        ValueError: If a requested label is not a configured target.

    Examples:
        >>> select_targets({"HS2022": {}, "HS2017": {}}, ["HS2017"])
        {'HS2017': {}}
        >>> select_targets({"HS2022": None, "HS2017": {}}, None)
        {'HS2017': {}}
    """
    # Cibles désactivées par un environnement (entrée nulle) : jamais traitées
    targets = active_targets(targets)
    if requested is None:
        return dict(targets)
    unknown = sorted(set(requested) - set(targets))
    if unknown:
        raise ValueError(f"Unknown BACI targets {unknown}; configured: {sorted(targets)}")
    return {label: cfg for label, cfg in targets.items() if label in set(requested)}


# Fonction d'ajout d'une année écrite à l'entrée d'une passe démarrée
def with_year_written(entry: RegistryEntry, year: int) -> RegistryEntry:
    """Return the entry of a started pass with one more year written.

    Args:
        entry: Entry of the vintage (started pass).
        year: Year just written.

    Returns:
        A copy whose ``years_written`` includes ``year`` (sorted, distinct).

    Examples:
        >>> entry = RegistryEntry(baci_unit("HS2017"), extra={"years_written": [2018]})
        >>> with_year_written(entry, 2017).extra["years_written"]
        [2017, 2018]
    """
    written = sorted({int(y) for y in entry.extra.get("years_written") or []} | {int(year)})
    return replace(entry, extra={**entry.extra, "years_written": written})


# ──────────────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────────────

# Fonction principale de redressement BACI multi-millésimes
def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point for the multi-vintage BACI reconstruction script.

    Reads every parameter from the ``baci`` block of the Kedro parameters
    (environment ``KEDRO_ENV``), including the HS vintages to reconstruct
    (``CLASSIFICATIONS.TARGETS``, optionally restricted by ``--targets`` or
    ``BACI_TARGETS``). Each target is processed independently, pass by pass on
    yearly chunks (``run_baci_passes``): a failure on one vintage is logged and
    does not prevent the others from running, but the script exits with an
    error once every target has been attempted if at least one failed.

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` when ``None``).

    Raises:
        RuntimeError: If at least one vintage failed, once every vintage has
            been attempted.
        ValueError: If a requested target is not configured.
    """
    # Chargement des configurations (chemins, identifiants et paramètres méthodologiques)
    comtrade_config = load_comtrade_config()
    baci_config = load_baci_config()
    runtime_config = load_runtime_config()

    # Construction des paramètres de modélisation
    baci_parameters_config = baci_config_from_params(baci_config.get("PARAMETERS"))
    # Activation de l'étape de réallocation des zones "Areas NES" (clé racine,
    # distincte de PARAMETERS.nes_partner_codes qui ne fait que déclarer les
    # codes éligibles)
    baci_parameters_config = replace(
        baci_parameters_config, apply_nes=bool(baci_config.get("APPLY_NES", True))
    )
    schema = baci_parameters_config.schema

    # Configuration du suivi d'exécution : sans URI (MLFLOW_TRACKING_URI non définie,
    # ou sans MLflow installé, ou serveur injoignable), get_tracker retourne un
    # tracker inerte et l'exécution est strictement inchangée
    tracking_config = baci_config.get("TRACKING") or {}
    log_artifacts = bool(tracking_config.get("LOG_ARTIFACTS", True))
    experiment = experiment_name("baci")

    # Configuration des millésimes cibles (restreints par --targets / BACI_TARGETS)
    # et du cache de correspondance
    classifications_config = baci_config["CLASSIFICATIONS"]
    targets_config = select_targets(classifications_config["TARGETS"], requested_targets(argv))
    concordance_path = classifications_config["CONCORDANCE_PATH"]
    force_refresh = classifications_config.get("FORCE_REFRESH", False)
    bucket = baci_config["BUCKET"]
    # Traitement par passes : fichiers de travail et taille maximale d'une tranche
    passes_config = baci_config.get("PASSES") or {}
    work_path = passes_config.get("WORK_PATH", "trade/work/baci")
    max_rows_per_chunk = passes_config.get("MAX_ROWS_PER_CHUNK")
    keep_work_files = bool(passes_config.get("KEEP_WORK_FILES", False))
    # Premières années des millésimes cibles (START_YEAR nul → règle PD-08)
    start_years = resolve_target_start_years(targets_config, runtime_config)
    # Borne haute optionnelle du périmètre temporel
    period_end = baci_parameters_config.period_end

    # Spécification du dataflow téléchargé auquel on souhaite appliquer la méthodologie BACI
    DATAFLOW = comtrade_config["DATAFLOW"]
    downloads_config = comtrade_config["DOWNLOADS"][DATAFLOW]
    completeness_config = baci_config.get("COMPLETENESS") or {}

    # Instant de référence capturé avant le traitement : la date consignée
    # correspond au début du redressement, jamais à sa fin, pour ne pas masquer
    # une mise à jour COMTRADE survenue pendant l'exécution
    processed_at = datetime.now(timezone.utc)

    # Porte de complétude (PS-14.1) : liste PLANIFIÉE reconstruite par la même
    # fonction que le téléchargement, confrontée au registre de téléchargement
    comtrade_client = ComtradeClient(subscription_key=os.environ.get(_SUBSCRIPTION_KEY_ENV))
    try:
        dims_codes = {
            "reporters": fetch_dimension_codelists("reporter", client=comtrade_client),
            "products": fetch_dimension_codelists("cmd:HS", client=comtrade_client),
        }
        planned = plan_queries(comtrade_config, runtime_config, comtrade_client, dims_codes)
    finally:
        comtrade_client.close()
    registry_view = DownloadRegistryView(
        downloads_config["PATHS"]["LAST_DOWNLOAD_PATH"],
        bucket=downloads_config["BUCKET"],
        dataflow=DATAFLOW,
    )
    batches = registry_view.batches_by_year()
    shares = completeness_by_year(planned, batches)
    years_eligible = list(
        eligible_years(
            batches,
            planned,
            float(completeness_config.get("MIN_SHARE", 1.0)),
            int(runtime_config["ANALYSIS_START_YEAR"]["comtrade"]),
            period_end=period_end,
        )
    )
    # Étiquette provisoire : périmètre produit planifié restreint (profil demo)
    products_filters = comtrade_config["split_filters"][DATAFLOW]["products"]
    # (produits non restreints → None dans les requêtes : périmètre complet)
    is_provisional = not any(q.products is None for q in planned) and is_provisional_scope(
        available_products=dims_codes["products"]["code"],
        planned_products={str(p) for q in planned for p in q.products},
        full_product_regex=completeness_config.get("FULL_PRODUCT_REGEX", r"^\d{6}$"),
        exclude=products_filters.get("exclude"),
    )
    # Logging
    logger.info(
        "Porte de complétude : %d année(s) éligible(s) sur %d planifiée(s) %s "
        "(seuil %s) ; périmètre provisoire : %s",
        len(years_eligible), len(shares), years_eligible,
        completeness_config.get("MIN_SHARE", 1.0), is_provisional,
    )

    # Sortie anticipée : aucune année complète (rattrapage en cours)
    if not years_eligible:
        logger.info("Aucune année complète : aucun millésime n'est redressé.")
        return

    # Fraîcheur : un fragment de registre par millésime, l'unité étant le
    # millésime entier (ses paramètres sont estimés sur toutes ses années).
    # Périmètre et watermark amont (dernier téléchargement des lots de ses années)
    registry = baci_registry(baci_config)
    requested = baci_requested(baci_parameters_config)
    force = ForceSpec.from_runtime(runtime_config)
    scopes = vintage_scopes(years_eligible, start_years)
    watermarks = {label: vintage_watermark(batches, years) for label, years in scopes.items()}
    plans = plan_baci_vintages(
        registry, scopes, watermarks, requested, force,
        baci_config.get("REFRESH"), processed_at,
        adopt_legacy_fingerprints=adopt_legacy_flag(baci_config.get("STATE")),
    )
    plans_by_label = {unit.get("vintage"): plan for unit, plan in plans.items()}
    # Logging
    logger.info(
        "Millésimes à redresser : %s",
        {label: plan.reason for label, plan in plans_by_label.items()} or "aucun",
    )

    # Sortie anticipée : aucun millésime périmé (entrées v1 adoptées écrites malgré tout)
    if not plans:
        registry.save()
        logger.info("Aucun millésime à redresser.")
        return
    # Millésimes redressés par cette exécution, dans l'ordre de la configuration
    targets_planned = {
        label: target_cfg
        for label, target_cfg in targets_config.items()
        if label in plans_by_label
    }

    # Lecture des fichiers Excel CEPII
    table_loader = TableLoader()
    table_saver = TableSaver()
    df_dist = table_loader.load(baci_config['PATHS']["DIST_CEPII"], bucket=bucket)
    df_geo = table_loader.load(baci_config['PATHS']["GEO_CEPII"], bucket=bucket)

    # Initialisation du connecteur au catalogue (schéma par défaut : la source,
    # les schémas résultat étant adressés explicitement à l'écriture)
    connector = build_connector(
        DuckLakeLocation(
            dbname=comtrade_config["DOWNLOADS"]["DBNAME"],
            catalog_alias=comtrade_config["DOWNLOADS"]["CATALOG_ALIAS"],
            schema=_schema_name(DATAFLOW),
            bucket=downloads_config["BUCKET"],
            data_path=downloads_config["PATHS"]["DATA_PATH"],
        ),
        pg=pg_credentials_from_env(),
        s3=s3_credentials_from_env(),
    )
    source_schema = _schema_name(DATAFLOW)

    # Etablissement d'une connexion
    conn = connector.connect()
    try:
        # Nomenclatures présentes par année (SELECT DISTINCT, jamais la table
        # entière) et paires de correspondance nécessaires aux millésimes planifiés
        classifications = classifications_by_year(
            conn,
            source_schema,
            years=sorted({y for label in targets_planned for y in scopes.get(label, [])}),
            classification_col=schema.classification_col,
            period_col=schema.period_col,
        )
        pairs = concordance_pairs(
            classifications, {label: scopes.get(label, []) for label in targets_planned}
        )

        # Résolution des tables de correspondance nécessaires (cache Parquet
        # partagé avec l'étape partenaires, client UNSD créé à la demande)
        concordances = prepare_concordances(
            pairs,
            client_factory=UNSDClient,
            loader=table_loader,
            saver=table_saver,
            concordance_path=concordance_path,
            bucket=bucket,
            force_refresh=force_refresh,
        )

        # Référentiels de nomenclature (tables de passage du cache UNSD, millésimes),
        # publiés dans le catalogue Comtrade (PS-28.4) ; non bloquant
        reference = publish_hs_reference(
            concordances,
            connector,
            params={
                "SCHEMA_PREFIX": comtrade_config["DOWNLOADS"]["REFERENCE"]["SCHEMA_PREFIX"],
                "NOMENCLATURES": runtime_config["NOMENCLATURES"]["HS"],
            },
            conn=conn,
        )
        logger.info(f"Référentiels SH : {reference['rows']} ; échecs : {reference['failures']}")

        # Redressement par passes, millésime par millésime. L'échec d'un
        # millésime n'interrompt pas les autres.
        reports: Dict[str, BaciReport] = {}
        failures: Dict[str, Exception] = {}
        for label, target_cfg in targets_planned.items():
            # Millésime sans année complète (rattrapage année-majeur en cours) :
            # rien à redresser, ce n'est pas un échec
            if not scopes.get(label):
                logger.info(
                    "Millésime %s : aucune année éligible >= %d, ignoré",
                    label, start_years[label],
                )
                continue
            # Un run par millésime : l'échec de l'un n'emporte pas les autres
            node = f"process_baci_{label}"
            tracker = CapturingTracker(
                get_tracker(
                    tracking_uri=None,
                    experiment=experiment,
                    run_name=run_name(f"baci-{label}-{datetime.now():%Y%m%d-%H%M}", node),
                    tags={"vintage": label, "is_provisional": str(is_provisional)},
                )
            )
            scope = RunScope(node, step="préparation de la passe")
            # Plan du millésime et identifiant de la passe d'estimation
            unit, plan = baci_unit(label), plans_by_label[label]
            fit_id = compute_fit_id(label, scopes[label], watermarks[label], requested)
            result_schema = _schema_name(target_cfg["RESULT_SCHEMA"])
            try:
                with tracker, guarded_run(scope, tracker):
                    # Point de reprise : entrée « démarrée » (years_written vide)
                    # écrite avant toute écriture de table, le dernier calcul
                    # réussi restant celui que lit l'étape réseau
                    registry.upsert(started_entry(registry.get(unit), unit, plan, fit_id, scopes[label]))
                    registry.save()
                    # Fraîcheur : décision du millésime et tag de forçage
                    tracker.log_metrics(plan_metrics({unit: plan}, n_candidates=len(scopes)))
                    tracker.set_tags({"fit_id": fit_id, "freshness_reason": plan.reason})
                    if force.forces_step(STEP, requested):
                        tracker.set_tags({"forced": force.describe()})

                    # Couverture du millésime : années éligibles et part minimale
                    # de lots téléchargés sur ses années planifiées
                    n_years_eligible = sum(y >= start_years[label] for y in years_eligible)
                    tracker.log_metrics(
                        coverage_metrics(years_eligible, shares, start_years[label], period_end)
                    )

                    # Harmonisation de chaque tranche vers le millésime cible
                    def harmonizer_factory(label: str = label) -> HsHarmonizer:
                        return HsHarmonizer(
                            concordances,
                            target_vintage=label,
                            classification_col=schema.classification_col,
                            product_col=schema.product_col,
                            period_col=schema.period_col,
                            value_cols=(schema.value_col, schema.cif_value_col, schema.fob_value_col),
                            weight_cols=(schema.netwgt_col,),
                            qty_col=schema.qty_col,
                            qty_unit_col=schema.qty_unit_col,
                        )

                    # Point de reprise mis à jour après chaque année écrite
                    def on_year_written(year: int, rows: int, unit: Unit = unit) -> None:
                        registry.upsert(with_year_written(registry.get(unit), year))
                        registry.save()

                    writer = DuckLakeYearWriter(
                        conn,
                        catalog_alias=connector.catalog_alias,
                        schema=result_schema,
                        primary_keys=baci_parameters_config.primary_keys,
                        columns={"fit_id": fit_id, "is_provisional": bool(is_provisional)},
                        run_id=os.environ.get("WORKFLOW_ID") or None,
                        commit_message=f"process_baci_hs {label}",
                        on_year_written=on_year_written,
                    )
                    sources = sorted({c for y in scopes[label] for c in classifications.get(y, [])})
                    io = DuckDBPassIO(
                        conn,
                        source_schema=source_schema,
                        years=scopes[label],
                        root=work_root(work_path, label, fit_id, bucket),
                        writer=writer,
                        period_col=schema.period_col,
                        product_col=schema.product_col,
                        harmonizer_factory=harmonizer_factory,
                        links=chapter_links(concordances, sources, label),
                        max_rows_per_chunk=max_rows_per_chunk,
                    )

                    # Redressement par passes, écriture année par année
                    scope.step = "redressement BACI"
                    report, rows_by_year = run_baci_passes(
                        io,
                        df_dist,
                        df_geo,
                        config=baci_parameters_config,
                        tracker=tracker,
                        log_artifacts=log_artifacts,
                        memory_probe=peak_memory_mb,
                    )
                    report.created = writer.created
                    reports[label] = report

                    # Registre du millésime : toutes les années du périmètre sont
                    # écrites, entrée terminée écrite après succès seulement — une
                    # date avancée à tort ferait sauter le recalcul des
                    # vulnérabilités de réseau. Un fragment par millésime : aucune
                    # course entre pods de millésimes différents
                    scope.step = "écriture du résultat"
                    registry.upsert(
                        completed_entry(
                            unit, plan, fit_id, scopes[label], processed_at,
                            watermarks[label], requested,
                            result_schema=result_schema,
                            n_rows=int(report.flows),
                            is_provisional=bool(is_provisional),
                        )
                    )
                    registry.save()
                    # Fichiers de travail supprimés en fin de passe réussie
                    if not keep_work_files:
                        io.cleanup()

                    # Envoi des métriques du redressement et de l'harmonisation
                    # (noms séparés par « / » : l'interface MLflow les regroupe par section)
                    tracker.log_metrics(rekey_metrics(report.to_metrics()))
                    harmonization = io.harmonization_report()
                    if harmonization is not None:
                        tracker.log_metrics(rekey_metrics(harmonization.to_metrics()))
                    tracker.set_tags({"result_schema": result_schema, "created": str(report.created)})
                    # Répartition des relations de nomenclature : mesure de la
                    # perte d'information à la conversion
                    if log_artifacts and harmonization is not None:
                        tracker.log_dict(
                            harmonization.relationship_distribution,
                            "classification/relationship_distribution.json",
                        )

                    # Rapport de run : contrôles, chiffres clés, sections par étape BACI.
                    # Il est construit sur les métriques et tables déjà produites, sans
                    # relecture de données, et publié avant toute sortie en erreur
                    scope.step = "rapport de run"
                    coefficients = tracker.dicts.get("gravity/coefficients.json", {})
                    gravity_table = pd.DataFrame(
                        {
                            "coefficient": coefficients.get("coefficients", {}),
                            "std_error": coefficients.get("std_errors", {}),
                        }
                    ).rename_axis("variable").reset_index()
                    artifacts = {**tracker.tables, "output/rows_by_year.csv": rows_by_year}
                    run_report = scope.build(
                        metrics=tracker.metrics,
                        units=Units(
                            planned=1,
                            succeeded=1,
                            failed=0,
                            planned_label=f"1 millésime ({n_years_eligible} années éligibles)",
                        ),
                        key_figures=key_figures_baci,
                        sections=lambda m: sections_baci(m, artifacts),
                        tables={
                            "conversion_rates": tracker.tables.get("tonnage/conversion_rates.csv", pd.DataFrame()),
                            "gravity_coefficients": gravity_table,
                            "sigma_by_country": tracker.tables.get("quality/sigma_by_country.csv", pd.DataFrame()),
                            "rows_by_year": rows_by_year,
                        },
                    )
                    scope.publish(tracker, run_report)

                # Logging
                logger.info("Redressement BACI terminé pour %s : %s", label, report)
            except Exception as exc:
                # Journalisation de l'échec, poursuite avec les autres millésimes
                logger.exception("Échec du redressement BACI pour le millésime %s", label)
                failures[label] = exc
    finally:
        conn.close()

    # Échec global si au moins un millésime a échoué, une fois tous tentés
    if failures:
        raise RuntimeError(
            f"{len(failures)} millésime(s) en échec sur {len(targets_planned)} : "
            f"{sorted(failures)}"
        ) from next(iter(failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
