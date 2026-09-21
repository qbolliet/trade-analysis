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
``UNSDClient`` puis mises en cache côté script (Parquet + registre JSON) :
elles sont invariantes une fois publiées, l'absence de fichier en cache est
donc le seul déclencheur de téléchargement (pas de vérification de fraîcheur
distante), sauf ``FORCE_REFRESH`` explicite en configuration.

L'échec d'un millésime n'interrompt pas les autres : chaque échec est capturé
et journalisé individuellement, et le script ne sort en erreur qu'en fin de
parcours si au moins un millésime a échoué.

Chaque millésime effectivement réécrit voit sa date de traitement consignée dans
le registre JSON ``PATHS.LAST_PROCESSING_PATH`` (même principe que le
``LAST_DOWNLOAD_PATH`` du téléchargement). C'est la seule chose que
``scripts/compute_network_vulnerabilities.py`` lit de ce script : le couplage
reste faible, aucun état en mémoire n'étant partagé.

Périmètre borné avant tout calcul (PS-14.1, en attendant le traitement par
passes de K-07) :

- **porte de complétude** : le registre de téléchargement Comtrade
  (``LAST_DOWNLOAD_PATH``) est confronté à la liste des requêtes PLANIFIÉES,
  reconstruite par la même fonction que le script de téléchargement
  (``scripts.download_comtrade.plan_queries``) ; seules les années dont la part
  de lots téléchargés au moins une fois atteint ``COMPLETENESS.MIN_SHARE`` sont
  redressées ;
- **lecture poussée en SQL** : seules ces années, bornées par le plus petit
  ``START_YEAR`` des cibles et ``PARAMETERS.period_end``, sont lues ;
- **étiquette ``is_provisional``** : vraie quand le périmètre produit planifié
  n'est qu'un sous-ensemble strict du périmètre HS6 complet (profil ``demo``,
  PD-06 point 3).

Fichiers de configuration lus : ``BACI_CONFIG_PATH``, ``COMTRADE_CONFIG_PATH``
et ``RUNTIME_CONFIG_PATH``.
"""
# Importation des modules
# Modules de base
import hashlib
import logging
import os
from pathlib import Path
from dataclasses import fields, replace
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
import yaml

# Modules de manipulation de données
from botocore.exceptions import ClientError
import duckdb
import pandas as pd

# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
)
from kedro_pipeline.steps.reference import publish_hs_reference
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
from statflows.storage.ducklake.tables import FACT_TABLE as _FACT_TABLE, write_dataframe
from statflows.storage.json import Loader as JsonLoader, Saver as JsonSaver
from statflows.core.download import _schema_name

# Module client des tables de correspondance de nomenclatures UNSD
from statflows import UNSDClient

# Module d'implémentation du traitement BACI
from macroforecast.trade.processing import required_columns, run_baci
from macroforecast.trade.processing import BaciConfig, ComtradeSchema, DEFAULT_CONFIG, BaciReport
from macroforecast.trade.processing import HsHarmonizer, resolve_vintage
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

# Nom du fichier de registre des téléchargements de tables de correspondance
_REGISTRY_FILE = "unsd_correspondance_tables.json"

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
    """Load configuration from file.

    Args:
        config_path: Path to config file. If None, uses default location or
            the COMTRADE_CONFIG_PATH environment variable.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        # Priorité 1 : variable d'environnement (pour flexibilité Kubernetes)
        config_path = os.environ.get("COMTRADE_CONFIG_PATH", "config/datasets/comtrade.yaml")

    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de chargement de la configuration
def load_baci_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load configuration from file.

    Args:
        config_path: Path to config file. If None, uses default location or
            the BACI_CONFIG_PATH environment variable.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        # Priorité 1 : variable d'environnement (pour flexibilité Kubernetes)
        config_path = os.environ.get("BACI_CONFIG_PATH", "config/baci.yaml")

    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


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

# Racine du registre de téléchargement statflows
_DOWNLOAD_REGISTRY_ROOT = "DOWNLOADS"


# Fonction de chargement du registre de téléchargement Comtrade
def load_download_registry(
    last_download_path: os.PathLike, bucket: Optional[str]
) -> Dict[str, Dict[str, Any]]:
    """Load the ``statflows`` download registry (empty when absent).

    Args:
        last_download_path: Registry path (``DOWNLOADS.<dataflow>.PATHS.LAST_DOWNLOAD_PATH``).
        bucket: S3 bucket, or ``None`` for a local file.

    Returns:
        Mapping ``identity_key -> {agency, dataflow, params, last_download}``.
    """
    data = JsonLoader().load(Path(last_download_path), bucket=bucket, missing_ok=True) or {}
    return data.get(_DOWNLOAD_REGISTRY_ROOT, {})


# Fonction de normalisation d'une sélection de produits
def _products_key(products: Any) -> Optional[Tuple[str, ...]]:
    """Normalise a product selection (list, scalar or ``None``) into a tuple key."""
    if products is None:
        return None
    if isinstance(products, (list, tuple)):
        return tuple(str(p) for p in products)
    return tuple(str(products).split(","))


# Fonction d'extraction de l'année d'une période
def _period_year(period: Any) -> int:
    """Return the year of a Comtrade period (``YYYY`` or ``YYYYMM``)."""
    return int(str(period)[:4])


# Fonction de calcul de la part des lots téléchargés par année
def completeness_by_year(
    planned: Iterable[Any],
    registry: Mapping[str, Mapping[str, Any]],
    dataflow: str,
) -> Dict[int, float]:
    """Share, per year, of the planned product batches downloaded at least once.

    A planned query (one period x one product batch) counts as downloaded when
    the registry holds an entry of the same dataflow with the same
    ``params.periods`` and ``params.products`` and a ``last_download`` date.
    Matching on these parameters, rather than on the registry key, keeps the
    gate independent of the physical registry layout (PS-12.3).

    Args:
        planned: Planned ``ComtradeQueryRequest`` objects (``periods``,
            ``products``), as built by ``plan_queries``.
        registry: Download registry (``identity_key -> entry``).
        dataflow: Dataflow of the planned queries.

    Returns:
        Mapping ``year -> share`` in ``[0, 1]``, for every planned year.

    Examples:
        >>> completeness_by_year(planned, registry, "C_A_HS")  # doctest: +SKIP
        {2024: 1.0, 2023: 0.5}
    """
    # Lots téléchargés au moins une fois : (période, produits)
    downloaded: Set[Tuple[str, Optional[Tuple[str, ...]]]] = set()
    for entry in registry.values():
        params = entry.get("params") or {}
        if entry.get("dataflow") != dataflow or not entry.get("last_download"):
            continue
        if params.get("periods") is None:
            continue
        downloaded.add((str(params["periods"]), _products_key(params.get("products"))))

    # Décompte des lots planifiés et téléchargés, par année
    totals: Dict[int, int] = {}
    done: Dict[int, int] = {}
    for query in planned:
        year = _period_year(query.periods)
        totals[year] = totals.get(year, 0) + 1
        if (str(query.periods), _products_key(query.products)) in downloaded:
            done[year] = done.get(year, 0) + 1

    return {year: done.get(year, 0) / total for year, total in totals.items()}


# Fonction de sélection des années éligibles
def eligible_years(
    shares: Mapping[int, float],
    min_share: float,
    period_end: Optional[int] = None,
) -> List[int]:
    """Years whose share of downloaded batches reaches ``min_share``.

    Args:
        shares: Mapping ``year -> share`` (:func:`completeness_by_year`).
        min_share: Completeness threshold (``COMPLETENESS.MIN_SHARE``).
        period_end: Last year kept (``PARAMETERS.period_end``), ``None`` for none.

    Returns:
        Sorted eligible years.

    Examples:
        >>> eligible_years({2022: 1.0, 2023: 0.5, 2024: 1.0}, 1.0, period_end=2023)
        [2022]
    """
    return sorted(
        year
        for year, share in shares.items()
        if share >= min_share and (period_end is None or year <= period_end)
    )


# Fonction de résolution des premières années des millésimes cibles
def resolve_target_start_years(
    targets: Mapping[str, Mapping[str, Any]],
    runtime_config: Mapping[str, Any],
) -> Dict[str, int]:
    """Resolve the first year of each BACI target vintage (PD-08).

    An explicit ``START_YEAR`` is kept; a null one resolves to
    ``max(runtime.NOMENCLATURES.HS[vintage], runtime.ANALYSIS_START_YEAR.comtrade)``.

    Args:
        targets: ``CLASSIFICATIONS.TARGETS`` of ``baci.yaml``.
        runtime_config: Parsed ``runtime`` mapping.

    Returns:
        Mapping ``vintage -> first year``.

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
        for label, cfg in targets.items()
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
        params: The ``PARAMETERS.SCHEMA`` mapping of ``config/baci.yaml`` (or
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
        params: The ``parameters`` mapping of ``config/baci.yaml`` (or ``None``).

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
# Cache des tables de correspondance HS (Parquet + registre JSON), logique
# propre à ce script : le package ne fait que convertir (HsHarmonizer), ni le
# téléchargement ni le cache n'y vivent.
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul d'une somme de contrôle du contenu d'une table
def _checksum(df_table: pd.DataFrame) -> str:
    """Compute a content hash of a table, to detect drift in a cached artefact.

    Args:
        df_table: Table to hash.

    Returns:
        Hex-encoded SHA-256 digest of the table's content.
    """
    hashed = pd.util.hash_pandas_object(df_table, index=False)
    return hashlib.sha256(hashed.to_numpy().tobytes()).hexdigest()


# Fonction de construction du chemin de cache d'une paire de millésimes
def _cached_table_path(concordance_path: str, source: str, target: str) -> str:
    """Build the Parquet cache path of a source/target vintage pair.

    Args:
        concordance_path: Root directory of the concordance cache.
        source: Source classification (e.g. ``"HS2022"``).
        target: Target classification (e.g. ``"HS2017"``).

    Returns:
        The Parquet cache path, e.g. ``"{concordance_path}/HS2022-HS2017.parquet"``.
    """
    return f"{concordance_path.rstrip('/')}/{source}-{target}.parquet"


# Fonction de construction du chemin du registre des téléchargements
def _registry_path(concordance_path: str) -> str:
    """Build the JSON registry path of the concordance cache.

    Args:
        concordance_path: Root directory of the concordance cache.

    Returns:
        The registry path, e.g. ``"{concordance_path}/registry.json"``.
    """
    return f"{concordance_path.rstrip('/')}/{_REGISTRY_FILE}"


# Fonction de lecture non bloquante d'une table mise en cache
def _load_cached_table(
    loader: TableLoader, path: str, bucket: Optional[str]
) -> Optional[pd.DataFrame]:
    """Read a cached Parquet table, or ``None`` when it is absent.

    Args:
        loader: Table loader (local or S3, dispatched on ``bucket``).
        path: Cache path (local path or S3 key).
        bucket: S3 bucket name, or ``None`` for a local cache.

    Returns:
        The cached table, or ``None`` when no cache file exists yet.
    """
    try:
        return loader.load(path, bucket=bucket)
    except (FileNotFoundError, ClientError):
        return None


# Fonction de chargement des tables de correspondance nécessaires, avec cache
def _ensure_concordances(
    pairs: Sequence[Tuple[str, str]],
    client: UNSDClient,
    loader: TableLoader,
    saver: TableSaver,
    concordance_path: str,
    bucket: Optional[str],
    force_refresh: bool = False,
) -> Dict[Tuple[str, str], pd.DataFrame]:
    """Load cached HS concordance tables, downloading only the missing ones.

    The correspondence tables are invariant once UNSD publishes them: the
    absence of a cached Parquet file is the only trigger for a download (no
    remote freshness check), unless ``force_refresh`` is set. One normalised
    table is cached per pair (``{concordance_path}/{source}-{target}.parquet``),
    alongside a JSON registry (``{concordance_path}/registry.json``) recording
    the download date, source URL, row count and content checksum of each pair
    actually downloaded — same principle as ``LAST_DOWNLOAD_PATH`` in
    ``download_comtrade.py``.

    Args:
        pairs: Distinct ``(source, target)`` vintage pairs to resolve (UNSD
            identifiers, e.g. ``("HS2022", "HS2017")``).
        client: UNSD correspondence-table client.
        loader: Table loader (local or S3) used to read cached Parquet tables.
        saver: Table saver (local or S3) used to write cached Parquet tables.
        concordance_path: Root directory of the concordance cache.
        bucket: S3 bucket name, or ``None`` for a local cache.
        force_refresh: When ``True``, re-download every pair even if already
            cached.

    Returns:
        Mapping ``(source, target) -> normalised concordance table``, one
        entry per requested pair.
    """
    # Registre des téléchargements (date, URL, volumétrie, somme de contrôle)
    registry_path = _registry_path(concordance_path)
    # Instances réutilisables : la connexion S3 paresseuse est ainsi établie une
    # seule fois et partagée par la lecture initiale et les écritures successives
    json_saver = JsonSaver()
    # Registre absent : premier téléchargement des tables de correspondance
    registry = JsonLoader().load(registry_path, bucket=bucket, missing_ok=True) or {}

    # Catalogue des tables déclarées (URL source de chaque paire)
    df_catalogue = client.list_available_tables().set_index(
        ["source_classification", "target_classification"]
    )

    concordances: Dict[Tuple[str, str], pd.DataFrame] = {}
    for source, target in pairs:
        key = f"{source}-{target}"
        table_path = _cached_table_path(concordance_path, source, target)

        # Cache existant : aucune vérification de fraîcheur distante
        df_table = None if force_refresh else _load_cached_table(loader, table_path, bucket)

        if df_table is None:
            # Logging
            logger.info("Téléchargement de la table de correspondance %s", key)
            df_table = client.get_correspondence(source, target, kind="conversion")
            saver.save(table_path, df_table, bucket=bucket, index=False)

            # Mise à jour du registre uniquement pour les paires téléchargées
            registry[key] = {
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "source_url": str(df_catalogue.loc[(source, target), "url"]),
                "n_rows": int(len(df_table)),
                "checksum": _checksum(df_table),
            }
            json_saver.save(
                registry_path, registry, bucket=bucket, indent=2, ensure_ascii=False
            )

        concordances[(source, target)] = df_table

    return concordances


# ──────────────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────────────

# Fonction principale de redressement BACI multi-millésimes
def main() -> None:
    """CLI entry point for the multi-vintage BACI reconstruction script.

    Reads every parameter from the YAML configuration (``BACI_CONFIG_PATH`` or
    the default ``config/baci.yaml``), including the HS vintages to reconstruct
    (``CLASSIFICATIONS.TARGETS``). Each target is processed independently: a
    failure on one vintage is logged and does not prevent the others from
    running, but the script exits with an error once every target has been
    attempted if at least one failed.

    Raises:
        RuntimeError: If at least one vintage failed, once every vintage has
            been attempted.
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

    # Configuration du suivi d'exécution : sans URI (ou sans MLflow installé,
    # ou serveur injoignable), get_tracker retourne un tracker inerte et
    # l'exécution est strictement inchangée
    mlflow_config = baci_config.get("MLFLOW") or {}
    log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))

    # Configuration des millésimes cibles et du cache de correspondance
    classifications_config = baci_config["CLASSIFICATIONS"]
    targets_config: Dict[str, Dict] = classifications_config["TARGETS"]
    concordance_path = classifications_config["CONCORDANCE_PATH"]
    force_refresh = classifications_config.get("FORCE_REFRESH", False)
    bucket = baci_config["BUCKET"]
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
    registry = load_download_registry(
        downloads_config["PATHS"]["LAST_DOWNLOAD_PATH"], bucket=downloads_config["BUCKET"]
    )
    shares = completeness_by_year(planned, registry, DATAFLOW)
    years_eligible = eligible_years(
        shares, float(completeness_config.get("MIN_SHARE", 1.0)), period_end=period_end
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

    # Etablissement d'une connexion
    conn = connector.connect()
    try:
        # Lecture de la table de faits COMTRADE restreinte en SQL aux années
        # éligibles, bornées par le plus petit START_YEAR des cibles et
        # period_end : colonnes requises par run_baci plus la colonne de
        # classification (absente de required_columns, indispensable à
        # l'harmonisation des nomenclatures). Le monobloc reste en mémoire
        # jusqu'au traitement par passes (K-07)
        columns = list(
            dict.fromkeys(required_columns(baci_parameters_config) + [schema.classification_col])
        )
        df_comtrade = _read_comtrade_fact_table(
            conn=conn,
            source_schema=_schema_name(DATAFLOW),
            columns=columns,
            period_col=schema.period_col,
            years=years_eligible,
            period_start=min(start_years.values()),
            period_end=period_end,
        )
        years = df_comtrade[schema.period_col].astype(str).str[:4].astype(int)

        # Passe 1 : tranche temporelle par millésime cible et paires de
        # correspondance nécessaires (millésimes présents dans chaque tranche,
        # hors le millésime cible lui-même)
        slices: Dict[str, pd.DataFrame] = {}
        pairs: Set[Tuple[str, str]] = set()
        for label, target_cfg in targets_config.items():
            df_slice = df_comtrade[years >= start_years[label]]
            slices[label] = df_slice
            codes_present = df_slice[schema.classification_col].dropna().unique()
            for code in codes_present:
                source_label = f"HS{resolve_vintage(code)}"
                if source_label != label:
                    pairs.add((source_label, label))

        # Résolution des tables de correspondance nécessaires (cache Parquet)
        client = UNSDClient()
        try:
            concordances = _ensure_concordances(
                sorted(pairs),
                client=client,
                loader=table_loader,
                saver=table_saver,
                concordance_path=concordance_path,
                bucket=bucket,
                force_refresh=force_refresh,
            )
        finally:
            client.close()

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

        # Passe 2 : harmonisation puis redressement BACI, par millésime cible.
        # L'échec d'un millésime n'interrompt pas les autres.
        reports: Dict[str, BaciReport] = {}
        failures: Dict[str, Exception] = {}
        # Entrées de registre des millésimes effectivement réécrits
        processed: Dict[str, Dict[str, object]] = {}
        for label, target_cfg in targets_config.items():
            # Millésime sans année complète (rattrapage année-majeur en cours) :
            # rien à redresser, ce n'est pas un échec
            if slices[label].empty:
                logger.info(
                    "Millésime %s : aucune année éligible >= %d, ignoré",
                    label, start_years[label],
                )
                continue
            # Un run par millésime : l'échec de l'un n'emporte pas les autres. Ouvert avant
            # l'harmonisation pour qu'un échec de celle-ci porte lui aussi son rapport
            node = f"process_baci_{label}"
            tracker = CapturingTracker(
                get_tracker(
                    tracking_uri=mlflow_config.get("TRACKING_URI"),
                    experiment=mlflow_config.get("EXPERIMENT", "trade-02-baci"),
                    run_name=run_name(f"baci-{label}-{datetime.now():%Y%m%d-%H%M}", node),
                    tags={"vintage": label, "is_provisional": str(is_provisional)},
                )
            )
            scope = RunScope(node, step="harmonisation de la nomenclature")
            try:
                with tracker, guarded_run(scope, tracker):
                    harmonizer = HsHarmonizer(
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
                    df_harmonised = harmonizer.fit_transform(slices[label])

                    # Couverture du millésime : années éligibles et part minimale
                    # de lots téléchargés sur ses années planifiées
                    n_years_eligible = sum(y >= start_years[label] for y in years_eligible)
                    tracker.log_metrics(
                        coverage_metrics(years_eligible, shares, start_years[label], period_end)
                    )

                    # Application de la méthodologie sur la tranche harmonisée
                    scope.step = "redressement BACI"
                    df_reconciled, report = run_baci(
                        df_comtrade=df_harmonised,
                        df_dist=df_dist,
                        df_geo=df_geo,
                        config=baci_parameters_config,
                        tracker=tracker,
                        log_artifacts=log_artifacts,
                    )
                    # Étiquette du périmètre (PD-06) : BACI sur un sous-ensemble
                    # de produits, distinct du BACI complet
                    df_reconciled["is_provisional"] = is_provisional

                    # Écriture du résultat dans le schéma dédié au millésime
                    scope.step = "écriture du résultat"
                    report.created = write_dataframe(
                        conn,
                        df_reconciled,
                        baci_parameters_config.primary_keys,
                        catalog_alias=connector.catalog_alias,
                        schema=_schema_name(target_cfg["RESULT_SCHEMA"]),
                        label=label,
                    )
                    reports[label] = report
                    # Traçage du millésime réécrit, indexé par son schéma
                    # résultat — identité non ambiguë de ce qui a été produit
                    result_schema = _schema_name(target_cfg["RESULT_SCHEMA"])
                    processed[result_schema] = {
                        "vintage": label,
                        "result_schema": result_schema,
                        "last_processed": processed_at.isoformat(),
                        "n_rows": int(len(df_reconciled)),
                    }

                    # Envoi des métriques du redressement et de l'harmonisation
                    # (noms séparés par « / » : l'interface MLflow les regroupe par section)
                    tracker.log_metrics(rekey_metrics(report.to_metrics()))
                    tracker.log_metrics(rekey_metrics(harmonizer.report_.to_metrics()))
                    tracker.set_tags(
                        {
                            "result_schema": _schema_name(target_cfg["RESULT_SCHEMA"]),
                            "created": str(report.created),
                        }
                    )
                    # Répartition des relations de nomenclature : mesure de la
                    # perte d'information à la conversion
                    if log_artifacts:
                        tracker.log_dict(
                            harmonizer.report_.relationship_distribution,
                            "classification/relationship_distribution.json",
                        )

                    # Rapport de run : contrôles, chiffres clés, sections par étape BACI.
                    # Il est construit sur les métriques et tables déjà produites, sans
                    # relecture de données, et publié avant toute sortie en erreur
                    scope.step = "rapport de run"
                    rows_by_year = (
                        df_reconciled[schema.period_col].value_counts().sort_index()
                        .rename_axis("year").rename("rows").reset_index()
                        if schema.period_col in df_reconciled else pd.DataFrame()
                    )
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

    # Registre des dates de traitement : écrit après succès de l'écriture des
    # millésimes concernés, jamais avant — une date avancée à tort ferait
    # silencieusement sauter le recalcul des vulnérabilités de réseau.
    # Écriture fusionnée unique en fin de script : sans course entre millésimes
    # tant qu'ils sont traités séquentiellement dans ce pod. Le fan-out d'un pod
    # par millésime (PD-05) imposera un fragment de registre par millésime (PD-10)
    if processed:
        last_processing_path = Path(baci_config["PATHS"]["LAST_PROCESSING_PATH"])
        # Fusion avec le registre existant : seuls les millésimes traités bougent
        registry = (
            JsonLoader().load(last_processing_path, bucket=bucket, missing_ok=True) or {}
        ).get(_PROCESSING_ROOT, {})
        registry.update(processed)
        # Écriture du registre mis à jour
        JsonSaver().save(
            last_processing_path,
            {_PROCESSING_ROOT: registry},
            bucket=bucket,
            indent=2,
            ensure_ascii=False,
        )
        # Logging
        logger.info(
            "%d millésime(s) consigné(s) dans le registre de traitement '%s'",
            len(processed),
            baci_config["PATHS"]["LAST_PROCESSING_PATH"],
        )

    # Échec global si au moins un millésime a échoué, une fois tous tentés
    if failures:
        raise RuntimeError(
            f"{len(failures)} millésime(s) en échec sur {len(targets_config)} : "
            f"{sorted(failures)}"
        ) from next(iter(failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
