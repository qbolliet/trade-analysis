"""BACI step: completeness gate, HS correspondence tables, chunked I/O of the passes.

Pure functions, I/O adapters and step functions shared by the BACI script and the
Kedro nodes. The methodology lives in
``macroforecast.trade.processing``; this module only moves data.

* **Completeness gate** — :func:`completeness_by_year` and :func:`eligible_years`:
  a year enters the BACI scope once its planned Comtrade batches have all been
  downloaded (a partial year would bias the estimations pooled over every year).
* **HS correspondence tables** — :func:`classifications_by_year` and
  :func:`concordance_pairs` list the pairs a vintage needs; :func:`prepare_concordances`
  loads them from the Parquet cache shared with the partner step, downloading
  only the missing ones. The UNSD tables convert the codes of a recent HS vintage
  into an older one and are invariant once published: the absence of a cached
  Parquet file is the only trigger for a download, unless a refresh is forced.
  One normalised table is cached per pair (``{path}/{source}-{target}.parquet``),
  next to a JSON registry recording, for every pair actually downloaded, the
  download date, source URL, row count and content checksum.
* **Chunked passes** — :class:`DuckDBPassIO` implements
  ``macroforecast.trade.processing.BaciPassIO``: Comtrade read one year at a time
  (``WHERE year = ?``, split into blocks of whole HS chapters when a year exceeds
  ``MAX_ROWS_PER_CHUNK``, see :func:`chapter_blocks`), harmonised to the vintage,
  intermediate mirror flows written as Parquet work files by DuckDB
  (``<WORK_PATH>/<vintage>/<fit_id>/{mirror,nes,freight}/year=<y>/block=<b>.parquet``)
  and read back with column projection, unit-value medians and freight-rate
  quantiles computed in SQL; :class:`DuckLakeYearWriter` persists the result one
  year per DuckLake transaction.
"""
# Importation des modules
# Modules de base
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
import hashlib
import logging
from pathlib import Path
import shutil
import sys
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Tuple

# Modules de manipulation de données
import duckdb
import pandas as pd

# Stockage JSON (registre du cache)
from statflows.storage.json import Loader as JsonLoader, Saver as JsonSaver
from statflows.core.factory import filter_codes
from statflows.storage.ducklake.tables import FACT_TABLE as _FACT_TABLE

# Modules du package
from kedro_pipeline.config import active_targets
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
from kedro_pipeline.io.ducklake import attached_catalog_alias
from kedro_pipeline.io.registry_views import ProductsKey
from kedro_pipeline.steps.result import (
    StepProgress,
    StepResult,
    UnitRuns,
    capturing,
    failure_message,
    mark,
    shared_runs,
)
from macroforecast.trade.methodology import methodology_params
from macroforecast.trade.processing import BACI_FINGERPRINT_EXCLUDED, BaciConfig
from macroforecast.trade.processing import DEFAULT_CONFIG  # noqa: F401  (exemples des docstrings)
from macroforecast.tracking import RunTracker

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom du fichier de registre des téléchargements de tables de correspondance
CONCORDANCE_REGISTRY_FILE = "unsd_correspondance_tables.json"


# Fonction de calcul d'une somme de contrôle du contenu d'une table
def table_checksum(df_table: pd.DataFrame) -> str:
    """Compute a content hash of a table, to detect drift in a cached artefact.

    Args:
        df_table: Table to hash.

    Returns:
        Hex-encoded SHA-256 digest of the table's content (index excluded).

    Examples:
        >>> len(table_checksum(pd.DataFrame({"a": [1, 2]})))
        64
    """
    hashed = pd.util.hash_pandas_object(df_table, index=False)
    return hashlib.sha256(hashed.to_numpy().tobytes()).hexdigest()


# Fonction de calcul d'une somme de contrôle d'un ensemble de tables
def concordances_checksum(concordances: Mapping[Tuple[str, str], pd.DataFrame]) -> str:
    """Compute one checksum over a set of correspondence tables.

    A corrected table published by UNSD changes this value, which is what makes
    the rows converted with it stale.

    Args:
        concordances: Mapping ``(source, target) -> table``.

    Returns:
        Hex-encoded SHA-256 digest of the sorted pair checksums.

    Examples:
        >>> table = pd.DataFrame({"source_code": ["010121"], "target_code": ["010121"]})
        >>> concordances_checksum({("HS2022", "HS2017"): table}) == concordances_checksum(
        ...     {("HS2022", "HS2017"): table.copy()})
        True
    """
    payload = "|".join(
        f"{source}-{target}:{table_checksum(df_table)}"
        for (source, target), df_table in sorted(concordances.items())
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# Fonction de construction du chemin de cache d'une paire de millésimes
def cached_table_path(concordance_path: str, source: str, target: str) -> str:
    """Build the Parquet cache path of a source/target vintage pair.

    Args:
        concordance_path: Root directory of the concordance cache.
        source: Source classification (e.g. ``"HS2022"``).
        target: Target classification (e.g. ``"HS2017"``).

    Returns:
        The Parquet cache path.

    Examples:
        >>> cached_table_path("cache/", "HS2022", "HS2017")
        'cache/HS2022-HS2017.parquet'
    """
    return f"{concordance_path.rstrip('/')}/{source}-{target}.parquet"


# Fonction de construction du chemin du registre des téléchargements
def concordance_registry_path(concordance_path: str) -> str:
    """Build the JSON registry path of the concordance cache.

    Args:
        concordance_path: Root directory of the concordance cache.

    Returns:
        The registry path.

    Examples:
        >>> concordance_registry_path("cache")
        'cache/unsd_correspondance_tables.json'
    """
    return f"{concordance_path.rstrip('/')}/{CONCORDANCE_REGISTRY_FILE}"


# Fonction de lecture non bloquante d'une table mise en cache
def load_cached_table(loader: Any, path: str, bucket: Optional[str]) -> Optional[pd.DataFrame]:
    """Read a cached Parquet table, or ``None`` when it is absent.

    Args:
        loader: Table loader (local or S3, dispatched on ``bucket``).
        path: Cache path (local path or S3 key).
        bucket: S3 bucket name, or ``None`` for a local cache.

    Returns:
        The cached table, or ``None`` when no cache file exists yet.
    """
    # Erreur S3 « objet absent » importée paresseusement : botocore n'est requis
    # que pour un cache S3
    missing: Tuple[type, ...] = (FileNotFoundError,)
    try:
        from botocore.exceptions import ClientError

        missing = (FileNotFoundError, ClientError)
    except ImportError:  # pragma: no cover - botocore installé avec statflows
        pass
    try:
        return loader.load(path, bucket=bucket)
    except missing:
        return None


# Fonction de listage des paires descendantes entre millésimes
def downward_pairs(
    targets: Iterable[str], nomenclatures: Mapping[str, int]
) -> List[Tuple[str, str]]:
    """List the ``(recent, older)`` pairs converting into each target vintage.

    Every vintage more recent than a target is a possible source: the flows
    of each later year are declared in the vintage in force that year.

    Args:
        targets: Target vintages.
        nomenclatures: Mapping vintage label -> entry-into-force year.

    Returns:
        Sorted distinct pairs ``(source, target)`` with
        ``entry(source) > entry(target)``.

    Examples:
        >>> downward_pairs(["HS2012"], {"HS2012": 2012, "HS2017": 2017, "HS2022": 2022})
        [('HS2017', 'HS2012'), ('HS2022', 'HS2012')]
    """
    entries = {str(label): int(year) for label, year in nomenclatures.items()}
    return sorted(
        {
            (source, target)
            for target in targets
            for source, entry in entries.items()
            if entry > entries[target]
        }
    )


# Fonction de chargement des tables de correspondance nécessaires, avec cache
def prepare_concordances(
    pairs: Sequence[Tuple[str, str]],
    *,
    client_factory: Callable[[], Any],
    loader: Any,
    saver: Any,
    concordance_path: str,
    bucket: Optional[str],
    force_refresh: bool = False,
) -> Dict[Tuple[str, str], pd.DataFrame]:
    """Load cached HS correspondence tables, downloading only the missing ones.

    Args:
        pairs: Distinct ``(source, target)`` vintage pairs to resolve (UNSD
            identifiers, e.g. ``("HS2022", "HS2017")``).
        client_factory: Builds the UNSD client (``statflows.UNSDClient``);
            called only when a pair must be downloaded, and closed afterwards.
        loader: Table loader (local or S3) reading the cached Parquet tables.
        saver: Table saver (local or S3) writing the cached Parquet tables.
        concordance_path: Root directory of the concordance cache.
        bucket: S3 bucket name, or ``None`` for a local cache.
        force_refresh: When ``True``, re-download every pair even if cached.

    Returns:
        Mapping ``(source, target) -> normalised conversion table``, one entry
        per requested pair.

    Raises:
        Exception: Whatever the UNSD client raises for a pair it cannot
            download (the caller decides whether that is fatal).
    """
    concordances: Dict[Tuple[str, str], pd.DataFrame] = {}
    to_download: List[Tuple[str, str]] = []
    # Cache existant : aucune vérification de fraîcheur distante
    for source, target in pairs:
        table_path = cached_table_path(concordance_path, source, target)
        df_table = None if force_refresh else load_cached_table(loader, table_path, bucket)
        if df_table is None:
            to_download.append((source, target))
        else:
            concordances[(source, target)] = df_table
    if not to_download:
        return concordances

    # Téléchargement des paires manquantes, client créé à la demande
    registry_path = concordance_registry_path(concordance_path)
    json_saver = JsonSaver()
    registry = JsonLoader().load(registry_path, bucket=bucket, missing_ok=True) or {}
    client = client_factory()
    try:
        # Catalogue des tables déclarées (URL source de chaque paire)
        df_catalogue = client.list_available_tables().set_index(
            ["source_classification", "target_classification"]
        )
        for source, target in to_download:
            key = f"{source}-{target}"
            # Logging
            logger.info("Téléchargement de la table de correspondance %s", key)
            df_table = client.get_correspondence(source, target, kind="conversion")
            saver.save(
                cached_table_path(concordance_path, source, target),
                df_table,
                bucket=bucket,
                index=False,
            )
            # Mise à jour du registre uniquement pour les paires téléchargées
            registry[key] = {
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "source_url": str(df_catalogue.loc[(source, target), "url"]),
                "n_rows": int(len(df_table)),
                "checksum": table_checksum(df_table),
            }
            json_saver.save(registry_path, registry, bucket=bucket, indent=2, ensure_ascii=False)
            concordances[(source, target)] = df_table
    finally:
        client.close()
    # Ordre des paires demandées
    return {pair: concordances[pair] for pair in pairs}


# ──────────────────────────────────────────────────────────────────────
# Porte de complétude (fonctions pures)
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul de la part des lots téléchargés par année
def completeness_by_year(
    planned: Iterable[Any],
    batches: Mapping[int, Mapping[Any, Any]],
) -> Dict[int, float]:
    """Share, per year, of the planned product batches downloaded at least once.

    A planned query (one period x one product batch) counts as downloaded when
    the registry view holds, for its year, a batch with the same products and a
    ``last_download`` date. Matching on the query parameters, rather than on the
    registry key, keeps the gate independent of the physical registry layout.

    Args:
        planned: Planned ``ComtradeQueryRequest`` objects (``periods``,
            ``products``), as built by ``plan_queries``.
        batches: Downloaded batches by year, as returned by
            ``DownloadRegistryView.batches_by_year`` (``year -> {products key ->
            last_download}``).

    Returns:
        Mapping ``year -> share`` in ``[0, 1]``, for every planned year.

    Examples:
        >>> from types import SimpleNamespace as Query
        >>> planned = [Query(periods="2023", products=["01"]), Query(periods="2023", products=["02"])]
        >>> completeness_by_year(planned, {2023: {("01",): "2026-01-01"}})
        {2023: 0.5}
    """
    from kedro_pipeline.io.registry_views import period_year, products_key

    # Décompte des lots planifiés et téléchargés, par année
    totals: Dict[int, int] = {}
    done: Dict[int, int] = {}
    for query in planned:
        year = period_year(query.periods)
        totals[year] = totals.get(year, 0) + 1
        if products_key(query.products) in batches.get(year, {}):
            done[year] = done.get(year, 0) + 1
    return {year: done.get(year, 0) / total for year, total in totals.items()}


# Fonction de sélection des années éligibles au redressement
def eligible_years(
    view: Any,
    planned_batches: Iterable[Any],
    min_share: float,
    start_year: Optional[int] = None,
    *,
    period_end: Optional[int] = None,
) -> Dict[int, float]:
    """Years whose share of downloaded batches reaches ``min_share``, with that share.

    BACI pools its estimations over every year of a vintage: a year enters the
    scope only once (nearly) all its planned batches were downloaded at least
    once — a partial year would bias every other year.

    Args:
        view: Download-registry view exposing ``batches_by_year()`` (e.g.
            ``DownloadRegistryView``), or the mapping it returns.
        planned_batches: Planned queries (one period x one product batch).
        min_share: Completeness threshold (``COMPLETENESS.MIN_SHARE``); a share
            exactly equal to it is eligible.
        start_year: First year considered (``ANALYSIS_START_YEAR.comtrade``);
            ``None`` for no lower bound.
        period_end: Last year considered (``PARAMETERS.period_end``); ``None``
            for no upper bound.

    Returns:
        Mapping ``year -> share``, sorted by year, of the eligible years.

    Examples:
        >>> from types import SimpleNamespace as Query
        >>> planned = [Query(periods=str(y), products=["01"]) for y in (2021, 2022, 2023)]
        >>> eligible_years({2022: {("01",): "x"}, 2023: {("01",): "x"}}, planned, 1.0, 2023)
        {2023: 1.0}
    """
    batches = view if isinstance(view, Mapping) else view.batches_by_year()
    shares = completeness_by_year(planned_batches, batches)
    return {
        year: share
        for year, share in sorted(shares.items())
        if share >= min_share
        and (start_year is None or year >= start_year)
        and (period_end is None or year <= period_end)
    }


# ──────────────────────────────────────────────────────────────────────
# Tables de correspondance nécessaires et blocs de chapitres
# ──────────────────────────────────────────────────────────────────────

# Expression SQL de l'année d'une période (texte « YYYY », « YYYYMM »…)
def _year_sql(period_col: str) -> str:
    """SQL expression of the year of a period column (first four characters)."""
    return f'CAST(substr(CAST("{period_col}" AS VARCHAR), 1, 4) AS INTEGER)'


# Expression SQL du chapitre SH2 d'un code produit
def _chapter_sql(product_col: str) -> str:
    """SQL expression of the HS chapter of a product column (zero-padded code)."""
    return f"""substr(lpad(CAST("{product_col}" AS VARCHAR), 6, '0'), 1, 2)"""


# Fonction de lecture des nomenclatures présentes par année
def classifications_by_year(
    conn: Any,
    source_schema: str,
    *,
    years: Sequence[int],
    classification_col: str = "classificationCode",
    period_col: str = "period",
) -> Dict[int, List[str]]:
    """List the HS classifications present in the Comtrade fact table, year by year.

    A ``SELECT DISTINCT`` with bound parameters: only the distinct pairs travel,
    never the fact table.

    Args:
        conn: Open DuckDB / DuckLake connection.
        source_schema: Schema holding the Comtrade ``fact_table``.
        years: Years to inspect.
        classification_col: Classification column.
        period_col: Period column.

    Returns:
        Mapping ``year -> sorted classification codes``.

    Examples:
        >>> import duckdb
        >>> conn = duckdb.connect()
        >>> _ = conn.execute('CREATE SCHEMA s; CREATE TABLE s.fact_table AS SELECT * FROM '
        ...                  "(VALUES ('2017', 'H5'), ('2023', 'H6'), ('2023', 'H5')) t(period, classificationCode)")
        >>> classifications_by_year(conn, "s", years=[2017, 2023])
        {2017: ['H5'], 2023: ['H5', 'H6']}
    """
    from statflows.storage.ducklake.tables import FACT_TABLE

    year_expr = _year_sql(period_col)
    rows = conn.execute(
        f'SELECT DISTINCT {year_expr} AS y, "{classification_col}" AS c '
        f'FROM "{source_schema}".{FACT_TABLE} '
        f"WHERE list_contains(?::INTEGER[], {year_expr})",
        [[int(year) for year in years]],
    ).fetchall()
    out: Dict[int, List[str]] = {}
    for year, code in rows:
        if code is not None:
            out.setdefault(int(year), []).append(str(code))
    return {year: sorted(codes) for year, codes in sorted(out.items())}


# Fonction de calcul des paires de correspondance nécessaires
def concordance_pairs(
    classifications: Mapping[int, Sequence[str]],
    scopes: Mapping[str, Sequence[int]],
) -> List[Tuple[str, str]]:
    """List the ``(source, target)`` vintage pairs needed by the BACI targets.

    Every classification present in a year of a target's scope, other than the
    target itself, must be converted into it.

    Args:
        classifications: Classifications present per year
            (:func:`classifications_by_year`).
        scopes: Years of each target vintage.

    Returns:
        Sorted distinct pairs (UNSD labels).

    Examples:
        >>> concordance_pairs({2017: ["H5"], 2023: ["H5", "H6"]}, {"HS2017": [2017, 2023]})
        [('HS2022', 'HS2017')]
    """
    from macroforecast.trade.processing import resolve_vintage

    pairs: Set[Tuple[str, str]] = set()
    for target, years in scopes.items():
        for year in years:
            for code in classifications.get(int(year), ()):
                source = f"HS{resolve_vintage(code)}"
                if source != target:
                    pairs.add((source, target))
    return sorted(pairs)


# Fonction de calcul des liens de chapitres induits par la conversion
def chapter_links(
    concordances: Mapping[Tuple[str, str], pd.DataFrame],
    sources: Sequence[str],
    target: str,
) -> List[Tuple[str, str]]:
    """List the ``(source chapter, target chapter)`` links of the conversions.

    A conversion may move a code to another HS chapter; the declarations of two
    source chapters converging on a same target chapter must then stay in the same
    block, so that no target product (nor any ``(exporter, product, year)`` group
    of the NES step) is split between blocks.

    Args:
        concordances: Correspondence tables (``prepare_concordances``).
        sources: Source classifications present (any spelling).
        target: Target vintage.

    Returns:
        Sorted distinct chapter links (identity links excluded).

    Examples:
        >>> table = pd.DataFrame({"source_code": ["850110", "840110"], "target_code": ["840110", "840110"]})
        >>> chapter_links({("HS2022", "HS2017"): table}, ["H6"], "HS2017")
        [('85', '84')]
    """
    from macroforecast.trade.processing import build_conversion_map, resolve_vintage

    links: Set[Tuple[str, str]] = set()
    for source in sources:
        if resolve_vintage(source) == resolve_vintage(target):
            continue
        for code, converted in build_conversion_map(concordances, source, target).items():
            if code[:2] != converted[:2]:
                links.add((code[:2], converted[:2]))
    return sorted(links)


# Fonction de découpage d'une année en blocs de chapitres
def chapter_blocks(
    rows_by_chapter: Mapping[str, int],
    links: Iterable[Tuple[str, str]],
    max_rows: Optional[int],
) -> List[List[str]]:
    """Split the chapters of a year into blocks of at most ``max_rows`` declarations.

    Chapters linked by a conversion (:func:`chapter_links`) form connected
    components that are never split; components are then packed, in chapter
    order, into blocks under ``max_rows``. Every estimation of the passes being
    additive over any partition of the observations, the result does not depend
    on the blocks.

    Args:
        rows_by_chapter: Declarations per HS chapter of the year.
        links: Chapter links ``(source, target)``.
        max_rows: Maximum declarations per block (``None``: a single block).

    Returns:
        Blocks of chapters, each sorted; a single block holding every chapter
        when no split is needed.

    Raises:
        ValueError: If a single component exceeds ``max_rows`` (no exact split
            exists; raise ``MAX_ROWS_PER_CHUNK`` or the pod memory).

    Examples:
        >>> chapter_blocks({"01": 5, "02": 5, "84": 6, "85": 4}, [("85", "84")], 10)
        [['01', '02'], ['84', '85']]
        >>> chapter_blocks({"01": 5}, [], None)
        [['01']]
    """
    chapters = sorted(rows_by_chapter)
    if max_rows is None or sum(rows_by_chapter.values()) <= max_rows:
        return [chapters]
    # Composantes connexes (union-find) des chapitres liés par une conversion
    parent = {chapter: chapter for chapter in chapters}

    def root(chapter: str) -> str:
        while parent[chapter] != chapter:
            parent[chapter] = parent[parent[chapter]]
            chapter = parent[chapter]
        return chapter

    for source, target in links:
        for chapter in (source, target):
            parent.setdefault(chapter, chapter)
        parent[root(source)] = root(target)
    components: Dict[str, List[str]] = {}
    for chapter in chapters:
        components.setdefault(root(chapter), []).append(chapter)

    # Regroupement des composantes, dans l'ordre des chapitres
    blocks: List[List[str]] = []
    current: List[str] = []
    current_rows = 0
    for members in sorted(components.values(), key=min):
        rows = sum(rows_by_chapter[chapter] for chapter in members)
        if rows > max_rows:
            raise ValueError(
                f"Chapters {members} hold {rows} declarations, more than "
                f"MAX_ROWS_PER_CHUNK={max_rows}, and cannot be split further "
                "(a product or a conversion would straddle two blocks)."
            )
        if current and current_rows + rows > max_rows:
            blocks.append(sorted(current))
            current, current_rows = [], 0
        current.extend(members)
        current_rows += rows
    if current:
        blocks.append(sorted(current))
    return blocks


# ──────────────────────────────────────────────────────────────────────
# Lecture de la table de faits Comtrade, une année à la fois
# ──────────────────────────────────────────────────────────────────────

# Fonction de décompte des déclarations d'une année par chapitre
def rows_by_chapter(
    conn: Any,
    source_schema: str,
    *,
    year: int,
    period_col: str = "period",
    product_col: str = "cmdCode",
) -> Dict[str, int]:
    """Count the Comtrade declarations of a year per HS chapter.

    Args:
        conn: Open DuckDB / DuckLake connection.
        source_schema: Schema holding the Comtrade ``fact_table``.
        year: Year to count.
        period_col: Period column.
        product_col: Product column.

    Returns:
        Mapping ``chapter -> declarations``.
    """
    from statflows.storage.ducklake.tables import FACT_TABLE

    chapter = _chapter_sql(product_col)
    rows = conn.execute(
        f'SELECT {chapter} AS chapter, count(*) FROM "{source_schema}".{FACT_TABLE} '
        f"WHERE {_year_sql(period_col)} = ? GROUP BY 1",
        [int(year)],
    ).fetchall()
    return {str(c): int(n) for c, n in rows}


# Fonction de lecture des déclarations d'une année (éventuellement d'un bloc)
def read_comtrade_year(
    conn: Any,
    source_schema: str,
    columns: Sequence[str],
    *,
    year: int,
    period_col: str = "period",
    product_col: str = "cmdCode",
    chapters: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Read the projected Comtrade declarations of one year, read-only.

    The year (and the chapters of a block) are pushed down to SQL as bound
    parameters; identifiers come from the configuration and are quoted.

    Args:
        conn: Open DuckDB / DuckLake connection.
        source_schema: Schema holding the Comtrade ``fact_table``.
        columns: Columns to project.
        year: Year to read.
        period_col: Period column.
        product_col: Product column.
        chapters: HS chapters of the block (``None``: the whole year).

    Returns:
        The declarations of the year (or block).

    Examples:
        >>> import duckdb
        >>> conn = duckdb.connect()
        >>> _ = conn.execute('CREATE SCHEMA s; CREATE TABLE s.fact_table AS SELECT * FROM '
        ...                  "(VALUES ('2017', '010121', 1.0), ('2017', '850110', 2.0), ('2018', '010121', 3.0)) "
        ...                  "t(period, cmdCode, primaryValue)")
        >>> read_comtrade_year(conn, "s", ["primaryValue"], year=2017, chapters=["85"])["primaryValue"].tolist()
        [2.0]
    """
    from statflows.storage.ducklake.tables import FACT_TABLE

    col_list = ", ".join(f'"{column}"' for column in columns)
    sql = f'SELECT {col_list} FROM "{source_schema}".{FACT_TABLE} WHERE {_year_sql(period_col)} = ?'
    params: List[Any] = [int(year)]
    if chapters is not None:
        sql += f" AND list_contains(?::VARCHAR[], {_chapter_sql(product_col)})"
        params.append([str(chapter) for chapter in chapters])
    return conn.execute(sql, params).df()


# ──────────────────────────────────────────────────────────────────────
# Entrées / sorties du redressement par passes (DuckDB + Parquet de travail)
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction de la racine des fichiers de travail d'une passe
def work_root(work_path: str, vintage: str, fit_id: str, bucket: Optional[str]) -> str:
    """Root of the work files of one estimation pass.

    Args:
        work_path: ``PASSES.WORK_PATH`` (key prefix under the bucket, or local
            directory when ``bucket`` is ``None``).
        vintage: Vintage label.
        fit_id: Identifier of the pass (same scope, watermark and methodology
            → same root, which lets an interrupted pass reuse its files).
        bucket: S3 bucket, ``None`` for a local directory.

    Returns:
        ``s3://<bucket>/<work_path>/<vintage>/<fit_id>`` or the local path.

    Examples:
        >>> work_root("trade/work/baci", "HS2017", "abc", "bkt")
        's3://bkt/trade/work/baci/HS2017/abc'
        >>> work_root("work", "HS2017", "abc", None)
        'work/HS2017/abc'
    """
    relative = f"{work_path.strip('/')}/{vintage}/{fit_id}"
    return f"s3://{bucket}/{relative}" if bucket else Path(relative).as_posix()


# Fonction de mesure du pic mémoire du processus
def peak_memory_mb() -> float:
    """Peak resident memory of the current process, in megabytes.

    Uses ``resource.getrusage`` on POSIX (kilobytes on Linux, bytes on macOS)
    and ``psutil``'s peak working set elsewhere; ``NaN`` when neither is
    available.

    Returns:
        The peak memory in MB.

    Examples:
        >>> peak_memory_mb() > 0
        True
    """
    try:
        import resource

        peak = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return peak / (1024.0 ** 2) if sys.platform == "darwin" else peak / 1024.0
    except ImportError:
        pass
    try:
        import psutil

        info = psutil.Process().memory_info()
        return float(getattr(info, "peak_wset", info.rss)) / (1024.0 ** 2)
    except ImportError:  # pragma: no cover - psutil installé avec l'extra tracking
        return float("nan")


# Implémentation DuckDB des entrées / sorties du redressement par passes
class DuckDBPassIO:
    """``BaciPassIO`` backed by DuckDB: Comtrade per year, Parquet work files, SQL statistics.

    * ``comtrade_chunks``: one query per year (``WHERE year = ?``), split into
      blocks of chapters (:func:`chapter_blocks`) when the year exceeds
      ``max_rows_per_chunk``; each chunk is harmonised to the vintage by a fresh
      harmoniser (``harmonizer_factory``), whose reports are merged exactly at
      the end (:meth:`harmonization_report`).
    * ``spill`` / ``read``: ``COPY (SELECT …) TO '<root>/<kind>/year=<y>/block=<b>.parquet'``
      and ``read_parquet`` with column projection.
    * ``median_uv``: the query of
      ``macroforecast.trade.processing.world_median_unit_values_sql`` over every
      mirror file, run chapter by chapter of products (exact: the median is per
      product), which bounds DuckDB's memory.
    * ``quantiles``: ``quantile_cont`` (linear interpolation, as pandas).
    * ``write_year``: delegated to ``writer`` (:class:`DuckLakeYearWriter`).
    * ``load_p0_state`` / ``save_p0_state``: ``<root>/p0/*.parquet``, the
      ``done.parquet`` marker being written last.

    Args:
        conn: Open DuckDB / DuckLake connection (S3 secret configured when the
            work files live on S3).
        source_schema: Schema of the Comtrade ``fact_table``.
        years: Years of the vintage scope.
        root: Root of the work files (:func:`work_root`).
        writer: Callable ``(year, blocks) -> None`` persisting a year.
        period_col: Comtrade period column.
        product_col: Comtrade product column.
        harmonizer_factory: Builds an unfitted ``HsHarmonizer`` towards the
            vintage (``None``: no harmonisation).
        links: Chapter links of the conversions (:func:`chapter_links`).
        max_rows_per_chunk: Maximum declarations per chunk (``None``: whole years).

    Examples:
        >>> import duckdb, tempfile
        >>> from macroforecast.trade.processing import ChunkKey
        >>> io = DuckDBPassIO(duckdb.connect(), source_schema="s", years=[2020],
        ...                   root=tempfile.mkdtemp(), writer=lambda year, blocks: None)
        >>> io.spill("freight", ChunkKey(2020), pd.DataFrame({"cif_rate": [0.1, 0.3, None]}))
        >>> io.read("freight", ChunkKey(2020))["cif_rate"].tolist()[:2]
        [0.1, 0.3]
        >>> io.quantiles("freight", "cif_rate", [0.5])
        [0.2]
        >>> io.cleanup()
    """

    def __init__(
        self,
        conn: Any,
        *,
        source_schema: str,
        years: Sequence[int],
        root: str,
        writer: Callable[[int, Iterator[pd.DataFrame]], None],
        period_col: str = "period",
        product_col: str = "cmdCode",
        harmonizer_factory: Optional[Callable[[], Any]] = None,
        links: Sequence[Tuple[str, str]] = (),
        max_rows_per_chunk: Optional[int] = None,
    ) -> None:
        self.conn = conn
        self.source_schema = source_schema
        self.years = sorted(int(year) for year in years)
        self.root = root.rstrip("/")
        self.writer = writer
        self.period_col = period_col
        self.product_col = product_col
        self.harmonizer_factory = harmonizer_factory
        self.links = list(links)
        self.max_rows_per_chunk = max_rows_per_chunk
        self.harmonizers: List[Any] = []
        self.chunks_read: List[Tuple[int, int, int]] = []

    # Chemins des fichiers de travail
    def _path(self, kind: str, key: Any) -> str:
        return f"{self.root}/{kind}/{key.path}.parquet"

    def _is_local(self) -> bool:
        return not self.root.startswith("s3://")

    # Écriture d'une table en Parquet par DuckDB
    def _copy(self, df: pd.DataFrame, path: str) -> None:
        if self._is_local():
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        view = "_baci_spill"
        self.conn.register(view, df)
        try:
            self.conn.execute(f"COPY (SELECT * FROM {view}) TO '{path}' (FORMAT PARQUET)")
        finally:
            self.conn.unregister(view)

    # Existence d'un fichier ou d'un motif (local comme S3)
    def _exists(self, pattern: str) -> bool:
        return bool(self.conn.execute("SELECT count(*) FROM glob(?)", [pattern]).fetchone()[0])

    def comtrade_chunks(self, columns: Sequence[str]) -> Iterator[Tuple[Any, pd.DataFrame]]:
        """Yield the harmonised declarations, year by year (and block by block)."""
        from macroforecast.trade.processing import ChunkKey

        for year in self.years:
            counts = rows_by_chapter(
                self.conn, self.source_schema, year=year,
                period_col=self.period_col, product_col=self.product_col,
            )
            if not counts:
                continue
            blocks = chapter_blocks(counts, self.links, self.max_rows_per_chunk)
            for block, chapters in enumerate(blocks):
                df_chunk = read_comtrade_year(
                    self.conn, self.source_schema, columns, year=year,
                    period_col=self.period_col, product_col=self.product_col,
                    chapters=None if len(blocks) == 1 else chapters,
                )
                n_raw = len(df_chunk)
                if self.harmonizer_factory is not None and n_raw:
                    harmonizer = self.harmonizer_factory()
                    df_chunk = harmonizer.fit_transform(df_chunk)
                    self.harmonizers.append(harmonizer)
                self.chunks_read.append((year, block, n_raw))
                # Logging
                logger.info("Tranche %s-%d : %d déclarations lues", year, block, n_raw)
                yield ChunkKey(year, block), df_chunk

    def harmonization_report(self) -> Any:
        """Exact report of the harmonisation of every chunk read (``None`` if none)."""
        from macroforecast.trade.processing.classification import merge_harmonization_reports

        return merge_harmonization_reports(self.harmonizers) if self.harmonizers else None

    def spill(self, kind: str, key: Any, df: pd.DataFrame) -> None:
        """Write the intermediate data of a chunk as a Parquet work file."""
        self._copy(df, self._path(kind, key))

    def read(self, kind: str, key: Any, columns: Optional[Sequence[str]] = None) -> pd.DataFrame:
        """Read back the intermediate data of a chunk, projected on ``columns``."""
        projection = "*" if columns is None else ", ".join(f'"{c}"' for c in columns)
        return self.conn.execute(
            f"SELECT {projection} FROM read_parquet('{self._path(kind, key)}')"
        ).df()

    def median_uv(
        self,
        df_rates: pd.DataFrame,
        *,
        tonne_conversion_factors: Mapping[int, float],
        prefer_netwgt: bool,
    ) -> pd.Series:
        """``UV^k`` per product over every mirror work file, chapter by chapter."""
        from macroforecast.trade.processing import world_median_unit_values_sql

        pattern = f"{self.root}/mirror/*/*.parquet"
        if not self._exists(pattern):
            return pd.Series(dtype="float64", name="uv")
        relation = f"read_parquet('{pattern}')"
        rates = df_rates.astype({"product": str, "unit": "int64", "rate": "float64"})
        self.conn.register("_baci_rates", rates)
        try:
            chapters = [
                row[0]
                for row in self.conn.execute(
                    f'SELECT DISTINCT substr("product", 1, 2) FROM {relation} ORDER BY 1'
                ).fetchall()
            ]
            sql = world_median_unit_values_sql(
                relation, "_baci_rates",
                tonne_conversion_factors=tonne_conversion_factors,
                prefer_netwgt=prefer_netwgt, product_range=True,
            )
            frames = []
            for chapter in chapters:
                low, high = f"{chapter}", f"{chapter}￿"
                frames.append(self.conn.execute(sql, [low, high, low, high]).df())
        finally:
            self.conn.unregister("_baci_rates")
        df_uv = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["product", "uv"])
        return df_uv.set_index("product")["uv"].astype("float64").sort_index()

    def quantiles(self, kind: str, column: str, probs: Sequence[float]) -> List[float]:
        """Linear-interpolation quantiles of a column over every work file of a kind."""
        probs_sql = ", ".join(repr(float(prob)) for prob in probs)
        value = self.conn.execute(
            f'SELECT quantile_cont("{column}", [{probs_sql}]) '
            f"FROM read_parquet('{self.root}/{kind}/*/*.parquet') "
            f'WHERE "{column}" IS NOT NULL AND NOT isnan("{column}")'
        ).fetchone()[0]
        if value is None:
            return [float("nan")] * len(probs)
        return [float(v) for v in value]

    def write_year(self, year: int, blocks: Iterator[pd.DataFrame]) -> None:
        """Persist the reconciled flows of a year (delegated to ``writer``)."""
        self.writer(year, blocks)

    def load_p0_state(self) -> Optional[Mapping[str, pd.DataFrame]]:
        """Return the preparation state of an interrupted pass with the same ``fit_id``."""
        if not self._exists(f"{self.root}/p0/done.parquet"):
            return None
        # Logging
        logger.info("État P0 réutilisé : %s", self.root)
        return {
            name: self.conn.execute(f"SELECT * FROM read_parquet('{self.root}/p0/{name}.parquet')").df()
            for name in ("regime_sums", "tonnage_stats", "summary")
        }

    def save_p0_state(self, frames: Mapping[str, pd.DataFrame]) -> None:
        """Save the preparation state; the ``done`` marker is written last."""
        for name, df in frames.items():
            self._copy(df, f"{self.root}/p0/{name}.parquet")
        self._copy(pd.DataFrame({"done": [True]}), f"{self.root}/p0/done.parquet")

    def cleanup(self) -> None:
        """Delete every work file of the pass (end of a successful pass)."""
        if self._is_local():
            shutil.rmtree(self.root, ignore_errors=True)
            return
        delete_s3_prefix(self.root)


# Fonction de suppression d'un préfixe S3
def delete_s3_prefix(uri: str) -> int:
    """Delete every object under an ``s3://bucket/prefix`` URI.

    Args:
        uri: ``s3://`` URI of the prefix.

    Returns:
        Number of objects deleted.
    """
    from statflows.storage._connection import S3Connection

    bucket, _, prefix = uri[len("s3://"):].partition("/")
    client = S3Connection()._connect().s3
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            deleted += len(keys)
    return deleted


# ──────────────────────────────────────────────────────────────────────
# Écriture du résultat, une année par transaction DuckLake
# ──────────────────────────────────────────────────────────────────────

# Écrivain des années redressées dans le schéma DuckLake du millésime
class DuckLakeYearWriter:
    """Write the reconciled flows of a vintage one year per DuckLake transaction.

    For each year: ``BEGIN``; delete the rows of the year
    (``DatabaseDeleter.delete_rows``); upsert every block of the year
    (``DatabaseUpdater.update_database``, no compaction; a constant column missing
    from an existing table — e.g. ``fit_id`` — is added with its metadata row);
    commit message (``run_id``, ``commit_message``); ``COMMIT``. A failure rolls
    the year back entirely. The very first write of a schema creates it. Both
    paths go through :class:`kedro_pipeline.io.ducklake.DuckLakeTable`. The
    rewrite of a year is idempotent, which makes the resumption of an
    interrupted pass safe.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias of the attached catalog.
        schema: Result schema of the vintage.
        primary_keys: Primary-key columns of the result.
        columns: Constant columns added to every row (``fit_id``,
            ``is_provisional``).
        year_col: Year column of the result.
        run_id: Run identifier recorded on the snapshots.
        commit_message: Commit message prefix.
        on_year_written: Callback ``(year, rows)`` run after each commit (e.g.
            the update of ``years_written`` in the freshness registry).

    Attributes:
        created: Whether the schema was created by this writer.

    Examples:
        >>> writer = DuckLakeYearWriter(conn, catalog_alias="comtrade", schema="baci_hs2017",
        ...                             primary_keys=["exporter", "importer", "product", "year"],
        ...                             columns={"fit_id": "3f2a"})  # doctest: +SKIP
        >>> writer(2021, iter([df_block_1, df_block_2]))  # doctest: +SKIP
    """

    def __init__(
        self,
        conn: Any,
        *,
        catalog_alias: str,
        schema: str,
        primary_keys: Sequence[str],
        columns: Optional[Mapping[str, Any]] = None,
        year_col: str = "year",
        run_id: Optional[str] = None,
        commit_message: Optional[str] = None,
        on_year_written: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        self.conn = conn
        self.catalog_alias = catalog_alias
        self.schema = schema
        self.primary_keys = list(primary_keys)
        self.columns = dict(columns or {})
        self.year_col = year_col
        self.run_id = run_id
        self.commit_message = commit_message
        self.on_year_written = on_year_written
        self.created = False

    # Ajout des colonnes constantes
    def _decorate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for name, value in self.columns.items():
            df[name] = value
        return df

    def __call__(self, year: int, blocks: Iterator[pd.DataFrame]) -> None:
        """Write the blocks of one year in a single transaction.

        Delegates to :meth:`kedro_pipeline.io.ducklake.DuckLakeTable.upsert_many`:
        the rows of the year are deleted, then every block is upserted, in one
        transaction (the schema is created by the first block on the very
        first write). The blocks are consumed one at a time, so the memory stays
        bounded by a block.

        Args:
            year: Year written.
            blocks: Reconciled flows of the year, block by block.
        """
        from kedro_pipeline.io.ducklake import DuckLakeTable

        rows = 0

        # Lots décorés des colonnes constantes, comptés au fil de l'écriture
        def decorated() -> Iterator[pd.DataFrame]:
            nonlocal rows
            for block in blocks:
                block = self._decorate(block)
                rows += len(block)
                yield block

        table = DuckLakeTable(self.conn, self.catalog_alias, self.schema)
        created = table.upsert_many(
            decorated(),
            self.primary_keys,
            delete_where=f'"{self.year_col}" = {int(year)}',
            run_id=self.run_id,
            commit_message=f"{self.commit_message or 'baci'} year={int(year)}",
            commit_info={"operation": "baci_year", "schema": self.schema, "year": int(year)},
        )
        self.created = self.created or created
        # Logging
        logger.info("Année %s écrite dans '%s' : %d lignes", year, self.schema, rows)
        if self.on_year_written is not None:
            self.on_year_written(int(year), rows)


# ──────────────────────────────────────────────────────────────────────
# Lecture ponctuelle de Comtrade, périmètre provisoire et couverture
# ──────────────────────────────────────────────────────────────────────

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

# Fonction de détection d'un périmètre produit restreint
def is_provisional_scope(
    available_products: Iterable[str],
    planned_products: Iterable[str],
    full_product_regex: str,
    exclude: Optional[Sequence[str]] = None,
) -> bool:
    """Tell whether the planned products are a strict subset of the full BACI scope.

    A BACI computed on a product subset is not the full BACI (reporter quality
    is estimated on every product): it is labelled provisional.

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


# Sections des métriques d'un redressement : un préfixe par étape de la méthodologie.
# Les champs scalaires du rapport global décrivent la sortie (flux, valeur, période) ou,
# pour le régime de valorisation des importations, l'étape de fobisation ; chaque
# rapport d'étape devient une section que l'interface MLflow regroupe à part
BACI_OUTPUT_SECTION = "output"
BACI_REGIME_SECTION = "valuation"
BACI_REPORT_SECTIONS: Dict[str, str] = {
    "tonnage": "conversion",
    "gravity": "gravity",
    "fobisation": "valuation",
    "mirror": "reconciliation",
    "quality_value": "quality/value",
    "quality_quantity": "quality/quantity",
    "nes": "nes",
}
# Préfixe des diagnostics d'harmonisation des nomenclatures
HARMONIZATION_SECTION = "harmonization"


# Fonction des métriques d'un redressement BACI, rangées par étape de la méthodologie
def baci_section_metrics(report: Any) -> Dict[str, float]:
    """Return the metrics of a BACI report, one ``/``-separated section per step.

    Correspondence between the report fields and the metric sections:

    * scalar fields ``flows``, ``n_input_declarations``, ``total_reconciled_value``,
      ``period_start``, ``period_end``, ``created`` → ``output/<field>``;
    * ``regime_country_years``, ``regime_fob_country_years``,
      ``regime_no_information`` (valuation regime of the imports) →
      ``valuation/<field>``;
    * step reports: ``tonnage`` → ``conversion/…``, ``gravity`` → ``gravity/…``
      (coefficients included), ``fobisation`` → ``valuation/…``, ``mirror`` →
      ``reconciliation/…``, ``quality_value`` → ``quality/value/…``,
      ``quality_quantity`` → ``quality/quantity/…``, ``nes`` → ``nes/…``.

    Non-numeric fields (``classification_code``) and non-finite values are
    dropped, as MLflow rejects them.

    Args:
        report: ``BaciReport`` of the reconstruction.

    Returns:
        Metric name -> finite value.

    Examples:
        >>> from macroforecast.trade.processing.baci import BaciReport
        >>> metrics = baci_section_metrics(BaciReport(flows=12, regime_country_years=3))
        >>> metrics["output/flows"], metrics["valuation/regime_country_years"]
        (12.0, 3.0)
        >>> any(name.startswith("baci/") for name in metrics)
        False
    """
    from dataclasses import fields as dataclass_fields

    from macroforecast.tracking import flatten_metrics

    metrics: Dict[str, float] = {}
    for item in dataclass_fields(report):
        value = getattr(report, item.name)
        if item.name in BACI_REPORT_SECTIONS:
            metrics.update(flatten_metrics(value, prefix=BACI_REPORT_SECTIONS[item.name], sep="/"))
        elif item.name.startswith("regime_"):
            metrics.update(flatten_metrics({item.name: value}, prefix=BACI_REGIME_SECTION, sep="/"))
        else:
            metrics.update(flatten_metrics({item.name: value}, prefix=BACI_OUTPUT_SECTION, sep="/"))
    return metrics


# Fonction des métriques de l'harmonisation des nomenclatures
def harmonization_metrics(report: Any) -> Dict[str, float]:
    """Return the metrics of an HS harmonisation report under ``harmonization/``.

    Args:
        report: ``HsHarmonizationReport`` of the vintage.

    Returns:
        Metric name -> finite value (``harmonization/n_codes_mapped``…).

    Examples:
        >>> from macroforecast.trade.processing.classification import HsHarmonizationReport
        >>> harmonization_metrics(HsHarmonizationReport(n_codes_mapped=4))["harmonization/n_codes_mapped"]
        4.0
    """
    from macroforecast.tracking import flatten_metrics

    return flatten_metrics(report, prefix=HARMONIZATION_SECTION, sep="/")



# ──────────────────────────────────────────────────────────────────────
# Registre de fraîcheur : un fragment par millésime, cadence de réestimation
# ──────────────────────────────────────────────────────────────────────

# Clé racine du registre JSON (première version) des dates de dernier traitement BACI,
# lu par l'étape réseau pour ne recalculer que les millésimes réécrits depuis son
# dernier passage
_PROCESSING_ROOT = "BACI"

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
# Fonctions d'étape : préparation du périmètre et redressement par millésime
# ──────────────────────────────────────────────────────────────────────

# Périmètre d'une exécution BACI, décidé avant tout redressement
@dataclass
class BaciScope:
    """Everything decided before re-estimating the BACI vintages of one run.

    Attributes:
        processed_at: Reference instant, captured before the run (recorded as
            ``last_computed``: an upstream update landing during the run is
            never masked).
        config: Methodological configuration (``APPLY_NES`` applied).
        years_eligible: Years passing the completeness gate.
        shares: Download share of the planned batches, by year.
        start_years: First year of every selected target vintage.
        scopes: Eligible years of every vintage.
        watermarks: Upstream watermark of every vintage.
        requested: Current methodological fingerprint.
        force: One-off forcing.
        plans: Plan of every vintage to re-estimate, by label.
        targets: Vintages to re-estimate by this run, label -> target block,
            in configuration order (empty: nothing to do).
        is_provisional: Whether the planned product scope is a strict subset
            of the full HS6 scope (restricted demonstration profile).
        period_end: Optional last year of the perimeter.
        source_schema: Schema of the Comtrade fact table.
        concordances: Correspondence tables ``(source, target) -> table``.
        classifications: Nomenclatures present in the source, by year.
        dist: CEPII distances.
        geo: CEPII geography.
        reference: Result of the publication of the HS reference tables.
    """

    processed_at: datetime
    config: Any
    years_eligible: List[int]
    shares: Dict[int, float]
    start_years: Dict[str, int]
    scopes: Dict[str, List[int]] = field(default_factory=dict)
    watermarks: Dict[str, Optional[datetime]] = field(default_factory=dict)
    requested: Dict[str, str] = field(default_factory=dict)
    force: Any = None
    plans: Dict[str, Any] = field(default_factory=dict)
    targets: Dict[str, Mapping[str, Any]] = field(default_factory=dict)
    is_provisional: bool = False
    period_end: Optional[int] = None
    source_schema: str = ""
    concordances: Dict[Tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    classifications: Dict[int, List[str]] = field(default_factory=dict)
    dist: Optional[pd.DataFrame] = None
    geo: Optional[pd.DataFrame] = None
    reference: Any = None


# Fonction d'étape : porte de complétude, plans des millésimes et entrées communes
def prepare_baci(
    comtrade: Any,
    state: FreshnessRegistry,
    registry_view: Any,
    planned: Sequence[Any],
    available_products: Iterable[str],
    *,
    params: Mapping[str, Any],
    comtrade_params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    targets: Optional[Sequence[str]] = None,
    force: Optional[ForceSpec] = None,
    adopt_legacy_fingerprints: Optional[bool] = None,
    now: Optional[datetime] = None,
    loader: Any = None,
    saver: Any = None,
    concordance_client_factory: Optional[Callable[[], Any]] = None,
    concordances_loader: Optional[Callable[..., Dict[Tuple[str, str], pd.DataFrame]]] = None,
    reference_publisher: Optional[Callable[..., Any]] = None,
) -> BaciScope:
    """Decide which BACI vintages to re-estimate and prepare their shared inputs.

    The planned Comtrade queries (built by the very function of the download
    step) are confronted with the download registry: only the years whose
    share of batches downloaded at least once reaches ``COMPLETENESS.MIN_SHARE``
    are re-estimated. The freshness registry then decides which vintages are
    due (never computed, interrupted pass, new complete year, upstream revision
    after ``REFRESH.MIN_INTERVAL_DAYS``, fingerprint change, forcing). When at
    least one vintage is due, the CEPII files are read and, on a connection of
    ``comtrade``, the nomenclatures present each year, the correspondence
    tables they need (shared Parquet cache, UNSD downloads on demand) and the HS
    reference tables (never blocking).

    Args:
        comtrade: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            Comtrade fact table (its catalog also holds the BACI schemas).
        state: BACI freshness registry (one fragment per vintage).
        registry_view: View of the Comtrade download registry
            (``batches_by_year()``).
        planned: Planned Comtrade queries (uncapped), as built by the download step.
        available_products: Product codelist of the source (provisional scope).
        params: The ``baci`` parameters.
        comtrade_params: The ``comtrade`` parameters (dataflow, product filters).
        runtime: The ``runtime`` parameters (``ANALYSIS_START_YEAR``,
            ``NOMENCLATURES``, forcing).
        targets: Vintages this run is restricted to; every enabled target when
            ``None``.
        force: One-off forcing; read from ``runtime`` alone when ``None``.
        adopt_legacy_fingerprints: Deployment migration flag; read from
            ``STATE`` alone when ``None``.
        now: Reference instant; the current UTC instant when ``None``.
        loader: Table loader of the CEPII files and of the correspondence cache.
        saver: Table saver of the correspondence cache.
        concordance_client_factory: Builds the UNSD client (missing tables only).
        concordances_loader: Loads the correspondence tables
            (:func:`prepare_concordances` by default).
        reference_publisher: Publishes the HS reference tables
            (``publish_hs_reference`` by default).

    Returns:
        The scope; ``targets`` is empty when no year is complete or no vintage
        is due (in the latter case the migrated legacy entries are saved).

    Raises:
        ValueError: If a requested target is not configured.
    """
    from macroforecast.storage import Loader as TableLoader, Saver as TableSaver
    from kedro_pipeline.steps._config import (
        baci_config_from_params,
        resolve_target_start_years,
        schema_name,
    )
    from kedro_pipeline.steps.reference import publish_hs_reference

    processed_at = now or datetime.now(timezone.utc)
    config = replace(
        baci_config_from_params(params.get("PARAMETERS")),
        # Activation de la réallocation des zones « Areas NES » (clé racine, distincte
        # de PARAMETERS.nes_partner_codes qui ne fait que déclarer les codes éligibles)
        apply_nes=bool(params.get("APPLY_NES", True)),
    )
    classifications_config = params["CLASSIFICATIONS"]
    targets_config = select_targets(classifications_config["TARGETS"], targets)
    start_years = resolve_target_start_years(targets_config, runtime)
    period_end = config.period_end
    dataflow = comtrade_params["DATAFLOW"]
    completeness = params.get("COMPLETENESS") or {}

    # Porte de complétude : liste planifiée confrontée au registre de téléchargement
    batches = registry_view.batches_by_year()
    shares = completeness_by_year(planned, batches)
    years_eligible = list(
        eligible_years(
            batches,
            planned,
            float(completeness.get("MIN_SHARE", 1.0)),
            int(runtime["ANALYSIS_START_YEAR"]["comtrade"]),
            period_end=period_end,
        )
    )
    # Étiquette provisoire : périmètre produit planifié restreint (produits non
    # restreints → None dans les requêtes : périmètre complet)
    products_filters = comtrade_params["split_filters"][dataflow]["products"]
    is_provisional = not any(q.products is None for q in planned) and is_provisional_scope(
        available_products=available_products,
        planned_products={str(p) for q in planned for p in q.products},
        full_product_regex=completeness.get("FULL_PRODUCT_REGEX", r"^\d{6}$"),
        exclude=products_filters.get("exclude"),
    )
    # Logging
    logger.info(
        "Porte de complétude : %d année(s) éligible(s) sur %d planifiée(s) %s "
        "(seuil %s) ; périmètre provisoire : %s",
        len(years_eligible), len(shares), years_eligible,
        completeness.get("MIN_SHARE", 1.0), is_provisional,
    )
    scope = BaciScope(
        processed_at=processed_at,
        config=config,
        years_eligible=years_eligible,
        shares=shares,
        start_years=start_years,
        is_provisional=is_provisional,
        period_end=period_end,
        source_schema=schema_name(dataflow),
    )

    # Aucune année complète (rattrapage en cours) : rien à redresser
    if not years_eligible:
        logger.info("Aucune année complète : aucun millésime n'est redressé.")
        return scope

    # Fraîcheur : un fragment par millésime, l'unité étant le millésime entier (ses
    # paramètres sont estimés sur toutes ses années) ; périmètre et watermark amont
    scope.requested = baci_requested(config)
    scope.force = force if force is not None else ForceSpec.from_runtime(runtime, environ={})
    scope.scopes = vintage_scopes(years_eligible, start_years)
    scope.watermarks = {label: vintage_watermark(batches, years) for label, years in scope.scopes.items()}
    plans = plan_baci_vintages(
        state, scope.scopes, scope.watermarks, scope.requested, scope.force,
        params.get("REFRESH"), processed_at,
        adopt_legacy_fingerprints=(
            adopt_legacy_fingerprints
            if adopt_legacy_fingerprints is not None
            else adopt_legacy_flag(params.get("STATE"), environ={})
        ),
    )
    scope.plans = {unit.get("vintage"): plan for unit, plan in plans.items()}
    # Logging
    logger.info(
        "Millésimes à redresser : %s",
        {label: plan.reason for label, plan in scope.plans.items()} or "aucun",
    )
    # Aucun millésime périmé : entrées v1 adoptées écrites malgré tout
    if not plans:
        state.save()
        logger.info("Aucun millésime à redresser.")
        return scope
    # Millésimes redressés par cette exécution, dans l'ordre de la configuration
    scope.targets = {
        label: target for label, target in targets_config.items() if label in scope.plans
    }

    # Lecture des fichiers CEPII
    loader = loader if loader is not None else TableLoader()
    saver = saver if saver is not None else TableSaver()
    bucket = params["BUCKET"]
    scope.dist = loader.load(params["PATHS"]["DIST_CEPII"], bucket=bucket)
    scope.geo = loader.load(params["PATHS"]["GEO_CEPII"], bucket=bucket)

    # Nomenclatures présentes par année (SELECT DISTINCT, jamais la table entière),
    # tables de correspondance nécessaires et référentiels de nomenclature
    connector = comtrade.connector()
    with comtrade.connect() as conn:
        scope.classifications = classifications_by_year(
            conn,
            scope.source_schema,
            years=sorted({y for label in scope.targets for y in scope.scopes.get(label, [])}),
            classification_col=config.schema.classification_col,
            period_col=config.schema.period_col,
        )
        pairs = concordance_pairs(
            scope.classifications, {label: scope.scopes.get(label, []) for label in scope.targets}
        )
        if concordance_client_factory is None:
            from statflows import UNSDClient as concordance_client_factory
        scope.concordances = (concordances_loader or prepare_concordances)(
            pairs,
            client_factory=concordance_client_factory,
            loader=loader,
            saver=saver,
            concordance_path=classifications_config["CONCORDANCE_PATH"],
            bucket=bucket,
            force_refresh=classifications_config.get("FORCE_REFRESH", False),
        )
        # Référentiels publiés dans le catalogue Comtrade ; non bloquant
        scope.reference = (reference_publisher or publish_hs_reference)(
            scope.concordances,
            connector,
            params={
                "SCHEMA_PREFIX": comtrade_params["DOWNLOADS"]["REFERENCE"]["SCHEMA_PREFIX"],
                "NOMENCLATURES": runtime["NOMENCLATURES"]["HS"],
            },
            conn=conn,
        )
        logger.info(f"Référentiels SH : {scope.reference['rows']} ; échecs : {scope.reference['failures']}")
    return scope


# Fonction d'étape : redressement d'un millésime par passes
def run_baci_vintage(
    vintage: str,
    scope: BaciScope,
    comtrade: Any,
    state: FreshnessRegistry,
    *,
    params: Mapping[str, Any],
    tracker: Optional[RunTracker] = None,
    progress: Optional[StepProgress] = None,
    run_id: Optional[str] = None,
    passes_runner: Optional[Callable[..., Any]] = None,
) -> StepResult:
    """Re-estimate one BACI vintage pass by pass and write it year by year.

    A "started" registry entry (pass identifier, years of the scope, no year
    written) precedes any write and is the resume point of an interrupted pass;
    each year written is added to it, and the completed entry is written only
    once every year was written. The Comtrade fact table is never read whole:
    the passes read one year at a time, harmonised to the vintage, and spill
    their intermediate mirror flows to Parquet work files (deleted after a
    successful pass unless ``PASSES.KEEP_WORK_FILES``).

    The vintage is the whole unit of its run: a failure raises (after the
    started entry was written), so that the run ends failed with its reduced
    description; :func:`run_baci_vintages` isolates it from the other vintages.

    Args:
        vintage: Target vintage label (``"HS2017"``), planned in ``scope``.
        scope: Scope prepared by :func:`prepare_baci`.
        comtrade: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            Comtrade fact table (the result schema lives in the same catalog).
        state: BACI freshness registry.
        params: The ``baci`` parameters (``PASSES``, ``TRACKING``, ``BUCKET``).
        tracker: Tracker of the open run of the vintage (never entered here).
        progress: Holder of the stage reached, named by a failure report.
        run_id: Run identifier recorded on the DuckLake snapshots (workflow id).
        passes_runner: Runner of the passes (``run_baci_passes`` by default).

    Returns:
        The step result of the vintage: metrics, artifacts (``tracker`` tables
        plus ``output/rows_by_year.csv``), report tables, ``outputs`` ``report``
        (BACI report) and ``fit_id``.

    Raises:
        Exception: Any failure of the pass, once the resume point is recorded.
    """
    from macroforecast.trade.processing import HsHarmonizer, run_baci_passes
    from kedro_pipeline.steps._config import schema_name

    tracker = capturing(tracker)
    config = scope.config
    schema = config.schema
    target = scope.targets[vintage]
    passes = params.get("PASSES") or {}
    log_artifacts = bool((params.get("TRACKING") or {}).get("LOG_ARTIFACTS", True))
    years = scope.scopes[vintage]

    # Plan du millésime et identifiant de la passe d'estimation
    mark(progress, "préparation de la passe")
    unit, plan = baci_unit(vintage), scope.plans[vintage]
    fit_id = compute_fit_id(vintage, years, scope.watermarks[vintage], scope.requested)
    result_schema = schema_name(target["RESULT_SCHEMA"])

    # Point de reprise : entrée « démarrée » écrite avant toute écriture de table, le
    # dernier calcul réussi restant celui que lit l'étape réseau
    state.upsert(started_entry(state.get(unit), unit, plan, fit_id, years))
    state.save()
    # Fraîcheur : décision du millésime et tag de forçage
    tracker.log_metrics(plan_metrics({unit: plan}, n_candidates=len(scope.scopes)))
    tracker.set_tags({"fit_id": fit_id, "freshness_reason": plan.reason})
    if scope.force.forces_step(STEP, scope.requested):
        tracker.set_tags({"forced": scope.force.describe()})

    # Couverture du millésime : années éligibles et part minimale de lots téléchargés
    n_years_eligible = sum(y >= scope.start_years[vintage] for y in scope.years_eligible)
    tracker.log_metrics(
        coverage_metrics(scope.years_eligible, scope.shares, scope.start_years[vintage], scope.period_end)
    )

    # Harmonisation de chaque tranche vers le millésime cible
    def harmonizer_factory(label: str = vintage) -> Any:
        return HsHarmonizer(
            scope.concordances,
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
    def on_year_written(year: int, rows: int) -> None:
        state.upsert(with_year_written(state.get(unit), year))
        state.save()

    with comtrade.connect() as conn:
        writer = DuckLakeYearWriter(
            conn,
            catalog_alias=attached_catalog_alias(comtrade),
            schema=result_schema,
            primary_keys=config.primary_keys,
            columns={"fit_id": fit_id, "is_provisional": bool(scope.is_provisional)},
            run_id=run_id,
            commit_message=f"process_baci_hs {vintage}",
            on_year_written=on_year_written,
        )
        sources = sorted({c for y in years for c in scope.classifications.get(y, [])})
        io = DuckDBPassIO(
            conn,
            source_schema=scope.source_schema,
            years=years,
            root=work_root(passes.get("WORK_PATH", "trade/work/baci"), vintage, fit_id, params["BUCKET"]),
            writer=writer,
            period_col=schema.period_col,
            product_col=schema.product_col,
            harmonizer_factory=harmonizer_factory,
            links=chapter_links(scope.concordances, sources, vintage),
            max_rows_per_chunk=passes.get("MAX_ROWS_PER_CHUNK"),
        )

        # Redressement par passes, écriture année par année
        mark(progress, "redressement BACI")
        report, rows_by_year = (passes_runner or run_baci_passes)(
            io,
            scope.dist,
            scope.geo,
            config=config,
            tracker=tracker,
            log_artifacts=log_artifacts,
            memory_probe=peak_memory_mb,
        )
        report.created = writer.created

        # Registre du millésime : entrée terminée écrite après succès seulement — une
        # date avancée à tort ferait sauter le recalcul des vulnérabilités de réseau.
        # Un fragment par millésime : aucune course entre pods de millésimes différents
        mark(progress, "écriture du résultat")
        state.upsert(
            completed_entry(
                unit, plan, fit_id, years, scope.processed_at, scope.watermarks[vintage],
                scope.requested,
                result_schema=result_schema,
                n_rows=int(report.flows),
                is_provisional=bool(scope.is_provisional),
            )
        )
        state.save()
        # Fichiers de travail supprimés en fin de passe réussie
        if not bool(passes.get("KEEP_WORK_FILES", False)):
            io.cleanup()

        # Métriques du redressement et de l'harmonisation, une section par étape
        tracker.log_metrics(baci_section_metrics(report))
        harmonization = io.harmonization_report()
        if harmonization is not None:
            tracker.log_metrics(harmonization_metrics(harmonization))
        tracker.set_tags({"result_schema": result_schema, "created": str(report.created)})
        # Répartition des relations de nomenclature : perte d'information à la conversion
        if log_artifacts and harmonization is not None:
            tracker.log_dict(
                harmonization.relationship_distribution,
                "classification/relationship_distribution.json",
            )

    # Tables du rapport de run, construites sur ce qui est déjà produit, sans relecture
    coefficients = tracker.dicts.get("gravity/coefficients.json", {})
    gravity_table = pd.DataFrame(
        {
            "coefficient": coefficients.get("coefficients", {}),
            "std_error": coefficients.get("std_errors", {}),
        }
    ).rename_axis("variable").reset_index()
    return StepResult(
        step="baci",
        n_units_planned=1,
        n_units_succeeded=1,
        metrics=dict(tracker.metrics),
        artifacts={**tracker.tables, "output/rows_by_year.csv": rows_by_year},
        tags=dict(tracker.tags),
        units_label=f"1 millésime ({n_years_eligible} années éligibles)",
        report_tables={
            "conversion_rates": tracker.tables.get("tonnage/conversion_rates.csv", pd.DataFrame()),
            "gravity_coefficients": gravity_table,
            "sigma_by_country": tracker.tables.get("quality/sigma_by_country.csv", pd.DataFrame()),
            "rows_by_year": rows_by_year,
        },
        outputs={"report": report, "fit_id": fit_id, "result_schema": result_schema},
    )


# Fonction d'étape : redressement de tous les millésimes planifiés, un run par millésime
def run_baci_vintages(
    scope: BaciScope,
    comtrade: Any,
    state: FreshnessRegistry,
    *,
    params: Mapping[str, Any],
    runs: Optional[UnitRuns] = None,
    run_id: Optional[str] = None,
    passes_runner: Optional[Callable[..., Any]] = None,
) -> StepResult:
    """Re-estimate every planned vintage, each in its own tracked run.

    The failure of one vintage does not prevent the others from running: it
    crosses its own run (failed, with its reduced description), is logged and
    recorded, and the step fails once every vintage was attempted.

    Args:
        scope: Scope prepared by :func:`prepare_baci` (``targets`` planned).
        comtrade: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            Comtrade fact table.
        state: BACI freshness registry.
        params: The ``baci`` parameters.
        runs: Factory of the run of each vintage (tags ``vintage`` and
            ``is_provisional``); every vintage in a null run when ``None``.
        run_id: Run identifier recorded on the DuckLake snapshots.
        passes_runner: Runner of the passes (``run_baci_passes`` by default).

    Returns:
        The step result: one unit per planned vintage, ``children`` holding the
        result of each successful vintage; ``failures`` raised as
        ``RuntimeError("<n> millésime(s) en échec sur <m> : [...]")`` chained to
        the first error.
    """
    runs = runs if runs is not None else shared_runs()
    children: Dict[str, StepResult] = {}
    errors: Dict[str, BaseException] = {}
    for label in scope.targets:
        # Millésime sans année complète (rattrapage année-majeur en cours) : rien à
        # redresser, ce n'est pas un échec
        if not scope.scopes.get(label):
            logger.info(
                "Millésime %s : aucune année éligible >= %d, ignoré", label, scope.start_years[label]
            )
            continue
        try:
            with runs(label, {"vintage": label, "is_provisional": str(scope.is_provisional)}) as run:
                child = run_baci_vintage(
                    label, scope, comtrade, state, params=params, tracker=run.tracker,
                    progress=run.progress, run_id=run_id, passes_runner=passes_runner,
                )
                run.publish(child)
            children[label] = child
            # Logging
            logger.info("Redressement BACI terminé pour %s : %s", label, child.outputs["report"])
        except Exception as exc:
            # Journalisation de l'échec, poursuite avec les autres millésimes
            logger.exception("Échec du redressement BACI pour le millésime %s", label)
            errors[label] = exc
    return _aggregate_units(
        "baci", children, errors, n_planned=len(scope.targets), noun="millésime",
    )


# Fonction d'agrégation des résultats des runs d'unités
def _aggregate_units(
    step: str,
    children: Mapping[str, StepResult],
    errors: Mapping[str, BaseException],
    *,
    n_planned: int,
    noun: str,
) -> StepResult:
    """Aggregate the results of per-unit runs, with the error raised on failure.

    Args:
        step: Step name.
        children: Result of every successful unit.
        errors: Exception of every failed unit.
        n_planned: Units planned.
        noun: Unit noun of the error message (``"millésime"``).

    Returns:
        The aggregated result; its ``failure_exception`` is
        ``RuntimeError("<n> <noun>(s) en échec sur <m> : [...]")`` chained to
        the first error.
    """
    failure: Optional[BaseException] = None
    if errors:
        failure = RuntimeError(
            f"{len(errors)} {noun}(s) en échec sur {n_planned} : {sorted(errors)}"
        )
        failure.__cause__ = next(iter(errors.values()))
    return StepResult(
        step=step,
        n_units_planned=n_planned,
        n_units_succeeded=len(children),
        failures={label: failure_message(exc) for label, exc in errors.items()},
        children=dict(children),
        reportable=False,
        failure_exception=failure,
    )
