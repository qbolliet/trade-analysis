"""BACI step: completeness gate, HS correspondence tables, chunked I/O of the passes.

Pure functions and I/O adapters called by ``scripts/process_baci_hs.py`` (and, in
time, by the Kedro nodes). The methodology lives in
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
from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
import shutil
import sys
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Tuple

# Modules de manipulation de données
import pandas as pd

# Stockage JSON (registre du cache)
from statflows.storage.json import Loader as JsonLoader, Saver as JsonSaver

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
    the year back entirely. The very first write of a schema creates it
    (``statflows.write_dataframe``). The rewrite of a year is idempotent, which
    makes the resumption of an interrupted pass safe.

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
        """Write the blocks of one year in a single transaction."""
        from dt_ducklake_manager import DatabaseUpdater
        from dt_ducklake_manager.operations.deleter import DatabaseDeleter
        from statflows.storage.ducklake.tables import fact_table_exists, write_dataframe

        message = f"{self.commit_message or 'baci'} year={int(year)}"
        iterator = iter(blocks)
        rows = 0
        # Première écriture du schéma : création (transaction propre), puis upsert
        # des blocs restants de l'année
        if not fact_table_exists(self.conn, self.catalog_alias, self.schema):
            first = next(iterator, None)
            if first is None:
                return
            first = self._decorate(first)
            self.created = write_dataframe(
                self.conn, first, self.primary_keys, catalog_alias=self.catalog_alias,
                schema=self.schema, run_id=self.run_id, commit_message=message,
            )
            rows += len(first)
            for block in iterator:
                block = self._decorate(block)
                write_dataframe(
                    self.conn, block, self.primary_keys, catalog_alias=self.catalog_alias,
                    schema=self.schema, run_id=self.run_id, commit_message=message,
                    update_options={"allow_new_columns": True, "compact_after_update": False},
                )
                rows += len(block)
        else:
            deleter = DatabaseDeleter(connection=self.conn, catalog_alias=self.catalog_alias, schema=self.schema)
            updater = DatabaseUpdater(
                connection=self.conn, categorical_threshold=None,
                catalog_alias=self.catalog_alias, schema=self.schema,
            )
            self.conn.begin()
            try:
                report = deleter.delete_rows(
                    f'"{self.year_col}" = {int(year)}', use_transaction=False,
                    perform_cleanup=False, compact_after_update=False,
                )
                if any("failed" in str(warning).lower() for warning in report.warnings):
                    raise RuntimeError(f"Deletion of year {year} failed: {report.warnings}")
                for block in iterator:
                    block = self._decorate(block)
                    if block.empty:
                        continue
                    ok = updater.update_database(
                        block, use_transaction=False, compact_after_update=False, allow_new_columns=True,
                    )
                    if not ok:
                        raise RuntimeError(f"Upsert of year {year} into '{self.schema}' failed")
                    rows += len(block)
                self._commit_message(message, year)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise
        # Logging
        logger.info("Année %s écrite dans '%s' : %d lignes", year, self.schema, rows)
        if self.on_year_written is not None:
            self.on_year_written(int(year), rows)

    # Message de commit de la transaction de l'année
    def _commit_message(self, message: str, year: int) -> None:
        """Record ``run_id`` and the message on the snapshot (never fails the write)."""
        import json

        try:
            self.conn.execute(
                f"CALL ducklake_set_commit_message('{self.catalog_alias}', ?, ?, extra_info := ?)",
                [self.run_id, message, json.dumps({"operation": "baci_year", "schema": self.schema, "year": int(year)})],
            )
        except Exception as exc:  # pragma: no cover - traçabilité seulement
            logger.debug("Message de commit non enregistré : %s", exc)
