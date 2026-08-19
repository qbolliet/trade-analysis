"""Vulnerability computation runner.

Iterates the registered vulnerability metrics over an entire trade DuckLake
catalog and writes the scores to a result DuckLake catalog — one column per
metric, keyed by ``date x nomenclature x indicator x flow x reporter`` (plus
frequency).

The source fact table is read through a direct, read-only DuckDB ``ATTACH`` (the
data lives in immutable Parquet files), while the result catalog is created or
upserted with the same ``DuckLakeTablesBuilder`` / ``DatabaseUpdater`` pattern as
:mod:`macroforecast.datasets.core.download`.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
import logging
from pathlib import Path
from typing import List, Optional, Sequence, Set, Tuple, Union
# Modules de manipulation de données
import duckdb
import narwhals as nw
import pandas as pd
# Module de gestion de la connexion à la base de données
from dt_ducklake_manager import (
    DatabaseUpdater,
    DuckLakeConnector,
    DuckLakeTablesBuilder,
)
# Modules du package
from .base import DEFAULT_CONFIG, VulnerabilityConfig, VulnerabilityMetric
from .metrics import default_metrics

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de la table de faits DuckLake (convention dt_ducklake_manager)
_FACT_TABLE = "fact_table"


# ──────────────────────────────────────────────────────────────────────
# Rapport d'exécution
# ──────────────────────────────────────────────────────────────────────

# Structure résumant l'exécution du calcul des vulnérabilités
@dataclass
class VulnerabilityReport:
    """Summary of a vulnerability run.

    Attributes:
        cells: Number of output cells (distinct key combinations) scored.
        metrics: Names of the metrics computed.
        created: Whether the result schema was created (vs. upserted).
    """
    cells: int = 0
    metrics: Optional[List[str]] = None
    created: bool = False


# ──────────────────────────────────────────────────────────────────────
# Calcul (backend-agnostique, narwhals)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'application de toutes les métriques sur un jeu de données
def compute_vulnerabilities(
    data: nw.DataFrame,
    metrics: Sequence[VulnerabilityMetric],
    config: VulnerabilityConfig = DEFAULT_CONFIG,
) -> nw.DataFrame:
    """Compute every metric and assemble a one-column-per-metric frame.

    Builds the canonical grid of distinct cells and left-joins each metric's
    output onto it, so cells a metric does not score (e.g. CDI2/CDI3 on non-import
    flows) carry a null value.

    Args:
        data: Narwhals frame of partner-level observations.
        metrics: Metric instances to apply.
        config: Column conventions (its ``key_columns`` define the grid).

    Returns:
        Narwhals frame with ``config.key_columns`` plus one column per metric.

    Raises:
        ValueError: If the input frame is missing any column required by one of
            the metrics (see :meth:`VulnerabilityMetric.required_columns`).
    """
    # Clés de la grille de sortie
    keys = list(config.key_columns)

    # Validation de schéma (fail-fast) : union des colonnes exigées par les métriques,
    # confrontée aux colonnes disponibles
    available = set(data.columns)
    required = set().union(*(metric.required_columns() for metric in metrics))
    missing = required - available
    if missing:
        # Métriques concernées par au moins une colonne manquante
        culprits = sorted(
            metric.name
            for metric in metrics
            if metric.required_columns() & missing
        )
        raise ValueError(
            f"Missing required column(s) {sorted(missing)} for metric(s) "
            f"{culprits}. Available columns: {sorted(available)}."
        )

    # Suppression des observations inexploitables (partenaire ou valeur nuls) :
    # un partenaire nul fausse les masques booléens du filtre des pays individuels.
    data = data.drop_nulls(subset=[config.partner_col, config.value_col])

    # Grille canonique : cellules distinctes de la base
    result = data.select(*keys).unique()

    # Jointure gauche de la sortie de chaque métrique sur la grille
    for metric in metrics:
        # Calcul de la métrique (frame indexé par les clés + colonne metric.name)
        scored = metric.compute(data)
        result = result.join(scored, on=keys, how="left")

    return result


# ──────────────────────────────────────────────────────────────────────
# Lecture de la source / écriture du résultat (DuckLake)
# ──────────────────────────────────────────────────────────────────────

# Fonction de conversion d'un chemin en littéral SQL à slashes avant
def _sql_path(path: Union[str, Path]) -> str:
    """Return a forward-slash string form of a path for SQL literals.

    Args:
        path: Filesystem path.

    Returns:
        The path as a forward-slash string (portable inside DuckDB literals).
    """
    # Slashes avant : portables dans les littéraux DuckDB, y compris sous Windows
    return Path(path).as_posix()


# Fonction de lecture de la table de faits source (DuckDB en lecture seule)
def _read_source_fact_table(
    source_catalog: Union[str, Path],
    source_data_path: Union[str, Path],
    source_schema: str,
    columns: Sequence[str],
) -> pd.DataFrame:
    """Read selected columns of a DuckLake fact table, read-only.

    Deliberately uses a hand-rolled ``ATTACH`` rather than ``DuckLakeConnector``:
    ``DuckLakeConnector.connect()`` fails to re-attach an existing catalog under
    Windows/OneDrive (the catalog stores a normalised ``DATA_PATH`` — lowercase
    drive, forward slashes — that the connector's raw ``str(Path)`` does not
    match), and the connector exposes no ``OVERRIDE_DATA_PATH`` option. The
    direct read-only ``ATTACH`` with ``OVERRIDE_DATA_PATH true`` is the assumed
    workaround. (If a future ``dt-ducklake-manager`` release adds an
    ``override_data_path`` option, switch this read to ``DuckLakeConnector``.)

    Args:
        source_catalog: Path to the source ``.ducklake`` catalog file.
        source_data_path: Directory of the source Parquet data files.
        source_schema: Schema holding the ``fact_table``.
        columns: Columns to project.

    Returns:
        A pandas DataFrame of the projected fact table.
    """
    # Connexion DuckDB en mémoire et chargement de l'extension DuckLake
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        # Attachement en lecture seule ; OVERRIDE_DATA_PATH tolère un chemin de
        # données déplacé/normalisé différemment de celui stocké dans le catalogue
        # (contournement du mismatch DATA_PATH sous Windows/OneDrive).
        conn.execute(
            f"ATTACH 'ducklake:{_sql_path(source_catalog)}' AS src "
            f"(DATA_PATH '{_sql_path(source_data_path)}/', READ_ONLY, "
            f"OVERRIDE_DATA_PATH true)"
        )
        col_list = ", ".join(f'"{c}"' for c in columns)
        return conn.execute(
            f"SELECT {col_list} FROM src.{source_schema}.{_FACT_TABLE}"
        ).df()
    finally:
        conn.close()


# Fonction de détection de l'existence de la fact table d'un schéma
# /!\ Voir si cela ne pourrait pas être mutualisé avec la méthode "_fact_table_exists" de SDMXDownloader dans macroforecast/datasets/core/download.py
def _fact_table_exists(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    schema: str,
) -> bool:
    """Return whether ``{schema}.fact_table`` exists in the attached catalog.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias of the attached catalog.
        schema: Target schema.

    Returns:
        ``True`` if the fact table already exists.
    """
    # Introspection des tables du catalogue attaché
    row = conn.execute(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [catalog_alias, schema, _FACT_TABLE],
    ).fetchone()
    return bool(row and row[0] > 0)


# Fonction d'écriture du résultat dans le catalogue DuckLake (création ou upsert)
# /!\ Voir si cela ne pourrait pas être mutualisé avec la méthode "_write_dataframe" de SDMXDownloader dans macroforecast/datasets/core/download.py
def _write_result(
    result: nw.DataFrame,
    primary_keys: Sequence[str],
    result_catalog: Union[str, Path],
    result_data_path: Union[str, Path],
    result_schema: str,
) -> bool:
    """Create or upsert the result table into the result DuckLake catalog.

    Mirrors :meth:`macroforecast.datasets.core.download.SDMXDownloader._write_dataframe`:
    builds the schema on first encounter, upserts by primary key afterwards. The
    narwhals frame is passed straight to ``DuckLakeTablesBuilder`` /
    ``DatabaseUpdater``

    Args:
        result: Result narwhals frame (grid keys + one column per metric).
        primary_keys: Primary-key columns (the grid keys).
        result_catalog: Path to the result ``.ducklake`` catalog file.
        result_data_path: Directory for the result Parquet data files.
        result_schema: Target schema in the result catalog.

    Returns:
        ``True`` if the schema was created, ``False`` if it was upserted.

    Raises:
        ValueError: If the update operation reports failure.
    """
    # Création du répertoire de données si nécessaire
    Path(result_data_path).mkdir(parents=True, exist_ok=True)
    Path(result_catalog).parent.mkdir(parents=True, exist_ok=True)

    # Connexion au catalogue résultat
    connector = DuckLakeConnector(str(result_catalog), str(result_data_path))
    conn = connector.connect()
    try:
        # Distinction création / mise à jour selon l'existence de la fact table
        if _fact_table_exists(conn, connector.catalog_alias, result_schema):
            # Mise à jour incrémentale (upsert par clé primaire)
            updater = DatabaseUpdater(
                connection=conn,
                categorical_threshold=None,
                schema=result_schema,
            )
            success = updater.update_database(
                result,
                use_transaction=True,
                compact_after_update=True,
            )

            # Vérification que l'opération s'est bien effectuée
            if not success:
                raise ValueError("DatabaseUpdater reported failure for result table")

            # Logging
            logger.info(
                f"Upserted {len(result)} rows into '{result_schema}'"
            )
            return False
        # Première construction : métadonnées + fact table
        builder = DuckLakeTablesBuilder(
            result,
            categorical_threshold=None,
            primary_keys=list(primary_keys),
            connection=conn,
            schema=result_schema,
        )
        builder.build_schema()

        # Logging
        logger.info(
            f"Created schema '{result_schema}' with {len(result)} rows "
            f"(primary keys: {list(primary_keys)})"
        )
        return True
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────
# Orchestration de bout en bout
# ──────────────────────────────────────────────────────────────────────

# Fonction d'orchestration : source DuckLake → métriques → résultat DuckLake
def run_vulnerabilities(
    source_catalog: Union[str, Path],
    source_data_path: Union[str, Path],
    result_catalog: Union[str, Path],
    result_data_path: Union[str, Path],
    *,
    source_schema: str = "DS_045409",
    result_schema: str = "vulnerabilities",
    metrics: Optional[Sequence[VulnerabilityMetric]] = None,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    backend: str = "pandas",
) -> VulnerabilityReport:
    """Compute trade-vulnerability metrics and write them to a result catalog.

    Reads the source fact table, applies every metric over the whole dataset,
    and persists the scores (one column per metric) into the result DuckLake
    catalog, keyed by ``config.key_columns``.

    Args:
        source_catalog: Path to the source ``.ducklake`` catalog file.
        source_data_path: Directory of the source Parquet data files.
        result_catalog: Path to the result ``.ducklake`` catalog file.
        result_data_path: Directory for the result Parquet data files.
        source_schema: Schema of the source ``fact_table``.
        result_schema: Target schema in the result catalog.
        metrics: Metric instances to apply. Defaults to
            :func:`~macroforecast.trade.vulnerabilities.metrics.default_metrics`.
        config: Column and partner-code conventions.
        backend: Native eager backend for narwhals computation (``"pandas"`` or,
            when installed, ``"polars"``/``"pyarrow"``).

    Returns:
        A :class:`VulnerabilityReport` summarising the run.
    """
    # Métriques par défaut si non fournies
    metric_list = list(metrics) if metrics is not None else default_metrics(config)

    # Colonnes nécessaires : clés de la grille + partenaire + valeur
    required = list(
        dict.fromkeys(
            [*config.key_columns, config.partner_col, config.value_col]
        )
    )

    # Lecture de la table de faits source (pandas)
    source_pdf = _read_source_fact_table(
        source_catalog, source_data_path, source_schema, required
    )

    # Bascule éventuelle vers un autre backend eager (polars, pyarrow…)
    if backend == "polars":
        import polars as pl  # Import paresseux : dépendance optionnelle

        native = pl.from_pandas(source_pdf)
    elif backend == "pyarrow":
        import pyarrow as pa  # Import paresseux : dépendance optionnelle

        native = pa.Table.from_pandas(source_pdf)
    else:
        native = source_pdf

    # Calcul des métriques via narwhals (agnostique du backend)
    data = nw.from_native(native, eager_only=True)
    result = compute_vulnerabilities(data, metric_list, config)

    # Écriture dans le catalogue résultat : le frame narwhals est passé tel quel,
    # builder/updater de dt_ducklake_manager acceptant IntoDataFrame (aucune
    # reconversion pandas nécessaire).
    created = _write_result(
        result,
        config.key_columns,
        result_catalog,
        result_data_path,
        result_schema,
    )

    return VulnerabilityReport(
        cells=len(result),
        metrics=[m.name for m in metric_list],
        created=created,
    )


# ──────────────────────────────────────────────────────────────────────
# Orchestration incrémentale (catalogue Postgres partagé source/résultat)
# ──────────────────────────────────────────────────────────────────────
#
# Contrairement à `run_vulnerabilities`/`_read_source_fact_table`, qui
# attachent un fichier catalogue DuckLake local en lecture seule, les fonctions
# ci-dessous s'appuient sur un connecteur `DuckLakeConnector.from_postgres`
# unique : source (schéma des données téléchargées) et résultat (schéma des
# vulnérabilités) sont deux schémas du même catalogue Postgres, donc
# accessibles depuis une seule connexion, sans ATTACH supplémentaire.

# Fonction de lecture filtrée de la table de faits source (couples reporter x produit ciblés) depuis une base de données postgres
# /!\ Retirer le suffixe "_pg" dans le nom de la fonction dans la mesure où il n'y a rien de spécifique à postgres dedans ?
def _read_source_fact_table_pg(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    source_schema: str,
    columns: Sequence[str],
    reporter_col: str,
    product_col: str,
    reporters_products: Sequence[Tuple[str, str]],
) -> pd.DataFrame:
    """Read fact-table rows restricted to given reporter x product pairs.

    Uses a connection already attached to the Postgres-backed catalog (via
    :meth:`DuckLakeConnector.from_postgres`), which exposes every schema of the
    catalog — the source schema is reached by qualified name, no extra
    ``ATTACH`` is required.

    Args:
        conn: Open DuckLake connection (Postgres-backed catalog).
        catalog_alias: Alias under which the catalog is attached.
        source_schema: Schema holding the source ``fact_table``.
        columns: Columns to project.
        reporter_col: Name of the reporter column.
        product_col: Name of the product column.
        reporters_products: Reporter x product pairs to restrict the read to.
            Must be non-empty.

    Returns:
        A pandas DataFrame of the filtered fact table.
    """
    # Projection des colonnes demandées
    col_list = ", ".join(f'"{c}"' for c in columns)
    # Prédicat SQL sur les couples (reporter, produit) ciblés
    values_clause = ", ".join(f"({r!r}, {p!r})" for r, p in reporters_products)
    return conn.execute(
        f'SELECT {col_list} FROM "{catalog_alias}"."{source_schema}"."{_FACT_TABLE}" '
        f'WHERE ("{reporter_col}", "{product_col}") IN ({values_clause})'
    ).df()


# Fonction d'écriture du résultat via une connexion déjà ouverte (catalogue Postgres partagé)
# /!\ Retirer le suffixe "_pg" dans le nom de la fonction dans la mesure où il n'y a rien de spécifique à postgres dedans ?
# /!\ Voir si cela ne pourrait pas être mutualisé avec la méthode "_write_dataframe" de SDMXDownloader dans macroforecast/datasets/core/download.py
def _write_result_pg(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    result: nw.DataFrame,
    primary_keys: Sequence[str],
    result_schema: str,
) -> bool:
    """Create or upsert the result table using an already-open DuckLake connection.

    Same create-then-upsert logic as :func:`_write_result`, reused here on a
    connection already attached to the shared Postgres catalog rather than
    opening a dedicated local-catalog connection.

    Args:
        conn: Open DuckLake connection (Postgres-backed catalog).
        catalog_alias: Alias under which the catalog is attached.
        result: Result narwhals frame (grid keys + one column per metric).
        primary_keys: Primary-key columns (the grid keys).
        result_schema: Target schema in the shared catalog.

    Returns:
        ``True`` if the schema was created, ``False`` if it was upserted.

    Raises:
        ValueError: If the update operation reports failure.
    """
    # Mise à jour de la table si elle existe déjà
    if _fact_table_exists(conn, catalog_alias, result_schema):
        # Mise à jour incrémentale (upsert par clé primaire) : ne touche que les
        # cellules recalculées, le reste de la table résultat est préservé.
        updater = DatabaseUpdater(
            connection=conn,
            categorical_threshold=None,
            schema=result_schema,
        )
        success = updater.update_database(
            result,
            use_transaction=True,
            compact_after_update=True,
        )
        # Vérification de la bonne réalisation de la mise à jour
        if not success:
            raise ValueError("DatabaseUpdater reported failure for result table")
        # Logging
        logger.info(f"Upserted {len(result)} rows into '{result_schema}'")
        return False

    # Première construction : métadonnées + fact table
    # Initialisation de la classe
    builder = DuckLakeTablesBuilder(
        result,
        categorical_threshold=None,
        primary_keys=list(primary_keys),
        connection=conn,
        schema=result_schema,
    )
    # Construction du schéma
    builder.build_schema()
    # Logging
    logger.info(
        f"Created schema '{result_schema}' with {len(result)} rows "
        f"(primary keys: {list(primary_keys)})"
    )
    return True


# Fonction d'orchestration incrémentale : catalogue Postgres partagé (source + résultat)
# /!\ Retirer les mentions à postgres dans la fonction dans la mesure où il n'y a rien de spécifique à postgres dedans ?
def run_vulnerabilities_incremental(
    connector: DuckLakeConnector,
    reporters_products: Set[Tuple[str, str]],
    *,
    result_connector: Optional[DuckLakeConnector] = None,
    source_schema: str = "DS045409",
    result_schema: str = "vulnerabilities",
    metrics: Optional[Sequence[VulnerabilityMetric]] = None,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    backend: str = "pandas",
) -> VulnerabilityReport:
    """Recompute vulnerability metrics for a subset of reporter x product cells.

    Production/incremental counterpart of :func:`run_vulnerabilities`. By
    default, source and result live as two schemas of the *same*
    Postgres-backed DuckLake catalog and share a single connection. Pass
    ``result_connector`` to target a different catalog for the result — a
    second connection is then opened for the write.

    Args:
        connector: DuckLake connector for the source catalog (see
            :meth:`DuckLakeConnector.from_postgres`).
        reporters_products: Reporter x product pairs to (re)compute. An empty
            set is a no-op (nothing is read or written).
        result_connector: DuckLake connector for the result catalog, if it
            differs from the source catalog. Defaults to ``connector``.
        source_schema: Schema holding the source ``fact_table``.
        result_schema: Schema to create or upsert the result into.
        metrics: Metric instances to apply. Defaults to
            :func:`~macroforecast.trade.vulnerabilities.metrics.default_metrics`.
        config: Column and partner-code conventions.
        backend: Native eager backend for narwhals computation.

    Returns:
        A :class:`VulnerabilityReport` summarising the run.
    """
    # Initialisation de la liste des métriques
    metric_list = list(metrics) if metrics is not None else default_metrics(config)

    # Rien à recalculer : sortie anticipée sans ouvrir de connexion
    if not reporters_products:
        logger.info("No reporter-product pairs to recalculate; early termination.")
        return VulnerabilityReport(
            cells=0, metrics=[m.name for m in metric_list], created=False
        )

    # Colonnes nécessaires : clés de la grille + partenaire + valeur
    required = list(
        dict.fromkeys([*config.key_columns, config.partner_col, config.value_col])
    )

    # Catalogue résultat : partagé avec la source par défaut (comportement historique)
    result_connector = result_connector or connector
    # Détection d'un catalogue résultat distinct : impose deux connexions séparées
    same_catalog = result_connector is connector

    # Connexion au catalogue source
    source_conn = connector.connect()
    try:
        # Lecture des seules lignes concernées par les couples reporter x produit ciblés
        source_pdf = _read_source_fact_table_pg(
            source_conn,
            connector.catalog_alias,
            source_schema,
            required,
            reporter_col="reporter",
            product_col="product",
            reporters_products=list(reporters_products),
        )

        # Conversion en narwhals et calcul des métriques
        data = nw.from_native(source_pdf, eager_only=True)
        result = compute_vulnerabilities(data, metric_list, config)

        if same_catalog:
            # Écriture sur la connexion déjà ouverte (même catalogue que la source)
            created = _write_result_pg(
                source_conn,
                connector.catalog_alias,
                result,
                config.key_columns,
                result_schema,
            )
        else:
            # Catalogue résultat distinct : ouverture d'une seconde connexion dédiée
            result_conn = result_connector.connect()
            try:
                created = _write_result_pg(
                    result_conn,
                    result_connector.catalog_alias,
                    result,
                    config.key_columns,
                    result_schema,
                )
            finally:
                result_conn.close()
    finally:
        source_conn.close()

    return VulnerabilityReport(
        cells=len(result),
        metrics=[m.name for m in metric_list],
        created=created,
    )
