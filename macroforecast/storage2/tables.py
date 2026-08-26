"""Shared DuckLake table helpers.

Gathers the create-then-upsert logic that every result-producing step of the
pipeline used to duplicate: the download orchestrator
(:mod:`macroforecast.datasets.core.download`), the vulnerability runner
(:mod:`macroforecast.trade.vulnerabilities.runner`) and the BACI processing
scripts.

Both helpers take an **already-open** DuckDB connection.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence
# Module de manipulation de la base de données
import duckdb
# Module de gestion des tables DuckLake
from dt_ducklake_manager import DatabaseUpdater, DuckLakeTablesBuilder

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de la table de faits DuckLake (convention dt_ducklake_manager)
FACT_TABLE = "fact_table"


# Gestionnaire de contexte de positionnement de la connexion sur un catalogue
@contextmanager
def _current_catalog(
    conn: duckdb.DuckDBPyConnection, catalog_alias: str
) -> Iterator[None]:
    """Temporarily make ``catalog_alias`` the connection's current catalog.

    Some ``dt_ducklake_manager`` writers qualify their DDL with the schema only,
    which resolves against whichever catalog the connection currently points at.
    This context manager pins that catalog for the duration of the block and
    restores the previous ``catalog.schema`` position afterwards, so the caller's
    session is left exactly as it was found.

    Args:
        conn: Open DuckDB connection.
        catalog_alias: Alias of the catalog to activate.

    Yields:
        ``None``, with the connection positioned on ``catalog_alias``.
    """
    # Position courante, restaurée en sortie de bloc
    previous_catalog, previous_schema = conn.execute(
        "SELECT current_database(), current_schema()"
    ).fetchone()
    # Court-circuit : la connexion est déjà sur le bon catalogue
    if previous_catalog == catalog_alias:
        yield
        return

    conn.execute(f"USE {catalog_alias}")
    try:
        yield
    finally:
        conn.execute(f"USE {previous_catalog}.{previous_schema}")


# Fonction de détection de l'existence de la table de faits d'un schéma
def fact_table_exists(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    schema: str,
    *,
    table: str = FACT_TABLE,
) -> bool:
    """Return whether ``{schema}.{table}`` exists in the attached catalog.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias under which the catalog is attached.
        schema: Target schema.
        table: Table name to look for. Defaults to the DuckLake fact table.

    Returns:
        ``True`` if the table already exists in that schema.

    Examples:
        >>> conn = duckdb.connect(":memory:")
        >>> fact_table_exists(conn, "db", "vulnerabilities")
        False
        >>> conn.close()
    """
    # Introspection des tables du catalogue attaché
    row = conn.execute(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [catalog_alias, schema, table],
    ).fetchone()
    return bool(row and row[0] > 0)


# Fonction d'écriture d'un jeu de données dans un schéma DuckLake (création ou upsert)
def write_dataframe(
    conn: duckdb.DuckDBPyConnection,
    data: Any,
    primary_keys: Sequence[str],
    *,
    catalog_alias: str,
    schema: str,
    categorical_threshold: Optional[int] = None,
    label: Optional[str] = None,
) -> bool:
    """Create the schema on first encounter, upsert by primary key afterwards.

    The distinction is made on the sole existence of the fact table, so the same
    call initialises a brand-new schema and incrementally updates an existing
    one — no caller ever has to branch on it.

    Args:
        conn: Open DuckLake connection, owned by the caller.
        data: Dataset to persist. Any ``IntoDataFrame`` accepted by
            ``dt_ducklake_manager`` (pandas, polars or narwhals frame), passed
            through without conversion.
        primary_keys: Primary-key columns, used both to build the schema and to
            upsert onto it.
        catalog_alias: Alias under which the catalog is attached.
        schema: Target schema in the catalog.
        categorical_threshold: Maximum cardinality for a column to be turned
            into a dimension table. ``None`` disables dimension tables.
        label: Optional prefix identifying the run in the logs (dataflow,
            vintage…).

    Returns:
        ``True`` if the schema was created, ``False`` if it was upserted.

    Raises:
        ValueError: If the update operation reports failure.
    """
    # Préfixe de journalisation : identifie le jeu de données écrit
    prefix = f"{label}: " if label else ""

    # Les écrivains de dt_ducklake_manager ne qualifient leurs identifiants que
    # du schéma : leurs requêtes se résolvent donc dans le catalogue *courant*
    # de la connexion, pas dans `catalog_alias` (que DatabaseUpdater ne consulte
    # que pour ses appels de maintenance). Positionnement explicite sur le
    # catalogue cible, restauré en sortie, pour que l'argument fasse autorité.
    with _current_catalog(conn, catalog_alias):
        # Mise à jour de la table si elle existe déjà
        if fact_table_exists(conn, catalog_alias, schema):
            # Mise à jour incrémentale (upsert par clé primaire) : ne touche que
            # les lignes fournies, le reste de la table est préservé.
            updater = DatabaseUpdater(
                connection=conn,
                categorical_threshold=categorical_threshold,
                ducklake_catalog_alias=catalog_alias,
                schema=schema,
            )
            success = updater.update_database(
                data,
                use_transaction=True,
                compact_after_update=True,
            )

            # Vérification de la bonne réalisation de la mise à jour
            if not success:
                raise ValueError(
                    f"{prefix}DatabaseUpdater reported failure for schema '{schema}'"
                )

            # Logging
            logger.info(f"{prefix}Upserted {len(data)} rows into '{schema}'")
            return False

        # Première construction : métadonnées, dimensions et table de faits
        builder = DuckLakeTablesBuilder(
            data,
            categorical_threshold=categorical_threshold,
            primary_keys=list(primary_keys),
            connection=conn,
            schema=schema,
        )
        builder.build_schema()

    # Logging
    logger.info(
        f"{prefix}Created schema '{schema}' with {len(data)} rows "
        f"(primary keys: {list(primary_keys)})"
    )
    return True
