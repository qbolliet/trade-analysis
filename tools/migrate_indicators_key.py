"""Recréation de la table ``indicators`` avec la clé de nomenclature.

La table des métriques partenaires était indexée par
``(freq, reporter, product, flow, indicators, TIME_PERIOD)``. Elle porte désormais
des lignes de plusieurs nomenclatures pour une même cellule : les lignes « en
vigueur » (codes tels que déclarés) et les lignes « historiques » (flux des
années postérieures convertis vers un millésime SH antérieur). Sa clé primaire
gagne donc ``classification`` en tête, et trois colonnes s'ajoutent :
``hs_vintage`` (millésime SH de rattachement), ``in_force`` et
``is_provisional``. Une clé primaire ne se modifie pas en place : la table est
recréée à partir de l'ancienne.

Toutes les lignes existantes sont des lignes en vigueur. Les colonnes sont
dérivées par les macros SQL générées depuis ``runtime.NOMENCLATURES.HS`` :
``classification = classification_of(product, année)`` (millésime en vigueur,
``CN<année>`` pour un code à 8 chiffres), ``hs_vintage = vintage_in_force(année)``,
``in_force = true``, ``is_provisional`` = drapeau ``IS_PROVISIONAL`` du profil.
Aucune métrique n'est recalculée : les valeurs sont recopiées telles quelles.

Déroulé (idempotent : une table déjà migrée n'est pas touchée) :

1. copie de toutes les tables du schéma dans un schéma de sauvegarde, puis
   suppression des tables d'origine (une transaction) ;
2. reconstruction par lots (un reporter par lot, mémoire bornée), table
   partitionnée par ``classification`` à sa création ;
3. contrôles : même nombre de lignes, clé primaire unique ; en cas d'écart, la
   sauvegarde est conservée et l'outil sort en erreur ;
4. suppression de la sauvegarde, sauf ``--keep-backup``.

Options :

- ``--drop-dependent`` : suppression des tables de synthèse et de cohérence,
  dont le contexte gagne ``hs_vintage`` (clé changée, contenu recalculé en
  entier à l'exécution suivante, l'empreinte de la synthèse ayant changé) ;
- ``--network`` : ajout et remplissage de ``network_indicators.in_force``
  (``classification = vintage_in_force(year)``) sans recalcul du réseau.

À exécuter depuis un service Onyxia (catalogue PostgreSQL et Parquet réels),
**entre deux exécutions** de l'étape partenaires, profil choisi par les mêmes
variables d'environnement que les scripts (``VULNERABILITIES_CONFIG_PATH``,
``SYNTHESIS_CONFIG_PATH``, ``EUROSTAT_CONFIG_PATH``, ``RUNTIME_CONFIG_PATH``).

Examples:
    Aperçu, puis migration du profil de démonstration::

        $ VULNERABILITIES_CONFIG_PATH=config/profiles/demo/vulnerabilities.yaml \\
          SYNTHESIS_CONFIG_PATH=config/profiles/demo/synthesis.yaml \\
          uv run python tools/migrate_indicators_key.py --dry-run
        $ ... uv run python tools/migrate_indicators_key.py --drop-dependent --network
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import argparse
from dataclasses import dataclass, field
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence

# Modules du pipeline
from kedro_pipeline.config import nomenclature_macros_sql

# Initialisation du logger
logger = logging.getLogger(__name__)

# Colonnes de nomenclature ajoutées (mêmes noms que l'étape partenaires)
CLASSIFICATION_COL = "classification"
NETWORK_IN_FORCE_COL = "in_force"
# Suffixe du schéma de sauvegarde
BACKUP_SUFFIX = "__before_classification"
# Expression de l'année d'une période dans les requêtes
_YEAR_SQL = 'CAST(substr(CAST("{period}" AS VARCHAR), 1, 4) AS INTEGER)'


# Classe de rapport de migration
@dataclass
class MigrationReport:
    """Outcome of :func:`migrate_indicators`.

    Attributes:
        status: ``"absent"`` (no table), ``"already_migrated"``, ``"planned"``
            (dry run) or ``"migrated"``.
        n_rows: Rows of the table before migration.
        n_batches: Number of write batches.
        backup_schema: Backup schema (``None`` once dropped, or when nothing
            was done).
        primary_keys: Primary key of the recreated table.
    """

    status: str
    n_rows: int = 0
    n_batches: int = 0
    backup_schema: Optional[str] = None
    primary_keys: List[str] = field(default_factory=list)


# Fonction de citation d'un nom qualifié
def _qualified(catalog_alias: str, schema: str, table: str) -> str:
    """Return the quoted qualified name of a table."""
    return f'"{catalog_alias}"."{schema}"."{table}"'


# Fonction de listage des tables d'un schéma
def schema_tables(conn: Any, catalog_alias: str, schema: str) -> List[str]:
    """List the tables of a schema of an attached catalog.

    Args:
        conn: Open connection.
        catalog_alias: Alias of the catalog.
        schema: Schema name.

    Returns:
        Table names, sorted.
    """
    rows = conn.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = ?",
        [catalog_alias, schema],
    ).fetchall()
    return sorted(row[0] for row in rows)


# Fonction de lecture des colonnes d'une table
def table_columns(conn: Any, catalog_alias: str, schema: str, table: str = "fact_table") -> List[str]:
    """Return the column names of a table, in order."""
    rows = conn.execute(f"DESCRIBE {_qualified(catalog_alias, schema, table)}").fetchall()
    return [row[0] for row in rows]


# Fonction de lecture de la clé primaire d'une table construite par dt_ducklake_manager
def primary_keys(conn: Any, catalog_alias: str, schema: str) -> List[str]:
    """Return the primary key recorded in the ``metadata`` table of a schema.

    Args:
        conn: Open connection.
        catalog_alias: Alias of the catalog.
        schema: Schema name.

    Returns:
        Primary-key columns, in the order of the fact table.
    """
    flagged = {
        row[0]
        for row in conn.execute(
            f"SELECT name FROM {_qualified(catalog_alias, schema, 'metadata')} WHERE is_primary_key"
        ).fetchall()
    }
    return [column for column in table_columns(conn, catalog_alias, schema) if column in flagged]


# Fonction de suppression de toutes les tables d'un schéma
def drop_schema_tables(conn: Any, catalog_alias: str, schema: str) -> List[str]:
    """Drop every table of a schema, then the schema itself.

    Args:
        conn: Open connection.
        catalog_alias: Alias of the catalog.
        schema: Schema to empty.

    Returns:
        The tables dropped (empty when the schema holds none).
    """
    tables = schema_tables(conn, catalog_alias, schema)
    for table in tables:
        conn.execute(f"DROP TABLE {_qualified(catalog_alias, schema, table)}")
    if tables:
        conn.execute(f'DROP SCHEMA IF EXISTS "{catalog_alias}"."{schema}"')
    return tables


# Fonction de migration de la table des métriques partenaires
def migrate_indicators(
    conn: Any,
    *,
    catalog_alias: str,
    schema: str,
    nomenclatures: Mapping[str, int],
    key_columns: Sequence[str],
    is_provisional: bool,
    batch_column: str = "reporter",
    product_col: str = "product",
    period_col: str = "TIME_PERIOD",
    backup_schema: Optional[str] = None,
    keep_backup: bool = False,
    dry_run: bool = False,
) -> MigrationReport:
    """Recreate the partner table with ``classification`` in its primary key.

    Args:
        conn: Open connection on the result catalog (owned by the caller).
        catalog_alias: Alias of the result catalog.
        schema: Schema of the partner table.
        nomenclatures: Mapping vintage label -> entry-into-force year.
        key_columns: Former primary key (cell key without classification).
        is_provisional: Value of the new ``is_provisional`` column.
        batch_column: Column splitting the rewrite into batches.
        product_col: Product column.
        period_col: Period column.
        backup_schema: Backup schema; ``<schema>__before_classification`` by
            default.
        keep_backup: Whether to keep the backup once the checks passed.
        dry_run: Only report what would be done.

    Returns:
        The migration report.

    Raises:
        RuntimeError: If the backup schema already holds tables, or if a check
            fails after the rewrite (the backup is then kept).
    """
    from statflows.storage.ducklake.tables import fact_table_exists, write_dataframe

    # Table absente ou déjà migrée : rien à faire (idempotence)
    if not fact_table_exists(conn, catalog_alias, schema):
        return MigrationReport("absent")
    if CLASSIFICATION_COL in table_columns(conn, catalog_alias, schema):
        return MigrationReport("already_migrated", primary_keys=primary_keys(conn, catalog_alias, schema))

    backup = backup_schema or f"{schema}{BACKUP_SUFFIX}"
    new_keys = [CLASSIFICATION_COL, *[key for key in key_columns if key != CLASSIFICATION_COL]]
    source = _qualified(catalog_alias, schema, "fact_table")
    n_rows = int(conn.execute(f"SELECT count(*) FROM {source}").fetchone()[0])
    batches = [
        row[0]
        for row in conn.execute(
            f'SELECT DISTINCT "{batch_column}" FROM {source} ORDER BY 1 NULLS LAST'
        ).fetchall()
    ]
    if dry_run:
        return MigrationReport("planned", n_rows, len(batches), backup, new_keys)

    # Sauvegarde puis suppression des tables d'origine, en une transaction
    if schema_tables(conn, catalog_alias, backup):
        raise RuntimeError(
            f"Backup schema '{backup}' already holds tables: restore or drop it first"
        )
    tables = schema_tables(conn, catalog_alias, schema)
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog_alias}"."{backup}"')
        for table in tables:
            conn.execute(
                f"CREATE TABLE {_qualified(catalog_alias, backup, table)} AS "
                f"SELECT * FROM {_qualified(catalog_alias, schema, table)}"
            )
        for table in tables:
            conn.execute(f"DROP TABLE {_qualified(catalog_alias, schema, table)}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    # Logging
    logger.info(f"{schema} : {len(tables)} table(s) sauvegardée(s) dans {backup}")

    # Reconstruction par lots, colonnes dérivées par les macros du référentiel
    for statement in nomenclature_macros_sql(nomenclatures):
        conn.execute(statement)
    year = _YEAR_SQL.format(period=period_col)
    query = (
        # Classification en tête : ordre des colonnes de la clé primaire
        f"SELECT classification_of(\"{product_col}\", {year}) AS \"{CLASSIFICATION_COL}\", *, "
        f"vintage_in_force({year}) AS hs_vintage, TRUE AS in_force, "
        f"{'TRUE' if is_provisional else 'FALSE'} AS is_provisional "
        f"FROM {_qualified(catalog_alias, backup, 'fact_table')} "
        f'WHERE "{batch_column}" IS NOT DISTINCT FROM ?'
    )
    for value in batches:
        df_batch = conn.execute(query, [value]).df()
        write_dataframe(
            conn,
            df_batch,
            new_keys,
            catalog_alias=catalog_alias,
            schema=schema,
            build_options={"partition_by": [CLASSIFICATION_COL]},
            update_options={"compact_after_update": False},
            commit_message=f"migrate_indicators_key {batch_column}={value}",
        )

    # Contrôles : volumétrie et unicité de la nouvelle clé
    n_after = int(conn.execute(f"SELECT count(*) FROM {source}").fetchone()[0])
    keys = ", ".join(f'"{key}"' for key in new_keys)
    n_distinct = int(
        conn.execute(f"SELECT count(*) FROM (SELECT DISTINCT {keys} FROM {source})").fetchone()[0]
    )
    if n_after != n_rows or n_distinct != n_after:
        raise RuntimeError(
            f"Migration check failed on '{schema}': {n_rows} rows before, {n_after} after, "
            f"{n_distinct} distinct keys. Backup kept in '{backup}'."
        )

    # Suppression de la sauvegarde, sauf demande contraire
    if not keep_backup:
        drop_schema_tables(conn, catalog_alias, backup)
        backup_kept: Optional[str] = None
    else:
        backup_kept = backup
    # Logging
    logger.info(f"{schema} : {n_rows} ligne(s) recréée(s) en {len(batches)} lot(s), clé {new_keys}")
    return MigrationReport("migrated", n_rows, len(batches), backup_kept, new_keys)


# Fonction d'ajout du drapeau « millésime en vigueur » à la table réseau
def backfill_network_in_force(
    conn: Any,
    *,
    catalog_alias: str,
    schema: str,
    nomenclatures: Mapping[str, int],
    classification_col: str = "classification",
    period_col: str = "year",
) -> int:
    """Add ``in_force`` to the network table without recomputing it.

    ``in_force`` tells whether a row's vintage is the one in force its year.
    Idempotent: nothing is done when the column already exists.

    Args:
        conn: Open connection on the result catalog.
        catalog_alias: Alias of the result catalog.
        schema: Schema of the network table.
        nomenclatures: Mapping vintage label -> entry-into-force year.
        classification_col: Classification column.
        period_col: Year column.

    Returns:
        Number of rows filled (0 when the table is absent or already filled).
    """
    from dt_ducklake_manager import DatabaseUpdater
    from statflows.storage.ducklake.tables import fact_table_exists

    if not fact_table_exists(conn, catalog_alias, schema):
        return 0
    if NETWORK_IN_FORCE_COL in table_columns(conn, catalog_alias, schema):
        return 0
    for statement in nomenclature_macros_sql(nomenclatures):
        conn.execute(statement)
    keys = primary_keys(conn, catalog_alias, schema)
    selected = ", ".join(f'"{key}"' for key in keys)
    df_flags = conn.execute(
        f'SELECT {selected}, "{classification_col}" = vintage_in_force("{period_col}") '
        f'AS "{NETWORK_IN_FORCE_COL}" FROM {_qualified(catalog_alias, schema, "fact_table")}'
    ).df()
    DatabaseUpdater(connection=conn, catalog_alias=catalog_alias, schema=schema).add_columns(
        df_flags, compact_after_update=False, commit_message="migrate_indicators_key network in_force"
    )
    return len(df_flags)


# Fonction principale
def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point (see the module docstring).

    Args:
        argv: Command-line arguments (``sys.argv`` when ``None``).

    Raises:
        RuntimeError: If a migration check fails.
    """
    from statflows.core.download import _schema_name

    from kedro_pipeline.io.ducklake import (
        DuckLakeLocation,
        build_connector,
        pg_credentials_from_env,
        s3_credentials_from_env,
    )
    from scripts.compute_synthetic_scores import load_synthesis_config
    from scripts.compute_trade_vulnerabilities import (
        load_eurostat_config,
        load_vulnerability_config,
        vulnerability_config_from_params,
    )
    from scripts.download_comtrade import load_runtime_config

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="Report without writing.")
    parser.add_argument("--keep-backup", action="store_true", help="Keep the backup schema.")
    parser.add_argument(
        "--drop-dependent", action="store_true",
        help="Drop the synthesis and coherence tables (context key changed).",
    )
    parser.add_argument(
        "--network", action="store_true", help="Add network_indicators.in_force."
    )
    args = parser.parse_args(argv)
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    # Configurations du profil (mêmes variables d'environnement que les scripts)
    eurostat_config = load_eurostat_config()
    vulnerability_config = load_vulnerability_config()
    runtime_config = load_runtime_config()
    nomenclatures = runtime_config["NOMENCLATURES"]["HS"]
    catalog = vulnerability_config["VULNERABILITIES"]
    block = catalog[eurostat_config["DATAFLOW"]]
    config = vulnerability_config_from_params(vulnerability_config.get("PARAMETERS"))
    schema = _schema_name(block["RESULT_SCHEMA"])

    connector = build_connector(
        DuckLakeLocation(
            dbname=catalog["DBNAME"],
            catalog_alias=catalog["CATALOG_ALIAS"],
            schema=schema,
            bucket=block["BUCKET"],
            data_path=block["PATHS"]["DATA_PATH"],
        ),
        pg=pg_credentials_from_env(),
        s3=s3_credentials_from_env(),
    )
    conn = connector.connect()
    try:
        report = migrate_indicators(
            conn,
            catalog_alias=connector.catalog_alias,
            schema=schema,
            nomenclatures=nomenclatures,
            key_columns=config.key_columns,
            is_provisional=bool(block.get("IS_PROVISIONAL", False)),
            batch_column=config.reporter_col,
            product_col=config.product_col,
            period_col=config.period_col,
            keep_backup=args.keep_backup,
            dry_run=args.dry_run,
        )
        logger.info(f"indicators : {report}")
        if args.drop_dependent:
            synthesis_config = load_synthesis_config()
            for name in ("SYNTHESIS", "COHERENCE"):
                dependent = _schema_name(synthesis_config[name]["RESULT_SCHEMA"])
                if args.dry_run:
                    logger.info(f"{dependent} : tables à supprimer {schema_tables(conn, connector.catalog_alias, dependent)}")
                else:
                    logger.info(f"{dependent} : supprimées {drop_schema_tables(conn, connector.catalog_alias, dependent)}")
        if args.network:
            network = _schema_name(vulnerability_config["NETWORK_VULNERABILITIES"]["RESULT_SCHEMA"])
            if args.dry_run:
                logger.info(f"{network} : colonnes {table_columns(conn, connector.catalog_alias, network)}")
            else:
                filled = backfill_network_in_force(
                    conn, catalog_alias=connector.catalog_alias, schema=network,
                    nomenclatures=nomenclatures,
                )
                logger.info(f"{network} : in_force renseigné sur {filled} ligne(s)")
    finally:
        conn.close()


# Exécution du script principal
if __name__ == "__main__":
    main()
