"""Serving catalog: DuckLake tables read directly by Superset (PS-29.1, PD-21).

:class:`ServingCatalog` is a lazy handle: nothing connects at instantiation.
:meth:`ServingCatalog.connect` opens one DuckDB session that attaches the
``serving`` catalog for writing and the source catalogs (``eurostat``,
``comtrade``, ``vulnerabilities``) ``READ_ONLY``, so the serving queries
reference them by alias. :meth:`ServingCatalog.publish` materialises every
table of a publication in **one** transaction: a concurrent reader (Superset)
keeps seeing the previous snapshot until ``COMMIT``, and any failure rolls the
whole publication back.

Behaviour checked on DuckDB 1.5.3 with a file catalog (ARCH PS-29.1):
``CREATE OR REPLACE TABLE … LIMIT 0`` + ``ALTER TABLE … SET PARTITIONED BY`` +
``INSERT … ORDER BY`` on several tables inside one transaction, snapshot
isolation of a concurrent reader, full ``ROLLBACK``, ``DELETE`` + ``INSERT`` of
some partitions, and file pruning on the partition column.
"""
# Importation des modules
# Modules de base
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, Mapping, Optional, Sequence, Set

# Modules internes
from kedro_pipeline.io.ducklake import DuckLakeLocation, build_connector

# Logger
logger = logging.getLogger(__name__)

# Modes de publication
MODES = ("full", "by_year")


# Exception levée lorsqu'une publication échoue (après ROLLBACK complet)
class ServingPublicationError(RuntimeError):
    """Raised when a publication fails; the whole transaction was rolled back.

    Attributes:
        table: Name of the serving table being written when the failure
            occurred (``None`` for a failure outside a table, e.g. at commit).
    """

    def __init__(self, message: str, table: Optional[str] = None) -> None:
        super().__init__(message)
        self.table = table


# Description déclarative d'une table de service
@dataclass(frozen=True)
class ServingTableSpec:
    """One serving table: its rendered query and its physical layout.

    Attributes:
        name: Table name in the serving schema (e.g. ``"cell_scores"``).
        sql: Rendered ``SELECT`` query producing the table.
        partitioned: Whether the table is partitioned by ``partition_by``
            (only partitioned tables support the ``by_year`` mode).
        sort_by: Insertion order (min/max statistics prune the files read).
        partition_by: Partition column; also the column filtered by
            ``by_year``.

    Examples:
        >>> spec = ServingTableSpec("t", "SELECT 2024 AS year", partitioned=True)
        >>> spec.partition_by
        'year'
    """

    name: str
    sql: str
    partitioned: bool = False
    sort_by: Sequence[str] = ()
    partition_by: str = "year"


# Statistiques d'écriture d'une table
@dataclass
class TableStats:
    """Outcome of the publication of one table.

    Attributes:
        rows: Row count of the table after the publication.
        seconds: Time spent writing the table.
        files: Number of Parquet data files of the table after ``COMMIT``.
        mode: Mode actually applied (``"full"`` or ``"by_year"``).
    """

    rows: int = 0
    seconds: float = 0.0
    files: int = 0
    mode: str = "full"

    # Méthode de conversion en métriques (préfixe « serving/<table>/ »)
    def to_metrics(self, table: str) -> Dict[str, float]:
        """Return the MLflow metrics of the table.

        Args:
            table: Serving table name.

        Returns:
            ``serving/<table>/rows``, ``…/seconds`` and ``…/files``.

        Examples:
            >>> TableStats(rows=3, seconds=0.5, files=1).to_metrics("t")["serving/t/rows"]
            3.0
        """
        return {
            f"serving/{table}/rows": float(self.rows),
            f"serving/{table}/seconds": float(self.seconds),
            f"serving/{table}/files": float(self.files),
        }


# Fonction de nettoyage d'une requête (point-virgule final)
def _strip_sql(sql: str) -> str:
    """Remove surrounding whitespace and trailing semicolons of a query."""
    return sql.strip().rstrip(";").strip()


# Fonction de citation d'un identifiant SQL
def _quote(identifier: str) -> str:
    """Quote a SQL identifier (double quotes escaped)."""
    return '"' + str(identifier).replace('"', '""') + '"'


# Poignée sur le catalogue de service
@dataclass
class ServingCatalog:
    """Lazy handle on the ``serving`` DuckLake catalog and its read-only sources.

    Args:
        location: Serving catalog (``dbname`` ``serving``, ``schema``
            ``serving.SCHEMA``, data under ``trade/datasets/serving/``).
        pg: PostgreSQL credentials (``pg_credentials_from_env``); the same
            ``trade-postgres-credentials`` as every other catalog.
        s3: S3 credentials (``s3_credentials_from_env``).
        sources: Source catalogs attached ``READ_ONLY``, keyed by alias.
        connector_factory: Builds an unconnected connector from
            ``(location, pg, s3, create_db_if_missing=…, read_only=…)``;
            :func:`~kedro_pipeline.io.ducklake.build_connector` by default,
            a file-catalog factory in tests.

    Examples:
        >>> catalog = ServingCatalog(location, pg, s3, sources)  # doctest: +SKIP
        >>> with catalog.connect() as conn:  # doctest: +SKIP
        ...     stats = catalog.publish(specs, "full", conn=conn)
    """

    location: DuckLakeLocation
    pg: Optional[Mapping[str, Any]]
    s3: Optional[Mapping[str, Any]]
    sources: Mapping[str, DuckLakeLocation] = field(default_factory=dict)
    connector_factory: Callable[..., Any] = build_connector

    # Propriété : nom qualifié du schéma de service
    @property
    def schema(self) -> str:
        """Qualified serving schema (``alias.schema``)."""
        return f"{self.location.catalog_alias}.{_quote(self.location.schema)}"

    # Méthode de construction du nom qualifié d'une table
    def qualified_name(self, table: str) -> str:
        """Return the qualified name of a serving table.

        Args:
            table: Table name.

        Returns:
            ``alias."schema"."table"``.
        """
        return f"{self.schema}.{_quote(table)}"

    # Gestionnaire de contexte : session attachée au catalogue et aux sources
    @contextmanager
    def connect(self) -> Iterator[Any]:
        """Open a DuckDB session attached to ``serving`` (write) and the sources.

        The serving catalog database is created if missing, as for every
        other catalog; the serving schema is created and activated. Sources
        are attached ``READ_ONLY`` without changing the current schema.

        Yields:
            The open DuckDB connection, closed on exit.
        """
        serving = self.connector_factory(
            self.location, self.pg, self.s3, create_db_if_missing=True
        )
        conn = serving.connect()
        try:
            for alias, source in self.sources.items():
                connector = self.connector_factory(
                    source, self.pg, self.s3, create_db_if_missing=False, read_only=True
                )
                # Catalogue secondaire : le USE du schéma de service est conservé
                connector.attach(conn, activate_schema=False)
                logger.info(f"Catalogue source '{alias}' attaché en lecture seule.")
            yield conn
        finally:
            conn.close()

    # Méthode de listage des tables présentes dans la session
    @staticmethod
    def existing_tables(conn: Any) -> Set[str]:
        """Return the tables visible in the session, as ``catalog.schema.table``.

        Args:
            conn: Open session (see :meth:`connect`).

        Returns:
            Unquoted qualified names of every table of every attached catalog.
        """
        rows = conn.execute(
            "SELECT database_name, schema_name, table_name FROM duckdb_tables()"
        ).fetchall()
        return {f"{database}.{schema}.{table}" for database, schema, table in rows}

    # Méthode de test d'existence d'une table de service
    def _table_exists(self, conn: Any, table: str) -> bool:
        """Tell whether a serving table exists (seen from the current transaction)."""
        return (
            f"{self.location.catalog_alias}.{self.location.schema}.{table}"
            in self.existing_tables(conn)
        )

    # Méthode de comparaison des colonnes produites et des colonnes existantes
    def _same_columns(self, conn: Any, spec: ServingTableSpec) -> bool:
        """Tell whether the query produces exactly the columns (names, types) of the table."""
        produced = conn.execute(
            f"DESCRIBE SELECT * FROM ({_strip_sql(spec.sql)})"
        ).fetchall()
        existing = conn.execute(
            f"DESCRIBE SELECT * FROM {self.qualified_name(spec.name)}"
        ).fetchall()
        return [row[:2] for row in produced] == [row[:2] for row in existing]

    # Méthode de comptage des fichiers de données d'une table
    def count_files(self, conn: Any, table: str) -> int:
        """Return the number of Parquet data files of a serving table.

        Args:
            conn: Open session.
            table: Serving table name.

        Returns:
            Number of files listed by ``ducklake_list_files``.
        """
        return int(
            conn.execute(
                "SELECT count(*) FROM ducklake_list_files(?, ?, schema => ?)",
                [self.location.catalog_alias, table, self.location.schema],
            ).fetchone()[0]
        )

    # Méthode d'écriture d'une table dans la transaction courante
    def _write_table(
        self, conn: Any, spec: ServingTableSpec, mode: str, years: Sequence[int]
    ) -> TableStats:
        """Write one table inside the open transaction; return its statistics."""
        start = time.perf_counter()
        target = self.qualified_name(spec.name)
        sql = _strip_sql(spec.sql)
        order_by = (
            " ORDER BY " + ", ".join(_quote(column) for column in spec.sort_by)
            if spec.sort_by
            else ""
        )

        # Mode par année : tables partitionnées existantes, colonnes inchangées
        effective = "full"
        if (
            mode == "by_year"
            and spec.partitioned
            and self._table_exists(conn, spec.name)
        ):
            if self._same_columns(conn, spec):
                effective = "by_year"
            else:
                logger.info(
                    f"Table '{spec.name}' : colonnes modifiées, retour en mode full."
                )

        if effective == "by_year":
            years_sql = ", ".join(str(int(year)) for year in years)
            partition = _quote(spec.partition_by)
            conn.execute(f"DELETE FROM {target} WHERE {partition} IN ({years_sql})")
            conn.execute(
                f"INSERT INTO {target} BY NAME SELECT * FROM ({sql}) "
                f"WHERE {partition} IN ({years_sql}){order_by}"
            )
        else:
            # Schéma seul, puis partitionnement, puis insertion triée
            conn.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM ({sql}) LIMIT 0")
            if spec.partitioned:
                conn.execute(
                    f"ALTER TABLE {target} SET PARTITIONED BY ({_quote(spec.partition_by)})"
                )
            conn.execute(f"INSERT INTO {target} BY NAME SELECT * FROM ({sql}){order_by}")

        rows = int(conn.execute(f"SELECT count(*) FROM {target}").fetchone()[0])
        return TableStats(
            rows=rows, seconds=time.perf_counter() - start, mode=effective
        )

    # Méthode de publication de l'ensemble des tables (une transaction)
    def publish(
        self,
        tables: Sequence[ServingTableSpec],
        mode: str = "full",
        *,
        years: Optional[Sequence[int]] = None,
        prelude: Sequence[str] = (),
        before_commit: Optional[Callable[[Any], None]] = None,
        conn: Any = None,
    ) -> Dict[str, TableStats]:
        """Publish every table in a single transaction (PS-29.1).

        ``full`` recreates each table (schema, partitioning, sorted insert);
        ``by_year`` deletes then reinserts the requested years of the
        partitioned tables whose columns are unchanged, and recreates every
        other table (``full`` fallback).

        Args:
            tables: Tables to publish, written in order.
            mode: ``"full"`` or ``"by_year"``.
            years: Years to refresh in ``by_year`` mode.
            prelude: Statements run before ``BEGIN`` (session macros).
            before_commit: Callback run on the connection just before
                ``COMMIT`` (controls; an exception rolls everything back).
            conn: Open session (see :meth:`connect`); a session is opened and
                closed when ``None``.

        Returns:
            Mapping table name -> :class:`TableStats`.

        Raises:
            ValueError: If ``mode`` is unknown, or ``by_year`` without years.
            ServingPublicationError: If any statement fails; the transaction
                was rolled back and the previous state is intact.
        """
        # Vérification des arguments
        if mode not in MODES:
            raise ValueError(f"Unknown serving mode '{mode}', expected one of {MODES}")
        if mode == "by_year" and not years:
            raise ValueError("The by_year mode requires a non-empty list of years")

        if conn is None:
            with self.connect() as own:
                return self.publish(
                    tables,
                    mode,
                    years=years,
                    prelude=prelude,
                    before_commit=before_commit,
                    conn=own,
                )

        # Macros de session (hors transaction)
        for statement in prelude:
            conn.execute(statement)

        stats: Dict[str, TableStats] = {}
        current: Optional[str] = None
        conn.execute("BEGIN TRANSACTION")
        try:
            for spec in tables:
                current = spec.name
                stats[spec.name] = self._write_table(conn, spec, mode, years or ())
                logger.info(
                    f"Table '{spec.name}' écrite ({stats[spec.name].mode}) : "
                    f"{stats[spec.name].rows} ligne(s)."
                )
            current = None
            if before_commit is not None:
                before_commit(conn)
            conn.execute("COMMIT")
        except Exception as exc:
            # Retour complet à l'état précédent : le tableau de bord reste cohérent
            try:
                conn.execute("ROLLBACK")
            except Exception as rollback_exc:  # transaction déjà annulée par DuckDB
                logger.debug(f"ROLLBACK : {rollback_exc}")
            where = f"table '{current}'" if current else "commit"
            raise ServingPublicationError(
                f"Serving publication rolled back ({where}): {exc}", table=current
            ) from exc

        # Nombre de fichiers : visible une fois la transaction validée
        for name, table_stats in stats.items():
            table_stats.files = self.count_files(conn, name)
        return stats
