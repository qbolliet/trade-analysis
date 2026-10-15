"""Low-level operations of the DuckLake maintenance: metadata queries, DuckLake procedures, PostgreSQL vacuum and backup.

The maintenance step (:mod:`kedro_pipeline.steps.maintenance`) sequences these
operations; this module holds the SQL, the object storage and the subprocess
calls, so that the step stays readable and each operation can be tested alone.

DuckLake procedures are called through direct SQL rather than through
``DuckLakeMaintenance``: the library wrappers swallow their errors and return
an empty result, which makes a failed operation indistinguishable from an idle
one, and the step must count the failures (a catalog fails when every one of
its operations failed). The expiry and cleanup thresholds are also derived from
an injected instant, which the library wrappers do not accept.

The metadata queries read the DuckLake metadata tables through the attached
``__ducklake_metadata_<alias>`` catalog.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import fnmatch
import gzip
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Clés de la colonne `changes` d'un snapshot qui désignent une écriture de données
WRITE_KEYS = frozenset(
    {
        "tables_created",
        "tables_inserted_into",
        "tables_deleted_from",
        "inlined_insert",
        "inlined_delete",
        "tables_altered",
    }
)
# Clés d'un snapshot produit par la maintenance elle-même : un tel snapshot ne compte
# pas comme une écriture (sinon chaque passage rendrait les tables « récentes »)
MAINTENANCE_KEYS = frozenset({"merge_adjacent", "rewrite_delete", "flushed_inlined"})

# Octets par mégaoctet pour la taille moyenne des fichiers
BYTES_PER_MB = 1_000_000.0


# Mesure du stockage d'une table
@dataclass(frozen=True)
class TableMeasure:
    """Storage of one table, read from the DuckLake metadata.

    Attributes:
        files: Live data files.
        bytes: Total size of the live data files.
        delete_files: Live delete files (tombstones).
        deleted_rows: Rows marked deleted by the live delete files.
        rows: Rows stored in the live data files, deleted rows excluded.

    Examples:
        >>> measure = TableMeasure(files=4, bytes=8_000_000, delete_files=1, deleted_rows=10, rows=90)
        >>> measure.avg_file_mb, measure.deleted_share
        (2.0, 0.1)
    """

    files: int = 0
    bytes: int = 0
    delete_files: int = 0
    deleted_rows: int = 0
    rows: int = 0

    # Propriété : taille moyenne d'un fichier de données (Mo)
    @property
    def avg_file_mb(self) -> float:
        """Average size of a data file, in megabytes (``0.0`` without file)."""
        return self.bytes / self.files / BYTES_PER_MB if self.files else 0.0

    # Propriété : part de lignes supprimées parmi les lignes stockées
    @property
    def deleted_share(self) -> float:
        """Share of the stored rows marked deleted (``0.0`` for an empty table)."""
        stored = self.rows + self.deleted_rows
        return self.deleted_rows / stored if stored else 0.0


# Fonction de citation d'un identifiant SQL
def quote_ident(name: str) -> str:
    """Return ``name`` as a quoted SQL identifier.

    Args:
        name: Identifier.

    Returns:
        The identifier between double quotes, inner quotes doubled.

    Examples:
        >>> quote_ident('a"b')
        '"a""b"'
    """
    return '"' + str(name).replace('"', '""') + '"'


# Fonction de citation d'un littéral textuel SQL
def _literal(value: str) -> str:
    """Return ``value`` as a SQL string literal (quotes doubled)."""
    return "'" + str(value).replace("'", "''") + "'"


# Fonction de construction du littéral horodaté d'un instant
def timestamptz_literal(instant: datetime) -> str:
    """Return the SQL ``TIMESTAMPTZ`` literal of an instant.

    Args:
        instant: Timezone-aware instant (naive values are read as UTC).

    Returns:
        The literal, expressed in UTC.

    Examples:
        >>> timestamptz_literal(datetime(2026, 10, 4, 12, 30, tzinfo=timezone.utc))
        "TIMESTAMPTZ '2026-10-04 12:30:00.000000+00'"
    """
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return f"TIMESTAMPTZ '{instant.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')}+00'"


# Fonction de nom du catalogue de métadonnées attaché
def metadata_catalog(alias: str) -> str:
    """Return the quoted name of the metadata catalog attached next to ``alias``.

    Args:
        alias: Alias of the DuckLake catalog.

    Returns:
        ``"__ducklake_metadata_<alias>"``, quoted.

    Examples:
        >>> metadata_catalog("comtrade")
        '"__ducklake_metadata_comtrade"'
    """
    return quote_ident(f"__ducklake_metadata_{alias}")


# Fonction de test du jour hebdomadaire
def is_weekly_day(now: datetime, weekly_day: int, timezone_name: str) -> bool:
    """Tell whether ``now`` falls on the weekly day, in a given timezone.

    Args:
        now: Reference instant (naive values are read as UTC).
        weekly_day: Weekday, ``0`` for Monday to ``6`` for Sunday.
        timezone_name: IANA timezone the weekday is read in.

    Returns:
        ``True`` when the local weekday of ``now`` is ``weekly_day``.

    Examples:
        >>> sunday = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)
        >>> is_weekly_day(sunday, 6, "Europe/Paris"), is_weekly_day(sunday, 0, "Europe/Paris")
        (True, False)
        >>> late_saturday = datetime(2026, 10, 10, 23, 30, tzinfo=timezone.utc)
        >>> is_weekly_day(late_saturday, 6, "Europe/Paris")
        True
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now.astimezone(ZoneInfo(timezone_name)).weekday() == int(weekly_day)


# Fonction de sélection des colonnes de partitionnement d'un schéma
def match_partitions(
    partitions: Optional[Mapping[str, Sequence[str]]], alias: str, schema: str
) -> Optional[List[str]]:
    """Return the partition columns configured for a schema of a catalog.

    Keys of ``partitions`` have the form ``<catalog>.<schema>``; the schema part
    accepts shell wildcards (``baci_hs*``). The first matching key wins.

    Args:
        partitions: Configured partitions, or ``None``.
        alias: Catalog alias.
        schema: Schema name.

    Returns:
        The partition columns, or ``None`` when no key matches.

    Examples:
        >>> parts = {"comtrade.baci_hs*": ["year"], "eurostat.DS_045409": ["reporter"]}
        >>> match_partitions(parts, "comtrade", "baci_hs2017")
        ['year']
        >>> match_partitions(parts, "eurostat", "other") is None
        True
    """
    for key, columns in (partitions or {}).items():
        catalog, _, pattern = str(key).partition(".")
        if catalog == alias and fnmatch.fnmatchcase(schema, pattern):
            return [str(column) for column in columns]
    return None


# Fonction de lecture des tables vivantes d'un catalogue
def list_tables(conn: Any, alias: str) -> Dict[int, Tuple[str, str]]:
    """List the live tables of a catalog.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.

    Returns:
        Table identifier -> ``(schema, table)``.
    """
    meta = metadata_catalog(alias)
    rows = conn.execute(
        f"SELECT t.table_id, s.schema_name, t.table_name FROM {meta}.ducklake_table t "
        f"JOIN {meta}.ducklake_schema s USING (schema_id) "
        "WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL"
    ).fetchall()
    return {int(table_id): (str(schema), str(table)) for table_id, schema, table in rows}


# Fonction de lecture des tables écrites depuis un instant
def recently_written(
    conn: Any, alias: str, since: datetime, tables: Mapping[int, Tuple[str, str]]
) -> Set[Tuple[str, str]]:
    """List the tables written since an instant, from the snapshot changes.

    A snapshot counts as a write when it carries one of :data:`WRITE_KEYS`; the
    snapshots produced by the maintenance itself (:data:`MAINTENANCE_KEYS`) are
    ignored, so that a second pass does not find the tables recent again. The
    changes name a table by its identifier, except the creations, which name it
    ``<schema>.<table>``.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        since: Oldest snapshot instant retained.
        tables: Live tables, as returned by :func:`list_tables`.

    Returns:
        The ``(schema, table)`` pairs still alive and written since ``since``.
    """
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    by_name = {f"{schema}.{table}": (schema, table) for schema, table in tables.values()}
    written: Set[Tuple[str, str]] = set()
    rows = conn.execute(
        "SELECT snapshot_time, changes FROM ducklake_snapshots(?) ORDER BY snapshot_id", [alias]
    ).fetchall()
    for snapshot_time, changes in rows:
        if snapshot_time < since or not changes:
            continue
        keys = set(changes)
        if keys & MAINTENANCE_KEYS:
            continue
        for key in keys & WRITE_KEYS:
            for value in changes[key]:
                text = str(value).replace('"', "")
                if text.isdigit() and int(text) in tables:
                    written.add(tables[int(text)])
                elif text in by_name:
                    written.add(by_name[text])
    return written


# Fonction de mesure du stockage de toutes les tables d'un catalogue
def measure_tables(
    conn: Any, alias: str, only: Optional[Tuple[str, str]] = None
) -> Dict[Tuple[str, str], TableMeasure]:
    """Measure the storage of the live tables of a catalog.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        only: ``(schema, table)`` to measure alone; every live table when ``None``.

    Returns:
        ``(schema, table)`` -> :class:`TableMeasure`. Rows and deletions held
        inline in the catalog (small writes, not yet flushed to files) are not
        counted: flush them first to see them.
    """
    meta = metadata_catalog(alias)
    scope = (
        f" AND s.schema_name = {_literal(only[0])} AND t.table_name = {_literal(only[1])}" if only else ""
    )
    rows = conn.execute(
        f"""
        SELECT s.schema_name, t.table_name,
          (SELECT count(*) FROM {meta}.ducklake_data_file f
            WHERE f.table_id = t.table_id AND f.end_snapshot IS NULL),
          (SELECT coalesce(sum(f.file_size_bytes), 0) FROM {meta}.ducklake_data_file f
            WHERE f.table_id = t.table_id AND f.end_snapshot IS NULL),
          (SELECT coalesce(sum(f.record_count), 0) FROM {meta}.ducklake_data_file f
            WHERE f.table_id = t.table_id AND f.end_snapshot IS NULL),
          (SELECT count(*) FROM {meta}.ducklake_delete_file d
            WHERE d.table_id = t.table_id AND d.end_snapshot IS NULL),
          (SELECT coalesce(sum(d.delete_count), 0) FROM {meta}.ducklake_delete_file d
            WHERE d.table_id = t.table_id AND d.end_snapshot IS NULL)
        FROM {meta}.ducklake_table t JOIN {meta}.ducklake_schema s USING (schema_id)
        WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL{scope}
        """
    ).fetchall()
    measures: Dict[Tuple[str, str], TableMeasure] = {}
    for schema, table, files, size, stored, delete_files, deleted in rows:
        measures[(str(schema), str(table))] = TableMeasure(
            files=int(files),
            bytes=int(size),
            delete_files=int(delete_files),
            deleted_rows=int(deleted),
            rows=int(stored) - int(deleted),
        )
    return measures


# Fonction de comptage des snapshots
def count_snapshots(conn: Any, alias: str) -> int:
    """Count the snapshots of a catalog.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.

    Returns:
        The number of snapshots.
    """
    return int(conn.execute("SELECT count(*) FROM ducklake_snapshots(?)", [alias]).fetchone()[0])


# Procédure : vidage des données inlinées
def flush_inlined(conn: Any, alias: str, schema: str, table: str) -> int:
    """Flush the rows held inline in the catalog to data files.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        schema: Schema of the table.
        table: Table name.

    Returns:
        The number of rows flushed.

    Raises:
        duckdb.Error: If the procedure fails.
    """
    rows = conn.execute(
        "SELECT rows_flushed FROM ducklake_flush_inlined_data(?, table_name := ?, schema_name := ?)",
        [alias, table, schema],
    ).fetchall()
    return int(sum(row[0] for row in rows))


# Procédure : fusion des fichiers adjacents
def merge_adjacent(conn: Any, alias: str, schema: str, table: str) -> Tuple[int, int]:
    """Merge the adjacent small data files of a table.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        schema: Schema of the table.
        table: Table name.

    Returns:
        ``(files processed, files created)``.

    Raises:
        duckdb.Error: If the procedure fails.
    """
    # Lecture complète du résultat : une lecture partielle laisserait la procédure sans effet
    rows = conn.execute(
        "SELECT files_processed, files_created FROM ducklake_merge_adjacent_files(?, ?, schema := ?)",
        [alias, table, schema],
    ).fetchall()
    return int(sum(row[0] for row in rows)), int(sum(row[1] for row in rows))


# Procédure : réécriture des fichiers portant des suppressions
def rewrite_data_files(
    conn: Any, alias: str, schema: str, table: str, delete_threshold: float
) -> Tuple[int, int]:
    """Rewrite the data files whose share of deleted rows exceeds a threshold.

    The threshold is always passed explicitly: the procedure does nothing
    without it.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        schema: Schema of the table.
        table: Table name.
        delete_threshold: Share of deleted rows of a file above which it is
            rewritten.

    Returns:
        ``(files processed, files created)``.

    Raises:
        duckdb.Error: If the procedure fails.
    """
    rows = conn.execute(
        "SELECT files_processed, files_created FROM "
        "ducklake_rewrite_data_files(?, ?, delete_threshold := ?, schema := ?)",
        [alias, table, float(delete_threshold), schema],
    ).fetchall()
    return int(sum(row[0] for row in rows)), int(sum(row[1] for row in rows))


# Lecture de l'instant d'un snapshot
def snapshot_time(conn: Any, alias: str, snapshot_id: int) -> Optional[datetime]:
    """Return the commit instant of a snapshot (UTC), ``None`` when it no longer exists.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        snapshot_id: Snapshot id.

    Returns:
        The instant.
    """
    row = conn.execute(
        "SELECT snapshot_time FROM ducklake_snapshots(?) WHERE snapshot_id = ?",
        [alias, int(snapshot_id)],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    instant = row[0]
    return instant.replace(tzinfo=timezone.utc) if instant.tzinfo is None else instant.astimezone(timezone.utc)


# Calcul de la date limite d'expiration des snapshots
def expiration_cutoff(
    conn: Any,
    alias: str,
    now: datetime,
    retention_days: float,
    *,
    keep_from: Optional[int] = None,
    max_retention_days: Optional[float] = None,
) -> datetime:
    """Return the instant before which the snapshots of a catalog expire.

    The retention cutoff (``now - retention_days``) moves back to the commit
    instant of ``keep_from`` when that one is older, within the ceiling
    ``now - max_retention_days``. A snapshot exactly at the cutoff is kept.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        now: Reference instant.
        retention_days: Base retention, in days.
        keep_from: Oldest snapshot a cursor still needs, or ``None``.
        max_retention_days: Ceiling of the extended retention, or ``None``.

    Returns:
        The cutoff instant.
    """
    cutoff = now - _days(retention_days)
    if keep_from is None:
        return cutoff
    kept = snapshot_time(conn, alias, keep_from)
    if kept is None or kept >= cutoff:
        return cutoff
    if max_retention_days is not None:
        kept = max(kept, now - _days(max_retention_days))
    return min(cutoff, kept)


# Procédure : expiration des snapshots
def expire_snapshots(
    conn: Any,
    alias: str,
    now: datetime,
    retention_days: float,
    *,
    keep_from: Optional[int] = None,
    max_retention_days: Optional[float] = None,
) -> int:
    """Expire the snapshots older than the retention, relative to ``now``.

    The cutoff is derived from the injected instant (the library wrapper reads
    the system clock). The most recent snapshot is always kept. With
    ``keep_from`` (the oldest cursor of a downstream step on this catalog,
    :func:`kedro_pipeline.io.cursors.oldest_cursors`), the cutoff never passes
    that snapshot, so the changes a step has not read yet stay readable; the
    ``max_retention_days`` ceiling still applies, so that an abandoned cursor
    never blocks the expiration forever (that step then falls back to a full
    recomputation).

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        now: Reference instant.
        retention_days: Age in days beyond which a snapshot expires.
        keep_from: Snapshot kept, together with every later one; ``None``
            when no cursor reads the catalog.
        max_retention_days: Ceiling of the retention extended by
            ``keep_from``; ``None`` for no ceiling.

    Returns:
        The number of snapshots expired.

    Raises:
        duckdb.Error: If the procedure fails.
    """
    cutoff = expiration_cutoff(
        conn, alias, now, retention_days,
        keep_from=keep_from, max_retention_days=max_retention_days,
    )
    rows = conn.execute(
        f"SELECT snapshot_id FROM ducklake_expire_snapshots(?, older_than := {timestamptz_literal(cutoff)})",
        [alias],
    ).fetchall()
    return len(rows)


# Procédure : suppression des fichiers devenus inutiles
def cleanup_old_files(conn: Any, alias: str, now: datetime, older_than_days: float) -> int:
    """Delete the files no snapshot references any more, scheduled before the cutoff.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        now: Reference instant.
        older_than_days: Age in days beyond which a scheduled file is deleted.

    Returns:
        The number of files deleted.

    Raises:
        duckdb.Error: If the procedure fails.
    """
    cutoff = now - _days(older_than_days)
    rows = conn.execute(
        f"SELECT path FROM ducklake_cleanup_old_files(?, older_than := {timestamptz_literal(cutoff)})",
        [alias],
    ).fetchall()
    return len(rows)


# Procédure : suppression des fichiers orphelins du répertoire de données
def delete_orphaned_files(conn: Any, alias: str, now: datetime, older_than_days: float) -> int:
    """Delete the files of the data directory no snapshot ever referenced.

    Only files older than the cutoff are considered, so that a file being
    written is never removed.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        now: Reference instant.
        older_than_days: Minimal age in days of a deleted file.

    Returns:
        The number of files deleted.

    Raises:
        duckdb.Error: If the procedure fails.
    """
    cutoff = now - _days(older_than_days)
    rows = conn.execute(
        f"SELECT path FROM ducklake_delete_orphaned_files(?, older_than := {timestamptz_literal(cutoff)}, "
        "dry_run := false)",
        [alias],
    ).fetchall()
    return len(rows)


# Fonction de conversion d'un nombre de jours en durée
def _days(days: float) -> timedelta:
    """Return a ``timedelta`` of ``days`` days."""
    return timedelta(days=float(days))


# Fonction de lecture des colonnes d'une table
def table_columns(conn: Any, alias: str, schema: str, table: str) -> List[str]:
    """List the live columns of a table.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        schema: Schema of the table.
        table: Table name.

    Returns:
        The column names, in table order.
    """
    meta = metadata_catalog(alias)
    rows = conn.execute(
        f"SELECT c.column_name FROM {meta}.ducklake_column c "
        f"JOIN {meta}.ducklake_table t ON t.table_id = c.table_id AND t.end_snapshot IS NULL "
        f"JOIN {meta}.ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL "
        "WHERE c.end_snapshot IS NULL AND c.parent_column IS NULL AND s.schema_name = ? AND t.table_name = ? "
        "ORDER BY c.column_order",
        [schema, table],
    ).fetchall()
    return [str(row[0]) for row in rows]


# Fonction de lecture du partitionnement courant d'une table
def current_partitioning(conn: Any, alias: str, schema: str, table: str) -> Optional[List[str]]:
    """Read the partition columns currently set on a table.

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        schema: Schema of the table.
        table: Table name.

    Returns:
        The partition expressions in key order (a column name for an identity
        partition, ``transform(column)`` otherwise), or ``None`` when the table
        is not partitioned.
    """
    meta = metadata_catalog(alias)
    rows = conn.execute(
        f"SELECT c.column_name, pc.transform FROM {meta}.ducklake_partition_info p "
        f"JOIN {meta}.ducklake_partition_column pc ON pc.partition_id = p.partition_id "
        f"JOIN {meta}.ducklake_table t ON t.table_id = p.table_id AND t.end_snapshot IS NULL "
        f"JOIN {meta}.ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL "
        f"JOIN {meta}.ducklake_column c ON c.table_id = p.table_id AND c.column_id = pc.column_id "
        "AND c.end_snapshot IS NULL "
        "WHERE p.end_snapshot IS NULL AND s.schema_name = ? AND t.table_name = ? "
        "ORDER BY pc.partition_key_index",
        [schema, table],
    ).fetchall()
    if not rows:
        return None
    return [str(column) if transform == "identity" else f"{transform}({column})" for column, transform in rows]


# Fonction de pose du partitionnement d'une table
def set_partitioning(
    conn: Any, alias: str, schema: str, table: str, columns: Sequence[str]
) -> None:
    """Set the partition columns of a table (future writes only).

    Args:
        conn: Open DuckDB connection with the catalog attached.
        alias: Catalog alias.
        schema: Schema of the table.
        table: Table name.
        columns: Partition columns, in key order.

    Raises:
        ValueError: If ``columns`` is empty.
        duckdb.Error: If DuckLake rejects the partitioning.
    """
    if not columns:
        raise ValueError("partition columns must not be empty")
    keys = ", ".join(quote_ident(column) for column in columns)
    conn.execute(
        f"ALTER TABLE {quote_ident(alias)}.{quote_ident(schema)}.{quote_ident(table)} "
        f"SET PARTITIONED BY ({keys})"
    )


# Fonction de commande de repartitionnement manuel
def repartition_command(alias: str, schema: str, table: str, columns: Sequence[str]) -> str:
    """Return the Python command that rewrites the existing files of a table by partition.

    The maintenance never runs it: rewriting every file of a large table is a
    deliberate operation.

    Args:
        alias: Catalog alias.
        schema: Schema of the table.
        table: Table name.
        columns: Partition columns.

    Returns:
        The command, to run on a connection attached to the catalog.

    Examples:
        >>> repartition_command("comtrade", "C_A_HS", "fact_table", ["refYear"])
        "DuckLakeMaintenance(conn, catalog_alias='comtrade', schema='C_A_HS').repartition('fact_table', partition_by=['refYear'], schema='C_A_HS')"
    """
    return (
        f"DuckLakeMaintenance(conn, catalog_alias={alias!r}, schema={schema!r})"
        f".repartition({table!r}, partition_by={list(columns)!r}, schema={schema!r})"
    )


# Fonction de nettoyage statistique du PostgreSQL du catalogue
def vacuum_analyze(pg: Mapping[str, Any], dbname: str) -> None:
    """Run ``VACUUM (ANALYZE)`` on the PostgreSQL database holding a catalog.

    The statement cannot run inside a transaction: the connection is opened in
    autocommit mode.

    Args:
        pg: PostgreSQL credentials (``host``, ``port``, ``user``, ``password``).
        dbname: Database of the catalog metadata.

    Raises:
        ImportError: If ``psycopg2`` is not installed.
        psycopg2.Error: If the connection or the statement fails.
    """
    # Import paresseux : psycopg2 vient de dt-ducklake-manager, absent des environnements fichiers
    import psycopg2

    connection = psycopg2.connect(
        host=pg["host"], port=pg["port"], user=pg["user"], password=pg["password"], dbname=dbname
    )
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("VACUUM (ANALYZE)")
    finally:
        connection.close()


# Fonction de normalisation du point d'accès S3
def _endpoint_url(endpoint: Optional[str]) -> Optional[str]:
    """Return the endpoint with a scheme (the credentials hold a bare host name)."""
    if not endpoint:
        return None
    return endpoint if "://" in endpoint else f"https://{endpoint}"


# Fonction de construction du client S3
def s3_client(s3: Mapping[str, Any]) -> Any:
    """Build a ``boto3`` S3 client from the S3 credentials.

    Args:
        s3: S3 credentials (``endpoint``, ``access_key_id``, ``secret_access_key``,
            ``session_token``).

    Returns:
        The client.
    """
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=_endpoint_url(s3.get("endpoint")),
        aws_access_key_id=s3.get("access_key_id"),
        aws_secret_access_key=s3.get("secret_access_key"),
        aws_session_token=s3.get("session_token") or None,
    )


# Fonction de sauvegarde logique d'une base PostgreSQL vers S3
def backup_catalog(
    pg: Mapping[str, Any],
    s3: Mapping[str, Any],
    dbname: str,
    bucket: str,
    key: str,
    *,
    client: Any = None,
) -> int:
    """Dump the PostgreSQL database of a catalog (``pg_dump | gzip``) to S3.

    The password travels in the environment of the ``pg_dump`` process, never
    on its command line. The dump is compressed on the fly into a temporary
    file, then uploaded.

    Args:
        pg: PostgreSQL credentials.
        s3: S3 credentials.
        dbname: Database to dump.
        bucket: Destination bucket.
        key: Destination object key.
        client: S3 client; built from ``s3`` when ``None``.

    Returns:
        The size in bytes of the uploaded archive.

    Raises:
        RuntimeError: If ``pg_dump`` exits with an error.
        OSError: If ``pg_dump`` cannot be started.
    """
    command = [
        "pg_dump", "--host", str(pg["host"]), "--port", str(pg["port"]),
        "--username", str(pg["user"]), "--dbname", dbname, "--no-password",
    ]
    environment = {**os.environ, "PGPASSWORD": str(pg["password"])}
    with tempfile.TemporaryDirectory() as directory:
        archive = os.path.join(directory, "dump.sql.gz")
        with tempfile.TemporaryFile() as errors:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=errors, env=environment)
            with gzip.open(archive, "wb") as output:
                shutil.copyfileobj(process.stdout, output)
            returncode = process.wait()
            if returncode != 0:
                errors.seek(0)
                message = errors.read().decode("utf-8", errors="replace").strip()[-500:]
                raise RuntimeError(f"pg_dump exited with status {returncode}: {message}")
        size = os.path.getsize(archive)
        (client or s3_client(s3)).upload_file(archive, bucket, key)
    return size


# Fonction de purge des anciennes sauvegardes
def prune_backups(client: Any, bucket: str, key: str, retention: int) -> List[str]:
    """Delete the oldest backups beyond the retention, next to ``key``.

    Backups are the objects sharing the directory of ``key``; their names start
    with the date, so the lexical order is the chronological one.

    Args:
        client: S3 client.
        bucket: Bucket of the backups.
        key: Key of the backup just written (gives the directory).
        retention: Number of backups kept, the newest first.

    Returns:
        The keys deleted.
    """
    prefix = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
    keys: List[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    obsolete = sorted(keys, reverse=True)[max(int(retention), 0):]
    for old in obsolete:
        client.delete_object(Bucket=bucket, Key=old)
    return obsolete


# Fonction de clé d'une sauvegarde
def backup_key(template: str, dbname: str, now: datetime, timezone_name: str) -> str:
    """Build the object key of a backup from the configured template.

    Args:
        template: Template with the ``{dbname}`` and ``{date}`` fields.
        dbname: Database name.
        now: Reference instant.
        timezone_name: IANA timezone of the date.

    Returns:
        The key.

    Examples:
        >>> backup_key("b/{dbname}/{date}.sql.gz", "serving",
        ...            datetime(2026, 10, 11, 1, 0, tzinfo=timezone.utc), "Europe/Paris")
        'b/serving/2026-10-11.sql.gz'
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    date = now.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")
    return template.format(dbname=dbname, date=date)
