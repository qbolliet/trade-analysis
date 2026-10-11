"""Maintenance step of the DuckLake catalogs, run at the end of every workflow.

A daily upsert pipeline leaves small files, delete files and snapshots behind
it; the serving catalog is even rewritten at every full publication. This step
keeps the catalogs fast and their storage bounded, without human intervention:

* per table written within the last hours: flush of the rows held inline,
  merge of the adjacent files, rewrite of the files carrying many deleted rows
  (every day when the deleted share exceeds a threshold, otherwise on the weekly
  day);
* per catalog: expiry of the old snapshots, then deletion of the files no
  snapshot references any more (and of the orphan files);
* on the weekly day: ``VACUUM (ANALYZE)`` of the PostgreSQL database of each
  catalog, then a logical backup (``pg_dump``) to object storage;
* once per table: the configured partitioning is set (it never rewrites the
  existing files: the command to run by hand is logged).

Every operation is non-fatal. A catalog is failed only when every operation
attempted on it failed. The closing of the orphan tracking runs is the caller's
(it follows this step).

No environment variable and no YAML path are read here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

# Modules de manipulation de données
import pandas as pd

# Modules du package
from kedro_pipeline.io import maintenance as ops
from kedro_pipeline.io.ducklake import DuckLakeLocation
from kedro_pipeline.io.freshness import utc_now
from kedro_pipeline.steps.result import StepResult, capturing, failure_message
from macroforecast.tracking import RunTracker

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de l'étape (mise en page du rapport de run)
STEP = "maintenance"
# Fuseau horaire du jour hebdomadaire lorsque la configuration n'en fixe pas
DEFAULT_TIMEZONE = "Europe/Paris"
# Schéma sur lequel la connexion d'un catalogue est positionnée (la maintenance les parcourt tous)
CONNECTION_SCHEMA = "main"
# Tag d'alerte posé sur le run
ALERT_TAG = "alert"
# Mesures relevées par schéma, avant et après la maintenance
MEASURES = ("files", "avg_file_mb", "delete_files", "deleted_rows", "rows")


# Bilan des opérations d'un catalogue
@dataclass
class _CatalogOutcome:
    """What the maintenance of one catalog did and measured.

    Attributes:
        attempted: Operations attempted.
        failed: Operations that raised.
        first_error: Message of the first failure.
        metrics: Metrics of the catalog.
        rows: One report row per maintained table.
        alerts: Alert labels raised by the catalog.
        max_files: Largest file count of a table after the maintenance.
        expired: Snapshots expired.
        cleaned: Files deleted.
        maintained: Tables maintained.
        backup: ``"ok"``, ``"failed"`` or ``None`` when no backup was due.
    """

    attempted: int = 0
    failed: int = 0
    first_error: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    rows: List[Dict[str, Any]] = field(default_factory=list)
    alerts: List[str] = field(default_factory=list)
    max_files: int = 0
    expired: int = 0
    cleaned: int = 0
    maintained: int = 0
    backup: Optional[str] = None

    # Exécution non fatale d'une opération
    def attempt(self, label: str, operation: Callable[[], Any]) -> Tuple[bool, Any]:
        """Run an operation, count it, and log its failure without raising.

        Args:
            label: Operation description, used in the log and the error summary.
            operation: Callable doing the work.

        Returns:
            ``(True, result)`` on success, ``(False, None)`` on failure.
        """
        self.attempted += 1
        try:
            return True, operation()
        except Exception as exc:
            self.failed += 1
            message = f"{label} : {failure_message(exc)}"
            self.first_error = self.first_error or message
            logger.warning(f"Maintenance DuckLake, opération en échec ({message})")
            return False, None


# Fonction d'étape : maintenance des catalogues
def run_maintenance(
    catalogs: Any,
    *,
    params: Mapping[str, Any],
    tracker: Optional[RunTracker] = None,
    now: Optional[datetime] = None,
) -> StepResult:
    """Maintain the DuckLake catalogs listed in the parameters.

    Args:
        catalogs: Factory of catalog connectors
            (:class:`kedro_pipeline.io.datasets.DuckLakeCatalogs`): ``connector(location)``
            returns an unconnected connector, ``pg`` and ``s3`` hold the
            credentials, ``has_postgres`` tells whether the catalogs live in
            PostgreSQL. May be ``None`` when no catalog is configured.
        params: The ``maintenance`` parameters (``CATALOGS``,
            ``ONLY_TABLES_WRITTEN_WITHIN_HOURS``, ``DELETE_RATIO_THRESHOLD``,
            ``WEEKLY_DAY``, ``SNAPSHOT_RETENTION_DAYS``,
            ``CLEANUP_OLDER_THAN_DAYS``, ``MAX_FILES_PER_TABLE``,
            ``DATA_INLINING_ROW_LIMIT``, ``PARTITIONS``, ``BACKUP``,
            ``TIMEZONE``). The ``STALE_RUNS`` block belongs to the caller.
        tracker: Tracker of the open run (never entered here).
        now: Reference instant, which dates the weekly day and the cutoffs of
            the expiry and cleanup; the current UTC instant when ``None``.

    Returns:
        One unit per catalog. ``failures`` names the catalogs whose every
        operation failed. Metrics: ``ducklake/<catalog>/<schema>/<measure>_before``
        and ``_after`` (``files``, ``avg_file_mb``, ``delete_files``,
        ``deleted_rows``, ``rows``), ``ducklake/<catalog>/snapshots``,
        ``ducklake/max_files_per_table`` and the totals ``ducklake/tables_maintained``,
        ``ducklake/snapshots_expired``, ``ducklake/files_cleaned``. The
        ``maintenance`` report table has one row per maintained table. The tag
        ``alert`` lists the alerts raised (``backup_failed``,
        ``max_files_per_table``).

    Examples:
        >>> result = run_maintenance(None, params={"CATALOGS": []})
        >>> result.n_units_planned, result.metrics["ducklake/max_files_per_table"]
        (0, 0.0)
    """
    tracker = capturing(tracker)
    instant = now or utc_now()
    entries = list(params.get("CATALOGS") or [])
    outcomes: Dict[str, _CatalogOutcome] = {}
    failures: Dict[str, str] = {}

    for entry in entries:
        alias = str(entry["catalog_alias"])
        outcome = _maintain_catalog(catalogs, entry, params, instant)
        outcomes[alias] = outcome
        # Un catalogue est en échec si toutes ses opérations ont échoué
        if outcome.attempted and outcome.failed == outcome.attempted:
            failures[alias] = (
                f"{outcome.failed} operation(s) failed on {alias}: {outcome.first_error}"
            )[:1000]

    metrics: Dict[str, float] = {}
    for outcome in outcomes.values():
        metrics.update(outcome.metrics)
    max_files = max((outcome.max_files for outcome in outcomes.values()), default=0)
    metrics["ducklake/max_files_per_table"] = float(max_files)
    metrics["ducklake/tables_maintained"] = float(sum(o.maintained for o in outcomes.values()))
    metrics["ducklake/snapshots_expired"] = float(sum(o.expired for o in outcomes.values()))
    metrics["ducklake/files_cleaned"] = float(sum(o.cleaned for o in outcomes.values()))
    metrics["ducklake/backups_failed"] = float(sum(o.backup == "failed" for o in outcomes.values()))
    metrics["ducklake/backups_done"] = float(sum(o.backup == "ok" for o in outcomes.values()))

    # Alerte sur le nombre de fichiers d'une table (après maintenance)
    alerts = sorted({alert for outcome in outcomes.values() for alert in outcome.alerts})
    limit = params.get("MAX_FILES_PER_TABLE")
    if limit is not None and max_files > int(limit):
        logger.warning(
            f"Alerte : une table compte {max_files} fichiers après maintenance "
            f"(seuil {int(limit)})."
        )
        alerts = sorted(set(alerts) | {"max_files_per_table"})
    tags = {ALERT_TAG: ",".join(alerts)} if alerts else {}

    tracker.log_metrics(metrics)
    if tags:
        tracker.set_tags(tags)

    table_rows = [row for outcome in outcomes.values() for row in outcome.rows]
    report = pd.DataFrame(table_rows, columns=_REPORT_COLUMNS)
    planned = len(entries)
    return StepResult(
        step=STEP,
        n_units_planned=planned,
        n_units_succeeded=planned - len(failures),
        failures=failures,
        metrics=metrics,
        tags=tags,
        units_label=f"{planned} catalogue(s)",
        report_tables={"maintenance": report},
        reportable=bool(entries),
        outputs={"alerts": alerts},
    )


# Colonnes de la table de rapport
_REPORT_COLUMNS = [
    "catalog", "schema", "table", "files_before", "files_after", "avg_file_mb_after",
    "delete_files_before", "delete_files_after", "deleted_rows_before", "deleted_rows_after",
    "rows_after", "operations", "failed_operations", "seconds",
]


# Maintenance d'un catalogue
def _maintain_catalog(
    catalogs: Any, entry: Mapping[str, Any], params: Mapping[str, Any], now: datetime
) -> _CatalogOutcome:
    """Run every maintenance operation on one catalog, none of them fatal.

    Args:
        catalogs: Factory of catalog connectors.
        entry: ``CATALOGS`` entry (``dbname``, ``catalog_alias``, ``bucket``,
            ``data_path``, optionally ``schema`` and ``table``).
        params: The ``maintenance`` parameters.
        now: Reference instant.

    Returns:
        The outcome of the catalog.
    """
    outcome = _CatalogOutcome()
    alias = str(entry["catalog_alias"])
    location = DuckLakeLocation(**{"schema": CONNECTION_SCHEMA, **entry})
    timezone_name = str(params.get("TIMEZONE") or DEFAULT_TIMEZONE)
    weekly = ops.is_weekly_day(now, int(params["WEEKLY_DAY"]), timezone_name)

    # Connexion : l'impossibilité de se connecter met le catalogue en échec
    opened, conn = outcome.attempt(f"connexion {alias}", lambda: catalogs.connector(location).connect())
    if not opened:
        return outcome
    try:
        _maintain_tables(conn, alias, location, params, now, weekly, outcome)
        _maintain_catalog_storage(conn, alias, params, now, outcome)
    finally:
        try:
            conn.close()
        except Exception as exc:
            logger.debug(f"Fermeture de la connexion {alias} : {exc}")

    # Opérations sur le PostgreSQL du catalogue : une fois par semaine
    if weekly and getattr(catalogs, "has_postgres", False):
        _maintain_postgres(catalogs, entry, params, now, timezone_name, outcome)
    return outcome


# Maintenance des tables écrites récemment et partitionnement
def _maintain_tables(
    conn: Any,
    alias: str,
    location: DuckLakeLocation,
    params: Mapping[str, Any],
    now: datetime,
    weekly: bool,
    outcome: _CatalogOutcome,
) -> None:
    """Measure, compact and partition the tables of a catalog.

    Args:
        conn: Open connection to the catalog.
        alias: Catalog alias.
        location: Catalog location (gives the fact table name).
        params: The ``maintenance`` parameters.
        now: Reference instant.
        weekly: Whether today is the weekly day.
        outcome: Outcome updated in place.
    """
    ok, tables = outcome.attempt(f"{alias}: liste des tables", lambda: ops.list_tables(conn, alias))
    if not ok:
        return
    since = now - timedelta(hours=float(params["ONLY_TABLES_WRITTEN_WITHIN_HOURS"]))
    ok, written = outcome.attempt(
        f"{alias}: tables écrites", lambda: ops.recently_written(conn, alias, since, tables)
    )
    written = written if ok else set()
    ok, before = outcome.attempt(f"{alias}: mesures avant", lambda: ops.measure_tables(conn, alias))
    before = before if ok else {}

    _apply_partitioning(conn, alias, location, params, tables, before, outcome)

    inlining = params.get("DATA_INLINING_ROW_LIMIT")
    threshold = float(params["DELETE_RATIO_THRESHOLD"])
    performed: Dict[Tuple[str, str], List[str]] = {}
    failed_ops: Dict[Tuple[str, str], List[str]] = {}
    seconds: Dict[Tuple[str, str], float] = {}
    for schema, table in sorted(written & set(tables.values())):
        key = (schema, table)
        label = f"{alias}.{schema}.{table}"
        performed[key], failed_ops[key] = [], []
        started = time.perf_counter()

        def run(name: str, operation: Callable[[], Any]) -> None:
            success, _ = outcome.attempt(f"{label}: {name}", operation)
            (performed if success else failed_ops)[key].append(name)

        if inlining:
            run("flush_inlined", lambda: ops.flush_inlined(conn, alias, schema, table))
        run("merge_adjacent_files", lambda: ops.merge_adjacent(conn, alias, schema, table))
        # Part de lignes supprimées relevée après le vidage : sous le seuil d'inlining, les
        # suppressions restent dans le catalogue et n'apparaissent qu'une fois vidées en fichiers
        success, current = outcome.attempt(
            f"{label}: mesure", lambda: ops.measure_tables(conn, alias, only=key)
        )
        share = (current or {}).get(key, ops.TableMeasure()).deleted_share if success else 0.0
        if weekly or share > threshold:
            run("rewrite_data_files", lambda: ops.rewrite_data_files(conn, alias, schema, table, threshold))
        seconds[key] = time.perf_counter() - started
        outcome.maintained += 1

    ok, after = outcome.attempt(f"{alias}: mesures après", lambda: ops.measure_tables(conn, alias))
    after = after if ok else before
    _record_measures(alias, before, after, performed, failed_ops, seconds, outcome)


# Maintenance du stockage du catalogue : snapshots et fichiers
def _maintain_catalog_storage(
    conn: Any, alias: str, params: Mapping[str, Any], now: datetime, outcome: _CatalogOutcome
) -> None:
    """Expire the old snapshots, then delete the files nothing references.

    Args:
        conn: Open connection to the catalog.
        alias: Catalog alias.
        params: The ``maintenance`` parameters.
        now: Reference instant.
        outcome: Outcome updated in place.
    """
    retention = float(params["SNAPSHOT_RETENTION_DAYS"])
    cleanup_age = float(params["CLEANUP_OLDER_THAN_DAYS"])
    ok, expired = outcome.attempt(
        f"{alias}: expire_snapshots", lambda: ops.expire_snapshots(conn, alias, now, retention)
    )
    outcome.expired += int(expired or 0) if ok else 0
    ok, cleaned = outcome.attempt(
        f"{alias}: cleanup_old_files", lambda: ops.cleanup_old_files(conn, alias, now, cleanup_age)
    )
    outcome.cleaned += int(cleaned or 0) if ok else 0
    ok, orphans = outcome.attempt(
        f"{alias}: delete_orphaned_files", lambda: ops.delete_orphaned_files(conn, alias, now, cleanup_age)
    )
    outcome.cleaned += int(orphans or 0) if ok else 0
    ok, snapshots = outcome.attempt(f"{alias}: snapshots", lambda: ops.count_snapshots(conn, alias))
    if ok:
        outcome.metrics[f"ducklake/{alias}/snapshots"] = float(snapshots)


# Pose idempotente du partitionnement
def _apply_partitioning(
    conn: Any,
    alias: str,
    location: DuckLakeLocation,
    params: Mapping[str, Any],
    tables: Mapping[int, Tuple[str, str]],
    measures: Mapping[Tuple[str, str], ops.TableMeasure],
    outcome: _CatalogOutcome,
) -> None:
    """Set the configured partitioning on the tables that lack it.

    The partitioning applies to the next writes only. A table that already
    holds files keeps them as they are: the command that rewrites them is
    logged, to be run by hand.

    Args:
        conn: Open connection to the catalog.
        alias: Catalog alias.
        location: Catalog location (gives the fact table name).
        params: The ``maintenance`` parameters (``PARTITIONS``).
        tables: Live tables of the catalog.
        measures: Storage of the tables before the maintenance.
        outcome: Outcome updated in place.
    """
    partitions = params.get("PARTITIONS") or {}
    for schema, table in sorted(set(tables.values())):
        columns = ops.match_partitions(partitions, alias, schema)
        if not columns or table != location.table:
            continue
        label = f"{alias}.{schema}.{table}"
        ok, current = outcome.attempt(
            f"{label}: partitionnement courant", lambda: ops.current_partitioning(conn, alias, schema, table)
        )
        if not ok or current == columns:
            continue
        ok, existing = outcome.attempt(
            f"{label}: colonnes", lambda: ops.table_columns(conn, alias, schema, table)
        )
        if not ok:
            continue
        missing = [column for column in columns if column not in existing]
        if missing:
            logger.warning(
                f"Partitionnement ignoré pour {label} : colonne(s) absente(s) {missing}."
            )
            continue
        success, _ = outcome.attempt(
            f"{label}: partitionnement", lambda: ops.set_partitioning(conn, alias, schema, table, columns)
        )
        if success:
            logger.info(f"Partitionnement posé sur {label} : {columns}.")
            if measures.get((schema, table), ops.TableMeasure()).files:
                logger.warning(
                    f"{label} contient déjà des fichiers non partitionnés, laissés en l'état ; "
                    "réécriture manuelle : " + ops.repartition_command(alias, schema, table, columns)
                )


# Relevé des mesures avant / après
def _record_measures(
    alias: str,
    before: Mapping[Tuple[str, str], ops.TableMeasure],
    after: Mapping[Tuple[str, str], ops.TableMeasure],
    performed: Mapping[Tuple[str, str], List[str]],
    failed_ops: Mapping[Tuple[str, str], List[str]],
    seconds: Mapping[Tuple[str, str], float],
    outcome: _CatalogOutcome,
) -> None:
    """Turn the measures into metrics (per schema) and report rows (per maintained table).

    Args:
        alias: Catalog alias.
        before: Measures before the maintenance.
        after: Measures after the maintenance.
        performed: Operations done, per maintained table.
        failed_ops: Operations failed, per maintained table.
        seconds: Duration of the operations, per maintained table.
        outcome: Outcome updated in place.
    """
    for suffix, measures in (("before", before), ("after", after)):
        per_schema: Dict[str, List[ops.TableMeasure]] = {}
        for (schema, _), measure in measures.items():
            per_schema.setdefault(schema, []).append(measure)
        for schema, group in per_schema.items():
            files = sum(m.files for m in group)
            total_bytes = sum(m.bytes for m in group)
            values = {
                "files": float(files),
                "avg_file_mb": total_bytes / files / ops.BYTES_PER_MB if files else 0.0,
                "delete_files": float(sum(m.delete_files for m in group)),
                "deleted_rows": float(sum(m.deleted_rows for m in group)),
                "rows": float(sum(m.rows for m in group)),
            }
            for measure_name in MEASURES:
                outcome.metrics[f"ducklake/{alias}/{schema}/{measure_name}_{suffix}"] = values[measure_name]
    outcome.max_files = max((m.files for m in after.values()), default=0)

    for key in sorted(performed):
        schema, table = key
        previous, current = before.get(key, ops.TableMeasure()), after.get(key, ops.TableMeasure())
        outcome.rows.append(
            {
                "catalog": alias, "schema": schema, "table": table,
                "files_before": previous.files, "files_after": current.files,
                "avg_file_mb_after": round(current.avg_file_mb, 3),
                "delete_files_before": previous.delete_files, "delete_files_after": current.delete_files,
                "deleted_rows_before": previous.deleted_rows, "deleted_rows_after": current.deleted_rows,
                "rows_after": current.rows,
                "operations": ",".join(performed[key]),
                "failed_operations": ",".join(failed_ops[key]),
                "seconds": round(seconds.get(key, 0.0), 3),
            }
        )


# Maintenance de la base PostgreSQL du catalogue
def _maintain_postgres(
    catalogs: Any,
    entry: Mapping[str, Any],
    params: Mapping[str, Any],
    now: datetime,
    timezone_name: str,
    outcome: _CatalogOutcome,
) -> None:
    """Vacuum the PostgreSQL database of a catalog, then back it up to object storage.

    A failed backup is a metric and an alert tag, never a failure of the node:
    the previous backups stay in place.

    Args:
        catalogs: Factory of catalog connectors (credentials).
        entry: ``CATALOGS`` entry.
        params: The ``maintenance`` parameters (``BACKUP``).
        now: Reference instant.
        timezone_name: Timezone of the weekday and of the backup date.
        outcome: Outcome updated in place.
    """
    alias, dbname = str(entry["catalog_alias"]), str(entry["dbname"])
    outcome.attempt(f"{alias}: VACUUM ANALYZE", lambda: ops.vacuum_analyze(catalogs.pg, dbname))

    backup = params.get("BACKUP") or {}
    if not backup.get("ENABLED"):
        return
    if not ops.is_weekly_day(now, int(backup["WEEKLY_DAY"]), timezone_name):
        return
    key = ops.backup_key(str(backup["PATH_TEMPLATE"]), dbname, now, timezone_name)
    bucket = str(entry["bucket"])

    def back_up() -> None:
        client = ops.s3_client(catalogs.s3)
        size = ops.backup_catalog(catalogs.pg, catalogs.s3, dbname, bucket, key, client=client)
        removed = ops.prune_backups(client, bucket, key, int(backup["RETENTION"]))
        logger.info(f"Sauvegarde {alias} : s3://{bucket}/{key} ({size} octets), {len(removed)} ancienne(s) purgée(s).")

    try:
        back_up()
        outcome.backup = "ok"
    except Exception as exc:
        outcome.backup = "failed"
        outcome.alerts.append("backup_failed")
        logger.error(f"Sauvegarde {alias} en échec : {failure_message(exc)}")
    outcome.metrics[f"ducklake/{alias}/backup_failed"] = float(outcome.backup == "failed")
