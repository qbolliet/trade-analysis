"""Tests de l'étape ``run_maintenance`` (K-14) sur un catalogue DuckLake fichier temporaire.

Les opérations de bas niveau sont couvertes par ``tests/test_io_maintenance.py`` ; on vérifie ici
l'enchaînement : tables récentes seulement, ordre des opérations, règle de réécriture, mesures,
alertes, partitionnement idempotent, sauvegarde non fatale, échec d'un catalogue.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pytest

from kedro_pipeline.io import maintenance as ops
from kedro_pipeline.steps import maintenance as step
from tradestat.tracking import CapturingTracker

ALIAS = "db"
SCHEMA = "s1"


# ──────────────────────────────────────────────────────────────────────
# Montage
# ──────────────────────────────────────────────────────────────────────


class _Borrowed:
    """Connexion prêtée : la maintenance la ferme, le test la garde ouverte."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def close(self) -> None:
        return None


class _Connector:
    def __init__(self, conn: Any, fail: bool = False) -> None:
        self._conn, self._fail = conn, fail

    def connect(self) -> Any:
        if self._fail:
            raise ConnectionError("catalog unreachable")
        return _Borrowed(self._conn)


class _Catalogs:
    """Fabrique de connecteurs factice : une seule connexion, partagée par tous les catalogues."""

    def __init__(self, conn: Any, *, postgres: bool = False, fail: bool = False) -> None:
        self.conn, self.fail = conn, fail
        self.has_postgres = postgres
        self.pg = {"host": "pg", "port": "5432", "user": "u", "password": "p"}
        self.s3 = {"endpoint": "e", "access_key_id": "k", "secret_access_key": "s", "session_token": None}

    def connector(self, location: Any, *, create_db_if_missing: bool = False) -> _Connector:
        return _Connector(self.conn, self.fail)


def _params(now: datetime, *, weekly: bool = False, **overrides: Any) -> Dict[str, Any]:
    """Paramètres complets ; ``weekly`` fait coïncider (ou non) le jour hebdomadaire avec ``now``."""
    weekday = now.astimezone(timezone.utc).weekday()
    params: Dict[str, Any] = {
        "CATALOGS": [{"dbname": ALIAS, "catalog_alias": ALIAS, "bucket": "bucket", "data_path": "p"}],
        "ONLY_TABLES_WRITTEN_WITHIN_HOURS": 24,
        "DELETE_RATIO_THRESHOLD": 0.1,
        "WEEKLY_DAY": weekday if weekly else (weekday + 3) % 7,
        "TIMEZONE": "UTC",
        "SNAPSHOT_RETENTION_DAYS": 7,
        "CLEANUP_OLDER_THAN_DAYS": 7,
        "MAX_FILES_PER_TABLE": 2000,
        "DATA_INLINING_ROW_LIMIT": 1000,
        "PARTITIONS": {},
        "BACKUP": {
            "ENABLED": True, "WEEKLY_DAY": weekday if weekly else (weekday + 3) % 7,
            "PATH_TEMPLATE": "trade/backups/{dbname}/{date}.sql.gz", "RETENTION": 2,
        },
    }
    params.update(overrides)
    return params


def _insert_batches(conn: Any, table: str, n_batches: int, rows: int = 20) -> None:
    for batch in range(n_batches):
        conn.execute(
            f"INSERT INTO {ALIAS}.{SCHEMA}.{table} SELECT {batch * rows} + i, i FROM range({rows}) t(i)"
        )


@pytest.fixture
def lake(ducklake_conn):
    conn, _ = ducklake_conn
    conn.execute(f"CREATE TABLE {ALIAS}.{SCHEMA}.fact_table (k INTEGER, v INTEGER)")
    _insert_batches(conn, "fact_table", 8)
    return conn


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ──────────────────────────────────────────────────────────────────────
# Enchaînement nominal
# ──────────────────────────────────────────────────────────────────────


def test_no_catalog_is_an_empty_result() -> None:
    result = step.run_maintenance(None, params={"CATALOGS": []})
    assert result.n_units_planned == 0 and not result.reportable and not result.failures
    assert result.metrics["ducklake/max_files_per_table"] == 0.0


def test_compacts_the_recently_written_tables_and_reports_before_after(lake) -> None:
    now = _now()
    tracker = CapturingTracker()
    checksum = lake.execute(f"SELECT sum(k), sum(v), count(*) FROM {ALIAS}.{SCHEMA}.fact_table").fetchone()

    result = step.run_maintenance(_Catalogs(lake), params=_params(now), tracker=tracker, now=now)

    assert result.n_units_planned == 1 and result.n_units_succeeded == 1 and not result.failures
    metrics = result.metrics
    assert metrics[f"ducklake/{ALIAS}/{SCHEMA}/files_before"] == 8
    assert metrics[f"ducklake/{ALIAS}/{SCHEMA}/files_after"] < 8
    assert metrics[f"ducklake/{ALIAS}/{SCHEMA}/rows_after"] == 160
    assert metrics[f"ducklake/{ALIAS}/{SCHEMA}/avg_file_mb_after"] > 0
    assert metrics[f"ducklake/{ALIAS}/snapshots"] >= 1
    assert metrics["ducklake/tables_maintained"] == 1
    assert metrics["ducklake/max_files_per_table"] == metrics[f"ducklake/{ALIAS}/{SCHEMA}/files_after"]
    # Métriques journalisées dans le run, table de rapport par table maintenue
    assert tracker.metrics["ducklake/tables_maintained"] == 1
    report = result.report_tables["maintenance"]
    assert list(report["table"]) == ["fact_table"] and report.loc[0, "files_before"] == 8
    assert report.loc[0, "operations"].split(",") == ["flush_inlined", "merge_adjacent_files"]
    # Les données ne changent pas
    assert lake.execute(f"SELECT sum(k), sum(v), count(*) FROM {ALIAS}.{SCHEMA}.fact_table").fetchone() == checksum


def test_tables_not_written_recently_are_left_alone(lake) -> None:
    now = _now() + timedelta(days=3)  # fenêtre de 24 h : les écritures sont plus anciennes
    result = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    assert result.metrics["ducklake/tables_maintained"] == 0
    assert result.metrics[f"ducklake/{ALIAS}/{SCHEMA}/files_after"] == 8
    assert result.report_tables["maintenance"].empty


def test_inlining_flush_follows_the_parameter(lake) -> None:
    lake.execute(f"INSERT INTO {ALIAS}.{SCHEMA}.fact_table VALUES (999, 1)")  # inlinée
    now = _now()
    off = step.run_maintenance(_Catalogs(lake), params=_params(now, DATA_INLINING_ROW_LIMIT=None), now=now)
    assert "flush_inlined" not in off.report_tables["maintenance"].loc[0, "operations"]
    on = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    assert "flush_inlined" in on.report_tables["maintenance"].loc[0, "operations"]


def test_rewrite_runs_on_a_high_deleted_share_or_on_the_weekly_day(lake) -> None:
    now = _now()
    # Part supprimée 12,5 % (> 10 %), répartie sur tous les fichiers : réécriture tous les jours
    lake.execute(f"DELETE FROM {ALIAS}.{SCHEMA}.fact_table WHERE k % 8 = 0")
    daily = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    assert "rewrite_data_files" in daily.report_tables["maintenance"].loc[0, "operations"]
    assert daily.report_tables["maintenance"].loc[0, "deleted_rows_after"] == 0

    # Part supprimée 7 % (< 10 %), jour ordinaire : pas de réécriture ; jour hebdomadaire : réécriture
    lake.execute(f"DELETE FROM {ALIAS}.{SCHEMA}.fact_table WHERE k % 16 = 1")
    ordinary = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    assert "rewrite_data_files" not in ordinary.report_tables["maintenance"].loc[0, "operations"]
    weekly = step.run_maintenance(_Catalogs(lake), params=_params(now, weekly=True), now=now)
    assert "rewrite_data_files" in weekly.report_tables["maintenance"].loc[0, "operations"]
    # Le seuil par fichier reste DELETE_RATIO_THRESHOLD : seuls les fichiers au-delà sont réécrits
    assert weekly.report_tables["maintenance"].loc[0, "deleted_rows_after"] <= 10


def test_second_pass_has_no_notable_effect(lake) -> None:
    now = _now()
    first = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    second = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    key = f"ducklake/{ALIAS}/{SCHEMA}/files"
    assert second.metrics[key + "_before"] == first.metrics[key + "_after"]
    assert second.metrics[key + "_after"] == second.metrics[key + "_before"]
    assert second.metrics["ducklake/snapshots_expired"] == 0 and second.metrics["ducklake/files_cleaned"] == 0


def test_snapshots_expire_beyond_the_retention(lake) -> None:
    now = _now()
    recent = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    assert recent.metrics["ducklake/snapshots_expired"] == 0
    later = now + timedelta(days=30)
    expired = step.run_maintenance(_Catalogs(lake), params=_params(later), now=later)
    assert expired.metrics["ducklake/snapshots_expired"] > 0
    assert expired.metrics[f"ducklake/{ALIAS}/snapshots"] == 1
    assert expired.metrics["ducklake/files_cleaned"] > 0


# ──────────────────────────────────────────────────────────────────────
# Alerte et partitionnement
# ──────────────────────────────────────────────────────────────────────


def test_too_many_files_raise_the_alert(lake, caplog) -> None:
    now = _now()
    with caplog.at_level(logging.WARNING):
        result = step.run_maintenance(_Catalogs(lake), params=_params(now, MAX_FILES_PER_TABLE=0), now=now)
    assert result.tags == {"alert": "max_files_per_table"}
    assert any("après maintenance" in record.message for record in caplog.records)
    ok = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    assert ok.tags == {}


def test_partitioning_is_set_once_and_never_rewrites(lake, caplog) -> None:
    now = _now()
    params = _params(now, PARTITIONS={f"{ALIAS}.{SCHEMA}": ["v"]})
    files_before = ops.measure_tables(lake, ALIAS)[(SCHEMA, "fact_table")].files
    with caplog.at_level(logging.INFO):
        step.run_maintenance(_Catalogs(lake), params=params, now=now)
    assert ops.current_partitioning(lake, ALIAS, SCHEMA, "fact_table") == ["v"]
    # La table avait déjà des fichiers : la commande de réécriture manuelle est journalisée
    messages = [record.message for record in caplog.records]
    assert any("repartition('fact_table'" in message for message in messages)
    # Les anciens fichiers restent non partitionnés (seule la fusion les a regroupés)
    assert ops.measure_tables(lake, ALIAS)[(SCHEMA, "fact_table")].files <= files_before

    # 2e passage : déjà partitionné, rien à poser ni à journaliser
    caplog.clear()
    with caplog.at_level(logging.INFO):
        step.run_maintenance(_Catalogs(lake), params=params, now=now)
    assert not any("Partitionnement" in record.message or "repartition" in record.message for record in caplog.records)


def test_partitioning_skips_a_missing_column_and_other_tables(lake, caplog) -> None:
    lake.execute(f"CREATE TABLE {ALIAS}.{SCHEMA}.dimension (code INTEGER)")
    now = _now()
    with caplog.at_level(logging.WARNING):
        result = step.run_maintenance(
            _Catalogs(lake), params=_params(now, PARTITIONS={f"{ALIAS}.{SCHEMA}": ["absent"]}), now=now
        )
    assert not result.failures
    assert ops.current_partitioning(lake, ALIAS, SCHEMA, "fact_table") is None
    assert any("colonne(s) absente(s)" in record.message for record in caplog.records)
    # Seule la table de faits est visée par le partitionnement
    assert ops.current_partitioning(lake, ALIAS, SCHEMA, "dimension") is None


# ──────────────────────────────────────────────────────────────────────
# Non-fatalité et échecs
# ──────────────────────────────────────────────────────────────────────


def test_a_failed_operation_does_not_fail_the_catalog(lake, monkeypatch) -> None:
    def broken(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("merge failed")

    monkeypatch.setattr(ops, "merge_adjacent", broken)
    now = _now()
    result = step.run_maintenance(_Catalogs(lake), params=_params(now), now=now)
    assert not result.failures and result.n_units_succeeded == 1
    assert result.report_tables["maintenance"].loc[0, "failed_operations"] == "merge_adjacent_files"


def test_a_catalog_fails_when_every_operation_fails(lake) -> None:
    now = _now()
    result = step.run_maintenance(_Catalogs(lake, fail=True), params=_params(now), now=now)
    assert list(result.failures) == [ALIAS] and result.n_units_succeeded == 0
    with pytest.raises(RuntimeError, match="1 unit\\(s\\) failed"):
        result.raise_if_failed()


def test_one_failed_catalog_does_not_stop_the_others(lake) -> None:
    now = _now()
    entries = [
        {"dbname": ALIAS, "catalog_alias": ALIAS, "bucket": "b", "data_path": "p"},
        {"dbname": "gone", "catalog_alias": "gone", "bucket": "b", "data_path": "p"},
    ]

    class _Mixed(_Catalogs):
        def connector(self, location: Any, *, create_db_if_missing: bool = False) -> _Connector:
            return _Connector(self.conn, fail=location.catalog_alias == "gone")

    result = step.run_maintenance(_Mixed(lake), params=_params(now, CATALOGS=entries), now=now)
    assert list(result.failures) == ["gone"]
    assert result.n_units_planned == 2 and result.n_units_succeeded == 1
    assert result.metrics[f"ducklake/{ALIAS}/{SCHEMA}/files_before"] == 8


# ──────────────────────────────────────────────────────────────────────
# PostgreSQL : nettoyage et sauvegarde hebdomadaires
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def postgres(monkeypatch):
    calls: Dict[str, list] = {"vacuum": [], "backup": [], "prune": []}
    monkeypatch.setattr(ops, "vacuum_analyze", lambda pg, dbname: calls["vacuum"].append(dbname))
    monkeypatch.setattr(ops, "s3_client", lambda s3: "client")

    def backup(pg, s3, dbname, bucket, key, *, client=None):
        calls["backup"].append((dbname, bucket, key, client))
        return 1234

    monkeypatch.setattr(ops, "backup_catalog", backup)
    monkeypatch.setattr(
        ops, "prune_backups", lambda client, bucket, key, retention: calls["prune"].append((key, retention)) or []
    )
    return calls


def test_weekly_day_vacuums_and_backs_up(lake, postgres) -> None:
    now = _now()
    result = step.run_maintenance(_Catalogs(lake, postgres=True), params=_params(now, weekly=True), now=now)
    assert postgres["vacuum"] == [ALIAS]
    (dbname, bucket, key, client), = postgres["backup"]
    assert (dbname, bucket, client) == (ALIAS, "bucket", "client")
    assert key == f"trade/backups/{ALIAS}/{now.strftime('%Y-%m-%d')}.sql.gz"
    assert postgres["prune"] == [(key, 2)]
    assert result.metrics["ducklake/backups_done"] == 1 and result.metrics["ducklake/backups_failed"] == 0
    assert result.tags == {}


def test_ordinary_day_and_file_catalogs_skip_postgres(lake, postgres) -> None:
    now = _now()
    step.run_maintenance(_Catalogs(lake, postgres=True), params=_params(now), now=now)
    step.run_maintenance(_Catalogs(lake, postgres=False), params=_params(now, weekly=True), now=now)
    assert postgres == {"vacuum": [], "backup": [], "prune": []}


def test_backup_can_be_disabled(lake, postgres) -> None:
    now = _now()
    params = _params(now, weekly=True)
    params["BACKUP"]["ENABLED"] = False
    step.run_maintenance(_Catalogs(lake, postgres=True), params=params, now=now)
    assert postgres["vacuum"] == [ALIAS] and postgres["backup"] == []


def test_a_failed_backup_is_a_metric_and_an_alert_not_a_failure(lake, postgres, monkeypatch) -> None:
    def broken(*args: Any, **kwargs: Any) -> int:
        raise RuntimeError("pg_dump exited with status 1")

    monkeypatch.setattr(ops, "backup_catalog", broken)
    now = _now()
    result = step.run_maintenance(_Catalogs(lake, postgres=True), params=_params(now, weekly=True), now=now)
    assert not result.failures and result.n_units_succeeded == 1
    assert result.metrics["ducklake/backups_failed"] == 1 and result.metrics[f"ducklake/{ALIAS}/backup_failed"] == 1
    assert result.tags == {"alert": "backup_failed"}
    assert postgres["prune"] == []  # les anciennes sauvegardes ne sont pas purgées sans nouvelle sauvegarde
