"""Tests des opérations de bas niveau de la maintenance DuckLake (``kedro_pipeline.io.maintenance``).

Catalogue DuckLake fichier temporaire (fixture ``ducklake_conn``), aucun réseau. Le sous-processus
``pg_dump`` est simulé, le stockage S3 par ``moto``.
"""

from __future__ import annotations

import gzip
import io
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List

import pytest

from kedro_pipeline.io import maintenance as ops

ALIAS = "db"
SCHEMA = "s1"
NOW = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)


# ──────────────────────────────────────────────────────────────────────
# Fonctions pures
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("instant", "weekly_day", "expected"),
    [
        (datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc), 6, True),   # dimanche
        (datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc), 0, False),
        # Samedi 23:30 UTC = dimanche 01:30 à Paris : le jour se lit dans le fuseau configuré
        (datetime(2026, 10, 10, 23, 30, tzinfo=timezone.utc), 6, True),
        (datetime(2026, 10, 11, 12, 0), 6, True),  # instant naïf lu en UTC
    ],
)
def test_weekly_day_is_read_in_the_configured_timezone(instant, weekly_day, expected) -> None:
    assert ops.is_weekly_day(instant, weekly_day, "Europe/Paris") is expected


def test_weekly_day_changes_with_the_timezone() -> None:
    instant = datetime(2026, 10, 10, 23, 30, tzinfo=timezone.utc)
    assert ops.is_weekly_day(instant, 5, "UTC") and not ops.is_weekly_day(instant, 5, "Europe/Paris")


def test_match_partitions_wildcards_and_first_match() -> None:
    partitions = {"comtrade.baci_hs*": ["year"], "comtrade.baci_hs2017": ["other"], "eurostat.DS_1": ["reporter"]}
    assert ops.match_partitions(partitions, "comtrade", "baci_hs2022") == ["year"]
    assert ops.match_partitions(partitions, "comtrade", "baci_hs2017") == ["year"]
    assert ops.match_partitions(partitions, "eurostat", "DS_1") == ["reporter"]
    assert ops.match_partitions(partitions, "eurostat", "DS_2") is None
    assert ops.match_partitions(None, "eurostat", "DS_1") is None


def test_table_measure_derived_values() -> None:
    measure = ops.TableMeasure(files=4, bytes=8_000_000, delete_files=1, deleted_rows=25, rows=75)
    assert measure.avg_file_mb == 2.0 and measure.deleted_share == 0.25
    empty = ops.TableMeasure()
    assert empty.avg_file_mb == 0.0 and empty.deleted_share == 0.0


# ──────────────────────────────────────────────────────────────────────
# Sauvegarde PostgreSQL : sous-processus simulé, S3 simulé
# ──────────────────────────────────────────────────────────────────────

PG = {"host": "pg.example", "port": "5432", "user": "u", "password": "s3cret-pw"}


class _FakeProcess:
    """``Popen`` factice : sort un dump SQL et un code retour."""

    def __init__(self, returncode: int = 0, payload: bytes = b"CREATE TABLE t ();\n") -> None:
        self.stdout = io.BytesIO(payload)
        self.returncode = returncode

    def wait(self) -> int:
        return self.returncode


class _FakeS3:
    """Client S3 minimal : relit l'archive au moment du dépôt (le répertoire temporaire disparaît ensuite)."""

    def __init__(self) -> None:
        self.uploads: List[tuple] = []

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        with gzip.open(filename, "rb") as handle:
            self.uploads.append((bucket, key, handle.read()))


def test_backup_streams_pg_dump_through_gzip_to_s3(monkeypatch) -> None:
    seen = {}

    def popen(command, **kwargs):
        seen["command"], seen["env"] = command, kwargs["env"]
        return _FakeProcess()

    monkeypatch.setattr(ops.subprocess, "Popen", popen)
    client = _FakeS3()
    size = ops.backup_catalog(PG, {}, "serving", "bucket", "t/serving/2026-10-11.sql.gz", client=client)

    assert size > 0
    assert client.uploads == [("bucket", "t/serving/2026-10-11.sql.gz", b"CREATE TABLE t ();\n")]
    assert seen["command"][0] == "pg_dump" and "serving" in seen["command"]
    # Le mot de passe voyage dans l'environnement, jamais dans la ligne de commande
    assert "s3cret-pw" not in " ".join(seen["command"]) and seen["env"]["PGPASSWORD"] == "s3cret-pw"


def test_backup_fails_when_pg_dump_fails(monkeypatch) -> None:
    monkeypatch.setattr(ops.subprocess, "Popen", lambda *a, **k: _FakeProcess(returncode=1))
    client = _FakeS3()
    with pytest.raises(RuntimeError, match="pg_dump exited with status 1"):
        ops.backup_catalog(PG, {}, "serving", "bucket", "k.sql.gz", client=client)
    # Rien n'est déposé : une sauvegarde partielle ne remplacerait pas une bonne
    assert client.uploads == []


def test_prune_backups_keeps_the_newest(aws_env) -> None:
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="backups-bucket")
        for day in ("2026-09-13", "2026-09-20", "2026-09-27", "2026-10-04", "2026-10-11"):
            client.put_object(Bucket="backups-bucket", Key=f"trade/backups/serving/{day}.sql.gz", Body=b"x")
        client.put_object(Bucket="backups-bucket", Key="trade/backups/other/2026-01-01.sql.gz", Body=b"x")
        removed = ops.prune_backups(client, "backups-bucket", "trade/backups/serving/2026-10-11.sql.gz", 2)
        assert removed == [
            "trade/backups/serving/2026-09-27.sql.gz",
            "trade/backups/serving/2026-09-20.sql.gz",
            "trade/backups/serving/2026-09-13.sql.gz",
        ]
        kept = [o["Key"] for o in client.list_objects_v2(Bucket="backups-bucket")["Contents"]]
        assert sorted(kept) == [
            "trade/backups/other/2026-01-01.sql.gz",
            "trade/backups/serving/2026-10-04.sql.gz",
            "trade/backups/serving/2026-10-11.sql.gz",
        ]


def test_endpoint_gets_a_scheme() -> None:
    assert ops._endpoint_url("minio.lab.example") == "https://minio.lab.example"
    assert ops._endpoint_url("http://localhost:9000") == "http://localhost:9000"
    assert ops._endpoint_url("") is None


# ──────────────────────────────────────────────────────────────────────
# Procédures DuckLake sur un catalogue fichier
# ──────────────────────────────────────────────────────────────────────


def _insert_batches(conn: Any, table: str, n_batches: int, rows: int = 20) -> None:
    """Petits lots de ``rows`` lignes (au-dessus du seuil d'inlining : un fichier chacun)."""
    for batch in range(n_batches):
        conn.execute(
            f"INSERT INTO {ALIAS}.{SCHEMA}.{table} "
            f"SELECT {batch * rows} + i, i FROM range({rows}) t(i)"
        )


@pytest.fixture
def lake(ducklake_conn):
    """Connexion et catalogue avec une table ``t`` de 6 fichiers de 20 lignes."""
    conn, _ = ducklake_conn
    conn.execute(f"CREATE TABLE {ALIAS}.{SCHEMA}.t (k INTEGER, v INTEGER)")
    _insert_batches(conn, "t", 6)
    return conn


def test_list_and_measure_tables(lake) -> None:
    tables = ops.list_tables(lake, ALIAS)
    assert set(tables.values()) == {(SCHEMA, "t")}
    measure = ops.measure_tables(lake, ALIAS)[(SCHEMA, "t")]
    assert (measure.files, measure.rows, measure.delete_files, measure.deleted_rows) == (6, 120, 0, 0)
    assert measure.avg_file_mb > 0


def test_recently_written_ignores_maintenance_snapshots(lake) -> None:
    tables = ops.list_tables(lake, ALIAS)
    since = datetime.now(timezone.utc) - timedelta(hours=1)
    assert ops.recently_written(lake, ALIAS, since, tables) == {(SCHEMA, "t")}
    # Fenêtre postérieure aux écritures : la table n'est plus récente
    assert ops.recently_written(lake, ALIAS, datetime.now(timezone.utc) + timedelta(hours=1), tables) == set()
    # Un snapshot de fusion n'est pas une écriture
    ops.merge_adjacent(lake, ALIAS, SCHEMA, "t")
    later = datetime.now(timezone.utc) + timedelta(seconds=1)
    assert ops.recently_written(lake, ALIAS, later, tables) == set()


def test_merge_reduces_the_files_and_keeps_the_data(lake) -> None:
    checksum = lake.execute(f"SELECT sum(k), sum(v), count(*) FROM {ALIAS}.{SCHEMA}.t").fetchall()[0]
    processed, created = ops.merge_adjacent(lake, ALIAS, SCHEMA, "t")
    assert processed == 6 and created >= 1
    assert ops.measure_tables(lake, ALIAS)[(SCHEMA, "t")].files < 6
    assert lake.execute(f"SELECT sum(k), sum(v), count(*) FROM {ALIAS}.{SCHEMA}.t").fetchall()[0] == checksum
    # Idempotence : rien de plus à fusionner
    assert ops.merge_adjacent(lake, ALIAS, SCHEMA, "t")[1] <= 1


def test_rewrite_applies_the_deleted_rows_threshold(lake) -> None:
    ops.merge_adjacent(lake, ALIAS, SCHEMA, "t")
    lake.execute(f"DELETE FROM {ALIAS}.{SCHEMA}.t WHERE k < 60")
    before = ops.measure_tables(lake, ALIAS)[(SCHEMA, "t")]
    assert before.deleted_rows == 60 and before.delete_files >= 1 and before.deleted_share == pytest.approx(0.5)
    # Seuil au-dessus de la part supprimée : aucune réécriture
    assert ops.rewrite_data_files(lake, ALIAS, SCHEMA, "t", 0.9) == (0, 0)
    processed, _ = ops.rewrite_data_files(lake, ALIAS, SCHEMA, "t", 0.1)
    after = ops.measure_tables(lake, ALIAS)[(SCHEMA, "t")]
    assert processed >= 1 and after.delete_files == 0 and after.deleted_rows == 0 and after.rows == 60


def test_flush_inlined_moves_small_writes_to_files(lake) -> None:
    lake.execute(f"INSERT INTO {ALIAS}.{SCHEMA}.t VALUES (1000, 1)")  # 1 ligne : inlinée
    before = ops.measure_tables(lake, ALIAS)[(SCHEMA, "t")].files
    assert ops.flush_inlined(lake, ALIAS, SCHEMA, "t") == 1
    assert ops.measure_tables(lake, ALIAS)[(SCHEMA, "t")].files == before + 1
    assert ops.flush_inlined(lake, ALIAS, SCHEMA, "t") == 0


def test_partitioning_is_read_back_and_set_once(lake) -> None:
    assert ops.current_partitioning(lake, ALIAS, SCHEMA, "t") is None
    assert ops.table_columns(lake, ALIAS, SCHEMA, "t") == ["k", "v"]
    ops.set_partitioning(lake, ALIAS, SCHEMA, "t", ["v"])
    assert ops.current_partitioning(lake, ALIAS, SCHEMA, "t") == ["v"]
    with pytest.raises(ValueError):
        ops.set_partitioning(lake, ALIAS, SCHEMA, "t", [])
    # Les fichiers existants ne sont pas réécrits
    assert ops.measure_tables(lake, ALIAS)[(SCHEMA, "t")].files == 6


def test_repartition_command_names_the_manual_call() -> None:
    command = ops.repartition_command("comtrade", "C_A_HS", "fact_table", ["refYear"])
    assert "repartition('fact_table'" in command and "['refYear']" in command


def test_expire_and_cleanup_use_the_injected_clock(lake) -> None:
    n_snapshots = ops.count_snapshots(lake, ALIAS)
    ops.merge_adjacent(lake, ALIAS, SCHEMA, "t")  # remplace 6 fichiers : à supprimer après expiration
    real_now = datetime.now(timezone.utc)

    # Horloge « maintenant » : tous les snapshots ont moins de 7 jours, aucun n'expire
    assert ops.expire_snapshots(lake, ALIAS, real_now, 7) == 0
    assert ops.cleanup_old_files(lake, ALIAS, real_now, 7) == 0
    assert ops.count_snapshots(lake, ALIAS) == n_snapshots + 1

    # Horloge 30 jours plus tard : tous expirent sauf le dernier, les fichiers remplacés sont supprimés
    later = real_now + timedelta(days=30)
    assert ops.expire_snapshots(lake, ALIAS, later, 7) == n_snapshots
    assert ops.count_snapshots(lake, ALIAS) == 1
    assert ops.cleanup_old_files(lake, ALIAS, later, 7) == 6
    # Idempotence
    assert ops.expire_snapshots(lake, ALIAS, later, 7) == 0
    assert ops.cleanup_old_files(lake, ALIAS, later, 7) == 0
    assert ops.delete_orphaned_files(lake, ALIAS, later, 7) == 0
    assert lake.execute(f"SELECT count(*) FROM {ALIAS}.{SCHEMA}.t").fetchall()[0][0] == 120


def test_expiration_never_passes_the_oldest_cursor_within_the_ceiling(lake) -> None:
    snapshots = [row[0] for row in lake.execute(
        f"SELECT snapshot_id FROM ducklake_snapshots('{ALIAS}') ORDER BY snapshot_id"
    ).fetchall()]
    cursor = snapshots[-2]
    later = datetime.now(timezone.utc) + timedelta(days=30)

    # Curseur conservé : seuls les snapshots qui le précèdent expirent
    expired = ops.expire_snapshots(lake, ALIAS, later, 7, keep_from=cursor, max_retention_days=60)
    remaining = [row[0] for row in lake.execute(
        f"SELECT snapshot_id FROM ducklake_snapshots('{ALIAS}') ORDER BY snapshot_id"
    ).fetchall()]
    assert remaining == [cursor, snapshots[-1]]
    assert expired == len(snapshots) - 2

    # Plafond dépassé : le curseur abandonné ne bloque plus l'expiration
    much_later = later + timedelta(days=60)
    assert ops.expire_snapshots(lake, ALIAS, much_later, 7, keep_from=cursor, max_retention_days=35) == 1
    # Curseur disparu : rétention simple
    cutoff = ops.expiration_cutoff(lake, ALIAS, much_later, 7, keep_from=cursor)
    assert cutoff == much_later - timedelta(days=7)
