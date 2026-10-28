"""Tests de bout en bout de la maintenance DuckLake (K-14), catalogues fichiers temporaires.

Les écritures passent par ``DuckLakeTable`` (upserts réels, un snapshot chacun) ; la maintenance par
le véritable nœud ``maintain_ducklake`` branché sur une fabrique de catalogues fichiers. L'horloge
de la maintenance est injectée (``now``) pour faire vieillir les snapshots sans attendre.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from conftest import build_serving_world, file_connector_factory
from kedro_pipeline.config import load_parameters
from kedro_pipeline.io import maintenance as ops
from kedro_pipeline.io.datasets import DuckLakeCatalogs
from kedro_pipeline.io.ducklake import DuckLakeLocation
from kedro_pipeline.pipelines.maintenance.nodes import maintain_ducklake
from kedro_pipeline.steps.maintenance import run_maintenance

pytestmark = pytest.mark.slow

N_UPSERTS = 200
ALIAS = "maint"
SCHEMA = "s1"
LOCATION = DuckLakeLocation(dbname=ALIAS, catalog_alias=ALIAS, schema=SCHEMA, bucket="b", data_path="p")


def _params(**overrides: Any) -> Dict[str, Any]:
    """Paramètres de production, restreints au catalogue du test."""
    params = dict(load_parameters("base")["maintenance"])
    params["CATALOGS"] = [
        {"dbname": ALIAS, "catalog_alias": ALIAS, "bucket": "b", "data_path": "p"}
    ]
    params["PARTITIONS"] = {}
    # Jamais de VACUUM ni de sauvegarde sur un catalogue fichier (aucun PostgreSQL)
    params.update(overrides)
    return params


def _checksum(conn: Any, qualified: str) -> str:
    """Empreinte du contenu complet, ordonné (``SELECT * ORDER BY``)."""
    frame = conn.execute(f"SELECT * FROM {qualified} ORDER BY ALL").df()
    return hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()


def _live_files(conn: Any, qualified_alias: str, schema: str, table: str) -> int:
    """Fichiers Parquet vivants d'une table : fichiers de données et fichiers de suppression."""
    data, deletes = conn.execute(
        f"SELECT count(data_file), count(delete_file) "
        f"FROM ducklake_list_files('{qualified_alias}', '{table}', schema => '{schema}')"
    ).fetchall()[0]
    return int(data) + int(deletes)


def _catalog_live_files(conn: Any, alias: str) -> int:
    """Fichiers Parquet vivants de toutes les tables d'un catalogue."""
    return sum(
        _live_files(conn, alias, schema, table)
        for schema, table in ops.list_tables(conn, alias).values()
    )


def _parquet_files(directory: Path) -> int:
    return sum(1 for _ in directory.rglob("*.parquet"))


@pytest.fixture(scope="module")
def populated(tmp_path_factory):
    """Catalogue fichier avec 200 petits upserts (un snapshot et un fichier chacun, inlining désactivé)."""
    pytest.importorskip("dt_ducklake_manager")
    root = tmp_path_factory.mktemp("maintenance")
    catalogs = DuckLakeCatalogs({}, {}, file_connector_factory(root))
    table = catalogs.table(LOCATION)
    with table.connect() as conn:
        for i in range(N_UPSERTS):
            # Trois clés remises à jour et deux nouvelles à chaque lot : fichiers de suppression compris
            frame = pd.DataFrame({"k": list(range(2 * i, 2 * i + 5)), "v": [float(i)] * 5})
            table.upsert(frame, ["k"], conn=conn)
    return root, catalogs, table


def test_two_hundred_upserts_are_compacted_expired_and_idempotent(populated) -> None:
    root, catalogs, table = populated
    qualified = f"{ALIAS}.{SCHEMA}.fact_table"
    tracking: Dict[str, Any] = {}
    real_now = datetime.now(timezone.utc)

    with table.connect() as conn:
        checksum = _checksum(conn, qualified)
        files_written = _live_files(conn, ALIAS, SCHEMA, "fact_table")
        snapshots_written = ops.count_snapshots(conn, ALIAS)
    assert files_written >= N_UPSERTS and snapshots_written >= N_UPSERTS

    # Passage 1 (horloge réelle) : fichiers fusionnés, aucun snapshot encore périmé
    first = run_maintenance(catalogs, params=_params(), now=real_now)
    assert not first.failures
    prefix = f"ducklake/{ALIAS}/{SCHEMA}"
    assert first.metrics[f"{prefix}/files_before"] >= N_UPSERTS
    assert first.metrics[f"{prefix}/files_after"] < 0.1 * first.metrics[f"{prefix}/files_before"]
    assert first.metrics[f"{prefix}/rows_after"] == first.metrics[f"{prefix}/rows_before"]
    assert first.metrics["ducklake/snapshots_expired"] == 0
    with table.connect() as conn:
        assert _checksum(conn, qualified) == checksum

    # Passage 2, même horloge : sans effet notable (idempotence)
    second = run_maintenance(catalogs, params=_params(), now=real_now)
    assert second.metrics[f"{prefix}/files_before"] == first.metrics[f"{prefix}/files_after"]
    assert second.metrics[f"{prefix}/files_after"] <= second.metrics[f"{prefix}/files_before"]
    assert second.metrics["ducklake/snapshots_expired"] == 0 and second.metrics["ducklake/files_cleaned"] == 0
    with table.connect() as conn:
        assert _checksum(conn, qualified) == checksum

    # Passage 3, 30 jours plus tard : snapshots périmés expirés, fichiers remplacés supprimés
    later = real_now + timedelta(days=30)
    third = run_maintenance(catalogs, params=_params(), now=later)
    assert third.metrics["ducklake/snapshots_expired"] >= N_UPSERTS
    assert third.metrics[f"ducklake/{ALIAS}/snapshots"] == 1
    assert third.metrics["ducklake/files_cleaned"] >= N_UPSERTS
    with table.connect() as conn:
        assert _checksum(conn, qualified) == checksum
        live = _catalog_live_files(conn, ALIAS)
    # Il ne reste sur le disque que les fichiers que le dernier snapshot référence (la table de
    # faits et les tables de métadonnées du schéma)
    assert _parquet_files(root / f"{ALIAS}_data") == live

    # Passage 4 : plus rien à expirer ni à supprimer
    fourth = run_maintenance(catalogs, params=_params(), now=later)
    assert fourth.metrics["ducklake/snapshots_expired"] == 0 and fourth.metrics["ducklake/files_cleaned"] == 0


def test_the_node_runs_the_whole_step_on_the_file_catalogs(populated) -> None:
    root, catalogs, table = populated
    outputs = maintain_ducklake(catalogs, _params(), {})
    metrics = outputs["metrics"]
    assert metrics["units/planned"] == 1 and metrics["units/failed"] == 0
    assert "ducklake/max_files_per_table" in metrics and metrics["mlflow/stale_runs_closed"] == 0


def test_partitioning_is_applied_once_and_leaves_the_existing_files(tmp_path, caplog) -> None:
    pytest.importorskip("dt_ducklake_manager")
    catalogs = DuckLakeCatalogs({}, {}, file_connector_factory(tmp_path))
    table = catalogs.table(LOCATION)
    table.upsert(pd.DataFrame({"k": list(range(30)), "year": [2020 + i % 3 for i in range(30)]}), ["k"])
    partitions = {f"{ALIAS}.{SCHEMA}": ["year"]}
    now = datetime.now(timezone.utc)

    with caplog.at_level("INFO"):
        run_maintenance(catalogs, params=_params(PARTITIONS=partitions), now=now)
    with table.connect() as conn:
        assert ops.current_partitioning(conn, ALIAS, SCHEMA, "fact_table") == ["year"]
        files_after_first = _live_files(conn, ALIAS, SCHEMA, "fact_table")
    assert any("repartition('fact_table'" in record.message for record in caplog.records)

    caplog.clear()
    with caplog.at_level("INFO"):
        run_maintenance(catalogs, params=_params(PARTITIONS=partitions), now=now)
    assert not any("Partitionnement" in record.message for record in caplog.records)
    with table.connect() as conn:
        assert _live_files(conn, ALIAS, SCHEMA, "fact_table") == files_after_first

    # Les écritures suivantes suivent le partitionnement posé
    table.upsert(pd.DataFrame({"k": list(range(100, 130)), "year": [2020 + i % 3 for i in range(30)]}), ["k"])
    with table.connect() as conn:
        paths = [row[0] for row in conn.execute(
            f"SELECT data_file FROM ducklake_list_files('{ALIAS}', 'fact_table', schema => '{SCHEMA}')"
        ).fetchall()]
    assert any("year=" in path for path in paths)


def test_serving_catalog_storage_is_bounded_after_successive_full_publications(tmp_path) -> None:
    from test_serving_catalog import _specs

    world = build_serving_world(tmp_path)
    serving = world.catalog.location
    catalogs = DuckLakeCatalogs({}, {}, world.factory)
    entry = {
        "dbname": serving.dbname, "catalog_alias": serving.catalog_alias,
        "bucket": serving.bucket, "data_path": serving.data_path,
    }
    params = _params(CATALOGS=[entry])
    data_directory = tmp_path / f"{serving.dbname}_data"

    # Cinq publications complètes : chacune recrée les tables, les fichiers précédents s'accumulent
    for factor in (1.0, 2.0, 3.0, 4.0, 5.0):
        with world.catalog.connect() as conn:
            world.catalog.publish(_specs(world, factor), "full", conn=conn)
    with world.catalog.connect() as conn:
        qualified = world.catalog.qualified_name("cells")
        checksum = _checksum(conn, qualified)
    files_before = _parquet_files(data_directory)

    # Rétention non échue : rien à supprimer
    now = datetime.now(timezone.utc)
    recent = run_maintenance(catalogs, params=params, now=now)
    assert not recent.failures and recent.metrics["ducklake/files_cleaned"] == 0

    # Rétention échue : snapshots expirés, fichiers des anciennes publications supprimés
    later = now + timedelta(days=30)
    expired = run_maintenance(catalogs, params=params, now=later)
    assert not expired.failures
    assert expired.metrics["ducklake/snapshots_expired"] > 0 and expired.metrics["ducklake/files_cleaned"] > 0
    files_after = _parquet_files(data_directory)
    assert files_after < files_before
    # Seuls les fichiers de la dernière publication demeurent, et son contenu est intact
    with world.catalog.connect() as conn:
        live = _catalog_live_files(conn, serving.catalog_alias)
        assert _checksum(conn, qualified) == checksum
    assert files_after == live
