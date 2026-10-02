"""Tests des options transmises à ``statflows`` 0.1.1 (PS-27, PD-11).

- ``download_buffering_options`` : ``DOWNLOADS.<DATAFLOW>.BUFFERING`` → arguments de
  ``download_updates`` ;
- clés de fragment du registre (reporter pour Eurostat, année pour Comtrade) ;
- ``compute_write_options`` : options de ``write_dataframe`` des étapes de calcul,
  vérifiées sur un catalogue DuckLake fichier (ajout de colonne, instantané).
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest
import yaml

from kedro_pipeline.io.ducklake import compute_write_options, download_buffering_options
from scripts.download_comtrade import registry_shard_key as comtrade_shard_key
from scripts.download_eurostat_comext import registry_shard_key as eurostat_shard_key
from statflows import ComtradeQueryRequest, EurostatQueryRequestV30
from statflows.core.download import SDMXDownloader, download_updates
from statflows.storage.ducklake.tables import FACT_TABLE, write_dataframe

ROOT = Path(__file__).resolve().parents[1]
CONFIG_FILES = {
    "comtrade": ["config/datasets/comtrade.yaml", "config/profiles/demo/comtrade.yaml"],
    "eurostat": ["config/datasets/eurostat.yaml", "config/profiles/demo/eurostat.yaml"],
}
EXPECTED_BUFFERING = {
    "REGISTRY_FLUSH_EVERY": 500,
    "REGISTRY_FLUSH_SECONDS": 300,
    "WRITE_BATCH_QUERIES": 200,
    "WRITE_BATCH_ROWS": 500000,
    "COMPACT_AFTER_UPDATE": False,
    "SHARD_REGISTRY": True,
}


def _buffering(path: str) -> Dict[str, Any]:
    config = yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))
    return config["DOWNLOADS"][config["DATAFLOW"]]["BUFFERING"]


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("path", [p for paths in CONFIG_FILES.values() for p in paths])
def test_buffering_block_in_every_download_config(path: str) -> None:
    """Les quatre fichiers (production et demo) portent le bloc BUFFERING attendu."""
    buffering = _buffering(path)
    assert {key: buffering[key] for key in EXPECTED_BUFFERING} == EXPECTED_BUFFERING
    assert buffering["DUCKLAKE_OPTIONS"] is None


def test_config_translates_to_statflows_arguments() -> None:
    """Toutes les clés produites existent dans la signature de ``download_updates``."""
    options = download_buffering_options(_buffering(CONFIG_FILES["eurostat"][0]), shard_key=eurostat_shard_key, environ={})
    accepted = set(inspect.signature(download_updates).parameters)
    assert set(options) <= accepted
    assert set(options) <= set(inspect.signature(SDMXDownloader).parameters)
    assert options["registry_flush_every"] == 500
    assert options["registry_flush_seconds"] == 300
    assert options["write_batch_queries"] == 200
    assert options["write_batch_rows"] == 500000
    assert options["registry_shard_key"] is eurostat_shard_key
    # Téléchargement : compaction désactivée, jamais d'ajout de colonne
    assert options["update_options"] == {"compact_after_update": False}


def test_buffering_defaults_reproduce_unbuffered_behaviour() -> None:
    options = download_buffering_options(None, environ={})
    assert options["registry_flush_every"] == 1
    assert options["registry_flush_seconds"] is None
    assert options["write_batch_rows"] is None and options["write_batch_queries"] is None
    assert options["registry_shard_key"] is None and options["run_id"] is None


def test_shard_registry_requires_a_shard_key() -> None:
    with pytest.raises(ValueError, match="SHARD_REGISTRY"):
        download_buffering_options({"SHARD_REGISTRY": True}, environ={})


def test_shard_registry_false_drops_the_key() -> None:
    options = download_buffering_options({"SHARD_REGISTRY": False}, shard_key=eurostat_shard_key, environ={})
    assert options["registry_shard_key"] is None


def test_buffering_run_id_comes_from_workflow_id() -> None:
    assert download_buffering_options(None, environ={"WORKFLOW_ID": "wf-1"})["run_id"] == "wf-1"


# ──────────────────────────────────────────────────────────────────────
# Clés de fragment
# ──────────────────────────────────────────────────────────────────────


def test_eurostat_shard_key_is_the_reporter() -> None:
    def query(reporter: Any) -> EurostatQueryRequestV30:
        return EurostatQueryRequestV30(dataflow="DS-045409", dimensions={"reporter": reporter, "product": "854140"})

    assert eurostat_shard_key(query("FR")) == "FR"
    assert eurostat_shard_key(query(["FR", "DE"])) == "FR_DE"
    assert eurostat_shard_key(EurostatQueryRequestV30(dataflow="DS-045409", dimensions={})) == "_default"


def test_comtrade_shard_key_is_the_year() -> None:
    def query(periods: Any) -> ComtradeQueryRequest:
        return ComtradeQueryRequest(dataflow="C_A_HS", periods=periods)

    assert comtrade_shard_key(query("2023")) == "2023"
    assert comtrade_shard_key(query("202305")) == "2023"
    assert comtrade_shard_key(query(["2023", "2022"])) == "2022_2023"
    assert comtrade_shard_key(ComtradeQueryRequest(dataflow="C_A_HS", period_start="2020")) == "_default"


# ──────────────────────────────────────────────────────────────────────
# Options d'écriture des étapes de calcul
# ──────────────────────────────────────────────────────────────────────


def test_compute_write_options_without_workflow_id() -> None:
    options = compute_write_options("compute_x C_A_HS", environ={})
    assert options == {
        "update_options": {"allow_new_columns": True, "compact_after_update": False},
        "run_id": None,
        "commit_message": "compute_x C_A_HS",
    }
    # Les trois clés existent dans la signature installée de write_dataframe
    assert {"update_options", "run_id", "commit_message"} <= set(inspect.signature(write_dataframe).parameters)


def test_compute_write_options_with_workflow_id() -> None:
    assert compute_write_options("m", environ={"WORKFLOW_ID": "wf-7k2qd"})["run_id"] == "wf-7k2qd"
    assert compute_write_options("m", environ={"WORKFLOW_ID": ""})["run_id"] is None


def test_write_dataframe_adds_new_metric_column(ducklake_conn) -> None:
    """Une nouvelle métrique (colonne) est ajoutée à la table, les lignes non touchées valent NULL."""
    conn, alias = ducklake_conn
    options = compute_write_options("compute_x unit", environ={"WORKFLOW_ID": "wf-1"})

    first = pd.DataFrame({"id": [1, 2], "HHI": [0.1, 0.2]})
    assert write_dataframe(conn, first, ["id"], catalog_alias=alias, schema="s1", **options) is True

    second = pd.DataFrame({"id": [2, 3], "HHI": [0.5, 0.3], "CDI2": [7.0, 8.0]})
    assert write_dataframe(conn, second, ["id"], catalog_alias=alias, schema="s1", **options) is False

    rows = conn.execute(f'SELECT id, HHI, CDI2 FROM {alias}.s1.{FACT_TABLE} ORDER BY id').fetchall()
    assert rows[0][:2] == (1, 0.1) and pd.isna(rows[0][2])
    assert rows[1] == (2, 0.5, 7.0)
    assert rows[2] == (3, 0.3, 8.0)


def test_write_dataframe_without_options_rejects_new_column(ducklake_conn) -> None:
    """Contrôle : sans ``allow_new_columns``, la nouvelle colonne n'est pas acceptée."""
    conn, alias = ducklake_conn
    write_dataframe(conn, pd.DataFrame({"id": [1], "HHI": [0.1]}), ["id"], catalog_alias=alias, schema="s1")
    with pytest.raises(Exception):
        write_dataframe(
            conn, pd.DataFrame({"id": [1], "HHI": [0.1], "CDI2": [1.0]}), ["id"], catalog_alias=alias, schema="s1"
        )
