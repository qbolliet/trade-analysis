"""Remplissage fictif de bout en bout : téléchargement statflows réel + porte BACI + redressement.

Rejoue ``scripts/seed_synthetic_comtrade.py`` sur un catalogue DuckLake temporaire
(connecteur factice qui rend la connexion du test) et vérifie que ce qu'il écrit
est exactement ce que lisent les étapes aval : registre de téléchargement (porte
de complétude de ``process_baci_hs``), table de faits (lecture SQL de BACI) et
redressement.
"""

from __future__ import annotations

import datetime as dt
import itertools
from dataclasses import replace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from kedro_pipeline.io.registry_views import DownloadRegistryView
from kedro_pipeline.synthetic.comtrade import ReportingConfig, SyntheticComtradeClient
from kedro_pipeline.synthetic.io import SYNTHETIC_FLAG, load_registry, mark_synthetic_entries
from scripts.download_comtrade import build_split_queries
from scripts.process_baci_hs import (
    _read_comtrade_fact_table,
    completeness_by_year,
    eligible_years,
)
from statflows.core.download import download_updates

pytestmark = pytest.mark.slow

PRODUCTS = ["280519", "810510", "854110", "300490"]


class _NoCloseConnection:
    """Enveloppe d'une connexion dont ``close`` est sans effet (rendue à plusieurs lecteurs)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def close(self) -> None:
        return None


class _FakeConnector:
    """Connecteur DuckLake factice : ``connect()`` rend la connexion du test."""

    def __init__(self, conn: Any, alias: str) -> None:
        self._conn, self.catalog_alias = conn, alias

    def connect(self) -> Any:
        return _NoCloseConnection(self._conn)


def test_seed_then_baci_gate_and_reconciliation(
    ducklake_conn, synthetic_world, synthetic_reference, synthetic_section, tmp_path
) -> None:
    """Téléchargement fictif → registre complet → lecture SQL → BACI, sans adaptation des étapes aval."""
    from macroforecast.trade.processing import BaciConfig, required_columns, run_baci

    conn, alias = ducklake_conn
    client = SyntheticComtradeClient(
        synthetic_world, synthetic_reference, ReportingConfig.from_mapping(synthetic_section["REPORTING"]),
        product_universe=PRODUCTS,
    )
    # Requêtes planifiées comme le téléchargement réel : année × lot de produits
    queries = build_split_queries(
        "C_A_HS",
        {"reporters": pd.DataFrame({"code": ["251"]}), "products": pd.DataFrame({"code": PRODUCTS})},
        {"frequency": "annual", "flows": ["M", "X"], "type_code": "C", "classification": "HS"},
        {"reporters": {"include": None}, "products": {"include": PRODUCTS}},
        periods=["2019", "2020", "2021"], products_step=2,
    )
    registry_path = tmp_path / "last_downloads" / "comtrade.json"
    started_at = dt.datetime.now(dt.timezone.utc)

    report = download_updates(
        client=client, queries=queries, connector=_FakeConnector(conn, alias),
        structures_path=tmp_path / "structures.json", last_download_path=registry_path,
        bucket=None, max_runtime=None,
    )
    assert report.processed == len(queries) == 6
    assert report.rows_written > 0 and report.empty == 0
    assert mark_synthetic_entries(registry_path, None, started_at) == len(queries)
    assert all(e[SYNTHETIC_FLAG] for e in load_registry(registry_path, None).values())

    # Porte de complétude de BACI : toutes les années sont complètes
    view = DownloadRegistryView(registry_path, bucket=None, dataflow="C_A_HS")
    shares = completeness_by_year(queries, view.batches_by_year())
    assert list(eligible_years(view, queries, 1.0)) == [2019, 2020, 2021]

    # Lecture SQL de BACI puis redressement
    config = replace(BaciConfig(), min_mirror_flows=5, fas_countries=("CAN",))
    columns = list(dict.fromkeys(required_columns(config) + [config.schema.classification_col]))
    declarations = _read_comtrade_fact_table(conn, "C_A_HS", columns, "period", [2019, 2020, 2021])
    assert len(declarations) > 100
    isos = list(synthetic_world.iso3)
    dist = pd.DataFrame(list(itertools.permutations(isos, 2)), columns=["iso_o", "iso_d"])
    dist["distw"] = np.random.default_rng(0).uniform(500, 15000, len(dist))
    dist["contig"] = 0
    geo = pd.DataFrame({"iso3": isos, "landlocked": 0})
    result, baci_report = run_baci(declarations, dist, geo, config=config)
    assert baci_report.flows == len(result) > 50

    # Rejouer le remplissage ne change rien (requêtes déjà téléchargées, pas de doublon)
    before = conn.execute('SELECT count(*) FROM "C_A_HS".fact_table').fetchone()[0]
    download_updates(
        client=client, queries=queries, connector=_FakeConnector(conn, alias),
        structures_path=tmp_path / "structures.json", last_download_path=registry_path,
        bucket=None, max_runtime=None,
    )
    assert conn.execute('SELECT count(*) FROM "C_A_HS".fact_table').fetchone()[0] == before


def test_buffered_sharded_download_is_read_by_the_view(
    ducklake_conn, synthetic_world, synthetic_reference, synthetic_section, tmp_path
) -> None:
    """Téléchargement tamponné + registre fragmenté par année (PS-27) relu par la vue (PS-12.3).

    Les options sont celles que ``scripts/download_comtrade.py`` déduit de la configuration ;
    le résultat (lots par année, part de complétude) doit être celui du mode non tamponné.
    """
    from kedro_pipeline.io.ducklake import download_buffering_options
    from scripts.download_comtrade import registry_shard_key

    conn, alias = ducklake_conn
    client = SyntheticComtradeClient(
        synthetic_world, synthetic_reference, ReportingConfig.from_mapping(synthetic_section["REPORTING"]),
        product_universe=PRODUCTS,
    )
    queries = build_split_queries(
        "C_A_HS",
        {"reporters": pd.DataFrame({"code": ["251"]}), "products": pd.DataFrame({"code": PRODUCTS})},
        {"frequency": "annual", "flows": ["M", "X"], "type_code": "C", "classification": "HS"},
        {"reporters": {"include": None}, "products": {"include": PRODUCTS}},
        periods=["2019", "2020", "2021"], products_step=2,
    )
    registry_path = tmp_path / "last_downloads" / "comtrade.json"
    buffering = {
        "REGISTRY_FLUSH_EVERY": 4, "REGISTRY_FLUSH_SECONDS": 300,
        "WRITE_BATCH_QUERIES": 3, "WRITE_BATCH_ROWS": 500000,
        "COMPACT_AFTER_UPDATE": False, "SHARD_REGISTRY": True,
    }

    report = download_updates(
        client=client, queries=queries, connector=_FakeConnector(conn, alias),
        structures_path=tmp_path / "structures.json", last_download_path=registry_path,
        bucket=None, max_runtime=None,
        **download_buffering_options(buffering, shard_key=registry_shard_key, environ={}),
    )
    assert report.processed == len(queries) == 6 and report.rows_written > 0

    # Registre fragmenté : un fichier par année, pas de fichier unique
    assert not registry_path.exists()
    assert sorted(path.stem for path in registry_path.with_suffix("").glob("*.json")) == ["2019", "2020", "2021"]

    # La vue lit les fragments ; la porte de complétude voit toutes les années complètes
    batches = DownloadRegistryView(registry_path, bucket=None, dataflow="C_A_HS").batches_by_year()
    assert sorted(batches) == [2019, 2020, 2021] and all(len(year_batches) == 2 for year_batches in batches.values())
    assert list(eligible_years(batches, queries, 1.0)) == [2019, 2020, 2021]
