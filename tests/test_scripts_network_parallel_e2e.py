"""Calcul parallèle des millésimes du réseau (catalogue DuckLake local SQLite).

Chaque millésime est calculé dans un worker qui ouvre ses connexions ; le parent écrit.
On vérifie que les scores ne dépendent pas du nombre de processus et que le calcul
séparé de l'écriture (``compute_network_vintage`` puis ``write_network_vintage``)
reproduit exactement ``run_network_vulnerabilities``.
"""

from __future__ import annotations

from functools import partial
from pathlib import Path

import narwhals as nw
import pandas as pd
import pytest

pytest.importorskip("dt_ducklake_manager")

from kedro_pipeline.io.ducklake import ConnectionReader, DuckLakeTable  # noqa: E402
from kedro_pipeline.parallel import parallel_map  # noqa: E402
from macroforecast.trade.vulnerabilities import NetworkVulnerabilityConfig  # noqa: E402
from macroforecast.trade.vulnerabilities.runner import (  # noqa: E402
    run_network_vulnerabilities,
    write_network_vintage,
)
from scripts.compute_network_vulnerabilities import (  # noqa: E402
    VintageTask,
    annotate_network_in_force,
    compute_vintage_task,
)
from tests.sqlite_catalog import (  # noqa: E402
    ALIAS,
    SqliteCatalogConnector,
    catalog_paths,
    open_catalog,
)
from tests.vulnerabilities.fixtures import network_frame  # noqa: E402

NOMENCLATURES = {"HS2017": 2017, "HS2022": 2022}
SCHEMAS = {"HS2017": "baci_hs2017", "HS2022": "baci_hs2022"}
KEYS = ["product", "year", "exporter", "importer"]
FLOWS = ("import", "export")


@pytest.fixture
def baci_catalog(tmp_path: Path):
    """Catalogue SQLite avec un schéma BACI factice par millésime."""
    from statflows.storage.ducklake.tables import write_dataframe

    (tmp_path / "data").mkdir()
    conn = open_catalog(*catalog_paths(tmp_path))
    base = network_frame().drop(columns=["classification"])
    for offset, (label, schema) in enumerate(SCHEMAS.items()):
        frame = base.copy()
        # Valeurs distinctes d'un millésime à l'autre
        frame["reconciled_value"] = frame["reconciled_value"] * (1 + offset)
        write_dataframe(conn, frame, KEYS, catalog_alias=ALIAS, schema=schema)
    try:
        yield conn
    finally:
        conn.close()


def _tasks(tmp_path: Path):
    reader = ConnectionReader(partial(SqliteCatalogConnector, *catalog_paths(tmp_path)))
    return [
        VintageTask(
            label=label, source_schema=schema, source_catalog_alias=ALIAS,
            source_reader=reader, result_reader=reader, result_catalog_alias=ALIAS,
            result_schema="network_indicators", config=NetworkVulnerabilityConfig(),
            flows=FLOWS, backend="pandas", log_artifacts=True, nomenclatures=NOMENCLATURES,
        )
        for label, schema in SCHEMAS.items()
    ]


def _sorted(frame: pd.DataFrame) -> pd.DataFrame:
    keys = ["classification", "product", "year", "flow"]
    return frame.sort_values(keys).reset_index(drop=True)


@pytest.mark.slow
def test_vintages_identical_for_one_and_two_processes(baci_catalog, tmp_path):
    runs = {
        n_jobs: {out.label: out for _, out in parallel_map(compute_vintage_task, _tasks(tmp_path), n_jobs)}
        for n_jobs in (1, 2)
    }
    for label in SCHEMAS:
        one, two = runs[1][label], runs[2][label]
        assert one.error is None and two.error is None
        pd.testing.assert_frame_equal(_sorted(one.result), _sorted(two.result), check_exact=True)
        assert one.report.cells == two.report.cells > 0
        assert "in_force" in one.result.columns
        # Paramètres et artefacts enregistrés pour le rejeu dans le run du millésime
        assert any(name == "log_params" for name, _, _ in one.recorded.calls)
    assert not _sorted(runs[1]["HS2017"].result).equals(_sorted(runs[1]["HS2022"].result))


@pytest.mark.slow
def test_split_compute_and_write_matches_the_one_call_runner(baci_catalog, tmp_path):
    config = NetworkVulnerabilityConfig()
    outcome = compute_vintage_task(_tasks(tmp_path)[0])
    assert outcome.error is None
    writer_split = DuckLakeTable(baci_catalog, ALIAS, "net_split", label="HS2017")
    write_network_vintage(
        nw.from_native(outcome.result, eager_only=True), outcome.report,
        classification="HS2017", config=config, result_conn=baci_catalog,
        result_catalog_alias=ALIAS, result_schema="net_split", writer=writer_split.writer(),
    )
    run_network_vulnerabilities(
        baci_catalog, source_catalog_alias=ALIAS, source_schema="baci_hs2017",
        classification="HS2017", result_schema="net_direct", config=config, flows=FLOWS,
        writer=DuckLakeTable(baci_catalog, ALIAS, "net_direct", label="HS2017").writer(),
        annotate=partial(annotate_network_in_force, nomenclatures=NOMENCLATURES, config=config),
    )
    split, direct = (
        baci_catalog.execute(f'SELECT * FROM "{ALIAS}"."{schema}"."fact_table"').df()
        for schema in ("net_split", "net_direct")
    )
    pd.testing.assert_frame_equal(_sorted(split), _sorted(direct), check_exact=True)
    assert outcome.report.created is True


@pytest.mark.slow
def test_a_failing_vintage_is_returned_not_raised(baci_catalog, tmp_path):
    tasks = _tasks(tmp_path)
    broken = VintageTask(**{**tasks[0].__dict__, "label": "HS1992", "source_schema": "absent"})
    results = {out.label: out for _, out in parallel_map(compute_vintage_task, [broken, tasks[1]], 2)}
    assert results["HS1992"].error is not None and results["HS1992"].result is None
    assert results["HS2022"].error is None and results["HS2022"].result is not None
