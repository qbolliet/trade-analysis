"""Audit de couverture d'une source téléchargée : une requête SQL agrégée, registre fictif."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from kedro_pipeline.io.ducklake import DuckLakeTable
from kedro_pipeline.steps.coverage import audit_coverage, coverage_metrics_of, coverage_query
from macroforecast.tracking import CapturingTracker

T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
RUNTIME = {"ANALYSIS_START_YEAR": {"eurostat": 1988, "comtrade": 1994}}


class _Registry:
    """Vue factice du registre de téléchargement (les deux dispositions)."""

    def __init__(self, pairs=None, batches=None) -> None:
        self.pairs, self.batches = pairs or {}, batches or {}

    def pairs_last_download(self):
        return dict(self.pairs)

    def batches_by_year(self):
        return dict(self.batches)


def test_coverage_query_aggregates_by_reporter() -> None:
    sql = coverage_query('"c"."s"."fact_table"', {"REPORTER": "r", "PERIOD": "p", "PRODUCT": "k"})
    assert "GROUP BY 1" in sql and 'count(DISTINCT "k")' in sql and "SELECT *" not in sql


def test_metrics_without_planned_query_or_pace() -> None:
    empty = pd.DataFrame(columns=["reporter", "min_period", "max_period", "n_products", "n_rows"])
    metrics = coverage_metrics_of(
        n_planned=0, n_never=0, by_reporter=empty, expected_reporters=[], start_year=None,
        n_processed=None,
    )
    assert metrics == {
        "coverage/queries_total": 0.0, "coverage/queries_never_downloaded": 0.0,
        "coverage/share_downloaded": 1.0, "coverage/eta_days": 0.0,
    }
    # Requêtes restantes sans rythme connu : pas d'estimation
    assert "coverage/eta_days" not in coverage_metrics_of(
        n_planned=2, n_never=1, by_reporter=empty, expected_reporters=[], start_year=None, n_processed=0,
    )


def test_eurostat_audit_on_a_local_catalog(ducklake_conn) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    rows = [
        {"reporter": reporter, "TIME_PERIOD": period, "product": product, "partner": "CN", "OBS_VALUE": 1.0}
        for reporter, periods in (("FR", ("1988", "1989")), ("DE", ("1995",)))
        for period in periods
        for product in ("01", "02")
    ]
    write_dataframe(conn, pd.DataFrame(rows), ["reporter", "TIME_PERIOD", "product", "partner"],
                    catalog_alias=alias, schema="comext")
    planned = [
        SimpleNamespace(dimensions={"reporter": reporter, "product": product})
        for product in ("01", "02") for reporter in ("FR", "DE")
    ]
    registry = _Registry(pairs={("FR", "01"): T0, ("FR", "02"): T0, ("DE", "01"): T0})
    tracker = CapturingTracker()
    result = audit_coverage(
        DuckLakeTable(conn, alias, "comext"), registry, planned, source="eurostat",
        params={"COVERAGE": {"EXPECTED_FULL_HISTORY_REPORTERS": ["FR", "DE", "IT"]}},
        runtime=RUNTIME, n_processed=2, tracker=tracker,
    )
    assert result.metrics["coverage/queries_total"] == 4
    assert result.metrics["coverage/queries_never_downloaded"] == 1
    assert result.metrics["coverage/share_downloaded"] == 0.75
    assert result.metrics["coverage/min_period"] == 1988
    # DE commence en 1995, IT est absent : deux reporters sous la première année
    assert result.metrics["coverage/reporters_below_start"] == 2
    assert result.metrics["coverage/eta_days"] == 0.5
    by_reporter = tracker.tables["coverage/by_reporter.csv"]
    assert by_reporter.set_index("reporter").loc["FR", ["min_period", "max_period", "n_products", "n_rows"]].tolist() == [
        1988, 1989, 2, 4
    ]
    assert "coverage/by_year.csv" not in tracker.tables
    assert result.n_units_planned == 4 and result.n_units_succeeded == 3


def test_comtrade_audit_shares_by_year(ducklake_conn) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    write_dataframe(
        conn,
        pd.DataFrame({"reporterCode": [251, 251], "period": ["2022", "2023"], "cmdCode": ["010121", "010121"],
                      "primaryValue": [1.0, 2.0]}),
        ["reporterCode", "period", "cmdCode"], catalog_alias=alias, schema="C_A_HS",
    )
    planned = [SimpleNamespace(periods=year, products=[code]) for year in ("2022", "2023") for code in ("01", "02")]
    registry = _Registry(batches={2022: {("01",): T0, ("02",): T0}, 2023: {("01",): T0}})
    tracker = CapturingTracker()
    audit_coverage(
        DuckLakeTable(conn, alias, "C_A_HS"), registry, planned, source="comtrade",
        params={}, runtime=RUNTIME, n_processed=1, tracker=tracker,
    )
    by_year = tracker.tables["coverage/by_year.csv"]
    assert by_year.to_dict("list") == {"year": [2022, 2023], "share_queries_downloaded": [1.0, 0.5]}
    assert tracker.metrics["coverage/min_period"] == 2022
    assert tracker.metrics["coverage/reporters_below_start"] == 0


@pytest.mark.parametrize("source", ["eurostat", "comtrade"])
def test_registry_failure_propagates_to_the_download_step(source) -> None:
    class Broken:
        def pairs_last_download(self):
            raise OSError("registre illisible")

        batches_by_year = pairs_last_download

    with pytest.raises(OSError):
        audit_coverage(None, Broken(), [], source=source, params={}, runtime=RUNTIME)
