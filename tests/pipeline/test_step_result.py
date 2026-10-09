"""Contrat commun des fonctions d'étape : résultat, runs d'unités, rapport, environnement."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from kedro_pipeline.io.ducklake import (
    BorrowedReader,
    ConnectionReader,
    DuckLakeLocation,
    DuckLakeTable,
    attached_catalog_alias,
)
from kedro_pipeline.io.tracking import build_step_report, report_layout
from kedro_pipeline.steps.result import StepResult, failure_message, mark, shared_runs
from macroforecast.tracking import CapturingTracker

ROOT = Path(__file__).resolve().parents[2]
LOCATION = DuckLakeLocation(
    dbname="vulnerabilities", catalog_alias="vulnerabilities", schema="indicators",
    bucket=None, data_path="data",
)


class _Connector:
    """Connecteur factice : alias propre, aucune connexion réelle."""

    catalog_alias = "attached"

    def connect(self):  # pragma: no cover - jamais appelé
        raise AssertionError("no connection expected")


def _factory(location, pg, s3, **kwargs):
    return _Connector()


def test_raise_if_failed_is_silent_without_failures() -> None:
    StepResult("synthesis", 3, 3).raise_if_failed()


def test_raise_if_failed_raises_the_step_exception_with_its_cause() -> None:
    cause = ValueError("gravité impossible")
    failure = RuntimeError("1 millésime(s) en échec sur 2 : ['HS2017']")
    failure.__cause__ = cause
    result = StepResult(
        "baci", 2, 1, failures={"HS2017": failure_message(cause)}, failure_exception=failure,
    )
    with pytest.raises(RuntimeError, match="1 millésime") as info:
        result.raise_if_failed()
    assert info.value.__cause__ is cause
    assert result.failures == {"HS2017": "ValueError: gravité impossible"}


def test_raise_if_failed_defaults_to_a_runtime_error_naming_the_units() -> None:
    with pytest.raises(RuntimeError, match=r"x: 2 unit\(s\) failed out of 3: \['a', 'b'\]"):
        StepResult("x", 3, 1, failures={"b": "boom", "a": "boom"}).raise_if_failed()


def test_fields_and_outputs_are_readable_by_key() -> None:
    result = StepResult("serving", 2, 2, outputs={"rows": {"t": 4}})
    assert result["failures"] == {} and result["rows"] == {"t": 4}
    with pytest.raises(KeyError):
        result["unknown"]


def test_failed_count_defaults_to_the_failures() -> None:
    assert StepResult("x", failures={"a": "boom"}).n_failed == 1
    assert StepResult("download", 10, 8, n_units_failed=2).n_failed == 2


def test_shared_runs_yield_the_caller_tracker_without_report() -> None:
    tracker = CapturingTracker()
    runs = shared_runs(tracker)
    with runs("HS2017", {"vintage": "HS2017"}) as run:
        run.tracker.log_metrics({"a/b": 1.0})
        run.publish(StepResult("baci"))
    assert run.tracker is tracker and run.progress is None and tracker.metrics == {"a/b": 1.0}


def test_mark_updates_the_progress_holder() -> None:
    class Progress:
        step = None

    progress = Progress()
    mark(progress, "redressement BACI")
    assert progress.step == "redressement BACI"


def test_step_report_uses_the_layout_and_the_run_metrics() -> None:
    result = StepResult(
        "serving", 2, 0, failures={"publication": "boom"},
        metrics={"serving/n_failures": 1.0}, units_label="2 tables (mode full)",
    )
    report = build_step_report(
        result, node="publish_serving", params={}, context={"duration_s": 3.0},
    )
    assert report.units.planned_label == "2 tables (mode full)" and report.units.failed == 1
    assert report.context["duration_s"] == 3.0 and report.health == "failed"
    assert report.failures == {"publication": "boom"}
    assert report_layout("download")[0].__name__ == "key_figures_downloads"


def test_lazy_handle_reader_is_picklable_and_borrowed_one_lends_its_connection() -> None:
    import pickle

    lazy = DuckLakeTable.lazy(LOCATION, None, None, connector_factory=_factory)
    reader = lazy.reader(["SELECT 1"])
    assert isinstance(reader, ConnectionReader) and reader.setup_statements == ("SELECT 1",)
    pickle.loads(pickle.dumps(reader))
    borrowed = DuckLakeTable("conn", "db", "indicators")
    assert isinstance(borrowed.reader(), BorrowedReader)
    with pytest.raises(RuntimeError, match="borrows a connection"):
        borrowed.connector()


def test_attached_alias_is_the_connector_one() -> None:
    lazy = DuckLakeTable.lazy(LOCATION, None, None, connector_factory=_factory)
    assert attached_catalog_alias(lazy) == "attached"
    assert attached_catalog_alias(DuckLakeTable("conn", "db", "indicators")) == "db"


def test_step_functions_never_read_the_environment() -> None:
    """Aucune lecture de variable d'environnement dans les fonctions d'étape."""
    pattern = re.compile(r"os\.environ|getenv\(")
    offenders = [
        path.name
        for path in sorted((ROOT / "kedro_pipeline" / "steps").glob("*.py"))
        if pattern.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_frame_artifacts_are_kept_as_given() -> None:
    frame = pd.DataFrame({"a": [1]})
    assert StepResult("x", artifacts={"t.csv": frame}).artifacts["t.csv"] is frame
