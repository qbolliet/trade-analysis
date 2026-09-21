"""Protocole de suivi : `log_text`, `CapturingTracker`, séparateur des métriques, statut FAILED."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest

from macroforecast.tracking import (
    NULL_TRACKER,
    CapturingTracker,
    MlflowTracker,
    NullTracker,
    flatten_metrics,
    rekey_metrics,
)


@dataclass
class _Inner:
    share: float = 0.5


@dataclass
class _Report:
    n: int = 3
    inner: _Inner = None
    flag: bool = True

    def __post_init__(self) -> None:
        self.inner = self.inner or _Inner()


def test_flatten_metrics_default_separator_is_unchanged() -> None:
    assert flatten_metrics(_Report(), prefix="step") == {"step.n": 3.0, "step.inner.share": 0.5, "step.flag": 1.0}


def test_flatten_metrics_slash_separator_groups_by_section() -> None:
    assert flatten_metrics(_Report(), prefix="step", sep="/") == {
        "step/n": 3.0, "step/inner/share": 0.5, "step/flag": 1.0,
    }
    assert flatten_metrics({"a": {"b": 1}}, sep="/") == {"a/b": 1.0}


def test_rekey_metrics_replaces_dots_only() -> None:
    assert rekey_metrics({"baci.gravity.r_squared": 0.7, "coverage/share_min": 1.0}) == {
        "baci/gravity/r_squared": 0.7, "coverage/share_min": 1.0,
    }


def test_null_tracker_accepts_log_text() -> None:
    assert NullTracker().log_text("texte", "report/summary.md") is None
    assert NULL_TRACKER.log_text("texte", "a.md") is None


def test_capturing_tracker_remembers_and_forwards() -> None:
    class Spy(NullTracker):
        def __init__(self) -> None:
            self.calls = []

        def __enter__(self):
            self.calls.append("enter")
            return self

        def __exit__(self, *exc) -> None:
            self.calls.append(("exit", exc[0]))

        def log_metrics(self, metrics, step=None) -> None:
            self.calls.append(("metrics", dict(metrics), step))

        def log_text(self, text, artifact_file) -> None:
            self.calls.append(("text", artifact_file))

    spy = Spy()
    tracker = CapturingTracker(spy)
    table = pd.DataFrame({"a": [1]})
    with tracker:
        tracker.log_metrics({"m": 1.0}, step=2)
        tracker.log_metrics({"n": 2.0})
        tracker.log_table(table, "d/t.csv")
        tracker.log_dict({"k": 1}, "d/o.json")
        tracker.log_text("txt", "report/summary.md")
        tracker.set_tags({"t": "v"})
        tracker.log_params({"p": 1})
    assert tracker.metrics == {"m": 1.0, "n": 2.0}
    assert tracker.tables["d/t.csv"] is table and tracker.dicts["d/o.json"] == {"k": 1}
    assert tracker.texts == {"report/summary.md": "txt"} and tracker.tags == {"t": "v"} and tracker.params == {"p": 1}
    assert spy.calls == ["enter", ("metrics", {"m": 1.0}, 2), ("metrics", {"n": 2.0}, None),
                         ("text", "report/summary.md"), ("exit", None)]


def test_capturing_tracker_without_inner_is_a_recorder_and_never_swallows() -> None:
    tracker = CapturingTracker()
    with pytest.raises(KeyError):
        with tracker:
            tracker.log_metrics({"m": 1.0})
            raise KeyError("métier")
    assert tracker.metrics == {"m": 1.0}


def test_mlflow_tracker_ends_the_run_failed_when_an_exception_crosses_it(mlflow_uri) -> None:
    import mlflow

    uri = mlflow_uri
    tracker = MlflowTracker(tracking_uri=uri, experiment="t", run_name="ko")
    with pytest.raises(RuntimeError):
        with tracker:
            raise RuntimeError("boum")
    with MlflowTracker(tracking_uri=uri, experiment="t", run_name="ok"):
        pass
    client = mlflow.tracking.MlflowClient(tracking_uri=uri)
    experiment = client.get_experiment_by_name("t")
    statuses = {run.info.run_name: run.info.status for run in client.search_runs([experiment.experiment_id])}
    assert statuses == {"ko": "FAILED", "ok": "FINISHED"}


def test_mlflow_tracker_log_text_and_failure_is_a_warning(mlflow_uri) -> None:
    import mlflow

    uri = mlflow_uri
    tracker = MlflowTracker(tracking_uri=uri, experiment="t")
    with tracker:
        tracker.log_text("# titre", "report/summary.md")
        run_id = mlflow.active_run().info.run_id
    client = mlflow.tracking.MlflowClient(tracking_uri=uri)
    assert [a.path for a in client.list_artifacts(run_id, "report")] == ["report/summary.md"]
    # Run non ouvert : l'écriture est ignorée sans lever
    tracker.log_text("x", "report/other.md")
