"""Publication du rapport de run : description, tags, métriques `checks/*`, artefacts."""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from kedro_pipeline.io.tracking import (
    DESCRIPTION_TAG,
    peak_memory_mb,
    publish_failure,
    publish_run_report,
    run_metrics,
)
from macroforecast.tracking import CapturingTracker, MlflowTracker, NullTracker
from macroforecast.tracking.report import Check, Section, Units, build_report
from scripts._run_report import RunScope, guarded_run, load_tracking_config, run_name

PARAMS = {"REPORT": {"HTML": True, "PLOTLY_JS": "cdn", "MAX_DESCRIPTION_CHARS": 7500, "MAX_TABLE_ROWS": 5}}


def _report():
    return build_report(
        "process_baci_HS2017",
        metrics={"baci/gravity/r_squared": 0.42, "baci/flows": 10.0},
        checks=[
            Check("baci/gravity/r_squared", ">=", 0.5, "warning", "R² de la gravité"),
            Check("baci/flows", ">", 0, "error", "Flux écrits"),
            Check("absent", ">", 0, "warning", "Absente"),
        ],
        units=Units(1, 1, 0),
        failures={},
        sections=[Section("Gravité", tables={"Ajustement": pd.DataFrame({"a": [1, 2]})})],
        tables={"conversion rates": pd.DataFrame({"product": ["2805"], "rate": [1.5]})},
        context={"workflow_id": "wf-42", "duration_s": 12.0, "peak_memory_mb": 300.0},
    )


def test_publish_on_a_recording_tracker() -> None:
    tracker = CapturingTracker()
    publish_run_report(tracker, _report(), PARAMS)
    assert tracker.tags["health"] == "warning" and tracker.tags["node"] == "process_baci_HS2017"
    assert tracker.tags["workflow_id"] == "wf-42" and tracker.tags["checks_failed"] == "R² de la gravité"
    assert tracker.tags[DESCRIPTION_TAG].startswith("### ⚠️ process_baci_HS2017 — avertissement")
    assert tracker.metrics == {
        "checks/n_passed": 3.0, "checks/n_warnings": 1.0, "checks/n_failed": 0.0, "checks/n_skipped": 1.0,
        "run/duration_seconds": 12.0, "run/peak_memory_mb": 300.0,
    }
    assert set(tracker.texts) == {"report/summary.md", "report/report.html"}
    assert set(tracker.tables) == {"report/checks.csv", "tables/conversion_rates.csv"}


def test_options_control_html_and_description_length() -> None:
    tracker = CapturingTracker()
    publish_run_report(tracker, _report(), {"REPORT": {"HTML": False, "MAX_DESCRIPTION_CHARS": 200}})
    assert "report/report.html" not in tracker.texts
    assert len(tracker.tags[DESCRIPTION_TAG]) <= 200
    # La copie intégrale n'est jamais tronquée
    assert "summary.md" not in tracker.texts["report/summary.md"]
    assert len(tracker.texts["report/summary.md"]) > 200


def test_workflow_id_falls_back_to_the_environment(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_ID", "wf-env")
    report = build_report("n", metrics={}, checks=[], units=Units(1, 1, 0))
    tracker = CapturingTracker()
    publish_run_report(tracker, report, {})
    assert tracker.tags["workflow_id"] == "wf-env"
    assert "checks_failed" not in tracker.tags


def test_failures_csv_only_when_a_unit_failed() -> None:
    report = build_report("n", metrics={}, checks=[], units=Units(2, 1, 1), failures={"HS2017": "boom"})
    tracker = CapturingTracker()
    publish_run_report(tracker, report, {})
    assert tracker.tables["failures.csv"].to_dict("records") == [{"unité": "HS2017", "message": "boom"}]
    assert tracker.tags["health"] == "failed"


class _BrokenTracker(NullTracker):
    """Tracker dont toute écriture lève : le rapport ne doit rien propager."""

    def _boom(self, *args, **kwargs):
        raise ConnectionError("serveur injoignable")

    log_metrics = log_table = log_text = set_tags = _boom


def test_a_broken_tracker_only_warns(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="kedro_pipeline.io.tracking"):
        publish_run_report(_BrokenTracker(), _report(), PARAMS)
        publish_failure(_BrokenTracker(), "n", ValueError("x"), PARAMS)
    messages = " ".join(record.getMessage() for record in caplog.records)
    assert "serveur injoignable" in messages and len(caplog.records) >= 5


def test_a_report_that_cannot_render_is_a_warning_and_the_rest_is_still_published(monkeypatch, caplog) -> None:
    report = _report()
    monkeypatch.setattr(type(report), "to_html", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("plotly")))
    tracker = CapturingTracker()
    with caplog.at_level(logging.WARNING, logger="kedro_pipeline.io.tracking"):
        publish_run_report(tracker, report, PARAMS)
    assert "report/report.html" not in tracker.texts and "report/summary.md" in tracker.texts
    assert tracker.tags["health"] == "warning"
    assert "plotly" in caplog.text


def test_run_metrics_and_peak_memory() -> None:
    assert run_metrics({"duration_s": 2.0}) == {"run/duration_seconds": 2.0}
    assert run_metrics({}) == {}
    assert (peak_memory_mb() or 1.0) > 0


def test_publish_on_a_local_mlflow_file_store(mlflow_uri) -> None:
    import mlflow

    tracker = CapturingTracker(MlflowTracker(tracking_uri=mlflow_uri, experiment="trade-test", run_name="n-wf-42"))
    with tracker:
        run_id = mlflow.active_run().info.run_id
        publish_run_report(tracker, _report(), PARAMS)

    client = mlflow.tracking.MlflowClient(tracking_uri=mlflow_uri)
    run = client.get_run(run_id)
    tags = run.data.tags
    assert tags[DESCRIPTION_TAG].startswith("### ⚠️ process_baci_HS2017 — avertissement")
    assert tags["health"] == "warning" and tags["workflow_id"] == "wf-42" and tags["node"] == "process_baci_HS2017"
    assert tags["checks_failed"] == "R² de la gravité"
    assert run.data.metrics["checks/n_warnings"] == 1.0 and run.data.metrics["checks/n_passed"] == 3.0
    assert run.data.metrics["run/duration_seconds"] == 12.0
    report_files = {a.path for a in client.list_artifacts(run_id, "report")}
    assert report_files == {"report/summary.md", "report/report.html", "report/checks.csv"}
    assert {a.path for a in client.list_artifacts(run_id, "tables")} == {"tables/conversion_rates.csv"}
    html = client.download_artifacts(run_id, "report/report.html")
    assert "R² de la gravité" in open(html, encoding="utf-8").read()


def test_description_of_maximal_length_is_accepted_by_the_store(mlflow_uri) -> None:
    """Le tag de description (7 500 caractères par défaut) est accepté (PQ-19)."""
    import mlflow

    long_failures = {f"unité-{i}": "x" * 100 for i in range(40)}
    report = build_report("n", metrics={}, checks=[], units=Units(40, 0, 40), failures=long_failures,
                          max_failures_listed=40)
    tracker = CapturingTracker(MlflowTracker(tracking_uri=mlflow_uri, experiment="t"))
    with tracker:
        run_id = mlflow.active_run().info.run_id
        publish_run_report(tracker, report, {"REPORT": {"HTML": False, "MAX_DESCRIPTION_CHARS": 7500}})
    tag = mlflow.tracking.MlflowClient(tracking_uri=mlflow_uri).get_run(run_id).data.tags[DESCRIPTION_TAG]
    assert 1000 < len(tag) <= 7500 and tag == tracker.tags[DESCRIPTION_TAG]


class TestScriptGlue:
    def test_run_name_uses_the_workflow_id_when_present(self, monkeypatch) -> None:
        monkeypatch.delenv("WORKFLOW_ID", raising=False)
        assert run_name("baci-HS2017-2026", "process_baci_HS2017") == "baci-HS2017-2026"
        monkeypatch.setenv("WORKFLOW_ID", "wf-7k2qd")
        assert run_name("baci-HS2017-2026", "process_baci_HS2017") == "process_baci_HS2017-wf-7k2qd"

    def test_scope_context_and_links(self, monkeypatch) -> None:
        monkeypatch.setenv("WORKFLOW_ID", "wf-1")
        monkeypatch.setenv("PROFILE", "demo")
        monkeypatch.setenv("IMAGE_TAG", "sha-abc")
        scope = RunScope("n", {"LINKS": {"ARGO_WORKFLOW": "https://argo/{workflow_id}"}})
        context = scope.context()
        assert context["workflow_id"] == "wf-1" and context["env"] == "demo" and context["image"] == "sha-abc"
        assert context["links"] == {"DAG Argo": "https://argo/wf-1"} and context["duration_s"] >= 0

    def test_link_needing_a_workflow_id_is_dropped_outside_a_workflow(self, monkeypatch) -> None:
        monkeypatch.delenv("WORKFLOW_ID", raising=False)
        scope = RunScope("n", {"LINKS": {"ARGO_WORKFLOW": "https://argo/{workflow_id}", "AUTRE": None}})
        assert "links" not in scope.context()

    def test_missing_configuration_is_a_warning_not_an_error(self, tmp_path, caplog) -> None:
        with caplog.at_level(logging.WARNING):
            assert load_tracking_config(str(tmp_path / "absent.yaml")) == {}
        assert "unreadable" in caplog.text

    def test_guarded_run_publishes_the_reduced_description_then_reraises(self) -> None:
        tracker = CapturingTracker()
        scope = RunScope("compute_synthetic_scores", {}, step="synthèse")
        with pytest.raises(ValueError, match="boum"):
            with tracker, guarded_run(scope, tracker):
                raise ValueError("boum")
        assert tracker.tags["health"] == "failed"
        assert tracker.tags[DESCRIPTION_TAG].splitlines()[:3] == [
            "### ❌ compute_synthetic_scores — échec",
            "`ValueError` : boum",
            "Dernière étape atteinte : `synthèse`",
        ]

    def test_guarded_run_leaves_a_published_report_alone(self) -> None:
        tracker = CapturingTracker()
        scope = RunScope("n", {})
        with pytest.raises(RuntimeError):
            with tracker, guarded_run(scope, tracker):
                scope.publish(tracker, scope.build(metrics={}, units=Units(1, 0, 1), failures={"a": "x"}))
                raise RuntimeError("agrégée")
        # Le rapport complet est conservé : pas de description réduite par-dessus
        assert "Unités en échec" in tracker.tags[DESCRIPTION_TAG]

    def test_guarded_run_does_not_intercept_system_exit_or_interrupt(self) -> None:
        for exc in (SystemExit(1), KeyboardInterrupt()):
            tracker = CapturingTracker()
            with pytest.raises(type(exc)):
                with tracker, guarded_run(RunScope("n", {}), tracker):
                    raise exc
            assert tracker.tags == {}

    def test_build_merges_run_metrics_for_key_figures_and_sections(self) -> None:
        seen = {}
        scope = RunScope("n", {"CHECKS": {"n": [{"metric": "run/duration_seconds", "op": ">=", "threshold": 0}]}})
        report = scope.build(
            metrics={"x": 1.0},
            units=Units(1, 1, 0),
            key_figures=lambda m: seen.setdefault("k", sorted(m)) and ["k"],
            sections=lambda m: seen.setdefault("s", sorted(m)) and [],
        )
        assert "run/duration_seconds" in seen["k"] and seen["k"] == seen["s"]
        assert report.checks[-1].status == "passed"
