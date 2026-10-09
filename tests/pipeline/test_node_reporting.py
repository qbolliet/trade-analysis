"""Rapport de run des nœuds Kedro : construction, ordre de publication, sections de métriques.

Tests sans MLflow : les trackers sont des ``CapturingTracker``.
"""

from __future__ import annotations

from dataclasses import fields
from fnmatch import fnmatchcase

import pandas as pd
import pytest

from kedro_pipeline.io.tracking import build_node_report, node_unit_runs
from kedro_pipeline.pipelines._common import NodeReporting, finish_step
from kedro_pipeline.steps.result import StepResult
from macroforecast.tracking import CapturingTracker

TRACKING = {
    "CHECKS": {
        "compute_network_vulnerabilities": [
            {"metric": "network/import/cells/n_total", "op": ">", "threshold": 0, "label": "Cellules"}
        ]
    },
    "REPORT": {"HTML": False},
}


# ── Rapport d'un nœud ──────────────────────────────────────────────────────


def test_unit_runs_are_checked_one_by_one() -> None:
    children = {
        "HS2017": StepResult("network", 1, 1, metrics={"network/import/cells/n_total": 4.0}),
        "HS2022": StepResult("network", 1, 1, metrics={"network/import/cells/n_total": 0.0}),
    }
    result = StepResult("network", 2, 2, children=children, reportable=False)
    report = build_node_report(result, node="compute_network_vulnerabilities", tracking=TRACKING)
    labels = [(item.check.label, item.status) for item in report.checks]
    assert labels[2:] == [("Cellules — HS2017", "passed"), ("Cellules — HS2022", "warning")]
    assert report.health == "warning" and report.units.planned == 2


def test_an_idle_node_is_reported_without_configured_checks() -> None:
    result = StepResult("network", reportable=False)
    report = build_node_report(result, node="compute_network_vulnerabilities", tracking=TRACKING)
    assert report.health == "ok" and len(report.checks) == 2
    assert report.key_figures == ["rien à recalculer : toutes les unités sont à jour"]


def test_unit_runs_log_into_the_node_run_at_the_vintage_step() -> None:
    calls = []

    class _Recorder(CapturingTracker):
        def log_metrics(self, metrics, step=None):
            calls.append((dict(metrics), step))

    with node_unit_runs(_Recorder())("HS2017", {"vintage": "HS2017"}) as run:
        run.tracker.log_metrics({"network/import/cells/n_total": 4.0})
        run.tracker.log_metrics({"timing/seconds": 1.0}, step=3)
    assert calls == [({"network/import/cells/n_total": 4.0}, 2017), ({"timing/seconds": 1.0}, 3)]


def test_the_report_is_published_after_the_registries_and_before_the_failure() -> None:
    journal = []

    class _Registry:
        def save(self) -> None:
            journal.append("registry")

    class _Tracker(CapturingTracker):
        def set_tags(self, tags):
            journal.append("report")
            super().set_tags(tags)

    tracker = _Tracker()
    reporting = NodeReporting("publish_serving", TRACKING, step_tracker=tracker, report_tracker=tracker)
    with pytest.raises(RuntimeError) as raised:
        finish_step(StepResult("serving", 1, 0, failures={"publication": "boom"}), _Registry(), reporting=reporting)
    assert journal[0] == "registry" and "report" in journal
    assert raised.value.report_published is True
    assert tracker.tags["health"] == "failed"
    assert tracker.tags["mlflow.note.content"].startswith("### ❌ publish_serving — échec")


# ── Sections des métriques des étapes ──────────────────────────────────────


def test_every_baci_report_field_lands_in_a_pd13_section() -> None:
    from macroforecast.trade.processing.baci import BaciReport
    from kedro_pipeline.steps.baci import BACI_REPORT_SECTIONS, baci_section_metrics

    report = BaciReport(flows=3, regime_country_years=2, total_reconciled_value=1.5)
    metrics = baci_section_metrics(report)
    sections = {"output", "valuation", "conversion", "gravity", "reconciliation", "quality", "nes"}
    assert metrics and {name.split("/")[0] for name in metrics} <= sections
    assert set(BACI_REPORT_SECTIONS) <= {f.name for f in fields(BaciReport)}
    assert metrics["output/flows"] == 3.0 and metrics["valuation/regime_country_years"] == 2.0


def test_query_metrics_split_http_and_rate_limiter_sections() -> None:
    from statflows.core.reports import QueryReport

    from kedro_pipeline.steps.downloads import query_metrics

    metrics = query_metrics(QueryReport(rows_written=5))
    assert {name.split("/")[0] for name in metrics} == {"download", "http", "rate_limit"}
    assert not any(name.startswith("query/") for name in metrics)


# ── Configuration du suivi ─────────────────────────────────────────────────


def test_mlflow_configuration_reads_only_tracking_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    from kedro_pipeline.config import load_config_section

    monkeypatch.setenv("MLFLOW_EXPERIMENT_NAME", "trade-02-baci")
    monkeypatch.setenv("WORKFLOW_ID", "wf-1")
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    mlflow_config = load_config_section("mlflow", "base")
    assert mlflow_config["server"]["mlflow_tracking_uri"] is None
    assert mlflow_config["tracking"]["experiment"]["name"] == "trade-02-baci"
    assert mlflow_config["tracking"]["run"]["name"] == "wf-1"


def test_stale_run_experiments_are_the_experiments_of_the_nodes() -> None:
    from kedro_pipeline.config import load_parameters
    from kedro_pipeline.pipeline_registry import register_pipelines

    parameters = load_parameters("base")
    tags = {
        tag.split(".", 1)[1]
        for node in register_pipelines(parameters)["__default__"].nodes
        for tag in node.tags
        if tag.startswith("experiment.")
    }
    assert set(parameters["tracking"]["STALE_RUNS"]["EXPERIMENTS"]) == tags


def test_every_reporting_node_reads_the_tracking_parameters() -> None:
    from kedro_pipeline.config import load_parameters
    from kedro_pipeline.pipeline_registry import register_pipelines

    parameters = load_parameters("base")
    pipeline = register_pipelines(parameters)["__default__"]
    inner = [node for task in pipeline.nodes for node in getattr(task, "_nodes", [task])]
    reporting = [node.name for node in inner if "params:tracking" in node.inputs]
    expected = [
        "audit_coverage_eurostat", "audit_coverage_comtrade", "prepare_baci",
        "compute_partner_vulnerabilities", "compute_network_vulnerabilities",
        "compute_synthetic_scores", "compute_synthesis_coherence", "publish_serving", "maintain_ducklake",
    ]
    assert set(expected) <= set(reporting)
    assert any(fnmatchcase(name, "process_baci_*") for name in reporting)
