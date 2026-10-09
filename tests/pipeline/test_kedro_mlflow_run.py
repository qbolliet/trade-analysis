"""Suivi MLflow du pipeline Kedro de bout en bout, sur l'environnement ``test``.

Chaque tâche de ``__default__`` est exécutée dans sa propre session Kedro, comme dans un
pod Argo (``kedro run --nodes <tâche>``), avec un magasin MLflow ``file:`` temporaire :

* un run par tâche, nommé ``<tâche>-<WORKFLOW_ID>``, tags de regroupement posés ;
* chaque run porte son rapport (description, contrôles, ``checks/*``, rapport HTML) ;
* métriques rangées par section (``gravity/…``, ``download/query/…``, ``partners/import/…``),
  chaque contrôle configuré visant une métrique réellement émise par son nœud ;
* métriques système, échec d'une étape, exception imprévue, runs orphelins, serveur
  injoignable et commande ``kedro run``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict

import pytest

pytestmark = pytest.mark.slow

pytest.importorskip("dt_ducklake_manager")
mlflow = pytest.importorskip("mlflow")

# Racine du dépôt (projet Kedro)
PROJECT = Path(__file__).resolve().parents[2]
# Expérience unique du test et identifiant de l'exécution simulée
EXPERIMENT = "trade-02-baci"
WORKFLOW_ID = "wf-test"
MAINTENANCE = "maintain_ducklake"


def _run_task(task: str) -> None:
    """Exécute une tâche dans une nouvelle session, comme un pod (FusedRunner d'argo-kedro)."""
    from argo_kedro.runners import FusedRunner
    from kedro.framework.session import KedroSession

    with KedroSession.create(project_path=PROJECT, env="test") as session:
        session.run(
            pipeline_names=["__default__"], node_names=[task], runner=FusedRunner(pipeline_name="__default__")
        )


def _runs_of(client: Any, workflow_id: str) -> Dict[str, Any]:
    """Runs d'une exécution, par tâche (le plus récent l'emporte)."""
    experiment = client.get_experiment_by_name(EXPERIMENT)
    runs = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.workflow_id = '{workflow_id}'",
        order_by=["attributes.start_time ASC"],
    )
    return {run.data.tags["node"]: run for run in runs}


def _artifacts(client: Any, run: Any, folder: str) -> set:
    return {artifact.path for artifact in client.list_artifacts(run.info.run_id, folder)}


@pytest.fixture(scope="module")
def workflow(tmp_path_factory: pytest.TempPathFactory):
    """Exécution complète, tâche par tâche, dans un magasin MLflow temporaire."""
    from kedro.framework.project import pipelines
    from kedro.framework.startup import bootstrap_project

    from kedro_pipeline import config

    base = tmp_path_factory.mktemp("k13")
    (base / "trade").mkdir()
    uri = (base / "mlruns").as_uri()
    with pytest.MonkeyPatch.context() as patch:
        for name in ("MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"):
            patch.delenv(name, raising=False)
        patch.setenv("TRADE_TEST_ROOT", str(base / "trade"))
        patch.setenv("KEDRO_ENV", "test")
        patch.setenv("KEDRO_DISABLE_TELEMETRY", "true")
        patch.setenv("MLFLOW_TRACKING_URI", uri)
        patch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
        patch.setenv("MLFLOW_EXPERIMENT_NAME", EXPERIMENT)
        patch.setenv("WORKFLOW_ID", WORKFLOW_ID)
        patch.setenv("GIT_SHA", "6f24c6c")
        patch.setenv("IMAGE_TAG", "6f24c6c")
        patch.setenv("MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING", "true")
        patch.setenv("MLFLOW_SYSTEM_METRICS_SAMPLING_INTERVAL", "1")
        config._cached_section.cache_clear()
        bootstrap_project(PROJECT)

        # Ordre topologique ; la maintenance en dernier (gestionnaire de fin de workflow)
        tasks = [node.name for node in pipelines["__default__"].nodes if node.name != MAINTENANCE]
        tasks.append(MAINTENANCE)
        for task in tasks:
            _run_task(task)

        client = mlflow.tracking.MlflowClient(uri)
        yield SimpleNamespace(root=base / "trade", uri=uri, client=client, tasks=tasks, patch=patch)
        config._cached_section.cache_clear()


# ── Un run par tâche, étiqueté ─────────────────────────────────────────────


def test_one_run_per_task_named_and_tagged(workflow) -> None:
    runs = _runs_of(workflow.client, WORKFLOW_ID)
    assert set(runs) == set(workflow.tasks)
    for task, run in runs.items():
        tags = run.data.tags
        assert run.info.status == "FINISHED", task
        assert run.info.run_name == f"{task}-{WORKFLOW_ID}"
        assert tags["git_sha"] == tags["image_tag"] == "6f24c6c"
        assert tags["kedro_env"] == "test" and tags["forced"] == "none"
        assert tags["health"] in {"ok", "warning"}, (task, tags.get("checks_failed"))
    # Expérience ouverte sur la liste des runs par l'interface MLflow 3 (et non sur les traces)
    experiment = workflow.client.get_experiment_by_name(EXPERIMENT)
    assert experiment.tags["mlflow.experimentKind"] == "custom_model_development"


def test_every_run_carries_its_report(workflow) -> None:
    for task, run in _runs_of(workflow.client, WORKFLOW_ID).items():
        lines = run.data.tags["mlflow.note.content"].splitlines()
        assert lines[0].startswith(("### ✅ ", "### ⚠️ ")) and f" {task} — " in lines[0], task
        assert lines[1].startswith(f"Exécution `{WORKFLOW_ID}` · env `test` · image `6f24c6c`"), task
        assert {"report/report.html", "report/checks.csv", "report/summary.md"} <= _artifacts(
            workflow.client, run, "report"
        ), task
        assert {"checks/n_passed", "checks/n_warnings", "checks/n_failed", "checks/n_skipped"} <= set(
            run.data.metrics
        ), task


# ── Métriques par section ──────────────────────────────────────────────────


def test_metrics_are_grouped_by_section(workflow) -> None:
    client, runs = workflow.client, _runs_of(workflow.client, WORKFLOW_ID)
    baci = runs["process_baci_hs2017"].data.metrics
    assert {"output/flows", "gravity/r_squared", "conversion/share_tonnage_missing"} <= set(baci)
    assert not any(name.startswith(("baci/", "hs/")) for name in baci)
    for source in ("eurostat", "comtrade"):
        run = runs[f"download_{source}"]
        history = client.get_metric_history(run.info.run_id, "download/query/rows_written")
        assert len({metric.step for metric in history}) > 1, source
        assert "http/n_requests" in run.data.metrics and "coverage/share_downloaded" in run.data.metrics
        assert not any(name.startswith("query/") for name in run.data.metrics)
    # Unités d'un même nœud : mêmes noms, une valeur par millésime (step = année)
    partners = runs["compute_partner_vulnerabilities"]
    steps = {m.step for m in client.get_metric_history(partners.info.run_id, "partners/import/cells/n_total")}
    assert steps and all(step >= 1988 for step in steps)


def test_every_configured_check_targets_a_metric_of_its_run(workflow) -> None:
    from kedro_pipeline.config import load_parameters
    from macroforecast.tracking.report import checks_for_node

    tracking = load_parameters("test")["tracking"]
    for task, run in _runs_of(workflow.client, WORKFLOW_ID).items():
        missing = [check.metric for check in checks_for_node(task, tracking) if check.metric not in run.data.metrics]
        assert not missing, f"{task} : contrôles sur des métriques jamais émises {missing}"


def test_system_metrics_are_logged_by_the_kedro_mlflow_runs(workflow) -> None:
    runs = _runs_of(workflow.client, WORKFLOW_ID).values()
    assert any(any(name.startswith("system/") for name in run.data.metrics) for run in runs)


# ── Échecs ─────────────────────────────────────────────────────────────────


def test_a_failed_step_publishes_a_failed_report(workflow, monkeypatch: pytest.MonkeyPatch) -> None:
    from kedro_pipeline.steps.result import StepResult

    monkeypatch.setenv("WORKFLOW_ID", "wf-failed")
    monkeypatch.setattr(
        "kedro_pipeline.pipelines.serving.nodes.publish_serving",
        lambda *args, **kwargs: StepResult("serving", 1, 0, failures={"publication": "boom"}),
    )
    with pytest.raises(Exception):
        _run_task("publish_serving")
    run = _runs_of(workflow.client, "wf-failed")["publish_serving"]
    assert run.info.status == "FAILED" and run.data.tags["health"] == "failed"
    assert run.data.tags["mlflow.note.content"].startswith("### ❌ publish_serving — échec")
    assert "report/checks.csv" in _artifacts(workflow.client, run, "report")


def test_an_exception_before_the_report_gets_the_reduced_description(
    workflow, monkeypatch: pytest.MonkeyPatch
) -> None:
    def crash(**kwargs: Any) -> None:
        raise ValueError("catalogue introuvable")

    monkeypatch.setenv("WORKFLOW_ID", "wf-crash")
    monkeypatch.setattr("kedro_pipeline.pipelines.serving.nodes.source_tables", crash)
    with pytest.raises(Exception):
        _run_task("publish_serving")
    run = _runs_of(workflow.client, "wf-crash")["publish_serving"]
    description = run.data.tags["mlflow.note.content"]
    assert run.info.status == "FAILED" and run.data.tags["health"] == "failed"
    assert description.startswith("### ❌ publish_serving — échec") and "`ValueError`" in description
    assert "report/report.html" not in _artifacts(workflow.client, run, "report")


def test_the_maintenance_closes_the_stale_runs_of_its_workflow(workflow) -> None:
    client = workflow.client
    experiment = client.get_experiment_by_name(EXPERIMENT).experiment_id
    stale = client.create_run(experiment, tags={"workflow_id": WORKFLOW_ID, "node": "process_baci_hs2017"})
    other = client.create_run(experiment, tags={"workflow_id": "wf-other", "node": "process_baci_hs2017"})

    _run_task(MAINTENANCE)

    closed = client.get_run(stale.info.run_id)
    assert closed.info.status == "FAILED" and closed.data.tags["health"] == "failed"
    assert closed.data.tags["mlflow.note.content"] == (
        "### ❌ process_baci_hs2017 — échec\ntâche interrompue — voir Argo"
    )
    assert client.get_run(other.info.run_id).info.status == "RUNNING"
    maintenance = _runs_of(client, WORKFLOW_ID)[MAINTENANCE]
    assert maintenance.info.status == "FINISHED" and maintenance.data.metrics["mlflow/stale_runs_closed"] == 1.0


def test_an_unreachable_server_never_stops_a_task(workflow, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:9")
    metrics = workflow.root / "reporting" / "metrics" / "prepare_baci.json"
    metrics.unlink()
    _run_task("prepare_baci")
    assert metrics.exists()
    assert mlflow.active_run() is None


def test_kedro_run_command_logs_its_task(workflow) -> None:
    env = {**os.environ, "WORKFLOW_ID": "wf-cli"}
    completed = subprocess.run(
        [sys.executable, "-m", "kedro", "run", "--env", "test", "--nodes", MAINTENANCE],
        cwd=PROJECT, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    assert completed.returncode == 0, completed.stdout[-4000:] + completed.stderr[-4000:]
    run = _runs_of(workflow.client, "wf-cli")[MAINTENANCE]
    assert run.info.run_name == f"{MAINTENANCE}-wf-cli" and run.data.tags["health"] == "ok"
