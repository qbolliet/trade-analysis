"""Le script ``publish_serving`` publie son rapport de run dans un vrai run MLflow local."""

from __future__ import annotations

import functools
from pathlib import Path

import pytest

from conftest import REPO_ROOT, build_serving_world

pytestmark = pytest.mark.slow


@pytest.fixture
def script_env(monkeypatch: pytest.MonkeyPatch, mlflow_uri: str):
    for name, value in {
        "PGHOST": "h", "PGPORT": "5432", "PGUSER": "u", "PGPASSWORD": "p", "PGDATABASE": "d",
        "AWS_S3_ENDPOINT": "e", "AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s",
        "SERVING_CONFIG_PATH": str(REPO_ROOT / "config" / "serving.yaml"),
        "EUROSTAT_CONFIG_PATH": str(REPO_ROOT / "config" / "datasets" / "eurostat.yaml"),
        "COMTRADE_CONFIG_PATH": str(REPO_ROOT / "config" / "datasets" / "comtrade.yaml"),
        "VULNERABILITIES_CONFIG_PATH": str(REPO_ROOT / "config" / "vulnerabilities.yaml"),
        "SYNTHESIS_CONFIG_PATH": str(REPO_ROOT / "config" / "synthesis.yaml"),
        "RUNTIME_CONFIG_PATH": str(REPO_ROOT / "config" / "runtime.yaml"),
        "TRACKING_CONFIG_PATH": str(REPO_ROOT / "config" / "tracking.yaml"),
        "MLFLOW_TRACKING_URI": mlflow_uri,
        "WORKFLOW_ID": "trade-pipeline-daily-abcde",
        "PROFILE": "test",
    }.items():
        monkeypatch.setenv(name, value)


def _patch_catalog(monkeypatch: pytest.MonkeyPatch, world) -> None:
    import scripts.publish_serving as script
    from kedro_pipeline.io.serving import ServingCatalog

    monkeypatch.setattr(
        script, "ServingCatalog", functools.partial(ServingCatalog, connector_factory=world.factory)
    )


def _only_run(uri: str):
    import mlflow

    client = mlflow.tracking.MlflowClient(tracking_uri=uri)
    runs = client.search_runs([e.experiment_id for e in client.search_experiments()])
    assert len(runs) == 1
    return client, runs[0]


def test_successful_publication_carries_a_healthy_report(tmp_path: Path, script_env, monkeypatch, mlflow_uri) -> None:
    import scripts.publish_serving as script

    world = build_serving_world(tmp_path)
    _patch_catalog(monkeypatch, world)
    script.main([])

    client, run = _only_run(mlflow_uri)
    assert client.get_experiment(run.info.experiment_id).name == "trade-04-serving"
    assert run.info.run_name == "publish_serving-trade-pipeline-daily-abcde"
    assert run.info.status == "FINISHED"
    tags = run.data.tags
    assert tags["health"] == "ok" and tags["node"] == "publish_serving"
    assert tags["workflow_id"] == "trade-pipeline-daily-abcde"
    assert tags["mlflow.note.content"].startswith("### ✅ publish_serving — ok")
    assert "env `test`" in tags["mlflow.note.content"]
    assert run.data.metrics["checks/n_failed"] == 0 and run.data.metrics["serving/cell_scores/rows"] > 0
    report_files = {a.path for a in client.list_artifacts(run.info.run_id, "report")}
    assert {"report/summary.md", "report/report.html", "report/checks.csv"} <= report_files


def test_failed_publication_is_reported_before_the_error_exit(tmp_path: Path, script_env, monkeypatch, mlflow_uri) -> None:
    import scripts.publish_serving as script

    world = build_serving_world(tmp_path, omit=("comext",))
    _patch_catalog(monkeypatch, world)
    with pytest.raises(SystemExit) as info:
        script.main(["--mode", "full"])
    assert info.value.code == 1

    client, run = _only_run(mlflow_uri)
    assert run.data.tags["health"] == "failed"
    description = run.data.tags["mlflow.note.content"]
    assert description.startswith("### ❌ publish_serving — échec") and "Unités en échec" in description
    assert "failures.csv" in {a.path for a in client.list_artifacts(run.info.run_id)}


def test_an_uncaught_exception_publishes_the_reduced_description_and_fails_the_run(
    tmp_path: Path, script_env, monkeypatch, mlflow_uri
) -> None:
    import scripts.publish_serving as script

    world = build_serving_world(tmp_path)
    _patch_catalog(monkeypatch, world)

    def boom(*args, **kwargs):
        raise RuntimeError("panne inattendue")

    monkeypatch.setattr(script, "publish_serving", boom)
    with pytest.raises(RuntimeError, match="panne inattendue"):
        script.main([])

    _, run = _only_run(mlflow_uri)
    assert run.info.status == "FAILED" and run.data.tags["health"] == "failed"
    assert run.data.tags["mlflow.note.content"].splitlines()[:2] == [
        "### ❌ publish_serving — échec", "`RuntimeError` : panne inattendue",
    ]
