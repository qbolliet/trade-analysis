"""Suivi dans le run actif ouvert par kedro-mlflow : tracker, datasets de suivi, runs orphelins.

Chaque test ouvre ses runs dans un magasin MLflow ``file:`` temporaire (fixture
``mlflow_uri``) ; aucun ne doit lever pour une raison de suivi.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

mlflow = pytest.importorskip("mlflow")

from kedro_pipeline.io.mlflow_datasets import MlflowRunMetricsDataset, MlflowTablesDataset  # noqa: E402
from kedro_pipeline.io.tracking import (  # noqa: E402
    DESCRIPTION_TAG,
    active_run_id,
    build_tracker,
    close_stale_runs,
)
from macroforecast.tracking import NULL_TRACKER, ActiveRunTracker  # noqa: E402


@pytest.fixture
def client(mlflow_uri: str):
    """Client du magasin temporaire, désigné comme magasin courant de MLflow."""
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("trade-test")
    yield mlflow.tracking.MlflowClient(mlflow_uri)
    while mlflow.active_run():
        mlflow.end_run()


# ── ActiveRunTracker ───────────────────────────────────────────────────────


def test_active_run_tracker_is_a_no_op_without_active_run(client) -> None:
    tracker = ActiveRunTracker()
    tracker.log_metrics({"a": 1.0})
    tracker.set_tags({"health": "ok"})
    tracker.log_table(pd.DataFrame({"a": [1]}), "t/x.csv")
    assert active_run_id() is None
    assert build_tracker() is NULL_TRACKER


def test_active_run_tracker_writes_into_the_active_run(client) -> None:
    with mlflow.start_run() as run:
        tracker = build_tracker()
        assert isinstance(tracker, ActiveRunTracker)
        tracker.log_metrics({"output/rows": 3.0, "nan": float("nan")}, step=2020)
        tracker.set_tags({"health": "ok"})
        tracker.log_params({"vintage": "HS2017"})
        tracker.log_text("# rapport", "report/summary.md")
        tracker.log_dict({"a": 1}, "gravity/coefficients.json")
        tracker.log_table(pd.DataFrame({"a": [1]}), "tables/x.csv")
        build_tracker(log_tables=False).log_table(pd.DataFrame({"a": [1]}), "tables/y.csv")
    data = client.get_run(run.info.run_id).data
    assert data.metrics == {"output/rows": 3.0}
    assert [m.step for m in client.get_metric_history(run.info.run_id, "output/rows")] == [2020]
    assert data.tags["health"] == "ok" and data.params["vintage"] == "HS2017"
    files = {a.path for folder in ("report", "gravity", "tables") for a in client.list_artifacts(run.info.run_id, folder)}
    assert files == {"report/summary.md", "gravity/coefficients.json", "tables/x.csv"}


def test_a_rejected_write_is_only_a_warning(client, caplog) -> None:
    with mlflow.start_run():
        tracker = ActiveRunTracker()
        tracker.log_params({"p": "1"})
        with caplog.at_level(logging.WARNING):
            # MLflow refuse de changer la valeur d'un paramètre déjà journalisé
            tracker.log_params({"p": "2"})
    assert "log_params failed" in caplog.text


# ── Datasets de suivi des nœuds ────────────────────────────────────────────


def test_metrics_dataset_writes_only_what_the_run_lacks(client) -> None:
    with mlflow.start_run() as run:
        ActiveRunTracker().log_metrics({"gravity/r_squared": 0.7})
        ActiveRunTracker().log_metrics({"network/import/cells/n_total": 5.0}, step=2017)
        MlflowRunMetricsDataset(prefix="").save(
            {
                "gravity/r_squared": 0.7,
                "units/planned": 2.0,
                "HS2017/network/import/cells/n_total": 5.0,
                "HS2022/network/import/cells/n_total": 7.0,
            }
        )
    run_id = run.info.run_id
    assert len(client.get_metric_history(run_id, "gravity/r_squared")) == 1
    assert client.get_run(run_id).data.metrics["units/planned"] == 2.0
    # Métrique d'unité déjà présente sous son nom : aucune écriture supplémentaire
    history = client.get_metric_history(run_id, "network/import/cells/n_total")
    assert [(m.step, m.value) for m in history] == [(2017, 5.0)]


def test_metrics_dataset_is_inert_without_run_and_keeps_names_unprefixed(client) -> None:
    MlflowRunMetricsDataset(prefix="").save({"units/planned": 1.0})
    assert client.search_runs([client.get_experiment_by_name("trade-test").experiment_id]) == []


def test_tables_dataset_writes_csv_artifacts_at_their_paths(client) -> None:
    with mlflow.start_run() as run:
        MlflowTablesDataset().save(
            {"download/queries": pd.DataFrame({"a": [1]}), "HS2017/output/rows_by_year": pd.DataFrame({"y": [1]}),
             "not_a_table": 3}
        )
    paths = {a.path for a in client.list_artifacts(run.info.run_id, "download")}
    paths |= {a.path for a in client.list_artifacts(run.info.run_id, "HS2017/output")}
    assert paths == {"download/queries.csv", "HS2017/output/rows_by_year.csv"}


# ── Runs orphelins ─────────────────────────────────────────────────────────


def test_close_stale_runs_closes_the_running_runs_of_the_workflow_only(client) -> None:
    experiment = client.get_experiment_by_name("trade-test").experiment_id
    stale = client.create_run(experiment, tags={"workflow_id": "wf-1", "node": "process_baci_hs2017"})
    other = client.create_run(experiment, tags={"workflow_id": "wf-2", "node": "publish_serving"})
    own = client.create_run(experiment, tags={"workflow_id": "wf-1", "node": "maintain_ducklake"})

    closed = close_stale_runs(
        "wf-1", ["trade-test", "absent"], timedelta(0), exclude_run_id=own.info.run_id, client=client
    )

    assert closed == 1
    run = client.get_run(stale.info.run_id)
    assert run.info.status == "FAILED" and run.data.tags["health"] == "failed"
    assert run.data.tags[DESCRIPTION_TAG] == "### ❌ process_baci_hs2017 — échec\ntâche interrompue — voir Argo"
    assert client.get_run(other.info.run_id).info.status == "RUNNING"
    assert client.get_run(own.info.run_id).info.status == "RUNNING"


def test_close_stale_runs_respects_the_minimum_age_and_never_raises(client) -> None:
    experiment = client.get_experiment_by_name("trade-test").experiment_id
    client.create_run(experiment, tags={"workflow_id": "wf-1"})
    assert close_stale_runs("wf-1", ["trade-test"], timedelta(hours=1), client=client) == 0
    assert close_stale_runs("wf-1", ["trade-test"], client=object()) == 0


# ── Hook kedro-mlflow gardé ────────────────────────────────────────────────


def test_guarded_hook_disables_tracking_on_an_unreachable_server(tmp_path: Path, monkeypatch) -> None:
    from types import SimpleNamespace

    from kedro_pipeline.mlflow_hook import GuardedMlflowHook, probe_tracking_uri

    assert probe_tracking_uri("http://127.0.0.1:9") is not None
    hook = GuardedMlflowHook()
    context = SimpleNamespace(
        config_loader={"mlflow": {"server": {"mlflow_tracking_uri": "http://127.0.0.1:9"}}},
        project_path=tmp_path,
    )
    # Avertissements relevés sur le logger du module (la configuration de journalisation de
    # Kedro peut ne pas propager jusqu'à la racine)
    warnings: list = []
    monkeypatch.setattr("kedro_pipeline.mlflow_hook.logger.warning", lambda message, *a: warnings.append(message))
    hook.after_context_created(context)
    assert not hook.tracking_enabled and any("injoignable" in message for message in warnings)
    # Hooks suivants inertes : aucune exception, aucun run ouvert
    hook.before_node_run(SimpleNamespace(name="n"), None, {"params:x": 1}, False)
    hook.after_pipeline_run({}, None, None)
    assert mlflow.active_run() is None


def test_guarded_hook_without_uri_disables_tracking(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from kedro_pipeline.mlflow_hook import GuardedMlflowHook

    hook = GuardedMlflowHook()
    hook.after_context_created(SimpleNamespace(config_loader={"mlflow": {}}, project_path=tmp_path))
    assert not hook.tracking_enabled
