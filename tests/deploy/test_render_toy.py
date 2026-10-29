"""Rendu des données du DAG d'un pipeline jouet : règles d'enrichissement et fichier de référence.

Le fichier de référence (``golden/toy/dag-toy.yaml``) se régénère par ``UPDATE_GOLDEN=1 uv run
pytest tests/deploy/test_render_toy.py`` ; tout écart est à relire avant validation.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml
from argo_kedro.pipeline import Node
from kedro.pipeline import Pipeline

from kedro_pipeline.deploy.config import RenderConfig
from kedro_pipeline.deploy.render import (
    Dependency,
    build_dag,
    cron_schedule,
    diff_files,
    dump_dag_data,
    exit_task,
    render_chart_files,
    render_dag_data,
    write_files,
)
from tests.deploy.toy import toy_config, toy_pipeline

GOLDEN_DIR = Path(__file__).parent / "golden" / "toy"


@pytest.fixture(scope="module")
def files():
    return render_chart_files(toy_pipeline(), toy_config())


@pytest.fixture(scope="module")
def data():
    return render_dag_data(toy_pipeline(), toy_config())


def test_golden_files(files) -> None:
    if os.environ.get("UPDATE_GOLDEN"):
        write_files(files, GOLDEN_DIR)
    assert sorted(files) == ["dag-toy.yaml"]
    assert diff_files(files, GOLDEN_DIR) == {}


def test_rendering_is_deterministic(files) -> None:
    assert render_chart_files(toy_pipeline(), toy_config()) == files


def test_the_file_reads_back_as_the_data(files, data) -> None:
    assert yaml.safe_load(files["dag-toy.yaml"]) == data
    assert data["formatVersion"] == 1 and data["kedroEnv"] == "toy" and data["podPipeline"] == "__default__"
    assert data["systemMetrics"] == {"enabled": False, "samplingSeconds": 10}


def test_daily_dag_keeps_the_fused_task_and_tolerates_its_failure() -> None:
    daily = build_dag(toy_pipeline().only_nodes_with_tags("cadence.daily"), toy_config())
    assert [task.name for task in daily] == ["download-toy", "publish-toy"]
    download, publish = daily
    # Tâche fusionnée : un seul pod, sans reprise (motif download_*)
    assert download.node == "download_toy" and download.retries == 0 and download.cpu == 1
    assert publish.dependencies == (Dependency("download-toy", strict=False),)
    assert publish.mutex == "toy-serving" and publish.template == "locked-toy-serving"


def test_weekly_dag_drops_dependencies_outside_the_entry_point() -> None:
    weekly = build_dag(toy_pipeline().only_nodes_with_tags("cadence.weekly"), toy_config())
    compute, publish = weekly
    # compute_toy lit la sortie du téléchargement quotidien : aucune dépendance Argo
    assert compute.dependencies == () and (compute.cpu, compute.mem) == (4, 32)
    assert compute.experiment == "toy-02-compute" and compute.retries == 1
    assert publish.depends == "compute-toy.Succeeded"


def test_the_exit_node_leaves_the_dags(data) -> None:
    default = toy_pipeline()
    config = toy_config()
    assert all(task.node != "cleanup_toy" for task in build_dag(default, config))
    exit_spec = exit_task(default, config)
    assert exit_spec.node == "cleanup_toy" and exit_spec.mutex == "toy-maintenance"
    assert data["exit"] == {
        "name": "cleanup-toy", "node": "cleanup_toy", "experiment": "toy-00-maintenance",
        "cpu": 1, "mem": 2, "retries": 1, "mutex": "toy-maintenance",
    }
    assert data["mutexes"] == ["toy-serving"]


def test_entry_points_carry_their_default_cron(data) -> None:
    entries = {entry["name"]: entry for entry in data["entryPoints"]}
    assert list(entries) == ["daily", "weekly"]
    assert entries["daily"]["cron"] == "15 1 * * *"
    # WEEKLY_DAY = 0 (lundi) → jour cron 1
    assert entries["weekly"]["cron"] == "0 2 * * 1"
    publish = entries["weekly"]["tasks"][-1]
    assert publish["depends"] == "compute-toy.Succeeded" and publish["mutex"] == "toy-serving"
    # Champs facultatifs absents plutôt que vides
    assert "depends" not in entries["daily"]["tasks"][0] and "mutex" not in entries["daily"]["tasks"][0]


def test_a_node_without_experiment_is_refused() -> None:
    pipeline = Pipeline([Node(lambda: 1, None, "x", name="lonely", tags=["cadence.daily"])])
    with pytest.raises(ValueError, match="experiment"):
        build_dag(pipeline, toy_config())


def test_an_unknown_machine_type_is_refused() -> None:
    pipeline = Pipeline([Node(lambda: 1, None, "x", name="big", tags=["experiment.x"], machine_type="huge")])
    with pytest.raises(KeyError, match="huge"):
        build_dag(pipeline, toy_config())


def test_an_entry_point_without_task_is_refused() -> None:
    empty = toy_pipeline().only_nodes_with_tags("onexit")
    with pytest.raises(ValueError, match="selects no node"):
        render_chart_files(empty, toy_config())


def test_dump_refuses_data_that_does_not_read_back() -> None:
    with pytest.raises(ValueError, match="read back"):
        dump_dag_data({"kedroEnv": "toy", "value": float("nan")})


@pytest.mark.parametrize(
    ("minute", "hour", "day", "expected"),
    [(0, 1, None, "0 1 * * *"), (0, 2, 5, "0 2 * * 6"), (0, 2, 6, "0 2 * * 0"), (5, 23, 0, "5 23 * * 1")],
)
def test_cron_schedule(minute: int, hour: int, day, expected: str) -> None:
    assert cron_schedule(minute, hour, day) == expected


def test_cron_schedule_refuses_invalid_values() -> None:
    with pytest.raises(ValueError):
        cron_schedule(0, 24)
    with pytest.raises(ValueError):
        cron_schedule(0, 2, 7)


def test_config_from_sections_reads_the_parameters() -> None:
    argo = {
        "machine_types": {"s": {"cpu": 1, "mem": 2, "num_gpu": 0}},
        "default_machine_type": "s",
        "workflow": {"entry_points": {"daily": {"tag": "cadence.daily", "minute": 30, "hour": 1}}},
    }
    parameters = {"runtime": {"WEEKLY_DAY": 2}, "tracking": {"SYSTEM_METRICS": {"ENABLED": False, "SAMPLING_SECONDS": 5}}}
    config = RenderConfig.from_sections(argo, parameters, "cloud")
    assert config.weekly_day == 2 and config.entry_points[0].minute == 30
    assert (config.system_metrics_enabled, config.system_metrics_sampling_seconds) == (False, 5)
    with pytest.raises(ValueError, match="Default machine type"):
        RenderConfig.from_sections({**argo, "default_machine_type": "x"}, parameters, "cloud")
