"""Rendu Argo d'un pipeline jouet : règles d'enrichissement et fichiers de référence.

Les fichiers de référence (``golden/toy/``) se régénèrent par ``UPDATE_GOLDEN=1 uv run
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
    diff_manifests,
    exit_task,
    render_pipeline_manifests,
    schedule_field,
    write_manifests,
)
from tests.deploy.toy import toy_config, toy_pipeline

GOLDEN_DIR = Path(__file__).parent / "golden" / "toy"


@pytest.fixture(scope="module")
def manifests():
    return render_pipeline_manifests(toy_pipeline(), toy_config())


def test_golden_files(manifests) -> None:
    if os.environ.get("UPDATE_GOLDEN"):
        write_manifests(manifests, GOLDEN_DIR)
    assert diff_manifests(manifests, GOLDEN_DIR) == {}


def test_rendering_is_deterministic(manifests) -> None:
    assert render_pipeline_manifests(toy_pipeline(), toy_config()) == manifests


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


def test_the_exit_node_leaves_the_dags(manifests) -> None:
    default = toy_pipeline()
    config = toy_config()
    assert all(task.node != "cleanup_toy" for task in build_dag(default, config))
    exit_spec = exit_task(default, config)
    assert exit_spec.node == "cleanup_toy" and exit_spec.mutex == "toy-maintenance"
    template = yaml.safe_load(manifests["workflowtemplate.yaml"])
    handler = {t["name"]: t for t in template["spec"]["templates"]}[template["spec"]["onExit"]]
    assert handler["synchronization"] == {"mutex": {"name": "toy-maintenance"}}
    assert handler["steps"][0][0]["arguments"]["parameters"][0] == {"name": "kedro-node", "value": "cleanup_toy"}


def test_crons_use_their_entry_point_and_schedule(manifests) -> None:
    daily = yaml.safe_load(manifests["cronworkflow-daily.yaml"])
    weekly = yaml.safe_load(manifests["cronworkflow-weekly.yaml"])
    assert daily["spec"]["schedules"] == ["15 1 * * *"]
    # WEEKLY_DAY = 0 (lundi) → jour cron 1
    assert weekly["spec"]["schedules"] == ["0 2 * * 1"]
    assert weekly["spec"]["workflowSpec"] == {
        "workflowTemplateRef": {"name": "toy-pipeline"}, "entrypoint": "weekly", "activeDeadlineSeconds": 7200,
    }


def test_older_argo_gets_a_single_schedule() -> None:
    manifests = render_pipeline_manifests(toy_pipeline(), toy_config(argo_version="3.5.8"))
    spec = yaml.safe_load(manifests["cronworkflow-daily.yaml"])["spec"]
    assert spec["schedule"] == "15 1 * * *" and "schedules" not in spec


def test_a_node_without_experiment_is_refused() -> None:
    pipeline = Pipeline([Node(lambda: 1, None, "x", name="lonely", tags=["cadence.daily"])])
    with pytest.raises(ValueError, match="experiment"):
        build_dag(pipeline, toy_config())


def test_an_unknown_machine_type_is_refused() -> None:
    pipeline = Pipeline([Node(lambda: 1, None, "x", name="big", tags=["experiment.x"], machine_type="huge")])
    with pytest.raises(KeyError, match="huge"):
        build_dag(pipeline, toy_config())


def test_an_entry_point_without_task_is_refused() -> None:
    config = toy_config()
    empty = toy_pipeline().only_nodes_with_tags("onexit")
    with pytest.raises(ValueError, match="selects no node"):
        render_pipeline_manifests(empty, config)


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


def test_schedule_field() -> None:
    assert schedule_field("3.6.10") == "schedules"
    assert schedule_field("v3.7.0") == "schedules"
    assert schedule_field("3.5.8") == "schedule"


def test_config_from_sections_reads_the_parameters() -> None:
    argo = {
        "namespace": "ns",
        "deployment": {"image": "img", "tag": "latest"},
        "machine_types": {"s": {"cpu": 1, "mem": 2, "num_gpu": 0}},
        "default_machine_type": "s",
        "workflow": {"name": "p", "environment": {"EMPTY": None, "N": 1}},
    }
    parameters = {"runtime": {"WEEKLY_DAY": 2}, "tracking": {"SYSTEM_METRICS": {"ENABLED": False, "SAMPLING_SECONDS": 5}}}
    config = RenderConfig.from_sections(argo, parameters, "cloud", image_tag="sha-1")
    assert (config.image_tag, config.weekly_day) == ("sha-1", 2)
    assert (config.system_metrics_enabled, config.system_metrics_sampling_seconds) == (False, 5)
    assert config.environment == {"EMPTY": "", "N": "1"}
    with pytest.raises(ValueError, match="Default machine type"):
        RenderConfig.from_sections({**argo, "default_machine_type": "x"}, parameters, "cloud")
