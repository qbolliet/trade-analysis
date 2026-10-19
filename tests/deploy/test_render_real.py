"""Rendu Argo du pipeline réel (environnement ``cloud``) et manifestes versionnés.

Le dernier test est le contrôle de la CI (``kedro trade render-argo --check``) : il échoue
tant que ``kubernetes/generated/`` n'a pas été régénéré après une modification des
pipelines, de ``argo.yml`` ou des gabarits.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

from kedro_pipeline.deploy.render import diff_manifests, render_manifests

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "kubernetes" / "generated"
SECRETS = {"trade-s3-credentials", "comtrade-api-credentials", "trade-postgres-credentials", "trade-mlflow-credentials"}
EXPERIMENTS = {
    "download-eurostat": "trade-01-downloads",
    "download-comtrade": "trade-01-downloads",
    "prepare-baci": "trade-02-baci",
    "process-baci-hs2017": "trade-02-baci",
    "compute-partner-vulnerabilities": "trade-03-vulnerabilities",
    "compute-network-vulnerabilities": "trade-03-vulnerabilities",
    "compute-synthetic-scores": "trade-03-vulnerabilities",
    "compute-synthesis-coherence": "trade-03-vulnerabilities",
    "publish-serving": "trade-04-serving",
}
VINTAGES = ("hs1992", "hs1996", "hs2002", "hs2007", "hs2012", "hs2017", "hs2022")


@pytest.fixture(scope="module")
def manifests() -> Dict[str, str]:
    return render_manifests("cloud")


@pytest.fixture(scope="module")
def template(manifests) -> Dict[str, Any]:
    return yaml.safe_load(manifests["workflowtemplate.yaml"])


def _templates(template: Dict[str, Any]) -> Dict[str, Any]:
    return {t["name"]: t for t in template["spec"]["templates"]}


def _tasks(template: Dict[str, Any], entry_point: str) -> Dict[str, Any]:
    return {task["name"]: task for task in _templates(template)[entry_point]["dag"]["tasks"]}


def _parameters(task: Dict[str, Any]) -> Dict[str, str]:
    return {p["name"]: p["value"] for p in task["arguments"]["parameters"]}


def test_three_manifests(manifests) -> None:
    assert sorted(manifests) == ["cronworkflow-daily.yaml", "cronworkflow-weekly.yaml", "workflowtemplate.yaml"]


def test_task_count_per_entry_point(template) -> None:
    assert len(_tasks(template, "daily")) == 4
    assert len(_tasks(template, "weekly")) == 12
    assert set(_tasks(template, "daily")) == {
        "download-eurostat", "download-comtrade", "compute-partner-vulnerabilities", "publish-serving",
    }


def test_key_dependencies(template) -> None:
    daily, weekly = _tasks(template, "daily"), _tasks(template, "weekly")
    # Partenaires : tolérants à l'échec du téléchargement Comext ; concordances BACI lues
    # telles quelles (prepare_baci appartient au weekly)
    assert daily["compute-partner-vulnerabilities"]["depends"] == "(download-eurostat.Succeeded || download-eurostat.Failed)"
    assert "download-" not in weekly["prepare-baci"].get("depends", "")
    for vintage in VINTAGES:
        assert weekly[f"process-baci-{vintage}"]["depends"] == "prepare-baci.Succeeded"
        assert f"(process-baci-{vintage}.Succeeded || process-baci-{vintage}.Failed)" in (
            weekly["compute-network-vulnerabilities"]["depends"]
        )
    assert "(compute-network-vulnerabilities.Succeeded" in weekly["compute-synthetic-scores"]["depends"]
    assert "(compute-synthetic-scores.Succeeded" in weekly["compute-synthesis-coherence"]["depends"]
    assert "(compute-synthesis-coherence.Succeeded" in weekly["publish-serving"]["depends"]
    assert "(compute-partner-vulnerabilities.Succeeded" in daily["publish-serving"]["depends"]


def test_experiment_resources_and_retries_per_task(template) -> None:
    tasks = {**_tasks(template, "daily"), **_tasks(template, "weekly")}
    for name, experiment in EXPERIMENTS.items():
        assert _parameters(tasks[name])["experiment"] == experiment, name
    for name in ("download-eurostat", "download-comtrade"):
        assert _parameters(tasks[name])["retries"] == "0"
        assert (_parameters(tasks[name])["cpu"], _parameters(tasks[name])["mem"]) == ("1", "8")
    assert _parameters(tasks["process-baci-hs2017"])["mem"] == "32"
    assert _parameters(tasks["compute-synthetic-scores"])["cpu"] == "8"
    assert _parameters(tasks["prepare-baci"])["retries"] == "1"


def test_mutex_on_serving_and_maintenance(template) -> None:
    templates = _templates(template)
    for entry_point in ("daily", "weekly"):
        assert _tasks(template, entry_point)["publish-serving"]["template"] == "locked-trade-serving"
    assert templates["locked-trade-serving"]["synchronization"] == {"mutex": {"name": "trade-serving"}}
    handler = templates[template["spec"]["onExit"]]
    assert handler["synchronization"] == {"mutex": {"name": "trade-maintenance"}}
    step = handler["steps"][0][0]
    assert _parameters(step)["kedro-node"] == "maintain_ducklake"
    assert _parameters(step)["experiment"] == "trade-00-maintenance"
    # La maintenance n'appartient à aucun DAG
    assert all("maintain-ducklake" not in _tasks(template, e) for e in ("daily", "weekly"))


def test_pod_environment_and_secrets(template) -> None:
    container = _templates(template)["kedro"]["container"]
    env = {item["name"]: item for item in container["env"]}
    secrets = {item["valueFrom"]["secretKeyRef"]["name"] for item in env.values() if "secretKeyRef" in item.get("valueFrom", {})}
    secrets |= {item["secretRef"]["name"] for item in container["envFrom"]}
    assert secrets == SECRETS
    assert "trade-serving-credentials" not in yaml.safe_dump(template)
    assert env["KEDRO_ENV"]["value"] == "cloud"
    assert env["MLFLOW_EXPERIMENT_NAME"]["value"] == "{{inputs.parameters.experiment}}"
    assert env["MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING"]["value"] == "true"
    assert env["MLFLOW_SYSTEM_METRICS_SAMPLING_INTERVAL"]["value"] == "30"
    assert env["AWS_SESSION_TOKEN"]["value"] == ""
    # Commande projet (kedro run ignorerait --params) et séparateur des forçages
    assert container["command"] == ["python", "-m", "kedro_pipeline"]
    assert "--env=cloud" in container["args"] and "--pipeline=__default__" in container["args"]
    params = next(arg for arg in container["args"] if arg.startswith("--params="))
    assert "runtime.FORCE_STEPS={{workflow.parameters.force-steps}}" in params


def test_workflow_parameters_and_spec(template) -> None:
    spec = template["spec"]
    parameters = {p["name"]: p["value"] for p in spec["arguments"]["parameters"]}
    assert set(parameters) == {"image-tag", "force-steps", "force-metrics", "force-methods", "extra-params"}
    assert parameters["force-steps"] == parameters["force-metrics"] == parameters["force-methods"] == ""
    assert (spec["serviceAccountName"], spec["parallelism"]) == ("workflow", 8)


def test_crons(manifests) -> None:
    daily = yaml.safe_load(manifests["cronworkflow-daily.yaml"])["spec"]
    weekly = yaml.safe_load(manifests["cronworkflow-weekly.yaml"])["spec"]
    assert daily["schedules"] == ["0 1 * * *"] and weekly["schedules"] == ["0 2 * * 6"]
    assert daily["workflowSpec"]["entrypoint"] == "daily" and daily["workflowSpec"]["activeDeadlineSeconds"] == 82800
    assert weekly["workflowSpec"]["entrypoint"] == "weekly" and weekly["workflowSpec"]["activeDeadlineSeconds"] == 518400
    assert daily["concurrencyPolicy"] == weekly["concurrencyPolicy"] == "Forbid"
    # Les CronWorkflow n'exposent aucun forçage
    assert "arguments" not in daily["workflowSpec"] and "arguments" not in weekly["workflowSpec"]


def test_rendering_is_deterministic(manifests) -> None:
    assert render_manifests("cloud") == manifests


def test_versioned_manifests_are_up_to_date(manifests) -> None:
    differences = diff_manifests(manifests, GENERATED)
    assert differences == {}, "Run `uv run kedro trade render-argo`:\n" + "\n".join(differences.values())
