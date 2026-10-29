"""Données du DAG du pipeline réel (environnements ``cloud`` et ``demo``) et fichiers versionnés.

Le dernier test est le contrôle de la CI (``kedro trade render-argo --check``) : il échoue
tant que ``helm/trade-pipeline/files/`` n'a pas été régénéré après une modification des
pipelines ou de ``argo.yml``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

from kedro_pipeline.deploy.render import diff_files, render_files

ROOT = Path(__file__).resolve().parents[2]
CHART_FILES = ROOT / "helm" / "trade-pipeline" / "files"
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
def files() -> Dict[str, str]:
    return render_files()


@pytest.fixture(scope="module")
def cloud(files) -> Dict[str, Any]:
    return yaml.safe_load(files["dag-cloud.yaml"])


def _tasks(data: Dict[str, Any], entry_point: str) -> Dict[str, Any]:
    entry = next(entry for entry in data["entryPoints"] if entry["name"] == entry_point)
    return {task["name"]: task for task in entry["tasks"]}


def test_one_file_per_chart_environment(files) -> None:
    assert sorted(files) == ["dag-cloud.yaml", "dag-demo.yaml"]
    assert yaml.safe_load(files["dag-demo.yaml"])["kedroEnv"] == "demo"


def test_task_count_per_entry_point(cloud, files) -> None:
    assert len(_tasks(cloud, "daily")) == 4
    assert len(_tasks(cloud, "weekly")) == 12
    assert set(_tasks(cloud, "daily")) == {
        "download-eurostat", "download-comtrade", "compute-partner-vulnerabilities", "publish-serving",
    }
    # demo : un seul millésime BACI cible
    demo = yaml.safe_load(files["dag-demo.yaml"])
    assert [name for name in _tasks(demo, "weekly") if name.startswith("process-baci-")] == ["process-baci-hs2017"]


def test_key_dependencies(cloud) -> None:
    daily, weekly = _tasks(cloud, "daily"), _tasks(cloud, "weekly")
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


def test_experiment_resources_and_retries_per_task(cloud) -> None:
    tasks = {**_tasks(cloud, "daily"), **_tasks(cloud, "weekly")}
    for name, experiment in EXPERIMENTS.items():
        assert tasks[name]["experiment"] == experiment, name
    for name in ("download-eurostat", "download-comtrade"):
        assert (tasks[name]["retries"], tasks[name]["cpu"], tasks[name]["mem"]) == (0, 1, 8)
    assert tasks["process-baci-hs2017"]["mem"] == 32
    assert tasks["compute-synthetic-scores"]["cpu"] == 8
    assert tasks["prepare-baci"]["retries"] == 1


def test_mutex_on_serving_and_maintenance(cloud) -> None:
    for entry_point in ("daily", "weekly"):
        assert _tasks(cloud, entry_point)["publish-serving"]["mutex"] == "trade-serving"
    assert cloud["mutexes"] == ["trade-serving"]
    assert cloud["exit"]["node"] == "maintain_ducklake" and cloud["exit"]["mutex"] == "trade-maintenance"
    assert cloud["exit"]["experiment"] == "trade-00-maintenance"
    # La maintenance n'appartient à aucun DAG
    assert all("maintain-ducklake" not in _tasks(cloud, e) for e in ("daily", "weekly"))


def test_default_crons_and_system_metrics(cloud) -> None:
    crons = {entry["name"]: entry["cron"] for entry in cloud["entryPoints"]}
    assert crons == {"daily": "0 1 * * *", "weekly": "0 2 * * 6"}
    assert cloud["systemMetrics"] == {"enabled": True, "samplingSeconds": 30}
    assert cloud["podPipeline"] == "__default__"


def test_rendering_is_deterministic(files) -> None:
    assert render_files() == files


def test_versioned_files_are_up_to_date(files) -> None:
    differences = diff_files(files, CHART_FILES)
    assert differences == {}, "Run `uv run kedro trade render-argo`:\n" + "\n".join(differences.values())
