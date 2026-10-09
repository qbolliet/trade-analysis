"""Exécutions Kedro de bout en bout sur l'environnement ``test`` (données simulées, sans réseau).

L'environnement ``config/test/`` remplace PostgreSQL + S3 par des catalogues DuckLake
FICHIERS et les API par les clients factices de ``tests/pipeline/fakes.py``, sous un
dossier temporaire (``TRADE_TEST_ROOT``). Une même racine enchaîne quatre exécutions :

1. pipeline complet : toutes les tables sont écrites, catalogue ``serving`` compris ;
2. pipeline complet à nouveau : rien à télécharger, BACI et réseau à jour ;
3. pipeline complet avec ``runtime.FORCE_STEPS=synthesis`` : la synthèse est forcée ;
4. point d'entrée ``daily`` : seules les quatre tâches quotidiennes s'exécutent.

Chaque exécution ouvre sa propre session Kedro (une session n'accepte qu'un ``run``)
et passe par le ``FusedRunner`` d'argo-kedro, seul capable d'exécuter les tâches
fusionnées du téléchargement (comme ``kedro run``).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

pytestmark = pytest.mark.slow

pytest.importorskip("dt_ducklake_manager")

# Racine du dépôt (projet Kedro)
PROJECT = Path(__file__).resolve().parents[2]
# Tâches du point d'entrée quotidien
DAILY_TASKS = {"download_eurostat", "download_comtrade", "compute_partner_vulnerabilities", "publish_serving"}
# Tables écrites par le pipeline (dataset du catalogue)
WRITTEN_TABLES = (
    "eurostat.comext", "comtrade.tariffline", "baci.hs2017", "vulnerabilities.partners",
    "vulnerabilities.network", "synthesis.scores", "synthesis.diagnostics",
    "reference.eurostat.products", "reference.comtrade.products", "reference.comtrade.hs_vintages",
)


def _runner_class():
    """``FusedRunner`` qui consigne les tâches qu'on lui fait exécuter."""
    from argo_kedro.runners import FusedRunner

    class RecordingRunner(FusedRunner):
        executed: List[str] = []

        def _run(self, pipeline, catalog, hook_manager, session_id=None):
            RecordingRunner.executed = [node.name for node in pipeline.nodes]
            return super()._run(pipeline, catalog, hook_manager, session_id)

    return RecordingRunner


@pytest.fixture
def test_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Racine temporaire de l'environnement ``test``, projet amorcé sur cet environnement."""
    from kedro.framework.startup import bootstrap_project

    from kedro_pipeline import config

    root = tmp_path / "trade"
    root.mkdir()
    monkeypatch.setenv("TRADE_TEST_ROOT", str(root))
    # Environnement lu à la construction des pipelines (nœuds BACI des cibles de test)
    monkeypatch.setenv("KEDRO_ENV", "test")
    monkeypatch.delenv("WORKFLOW_ID", raising=False)
    # Aucun suivi vers un serveur MLflow ambiant (le magasin fichier de l'environnement test
    # est refusé sans MLFLOW_ALLOW_FILE_STORE : suivi désactivé, avec un avertissement)
    for name in ("MLFLOW_TRACKING_URI", "MLFLOW_ALLOW_FILE_STORE"):
        monkeypatch.delenv(name, raising=False)
    config._cached_section.cache_clear()
    bootstrap_project(PROJECT)
    yield root
    config._cached_section.cache_clear()


def _run(
    root: Path, pipeline: str = "__default__", runtime_params: Optional[Dict[str, Any]] = None
) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
    """Exécute un pipeline dans une nouvelle session ; renvoie les tâches et leurs métriques."""
    from kedro.framework.session import KedroSession

    runner = _runner_class()
    with KedroSession.create(project_path=PROJECT, env="test", runtime_params=runtime_params) as session:
        session.run(pipeline_name=pipeline, runner=runner(pipeline_name=pipeline))
    metrics = {
        path.stem: json.loads(path.read_text(encoding="utf-8"))
        for path in (root / "reporting" / "metrics").glob("*.json")
    }
    return list(runner.executed), metrics


def _row_count(name: str) -> int:
    """Nombre de lignes d'une table du catalogue de test (0 si absente)."""
    from kedro.framework.session import KedroSession

    with KedroSession.create(project_path=PROJECT, env="test") as session:
        handle = session.load_context().catalog.load(name)
    if not handle.exists():
        return 0
    return int(handle.query(f"SELECT count(*) AS n FROM {handle.qualified_name}")["n"].iloc[0])


def _serving_tables() -> set:
    """Tables du schéma de restitution du catalogue fichier ``serving``."""
    from kedro.framework.session import KedroSession

    with KedroSession.create(project_path=PROJECT, env="test") as session:
        catalog = session.load_context().catalog.load("serving.tables")
    with catalog.connect() as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ?",
            [catalog.location.catalog_alias, catalog.location.schema],
        ).fetchall()
    return {name for (name,) in rows}


def _planned(metrics: Dict[str, Dict[str, float]], node: str) -> float:
    return metrics[node]["units/planned"]


def test_full_runs_are_incremental_forcible_and_split_by_cadence(test_env: Path) -> None:
    from kedro.framework.project import pipelines

    from kedro_pipeline.config import load_parameters

    # ── 1. Première exécution : toutes les tables sont écrites ─────────────────
    executed, metrics = _run(test_env)
    assert set(executed) == {node.name for node in pipelines["__default__"].nodes}
    for node in (
        "download_eurostat", "download_comtrade", "prepare_baci", "process_baci_hs2017",
        "compute_partner_vulnerabilities", "compute_network_vulnerabilities",
        "compute_synthetic_scores", "compute_synthesis_coherence", "publish_serving",
    ):
        assert _planned(metrics, node) > 0, node
        assert metrics[node]["units/failed"] == 0, node
    assert metrics["download_eurostat"]["download/rows_written"] > 0
    assert metrics["download_comtrade"]["download/rows_written"] > 0
    for name in WRITTEN_TABLES:
        assert _row_count(name) > 0, name
    serving_tables = set(load_parameters("test")["serving"]["TABLES"])
    assert serving_tables <= _serving_tables()

    # ── 2. Seconde exécution : rien à télécharger, BACI et réseau à jour ───────
    executed, metrics = _run(test_env)
    assert metrics["download_eurostat"]["download/rows_written"] == 0
    assert metrics["download_comtrade"]["download/rows_written"] == 0
    for node in ("prepare_baci", "process_baci_hs2017", "compute_network_vulnerabilities"):
        assert _planned(metrics, node) == 0, node
    # Comportement figé (à corriger dans statflows) : une requête re-téléchargée sans
    # nouvelle donnée avance tout de même sa date de dernier téléchargement, que les
    # métriques partenaires prennent pour filigrane amont ; partenaires, synthèse et
    # cohérence recalculent donc tout pour « nouvelles données » à chaque exécution
    partners = metrics["compute_partner_vulnerabilities"]
    assert partners["units/planned"] > 0
    assert sum(v for k, v in partners.items() if k.endswith("freshness/units_new_data")) > 0
    assert metrics["compute_synthetic_scores"]["freshness/units_new_data"] > 0
    assert metrics["compute_synthetic_scores"]["freshness/units_first"] == 0

    # ── 3. Forçage de la synthèse ──────────────────────────────────────────────
    executed, metrics = _run(test_env, runtime_params={"runtime": {"FORCE_STEPS": "synthesis"}})
    synthesis = metrics["compute_synthetic_scores"]
    assert synthesis["freshness/units_forced"] == synthesis["freshness/units_planned"] > 0
    for node in ("prepare_baci", "process_baci_hs2017", "compute_network_vulnerabilities"):
        assert _planned(metrics, node) == 0, node
    partners = metrics["compute_partner_vulnerabilities"]
    assert sum(v for k, v in partners.items() if k.endswith("freshness/units_forced")) == 0

    # ── 4. Point d'entrée quotidien ───────────────────────────────────────────
    executed, _ = _run(test_env, pipeline="daily")
    assert set(executed) == DAILY_TASKS


def test_kedro_run_command_on_the_test_environment(tmp_path: Path) -> None:
    """``kedro run --env test`` (commande d'argo-kedro, FusedRunner) passe de bout en bout."""
    env = {
        **os.environ,
        "TRADE_TEST_ROOT": str(tmp_path),
        "KEDRO_ENV": "test",
        "KEDRO_DISABLE_TELEMETRY": "true",
    }
    for name in ("WORKFLOW_ID", "MLFLOW_TRACKING_URI", "MLFLOW_ALLOW_FILE_STORE"):
        env.pop(name, None)
    completed = subprocess.run(
        [sys.executable, "-m", "kedro", "run", "--env", "test"],
        cwd=PROJECT, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    assert completed.returncode == 0, completed.stdout[-4000:] + completed.stderr[-4000:]
    assert (tmp_path / "reporting" / "metrics" / "publish_serving.json").exists()
