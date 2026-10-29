"""Exécutions Kedro de bout en bout sur l'environnement ``test`` (données simulées, sans réseau).

L'environnement ``config/test/`` remplace PostgreSQL + S3 par des catalogues DuckLake
FICHIERS et les API par les clients factices de ``tests/pipeline/fakes.py``, sous un
dossier temporaire (``TRADE_TEST_ROOT``). Une même racine enchaîne les exécutions :

1. pipeline complet : toutes les tables sont écrites, catalogue ``serving`` compris ;
2. pipeline complet à nouveau : rien à télécharger, et aucune unité partenaire, BACI,
   réseau, synthèse ni cohérence à recalculer ;
2b. une observation Eurostat révisée par le fournisseur : seules les unités concernées
   sont recalculées (couples du reporter, contextes de la période, cohérence qui les
   suit), puis plus rien à la passe suivante ;
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
# Nœuds dont la décision de fraîcheur est mesurée (unités planifiées)
DECIDING_NODES = (
    "compute_partner_vulnerabilities", "compute_network_vulnerabilities",
    "compute_synthetic_scores", "compute_synthesis_coherence",
)
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


def _query(name: str, sql: str):
    """Résultat d'une requête sur une table du catalogue de test (``{table}`` substitué)."""
    from kedro.framework.session import KedroSession

    with KedroSession.create(project_path=PROJECT, env="test") as session:
        handle = session.load_context().catalog.load(name)
    return handle.query(sql.format(table=handle.qualified_name))


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


def _recomputed(metrics: Dict[str, Dict[str, float]], node: str) -> float:
    """Unités que les décisions de fraîcheur du nœud ont planifiées (0 si le nœud n'en a pas écrit)."""
    return sum(v for k, v in metrics.get(node, {}).items() if k.endswith("freshness/units_planned"))


def test_full_runs_are_incremental_forcible_and_split_by_cadence(test_env: Path, monkeypatch) -> None:
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
    first_recomputed = {node: _recomputed(metrics, node) for node in DECIDING_NODES}
    assert all(count > 0 for count in first_recomputed.values()), first_recomputed
    serving_tables = set(load_parameters("test")["serving"]["TABLES"])
    assert serving_tables <= _serving_tables()
    # Classement par finesse de nomenclature : niveau du code écrit par les partenaires,
    # clé de contexte de la synthèse et de la cohérence ; rangs par pays de 1 à n par niveau
    # (le monde de test ne compte que des codes SH6 : la séparation de plusieurs niveaux
    # repose sur le mécanisme générique des colonnes de contexte)
    levels = _query("vulnerabilities.partners", "SELECT DISTINCT product_level FROM {table}")
    assert set(levels["product_level"]) <= {2, 4, 6, 8} and len(levels) > 0
    groups = _query("synthesis.scores", """
        SELECT product_level, min(rank_by_reporter) AS first,
               max(rank_by_reporter) AS last, count(rank_by_reporter) AS n,
               bool_and(length(CAST(product AS VARCHAR)) + length(CAST(product AS VARCHAR)) % 2
                        = product_level) AS same_level
        FROM {table}
        GROUP BY hs_vintage, freq, flow, indicators, "TIME_PERIOD", product_level, reporter, method
        HAVING count(rank_by_reporter) > 0""")
    assert len(groups) > 0 and groups["same_level"].all()
    # Rangs propres au groupe (ex aequo au rang moyen) : bornés par 1 et par sa taille
    assert (groups["first"] >= 1).all() and (groups["last"] <= groups["n"]).all()
    assert (groups["first"] == 1).mean() > 0.5
    diagnostics = _query("synthesis.diagnostics", "SELECT DISTINCT product_level FROM {table}")
    assert set(diagnostics["product_level"]) == set(groups["product_level"])

    # ── 2. Seconde exécution : rien à télécharger, aucune étape aval à recalculer ──
    executed, metrics = _run(test_env)
    assert metrics["download_eurostat"]["download/rows_written"] == 0
    assert metrics["download_comtrade"]["download/rows_written"] == 0
    # Une requête rejouée sans nouvelle donnée ne modifie rien : les étapes décident sur
    # les données modifiées, pas sur la date du dernier téléchargement
    for node in DECIDING_NODES:
        assert _recomputed(metrics, node) == 0, node
    for node in ("prepare_baci", "process_baci_hs2017", "compute_network_vulnerabilities"):
        assert _planned(metrics, node) == 0, node

    # ── 2b. Une observation révisée par le fournisseur : seules les unités concernées ──
    from tests.pipeline.fakes import FakeEurostatClient

    monkeypatch.setattr(FakeEurostatClient, "revisions", {("FR", "2022", "CN"): 3.0})
    executed, metrics = _run(test_env)
    monkeypatch.setattr(FakeEurostatClient, "revisions", {})
    assert metrics["download_eurostat"]["download/rows_written"] > 0
    # Partenaires : les couples du seul reporter révisé, jamais tous
    partners = _recomputed(metrics, "compute_partner_vulnerabilities")
    assert 0 < partners < first_recomputed["compute_partner_vulnerabilities"]
    # Cascade : seuls les contextes de la période révisée sont resynthétisés, puis la cohérence
    synthesis = _recomputed(metrics, "compute_synthetic_scores")
    assert 0 < synthesis < first_recomputed["compute_synthetic_scores"]
    assert _recomputed(metrics, "compute_synthesis_coherence") == synthesis
    # BACI et réseau ne lisent pas Eurostat : rien à recalculer
    for node in ("prepare_baci", "process_baci_hs2017", "compute_network_vulnerabilities"):
        assert _planned(metrics, node) == 0, node
    # La révision une fois prise en compte, plus rien n'est périmé
    executed, metrics = _run(test_env)
    for node in DECIDING_NODES:
        assert _recomputed(metrics, node) == 0, node

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
