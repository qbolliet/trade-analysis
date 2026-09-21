"""``process_baci_hs.main()`` de bout en bout sur données fictives, avec rapport de run MLflow.

Le script est exécuté tel quel ; seuls sont remplacés ses accès externes : connecteur DuckLake
(catalogue fichier temporaire rempli par le téléchargement fictif), API Comtrade (planification
des requêtes), tables de correspondance UNSD, fichiers CEPII et référentiels. Le run MLflow local
qui en résulte sert aussi à la vérification manuelle de PQ-19 (``pytest --basetemp=<dossier>``
conserve le magasin).
"""

from __future__ import annotations

import datetime as dt
import itertools
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from conftest import REPO_ROOT

pytestmark = pytest.mark.slow

PRODUCTS = ["280519", "810510", "854110", "300490"]


class _NoCloseConnection:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def close(self) -> None:
        return None


class _FakeConnector:
    def __init__(self, conn: Any, alias: str) -> None:
        self._conn, self.catalog_alias = conn, alias

    def connect(self) -> Any:
        return _NoCloseConnection(self._conn)


class _Client:
    """Client d'API factice : ``close`` sans effet."""

    def close(self) -> None:
        return None


@pytest.fixture
def baci_world(ducklake_conn, synthetic_world, synthetic_reference, synthetic_section, tmp_path, monkeypatch):
    """Catalogue fictif rempli, configurations temporaires et accès externes remplacés."""
    from kedro_pipeline.synthetic.comtrade import ReportingConfig, SyntheticComtradeClient
    from scripts.download_comtrade import build_split_queries
    from statflows.core.download import download_updates
    import scripts.process_baci_hs as script

    conn, alias = ducklake_conn
    client = SyntheticComtradeClient(
        synthetic_world, synthetic_reference, ReportingConfig.from_mapping(synthetic_section["REPORTING"]),
        product_universe=PRODUCTS,
    )
    queries = build_split_queries(
        "C_A_HS",
        {"reporters": pd.DataFrame({"code": ["251"]}), "products": pd.DataFrame({"code": PRODUCTS})},
        {"frequency": "annual", "flows": ["M", "X"], "type_code": "C", "classification": "HS"},
        {"reporters": {"include": None}, "products": {"include": PRODUCTS}},
        periods=["2019", "2020", "2021"], products_step=2,
    )
    registry_path = tmp_path / "last_downloads.json"
    download_updates(
        client=client, queries=queries, connector=_FakeConnector(conn, alias),
        structures_path=tmp_path / "structures.json", last_download_path=registry_path,
        bucket=None, max_runtime=None,
    )

    # Configurations : profil demo, sans S3 et avec des chemins temporaires
    demo = REPO_ROOT / "config" / "profiles" / "demo"
    comtrade = yaml.safe_load((demo / "comtrade.yaml").read_text(encoding="utf-8"))
    comtrade["DOWNLOADS"]["C_A_HS"]["BUCKET"] = None
    comtrade["DOWNLOADS"]["C_A_HS"]["PATHS"]["LAST_DOWNLOAD_PATH"] = str(registry_path)
    baci = yaml.safe_load((demo / "baci.yaml").read_text(encoding="utf-8"))
    baci["BUCKET"] = None
    baci["PATHS"]["LAST_PROCESSING_PATH"] = str(tmp_path / "last_processing.json")
    baci["PARAMETERS"] = {"SCHEMA": {"distance_column": "distw"}, "min_mirror_flows": 5, "fas_countries": ["CAN"]}
    paths = {}
    for name, content in (("comtrade", comtrade), ("baci", baci)):
        paths[name] = tmp_path / f"{name}.yaml"
        paths[name].write_text(yaml.safe_dump(content), encoding="utf-8")
    monkeypatch.setenv("COMTRADE_CONFIG_PATH", str(paths["comtrade"]))
    monkeypatch.setenv("BACI_CONFIG_PATH", str(paths["baci"]))
    monkeypatch.setenv("RUNTIME_CONFIG_PATH", str(demo / "runtime.yaml"))
    monkeypatch.setenv("TRACKING_CONFIG_PATH", str(REPO_ROOT / "config" / "tracking.yaml"))

    # Accès externes
    isos = list(synthetic_world.iso3)
    dist = pd.DataFrame(list(itertools.permutations(isos, 2)), columns=["iso_o", "iso_d"])
    dist["distw"] = np.random.default_rng(0).uniform(500, 15000, len(dist))
    dist["contig"] = 0
    geo = pd.DataFrame({"iso3": isos, "landlocked": 0})
    monkeypatch.setattr(script, "ComtradeClient", lambda **kwargs: _Client())
    monkeypatch.setattr(script, "UNSDClient", lambda **kwargs: _Client())
    monkeypatch.setattr(script, "fetch_dimension_codelists", lambda *a, **k: pd.DataFrame({"code": PRODUCTS}))
    monkeypatch.setattr(script, "plan_queries", lambda *a, **k: queries)
    monkeypatch.setattr(script, "_ensure_concordances", lambda *a, **k: {})
    monkeypatch.setattr(script, "publish_hs_reference", lambda *a, **k: {"rows": {}, "failures": {}})
    monkeypatch.setattr(script, "build_connector", lambda *a, **k: _FakeConnector(conn, alias))
    monkeypatch.setattr(script, "pg_credentials_from_env", lambda: None)
    monkeypatch.setattr(script, "s3_credentials_from_env", lambda: None)
    monkeypatch.setattr(
        script.TableLoader, "load",
        lambda self, path, bucket=None, **kw: dist if "dist" in str(path) else geo,
    )
    return script, conn, alias


def _runs(uri: str):
    import mlflow

    client = mlflow.tracking.MlflowClient(tracking_uri=uri)
    runs = client.search_runs([e.experiment_id for e in client.search_experiments()])
    return client, runs


def test_main_publishes_the_baci_run_report(baci_world, monkeypatch, mlflow_uri) -> None:
    script, conn, alias = baci_world
    monkeypatch.setenv("MLFLOW_TRACKING_URI", mlflow_uri)
    monkeypatch.setenv("WORKFLOW_ID", "trade-pipeline-weekly-7k2qd")
    monkeypatch.setenv("PROFILE", "demo")
    monkeypatch.setenv("IMAGE_TAG", "sha-6f24c6c")
    # Métriques système (onglet « System metrics ») : psutil est dans l'extra `tracking`
    monkeypatch.setenv("MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING", "true")
    monkeypatch.setenv("MLFLOW_SYSTEM_METRICS_SAMPLING_INTERVAL", "0.5")

    # Le run fictif est trop court pour qu'un échantillon système soit journalisé : le
    # redressement est ralenti de quelques intervalles d'échantillonnage
    real_run_baci = script.run_baci

    def slow_run_baci(*args, **kwargs):
        time.sleep(2.5)
        return real_run_baci(*args, **kwargs)

    monkeypatch.setattr(script, "run_baci", slow_run_baci)
    script.main()

    client, runs = _runs(mlflow_uri)
    assert len(runs) == 1
    run = runs[0]
    assert client.get_experiment(run.info.experiment_id).name == "demo-trade-02-baci"
    assert run.info.run_name == "process_baci_HS2017-trade-pipeline-weekly-7k2qd"
    assert run.info.status == "FINISHED"
    tags, metrics = run.data.tags, run.data.metrics

    # Description conforme à PS-31.3
    description = tags["mlflow.note.content"]
    lines = description.splitlines()
    assert lines[0].startswith("### ") and "process_baci_HS2017" in lines[0]
    assert lines[1].startswith("Exécution `trade-pipeline-weekly-7k2qd` · env `demo` · image `sha-6f24c6c` · ")
    assert "| Unités prévues | Réussies | En échec |" in description and "| 1 millésime (3 années éligibles) | 1 | 0 |" in description
    assert "**Contrôles** :" in description and "**Chiffres clés** :" in description
    assert description.rstrip().endswith("ressources (System metrics)")
    assert tags["health"] in ("ok", "warning") and tags["node"] == "process_baci_HS2017"

    # Métriques hiérarchisées par « / » (Model metrics) et contrôles
    assert metrics["baci/flows"] > 0 and "baci/gravity/r_squared" in metrics and metrics["coverage/years_eligible"] == 3
    assert not any(name.startswith("baci.") for name in metrics)
    assert metrics["checks/n_failed"] == 0 and metrics["run/duration_seconds"] > 0

    # Artefacts : rapport HTML (une section par étape BACI), résumé, contrôles, tables
    files = {a.path for a in client.list_artifacts(run.info.run_id, "report")}
    assert files == {"report/summary.md", "report/report.html", "report/checks.csv"}
    tables = {a.path for a in client.list_artifacts(run.info.run_id, "tables")}
    assert {"tables/conversion_rates.csv", "tables/gravity_coefficients.csv", "tables/sigma_by_country.csv",
            "tables/rows_by_year.csv"} <= tables
    html = Path(client.download_artifacts(run.info.run_id, "report/report.html")).read_text(encoding="utf-8")
    for section in ("Conversion en tonnes", "Fobisation", "Gravité", "Qualité des déclarants",
                    "Valorisation et réconciliation", "NES", "Harmonisation de nomenclature", "Sortie",
                    "Temps et ressources"):
        assert f"<h2>{section}" in html, section
    summary = Path(client.download_artifacts(run.info.run_id, "report/summary.md")).read_text(encoding="utf-8")
    assert summary == description

    # Onglet « System metrics » : métriques `system/*` échantillonnées par MLflow
    system = client.get_metric_history(run.info.run_id, "system/cpu_utilization_percentage")
    assert system, "aucune métrique système : psutil ou la variable d'environnement manque"


def test_a_failing_vintage_gets_its_failure_report_and_the_script_still_raises(
    baci_world, monkeypatch, mlflow_uri
) -> None:
    script, conn, alias = baci_world
    monkeypatch.setenv("MLFLOW_TRACKING_URI", mlflow_uri)

    def boom(*args, **kwargs):
        raise ValueError("gravité impossible")

    monkeypatch.setattr(script, "run_baci", boom)
    with pytest.raises(RuntimeError, match="1 millésime"):
        script.main()

    _, runs = _runs(mlflow_uri)
    assert len(runs) == 1 and runs[0].info.status == "FAILED"
    description = runs[0].data.tags["mlflow.note.content"]
    assert description.splitlines()[:3] == [
        "### ❌ process_baci_HS2017 — échec",
        "`ValueError` : gravité impossible",
        "Dernière étape atteinte : `redressement BACI`",
    ]
    assert runs[0].data.tags["health"] == "failed"
