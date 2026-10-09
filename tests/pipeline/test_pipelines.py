"""Structure des pipelines Kedro : tâches, tags, types de machine, cadences, entrées libres.

Les tâches et leurs métadonnées d'ordonnancement sont lues par le rendu Argo : un
écart (tag de cadence oublié, type de machine erroné, tâche fusionnée défaite) changerait
silencieusement le workflow déployé.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pytest

from kedro_pipeline.config import load_parameters
from kedro_pipeline.pipeline_registry import register_pipelines

ROOT = Path(__file__).resolve().parents[2]
VINTAGES = ("hs1992", "hs1996", "hs2002", "hs2007", "hs2012", "hs2017", "hs2022")

# Tâche -> (tags, type de machine) attendus en environnement base
EXPECTED: Dict[str, tuple] = {
    "download_eurostat": ({"experiment.trade-01-downloads", "cadence.daily"}, "io-small"),
    "download_comtrade": ({"experiment.trade-01-downloads", "cadence.daily"}, "io-small"),
    "prepare_baci": ({"experiment.trade-02-baci", "cadence.weekly"}, "compute-medium"),
    **{
        f"process_baci_{vintage}": ({"experiment.trade-02-baci", "cadence.weekly"}, "baci-large")
        for vintage in VINTAGES
    },
    "compute_partner_vulnerabilities": (
        {"experiment.trade-03-vulnerabilities", "cadence.daily"}, "compute-medium"),
    "compute_network_vulnerabilities": (
        {"experiment.trade-03-vulnerabilities", "cadence.weekly"}, "compute-medium"),
    "compute_synthetic_scores": (
        {"experiment.trade-03-vulnerabilities", "cadence.weekly"}, "synthesis-cpu"),
    "compute_synthesis_coherence": (
        {"experiment.trade-03-vulnerabilities", "cadence.weekly"}, "synthesis-cpu"),
    "publish_serving": (
        {"experiment.trade-04-serving", "cadence.daily", "cadence.weekly", "mutex.trade-serving"},
        "compute-medium"),
    "maintain_ducklake": (
        {"experiment.trade-00-maintenance", "onexit", "mutex.trade-maintenance"}, "io-small"),
}


@pytest.fixture(scope="module")
def pipelines():
    return register_pipelines(load_parameters("base"))


def _catalog(env: str = "base"):
    """Catalogue instancié de l'environnement (sans session ni connexion)."""
    from kedro.config import OmegaConfigLoader
    from kedro.io import DataCatalog

    from kedro_pipeline.settings import CONFIG_LOADER_ARGS

    loader = OmegaConfigLoader(conf_source=str(ROOT / "config"), env=env, **CONFIG_LOADER_ARGS)
    return DataCatalog.from_config(loader["catalog"], loader["credentials"])


def test_default_pipeline_has_the_sixteen_scheduled_tasks(pipelines) -> None:
    default = pipelines["__default__"]
    assert {node.name: (set(node.tags), node.machine_type) for node in default.nodes} == EXPECTED


def test_download_tasks_stay_fused(pipelines) -> None:
    """La somme conserve les tâches fusionnées (l'addition de Kedro les déferait)."""
    from argo_kedro.pipeline.fused_pipeline import FusedNode

    tasks = {node.name: node for node in pipelines["__default__"].nodes}
    for source in ("eurostat", "comtrade"):
        task = tasks[f"download_{source}"]
        assert isinstance(task, FusedNode)
        assert [node.name for node in task._nodes] == [
            f"fetch_{source}", f"publish_reference_{source}", f"audit_coverage_{source}",
        ]
        # Le plan circule en mémoire à l'intérieur de la tâche seulement
        assert f"{source}.download_plan" not in task.inputs


def test_cadences_select_their_tasks(pipelines) -> None:
    daily = {node.name for node in pipelines["daily"].nodes}
    weekly = {node.name for node in pipelines["weekly"].nodes}
    assert daily == {"download_eurostat", "download_comtrade", "compute_partner_vulnerabilities", "publish_serving"}
    assert weekly == {
        "prepare_baci", *(f"process_baci_{v}" for v in VINTAGES), "compute_network_vulnerabilities",
        "compute_synthetic_scores", "compute_synthesis_coherence", "publish_serving",
    }
    # La maintenance n'appartient à aucune cadence : elle clôt les deux
    assert "maintain_ducklake" not in daily | weekly


@pytest.mark.parametrize("name", ["__default__", "daily", "weekly", "downloads", "baci", "serving"])
def test_free_inputs_are_catalog_datasets(pipelines, name: str) -> None:
    """Une entrée libre (non produite dans le pipeline) se charge depuis le catalogue.

    C'est ce qui permet à ``daily`` de lire tel quel ce que ``weekly`` a écrit : aucune
    entrée libre n'est un résultat en mémoire d'un nœud exclu.
    """
    from kedro.io import MemoryDataset

    catalog = _catalog()
    for dataset in pipelines[name].inputs():
        if dataset.startswith("params:"):
            continue
        resolved = catalog.get(dataset)
        assert resolved is not None, dataset
        assert not isinstance(resolved, MemoryDataset), dataset


def test_every_output_is_declared_in_the_catalog(pipelines) -> None:
    """Toute sortie échangée entre tâches est un dataset du catalogue (jamais implicite)."""
    catalog = _catalog()
    for node in pipelines["__default__"].nodes:
        for dataset in node.outputs:
            assert catalog.get(dataset) is not None, (node.name, dataset)


def test_baci_tasks_follow_the_enabled_targets() -> None:
    """Un nœud de redressement par cible active (entrée nulle = millésime désactivé)."""
    from kedro_pipeline.pipelines.baci import create_pipeline

    parameters = load_parameters("base")
    parameters["baci"]["CLASSIFICATIONS"]["TARGETS"] = {
        "HS2022": {"START_YEAR": 2022, "RESULT_SCHEMA": "baci_hs2022"},
        "HS2017": None,
        "HS2012": {"START_YEAR": 2012, "RESULT_SCHEMA": "baci_hs2012"},
    }
    names = {node.name for node in create_pipeline(parameters).nodes}
    assert names == {"prepare_baci", "process_baci_hs2022", "process_baci_hs2012"}


@pytest.mark.parametrize("env", ["base", "demo"])
def test_handles_built_by_the_nodes_designate_the_catalog_tables(env: str) -> None:
    """Les poignées écrites par les nœuds désignent exactement les tables du catalogue."""
    from kedro_pipeline.config_check import expected_datasets
    from kedro_pipeline.io.datasets import DuckLakeCatalogs
    from kedro_pipeline.pipelines.baci.nodes import vintage_schema
    from kedro_pipeline.pipelines.synthesis.nodes import synthesis_table
    from kedro_pipeline.pipelines.vulnerabilities.nodes import result_table
    from kedro_pipeline.steps._config import (
        HS_REFERENCE_TABLES,
        baci_target_schemas,
        download_location,
        reference_location,
        reference_tables,
        serving_location,
    )

    params = load_parameters(env)
    expected = expected_datasets(params)
    catalogs = DuckLakeCatalogs({}, {}, None)
    built = {
        "eurostat.comext": download_location(params["eurostat"]),
        "comtrade.tariffline": download_location(params["comtrade"]),
        "vulnerabilities.partners": result_table(
            catalogs, params["vulnerabilities"],
            params["vulnerabilities"]["VULNERABILITIES"][params["eurostat"]["DATAFLOW"]],
        ).location,
        "vulnerabilities.network": result_table(
            catalogs, params["vulnerabilities"], params["vulnerabilities"]["NETWORK_VULNERABILITIES"]
        ).location,
        "synthesis.scores": synthesis_table(catalogs, params["vulnerabilities"], params["synthesis"], "SYNTHESIS").location,
        "synthesis.diagnostics": synthesis_table(
            catalogs, params["vulnerabilities"], params["synthesis"], "COHERENCE").location,
    }
    for source in ("eurostat", "comtrade"):
        tables = reference_tables(params[source]) + (list(HS_REFERENCE_TABLES) if source == "comtrade" else [])
        for table in tables:
            built[f"reference.{source}.{table}"] = reference_location(params[source], table)
    for label in baci_target_schemas(params["baci"]):
        built[f"baci.{label.lower()}"] = download_location(
            params["comtrade"], schema=vintage_schema(label, params["baci"])
        )
    for name, location in built.items():
        assert location == expected[name], name
    assert serving_location(params["serving"]) == expected["serving.tables"].location
