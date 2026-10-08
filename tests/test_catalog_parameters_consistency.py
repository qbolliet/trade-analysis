"""Cohérence catalogue Kedro ↔ paramètres, sur chaque environnement.

Le catalogue répète les emplacements lus par les étapes dans les paramètres (base de
métadonnées, alias, schéma — ``RESULT_SCHEMA`` ou dataflow assaini —, bucket, chemin de
données, gabarit des registres) : tout écart ferait écrire un nœud ailleurs que le script
équivalent. Le test échoue au premier écart, avec son détail.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from kedro_pipeline.config import load_parameters
from kedro_pipeline.config_check import (
    ExpectedRegistry,
    ExpectedServing,
    catalog_mismatches,
    expected_datasets,
)
from kedro_pipeline.io.ducklake import DuckLakeLocation

ROOT = Path(__file__).resolve().parents[1]


def _catalog(env: str):
    """Catalogue instancié de l'environnement (sans session ni connexion)."""
    from kedro.config import OmegaConfigLoader
    from kedro.io import DataCatalog

    from kedro_pipeline.settings import CONFIG_LOADER_ARGS

    loader = OmegaConfigLoader(conf_source=str(ROOT / "config"), env=env, **CONFIG_LOADER_ARGS)
    return DataCatalog.from_config(loader["catalog"], loader["credentials"])


@pytest.mark.parametrize("env", ["base", "demo"])
def test_catalog_matches_parameters(env: str) -> None:
    assert catalog_mismatches(_catalog(env), load_parameters(env)) == []


def test_every_baci_target_has_its_factory_dataset() -> None:
    """Un dataset par millésime cible, résolu par la factory « baci.{vintage} »."""
    expected = expected_datasets(load_parameters("base"))
    baci = sorted(name for name in expected if name.startswith("baci."))
    assert baci == [f"baci.hs{year}" for year in (1992, 1996, 2002, 2007, 2012, 2017, 2022)]
    assert expected["baci.hs1992"].schema == "baci_hs1992"
    demo = expected_datasets(load_parameters("demo"))
    assert sorted(name for name in demo if name.startswith("baci.")) == ["baci.hs2017"]
    assert demo["baci.hs2017"].schema == "demo_baci_hs2017"


def test_dataflow_schemas_are_sanitised() -> None:
    """Le schéma d'un dataflow est assaini comme par les téléchargements (DS-045409 → DS_045409)."""
    expected = expected_datasets(load_parameters("base"))
    assert expected["eurostat.comext"].schema == "DS_045409"
    assert expected["reference.comtrade.hs_concordance"].schema == "reference_hs_concordance"
    assert isinstance(expected["state.partners"], ExpectedRegistry)
    assert isinstance(expected["serving.tables"], ExpectedServing)


def test_a_divergent_schema_is_reported() -> None:
    """Un schéma modifié dans les paramètres seulement est signalé."""
    parameters = load_parameters("base")
    parameters["synthesis"]["SYNTHESIS"]["RESULT_SCHEMA"] = "synthesis_v2"
    messages = catalog_mismatches(_catalog("base"), parameters)
    assert len(messages) == 1 and messages[0].startswith("synthesis.scores:")
    assert "synthesis_v2" in messages[0]


def test_a_missing_dataset_is_reported() -> None:
    """Un dataset attendu absent du catalogue est signalé."""

    class _Catalog:
        def get(self, name):
            return None

    messages = catalog_mismatches(_Catalog(), load_parameters("base"))
    assert any(message == "eurostat.comext: missing from the catalog" for message in messages)


def test_expected_locations_are_ducklake_locations() -> None:
    expected = expected_datasets(load_parameters("base"))
    tables = {name: value for name, value in expected.items() if isinstance(value, DuckLakeLocation)}
    assert {"vulnerabilities.partners", "vulnerabilities.network", "synthesis.diagnostics"} <= set(tables)
