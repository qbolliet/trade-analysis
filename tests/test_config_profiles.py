"""Tests de cohérence de la configuration Kedro et de l'environnement ``demo``.

Vérifie que le bloc ``runtime`` porte les profondeurs historiques et les millésimes
attendus, que l'environnement ``config/demo/`` ne fait que SURCHARGER des clés qui
existent en ``base`` (fusion récursive « soft »), et les invariants du périmètre de
démonstration (pas de plafond de requêtes, pas de liste de produits prioritaires,
sorties ``demo`` isolées).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, List, Tuple

import pytest
import yaml

from kedro_pipeline.config import active_targets, experiment_name, load_parameters


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
DEMO = CONFIG / "demo"
BASE = load_parameters("base")
MERGED_DEMO = load_parameters("demo")
# Fichiers de surcharge de l'environnement demo
DEMO_FILES = sorted(DEMO.glob("parameters_*.yml"))


def _load(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _unknown_keys(override: Any, base: Any, path: Tuple[str, ...] = ()) -> List[str]:
    """Clés d'une surcharge absentes de la configuration de base (récursif sur les mappings)."""
    if not isinstance(override, dict):
        return []
    if not isinstance(base, dict):
        return ["/".join(path) or "<root>"]
    unknown: List[str] = []
    for key, value in override.items():
        if key not in base:
            unknown.append("/".join(path + (str(key),)))
        else:
            unknown += _unknown_keys(value, base[key], path + (str(key),))
    return unknown


def _walk(node: Any, path: Tuple[str, ...] = ()) -> Iterator[Tuple[Tuple[str, ...], Any]]:
    """Parcours (chemin, valeur) de toutes les clés d'un document."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield path + (str(key),), value
            yield from _walk(value, path + (str(key),))
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item, path)


def test_runtime_config_values() -> None:
    """Profondeur historique par source et millésimes HS."""
    runtime = BASE["runtime"]
    assert runtime["ANALYSIS_START_YEAR"] == {"eurostat": 1988, "comtrade": 1994}
    assert runtime["NOMENCLATURES"]["HS"] == {
        "HS1992": 1988, "HS1996": 1996, "HS2002": 2002, "HS2007": 2007,
        "HS2012": 2012, "HS2017": 2017, "HS2022": 2022,
    }
    assert runtime["WEEKLY_DAY"] == 5
    assert runtime["N_JOBS"] is None


def test_baci_targets_cover_every_vintage_newest_first() -> None:
    """Cibles BACI : sept millésimes, du plus récent au plus ancien, START_YEAR ≥ 1994."""
    from scripts.process_baci_hs import resolve_target_start_years

    runtime = BASE["runtime"]
    targets = BASE["baci"]["CLASSIFICATIONS"]["TARGETS"]
    assert list(targets) == ["HS2022", "HS2017", "HS2012", "HS2007", "HS2002", "HS1996", "HS1992"]
    assert set(targets) <= set(runtime["NOMENCLATURES"]["HS"])
    starts = resolve_target_start_years(targets, runtime)
    assert starts["HS1992"] == 1994
    assert min(starts.values()) >= runtime["ANALYSIS_START_YEAR"]["comtrade"]
    assert all(cfg["RESULT_SCHEMA"] == f"baci_{label.lower()}" for label, cfg in targets.items())


@pytest.mark.parametrize("path", DEMO_FILES, ids=lambda path: path.name)
def test_demo_overrides_only_existing_keys(path: Path) -> None:
    """Une surcharge demo ne vise que des clés de base (une faute de frappe serait ignorée)."""
    override = _load(path)
    assert isinstance(override, dict) and len(override) == 1
    assert _unknown_keys(override, BASE) == []


@pytest.mark.parametrize("path", DEMO_FILES, ids=lambda path: path.name)
def test_demo_files_declare_provisional_scope(path: Path) -> None:
    """Chaque surcharge demo signale en tête le périmètre provisoire."""
    head = path.read_text(encoding="utf-8").splitlines()[:10]
    assert any("PÉRIMÈTRE PROVISOIRE" in line for line in head)


def test_demo_serving_uses_same_catalog_and_its_own_schema() -> None:
    """Un seul catalogue `serving` (un DATA_PATH, lu par Superset) : seul le schéma change."""
    prod = BASE["serving"]
    demo = MERGED_DEMO["serving"]
    assert (demo["DBNAME"], demo["DATA_PATH"]) == (prod["DBNAME"], prod["DATA_PATH"])
    assert (prod["SCHEMA"], demo["SCHEMA"]) == ("dashboard", "demo_dashboard")
    assert experiment_name("serving", "demo").startswith("demo-")
    assert demo["TABLES"] == prod["TABLES"]


@pytest.mark.parametrize("block", ["downloads", "baci", "vulnerabilities", "serving"])
def test_demo_experiments_are_prefixed(block: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Expériences MLflow demo isolées de la production (préfixe demo-)."""
    monkeypatch.delenv("MLFLOW_EXPERIMENT_NAME", raising=False)
    assert experiment_name(block, "demo") == f"demo-{experiment_name(block, 'base')}"


def test_demo_outputs_are_isolated() -> None:
    """Schémas résultats préfixés demo_, chemins sous trade/demo/, catalogues bruts demo_."""
    targets = active_targets(MERGED_DEMO["baci"]["CLASSIFICATIONS"]["TARGETS"])
    assert targets == {"HS2017": {"START_YEAR": 2017, "RESULT_SCHEMA": "demo_baci_hs2017"}}
    demo = dict(MERGED_DEMO)
    demo["baci"] = {**demo["baci"], "CLASSIFICATIONS": {
        **demo["baci"]["CLASSIFICATIONS"], "TARGETS": targets,
    }}
    for name in ("comtrade", "eurostat", "baci", "vulnerabilities", "synthesis", "serving"):
        for path, value in _walk(demo[name]):
            if path[-1] == "RESULT_SCHEMA" or path[-2:] == ("SOURCES", "SCHEMA"):
                assert str(value).startswith("demo_"), (name, path, value)
            # Exception : le catalogue `serving` (seul attaché par Superset) est partagé et
            # DuckLake n'admet qu'un DATA_PATH par catalogue ; le demo y est isolé par son
            # schéma demo_dashboard (test_demo_serving_uses_same_catalog_and_its_own_schema)
            if name == "serving" and path == ("DATA_PATH",):
                continue
            if path[-1] in {
                "LAST_DOWNLOAD_PATH", "LAST_COMPUTATION_PATH", "LAST_PROCESSING_PATH",
                "DATA_PATH", "PATH_TEMPLATE", "WORK_PATH",
            }:
                assert str(value).startswith("trade/demo/"), (name, path, value)
            if path == ("DOWNLOADS", "DBNAME"):
                assert str(value).startswith("demo_"), (name, path, value)


def test_demo_scope_and_synthesis_parameters() -> None:
    """Périmètre demo : années ≥ 2015, produits ciblés, synthèse adaptée."""
    comtrade = MERGED_DEMO["comtrade"]
    assert comtrade["split_filters"]["C_A_HS"]["periods"]["start"] == 2015
    regex = comtrade["split_filters"]["C_A_HS"]["products"]["include_regex"]
    assert regex.startswith("^(2805|2846|8105|8112|2844|3004|8541|8542|8507)")
    synthesis = MERGED_DEMO["synthesis"]["SYNTHESIS"]
    assert synthesis["PARAMETERS"]["min_group_size"] == 10
    assert synthesis["FILTERS"]["LAST_N_PERIODS"] is None
    # Même comportement de téléchargement qu'en production
    assert comtrade["parameters"] == BASE["comtrade"]["parameters"]
    assert MERGED_DEMO["runtime"] == BASE["runtime"]
    assert MERGED_DEMO["tracking"] == BASE["tracking"]


def test_interpolations_are_resolved_across_files() -> None:
    """Valeurs interpolées depuis d'autres blocs : résolues après fusion des fichiers."""
    start = BASE["runtime"]["ANALYSIS_START_YEAR"]["comtrade"]
    assert BASE["comtrade"]["split_filters"]["C_A_HS"]["periods"]["start"] == start
    assert BASE["baci"]["CLASSIFICATIONS"]["TARGETS"]["HS1992"]["START_YEAR"] == start
    for params in (BASE, MERGED_DEMO):
        assert params["synthesis"]["SYNTHESIS"]["FLOWS"] == params["vulnerabilities"]["FLOWS"]


@pytest.mark.parametrize("env", ["base", "demo"])
@pytest.mark.parametrize("name", ["comtrade", "eurostat"])
def test_dataset_configs_have_uncapped_queries(name: str, env: str) -> None:
    """``max_queries`` nul partout, dataflow déclaré, 10 h de run."""
    config = load_parameters(env)[name]
    dataflow = config["DATAFLOW"]
    assert config["parameters"][dataflow]["max_queries"] is None
    assert config["DOWNLOADS"][dataflow]["MAX_RUNTIME"]["HOURS"] == 10


@pytest.mark.parametrize("env", ["base", "demo"])
@pytest.mark.parametrize("name", ["comtrade", "eurostat"])
def test_dataset_configs_declare_tolerated_error_ratio(name: str, env: str) -> None:
    """Seuil d'échec des téléchargements déclaré, dans [0, 1] (sinon l'étape resterait « réussie »)."""
    config = load_parameters(env)[name]
    ratio = config["DOWNLOADS"][config["DATAFLOW"]]["MAX_ERROR_RATIO"]
    assert 0.0 <= ratio < 1.0


def test_eurostat_reporters_include_union() -> None:
    """Le reporter agrégé EU27_2020 suit les 27 États membres."""
    reporters = BASE["eurostat"]["split_filters"]["DS-045409"]["reporter"]["include"]
    assert len(reporters) == 28 and reporters[-1] == "EU27_2020"


def test_mlflow_server_and_experiments_are_not_parameters() -> None:
    """URI et expériences MLflow hors paramètres ; options de journalisation sous TRACKING."""
    for params in (BASE, MERGED_DEMO):
        for name, block in params.items():
            for path, _ in _walk(block):
                assert path[-1] not in {"MLFLOW", "TRACKING_URI", "EXPERIMENT"}, (name, path)
    assert BASE["vulnerabilities"]["TRACKING"] == {"LOG_ARTIFACTS": True, "DRIFT": True}
    assert BASE["vulnerabilities"]["NETWORK_VULNERABILITIES"]["TRACKING"]["DRIFT"] is True


def test_no_priority_key_in_config() -> None:
    """Aucune liste de produits prioritaires."""
    for path in CONFIG.rglob("*.yml"):
        for key_path, _ in _walk(_load(path) or {}):
            assert "priority" not in key_path[-1].lower(), (path, key_path)


@pytest.mark.parametrize("env", ["base", "demo"])
def test_flows_are_the_same_for_partners_network_and_synthesis(env: str) -> None:
    """Les sens de flux sont communs aux trois étapes ; le flux sépare les contextes."""
    params = load_parameters(env)
    vulnerabilities = params["vulnerabilities"]
    synthesis = params["synthesis"]["SYNTHESIS"]
    flows = vulnerabilities["FLOWS"]
    assert flows == ["import", "export"]
    assert vulnerabilities["NETWORK_VULNERABILITIES"]["FLOWS"] == flows
    assert synthesis["FLOWS"] == flows
    # Import et export jamais comparés : le flux est une clé de contexte
    assert "flow" in synthesis["PARAMETERS"]["context_columns"]
    # Filtre de flux généré (plus de littéral) ; reporter agrégé exclu de la synthèse
    assert '"flow"' not in synthesis["FILTERS"]["WHERE"]
    assert "EU27_2020" in synthesis["FILTERS"]["WHERE"]
    network_join = synthesis["SOURCES"][1]["JOIN"]["ON"]
    assert 'n."flow" = p."flow"' in network_join


@pytest.mark.parametrize(("env", "vintages", "provisional"), [
    ("base", "all", False),
    ("demo", ["HS2017"], True),
])
def test_nomenclature_vintages_and_provisional_flag(env: str, vintages, provisional) -> None:
    """Millésimes historiques, drapeau provisoire et registre fragmenté par classification."""
    from kedro_pipeline.config import requested_vintages

    params = load_parameters(env)
    vulnerabilities = params["vulnerabilities"]
    runtime = params["runtime"]
    assert vulnerabilities["VINTAGES"] == vintages
    # Valeur valide au regard du référentiel
    assert requested_vintages(vulnerabilities["VINTAGES"], runtime["NOMENCLATURES"]["HS"])
    assert vulnerabilities["VINTAGES_ON_UNMAPPED"] in ("raise", "drop", "keep")
    block = vulnerabilities["VULNERABILITIES"]["DS-045409"]
    assert block["IS_PROVISIONAL"] is provisional
    assert "{classification}" in block["STATE"]["PATH_TEMPLATE"]


@pytest.mark.parametrize("env", ["base", "demo"])
def test_synthesis_joins_network_on_the_vintage_of_each_row(env: str) -> None:
    """Plus de millésime en dur : jointure sur hs_vintage, contexte par millésime."""
    synthesis = load_parameters(env)["synthesis"]["SYNTHESIS"]
    assert synthesis["VINTAGES"] == "in_force"
    assert synthesis["PARAMETERS"]["context_columns"][0] == "hs_vintage"
    join = synthesis["SOURCES"][1]["JOIN"]
    assert 'n."classification" = p."hs_vintage"' in join["ON"]
    assert "WHERE" not in join
    for folder in (CONFIG / "base", DEMO):
        for name in ("synthesis", "serving", "vulnerabilities"):
            path = folder / f"parameters_{name}.yml"
            if path.exists():
                assert "HS2022" not in path.read_text(encoding="utf-8"), path
