"""Tests de cohérence des fichiers de configuration et du profil ``demo`` (PS-04).

Vérifie que ``config/runtime.yaml`` porte les valeurs de PD-08, que chaque
fichier du profil ``config/profiles/demo/`` se charge et contient toutes les
clés du fichier de production correspondant, et les invariants de phase 0
(pas de plafond de requêtes, pas de liste de produits prioritaires, sorties
``demo`` isolées).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, List, Tuple

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
DEMO = CONFIG / "profiles" / "demo"

# Fichier de production → copie demo
PROFILE_FILES = {
    "comtrade.yaml": CONFIG / "datasets" / "comtrade.yaml",
    "eurostat.yaml": CONFIG / "datasets" / "eurostat.yaml",
    "baci.yaml": CONFIG / "baci.yaml",
    "vulnerabilities.yaml": CONFIG / "vulnerabilities.yaml",
    "synthesis.yaml": CONFIG / "synthesis.yaml",
    "runtime.yaml": CONFIG / "runtime.yaml",
    "serving.yaml": CONFIG / "serving.yaml",
}

# Mappings qui sont des collections de données (et non des schémas de clés) :
# le demo peut en retenir un sous-ensemble, chaque entrée gardant les clés
# d'une entrée de production
COLLECTIONS = {("CLASSIFICATIONS", "TARGETS")}


def _load(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _missing_keys(prod: Any, demo: Any, path: Tuple[str, ...] = ()) -> List[str]:
    """Clés présentes en production et absentes du demo (récursif sur les mappings)."""
    if not isinstance(prod, dict):
        return []
    if not isinstance(demo, dict):
        return ["/".join(path) or "<root>"]
    missing: List[str] = []
    if path in COLLECTIONS:
        # Chaque entrée demo doit avoir le schéma d'une entrée de production
        reference = next(iter(prod.values()))
        for key, value in demo.items():
            missing += _missing_keys(reference, value, path + (str(key),))
        return missing
    for key, value in prod.items():
        if key not in demo:
            missing.append("/".join(path + (str(key),)))
        else:
            missing += _missing_keys(value, demo[key], path + (str(key),))
    return missing


def _walk(node: Any, path: Tuple[str, ...] = ()) -> Iterator[Tuple[Tuple[str, ...], Any]]:
    """Parcours (chemin, valeur) de toutes les clés d'un document YAML."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield path + (str(key),), value
            yield from _walk(value, path + (str(key),))
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item, path)


def test_runtime_config_values() -> None:
    """Profondeur historique par source et millésimes HS (PD-08, PS-04.1)."""
    runtime = _load(CONFIG / "runtime.yaml")["runtime"]
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

    runtime = _load(CONFIG / "runtime.yaml")["runtime"]
    targets = _load(CONFIG / "baci.yaml")["CLASSIFICATIONS"]["TARGETS"]
    assert list(targets) == ["HS2022", "HS2017", "HS2012", "HS2007", "HS2002", "HS1996", "HS1992"]
    assert set(targets) <= set(runtime["NOMENCLATURES"]["HS"])
    starts = resolve_target_start_years(targets, runtime)
    assert starts["HS1992"] == 1994
    assert min(starts.values()) >= runtime["ANALYSIS_START_YEAR"]["comtrade"]
    assert all(cfg["RESULT_SCHEMA"] == f"baci_{label.lower()}" for label, cfg in targets.items())


@pytest.mark.parametrize("name", sorted(PROFILE_FILES))
def test_demo_profile_loads_and_has_every_production_key(name: str) -> None:
    """Chaque profil demo se charge et ne manque d'aucune clé de production."""
    demo = _load(DEMO / name)
    prod = _load(PROFILE_FILES[name])
    assert isinstance(demo, dict)
    assert _missing_keys(prod, demo) == []


@pytest.mark.parametrize("name", sorted(PROFILE_FILES))
def test_demo_profile_declares_provisional_scope(name: str) -> None:
    """Chaque fichier demo signale en tête le périmètre provisoire (PQ-07)."""
    head = (DEMO / name).read_text(encoding="utf-8").splitlines()[:10]
    assert any("PQ-07" in line for line in head)


def test_demo_serving_uses_same_catalog_and_its_own_schema() -> None:
    """Un seul catalogue `serving` (un DATA_PATH, lu par Superset) : seul le schéma change."""
    prod = _load(CONFIG / "serving.yaml")["serving"]
    demo = _load(DEMO / "serving.yaml")["serving"]
    assert (demo["DBNAME"], demo["DATA_PATH"]) == (prod["DBNAME"], prod["DATA_PATH"])
    assert (prod["SCHEMA"], demo["SCHEMA"]) == ("dashboard", "demo_dashboard")
    assert demo["MLFLOW"]["EXPERIMENT"].startswith("demo-")
    assert demo["TABLES"] == prod["TABLES"]


def test_demo_outputs_are_isolated() -> None:
    """Schémas résultats préfixés demo_, chemins sous trade/demo/, catalogues bruts demo_."""
    baci = _load(DEMO / "baci.yaml")
    assert baci["CLASSIFICATIONS"]["TARGETS"] == {
        "HS2017": {"START_YEAR": 2017, "RESULT_SCHEMA": "demo_baci_hs2017"}
    }
    for name in PROFILE_FILES:
        for path, value in _walk(_load(DEMO / name)):
            if path[-1] == "RESULT_SCHEMA" or path[-2:] == ("SOURCES", "SCHEMA"):
                assert str(value).startswith("demo_"), (name, path, value)
            # Exception : le catalogue `serving` (seul attaché par Superset) est partagé et
            # DuckLake n'admet qu'un DATA_PATH par catalogue ; le demo y est isolé par son
            # schéma demo_dashboard (test_demo_serving_uses_same_catalog_and_its_own_schema)
            if path == ("serving", "DATA_PATH"):
                continue
            if path[-1] in {"LAST_DOWNLOAD_PATH", "LAST_COMPUTATION_PATH", "LAST_PROCESSING_PATH", "DATA_PATH"}:
                assert str(value).startswith("trade/demo/"), (name, path, value)
            if path[-1] == "DBNAME" and name in {"comtrade.yaml", "eurostat.yaml"}:
                assert str(value).startswith("demo_"), (name, path, value)


def test_demo_scope_and_synthesis_parameters() -> None:
    """Périmètre demo (PS-04.4) : années ≥ 2015, produits ciblés, synthèse adaptée."""
    comtrade = _load(DEMO / "comtrade.yaml")
    assert comtrade["split_filters"]["C_A_HS"]["periods"]["start"] == 2015
    regex = comtrade["split_filters"]["C_A_HS"]["products"]["include_regex"]
    assert regex.startswith("^(2805|2846|8105|8112|2844|3004|8541|8542|8507)")
    synthesis = _load(DEMO / "synthesis.yaml")["SYNTHESIS"]
    assert synthesis["PARAMETERS"]["min_group_size"] == 10
    assert synthesis["FILTERS"]["LAST_N_PERIODS"] is None
    # Même comportement de téléchargement qu'en production (PD-06)
    prod = _load(CONFIG / "datasets" / "comtrade.yaml")
    assert comtrade["parameters"] == prod["parameters"]
    assert _load(DEMO / "runtime.yaml") == _load(CONFIG / "runtime.yaml")


@pytest.mark.parametrize(
    "path",
    [
        CONFIG / "datasets" / "comtrade.yaml",
        CONFIG / "datasets" / "eurostat.yaml",
        DEMO / "comtrade.yaml",
        DEMO / "eurostat.yaml",
    ],
)
def test_dataset_configs_have_uncapped_queries(path: Path) -> None:
    """``max_queries`` nul partout (C-01), dataflow déclaré (C-05), 10 h de run."""
    config = _load(path)
    dataflow = config["DATAFLOW"]
    assert config["parameters"][dataflow]["max_queries"] is None
    assert config["DOWNLOADS"][dataflow]["MAX_RUNTIME"]["HOURS"] == 10


@pytest.mark.parametrize(
    "path",
    [
        CONFIG / "datasets" / "comtrade.yaml",
        CONFIG / "datasets" / "eurostat.yaml",
        DEMO / "comtrade.yaml",
        DEMO / "eurostat.yaml",
    ],
)
def test_dataset_configs_declare_tolerated_error_ratio(path: Path) -> None:
    """Seuil d'échec des téléchargements déclaré, dans [0, 1] (sinon l'étape resterait « réussie »)."""
    config = _load(path)
    ratio = config["DOWNLOADS"][config["DATAFLOW"]]["MAX_ERROR_RATIO"]
    assert 0.0 <= ratio < 1.0


def test_eurostat_reporters_include_union() -> None:
    """Le reporter agrégé EU27_2020 suit les 27 États membres (PD-21)."""
    reporters = _load(CONFIG / "datasets" / "eurostat.yaml")["split_filters"]["DS-045409"]["reporter"]["include"]
    assert len(reporters) == 28 and reporters[-1] == "EU27_2020"


def test_no_priority_key_in_config() -> None:
    """Aucune liste de produits prioritaires (PD-06)."""
    for path in CONFIG.rglob("*.yaml"):
        for key_path, _ in _walk(_load(path) or {}):
            assert "priority" not in key_path[-1].lower(), (path, key_path)
