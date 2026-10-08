"""Tests du chargement de la configuration hors session Kedro (``kedro_pipeline.config``).

Les scripts transitoires lisent leurs blocs par ``load_parameters()`` : même fusion que
Kedro (arguments de ``settings.py``), environnement choisi par ``KEDRO_ENV``. Leurs
fonctions ``load_*_config`` gardent leur signature : sans chemin, le bloc des paramètres ;
avec un chemin, le fichier fourni (avec ou sans clé racine).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from kedro_pipeline.config import (
    active_targets,
    experiment_name,
    load_config_section,
    load_parameters,
    read_config_file,
    resolve_env,
)

ROOT_KEYS = {
    "baci", "comtrade", "eurostat", "maintenance", "oecd", "runtime", "serving",
    "synthesis", "tracking", "vulnerabilities",
}


def test_every_parameter_file_has_a_single_root_key() -> None:
    """Un fichier par domaine, une clé racine par fichier (fusion Kedro sans doublon)."""
    root = Path(__file__).resolve().parents[1] / "config" / "base"
    roots = set()
    for path in root.glob("parameters_*.yml"):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert list(document) == [path.stem.removeprefix("parameters_")], path
        roots |= set(document)
    assert roots == ROOT_KEYS
    assert set(load_parameters("base")) == ROOT_KEYS


def test_environment_defaults_to_kedro_env_then_local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEDRO_ENV", raising=False)
    assert resolve_env() == "local"
    monkeypatch.setenv("KEDRO_ENV", "demo")
    assert resolve_env() == "demo"
    assert load_parameters()["serving"]["SCHEMA"] == "demo_dashboard"
    assert resolve_env("base") == "base"


def test_local_and_cloud_environments_equal_base_today() -> None:
    """Aucune surcharge effective en local (vide) ni en cloud (N_JOBS nul, comme en base)."""
    base = load_parameters("base")
    assert load_parameters("local") == base
    assert load_parameters("cloud") == base


def test_loaded_parameters_are_independent_copies() -> None:
    """Le cache du chargeur n'est jamais modifié par un appelant."""
    first = load_parameters("base")
    first["runtime"]["WEEKLY_DAY"] = -1
    assert load_parameters("base")["runtime"]["WEEKLY_DAY"] == 5


def test_demo_merge_is_recursive_and_replaces_lists() -> None:
    """Surcharge partielle d'un bloc : clés non surchargées conservées, listes remplacées."""
    base, demo = load_parameters("base"), load_parameters("demo")
    # Clé surchargée et voisine conservée
    assert demo["comtrade"]["DOWNLOADS"]["DBNAME"] == "demo_comtrade"
    assert demo["comtrade"]["DOWNLOADS"]["REFERENCE"] == base["comtrade"]["DOWNLOADS"]["REFERENCE"]
    # Liste remplacée en entier
    assert [source["SCHEMA"] for source in demo["synthesis"]["SYNTHESIS"]["SOURCES"]] == [
        "demo_indicators", "demo_network_indicators",
    ]
    # Cibles BACI : entrées nulles de l'environnement écartées à la lecture
    assert list(active_targets(demo["baci"]["CLASSIFICATIONS"]["TARGETS"])) == ["HS2017"]
    assert list(active_targets(base["baci"]["CLASSIFICATIONS"]["TARGETS"]))[0] == "HS2022"


def test_credentials_are_resolved_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Identifiants lus par oc.env, entrée composée pour les datasets DuckLake."""
    monkeypatch.setenv("PGHOST", "pg.example")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    credentials = load_config_section("credentials", "base")
    assert credentials["ducklake_postgres"]["host"] == "pg.example"
    assert credentials["ducklake"]["postgres"] == credentials["ducklake_postgres"]
    assert credentials["ducklake"]["s3"]["session_token"] is None


def test_oc_env_is_refused_in_parameters(tmp_path: Path) -> None:
    """Les secrets ne passent jamais par les paramètres (oc.env réservé aux credentials)."""
    from kedro.config import OmegaConfigLoader

    from kedro_pipeline.settings import CONFIG_LOADER_ARGS

    (tmp_path / "base").mkdir()
    (tmp_path / "local").mkdir()
    (tmp_path / "base" / "parameters_x.yml").write_text("x: {host: '${oc.env:PGHOST}'}\n")
    loader = OmegaConfigLoader(conf_source=str(tmp_path), env="local", **CONFIG_LOADER_ARGS)
    with pytest.raises(Exception, match="oc.env"):
        loader["parameters"]


@pytest.mark.parametrize(("block", "env", "name"), [
    ("downloads", "base", "trade-01-downloads"),
    ("baci", "base", "trade-02-baci"),
    ("vulnerabilities", "base", "trade-03-vulnerabilities"),
    ("serving", "demo", "demo-trade-04-serving"),
])
def test_experiment_names(block: str, env: str, name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MLFLOW_EXPERIMENT_NAME", raising=False)
    assert experiment_name(block, env) == name


def test_experiment_variable_takes_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MLFLOW_EXPERIMENT_NAME", "one-off")
    assert experiment_name("baci", "base") == "one-off"
    monkeypatch.delenv("MLFLOW_EXPERIMENT_NAME")
    with pytest.raises(KeyError):
        experiment_name("unknown", "base")


def test_script_loaders_read_parameters_or_the_given_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``load_*_config()`` : bloc des paramètres ; ``load_*_config(chemin)`` : le fichier."""
    from scripts.compute_synthetic_scores import load_synthesis_config
    from scripts.download_comtrade import load_config, load_runtime_config
    from scripts.process_baci_hs import load_baci_config

    monkeypatch.setenv("KEDRO_ENV", "demo")
    demo = load_parameters("demo")
    assert load_config() == demo["comtrade"]
    assert load_baci_config() == demo["baci"]
    assert load_runtime_config() == demo["runtime"]
    assert load_synthesis_config() == demo["synthesis"]
    # Fichier explicite, avec clé racine (format config/<env>/) ou sans (format historique)
    rooted = tmp_path / "rooted.yml"
    rooted.write_text(yaml.safe_dump({"baci": {"BUCKET": "x"}}), encoding="utf-8")
    flat = tmp_path / "flat.yml"
    flat.write_text(yaml.safe_dump({"BUCKET": "y"}), encoding="utf-8")
    assert load_baci_config(rooted) == {"BUCKET": "x"}
    assert load_baci_config(flat) == {"BUCKET": "y"}
    runtime = tmp_path / "runtime.yml"
    runtime.write_text(yaml.safe_dump({"runtime": {"WEEKLY_DAY": 3}}), encoding="utf-8")
    assert load_runtime_config(runtime) == {"WEEKLY_DAY": 3}
    assert read_config_file(flat, None) == {"BUCKET": "y"}


def test_tracking_config_falls_back_to_an_empty_report(tmp_path: Path) -> None:
    """Rapport de run : bloc tracking des paramètres, vide si le fichier fourni manque."""
    from scripts._run_report import load_tracking_config

    assert load_tracking_config()["REPORT"]["MAX_TABLE_ROWS"] == 50
    assert load_tracking_config(str(tmp_path / "absent.yml")) == {}
