"""Tests du projet Kedro : métadonnées, réglages, registre des pipelines, commande projet.

Aucune connexion ni secret : la session se crée, le contexte se charge et la commande
``kedro trade config-check`` compare catalogue et paramètres sur chaque environnement.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def project() -> Path:
    """Projet Kedro initialisé (métadonnées de pyproject.toml, réglages du paquet)."""
    from kedro.framework.startup import bootstrap_project

    metadata = bootstrap_project(ROOT)
    assert metadata.package_name == "kedro_pipeline"
    assert metadata.project_name == "trade-analysis"
    return ROOT


def test_settings_use_config_folder_and_soft_parameter_merge(project: Path) -> None:
    """Configuration lue dans config/, paramètres fusionnés récursivement, hooks projet."""
    from kedro.framework.project import settings

    from kedro_pipeline.hooks import TradeRunHooks

    assert settings.CONF_SOURCE == "config"
    assert settings.CONFIG_LOADER_ARGS["merge_strategy"] == {"parameters": "soft"}
    assert {"argo", "mlflow", "experiments"} <= set(settings.CONFIG_LOADER_ARGS["config_patterns"])
    assert any(isinstance(hook, TradeRunHooks) for hook in settings.HOOKS)


def test_registry_has_the_business_pipelines_and_both_cadences(project: Path) -> None:
    """Six pipelines métier, leur somme et les deux cadences planifiées."""
    from kedro_pipeline.config import load_parameters
    from kedro_pipeline.pipeline_registry import register_pipelines

    pipelines = register_pipelines(load_parameters("base"))
    assert set(pipelines) == {
        "downloads", "baci", "vulnerabilities", "synthesis", "serving", "maintenance",
        "__default__", "daily", "weekly",
    }
    assert len(pipelines["__default__"].nodes) == 16
    assert len(pipelines["daily"].nodes) == 4


@pytest.mark.parametrize("env", ["base", "demo", "cloud", "local"])
def test_session_loads_context_of_every_environment(project: Path, env: str) -> None:
    """La session se crée sur chaque environnement (paramètres, catalogue, identifiants)."""
    from kedro.framework.session import KedroSession

    with KedroSession.create(project_path=project, env=env) as session:
        context = session.load_context()
        assert context.env == env
        assert "runtime" in context.params
        assert "comtrade.tariffline" in context.catalog


@pytest.mark.parametrize("env", ["base", "demo"])
def test_config_check_command_passes(project: Path, env: str) -> None:
    """``kedro trade config-check`` : catalogue et paramètres cohérents."""
    from kedro_pipeline.cli import cli

    result = CliRunner().invoke(cli, ["trade", "config-check", "--env", env])
    assert result.exit_code == 0, result.output
    assert f"Environment '{env}'" in result.output
