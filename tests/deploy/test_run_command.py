"""Commande ``run`` du projet, appelée par les pods (``python -m kedro_pipeline``).

``kedro run`` reste la commande d'argo-kedro (commande globale de plugin, prioritaire sur
les commandes projet) et ignore ``--params`` : les pods appellent donc la commande du
projet, qui transmet les paramètres d'exécution à la session. L'exécution réelle, hooks
MLflow compris, est vérifiée par ``tests/pipeline/test_kedro_mlflow_run.py`` (lent).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

import pytest
from click.testing import CliRunner
from kedro.framework.cli.utils import find_run_command

from kedro_pipeline import cli


class _FakeSession:
    """Session Kedro simulée : enregistre la création et l'exécution."""

    calls: List[Dict[str, Any]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.created = kwargs

    @classmethod
    def create(cls, **kwargs: Any) -> "_FakeSession":
        session = cls(**kwargs)
        cls.calls.append({"create": kwargs})
        return session

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def load_context(self) -> Any:
        return SimpleNamespace(argo=SimpleNamespace(runner=SimpleNamespace(use_memory_datasets=True)))

    def run(self, **kwargs: Any) -> None:
        _FakeSession.calls.append({"run": kwargs})


@pytest.fixture()
def fake_session(monkeypatch: pytest.MonkeyPatch):
    import kedro.framework.session as session_module
    import kedro.framework.startup as startup_module

    _FakeSession.calls = []
    monkeypatch.setattr(session_module, "KedroSession", _FakeSession)
    monkeypatch.setattr(startup_module, "bootstrap_project", lambda path: None)
    return _FakeSession


def test_python_m_kedro_pipeline_finds_the_project_run_command() -> None:
    assert find_run_command("kedro_pipeline") is cli.run


def test_run_passes_the_runtime_parameters_and_the_fused_runner(fake_session) -> None:
    from argo_kedro.runners import FusedRunner

    result = CliRunner().invoke(
        cli.run,
        [
            "--pipeline=__default__", "--env=test", "--nodes=compute_synthetic_scores",
            "--params=runtime.FORCE_STEPS=partners;synthesis,runtime.FORCE_METRICS=,runtime.FORCE_SCOPE.PERIODS=2020;2021,",
        ],
    )
    assert result.exit_code == 0, result.output
    created, ran = fake_session.calls[0]["create"], fake_session.calls[1]["run"]
    assert created["env"] == "test"
    # Valeur vide → None (aucun forçage) ; « ; » conservé à l'intérieur des valeurs
    assert created["runtime_params"] == {
        "runtime": {"FORCE_STEPS": "partners;synthesis", "FORCE_METRICS": None, "FORCE_SCOPE": {"PERIODS": "2020;2021"}}
    }
    assert ran["pipeline_names"] == ["__default__"] and ran["node_names"] == ["compute_synthetic_scores"]
    assert isinstance(ran["runner"], FusedRunner) and ran["runner"]._use_memory_datasets is True


def test_run_without_params_runs_the_whole_pipeline(fake_session) -> None:
    result = CliRunner().invoke(cli.run, ["--env", "test"])
    assert result.exit_code == 0, result.output
    assert fake_session.calls[0]["create"]["runtime_params"] is None
    assert fake_session.calls[1]["run"]["node_names"] is None


def test_a_comma_inside_a_value_is_refused(fake_session) -> None:
    # Kedro découpe --params sur les virgules avant de séparer clés et valeurs
    result = CliRunner().invoke(cli.run, ["--params", "runtime.FORCE_STEPS=partners,synthesis"])
    assert result.exit_code != 0 and "must contain a key and a value" in result.output


def test_the_forcing_parameters_reach_the_force_spec() -> None:
    from kedro_pipeline.io.freshness import ForceSpec

    runtime = {"FORCE_STEPS": "partners;synthesis", "FORCE_METRICS": None, "FORCE_METHODS": None}
    assert set(ForceSpec.from_runtime(runtime, environ={}).steps) == {"partners", "synthesis"}
