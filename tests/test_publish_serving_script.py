"""Tests du script enveloppe ``scripts/publish_serving.py`` (``serving-script``)."""

from __future__ import annotations

import functools
from pathlib import Path

import pytest

from conftest import REPO_ROOT, build_serving_world


@pytest.fixture
def script_env(monkeypatch: pytest.MonkeyPatch):
    """Variables d'environnement factices (identifiants jamais utilisés par les catalogues fichiers)."""
    for name, value in {
        "PGHOST": "h", "PGPORT": "5432", "PGUSER": "u", "PGPASSWORD": "p", "PGDATABASE": "d",
        "AWS_S3_ENDPOINT": "e", "AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s",
        "SERVING_CONFIG_PATH": str(REPO_ROOT / "config" / "serving.yaml"),
        "EUROSTAT_CONFIG_PATH": str(REPO_ROOT / "config" / "datasets" / "eurostat.yaml"),
        "COMTRADE_CONFIG_PATH": str(REPO_ROOT / "config" / "datasets" / "comtrade.yaml"),
        "VULNERABILITIES_CONFIG_PATH": str(REPO_ROOT / "config" / "vulnerabilities.yaml"),
        "SYNTHESIS_CONFIG_PATH": str(REPO_ROOT / "config" / "synthesis.yaml"),
        "RUNTIME_CONFIG_PATH": str(REPO_ROOT / "config" / "runtime.yaml"),
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)


def _patch_catalog(monkeypatch: pytest.MonkeyPatch, world) -> None:
    """Remplace la fabrique PostgreSQL par celle des catalogues fichiers."""
    import scripts.publish_serving as script
    from kedro_pipeline.io.serving import ServingCatalog

    monkeypatch.setattr(
        script,
        "ServingCatalog",
        functools.partial(ServingCatalog, connector_factory=world.factory),
    )


def test_script_publishes_and_exits_zero(tmp_path: Path, script_env, monkeypatch) -> None:
    import scripts.publish_serving as script

    world = build_serving_world(tmp_path)
    _patch_catalog(monkeypatch, world)
    script.main([])
    with world.catalog.connect() as conn:
        assert conn.execute(
            f"SELECT count(*) FROM {world.catalog.qualified_name('cell_scores')}"
        ).fetchone()[0] > 0


def test_script_exits_non_zero_on_failure(tmp_path: Path, script_env, monkeypatch) -> None:
    import scripts.publish_serving as script

    world = build_serving_world(tmp_path, omit=("comext",))
    _patch_catalog(monkeypatch, world)
    with pytest.raises(SystemExit) as info:
        script.main(["--mode", "full"])
    assert info.value.code == 1


def test_script_help_needs_no_environment(capsys) -> None:
    import scripts.publish_serving as script

    with pytest.raises(SystemExit) as info:
        script.main(["--help"])
    assert info.value.code == 0
    assert "serving" in capsys.readouterr().out
