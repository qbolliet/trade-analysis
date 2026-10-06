"""Tests de la commande d'invalidation des empreintes (``scripts/invalidate_freshness.py``).

Le registre de l'étape est remplacé par un registre local fictif (paramètre
``registry_factory`` de ``main``) : validation des noms et des filtres,
aperçu sans écriture, invalidation persistée puis recalcul à la passe suivante,
et construction des registres réels de chaque étape depuis la configuration.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    RegistryEntry,
    Unit,
    units_to_compute,
)
from scripts.invalidate_freshness import (
    STEPS,
    build_registry,
    build_scope,
    main,
    resolve_names,
    step_names,
)

T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
# Empreintes qualifiées par sens de flux, comme celles de l'étape partenaires
REQUESTED = {"HHI/import": "h", "HHI/export": "x", "CDI2/import": "c", "CDI3/import": "d"}
FR = Unit.of(classification="HS2022", reporter="FR", product="280530")
DE = Unit.of(classification="HS2022", reporter="DE", product="854140")


def _registry(tmp_path: Path) -> FreshnessRegistry:
    return FreshnessRegistry(
        f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json", None, "partners",
        shard_of=lambda unit: unit.key,
    )


@pytest.fixture
def factory(tmp_path: Path):
    """Registry factory over a saved partners registry holding FR and DE."""
    registry = _registry(tmp_path)
    for unit in (FR, DE):
        registry.upsert(RegistryEntry(unit, T0, T0, dict(REQUESTED), "first"))
    registry.save()
    return lambda step: _registry(tmp_path)


def _stale(tmp_path: Path):
    return set(units_to_compute([FR, DE], _registry(tmp_path), {}, REQUESTED, ForceSpec(), step="partners"))


def test_main_invalidates_and_next_pass_recomputes(tmp_path: Path, factory) -> None:
    assert main(["--step", "partners", "--metrics", "HHI", "--reporters", "FR"], factory) == 0
    assert _stale(tmp_path) == {FR}


def test_plain_name_invalidates_every_direction_qualified_name_only_one(tmp_path: Path, factory) -> None:
    assert main(["--step", "partners", "--metrics", "HHI/export", "--reporters", "FR"], factory) == 0
    assert set(_registry(tmp_path).get(FR).fingerprints) == {"HHI/import", "CDI2/import", "CDI3/import"}
    assert main(["--step", "partners", "--metrics", "HHI"], factory) == 0
    assert set(_registry(tmp_path).get(DE).fingerprints) == {"CDI2/import", "CDI3/import"}


def test_main_dry_run_writes_nothing(tmp_path: Path, factory) -> None:
    assert main(["--step", "partners", "--metrics", "HHI", "--dry-run"], factory) == 0
    assert _stale(tmp_path) == set()


def test_main_without_names_invalidates_every_fingerprint(tmp_path: Path, factory) -> None:
    assert main(["--step", "partners"], factory) == 0
    assert _stale(tmp_path) == {FR, DE}
    assert _registry(tmp_path).get(DE).fingerprints == {}


@pytest.mark.parametrize(
    "argv",
    [
        ["--step", "partners", "--metrics", "HHHI"],
        ["--step", "partners", "--metrics", "HHI/transit"],
        ["--step", "network", "--reporters", "FR"],
        ["--step", "baci", "--metrics", "HHI"],
        ["--step", "synthesis", "--products", "28"],
    ],
)
def test_main_rejects_unknown_names_and_inapplicable_filters(tmp_path: Path, factory, argv) -> None:
    assert main(argv, factory) == 2
    assert _stale(tmp_path) == set()


def test_step_names_and_resolution() -> None:
    assert step_names("network")[:1] and "SPOF" in step_names("network")
    assert step_names("coherence") == ("coherence",)
    assert resolve_names("synthesis", ["synthesis"]) == {"synthesis"}
    with pytest.raises(ValueError):
        step_names("download")


def test_build_scope_accepts_both_separators() -> None:
    assert build_scope("partners", products="28,8541", reporters="FR;DE").products == ("28", "8541")
    assert build_scope("baci", vintages="HS2017").vintages == ("HS2017",)


@pytest.mark.parametrize("step", STEPS)
def test_build_registry_from_demo_configuration(step: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Configuration du profil demo : chemins des registres sous trade/demo/
    demo = Path("config/profiles/demo")
    monkeypatch.setenv("VULNERABILITIES_CONFIG_PATH", str(demo / "vulnerabilities.yaml"))
    monkeypatch.setenv("EUROSTAT_CONFIG_PATH", str(demo / "eurostat.yaml"))
    monkeypatch.setenv("BACI_CONFIG_PATH", str(demo / "baci.yaml"))
    monkeypatch.setenv("SYNTHESIS_CONFIG_PATH", str(demo / "synthesis.yaml"))
    monkeypatch.setenv("RUNTIME_CONFIG_PATH", str(demo / "runtime.yaml"))
    registry = build_registry(step)
    assert registry.step == step
    assert registry.path_template.startswith("trade/demo/")
