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
    # Environnement demo : chemins des registres sous trade/demo/
    monkeypatch.setenv("KEDRO_ENV", "demo")
    registry = build_registry(step)
    assert registry.step == step
    assert registry.path_template.startswith("trade/demo/")


# ──────────────────────────────────────────────────────────────────────
# Synthèse : empreintes par méthode et registre par contexte
# ──────────────────────────────────────────────────────────────────────


def _synthesis_registry(tmp_path: Path) -> FreshnessRegistry:
    """Registre de synthèse par contexte (un fichier par période)."""
    from scripts.compute_synthetic_scores import context_registry

    return context_registry(
        {"STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/synthesis/{{TIME_PERIOD}}.json"}},
        None, "synthesis", ("hs_vintage", "freq", "flow", "indicators", "TIME_PERIOD"),
    )


def test_synthesis_method_names_are_invalidable(tmp_path: Path) -> None:
    """Une méthode configurée s'invalide seule, sur les contextes du périmètre."""
    from scripts.invalidate_freshness import configured_methods

    method = configured_methods()[0]
    assert step_names("synthesis", [method]) == ("synthesis", "consensus", method)
    assert resolve_names("synthesis", [method], [method]) == {method}
    with pytest.raises(ValueError):
        resolve_names("synthesis", ["not_a_method"], [method])

    # Deux contextes, deux périodes ; invalidation de la méthode sur 2023 seulement
    fingerprints = {method: "m", "consensus": "c", "synthesis": "s"}
    contexts = [
        Unit.of(hs_vintage="HS2022", freq="A", flow="1", indicators="V", TIME_PERIOD=period)
        for period in ("2022", "2023")
    ]
    registry = _synthesis_registry(tmp_path)
    for unit in contexts:
        registry.upsert(RegistryEntry(unit, T0, T0, dict(fingerprints), "first"))
    registry.save()

    argv = ["--step", "synthesis", "--metrics", method, "--periods", "2023"]
    assert main(argv, lambda step: _synthesis_registry(tmp_path)) == 0
    reread = _synthesis_registry(tmp_path)
    assert set(reread.get(contexts[1]).fingerprints) == {"consensus", "synthesis"}
    assert reread.get(contexts[0]).fingerprints == fingerprints
    # Nom inconnu : refusé, rien n'est écrit
    assert main(["--step", "synthesis", "--metrics", "typo"], lambda step: _synthesis_registry(tmp_path)) == 2
