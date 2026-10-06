"""Branchement du registre de fraîcheur v2 sur chaque étape (registres fictifs).

Pour les partenaires et le réseau : première passe = tout, deuxième passe =
rien, invalidation de l'empreinte d'une métrique (correction de formule) = tout,
forçage d'un reporter = ce reporter seulement, forçage d'une métrique = tout
puis plus rien. Pour BACI : cadence de réestimation (nouvelle année complète,
révision amont sous ou au-delà de ``MIN_INTERVAL_DAYS``, passe interrompue,
empreinte modifiée ou invalidée). Pour la synthèse et la cohérence : unité
globale, ``FORCE`` historique et ``FORCE_STEPS``, invalidation, lecture d'un
registre v1.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict

import pytest

from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    RegistryEntry,
    Unit,
    UnitPlan,
    qualifiers_to_compute,
    summarize_upstream,
)
from macroforecast.trade.processing import DEFAULT_CONFIG as BACI_DEFAULT_CONFIG
from macroforecast.trade.vulnerabilities import (
    NetworkVulnerabilityConfig,
    VulnerabilityConfig,
)
from scripts.compute_network_vulnerabilities import (
    network_registry,
    network_requested,
    network_upstream,
    plan_network_units,
)
from scripts.compute_synthetic_scores import (
    GLOBAL_UNIT,
    global_entry,
    global_registry,
    plan_global_unit,
    synthesis_requested,
)
from scripts.compute_trade_vulnerabilities import (
    partner_classification,
    partner_registry,
    partner_requested,
    partner_units,
    plan_partner_units,
    record_computed_units,
)
from scripts.process_baci_hs import (
    baci_registry,
    baci_requested,
    baci_unit,
    completed_entry,
    compute_fit_id,
    plan_baci_vintages,
    started_entry,
    vintage_scopes,
    vintage_watermark,
)

# Instants de référence
T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
NOMENCLATURES = {"HS1992": 1988, "HS2017": 2017, "HS2022": 2022}


# ──────────────────────────────────────────────────────────────────────
# Partenaires
# ──────────────────────────────────────────────────────────────────────


def _partner_block(tmp_path: Path) -> Dict:
    """Partners configuration block pointing to ``tmp_path``."""
    return {
        "BUCKET": None,
        "PATHS": {"LAST_COMPUTATION_PATH": f"{tmp_path.as_posix()}/v1/last_computation.json"},
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json"},
    }


def _partner_pass(tmp_path: Path, units, requested, force=ForceSpec(), now=T0) -> Dict[Unit, UnitPlan]:
    """Plan, then record as computed and save (one simulated run)."""
    registry = partner_registry(_partner_block(tmp_path), "HS2022")
    plans = plan_partner_units(registry, units, requested, force)
    record_computed_units(registry, plans, units, requested, now)
    registry.save()
    return plans


@pytest.fixture
def partner_units_fixture() -> Dict[Unit, datetime]:
    downloads = {
        ("FR", "280530"): T0, ("FR", "854140"): T0,
        ("DE", "280530"): T0, ("DE", "85414000"): T0,
    }
    return partner_units(downloads, partner_classification(NOMENCLATURES))


def test_partner_classification_is_latest_vintage_for_every_code(partner_units_fixture) -> None:
    assert {unit.get("classification") for unit in partner_units_fixture} == {"HS2022"}


def test_partners_first_pass_all_second_pass_nothing(tmp_path: Path, partner_units_fixture) -> None:
    requested = partner_requested(VulnerabilityConfig())
    first = _partner_pass(tmp_path, partner_units_fixture, requested)
    assert set(first) == set(partner_units_fixture)
    assert {plan.reason for plan in first.values()} == {"first"}
    assert _partner_pass(tmp_path, partner_units_fixture, requested, now=T0 + timedelta(hours=1)) == {}
    # Un fragment par classification x reporter
    assert sorted(p.name for p in (tmp_path / "state" / "HS2022").iterdir()) == ["DE.json", "FR.json"]


def test_partners_invalidated_metric_recomputes_everything_once(tmp_path: Path, partner_units_fixture) -> None:
    requested = partner_requested(VulnerabilityConfig())
    _partner_pass(tmp_path, partner_units_fixture, requested)
    # Correction de la formule de HHI : invalidation de son empreinte partout
    registry = partner_registry(_partner_block(tmp_path), "HS2022")
    assert len(registry.invalidate({"HHI"})) == len(partner_units_fixture)
    registry.save()
    plans = _partner_pass(tmp_path, partner_units_fixture, requested)
    assert set(plans) == set(partner_units_fixture)
    assert {plan for plan in plans.values()} == {UnitPlan("fingerprint", frozenset({"HHI/import"}))}
    # Empreintes réenregistrées : la passe suivante ne recalcule rien
    assert _partner_pass(tmp_path, partner_units_fixture, requested) == {}


def test_partners_alert_threshold_change_recomputes_but_diagnostic_option_does_not(
    tmp_path: Path, partner_units_fixture
) -> None:
    base = VulnerabilityConfig()
    _partner_pass(tmp_path, partner_units_fixture, partner_requested(base))
    assert _partner_pass(tmp_path, partner_units_fixture, partner_requested(replace(base, artifact_top_n=5))) == {}
    changed = replace(base, metric_alert_thresholds=(("HHI", 0.4), ("CDI2", 0.5), ("CDI3", 1.0)))
    assert len(_partner_pass(tmp_path, partner_units_fixture, partner_requested(changed))) == 4


def test_partners_force_reporter_only_that_reporter(tmp_path: Path, partner_units_fixture) -> None:
    requested = partner_requested(VulnerabilityConfig())
    _partner_pass(tmp_path, partner_units_fixture, requested)
    force = ForceSpec.from_runtime({"FORCE_STEPS": "partners"}, environ={"FORCE_REPORTERS": "FR"})
    plans = _partner_pass(tmp_path, partner_units_fixture, requested, force)
    assert {unit.get("reporter") for unit in plans} == {"FR"}
    assert {plan.reason for plan in plans.values()} == {"forced"}


def test_partners_force_metric_then_nothing(tmp_path: Path, partner_units_fixture) -> None:
    requested = partner_requested(VulnerabilityConfig())
    _partner_pass(tmp_path, partner_units_fixture, requested)
    # FORCE_METRICS seul : l'étape qui calcule HHI est forcée
    force = ForceSpec.from_runtime({"FORCE_METRICS": "HHI"}, environ={})
    plans = _partner_pass(tmp_path, partner_units_fixture, requested, force)
    assert set(plans) == set(partner_units_fixture)
    assert {plan for plan in plans.values()} == {UnitPlan("forced", frozenset({"HHI/import"}))}
    # Empreintes inchangées : la passe suivante, sans forçage, ne recalcule rien
    assert _partner_pass(tmp_path, partner_units_fixture, requested) == {}


def test_partners_adding_export_plans_only_the_export_direction(
    tmp_path: Path, partner_units_fixture
) -> None:
    config = VulnerabilityConfig()
    _partner_pass(tmp_path, partner_units_fixture, partner_requested(config, ("import",)))
    both = partner_requested(config, ("import", "export"))
    plans = _partner_pass(tmp_path, partner_units_fixture, both)
    assert set(plans) == set(partner_units_fixture)
    assert {plan for plan in plans.values()} == {
        UnitPlan("fingerprint", frozenset({"HHI/export", "CDI2/export", "CDI3/export"}))
    }
    assert qualifiers_to_compute(plans, ("import", "export")) == ("export",)
    # Les deux sens à jour : plus rien ; forcer HHI couvre ses deux sens
    assert _partner_pass(tmp_path, partner_units_fixture, both) == {}
    forced = _partner_pass(tmp_path, partner_units_fixture, both, ForceSpec(metrics=("HHI",)))
    assert {plan.names for plan in forced.values()} == {frozenset({"HHI/import", "HHI/export"})}
    assert qualifiers_to_compute(forced, ("import", "export")) == ("import", "export")


def test_partners_invalidating_one_direction(tmp_path: Path, partner_units_fixture) -> None:
    both = partner_requested(VulnerabilityConfig(), ("import", "export"))
    _partner_pass(tmp_path, partner_units_fixture, both)
    registry = partner_registry(_partner_block(tmp_path), "HS2022")
    registry.invalidate({"CDI3/export"})
    registry.save()
    plans = _partner_pass(tmp_path, partner_units_fixture, both)
    assert {plan.names for plan in plans.values()} == {frozenset({"CDI3/export"})}


def test_partners_new_download_recomputes_the_pair(tmp_path: Path, partner_units_fixture) -> None:
    requested = partner_requested(VulnerabilityConfig())
    _partner_pass(tmp_path, partner_units_fixture, requested)
    unit = next(iter(partner_units_fixture))
    plans = _partner_pass(tmp_path, {**partner_units_fixture, unit: T0 + timedelta(days=1)}, requested)
    assert plans == {unit: UnitPlan("new_data", frozenset(requested))}


def test_partners_v1_registry_migration(tmp_path: Path, partner_units_fixture) -> None:
    block = _partner_block(tmp_path)
    v1 = Path(block["PATHS"]["LAST_COMPUTATION_PATH"])
    v1.parent.mkdir(parents=True)
    v1.write_text(json.dumps({"VULNERABILITIES": {
        f"{u.get('reporter')}|{u.get('product')}": {
            "reporter": u.get("reporter"), "product": u.get("product"), "last_computed": T0.isoformat()}
        for u in partner_units_fixture}}), encoding="utf-8")
    requested = partner_requested(VulnerabilityConfig())
    # Sans adoption : empreintes manquantes → tout recalculer
    registry = partner_registry(block, "HS2022")
    plans = plan_partner_units(registry, partner_units_fixture, requested, ForceSpec())
    assert {plan.reason for plan in plans.values()} == {"fingerprint"} and len(plans) == 4
    # Avec adoption : rien à recalculer, entrées réécrites en fragments v2
    registry = partner_registry(block, "HS2022")
    assert plan_partner_units(
        registry, partner_units_fixture, requested, ForceSpec(), adopt_legacy_fingerprints=True
    ) == {}
    assert len(registry.save()) == 2
    v1.unlink()
    assert plan_partner_units(partner_registry(block, "HS2022"), partner_units_fixture, requested, ForceSpec()) == {}


# ──────────────────────────────────────────────────────────────────────
# Réseau
# ──────────────────────────────────────────────────────────────────────


def _baci_config(tmp_path: Path) -> Dict:
    return {
        "BUCKET": None,
        "PATHS": {"LAST_PROCESSING_PATH": f"{tmp_path.as_posix()}/v1/baci.json"},
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/baci/{{vintage}}.json"},
    }


def _network_config(tmp_path: Path) -> Dict:
    return {
        "BUCKET": None,
        "PATHS": {"LAST_COMPUTATION_PATH": f"{tmp_path.as_posix()}/v1/network.json"},
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/network/{{vintage}}.json"},
    }


def _write_baci(tmp_path: Path, vintage: str, computed: datetime, complete: bool = True) -> None:
    """Record a BACI pass on a vintage."""
    registry = baci_registry(_baci_config(tmp_path))
    unit = baci_unit(vintage)
    plan = UnitPlan("first", frozenset({"baci"}))
    if complete:
        entry = completed_entry(unit, plan, "fit", [2022, 2023], computed, T0, {"baci": "b"})
    else:
        entry = started_entry(registry.get(unit), unit, plan, "fit2", [2022, 2023, 2024])
    registry.upsert(entry)
    registry.save()


def _network_pass(tmp_path: Path, requested, force=ForceSpec(), now=T0) -> Dict[Unit, UnitPlan]:
    targets = {"HS2017": "baci_hs2017", "HS2022": "baci_hs2022"}
    units = network_upstream(baci_registry(_baci_config(tmp_path)), targets)
    registry = network_registry(_network_config(tmp_path))
    plans = plan_network_units(registry, units, requested, force)
    record_computed_units(registry, plans, units, requested, now)
    registry.save()
    return plans


def test_network_first_second_invalidation_and_force(tmp_path: Path) -> None:
    _write_baci(tmp_path, "HS2017", T0)
    _write_baci(tmp_path, "HS2022", T0)
    requested = network_requested(NetworkVulnerabilityConfig())
    assert {plan.reason for plan in _network_pass(tmp_path, requested).values()} == {"first"}
    assert _network_pass(tmp_path, requested) == {}
    # Correction de la formule du diamètre : invalidation sur tous les millésimes
    registry = network_registry(_network_config(tmp_path))
    registry.invalidate({"DIAMETER"})
    registry.save()
    assert len(_network_pass(tmp_path, requested)) == 2
    # Forçage d'un millésime seulement
    force = ForceSpec(steps=frozenset({"network"}), vintages=("HS2017",))
    assert [unit.get("vintage") for unit in _network_pass(tmp_path, requested, force)] == ["HS2017"]


def test_network_adding_export_plans_only_the_export_direction(tmp_path: Path) -> None:
    _write_baci(tmp_path, "HS2017", T0)
    config = NetworkVulnerabilityConfig()
    _network_pass(tmp_path, network_requested(config, ("import",)))
    plans = _network_pass(tmp_path, network_requested(config, ("import", "export")))
    assert [unit.get("vintage") for unit in plans] == ["HS2017"]
    names = next(iter(plans.values())).names
    assert names == {f"{name}/export" for name in (
        "CENTRALITY_RISK", "CLUSTERING_W", "DIAMETER", "WORLD_HHI", "SPOF", "SPOF_DECILE"
    )}
    assert qualifiers_to_compute(plans, ("import", "export")) == ("export",)


def test_network_follows_baci_and_skips_incomplete_pass(tmp_path: Path) -> None:
    _write_baci(tmp_path, "HS2017", T0)
    _write_baci(tmp_path, "HS2022", T0)
    requested = network_requested(NetworkVulnerabilityConfig())
    _network_pass(tmp_path, requested)
    # Nouvelle passe BACI terminée sur HS2017 : seul HS2017 est recalculé
    _write_baci(tmp_path, "HS2017", T0 + timedelta(days=7))
    assert [unit.get("vintage") for unit in _network_pass(tmp_path, requested)] == ["HS2017"]
    # Passe BACI interrompue sur HS2022 : le millésime n'est pas scoré (table mixte)
    _write_baci(tmp_path, "HS2022", T0 + timedelta(days=8), complete=False)
    units = network_upstream(baci_registry(_baci_config(tmp_path)), {"HS2022": "baci_hs2022"})
    assert units == {}


# ──────────────────────────────────────────────────────────────────────
# BACI
# ──────────────────────────────────────────────────────────────────────

REFRESH = {"MIN_INTERVAL_DAYS": 7, "ON_NEW_COMPLETE_YEAR": True}


def _baci_plan(tmp_path: Path, scope, watermark, now, requested=None, force=ForceSpec(), refresh=REFRESH):
    registry = baci_registry(_baci_config(tmp_path))
    requested = requested or baci_requested(BACI_DEFAULT_CONFIG)
    return plan_baci_vintages(
        registry, {"HS2017": scope}, {"HS2017": watermark}, requested, force, refresh, now
    ), registry, requested


def _complete_pass(tmp_path: Path, scope, watermark, now, requested=None) -> None:
    plans, registry, requested = _baci_plan(tmp_path, scope, watermark, now, requested)
    for unit, plan in plans.items():
        fit = compute_fit_id("HS2017", scope, watermark, requested)
        registry.upsert(started_entry(registry.get(unit), unit, plan, fit, scope))
        registry.upsert(completed_entry(unit, plan, fit, scope, now, watermark, requested))
    registry.save()


@pytest.mark.parametrize(
    "case, scope, watermark, days_later, expected",
    [
        ("rien de neuf", [2022, 2023], T0, 30, None),
        ("nouvelle année complète", [2022, 2023, 2024], T0 + timedelta(days=1), 1, "new_data"),
        ("révision amont sous l'intervalle", [2022, 2023], T0 + timedelta(days=1), 3, None),
        ("révision amont au-delà de l'intervalle", [2022, 2023], T0 + timedelta(days=1), 8, "new_data"),
    ],
)
def test_baci_cadence(tmp_path: Path, case, scope, watermark, days_later, expected) -> None:
    _complete_pass(tmp_path, [2022, 2023], T0, T0)
    plans, _, _ = _baci_plan(tmp_path, scope, watermark, T0 + timedelta(days=days_later))
    reason = plans[baci_unit("HS2017")].reason if plans else None
    assert reason == expected, case


def test_baci_new_complete_year_waits_when_disabled(tmp_path: Path) -> None:
    _complete_pass(tmp_path, [2022, 2023], T0, T0)
    refresh = {"MIN_INTERVAL_DAYS": 7, "ON_NEW_COMPLETE_YEAR": False}
    plans, _, _ = _baci_plan(tmp_path, [2022, 2023, 2024], T0, T0 + timedelta(days=1), refresh=refresh)
    assert plans == {}
    plans, _, _ = _baci_plan(tmp_path, [2022, 2023, 2024], T0, T0 + timedelta(days=7), refresh=refresh)
    assert plans[baci_unit("HS2017")].reason == "new_data"


def test_baci_interrupted_pass_is_resumed(tmp_path: Path) -> None:
    _complete_pass(tmp_path, [2022, 2023], T0, T0)
    plans, registry, requested = _baci_plan(tmp_path, [2022, 2023, 2024], T0, T0 + timedelta(days=1))
    unit = baci_unit("HS2017")
    # Entrée « démarrée » écrite, puis interruption avant l'entrée terminée
    registry.upsert(started_entry(registry.get(unit), unit, plans[unit], "fit", [2022, 2023, 2024]))
    registry.save()
    entry = baci_registry(_baci_config(tmp_path)).get(unit)
    assert entry.extra["years_written"] == [] and entry.last_computed == T0
    plans, _, _ = _baci_plan(tmp_path, [2022, 2023, 2024], T0, T0 + timedelta(days=2))
    assert plans[unit].reason == "new_data"


def test_baci_first_fingerprint_and_force(tmp_path: Path) -> None:
    plans, _, _ = _baci_plan(tmp_path, [2022], T0, T0)
    assert plans[baci_unit("HS2017")].reason == "first"
    _complete_pass(tmp_path, [2022], T0, T0)
    changed = baci_requested(replace(BACI_DEFAULT_CONFIG, cook_factor=3.0))
    plans, _, _ = _baci_plan(tmp_path, [2022], T0, T0 + timedelta(days=1), requested=changed)
    assert plans[baci_unit("HS2017")].reason == "fingerprint"
    # Forçage : un filtre sur les périodes n'a pas d'effet (millésime entier)
    force = ForceSpec(steps=frozenset({"baci"}), vintages=("HS2017",), periods=("2020",))
    plans, _, _ = _baci_plan(tmp_path, [2022], T0, T0 + timedelta(days=1), force=force)
    assert plans[baci_unit("HS2017")].reason == "forced"


def test_baci_scopes_and_watermark() -> None:
    assert vintage_scopes([2016, 2017, 2023], {"HS2022": 2022, "HS2017": 2017, "HS2012": 2024}) == {
        "HS2022": [2023], "HS2017": [2017, 2023],
    }
    batches = {2022: {("a",): T0}, 2023: {("a",): T0 + timedelta(days=2), ("b",): T0}}
    assert vintage_watermark(batches, [2022, 2023]) == T0 + timedelta(days=2)
    assert vintage_watermark(batches, [2019]) is None


def test_baci_v1_registry_is_read(tmp_path: Path) -> None:
    config = _baci_config(tmp_path)
    v1 = Path(config["PATHS"]["LAST_PROCESSING_PATH"])
    v1.parent.mkdir(parents=True)
    v1.write_text(json.dumps({"BACI": {"baci_hs2017": {
        "vintage": "HS2017", "result_schema": "baci_hs2017",
        "last_processed": T0.isoformat(), "n_rows": 10}}}), encoding="utf-8")
    entry = baci_registry(config).get(baci_unit("HS2017"))
    assert entry.legacy and entry.last_computed == T0 and entry.extra["n_rows"] == 10


# ──────────────────────────────────────────────────────────────────────
# Synthèse et cohérence (unité globale)
# ──────────────────────────────────────────────────────────────────────


def _global(tmp_path: Path, name: str = "synthesis.json") -> FreshnessRegistry:
    return global_registry(tmp_path / name, None, "synthesis", "SYNTHESIS")


@pytest.mark.parametrize(
    "yaml_force, force, expected",
    [
        (False, ForceSpec(), None),
        (True, ForceSpec(), "forced"),
        (False, ForceSpec(steps=frozenset({"synthesis"})), "forced"),
        (False, ForceSpec(methods=("critic_sum",)), None),
    ],
)
def test_synthesis_global_unit_forcing(tmp_path: Path, yaml_force, force, expected) -> None:
    from macroforecast.trade.aggregation import SynthesisConfig

    requested = synthesis_requested(SynthesisConfig())
    registry = _global(tmp_path)
    summary = summarize_upstream([RegistryEntry(Unit.of(vintage="HS2017"), T0, reason="new_data")])
    registry.upsert(global_entry(UnitPlan("first", frozenset()), T0, summary, requested))
    registry.save()
    plans = plan_global_unit(_global(tmp_path), T0, requested, force, step="synthesis", yaml_force=yaml_force)
    assert (plans[GLOBAL_UNIT].reason if plans else None) == expected


def test_synthesis_upstream_new_data_and_method_change(tmp_path: Path) -> None:
    from macroforecast.trade.aggregation import MethodSpec, SynthesisConfig

    requested = synthesis_requested(SynthesisConfig())
    registry = _global(tmp_path)
    assert plan_global_unit(registry, None, requested, ForceSpec(), step="synthesis") == {}
    assert plan_global_unit(registry, T0, requested, ForceSpec(), step="synthesis")[GLOBAL_UNIT].reason == "first"
    summary = summarize_upstream([RegistryEntry(Unit.of(v="a"), T0, reason="new_data")])
    registry.upsert(global_entry(UnitPlan("first", frozenset()), T0, summary, requested))
    registry.save()
    later = T0 + timedelta(days=1)
    assert plan_global_unit(_global(tmp_path), later, requested, ForceSpec(), step="synthesis")[
        GLOBAL_UNIT].reason == "new_data"
    added = synthesis_requested(SynthesisConfig(methods=(MethodSpec(name="rank_mean", kind="rank_mean"),)))
    assert plan_global_unit(_global(tmp_path), T0, added, ForceSpec(), step="synthesis")[
        GLOBAL_UNIT].reason == "fingerprint"


def test_synthesis_v1_registry_read_and_overwritten_in_place(tmp_path: Path) -> None:
    from macroforecast.trade.aggregation import SynthesisConfig

    path = tmp_path / "synthesis.json"
    path.write_text(json.dumps({"SYNTHESIS": {"last_computed": T0.isoformat(), "n_cells": 3}}), encoding="utf-8")
    requested = synthesis_requested(SynthesisConfig())
    registry = _global(tmp_path)
    entry = registry.get(GLOBAL_UNIT)
    assert entry.legacy and entry.extra["n_cells"] == 3
    # Amont antérieur à la synthèse v1 : seule l'empreinte manquante déclenche…
    assert plan_global_unit(registry, T0, requested, ForceSpec(), step="synthesis")[
        GLOBAL_UNIT].reason == "fingerprint"
    # … sauf adoption au déploiement
    assert plan_global_unit(
        registry, T0, requested, ForceSpec(), step="synthesis", adopt_legacy_fingerprints=True
    ) == {}
    registry.save()
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == 2


def test_global_entry_exposes_upstream_reasons_for_cascade() -> None:
    summary = summarize_upstream(
        [RegistryEntry(Unit.of(v="a"), T0, reason="fingerprint")], since=T0 - timedelta(days=1)
    )
    entry = global_entry(UnitPlan("new_data", frozenset()), T0, summary, {"synthesis": "x"}, n_cells=1)
    assert entry.extra["upstream_reasons"]["full_change"] is True
    assert entry.upstream_watermark == T0


def test_baci_invalidation_bypasses_the_cadence(tmp_path: Path) -> None:
    _complete_pass(tmp_path, [2022], T0, T0)
    # Correction d'une étape BACI : invalidation du millésime, passe dès le lendemain
    registry = baci_registry(_baci_config(tmp_path))
    assert registry.invalidate(scope=ForceSpec(vintages=("HS2017",))) == [baci_unit("HS2017")]
    registry.save()
    plans, _, _ = _baci_plan(tmp_path, [2022], T0, T0 + timedelta(days=1))
    assert plans[baci_unit("HS2017")].reason == "fingerprint"


def test_synthesis_invalidation_recomputes_global_unit(tmp_path: Path) -> None:
    from macroforecast.trade.aggregation import SynthesisConfig

    requested = synthesis_requested(SynthesisConfig())
    registry = _global(tmp_path)
    summary = summarize_upstream([RegistryEntry(Unit.of(v="a"), T0, reason="new_data")])
    registry.upsert(global_entry(UnitPlan("first", frozenset()), T0, summary, requested))
    registry.save()
    assert plan_global_unit(_global(tmp_path), T0, requested, ForceSpec(), step="synthesis") == {}
    registry = _global(tmp_path)
    registry.invalidate()
    registry.save()
    assert plan_global_unit(_global(tmp_path), T0, requested, ForceSpec(), step="synthesis")[
        GLOBAL_UNIT].reason == "fingerprint"
