"""Branchement du registre de fraîcheur sur chaque étape (registres fictifs).

Pour les partenaires et le réseau : première passe = tout, deuxième passe =
rien, invalidation de l'empreinte d'une métrique (correction de formule) = tout,
forçage d'un reporter = ce reporter seulement, forçage d'une métrique = tout
puis plus rien. Pour BACI : cadence de réestimation (nouvelle année complète,
révision amont sous ou au-delà de ``MIN_INTERVAL_DAYS``, passe interrompue,
empreinte modifiée ou invalidée). L'amont des partenaires et de BACI est une
marque de données modifiées (``UpstreamMark``), pas une date de téléchargement.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict

import pytest

from kedro_pipeline.io.freshness import (
    ForceSpec,
    RegistryEntry,
    Unit,
    UnitPlan,
    UpstreamMark,
    qualifiers_to_compute,
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
from scripts.compute_trade_vulnerabilities import (
    historical_requested,
    historical_units,
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
    vintage_mark,
    vintage_scopes,
)

# Instants de référence
T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
NOMENCLATURES = {"HS1992": 1988, "HS2017": 2017, "HS2022": 2022}


# Fonction de construction d'une marque de données modifiées (jour, snapshot)
def _mark(days: int = 0, snapshot: int = 1) -> UpstreamMark:
    """Data-modification mark ``days`` after ``T0``, committed at ``snapshot``."""
    return UpstreamMark.of_entry(T0 + timedelta(days=days), snapshot)


# ──────────────────────────────────────────────────────────────────────
# Partenaires
# ──────────────────────────────────────────────────────────────────────


def _partner_block(tmp_path: Path) -> Dict:
    """Partners configuration block pointing to ``tmp_path``."""
    return {
        "BUCKET": None,
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json"},
    }


def _partner_pass(tmp_path: Path, units, requested, force=ForceSpec(), now=T0) -> Dict[Unit, UnitPlan]:
    """Plan, then record as computed and save (one simulated run)."""
    registry = partner_registry(_partner_block(tmp_path))
    plans = plan_partner_units(registry, units, requested, force)
    record_computed_units(registry, plans, units, requested, now)
    registry.save()
    return plans


def _concordance(target_of_854142: str = "854140"):
    """Table de passage HS2022 -> HS2017 : 854141 et 854142 fusionnés dans 854140."""
    import pandas as pd

    return {
        ("HS2022", "HS2017"): pd.DataFrame(
            {"source_code": ["854141", "854142"], "target_code": ["854140", target_of_854142]}
        )
    }


def test_historical_unit_waits_for_a_source_never_downloaded(tmp_path: Path) -> None:
    conversions = {"HS2022": {"854141": "854140", "854142": "854140"}}
    # 854142 jamais écrit pour FR : l'unité attend, aucune somme partielle
    waiting = historical_units({("FR", "854141"): _mark(0, 1)}, conversions, "HS2017")
    assert waiting.watermarks == {} and len(waiting.waiting) == 1
    # Source écrite ensuite : l'unité existe, avec la marque combinée la plus récente
    ready = historical_units({("FR", "854141"): _mark(0, 1), ("FR", "854142"): _mark(1, 2)}, conversions, "HS2017")
    (unit, mark), = ready.watermarks.items()
    assert unit.key == "HS2017|FR|854140" and mark == _mark(1, 2)
    requested = historical_requested(partner_requested(VulnerabilityConfig()), _concordance(), "HS2017", "drop")
    assert {plan.reason for plan in _partner_pass(tmp_path, ready.watermarks, requested).values()} == {"first"}
    # Données d'une seule source modifiées : recalcul « new_data »
    newer = historical_units(
        {("FR", "854141"): _mark(2, 3), ("FR", "854142"): _mark(1, 2)}, conversions, "HS2017"
    )
    replanned = _partner_pass(tmp_path, newer.watermarks, requested, now=T0 + timedelta(days=3))
    assert {plan.reason for plan in replanned.values()} == {"new_data"}


def test_modified_correspondence_table_recomputes_historical_units(tmp_path: Path) -> None:
    units = historical_units(
        {("FR", "854141"): _mark(), ("FR", "854142"): _mark()},
        {"HS2022": {"854141": "854140", "854142": "854140"}},
        "HS2017",
    ).watermarks
    base = partner_requested(VulnerabilityConfig())
    requested = historical_requested(base, _concordance(), "HS2017", "drop")
    _partner_pass(tmp_path, units, requested)
    # Même table : rien à recalculer
    assert _partner_pass(tmp_path, units, requested, now=T0 + timedelta(hours=1)) == {}
    # Table UNSD corrigée : empreinte « concordance » changée, recalcul complet
    corrected = historical_requested(base, _concordance("854149"), "HS2017", "drop")
    plans = _partner_pass(tmp_path, units, corrected, now=T0 + timedelta(hours=2))
    assert {plan.reason for plan in plans.values()} == {"fingerprint"}
    assert all("concordance" in plan.names for plan in plans.values())
    # Fragment rangé sous le millésime historique
    assert (tmp_path / "state" / "HS2017" / "FR.json").exists()


@pytest.fixture
def partner_units_fixture() -> Dict[Unit, UpstreamMark]:
    downloads = {
        ("FR", "280530"): _mark(), ("FR", "854140"): _mark(),
        ("DE", "280530"): _mark(), ("DE", "85414000"): _mark(),
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
    registry = partner_registry(_partner_block(tmp_path))
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
    registry = partner_registry(_partner_block(tmp_path))
    registry.invalidate({"CDI3/export"})
    registry.save()
    plans = _partner_pass(tmp_path, partner_units_fixture, both)
    assert {plan.names for plan in plans.values()} == {frozenset({"CDI3/export"})}


def test_partners_modified_data_recomputes_the_pair(tmp_path: Path, partner_units_fixture) -> None:
    requested = partner_requested(VulnerabilityConfig())
    _partner_pass(tmp_path, partner_units_fixture, requested)
    unit = next(iter(partner_units_fixture))
    plans = _partner_pass(tmp_path, {**partner_units_fixture, unit: _mark(1, 2)}, requested)
    assert plans == {unit: UnitPlan("new_data", frozenset(requested))}


# ──────────────────────────────────────────────────────────────────────
# Réseau
# ──────────────────────────────────────────────────────────────────────


def _baci_config(tmp_path: Path) -> Dict:
    return {
        "BUCKET": None,
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/baci/{{vintage}}.json"},
    }


def _network_config(tmp_path: Path) -> Dict:
    return {
        "BUCKET": None,
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/network/{{vintage}}.json"},
    }


def _write_baci(tmp_path: Path, vintage: str, computed: datetime, complete: bool = True) -> None:
    """Record a BACI pass on a vintage."""
    registry = baci_registry(_baci_config(tmp_path))
    unit = baci_unit(vintage)
    plan = UnitPlan("first", frozenset({"baci"}))
    if complete:
        entry = completed_entry(unit, plan, "fit", [2022, 2023], computed, _mark(), {"baci": "b"})
    else:
        entry = started_entry(registry.get(unit), unit, plan, "fit2", [2022, 2023, 2024])
    registry.upsert(entry)
    registry.save()


def _network_pass(tmp_path: Path, requested, force=ForceSpec(), now=T0) -> Dict[Unit, UnitPlan]:
    targets = {"HS2017": "baci_hs2017", "HS2022": "baci_hs2022"}
    units = network_upstream(baci_registry(_baci_config(tmp_path)), targets)
    registry = network_registry(_network_config(tmp_path))
    plans = plan_network_units(registry, units, requested, force)
    # Enregistrement du calcul : le dernier calcul BACI du millésime devient le watermark amont
    for unit, plan in plans.items():
        registry.upsert(RegistryEntry(
            unit=unit, last_computed=now, upstream_watermark=units[unit],
            fingerprints=dict(requested), reason=plan.reason,
        ))
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


def _baci_plan(tmp_path: Path, scope, mark, now, requested=None, force=ForceSpec(), refresh=REFRESH):
    registry = baci_registry(_baci_config(tmp_path))
    requested = requested or baci_requested(BACI_DEFAULT_CONFIG)
    return plan_baci_vintages(
        registry, {"HS2017": scope}, {"HS2017": mark}, requested, force, refresh, now
    ), registry, requested


def _complete_pass(tmp_path: Path, scope, mark, now, requested=None) -> None:
    plans, registry, requested = _baci_plan(tmp_path, scope, mark, now, requested)
    for unit, plan in plans.items():
        fit = compute_fit_id("HS2017", scope, mark, requested)
        registry.upsert(started_entry(registry.get(unit), unit, plan, fit, scope))
        registry.upsert(completed_entry(unit, plan, fit, scope, now, mark, requested))
    registry.save()


@pytest.mark.parametrize(
    "case, scope, mark, days_later, expected",
    [
        ("rien de neuf", [2022, 2023], _mark(0, 1), 30, None),
        ("nouvelle année complète", [2022, 2023, 2024], _mark(1, 2), 1, "new_data"),
        ("données modifiées sous l'intervalle", [2022, 2023], _mark(1, 2), 3, None),
        ("données modifiées au-delà de l'intervalle", [2022, 2023], _mark(1, 2), 8, "new_data"),
    ],
)
def test_baci_cadence(tmp_path: Path, case, scope, mark, days_later, expected) -> None:
    _complete_pass(tmp_path, [2022, 2023], _mark(0, 1), T0)
    plans, _, _ = _baci_plan(tmp_path, scope, mark, T0 + timedelta(days=days_later))
    reason = plans[baci_unit("HS2017")].reason if plans else None
    assert reason == expected, case


def test_baci_new_complete_year_waits_when_disabled(tmp_path: Path) -> None:
    _complete_pass(tmp_path, [2022, 2023], _mark(), T0)
    refresh = {"MIN_INTERVAL_DAYS": 7, "ON_NEW_COMPLETE_YEAR": False}
    plans, _, _ = _baci_plan(tmp_path, [2022, 2023, 2024], _mark(), T0 + timedelta(days=1), refresh=refresh)
    assert plans == {}
    plans, _, _ = _baci_plan(tmp_path, [2022, 2023, 2024], _mark(), T0 + timedelta(days=7), refresh=refresh)
    assert plans[baci_unit("HS2017")].reason == "new_data"


def test_baci_interrupted_pass_is_resumed(tmp_path: Path) -> None:
    _complete_pass(tmp_path, [2022, 2023], _mark(), T0)
    plans, registry, requested = _baci_plan(tmp_path, [2022, 2023, 2024], _mark(), T0 + timedelta(days=1))
    unit = baci_unit("HS2017")
    # Entrée « démarrée » écrite, puis interruption avant l'entrée terminée
    registry.upsert(started_entry(registry.get(unit), unit, plans[unit], "fit", [2022, 2023, 2024]))
    registry.save()
    entry = baci_registry(_baci_config(tmp_path)).get(unit)
    assert entry.extra["years_written"] == [] and entry.last_computed == T0
    plans, _, _ = _baci_plan(tmp_path, [2022, 2023, 2024], _mark(), T0 + timedelta(days=2))
    assert plans[unit].reason == "new_data"


def test_baci_first_fingerprint_and_force(tmp_path: Path) -> None:
    plans, _, _ = _baci_plan(tmp_path, [2022], _mark(), T0)
    assert plans[baci_unit("HS2017")].reason == "first"
    _complete_pass(tmp_path, [2022], _mark(), T0)
    changed = baci_requested(replace(BACI_DEFAULT_CONFIG, cook_factor=3.0))
    plans, _, _ = _baci_plan(tmp_path, [2022], _mark(), T0 + timedelta(days=1), requested=changed)
    assert plans[baci_unit("HS2017")].reason == "fingerprint"
    # Forçage : un filtre sur les périodes n'a pas d'effet (millésime entier)
    force = ForceSpec(steps=frozenset({"baci"}), vintages=("HS2017",), periods=("2020",))
    plans, _, _ = _baci_plan(tmp_path, [2022], _mark(), T0 + timedelta(days=1), force=force)
    assert plans[baci_unit("HS2017")].reason == "forced"


def test_baci_scopes_and_mark() -> None:
    assert vintage_scopes([2016, 2017, 2023], {"HS2022": 2022, "HS2017": 2017, "HS2012": 2024}) == {
        "HS2022": [2023], "HS2017": [2017, 2023],
    }
    marks = {2022: {("a",): _mark(0, 1)}, 2023: {("a",): _mark(2, 5), ("b",): _mark(0, 2)}}
    assert vintage_mark(marks, [2022, 2023]) == _mark(2, 5)
    assert vintage_mark(marks, [2019]) is None


def test_baci_invalidation_bypasses_the_cadence(tmp_path: Path) -> None:
    _complete_pass(tmp_path, [2022], _mark(), T0)
    # Correction d'une étape BACI : invalidation du millésime, passe dès le lendemain
    registry = baci_registry(_baci_config(tmp_path))
    assert registry.invalidate(scope=ForceSpec(vintages=("HS2017",))) == [baci_unit("HS2017")]
    registry.save()
    plans, _, _ = _baci_plan(tmp_path, [2022], _mark(), T0 + timedelta(days=1))
    assert plans[baci_unit("HS2017")].reason == "fingerprint"
