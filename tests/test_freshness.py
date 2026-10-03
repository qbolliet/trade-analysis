"""Tests du module générique de fraîcheur ``kedro_pipeline.io.freshness``.

Couvre l'empreinte méthodologique, les unités, le forçage (séparateurs, priorité
de l'environnement, périmètre), la décision ``units_to_compute`` (priorités
first > forced > new_data > fingerprint), le registre fragmenté (chargement
paresseux, écriture des seuls fragments modifiés, registres v1, adoption des
empreintes, lecture récursive locale et S3) et la lecture aval.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

import pytest

from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    LegacySource,
    RegistryEntry,
    Unit,
    UnitPlan,
    adopt_legacy_flag,
    fingerprint,
    legacy_entry,
    parse_force_list,
    parse_instant,
    plan_metrics,
    summarize_upstream,
    units_to_compute,
)

# Instants de référence
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T1 = T0 + timedelta(days=1)
T2 = T0 + timedelta(days=2)

# Empreintes demandées de référence
REQUESTED = {"HHI": "h1", "CDI2": "c1"}

# Unités de référence
FR = Unit.of(classification="HS2022", reporter="FR", product="280530")
DE = Unit.of(classification="HS2022", reporter="DE", product="854140")


# Fonction de construction d'un registre partenaires local
def _registry(tmp_path: Path, legacy: Optional[LegacySource] = None) -> FreshnessRegistry:
    """Build a partners-like registry under ``tmp_path``."""
    return FreshnessRegistry(
        f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json",
        None,
        "partners",
        shard_of=lambda unit: f"{unit.get('classification')}/{unit.get('reporter')}",
        legacy=legacy,
    )


# Fonction de construction d'une entrée calculée
def _entry(unit: Unit, fingerprints: Dict[str, str] = REQUESTED, watermark=T0, computed=T1) -> RegistryEntry:
    """Build a computed entry."""
    return RegistryEntry(unit, computed, watermark, dict(fingerprints), "new_data")


# ──────────────────────────────────────────────────────────────────────
# Empreinte et unités
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "left, right, equal",
    [
        (("HHI", {"a": 1, "b": 2}), ("HHI", {"b": 2, "a": 1}), True),
        (("HHI", {"a": 1}), ("HHI", {"a": 2}), False),
        (("HHI", {"a": 1}), ("CDI2", {"a": 1}), False),
        (("HHI", {"a": [1, 2]}), ("HHI", {"a": [1, 2]}), True),
    ],
)
def test_fingerprint_stability_and_sensitivity(left, right, equal) -> None:
    assert (fingerprint(*left) == fingerprint(*right)) is equal
    assert len(fingerprint(*left)) == 16


def test_unit_hash_key_and_roundtrip() -> None:
    assert FR == Unit.of(classification="HS2022", reporter="FR", product="280530")
    assert len({FR, Unit.from_mapping(FR.as_dict())}) == 1
    assert FR.key == "HS2022|FR|280530"
    assert Unit.of(year=2020).get("year") == "2020"
    with pytest.raises(ValueError):
        Unit.of()


def test_entry_json_roundtrip_keeps_extra_at_top_level() -> None:
    entry = RegistryEntry(Unit.of(vintage="HS2017"), T1, T0, {"baci": "x"}, "forced",
                          {"fit_id": "f", "years_written": [2017]})
    payload = entry.to_json()
    assert payload["fit_id"] == "f" and "extra" not in payload
    assert RegistryEntry.from_json(payload) == entry


# ──────────────────────────────────────────────────────────────────────
# Forçage
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value, expected",
    [
        ("FR,DE", ("FR", "DE")),
        ("FR;DE", ("FR", "DE")),
        (" FR ; DE , IT ", ("FR", "DE", "IT")),
        (["FR", "DE;IT"], ("FR", "DE", "IT")),
        (2020, ("2020",)),
        ("", ()),
        (None, ()),
        ("FR,FR", ("FR",)),
    ],
)
def test_parse_force_list(value, expected) -> None:
    assert parse_force_list(value) == expected


def test_force_spec_from_runtime_reads_yaml_and_env_overrides() -> None:
    runtime = {
        "runtime": {
            "FORCE_STEPS": "partners",
            "FORCE_METRICS": "",
            "FORCE_SCOPE": {"REPORTERS": "DE", "PRODUCTS": "", "PERIODS": "", "VINTAGES": ""},
        }
    }
    spec = ForceSpec.from_runtime(runtime, environ={"FORCE_REPORTERS": "FR;IT", "FORCE_STEPS": " "})
    assert spec.steps == {"partners"}
    assert spec.reporters == ("FR", "IT")
    # Paramètres internes acceptés tels quels (sans clé racine « runtime »)
    assert ForceSpec.from_runtime({"FORCE_STEPS": "ALL"}, environ={}).steps == {"all"}
    assert ForceSpec.from_runtime(None, environ={}).is_empty


@pytest.mark.parametrize(
    "spec, step, unit, expected",
    [
        (ForceSpec(steps=frozenset({"all"})), "network", Unit.of(vintage="HS2017"), True),
        (ForceSpec(steps=frozenset({"partners"})), "network", Unit.of(vintage="HS2017"), False),
        (ForceSpec(steps=frozenset({"partners"}), reporters=("FR",)), "partners", FR, True),
        (ForceSpec(steps=frozenset({"partners"}), reporters=("FR",)), "partners", DE, False),
        (ForceSpec(steps=frozenset({"partners"}), products=("85",)), "partners", DE, True),
        (ForceSpec(steps=frozenset({"partners"}), products=("85",)), "partners", FR, False),
        # Dimension absente de l'unité : filtre ignoré (BACI réestime le millésime entier)
        (ForceSpec(steps=frozenset({"baci"}), periods=("2020",)), "baci", Unit.of(vintage="HS2017"), True),
        (ForceSpec(steps=frozenset({"baci"}), vintages=("HS2022",)), "baci", Unit.of(vintage="HS2017"), False),
        (ForceSpec(steps=frozenset({"partners"}), vintages=("HS2022",)), "partners", FR, True),
    ],
)
def test_force_spec_covers(spec, step, unit, expected) -> None:
    assert spec.covers(step, unit, REQUESTED) is expected


def test_force_metrics_alone_forces_steps_computing_them() -> None:
    spec = ForceSpec(metrics=("HHI",))
    assert spec.forces_step("partners", REQUESTED)
    assert not spec.forces_step("network", {"SPOF": "s"})
    assert spec.names_for(REQUESTED) == {"HHI"}
    # Étape forcée sans nom correspondant : toutes les métriques demandées
    assert ForceSpec(steps=frozenset({"all"}), metrics=("HHI",)).names_for({"SPOF": "s"}) == {"SPOF"}


def test_force_describe() -> None:
    spec = ForceSpec(steps=frozenset({"partners"}), metrics=("HHI",), reporters=("FR",))
    assert spec.describe() == "steps=partners;metrics=HHI;reporters=FR"
    assert ForceSpec().describe() == ""


@pytest.mark.parametrize(
    "state, environ, expected",
    [
        ({"ADOPT_LEGACY_FINGERPRINTS": True}, {}, True),
        ({"ADOPT_LEGACY_FINGERPRINTS": True}, {"ADOPT_LEGACY_FINGERPRINTS": "false"}, False),
        (None, {"ADOPT_LEGACY_FINGERPRINTS": "1"}, True),
        (None, {}, False),
    ],
)
def test_adopt_legacy_flag(state, environ, expected) -> None:
    assert adopt_legacy_flag(state, environ) is expected


# ──────────────────────────────────────────────────────────────────────
# Décision
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "case, entry, upstream, force, expected",
    [
        ("jamais calculé", None, T0, ForceSpec(), UnitPlan("first", frozenset(REQUESTED))),
        ("passe démarrée jamais terminée",
         RegistryEntry(FR, None, None, {}, "first"), T0, ForceSpec(),
         UnitPlan("first", frozenset(REQUESTED))),
        ("à jour", _entry(FR), T0, ForceSpec(), None),
        ("amont plus récent", _entry(FR), T2, ForceSpec(), UnitPlan("new_data", frozenset(REQUESTED))),
        ("amont inconnu", _entry(FR), None, ForceSpec(), None),
        ("métrique ajoutée", _entry(FR, {"HHI": "h1"}), T0, ForceSpec(),
         UnitPlan("fingerprint", frozenset({"CDI2"}))),
        ("paramètre modifié", _entry(FR, {"HHI": "h0", "CDI2": "c1"}), T0, ForceSpec(),
         UnitPlan("fingerprint", frozenset({"HHI"}))),
        ("métrique retirée seulement", _entry(FR, {**REQUESTED, "OLD": "o"}), T0, ForceSpec(), None),
        ("forçage par étape", _entry(FR), T0, ForceSpec(steps=frozenset({"partners"})),
         UnitPlan("forced", frozenset(REQUESTED))),
        ("forçage d'une métrique", _entry(FR), T0, ForceSpec(metrics=("HHI",)),
         UnitPlan("forced", frozenset({"HHI"}))),
        ("forcé l'emporte sur new_data", _entry(FR), T2, ForceSpec(steps=frozenset({"all"})),
         UnitPlan("forced", frozenset(REQUESTED))),
        ("new_data l'emporte sur fingerprint", _entry(FR, {}), T2, ForceSpec(),
         UnitPlan("new_data", frozenset(REQUESTED))),
        ("first l'emporte sur forcé", None, T0, ForceSpec(steps=frozenset({"all"})),
         UnitPlan("first", frozenset(REQUESTED))),
    ],
)
def test_units_to_compute_priorities(tmp_path: Path, case, entry, upstream, force, expected) -> None:
    registry = _registry(tmp_path)
    if entry is not None:
        registry.upsert(entry)
    plans = units_to_compute([FR], registry, {FR: upstream}, REQUESTED, force, step="partners")
    assert plans.get(FR) == expected, case


def test_units_to_compute_custom_new_data_rule(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    registry.upsert(_entry(FR))
    plans = units_to_compute(
        [FR], registry, {}, REQUESTED, ForceSpec(), step="partners",
        is_new_data=lambda unit, entry, watermark: entry.extra.get("resume", True),
    )
    assert plans[FR].reason == "new_data"


# ──────────────────────────────────────────────────────────────────────
# Registre fragmenté
# ──────────────────────────────────────────────────────────────────────


def test_registry_save_writes_v2_fragments_and_reloads(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    registry.upsert(_entry(FR))
    written = registry.save()
    assert written == [f"{tmp_path.as_posix()}/state/HS2022/FR.json"]
    document = json.loads(Path(written[0]).read_text(encoding="utf-8"))
    assert document["schema_version"] == 2
    assert document["step"] == "partners" and document["fragment"] == "HS2022/FR"
    assert document["entries"]["HS2022|FR|280530"]["unit"]["reporter"] == "FR"
    # Relecture par une autre instance
    assert _registry(tmp_path).get(FR) == _entry(FR)
    # Rien de modifié : aucun fragment réécrit
    assert registry.save() == []


def test_registry_lazy_loading_reads_only_touched_fragments(tmp_path: Path) -> None:
    first = _registry(tmp_path)
    first.upsert(_entry(FR))
    first.upsert(_entry(DE))
    first.save()
    second = _registry(tmp_path)
    second.get(FR)
    assert second.loaded_paths == [f"{tmp_path.as_posix()}/state/HS2022/FR.json"]


def test_registry_parallel_pods_on_disjoint_fragments_keep_all_entries(tmp_path: Path) -> None:
    # Deux pods chargent l'état initial, puis écrivent chacun leur fragment
    pod_a, pod_b = _registry(tmp_path), _registry(tmp_path)
    pod_a.upsert(_entry(FR))
    pod_b.upsert(_entry(DE))
    pod_b.save()
    pod_a.save()
    reader = _registry(tmp_path)
    assert {entry.unit for entry in reader.iter_entries()} == {FR, DE}


def test_registry_upsert_preserves_other_entries_of_the_fragment(tmp_path: Path) -> None:
    other = Unit.of(classification="HS2022", reporter="FR", product="854140")
    first = _registry(tmp_path)
    first.upsert(_entry(FR))
    first.save()
    second = _registry(tmp_path)
    second.upsert(_entry(other))
    second.save()
    assert _registry(tmp_path).get(FR) is not None


# Fonction d'analyse du registre v1 des partenaires (fixture)
def _parse_v1(data):
    """Parse a v1 partners registry into legacy entries."""
    for item in data.get("VULNERABILITIES", {}).values():
        yield legacy_entry(
            Unit.of(classification="HS2022", reporter=item["reporter"], product=item["product"]),
            item["last_computed"],
        )


def _write_v1(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"VULNERABILITIES": {"FR|280530": {
        "reporter": "FR", "product": "280530", "last_computed": T1.isoformat()}}}), encoding="utf-8")


def test_registry_reads_v1_registry_at_another_path(tmp_path: Path) -> None:
    v1 = tmp_path / "v1" / "last_computation.json"
    _write_v1(v1)
    registry = _registry(tmp_path, LegacySource(v1, None, _parse_v1))
    entry = registry.get(FR)
    assert entry is not None and entry.legacy and entry.fingerprints == {}
    assert entry.upstream_watermark == entry.last_computed == T1
    # Empreinte manquante : jamais calculé avec cette méthodologie → tout recalculer
    plans = units_to_compute([FR], registry, {FR: T0}, REQUESTED, ForceSpec(), step="partners")
    assert plans[FR] == UnitPlan("fingerprint", frozenset(REQUESTED))


def test_adopt_legacy_fingerprints_migrates_without_recompute(tmp_path: Path) -> None:
    v1 = tmp_path / "v1" / "last_computation.json"
    _write_v1(v1)
    registry = _registry(tmp_path, LegacySource(v1, None, _parse_v1))
    plans = units_to_compute(
        [FR], registry, {FR: T0}, REQUESTED, ForceSpec(), step="partners",
        adopt_legacy_fingerprints=True,
    )
    assert plans == {}
    registry.save()
    # L'entrée migrée vit désormais dans son fragment v2, sans le registre v1
    migrated = _registry(tmp_path).get(FR)
    assert migrated is not None and not migrated.legacy and migrated.fingerprints == REQUESTED


def test_single_fragment_registry_reads_v1_at_same_path(tmp_path: Path) -> None:
    path = tmp_path / "synthesis.json"
    path.write_text(json.dumps({"SYNTHESIS": {"last_computed": T1.isoformat()}}), encoding="utf-8")
    unit = Unit.of(scope="global")
    legacy = LegacySource(path, None, lambda data: [legacy_entry(unit, data["SYNTHESIS"]["last_computed"])])
    registry = FreshnessRegistry(path, None, "synthesis", lambda _: "global", legacy=legacy)
    assert registry.get(unit).legacy
    registry.upsert(RegistryEntry(unit, T2, T1, {"synthesis": "s"}, "new_data"))
    registry.save()
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["schema_version"] == 2
    assert FreshnessRegistry(path, None, "synthesis", lambda _: "global").get(unit).last_computed == T2


def test_iter_entries_lists_fragments_recursively_and_ignores_foreign_files(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    registry.upsert(_entry(FR))
    registry.upsert(_entry(Unit.of(classification="HS2017", reporter="DE", product="01")))
    registry.save()
    (tmp_path / "state" / "notes.json").write_text("{}", encoding="utf-8")
    assert len(list(_registry(tmp_path).iter_entries())) == 2


def test_iter_entries_on_s3(s3_bucket: str) -> None:
    template = "trade/state/partners/{classification}/{reporter}.json"
    registry = FreshnessRegistry(template, s3_bucket, "partners", lambda unit: unit.key)
    registry.upsert(_entry(FR))
    registry.upsert(_entry(DE))
    registry.save()
    reader = FreshnessRegistry(template, s3_bucket, "partners", lambda unit: unit.key)
    assert {entry.unit for entry in reader.iter_entries()} == {FR, DE}
    assert reader.get(FR).fingerprints == REQUESTED


def test_path_of_requires_template_dimensions(tmp_path: Path) -> None:
    with pytest.raises(KeyError):
        _registry(tmp_path).path_of(Unit.of(vintage="HS2017"))


# ──────────────────────────────────────────────────────────────────────
# Métriques et lecture aval
# ──────────────────────────────────────────────────────────────────────


def test_plan_metrics_counts_every_reason() -> None:
    plans = {
        FR: UnitPlan("first", frozenset()),
        DE: UnitPlan("forced", frozenset()),
    }
    metrics = plan_metrics(plans, n_candidates=5)
    assert metrics["freshness/units_planned"] == 2.0
    assert metrics["freshness/units_first"] == metrics["freshness/units_forced"] == 1.0
    assert metrics["freshness/units_new_data"] == metrics["freshness/units_fingerprint"] == 0.0
    assert metrics["freshness/units_candidates"] == 5.0


@pytest.mark.parametrize(
    "reasons, since, full_change",
    [
        (["new_data", "new_data"], None, False),
        (["new_data", "fingerprint"], None, True),
        (["forced", "new_data"], T2, False),
    ],
)
def test_summarize_upstream(reasons, since, full_change) -> None:
    entries = [
        RegistryEntry(Unit.of(v=str(i)), T0 + timedelta(days=i), reason=reason)
        for i, reason in enumerate(reasons)
    ]
    summary = summarize_upstream(entries, since)
    assert summary.watermark == T0 + timedelta(days=len(reasons) - 1)
    assert summary.full_change is full_change
    assert summary.to_json()["n_units"] == len(reasons)


def test_parse_instant_naive_is_utc() -> None:
    assert parse_instant("2026-01-01T00:00:00") == T0


# ──────────────────────────────────────────────────────────────────────
# Invalidation des empreintes (procédure après correction d'une formule)
# ──────────────────────────────────────────────────────────────────────


def _saved_registry(tmp_path: Path) -> FreshnessRegistry:
    """Registry holding FR and DE, both fresh, saved to disk."""
    registry = _registry(tmp_path)
    registry.upsert(_entry(FR))
    registry.upsert(_entry(DE))
    registry.save()
    return _registry(tmp_path)


@pytest.mark.parametrize(
    "names, scope, expected_units, expected_plan",
    [
        ({"HHI"}, None, {FR, DE}, UnitPlan("fingerprint", frozenset({"HHI"}))),
        (None, None, {FR, DE}, UnitPlan("fingerprint", frozenset(REQUESTED))),
        ({"HHI"}, ForceSpec(reporters=("FR",)), {FR}, UnitPlan("fingerprint", frozenset({"HHI"}))),
        ({"HHI"}, ForceSpec(products=("85",)), {DE}, UnitPlan("fingerprint", frozenset({"HHI"}))),
        ({"UNKNOWN"}, None, set(), None),
    ],
)
def test_invalidate_then_next_pass_recomputes_only_invalidated_units(
    tmp_path: Path, names, scope, expected_units, expected_plan
) -> None:
    registry = _saved_registry(tmp_path)
    invalidated = registry.invalidate(names, scope)
    assert set(invalidated) == expected_units
    registry.save()
    # Prochaine passe (registre relu) : seules les unités invalidées sont recalculées
    plans = units_to_compute([FR, DE], _registry(tmp_path), {FR: T0, DE: T0}, REQUESTED,
                             ForceSpec(), step="partners")
    assert set(plans) == expected_units
    assert all(plan == expected_plan for plan in plans.values())


def test_invalidate_keeps_history_of_the_unit(tmp_path: Path) -> None:
    registry = _saved_registry(tmp_path)
    registry.invalidate({"HHI"})
    registry.save()
    entry = _registry(tmp_path).get(FR)
    assert entry.last_computed == T1 and entry.upstream_watermark == T0
    assert entry.fingerprints == {"CDI2": "c1"}


def test_invalidate_without_save_writes_nothing(tmp_path: Path) -> None:
    registry = _saved_registry(tmp_path)
    assert registry.invalidate({"HHI"})
    # Aperçu : rien n'est persisté sans save()
    assert _registry(tmp_path).get(FR).fingerprints == REQUESTED


def test_invalidation_survives_a_failed_run(tmp_path: Path) -> None:
    registry = _saved_registry(tmp_path)
    registry.invalidate({"HHI"}, ForceSpec(reporters=("FR",)))
    registry.save()
    # Exécution échouée : rien n'est enregistré, la passe suivante reprend FR
    for _ in range(2):
        plans = units_to_compute([FR, DE], _registry(tmp_path), {}, REQUESTED, ForceSpec(), step="partners")
        assert set(plans) == {FR}


def test_invalidated_reason_cascades_downstream(tmp_path: Path) -> None:
    registry = _saved_registry(tmp_path)
    registry.invalidate({"HHI"})
    registry.save()
    registry = _registry(tmp_path)
    plans = units_to_compute([FR, DE], registry, {}, REQUESTED, ForceSpec(), step="partners")
    for unit, plan in plans.items():
        registry.upsert(RegistryEntry(unit, T2, T0, dict(REQUESTED), plan.reason))
    registry.save()
    summary = summarize_upstream(_registry(tmp_path).iter_entries(), since=T1)
    assert summary.full_change and summary.reasons_since == {"fingerprint": 2}


def test_invalidate_single_fragment_registry(tmp_path: Path) -> None:
    unit = Unit.of(scope="global")
    path = tmp_path / "synthesis.json"
    registry = FreshnessRegistry(path, None, "synthesis", lambda _: "global")
    registry.upsert(RegistryEntry(unit, T1, T0, {"synthesis": "s"}, "new_data"))
    registry.save()
    registry = FreshnessRegistry(path, None, "synthesis", lambda _: "global")
    assert registry.invalidate() == [unit]
    registry.save()
    assert FreshnessRegistry(path, None, "synthesis", lambda _: "global").get(unit).fingerprints == {}
