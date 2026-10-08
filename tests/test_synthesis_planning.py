"""Planification incrémentale de la synthèse par contexte et par méthode (fonctions pures).

Règles de décision d'un contexte (premier calcul, forçage, nouvelles données
complètes ou récentes, empreintes par méthode), pertinence des changements amont
par millésime SH, budget de rattrapage, réglages, cadence et requêtes SQL.
Registres locaux sous ``tmp_path``, aucune base.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    RegistryEntry,
    Unit,
    UnitPlan,
    parse_instant,
)
from macroforecast.trade.aggregation import MethodSpec, SynthesisConfig
from scripts.compute_synthetic_scores import (
    CONSENSUS_FINGERPRINT,
    INPUTS_FINGERPRINT,
    UpstreamMarks,
    build_contexts_query,
    build_scores_query,
    build_source_query,
    cadence_check_requested,
    context_entry,
    context_registry,
    context_unit,
    default_recent_periods,
    expand_synthesis_names,
    incremental_settings,
    method_fingerprint_params,
    plan_methods,
    plan_synthesis_contexts,
    recent_period_values,
    replacement_predicate,
    select_contexts,
    skipped_by_cadence,
    synthesis_context_requested,
)

# Référentiel restreint des millésimes et colonnes de contexte de la configuration réelle
NOMENCLATURES = {"HS2017": 2017, "HS2022": 2022}
COLUMNS = ("hs_vintage", "freq", "flow", "indicators", "TIME_PERIOD")
# Instants : premier calcul amont, calcul de la synthèse, changement amont ultérieur
T0 = parse_instant("2026-01-01T00:00:00+00:00")
T1 = parse_instant("2026-01-02T00:00:00+00:00")
T2 = parse_instant("2026-01-03T00:00:00+00:00")
SOURCES = [{"SCHEMA": "indicators", "ALIAS": "p", "COLUMNS": ["HHI", "CDI2"]}]


def _config(*extra: MethodSpec) -> SynthesisConfig:
    """Configuration à deux méthodes (plus ``extra``), consensus Borda."""
    return SynthesisConfig(
        context_columns=COLUMNS,
        metric_columns=("HHI", "CDI2"),
        methods=(MethodSpec(name="mpi", kind="mpi"), MethodSpec(name="rank_mean", kind="rank_mean"),
                 *extra),
        consensus=("borda",),
    )


def _units() -> list[Unit]:
    """Contextes en vigueur 2019-2023 (HS2017 puis HS2022) et un contexte historique HS2017 2023."""
    units = [
        context_unit(COLUMNS, ("HS2017" if year < 2022 else "HS2022", "A", 1, "V", str(year)))
        for year in range(2019, 2024)
    ]
    units.append(context_unit(COLUMNS, ("HS2017", "A", 1, "V", "2023")))
    return units


def _registry(tmp_path: Path) -> FreshnessRegistry:
    """Registre de synthèse par contexte, un fichier par période."""
    return context_registry(
        {"STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/synthesis/{{TIME_PERIOD}}.json"}},
        None, "synthesis", COLUMNS,
    )


def _requested(config: SynthesisConfig, filters=None):
    return synthesis_context_requested(config, SOURCES, filters or {}, ["import"], "all")


def _record_all(registry: FreshnessRegistry, requested, watermark=T0) -> None:
    """Passe complète enregistrée sur tous les contextes."""
    for unit in _units():
        registry.upsert(context_entry(
            unit, UnitPlan("first", frozenset(requested)), T1, watermark, requested, None,
        ))
    registry.save()


def _marks(partners=(), networks=()) -> UpstreamMarks:
    return UpstreamMarks.from_entries(
        list(partners), list(networks), partner_label="HS2022", nomenclatures=NOMENCLATURES,
    )


def _partner(reason: str, when=T2, classification="HS2022") -> RegistryEntry:
    return RegistryEntry(
        Unit.of(classification=classification, reporter="FR", product="854110"), when, reason=reason
    )


def _network(vintage: str, when=T2, reason="new_data") -> RegistryEntry:
    return RegistryEntry(Unit.of(vintage=vintage), when, reason=reason)


def _plan(tmp_path, config, *, marks=None, force=ForceSpec(), recent=None, filters=None):
    requested = _requested(config, filters)
    return plan_synthesis_contexts(
        _units(), _registry(tmp_path), requested, marks or _marks([_partner("first", T0)]),
        force, method_names=[spec.name for spec in config.methods], recent_periods=recent,
    )


def _keys(plans) -> set[str]:
    return {unit.key for unit in plans}


# ──────────────────────────────────────────────────────────────────────
# Règles de décision
# ──────────────────────────────────────────────────────────────────────


def test_first_pass_plans_every_context_and_name(tmp_path: Path) -> None:
    config = _config()
    plans = _plan(tmp_path, config)
    assert len(plans) == len(_units())
    assert {plan.reason for plan in plans.values()} == {"first"}
    assert all(plan.names == frozenset(_requested(config)) for plan in plans.values())


def test_second_pass_plans_nothing(tmp_path: Path) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    assert _plan(tmp_path, config) == {}


def test_forcing_step_methods_and_scope(tmp_path: Path) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    # Étape forcée : tous les contextes, tous les noms
    plans = _plan(tmp_path, config, force=ForceSpec(steps=frozenset({"synthesis"})))
    assert len(plans) == len(_units()) and {p.reason for p in plans.values()} == {"forced"}
    # Méthode forcée : cette méthode et le consensus seulement
    plans = _plan(tmp_path, config, force=ForceSpec(methods=("mpi",)))
    assert {p.names for p in plans.values()} == {frozenset({"mpi", CONSENSUS_FINGERPRINT})}
    # Périmètre : une période, un millésime SH de contexte
    plans = _plan(tmp_path, config, force=ForceSpec(steps=frozenset({"synthesis"}), periods=("2023",)))
    assert {unit.get("TIME_PERIOD") for unit in plans} == {"2023"} and len(plans) == 2
    plans = _plan(tmp_path, config, force=ForceSpec(steps=frozenset({"synthesis"}), vintages=("HS2017",)))
    assert {unit.get("hs_vintage") for unit in plans} == {"HS2017"} and len(plans) == 4


@pytest.mark.parametrize("reason", ["first", "fingerprint", "forced"])
def test_partner_full_change_recomputes_every_in_force_period(tmp_path: Path, reason: str) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    plans = _plan(tmp_path, config, marks=_marks([_partner("first", T0), _partner(reason)]), recent=1)
    # Toutes les périodes en vigueur, jamais le contexte historique
    assert _keys(plans) == {unit.key for unit in _units()[:5]}
    assert {p.reason for p in plans.values()} == {"new_data"}
    assert all(p.names == frozenset(_requested(config)) for p in plans.values())


def test_partner_new_data_recomputes_recent_periods_only(tmp_path: Path) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    plans = _plan(tmp_path, config, marks=_marks([_partner("first", T0), _partner("new_data")]), recent=2)
    # Deux périodes les plus récentes, contextes en vigueur seulement
    assert {(u.get("hs_vintage"), u.get("TIME_PERIOD")) for u in plans} == {
        ("HS2022", "2022"), ("HS2022", "2023"),
    }
    # Sans profondeur connue : toutes les périodes sont récentes
    plans = _plan(tmp_path, config, marks=_marks([_partner("first", T0), _partner("new_data")]), recent=None)
    assert len(plans) == 5


def test_historical_partner_unit_only_touches_its_vintage(tmp_path: Path) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    marks = _marks([_partner("first", T0), _partner("fingerprint", classification="HS2017")])
    plans = _plan(tmp_path, config, marks=marks)
    assert _keys(plans) == {"HS2017|A|1|V|2023"}


def test_network_change_touches_contexts_of_its_vintage(tmp_path: Path) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    plans = _plan(tmp_path, config, marks=_marks([_partner("first", T0)], [_network("HS2017")]))
    assert {unit.get("hs_vintage") for unit in plans} == {"HS2017"} and len(plans) == 4
    # Changement ancien (antérieur au watermark enregistré) : rien
    plans = _plan(tmp_path, config, marks=_marks([_partner("first", T0)], [_network("HS2017", T0)]))
    assert plans == {}


def test_added_and_removed_methods(tmp_path: Path) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    # Méthode ajoutée : elle seule et le consensus, sur tous les contextes
    plans = _plan(tmp_path, _config(MethodSpec(name="bod", kind="bod")))
    assert len(plans) == len(_units()) and {p.reason for p in plans.values()} == {"fingerprint"}
    assert {p.names for p in plans.values()} == {frozenset({"bod", CONSENSUS_FINGERPRINT})}
    # Méthode retirée : consensus seul (il ne classe plus la méthode retirée)
    reduced = SynthesisConfig(
        context_columns=COLUMNS, metric_columns=("HHI", "CDI2"),
        methods=(MethodSpec(name="mpi", kind="mpi"),), consensus=("borda",),
    )
    plans = _plan(tmp_path, reduced)
    assert {p.names for p in plans.values()} == {frozenset({CONSENSUS_FINGERPRINT})}
    assert plan_methods(next(iter(plans.values())).names, ["mpi"]) == ()


def test_modified_and_invalidated_method(tmp_path: Path) -> None:
    config = _config()
    registry = _registry(tmp_path)
    _record_all(registry, _requested(config))
    # Paramètre modifié d'une méthode : elle seule (et le consensus)
    modified = SynthesisConfig(
        context_columns=COLUMNS, metric_columns=("HHI", "CDI2"),
        methods=(MethodSpec(name="mpi", kind="mpi", min_group_size=5),
                 MethodSpec(name="rank_mean", kind="rank_mean")),
        consensus=("borda",),
    )
    plans = _plan(tmp_path, modified)
    assert {p.names for p in plans.values()} == {frozenset({"mpi", CONSENSUS_FINGERPRINT})}
    # Implémentation corrigée : empreinte invalidée, même plan
    registry.invalidate({"rank_mean"})
    registry.save()
    plans = _plan(tmp_path, config)
    assert {p.names for p in plans.values()} == {frozenset({"rank_mean", CONSENSUS_FINGERPRINT})}


def test_changed_input_selection_recomputes_every_method(tmp_path: Path) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    plans = _plan(tmp_path, config, filters={"WHERE": "p.freq = 'A'"})
    assert len(plans) == len(_units())
    assert all(p.names == frozenset(_requested(config)) for p in plans.values())


# ──────────────────────────────────────────────────────────────────────
# Empreintes, noms, budget, réglages, cadence
# ──────────────────────────────────────────────────────────────────────


def test_method_fingerprint_params_are_scoped_to_the_method() -> None:
    base = SynthesisConfig(metric_columns=("HHI", "CDI2"))
    smaa = MethodSpec(name="smaa", kind="smaa")
    mpi = MethodSpec(name="mpi", kind="mpi")
    # La graine ne concerne que les méthodes aléatoires
    other_seed = SynthesisConfig(metric_columns=("HHI", "CDI2"), random_state=7)
    assert method_fingerprint_params(mpi, base) == method_fingerprint_params(mpi, other_seed)
    assert method_fingerprint_params(smaa, base) != method_fingerprint_params(smaa, other_seed)
    # Polarité d'une métrique hors de la méthode : sans effet
    subset = MethodSpec(name="mpi_hhi", kind="mpi", metrics=("HHI",))
    polarised = SynthesisConfig(metric_columns=("HHI", "CDI2"), polarities=(("CDI2", -1),))
    assert method_fingerprint_params(subset, base) == method_fingerprint_params(subset, polarised)
    assert method_fingerprint_params(mpi, base) != method_fingerprint_params(mpi, polarised)


def test_requested_names_and_expansion() -> None:
    config = _config()
    requested = _requested(config)
    assert set(requested) == {"mpi", "rank_mean", CONSENSUS_FINGERPRINT, INPUTS_FINGERPRINT}
    assert expand_synthesis_names({INPUTS_FINGERPRINT}, requested, ["mpi", "rank_mean"]) == frozenset(requested)
    without_consensus = synthesis_context_requested(
        SynthesisConfig(metric_columns=("HHI",), methods=(MethodSpec(name="mpi", kind="mpi"),), consensus=())
    )
    assert CONSENSUS_FINGERPRINT not in without_consensus
    with pytest.raises(ValueError):
        synthesis_context_requested(SynthesisConfig(
            metric_columns=("HHI",), methods=(MethodSpec(name="consensus", kind="mpi"),)))


def test_budget_orders_by_period_and_reports_backlog() -> None:
    units = _units()
    plans = {unit: UnitPlan("first", frozenset()) for unit in units}
    selected, backlog = select_contexts(plans, units, "TIME_PERIOD", 2)
    assert [unit.get("TIME_PERIOD") for unit in selected] == ["2023", "2023"] and backlog == 4
    selected, backlog = select_contexts(plans, units, "TIME_PERIOD", None)
    assert len(selected) == 6 and backlog == 0
    assert [unit.get("TIME_PERIOD") for unit in selected][:3] == ["2023", "2023", "2022"]


def test_recent_periods_and_settings() -> None:
    assert recent_period_values(_units(), "TIME_PERIOD", None) is None
    assert recent_period_values(_units(), "TIME_PERIOD", 1) == frozenset({"2023"})
    eurostat = {"DOWNLOADS": {"DS-045409": {"N_LAST_OBSERVATIONS": 10}, "OTHER": {"N_LAST_OBSERVATIONS": 4}}}
    assert default_recent_periods(eurostat) == 10
    settings = incremental_settings({"MAX_CONTEXTS_PER_RUN": None, "CADENCE": {"MIN_INTERVAL_DAYS": 3}}, eurostat)
    assert (settings.recent_periods, settings.max_contexts, settings.min_interval_days) == (10, None, 3.0)
    assert incremental_settings({"RECENT_PERIODS": 2}, eurostat).recent_periods == 2
    with pytest.raises(ValueError):
        incremental_settings({"MAX_CONTEXTS_PER_RUN": 0})


def test_cadence_check() -> None:
    now = parse_instant("2026-10-06T00:00:00+00:00")
    assert skipped_by_cadence(now - timedelta(days=2), now, 6, forced=False)
    assert not skipped_by_cadence(now - timedelta(days=7), now, 6, forced=False)
    assert not skipped_by_cadence(now - timedelta(days=2), now, 6, forced=True)
    assert not skipped_by_cadence(None, now, 6, forced=False)
    assert cadence_check_requested(True, {}) and cadence_check_requested(False, {"CADENCE_CHECK": "true"})
    assert not cadence_check_requested(False, {"CADENCE_CHECK": "0"})


def test_registry_template_must_name_context_columns(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="reporter"):
        context_registry({"STATE": {"PATH_TEMPLATE": f"{tmp_path}/{{reporter}}.json"}}, None, "synthesis", COLUMNS)
    registry = _registry(tmp_path)
    assert registry.path_of(_units()[0]).endswith("/synthesis/2019.json")


def test_context_entry_merges_fingerprints() -> None:
    unit = _units()[0]
    previous = RegistryEntry(unit, T0, T0, fingerprints={"mpi": "old", "rank_mean": "kept"})
    entry = context_entry(unit, UnitPlan("fingerprint", frozenset({"mpi"})), T1, T1,
                          {"mpi": "new", "rank_mean": "current"}, previous, n_cells=3)
    assert entry.fingerprints == {"mpi": "new", "rank_mean": "kept"}
    assert entry.extra == {"n_cells": 3} and entry.reason == "fingerprint"


# ──────────────────────────────────────────────────────────────────────
# Requêtes SQL
# ──────────────────────────────────────────────────────────────────────


def test_contexts_query_reads_the_grid_only() -> None:
    query = build_contexts_query(
        [*SOURCES, {"SCHEMA": "network_indicators", "ALIAS": "n", "COLUMNS": ["WORLD_HHI"],
                    "JOIN": {"ON": ["n.product = p.product"]}}],
        {"WHERE": "p.freq = 'A'", "LAST_N_PERIODS": 3}, "db", COLUMNS, [1, 2], "in_force",
    )
    assert query.startswith('SELECT DISTINCT p."hs_vintage", p."freq"')
    assert "JOIN" not in query and "WORLD_HHI" not in query
    assert "p.freq = 'A'" in query and 'p."flow" IN (1, 2)' in query
    assert 'p."in_force" = true' in query and "LIMIT 3" in query


def test_source_query_restricted_to_contexts_is_otherwise_unchanged() -> None:
    filters = {"WHERE": "p.freq = 'A'", "LAST_N_PERIODS": None}
    base = build_source_query(SOURCES, filters, "db", [1], "in_force")
    restricted = build_source_query(
        SOURCES, filters, "db", [1], "in_force",
        contexts=[("HS2022", "A", 1, "V", "2023")], context_columns=COLUMNS,
    )
    assert restricted.startswith(base)
    assert '(p."hs_vintage", p."freq", p."flow", p."indicators", p."TIME_PERIOD") IN (' in restricted
    assert build_source_query(SOURCES, filters, "db", [1], "in_force", contexts=None) == base
    with pytest.raises(ValueError):
        build_source_query(SOURCES, filters, "db", contexts=[("A",)])


def test_scores_query_restricted_to_methods() -> None:
    base = build_scores_query("db", "synthesis", ["freq"], [("A",)])
    assert build_scores_query("db", "synthesis", ["freq"], [("A",)], methods=None) == base
    restricted = build_scores_query("db", "synthesis", ["freq"], [("A",)], methods=["mpi", "bod"])
    assert restricted == base + "\n  AND \"method\" IN ('mpi', 'bod')"


def test_replacement_predicate_groups_contexts_by_names() -> None:
    predicate = replacement_predicate(
        ["freq", "flow"],
        {("mpi",): [("A", 1), ("A", 2)], ("mpi", "bod"): [("Q", 1)], (): [("M", 1)]},
        "method",
        restriction='"family" = \'fit\'',
    )
    assert predicate.startswith('"family" = \'fit\' AND (')
    assert predicate.count(" OR ") == 1
    assert "('M', 1)" not in predicate
    assert replacement_predicate(["freq"], {(): [("A",)]}, "method") is None
