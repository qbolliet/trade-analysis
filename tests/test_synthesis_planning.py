"""Planification incrémentale de la synthèse par contexte et par méthode (fonctions pures).

Règles de décision d'un contexte (premier calcul, forçage, contexte touché par un
changement amont et marqué en attente, empreintes par méthode), budget de
rattrapage, réglages, cadence et requêtes SQL. Registres locaux sous
``tmp_path``, aucune base.
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
    mark_pending,
    parse_instant,
)
from tradestat.trade.aggregation import MethodSpec, SynthesisConfig
from scripts.compute_synthetic_scores import (
    CONSENSUS_FINGERPRINT,
    INPUTS_FINGERPRINT,
    build_contexts_query,
    build_scores_query,
    build_source_query,
    cadence_check_requested,
    context_entry,
    context_registry,
    context_unit,
    expand_synthesis_names,
    incremental_settings,
    method_fingerprint_params,
    plan_methods,
    plan_pending_contexts,
    replacement_predicate,
    select_contexts,
    skipped_by_cadence,
    synthesis_context_requested,
)

# Colonnes de contexte de la configuration réelle
COLUMNS = ("hs_vintage", "freq", "flow", "indicators", "TIME_PERIOD")
# Instants : calcul de la synthèse, calcul ultérieur
T0 = parse_instant("2026-01-01T00:00:00+00:00")
T1 = parse_instant("2026-01-02T00:00:00+00:00")
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


def _record_all(registry: FreshnessRegistry, requested) -> None:
    """Passe complète enregistrée sur tous les contextes."""
    for unit in _units():
        registry.upsert(context_entry(
            unit, UnitPlan("first", frozenset(requested)), T1, requested, None,
        ))
    registry.save()


def _plan(tmp_path, config, *, pending=(), force=ForceSpec(), filters=None):
    """Plans des contextes, ``pending`` étant les contextes touchés par un changement amont."""
    requested = _requested(config, filters)
    registry = _registry(tmp_path)
    mark_pending(registry, pending, 42)
    return plan_pending_contexts(
        _units(), registry, requested, force, method_names=[spec.name for spec in config.methods],
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


def test_touched_contexts_are_planned_pending_with_every_name(tmp_path: Path) -> None:
    config = _config()
    _record_all(_registry(tmp_path), _requested(config))
    # Contextes touchés par un changement amont, toutes périodes confondues
    touched = [_units()[0], _units()[4]]
    plans = _plan(tmp_path, config, pending=touched)
    assert _keys(plans) == {unit.key for unit in touched}
    assert {plan.reason for plan in plans.values()} == {"pending"}
    assert all(plan.names == frozenset(_requested(config)) for plan in plans.values())
    # Aucun contexte touché : rien à recalculer
    assert _plan(tmp_path, config) == {}


def test_pending_flag_lasts_until_the_context_is_recorded(tmp_path: Path) -> None:
    config = _config()
    requested = _requested(config)
    registry = _registry(tmp_path)
    _record_all(registry, requested)
    unit = _units()[1]
    mark_pending(registry, [unit], 7)
    registry.save()
    # Run interrompu : le marqueur survit, le contexte reste planifié
    assert _keys(_plan(tmp_path, config)) == {unit.key}
    # Enregistrement après succès : le marqueur est effacé
    reread = _registry(tmp_path)
    reread.upsert(context_entry(unit, UnitPlan("pending", frozenset(requested)), T1, requested, reread.get(unit)))
    reread.save()
    assert _plan(tmp_path, config) == {}


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


def test_incremental_settings() -> None:
    settings = incremental_settings({"MAX_CONTEXTS_PER_RUN": None, "CADENCE": {"MIN_INTERVAL_DAYS": 3}}, n_jobs=1)
    assert (settings.max_contexts, settings.min_interval_days) == (None, 3.0)
    assert incremental_settings({"MAX_CONTEXTS_PER_RUN": 5}, n_jobs=1).max_contexts == 5
    with pytest.raises(ValueError):
        incremental_settings({"MAX_CONTEXTS_PER_RUN": 0}, n_jobs=1)


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
    entry = context_entry(unit, UnitPlan("fingerprint", frozenset({"mpi"})), T1,
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
