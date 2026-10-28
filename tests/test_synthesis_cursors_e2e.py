"""Synthèse pilotée par curseurs DuckLake (catalogue temporaire, de bout en bout).

Comportement figé :

1. premier passage : tous les contextes (``first``), curseurs initialisés ;
2. aucun changement en entrée : aucun contexte, curseurs avancés ;
3. révision d'une **période ancienne** d'une cellule partenaire : seul son contexte
   est recalculé (quelle que soit sa période), et le résultat est identique à un
   recalcul complet ;
4. révision d'une ligne réseau : les seuls contextes qu'elle joint ;
5. suppression de lignes de la grille : contexte recalculé, identique au complet ;
6. contexte en échec : il reste ``pending`` et est repris au passage suivant ;
7. snapshots expirés après le curseur : repli complet ;
8. curseur perdu alors que des contextes sont déjà calculés : repli complet.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("dt_ducklake_manager")

from statflows.storage.ducklake.tables import write_dataframe  # noqa: E402

from kedro_pipeline.io.cursors import StepCursors  # noqa: E402
from kedro_pipeline.io.freshness import (  # noqa: E402
    ForceSpec,
    FreshnessRegistry,
    Unit,
)
from kedro_pipeline.steps import synthesis as synthesis_module  # noqa: E402
from kedro_pipeline.steps.synthesis import (  # noqa: E402
    IncrementalSettings,
    build_source_query,
    context_registry,
    context_unit,
    read_source_metrics,
    run_incremental_synthesis,
    synthesis_context_requested,
)
from tradestat.trade.aggregation import MethodSpec, SynthesisConfig, run_synthesis  # noqa: E402

# Sources : grille partenaires, réseau joint par année et sens (bloc CHANGE)
SOURCES = [
    {"SCHEMA": "indicators", "ALIAS": "p", "COLUMNS": ["HHI", "CDI2", "CDI3"]},
    {
        "SCHEMA": "network_indicators",
        "ALIAS": "n",
        "COLUMNS": ["WORLD_HHI", "CENTRALITY_RISK", "CLUSTERING_W"],
        "JOIN": {
            "ON": [
                'substr(p."product", 1, 6) = n."product"',
                'CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER) = n."year"',
                'n."flow" = p."flow"',
            ],
            "WHERE": "n.\"classification\" = 'HS2022'",
        },
        "CHANGE": {
            "KEYS": ["year", "flow"],
            "ON": [
                'n."year" = CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER)',
                'n."flow" = p."flow"',
            ],
        },
    },
]
FILTERS = {"WHERE": "p.\"indicators\" = 'VALUE_IN_EUROS' AND p.\"freq\" = 'A'", "LAST_N_PERIODS": None}
FLOW_CODES = [1, 2]
INDICATOR_KEYS = ["freq", "flow", "indicators", "TIME_PERIOD", "reporter", "product"]
SCORE_KEYS = ["freq", "flow", "indicators", "TIME_PERIOD", "reporter", "product", "method"]
CONFIG = SynthesisConfig(
    metric_columns=("HHI", "CDI2", "CDI3", "WORLD_HHI", "CENTRALITY_RISK", "CLUSTERING_W"),
    levels=("by_product", "global"),
    min_group_size=3,
    consensus=("borda",),
    consensus_top_n=20,
    methods=(
        MethodSpec(name="rank_mean", kind="rank_mean"),
        MethodSpec(name="mpi", kind="mpi"),
    ),
)


def _registry(root: Path) -> FreshnessRegistry:
    return context_registry(
        {"STATE": {"PATH_TEMPLATE": f"{root.as_posix()}/synthesis/{{TIME_PERIOD}}.json"}},
        None, "synthesis", CONFIG.context_columns,
    )


def _cursors(root: Path) -> StepCursors:
    registry = FreshnessRegistry(
        f"{root.as_posix()}/cursors/{{step}}.json", None, "cursors",
        shard_of=lambda unit: unit.get("step"),
    )
    return StepCursors(registry, "synthesis")


def _synthesise(sources, root: Path, *, settings=IncrementalSettings(), tracker=None):
    return run_incremental_synthesis(
        sources.conn, sources.conn, CONFIG,
        sources=SOURCES, filters=FILTERS, catalog_alias=sources.catalog_alias,
        result_schema="synthesis", diagnostics_schema="synthesis_diagnostics",
        registry=_registry(root),
        requested=synthesis_context_requested(CONFIG, SOURCES, FILTERS, ["import", "export"], None),
        force=ForceSpec(), settings=settings, flow_codes=FLOW_CODES,
        log_artifacts=False, cursors=_cursors(root), tracker=tracker,
    )


def _context(period: str, flow: int) -> Unit:
    return context_unit(CONFIG.context_columns, ("A", flow, "VALUE_IN_EUROS", period))


def _revise_indicator(sources, period: str, flow: int, reporter: str, product: str, value: float) -> None:
    conn, alias = sources.conn, sources.catalog_alias
    row = conn.execute(
        f'SELECT * FROM "{alias}"."indicators"."fact_table" '
        f"WHERE \"TIME_PERIOD\" = '{period}' AND flow = {flow} AND reporter = '{reporter}' "
        f"AND product = '{product}'"
    ).df()
    row["HHI"] = value
    write_dataframe(conn, row, INDICATOR_KEYS, catalog_alias=alias, schema="indicators")


def _assert_equal_to_full_recomputation(sources) -> None:
    """Scores stockés identiques à une synthèse complète sur l'état courant des sources."""
    conn, alias = sources.conn, sources.catalog_alias
    df_source = read_source_metrics(
        conn, build_source_query(SOURCES, FILTERS, alias, FLOW_CODES), ("reporter", "product")
    )
    complete, _, _ = run_synthesis(df_source, CONFIG, log_artifacts=False)
    stored = conn.execute(f'SELECT * FROM "{alias}"."synthesis"."fact_table"').df()
    left = stored[list(complete.columns)].sort_values(SCORE_KEYS).reset_index(drop=True)
    right = complete.sort_values(SCORE_KEYS).reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_dtype=False)


@pytest.mark.slow
def test_cursor_driven_synthesis(synthesis_source_tables, tmp_path: Path, monkeypatch) -> None:
    sources = synthesis_source_tables
    conn, alias = sources.conn, sources.catalog_alias
    old, recent = min(sources.periods), max(sources.periods)
    n_contexts = len(sources.periods) * len(FLOW_CODES)

    # 1. Premier passage : tous les contextes, curseurs initialisés
    first = _synthesise(sources, tmp_path)
    assert len(first.computed) == n_contexts and first.failures == {}
    assert {plan.reason for plan in first.plans.values()} == {"first"}
    cursors = _cursors(tmp_path)
    assert cursors.snapshot(alias, "indicators") == first.pinned
    assert cursors.snapshot(alias, "network_indicators") == first.pinned

    # 2. Aucun changement : rien à recalculer, curseurs avancés
    second = _synthesise(sources, tmp_path)
    assert second.plans == {} and second.computed == []
    assert _cursors(tmp_path).snapshot(alias, "indicators") == second.pinned > first.pinned

    # 3. Révision d'une période ancienne : son seul contexte, identique au complet
    _revise_indicator(sources, old, 1, "FR", sources.products[3], 0.987)
    third = _synthesise(sources, tmp_path)
    assert set(third.computed) == {_context(old, 1)}
    assert {plan.reason for plan in third.plans.values()} == {"pending"}
    _assert_equal_to_full_recomputation(sources)

    # 4. Révision d'une ligne réseau : les contextes de son année et de son sens
    network = conn.execute(
        f'SELECT * FROM "{alias}"."network_indicators"."fact_table" '
        f"WHERE year = {int(recent)} AND flow = 2 LIMIT 1"
    ).df()
    network["WORLD_HHI"] = 0.5
    write_dataframe(conn, network, ["product", "year", "classification", "flow"],
                    catalog_alias=alias, schema="network_indicators")
    fourth = _synthesise(sources, tmp_path)
    assert set(fourth.computed) == {_context(recent, 2)}
    _assert_equal_to_full_recomputation(sources)

    # 5. Suppression de cellules de la grille : contexte recalculé sans elles
    conn.execute(
        f'DELETE FROM "{alias}"."indicators"."fact_table" '
        f"WHERE \"TIME_PERIOD\" = '{recent}' AND flow = 1 AND reporter = 'IT' "
        f"AND product IN ('{sources.products[0]}', '{sources.products[1]}')"
    )
    fifth = _synthesise(sources, tmp_path)
    assert set(fifth.computed) == {_context(recent, 1)}
    stored = conn.execute(
        f'SELECT count(*) FROM "{alias}"."synthesis"."fact_table" '
        f"WHERE \"TIME_PERIOD\" = '{recent}' AND flow = 1 AND reporter = 'IT' "
        f"AND product = '{sources.products[0]}'"
    ).fetchone()[0]
    assert stored == 0
    _assert_equal_to_full_recomputation(sources)

    # 6. Contexte en échec : reste en attente, repris au passage suivant sans changement amont
    _revise_indicator(sources, old, 2, "DE", sources.products[5], 0.123)
    failing = _context(old, 2)
    original = synthesis_module.compute_synthesis_context

    def flaky(df_context, config, **kwargs):
        raise RuntimeError("panne simulée")

    monkeypatch.setattr(synthesis_module, "compute_synthesis_context", flaky)
    failed = _synthesise(sources, tmp_path)
    assert failed.computed == [] and failing.key in failed.failures
    assert _registry(tmp_path).get(failing).pending_since is not None
    monkeypatch.setattr(synthesis_module, "compute_synthesis_context", original)
    resumed = _synthesise(sources, tmp_path)
    assert set(resumed.computed) == {failing}
    assert _registry(tmp_path).get(failing).pending_since is None
    _assert_equal_to_full_recomputation(sources)

    # 7. Snapshots expirés après le curseur : repli complet
    conn.execute(f"CALL ducklake_expire_snapshots('{alias}', older_than => now() + INTERVAL 1 DAY)")
    _revise_indicator(sources, recent, 2, "ES", sources.products[7], 0.42)
    expired = _synthesise(sources, tmp_path)
    assert len(expired.computed) == n_contexts
    _assert_equal_to_full_recomputation(sources)

    # 8. Curseur perdu alors que les contextes sont calculés : repli complet, résultat inchangé
    (tmp_path / "cursors").joinpath("synthesis.json").unlink()
    lost = _synthesise(sources, tmp_path)
    assert len(lost.computed) == n_contexts
    assert {plan.reason for plan in lost.plans.values()} == {"pending"}
    _assert_equal_to_full_recomputation(sources)
    # Puis plus rien à recalculer
    assert _synthesise(sources, tmp_path).plans == {}
