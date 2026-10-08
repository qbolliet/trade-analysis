"""Synthèse et cohérence incrémentales de bout en bout (catalogue DuckLake temporaire).

Scénario sur la grille factice ``synthesis_source_tables`` (deux périodes, deux
flux, donc quatre contextes), registres de fraîcheur sous ``tmp_path`` :

1. première passe : tous les contextes, toutes les méthodes ;
2. seconde passe : aucun contexte ;
3. méthode ajoutée à la configuration : elle seule et le consensus sont écrits,
   sur tous les contextes, avec les valeurs d'un calcul complet ;
4. empreinte d'une métrique partenaire invalidée puis recalculée (raison
   ``fingerprint``, cascade) : tous les contextes ;
5. budget de deux contextes : deux contextes (période la plus récente), puis les
   suivants à la passe d'après.

La cohérence suit : première passe, puis rien, puis seuls les contextes que la
synthèse a recalculés depuis.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("dt_ducklake_manager")

from kedro_pipeline.io.freshness import (  # noqa: E402
    ForceSpec,
    FreshnessRegistry,
    RegistryEntry,
    Unit,
    parse_instant,
)
from macroforecast.trade.aggregation import (  # noqa: E402
    CoherenceConfig,
    MethodSpec,
    SynthesisConfig,
    run_synthesis,
)
from scripts.compute_synthesis_coherence import (  # noqa: E402
    coherence_requested,
    run_incremental_coherence,
)
from scripts.compute_synthetic_scores import (  # noqa: E402
    IncrementalSettings,
    UpstreamMarks,
    build_source_query,
    context_registry,
    read_source_metrics,
    run_incremental_synthesis,
    synthesis_context_requested,
)

# Sources et filtres de la grille factice (jointure réseau sur le millésime HS2022)
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
    },
]
FILTERS = {"WHERE": "p.\"indicators\" = 'VALUE_IN_EUROS' AND p.\"freq\" = 'A'", "LAST_N_PERIODS": None}
FLOW_CODES = [1, 2]
# Instants des calculs partenaires simulés (antérieurs aux passes de synthèse)
T0 = parse_instant("2026-01-01T00:00:00+00:00")
T1 = parse_instant("2026-02-01T00:00:00+00:00")

# Méthodes : rapides, déterministes et aléatoires, consensus Borda et Copeland
_METHODS = (
    MethodSpec(name="pareto", kind="pareto", params={"epsilon": 0.1}),
    MethodSpec(name="rank_mean", kind="rank_mean"),
    MethodSpec(name="critic_sum", kind="weighted", params={"weighting": "critic"}),
    MethodSpec(name="mpi", kind="mpi"),
)
_ADDED = MethodSpec(name="bod", kind="bod", params={"rho": 4.0})


def _config(*extra: MethodSpec) -> SynthesisConfig:
    return SynthesisConfig(
        metric_columns=("HHI", "CDI2", "CDI3", "WORLD_HHI", "CENTRALITY_RISK", "CLUSTERING_W"),
        levels=("by_product", "by_reporter", "global"),
        min_group_size=3,
        consensus=("borda", "copeland"),
        consensus_top_n=20,
        methods=(*_METHODS, *extra),
    )


def _registry(root: Path, step: str) -> FreshnessRegistry:
    """Registre par contexte de l'étape, relu depuis le disque à chaque appel."""
    return context_registry(
        {"STATE": {"PATH_TEMPLATE": f"{root.as_posix()}/{step}/{{TIME_PERIOD}}.json"}},
        None, step, SynthesisConfig().context_columns,
    )


def _partners(root: Path) -> FreshnessRegistry:
    """Registre partenaires simulé (une unité par reporter et produit)."""
    return FreshnessRegistry(
        f"{root.as_posix()}/partners/{{reporter}}.json", None, "partners",
        shard_of=lambda unit: unit.get("reporter"),
    )


def _seed_partners(root: Path, sources) -> None:
    """Unités partenaires calculées pour la première fois en T0."""
    registry = _partners(root)
    for reporter in sources.reporters:
        for product in sources.products:
            registry.upsert(RegistryEntry(
                Unit.of(classification="HS2022", reporter=reporter, product=product), T0,
                upstream_watermark=T0, reason="first",
            ))
    registry.save()


def _marks(root: Path) -> UpstreamMarks:
    return UpstreamMarks.from_entries(
        list(_partners(root).iter_entries()), [], partner_label="HS2022", nomenclatures=None
    )


def _synthesise(sources, root: Path, config: SynthesisConfig, *, settings=IncrementalSettings(),
                force=ForceSpec()):
    """Une passe incrémentale de synthèse."""
    return run_incremental_synthesis(
        sources.conn, sources.conn, config,
        sources=SOURCES, filters=FILTERS, catalog_alias=sources.catalog_alias,
        result_schema="synthesis", diagnostics_schema="synthesis_diagnostics",
        registry=_registry(root, "synthesis"),
        requested=synthesis_context_requested(config, SOURCES, FILTERS, ["import", "export"], None),
        marks=_marks(root), force=force, settings=settings, flow_codes=FLOW_CODES,
        log_artifacts=False,
    )


def _cohere(sources, root: Path, config: SynthesisConfig):
    """Une passe incrémentale de cohérence."""
    coherence = CoherenceConfig(lomo=False)
    return run_incremental_coherence(
        sources.conn, sources.conn, config, coherence,
        sources=SOURCES, filters=FILTERS, catalog_alias=sources.catalog_alias,
        scores_schema="synthesis", diagnostics_schema="synthesis_diagnostics",
        registry=_registry(root, "coherence"),
        synthesis_registry=_registry(root, "synthesis"),
        requested=coherence_requested(coherence), force=ForceSpec(),
        settings=IncrementalSettings(), flow_codes=FLOW_CODES, log_artifacts=False,
    )


def _snapshot(conn, alias: str) -> int:
    return int(conn.execute(f"SELECT max(snapshot_id) FROM ducklake_snapshots('{alias}')").fetchone()[0])


def _changed_methods(conn, alias: str, start: int, end: int) -> set:
    """Méthodes des lignes écrites (insérées ou supprimées) dans ``synthesis`` entre deux snapshots."""
    frame = conn.execute(
        f"SELECT DISTINCT method FROM ducklake_table_changes('{alias}', 'synthesis', 'fact_table', {start + 1}, {end})"
    ).df()
    return set(frame["method"])


def _scores(conn, alias: str) -> pd.DataFrame:
    return conn.execute(f'SELECT * FROM "{alias}"."synthesis"."fact_table"').df()


@pytest.mark.slow
def test_incremental_synthesis_and_coherence(synthesis_source_tables, tmp_path: Path) -> None:
    sources = synthesis_source_tables
    conn, alias = sources.conn, sources.catalog_alias
    n_contexts = len(sources.periods) * len(FLOW_CODES)
    _seed_partners(tmp_path, sources)
    config = _config()

    # 1. Première passe : tous les contextes, toutes les méthodes et le consensus
    first = _synthesise(sources, tmp_path, config)
    assert first.failures == {}
    assert len(first.computed) == n_contexts and first.backlog == 0
    assert {plan.reason for plan in first.plans.values()} == {"first"}
    stored = _scores(conn, alias)
    assert set(stored["method"]) == {spec.name for spec in _METHODS} | {
        "consensus_borda", "consensus_copeland"
    }
    coherence = _cohere(sources, tmp_path, config)
    assert coherence.failures == {} and len(coherence.computed) == n_contexts

    # 2. Seconde passe : rien n'est périmé (synthèse comme cohérence)
    second = _synthesise(sources, tmp_path, config)
    assert second.plans == {} and second.computed == []
    assert _cohere(sources, tmp_path, config).computed == []

    # 3. Méthode ajoutée : elle seule et le consensus, sur tous les contextes
    extended = _config(_ADDED)
    before = _snapshot(conn, alias)
    third = _synthesise(sources, tmp_path, extended)
    after = _snapshot(conn, alias)
    assert third.failures == {} and len(third.computed) == n_contexts
    assert {plan.reason for plan in third.plans.values()} == {"fingerprint"}
    assert {plan.names for plan in third.plans.values()} == {frozenset({"bod", "consensus"})}
    assert _changed_methods(conn, alias, before, after) <= {
        "bod", "consensus_borda", "consensus_copeland"
    }
    # Valeurs identiques à un calcul complet avec la méthode ajoutée
    df_source = read_source_metrics(
        conn, build_source_query(SOURCES, FILTERS, alias, FLOW_CODES), ("reporter", "product")
    )
    complete, _, _ = run_synthesis(df_source, extended, log_artifacts=False)
    keys = ["freq", "flow", "indicators", "TIME_PERIOD", "reporter", "product", "method"]
    stored = _scores(conn, alias)
    # Méthodes non recalculées inchangées, méthode ajoutée et consensus égaux au complet
    for methods in (["pareto", "mpi"], ["bod", "consensus_borda", "consensus_copeland"]):
        left = stored[stored["method"].isin(methods)][list(complete.columns)].sort_values(keys)
        right = complete[complete["method"].isin(methods)].sort_values(keys)
        pd.testing.assert_frame_equal(
            left.reset_index(drop=True), right.reset_index(drop=True), check_dtype=False
        )
    # La cohérence suit : tous les contextes ont été resynthétisés
    assert len(_cohere(sources, tmp_path, extended).computed) == n_contexts

    # 4. Empreinte partenaire invalidée puis recalculée (cascade, raison fingerprint)
    partners = _partners(tmp_path)
    partners.upsert(RegistryEntry(
        Unit.of(classification="HS2022", reporter="FR", product=sources.products[0]), T1,
        upstream_watermark=T1, reason="fingerprint",
    ))
    partners.save()
    fourth = _synthesise(sources, tmp_path, extended)
    assert len(fourth.computed) == n_contexts
    assert {plan.reason for plan in fourth.plans.values()} == {"new_data"}
    assert all(plan.names >= {"bod", "mpi", "consensus", "synthesis"} for plan in fourth.plans.values())

    # Forçage d'une période : la cohérence ne reprend que les contextes resynthétisés
    _cohere(sources, tmp_path, extended)
    forced = _synthesise(
        sources, tmp_path, extended,
        force=ForceSpec(steps=frozenset({"synthesis"}), periods=(sources.periods[-1],)),
    )
    assert {unit.get("TIME_PERIOD") for unit in forced.computed} == {sources.periods[-1]}
    followed = _cohere(sources, tmp_path, extended)
    assert {unit.get("TIME_PERIOD") for unit in followed.computed} == {sources.periods[-1]}
    assert len(followed.computed) == len(FLOW_CODES)


@pytest.mark.slow
def test_budget_defers_the_oldest_contexts(synthesis_source_tables, tmp_path: Path) -> None:
    sources = synthesis_source_tables
    _seed_partners(tmp_path, sources)
    config = _config()
    budget = IncrementalSettings(max_contexts=2, write_batch_contexts=1)

    # Passe 1 : deux contextes, ceux de la période la plus récente
    first = _synthesise(sources, tmp_path, config, settings=budget)
    assert len(first.computed) == 2 and first.backlog == 2
    assert {unit.get("TIME_PERIOD") for unit in first.computed} == {max(sources.periods)}
    # Passe 2 : les deux suivants ; passe 3 : rien
    second = _synthesise(sources, tmp_path, config, settings=budget)
    assert len(second.computed) == 2 and second.backlog == 0
    assert {unit.get("TIME_PERIOD") for unit in second.computed} == {min(sources.periods)}
    assert _synthesise(sources, tmp_path, config, settings=budget).computed == []
