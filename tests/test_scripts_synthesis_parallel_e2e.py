"""Synthèse et cohérence parallèles de bout en bout (catalogue DuckLake local SQLite).

Le catalogue a ses métadonnées dans un fichier SQLite : plusieurs processus peuvent
l'ouvrir en même temps (un catalogue DuckDB fichier n'admet qu'un processus). Chaque
scénario écrit dans ses propres schémas résultat du même catalogue, puis on compare les
tables :

* ``n_jobs=1`` et ``n_jobs=2`` produisent des tables strictement égales (méthodes
  aléatoires SMAA et bootstrap comprises) ;
* une méthode déclarée séquentielle (calculée dans le parent, consensus en dernier)
  donne les mêmes tables que le calcul entièrement parallèle ;
* l'échec d'un contexte n'empêche pas les autres.
"""

from __future__ import annotations

from functools import partial
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("dt_ducklake_manager")

from kedro_pipeline.io.ducklake import ConnectionReader  # noqa: E402
from tests.sqlite_catalog import (  # noqa: E402
    ALIAS,
    SqliteCatalogConnector,
    catalog_paths,
    open_catalog,
)
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
)
from kedro_pipeline.steps import synthesis as compute_synthetic_scores  # noqa: E402
from scripts.compute_synthesis_coherence import (  # noqa: E402
    coherence_requested,
    run_incremental_coherence,
)
from scripts.compute_synthetic_scores import (  # noqa: E402
    IncrementalSettings,
    UpstreamMarks,
    context_registry,
    run_incremental_synthesis,
    synthesis_context_requested,
)

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
T0 = parse_instant("2026-01-01T00:00:00+00:00")

# Méthodes : déterministes, aléatoires (SMAA, quantile de cône) et bootstrappées
_METHODS = (
    MethodSpec(name="rank_mean", kind="rank_mean"),
    MethodSpec(name="critic_sum", kind="weighted", params={"weighting": "critic"}),
    MethodSpec(name="mpi", kind="mpi"),
    MethodSpec(name="smaa", kind="smaa", params={"aggregation": "weighted_sum"}),
    MethodSpec(name="cone_quantile", kind="cone_quantile"),
)


@pytest.fixture
def ducklake_conn(tmp_path: Path):
    """Remplace la fixture de la suite : catalogue à métadonnées SQLite, multi-processus."""
    (tmp_path / "data").mkdir()
    conn = open_catalog(*catalog_paths(tmp_path))
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {ALIAS}.s1")
    conn.execute(f"USE {ALIAS}.main")
    try:
        yield conn, ALIAS
    finally:
        conn.close()


def _config(methods=_METHODS) -> SynthesisConfig:
    return SynthesisConfig(
        metric_columns=("HHI", "CDI2", "CDI3", "WORLD_HHI", "CENTRALITY_RISK", "CLUSTERING_W"),
        levels=("by_product", "by_reporter", "global"),
        min_group_size=3,
        consensus=("borda", "copeland"),
        consensus_top_n=20,
        smaa_n_draws=60,
        bootstrap_methods=("critic_sum",),
        bootstrap_n=6,
        bootstrap_levels=("by_product", "by_reporter"),
        random_state=3,
        methods=methods,
    )


def _registry(root: Path, step: str) -> FreshnessRegistry:
    return context_registry(
        {"STATE": {"PATH_TEMPLATE": f"{root.as_posix()}/{step}/{{TIME_PERIOD}}.json"}},
        None, step, SynthesisConfig().context_columns,
    )


def _marks(root: Path, sources) -> UpstreamMarks:
    registry = FreshnessRegistry(
        f"{root.as_posix()}/partners/{{reporter}}.json", None, "partners",
        shard_of=lambda unit: unit.get("reporter"),
    )
    for reporter in sources.reporters:
        for product in sources.products:
            registry.upsert(RegistryEntry(
                Unit.of(classification="HS2022", reporter=reporter, product=product), T0,
                upstream_watermark=T0, reason="first",
            ))
    registry.save()
    return UpstreamMarks.from_entries(
        list(registry.iter_entries()), [], partner_label="HS2022", nomenclatures=None
    )


def _reader(tmp_path: Path) -> ConnectionReader:
    """Lecteur des workers : leur propre connexion sur le catalogue de la fixture."""
    return ConnectionReader(partial(SqliteCatalogConnector, *catalog_paths(tmp_path)))


def _synthesise(sources, root: Path, name: str, *, n_jobs: int, sequential=(), config=None):
    """Une passe de synthèse dans les schémas ``name`` (scores) et ``name_diag`` (fit)."""
    config = config or _config()
    reader = _reader(root)
    root = root / name
    return run_incremental_synthesis(
        sources.conn, sources.conn, config,
        sources=SOURCES, filters=FILTERS, catalog_alias=sources.catalog_alias,
        result_schema=name, diagnostics_schema=f"{name}_diag",
        registry=_registry(root, "synthesis"),
        requested=synthesis_context_requested(config, SOURCES, FILTERS, ["import", "export"], None),
        marks=_marks(root, sources), force=ForceSpec(),
        settings=IncrementalSettings(
            n_jobs=n_jobs, sequential_methods=tuple(sequential), write_batch_contexts=3
        ),
        flow_codes=FLOW_CODES, log_artifacts=False, reader=reader,
    )


def _cohere(sources, root: Path, name: str, *, n_jobs: int, scores_schema: str):
    config = _config()
    coherence = CoherenceConfig(lomo=True, lomo_methods=("critic_sum",))
    reader = _reader(root)
    root = root / name
    return run_incremental_coherence(
        sources.conn, sources.conn, config, coherence,
        sources=SOURCES, filters=FILTERS, catalog_alias=sources.catalog_alias,
        scores_schema=scores_schema, diagnostics_schema=f"{name}_diag",
        registry=_registry(root, "coherence"),
        synthesis_registry=_registry(root.parent / scores_schema, "synthesis"),
        requested=coherence_requested(coherence), force=ForceSpec(),
        settings=IncrementalSettings(n_jobs=n_jobs, write_batch_contexts=3),
        flow_codes=FLOW_CODES, log_artifacts=False, reader=reader,
    )


def _table(sources, schema: str) -> pd.DataFrame:
    frame = sources.conn.execute(
        f'SELECT * FROM "{sources.catalog_alias}"."{schema}"."fact_table"'
    ).df()
    keys = [column for column in frame.columns if frame[column].dtype == object or column == "flow"]
    return frame.sort_values(keys, kind="mergesort").reset_index(drop=True)


def _assert_same(sources, left: str, right: str) -> None:
    pd.testing.assert_frame_equal(
        _table(sources, left), _table(sources, right), check_exact=True
    )


@pytest.mark.slow
def test_scores_identical_for_one_and_two_processes(synthesis_source_tables, tmp_path):
    sources = synthesis_source_tables
    n_contexts = len(sources.periods) * len(FLOW_CODES)
    assert n_contexts >= 4

    one = _synthesise(sources, tmp_path, "j1", n_jobs=1)
    two = _synthesise(sources, tmp_path, "j2", n_jobs=2)
    assert one.failures == {} and two.failures == {}
    assert len(one.computed) == len(two.computed) == n_contexts

    stored = _table(sources, "j1")
    assert {"smaa", "cone_quantile", "consensus_borda"} <= set(stored["method"])
    _assert_same(sources, "j1", "j2")
    _assert_same(sources, "j1_diag", "j2_diag")

    # Cohérence : mêmes tables entre un et deux processus, sur les mêmes scores
    c1 = _cohere(sources, tmp_path, "c1", n_jobs=1, scores_schema="j1")
    c2 = _cohere(sources, tmp_path, "c2", n_jobs=2, scores_schema="j1")
    assert c1.failures == {} and c2.failures == {}
    assert len(c1.computed) == len(c2.computed) == n_contexts
    _assert_same(sources, "c1_diag", "c2_diag")


@pytest.mark.slow
def test_sequential_method_gives_the_same_tables(synthesis_source_tables, tmp_path):
    sources = synthesis_source_tables
    _synthesise(sources, tmp_path, "full", n_jobs=1)
    seq = _synthesise(sources, tmp_path, "seq", n_jobs=2, sequential=("mpi", "smaa"))
    assert seq.failures == {}
    assert len(seq.computed) == len(sources.periods) * len(FLOW_CODES)
    _assert_same(sources, "full", "seq")
    _assert_same(sources, "full_diag", "seq_diag")
    # Registre avancé une seule fois par contexte, pas de rapport compté deux fois
    assert len(seq.reports) == len(seq.computed)


@pytest.mark.slow
def test_unknown_sequential_method_is_refused(synthesis_source_tables, tmp_path):
    with pytest.raises(ValueError, match="SEQUENTIAL_METHODS"):
        _synthesise(synthesis_source_tables, tmp_path, "bad", n_jobs=1, sequential=("nope",))


@pytest.mark.slow
def test_failing_context_does_not_stop_the_others(synthesis_source_tables, tmp_path, monkeypatch):
    sources = synthesis_source_tables
    real = compute_synthetic_scores.compute_synthesis_context

    def flaky(df_context, config, **kwargs):
        if str(df_context["TIME_PERIOD"].iloc[0]) == "2022" and int(df_context["flow"].iloc[0]) == 1:
            raise RuntimeError("contexte en échec")
        return real(df_context, config, **kwargs)

    monkeypatch.setattr(compute_synthetic_scores, "compute_synthesis_context", flaky)
    outcome = _synthesise(sources, tmp_path, "flaky", n_jobs=1)
    assert len(outcome.failures) == 1
    assert isinstance(next(iter(outcome.failures.values())), RuntimeError)
    assert len(outcome.computed) == len(sources.periods) * len(FLOW_CODES) - 1
    written = _table(sources, "flaky")
    assert set(zip(written["TIME_PERIOD"].astype(str), written["flow"])) == {
        (period, flow) for period in sources.periods for flow in FLOW_CODES
    } - {("2022", 1)}


def test_parallel_run_without_reader_is_refused(synthesis_source_tables, tmp_path):
    sources = synthesis_source_tables
    config = _config()
    with pytest.raises(ValueError, match="reader"):
        run_incremental_synthesis(
            sources.conn, sources.conn, config,
            sources=SOURCES, filters=FILTERS, catalog_alias=sources.catalog_alias,
            result_schema="x", diagnostics_schema="x_diag",
            registry=_registry(tmp_path, "synthesis"),
            requested=synthesis_context_requested(config, SOURCES, FILTERS, ["import", "export"], None),
            marks=_marks(tmp_path, sources), force=ForceSpec(),
            settings=IncrementalSettings(n_jobs=2), flow_codes=FLOW_CODES,
        )
