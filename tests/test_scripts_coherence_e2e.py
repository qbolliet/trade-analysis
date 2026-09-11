"""Test de bout en bout de ``scripts/compute_synthesis_coherence.py``.

Contrairement à ``tests/test_scripts_coherence.py`` (helpers purs), ce module
exerce la partie « lecture -> run_coherence -> écriture » (S-2.5, S-2.6) sur un
catalogue DuckLake temporaire (fixture ``synthesis_source_tables`` de
``tests/conftest.py``), sans ``DuckLakeConnector.from_postgres`` ni variable
d'environnement. La cohérence dépendant des scores, la synthèse est d'abord
exécutée via ``scripts.compute_synthetic_scores.run_from_connections`` (même
schéma « synthesis_diagnostics », famille ``fit``), puis la cohérence via
``scripts.compute_synthesis_coherence.run_from_connections`` (familles
``metrics`` et ``methods``). Marqué ``slow``.
"""

from __future__ import annotations

import pandas as pd
import pytest

# Les deux scripts importent ``dt_ducklake_manager`` en tête de module
pytest.importorskip("dt_ducklake_manager")

from scripts.compute_synthetic_scores import (  # noqa: E402
    build_source_query,
    run_from_connections as run_synthesis_from_connections,
)
from scripts.compute_synthesis_coherence import (  # noqa: E402
    run_from_connections as run_coherence_from_connections,
)
from macroforecast.trade.aggregation import (  # noqa: E402
    CoherenceConfig,
    MethodSpec,
    SynthesisConfig,
)


# ──────────────────────────────────────────────────────────────────────
# Configuration et requête source de test (mêmes principes que
# ``tests/test_scripts_synthesis_e2e.py`` ; dupliqués plutôt qu'importés
# d'un module de test à l'autre pour garder chaque fichier autonome)
# ──────────────────────────────────────────────────────────────────────


def _build_synthesis_config() -> SynthesisConfig:
    """Build the ``SynthesisConfig`` the scores are produced with."""
    return SynthesisConfig(
        metric_columns=(
            "HHI", "CDI2", "CDI3", "EXPORT_HHI", "CENTRALITY_RISK", "CLUSTERING_W",
        ),
        levels=("by_product", "by_reporter", "global"),
        min_group_size=3,
        consensus=("borda", "copeland"),
        random_state=0,
        methods=(
            MethodSpec(
                name="pareto", kind="pareto",
                params={
                    "epsilon": 0.1, "epsilon_scale": "mad",
                    "reduce": {"threshold": 0.3}, "layers": True,
                },
            ),
            MethodSpec(name="rank_mean", kind="rank_mean"),
            MethodSpec(
                name="entropy_sum", kind="weighted",
                params={"weighting": "entropy", "aggregation": "weighted_sum"},
            ),
            MethodSpec(
                name="critic_sum", kind="weighted",
                params={
                    "weighting": "critic", "aggregation": "weighted_sum",
                    "weighting_params": {"method": "spearman", "scale": "mad"},
                },
            ),
            MethodSpec(name="mpi", kind="mpi"),
            MethodSpec(
                name="topsis_critic", kind="topsis",
                params={"weighting": "critic", "robust": True},
            ),
            MethodSpec(
                name="vikor_critic", kind="vikor",
                params={"weighting": "critic", "v": 0.5},
            ),
            MethodSpec(
                name="bod", kind="bod",
                params={"rho": 4.0, "restriction": "assurance_region"},
            ),
            MethodSpec(
                name="kantorovich", kind="kantorovich",
                metrics=("HHI", "CDI2", "CDI3", "EXPORT_HHI"),
                params={
                    "epsilon": 0.1, "n_target": 4096, "fit_sample_size": 20_000,
                    "alpha": 0.05, "theta0_degrees": 60.0,
                    "conformal": "split", "alert": "projected",
                },
            ),
        ),
    )


# Cohérence légère : profondeurs top-k réduites (groupes de 4/30/120 lignes),
# LOMO exercé sur une méthode pondérée réellement présente (`critic_sum`)
def _build_coherence_config() -> CoherenceConfig:
    """Build the ``CoherenceConfig`` exercised by the end-to-end test."""
    return CoherenceConfig(
        topk_depths=(5, 10),
        rbo_p=0.9,
        dispute_fraction=0.01,
        lomo=True,
        lomo_methods=("critic_sum",),
        metric_pairs=True,
    )


def _build_query(catalog_alias: str) -> str:
    """Build the S-2.3 source query joining the fixture's two schemas."""
    sources = [
        {"SCHEMA": "indicators", "ALIAS": "p", "COLUMNS": ["HHI", "CDI2", "CDI3"]},
        {
            "SCHEMA": "network_indicators",
            "ALIAS": "n",
            "COLUMNS": ["EXPORT_HHI", "CENTRALITY_RISK", "CLUSTERING_W"],
            "JOIN": {
                "ON": [
                    'substr(p."product", 1, 6) = n."product"',
                    'CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER) = n."year"',
                ],
                "WHERE": "n.\"classification\" = 'HS2022'",
            },
        },
    ]
    filters = {
        "WHERE": (
            'p."flow" = 1 AND p."indicators" = \'VALUE_IN_EUROS\' '
            'AND p."freq" = \'A\''
        ),
        "LAST_N_PERIODS": None,
    }
    return build_source_query(sources, filters, catalog_alias)


def _read_diagnostics(conn, catalog_alias: str) -> pd.DataFrame:
    """Read back the whole ``synthesis_diagnostics`` fact table."""
    from statflows.storage.ducklake.tables import FACT_TABLE

    return conn.execute(
        f'SELECT * FROM "{catalog_alias}"."synthesis_diagnostics"."{FACT_TABLE}"'
    ).df()


# ──────────────────────────────────────────────────────────────────────
# Test de bout en bout
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.slow
def test_coherence_run_from_connections_end_to_end(synthesis_source_tables) -> None:
    """Synthèse puis cohérence, relecture, puis upsert idempotent (S-2.6)."""
    conn = synthesis_source_tables.conn
    catalog_alias = synthesis_source_tables.catalog_alias

    synthesis_config = _build_synthesis_config()
    coherence_config = _build_coherence_config()
    query = _build_query(catalog_alias)

    # Préalable : les scores (schéma « synthesis ») et les diagnostics « fit »
    # (schéma « synthesis_diagnostics ») du script de synthèse
    synth_reports, synth_failures, _, _ = run_synthesis_from_connections(
        conn,
        conn,
        query,
        synthesis_config,
        catalog_alias=catalog_alias,
        result_schema="synthesis",
        diagnostics_schema="synthesis_diagnostics",
        log_artifacts=False,
    )
    assert synth_failures == {}
    assert synth_reports

    reports, failures, created_any, n_contexts = run_coherence_from_connections(
        conn,
        conn,
        query,
        "synthesis",
        synthesis_config,
        coherence_config,
        catalog_alias=catalog_alias,
        diagnostics_schema="synthesis_diagnostics",
        log_artifacts=False,
    )

    assert failures == {}
    assert n_contexts == len(synthesis_source_tables.periods)
    assert len(reports) == n_contexts
    # Le schéma existe déjà (créé par la synthèse) : cet appel n'upserte que
    assert created_any is False

    df_diagnostics = _read_diagnostics(conn, catalog_alias)

    # Les trois familles de S-2.5/S-2.6 sont présentes : « fit » écrite par la
    # synthèse, « metrics »/« methods » par la cohérence
    assert set(df_diagnostics["family"].unique()) == {"metrics", "methods", "fit"}

    # Clé primaire unique (S-2.6)
    context_columns = list(synthesis_config.context_columns)
    diagnostics_keys = [
        *context_columns,
        "level",
        synthesis_config.reporter_col,
        synthesis_config.product_col,
        "family",
        "statistic",
        "item_a",
        "item_b",
    ]
    assert not df_diagnostics.duplicated(subset=diagnostics_keys).any()

    # Ré-exécution : upsert idempotent, aucune ligne créée ni perdue
    reports2, failures2, created_any2, n_contexts2 = run_coherence_from_connections(
        conn,
        conn,
        query,
        "synthesis",
        synthesis_config,
        coherence_config,
        catalog_alias=catalog_alias,
        diagnostics_schema="synthesis_diagnostics",
        log_artifacts=False,
    )
    assert failures2 == {}
    assert n_contexts2 == n_contexts
    assert created_any2 is False

    df_diagnostics_again = _read_diagnostics(conn, catalog_alias)
    left = df_diagnostics.sort_values(diagnostics_keys).reset_index(drop=True)
    right = df_diagnostics_again.sort_values(diagnostics_keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_dtype=False)
