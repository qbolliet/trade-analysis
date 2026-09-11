"""Test de bout en bout de ``scripts/compute_synthetic_scores.py``.

Contrairement à ``tests/test_scripts_synthesis.py`` (helpers purs), ce module
exerce la partie « lecture -> run_synthesis -> écriture » (S-2.3, S-2.4) sur un
catalogue DuckLake temporaire (fixture ``synthesis_source_tables`` de
``tests/conftest.py``), sans ``DuckLakeConnector.from_postgres`` ni variable
d'environnement : ``run_from_connections`` est appelé directement sur la
connexion de la fixture. Marqué ``slow`` (calcul réel sur 240 cellules x 2
contextes, plusieurs méthodes).
"""

from __future__ import annotations

import pandas as pd
import pytest

# Le script importe ``dt_ducklake_manager`` en tête de module
pytest.importorskip("dt_ducklake_manager")

from scripts.compute_synthetic_scores import (  # noqa: E402
    build_source_query,
    run_from_connections,
)
from macroforecast.trade.aggregation import MethodSpec, SynthesisConfig  # noqa: E402
from macroforecast.trade.aggregation.synthesis import score_columns  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Configuration et requête source de test (S-2.2 / S-2.3, allégées)
# ──────────────────────────────────────────────────────────────────────

# Une méthode par famille de S-1.4 (pareto, rang, deux pondérées, MPI, TOPSIS,
# VIKOR, BoD) plus Kantorovitch (toujours sauté ici : `min_group_size=500` >
# `n_global=120`, ce qui exerce le chemin « méthode présente mais sautée »
# sans dépendre de `jax`). `min_group_size=3` abaisse le plancher global pour
# que les groupes `by_product` (4 lignes) soient effectivement scorés par les
# méthodes dont le défaut S-1.4 est <= 3 (pareto, rang, MPI, BoD) ; les
# méthodes à défaut 30 (pondérées, TOPSIS, VIKOR) y restent sautées, comme en
# production, et sont scorées à `by_reporter` (30) et `global` (120).
def _build_synthesis_config() -> SynthesisConfig:
    """Build the ``SynthesisConfig`` exercised by the end-to-end test."""
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


# Requête source de test : jointure indicators/network_indicators de S-2.2, sans
# restriction de périodes (la fixture n'en écrit que deux)
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


def _read_scores(conn, catalog_alias: str) -> pd.DataFrame:
    """Read back the whole ``synthesis`` fact table."""
    from statflows.storage.ducklake.tables import FACT_TABLE

    return conn.execute(
        f'SELECT * FROM "{catalog_alias}"."synthesis"."{FACT_TABLE}"'
    ).df()


# ──────────────────────────────────────────────────────────────────────
# Test de bout en bout
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.slow
def test_synthesis_run_from_connections_end_to_end(synthesis_source_tables) -> None:
    """Lecture -> run_synthesis -> écriture, relecture, puis upsert idempotent."""
    conn = synthesis_source_tables.conn
    catalog_alias = synthesis_source_tables.catalog_alias
    n_reporters = len(synthesis_source_tables.reporters)
    n_products = len(synthesis_source_tables.products)
    n_periods = len(synthesis_source_tables.periods)

    config = _build_synthesis_config()
    query = _build_query(catalog_alias)

    reports, failures, created_any, n_contexts = run_from_connections(
        conn,
        conn,
        query,
        config,
        catalog_alias=catalog_alias,
        result_schema="synthesis",
        diagnostics_schema="synthesis_diagnostics",
        log_artifacts=False,
    )

    # Aucun contexte en échec, un contexte par période
    assert failures == {}
    assert n_contexts == n_periods
    assert len(reports) == n_periods
    assert created_any is True
    assert sum(report.n_cells for report in reports) == (
        n_periods * n_reporters * n_products
    )

    df_scores = _read_scores(conn, catalog_alias)

    # Colonnes de la table des scores (S-2.4)
    assert set(score_columns(config)) == set(df_scores.columns)

    # Clé primaire unique (S-2.4)
    scores_keys = [
        *config.context_columns, config.reporter_col, config.product_col, "method",
    ]
    assert not df_scores.duplicated(subset=scores_keys).any()

    # Chaque méthode configurée (et chaque pseudo-méthode de consensus) est présente
    expected_methods = {spec.name for spec in config.methods} | {
        f"consensus_{rule}" for rule in config.consensus
    }
    assert set(df_scores["method"].unique()) == expected_methods

    # Tailles de groupe : la grille de la fixture est complète (aucune valeur
    # manquante), donc `n_*` vaut la taille du groupe pour toute méthode,
    # sautée ou non (D-15/D-16 : `n` est renseigné avant le test de saut)
    assert (df_scores["n_by_product"] == n_reporters).all()
    assert (df_scores["n_by_reporter"] == n_products).all()
    assert (df_scores["n_global"] == n_reporters * n_products).all()

    # Ré-exécution : upsert idempotent, aucune ligne créée ni perdue
    reports2, failures2, created_any2, n_contexts2 = run_from_connections(
        conn,
        conn,
        query,
        config,
        catalog_alias=catalog_alias,
        result_schema="synthesis",
        diagnostics_schema="synthesis_diagnostics",
        log_artifacts=False,
    )
    assert failures2 == {}
    assert n_contexts2 == n_periods
    assert created_any2 is False

    df_scores_again = _read_scores(conn, catalog_alias)
    left = df_scores.sort_values(scores_keys).reset_index(drop=True)
    right = df_scores_again.sort_values(scores_keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_dtype=False)
