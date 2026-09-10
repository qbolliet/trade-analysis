"""Tests du runner de synthèse (``macroforecast.trade.aggregation.synthesis``).

``run_synthesis`` est une fonction pure : table de cellules → table longue de
scores (S-2.4), diagnostics d'ajustement (S-2.5.c) et rapport (S-2.7). Les
tests vérifient le contrat de sortie (colonnes, clé primaire, sentinelles), les
règles de saut (D-15, ``min_group_size``) et la cohérence rang / score par
groupe (D-16). Aucun test ne dépend de ``jax``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from macroforecast.trade.aggregation.methods import MethodSpec
from macroforecast.trade.aggregation.synthesis import (
    CONSENSUS_PREFIX,
    GROUP_SENTINEL,
    ITEM_SENTINEL,
    LEVELS,
    SynthesisConfig,
    group_keys,
    iter_groups,
    run_synthesis,
    score_columns,
)

# Clé primaire de la table des scores (S-2.4)
PRIMARY_KEY = [
    "freq",
    "flow",
    "indicators",
    "TIME_PERIOD",
    "reporter",
    "product",
    "method",
]

# Configuration réduite : huit méthodes, tirages courts, bootstrap au global
_METHODS = (
    MethodSpec(name="pareto", kind="pareto", params={"epsilon": 0.1}),
    MethodSpec(name="rank_mean", kind="rank_mean"),
    MethodSpec(name="critic_sum", kind="weighted", params={"weighting": "critic"}),
    MethodSpec(name="auto_sum", kind="weighted", params={"weighting": "auto"}),
    MethodSpec(name="bod", kind="bod"),
    MethodSpec(name="smaa", kind="smaa"),
    MethodSpec(name="cone_quantile", kind="cone_quantile"),
    MethodSpec(name="mpi", kind="mpi"),
)


@pytest.fixture
def _config() -> SynthesisConfig:
    """Configuration de synthèse réduite, déterministe et rapide."""
    return SynthesisConfig(
        metric_columns=("HHI", "CDI2", "CDI3", "EXPORT_HHI"),
        methods=_METHODS,
        min_group_size=3,
        smaa_n_draws=64,
        smaa_k=5,
        consensus=("borda", "copeland"),
        consensus_top_n=20,
        bootstrap_methods=("critic_sum",),
        bootstrap_levels=("global",),
        bootstrap_n=3,
    )


@pytest.fixture
def _run(df_synthesis_toy: pd.DataFrame, _config: SynthesisConfig):
    """Exécution unique de la synthèse, partagée par les tests."""
    return run_synthesis(df_synthesis_toy, _config)


# ──────────────────────────────────────────────────────────────────────
# Groupes et colonnes
# ──────────────────────────────────────────────────────────────────────


def test_group_keys_and_levels() -> None:
    """Un groupe par produit, par pays, puis un groupe unique (D-09)."""
    config = SynthesisConfig()
    assert LEVELS == ("by_product", "by_reporter", "global")
    assert group_keys("by_product", config) == ("product",)
    assert group_keys("by_reporter", config) == ("reporter",)
    assert group_keys("global", config) == ()
    with pytest.raises(ValueError, match="Unknown level"):
        group_keys("by_country", config)


def test_iter_groups_partitions_the_context(
    df_synthesis_toy: pd.DataFrame, _config: SynthesisConfig
) -> None:
    """Les groupes d'un niveau partitionnent exactement les lignes du contexte."""
    df_context = df_synthesis_toy[df_synthesis_toy["TIME_PERIOD"] == "2023"]
    for level in LEVELS:
        positions = np.concatenate(
            [group for _, group in iter_groups(df_context, _config, level)]
        )
        assert sorted(positions.tolist()) == list(range(len(df_context)))
    # Niveau global : un groupe unique portant la sentinelle
    groups = list(iter_groups(df_context, _config, "global"))
    assert len(groups) == 1 and groups[0][0] == GROUP_SENTINEL


# ──────────────────────────────────────────────────────────────────────
# Contrat de sortie (S-2.4)
# ──────────────────────────────────────────────────────────────────────


def test_scores_carry_the_s24_columns(_run, _config: SynthesisConfig) -> None:
    """Colonnes exactement celles de S-2.4, dans l'ordre."""
    df_scores, _, _ = _run
    assert list(df_scores.columns) == list(score_columns(_config))
    for level in LEVELS:
        assert str(df_scores[f"n_{level}"].dtype) == "Int64"
        assert str(df_scores[f"alert_{level}"].dtype) == "boolean"


def test_primary_key_is_unique(_run, df_synthesis_toy: pd.DataFrame) -> None:
    """Une ligne par cellule x méthode, clé primaire unique (S-2.4)."""
    df_scores, _, _ = _run
    assert not df_scores.duplicated(PRIMARY_KEY).any()
    n_methods = len(_METHODS) + 2  # deux pseudo-méthodes de consensus
    assert len(df_scores) == len(df_synthesis_toy) * n_methods


def test_consensus_pseudo_methods_are_present(_run) -> None:
    """``consensus_borda`` et ``consensus_copeland`` figurent dans la table."""
    df_scores, _, _ = _run
    methods = set(df_scores["method"])
    assert f"{CONSENSUS_PREFIX}borda" in methods
    assert f"{CONSENSUS_PREFIX}copeland" in methods
    assert f"{CONSENSUS_PREFIX}kemeny" not in methods
    # Score du consensus : opposé du rang (D-16)
    consensus = df_scores[df_scores["method"] == f"{CONSENSUS_PREFIX}borda"]
    scored = consensus.dropna(subset=["rank_global"])
    assert not scored.empty
    assert np.allclose(scored["score_global"], -scored["rank_global"])


def test_rank_matches_score_within_every_group(_run, _config: SynthesisConfig) -> None:
    """Par groupe et par méthode, ``rank = rankdata(-score)`` (D-16)."""
    df_scores, _, _ = _run
    context = list(_config.context_columns)
    for level in LEVELS:
        keys = context + list(group_keys(level, _config)) + ["method"]
        for _, frame in df_scores.groupby(keys, sort=False):
            scored = frame.dropna(subset=[f"score_{level}"])
            if scored.empty:
                continue
            expected = stats.rankdata(-scored[f"score_{level}"].to_numpy())
            assert np.allclose(scored[f"rank_{level}"].to_numpy(), expected)


def test_incomplete_rows_are_not_scored(
    _run, df_synthesis_toy: pd.DataFrame, _config: SynthesisConfig
) -> None:
    """D-15 : une cellule incomplète reçoit ``NaN`` en score et en rang."""
    df_scores, df_fit, _ = _run
    incomplete = df_synthesis_toy[df_synthesis_toy["EXPORT_HHI"].isna()]
    assert not incomplete.empty
    merged = df_scores.merge(incomplete[PRIMARY_KEY[:-1]], on=PRIMARY_KEY[:-1])
    real_methods = merged[~merged["method"].str.startswith(CONSENSUS_PREFIX)]
    for level in LEVELS:
        assert real_methods[f"score_{level}"].isna().all()
        assert real_methods[f"rank_{level}"].isna().all()
    # Le nombre de lignes exclues est rapporté (S-2.5.c)
    assert "n_skipped_incomplete" in set(df_fit["statistic"])


def test_group_size_columns_count_the_scored_rows(_run) -> None:
    """``n_by_reporter`` compte les lignes effectivement scorées par la méthode."""
    df_scores, _, _ = _run
    mpi = df_scores[
        (df_scores["method"] == "mpi")
        & (df_scores["TIME_PERIOD"] == "2023")
        & (df_scores["reporter"] == "FR")
    ]
    scored = mpi["score_by_reporter"].notna().sum()
    assert set(mpi["n_by_reporter"].dropna().unique()) == {scored}


def test_method_below_min_group_size_is_skipped(_run) -> None:
    """Sous ``min_group_size``, la méthode est sautée avec sa ligne de diagnostic."""
    df_scores, df_fit, report = _run
    # `critic_sum` (défaut S-1.4 : 30) ne peut pas être ajustée sur 3 pays
    critic = df_scores[df_scores["method"] == "critic_sum"]
    assert critic["score_by_product"].isna().all()
    assert critic["n_by_product"].notna().all()
    skipped = df_fit[df_fit["statistic"] == "skipped_min_group_size"]
    assert "critic_sum" in set(skipped["item_a"])
    assert report.levels["by_product"].n_methods_skipped > 0
    # Au niveau global (60 cellules), la même méthode est bien ajustée
    assert critic["score_global"].notna().any()


def test_alert_column_is_nullable_boolean(_run) -> None:
    """Seules les méthodes définissant une alerte renseignent ``alert_*``."""
    df_scores, _, _ = _run
    pareto = df_scores[df_scores["method"] == "pareto"]
    assert pareto["alert_global"].notna().any()
    mpi = df_scores[df_scores["method"] == "mpi"]
    assert mpi["alert_global"].isna().all()


def test_bootstrap_bounds_are_filled_where_requested(_run) -> None:
    """D-08 : bornes de rang renseignées pour les méthodes et niveaux demandés."""
    df_scores, _, _ = _run
    critic = df_scores[df_scores["method"] == "critic_sum"]
    bounded = critic.dropna(subset=["rank_low_global"])
    assert not bounded.empty
    assert (bounded["rank_low_global"] <= bounded["rank_high_global"]).all()
    # Niveaux hors `bootstrap_levels` : aucune borne
    assert critic["rank_low_by_reporter"].isna().all()


# ──────────────────────────────────────────────────────────────────────
# Diagnostics (S-2.6) et rapport (S-2.7)
# ──────────────────────────────────────────────────────────────────────


def test_fit_diagnostics_shape_and_sentinels(_run, _config: SynthesisConfig) -> None:
    """Format long S-2.6, famille ``fit``, sentinelles ``'ALL'`` et ``''``."""
    _, df_fit, _ = _run
    expected = [
        *_config.context_columns,
        "level",
        "reporter",
        "product",
        "family",
        "statistic",
        "item_a",
        "item_b",
        "value",
        "n",
    ]
    assert list(df_fit.columns) == expected
    assert set(df_fit["family"]) == {"fit"}
    assert not df_fit[["reporter", "product", "item_a", "item_b"]].isna().any().any()
    # Groupe absent : sentinelle 'ALL' ; objet absent : sentinelle ''
    at_product = df_fit[df_fit["level"] == "by_product"]
    assert set(at_product["reporter"]) == {GROUP_SENTINEL}
    weights = df_fit[df_fit["statistic"] == "weight"]
    assert not weights.empty and (weights["item_b"] != ITEM_SENTINEL).all()
    assert (df_fit[df_fit["statistic"] == "kmo"]["item_b"] == ITEM_SENTINEL).all()


def test_auto_selection_is_reported(_run) -> None:
    """La méta-pondération (A-05) persiste son schéma retenu."""
    _, df_fit, _ = _run
    selections = df_fit[df_fit["statistic"].str.startswith("auto_selected__")]
    assert not selections.empty
    assert (selections["value"] == 1.0).all()
    assert set(selections["item_a"]) == {"auto_sum"}


def test_report_metrics_are_finite(_run) -> None:
    """``to_metrics()`` ne renvoie ni ``NaN`` ni infini (contrainte MLflow)."""
    _, _, report = _run
    metrics = report.to_metrics()
    assert metrics["synthesis.n_contexts"] == 2.0
    assert all(np.isfinite(value) for value in metrics.values())
    for level in LEVELS:
        assert metrics[f"synthesis.{level}.n_groups"] > 0
        assert f"synthesis.{level}.seconds_mpi" in metrics


def test_runner_is_pure(df_synthesis_toy: pd.DataFrame, _config: SynthesisConfig) -> None:
    """Le runner ne modifie pas sa table d'entrée et reste reproductible."""
    before = df_synthesis_toy.copy()
    first, _, _ = run_synthesis(df_synthesis_toy, _config, log_artifacts=False)
    pd.testing.assert_frame_equal(df_synthesis_toy, before)
    second, _, _ = run_synthesis(df_synthesis_toy, _config, log_artifacts=False)
    pd.testing.assert_frame_equal(first, second)
