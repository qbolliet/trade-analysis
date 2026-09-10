"""Tests du protocole de comparaison et des classements consensus.

Module ``macroforecast.trade.aggregation.diagnostics`` : concordance (τ_b, τ
pondéré, W), recouvrement de tête (RBO), cohérence avec la dominance
(inversions strictes contre ex æquo, rang des membres du front), stabilité
(bootstrap sur la population complète, SMAA à stockage ``(n, k)``), consensus
(Borda, Copeland présélectionné, Kemeny) et rapport de cohérence.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from macroforecast.trade.aggregation.base import AggregationConfig
from macroforecast.trade.aggregation.estimators import WeightedAggregator
from macroforecast.trade.aggregation.functions import weighted_sum_score
from macroforecast.trade.aggregation.diagnostics import (
    bootstrap_rank_stability,
    borda_rank,
    compute_coherence_report,
    copeland_rank,
    dominance_violation_rate,
    front_rank_summary,
    kemeny_rank,
    kendall_tau_b_matrix,
    kendall_w,
    leave_one_metric_out,
    rank_biased_overlap,
    smaa_rank_acceptability,
    topk_overlap,
    weighted_tau_matrix,
)


# ──────────────────────────────────────────────────────────────────────
# Q1 — Concordance globale
# ──────────────────────────────────────────────────────────────────────


def test_kendall_tau_matrix_is_symmetric_with_unit_diagonal() -> None:
    """τ_b : matrice symétrique, diagonale 1."""
    rng = np.random.default_rng(0)
    scores = {name: rng.random(30) for name in ("a", "b", "c")}
    matrix = kendall_tau_b_matrix(scores)
    np.testing.assert_allclose(np.diag(matrix), 1.0)
    np.testing.assert_allclose(matrix.to_numpy(), matrix.to_numpy().T)
    assert matrix.loc["a", "b"] == matrix.loc["b", "a"]


def test_kendall_w_is_one_for_identical_orderings() -> None:
    """Des scores induisant le même ordre donnent une concordance parfaite ``W = 1``."""
    scores = {
        "a": np.array([3.0, 2.0, 1.0, 0.0]),
        "b": np.array([9.0, 4.0, 2.0, 1.0]),
        "c": np.array([100.0, 30.0, 20.0, 5.0]),
    }
    assert kendall_w(scores) == pytest.approx(1.0)


def test_kendall_w_is_low_for_opposed_orderings() -> None:
    """Deux classements strictement inversés : concordance nulle."""
    scores = {"a": np.array([1.0, 2.0, 3.0, 4.0]), "b": np.array([4.0, 3.0, 2.0, 1.0])}
    assert kendall_w(scores) == pytest.approx(0.0, abs=1e-9)


# ──────────────────────────────────────────────────────────────────────
# Q2 — Concordance en tête
# ──────────────────────────────────────────────────────────────────────


def test_topk_overlap_counts_shared_head() -> None:
    """Recouvrement de Jaccard du top-k (indépendant de l'ordre interne)."""
    assert topk_overlap(["a", "b", "c"], ["b", "a", "d"], k=2) == 1.0
    assert topk_overlap(["a", "b", "c"], ["d", "e", "a"], k=2) == 0.0


def test_rbo_identical_rankings_match_truncated_formula() -> None:
    """RBO(A, A) tronqué à la profondeur ``|A|`` vaut ``1 - p**|A|``."""
    ranking = list("abcdefgh")
    depth = len(ranking)
    for p in (0.5, 0.8, 0.9, 0.98):
        assert rank_biased_overlap(ranking, ranking, p=p) == pytest.approx(1.0 - p ** depth)


def test_rbo_is_lower_for_a_swapped_head() -> None:
    """Une inversion en tête fait chuter le RBO sous celui de l'accord parfait."""
    a = list("abcde")
    b = list("bacde")
    assert rank_biased_overlap(b, a, p=0.9) < rank_biased_overlap(a, a, p=0.9)


# ──────────────────────────────────────────────────────────────────────
# Q3 — Cohérence avec la dominance
# ──────────────────────────────────────────────────────────────────────


def test_dominance_violation_rate_zero_when_score_respects_order() -> None:
    """Un score qui respecte la dominance ne viole rien ; le score opposé
    inverse strictement toutes les paires comparables."""
    X = np.array([[2.0, 2.0], [1.0, 1.0]])
    respectful = dominance_violation_rate(X, np.array([2.0, 1.0]))
    assert (respectful.strict, respectful.tie, respectful.n_pairs) == (0.0, 0.0, 1)
    reversed_score = dominance_violation_rate(X, np.array([1.0, 2.0]))
    assert (reversed_score.strict, reversed_score.tie) == (1.0, 0.0)


def test_dominance_violation_rate_separates_ties_from_inversions() -> None:
    """M-10 : un ex æquo entre deux produits comparables alimente ``tie``, pas
    ``strict`` ; les deux taux se partagent les paires dominantes."""
    # Trois points totalement ordonnés : (0, 1), (0, 2) et (1, 2) sont dominantes
    X = np.array([[3.0, 3.0], [2.0, 2.0], [1.0, 1.0]])
    rates = dominance_violation_rate(X, np.array([1.0, 1.0, 0.0]))
    assert rates.n_pairs == 3
    # Seule la paire (0, 1) est ex æquo, les deux autres sont respectées
    assert rates.tie == pytest.approx(1 / 3)
    assert rates.strict == 0.0


def test_dominance_violation_rate_nan_without_comparable_pair() -> None:
    """Relation de dominance vide : les taux ne sont pas définis (``NaN``)."""
    X = np.array([[1.0, 0.0], [0.0, 1.0]])
    rates = dominance_violation_rate(X, np.array([0.3, 0.7]))
    assert np.isnan(rates.strict) and np.isnan(rates.tie)
    assert rates.n_pairs == 0


def test_dominance_violation_rate_sampling_matches_exact_computation() -> None:
    """Au-dessus du seuil, l'estimation par échantillon de paires retrouve le
    taux exact (ici 1, le score étant l'opposé de la somme)."""
    rng = np.random.default_rng(0)
    X = rng.random((300, 2))
    scores = -X.sum(axis=1)
    exact = dominance_violation_rate(X, scores)
    sampled = dominance_violation_rate(
        X, scores, large_n_threshold=100, n_pairs_sample=20_000, random_state=0
    )
    assert exact.strict == pytest.approx(1.0)
    assert sampled.strict == pytest.approx(exact.strict, abs=0.02)
    assert 0 < sampled.n_pairs < exact.n_pairs


def test_front_rank_summary_reads_front_members_ranks() -> None:
    """Rang médian et rang maximal des membres du front ; front vide → ``NaN``."""
    mask = np.array([True, False, True, False])
    median_rank, max_rank = front_rank_summary(mask, np.array([0.9, 0.4, 0.7, 0.1]))
    assert (median_rank, max_rank) == (1.5, 2.0)
    assert all(np.isnan(v) for v in front_rank_summary(np.zeros(4, dtype=bool), np.arange(4.0)))


def test_weighted_tau_matrix_is_symmetric_and_penalises_head_swaps() -> None:
    """A-06 : matrice symétrique de diagonale 1 ; un désaccord en tête pèse plus
    lourd qu'un désaccord en queue."""
    base = np.arange(20.0)[::-1]
    head_swap = base.copy()
    head_swap[[0, 1]] = head_swap[[1, 0]]
    tail_swap = base.copy()
    tail_swap[[18, 19]] = tail_swap[[19, 18]]
    matrix = weighted_tau_matrix({"base": base, "head": head_swap, "tail": tail_swap})
    np.testing.assert_allclose(np.diag(matrix), 1.0)
    np.testing.assert_allclose(matrix.to_numpy(), matrix.to_numpy().T)
    assert matrix.loc["base", "head"] < matrix.loc["base", "tail"]


# ──────────────────────────────────────────────────────────────────────
# Q4 — Stabilité
# ──────────────────────────────────────────────────────────────────────


def test_smaa_rank_acceptability_is_stored_on_k_ranks_only() -> None:
    """I-09 : la matrice d'acceptabilité est ``(n, k)`` et chaque ligne somme à
    au plus 1, la masse manquante étant les tirages au-delà du rang ``k``."""
    rng = np.random.default_rng(0)
    X = rng.random((6, 3))
    result = smaa_rank_acceptability(
        X, weighted_sum_score, k=2, n_draws=400, random_state=0
    )
    assert result.rank_acceptability.shape == (6, 2)
    assert result.k == 2
    row_sums = result.rank_acceptability.sum(axis=1)
    assert row_sums.max() <= 1.0 + 1e-12
    np.testing.assert_allclose(row_sums, result.confidence_factor)
    # Sur 6 produits, seuls 2 rangs sont stockés à chaque tirage
    assert result.rank_acceptability.sum() == pytest.approx(2.0)


def test_smaa_batching_leaves_the_result_unchanged() -> None:
    """Le découpage en lots de tirages ne change pas le résultat accumulé."""
    rng = np.random.default_rng(0)
    X = rng.random((10, 3))
    draws = np.asarray(
        smaa_rank_acceptability(X, weighted_sum_score, k=3, n_draws=200, random_state=0)
        .rank_acceptability
    )
    batched = smaa_rank_acceptability(
        X, weighted_sum_score, k=3, n_draws=200, random_state=0, batch_size=7
    )
    np.testing.assert_allclose(batched.rank_acceptability, draws)


def test_smaa_confidence_factor_is_a_probability() -> None:
    """Le facteur de confiance (part des tirages plaçant la cellule dans le
    top-k) est dans ``[0, 1]``."""
    rng = np.random.default_rng(0)
    X = rng.random((8, 3))
    result = smaa_rank_acceptability(
        X, weighted_sum_score, k=3, n_draws=400, random_state=0
    )
    assert result.confidence_factor.shape == (8,)
    assert result.confidence_factor.min() >= 0.0
    assert result.confidence_factor.max() <= 1.0


def test_smaa_confidence_factor_is_one_at_full_depth() -> None:
    """Avec ``k = n`` toute cellule est toujours dans le top-k : facteur égal à 1."""
    rng = np.random.default_rng(0)
    X = rng.random((5, 3))
    result = smaa_rank_acceptability(
        X, weighted_sum_score, k=5, n_draws=200, random_state=0
    )
    np.testing.assert_allclose(result.confidence_factor, 1.0)


def test_bootstrap_rank_stability_columns_and_bounds(df_metrics_toy) -> None:
    """Le bootstrap renvoie les colonnes attendues et des rangs dans ``[1, n]``."""
    clean = df_metrics_toy.dropna().reset_index(drop=True)
    config = AggregationConfig(
        id_columns=("id",), metric_columns=("HHI", "CDI2", "CDI3")
    )
    result = bootstrap_rank_stability(
        clean,
        config,
        lambda cfg: WeightedAggregator(weighting="equal"),
        n_boot=25,
        random_state=0,
    )
    assert sorted(result.columns) == [
        "rank_high",
        "rank_low",
        "rank_median",
        "rank_sd",
    ]
    assert len(result) == len(clean)
    assert result["rank_median"].between(1, len(clean)).all()
    assert (result["rank_low"] <= result["rank_high"]).all()


def test_bootstrap_gives_every_identifier_exactly_n_boot_ranks(df_metrics_toy) -> None:
    """D-08 : chaque produit est classé sur la population complète à chaque
    tirage, donc reçoit exactement ``n_boot`` rangs comparables — y compris
    ceux absents d'un rééchantillon, dont l'écart-type reste défini."""
    clean = df_metrics_toy.dropna().reset_index(drop=True)
    config = AggregationConfig(
        id_columns=("id",), metric_columns=("HHI", "CDI2", "CDI3")
    )
    result = bootstrap_rank_stability(
        clean,
        config,
        lambda cfg: WeightedAggregator(weighting="equal"),
        n_boot=15,
        random_state=0,
    )
    # Aucun produit manquant, aucune statistique indéfinie
    assert list(result.index) == list(clean["id"])
    assert result.notna().all().all()
    # Les rangs d'un tirage sont ceux de la population complète : la médiane
    # d'un produit reste dans [1, n] et les bornes encadrent la médiane
    assert (result["rank_low"] <= result["rank_median"]).all()
    assert (result["rank_median"] <= result["rank_high"]).all()
    assert (result["rank_sd"] >= 0).all()


def test_leave_one_metric_out_reports_one_row_per_metric(df_metrics_toy) -> None:
    """Un retrait par métrique, avec τ de Kendall et recouvrement de tête."""
    clean = df_metrics_toy.dropna().reset_index(drop=True)
    config = AggregationConfig(
        id_columns=("id",), metric_columns=("HHI", "CDI2", "CDI3")
    )
    result = leave_one_metric_out(
        clean, config, lambda cfg: WeightedAggregator(weighting="equal")
    )
    assert sorted(result.index) == ["CDI2", "CDI3", "HHI"]
    assert {"kendall_tau", "topk_overlap"} == set(result.columns)


# ──────────────────────────────────────────────────────────────────────
# Classements consensus
# ──────────────────────────────────────────────────────────────────────


def test_borda_and_copeland_agree_on_a_three_element_case() -> None:
    """Deux méthodes en désaccord sur les deux premières cellules : Borda et
    Copeland les laissent ex æquo, la troisième reste dernière."""
    scores = {"a": np.array([3.0, 2.0, 1.0]), "b": np.array([2.0, 3.0, 1.0])}
    np.testing.assert_allclose(borda_rank(scores), [1.5, 1.5, 3.0])
    np.testing.assert_allclose(copeland_rank(scores), [1.5, 1.5, 3.0])


def test_borda_rank_follows_unanimous_order() -> None:
    """Accord unanime : le consensus de Borda reproduit l'ordre commun."""
    scores = {"a": np.array([5.0, 3.0, 1.0]), "b": np.array([9.0, 4.0, 2.0])}
    np.testing.assert_allclose(borda_rank(scores), [1.0, 2.0, 3.0])


def test_copeland_preselection_matches_full_copeland() -> None:
    """I-17 : sur un petit cas, la présélection de Borda couvrant tout le groupe
    redonne exactement le Copeland complet."""
    rng = np.random.default_rng(1)
    scores = {name: rng.random(12) for name in ("a", "b", "c")}
    np.testing.assert_allclose(
        copeland_rank(scores, top_n=12), copeland_rank(scores)
    )


def test_copeland_preselection_keeps_borda_order_outside_the_core() -> None:
    """Hors présélection, l'ordre de Borda est conservé, à la suite du noyau."""
    scores = {
        "a": np.array([5.0, 4.0, 3.0, 2.0, 1.0]),
        "b": np.array([5.0, 4.0, 3.0, 1.0, 2.0]),
    }
    ranks = copeland_rank(scores, top_n=2)
    np.testing.assert_allclose(ranks[:3], [1.0, 2.0, 3.0])
    assert ranks[3] < ranks[4]


def test_kemeny_rank_on_small_preselection() -> None:
    """Kemeny sur trois cellules : permutation valide, la cellule unanimement
    dernière garde le rang 3."""
    scores = {"a": np.array([3.0, 2.0, 1.0]), "b": np.array([2.0, 3.0, 1.0])}
    ranks = kemeny_rank(scores, top_n=3)
    assert sorted(ranks) == [1.0, 2.0, 3.0]
    assert ranks[2] == 3.0



# ──────────────────────────────────────────────────────────────────────
# Rapport composite
# ──────────────────────────────────────────────────────────────────────


def test_coherence_report_dispute_threshold_scales_with_group_size() -> None:
    """I-08 : le seuil de litige est une fraction de la taille du groupe, avec
    un plancher d'un rang."""
    rng = np.random.default_rng(0)
    n = 300
    index = pd.Index([f"cell_{i:03d}" for i in range(n)])
    X = rng.random((n, 3))
    scores = {
        "sum": pd.Series(X.sum(axis=1), index=index),
        "first": pd.Series(X[:, 0], index=index),
    }
    report = compute_coherence_report(scores, X, dispute_fraction=0.01)
    assert report.dispute_threshold == 3
    # Fraction nulle : plancher à un rang, et tout désaccord devient un litige
    assert compute_coherence_report(scores, X, dispute_fraction=0.0).dispute_threshold == 1


def test_coherence_report_carries_violations_and_front_ranks() -> None:
    """Le rapport expose les taux séparés, le rang des membres du front et la
    matrice des τ pondérés ; ``to_metrics`` les aplatit."""
    index = pd.Index(["a", "b", "c"])
    scores = {
        "sum": pd.Series([0.9, 0.5, 0.1], index=index),
        "geo": pd.Series([0.8, 0.6, 0.2], index=index),
    }
    X = np.array([[0.9, 0.9], [0.5, 0.5], [0.1, 0.1]])
    report = compute_coherence_report(scores, X)
    assert report.violation_rate["sum"].strict == 0.0
    assert report.violation_rate["sum"].tie == 0.0
    assert report.front_rank["sum"] == (1.0, 1.0)
    assert report.weighted_tau_matrix.shape == (2, 2)
    metrics = report.to_metrics()
    assert metrics["aggregation.coherence.violation_strict_sum"] == 0.0
    assert metrics["aggregation.coherence.front_rank_max_geo"] == 1.0
