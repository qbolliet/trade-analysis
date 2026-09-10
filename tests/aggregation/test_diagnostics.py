"""Tests du protocole de comparaison et des classements consensus.

Module ``macroforecast.trade.aggregation.diagnostics`` : concordance (τ_b, W),
recouvrement de tête (RBO), cohérence avec la dominance, stabilité (bootstrap,
SMAA) et consensus (Borda, Copeland, Kemeny).
"""

from __future__ import annotations

import numpy as np
import pytest

from macroforecast.trade.aggregation.base import AggregationConfig
from macroforecast.trade.aggregation.estimators import WeightedAggregator
from macroforecast.trade.aggregation.functions import weighted_sum_score
from macroforecast.trade.aggregation.diagnostics import (
    bootstrap_rank_stability,
    borda_rank,
    copeland_rank,
    dominance_violation_rate,
    kemeny_rank,
    kendall_tau_b_matrix,
    kendall_w,
    leave_one_metric_out,
    rank_biased_overlap,
    smaa_rank_acceptability,
    topk_overlap,
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
    """Un score qui respecte la dominance a un taux de violation nul ; le score
    opposé viole toutes les paires comparables."""
    X = np.array([[2.0, 2.0], [1.0, 1.0]])
    assert dominance_violation_rate(X, np.array([2.0, 1.0])) == 0.0
    assert dominance_violation_rate(X, np.array([1.0, 2.0])) == 1.0


def test_dominance_violation_rate_nan_without_comparable_pair() -> None:
    """Relation de dominance vide : le taux n'est pas défini (``NaN``)."""
    X = np.array([[1.0, 0.0], [0.0, 1.0]])
    assert np.isnan(dominance_violation_rate(X, np.array([0.3, 0.7])))


# ──────────────────────────────────────────────────────────────────────
# Q4 — Stabilité
# ──────────────────────────────────────────────────────────────────────


def test_smaa_rank_acceptability_rows_sum_to_one() -> None:
    """Chaque ligne de la matrice d'acceptabilité est une distribution de rang."""
    rng = np.random.default_rng(0)
    X = rng.random((6, 3))
    result = smaa_rank_acceptability(
        X, weighted_sum_score, k=2, n_draws=400, random_state=0
    )
    np.testing.assert_allclose(result.rank_acceptability.sum(axis=1), 1.0)


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
        "n_draws_observed",
        "rank_high",
        "rank_low",
        "rank_median",
    ]
    assert result["rank_median"].between(1, len(clean)).all()


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


def test_kemeny_rank_on_small_preselection() -> None:
    """Kemeny sur trois cellules : permutation valide, la cellule unanimement
    dernière garde le rang 3."""
    scores = {"a": np.array([3.0, 2.0, 1.0]), "b": np.array([2.0, 3.0, 1.0])}
    ranks = kemeny_rank(scores, top_n=3)
    assert sorted(ranks) == [1.0, 2.0, 3.0]
    assert ranks[2] == 3.0
