"""Tests de la dominance de Pareto (``macroforecast.trade.aggregation.pareto``).

Couvre les invariants testables de S-1.3, les propriétés mathématiques de
l'ordre partiel, et l'équivalence exacte entre les implémentations de
référence (matricielles) et leurs variantes à grand ``n`` (A-07). Le bug M-02
(ε-dominance qui vidait le front, I-02) est désormais corrigé : le ``xfail``
qui le documentait a laissé place aux tests de propriété du glouton de
Laumanns.
"""

from __future__ import annotations

import numpy as np
import pytest
from sklearn.base import clone

from macroforecast.trade.aggregation.pareto import (
    MetricReducer,
    ParetoScorer,
    dominance_count,
    dominance_count_chunked,
    epsilon_pareto_front,
    epsilon_pareto_set,
    non_dominated_sort,
    non_dominated_sort_sweep,
    normalized_dominance_depth,
    pareto_dominance_matrix,
    pareto_front,
    pareto_front_sweep,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures locales
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def _small_matrix() -> np.ndarray:
    """Nuage ``(120, 3)`` : assez de paires comparables pour les propriétés d'ordre."""
    return np.random.default_rng(1).random((120, 3))


@pytest.fixture
def _tied_matrix() -> np.ndarray:
    """Nuage ``(80, 2)`` à valeurs discrètes : doublons et ex æquo garantis."""
    return np.random.default_rng(2).integers(0, 4, size=(80, 2)).astype(float)


@pytest.fixture
def _redundant_matrix() -> np.ndarray:
    """Nuage ``(120, 6)`` : trois paires de colonnes quasi colinéaires."""
    rng = np.random.default_rng(3)
    factors = rng.random((120, 3))
    columns = []
    for j in range(3):
        columns.append(factors[:, j])
        # Transformation monotone du facteur : corrélation de Spearman quasi unitaire
        columns.append(factors[:, j] ** 2 + rng.normal(scale=1e-3, size=120))
    return np.column_stack(columns)


# ──────────────────────────────────────────────────────────────────────
# Matrice de dominance et front
# ──────────────────────────────────────────────────────────────────────


def test_dominance_matrix_is_irreflexive_and_asymmetric(X_uniform: np.ndarray) -> None:
    """Aucun point ne se domine lui-même ; la dominance stricte est asymétrique."""
    dominance = pareto_dominance_matrix(X_uniform)
    assert not np.any(np.diag(dominance))
    assert not np.any(dominance & dominance.T)


def test_front_equals_negation_of_any_dominator(X_uniform: np.ndarray) -> None:
    """S-1.3 : ``pareto_front(X) == ~any(pareto_dominance_matrix(X), axis=0)``."""
    dominance = pareto_dominance_matrix(X_uniform)
    expected = ~np.any(dominance, axis=0)
    np.testing.assert_array_equal(pareto_front(X_uniform), expected)


def test_front_is_non_empty_and_contains_global_argmax(X_uniform: np.ndarray) -> None:
    """Le front n'est jamais vide et contient le point de somme maximale.

    Le maximum d'une forme linéaire à coefficients positifs sur un nuage fini
    est atteint sur un point non dominé.
    """
    front = pareto_front(X_uniform)
    assert front.any()
    assert front[np.argmax(X_uniform.sum(axis=1))]


def test_front_invariant_under_monotone_coordinate_transform(X_uniform: np.ndarray) -> None:
    """La dominance ne dépend que de l'ordre : invariante par transformation
    strictement croissante appliquée coordonnée par coordonnée."""
    reference = pareto_front(X_uniform)
    np.testing.assert_array_equal(pareto_front(X_uniform ** 3), reference)
    np.testing.assert_array_equal(pareto_front(np.log(X_uniform + 0.01)), reference)


# ──────────────────────────────────────────────────────────────────────
# Équivalence des implémentations de référence et des variantes A-07
# ──────────────────────────────────────────────────────────────────────


def test_front_sweep_matches_matrix_front(X_uniform: np.ndarray) -> None:
    """S-1.3 : ``front_sweep(X) == ~any(pareto_dominance_matrix(X), axis=0)``."""
    np.testing.assert_array_equal(
        pareto_front_sweep(X_uniform), pareto_front(X_uniform)
    )


def test_front_sweep_matches_matrix_front_with_ties(_tied_matrix: np.ndarray) -> None:
    """Doublons et ex æquo : les deux implémentations restent identiques.

    La dominance stricte exige un gain strict, donc deux lignes identiques
    restent toutes deux sur le front dans les deux variantes.
    """
    np.testing.assert_array_equal(
        pareto_front_sweep(_tied_matrix), pareto_front(_tied_matrix)
    )


def test_front_sweep_handles_single_row() -> None:
    """Cas limite ``n = 1`` : la ligne unique est sur le front."""
    np.testing.assert_array_equal(pareto_front_sweep(np.array([[1.0, 2.0]])), [True])


@pytest.mark.parametrize("chunk_size", [1, 7, 64, 2048, 10_000])
def test_dominance_count_chunked_matches_reference(
    _small_matrix: np.ndarray, chunk_size: int
) -> None:
    """Le comptage par blocs coïncide exactement avec le comptage matriciel."""
    np.testing.assert_array_equal(
        dominance_count_chunked(_small_matrix, chunk_size=chunk_size),
        dominance_count(_small_matrix),
    )


def test_dominance_count_chunked_matches_reference_with_ties(
    _tied_matrix: np.ndarray,
) -> None:
    """Même égalité en présence de doublons (diagonale et ex æquo)."""
    np.testing.assert_array_equal(
        dominance_count_chunked(_tied_matrix, chunk_size=16),
        dominance_count(_tied_matrix),
    )


def test_dominance_count_chunked_rejects_invalid_chunk(_small_matrix: np.ndarray) -> None:
    """``chunk_size`` doit être strictement positif."""
    with pytest.raises(ValueError, match="chunk_size"):
        dominance_count_chunked(_small_matrix, chunk_size=0)


def test_non_dominated_sort_sweep_matches_reference(_small_matrix: np.ndarray) -> None:
    """L'épluchage par balayage donne les mêmes couches que la version matricielle."""
    np.testing.assert_array_equal(
        non_dominated_sort_sweep(_small_matrix), non_dominated_sort(_small_matrix)
    )


def test_non_dominated_sort_sweep_truncates_at_max_layers(
    _small_matrix: np.ndarray,
) -> None:
    """``max_layers`` tronque l'épluchage : couche ``0`` au-delà, identique en deçà."""
    full = non_dominated_sort(_small_matrix)
    truncated = non_dominated_sort_sweep(_small_matrix, max_layers=2)
    assert set(np.unique(truncated)) <= {0, 1, 2}
    np.testing.assert_array_equal(truncated[full <= 2], full[full <= 2])
    assert np.all(truncated[full > 2] == 0)


def test_non_dominated_sort_sweep_rejects_invalid_max_layers(
    _small_matrix: np.ndarray,
) -> None:
    """``max_layers`` doit être strictement positif."""
    with pytest.raises(ValueError, match="max_layers"):
        non_dominated_sort_sweep(_small_matrix, max_layers=0)


# ──────────────────────────────────────────────────────────────────────
# Sous-ensemble ε-représentatif (M-02, glouton de Laumanns)
# ──────────────────────────────────────────────────────────────────────


def test_epsilon_zero_recovers_exact_front(X_uniform: np.ndarray) -> None:
    """``epsilon = 0`` redonne exactement le front exact."""
    np.testing.assert_array_equal(
        epsilon_pareto_set(X_uniform, 0.0), pareto_front(X_uniform)
    )


def test_epsilon_set_is_subset_of_exact_front(X_uniform: np.ndarray) -> None:
    """Le sous-ensemble ε est inclus dans le front exact, par construction."""
    exact = pareto_front(X_uniform)
    reduced = epsilon_pareto_set(X_uniform, 0.05)
    assert np.all(~reduced | exact)


def test_epsilon_set_keeps_the_global_argmax(X_uniform: np.ndarray) -> None:
    """M-02 : le point de somme maximale reste dans le sous-ensemble ε.

    Il est traité en premier par le glouton (``sort_key="sum"``) et n'est donc
    jamais rejeté — contrairement à l'ancienne ε-dominance, symétrique, qui
    l'excluait dès qu'un voisin était à moins de ``epsilon``.
    """
    for epsilon in (0.01, 0.05, 0.2, 1.0, 10.0):
        selected = epsilon_pareto_set(X_uniform, epsilon)
        assert selected[np.argmax(X_uniform.sum(axis=1))]


def test_epsilon_set_never_empty_on_the_documented_counter_example() -> None:
    """Non-régression M-02 : le front ε ne vide plus le nuage.

    ``pareto_front([[3, 3], [2, 2], [1, 1]], epsilon=1.5)`` renvoyait
    ``[False, False, False]``, maximum global compris.
    """
    X = np.array([[3.0, 3.0], [2.0, 2.0], [1.0, 1.0]])
    np.testing.assert_array_equal(epsilon_pareto_set(X, 1.5), [True, False, False])


def test_epsilon_set_cardinal_is_non_increasing(X_uniform: np.ndarray) -> None:
    """``|F_ε|`` décroît quand ``epsilon`` croît."""
    sizes = [
        int(epsilon_pareto_set(X_uniform, epsilon).sum())
        for epsilon in (0.0, 0.01, 0.05, 0.2, 1.0)
    ]
    assert sizes == sorted(sizes, reverse=True)


def test_epsilon_set_inclusion_is_not_guaranteed() -> None:
    """Le glouton n'est **pas** monotone par inclusion, et c'est documenté.

    Le parcours suit un ordre fixe : un point rejeté à petit ``epsilon`` par un
    voisin qui disparaît à plus grand ``epsilon`` réapparaît. Contre-exemple
    figé ici pour que la propriété réellement offerte reste explicite.
    """
    X = np.array([[20.0, 0.0], [11.0, 3.0], [9.0, 4.0]])
    # Les trois lignes sont sur le front exact
    np.testing.assert_array_equal(pareto_front(X), [True, True, True])
    np.testing.assert_array_equal(epsilon_pareto_set(X, 1.0), [True, True, False])
    np.testing.assert_array_equal(epsilon_pareto_set(X, 3.0), [True, False, True])


def test_epsilon_set_accepts_a_vector_epsilon(X_uniform: np.ndarray) -> None:
    """``epsilon`` vectoriel ``(d,)`` : tolérance propre à chaque métrique."""
    scalar = epsilon_pareto_set(X_uniform, 0.1)
    vector = epsilon_pareto_set(X_uniform, np.full(X_uniform.shape[1], 0.1))
    np.testing.assert_array_equal(scalar, vector)
    # Une tolérance nulle sur une seule métrique reste admissible
    uneven = np.array([0.0, 0.2, 0.2])
    assert epsilon_pareto_set(X_uniform, uneven).sum() >= 1


def test_epsilon_set_rejects_invalid_arguments(X_uniform: np.ndarray) -> None:
    """Arguments incohérents : ``epsilon``, ``sort_key`` et ``counts``."""
    with pytest.raises(ValueError, match="epsilon has shape"):
        epsilon_pareto_set(X_uniform, np.zeros(7))
    with pytest.raises(ValueError, match="non-negative"):
        epsilon_pareto_set(X_uniform, -0.1)
    with pytest.raises(ValueError, match="Unknown sort_key"):
        epsilon_pareto_set(X_uniform, 0.1, sort_key="median")
    with pytest.raises(ValueError, match="counts has shape"):
        epsilon_pareto_set(X_uniform, 0.1, counts=np.zeros(3))


def test_epsilon_set_tie_break_by_counts_is_deterministic(_tied_matrix: np.ndarray) -> None:
    """Le départage par comptage de dominance est reproductible et reste un sous-front."""
    counts = dominance_count(_tied_matrix)
    first = epsilon_pareto_set(_tied_matrix, 0.5, counts=counts)
    second = epsilon_pareto_set(_tied_matrix, 0.5, counts=counts)
    np.testing.assert_array_equal(first, second)
    assert np.all(~first | pareto_front(_tied_matrix))


def test_epsilon_pareto_front_is_an_alias(X_uniform: np.ndarray) -> None:
    """``epsilon_pareto_front`` délègue à ``epsilon_pareto_set``."""
    np.testing.assert_array_equal(
        epsilon_pareto_front(X_uniform, 0.1), epsilon_pareto_set(X_uniform, 0.1)
    )


# ──────────────────────────────────────────────────────────────────────
# Comptage de dominance et couches
# ──────────────────────────────────────────────────────────────────────


def test_dominance_count_matches_brute_force(_small_matrix: np.ndarray) -> None:
    """``δ(i) = #dominés - #dominateurs`` coïncide avec le double comptage direct."""
    dominance = pareto_dominance_matrix(_small_matrix)
    brute = dominance.sum(axis=1) - dominance.sum(axis=0)
    np.testing.assert_array_equal(dominance_count(_small_matrix), brute)


def test_dominance_count_gap_on_dominant_pairs(_small_matrix: np.ndarray) -> None:
    """``x_i ≻ x_k`` ⟹ ``count[i] >= count[k] + 2`` (transitivité de l'ordre)."""
    dominance = pareto_dominance_matrix(_small_matrix)
    count = dominance_count(_small_matrix)
    dominant, dominated = np.where(dominance)
    assert np.all(count[dominant] >= count[dominated] + 2)


def test_normalized_depth_is_bounded_and_proportional(_small_matrix: np.ndarray) -> None:
    """La profondeur normalisée vaut ``count / (n - 1)`` et reste dans ``[-1, 1]``."""
    depth = normalized_dominance_depth(_small_matrix)
    count = dominance_count(_small_matrix)
    np.testing.assert_allclose(depth, count / (_small_matrix.shape[0] - 1))
    assert depth.min() >= -1.0 and depth.max() <= 1.0


def test_non_dominated_sort_layer_one_is_the_front(_small_matrix: np.ndarray) -> None:
    """La couche 1 du tri non dominé est exactement le front de Pareto."""
    layers = non_dominated_sort(_small_matrix)
    np.testing.assert_array_equal(layers == 1, pareto_front(_small_matrix))


def test_non_dominated_sort_orders_dominant_pairs(_small_matrix: np.ndarray) -> None:
    """``x_i ≻ x_k`` ⟹ ``layers[i] < layers[k]``."""
    dominance = pareto_dominance_matrix(_small_matrix)
    layers = non_dominated_sort(_small_matrix)
    dominant, dominated = np.where(dominance)
    assert np.all(layers[dominant] < layers[dominated])


def test_dominance_count_layer_one_dominates_deeper_layers() -> None:
    """Sur une chaîne totalement ordonnée, couches et comptage sont strictement monotones."""
    X = np.array([[5.0], [4.0], [3.0], [2.0], [1.0]])
    np.testing.assert_array_equal(non_dominated_sort(X), [1, 2, 3, 4, 5])
    np.testing.assert_array_equal(dominance_count(X), [4, 2, 0, -2, -4])


# ──────────────────────────────────────────────────────────────────────
# Réduction préalable de la dimension (M-19)
# ──────────────────────────────────────────────────────────────────────


def test_reducer_groups_partition_the_columns(_redundant_matrix: np.ndarray) -> None:
    """``groups_`` forme une partition de ``range(d)``, ordonnée par premier indice."""
    reducer = MetricReducer().fit(_redundant_matrix)
    flattened = [index for group in reducer.groups_ for index in group]
    assert sorted(flattened) == list(range(_redundant_matrix.shape[1]))
    assert len(flattened) == len(set(flattened))
    assert [group[0] for group in reducer.groups_] == sorted(
        group[0] for group in reducer.groups_
    )


def test_reducer_groups_redundant_columns_together(_redundant_matrix: np.ndarray) -> None:
    """Deux colonnes liées par une transformation monotone tombent dans le même groupe."""
    reducer = MetricReducer(threshold=0.3).fit(_redundant_matrix)
    assert reducer.groups_ == [(0, 1), (2, 3), (4, 5)]
    assert reducer.transform(_redundant_matrix).shape == (120, 3)


def test_reducer_threshold_zero_keeps_every_column(_redundant_matrix: np.ndarray) -> None:
    """Un seuil nul n'agrège rien : autant de groupes que de métriques."""
    reducer = MetricReducer(threshold=0.0).fit(_redundant_matrix)
    assert len(reducer.groups_) == _redundant_matrix.shape[1]


def test_reducer_n_groups_takes_precedence(_redundant_matrix: np.ndarray) -> None:
    """``n_groups`` fixe le nombre de groupes et prime sur ``threshold``."""
    reducer = MetricReducer(n_groups=2).fit(_redundant_matrix)
    assert len(reducer.groups_) == 2
    assert reducer.transform(_redundant_matrix).shape == (120, 2)


def test_reducer_rejects_out_of_range_n_groups(_redundant_matrix: np.ndarray) -> None:
    """``n_groups`` doit appartenir à ``[1, d]``."""
    with pytest.raises(ValueError, match="n_groups"):
        MetricReducer(n_groups=99).fit(_redundant_matrix)


def test_reducer_front_is_included_in_the_full_front(_redundant_matrix: np.ndarray) -> None:
    """M-19 : la dominance sur les moyennes est plus riche, donc ``F1(réduit) ⊆ F1(complet)``.

    La moyenne étant croissante en chacun de ses arguments, toute dominance sur
    les colonnes se transmet aux sous-indices ; le front ne peut que rétrécir.
    """
    full_front = pareto_front(_redundant_matrix)
    reduced = MetricReducer(threshold=0.3).fit_transform(_redundant_matrix)
    reduced_front = pareto_front(reduced)
    assert np.all(~reduced_front | full_front)
    assert reduced_front.sum() < full_front.sum()


def test_reducer_is_invariant_under_monotone_transform(_redundant_matrix: np.ndarray) -> None:
    """La rang-normalisation rend la réduction invariante par transformation monotone."""
    reference = MetricReducer(threshold=0.3).fit_transform(_redundant_matrix)
    transformed = MetricReducer(threshold=0.3).fit_transform(
        np.exp(_redundant_matrix)
    )
    np.testing.assert_allclose(transformed, reference)


def test_reducer_handles_a_single_metric() -> None:
    """Cas limite ``d = 1`` : la CAH est court-circuitée, groupe unique."""
    X = np.random.default_rng(0).random((30, 1))
    reducer = MetricReducer().fit(X)
    assert reducer.groups_ == [(0,)]
    assert reducer.transform(X).shape == (30, 1)


# ──────────────────────────────────────────────────────────────────────
# Estimateur ParetoScorer (S-1.3)
# ──────────────────────────────────────────────────────────────────────


def test_scorer_front_matches_the_functional_api(_small_matrix: np.ndarray) -> None:
    """Sans réduction, le front de l'estimateur est le front exact."""
    scorer = ParetoScorer().fit(_small_matrix)
    np.testing.assert_array_equal(scorer.front(_small_matrix), pareto_front(_small_matrix))


def test_scorer_epsilon_front_is_a_subset_keeping_the_argmax(
    _small_matrix: np.ndarray,
) -> None:
    """S-1.3 : ``epsilon_front(X) ⊆ front(X)`` et ``argmax(sum) ∈ epsilon_front(X)``."""
    scorer = ParetoScorer(epsilon=0.5).fit(_small_matrix)
    front = scorer.front(_small_matrix)
    epsilon_front = scorer.epsilon_front(_small_matrix)
    assert np.all(~epsilon_front | front)
    assert epsilon_front[np.argmax(_small_matrix.sum(axis=1))]
    assert epsilon_front.sum() < front.sum()


def test_scorer_epsilon_zero_matches_the_exact_front(_small_matrix: np.ndarray) -> None:
    """S-1.3 : ``epsilon = 0`` ⟹ égalité des deux fronts."""
    scorer = ParetoScorer(epsilon=0.0).fit(_small_matrix)
    np.testing.assert_array_equal(
        scorer.epsilon_front(_small_matrix), scorer.front(_small_matrix)
    )


def test_scorer_epsilon_is_calibrated_on_a_robust_scale(_small_matrix: np.ndarray) -> None:
    """``epsilon_`` vaut le multiplicateur fois l'échelle robuste par colonne."""
    from scipy import stats

    scorer = ParetoScorer(epsilon=0.1, epsilon_scale="mad").fit(_small_matrix)
    expected = 0.1 * stats.median_abs_deviation(_small_matrix, axis=0, scale=1.0)
    np.testing.assert_allclose(scorer.epsilon_, expected)
    assert scorer.epsilon_.shape == (_small_matrix.shape[1],)


def test_scorer_accepts_an_absolute_epsilon_vector(_small_matrix: np.ndarray) -> None:
    """Un ``epsilon`` vectoriel est repris tel quel, sans mise à l'échelle."""
    absolute = np.array([0.1, 0.2, 0.3])
    scorer = ParetoScorer(epsilon=absolute).fit(_small_matrix)
    np.testing.assert_allclose(scorer.epsilon_, absolute)


def test_scorer_layers_and_count_respect_the_partial_order(
    _small_matrix: np.ndarray,
) -> None:
    """S-1.3 : ``x_i ≻ x_k`` ⟹ ``layers[i] < layers[k]`` et ``count[i] ≥ count[k] + 2``."""
    scorer = ParetoScorer().fit(_small_matrix)
    layers = scorer.layers(_small_matrix)
    count = scorer.dominance_count(_small_matrix)
    dominant, dominated = np.where(pareto_dominance_matrix(_small_matrix))
    assert np.all(layers[dominant] < layers[dominated])
    assert np.all(count[dominant] >= count[dominated] + 2)
    np.testing.assert_array_equal(layers == 1, scorer.front(_small_matrix))


def test_scorer_layers_disabled_returns_zeros(_small_matrix: np.ndarray) -> None:
    """``compute_layers=False`` renvoie la couche conventionnelle ``0``."""
    scorer = ParetoScorer(compute_layers=False).fit(_small_matrix)
    np.testing.assert_array_equal(
        scorer.layers(_small_matrix), np.zeros(_small_matrix.shape[0], dtype=int)
    )


def test_scorer_predict_is_the_normalized_depth(_small_matrix: np.ndarray) -> None:
    """``predict`` renvoie ``count / (n - 1)`` ; ``score="dominance_count"`` le brut."""
    scorer = ParetoScorer().fit(_small_matrix)
    n = _small_matrix.shape[0]
    np.testing.assert_allclose(
        scorer.predict(_small_matrix), dominance_count(_small_matrix) / (n - 1)
    )
    raw = ParetoScorer(score="dominance_count").fit(_small_matrix)
    np.testing.assert_array_equal(
        raw.predict(_small_matrix), dominance_count(_small_matrix)
    )


def test_scorer_alert_falls_back_to_the_exact_front(_small_matrix: np.ndarray) -> None:
    """Sans ``epsilon``, l'alerte est l'appartenance au front exact."""
    scorer = ParetoScorer().fit(_small_matrix)
    np.testing.assert_array_equal(
        scorer.alert(_small_matrix), scorer.front(_small_matrix)
    )


def test_scorer_large_n_path_agrees_with_the_small_n_path(
    _small_matrix: np.ndarray,
) -> None:
    """Les deux chemins (matriciel et par blocs) donnent le même résultat.

    Un ``large_n_threshold`` artificiellement bas force la bascule vers les
    algorithmes de A-07 sur une matrice de taille modeste.
    """
    small = ParetoScorer(epsilon=0.5, large_n_threshold=10**6).fit(_small_matrix)
    large = ParetoScorer(
        epsilon=0.5, large_n_threshold=10, chunk_size=16
    ).fit(_small_matrix)
    np.testing.assert_array_equal(
        large.dominance_count(_small_matrix), small.dominance_count(_small_matrix)
    )
    np.testing.assert_array_equal(large.front(_small_matrix), small.front(_small_matrix))
    np.testing.assert_allclose(large.predict(_small_matrix), small.predict(_small_matrix))
    # Le départage par comptage est abandonné à grand n : le front ε reste valide
    epsilon_front = large.epsilon_front(_small_matrix)
    assert np.all(~epsilon_front | large.front(_small_matrix))
    assert epsilon_front[np.argmax(_small_matrix.sum(axis=1))]


def test_scorer_applies_the_metric_reducer(_redundant_matrix: np.ndarray) -> None:
    """``reduce`` est cloné, ajusté dans ``fit``, et dimensionne ``epsilon_``."""
    reducer = MetricReducer(threshold=0.3)
    scorer = ParetoScorer(epsilon=0.1, reduce=reducer).fit(_redundant_matrix)
    assert scorer.n_features_in_ == 6
    assert scorer.n_features_reduced_ == 3
    assert scorer.epsilon_.shape == (3,)
    # L'objet de l'appelant n'est pas muté par l'ajustement
    assert not hasattr(reducer, "groups_")
    assert scorer.reducer_ is not reducer
    np.testing.assert_array_equal(
        scorer.front(_redundant_matrix),
        pareto_front(reducer.fit_transform(_redundant_matrix)),
    )


def test_scorer_rejects_unknown_options(_small_matrix: np.ndarray) -> None:
    """Les options sont validées contre leurs registres respectifs."""
    with pytest.raises(ValueError, match="Unknown score"):
        ParetoScorer(score="borda").fit(_small_matrix)
    with pytest.raises(ValueError, match="Unknown sort_key"):
        ParetoScorer(sort_key="median").fit(_small_matrix)
    with pytest.raises(ValueError, match="Unknown epsilon_scale"):
        ParetoScorer(epsilon=0.1, epsilon_scale="range").fit(_small_matrix)


def test_scorer_follows_the_sklearn_estimator_contract(_small_matrix: np.ndarray) -> None:
    """``get_params`` / ``clone`` fonctionnent et ``layers`` reste une méthode.

    Le paramètre de configuration s'appelle ``compute_layers`` précisément pour
    ne pas masquer la méthode ``layers`` (S-1.3 les nommait tous deux
    ``layers``).
    """
    scorer = ParetoScorer(epsilon=0.5, compute_layers=False, chunk_size=32)
    params = scorer.get_params()
    assert params["compute_layers"] is False
    assert params["chunk_size"] == 32
    copy = clone(scorer)
    assert copy.get_params() == params
    assert callable(ParetoScorer().fit(_small_matrix).layers)


def test_scorer_fit_predict_matches_fit_then_predict(_small_matrix: np.ndarray) -> None:
    """``fit_predict`` est l'alias conventionnel de ``fit(X).predict(X)``."""
    np.testing.assert_allclose(
        ParetoScorer().fit_predict(_small_matrix),
        ParetoScorer().fit(_small_matrix).predict(_small_matrix),
    )
