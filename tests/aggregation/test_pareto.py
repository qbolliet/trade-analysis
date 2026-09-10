"""Tests de la dominance de Pareto (``macroforecast.trade.aggregation.pareto``).

Couvre les invariants testables de S-1.3 et les propriétés mathématiques de
l'ordre partiel ; un ``xfail`` documente le bug M-02 (ε-dominance qui vide le
front, I-02).
"""

from __future__ import annotations

import numpy as np
import pytest

from macroforecast.trade.aggregation.pareto import (
    dominance_count,
    non_dominated_sort,
    normalized_dominance_depth,
    pareto_dominance_matrix,
    pareto_front,
)


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
# ε-dominance (comportement courant et bug M-02)
# ──────────────────────────────────────────────────────────────────────


def test_epsilon_zero_recovers_exact_front(X_uniform: np.ndarray) -> None:
    """``epsilon = 0`` redonne exactement le front exact."""
    np.testing.assert_array_equal(
        pareto_front(X_uniform, epsilon=0.0), pareto_front(X_uniform)
    )


def test_epsilon_front_is_subset_of_exact_front(X_uniform: np.ndarray) -> None:
    """Élargir le cône de dominance ne peut qu'exclure des points : le
    front ε est inclus dans le front exact."""
    exact = pareto_front(X_uniform)
    widened = pareto_front(X_uniform, epsilon=0.05)
    assert np.all(~widened | exact)


@pytest.mark.xfail(strict=True, reason="I-02 : la ε-dominance (M-02) vide le front et exclut le maximum")
def test_epsilon_front_keeps_the_global_argmax(X_uniform: np.ndarray) -> None:
    """I-02 : le point de somme maximale doit rester dans le front ε.

    Avec la relation de Laumanns (correctif attendu) le point de tri le plus
    élevé est conservé d'office ; la relation actuelle l'exclut dès qu'un
    voisin est à moins de ``epsilon``.
    """
    front = pareto_front(X_uniform, epsilon=0.2)
    assert front[np.argmax(X_uniform.sum(axis=1))]


# ──────────────────────────────────────────────────────────────────────
# Comptage de dominance et couches
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def _small_matrix() -> np.ndarray:
    """Nuage ``(120, 3)`` : assez de paires comparables pour les propriétés d'ordre."""
    return np.random.default_rng(1).random((120, 3))


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
