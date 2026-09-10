"""Tests des fonctions d'agrégation (``macroforecast.trade.aggregation.functions``).

Monotonie stricte sur les paires dominantes (somme pondérée, moyenne
géométrique, TOPSIS), non-monotonie construite de l'indice MPI, et un
``xfail`` pour I-07 (la moyenne géométrique ne valide pas ``X >= 0``).
"""

from __future__ import annotations

import numpy as np
import pytest

from macroforecast.trade.aggregation.functions import (
    geometric_mean_score,
    mahalanobis_score,
    mpi_score,
    topsis_score,
    weighted_sum_score,
)
from macroforecast.trade.aggregation.pareto import pareto_dominance_matrix


@pytest.fixture
def _dominant_pairs(X_minmax: np.ndarray):
    """Matrice normalisée et indices ``(i, k)`` des paires où ``x_i ≻ x_k``."""
    dominance = pareto_dominance_matrix(X_minmax)
    dominant, dominated = np.where(dominance)
    assert dominant.size > 50  # garde-fou : assez de paires pour la propriété
    return X_minmax, dominant, dominated


# ──────────────────────────────────────────────────────────────────────
# Monotonie stricte sur les paires dominantes
# ──────────────────────────────────────────────────────────────────────


def test_weighted_sum_strictly_monotone_on_dominant_pairs(_dominant_pairs) -> None:
    """Somme pondérée à poids strictement positifs : ``x_i ≻ x_k`` ⟹ ``s_i > s_k``."""
    X, dominant, dominated = _dominant_pairs
    scores = weighted_sum_score(X, np.array([0.2, 0.3, 0.5]))
    assert np.all(scores[dominant] > scores[dominated])


def test_geometric_mean_strictly_monotone_on_dominant_pairs(_dominant_pairs) -> None:
    """Moyenne géométrique pondérée (données ``>= 0``) : strictement monotone
    sur les paires dominantes."""
    X, dominant, dominated = _dominant_pairs
    scores = geometric_mean_score(X, np.array([0.2, 0.3, 0.5]))
    assert np.all(scores[dominant] > scores[dominated])


def test_topsis_strictly_monotone_on_dominant_pairs(_dominant_pairs) -> None:
    """TOPSIS à poids strictement positifs : strictement monotone sur les
    paires dominantes."""
    X, dominant, dominated = _dominant_pairs
    scores = topsis_score(X, np.array([0.2, 0.3, 0.5]))
    assert np.all(scores[dominant] > scores[dominated])


def test_weighted_sum_matches_definition() -> None:
    """``s_i = Σ_j w_j x_ij``."""
    X = np.array([[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]])
    np.testing.assert_allclose(
        weighted_sum_score(X, np.array([0.3, 0.7])), [0.3, 0.7, 0.5]
    )


def test_topsis_scores_lie_in_unit_interval(X_minmax: np.ndarray) -> None:
    """Le score TOPSIS (proximité relative) est dans ``[0, 1]``."""
    scores = topsis_score(X_minmax, np.array([0.2, 0.3, 0.5]))
    assert scores.min() >= 0.0 and scores.max() <= 1.0


# ──────────────────────────────────────────────────────────────────────
# Moyenne géométrique : validation des données
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.filterwarnings("ignore:invalid value encountered in log:RuntimeWarning")
@pytest.mark.xfail(strict=True, reason="I-07 : geometric_mean_score ne valide pas X >= 0 (renvoie des NaN)")
def test_geometric_mean_rejects_centered_data(X_uniform: np.ndarray) -> None:
    """I-07 : sur des données centrées-réduites (valeurs négatives), la moyenne
    géométrique doit lever ``ValueError`` plutôt que produire des ``NaN``."""
    Z = (X_uniform - X_uniform.mean(axis=0)) / X_uniform.std(axis=0)
    with pytest.raises(ValueError):
        geometric_mean_score(Z, np.full(3, 1.0 / 3.0))


# ──────────────────────────────────────────────────────────────────────
# MPI : non-monotonie (M-08, propriété 1.5 de la note)
# ──────────────────────────────────────────────────────────────────────


def _mpi_imbalance_matrix() -> np.ndarray:
    """Nuage où une cellule est un point aberrant extrême sur deux métriques
    et proche de la médiane sur la troisième.

    Le grand nombre de lignes rend le score z de la cellule sur les deux
    métriques aberrantes suffisamment élevé pour que réduire son déséquilibre
    (augmenter la métrique faible) fasse *baisser* l'indice MPI+.
    """
    n = 300
    spike = np.full(n, 1e-6)
    spike[0] = 1.0
    moderate = np.linspace(0.0, 1.0, n)
    moderate[0] = moderate[n // 2]  # cellule 0 : au centre sur la métrique modérée
    return np.column_stack([moderate, spike.copy(), spike.copy()])


def test_mpi_is_not_monotone() -> None:
    """Contre-exemple explicite : augmenter la métrique la plus faible d'une
    cellule (toutes choses égales par ailleurs) fait *diminuer* son MPI+."""
    X = _mpi_imbalance_matrix()
    baseline = mpi_score(X)

    raised = X.copy()
    raised[0, 0] += 1e-3  # augmentation stricte d'une coordonnée de la cellule 0

    assert mpi_score(raised)[0] < baseline[0]


def test_mpi_warns_when_weights_supplied() -> None:
    """L'indice MPI est sans poids : fournir ``weights`` déclenche un avertissement."""
    X = np.array([[2.0, 2.0], [1.0, 3.0], [3.0, 1.0]])
    with pytest.warns(UserWarning):
        mpi_score(X, weights=np.array([0.5, 0.5]))


# ──────────────────────────────────────────────────────────────────────
# Mahalanobis
# ──────────────────────────────────────────────────────────────────────


def test_mahalanobis_scores_are_non_negative() -> None:
    """Distance dans l'espace blanchi : toujours ``>= 0``."""
    rng = np.random.default_rng(0)
    X = rng.normal(size=(80, 3)) + 5.0
    scores = mahalanobis_score(X, covariance_estimator="empirical")
    assert np.all(scores >= 0.0)


def test_mahalanobis_warns_when_weights_supplied() -> None:
    """La pondération est portée par la covariance : ``weights`` déclenche un avertissement."""
    rng = np.random.default_rng(0)
    X = rng.normal(size=(50, 3)) + 5.0
    with pytest.warns(UserWarning):
        mahalanobis_score(X, weights=np.ones(3), covariance_estimator="empirical")
