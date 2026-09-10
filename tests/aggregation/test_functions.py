"""Tests des fonctions d'agrégation (``macroforecast.trade.aggregation.functions``).

Monotonie stricte sur les paires dominantes (somme pondérée, moyenne
géométrique, TOPSIS, VIKOR, rang moyen), monotonie faible du quantile de
cône, non-monotonie construite de l'indice MPI, validation ``X >= 0`` de la
moyenne géométrique (I-07) et équivalence de la projection blanchie à la
projection brute sur nuage gaussien isotrope (A-04).
"""

from __future__ import annotations

import numpy as np
import pytest
from scipy import stats

from macroforecast.trade.aggregation.functions import (
    _inverse_sqrt,
    cone_quantile_bounds,
    cone_quantile_score,
    geometric_mean_score,
    mahalanobis_score,
    mpi_score,
    rank_mean_score,
    topsis_score,
    vikor_score,
    weighted_sum_score,
    whitened_projection_score,
)
from macroforecast.trade.aggregation.weights import dirichlet_weights
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


def test_geometric_mean_rejects_centered_data(X_uniform: np.ndarray) -> None:
    """I-07 : sur des données centrées-réduites (valeurs négatives), la moyenne
    géométrique lève ``ValueError`` plutôt que de produire des ``NaN``."""
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


# ──────────────────────────────────────────────────────────────────────
# VIKOR (A-03)
# ──────────────────────────────────────────────────────────────────────


def test_vikor_strictly_monotone_on_dominant_pairs(_dominant_pairs) -> None:
    """VIKOR à poids strictement positifs et ``v > 0`` : ``x_i ≻ x_k`` ⟹
    ``s_i > s_k`` (l'utilité de groupe ``S`` sépare strictement les paires
    dominantes, le regret ``R`` ne fait que les ordonner faiblement)."""
    X, dominant, dominated = _dominant_pairs
    scores = vikor_score(X, np.array([0.2, 0.3, 0.5]))
    assert np.all(scores[dominant] > scores[dominated])


def test_vikor_scores_lie_in_unit_interval(X_minmax: np.ndarray) -> None:
    """``Q_i`` est une combinaison convexe de deux positions relatives dans
    ``[0, 1]`` : le score ``1 - Q_i`` y reste."""
    scores = vikor_score(X_minmax, np.array([0.2, 0.3, 0.5]))
    assert scores.min() >= 0.0 and scores.max() <= 1.0


def test_vikor_matches_opricovic_definition() -> None:
    """Vérification sur un cas calculable à la main (``v = 0.5``)."""
    X = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
    weights = np.array([0.5, 0.5])
    # Ecarts normalises a l'ideal (1, 1) : S = [0.5, 0.5, 0], R = [0.5, 0.5, 0]
    # => Q = [1, 1, 0] et le score vaut 1 - Q
    np.testing.assert_allclose(vikor_score(X, weights), [0.0, 0.0, 1.0])


def test_vikor_keeps_degenerate_denominator() -> None:
    """``S⁺ = S⁻`` : le terme correspondant vaut ``0`` au lieu de ``NaN``."""
    # Nuage ou toutes les lignes portent la meme utilite de groupe et le meme
    # regret : les deux denominateurs de `Q` sont nuls
    X = np.array([[1.0, 0.0], [0.0, 1.0]])
    scores = vikor_score(X, np.array([0.5, 0.5]))
    assert np.all(np.isfinite(scores))
    np.testing.assert_allclose(scores, [1.0, 1.0])


def test_vikor_rejects_out_of_range_compromise() -> None:
    """``v`` hors de ``[0, 1]`` : ``ValueError``."""
    with pytest.raises(ValueError):
        vikor_score(np.array([[1.0, 0.0], [0.0, 1.0]]), np.array([0.5, 0.5]), v=1.5)


# ──────────────────────────────────────────────────────────────────────
# Score de rang moyen (A-02)
# ──────────────────────────────────────────────────────────────────────


def test_rank_mean_strictly_monotone_on_dominant_pairs(_dominant_pairs) -> None:
    """Rang moyen à poids strictement positifs : ``x_i ≻ x_k`` ⟹ ``s_i > s_k``."""
    X, dominant, dominated = _dominant_pairs
    scores = rank_mean_score(X, np.array([0.2, 0.3, 0.5]))
    assert np.all(scores[dominant] > scores[dominated])


def test_rank_mean_scores_lie_in_unit_interval(X_minmax: np.ndarray) -> None:
    """Moyenne pondérée de rangs normalisés : dans ``[0, 1]``."""
    scores = rank_mean_score(X_minmax)
    assert scores.min() >= 0.0 and scores.max() <= 1.0


def test_rank_mean_is_invariant_under_monotone_column_transforms(
    X_minmax: np.ndarray,
) -> None:
    """Le score ne dépend que des rangs : toute transformation strictement
    croissante appliquée colonne par colonne le laisse inchangé."""
    baseline = rank_mean_score(X_minmax)
    transformed = np.column_stack(
        [
            np.exp(X_minmax[:, 0]),
            3.0 * X_minmax[:, 1] + 7.0,
            np.log1p(X_minmax[:, 2]),
        ]
    )
    np.testing.assert_allclose(rank_mean_score(transformed), baseline)


def test_rank_mean_averages_ties() -> None:
    """Ex æquo moyennés : deux lignes identiques partagent le rang moyen."""
    X = np.array([[1.0], [2.0], [2.0], [3.0]])
    np.testing.assert_allclose(
        rank_mean_score(X), [0.0, 1.5 / 3.0, 1.5 / 3.0, 1.0]
    )


# ──────────────────────────────────────────────────────────────────────
# Projection blanchie orientée (A-04)
# ──────────────────────────────────────────────────────────────────────


def _isotropic_cloud(n: int, d: int) -> np.ndarray:
    """Nuage gaussien dont la covariance empirique est *exactement* l'identité.

    Le pré-blanchiment retire le bruit d'estimation de ``Σ``, qui domine
    autrement l'écart à la projection brute à ``n`` fini.
    """
    rng = np.random.default_rng(0)
    X = rng.normal(size=(n, d))
    return (X - X.mean(axis=0)) @ _inverse_sqrt(np.cov(X, rowvar=False, bias=True))


def test_whitened_projection_matches_plain_projection_when_isotropic() -> None:
    """A-04 : sur un nuage isotrope, ``Σ^{-1/2}`` est l'identité et le score se
    réduit à la projection brute sur la direction de l'idéal, c'est-à-dire à la
    somme des coordonnées (``τ > 0.99``, l'écart résiduel étant celui du pôle
    idéal empirique à la première bissectrice)."""
    X = _isotropic_cloud(2000, 3)
    scores = whitened_projection_score(X, covariance_estimator="empirical")
    tau, _ = stats.kendalltau(scores, X.sum(axis=1))
    assert tau > 0.99


def test_whitened_projection_is_oriented() -> None:
    """Contrairement à la distance de Mahalanobis, le score est signé : la queue
    basse se classe *en dessous* du centre, non loin de lui."""
    rng = np.random.default_rng(0)
    X = rng.normal(size=(300, 3))
    scores = whitened_projection_score(X, covariance_estimator="empirical")
    low_tail = np.argmin(X.sum(axis=1))
    high_tail = np.argmax(X.sum(axis=1))
    assert scores[low_tail] < 0.0 < scores[high_tail]


def test_whitened_projection_warns_when_weights_supplied() -> None:
    """La pondération est portée par la covariance et par la direction de
    l'idéal : ``weights`` déclenche un avertissement."""
    rng = np.random.default_rng(0)
    X = rng.normal(size=(60, 3))
    with pytest.warns(UserWarning):
        whitened_projection_score(
            X, weights=np.ones(3), covariance_estimator="empirical"
        )


def test_inverse_sqrt_squares_back_to_the_precision() -> None:
    """``Σ^{-1/2} Σ Σ^{-1/2} = I`` (décomposition symétrique par ``eigh``)."""
    rng = np.random.default_rng(0)
    factor = rng.normal(size=(4, 4))
    covariance = factor @ factor.T + np.eye(4)
    whitener = _inverse_sqrt(covariance)
    np.testing.assert_allclose(whitener @ covariance @ whitener, np.eye(4), atol=1e-8)


# ──────────────────────────────────────────────────────────────────────
# Mahalanobis : centre de référence (M-06)
# ──────────────────────────────────────────────────────────────────────


def test_mahalanobis_robust_center_is_the_center_outward_rank() -> None:
    """``center="robust_center"`` : le score est la distance au centre, minimale
    au centre du nuage et croissante avec l'éloignement radial."""
    X = _isotropic_cloud(400, 3)
    scores = mahalanobis_score(
        X, covariance_estimator="empirical", center="robust_center"
    )
    radius = np.linalg.norm(X - X.mean(axis=0), axis=1)
    tau, _ = stats.kendalltau(scores, radius)
    assert tau > 0.99
    # Non oriente : la queue basse se classe aussi haut que la queue haute
    assert scores[np.argmin(X.sum(axis=1))] > np.median(scores)


def test_mahalanobis_rejects_unknown_center() -> None:
    """Un centre inconnu lève ``ValueError``."""
    rng = np.random.default_rng(0)
    with pytest.raises(ValueError):
        mahalanobis_score(rng.normal(size=(50, 3)), center="barycentre")


# ──────────────────────────────────────────────────────────────────────
# Quantile de cône (A-01)
# ──────────────────────────────────────────────────────────────────────


def test_cone_quantile_lies_in_unit_interval(X_minmax: np.ndarray) -> None:
    """``F̂_C`` est un rang normalisé : dans ``(0, 1]``."""
    draws = dirichlet_weights(3, 200, random_state=0)
    scores = cone_quantile_score(X_minmax, draws)
    assert scores.min() > 0.0 and scores.max() <= 1.0


def test_cone_quantile_weakly_monotone_on_dominant_pairs(_dominant_pairs) -> None:
    """``x_i ≻ x_k`` ⟹ ``F̂_C(x_i) >= F̂_C(x_k)`` : monotonie faible pour la
    dominance, valable tirage par tirage donc conservée par le minimum."""
    X, dominant, dominated = _dominant_pairs
    draws = dirichlet_weights(X.shape[1], 200, random_state=0)
    scores = cone_quantile_score(X, draws)
    assert np.all(scores[dominant] >= scores[dominated])


def test_cone_quantile_bounds_are_ordered(X_minmax: np.ndarray) -> None:
    """La borne « pire cas » minore la borne « meilleur cas », et le score par
    défaut est la première."""
    draws = dirichlet_weights(3, 200, random_state=0)
    lower, upper = cone_quantile_bounds(X_minmax, draws)
    assert np.all(lower <= upper)
    np.testing.assert_allclose(cone_quantile_score(X_minmax, draws), lower)
    np.testing.assert_allclose(
        cone_quantile_score(X_minmax, draws, bound="upper"), upper
    )


def test_cone_quantile_blocking_does_not_change_the_result(
    X_minmax: np.ndarray,
) -> None:
    """La vectorisation par blocs de tirages est un pur détail d'exécution."""
    draws = dirichlet_weights(3, 300, random_state=0)
    np.testing.assert_allclose(
        cone_quantile_score(X_minmax, draws, block_size=7),
        cone_quantile_score(X_minmax, draws, block_size=1000),
    )


def test_cone_quantile_rejects_mismatched_draws(X_minmax: np.ndarray) -> None:
    """Un simplexe de dimension incompatible lève ``ValueError``."""
    with pytest.raises(ValueError):
        cone_quantile_score(X_minmax, dirichlet_weights(4, 10, random_state=0))
