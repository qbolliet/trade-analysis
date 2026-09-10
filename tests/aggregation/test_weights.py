"""Tests des pondérations endogènes (``macroforecast.trade.aggregation.weights``).

Entropie, CRITIC, ACP (variante OCDE), tirage de Dirichlet et *benefit of the
doubt*. Trois ``xfail`` documentent I-06 (CRITIC ``NaN`` sur colonne
constante), M-04/I-05 (formule OCDE non conforme) et I-04 (BoD pénalise une
ligne à zéro).
"""

from __future__ import annotations

import numpy as np
import pytest

from macroforecast.trade.aggregation.weights import (
    benefit_of_doubt_weights,
    critic_weights,
    dirichlet_weights,
    entropy_weights,
    pca_weights,
)


# ──────────────────────────────────────────────────────────────────────
# Entropie de Shannon
# ──────────────────────────────────────────────────────────────────────


def test_entropy_weights_form_a_simplex_vector(X_minmax: np.ndarray) -> None:
    """Vecteur du simplexe : composantes ``>= 0`` sommant à 1."""
    weights = entropy_weights(X_minmax)
    assert weights.shape == (3,)
    assert np.all(weights >= 0.0)
    assert weights.sum() == pytest.approx(1.0)


def test_entropy_weights_zero_for_constant_nonzero_column() -> None:
    """Une colonne (strictement positive) sans dispersion n'apporte aucune
    information : poids nul."""
    X = np.array([[0.0, 0.5], [0.5, 0.5], [1.0, 0.5]])
    weights = entropy_weights(X)
    assert weights[1] == pytest.approx(0.0)


def test_entropy_weights_rejects_negative_input() -> None:
    """L'entropie n'est pas invariante par translation : ``X >= 0`` exigé."""
    with pytest.raises(ValueError):
        entropy_weights(np.array([[-1.0, 0.5], [0.2, 0.3]]))


# ──────────────────────────────────────────────────────────────────────
# CRITIC
# ──────────────────────────────────────────────────────────────────────


def test_critic_weights_form_a_simplex_vector(X_minmax: np.ndarray) -> None:
    """Vecteur du simplexe pour les deux variantes de corrélation."""
    for method in ("pearson", "spearman"):
        weights = critic_weights(X_minmax, method=method)
        assert np.all(weights >= 0.0)
        assert weights.sum() == pytest.approx(1.0)


def test_critic_down_weights_a_redundant_metric() -> None:
    """Deux métriques quasi identiques se partagent le poids d'une seule ;
    la métrique originale (décorrélée) pèse davantage."""
    rng = np.random.default_rng(0)
    base = rng.random(200)
    independent = rng.random(200)
    X = np.column_stack([base, base + rng.normal(scale=1e-3, size=200), independent])
    weights = critic_weights(X)
    assert weights[2] > weights[0]
    assert weights[2] > weights[1]


@pytest.mark.filterwarnings("ignore:invalid value encountered in divide:RuntimeWarning")
@pytest.mark.xfail(strict=True, reason="I-06 : np.corrcoef renvoie NaN sur une colonne constante")
def test_critic_weights_handle_constant_column(X_with_constant_column: np.ndarray) -> None:
    """I-06 : une colonne constante doit donner un poids nul, pas ``NaN`` partout."""
    weights = critic_weights(X_with_constant_column)
    assert not np.any(np.isnan(weights))


# ──────────────────────────────────────────────────────────────────────
# ACP / OCDE–JRC
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def _oecd_factor_matrix() -> np.ndarray:
    """Structure à deux facteurs : quatre indicateurs sur ``f1``, un sur ``f2``.

    Cas de l'exemple M-04 : le *Handbook* attribue ``w = 0,2`` à chacun des
    cinq indicateurs.
    """
    rng = np.random.default_rng(0)
    f1 = rng.normal(size=400)
    f2 = rng.normal(size=400)
    columns = [f1 + rng.normal(scale=0.25, size=400) for _ in range(4)]
    columns.append(f2 + rng.normal(scale=0.25, size=400))
    X = np.column_stack(columns)
    return (X - X.mean(axis=0)) / X.std(axis=0)


def test_pca_simple_variant_returns_a_direction(_oecd_factor_matrix: np.ndarray) -> None:
    """Variante « axe 1 » : une direction de norme 1, signe fixé positif."""
    loading, report = pca_weights(_oecd_factor_matrix)
    assert loading.shape == (5,)
    assert np.linalg.norm(loading) == pytest.approx(1.0)
    assert loading.sum() > 0.0
    assert report.variance_share_axis1 > 0.0


def test_pca_oecd_variant_is_a_simplex_vector(_oecd_factor_matrix: np.ndarray) -> None:
    """Variante OCDE (``rotate=True``) : poids ``>= 0`` sommant à 1."""
    weights, _ = pca_weights(_oecd_factor_matrix, rotate=True)
    assert np.all(weights >= 0.0)
    assert weights.sum() == pytest.approx(1.0)


def test_pca_oecd_variant_groups_the_common_factor(_oecd_factor_matrix: np.ndarray) -> None:
    """Les quatre indicateurs du facteur commun reçoivent un poids homogène."""
    weights, _ = pca_weights(_oecd_factor_matrix, rotate=True)
    np.testing.assert_allclose(weights[:4], weights[0], rtol=0.1)


@pytest.mark.xfail(
    strict=True,
    reason="M-04/I-05 : la formule OCDE utilise les valeurs propres avant rotation",
)
def test_pca_oecd_matches_handbook_weights(_oecd_factor_matrix: np.ndarray) -> None:
    """M-04 : sur l'exemple du *Handbook*, chacun des cinq indicateurs pèse ``0,2``."""
    weights, _ = pca_weights(_oecd_factor_matrix, rotate=True)
    np.testing.assert_allclose(weights, 0.2, atol=0.05)


# ──────────────────────────────────────────────────────────────────────
# Tirage de Dirichlet (SMAA)
# ──────────────────────────────────────────────────────────────────────


def test_dirichlet_weights_are_simplex_points_and_reproducible() -> None:
    """Chaque tirage est sur le simplexe ; la graine rend le tirage reproductible."""
    draws = dirichlet_weights(3, 500, random_state=0)
    assert draws.shape == (500, 3)
    assert np.all(draws >= 0.0)
    np.testing.assert_allclose(draws.sum(axis=1), 1.0)
    np.testing.assert_array_equal(draws, dirichlet_weights(3, 500, random_state=0))


# ──────────────────────────────────────────────────────────────────────
# Benefit of the doubt
# ──────────────────────────────────────────────────────────────────────


def test_bod_scores_are_bounded_by_one(X_minmax: np.ndarray) -> None:
    """Tout score fini est dans ``(0, 1]`` (contrainte de non-domination)."""
    scores, weights = benefit_of_doubt_weights(X_minmax, kappa=0.5)
    finite = np.isfinite(scores)
    assert np.all(scores[finite] <= 1.0 + 1e-6)
    assert weights.shape == (X_minmax.shape[0], 3)


def test_bod_front_row_reaches_score_one() -> None:
    """Un point qui domine tous les autres atteint le score 1."""
    X = np.array([[1.0, 1.0], [0.4, 0.5], [0.2, 0.3], [0.6, 0.1]])
    scores, _ = benefit_of_doubt_weights(X, kappa=0.5)
    assert scores[0] == pytest.approx(1.0, abs=1e-6)


@pytest.mark.xfail(
    strict=True,
    reason="I-04/M-03 : la restriction de parts impose un score nul à toute ligne comportant un 0",
)
def test_bod_does_not_penalise_a_zero_coordinate() -> None:
    """I-04 : la ligne ``[0, 1, 1]``, maximale sur deux métriques, doit obtenir
    un score strictement positif."""
    X = np.array([[0.0, 1.0, 1.0], [0.5, 0.5, 0.5], [1.0, 0.2, 0.3]])
    scores, _ = benefit_of_doubt_weights(X, kappa=0.5)
    assert scores[0] > 1e-9
