"""Tests des pondérations endogènes (``macroforecast.trade.aggregation.weights``).

Entropie, CRITIC, ACP (direction et variante OCDE), méta-pondération ``auto``,
tirage de Dirichlet et *benefit of the doubt*. Les ``xfail`` I-04 et I-06 ont
été retirés : les correctifs correspondants sont désormais attendus.
"""

from __future__ import annotations

import numpy as np
import pytest

from macroforecast.trade.aggregation.estimators import WeightedAggregator
from macroforecast.trade.aggregation.weights import (
    auto_weights,
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


def test_entropy_weights_zero_for_null_column(recwarn: pytest.WarningsRecorder) -> None:
    """M-12 : une colonne identiquement nulle (colonne constante après min–max)
    reçoit un poids nul, sans avertissement numpy."""
    X = np.array([[0.0, 0.0], [0.5, 0.0], [1.0, 0.0]])
    with np.errstate(all="raise"):
        weights = entropy_weights(X)
    assert weights[1] == pytest.approx(0.0)
    assert weights[0] == pytest.approx(1.0)
    assert not [w for w in recwarn if issubclass(w.category, RuntimeWarning)]


def test_entropy_weights_rejects_negative_input() -> None:
    """L'entropie n'est pas invariante par translation : ``X >= 0`` exigé."""
    with pytest.raises(ValueError, match="non-negative"):
        entropy_weights(np.array([[-1.0, 0.5], [0.2, 0.3]]))


# ──────────────────────────────────────────────────────────────────────
# CRITIC
# ──────────────────────────────────────────────────────────────────────


def test_critic_weights_form_a_simplex_vector(X_minmax: np.ndarray) -> None:
    """Vecteur du simplexe pour toutes les combinaisons corrélation × échelle."""
    for method in ("pearson", "spearman"):
        for scale in ("std", "mad"):
            weights = critic_weights(X_minmax, method=method, scale=scale)
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


def test_critic_weights_handle_constant_column(X_with_constant_column: np.ndarray) -> None:
    """I-06 : une colonne constante donne un poids nul, pas ``NaN`` partout."""
    weights = critic_weights(X_with_constant_column)
    assert not np.any(np.isnan(weights))
    assert weights[1] == pytest.approx(0.0)
    assert weights.sum() == pytest.approx(1.0)


def test_critic_weights_uniform_when_every_column_is_constant() -> None:
    """Toutes les colonnes constantes : aucun contraste, repli uniforme."""
    X = np.full((10, 3), 0.4)
    weights = critic_weights(X)
    np.testing.assert_allclose(weights, 1.0 / 3.0)


def test_critic_abs_corr_treats_anticorrelation_as_redundancy() -> None:
    """M-13 : avec ``abs_corr=True``, une métrique fortement anticorrélée n'est
    plus la plus « originale » du lot."""
    rng = np.random.default_rng(0)
    base = rng.random(300)
    X = np.column_stack([base, -base + rng.normal(scale=1e-3, size=300), rng.random(300)])
    signed = critic_weights(X)
    absolute = critic_weights(X, abs_corr=True)
    # La colonne 1 est l'opposée de la colonne 0 : maximale en r signé,
    # redondante en |r|
    assert signed[1] > absolute[1]
    assert absolute[2] > absolute[1]


def test_critic_weights_reject_unknown_parameters(X_minmax: np.ndarray) -> None:
    """Les noms de méthode et d'échelle sont validés explicitement."""
    with pytest.raises(ValueError, match="method"):
        critic_weights(X_minmax, method="kendall")
    with pytest.raises(ValueError, match="scale"):
        critic_weights(X_minmax, scale="iqr")


# ──────────────────────────────────────────────────────────────────────
# ACP / OCDE–JRC
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def _oecd_factor_matrix() -> np.ndarray:
    """Structure à deux facteurs : quatre indicateurs sur ``f1``, un sur ``f2``.

    Cas de l'exemple M-04 : le *Handbook* attribue ``w = 0,2`` à chacun des
    cinq indicateurs. Le bruit propre est faible (5 % de la variance) pour que
    les saturations après rotation soient proches de 1 et donc que
    ``V_1 ≈ 4`` et ``V_2 ≈ 1``, la structure idéalisée de l'exemple.
    """
    rng = np.random.default_rng(0)
    n = 2000
    f1 = rng.normal(size=n)
    f2 = rng.normal(size=n)
    signal, noise = np.sqrt(0.95), np.sqrt(0.05)
    columns = [signal * f1 + noise * rng.normal(size=n) for _ in range(4)]
    columns.append(signal * f2 + noise * rng.normal(size=n))
    return np.column_stack(columns)


def test_pca_simple_variant_returns_a_direction(_oecd_factor_matrix: np.ndarray) -> None:
    """Variante « direction » : une direction de norme 1, signe fixé positif."""
    direction, report = pca_weights(_oecd_factor_matrix)
    assert direction.shape == (5,)
    assert np.linalg.norm(direction) == pytest.approx(1.0)
    assert direction.sum() > 0.0
    assert report.variance_share_axis1 > 0.0


def test_pca_direction_is_invariant_to_the_input_scale(
    _oecd_factor_matrix: np.ndarray,
) -> None:
    """I-05 : l'ACP porte sur la matrice de corrélation, donc le résultat ne
    dépend ni du centrage ni de l'échelle des colonnes."""
    X = _oecd_factor_matrix
    standardised = (X - X.mean(axis=0)) / X.std(axis=0)
    minmax = (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0))
    np.testing.assert_allclose(
        pca_weights(standardised)[0], pca_weights(minmax)[0], atol=1e-8
    )
    np.testing.assert_allclose(
        pca_weights(standardised, rotate=True, min_eigenvalue=0.9)[0],
        pca_weights(minmax, rotate=True, min_eigenvalue=0.9)[0],
        atol=1e-8,
    )


def test_pca_oecd_variant_is_a_simplex_vector(_oecd_factor_matrix: np.ndarray) -> None:
    """Variante OCDE (``rotate=True``) : poids ``>= 0`` sommant à 1."""
    weights, _ = pca_weights(_oecd_factor_matrix, rotate=True, min_eigenvalue=0.9)
    assert np.all(weights >= 0.0)
    assert weights.sum() == pytest.approx(1.0)


def test_pca_oecd_matches_handbook_weights(_oecd_factor_matrix: np.ndarray) -> None:
    """M-04 : sur l'exemple du *Handbook*, chacun des cinq indicateurs pèse ``0,2``.

    La formule corrigée (variances **après** rotation, normalisation
    intra-facteur) donne ``0,2`` partout ; celle du document, renormalisée,
    donnerait ``0,235`` pour les quatre premiers et ``0,059`` pour le
    cinquième.
    """
    weights, _ = pca_weights(_oecd_factor_matrix, rotate=True, min_eigenvalue=0.9)
    np.testing.assert_allclose(weights, 0.2, atol=0.03)


def test_pca_oecd_falls_back_to_the_first_axis(_oecd_factor_matrix: np.ndarray) -> None:
    """Aucun axe au-dessus du seuil : repli sur le premier axe seul, poids
    toujours simpliciaux."""
    weights, report = pca_weights(
        _oecd_factor_matrix, rotate=True, min_eigenvalue=100.0
    )
    assert report.explained_variance_ratio.shape == (1,)
    assert np.all(weights >= 0.0)
    assert weights.sum() == pytest.approx(1.0)


def test_pca_weights_handle_a_single_metric() -> None:
    """``d = 1`` : la direction est ``[1]`` et le poids OCDE vaut 1."""
    X = np.linspace(0.0, 1.0, 20).reshape(-1, 1)
    direction, _ = pca_weights(X)
    np.testing.assert_allclose(direction, [1.0])
    weights, _ = pca_weights(X, rotate=True)
    np.testing.assert_allclose(weights, [1.0])


def test_pca_weights_handle_a_constant_column(X_with_constant_column: np.ndarray) -> None:
    """Colonne constante : corrélations nulles, aucun ``NaN`` dans les poids."""
    weights, _ = pca_weights(X_with_constant_column, rotate=True, min_eigenvalue=0.9)
    assert not np.any(np.isnan(weights))
    assert weights.sum() == pytest.approx(1.0)


# ──────────────────────────────────────────────────────────────────────
# Méta-pondération « auto » (S-1.5)
# ──────────────────────────────────────────────────────────────────────


def test_auto_weights_selects_pca_on_a_factorial_structure(
    _oecd_factor_matrix: np.ndarray,
) -> None:
    """Structure factorielle nette (KMO élevé, Bartlett rejeté, axe 1 dominant)
    : la règle S-1.5 retient l'ACP-OCDE."""
    X = _oecd_factor_matrix
    minmax = (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0))
    weights, report = auto_weights(minmax)
    assert report.selected == "pca_oecd"
    assert report.candidates_tried[0] == "pca_oecd"
    assert report.kmo >= 0.6
    assert report.bartlett_p <= 0.05
    assert weights.sum() == pytest.approx(1.0)


def test_auto_weights_selects_critic_on_redundant_metrics() -> None:
    """Pas de structure factorielle dominante mais des métriques redondantes :
    CRITIC est retenu."""
    rng = np.random.default_rng(1)
    n = 400
    base = rng.random(n)
    X = np.column_stack(
        [
            base,
            0.6 * base + 0.4 * rng.random(n),
            rng.random(n),
            rng.random(n),
            rng.random(n),
        ]
    )
    minmax = (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0))
    weights, report = auto_weights(minmax, axis1_share_min=0.9, redundancy_rho=0.05)
    assert report.selected == "critic"
    assert weights.sum() == pytest.approx(1.0)


def test_auto_weights_selects_entropy_without_structure_nor_redundancy(
    X_minmax: np.ndarray,
) -> None:
    """Métriques indépendantes : ni ACP ni CRITIC, l'entropie fait foi."""
    weights, report = auto_weights(X_minmax)
    assert report.selected == "entropy"
    assert report.mean_abs_rho < 0.3
    assert weights.sum() == pytest.approx(1.0)


def test_auto_weights_degeneracy_guard_falls_back(X_minmax: np.ndarray) -> None:
    """Garde de dégénérescence : un seuil ``max_weight`` très bas est
    inatteignable par tout candidat sauf ``equal``."""
    weights, report = auto_weights(X_minmax, max_weight=0.34)
    assert report.selected == "equal"
    assert report.candidates_tried[-1] == "equal"
    assert len(report.candidates_tried) > 1
    np.testing.assert_allclose(weights, 1.0 / 3.0)


def test_auto_weights_handles_a_single_metric() -> None:
    """``d = 1`` : repli immédiat sur ``equal`` (aucun diagnostic défini)."""
    weights, report = auto_weights(np.linspace(0.0, 1.0, 20).reshape(-1, 1))
    np.testing.assert_allclose(weights, [1.0])
    assert report.selected == "equal"
    assert np.isnan(report.kmo)


def test_auto_weights_handles_more_metrics_than_products() -> None:
    """``n <= d`` : KMO et Bartlett indéfinis, l'ACP n'est jamais candidate."""
    rng = np.random.default_rng(0)
    X = rng.random((4, 6))
    weights, report = auto_weights(X)
    assert np.isnan(report.kmo)
    assert np.isnan(report.bartlett_p)
    assert report.selected != "pca_oecd"
    assert weights.sum() == pytest.approx(1.0)


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
    """Tout score fini est dans ``[0, 1]`` (contrainte de non-domination)."""
    scores, weights = benefit_of_doubt_weights(X_minmax)
    finite = np.isfinite(scores)
    assert np.all(scores[finite] >= -1e-9)
    assert np.all(scores[finite] <= 1.0 + 1e-6)
    assert weights.shape == (X_minmax.shape[0], 3)


def test_bod_front_row_reaches_score_one() -> None:
    """Un point qui domine tous les autres atteint le score 1."""
    X = np.array([[1.0, 1.0], [0.4, 0.5], [0.2, 0.3], [0.6, 0.1]])
    scores, _ = benefit_of_doubt_weights(X)
    assert scores[0] == pytest.approx(1.0, abs=1e-6)


def test_bod_does_not_penalise_a_zero_coordinate() -> None:
    """I-04 : la ligne ``[0, 1, 1]``, maximale sur deux métriques, obtient un
    score élevé avec la région d'assurance (elle valait 0 avec les parts)."""
    X = np.array([[0.0, 1.0, 1.0], [0.5, 0.5, 0.5], [1.0, 0.2, 0.3]])
    scores, _ = benefit_of_doubt_weights(X, restriction="assurance_region")
    assert scores[0] > 0.5


def test_bod_shares_restriction_needs_a_positive_floor() -> None:
    """M-03 : ``delta`` est le paramètre de sensibilité qui rend la variante
    « parts » utilisable ; ``delta = 0`` est refusé."""
    X = np.array([[0.0, 1.0, 1.0], [0.5, 0.5, 0.5], [1.0, 0.2, 0.3]])
    with pytest.raises(ValueError, match="delta"):
        benefit_of_doubt_weights(X, restriction="shares", delta=0.0)

    scores, _ = benefit_of_doubt_weights(X, restriction="shares", delta=0.05)
    assert scores[0] > 1e-9


def test_bod_assurance_region_keeps_every_weight_positive() -> None:
    """``rho`` fini borne les rapports de poids : aucun poids nul (D-05)."""
    X = np.array([[0.0, 1.0, 1.0], [0.5, 0.5, 0.5], [1.0, 0.2, 0.3]])
    _, weights = benefit_of_doubt_weights(X, restriction="assurance_region", rho=4.0)
    finite = np.isfinite(weights).all(axis=1)
    assert np.all(weights[finite] > 0.0)


def test_bod_unrestricted_programme_admits_null_weights() -> None:
    """``restriction=None`` : le programme dégénère sur la métrique la plus
    favorable, ce que les restrictions sont censées empêcher."""
    X = np.array([[1.0, 0.0], [1.0, 1.0]])
    scores, weights = benefit_of_doubt_weights(X, restriction=None)
    # M-03, constat 2 : la ligne dominée atteint 1 sous un poids nul
    assert scores[0] == pytest.approx(1.0, abs=1e-6)
    assert np.min(weights[0]) == pytest.approx(0.0, abs=1e-9)


def test_bod_rejects_negative_input() -> None:
    """M-03, constat 3 : le score n'a aucun sens sur données centrées."""
    with pytest.raises(ValueError, match="non-negative"):
        benefit_of_doubt_weights(np.array([[-1.0, 0.5], [0.2, 0.3]]))


def test_bod_rejects_unknown_restriction(X_minmax: np.ndarray) -> None:
    """Le nom de la restriction est validé explicitement."""
    with pytest.raises(ValueError, match="restriction"):
        benefit_of_doubt_weights(X_minmax[:20], restriction="cone")


def test_bod_front_restriction_is_exact(X_minmax: np.ndarray) -> None:
    """La restriction des contraintes au front exact ne change aucun score."""
    X = X_minmax[:60]
    restricted, _ = benefit_of_doubt_weights(X, restrict_to_front=True)
    full, _ = benefit_of_doubt_weights(X, restrict_to_front=False)
    np.testing.assert_allclose(restricted, full, atol=1e-8)


# ──────────────────────────────────────────────────────────────────────
# Validation des poids par `WeightedAggregator` (I-05)
# ──────────────────────────────────────────────────────────────────────


def test_weighted_aggregator_rejects_the_pca_direction(X_minmax: np.ndarray) -> None:
    """La clé ``"pca"`` renvoie une direction : refusée, avec renvoi vers
    ``PcaProjectionScorer``."""
    with pytest.raises(ValueError, match="PcaProjectionScorer"):
        WeightedAggregator(weighting="pca", aggregation="topsis").fit(X_minmax)


def test_weighted_aggregator_accepts_the_oecd_weighting(X_minmax: np.ndarray) -> None:
    """La variante OCDE est simpliciale : acceptée par toutes les agrégations."""
    for aggregation in ("weighted_sum", "geometric_mean", "topsis"):
        estimator = WeightedAggregator(
            weighting="pca_oecd",
            aggregation=aggregation,
            weighting_params={"min_eigenvalue": 0.9},
        ).fit(X_minmax)
        assert estimator.weights_.sum() == pytest.approx(1.0)
        assert np.all(estimator.weights_ >= 0.0)


def test_weighted_aggregator_accepts_the_auto_weighting(X_minmax: np.ndarray) -> None:
    """La méta-pondération renvoie un couple ``(w, rapport)`` ; le rapport est
    exposé sur l'estimateur."""
    estimator = WeightedAggregator(weighting="auto").fit(X_minmax)
    assert estimator.weights_.sum() == pytest.approx(1.0)
    assert estimator.weighting_report_.selected in {
        "pca_oecd",
        "critic",
        "entropy",
        "equal",
    }


def test_weighted_aggregator_rejects_non_simplex_weights(X_minmax: np.ndarray) -> None:
    """Un schéma renvoyant des poids hors du simplexe est rejeté avec un
    message explicite."""
    with pytest.raises(ValueError, match="negative"):
        WeightedAggregator._check_simplex(np.array([-0.2, 1.2]))
    with pytest.raises(ValueError, match="sum to"):
        WeightedAggregator._check_simplex(np.array([0.2, 0.2]))
