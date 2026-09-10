"""Tests du transport optimal (``macroforecast.trade.aggregation.optimal_transport``).

La grille de référence est testable sans ``jax`` ; le solveur de Sinkhorn est
ignoré proprement en son absence (marqueur ``requires_ott``). Les tests de
grille figent la correction M-01/I-03 : les rayons suivent désormais la loi
sphérique uniforme (``ρ ~ U[0, 1]``), pour laquelle les normes sont uniformes,
et non plus la loi de Lebesgue de la boule (``ρ^{1/d}``, normes en Beta(d, 1)).
Les tests du solveur couvrent la calibration conforme (M-07/A-08), le
sous-échantillonnage (D-11) et le diagnostic d'ellipticité corrigé (I-10).
"""

from __future__ import annotations

import importlib.util

import numpy as np
import pytest
from scipy import stats

from macroforecast.trade.aggregation.optimal_transport import (
    OrientedKantorovichScorer,
    ellipticity_screen,
    spherical_uniform_grid,
)

# Régularisation entropique des tests : la carte entropique est un barycentre pondéré,
# donc contracte les rangs vers le centre ; un `epsilon` nettement plus petit que le
# défaut est nécessaire pour que le contrôle KS ne détecte plus ce biais (cf. docstring
# de `OrientedKantorovichScorer.epsilon`)
_TEST_EPSILON = 0.005

# Marqueur des tests du solveur : la grille reste testable sans la dépendance optionnelle
requires_ott = pytest.mark.skipif(
    importlib.util.find_spec("ott") is None,
    reason="le solveur de Sinkhorn requiert l'extra optimal-transport",
)


# ──────────────────────────────────────────────────────────────────────
# Grille de référence sur la boule unité
# ──────────────────────────────────────────────────────────────────────


def test_spherical_grid_lies_within_the_unit_ball() -> None:
    """Tous les points de la grille sont de norme ``<= 1``."""
    grid = spherical_uniform_grid(2048, 4, seed=0)
    assert grid.shape == (2048, 4)
    assert np.all(np.linalg.norm(grid, axis=1) <= 1.0 + 1e-9)


def test_spherical_grid_directions_are_isotropic() -> None:
    """Les directions couvrent la sphère : moyenne des points proche de 0."""
    grid = spherical_uniform_grid(4096, 3, seed=0)
    np.testing.assert_allclose(grid.mean(axis=0), 0.0, atol=0.05)


def test_spherical_grid_is_reproducible() -> None:
    """La graine du moteur de Sobol rend la grille déterministe."""
    np.testing.assert_array_equal(
        spherical_uniform_grid(512, 3, seed=7),
        spherical_uniform_grid(512, 3, seed=7),
    )


def test_spherical_grid_norms_are_uniform() -> None:
    """M-01 : sous la loi sphérique uniforme, ``‖U‖ ~ U[0, 1]`` — c'est la cible
    qui rend légitime le test KS de convergence de la carte de transport."""
    grid = spherical_uniform_grid(4096, 6, seed=0)
    norms = np.linalg.norm(grid, axis=1)
    _, p_value = stats.kstest(norms, "uniform")
    assert p_value > 0.01


def test_lebesgue_grid_norms_follow_a_beta_law() -> None:
    """M-01 : l'ancienne loi (``ρ^{1/d}``) donne des normes en Beta(d, 1), d'où le
    rejet systématique du contrôle d'uniformité qu'elle provoquait."""
    grid = spherical_uniform_grid(4096, 6, seed=0, radial="lebesgue")
    norms = np.linalg.norm(grid, axis=1)
    assert stats.kstest(norms, "uniform").pvalue < 0.01
    assert stats.kstest(norms, stats.beta(6, 1).cdf).pvalue > 0.01


def test_spherical_grid_rejects_an_unknown_radial_law() -> None:
    """Une loi radiale inconnue lève une ``ValueError`` nommant les valeurs admises."""
    with pytest.raises(ValueError, match="uniform"):
        spherical_uniform_grid(64, 2, radial="gaussian")


# ──────────────────────────────────────────────────────────────────────
# Dépendance optionnelle
# ──────────────────────────────────────────────────────────────────────


def test_scorer_requires_the_optimal_transport_extra() -> None:
    """Sans ``jax`` / ``ott-jax``, l'ajustement lève un ``ImportError`` nommant
    l'extra à installer ; avec la dépendance, ce test est ignoré."""
    try:
        import ott  # noqa: F401
    except ImportError:
        X = np.random.default_rng(0).random((20, 2))
        with pytest.raises(ImportError, match="optimal-transport"):
            OrientedKantorovichScorer(n_target=64).fit(X)
    else:
        pytest.skip("ott installé : l'ImportError ne peut pas être déclenchée")


# ──────────────────────────────────────────────────────────────────────
# Solveur de Sinkhorn (dépendance optionnelle jax / ott-jax)
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def X_gaussian() -> np.ndarray:
    """Nuage gaussien isotrope 2 000 x 3, cas elliptique de référence."""
    return np.random.default_rng(0).normal(size=(2000, 3))


@pytest.fixture(scope="module")
def fitted_scorer(X_gaussian: np.ndarray) -> OrientedKantorovichScorer:
    """Estimateur ajusté sur le nuage gaussien, partagé par les tests du solveur."""
    return OrientedKantorovichScorer(epsilon=_TEST_EPSILON).fit(X_gaussian)


@requires_ott
def test_transported_norms_are_uniform(
    fitted_scorer: OrientedKantorovichScorer, X_gaussian: np.ndarray
) -> None:
    """Le contrôle de convergence de l'algorithme 4 : sous la loi sphérique
    uniforme, une carte convergée laisse les rangs uniformes sur ``[0, 1]``."""
    report = fitted_scorer.fit_report(X_gaussian)
    assert report.converged
    assert report.uniformity_ks_p_value > 0.01
    assert np.all(fitted_scorer.ranks(X_gaussian) <= 1.0 + 1e-6)


@requires_ott
def test_projection_score_orders_like_the_coordinate_sum(
    fitted_scorer: OrientedKantorovichScorer, X_gaussian: np.ndarray
) -> None:
    """Sur une gaussienne isotrope, le score orienté ordonne comme la somme des
    coordonnées.

    La concordance est forte mais bornée : le score center-outward exact vaut
    ``h(r)/r · ⟨z, u*⟩`` (M-06), donc une modulation radiale de la projection,
    dont le τ_b avec la somme plafonne à ``0,93`` même pour la carte exacte.
    """
    tau, _ = stats.kendalltau(
        fitted_scorer.predict(X_gaussian), X_gaussian.sum(axis=1)
    )
    assert tau > 0.85


@requires_ott
def test_direction_is_stable_across_pole_quantiles(
    fitted_scorer: OrientedKantorovichScorer,
) -> None:
    """Le pôle ``x⁺`` est hors du nuage : ``direction_cos_`` mesure la part de la
    direction qui tient à cette extrapolation."""
    assert fitted_scorer.direction_cos_ > 0.9


@requires_ott
def test_score_variants_are_consistent(X_gaussian: np.ndarray) -> None:
    """Les trois variantes de §6.3 : le cône annule hors du cône, la modulation
    pondère le rang par l'alignement, toutes deux restent dans ``[0, 1]``."""
    for variant in ("cone", "modulated"):
        scorer = OrientedKantorovichScorer(
            epsilon=_TEST_EPSILON, score=variant
        ).fit(X_gaussian)
        scores = scorer.predict(X_gaussian)
        assert np.all(scores >= 0.0)
        assert np.all(scores <= 1.0 + 1e-6)
        if variant == "cone":
            outside = (
                scorer.signs(X_gaussian) @ scorer.direction_
                < np.cos(np.radians(scorer.theta0_degrees))
            )
            assert np.all(scores[outside] == 0.0)
            assert np.any(scores > 0.0)


@requires_ott
@pytest.mark.parametrize("alert_score", ["projected", "radial"])
def test_alert_share_matches_the_nominal_level(
    X_gaussian: np.ndarray, alert_score: str
) -> None:
    """M-07 : le seuil split-conforme est le ``⌈(1-α)(n_cal+1)⌉``-ième plus petit
    score de non-conformité, donc la part d'alertes sur la calibration vaut ``α``."""
    scorer = OrientedKantorovichScorer(
        epsilon=_TEST_EPSILON, alpha=0.05, alert_score=alert_score
    ).fit(X_gaussian)
    report = scorer.fit_report(X_gaussian)
    assert report.alert_share == pytest.approx(0.05, abs=0.02)
    assert report.n_cal == report.n_fit == 1000
    assert np.issubdtype(scorer.alert(X_gaussian).dtype, np.bool_)


@requires_ott
def test_descriptive_calibration_uses_the_whole_fitting_sample(
    X_gaussian: np.ndarray,
) -> None:
    """``conformal="fit"`` : seuil descriptif, aucun échantillon réservé."""
    scorer = OrientedKantorovichScorer(
        epsilon=_TEST_EPSILON, conformal="fit"
    ).fit(X_gaussian)
    report = scorer.fit_report(X_gaussian)
    assert (report.n_fit, report.n_cal) == (2000, 0)
    assert report.alert_share == pytest.approx(0.05, abs=0.02)


@requires_ott
def test_fit_is_reproducible(X_gaussian: np.ndarray) -> None:
    """La graine fixe le sous-échantillonnage, la coupe de calibration et la grille."""
    scores = [
        OrientedKantorovichScorer(epsilon=_TEST_EPSILON, seed=3)
        .fit(X_gaussian)
        .predict(X_gaussian)
        for _ in range(2)
    ]
    np.testing.assert_allclose(scores[0], scores[1])


@requires_ott
def test_subsampling_bounds_the_sinkhorn_problem() -> None:
    """D-11 : au-delà de ``fit_sample_size``, l'ajustement porte sur un
    sous-échantillon, tous les points restant transportés par lots."""
    X = np.random.default_rng(1).normal(size=(30_000, 3))
    scorer = OrientedKantorovichScorer(
        epsilon=_TEST_EPSILON, fit_sample_size=5000, batch_size=4096
    ).fit(X)
    report = scorer.fit_report(X)
    assert report.n_fit + report.n_cal == 5000
    assert scorer.predict(X).shape == (30_000,)


@requires_ott
def test_rejects_unknown_categorical_parameters(X_gaussian: np.ndarray) -> None:
    """Les paramètres catégoriels sont validés dans ``fit`` (convention sklearn)."""
    with pytest.raises(ValueError, match="score"):
        OrientedKantorovichScorer(score="euclidean").fit(X_gaussian)
    with pytest.raises(ValueError, match="alpha"):
        OrientedKantorovichScorer(alpha=1.5).fit(X_gaussian)


@requires_ott
def test_scorer_follows_the_sklearn_estimator_api() -> None:
    """``BaseEstimator`` : ``get_params`` et ``clone`` (I-03)."""
    from sklearn.base import clone

    scorer = OrientedKantorovichScorer(epsilon=0.02, score="modulated")
    assert scorer.get_params()["score"] == "modulated"
    assert clone(scorer).get_params() == scorer.get_params()


# ──────────────────────────────────────────────────────────────────────
# Diagnostic d'ellipticité
# ──────────────────────────────────────────────────────────────────────


@requires_ott
def test_ellipticity_screen_compares_with_the_linear_counterparts(
    fitted_scorer: OrientedKantorovichScorer, X_gaussian: np.ndarray
) -> None:
    """I-10/M-06 : sur un nuage gaussien, le rang center-outward est une
    transformation monotone de la distance de Mahalanobis au centre robuste, et
    le score orienté coïncide avec la projection blanchie — les deux τ sont donc
    élevés, ce qui *décrit* la redondance sans interdire le calcul (D-07)."""
    screen = ellipticity_screen(
        X_gaussian, fitted_scorer, covariance_estimator="empirical"
    )
    assert sorted(screen) == ["tau_proj", "tau_rank"]
    assert screen["tau_proj"] > 0.85
    assert screen["tau_rank"] > 0.85
