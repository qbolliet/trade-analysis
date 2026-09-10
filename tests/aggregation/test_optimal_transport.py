"""Tests du transport optimal (``macroforecast.trade.aggregation.optimal_transport``).

Seule la grille de référence est testable sans ``jax`` ; le solveur de Sinkhorn
est ignoré proprement en son absence. Un ``xfail`` documente M-01/I-03 : la
grille tire les rayons en ``ρ**(1/d)`` (loi de Lebesgue sur la boule) au lieu
de ``ρ`` (loi sphérique uniforme), ce qui rend le contrôle KADR non uniforme.
"""

from __future__ import annotations

import numpy as np
import pytest
from scipy import stats

from macroforecast.trade.aggregation.optimal_transport import (
    ellipticity_screen,
    spherical_uniform_grid,
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


@pytest.mark.xfail(
    strict=True,
    reason="M-01/I-03 : rayons en rho**(1/d) -> normes en loi Beta(d, 1), pas uniformes",
)
def test_spherical_grid_norms_are_uniform() -> None:
    """M-01 : sous la loi sphérique uniforme, ``‖U‖ ~ U[0, 1]`` — c'est la cible
    qui rend légitime le test KS de convergence de la carte de transport."""
    grid = spherical_uniform_grid(4096, 6, seed=0)
    norms = np.linalg.norm(grid, axis=1)
    _, p_value = stats.kstest(norms, "uniform")
    assert p_value > 0.01


# ──────────────────────────────────────────────────────────────────────
# Diagnostic d'ellipticité
# ──────────────────────────────────────────────────────────────────────


def test_ellipticity_screen_is_kendall_tau_between_rankings() -> None:
    """Le diagnostic renvoie le τ_b de Kendall entre les deux scores."""
    a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    b = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    assert ellipticity_screen(a, b) == pytest.approx(1.0)
    assert ellipticity_screen(a, b[::-1]) == pytest.approx(-1.0)


# ──────────────────────────────────────────────────────────────────────
# Solveur de Sinkhorn (dépendance optionnelle jax / ott-jax)
# ──────────────────────────────────────────────────────────────────────


def test_scorer_requires_the_optimal_transport_extra() -> None:
    """Sans ``jax`` / ``ott-jax``, l'ajustement lève un ``ImportError`` explicite ;
    avec la dépendance, ce test est ignoré (le solveur de Sinkhorn relève du
    fichier ``test_optimal_transport`` complet du plan S-4, hors de ce lot)."""
    try:
        import jax  # noqa: F401
    except ImportError:
        from macroforecast.trade.aggregation.optimal_transport import (
            OrientedKantorovichScorer,
        )

        X = np.random.default_rng(0).random((20, 2))
        with pytest.raises(ImportError):
            OrientedKantorovichScorer(n_target=64).fit(X)
    else:
        pytest.skip("jax installé : le lot courant ne couvre pas le solveur Sinkhorn")
