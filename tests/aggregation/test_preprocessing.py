"""Tests du prétraitement (``macroforecast.trade.aggregation.preprocessing``).

Polarité, winsorisation, normalisations (rang, médiane/MAD) et diagnostics de
corrélation (KMO, Bartlett). Un ``xfail`` documente I-11 (``Winsorizer`` ne
supporte pas ``quantile=None`` alors que ce doit devenir le défaut).
"""

from __future__ import annotations

import numpy as np
import pytest

from macroforecast.trade.aggregation.base import AggregationConfig, split_frame
from macroforecast.trade.aggregation.preprocessing import (
    GaussianQuantileScaler,
    MedianMadScaler,
    PolarityOrienter,
    RankScaler,
    Winsorizer,
    bartlett_sphericity,
    kmo_statistic,
    spearman_correlation_matrix,
)


# ──────────────────────────────────────────────────────────────────────
# Orientation des polarités
# ──────────────────────────────────────────────────────────────────────


def test_polarity_orienter_flips_only_negative_columns(X_uniform: np.ndarray) -> None:
    """Seules les colonnes de polarité ``-1`` changent de signe (transformation ``x ↦ -x``)."""
    oriented = PolarityOrienter([1, -1, 1]).fit_transform(X_uniform)
    np.testing.assert_array_equal(oriented[:, 0], X_uniform[:, 0])
    np.testing.assert_array_equal(oriented[:, 1], -X_uniform[:, 1])
    np.testing.assert_array_equal(oriented[:, 2], X_uniform[:, 2])


def test_polarity_orienter_identity_when_polarities_none(X_uniform: np.ndarray) -> None:
    """``polarities=None`` : la matrice est laissée telle quelle."""
    np.testing.assert_array_equal(
        PolarityOrienter().fit_transform(X_uniform), X_uniform
    )


def test_polarity_orienter_rejects_length_mismatch(X_uniform: np.ndarray) -> None:
    """Un vecteur de polarités de mauvaise longueur lève ``ValueError``."""
    with pytest.raises(ValueError):
        PolarityOrienter([1, -1]).fit(X_uniform)


def test_polarity_orientation_preserves_correlation_structure(X_uniform: np.ndarray) -> None:
    """L'orientation affine ``x ↦ -x`` laisse invariante la valeur absolue des corrélations."""
    oriented = PolarityOrienter([1, -1, 1]).fit_transform(X_uniform)
    np.testing.assert_allclose(
        np.abs(np.corrcoef(oriented, rowvar=False)),
        np.abs(np.corrcoef(X_uniform, rowvar=False)),
    )


# ──────────────────────────────────────────────────────────────────────
# Winsorisation
# ──────────────────────────────────────────────────────────────────────


def test_winsorizer_caps_upper_tail() -> None:
    """Les valeurs au-dessus du quantile ajusté sont rabattues sur la borne."""
    X = np.arange(100, dtype=float).reshape(-1, 1)
    out = Winsorizer(quantile=0.9).fit_transform(X)
    bound = np.quantile(X, 0.9)
    assert out.max() == pytest.approx(bound)
    # Les valeurs sous la borne sont inchangées
    below = X[:, 0] <= bound
    np.testing.assert_array_equal(out[below, 0], X[below, 0])


def test_winsorizer_two_sided_also_floors_lower_tail() -> None:
    """``two_sided=True`` plafonne et planchéie symétriquement."""
    X = np.arange(100, dtype=float).reshape(-1, 1)
    out = Winsorizer(quantile=0.9, two_sided=True).fit_transform(X)
    assert out.min() == pytest.approx(np.quantile(X, 0.1))
    assert out.max() == pytest.approx(np.quantile(X, 0.9))


def test_winsorizer_preserves_order_below_the_cap() -> None:
    """Sous la borne, le plafonnement est l'identité : l'ordre des observations
    non plafonnées est strictement conservé."""
    X = np.arange(100, dtype=float).reshape(-1, 1)
    out = Winsorizer(quantile=0.9).fit_transform(X)
    below = X[:, 0] < np.quantile(X, 0.9)
    diffs = np.diff(out[below, 0])
    assert np.all(diffs > 0)


@pytest.mark.xfail(strict=True, reason="I-11 : quantile=None doit agir comme l'identité (défaut cible)")
def test_winsorizer_none_quantile_is_identity(X_uniform: np.ndarray) -> None:
    """I-11 : ``Winsorizer(quantile=None)`` doit devenir une identité."""
    out = Winsorizer(quantile=None).fit_transform(X_uniform)
    np.testing.assert_array_equal(out, X_uniform)


# ──────────────────────────────────────────────────────────────────────
# Normalisation par rang
# ──────────────────────────────────────────────────────────────────────


def test_rank_scaler_exact_on_fitted_sample() -> None:
    """``fit_transform`` renvoie ``(rang - 1) / (n - 1)``, extrêmes à 0 et 1."""
    out = RankScaler().fit_transform(np.array([[30.0], [10.0], [20.0], [40.0]]))
    np.testing.assert_allclose(out.ravel(), [2 / 3, 0.0, 1 / 3, 1.0])


def test_rank_scaler_averages_tied_ranks() -> None:
    """Les ex æquo reçoivent la moyenne de leurs rangs."""
    out = RankScaler().fit_transform(np.array([[10.0], [10.0], [20.0], [40.0]]))
    np.testing.assert_allclose(out.ravel(), [1 / 6, 1 / 6, 2 / 3, 1.0])


def test_rank_scaler_out_of_sample_is_monotone_and_clipped() -> None:
    """Hors échantillon (référence sans ex æquo) : interpolation croissante,
    bornée à ``[0, 1]``."""
    scaler = RankScaler().fit(np.array([[0.0], [1.0], [2.0], [3.0], [4.0]]))
    out = scaler.transform(np.array([[-5.0], [1.5], [100.0]])).ravel()
    assert out[0] == 0.0 and out[-1] == 1.0
    assert 0.0 < out[1] < 1.0


# ──────────────────────────────────────────────────────────────────────
# Normalisation médiane / MAD
# ──────────────────────────────────────────────────────────────────────


def test_median_mad_scaler_centers_on_median() -> None:
    """``(x - médiane) / MAD`` : la médiane est envoyée sur 0."""
    X = np.array([[1.0], [2.0], [3.0], [4.0], [5.0]])
    out = MedianMadScaler().fit_transform(X)
    np.testing.assert_allclose(out.ravel(), [-2.0, -1.0, 0.0, 1.0, 2.0])


def test_median_mad_scaler_is_insensitive_to_extremes() -> None:
    """Un point aberrant ne déplace ni la médiane ni le MAD des autres points."""
    base = np.array([[1.0], [2.0], [3.0], [4.0], [5.0]])
    contaminated = np.array([[1.0], [2.0], [3.0], [4.0], [1000.0]])
    fitted_base = MedianMadScaler().fit(base)
    fitted_contaminated = MedianMadScaler().fit(contaminated)
    assert fitted_base.median_ == fitted_contaminated.median_
    assert fitted_base.mad_ == fitted_contaminated.mad_


def test_gaussian_quantile_scaler_maps_median_to_zero() -> None:
    """La médiane empirique est envoyée sur 0 (marge normale centrée)."""
    X = np.linspace(0.0, 1.0, 101).reshape(-1, 1)
    out = GaussianQuantileScaler().fit_transform(X)
    assert out[50, 0] == pytest.approx(0.0, abs=1e-6)


# ──────────────────────────────────────────────────────────────────────
# Diagnostics de corrélation
# ──────────────────────────────────────────────────────────────────────


def test_spearman_matrix_is_symmetric_unit_diagonal(X_correlated: np.ndarray) -> None:
    """Matrice de Spearman : symétrique, diagonale unité, forme ``(d, d)``."""
    matrix = spearman_correlation_matrix(X_correlated)
    assert matrix.shape == (4, 4)
    np.testing.assert_allclose(np.diag(matrix), 1.0)
    np.testing.assert_allclose(matrix, matrix.T)


def test_spearman_matrix_detects_monotone_link() -> None:
    """Deux colonnes liées de façon monotone : corrélation de rang égale à 1."""
    X = np.column_stack([np.arange(10.0), np.arange(10.0) ** 2])
    np.testing.assert_allclose(spearman_correlation_matrix(X), np.ones((2, 2)))


def test_kmo_and_bartlett_separate_structured_from_independent(
    X_correlated: np.ndarray,
) -> None:
    """Structure factorielle connue : KMO élevé et sphéricité rejetée ; bruit
    indépendant : KMO faible et sphéricité non rejetée."""
    kmo_structured, _ = kmo_statistic(X_correlated)
    _, p_structured = bartlett_sphericity(X_correlated)

    independent = np.random.default_rng(2).normal(size=(300, 4))
    kmo_independent, _ = kmo_statistic(independent)
    _, p_independent = bartlett_sphericity(independent)

    assert kmo_structured > 0.6 > kmo_independent
    assert p_structured < 0.01 < p_independent


# ──────────────────────────────────────────────────────────────────────
# Découpage de table et valeurs manquantes
# ──────────────────────────────────────────────────────────────────────


def test_split_frame_propagates_missing_values(df_metrics_toy) -> None:
    """I-16 : ``split_frame`` transmet les ``NaN`` tels quels ; en aval un
    estimateur sklearn les rejette (le runner devra filtrer par méthode)."""
    config = AggregationConfig(
        id_columns=("id",), metric_columns=("HHI", "CDI2", "CDI3")
    )
    X, index = split_frame(df_metrics_toy, config)
    assert np.isnan(X).sum() == 3
    assert len(index) == 40

    from macroforecast.trade.aggregation.estimators import WeightedAggregator

    with pytest.raises(ValueError):
        WeightedAggregator(weighting="equal").fit(X)
