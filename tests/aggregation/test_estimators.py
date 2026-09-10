"""Tests des estimateurs sklearn (``macroforecast.trade.aggregation.estimators``).

Couverture des deux estimateurs qui refusent de choisir une pondération et
explorent le simplexe entier — :class:`ConeQuantileScorer` (A-01) et
:class:`SmaaScorer` — et des registres qu'ils partagent avec
:class:`WeightedAggregator` (A-02, A-03, A-04).
"""

from __future__ import annotations

import numpy as np
import pytest

from macroforecast.trade.aggregation.estimators import (
    AGGREGATION_REGISTRY,
    _WEIGHT_FREE_AGGREGATIONS,
    ConeQuantileScorer,
    SmaaScorer,
    WeightedAggregator,
)


# ──────────────────────────────────────────────────────────────────────
# Registres
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "aggregation", ["rank_mean", "vikor", "whitened_projection"]
)
def test_new_aggregations_are_registered(aggregation: str) -> None:
    """Les trois méthodes ajoutées sont pilotables par configuration."""
    assert aggregation in AGGREGATION_REGISTRY


def test_weight_free_aggregations_are_the_three_implicit_ones() -> None:
    """MPI, Mahalanobis et projection blanchie portent leur pondération
    implicitement : aucun vecteur de poids ne doit leur être ajusté."""
    assert _WEIGHT_FREE_AGGREGATIONS == {"mpi", "mahalanobis", "whitened_projection"}


@pytest.mark.parametrize("aggregation", ["rank_mean", "vikor"])
def test_weighted_aggregator_drives_the_new_aggregations(
    X_minmax: np.ndarray, aggregation: str
) -> None:
    """Bout en bout : pondération endogène + nouvelle agrégation."""
    aggregator = WeightedAggregator(weighting="entropy", aggregation=aggregation)
    scores = aggregator.fit(X_minmax).predict(X_minmax)
    assert scores.shape == (X_minmax.shape[0],)
    assert aggregator.weights_.shape == (X_minmax.shape[1],)


def test_weighted_aggregator_skips_weights_for_whitened_projection(
    X_uniform: np.ndarray,
) -> None:
    """Agrégation sans poids : ``weights_`` reste ``None`` et aucun
    avertissement n'est émis."""
    aggregator = WeightedAggregator(aggregation="whitened_projection").fit(X_uniform)
    assert aggregator.weights_ is None
    scores = aggregator.predict(X_uniform)
    assert scores.shape == (X_uniform.shape[0],)


# ──────────────────────────────────────────────────────────────────────
# ConeQuantileScorer (A-01)
# ──────────────────────────────────────────────────────────────────────


def test_cone_quantile_scorer_reuses_its_draws(X_minmax: np.ndarray) -> None:
    """Les tirages sont fixés dans ``fit`` : deux ``predict`` successifs
    renvoient exactement le même score."""
    scorer = ConeQuantileScorer(n_draws=200, random_state=0).fit(X_minmax)
    np.testing.assert_array_equal(scorer.predict(X_minmax), scorer.predict(X_minmax))
    assert scorer.weight_draws_.shape == (200, X_minmax.shape[1])


def test_cone_quantile_scorer_bounds_frame_the_score(X_minmax: np.ndarray) -> None:
    """Le score par défaut est la borne « pire cas » ; l'écart des deux bornes
    mesure l'indétermination par produit."""
    scorer = ConeQuantileScorer(n_draws=200).fit(X_minmax)
    lower, upper = scorer.bounds(X_minmax)
    np.testing.assert_array_equal(scorer.predict(X_minmax), lower)
    assert np.all(lower <= upper)


def test_cone_quantile_scorer_upper_bound_dominates_lower(
    X_minmax: np.ndarray,
) -> None:
    """``bound="upper"`` renvoie la borne « meilleur cas »."""
    lower_scorer = ConeQuantileScorer(n_draws=200, random_state=0).fit(X_minmax)
    upper_scorer = ConeQuantileScorer(
        n_draws=200, random_state=0, bound="upper"
    ).fit(X_minmax)
    assert np.all(upper_scorer.predict(X_minmax) >= lower_scorer.predict(X_minmax))


def test_cone_quantile_scorer_rejects_unknown_bound(X_minmax: np.ndarray) -> None:
    """Une borne inconnue lève ``ValueError`` dès ``fit``."""
    with pytest.raises(ValueError):
        ConeQuantileScorer(bound="median").fit(X_minmax)


# ──────────────────────────────────────────────────────────────────────
# SmaaScorer
# ──────────────────────────────────────────────────────────────────────


def test_smaa_scorer_returns_the_confidence_factor(X_minmax: np.ndarray) -> None:
    """Le score est la part des pondérations admissibles plaçant le produit
    dans les ``k`` premiers : dans ``[0, 1]``, et ``result_`` conserve le
    ``SmaaResult`` complet."""
    scorer = SmaaScorer(k=50, n_draws=200, random_state=0)
    scores = scorer.fit_predict(X_minmax)
    assert scores.shape == (X_minmax.shape[0],)
    assert scores.min() >= 0.0 and scores.max() <= 1.0
    np.testing.assert_allclose(scores, scorer.result_.confidence_factor)
    assert scorer.result_.central_weight.shape == X_minmax.shape[:1] + (
        X_minmax.shape[1],
    )


def test_smaa_scorer_reuses_its_draws(X_minmax: np.ndarray) -> None:
    """Tirages fixés dans ``fit`` : ``predict`` est déterministe."""
    scorer = SmaaScorer(k=50, n_draws=200, random_state=0).fit(X_minmax)
    np.testing.assert_array_equal(scorer.predict(X_minmax), scorer.predict(X_minmax))
    assert scorer.weight_draws_.shape == (200, X_minmax.shape[1])


def test_smaa_scorer_confidence_factor_ranks_the_dominant_first() -> None:
    """Un produit dominant est dans le top 1 sous *toute* pondération ; un
    produit dominé n'y est jamais."""
    X = np.array([[1.0, 1.0], [0.5, 0.6], [0.0, 0.0]])
    scores = SmaaScorer(k=1, n_draws=200, random_state=0).fit_predict(X)
    np.testing.assert_allclose(scores, [1.0, 0.0, 0.0])


def test_smaa_scorer_rejects_weight_free_aggregation(X_minmax: np.ndarray) -> None:
    """Explorer le simplexe n'a aucun sens pour une agrégation qui ignore ses
    poids : ``ValueError`` explicite."""
    with pytest.raises(ValueError, match="ignores its weights"):
        SmaaScorer(aggregation="mahalanobis").fit(X_minmax)


def test_smaa_scorer_rejects_unknown_aggregation(X_minmax: np.ndarray) -> None:
    """Une agrégation hors registre lève ``ValueError``."""
    with pytest.raises(ValueError, match="Unknown aggregation"):
        SmaaScorer(aggregation="borda").fit(X_minmax)
