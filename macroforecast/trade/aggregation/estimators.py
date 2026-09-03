"""sklearn-facing weighting + aggregation estimators.

The bridge between the weighting schemes of
:mod:`~macroforecast.trade.aggregation.weights`, the aggregation functions of
:mod:`~macroforecast.trade.aggregation.functions` and a real
``sklearn.pipeline.Pipeline``: :class:`WeightedAggregator` is a genuine
``sklearn.base.BaseEstimator`` with ``fit``/``predict``, so it composes with
the transformers of
:mod:`~macroforecast.trade.aggregation.preprocessing` exactly like any
scikit-learn estimator::

    from sklearn.pipeline import Pipeline
    from macroforecast.trade.aggregation.preprocessing import (
        PolarityOrienter, Winsorizer, make_normalizer,
    )
    from macroforecast.trade.aggregation.estimators import WeightedAggregator

    pipeline = Pipeline([
        ("orient", PolarityOrienter(polarities)),
        ("winsorize", Winsorizer(quantile=0.99)),
        ("scale", make_normalizer("robust")),
        ("aggregate", WeightedAggregator(weighting="critic", aggregation="weighted_sum")),
    ])
    scores = pipeline.fit(X).predict(X)
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Callable, Dict, Optional
# Modules de manipulation de données
import numpy as np
from sklearn.base import BaseEstimator
from sklearn.utils.validation import check_array, check_is_fitted
# Modules du package
from . import functions, weights
from .pareto import dominance_count

# Registre des pondérations, point d'entrée piloté par configuration
WEIGHTING_REGISTRY: Dict[str, Callable[..., Any]] = {
    "entropy": weights.entropy_weights,
    "critic": weights.critic_weights,
    "pca": weights.pca_weights,
    "equal": lambda X: np.full(X.shape[1], 1.0 / X.shape[1]),
}

# Registre des fonctions d'agrégation, point d'entrée piloté par configuration
AGGREGATION_REGISTRY: Dict[str, Callable[..., np.ndarray]] = {
    "weighted_sum": functions.weighted_sum_score,
    "geometric_mean": functions.geometric_mean_score,
    "mpi": functions.mpi_score,
    "topsis": functions.topsis_score,
    "mahalanobis": functions.mahalanobis_score,
}

# Fonctions d'agrégation n'exigeant aucun poids (score déjà défini sans `weighting`)
_WEIGHT_FREE_AGGREGATIONS = frozenset({"mpi", "mahalanobis"})


# Estimateur sklearn de pondération + agrégation
class WeightedAggregator(BaseEstimator):
    """Combine an endogenous weighting scheme with an aggregation function.

    ``fit`` computes and stores the weight vector (skipped when the chosen
    aggregation is weight-free, e.g. ``"mpi"``); ``predict`` applies the
    aggregation function with those fitted weights — the last step of a
    ``sklearn.pipeline.Pipeline``, following ``Pipeline.fit(X).predict(X)``.

    Args:
        weighting: Name of a scheme in :data:`WEIGHTING_REGISTRY`, or
            ``"none"`` to force a weight-free aggregation.
        aggregation: Name of a function in :data:`AGGREGATION_REGISTRY`.
        weighting_params: Extra keyword arguments forwarded to the weighting
            function (e.g. ``{"rotate": True}`` for PCA).
        aggregation_params: Extra keyword arguments forwarded to the
            aggregation function (e.g. ``{"epsilon": 1e-2}`` for the
            geometric mean).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.1, 0.9], [0.9, 0.1], [0.5, 0.5]])
        >>> aggregator = WeightedAggregator(weighting="entropy", aggregation="weighted_sum")
        >>> scores = aggregator.fit(X).predict(X)
        >>> scores.shape
        (3,)
        >>> aggregator.weights_.shape
        (2,)
    """

    # Initialisation
    def __init__(
        self,
        weighting: str = "entropy",
        aggregation: str = "weighted_sum",
        weighting_params: Optional[Dict[str, Any]] = None,
        aggregation_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.weighting = weighting
        self.aggregation = aggregation
        self.weighting_params = weighting_params
        self.aggregation_params = aggregation_params

    # Ajustement : calcul et mémorisation du vecteur de poids
    def fit(self, X: np.ndarray, y: None = None) -> "WeightedAggregator":
        """Compute the weight vector from ``X``.

        Args:
            X: Metric matrix of shape ``(n, d)``, already oriented,
                winsorised and normalised.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``, with ``weights_`` fitted (``None`` for a weight-free
            aggregation).

        Raises:
            ValueError: If ``weighting`` or ``aggregation`` names an unknown
                scheme.
        """
        X = check_array(X)
        self.n_features_in_ = X.shape[1]

        if self.aggregation not in AGGREGATION_REGISTRY:
            raise ValueError(
                f"Unknown aggregation {self.aggregation!r}. "
                f"Available: {sorted(AGGREGATION_REGISTRY)}."
            )

        weight_free = (
            self.weighting == "none" or self.aggregation in _WEIGHT_FREE_AGGREGATIONS
        )
        if weight_free:
            self.weights_ = None
            return self

        if self.weighting not in WEIGHTING_REGISTRY:
            raise ValueError(
                f"Unknown weighting {self.weighting!r}. "
                f"Available: {sorted(WEIGHTING_REGISTRY)}."
            )
        params = self.weighting_params or {}
        result = WEIGHTING_REGISTRY[self.weighting](X, **params)
        # L'ACP renvoie (poids, rapport) ; les autres schémas renvoient le poids seul
        self.weights_ = result[0] if isinstance(result, tuple) else result
        return self

    # Prédiction : application de la fonction d'agrégation avec les poids ajustés
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Apply the aggregation function with the fitted weights.

        Args:
            X: Metric matrix of shape ``(n, d)``, same preprocessing as at
                fit time.

        Returns:
            Score vector of shape ``(n,)``.
        """
        check_is_fitted(self, "n_features_in_")
        X = check_array(X)
        params = self.aggregation_params or {}
        return AGGREGATION_REGISTRY[self.aggregation](X, self.weights_, **params)

    # Alias sklearn conventionnel : ajustement puis prédiction sur les mêmes données
    def fit_predict(self, X: np.ndarray, y: None = None) -> np.ndarray:
        """Fit on ``X`` then predict on the same matrix.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            Score vector of shape ``(n,)``.
        """
        return self.fit(X, y).predict(X)


# Estimateur sklearn du comptage de dominance (sans poids)
class DominanceCountScorer(BaseEstimator):
    """Weight-free scorer wrapping :func:`~macroforecast.trade.aggregation.pareto.dominance_count`.

    Same ``fit``/``predict`` contract as :class:`WeightedAggregator`, so it
    drops into the same pipeline slot as a parameter-free baseline — the
    note's own recommendation as the zero-cost, zero-assumption first step
    of the workflow (§7).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> DominanceCountScorer().fit(X).predict(X)
        array([ 2, -2,  0])
    """

    # Ajustement : aucun état à estimer (le comptage est une statistique de X seul)
    def fit(self, X: np.ndarray, y: None = None) -> "DominanceCountScorer":
        """Validate the input shape.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``.
        """
        X = check_array(X)
        self.n_features_in_ = X.shape[1]
        return self

    # Prédiction : comptage de dominance
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Compute the dominance count of every row.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Integer score vector of shape ``(n,)``.
        """
        check_is_fitted(self, "n_features_in_")
        return dominance_count(check_array(X))
