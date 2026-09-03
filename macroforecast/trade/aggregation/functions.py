"""Aggregation functions.

Implements §5 of the methodological note: once a weight vector is available
(or not — MPI needs none), how the metrics are combined into a single score.
This is an independent question from *how the weights were obtained*
(§4 / :mod:`~macroforecast.trade.aggregation.weights`): it is about how far a
favourable metric can buy back an unfavourable one.

Every function shares the signature ``f(X, weights=None, **params) ->
np.ndarray`` — a vector of shape ``(n,)``, higher meaning more vulnerable —
so that :mod:`~macroforecast.trade.aggregation.estimators` can dispatch on a
name through a plain registry.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Optional
import warnings
# Modules de manipulation de données
import numpy as np
from sklearn.covariance import EmpiricalCovariance, LedoitWolf, MinCovDet
from sklearn.utils.validation import check_array


# ──────────────────────────────────────────────────────────────────────
# Somme pondérée
# ──────────────────────────────────────────────────────────────────────

# Fonction de score par somme pondérée
def weighted_sum_score(X: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """Compute the fully compensatory weighted sum ``Σ_j w_j x_ij``.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        weights: Weight vector of shape ``(d,)``.

    Returns:
        Score vector of shape ``(n,)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 0.0], [0.0, 1.0]])
        >>> weighted_sum_score(X, np.array([0.3, 0.7]))
        array([0.3, 0.7])
    """
    X = check_array(X)
    return X @ np.asarray(weights)


# ──────────────────────────────────────────────────────────────────────
# Moyenne géométrique pondérée
# ──────────────────────────────────────────────────────────────────────

# Fonction de score par moyenne géométrique pondérée
def geometric_mean_score(
    X: np.ndarray, weights: np.ndarray, *, epsilon: float = 1e-3
) -> np.ndarray:
    """Compute the weighted geometric mean ``Π_j x_ij^{w_j}``.

    Equivalent to a weighted sum on the log scale: substitutability between
    metrics becomes limited, and a value near zero pulls the whole score
    down. Values are shifted by ``epsilon`` before the log to avoid ``-inf``
    on an exact zero — the note flags this shift itself as worth a
    sensitivity check.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity,
            non-negative.
        weights: Weight vector of shape ``(d,)``.
        epsilon: Additive shift applied before taking the logarithm.

    Returns:
        Score vector of shape ``(n,)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 1.0], [4.0, 0.25]])
        >>> round(float(geometric_mean_score(X, np.array([0.5, 0.5]))[0]), 3)
        1.001
    """
    X = check_array(X)
    log_terms = np.log(X + epsilon)
    return np.exp(log_terms @ np.asarray(weights))


# ──────────────────────────────────────────────────────────────────────
# Indice à pénalité de déséquilibre (Mazziotta-Pareto)
# ──────────────────────────────────────────────────────────────────────

# Fonction de score par indice de Mazziotta-Pareto
def mpi_score(X: np.ndarray, weights: Optional[np.ndarray] = None) -> np.ndarray:
    """Compute the Mazziotta-Pareto penalised-imbalance index ``MPI+``.

    Rescales every metric to mean 100 / standard deviation 10, then penalises
    a product's *horizontal* dispersion across its own metrics:
    ``MPI+_i = M_i + S_i² / M_i``, where ``M_i`` and ``S_i`` are the row mean
    and standard deviation. Free of any weight by construction — the
    ``weights`` argument only exists for signature uniformity with the other
    aggregation functions and triggers a warning if supplied.

    Warning:
        Not monotone (property 1.5 of the note): raising a product's lowest
        metric lowers its row dispersion ``S_i``, and can therefore *lower*
        the index. This is intrinsic to any imbalance penalty and must be
        measured (dominance-violation rate), not silently accepted.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        weights: Ignored; present only for signature uniformity.

    Returns:
        Score vector of shape ``(n,)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 3.0], [3.0, 1.0]])
        >>> scores = mpi_score(X)
        >>> [round(float(s), 3) for s in scores]
        [100.0, 101.0, 101.0]
    """
    if weights is not None:
        warnings.warn(
            "mpi_score ignores `weights`: the Mazziotta-Pareto index is "
            "weight-free by construction.",
            stacklevel=2,
        )
    X = check_array(X)
    column_mean = X.mean(axis=0)
    column_std = X.std(axis=0, ddof=1) if X.shape[0] > 1 else np.ones(X.shape[1])
    column_std = np.where(column_std > 0, column_std, 1.0)

    z = 100.0 + 10.0 * (X - column_mean) / column_std
    row_mean = z.mean(axis=1)
    row_std = z.std(axis=1, ddof=0)
    return row_mean + (row_std**2) / row_mean


# ──────────────────────────────────────────────────────────────────────
# TOPSIS
# ──────────────────────────────────────────────────────────────────────

# Fonction de score TOPSIS
def topsis_score(
    X: np.ndarray,
    weights: np.ndarray,
    *,
    robust: bool = False,
    quantile: float = 0.01,
) -> np.ndarray:
    """Rank products by relative closeness to an empirical "ideal" pole.

    Both the ideal (maximally vulnerable) and anti-ideal poles are built from
    the data itself, then every product is scored by
    ``D⁻ / (D⁺ + D⁻)``, its normalised Euclidean distance to the anti-ideal
    relative to the sum of both distances.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        weights: Weight vector of shape ``(d,)``.
        robust: When ``True``, the poles are taken at ``quantile`` /
            ``1 - quantile`` instead of the raw min/max, curbing the
            sensitivity to extreme points the note flags for the plain
            variant.
        quantile: Tail quantile used when ``robust=True``.

    Returns:
        Score vector of shape ``(n,)``, in ``[0, 1]``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]])
        >>> scores = topsis_score(X, np.array([0.5, 0.5]))
        >>> round(float(scores[2]), 6)
        0.5
    """
    X = check_array(X)
    weights = np.asarray(weights)

    # Normalisation vectorielle pondérée
    norm = np.sqrt(np.sum(X**2, axis=0))
    norm = np.where(norm > 0, norm, 1.0)
    v = weights * X / norm

    if robust:
        pole_positive = np.quantile(v, 1.0 - quantile, axis=0)
        pole_negative = np.quantile(v, quantile, axis=0)
    else:
        pole_positive = v.max(axis=0)
        pole_negative = v.min(axis=0)

    distance_positive = np.sqrt(np.sum((v - pole_positive) ** 2, axis=1))
    distance_negative = np.sqrt(np.sum((v - pole_negative) ** 2, axis=1))
    denominator = distance_positive + distance_negative
    denominator = np.where(denominator > 0, denominator, 1.0)
    return distance_negative / denominator


# ──────────────────────────────────────────────────────────────────────
# Distance de Mahalanobis à l'anti-idéal
# ──────────────────────────────────────────────────────────────────────

# Registre des estimateurs de covariance robustes
_COVARIANCE_ESTIMATORS = {
    "mcd": MinCovDet,
    "ledoit_wolf": LedoitWolf,
    "empirical": EmpiricalCovariance,
}


# Fonction de score par distance de Mahalanobis
def mahalanobis_score(
    X: np.ndarray,
    weights: Optional[np.ndarray] = None,
    *,
    covariance_estimator: str = "mcd",
    anti_ideal_quantile: float = 0.01,
) -> np.ndarray:
    """Compute the Mahalanobis distance to an empirical anti-ideal pole.

    Whitens the metric space by the (robust) covariance of the cloud before
    measuring the distance, so that correlated metrics no longer count the
    same underlying information twice — the equivalent of a weighted sum on
    the decorrelated directions rather than on the raw metrics (see the
    note's remark on the implicit weights carried by ``Σ^{-1/2}``).

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        weights: Ignored; present only for signature uniformity (the
            weighting here is entirely carried by the covariance structure).
        covariance_estimator: ``"mcd"`` (minimum covariance determinant,
            robust — the note's recommendation), ``"ledoit_wolf"`` (shrinkage,
            preferable when ``d`` is large relative to ``n``) or
            ``"empirical"`` (plain sample covariance).
        anti_ideal_quantile: Marginal quantile defining the anti-ideal pole
            (the least-vulnerable synthetic product), per column.

    Returns:
        Score vector of shape ``(n,)``, non-negative.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> X = rng.normal(size=(50, 3)) + 5.0
        >>> scores = mahalanobis_score(X, covariance_estimator="empirical")
        >>> bool(np.all(scores >= 0))
        True
    """
    if weights is not None:
        warnings.warn(
            "mahalanobis_score ignores `weights`: the implicit weighting is "
            "carried entirely by the covariance structure.",
            stacklevel=2,
        )
    X = check_array(X)
    if covariance_estimator not in _COVARIANCE_ESTIMATORS:
        raise ValueError(
            f"Unknown covariance_estimator {covariance_estimator!r}. "
            f"Available: {sorted(_COVARIANCE_ESTIMATORS)}."
        )

    estimator = _COVARIANCE_ESTIMATORS[covariance_estimator]().fit(X)
    precision = estimator.get_precision()
    anti_ideal = np.quantile(X, anti_ideal_quantile, axis=0)

    centered = X - anti_ideal
    squared = np.einsum("ij,jk,ik->i", centered, precision, centered)
    return np.sqrt(np.maximum(squared, 0.0))
