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

The family spans the whole compensation spectrum: fully compensatory
(:func:`weighted_sum_score`, :func:`rank_mean_score`), partially
compensatory (:func:`geometric_mean_score`, :func:`mpi_score`,
:func:`vikor_score`), geometric in the whitened space
(:func:`topsis_score`, :func:`mahalanobis_score`,
:func:`whitened_projection_score`) and fully non-compensatory
(:func:`cone_quantile_score`, the worst one-dimensional rank over the
whole simplex).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Optional, Tuple
import warnings
# Modules de manipulation de données
import numpy as np
from scipy import stats
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

    Raises:
        ValueError: If ``X`` holds a negative value — the logarithm would
            return ``NaN`` silently (I-07).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 1.0], [4.0, 0.25]])
        >>> round(float(geometric_mean_score(X, np.array([0.5, 0.5]))[0]), 3)
        1.001
    """
    X = check_array(X)
    # Validation explicite : sur donnees centrees-reduites le log renverrait
    # des `NaN` sans message (I-07)
    if np.any(X < 0.0):
        raise ValueError(
            f"geometric_mean_score requires a non-negative matrix (min = "
            f"{float(X.min()):.6g}); apply a min-max or rank normalisation "
            "first — a standardisation ('standard', 'robust', "
            "'quantile_gaussian') produces negative values on which the "
            "logarithm is undefined."
        )
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
# VIKOR
# ──────────────────────────────────────────────────────────────────────


# Fonction de score VIKOR
def vikor_score(
    X: np.ndarray,
    weights: np.ndarray,
    *,
    v: float = 0.5,
    robust: bool = False,
    quantile: float = 0.01,
) -> np.ndarray:
    """Rank products by the VIKOR compromise between group utility and regret.

    The natural sibling of :func:`topsis_score`, with an *explicit*
    compensation parameter: the normalised gap to the ideal pole is summed
    over the metrics (group utility ``S_i``, fully compensatory) and taken at
    its worst metric (individual regret ``R_i``, non-compensatory), then the
    two are blended by ``v`` (Opricovic & Tzeng 2004)::

        S_i = Σ_j w_j (A⁺_j - x_ij) / (A⁺_j - A⁻_j)
        R_i = max_j w_j (A⁺_j - x_ij) / (A⁺_j - A⁻_j)
        Q_i = v (S_i - S⁻) / (S⁺ - S⁻) + (1 - v) (R_i - R⁻) / (R⁺ - R⁻)

    ``S`` and ``R`` are *costs* (distance to the ideal), so the score
    returned is ``1 - Q_i`` to keep the module convention "higher = more
    vulnerable" (D-16). Strictly monotone for ``w > 0`` and ``v > 0``.

    A degenerate blending denominator (``S⁺ = S⁻``, or ``R⁺ = R⁻``, i.e. a
    cloud where every product carries the same utility or the same regret)
    contributes ``0`` to ``Q`` rather than a ``NaN``: the corresponding term
    simply carries no information.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        weights: Weight vector of shape ``(d,)``, a point of the simplex.
        v: Weight of the group utility in the compromise, in ``[0, 1]``;
            ``1`` gives the pure (compensatory) utility, ``0`` the pure
            (non-compensatory) regret, ``0.5`` the "consensus" default of
            the original paper.
        robust: When ``True``, the poles ``A⁺`` / ``A⁻`` are taken at
            ``1 - quantile`` / ``quantile`` instead of the raw max/min, as
            for :func:`topsis_score`.
        quantile: Tail quantile used when ``robust=True``.

    Returns:
        Score vector of shape ``(n,)``, in ``[0, 1]`` (up to the pole
        clipping when ``robust=True``).

    Raises:
        ValueError: If ``v`` lies outside ``[0, 1]``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 1.0], [0.5, 0.5], [0.0, 0.0]])
        >>> scores = vikor_score(X, np.array([0.5, 0.5]))
        >>> [round(float(s), 3) for s in scores]
        [1.0, 0.5, 0.0]
    """
    if not 0.0 <= v <= 1.0:
        raise ValueError(f"The compromise parameter v must lie in [0, 1], got {v!r}.")
    X = check_array(X)
    weights = np.asarray(weights, dtype=float)

    if robust:
        pole_positive = np.quantile(X, 1.0 - quantile, axis=0)
        pole_negative = np.quantile(X, quantile, axis=0)
    else:
        pole_positive = X.max(axis=0)
        pole_negative = X.min(axis=0)

    # Ecart normalise a l'ideal ; colonne constante (etendue nulle) neutralisee
    spread = pole_positive - pole_negative
    spread = np.where(spread > 0, spread, 1.0)
    gap = weights * (pole_positive - X) / spread

    utility = gap.sum(axis=1)
    regret = gap.max(axis=1)
    return 1.0 - (
        v * _minmax_share(utility) + (1.0 - v) * _minmax_share(regret)
    )


# Fonction utilitaire de position relative dans l'etendue d'un vecteur
def _minmax_share(values: np.ndarray) -> np.ndarray:
    """Rescale a vector to ``[0, 1]`` by its own range, ``0`` if degenerate.

    Args:
        values: Vector of shape ``(n,)``.

    Returns:
        Vector of shape ``(n,)`` in ``[0, 1]``; all-zero when the range
        vanishes (the term then carries no information, VIKOR convention).

    Examples:
        >>> import numpy as np
        >>> _minmax_share(np.array([1.0, 2.0, 3.0]))
        array([0. , 0.5, 1. ])
        >>> _minmax_share(np.array([2.0, 2.0]))
        array([0., 0.])
    """
    low = values.min()
    span = values.max() - low
    if span <= 0:
        return np.zeros_like(values)
    return (values - low) / span


# ──────────────────────────────────────────────────────────────────────
# Score de rang moyen (Borda sur les métriques)
# ──────────────────────────────────────────────────────────────────────


# Fonction de score par moyenne des rangs normalisés
def rank_mean_score(
    X: np.ndarray, weights: Optional[np.ndarray] = None
) -> np.ndarray:
    """Average the normalised within-column ranks ``(rg - 1) / (n - 1)``.

    The reference method of the applied literature (Arjona et al. 2023,
    "rank approach"; Borda 1781): every metric is replaced by the rank it
    assigns to each product, rescaled to ``[0, 1]``, and the ranks are then
    averaged — equally by default, or with the supplied weights. Ties are
    averaged.

    Note:
        Being built on ranks only, the score is **invariant under any
        strictly increasing transformation of the columns**: winsorising,
        taking a logarithm or applying a min-max rescaling of a metric leaves
        it unchanged. That is the whole point (it removes the influence of
        the marginal shapes) and its whole cost (an order-of-magnitude gap
        between two products counts exactly as much as an epsilon).
        Equivalent to a weighted sum applied to a ``RankScaler`` output,
        exposed here under its own name for the readability of the
        configuration (A-02).

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        weights: Weight vector of shape ``(d,)``; ``None`` (default) uses
            equal weights ``1 / d``.

    Returns:
        Score vector of shape ``(n,)``, in ``[0, 1]`` (all-zero when
        ``n == 1``).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[3.0, 1.0], [2.0, 2.0], [1.0, 3.0]])
        >>> rank_mean_score(X)
        array([0.5, 0.5, 0.5])
        >>> rank_mean_score(np.array([[1.0], [2.0], [3.0]]))
        array([0. , 0.5, 1. ])
    """
    X = check_array(X)
    n, d = X.shape
    weights = np.full(d, 1.0 / d) if weights is None else np.asarray(weights, dtype=float)

    ranks = stats.rankdata(X, method="average", axis=0)
    # Normalisation en [0, 1] ; un seul produit ne definit aucun rang relatif
    normalized = (ranks - 1.0) / (n - 1) if n > 1 else np.zeros_like(ranks)
    return normalized @ weights


# ──────────────────────────────────────────────────────────────────────
# Distance de Mahalanobis
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
    center: str = "anti_ideal",
    anti_ideal_quantile: float = 0.01,
) -> np.ndarray:
    """Compute the Mahalanobis distance to an anti-ideal pole or to the centre.

    Whitens the metric space by the (robust) covariance of the cloud before
    measuring the distance, so that correlated metrics no longer count the
    same underlying information twice. The result is a **norm in the whitened
    space**, not a weighted sum (M-06): it is not oriented, and a product
    lying *below* the anti-ideal on every metric sits at a positive distance
    just like a genuinely vulnerable one. Use
    :func:`whitened_projection_score` for the oriented linear counterpart.

    Two reference points are available:

    * ``center="anti_ideal"`` — the marginal low-tail pole, the historical
      variant kept as a method in its own right, with its non-monotonicity
      measured rather than assumed away;
    * ``center="robust_center"`` — the robust location ``mu`` of the fitted
      covariance estimator. This is the one that matters theoretically: for
      an elliptical distribution, ``d_Mah(x, mu)`` is a strictly increasing
      transformation of the *center-outward* rank ``||T(x)||`` of the Brenier
      map, hence the natural linear reference of the elliptical diagnostic
      (M-06). It measures *atypicality*, not vulnerability: a product extreme
      in the low tail scores as high as one extreme in the high tail.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        weights: Ignored; present only for signature uniformity (the
            weighting here is entirely carried by the covariance structure).
        covariance_estimator: ``"mcd"`` (minimum covariance determinant,
            robust — the note's recommendation, but slow beyond a few tens of
            thousands of rows), ``"ledoit_wolf"`` (shrinkage, preferable when
            ``d`` is large relative to ``n`` or when ``n`` is large) or
            ``"empirical"`` (plain sample covariance).
        center: ``"anti_ideal"`` (default) for the marginal-quantile pole, or
            ``"robust_center"`` for the estimator's own location.
        anti_ideal_quantile: Marginal quantile defining the anti-ideal pole
            (the least-vulnerable synthetic product), per column; ignored
            when ``center="robust_center"``.

    Returns:
        Score vector of shape ``(n,)``, non-negative.

    Raises:
        ValueError: If ``covariance_estimator`` or ``center`` is unknown.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> X = rng.normal(size=(50, 3)) + 5.0
        >>> scores = mahalanobis_score(X, covariance_estimator="empirical")
        >>> bool(np.all(scores >= 0))
        True
        >>> centred = mahalanobis_score(
        ...     X, covariance_estimator="empirical", center="robust_center")
        >>> bool(np.all(centred >= 0))
        True
    """
    if weights is not None:
        warnings.warn(
            "mahalanobis_score ignores `weights`: the implicit weighting is "
            "carried entirely by the covariance structure.",
            stacklevel=2,
        )
    X = check_array(X)
    if center not in ("anti_ideal", "robust_center"):
        raise ValueError(
            f"Unknown center {center!r}. Available: 'anti_ideal', 'robust_center'."
        )

    estimator = _fit_covariance(X, covariance_estimator)
    precision = estimator.get_precision()
    reference = (
        np.quantile(X, anti_ideal_quantile, axis=0)
        if center == "anti_ideal"
        else np.asarray(estimator.location_, dtype=float)
    )

    centered = X - reference
    squared = np.einsum("ij,jk,ik->i", centered, precision, centered)
    return np.sqrt(np.maximum(squared, 0.0))


# ──────────────────────────────────────────────────────────────────────
# Projection blanchie orientée
# ──────────────────────────────────────────────────────────────────────


# Fonction de score par projection blanchie orientée
def whitened_projection_score(
    X: np.ndarray,
    weights: Optional[np.ndarray] = None,
    *,
    covariance_estimator: str = "mcd",
    ideal_quantile: float = 0.99,
) -> np.ndarray:
    """Project the whitened cloud on the whitened direction of the ideal pole.

    ``s(x) = <Sigma^{-1/2}(x - mu), v>`` with ``v = Sigma^{-1/2}(x+ - mu)``
    normalised to unit length, and ``mu``, ``Sigma`` estimated robustly.
    Decorrelates the metrics exactly like :func:`mahalanobis_score`, but keeps
    an **orientation**: unlike a distance, it is signed, so a product in the
    low tail scores below the centre instead of far from it (M-06, A-04).

    Note:
        This is the exact linear counterpart of the oriented Kantorovich
        score in the elliptical case. For an elliptical law of centre ``mu``
        and dispersion ``Sigma``, the Brenier map to the spherical uniform is
        ``T(z) = h(r) Sigma^{-1/2}(z - mu) / r`` with
        ``r = ||Sigma^{-1/2}(z - mu)||``, so the oriented score
        ``<T(z), u*>`` equals ``h(r) / r`` times this projection — the same
        object up to a radial modulation. It is therefore both a method in its
        own right and the reference term of the ellipticity diagnostic
        ``tau_b(s_OT_proj, s_white)`` (I-10).

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity; a
            standardising normalisation (``standard``, ``quantile_gaussian``)
            is the admissible input.
        weights: Ignored; present only for signature uniformity (the
            weighting is carried by the covariance structure and by the
            direction of the ideal pole).
        covariance_estimator: ``"mcd"``, ``"ledoit_wolf"`` or
            ``"empirical"`` — see :func:`mahalanobis_score`.
        ideal_quantile: Marginal quantile defining the ideal pole ``x+``
            (the maximally vulnerable synthetic product), per column.

    Returns:
        Score vector of shape ``(n,)``, signed and centred on the robust
        location.

    Raises:
        ValueError: If ``covariance_estimator`` is unknown, or if the ideal
            pole coincides with the robust centre (no direction to project
            on).

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> X = rng.normal(size=(200, 3))
        >>> scores = whitened_projection_score(X, covariance_estimator="empirical")
        >>> scores.shape
        (200,)
        >>> bool(np.corrcoef(scores, X.sum(axis=1))[0, 1] > 0.99)
        True
    """
    if weights is not None:
        warnings.warn(
            "whitened_projection_score ignores `weights`: the implicit "
            "weighting is carried by the covariance structure and by the "
            "direction of the ideal pole.",
            stacklevel=2,
        )
    X = check_array(X)
    estimator = _fit_covariance(X, covariance_estimator)
    location = np.asarray(estimator.location_, dtype=float)
    whitener = _inverse_sqrt(np.asarray(estimator.covariance_, dtype=float))

    # Direction blanchie du pôle idéal, normalisée
    ideal = np.quantile(X, ideal_quantile, axis=0)
    direction = whitener @ (ideal - location)
    norm = float(np.linalg.norm(direction))
    if norm <= 0:
        raise ValueError(
            "The ideal pole coincides with the robust centre in the whitened "
            "space: no direction to project on. Check `ideal_quantile` and "
            "the dispersion of the metrics."
        )
    return ((X - location) @ whitener) @ (direction / norm)


# Fonction d'ajustement d'un estimateur de covariance du registre
def _fit_covariance(X: np.ndarray, covariance_estimator: str):
    """Fit one of the registered covariance estimators on ``X``.

    Args:
        X: Metric matrix of shape ``(n, d)``.
        covariance_estimator: Key of :data:`_COVARIANCE_ESTIMATORS`.

    Returns:
        The fitted ``sklearn.covariance`` estimator, exposing ``location_``,
        ``covariance_`` and ``get_precision()``.

    Raises:
        ValueError: If the key is unknown.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> fitted = _fit_covariance(rng.normal(size=(50, 2)), "empirical")
        >>> fitted.covariance_.shape
        (2, 2)
    """
    if covariance_estimator not in _COVARIANCE_ESTIMATORS:
        raise ValueError(
            f"Unknown covariance_estimator {covariance_estimator!r}. "
            f"Available: {sorted(_COVARIANCE_ESTIMATORS)}."
        )
    return _COVARIANCE_ESTIMATORS[covariance_estimator]().fit(X)


# Fonction de racine carrée inverse d'une matrice symétrique semi-définie positive
def _inverse_sqrt(covariance: np.ndarray, *, tolerance: float = 1e-12) -> np.ndarray:
    """Compute ``Sigma^{-1/2}`` by symmetric eigendecomposition.

    Args:
        covariance: Symmetric positive semi-definite matrix of shape
            ``(d, d)``.
        tolerance: Relative floor applied to the eigenvalues, guarding a
            near-singular covariance (collinear metrics).

    Returns:
        The symmetric matrix ``Sigma^{-1/2}`` of shape ``(d, d)``.

    Examples:
        >>> import numpy as np
        >>> whitener = _inverse_sqrt(np.array([[4.0, 0.0], [0.0, 9.0]]))
        >>> np.round(whitener, 6)
        array([[0.5     , 0.      ],
               [0.      , 0.333333]])
    """
    # Décomposition symétrique : `eigh` garantit des valeurs propres réelles
    eigenvalues, eigenvectors = np.linalg.eigh((covariance + covariance.T) / 2.0)
    floor = tolerance * max(float(eigenvalues.max()), 1.0)
    eigenvalues = np.maximum(eigenvalues, floor)
    return (eigenvectors * eigenvalues**-0.5) @ eigenvectors.T


# ──────────────────────────────────────────────────────────────────────
# Quantile de cône (Hamel & Kostner)
# ──────────────────────────────────────────────────────────────────────


# Fonction de bornes du quantile de cône
def cone_quantile_bounds(
    X: np.ndarray, weight_draws: np.ndarray, *, block_size: int = 256
) -> Tuple[np.ndarray, np.ndarray]:
    """Bound the cone distribution function over a sample of the simplex.

    Hamel and Kostner (2018) define the *cone distribution function*
    ``F_C(x) = inf_{w in Delta} F_{w'X}(w'x)``: the **worst** one-dimensional
    rank of ``x`` among all non-negative weightings. It is weight-free,
    oriented, monotone for Pareto dominance, and reads as an interpretable
    level ("``x`` stands above ``F_C(x)`` of the population under *every*
    admissible weighting"). The supremum version gives the best case, and the
    gap between the two measures the per-product indeterminacy left by the
    refusal to rank the criteria — the natural complement of SMAA.

    Both are estimated on a finite sample ``w^(1..T)`` of the simplex, so the
    lower bound over-estimates the true infimum and the upper bound
    under-estimates the true supremum.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity. Rank-based,
            so any monotone per-column transformation leaves it unchanged.
        weight_draws: Simplex sample of shape ``(T, d)``, e.g. from
            :func:`~macroforecast.trade.aggregation.weights.dirichlet_weights`.
        block_size: Number of draws scored at once; caps the temporary
            ``(n, block_size)`` projection matrix.

    Returns:
        Tuple ``(lower, upper)`` of two vectors of shape ``(n,)``, both in
        ``(0, 1]``, with ``lower <= upper`` componentwise.

    Raises:
        ValueError: If ``weight_draws`` is not a ``(T, d)`` matrix matching
            the number of columns of ``X``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 1.0], [0.5, 0.5], [0.0, 0.0]])
        >>> W = np.array([[1.0, 0.0], [0.5, 0.5], [0.0, 1.0]])
        >>> lower, upper = cone_quantile_bounds(X, W)
        >>> [round(float(value), 3) for value in lower]
        [1.0, 0.667, 0.333]
    """
    X = check_array(X)
    weight_draws = np.atleast_2d(np.asarray(weight_draws, dtype=float))
    if weight_draws.ndim != 2 or weight_draws.shape[1] != X.shape[1]:
        raise ValueError(
            f"weight_draws must have shape (T, {X.shape[1]}), got "
            f"{weight_draws.shape}."
        )
    n = X.shape[0]
    block_size = max(int(block_size), 1)

    lower = np.full(n, np.inf)
    upper = np.full(n, -np.inf)
    # Vectorisation par blocs de tirages : `X @ W_b'` puis rangs par colonne
    for start in range(0, weight_draws.shape[0], block_size):
        block = weight_draws[start : start + block_size]
        ranks = stats.rankdata(X @ block.T, method="average", axis=0) / n
        lower = np.minimum(lower, ranks.min(axis=1))
        upper = np.maximum(upper, ranks.max(axis=1))
    return lower, upper


# Fonction de score par quantile de cône
def cone_quantile_score(
    X: np.ndarray,
    weight_draws: np.ndarray,
    *,
    bound: str = "lower",
    block_size: int = 256,
) -> np.ndarray:
    """Score by the empirical cone distribution function ``F_C``.

    The fully non-compensatory end of the family: no weighting is chosen, the
    product is judged under the weighting that is *least* favourable to it
    (``bound="lower"``, the cone distribution function proper) or the most
    favourable one (``bound="upper"``). See :func:`cone_quantile_bounds` for
    the definition and the caveats of a finite simplex sample.

    Weakly monotone for Pareto dominance by construction: ``x_i > x_k`` for
    the dominance order implies ``w'x_i >= w'x_k`` for every ``w >= 0``, hence
    a higher rank under every single draw (A-01).

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        weight_draws: Simplex sample of shape ``(T, d)``.
        bound: ``"lower"`` (default, the worst case, i.e. ``F_C`` proper) or
            ``"upper"`` (the best case).
        block_size: Number of draws scored at once.

    Returns:
        Score vector of shape ``(n,)``, in ``(0, 1]``.

    Raises:
        ValueError: If ``bound`` is unknown, or if ``weight_draws`` does not
            match the number of columns of ``X``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 1.0], [0.5, 0.5], [0.0, 0.0]])
        >>> W = np.array([[1.0, 0.0], [0.5, 0.5], [0.0, 1.0]])
        >>> [round(float(s), 3) for s in cone_quantile_score(X, W)]
        [1.0, 0.667, 0.333]
    """
    if bound not in ("lower", "upper"):
        raise ValueError(f"Unknown bound {bound!r}. Available: 'lower', 'upper'.")
    lower, upper = cone_quantile_bounds(X, weight_draws, block_size=block_size)
    return lower if bound == "lower" else upper
