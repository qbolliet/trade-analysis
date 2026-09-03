"""Polarity, winsorisation, normalisation and correlation diagnostics.

Implements §2 of the methodological note: every transformation applied to the
raw metric matrix ``X`` (shape ``(n, d)``) before any weighting or aggregation
is attempted. All transformers follow the scikit-learn ``BaseEstimator`` /
``TransformerMixin`` contract (``fit``/``transform``/``fit_transform``), so
they compose in a real :class:`sklearn.pipeline.Pipeline` alongside the
estimators of :mod:`~macroforecast.trade.aggregation.estimators`.

Two normalisation schemes are custom rather than thin sklearn wrappers,
deliberately:

* :class:`RankScaler` — sklearn has no plain "rank rescaled to ``[0, 1]``"
  transformer (``QuantileTransformer`` targets a uniform *or* normal output
  distribution, not the literature's ``(rank - 1) / (n - 1)`` formula).
* :class:`MedianMadScaler` — sklearn's ``RobustScaler`` centres on the median
  but scales by the interquartile range, not the median absolute deviation
  the note's "robust" scheme calls for (recommended with HHI-like metrics).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
from typing import Optional, Tuple
# Modules de manipulation de données
import numpy as np
from scipy import stats
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import MinMaxScaler, QuantileTransformer, StandardScaler
from sklearn.utils.validation import check_array, check_is_fitted


# ──────────────────────────────────────────────────────────────────────
# Orientation des polarités
# ──────────────────────────────────────────────────────────────────────

# Transformateur d'orientation des métriques en polarité positive
class PolarityOrienter(BaseEstimator, TransformerMixin):
    """Flip every negative-polarity column so that "high = more vulnerable".

    The only orientation the note qualifies as affine (``x ↦ -x``), hence the
    only one that leaves invariant the results of the covariance-based
    methods (PCA, Mahalanobis) — as opposed to ``max - x`` or ``1 / x``.

    Args:
        polarities: Sequence of ``+1``/``-1`` aligned on the input columns.
            ``None`` (default) leaves every column as-is, i.e. no orientation
            is applied — a caller with no negative-polarity metric can omit
            this step entirely.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.2, 10.0], [0.8, 2.0]])
        >>> PolarityOrienter([1, -1]).fit_transform(X)
        array([[  0.2, -10. ],
               [  0.8,  -2. ]])
    """

    # Initialisation
    def __init__(self, polarities: Optional[np.ndarray] = None) -> None:
        self.polarities = polarities

    # Ajustement : validation de la dimension, aucun état à estimer
    def fit(self, X: np.ndarray, y: None = None) -> "PolarityOrienter":
        """Validate the input shape against ``polarities``.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``.
        """
        X = check_array(X)
        self.n_features_in_ = X.shape[1]
        if self.polarities is not None and len(self.polarities) != X.shape[1]:
            raise ValueError(
                f"polarities has length {len(self.polarities)}, "
                f"expected {X.shape[1]} (number of columns of X)."
            )
        return self

    # Transformation : inversion de signe des colonnes de polarité négative
    def transform(self, X: np.ndarray) -> np.ndarray:
        """Flip the negative-polarity columns.

        Args:
            X: Metric matrix of shape ``(n, d)``.

        Returns:
            Oriented matrix, same shape, all columns in positive polarity.
        """
        check_is_fitted(self, "n_features_in_")
        X = check_array(X)
        if self.polarities is None:
            return X
        # Vecteur de signes, diffusé sur les colonnes
        signs = np.asarray(self.polarities)
        return X * signs


# ──────────────────────────────────────────────────────────────────────
# Winsorisation
# ──────────────────────────────────────────────────────────────────────

# Transformateur de winsorisation des queues de distribution
class Winsorizer(BaseEstimator, TransformerMixin):
    """Cap extreme values at an empirical quantile, per column.

    Stabilises scale statistics (min-max range, standard deviation) without
    reordering observations below the cap — a strictly increasing
    per-coordinate transform leaves the Pareto-dominance relation invariant
    (see :mod:`~macroforecast.trade.aggregation.pareto`).

    Args:
        quantile: Upper quantile at which values are capped (0.99 in the
            note, typical for HHI-like concentration indices).
        two_sided: When ``True``, also floors values at ``1 - quantile``.
            ``False`` by default: concentration indices are right-skewed and
            the extremes being sought are precisely the upper tail.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0], [2.0], [3.0], [100.0]])
        >>> Winsorizer(quantile=0.75).fit_transform(X)
        array([[ 1.  ],
               [ 2.  ],
               [ 3.  ],
               [27.25]])
    """

    # Initialisation
    def __init__(self, quantile: float = 0.99, two_sided: bool = False) -> None:
        self.quantile = quantile
        self.two_sided = two_sided

    # Ajustement : bornes empiriques par colonne
    def fit(self, X: np.ndarray, y: None = None) -> "Winsorizer":
        """Compute the per-column capping bounds.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``, with ``upper_`` (and ``lower_`` when ``two_sided``)
            fitted.
        """
        X = check_array(X)
        self.n_features_in_ = X.shape[1]
        self.upper_ = np.quantile(X, self.quantile, axis=0)
        if self.two_sided:
            self.lower_ = np.quantile(X, 1.0 - self.quantile, axis=0)
        return self

    # Transformation : plafonnement (et plancher éventuel)
    def transform(self, X: np.ndarray) -> np.ndarray:
        """Cap the input at the fitted bounds.

        Args:
            X: Metric matrix of shape ``(n, d)``.

        Returns:
            Winsorised matrix, same shape.
        """
        check_is_fitted(self, "upper_")
        X = check_array(X)
        out = np.minimum(X, self.upper_)
        if self.two_sided:
            out = np.maximum(out, self.lower_)
        return out


# ──────────────────────────────────────────────────────────────────────
# Normalisations
# ──────────────────────────────────────────────────────────────────────

# Transformateur de rang normalisé
class RankScaler(BaseEstimator, TransformerMixin):
    """Rescale each column to its normalised rank ``(rank - 1) / (n - 1)``.

    Destroys cardinal information (a huge gap and a tiny gap between
    consecutive ranks become identical) but makes the score invariant to any
    monotone transform of the raw metric — the trade-off documented in the
    note's normalisation table.

    Args:
        interpolation: Interpolation used by :func:`numpy.interp` to place a
            value not seen at fit time within the fitted empirical
            distribution (``transform`` on new data only; ``fit_transform``
            on the fitted sample uses the exact average-rank formula).

    Examples:
        >>> import numpy as np
        >>> RankScaler().fit_transform(np.array([[30.0], [10.0], [20.0]]))
        array([[1. ],
               [0. ],
               [0.5]])
    """

    # Initialisation
    def __init__(self, interpolation: str = "average") -> None:
        self.interpolation = interpolation

    # Ajustement : mémorisation de l'échantillon de référence, trié par colonne
    def fit(self, X: np.ndarray, y: None = None) -> "RankScaler":
        """Store the fitted sample, sorted per column, as the reference CDF.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``.
        """
        X = check_array(X)
        self.n_features_in_ = X.shape[1]
        self.n_samples_in_ = X.shape[0]
        # Echantillon de référence trié, une colonne à la fois
        self.sorted_train_ = np.sort(X, axis=0)
        return self

    # Transformation : rang normalisé, exact sur l'échantillon ajusté
    def transform(self, X: np.ndarray) -> np.ndarray:
        """Map values to their normalised rank against the fitted sample.

        Args:
            X: Metric matrix of shape ``(n, d)``.

        Returns:
            Matrix of ranks rescaled to ``[0, 1]`` per column.
        """
        check_is_fitted(self, "sorted_train_")
        X = check_array(X)
        n = self.n_samples_in_
        if n <= 1:
            return np.zeros_like(X)

        # Cas courant : transformation du jeu ajusté lui-même -> formule
        # exacte avec gestion des ex-æquo (moyenne des rangs concurrents)
        if X.shape[0] == n and np.array_equal(np.sort(X, axis=0), self.sorted_train_):
            ranks = np.apply_along_axis(stats.rankdata, 0, X, method=self.interpolation)
            return (ranks - 1.0) / (n - 1.0)

        # Cas général : interpolation sur la CDF empirique ajustée
        out = np.empty_like(X, dtype=float)
        reference_ranks = (np.arange(n) ) / (n - 1.0)
        for column in range(X.shape[1]):
            out[:, column] = np.interp(
                X[:, column], self.sorted_train_[:, column], reference_ranks
            )
        return out


# Transformateur médiane / écart absolu médian
class MedianMadScaler(BaseEstimator, TransformerMixin):
    """Centre on the median and scale by the median absolute deviation.

    ``(x - median) / MAD``, insensitive to extremes — the note's "robust"
    normalisation scheme, recommended for HHI-like metrics. Deliberately
    distinct from :class:`sklearn.preprocessing.RobustScaler`, which scales
    by the interquartile range rather than the MAD.

    Args:
        eps: Floor applied to the MAD before dividing, guarding against a
            column with more than half its mass at a single value (a
            degenerate but not impossible case for a concentration index).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0], [2.0], [3.0], [4.0], [5.0]])
        >>> MedianMadScaler().fit_transform(X)
        array([[-2.],
               [-1.],
               [ 0.],
               [ 1.],
               [ 2.]])
    """

    # Initialisation
    def __init__(self, eps: float = 1e-12) -> None:
        self.eps = eps

    # Ajustement : médiane et MAD par colonne
    def fit(self, X: np.ndarray, y: None = None) -> "MedianMadScaler":
        """Compute the per-column median and MAD.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``.
        """
        X = check_array(X)
        self.n_features_in_ = X.shape[1]
        self.median_ = np.median(X, axis=0)
        mad = stats.median_abs_deviation(X, axis=0, scale=1.0)
        self.mad_ = np.maximum(mad, self.eps)
        return self

    # Transformation : centrage-réduction robuste
    def transform(self, X: np.ndarray) -> np.ndarray:
        """Apply the fitted median/MAD scaling.

        Args:
            X: Metric matrix of shape ``(n, d)``.

        Returns:
            Scaled matrix, same shape.
        """
        check_is_fitted(self, "median_")
        X = check_array(X)
        return (X - self.median_) / self.mad_


# Transformateur de quantile gaussien
class GaussianQuantileScaler(BaseEstimator, TransformerMixin):
    """Map each column onto a standard normal marginal via its empirical CDF.

    Thin wrapper around
    :class:`sklearn.preprocessing.QuantileTransformer(output_distribution="normal")`,
    exposed under the note's own name — the recommended scheme ahead of a PCA
    or an optimal-transport score (Gaussian margins, correlation structure
    preserved as a copula).

    Args:
        n_quantiles: Number of quantiles used to estimate the CDF. ``None``
            (default) uses one per training sample, capped at 1000 by
            scikit-learn's own default.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0], [2.0], [3.0], [4.0], [5.0]])
        >>> scaled = GaussianQuantileScaler().fit_transform(X)
        >>> round(float(scaled[2, 0]), 6)
        0.0
    """

    # Initialisation
    def __init__(self, n_quantiles: Optional[int] = None) -> None:
        self.n_quantiles = n_quantiles

    # Ajustement : délégation au QuantileTransformer sklearn sous-jacent
    def fit(self, X: np.ndarray, y: None = None) -> "GaussianQuantileScaler":
        """Fit the underlying ``QuantileTransformer``.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``.
        """
        X = check_array(X)
        # Un quantile par échantillon d'ajustement, sauf borne explicite
        n_quantiles = self.n_quantiles or min(X.shape[0], 1000)
        self._transformer = QuantileTransformer(
            n_quantiles=max(n_quantiles, 2), output_distribution="normal"
        )
        self._transformer.fit(X)
        self.n_features_in_ = X.shape[1]
        return self

    # Transformation : délégation
    def transform(self, X: np.ndarray) -> np.ndarray:
        """Apply the fitted quantile transform.

        Args:
            X: Metric matrix of shape ``(n, d)``.

        Returns:
            Matrix with standard-normal marginals, same shape.
        """
        check_is_fitted(self, "_transformer")
        return self._transformer.transform(check_array(X))


# Registre des schémas de normalisation, point d'entrée piloté par configuration
NORMALIZER_REGISTRY = {
    "minmax": MinMaxScaler,
    "standard": StandardScaler,
    "robust": MedianMadScaler,
    "rank": RankScaler,
    "quantile_gaussian": GaussianQuantileScaler,
}


# Fabrique de transformateur de normalisation
def make_normalizer(name: str, **kwargs) -> TransformerMixin:
    """Instantiate the normalisation scheme named ``name``.

    The configuration-driven entry point of the module: a caller reading a
    YAML file passes the scheme name (and its own hyperparameters) straight
    through, with no ``if/elif`` chain to maintain at the call site.

    Args:
        name: One of ``"minmax"``, ``"standard"``, ``"robust"`` (median/MAD,
            see :class:`MedianMadScaler`), ``"rank"`` (see :class:`RankScaler`)
            or ``"quantile_gaussian"`` (see :class:`GaussianQuantileScaler`).
        **kwargs: Forwarded to the transformer's constructor.

    Returns:
        A fresh, unfitted transformer instance.

    Raises:
        ValueError: If ``name`` is not a known scheme.

    Examples:
        >>> type(make_normalizer("rank")).__name__
        'RankScaler'
    """
    if name not in NORMALIZER_REGISTRY:
        raise ValueError(
            f"Unknown normalisation scheme {name!r}. "
            f"Available: {sorted(NORMALIZER_REGISTRY)}."
        )
    return NORMALIZER_REGISTRY[name](**kwargs)


# ──────────────────────────────────────────────────────────────────────
# Diagnostics de corrélation
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul de la matrice de corrélation de Spearman
def spearman_correlation_matrix(X: np.ndarray) -> np.ndarray:
    """Compute the Spearman (rank) correlation matrix of the metrics.

    Preferred over Pearson for the pre-aggregation correlation diagnostic:
    monotone rather than linear, robust to the heavy tails typical of
    concentration indices.

    Args:
        X: Metric matrix of shape ``(n, d)``.

    Returns:
        Correlation matrix of shape ``(d, d)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 2.0], [2.0, 4.0], [3.0, 6.0]])
        >>> spearman_correlation_matrix(X)
        array([[1., 1.],
               [1., 1.]])
    """
    X = check_array(X)
    if X.shape[1] == 1:
        return np.ones((1, 1))
    correlation, _ = stats.spearmanr(X)
    # Deux colonnes : spearmanr renvoie un scalaire plutôt qu'une matrice
    if X.shape[1] == 2:
        return np.array([[1.0, correlation], [correlation, 1.0]])
    return np.asarray(correlation)


# Fonction de calcul de l'indice de Kaiser-Meyer-Olkin
def kmo_statistic(X: np.ndarray) -> Tuple[float, np.ndarray]:
    """Compute the Kaiser-Meyer-Olkin measure of sampling adequacy.

    Compares the sum of squared correlations to the sum of squared partial
    correlations (derived from the inverse correlation matrix): a KMO close
    to 1 means the correlations are largely explained by common factors — a
    plausible factorial structure for a PCA-based weighting; a KMO well below
    0.5 signals near-independent metrics, better served by a dispersion-based
    weighting or a partial order.

    Args:
        X: Metric matrix of shape ``(n, d)``, ``d >= 2``.

    Returns:
        Tuple ``(kmo_overall, kmo_per_variable)``.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> base = rng.normal(size=200)
        >>> X = np.column_stack([base + rng.normal(scale=0.1, size=200) for _ in range(3)])
        >>> overall, _ = kmo_statistic(X)
        >>> overall > 0.5
        True
    """
    X = check_array(X)
    correlation = np.corrcoef(X, rowvar=False)
    # Matrice des corrélations partielles, tirée de l'inverse de R
    inverse = np.linalg.pinv(correlation)
    diag = np.sqrt(np.diag(inverse))
    partial = -inverse / np.outer(diag, diag)
    np.fill_diagonal(partial, 0.0)
    np.fill_diagonal(correlation, 0.0)

    sum_corr_sq = np.sum(correlation**2)
    sum_partial_sq = np.sum(partial**2)
    kmo_overall = sum_corr_sq / (sum_corr_sq + sum_partial_sq)

    # Indices par variable (mêmes sommes, restreintes à une ligne)
    corr_row = np.sum(correlation**2, axis=0)
    partial_row = np.sum(partial**2, axis=0)
    kmo_per_variable = corr_row / (corr_row + partial_row)
    return float(kmo_overall), kmo_per_variable


# Fonction de test de sphéricité de Bartlett
def bartlett_sphericity(X: np.ndarray) -> Tuple[float, float]:
    """Test whether the correlation matrix significantly departs from identity.

    A non-rejection means no factorial structure is detectable at all: a
    PCA-based weighting would then rest on noise.

    Args:
        X: Metric matrix of shape ``(n, d)``, ``d >= 2``.

    Returns:
        Tuple ``(chi2_statistic, p_value)``.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> base = rng.normal(size=300)
        >>> X = np.column_stack([base + rng.normal(scale=0.1, size=300) for _ in range(3)])
        >>> chi2, p_value = bartlett_sphericity(X)
        >>> p_value < 0.05
        True
    """
    X = check_array(X)
    n, d = X.shape
    correlation = np.corrcoef(X, rowvar=False)
    sign, logdet = np.linalg.slogdet(correlation)
    if sign <= 0:
        # Déterminant non défini (matrice singulière) : sphéricité rejetée d'office
        return float("inf"), 0.0
    statistic = -(n - 1 - (2 * d + 5) / 6) * logdet
    degrees_of_freedom = d * (d - 1) / 2
    p_value = float(stats.chi2.sf(statistic, degrees_of_freedom))
    return float(statistic), p_value


# Rapport de diagnostic de corrélation
@dataclass
class CorrelationDiagnostics:
    """Pre-aggregation correlation diagnostic of the metric matrix.

    Attributes:
        matrix: Spearman correlation matrix, shape ``(d, d)``.
        kmo: Overall Kaiser-Meyer-Olkin measure.
        kmo_per_variable: KMO per metric, shape ``(d,)``.
        bartlett_chi2: Bartlett's sphericity test statistic.
        bartlett_p_value: Associated p-value.
    """
    matrix: np.ndarray
    kmo: float
    kmo_per_variable: np.ndarray
    bartlett_chi2: float
    bartlett_p_value: float


# Fonction de calcul du diagnostic de corrélation complet
def compute_correlation_diagnostics(X: np.ndarray) -> CorrelationDiagnostics:
    """Assemble the correlation matrix, KMO and Bartlett's test in one call.

    Args:
        X: Metric matrix of shape ``(n, d)``, already oriented in positive
            polarity (the diagnostic itself is polarity-agnostic, but running
            it downstream of :class:`PolarityOrienter` keeps every diagnostic
            of the pipeline on the same oriented matrix).

    Returns:
        The :class:`CorrelationDiagnostics` of the matrix.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 2.0], [2.0, 4.0], [3.0, 6.0], [4.0, 8.0]])
        >>> diagnostics = compute_correlation_diagnostics(X)
        >>> float(diagnostics.matrix[0, 1])
        1.0
    """
    matrix = spearman_correlation_matrix(X)
    kmo, kmo_per_variable = kmo_statistic(X)
    chi2, p_value = bartlett_sphericity(X)
    return CorrelationDiagnostics(
        matrix=matrix,
        kmo=kmo,
        kmo_per_variable=kmo_per_variable,
        bartlett_chi2=chi2,
        bartlett_p_value=p_value,
    )
