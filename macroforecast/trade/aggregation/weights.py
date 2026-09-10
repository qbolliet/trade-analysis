"""Endogenous weighting schemes.

Implements §4 of the methodological note: weights entirely determined by the
metric matrix ``X`` — none is fixed by expert judgement. Three kinds of object
are produced here, and they are *not* interchangeable (M-05):

* **Simplicial weightings** — :func:`entropy_weights`, :func:`critic_weights`,
  :func:`pca_weights` with ``rotate=True`` (OECD/JRC) and the meta-selection
  :func:`auto_weights`: a vector ``w`` of the simplex ``Δ^{d-1}``, the only
  admissible input of a weighted sum, a geometric mean, TOPSIS or VIKOR.
* **Directions** — :func:`pca_weights` with ``rotate=False``: a unit-norm
  vector whose components may be negative, defining a linear score on
  *standardised* data. It has no business in a geometric mean or in TOPSIS;
  :class:`~macroforecast.trade.aggregation.estimators.PcaProjectionScorer`
  is the estimator that applies it correctly.
* **Individual weightings** — :func:`benefit_of_doubt_weights`: one weight
  vector *per product*, so no shared vector at all, and a score rather than a
  weighting as its primary output.

:func:`dirichlet_weights` stands apart: it draws weights uniformly on the
simplex for the SMAA protocol rather than estimating any.

Every scheme returns plain ``numpy`` arrays; the sklearn-facing layer lives in
:mod:`~macroforecast.trade.aggregation.estimators`.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import warnings
from dataclasses import dataclass
from typing import List, Optional, Tuple
# Modules de manipulation de données
import numpy as np
from scipy import stats
from scipy.optimize import linprog
from sklearn.utils.validation import check_array
# Modules du package
from .pareto import pareto_front
from .preprocessing import (
    bartlett_sphericity,
    kmo_statistic,
    spearman_correlation_matrix,
)


# ──────────────────────────────────────────────────────────────────────
# Utilitaires de corrélation
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul d'une matrice de corrélation régularisée
def _safe_correlation(X: np.ndarray, method: str = "pearson") -> np.ndarray:
    """Compute a correlation matrix free of ``NaN`` on constant columns.

    ``numpy.corrcoef`` (and ``scipy.stats.spearmanr``) return ``NaN`` for a
    column of zero variance, which propagates to every weight downstream
    (I-06). The convention adopted here is the natural one: a constant column
    is *uncorrelated* with everything (off-diagonal 0) and perfectly
    correlated with itself (diagonal 1).

    Args:
        X: Metric matrix of shape ``(n, d)``.
        method: ``"pearson"`` or ``"spearman"``.

    Returns:
        Correlation matrix of shape ``(d, d)``, without ``NaN``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.0, 0.5], [0.5, 0.5], [1.0, 0.5]])
        >>> _safe_correlation(X)
        array([[1., 0.],
               [0., 1.]])
    """
    d = X.shape[1]
    if d == 1 or X.shape[0] < 2:
        return np.eye(d)

    # Les colonnes constantes déclenchent une division par zéro dans les deux
    # bibliothèques : avertissements neutralisés, NaN remplacés ensuite
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with np.errstate(divide="ignore", invalid="ignore"):
            if method == "spearman":
                correlation = spearman_correlation_matrix(X)
            else:
                correlation = np.corrcoef(X, rowvar=False)

    correlation = np.nan_to_num(np.asarray(correlation, dtype=float), nan=0.0)
    np.fill_diagonal(correlation, 1.0)
    return correlation


# ──────────────────────────────────────────────────────────────────────
# Entropie de Shannon
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul des poids par entropie de Shannon
def entropy_weights(X: np.ndarray) -> np.ndarray:
    """Weight each metric by ``1 - normalised Shannon entropy`` of its column.

    A metric whose values are near-identical across products carries little
    information (high entropy) and receives a low weight; a widely spread
    metric receives a high weight.

    The scheme is **not invariant by translation**: it reads ``X`` as a set of
    per-column probability distributions, so only a normalisation mapping
    onto a common non-negative scale is admissible — **min-max** (the default
    of the pipeline) or **ranks** (M-12). A standardised or median/MAD
    normalisation would produce negative values and is rejected.

    Degenerate columns (M-12): a column that is constant *and non-zero*
    spreads its mass uniformly, reaching maximal entropy, hence a weight of
    0; a column that is identically zero (a constant column *after* a min-max
    normalisation) carries no information either and is likewise given a
    weight of 0 — a case ``0 / 0`` that must be handled explicitly rather
    than left to ``numpy`` to warn about.

    Args:
        X: Metric matrix of shape ``(n, d)``, non-negative (min-max or ranks).

    Returns:
        Weight vector of shape ``(d,)``, summing to 1.

    Raises:
        ValueError: If ``X`` holds a negative value.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.0, 0.5], [0.5, 0.5], [1.0, 0.5]])
        >>> weights = entropy_weights(X)
        >>> round(float(weights[1]), 6)
        0.0
        >>> Z = np.array([[0.0, 0.0], [0.5, 0.0], [1.0, 0.0]])
        >>> round(float(entropy_weights(Z)[1]), 6)
        0.0
    """
    X = check_array(X)
    if np.any(X < 0):
        raise ValueError(
            "entropy_weights requires a non-negative matrix "
            "(apply a min-max or rank normalisation first)."
        )
    n, d = X.shape
    if n <= 1:
        return np.full(d, 1.0 / d)

    # Conversion de chaque colonne en distribution de probabilité ; une colonne
    # identiquement nulle n'en définit aucune et sera neutralisée plus bas
    column_sums = X.sum(axis=0)
    null_columns = column_sums <= 0
    safe_sums = np.where(null_columns, 1.0, column_sums)
    p = X / safe_sums

    # Entropie normalisée, convention 0 ln 0 = 0 : le logarithme n'est évalué
    # que sur les composantes strictement positives (aucun avertissement numpy)
    p_log_p = np.zeros_like(p)
    positive = p > 0
    p_log_p[positive] = p[positive] * np.log(p[positive])
    entropy = -p_log_p.sum(axis=0) / np.log(n)
    divergence = 1.0 - entropy
    # Colonne nulle : divergence conventionnellement nulle (poids nul, M-12)
    divergence[null_columns] = 0.0

    if divergence.sum() == 0:
        return np.full(d, 1.0 / d)
    return divergence / divergence.sum()


# ──────────────────────────────────────────────────────────────────────
# CRITIC
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul des poids CRITIC
def critic_weights(
    X: np.ndarray,
    method: str = "pearson",
    *,
    scale: str = "std",
    abs_corr: bool = False,
) -> np.ndarray:
    """Weight each metric by dispersion times originality (CRITIC).

    ``C_j = s_j · Σ_k (1 - r_jk)``, maximal when metric ``j`` is
    anti-correlated with every other and minimal when it is a near-replica of
    another — correcting the blind spot of :func:`entropy_weights`, which
    looks at one column at a time and would double-count two near-identical
    metrics.

    Degenerate columns (I-06): a constant column has an undefined correlation
    with every other; the convention of :func:`_safe_correlation` (0
    off-diagonal, 1 on the diagonal) applies, and since ``s_j = 0`` such a
    column receives a weight of exactly 0 rather than turning every weight
    into ``NaN``. When *every* column is constant the contrast vanishes
    altogether and the uniform vector is returned as a last resort.

    Args:
        X: Metric matrix of shape ``(n, d)``, typically min-max normalised.
        method: ``"pearson"`` (the original CRITIC) or ``"spearman"`` (the
            note's robust variant, monotone rather than linear).
        scale: Dispersion statistic — ``"std"`` (standard deviation,
            ``ddof=1``) or ``"mad"`` (median absolute deviation, the robust
            counterpart recommended with ``method="spearman"``, M-13).
        abs_corr: When ``True``, originality is measured by
            ``Σ_k (1 - |r_jk|)`` instead of ``Σ_k (1 - r_jk)``: a strong
            *anti*-correlation is then treated as redundancy too (two metrics
            carrying the same information with opposite signs) rather than as
            maximal originality. An explicit modelling assumption, not a
            correction (M-13).

    Returns:
        Weight vector of shape ``(d,)``, summing to 1.

    Raises:
        ValueError: If ``method`` or ``scale`` is unknown.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.0, 0.0], [0.5, 1.0], [1.0, 0.2]])
        >>> weights = critic_weights(X)
        >>> round(float(weights.sum()), 6)
        1.0
        >>> Z = np.array([[0.0, 0.5], [0.5, 0.5], [1.0, 0.5]])
        >>> round(float(critic_weights(Z)[1]), 6)
        0.0
    """
    X = check_array(X)
    d = X.shape[1]
    if method not in ("pearson", "spearman"):
        raise ValueError(
            f"Unknown correlation method {method!r}. Available: 'pearson', 'spearman'."
        )
    if scale not in ("std", "mad"):
        raise ValueError(f"Unknown scale {scale!r}. Available: 'std', 'mad'.")

    if X.shape[0] > 1:
        if scale == "mad":
            dispersion = stats.median_abs_deviation(X, axis=0, scale=1.0)
        else:
            dispersion = X.std(axis=0, ddof=1)
    else:
        dispersion = np.zeros(d)

    correlation = _safe_correlation(X, method=method)
    if abs_corr:
        correlation = np.abs(correlation)

    # Une dispersion nulle annule le poids de la métrique (colonne constante)
    contrast = dispersion * np.sum(1.0 - correlation, axis=1)
    if contrast.sum() == 0:
        return np.full(d, 1.0 / d)
    return contrast / contrast.sum()


# ──────────────────────────────────────────────────────────────────────
# ACP
# ──────────────────────────────────────────────────────────────────────

# Rapport de pondération par ACP
@dataclass
class PcaWeightingReport:
    """Diagnostics of a PCA-based weighting.

    Attributes:
        explained_variance_ratio: Variance share of every retained axis.
        has_negative_loadings: Whether some loading is negative — such a
            weighting *violates monotonicity* (property 1.5 of the note): a
            higher vulnerability on that metric could lower the score.
        variance_share_axis1: Variance share of the first axis alone; below
            ~0.5 there is no dominant factor and the axis is unstable under
            resampling.
    """
    explained_variance_ratio: np.ndarray
    has_negative_loadings: bool
    variance_share_axis1: float


# Fonction de rotation varimax
def _varimax(loadings: np.ndarray, gamma: float = 1.0, max_iter: int = 20, tol: float = 1e-6) -> np.ndarray:
    """Apply an orthogonal varimax rotation to a loadings matrix.

    Standard Kaiser varimax by iterated SVD; a no-op when a single axis is
    retained (a rotation needs at least two axes to mix).

    Args:
        loadings: Loadings matrix of shape ``(d, m)``.
        gamma: Varimax parameter (1.0 for the classical criterion).
        max_iter: Maximum number of SVD iterations.
        tol: Relative-improvement tolerance for early stopping.

    Returns:
        Rotated loadings, same shape as ``loadings``.
    """
    n_features, n_axes = loadings.shape
    if n_axes < 2:
        return loadings

    rotation = np.eye(n_axes)
    variance = 0.0
    for _ in range(max_iter):
        rotated = loadings @ rotation
        cubed = rotated**3
        correction = rotated @ np.diag(np.diag(rotated.T @ rotated)) / n_features
        u, s, vt = np.linalg.svd(loadings.T @ (cubed - gamma * correction))
        rotation = u @ vt
        new_variance = np.sum(s)
        if variance != 0 and new_variance < variance * (1 + tol):
            break
        variance = new_variance
    return loadings @ rotation


# Fonction de diagonalisation de la matrice de corrélation
def _correlation_eigen(X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Diagonalise the correlation matrix of ``X``, axes sorted by variance.

    The correlation matrix rather than the covariance one: the weighting must
    not depend on whether the caller normalised in min-max, in ranks or by
    standardisation (I-05).

    Args:
        X: Metric matrix of shape ``(n, d)``.

    Returns:
        Tuple ``(eigenvalues, eigenvectors)`` of shapes ``(d,)`` and
        ``(d, d)``, sorted by decreasing eigenvalue; eigenvalues are clipped
        at 0 (a correlation matrix is positive semi-definite, up to rounding).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.0, 0.0], [1.0, 1.0], [2.0, 2.0], [3.0, 3.0]])
        >>> eigenvalues, _ = _correlation_eigen(X)
        >>> [round(float(value), 6) for value in eigenvalues]
        [2.0, 0.0]
    """
    correlation = _safe_correlation(X, method="pearson")
    eigenvalues, eigenvectors = np.linalg.eigh(correlation)
    order = np.argsort(eigenvalues)[::-1]
    return np.clip(eigenvalues[order], 0.0, None), eigenvectors[:, order]


# Fonction de pondération par ACP
def pca_weights(
    X: np.ndarray,
    *,
    rotate: bool = False,
    min_eigenvalue: float = 1.0,
) -> Tuple[np.ndarray, PcaWeightingReport]:
    """Derive weights (or a direction) from a principal-component analysis.

    The analysis always runs on the **correlation** matrix of ``X``
    (``numpy.corrcoef``), so the result does not depend on the upstream
    normalisation — in particular it is no longer silently wrong when the
    pipeline normalises in min-max rather than standardising (I-05).

    Two very different objects are returned depending on ``rotate``:

    * ``rotate=False`` — a **direction** (§4.3's simple variant): the
      unit-norm first-axis loadings, sign fixed so that they sum positive.
      Components may be negative: this is *not* a point of the simplex, it
      defines a linear score on *standardised* data and must not be handed to
      a geometric mean or to TOPSIS (M-05). Use
      :class:`~macroforecast.trade.aggregation.estimators.PcaProjectionScorer`,
      which standardises internally.
    * ``rotate=True`` — the **OECD/JRC weighting** (Nardo et al. 2008,
      pp. 89-90, after Nicoletti et al. 2000): axes of eigenvalue above
      ``min_eigenvalue`` are varimax-rotated, each metric is assigned to the
      factor ``m*(j)`` carrying its largest squared loading, and

      ``w_j = (V_{m*} / Σ_m V_m) · ℓ'_{j m*}² / Σ_{k : m*(k) = m*} ℓ'_{k m*}²``

      with ``V_m = Σ_j ℓ'_{jm}²`` the variance of factor ``m`` **after**
      rotation. Those weights sum to 1 without renormalisation — the earlier
      implementation used the eigenvalues *before* rotation and skipped the
      within-factor normalisation, which is a different formula (M-04).
      ``Σ_m V_m`` runs over the factors that actually receive a metric, so
      the unit sum holds even when a retained axis attracts none.

    Args:
        X: Metric matrix of shape ``(n, d)``, any normalisation (the
            correlation matrix is scale- and location-free).
        rotate: ``False`` (default) for the first-axis direction, ``True``
            for the OECD/JRC simplicial weighting.
        min_eigenvalue: Retention threshold of the Kaiser criterion
            (``rotate=True`` only). Default ``1.0``: an axis is kept when it
            explains more than a single standardised metric. A structure with
            an isolated metric sits exactly *at* the threshold (its axis has
            eigenvalue 1 in population), so a slightly lower value is needed
            to retain it. Falls back to the first axis alone when no axis
            passes.

    Returns:
        Tuple ``(weights, report)`` — ``weights`` is a direction when
        ``rotate=False`` and a point of the simplex when ``rotate=True``.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> base = rng.normal(size=100)
        >>> X = np.column_stack([base, base, rng.normal(size=100)])
        >>> direction, report = pca_weights(X)
        >>> direction.shape
        (3,)
        >>> bool(report.variance_share_axis1 > 0.5)
        True
        >>> weights, _ = pca_weights(X, rotate=True, min_eigenvalue=0.9)
        >>> round(float(weights.sum()), 6)
        1.0
    """
    X = check_array(X)
    d = X.shape[1]
    eigenvalues, eigenvectors = _correlation_eigen(X)
    # La trace d'une matrice de corrélation vaut d : la part de variance d'un
    # axe est directement sa valeur propre rapportée à d
    total = eigenvalues.sum()
    explained = eigenvalues / total if total > 0 else np.full(d, 1.0 / d)

    if not rotate:
        # Variante « direction » : premier axe seul, norme 1, signe fixé
        direction = eigenvectors[:, 0]
        if direction.sum() < 0:
            direction = -direction
        report = PcaWeightingReport(
            explained_variance_ratio=explained,
            has_negative_loadings=bool(np.any(direction < 0)),
            variance_share_axis1=float(explained[0]),
        )
        return direction, report

    # Variante OCDE-JRC : axes de Kaiser, rotation varimax, affectation
    keep = eigenvalues > min_eigenvalue
    if not np.any(keep):
        # Repli : aucun axe ne dépasse le critère, on garde le premier
        keep = np.zeros(d, dtype=bool)
        keep[0] = True

    # Saturations = vecteurs propres pondérés par la racine de la valeur propre
    raw_loadings = eigenvectors[:, keep] * np.sqrt(eigenvalues[keep])
    rotated_loadings = _varimax(raw_loadings)

    squared = rotated_loadings**2
    factor_variance = squared.sum(axis=0)
    assignment = np.argmax(squared, axis=1)

    if factor_variance.sum() <= 0:
        weight = np.full(d, 1.0 / d)
    else:
        # Somme des variances restreinte aux facteurs effectivement dotés d'une
        # métrique : condition de la somme unitaire sans renormalisation
        used = np.unique(assignment)
        variance_total = factor_variance[used].sum()
        weight = np.zeros(d)
        for factor in used:
            members = assignment == factor
            within = squared[members, factor]
            if within.sum() <= 0:
                # Facteur dégénéré : répartition uniforme entre ses métriques
                within = np.ones(members.sum())
            share = factor_variance[factor] / variance_total
            weight[members] = share * within / within.sum()

    report = PcaWeightingReport(
        explained_variance_ratio=explained[keep],
        has_negative_loadings=bool(np.any(rotated_loadings < 0)),
        variance_share_axis1=float(explained[0]),
    )
    return weight, report


# ──────────────────────────────────────────────────────────────────────
# Méta-pondération « auto »
# ──────────────────────────────────────────────────────────────────────

# Rapport de sélection de la méta-pondération
@dataclass
class AutoWeightingReport:
    """Trace of the weighting scheme selected by :func:`auto_weights`.

    Attributes:
        selected: Name of the retained scheme — ``"pca_oecd"``, ``"critic"``,
            ``"entropy"`` or ``"equal"``.
        candidates_tried: Schemes examined, in order; all but the last were
            rejected by the degeneracy guard.
        kmo: Kaiser-Meyer-Olkin measure of the standardised matrix
            (``NaN`` when undefined, e.g. ``d == 1`` or ``n <= d``).
        bartlett_p: p-value of Bartlett's sphericity test (``NaN`` when
            undefined).
        axis1_share: Variance share of the first principal axis, ``λ₁ / d``.
        mean_abs_rho: Mean absolute off-diagonal Spearman correlation, the
            redundancy indicator.
        max_weight: Largest component of the returned weight vector — the
            statistic the degeneracy guard is expressed on.
    """
    selected: str
    candidates_tried: List[str]
    kmo: float
    bartlett_p: float
    axis1_share: float
    mean_abs_rho: float
    max_weight: float


# Ordre de repli de la méta-pondération (S-1.5, étape 6)
_AUTO_FALLBACK_ORDER: Tuple[str, ...] = ("pca_oecd", "critic", "entropy", "equal")


# Fonction de méta-sélection de la pondération
def auto_weights(
    X: np.ndarray,
    *,
    kmo_min: float = 0.6,
    bartlett_alpha: float = 0.05,
    axis1_share_min: float = 0.5,
    redundancy_rho: float = 0.3,
    max_weight: float = 0.6,
) -> Tuple[np.ndarray, AutoWeightingReport]:
    """Select the shared weighting scheme best suited to the group (S-1.5).

    A **heuristic**, not a result: it formalises the reading order an analyst
    would follow — a genuine factorial structure justifies the OECD/JRC
    weighting, otherwise redundancy between metrics calls for CRITIC, and
    failing both, dispersion alone (entropy) is all that is left. The rule is:

    1. ``d == 1`` → ``equal``.
    2. Diagnostics on the standardised matrix: KMO, Bartlett's p-value,
       ``λ₁ / d`` and the mean absolute off-diagonal Spearman correlation.
    3. ``kmo >= kmo_min`` **and** ``bartlett_p <= bartlett_alpha`` **and**
       ``λ₁ / d >= axis1_share_min`` → candidate ``pca_oecd``.
    4. Otherwise ``ρ̄ >= redundancy_rho`` → candidate ``critic``.
    5. Otherwise → candidate ``entropy``.
    6. Degeneracy guard: a candidate whose largest weight reaches
       ``max_weight`` concentrates the score on a single metric and is
       rejected in favour of the next scheme in the order
       ``pca_oecd → critic → entropy → equal``.

    ``bod`` is deliberately out of the selection: it produces one weight
    vector *per product*, not a shared one (D-04).

    Args:
        X: Metric matrix of shape ``(n, d)``, oriented and normalised as the
            downstream aggregation requires (``entropy`` demands ``X >= 0``:
            an ``entropy`` candidate on a matrix holding negative values is
            skipped in favour of ``equal``).
        kmo_min: Minimal KMO for a factorial structure to be deemed plausible.
        bartlett_alpha: Significance level of Bartlett's sphericity test.
        axis1_share_min: Minimal variance share of the first axis.
        redundancy_rho: Mean absolute correlation above which the metrics are
            deemed redundant enough for CRITIC to be worth its cost.
        max_weight: Degeneracy threshold on the largest weight.

    Returns:
        Tuple ``(weights, report)``, ``weights`` a point of the simplex.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> base = rng.random(300)
        >>> X = np.column_stack([base, base + rng.normal(scale=0.05, size=300)])
        >>> X = (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0))
        >>> weights, report = auto_weights(X)
        >>> round(float(weights.sum()), 6)
        1.0
        >>> report.selected in {'pca_oecd', 'critic', 'entropy', 'equal'}
        True
    """
    X = check_array(X)
    n, d = X.shape
    uniform = np.full(d, 1.0 / d)

    if d == 1:
        return uniform, AutoWeightingReport(
            selected="equal",
            candidates_tried=["equal"],
            kmo=float("nan"),
            bartlett_p=float("nan"),
            axis1_share=1.0,
            mean_abs_rho=float("nan"),
            max_weight=1.0,
        )

    # Diagnostics sur la matrice standardisée (invariants d'échelle, mais la
    # convention de S-1.5 est explicite)
    center = X.mean(axis=0)
    spread = X.std(axis=0)
    Z = (X - center) / np.where(spread > 0, spread, 1.0)

    eigenvalues, _ = _correlation_eigen(Z)
    axis1_share = float(eigenvalues[0] / d)

    rho = _safe_correlation(Z, method="spearman")
    off_diagonal = ~np.eye(d, dtype=bool)
    mean_abs_rho = float(np.mean(np.abs(rho[off_diagonal])))

    # KMO et Bartlett sont indéfinis sur un groupe trop petit ou singulier
    if n > d:
        try:
            kmo = float(kmo_statistic(Z)[0])
            bartlett_p = float(bartlett_sphericity(Z)[1])
        except (np.linalg.LinAlgError, ValueError):
            kmo, bartlett_p = float("nan"), float("nan")
    else:
        kmo, bartlett_p = float("nan"), float("nan")

    factorial = (
        np.isfinite(kmo)
        and np.isfinite(bartlett_p)
        and kmo >= kmo_min
        and bartlett_p <= bartlett_alpha
        and axis1_share >= axis1_share_min
    )
    if factorial:
        first = "pca_oecd"
    elif mean_abs_rho >= redundancy_rho:
        first = "critic"
    else:
        first = "entropy"

    # Garde de dégénérescence : parcours de l'ordre de repli à partir du candidat
    candidates = list(_AUTO_FALLBACK_ORDER[_AUTO_FALLBACK_ORDER.index(first):])
    tried: List[str] = []
    weights = uniform
    selected = "equal"
    for candidate in candidates:
        tried.append(candidate)
        selected = candidate
        if candidate == "pca_oecd":
            weights = pca_weights(X, rotate=True)[0]
        elif candidate == "critic":
            weights = critic_weights(X, method="spearman")
        elif candidate == "entropy":
            if np.any(X < 0):
                # Entropie inadmissible sur données signées : candidat écarté
                continue
            weights = entropy_weights(X)
        else:
            weights = uniform
        if np.max(weights) < max_weight:
            break

    return weights, AutoWeightingReport(
        selected=selected,
        candidates_tried=tried,
        kmo=kmo,
        bartlett_p=bartlett_p,
        axis1_share=axis1_share,
        mean_abs_rho=mean_abs_rho,
        max_weight=float(np.max(weights)),
    )


# ──────────────────────────────────────────────────────────────────────
# Benefit of the doubt
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction des contraintes de restriction des poids
def _restriction_rows(
    x_o: np.ndarray,
    *,
    restriction: Optional[str],
    rho: Optional[float],
    kappa: float,
    delta: float,
) -> Tuple[np.ndarray, np.ndarray]:
    """Build the weight-restriction rows of one benefit-of-the-doubt programme.

    Args:
        x_o: Metric vector of the scored product, shape ``(d,)``.
        restriction: ``"assurance_region"``, ``"shares"`` or ``None``.
        rho: Assurance-region ratio bound, ``>= 1`` (``None`` = unrestricted).
        kappa: Share-restriction parameter in ``(0, 1]``.
        delta: Floor added to ``x_o`` for the share restrictions.

    Returns:
        Tuple ``(A_ub, b_ub)``; both are empty when no restriction applies.

    Raises:
        ValueError: If ``restriction`` is unknown, or a parameter is out of
            range.

    Examples:
        >>> import numpy as np
        >>> rows, bounds = _restriction_rows(
        ...     np.array([1.0, 0.0]),
        ...     restriction="assurance_region", rho=2.0, kappa=0.5, delta=1e-3,
        ... )
        >>> rows.shape
        (2, 2)
    """
    d = x_o.shape[0]
    identity = np.eye(d)
    rows: List[np.ndarray] = []

    if restriction is None:
        pass
    elif restriction == "assurance_region":
        if rho is not None:
            if rho < 1.0:
                raise ValueError(f"rho must be >= 1 (got {rho}).")
            # w_j <= rho w_k et w_k <= rho w_j pour tout couple : contraintes
            # lineaires, symetriques et toujours realisables (w uniforme)
            for j in range(d):
                for k in range(j + 1, d):
                    rows.append(identity[j] - rho * identity[k])
                    rows.append(identity[k] - rho * identity[j])
    elif restriction == "shares":
        if not 0.0 < kappa <= 1.0:
            raise ValueError(f"kappa must lie in (0, 1] (got {kappa}).")
        if delta <= 0.0:
            raise ValueError(
                f"delta must be > 0 for the 'shares' restriction (got {delta}): "
                "a null coordinate would otherwise force a null score (M-03)."
            )
        shifted = x_o + delta
        lower = kappa / d
        upper = 1.0 / (kappa * d)
        for j in range(d):
            rows.append(lower * shifted - identity[j] * shifted[j])
            rows.append(identity[j] * shifted[j] - upper * shifted)
    else:
        raise ValueError(
            f"Unknown restriction {restriction!r}. "
            "Available: 'assurance_region', 'shares', None."
        )

    if not rows:
        return np.empty((0, d)), np.empty(0)
    return np.asarray(rows, dtype=float), np.zeros(len(rows))


# Fonction de resolution d'un programme benefit of the doubt
def _solve_bod(
    x_o: np.ndarray,
    constraint_rows: np.ndarray,
    restriction_rows: np.ndarray,
    restriction_bounds: np.ndarray,
) -> Tuple[float, np.ndarray]:
    """Solve the linear programme of a single product.

    Args:
        x_o: Metric vector of the scored product, shape ``(d,)``.
        constraint_rows: Non-domination constraint matrix, shape ``(m, d)``.
        restriction_rows: Weight-restriction matrix, shape ``(r, d)``.
        restriction_bounds: Right-hand side of the restrictions, shape ``(r,)``.

    Returns:
        Tuple ``(score, weights)``; ``(nan, nan-vector)`` when the programme
        is infeasible or unbounded.

    Examples:
        >>> import numpy as np
        >>> score, weights = _solve_bod(
        ...     np.array([1.0, 1.0]), np.array([[1.0, 1.0]]),
        ...     np.empty((0, 2)), np.empty(0),
        ... )
        >>> round(score, 6)
        1.0
    """
    d = x_o.shape[0]
    a_ub = np.vstack([constraint_rows, restriction_rows])
    b_ub = np.concatenate([np.ones(len(constraint_rows)), restriction_bounds])
    result = linprog(
        -x_o,
        A_ub=a_ub if len(a_ub) else None,
        b_ub=b_ub if len(b_ub) else None,
        bounds=[(0, None)] * d,
        method="highs",
    )
    if not result.success:
        return float("nan"), np.full(d, np.nan)
    return float(-result.fun), np.asarray(result.x, dtype=float)


# Fonction de calcul du score et des poids individuels benefit of the doubt
def benefit_of_doubt_weights(
    X: np.ndarray,
    *,
    restriction: Optional[str] = "assurance_region",
    rho: Optional[float] = 4.0,
    kappa: float = 0.5,
    delta: float = 1e-3,
    restrict_to_front: bool = True,
    n_jobs: Optional[int] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Solve one linear programme per product for its most favourable weights.

    Each product is scored under the weighting that makes it *as vulnerable
    as possible*, subject to that same weighting never scoring another
    product above 1 (algorithm 3 of the note). Without any restriction the
    programme puts all the weight on the single metric a product happens to
    shine on, hence the two variants below.

    Restrictions (M-03 / D-05):

    * ``"assurance_region"`` (default) -- bounded weight *ratios*,
      ``w_j / w_k`` in ``[1/rho, rho]`` for every pair, written as the linear
      constraints ``w_j <= rho w_k``. Symmetric between criteria, always
      feasible (the uniform vector satisfies them) and, crucially,
      **insensitive to null coordinates**.
    * ``"shares"`` -- the contribution-share restrictions
      ``kappa/d <= w_j x_oj / sum_k w_k x_ok <= 1/(kappa d)``, applied to
      ``x_o + delta``. On raw min-max data those restrictions force a null
      score on every product holding a zero -- that is, at least the argmin of
      every column (M-03); the floor ``delta > 0`` is what makes the variant
      usable at all and is a genuine **sensitivity parameter**, not a
      numerical detail: the smaller it is, the closer the behaviour to the
      degenerate one.
    * ``None`` -- unrestricted programme (weak DEA efficiency).

    Exact properties: the score lies in ``[0, 1]``; ``s = 1`` characterises
    membership of the *enlarged upper convex envelope* of the cloud (weak
    efficiency under the restrictions in force), **not** non-domination --
    with ``rho=None`` a null weight is admissible, so ``x_o = (1, 0)`` and
    ``x_k = (1, 1)`` both reach 1. Conversely, with ``rho`` finite every
    weight is strictly positive and ``s = 1`` does imply non-domination, but a
    point of the Pareto front need *not* reach 1: only those lying on that
    enlarged envelope do. Monotonicity is weak (``>=``), not strict.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity and
            **non-negative** -- a min-max or rank normalisation. Centred data
            make both the constraint ``Xw <= 1`` and the ``[0, 1]`` range
            meaningless.
        restriction: ``"assurance_region"``, ``"shares"`` or ``None``.
        rho: Assurance-region ratio bound, ``>= 1``; ``None`` removes the
            restriction (``rho`` infinite).
        kappa: Share-restriction parameter in ``(0, 1]``; ``0.5`` means no
            metric may contribute less than half nor more than double its
            uniform share.
        delta: Floor added to ``X`` in the share restrictions, ``> 0``.
        restrict_to_front: When ``True``, the non-domination constraint is
            only imposed against the **exact** Pareto front
            (:func:`~macroforecast.trade.aggregation.pareto.pareto_front`,
            never the ``epsilon`` front): the maximum of a linear form with
            non-negative coefficients over a finite set is attained at a
            non-dominated point, so the restriction is exact, not an
            approximation.
        n_jobs: Number of parallel workers for the ``n`` programmes, passed
            to ``joblib`` when it is installed (lazy import); ``None``, ``1``
            or a missing ``joblib`` runs the loop sequentially.

    Returns:
        Tuple ``(scores, weights)``: ``scores`` of shape ``(n,)`` in
        ``[0, 1]`` (``NaN`` where the programme is infeasible), ``weights``
        of shape ``(n, d)``, one individual weight vector per product.

    Raises:
        ValueError: If ``X`` holds a negative value, or a parameter is out of
            range.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.0, 1.0, 1.0], [0.5, 0.5, 0.5], [1.0, 0.2, 0.3]])
        >>> scores, weights = benefit_of_doubt_weights(X)
        >>> bool(scores[0] > 0.5)
        True
        >>> bool(np.all(scores[np.isfinite(scores)] <= 1 + 1e-6))
        True
    """
    X = check_array(X)
    if np.any(X < 0):
        raise ValueError(
            "benefit_of_doubt_weights requires a non-negative matrix "
            "(apply a min-max or rank normalisation first)."
        )
    n, d = X.shape
    # Front exact uniquement : le front epsilon (M-02) n'est pas valide ici
    constraint_rows = X[pareto_front(X)] if restrict_to_front else X

    # Programmes independants d'un produit a l'autre : parallelisation possible
    def solve(index: int) -> Tuple[float, np.ndarray]:
        x_o = X[index]
        rows, bounds = _restriction_rows(
            x_o, restriction=restriction, rho=rho, kappa=kappa, delta=delta
        )
        return _solve_bod(x_o, constraint_rows, rows, bounds)

    results: List[Tuple[float, np.ndarray]]
    if n_jobs is not None and n_jobs != 1:
        try:
            # Importation paresseuse : joblib reste une dependance facultative
            from joblib import Parallel, delayed
        except ImportError:
            results = [solve(index) for index in range(n)]
        else:
            results = list(
                Parallel(n_jobs=n_jobs)(delayed(solve)(index) for index in range(n))
            )
    else:
        results = [solve(index) for index in range(n)]

    scores = np.array([score for score, _ in results], dtype=float)
    weights = np.array([weight for _, weight in results], dtype=float).reshape(n, d)
    return scores, weights


# ──────────────────────────────────────────────────────────────────────
# SMAA : tirage de poids uniforme sur le simplexe
# ──────────────────────────────────────────────────────────────────────

# Fonction de tirage de poids uniformes sur le simplexe
def dirichlet_weights(
    d: int, n_draws: int, random_state: Optional[int] = None
) -> np.ndarray:
    """Draw weight vectors uniformly on the simplex, ``w ~ Dir(1, ..., 1)``.

    The exact formalisation of "no ability to rank the criteria" — the
    building block of the SMAA protocol (§6.4 / algorithm 5 of the note).

    Args:
        d: Number of metrics (simplex dimension).
        n_draws: Number of weight vectors to draw.
        random_state: Seed forwarded to ``numpy.random.default_rng``.

    Returns:
        Array of shape ``(n_draws, d)``, each row summing to 1.

    Examples:
        >>> import numpy as np
        >>> draws = dirichlet_weights(3, 5, random_state=0)
        >>> draws.shape
        (5, 3)
        >>> bool(np.allclose(draws.sum(axis=1), 1.0))
        True
    """
    rng = np.random.default_rng(random_state)
    return rng.dirichlet(np.ones(d), size=n_draws)
