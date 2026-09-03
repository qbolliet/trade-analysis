"""Endogenous weighting schemes.

Implements §4 of the methodological note: a weight vector ``w`` in the
simplex, entirely determined by the metric matrix ``X`` — no weight is fixed
by expert judgement. Four schemes differ only in which statistic of ``X``
they turn into weights (entropy, CRITIC, PCA), plus the *benefit of the
doubt* programme, whose "weight vector" is individual to each product rather
than shared.

Every scheme returns a plain ``numpy.ndarray`` — no estimator wrapper here;
:class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator` is the
sklearn-facing layer that calls into this module.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
from typing import Optional, Tuple
# Modules de manipulation de données
import numpy as np
from scipy.optimize import linprog
from sklearn.decomposition import PCA
from sklearn.utils.validation import check_array
# Modules du package
from .pareto import pareto_front
from .preprocessing import spearman_correlation_matrix


# ──────────────────────────────────────────────────────────────────────
# Entropie de Shannon
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul des poids par entropie de Shannon
def entropy_weights(X: np.ndarray) -> np.ndarray:
    """Weight each metric by ``1 - normalised Shannon entropy`` of its column.

    A metric whose values are near-identical across products carries little
    information (high entropy) and receives a low weight; a widely spread
    metric receives a high weight. Requires ``X >= 0`` (a min-max
    normalisation upstream), the entropy weighting being non-invariant by
    translation (§4.1 of the note).

    Args:
        X: Metric matrix of shape ``(n, d)``, non-negative.

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
    """
    X = check_array(X)
    if np.any(X < 0):
        raise ValueError(
            "entropy_weights requires a non-negative matrix "
            "(apply a min-max normalisation first)."
        )
    n, d = X.shape
    if n <= 1:
        return np.full(d, 1.0 / d)

    # Conversion de chaque colonne en distribution de probabilité
    column_sums = X.sum(axis=0)
    safe_sums = np.where(column_sums > 0, column_sums, 1.0)
    p = X / safe_sums

    # Entropie normalisée, convention 0 ln 0 = 0 (log(0) neutralisé par le
    # np.where : le calcul intermédiaire déclenche un avertissement bénin,
    # explicitement ignoré ici plutôt que masqué en amont)
    with np.errstate(divide="ignore", invalid="ignore"):
        p_log_p = np.where(p > 0, p * np.log(p), 0.0)
    entropy = -p_log_p.sum(axis=0) / np.log(n)
    divergence = 1.0 - entropy

    if divergence.sum() == 0:
        return np.full(d, 1.0 / d)
    return divergence / divergence.sum()


# ──────────────────────────────────────────────────────────────────────
# CRITIC
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul des poids CRITIC
def critic_weights(X: np.ndarray, method: str = "pearson") -> np.ndarray:
    """Weight each metric by dispersion times originality (CRITIC).

    ``C_j = σ_j · Σ_k (1 - r_jk)``, maximal when metric ``j`` is
    anti-correlated with every other and minimal when it is a near-replica of
    another — correcting the blind spot of :func:`entropy_weights`, which
    looks at one column at a time and would double-count two near-identical
    metrics.

    Args:
        X: Metric matrix of shape ``(n, d)``, typically min-max normalised.
        method: ``"pearson"`` (the original CRITIC) or ``"spearman"`` (the
            note's recommended robust variant, paired with the median/MAD
            standard deviation... left to the caller: this function always
            uses the plain standard deviation, only the correlation method
            varies).

    Returns:
        Weight vector of shape ``(d,)``, summing to 1.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.0, 0.0], [0.5, 1.0], [1.0, 0.2]])
        >>> weights = critic_weights(X)
        >>> round(float(weights.sum()), 6)
        1.0
    """
    X = check_array(X)
    d = X.shape[1]
    sigma = X.std(axis=0, ddof=1) if X.shape[0] > 1 else np.zeros(d)

    if method == "spearman":
        correlation = spearman_correlation_matrix(X)
    else:
        correlation = np.corrcoef(X, rowvar=False) if d > 1 else np.ones((1, 1))

    contrast = sigma * np.sum(1.0 - correlation, axis=1)
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


# Fonction de pondération par ACP
def pca_weights(
    X: np.ndarray, *, rotate: bool = False, n_components: Optional[int] = None
) -> Tuple[np.ndarray, PcaWeightingReport]:
    """Derive weights from a principal-component analysis.

    Args:
        X: Metric matrix of shape ``(n, d)``, **assumed already standardised**
            (mean 0, unit variance — e.g. via
            ``sklearn.preprocessing.StandardScaler`` upstream), so that the
            covariance PCA operates on is the correlation matrix ``R`` of the
            note.
        rotate: ``False`` (default) returns the sign-fixed first-axis
            loadings directly as weights (§4.3's simple variant): a linear
            combination, not necessarily non-negative or unit-sum. ``True``
            switches to the OECD/JRC multi-axis procedure: axes with
            eigenvalue above 1 (Kaiser criterion) are varimax-rotated, and
            each metric's weight is ``max_m loading_jm^2 · λ_m / Σλ`` — always
            non-negative, summing to 1.
        n_components: Number of axes considered by the multi-axis variant
            (``rotate=True``). Defaults to ``d``; every axis with eigenvalue
            below 1 is then dropped by the Kaiser criterion.

    Returns:
        Tuple ``(weights, report)``.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> base = rng.normal(size=100)
        >>> X = np.column_stack([base, base, rng.normal(size=100)])
        >>> X = (X - X.mean(axis=0)) / X.std(axis=0)
        >>> weights, report = pca_weights(X)
        >>> weights.shape
        (3,)
        >>> report.variance_share_axis1 > 0.5
        True
    """
    X = check_array(X)
    d = X.shape[1]

    if not rotate:
        # Variante simple : premier axe seul, signe fixé
        pca = PCA(n_components=1)
        pca.fit(X)
        loading = pca.components_[0]
        if loading.sum() < 0:
            loading = -loading
        report = PcaWeightingReport(
            explained_variance_ratio=pca.explained_variance_ratio_,
            has_negative_loadings=bool(np.any(loading < 0)),
            variance_share_axis1=float(pca.explained_variance_ratio_[0]),
        )
        return loading, report

    # Variante multi-axes OCDE-JRC : rotation varimax des axes de valeur propre > 1
    n_components = n_components or d
    pca = PCA(n_components=min(n_components, d))
    pca.fit(X)
    eigenvalues = pca.explained_variance_
    keep = eigenvalues > 1.0
    if not np.any(keep):
        # Repli : aucun axe ne dépasse le critère de Kaiser, on garde le premier
        keep = np.zeros_like(eigenvalues, dtype=bool)
        keep[0] = True

    selected_eigenvalues = eigenvalues[keep]
    # Saturations (loadings) = vecteurs propres pondérés par sqrt(valeur propre)
    raw_loadings = pca.components_[keep].T * np.sqrt(selected_eigenvalues)
    rotated_loadings = _varimax(raw_loadings)

    lambda_share = selected_eigenvalues / selected_eigenvalues.sum()
    contribution = rotated_loadings**2 * lambda_share[None, :]
    weight = np.max(contribution, axis=1)
    weight = weight / weight.sum() if weight.sum() > 0 else np.full(d, 1.0 / d)

    report = PcaWeightingReport(
        explained_variance_ratio=pca.explained_variance_ratio_[keep],
        has_negative_loadings=bool(np.any(raw_loadings < 0)),
        variance_share_axis1=float(pca.explained_variance_ratio_[0]),
    )
    return weight, report


# ──────────────────────────────────────────────────────────────────────
# Benefit of the doubt
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul du score et des poids individuels benefit of the doubt
def benefit_of_doubt_weights(
    X: np.ndarray, *, kappa: float = 0.5, restrict_to_front: bool = True
) -> Tuple[np.ndarray, np.ndarray]:
    """Solve one linear programme per product for its most favourable weights.

    Each product is scored under the weighting that makes it *as vulnerable
    as possible*, subject to that same weighting never scoring another
    product above 1 (algorithm 3 of the note). Share restrictions
    (``κ/d ≤ share_j ≤ 1/(κd)``) prevent the degenerate solution that puts
    all the weight on the single metric a product happens to shine on.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity, positive
            values (a share is undefined otherwise).
        kappa: Share-restriction parameter in ``(0, 1]``; ``0.5`` means no
            metric may contribute less than half nor more than double its
            uniform share.
        restrict_to_front: When ``True``, the non-domination constraint is
            only imposed against the Pareto front (see
            :func:`~macroforecast.trade.aggregation.pareto.pareto_front`) —
            the note's own remedy for the cost of ``n`` programmes each with
            ``n`` constraints, valid because only the upper envelope of the
            cloud can ever bind such a constraint.

    Returns:
        Tuple ``(scores, weights)``: ``scores`` of shape ``(n,)`` in
        ``(0, 1]`` (``NaN`` where the programme is infeasible), ``weights``
        of shape ``(n, d)``, one individual weight vector per product.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 0.2], [0.2, 1.0], [0.5, 0.5]])
        >>> scores, weights = benefit_of_doubt_weights(X, kappa=0.5)
        >>> bool(np.all(scores[np.isfinite(scores)] <= 1 + 1e-6))
        True
    """
    X = check_array(X)
    n, d = X.shape
    constraint_rows = X[pareto_front(X)] if restrict_to_front else X

    lower = kappa / d
    upper = 1.0 / (kappa * d)

    scores = np.full(n, np.nan)
    weights = np.full((n, d), np.nan)
    identity = np.eye(d)
    for product in range(n):
        x_o = X[product]
        # Objectif : maximiser x_o . w <=> minimiser -x_o . w
        objective = -x_o

        # Contraintes de non-domination : constraint_rows @ w <= 1
        a_ub = [row.copy() for row in constraint_rows]
        b_ub = [1.0] * len(constraint_rows)

        # Contraintes de restriction de part, linéarisées (cf. note, §4.4)
        for j in range(d):
            row_lower = lower * x_o - identity[j] * x_o[j]
            a_ub.append(row_lower)
            b_ub.append(0.0)
            row_upper = identity[j] * x_o[j] - upper * x_o
            a_ub.append(row_upper)
            b_ub.append(0.0)

        result = linprog(
            objective,
            A_ub=np.asarray(a_ub),
            b_ub=np.asarray(b_ub),
            bounds=[(0, None)] * d,
            method="highs",
        )
        if result.success:
            scores[product] = -result.fun
            weights[product] = result.x
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
