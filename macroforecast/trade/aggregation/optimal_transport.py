"""Oriented Kantorovitch score (optimal transport).

Implements §6 of the methodological note. The empirical distribution of the
metrics is transported onto the uniform measure of the unit ball (the
*center-outward* distribution/rank/sign of Hallin et al.), which normalises
scale, skew and correlation in one shot, without a single per-coordinate
normalisation decision. The raw rank ``‖T(z)‖`` is not orientable — it grows
from the centre of the cloud towards its periphery in *every* direction at
once, so it measures atypicality, not vulnerability — hence the reorientation
of §6.3: the transported points are projected on the image of a synthetic
"maximally vulnerable" product, which restores a meaningful direction.

Optional dependency:
    Only this module needs ``jax`` and ``ott-jax`` (the Sinkhorn solver and
    its entropic, out-of-sample transport map) — the rest of
    :mod:`macroforecast.trade.aggregation` imports and runs without them.
    Install the ``optimal-transport`` extra
    (``pip install macroforecast[optimal-transport]``) to use this module;
    every public function raises a clear :class:`ImportError` otherwise, the
    same convention followed by
    :func:`~macroforecast.trade.processing.baci` for ``dt-ducklake-manager``.

The note itself ranks this method last in the recommended workflow (§7): it
only earns its extra hyperparameters (``epsilon``, grid size) and dependency
weight when the metric cloud is markedly non-elliptical — see
:func:`ellipticity_screen`, which implements the note's own decision
criterion (a Kendall τ above 0.95 against the Mahalanobis score means the
transport reduces to it, and its extra cost is not justified).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
from typing import Optional
# Modules de manipulation de données
import numpy as np
from scipy import stats
from scipy.stats import qmc
from sklearn.utils.validation import check_array

# Nom de l'extra pip à installer pour activer ce module
_EXTRA_NAME = "optimal-transport"


# Fonction de vérification de la disponibilité de jax/ott-jax (import paresseux)
def _require_ott():
    """Import ``jax`` and ``ott`` lazily, with an explicit error otherwise.

    Returns:
        Tuple of the imported ``jax.numpy``, ``ott.geometry.pointcloud``,
        ``ott.problems.linear.linear_problem`` and
        ``ott.solvers.linear.sinkhorn`` modules.

    Raises:
        ImportError: If ``jax`` or ``ott-jax`` is not installed.
    """
    try:
        import jax.numpy as jnp
        from ott.geometry import pointcloud
        from ott.problems.linear import linear_problem
        from ott.solvers.linear import sinkhorn
    except ImportError as error:
        raise ImportError(
            "The oriented Kantorovitch score requires the optional "
            f"'{_EXTRA_NAME}' extra: install it with "
            f"`pip install macroforecast[{_EXTRA_NAME}]` "
            "(jax + ott-jax)."
        ) from error
    return jnp, pointcloud, linear_problem, sinkhorn


# ──────────────────────────────────────────────────────────────────────
# Grille sphérique (numpy/scipy pur, sans jax)
# ──────────────────────────────────────────────────────────────────────

# Fonction de tirage d'une grille à faible discrépance sur la boule unité
def spherical_uniform_grid(n_points: int, dim: int, seed: int = 0) -> np.ndarray:
    """Draw a low-discrepancy sample from the uniform measure on the unit ball.

    Pure numpy/scipy — no jax dependency here, only the Sinkhorn solve itself
    needs it. A Sobol sequence on the hypercube is mapped to quasi-uniform
    directions on the sphere via the inverse Gaussian CDF, combined with
    stratified radii (the ``ρ^{1/dim}`` power law that makes the radial
    density uniform *in the ball*, not on the sphere).

    Args:
        n_points: Number of target points in the reference measure.
        dim: Ambient dimension of the score space.
        seed: Seed of the Sobol engine.

    Returns:
        Array of shape ``(n_points, dim)`` holding the reference grid.

    Examples:
        >>> grid = spherical_uniform_grid(8, 2, seed=0)
        >>> grid.shape
        (8, 2)
        >>> bool(np.all(np.linalg.norm(grid, axis=1) <= 1 + 1e-9))
        True
    """
    # Suite de Sobol sur l'hypercube (dim directions + 1 rayon), puis
    # transformation gaussienne pour des directions quasi uniformes
    engine = qmc.Sobol(d=dim + 1, scramble=True, seed=seed)
    sample = engine.random(n_points)
    sample = np.clip(sample, 1e-6, 1.0 - 1e-6)

    gaussians = stats.norm.ppf(sample[:, :dim])
    norms = np.linalg.norm(gaussians, axis=1, keepdims=True)
    directions = gaussians / norms

    # Rayons stratifiés : la puissance 1/dim assure l'uniformité dans la boule
    radii = sample[:, dim:] ** (1.0 / dim)
    return directions * radii


# ──────────────────────────────────────────────────────────────────────
# Score de Kantorovitch orienté
# ──────────────────────────────────────────────────────────────────────

# Rapport d'ajustement du score de transport
@dataclass
class OrientedKantorovichReport:
    """Fit diagnostics of :class:`OrientedKantorovichScorer`.

    Attributes:
        converged: Whether the Sinkhorn solve converged.
        uniformity_ks_statistic: Kolmogorov-Smirnov statistic comparing the
            transported norms ``‖T̄(x_i)‖`` to the uniform distribution on
            ``[0, 1]`` — the control the note recommends: a converged
            transport map should leave the norms approximately uniform.
        uniformity_ks_p_value: Associated p-value.
    """
    converged: bool
    uniformity_ks_statistic: float
    uniformity_ks_p_value: float


# Estimateur sklearn du score de Kantorovitch orienté
class OrientedKantorovichScorer:
    """Rank products by an orientation-corrected center-outward score.

    Follows the ``fit`` / ``score_samples`` convention rather than full
    ``BaseEstimator`` inheritance (the fitted state is a JAX transport
    function, not an array sklearn's ``check_is_fitted`` machinery expects).

    Args:
        epsilon: Entropic regularisation strength of the Sinkhorn solve
            (``0.1`` is the note's robust default).
        n_target: Size of the reference grid on the unit ball (``2**12`` to
            ``2**15`` in the note, growing with the dimension).
        pole_quantile: Marginal quantile defining the synthetic "maximally
            vulnerable" pole used to fix the orientation.
        seed: Seed of the reference-grid Sobol engine.

    Examples:
        >>> scorer = OrientedKantorovichScorer(n_target=64)  # doctest: +SKIP
        >>> scores = scorer.fit(X).score_samples(X)  # doctest: +SKIP
    """

    # Initialisation
    def __init__(
        self,
        epsilon: float = 0.1,
        n_target: int = 2**12,
        pole_quantile: float = 0.99,
        seed: int = 0,
    ) -> None:
        self.epsilon = epsilon
        self.n_target = n_target
        self.pole_quantile = pole_quantile
        self.seed = seed

    # Ajustement : résolution de Sinkhorn et fixation de la direction u*
    def fit(self, X: np.ndarray, y: None = None) -> "OrientedKantorovichScorer":
        """Solve the entropic transport problem and fix the vulnerability direction.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``, with the fitted transport map and direction ``u*``.

        Raises:
            ImportError: If ``jax``/``ott-jax`` is not installed.
        """
        jnp, pointcloud, linear_problem, sinkhorn = _require_ott()
        X = check_array(X)
        grid = spherical_uniform_grid(self.n_target, X.shape[1], seed=self.seed)

        # Résolution du problème de Sinkhorn entre les scores et la boule uniforme
        geometry = pointcloud.PointCloud(jnp.asarray(X), jnp.asarray(grid), epsilon=self.epsilon)
        solution = sinkhorn.Sinkhorn()(linear_problem.LinearProblem(geometry))
        self._converged = bool(solution.converged)
        self._transport = solution.to_dual_potentials().transport

        # Direction de vulnérabilité, estimée sur les données
        pole = np.quantile(X, self.pole_quantile, axis=0)
        pole_image = np.asarray(self._transport(jnp.asarray(pole[None, :]))[0])
        self._direction = pole_image / np.linalg.norm(pole_image)
        self.n_features_in_ = X.shape[1]
        return self

    # Transport hors échantillon (interne)
    def _transport_points(self, X: np.ndarray) -> np.ndarray:
        """Apply the fitted entropic transport map to new points."""
        jnp, *_ = _require_ott()
        return np.asarray(self._transport(jnp.asarray(check_array(X))))

    # Score orienté : projection signée
    def score_samples(self, X: np.ndarray) -> np.ndarray:
        """Project the transported points on the vulnerability direction.

        The simplest of the note's three variants (§6.3): reads as a
        weighted sum in the transported space, with the direction ``u*`` as
        estimated coefficients.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Score vector of shape ``(n,)``, higher meaning more vulnerable.
        """
        return self._transport_points(X) @ self._direction

    # Normes transportées : contrôle d'uniformité
    def transported_norms(self, X: np.ndarray) -> np.ndarray:
        """Return ``‖T̄(x_i)‖`` for every row — the (unoriented) rank.

        Feeds the uniformity control of :func:`fit_report`: a converged,
        valid transport map leaves these norms approximately uniform on
        ``[0, 1]``, whatever the source distribution.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Norm vector of shape ``(n,)``.
        """
        return np.linalg.norm(self._transport_points(X), axis=1)

    # Rapport de diagnostic du transport
    def fit_report(self, X: np.ndarray) -> OrientedKantorovichReport:
        """Build the convergence and uniformity diagnostics of the fit.

        Args:
            X: Metric matrix the scorer was fitted on (or a held-out sample).

        Returns:
            The :class:`OrientedKantorovichReport`.
        """
        norms = self.transported_norms(X)
        statistic, p_value = stats.kstest(norms, "uniform")
        return OrientedKantorovichReport(
            converged=self._converged,
            uniformity_ks_statistic=float(statistic),
            uniformity_ks_p_value=float(p_value),
        )


# ──────────────────────────────────────────────────────────────────────
# Critère de décision : le transport optimal apporte-t-il quelque chose ?
# ──────────────────────────────────────────────────────────────────────

# Fonction de test de la pertinence du transport optimal face à Mahalanobis
def ellipticity_screen(mahalanobis_scores: np.ndarray, ot_scores: np.ndarray) -> float:
    """Compare the OT-projection score with the Mahalanobis score.

    Implements the note's decision criterion (§6.5): when the metric cloud
    is (near-)elliptical, Brenier's transport map is affine and the oriented
    Kantorovitch score reduces exactly to the Mahalanobis distance. A high
    Kendall τ between the two rankings is the empirical signature of that
    case — the note suggests ``0.95`` as the threshold above which the extra
    complexity of optimal transport is not justified.

    Args:
        mahalanobis_scores: Scores of
            :func:`~macroforecast.trade.aggregation.functions.mahalanobis_score`.
        ot_scores: Scores of :meth:`OrientedKantorovichScorer.score_samples`.

    Returns:
        Kendall's τ_b between the two rankings.

    Examples:
        >>> import numpy as np
        >>> a = np.array([1.0, 2.0, 3.0, 4.0])
        >>> b = np.array([1.1, 2.2, 2.9, 4.3])
        >>> ellipticity_screen(a, b) > 0.95
        True
    """
    tau, _ = stats.kendalltau(mahalanobis_scores, ot_scores)
    return float(tau)
