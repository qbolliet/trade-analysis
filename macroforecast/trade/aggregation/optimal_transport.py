"""Oriented Kantorovitch score (optimal transport).

Implements §6 of the methodological note. The empirical distribution of the
metrics is transported onto the **spherical uniform** measure ``U_d`` of the
unit ball (the *center-outward* distribution/rank/sign of Hallin et al. 2021
and Chernozhukov et al. 2017), which normalises scale, skew and correlation
in one shot, without a single per-coordinate normalisation decision. The raw
rank ``‖T(z)‖`` is not orientable — it grows from the centre of the cloud
towards its periphery in *every* direction at once, so it measures
atypicality, not vulnerability — hence the reorientation of §6.3: the
transported points are projected on the image of a synthetic "maximally
vulnerable" product, which restores a meaningful direction.

Reference measure:
    ``U_d`` = (uniform direction on ``S^{d-1}``) × (**uniform** radius on
    ``[0, 1]``), *not* the Lebesgue-uniform measure of the ball. Only under
    ``U_d`` are the transported norms uniform on ``[0, 1]`` and the quantile
    regions of coverage ``1 - α`` — the two properties the convergence
    control and the alert threshold rely on (M-01).

Expected input:
    A standardising normalisation (``quantile_gaussian``, the default of
    D-13, or ``standard``). The entropic regularisation ``epsilon`` is
    relative to the mean transport cost (``scale_cost="mean"``), so it no
    longer depends on the absolute scale of the metrics, but the *relative*
    dispersion of the columns still shapes the map.

Scaling (D-11):
    The Sinkhorn solve is run on a subsample of at most ``fit_sample_size``
    rows and every point is transported in batches of ``batch_size``, the
    entropic map being defined out of sample. A group of ``n ≈ 2·10^5`` rows
    is therefore scored with a memory footprint that does not grow with
    ``n``.

Optional dependency:
    Only this module needs ``jax`` and ``ott-jax`` (the Sinkhorn solver and
    its entropic, out-of-sample transport map) — the rest of
    :mod:`macroforecast.trade.aggregation` imports and runs without them.
    Install the ``optimal-transport`` extra
    (``pip install macroforecast[optimal-transport]``) to use this module;
    every public entry point raises a clear :class:`ImportError` otherwise,
    the same convention followed by
    :func:`~macroforecast.trade.processing.baci` for ``dt-ducklake-manager``.

The ellipticity diagnostic (:func:`ellipticity_screen`) is **reported, not a
gate** (D-07): it says whether the transport added anything to its linear
counterpart on this group, it does not decide whether the score is computed.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import math
import warnings
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
# Modules de manipulation de données
import numpy as np
from scipy import stats
from scipy.stats import qmc
from sklearn.base import BaseEstimator
from sklearn.utils.validation import check_array, check_is_fitted
# Modules du package
from .functions import mahalanobis_score, whitened_projection_score

# Nom de l'extra pip à installer pour activer ce module
_EXTRA_NAME = "optimal-transport"

# Quantile de référence de la mesure de sensibilité de la direction (`direction_cos_`) :
# point de comparaison fixe, moins extrapolé que `pole_quantile`
_SENSITIVITY_QUANTILE = 0.9

# Tolérance de nullité d'une norme transportée (signe indéfini au centre)
_NORM_TOLERANCE = 1e-12

# Variantes admissibles des paramètres catégoriels
_SCORE_VARIANTS = ("projection", "cone", "modulated")
_ALERT_VARIANTS = ("radial", "projected")
_CONFORMAL_MODES = ("split", "fit")
_RADIAL_LAWS = ("uniform", "lebesgue")


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
def spherical_uniform_grid(
    n_points: int,
    dim: int,
    seed: int = 0,
    radial: str = "uniform",
) -> np.ndarray:
    """Draw a low-discrepancy sample from a reference measure on the unit ball.

    Pure numpy/scipy — no jax dependency here, only the Sinkhorn solve itself
    needs it. A Sobol sequence on the hypercube is mapped to quasi-uniform
    directions on the sphere via the inverse Gaussian CDF, then combined with
    stratified radii drawn from one of two laws:

    * ``radial="uniform"`` — the **spherical uniform** law ``U_d`` of the
      center-outward literature: ``u = ρ·θ`` with ``ρ ~ U[0, 1]`` and ``θ``
      uniform on the sphere. Under ``U_d``, ``‖u‖ ~ U[0, 1]`` and
      ``P(‖u‖ ≤ r) = r``, so the ball ``B(0, 1-α)`` carries mass ``1-α``
      exactly. This is what makes the Kolmogorov-Smirnov control of
      :meth:`OrientedKantorovichScorer.fit_report` and the radial alert
      threshold legitimate (M-01).
    * ``radial="lebesgue"`` — the Lebesgue-uniform law *in* the ball
      (radii in ``ρ^{1/dim}``), kept for comparison only: its norms follow a
      ``Beta(dim, 1)`` law and ``P(‖u‖ ≤ r) = r^dim``.

    Args:
        n_points: Number of target points in the reference measure.
        dim: Ambient dimension of the score space.
        seed: Seed of the Sobol engine.
        radial: Radial law, ``"uniform"`` (default) or ``"lebesgue"``.

    Returns:
        Array of shape ``(n_points, dim)`` holding the reference grid.

    Raises:
        ValueError: If ``radial`` is unknown.

    Examples:
        >>> grid = spherical_uniform_grid(8, 2, seed=0)
        >>> grid.shape
        (8, 2)
        >>> bool(np.all(np.linalg.norm(grid, axis=1) <= 1 + 1e-9))
        True

        Under the spherical uniform law the norms are uniform on ``[0, 1]``:

        >>> from scipy import stats
        >>> norms = np.linalg.norm(spherical_uniform_grid(4096, 6, seed=0), axis=1)
        >>> bool(stats.kstest(norms, "uniform").pvalue > 0.01)
        True

        Under the Lebesgue law of the ball they are not:

        >>> lebesgue = spherical_uniform_grid(4096, 6, seed=0, radial="lebesgue")
        >>> bool(stats.kstest(np.linalg.norm(lebesgue, axis=1), "uniform").pvalue > 0.01)
        False
    """
    # Vérification des arguments
    if radial not in _RADIAL_LAWS:
        raise ValueError(
            f"Unknown radial law {radial!r}. Available: {list(_RADIAL_LAWS)}."
        )

    # Suite de Sobol sur l'hypercube (dim directions + 1 rayon), puis
    # transformation gaussienne pour des directions quasi uniformes
    engine = qmc.Sobol(d=dim + 1, scramble=True, seed=seed)
    sample = engine.random(n_points)
    sample = np.clip(sample, 1e-6, 1.0 - 1e-6)

    gaussians = stats.norm.ppf(sample[:, :dim])
    norms = np.linalg.norm(gaussians, axis=1, keepdims=True)
    directions = gaussians / norms

    # Rayons stratifiés par la dernière coordonnée de Sobol : `ρ` sous la loi
    # sphérique uniforme, `ρ^{1/dim}` sous la loi de Lebesgue de la boule
    radii = sample[:, dim:]
    if radial == "lebesgue":
        radii = radii ** (1.0 / dim)
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
            ``[0, 1]`` — the control the note recommends: under the spherical
            uniform reference, a converged transport map leaves the norms
            approximately uniform (M-01).
        uniformity_ks_p_value: Associated p-value.
        direction_cos: Cosine between the vulnerability direction estimated at
            the ``pole_quantile`` pole and the one estimated at a less
            extrapolated pole. Close to ``1`` means the orientation does not
            hinge on the behaviour of the map outside the support of the cloud.
        alert_threshold: Fitted alert threshold (``r̂_α`` or ``q̂_{1-α}``
            depending on ``alert_score``); ``inf`` when ``α`` is too small for
            the calibration sample to resolve it.
        alert_share: Share of the calibration sample above the threshold —
            close to ``α`` by construction under split-conformal calibration.
        n_fit: Number of rows the transport map was solved on.
        n_cal: Number of rows held out for the conformal calibration
            (``0`` under ``conformal="fit"``).
    """
    converged: bool
    uniformity_ks_statistic: float
    uniformity_ks_p_value: float
    direction_cos: float
    alert_threshold: float
    alert_share: float
    n_fit: int
    n_cal: int


# Estimateur sklearn du score de Kantorovitch orienté
class OrientedKantorovichScorer(BaseEstimator):
    """Rank products by an orientation-corrected center-outward score.

    A regular ``sklearn`` estimator (``fit`` / ``predict``, hence
    ``get_params`` and ``clone``), extended with the objects the
    center-outward construction needs: :meth:`transport`, :meth:`ranks`,
    :meth:`signs` and :meth:`alert`.

    Note:
        The constructor argument selecting the non-conformity score is named
        ``alert_score`` rather than ``alert``: ``alert`` is the name of the
        method (the estimator protocol of S-1.4), and sklearn requires
        ``__init__`` to store every parameter under its own name, so an
        argument named ``alert`` would shadow the method on the instance.

    Args:
        epsilon: Entropic regularisation strength of the Sinkhorn solve,
            **relative** to the cost rescaling of ``scale_cost`` (``0.005`` is
            robust default); the absolute value actually solved is
            exposed as ``epsilon_`` after :meth:`fit`. The entropic map is a
            softmax average of grid points, so it shrinks the ranks towards
            the centre: the smaller ``epsilon``, the closer the transported
            norms to their uniform target.
        scale_cost: Cost rescaling used to make ``epsilon`` scale-free;
            forwarded to ``ott``'s ``PointCloud`` to *measure* the scaling
            (``"mean"``, ``"max_cost"``, ``"median"`` or a float), never to
            the solved geometry — see :meth:`fit`.
        n_target: Size of the reference grid on the unit ball (``2**12`` to
            ``2**15`` in the note, growing with the dimension).
        pole_quantile: Marginal quantile defining the synthetic "maximally
            vulnerable" pole ``x⁺`` used to fix the orientation — the same
            construction as
            :func:`~macroforecast.trade.aggregation.functions.whitened_projection_score`,
            so that the two scores share a direction and stay comparable.
        seed: Seed of the reference-grid Sobol engine and of the subsampling
            and calibration draws.
        fit_sample_size: Maximum number of rows the Sinkhorn problem is solved
            on; larger inputs are subsampled without replacement.
        batch_size: Number of rows transported per batch out of sample.
        score: Oriented score variant returned by :meth:`predict` —
            ``"projection"`` ``⟨T̄(x), u*⟩``, ``"cone"``
            ``‖T̄(x)‖·1{⟨S(x), u*⟩ ≥ cos θ₀}`` or ``"modulated"``
            ``‖T̄(x)‖·(1 + ⟨S(x), u*⟩)/2``.
        alpha: Nominal alert level.
        theta0_degrees: Half-angle ``θ₀`` of the vulnerability cone, in
            degrees.
        conformal: ``"split"`` for a genuine split-conformal threshold
            (calibrated on rows the map never saw, the only variant with a
            finite-sample guarantee, M-07) or ``"fit"`` for a descriptive
            empirical quantile on the fitting sample.
        calibration_fraction: Share of the subsample **held out for
            calibration** under ``conformal="split"``.
        alert_score: Non-conformity score of :meth:`alert` — ``"radial"``
            (``‖T̄(x)‖``, combined with the cone condition) or ``"projected"``
            (the oriented score itself).
        max_iterations: Maximum number of Sinkhorn iterations.
        threshold: Convergence threshold of the Sinkhorn solve.

    Examples:
        >>> scorer = OrientedKantorovichScorer(n_target=64)  # doctest: +SKIP
        >>> scores = scorer.fit(X).predict(X)  # doctest: +SKIP
    """

    # Initialisation
    def __init__(
        self,
        epsilon: float = 0.005,
        n_target: int = 4096,
        pole_quantile: float = 0.99,
        seed: int = 0,
        scale_cost: str = "mean",
        fit_sample_size: int = 20_000,
        batch_size: int = 8192,
        score: str = "projection",
        alpha: float = 0.05,
        theta0_degrees: float = 60.0,
        conformal: str = "split",
        calibration_fraction: float = 0.5,
        alert_score: str = "projected",
        max_iterations: int = 2000,
        threshold: float = 1e-3,
    ) -> None:
        self.epsilon = epsilon
        self.n_target = n_target
        self.pole_quantile = pole_quantile
        self.seed = seed
        self.scale_cost = scale_cost
        self.fit_sample_size = fit_sample_size
        self.batch_size = batch_size
        self.score = score
        self.alpha = alpha
        self.theta0_degrees = theta0_degrees
        self.conformal = conformal
        self.calibration_fraction = calibration_fraction
        self.alert_score = alert_score
        self.max_iterations = max_iterations
        self.threshold = threshold

    # Ajustement : Sinkhorn, direction u* et calibration du seuil d'alerte
    def fit(self, X: np.ndarray, y: None = None) -> "OrientedKantorovichScorer":
        """Solve the entropic transport problem and calibrate the alert threshold.

        Four steps on nested, disjoint subsets: subsampling down to
        ``fit_sample_size`` rows, split into a fitting and a calibration half,
        Sinkhorn solve and orientation on the fitting half **only** (the
        exchangeability condition of the conformal guarantee — a direction
        estimated on the calibration rows would leak into the threshold), then
        calibration of the threshold on the held-out half.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity, already
                normalised (``quantile_gaussian`` or ``standard``).
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``, fitted.

        Raises:
            ImportError: If ``jax``/``ott-jax`` is not installed.
            ValueError: If a parameter is outside its admissible set, if the
                calibration split leaves an empty half, or if the pole is
                transported onto the centre (no direction to orient on).
        """
        jnp, pointcloud, linear_problem, sinkhorn = _require_ott()
        X = check_array(X)
        self._check_params()

        X_fit, X_calibration = self._split_sample(X)
        self.n_fit_ = int(X_fit.shape[0])
        self.n_cal_ = int(0 if X_calibration is None else X_calibration.shape[0])
        self.n_features_in_ = int(X.shape[1])

        # Résolution du problème de Sinkhorn entre les métriques et la loi sphérique uniforme
        grid = spherical_uniform_grid(self.n_target, self.n_features_in_, seed=self.seed)
        source, target = jnp.asarray(X_fit), jnp.asarray(grid)
        # Mise à l'échelle repliée dans `epsilon` plutôt que déléguée au coût : les
        # potentiels duaux d'`ott` supposent le coût quadratique *non* redimensionné, et
        # `transport` renvoie une carte distordue si la géométrie porte un `scale_cost`.
        # Résoudre `(C/κ, ε)` équivaut à résoudre `(C, κ·ε)`, d'où un `epsilon` relatif au
        # coût moyen (I-03) et une carte valide.
        self.epsilon_ = self.epsilon / float(
            pointcloud.PointCloud(source, target, scale_cost=self.scale_cost).inv_scale_cost
        )
        geometry = pointcloud.PointCloud(
            source, target, epsilon=self.epsilon_, scale_cost=1.0
        )
        solver = sinkhorn.Sinkhorn(
            max_iterations=self.max_iterations, threshold=self.threshold
        )
        solution = solver(linear_problem.LinearProblem(geometry))
        self.converged_ = bool(solution.converged)
        self._transport = solution.to_dual_potentials().transport
        # Non-convergence signalée, sans exception : la carte reste exploitable
        if not self.converged_:
            warnings.warn(
                "The Sinkhorn solve did not converge within "
                f"{self.max_iterations} iterations (threshold {self.threshold}): "
                "the transported ranks may not be uniform. Raise "
                "`max_iterations` or `epsilon`, or check the normalisation of "
                "the metrics.",
                stacklevel=2,
            )

        # Direction de vulnérabilité et sensibilité au degré d'extrapolation du pôle
        self.direction_ = self._pole_direction(X_fit, self.pole_quantile)
        reference = self._pole_direction(X_fit, _SENSITIVITY_QUANTILE)
        self.direction_cos_ = float(self.direction_ @ reference)

        # Calibration du seuil d'alerte, sur la moitié réservée en mode `split`
        sample = X_fit if X_calibration is None else X_calibration
        nonconformity = self._nonconformity(sample)
        self.threshold_ = self._calibrate(nonconformity)
        self.calibration_alert_share_ = float(np.mean(nonconformity > self.threshold_))
        return self

    # Transport hors échantillon, par lots
    def transport(self, X: np.ndarray) -> np.ndarray:
        """Map new points onto the unit ball with the fitted entropic map.

        The entropic map is defined out of sample, so ``X`` need not be the
        fitting sample; batching bounds the memory of the ``(batch, m)`` cost
        matrix independently of ``n``.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Transported points ``T̄(x_i)`` of shape ``(n, d)``, inside the unit
            ball.
        """
        check_is_fitted(self, "direction_")
        jnp, *_ = _require_ott()
        X = check_array(X)

        # Transport par lots : la matrice de coût d'un lot est `(batch_size, n_target)`
        batches = [
            np.asarray(self._transport(jnp.asarray(X[start : start + self.batch_size])))
            for start in range(0, X.shape[0], self.batch_size)
        ]
        return np.concatenate(batches, axis=0)

    # Rangs center-outward
    def ranks(self, X: np.ndarray) -> np.ndarray:
        """Return the center-outward ranks ``R(x) = ‖T̄(x)‖``.

        Under a converged map and the spherical uniform reference, these are
        approximately uniform on ``[0, 1]`` — the control of
        :meth:`fit_report`. They measure atypicality, not vulnerability: a
        product extreme in the low tail ranks as high as one extreme in the
        high tail, hence the reorientation of :meth:`predict`.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Rank vector of shape ``(n,)``, in ``[0, 1]``.
        """
        return np.linalg.norm(self.transport(X), axis=1)

    # Signes center-outward
    def signs(self, X: np.ndarray) -> np.ndarray:
        """Return the center-outward signs ``S(x) = T̄(x)/‖T̄(x)‖``.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Array of shape ``(n, d)`` of unit vectors; the zero vector for the
            points transported onto the centre, where the sign is undefined.
        """
        return _unit_vectors(self.transport(X))

    # Score orienté
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Score the rows with the oriented variant selected by ``score``.

        The three variants of §6.3 trade orientation against radial
        information: ``"projection"`` reads as a weighted sum in the
        transported space, with ``u*`` as estimated coefficients; ``"cone"``
        keeps the rank but zeroes out every point outside the vulnerability
        cone; ``"modulated"`` weighs the rank by the alignment with ``u*``, a
        continuous compromise between the two.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Score vector of shape ``(n,)``, higher meaning more vulnerable.
        """
        return self._score(self.transport(X), self.score)

    # Alerte booléenne au niveau alpha
    def alert(self, X: np.ndarray) -> np.ndarray:
        """Flag the rows exceeding the calibrated alert threshold.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Boolean vector of shape ``(n,)``. Under ``alert_score="radial"`` a
            row is flagged when its rank exceeds ``r̂_α`` *and* it lies in the
            vulnerability cone; under ``"projected"`` when its oriented score
            exceeds ``q̂_{1-α}``.
        """
        check_is_fitted(self, "threshold_")
        transported = self.transport(X)
        if self.alert_score == "radial":
            radial = np.linalg.norm(transported, axis=1) > self.threshold_
            aligned = _unit_vectors(transported) @ self.direction_ >= self._cos_theta0()
            return radial & aligned
        return self._score(transported, "projection") > self.threshold_

    # Rapport de diagnostic du transport
    def fit_report(self, X: np.ndarray) -> OrientedKantorovichReport:
        """Build the convergence, orientation and calibration diagnostics.

        Args:
            X: Metric matrix the uniformity control is run on — the fitting
                sample or a held-out one.

        Returns:
            The :class:`OrientedKantorovichReport`.
        """
        check_is_fitted(self, "threshold_")
        statistic, p_value = stats.kstest(self.ranks(X), "uniform")
        return OrientedKantorovichReport(
            converged=self.converged_,
            uniformity_ks_statistic=float(statistic),
            uniformity_ks_p_value=float(p_value),
            direction_cos=self.direction_cos_,
            alert_threshold=float(self.threshold_),
            alert_share=self.calibration_alert_share_,
            n_fit=self.n_fit_,
            n_cal=self.n_cal_,
        )

    # Vérification des paramètres catégoriels et numériques
    def _check_params(self) -> None:
        """Validate the categorical and numeric parameters.

        Raises:
            ValueError: If a parameter is outside its admissible set.
        """
        for value, admissible, name in (
            (self.score, _SCORE_VARIANTS, "score"),
            (self.alert_score, _ALERT_VARIANTS, "alert_score"),
            (self.conformal, _CONFORMAL_MODES, "conformal"),
        ):
            if value not in admissible:
                raise ValueError(
                    f"Unknown {name} {value!r}. Available: {list(admissible)}."
                )
        if not 0.0 < self.alpha < 1.0:
            raise ValueError(f"`alpha` must lie in (0, 1), got {self.alpha}.")
        if not 0.0 < self.calibration_fraction < 1.0:
            raise ValueError(
                "`calibration_fraction` must lie in (0, 1), got "
                f"{self.calibration_fraction}."
            )

    # Découpage sous-échantillon d'ajustement / échantillon de calibration
    def _split_sample(self, X: np.ndarray) -> Tuple[np.ndarray, Optional[np.ndarray]]:
        """Subsample ``X`` and split it into a fitting and a calibration part.

        Args:
            X: Validated metric matrix of shape ``(n, d)``.

        Returns:
            Tuple ``(X_fit, X_calibration)``; the second element is ``None``
            under ``conformal="fit"``, where the threshold is descriptive and
            read on the fitting sample itself.

        Raises:
            ValueError: If the split leaves either part empty.
        """
        generator = np.random.default_rng(self.seed)
        n = X.shape[0]

        # Sous-échantillonnage sans remise : Sinkhorn est en `O(n · m)` mémoire
        if n > self.fit_sample_size:
            X = X[generator.choice(n, self.fit_sample_size, replace=False)]
        if self.conformal == "fit":
            return X, None

        # Permutation préalable : la coupe doit être indépendante de l'ordre des lignes
        shuffled = X[generator.permutation(X.shape[0])]
        n_calibration = int(np.floor(self.calibration_fraction * shuffled.shape[0]))
        if n_calibration < 1 or n_calibration >= shuffled.shape[0]:
            raise ValueError(
                "A split-conformal calibration needs both halves non-empty: "
                f"{shuffled.shape[0]} rows and calibration_fraction="
                f"{self.calibration_fraction} leave {n_calibration} calibration "
                "rows. Adjust `calibration_fraction`, pass more rows, or use "
                "conformal='fit'."
            )
        return shuffled[n_calibration:], shuffled[:n_calibration]

    # Direction de vulnérabilité associée à un quantile marginal
    def _pole_direction(self, X: np.ndarray, quantile: float) -> np.ndarray:
        """Normalise the image of the marginal-quantile pole.

        The pole ``x⁺`` is a corner: it lies inside the marginal bounding box
        but usually outside the convex hull of the cloud. No clipping is
        applied — the dual entropic map is defined on the whole space and its
        image is a convex combination of grid points, hence inside the unit
        ball. Only the degenerate case (pole transported onto the centre, no
        direction left) is rejected.

        Args:
            X: Fitting sample of shape ``(n_fit, d)``.
            quantile: Marginal quantile defining the pole.

        Returns:
            Unit vector of shape ``(d,)``.

        Raises:
            ValueError: If the pole is transported onto the centre.
        """
        jnp, *_ = _require_ott()
        pole = np.quantile(X, quantile, axis=0)
        image = np.asarray(self._transport(jnp.asarray(pole[None, :])))[0]
        norm = float(np.linalg.norm(image))
        if norm <= _NORM_TOLERANCE:
            raise ValueError(
                f"The pole at quantile {quantile} is transported onto the centre "
                "of the ball: no direction to orient the score on. `epsilon` is "
                "probably too large, or the metric cloud is degenerate."
            )
        return image / norm

    # Score de non-conformité de la calibration conforme
    def _nonconformity(self, X: np.ndarray) -> np.ndarray:
        """Compute the non-conformity score selected by ``alert_score``.

        Args:
            X: Calibration sample of shape ``(n_cal, d)``.

        Returns:
            Non-conformity vector of shape ``(n_cal,)``.
        """
        transported = self.transport(X)
        if self.alert_score == "radial":
            return np.linalg.norm(transported, axis=1)
        return self._score(transported, "projection")

    # Seuil conforme ou quantile empirique descriptif
    def _calibrate(self, nonconformity: np.ndarray) -> float:
        """Turn the calibration scores into an alert threshold.

        Under ``conformal="split"``, the threshold is the
        ``⌈(1-α)(n_cal+1)⌉``-th smallest non-conformity score, the
        finite-sample conformal quantile (M-07). Under ``conformal="fit"``, it
        is the plain empirical quantile of the fitting sample — descriptive,
        with no coverage guarantee.

        Args:
            nonconformity: Non-conformity scores of the calibration sample.

        Returns:
            The threshold; ``inf`` when ``α`` is too small for the calibration
            sample to resolve it (no row is ever flagged).
        """
        if self.conformal == "fit":
            return float(np.quantile(nonconformity, 1.0 - self.alpha))

        n_calibration = nonconformity.shape[0]
        rank = math.ceil((1.0 - self.alpha) * (n_calibration + 1))
        if rank > n_calibration:
            warnings.warn(
                f"A split-conformal threshold at alpha={self.alpha} needs at "
                f"least {math.ceil(1.0 / self.alpha) - 1} calibration rows, "
                f"{n_calibration} available: the threshold is infinite and no "
                "row is flagged. Raise `alpha`, `fit_sample_size` or "
                "`calibration_fraction`.",
                stacklevel=2,
            )
            return float("inf")
        return float(np.sort(nonconformity)[rank - 1])

    # Variantes de score orienté, à partir des points transportés
    def _score(self, transported: np.ndarray, variant: str) -> np.ndarray:
        """Apply one oriented score variant to already transported points.

        Args:
            transported: Transported points of shape ``(n, d)``.
            variant: One of :data:`_SCORE_VARIANTS`.

        Returns:
            Score vector of shape ``(n,)``.
        """
        if variant == "projection":
            return transported @ self.direction_

        norms = np.linalg.norm(transported, axis=1)
        alignment = _unit_vectors(transported) @ self.direction_
        if variant == "cone":
            return norms * (alignment >= self._cos_theta0())
        return norms * (1.0 + alignment) / 2.0

    # Cosinus du demi-angle du cône de vulnérabilité
    def _cos_theta0(self) -> float:
        """Return ``cos θ₀``, the cone membership threshold on ``⟨S(x), u*⟩``.

        Returns:
            The cosine of ``theta0_degrees``.
        """
        return math.cos(math.radians(self.theta0_degrees))


# Fonction de normalisation ligne à ligne, robuste au vecteur nul
def _unit_vectors(points: np.ndarray) -> np.ndarray:
    """Normalise every row, leaving the (undefined) zero rows at zero.

    Args:
        points: Array of shape ``(n, d)``.

    Returns:
        Array of shape ``(n, d)`` of unit rows, zero where the input row was
        the centre.

    Examples:
        >>> import numpy as np
        >>> _unit_vectors(np.array([[3.0, 4.0], [0.0, 0.0]]))
        array([[0.6, 0.8],
               [0. , 0. ]])
    """
    norms = np.linalg.norm(points, axis=1, keepdims=True)
    return np.divide(
        points, norms, out=np.zeros_like(points), where=norms > _NORM_TOLERANCE
    )


# ──────────────────────────────────────────────────────────────────────
# Diagnostic d'ellipticité : le transport a-t-il apporté quelque chose ?
# ──────────────────────────────────────────────────────────────────────

# Fonction de comparaison du transport à ses contreparties linéaires
def ellipticity_screen(
    X: np.ndarray,
    scorer: OrientedKantorovichScorer,
    *,
    covariance_estimator: str = "mcd",
) -> Dict[str, float]:
    """Compare the transport-based score and rank with their linear counterparts.

    For an elliptical law of centre ``μ`` and dispersion ``Σ``, the Brenier map
    onto the spherical uniform is ``T(z) = h(r)·Σ^{-1/2}(z-μ)/r`` with
    ``r = ‖Σ^{-1/2}(z-μ)‖`` and ``h`` increasing. So the **rank** ``‖T(z)‖`` is
    a monotone transformation of the Mahalanobis distance **to the centre**,
    and the **oriented score** is the whitened projection modulated radially.
    Neither reduces to the Mahalanobis distance to the anti-ideal pole, which
    is what the previous implementation compared them to: two non-comparable
    objects, whose ``τ`` could be low on a perfectly elliptical cloud (M-06).

    The two ``τ_b`` are a **diagnostic, not a gate** (D-07): they say whether
    the transport added anything to its linear counterpart on this group
    (``τ`` close to ``1``: it did not), they do not decide whether the score is
    computed.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity — the matrix
            ``scorer`` was fitted on, or a comparable one.
        scorer: A fitted :class:`OrientedKantorovichScorer`.
        covariance_estimator: ``"mcd"``, ``"ledoit_wolf"`` or ``"empirical"``,
            forwarded to both linear references — see
            :func:`~macroforecast.trade.aggregation.functions.mahalanobis_score`.

    Returns:
        Mapping with two entries: ``"tau_proj"``, Kendall's ``τ_b`` between the
        oriented score and
        :func:`~macroforecast.trade.aggregation.functions.whitened_projection_score`,
        and ``"tau_rank"``, ``τ_b`` between the center-outward rank and the
        Mahalanobis distance to the robust centre.

    Raises:
        ValueError: If ``covariance_estimator`` is unknown.

    Examples:
        >>> screen = ellipticity_screen(X, scorer)  # doctest: +SKIP
        >>> sorted(screen)  # doctest: +SKIP
        ['tau_proj', 'tau_rank']
    """
    X = check_array(X)
    linear_projection = whitened_projection_score(
        X, covariance_estimator=covariance_estimator
    )
    linear_rank = mahalanobis_score(
        X, covariance_estimator=covariance_estimator, center="robust_center"
    )
    tau_proj, _ = stats.kendalltau(scorer.predict(X), linear_projection)
    tau_rank, _ = stats.kendalltau(scorer.ranks(X), linear_rank)
    return {"tau_proj": float(tau_proj), "tau_rank": float(tau_rank)}
