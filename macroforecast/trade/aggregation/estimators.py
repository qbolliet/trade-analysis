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

Three estimators live here, matching the three kinds of weighting of
:mod:`~macroforecast.trade.aggregation.weights` (M-05):

* :class:`WeightedAggregator` — shared **simplicial** weights plus an
  aggregation function. ``fit`` now checks that the weights really are a point
  of the simplex whenever the aggregation requires it, so the first-axis PCA
  *direction* (the ``"pca"`` key) is rejected here rather than silently fed to
  TOPSIS or to a geometric mean (I-05).
* :class:`PcaProjectionScorer` — the **direction**, applied to internally
  standardised data.
* :class:`BenefitOfDoubtScorer` — the **individual** weightings, with the
  ``fit``/``predict`` separation the bare function cannot offer (I-04).

Two more estimators refuse to pick a weighting at all and explore the whole
simplex instead, drawing it once at ``fit`` time so that ``predict`` stays
reproducible: :class:`ConeQuantileScorer` (worst-case one-dimensional rank,
A-01) and :class:`SmaaScorer` (share of admissible weightings placing the
product in the top ``k``).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Callable, Dict, Optional, Tuple
# Modules de manipulation de données
import numpy as np
from sklearn.base import BaseEstimator
from sklearn.utils.validation import check_array, check_is_fitted
# Modules du package
from . import functions, weights
from .diagnostics import smaa_rank_acceptability
from .pareto import ParetoScorer, dominance_count, pareto_front
from .weights import dirichlet_weights

# Registre des pondérations, point d'entrée piloté par configuration
WEIGHTING_REGISTRY: Dict[str, Callable[..., Any]] = {
    "entropy": weights.entropy_weights,
    "critic": weights.critic_weights,
    # Direction de l'axe 1 (renvoie un couple) : hors du simplexe, réservée à
    # `PcaProjectionScorer` et refusée par `WeightedAggregator`
    "pca": weights.pca_weights,
    # Pondération OCDE-JRC (renvoie un couple) : vecteur du simplexe
    "pca_oecd": lambda X, **kwargs: weights.pca_weights(X, rotate=True, **kwargs),
    # Méta-sélection S-1.5 (renvoie un couple `(w, AutoWeightingReport)`)
    "auto": weights.auto_weights,
    "equal": lambda X: np.full(X.shape[1], 1.0 / X.shape[1]),
}

# Agrégations exigeant un vecteur du simplexe (poids >= 0 de somme 1, M-05)
_SIMPLEX_AGGREGATIONS = frozenset(
    {"weighted_sum", "geometric_mean", "rank_mean", "topsis", "vikor"}
)

# Tolérance de la vérification de somme unitaire des poids
_SIMPLEX_TOLERANCE = 1e-8

# Registre des fonctions d'agrégation, point d'entrée piloté par configuration
AGGREGATION_REGISTRY: Dict[str, Callable[..., np.ndarray]] = {
    "weighted_sum": functions.weighted_sum_score,
    "geometric_mean": functions.geometric_mean_score,
    "rank_mean": functions.rank_mean_score,
    "mpi": functions.mpi_score,
    "topsis": functions.topsis_score,
    "vikor": functions.vikor_score,
    "mahalanobis": functions.mahalanobis_score,
    "whitened_projection": functions.whitened_projection_score,
}

# Fonctions d'agrégation n'exigeant aucun poids (score déjà défini sans `weighting`)
_WEIGHT_FREE_AGGREGATIONS = frozenset(
    {"mpi", "mahalanobis", "whitened_projection"}
)


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
        if self.weighting == "pca":
            raise ValueError(
                "The 'pca' weighting returns a *direction* (unit-norm first-axis "
                "loadings, possibly negative), not a point of the simplex: it "
                "defines a linear score on standardised data and cannot be fed to "
                "an aggregation function. Use PcaProjectionScorer for the "
                "projection, or the 'pca_oecd' weighting for the simplicial "
                "OECD/JRC weights."
            )
        params = self.weighting_params or {}
        result = WEIGHTING_REGISTRY[self.weighting](X, **params)
        # L'ACP et la méta-sélection renvoient (poids, rapport) ; les autres
        # schémas renvoient le poids seul
        if isinstance(result, tuple):
            self.weights_, self.weighting_report_ = result[0], result[1]
        else:
            self.weights_, self.weighting_report_ = result, None

        # Vérification du simplexe : une somme pondérée, une moyenne géométrique
        # ou un TOPSIS n'ont aucun sens avec un poids négatif ou de somme != 1
        if self.aggregation in _SIMPLEX_AGGREGATIONS:
            self._check_simplex(self.weights_)
        return self

    # Vérification de l'appartenance des poids au simplexe
    @staticmethod
    def _check_simplex(weight: np.ndarray) -> None:
        """Check that ``weight`` is a point of the simplex.

        Args:
            weight: Candidate weight vector of shape ``(d,)``.

        Raises:
            ValueError: If a component is negative or ``NaN``, or if the sum
                departs from 1 by more than ``1e-8``.
        """
        weight = np.asarray(weight, dtype=float)
        if np.any(~np.isfinite(weight)):
            raise ValueError(
                "The fitted weights hold a non-finite value; the aggregation "
                "functions of this group require a point of the simplex."
            )
        if np.any(weight < 0.0):
            raise ValueError(
                f"The fitted weights hold a negative component (min = "
                f"{float(weight.min()):.6g}); the aggregations "
                f"{sorted(_SIMPLEX_AGGREGATIONS)} require non-negative weights "
                "summing to 1 (M-05)."
            )
        total = float(weight.sum())
        if abs(total - 1.0) > _SIMPLEX_TOLERANCE:
            raise ValueError(
                f"The fitted weights sum to {total:.12g} instead of 1 (tolerance "
                f"{_SIMPLEX_TOLERANCE:g}); the aggregations "
                f"{sorted(_SIMPLEX_AGGREGATIONS)} require a point of the simplex "
                "(M-05)."
            )

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


# Estimateur sklearn de projection sur l'axe 1 de l'ACP
class PcaProjectionScorer(BaseEstimator):
    """Score by projection on the first principal axis (a *direction*, M-05).

    The first-axis loadings are not a point of the simplex: they may be
    negative, and the score they define is a linear form on **standardised**
    data. Handing them to :class:`WeightedAggregator` would therefore be
    wrong twice over -- the aggregation would apply them to whatever
    normalisation the pipeline produced, and a geometric mean or TOPSIS would
    silently accept negative weights (I-05). This estimator closes both
    holes: it standardises internally at ``fit`` time and reuses the fitted
    centre and scale at ``predict`` time.

    Args:
        sign: Convention fixing the direction's sign -- ``"positive_sum"``
            (default) flips it so the loadings sum positive, the orientation
            under which "high = more vulnerable" holds for a dominant common
            factor. ``"none"`` leaves the sign returned by the
            diagonalisation.

    Attributes:
        mean_: Per-column mean fitted on the training matrix, shape ``(d,)``.
        scale_: Per-column standard deviation (floored at ``1`` on a constant
            column), shape ``(d,)``.
        direction_: Unit-norm loading vector, shape ``(d,)``.
        report_: The :class:`~macroforecast.trade.aggregation.weights.PcaWeightingReport`
            of the fit -- in particular ``has_negative_loadings``, which flags
            a violation of monotonicity.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> base = rng.normal(size=200)
        >>> X = np.column_stack([base + rng.normal(scale=0.2, size=200) for _ in range(3)])
        >>> scorer = PcaProjectionScorer().fit(X)
        >>> scorer.predict(X).shape
        (200,)
        >>> bool(scorer.report_.variance_share_axis1 > 0.5)
        True
    """

    # Initialisation
    def __init__(self, sign: str = "positive_sum") -> None:
        self.sign = sign

    # Ajustement : standardisation interne puis direction de l'axe 1
    def fit(self, X: np.ndarray, y: None = None) -> "PcaProjectionScorer":
        """Fit the standardisation and the first-axis direction.

        Args:
            X: Metric matrix of shape ``(n, d)``, oriented in positive
                polarity; any normalisation (the standardisation is applied
                here).
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``.

        Raises:
            ValueError: If ``sign`` is unknown.
        """
        if self.sign not in ("positive_sum", "none"):
            raise ValueError(
                f"Unknown sign convention {self.sign!r}. "
                "Available: 'positive_sum', 'none'."
            )
        X = check_array(X)
        self.n_features_in_ = X.shape[1]
        # Standardisation memorisee : la projection doit rester comparable
        # d'un appel de `predict` a l'autre
        self.mean_ = X.mean(axis=0)
        spread = X.std(axis=0)
        self.scale_ = np.where(spread > 0, spread, 1.0)

        direction, report = weights.pca_weights((X - self.mean_) / self.scale_)
        if self.sign == "none" and direction.sum() < 0:
            # `pca_weights` fixe deja le signe : convention "none" = brut
            direction = -direction
        self.direction_ = direction
        self.report_ = report
        return self

    # Prediction : projection des donnees standardisees sur la direction
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Project the standardised matrix on the fitted direction.

        Args:
            X: Metric matrix of shape ``(n, d)``, same preprocessing as at
                fit time.

        Returns:
            Score vector of shape ``(n,)``, centred on the training mean.
        """
        check_is_fitted(self, "direction_")
        X = check_array(X)
        return ((X - self.mean_) / self.scale_) @ self.direction_

    # Alias sklearn conventionnel : ajustement puis prediction
    def fit_predict(self, X: np.ndarray, y: None = None) -> np.ndarray:
        """Fit on ``X`` then predict on the same matrix.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            Score vector of shape ``(n,)``.
        """
        return self.fit(X, y).predict(X)


# Estimateur sklearn du benefit of the doubt
class BenefitOfDoubtScorer(BaseEstimator):
    """Score every product under its own most favourable weighting (BoD).

    ``fit`` memorises the rows that carry the non-domination constraints (the
    exact Pareto front of the training matrix, D-05); ``predict`` solves one
    linear programme per row **against those fixed constraints**, so a
    product can be scored outside the sample that defined the frontier -- the
    ``fit``/``predict`` separation the bare
    :func:`~macroforecast.trade.aggregation.weights.benefit_of_doubt_weights`
    function does not offer (I-04).

    Args:
        restriction: ``"assurance_region"`` (default), ``"shares"`` or
            ``None`` -- see
            :func:`~macroforecast.trade.aggregation.weights.benefit_of_doubt_weights`
            for the exact meaning and the properties each one buys.
        rho: Assurance-region ratio bound, ``>= 1``; ``None`` removes it.
        kappa: Share-restriction parameter in ``(0, 1]``.
        delta: Floor added to ``X`` in the share restrictions, ``> 0``.
        restrict_to_front: Restrict the constraints to the exact Pareto front.
        n_jobs: Parallel workers passed to ``joblib`` when installed.

    Attributes:
        constraint_rows_: Rows carrying the non-domination constraints, shape
            ``(m, d)``.
        weights_: Individual weight vectors of the last ``predict`` call,
            shape ``(n, d)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.0, 1.0, 1.0], [0.5, 0.5, 0.5], [1.0, 0.2, 0.3]])
        >>> scorer = BenefitOfDoubtScorer().fit(X)
        >>> scores = scorer.predict(X)
        >>> bool(scores[0] > 0.5)
        True
        >>> scorer.weights_.shape
        (3, 3)
    """

    # Initialisation
    def __init__(
        self,
        restriction: Optional[str] = "assurance_region",
        rho: Optional[float] = 4.0,
        kappa: float = 0.5,
        delta: float = 1e-3,
        restrict_to_front: bool = True,
        n_jobs: Optional[int] = None,
    ) -> None:
        self.restriction = restriction
        self.rho = rho
        self.kappa = kappa
        self.delta = delta
        self.restrict_to_front = restrict_to_front
        self.n_jobs = n_jobs

    # Ajustement : memorisation des lignes portant les contraintes
    def fit(self, X: np.ndarray, y: None = None) -> "BenefitOfDoubtScorer":
        """Memorise the non-domination constraints of the training matrix.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity and
                non-negative (min-max or rank normalisation).
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``.

        Raises:
            ValueError: If ``X`` holds a negative value.
        """
        X = check_array(X)
        if np.any(X < 0):
            raise ValueError(
                "BenefitOfDoubtScorer requires a non-negative matrix "
                "(apply a min-max or rank normalisation first)."
            )
        self.n_features_in_ = X.shape[1]
        self.constraint_rows_ = X[pareto_front(X)] if self.restrict_to_front else X
        return self

    # Prediction : un programme lineaire par ligne, contre les contraintes ajustees
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Score every row against the fitted constraints.

        Args:
            X: Metric matrix of shape ``(n, d)``, same preprocessing as at
                fit time.

        Returns:
            Score vector of shape ``(n,)`` in ``[0, 1]`` (``NaN`` where the
            programme is infeasible); ``weights_`` holds the matching
            individual weight vectors.

        Raises:
            ValueError: If ``X`` holds a negative value.
        """
        check_is_fitted(self, "constraint_rows_")
        X = check_array(X)
        if np.any(X < 0):
            raise ValueError(
                "BenefitOfDoubtScorer requires a non-negative matrix "
                "(apply a min-max or rank normalisation first)."
            )

        def solve(index: int):
            x_o = X[index]
            rows, bounds = weights._restriction_rows(
                x_o,
                restriction=self.restriction,
                rho=self.rho,
                kappa=self.kappa,
                delta=self.delta,
            )
            return weights._solve_bod(x_o, self.constraint_rows_, rows, bounds)

        n = X.shape[0]
        if self.n_jobs is not None and self.n_jobs != 1:
            try:
                # Importation paresseuse : joblib reste facultatif
                from joblib import Parallel, delayed
            except ImportError:
                results = [solve(index) for index in range(n)]
            else:
                results = list(
                    Parallel(n_jobs=self.n_jobs)(
                        delayed(solve)(index) for index in range(n)
                    )
                )
        else:
            results = [solve(index) for index in range(n)]

        self.weights_ = np.array(
            [weight for _, weight in results], dtype=float
        ).reshape(n, X.shape[1])
        return np.array([score for score, _ in results], dtype=float)

    # Alias sklearn conventionnel : ajustement puis prediction
    def fit_predict(self, X: np.ndarray, y: None = None) -> np.ndarray:
        """Fit on ``X`` then predict on the same matrix.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            Score vector of shape ``(n,)``.
        """
        return self.fit(X, y).predict(X)


# Estimateur sklearn du quantile de cône (Hamel & Kostner)
class ConeQuantileScorer(BaseEstimator):
    """Score by the empirical cone distribution function (A-01).

    The weight-free, fully non-compensatory counterpart of
    :class:`WeightedAggregator`: instead of committing to one weight vector,
    every product is judged under the admissible weighting *least* favourable
    to it (``bound="lower"``, the cone distribution function of Hamel &
    Kostner 2018) or the most favourable one (``bound="upper"``). The gap
    between the two bounds — available through
    :meth:`bounds` — measures the per-product indeterminacy left by the
    refusal to rank the criteria.

    The simplex sample is drawn **once**, at ``fit`` time, and reused by
    ``predict``: two calls on the same matrix therefore return the same
    scores, and a product scored out of sample is judged against the very
    weightings that produced the training scores.

    Args:
        n_draws: Number of Dirichlet draws sampling the simplex.
        random_state: Seed of the weight sampler.
        bound: ``"lower"`` (default) or ``"upper"``.
        block_size: Number of draws scored at once in ``predict``.

    Attributes:
        weight_draws_: Simplex sample of shape ``(n_draws, d)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[1.0, 1.0], [0.5, 0.5], [0.0, 0.0], [1.0, 0.0]])
        >>> scorer = ConeQuantileScorer(n_draws=200).fit(X)
        >>> scores = scorer.predict(X)
        >>> bool(scores[0] == 1.0 and scores[2] == 0.25)
        True
        >>> lower, upper = scorer.bounds(X)
        >>> bool(np.all(lower <= upper))
        True
    """

    # Initialisation
    def __init__(
        self,
        n_draws: int = 2000,
        random_state: Optional[int] = 0,
        bound: str = "lower",
        block_size: int = 256,
    ) -> None:
        self.n_draws = n_draws
        self.random_state = random_state
        self.bound = bound
        self.block_size = block_size

    # Ajustement : tirage du simplexe, réutilisé par `predict`
    def fit(self, X: np.ndarray, y: None = None) -> "ConeQuantileScorer":
        """Draw the simplex sample the scores will be computed against.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity; the
                score being rank-based, no normalisation is required.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``, with ``weight_draws_`` fitted.

        Raises:
            ValueError: If ``bound`` is unknown.
        """
        if self.bound not in ("lower", "upper"):
            raise ValueError(
                f"Unknown bound {self.bound!r}. Available: 'lower', 'upper'."
            )
        X = check_array(X)
        self.n_features_in_ = X.shape[1]
        self.weight_draws_ = dirichlet_weights(
            X.shape[1], self.n_draws, random_state=self.random_state
        )
        return self

    # Prédiction : pire (ou meilleur) rang unidimensionnel sur les tirages
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Score every row by the fitted bound of the cone quantile.

        Args:
            X: Metric matrix of shape ``(n, d)``, same preprocessing as at
                fit time.

        Returns:
            Score vector of shape ``(n,)``, in ``(0, 1]``.
        """
        check_is_fitted(self, "weight_draws_")
        return functions.cone_quantile_score(
            check_array(X),
            self.weight_draws_,
            bound=self.bound,
            block_size=self.block_size,
        )

    # Bornes inférieure et supérieure : indétermination par produit
    def bounds(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Return both bounds of the cone quantile.

        Args:
            X: Metric matrix of shape ``(n, d)``.

        Returns:
            Tuple ``(lower, upper)`` of two vectors of shape ``(n,)``; their
            difference is the indeterminacy of the product's position.
        """
        check_is_fitted(self, "weight_draws_")
        return functions.cone_quantile_bounds(
            check_array(X), self.weight_draws_, block_size=self.block_size
        )

    # Alias sklearn conventionnel : ajustement puis prédiction
    def fit_predict(self, X: np.ndarray, y: None = None) -> np.ndarray:
        """Fit on ``X`` then predict on the same matrix.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            Score vector of shape ``(n,)``.
        """
        return self.fit(X, y).predict(X)


# Estimateur sklearn du protocole SMAA
class SmaaScorer(BaseEstimator):
    """Score by the SMAA confidence factor: "in the top ``k`` how often?".

    Turns the robustness protocol of
    :func:`~macroforecast.trade.aggregation.diagnostics.smaa_rank_acceptability`
    into a method in its own right: the score of a product is the share of
    admissible weightings under which it ranks within the top ``k`` — the
    statement fit for an administrative report, and a genuine
    "higher = more vulnerable" score (D-16).

    As for :class:`ConeQuantileScorer`, the simplex sample is drawn once at
    ``fit`` time and reused by ``predict``. ``predict`` stores the full
    :class:`~macroforecast.trade.aggregation.diagnostics.SmaaResult` of the
    call in ``result_``, so the rank acceptabilities and the central weights
    stay available for the report.

    Args:
        aggregation: Name of a weight-taking function in
            :data:`AGGREGATION_REGISTRY`.
        aggregation_params: Extra keyword arguments forwarded to it.
        k: Rank depth defining the confidence factor.
        n_draws: Number of Dirichlet draws.
        random_state: Seed of the weight sampler.

    Attributes:
        weight_draws_: Simplex sample of shape ``(n_draws, d)``.
        result_: :class:`~macroforecast.trade.aggregation.diagnostics.SmaaResult`
            of the last ``predict`` call.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[0.9, 0.8], [0.5, 0.5], [0.1, 0.2]])
        >>> scorer = SmaaScorer(k=1, n_draws=200)
        >>> scores = scorer.fit_predict(X)
        >>> bool(scores[0] == 1.0 and scores[2] == 0.0)
        True
        >>> scorer.result_.central_weight.shape
        (3, 2)
    """

    # Initialisation
    def __init__(
        self,
        aggregation: str = "weighted_sum",
        aggregation_params: Optional[Dict[str, Any]] = None,
        k: int = 50,
        n_draws: int = 2000,
        random_state: Optional[int] = 0,
    ) -> None:
        self.aggregation = aggregation
        self.aggregation_params = aggregation_params
        self.k = k
        self.n_draws = n_draws
        self.random_state = random_state

    # Ajustement : tirage du simplexe, réutilisé par `predict`
    def fit(self, X: np.ndarray, y: None = None) -> "SmaaScorer":
        """Draw the simplex sample the exploration will run on.

        Args:
            X: Metric matrix of shape ``(n, d)``, already oriented,
                winsorised and normalised.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``, with ``weight_draws_`` fitted.

        Raises:
            ValueError: If ``aggregation`` names an unknown function, or one
                that ignores the weights (exploring the simplex would then
                return a constant).
        """
        if self.aggregation not in AGGREGATION_REGISTRY:
            raise ValueError(
                f"Unknown aggregation {self.aggregation!r}. "
                f"Available: {sorted(AGGREGATION_REGISTRY)}."
            )
        if self.aggregation in _WEIGHT_FREE_AGGREGATIONS:
            raise ValueError(
                f"The aggregation {self.aggregation!r} ignores its weights: "
                "exploring the weight simplex would leave the ranking "
                "unchanged. Pick a weight-taking aggregation "
                f"({sorted(set(AGGREGATION_REGISTRY) - _WEIGHT_FREE_AGGREGATIONS)})."
            )
        X = check_array(X)
        self.n_features_in_ = X.shape[1]
        self.weight_draws_ = dirichlet_weights(
            X.shape[1], self.n_draws, random_state=self.random_state
        )
        return self

    # Prédiction : facteur de confiance sur les tirages ajustés
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Compute the confidence factor of every row.

        Args:
            X: Metric matrix of shape ``(n, d)``, same preprocessing as at
                fit time.

        Returns:
            Score vector of shape ``(n,)``, in ``[0, 1]``; ``result_`` holds
            the full :class:`SmaaResult` of the call.
        """
        check_is_fitted(self, "weight_draws_")
        X = check_array(X)
        self.result_ = smaa_rank_acceptability(
            X,
            AGGREGATION_REGISTRY[self.aggregation],
            aggregation_params=self.aggregation_params,
            k=self.k,
            weight_draws=self.weight_draws_,
        )
        return self.result_.confidence_factor

    # Alias sklearn conventionnel : ajustement puis prédiction
    def fit_predict(self, X: np.ndarray, y: None = None) -> np.ndarray:
        """Fit on ``X`` then predict on the same matrix.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            Score vector of shape ``(n,)``.
        """
        return self.fit(X, y).predict(X)
