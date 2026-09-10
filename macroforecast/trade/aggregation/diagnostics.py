"""Comparison and robustness protocol, and consensus rankings.

Implements §6 of the methodological note: nothing guarantees that the
scores produced by different weighting/aggregation choices agree, and that
disagreement carries the most useful information — it delimits what the
result owes to the data from what it owes to the methodological choice. Five
questions structure the protocol:

* Q1 — global concordance between rankings (:func:`kendall_tau_b_matrix`,
  :func:`kendall_w`, :func:`cluster_methods`);
* Q2 — concordance at the top of the ranking (:func:`rank_biased_overlap`,
  :func:`topk_overlap`, :func:`weighted_tau_matrix`);
* Q3 — coherence with Pareto dominance, the one hard test
  (:func:`dominance_violation_rate`, :func:`front_rank_summary`);
* Q4 — stability under resampling (:func:`bootstrap_rank_stability`,
  :func:`smaa_rank_acceptability`);
* Q5 — is the score reducible to a single metric?
  (:func:`leave_one_metric_out`).

:func:`compute_coherence_report` assembles the whole dashboard (algorithm 6
of the note) into a :class:`CoherenceReport`, alongside the three
weight-independent consensus rankings of §6.6 (:func:`borda_rank`,
:func:`copeland_rank`, :func:`kemeny_rank`).

Every ranking here follows the "rank 1 = most vulnerable" convention — the
opposite of the "higher score = more vulnerable" convention used by
:mod:`~macroforecast.trade.aggregation.functions`, since a *rank* is exactly
what this module's consensus functions are asked to produce.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from collections import defaultdict
from dataclasses import dataclass, field, replace
from itertools import combinations
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
# Modules de manipulation de données
import numpy as np
import pandas as pd
from scipy import sparse, stats
from scipy.cluster.hierarchy import linkage
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.spatial.distance import squareform
# Modules du package
from .base import AggregationConfig, split_frame
from .pareto import pareto_dominance_matrix, pareto_front_sweep
from .weights import dirichlet_weights


# ──────────────────────────────────────────────────────────────────────
# Q1 — Concordance globale
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul de la matrice des tau_b de Kendall entre méthodes
def kendall_tau_b_matrix(scores_by_method: Mapping[str, np.ndarray]) -> pd.DataFrame:
    """Compute the pairwise Kendall τ_b between every pair of methods.

    Preferred over Spearman's ρ for this diagnostic: it reads directly as a
    share of concordant pairs, handles ties, and is less sensitive to shifts
    among median ranks — which matter little for a vulnerability ranking.

    Args:
        scores_by_method: Mapping of method name to its score vector, every
            vector of the same length and in the same row order.

    Returns:
        Symmetric ``DataFrame`` of shape ``(K, K)``, indexed and columned by
        method name, diagonal 1.

    Examples:
        >>> import numpy as np
        >>> scores = {"a": np.array([3.0, 2.0, 1.0]), "b": np.array([3.0, 1.0, 2.0])}
        >>> matrix = kendall_tau_b_matrix(scores)
        >>> round(float(matrix.loc["a", "b"]), 3)
        0.333
    """
    names = list(scores_by_method)
    n_methods = len(names)
    matrix = np.eye(n_methods)
    for row, col in combinations(range(n_methods), 2):
        tau, _ = stats.kendalltau(
            scores_by_method[names[row]], scores_by_method[names[col]]
        )
        matrix[row, col] = matrix[col, row] = tau
    return pd.DataFrame(matrix, index=names, columns=names)


# Fonction de calcul de la matrice des tau de Kendall pondérés entre méthodes
def weighted_tau_matrix(
    scores_by_method: Mapping[str, np.ndarray], *, rank: bool = True
) -> pd.DataFrame:
    """Compute the pairwise weighted Kendall τ between every pair of methods.

    Vigna's (2015) weighted τ gives a concordant or discordant pair a
    hyperbolic weight in the ranks of its two elements, so a disagreement
    among the most vulnerable products weighs far more than one deep in the
    tail — the same concern as the RBO, but symmetric and free of any depth
    parameter (A-06). Reported alongside :func:`kendall_tau_b_matrix`, whose
    uniform weighting answers the different, global question.

    Args:
        scores_by_method: Mapping of method name to its score vector, every
            vector of the same length and in the same row order, higher
            meaning more vulnerable.
        rank: Forwarded to :func:`scipy.stats.weightedtau`; ``True`` averages
            the coefficient over the two rankings induced by the two score
            vectors, which is what makes the result symmetric.

    Returns:
        Symmetric ``DataFrame`` of shape ``(K, K)``, indexed and columned by
        method name, diagonal 1.

    Examples:
        >>> import numpy as np
        >>> scores = {"a": np.array([3.0, 2.0, 1.0]), "b": np.array([3.0, 1.0, 2.0])}
        >>> matrix = weighted_tau_matrix(scores)
        >>> bool(matrix.loc["a", "b"] < 1.0)
        True
        >>> float(matrix.loc["a", "a"])
        1.0
    """
    names = list(scores_by_method)
    n_methods = len(names)
    matrix = np.eye(n_methods)
    for row, col in combinations(range(n_methods), 2):
        tau, _ = stats.weightedtau(
            np.asarray(scores_by_method[names[row]], dtype=float),
            np.asarray(scores_by_method[names[col]], dtype=float),
            rank=rank,
        )
        matrix[row, col] = matrix[col, row] = tau
    return pd.DataFrame(matrix, index=names, columns=names)


# Fonction de calcul du coefficient de concordance W de Kendall
def kendall_w(scores_by_method: Mapping[str, np.ndarray]) -> float:
    """Compute Kendall's coefficient of concordance ``W`` across ``K`` rankings.

    ``W = 1`` for perfect agreement; a low ``W`` means no single ranking
    should be published without an explicit statement of its uncertainty.

    Args:
        scores_by_method: Mapping of method name to its score vector.

    Returns:
        ``W`` in ``[0, 1]``.

    Examples:
        >>> import numpy as np
        >>> scores = {"a": np.array([3.0, 2.0, 1.0]), "b": np.array([3.0, 2.0, 1.0])}
        >>> kendall_w(scores)
        1.0
    """
    ranks = np.column_stack(
        [stats.rankdata(-s, method="average") for s in scores_by_method.values()]
    )
    n, n_methods = ranks.shape
    rank_sums = ranks.sum(axis=1)
    mean_sum = n_methods * (n + 1) / 2
    ss = np.sum((rank_sums - mean_sum) ** 2)
    denominator = n_methods**2 * (n**3 - n)
    if denominator == 0:
        return float("nan")
    return float(12 * ss / denominator)


# Fonction de classification ascendante hiérarchique des méthodes
def cluster_methods(tau_matrix: pd.DataFrame, method: str = "average") -> np.ndarray:
    """Hierarchically cluster methods on ``1 - τ_b`` dissimilarity.

    The resulting groups generally line up with the major methodological
    choices (compensatory or not, decorrelated or not) rather than with
    superficial naming differences.

    Args:
        tau_matrix: Output of :func:`kendall_tau_b_matrix`.
        method: Linkage method forwarded to
            :func:`scipy.cluster.hierarchy.linkage`.

    Returns:
        The SciPy linkage matrix, orderable with
        :func:`scipy.cluster.hierarchy.fcluster` or plottable with
        :func:`scipy.cluster.hierarchy.dendrogram`.

    Examples:
        >>> import numpy as np
        >>> scores = {
        ...     "a": np.array([4.0, 3.0, 2.0, 1.0]),
        ...     "b": np.array([4.0, 3.0, 2.0, 1.0]),
        ...     "c": np.array([1.0, 2.0, 3.0, 4.0]),
        ... }
        >>> linkage_matrix = cluster_methods(kendall_tau_b_matrix(scores))
        >>> linkage_matrix.shape
        (2, 4)
    """
    distance = 1.0 - tau_matrix.to_numpy()
    np.fill_diagonal(distance, 0.0)
    condensed = squareform(distance, checks=False)
    return linkage(condensed, method=method)


# ──────────────────────────────────────────────────────────────────────
# Q2 — Concordance en tête de classement
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul du recouvrement du top-k
def topk_overlap(ranking_a: Sequence[Any], ranking_b: Sequence[Any], k: int) -> float:
    """Compute the Jaccard overlap of two rankings' top ``k``.

    Args:
        ranking_a: Ids ordered from most to least vulnerable.
        ranking_b: Same, from another method.
        k: Depth compared.

    Returns:
        ``|top_k(A) ∩ top_k(B)| / k``.

    Examples:
        >>> topk_overlap(["a", "b", "c"], ["b", "a", "d"], k=2)
        1.0
    """
    top_a = set(ranking_a[:k])
    top_b = set(ranking_b[:k])
    return len(top_a & top_b) / k


# Fonction de calcul du rank-biased overlap
def rank_biased_overlap(
    ranking_a: Sequence[Any], ranking_b: Sequence[Any], p: float = 0.98
) -> float:
    """Compute the (truncated) rank-biased overlap of two rankings.

    Weights :func:`topk_overlap` geometrically over depth, giving much more
    weight to the first ranks. ``p`` sets the depth of interest: the expected
    rank examined is ``1 / (1 - p)``, so ``p = 0.98`` centres attention on
    the top 50.

    Truncated at ``min(len(ranking_a), len(ranking_b))`` rather than
    extrapolated to infinity — a good approximation whenever that length is
    large relative to ``1 / (1 - p)``, which the geometric weighting makes
    the deep tail contribute negligibly to.

    Args:
        ranking_a: Ids ordered from most to least vulnerable.
        ranking_b: Same, from another method.
        p: Persistence parameter in ``(0, 1)``.

    Returns:
        RBO score in ``[0, 1]``.

    Examples:
        >>> round(rank_biased_overlap(["a", "b", "c"], ["a", "b", "c"], p=0.9), 3)
        0.271
        >>> rank_biased_overlap(["a", "b"], ["b", "a"], p=0.9) < 1.0
        True
    """
    depth = min(len(ranking_a), len(ranking_b))
    seen_a: set = set()
    seen_b: set = set()
    overlap = 0.0
    for position in range(1, depth + 1):
        seen_a.add(ranking_a[position - 1])
        seen_b.add(ranking_b[position - 1])
        agreement = len(seen_a & seen_b) / position
        overlap += (1.0 - p) * p ** (position - 1) * agreement
    return overlap


# ──────────────────────────────────────────────────────────────────────
# Q3 — Cohérence avec la dominance
# ──────────────────────────────────────────────────────────────────────

# Taux de violation de la dominance, inversions strictes et ex æquo distingués
@dataclass(frozen=True)
class ViolationRates:
    """Dominance-violation rates of a score, ties told apart from inversions.

    Counting an equality as an inversion penalises every method producing
    legitimate ties (benefit of the doubt saturating at 1, discrete scores),
    which is why the two are reported separately (M-10): only ``strict > 0``
    invalidates a score on its own, ``tie > 0`` merely measures how much of
    the dominance order the score leaves undecided.

    Attributes:
        strict: Share of dominant pairs ``(i, k)`` — ``i`` dominating ``k`` —
            with ``s_i < s_k``, i.e. genuinely reversed. ``NaN`` when no pair
            is comparable.
        tie: Share of dominant pairs with ``s_i == s_k``. ``NaN`` when no
            pair is comparable.
        n_pairs: Number of dominant pairs the rates were computed on — the
            whole relation for an exact computation, the dominant pairs found
            in the random sample for an estimated one.
    """
    strict: float
    tie: float
    n_pairs: int


# Comptage des violations sur un échantillon aléatoire de paires
def _sampled_violation_counts(
    X: np.ndarray,
    scores: np.ndarray,
    n_pairs_sample: int,
    random_state: Optional[int],
    chunk_size: int = 50_000,
) -> Tuple[int, int, int]:
    """Count dominant pairs and their violations on a random sample of pairs.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        scores: Score vector of shape ``(n,)``.
        n_pairs_sample: Number of ordered pairs drawn uniformly with
            replacement (self-pairs discarded).
        random_state: Seed of the pair sampler.
        chunk_size: Number of pairs materialised at once, bounding the memory
            of the comparison to ``O(chunk_size · d)``.

    Returns:
        Tuple ``(n_dominant, n_strict, n_tie)`` counted over the sample.
    """
    rng = np.random.default_rng(random_state)
    n = X.shape[0]
    n_dominant = n_strict = n_tie = 0
    drawn = 0
    while drawn < n_pairs_sample:
        size = min(chunk_size, n_pairs_sample - drawn)
        drawn += size
        left = rng.integers(0, n, size=size)
        right = rng.integers(0, n, size=size)
        # Elimination des paires dégénérées (un point ne se domine pas lui-même)
        keep = left != right
        left, right = left[keep], right[keep]
        difference = X[left] - X[right]
        dominant = np.all(difference >= 0, axis=1) & np.any(difference > 0, axis=1)
        if not dominant.any():
            continue
        gap = scores[left[dominant]] - scores[right[dominant]]
        n_dominant += int(dominant.sum())
        n_strict += int(np.sum(gap < 0))
        n_tie += int(np.sum(gap == 0))
    return n_dominant, n_strict, n_tie


# Fonction de calcul des taux de violation de la dominance
def dominance_violation_rate(
    X: np.ndarray,
    scores: np.ndarray,
    *,
    large_n_threshold: int = 20_000,
    n_pairs_sample: int = 1_000_000,
    random_state: Optional[int] = None,
) -> ViolationRates:
    """Compute the dominance-violation rates ``V(s)`` of a score.

    The one control resting on no convention, capable of invalidating a
    method on its own: a strict violation reverses a pair on which *every*
    metric agrees. Equalities are reported apart (:class:`ViolationRates`,
    M-10).

    The exact computation materialises the ``(n, n)`` dominance matrix, out
    of reach at the global level (``n ≈ 2·10⁵`` → 40 Gb). Above
    ``large_n_threshold`` rows the rates are therefore *estimated* on
    ``n_pairs_sample`` ordered pairs drawn uniformly with replacement: each
    rate is the ratio of two sample counts, consistent for the corresponding
    population ratio, and its precision is driven by the number of dominant
    pairs actually drawn (``ViolationRates.n_pairs``, which the caller should
    read before trusting a rate — a nearly empty dominance relation yields
    few of them).

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        scores: Score vector of shape ``(n,)``, higher meaning more
            vulnerable.
        large_n_threshold: Number of rows above which the rates are estimated
            on a random sample of pairs rather than computed exactly.
        n_pairs_sample: Number of ordered pairs drawn when sampling.
        random_state: Seed of the pair sampler; ignored below the threshold,
            where the computation is exact and deterministic.

    Returns:
        The :class:`ViolationRates` of the score; both rates ``NaN`` and
        ``n_pairs`` zero when no pair is comparable (an empty dominance
        relation).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0]])
        >>> dominance_violation_rate(X, np.array([1.0, 2.0]))
        ViolationRates(strict=1.0, tie=0.0, n_pairs=1)
        >>> dominance_violation_rate(X, np.array([2.0, 1.0]))
        ViolationRates(strict=0.0, tie=0.0, n_pairs=1)
        >>> dominance_violation_rate(X, np.array([1.0, 1.0]))
        ViolationRates(strict=0.0, tie=1.0, n_pairs=1)
    """
    X = np.asarray(X, dtype=float)
    scores = np.asarray(scores, dtype=float)
    if X.shape[0] > large_n_threshold:
        n_pairs, n_strict, n_tie = _sampled_violation_counts(
            X, scores, n_pairs_sample, random_state
        )
    else:
        dominance = pareto_dominance_matrix(X)
        n_pairs = int(dominance.sum())
        n_strict = int(np.sum(dominance & (scores[:, None] < scores[None, :])))
        n_tie = int(np.sum(dominance & (scores[:, None] == scores[None, :])))
    if n_pairs == 0:
        return ViolationRates(float("nan"), float("nan"), 0)
    return ViolationRates(n_strict / n_pairs, n_tie / n_pairs, n_pairs)


# Fonction de résumé du rang des membres du front de Pareto
def front_rank_summary(
    front_mask: np.ndarray, scores: np.ndarray
) -> Tuple[float, float]:
    """Summarise where a score places the members of the Pareto front.

    The control associated with the dominance check (§7.3 of the note): a
    non-dominated product deserves a high rank, and a front member ranked
    3 000th signals a score whose weighting has neutralised the very metric
    on which that product stands out. Reported rather than enforced — a
    large front (many metrics, few products) mechanically pushes its median
    rank down.

    Args:
        front_mask: Boolean mask of shape ``(n,)`` flagging the non-dominated
            products (:func:`~macroforecast.trade.aggregation.pareto.pareto_front`).
        scores: Score vector of shape ``(n,)``, higher meaning more
            vulnerable.

    Returns:
        Tuple ``(median_rank, max_rank)`` over the front members, ranks
        following the ``1`` = most vulnerable convention; ``(NaN, NaN)`` for
        an empty front.

    Examples:
        >>> import numpy as np
        >>> mask = np.array([True, False, True, False])
        >>> front_rank_summary(mask, np.array([0.9, 0.4, 0.7, 0.1]))
        (1.5, 2.0)
    """
    front_mask = np.asarray(front_mask, dtype=bool)
    scores = np.asarray(scores, dtype=float)
    if not front_mask.any():
        return float("nan"), float("nan")
    ranks = stats.rankdata(-scores, method="average")[front_mask]
    return float(np.median(ranks)), float(ranks.max())


# ──────────────────────────────────────────────────────────────────────
# Q4 — Stabilité
# ──────────────────────────────────────────────────────────────────────

# Fonction de bootstrap de la stabilité des rangs
def bootstrap_rank_stability(
    df_data: pd.DataFrame,
    config: AggregationConfig,
    pipeline_factory: Callable[[AggregationConfig], Any],
    *,
    n_boot: int = 50,
    ci: float = 0.9,
    random_state: Optional[int] = None,
) -> pd.DataFrame:
    """Bootstrap the estimation of a method and rank the whole population.

    Each draw resamples the rows with replacement, **fits** the whole chain
    on the resample — *including the weights*, since that they are estimated
    is precisely the point (§6.4 of the note) — then **scores and ranks the
    original, complete population** (D-08). Ranking inside the resample
    instead, as an earlier version did, produces ranks living on a different
    support from draw to draw and leaves the products absent from a draw
    without any rank at all; here every identifier receives exactly
    ``n_boot`` comparable ranks.

    Only the estimation uncertainty of the method is covered. The uncertainty
    of the metrics themselves (``x_ij`` measured with error) would require
    per-cell standard errors, unavailable at the aggregation step (M-11), and
    is documented as an extension rather than implemented.

    A rank is a statistic of the whole sample, and its interval reads as in
    the *league tables* literature (Goldstein & Spiegelhalter, 1996): a
    product whose 90% interval spans ``[3, 412]`` must never be reported as
    "third most vulnerable". Percentile intervals on a rank are conservative
    in the middle of the ranking and their coverage is only approximate for
    near-tied products, the discreteness of the rank statistic being the
    reason (Xie, Singh & Zhang, 2009).

    Args:
        df_data: Wide metric table (``config.id_columns`` +
            ``config.metric_columns``).
        config: Column conventions.
        pipeline_factory: Zero-state factory building a fresh, unfitted
            estimator exposing ``fit(X).predict(X)`` (a
            ``sklearn.pipeline.Pipeline`` or a
            :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`),
            given the (unchanged, here) configuration. Methods with no fitted
            state (Pareto, MPI) have no bootstrap of this kind.
        n_boot: Number of bootstrap draws, kept small by default: the whole
            chain is refit on each one.
        ci: Width of the reported rank interval (0.9 → the 5th-95th
            percentile).
        random_state: Seed of the resampling generator.

    Returns:
        ``DataFrame`` indexed by product id, with ``rank_median``,
        ``rank_low``, ``rank_high`` and ``rank_sd``, every statistic computed
        on the same ``n_boot`` ranks per product.

    Examples:
        >>> import pandas as pd
        >>> from macroforecast.trade.aggregation.estimators import WeightedAggregator
        >>> df = pd.DataFrame({"id": ["a", "b", "c"], "HHI": [0.8, 0.3, 0.5]})
        >>> config = AggregationConfig(id_columns=("id",), metric_columns=("HHI",))
        >>> result = bootstrap_rank_stability(
        ...     df, config,
        ...     lambda cfg: WeightedAggregator(weighting="equal"),
        ...     n_boot=20, random_state=0,
        ... )
        >>> sorted(result.columns)
        ['rank_high', 'rank_low', 'rank_median', 'rank_sd']
        >>> float(result.loc["a", "rank_median"])
        1.0
    """
    rng = np.random.default_rng(random_state)
    X_full, index_full = split_frame(df_data, config)
    n = X_full.shape[0]
    alpha = (1.0 - ci) / 2.0

    # Une ligne par tirage, une colonne par produit : rangs sur la population complète
    ranks_by_draw = np.empty((n_boot, n), dtype=float)
    for draw_id in range(n_boot):
        draw = rng.integers(0, n, size=n)
        estimator = pipeline_factory(config)
        scores = estimator.fit(X_full[draw]).predict(X_full)
        ranks_by_draw[draw_id] = stats.rankdata(-np.asarray(scores), method="average")

    return pd.DataFrame(
        {
            "rank_median": np.median(ranks_by_draw, axis=0),
            "rank_low": np.quantile(ranks_by_draw, alpha, axis=0),
            "rank_high": np.quantile(ranks_by_draw, 1.0 - alpha, axis=0),
            "rank_sd": (
                ranks_by_draw.std(axis=0, ddof=1) if n_boot > 1 else np.zeros(n)
            ),
        },
        index=index_full,
    )


# Résultat de l'exploration SMAA
@dataclass
class SmaaResult:
    """Stochastic multicriteria acceptability analysis result.

    Attributes:
        rank_acceptability: Array of shape ``(n, k)``; entry ``[i, r]`` is
            the share of weight draws under which product ``i`` ranked
            ``r + 1``. Only the first ``k`` ranks are stored — a full
            ``(n, n)`` matrix reaches 200 Mb at ``n = 5 000`` and is out of
            reach at the global level (I-09) — so a row sums to *at most* 1,
            the missing mass being the draws placing the product beyond rank
            ``k``.
        central_weight: Array of shape ``(n, d)``; row ``i`` is the average
            weight vector among the draws where product ``i`` ranked first
            (``NaN`` row when it never did).
        confidence_factor: Array of shape ``(n,)``; share of weight draws
            under which product ``i`` ranked within the top ``k`` — the
            statement fit for an administrative report ("in the top 50
            under 94% of admissible weightings"). Equal to the row sums of
            ``rank_acceptability``.
        k: Rank depth actually stored, ``min(k, n)``.
    """
    rank_acceptability: np.ndarray
    central_weight: np.ndarray
    confidence_factor: np.ndarray
    k: int = 0


# Fonction d'exploration SMAA sur le simplexe des poids
def smaa_rank_acceptability(
    X: np.ndarray,
    aggregation_fn: Callable[..., np.ndarray],
    *,
    aggregation_params: Optional[Dict[str, Any]] = None,
    k: int = 50,
    n_draws: int = 10_000,
    batch_size: int = 256,
    random_state: Optional[int] = None,
    weight_draws: Optional[np.ndarray] = None,
) -> SmaaResult:
    """Explore the whole admissible weight simplex instead of picking one vector.

    The exact formalisation of "unable to rank the criteria": ``n_draws``
    weight vectors are drawn uniformly on the simplex
    (:func:`~macroforecast.trade.aggregation.weights.dirichlet_weights`), the
    induced ranking is computed for each, and the rank distribution of every
    product is accumulated (algorithm 5 of the note).

    Draws are processed in batches: the scores of a batch form an
    ``(n, T_b)`` matrix ranked column by column by a single ``argsort``, and
    only ranks ``1..k`` are accumulated, holding memory at
    ``O(n · k + n · batch_size)`` instead of the ``O(n²)`` of a full rank
    matrix.

    Args:
        X: Metric matrix of shape ``(n, d)``, already preprocessed
            (oriented, winsorised, normalised).
        aggregation_fn: A weight-taking aggregation function of
            :mod:`~macroforecast.trade.aggregation.functions` (e.g.
            :func:`~macroforecast.trade.aggregation.functions.weighted_sum_score`).
        aggregation_params: Extra keyword arguments forwarded to
            ``aggregation_fn``.
        k: Rank depth stored and depth of the reported confidence factor,
            clipped to ``n``.
        n_draws: Number of Dirichlet weight draws; ignored when
            ``weight_draws`` is supplied.
        batch_size: Number of draws scored and ranked at once.
        random_state: Seed of the weight sampler; ignored when
            ``weight_draws`` is supplied.
        weight_draws: Pre-drawn simplex sample of shape ``(T, d)`` to reuse
            instead of drawing a fresh one — how
            :class:`~macroforecast.trade.aggregation.estimators.SmaaScorer`
            keeps the draws fixed between ``fit`` and ``predict``, and how the
            same sample is shared with the cone quantile (A-01).

    Returns:
        The :class:`SmaaResult` of the exploration.

    Raises:
        ValueError: If ``weight_draws`` does not match the number of columns
            of ``X``.

    Examples:
        >>> import numpy as np
        >>> from macroforecast.trade.aggregation.functions import weighted_sum_score
        >>> X = np.array([[0.8, 0.2], [0.1, 0.9], [0.5, 0.5]])
        >>> result = smaa_rank_acceptability(
        ...     X, weighted_sum_score, k=1, n_draws=200, random_state=0)
        >>> result.rank_acceptability.shape
        (3, 1)
        >>> result.confidence_factor.shape
        (3,)
    """
    X = np.asarray(X, dtype=float)
    n, d = X.shape
    params = aggregation_params or {}
    if weight_draws is None:
        weight_draws = dirichlet_weights(d, n_draws, random_state=random_state)
    else:
        weight_draws = np.atleast_2d(np.asarray(weight_draws, dtype=float))
        if weight_draws.shape[1] != d:
            raise ValueError(
                f"weight_draws must have shape (T, {d}), got {weight_draws.shape}."
            )
        n_draws = weight_draws.shape[0]

    k_stored = int(min(k, n))
    rank_counts = np.zeros((n, k_stored), dtype=np.int64)
    weight_sum_top1 = np.zeros((n, d))
    n_top1 = np.zeros(n, dtype=np.int64)
    positions = np.arange(1, n + 1)[:, None]

    for start in range(0, n_draws, batch_size):
        batch = weight_draws[start : start + batch_size]
        # Scores du lot : une colonne par tirage
        score_batch = np.column_stack(
            [aggregation_fn(X, weight, **params) for weight in batch]
        )
        # Rangs ordinaux par colonne, ex æquo départagés par l'ordre des lignes
        order = np.argsort(-score_batch, axis=0, kind="stable")
        ranks = np.empty_like(order)
        np.put_along_axis(ranks, order, np.broadcast_to(positions, order.shape), axis=0)
        # Accumulation des seuls rangs de tête (stockage (n, k))
        rows, columns = np.nonzero(ranks <= k_stored)
        np.add.at(rank_counts, (rows, ranks[rows, columns] - 1), 1)
        # Poids centraux : moyenne des tirages plaçant le produit en tête
        top1 = order[0]
        np.add.at(weight_sum_top1, top1, batch)
        np.add.at(n_top1, top1, 1)

    rank_acceptability = rank_counts / n_draws
    confidence_factor = rank_acceptability.sum(axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        central_weight = weight_sum_top1 / n_top1[:, None]
    central_weight[n_top1 == 0] = np.nan
    return SmaaResult(rank_acceptability, central_weight, confidence_factor, k_stored)


# ──────────────────────────────────────────────────────────────────────
# Q5 — Réductibilité à une seule métrique
# ──────────────────────────────────────────────────────────────────────

# Fonction de retrait successif de chaque métrique
def leave_one_metric_out(
    df_data: pd.DataFrame,
    config: AggregationConfig,
    pipeline_factory: Callable[[AggregationConfig], Any],
    *,
    k: int = 50,
) -> pd.DataFrame:
    """Recompute the whole chain with each metric removed in turn.

    A metric whose removal changes nothing is either redundant or neutralised
    by the weighting; one whose removal upends everything carries the result
    on its own — both situations call for an explicit comment (§6.5 of the
    note).

    Args:
        df_data: Wide metric table.
        config: Column conventions (its ``metric_columns`` are the candidates
            for removal).
        pipeline_factory: Factory building a fresh, unfitted estimator given
            the (possibly metric-reduced) configuration — receiving the
            configuration lets it size any per-metric hyperparameter (e.g.
            ``PolarityOrienter``'s polarity vector) to the reduced dimension.
        k: Rank depth of the reported top-k overlap.

    Returns:
        ``DataFrame`` indexed by removed metric name, with ``kendall_tau``
        and ``topk_overlap`` against the full-metric ranking.

    Examples:
        >>> import pandas as pd
        >>> from macroforecast.trade.aggregation.estimators import WeightedAggregator
        >>> df = pd.DataFrame({
        ...     "id": ["a", "b", "c"], "HHI": [0.8, 0.3, 0.5], "CDI2": [0.6, 0.4, 0.5],
        ... })
        >>> config = AggregationConfig(id_columns=("id",), metric_columns=("HHI", "CDI2"))
        >>> result = leave_one_metric_out(
        ...     df, config, lambda cfg: WeightedAggregator(weighting="equal"))
        >>> sorted(result.index)
        ['CDI2', 'HHI']
    """
    X_full, index_full = split_frame(df_data, config)
    full_estimator = pipeline_factory(config)
    full_scores = full_estimator.fit(X_full).predict(X_full)
    full_ranking = list(index_full[np.argsort(-full_scores)])

    records = []
    for metric in config.metric_columns:
        reduced_config = replace(
            config,
            metric_columns=tuple(c for c in config.metric_columns if c != metric),
            polarities={
                name: value for name, value in config.polarities.items() if name != metric
            },
        )
        X_reduced, _ = split_frame(df_data, reduced_config)
        estimator = pipeline_factory(reduced_config)
        scores = estimator.fit(X_reduced).predict(X_reduced)
        ranking = list(index_full[np.argsort(-scores)])

        tau, _ = stats.kendalltau(full_scores, scores)
        records.append(
            {
                "metric": metric,
                "kendall_tau": float(tau),
                "topk_overlap": topk_overlap(full_ranking, ranking, k=min(k, len(ranking))),
            }
        )
    return pd.DataFrame.from_records(records).set_index("metric")


# ──────────────────────────────────────────────────────────────────────
# Classements consensus
# ──────────────────────────────────────────────────────────────────────

# Fonction de classement consensus de Borda
def borda_rank(scores_by_method: Mapping[str, np.ndarray]) -> np.ndarray:
    """Rank products by their average rank across methods (Borda consensus).

    Immediate and robust, but sensitive to methods strongly correlated with
    one another, which then vote several times for the same product (see
    :func:`cluster_methods` to detect and correct for that).

    Args:
        scores_by_method: Mapping of method name to its score vector.

    Returns:
        Rank array of shape ``(n,)``, ``1`` = most vulnerable.

    Examples:
        >>> import numpy as np
        >>> scores = {"a": np.array([3.0, 1.0, 2.0]), "b": np.array([3.0, 2.0, 1.0])}
        >>> borda_rank(scores)
        array([1. , 2.5, 2.5])
    """
    ranks = np.column_stack(
        [stats.rankdata(-s, method="average") for s in scores_by_method.values()]
    )
    mean_rank = ranks.mean(axis=1)
    return stats.rankdata(mean_rank, method="average")


# Fonction de classement consensus de Copeland
def copeland_rank(
    scores_by_method: Mapping[str, np.ndarray], *, top_n: Optional[int] = None
) -> np.ndarray:
    """Rank products by net pairwise-majority wins (Copeland consensus).

    For every ordered pair, count the methods placing ``i`` ahead of ``k``;
    ``i`` wins the duel on majority. The Copeland score is the count of duels
    won minus duels lost — more robust to extreme rank values than Borda.

    Every duel of the group is an entry of an ``(n, n)`` vote matrix, out of
    reach at the global level (I-17), hence the same preselection contract as
    :func:`kemeny_rank`: with ``top_n`` set, the duels are solved exactly on
    the ``top_n`` first products of the Borda consensus and the remainder
    keeps its Borda order, appended after that core.

    Args:
        scores_by_method: Mapping of method name to its score vector.
        top_n: Size of the Borda preselection the duels are restricted to.
            ``None`` (the default) runs every duel, which is exact but
            quadratic in memory.

    Returns:
        Rank array of shape ``(n,)``, ``1`` = most vulnerable.

    Examples:
        >>> import numpy as np
        >>> scores = {"a": np.array([3.0, 1.0, 2.0]), "b": np.array([3.0, 2.0, 1.0])}
        >>> copeland_rank(scores)
        array([1. , 2.5, 2.5])
        >>> copeland_rank(scores, top_n=1)
        array([1., 2., 3.])
    """
    methods = [np.asarray(s) for s in scores_by_method.values()]
    n_total = methods[0].shape[0]

    borda = borda_rank(scores_by_method)
    order = np.argsort(borda, kind="stable")
    n = n_total if top_n is None else min(top_n, n_total)
    preselected = order[:n]
    remainder = order[n:]

    sub_methods = [s[preselected] for s in methods]
    votes_i_over_k = np.zeros((n, n))
    for scores in sub_methods:
        votes_i_over_k += (scores[:, None] > scores[None, :]).astype(int)

    majority = votes_i_over_k > (len(methods) / 2.0)
    net_wins = majority.sum(axis=1).astype(float) - majority.sum(axis=0).astype(float)
    consensus_local = stats.rankdata(-net_wins, method="average")

    final_rank = np.empty(n_total)
    final_rank[preselected] = consensus_local
    # Hors présélection : ordre de Borda, à la suite du noyau résolu exactement
    final_rank[remainder] = n + np.arange(1, len(remainder) + 1)
    return final_rank


# Fonction de classement consensus par médiane de Kemeny
def kemeny_rank(scores_by_method: Mapping[str, np.ndarray], *, top_n: int = 100) -> np.ndarray:
    """Rank products by the Kemeny median, on a Borda preselection.

    Minimises the total Kendall distance to every input ranking — the best
    axiomatically-grounded consensus, but NP-hard: solved by mixed-integer
    programming (``scipy.optimize.milp``) restricted to the ``top_n``
    products of the Borda consensus, as the note recommends for large ``n``
    (§6.6). Products outside the preselection keep their Borda order,
    appended after the MILP-solved core.

    Args:
        scores_by_method: Mapping of method name to its score vector.
        top_n: Size of the Borda preselection solved exactly. Solving grows
            roughly cubically in ``top_n`` (the transitivity constraints);
            values much above 100-150 can become slow.

    Returns:
        Rank array of shape ``(n,)``, ``1`` = most vulnerable.

    Examples:
        >>> import numpy as np
        >>> scores = {"a": np.array([3.0, 1.0, 2.0]), "b": np.array([3.0, 2.0, 1.0])}
        >>> kemeny_rank(scores, top_n=3)
        array([1., 2., 3.])
    """
    methods = [np.asarray(s) for s in scores_by_method.values()]
    n_total = methods[0].shape[0]

    borda = borda_rank(scores_by_method)
    order = np.argsort(borda)
    n = min(top_n, n_total)
    preselected = order[:n]
    remainder = order[n:]

    sub_methods = [s[preselected] for s in methods]
    # d[i, j] = nombre de méthodes classant i avant j
    disagreement = np.zeros((n, n))
    for scores in sub_methods:
        disagreement += (scores[:, None] > scores[None, :]).astype(float)

    # Variables binaires x_ij = 1 si i est placé avant j dans le consensus
    pairs = [(i, j) for i in range(n) for j in range(n) if i != j]
    index_of = {pair: position for position, pair in enumerate(pairs)}
    n_vars = len(pairs)
    # Coût de x_ij = 1 : nombre de méthodes en désaccord (classant j avant i)
    cost = np.array([disagreement[j, i] for i, j in pairs])

    rows: List[int] = []
    cols: List[int] = []
    data: List[float] = []
    lower: List[float] = []
    upper: List[float] = []

    def add_constraint(entries: Sequence[Tuple[int, float]], lo: float, hi: float) -> None:
        row_id = len(lower)
        for col, value in entries:
            rows.append(row_id)
            cols.append(col)
            data.append(value)
        lower.append(lo)
        upper.append(hi)

    # Antisymétrie : x_ij + x_ji = 1
    for i, j in combinations(range(n), 2):
        add_constraint([(index_of[(i, j)], 1.0), (index_of[(j, i)], 1.0)], 1.0, 1.0)
    # Transitivité : interdiction des deux orientations cycliques de chaque triplet
    for i, j, k in combinations(range(n), 3):
        add_constraint(
            [(index_of[(i, j)], 1.0), (index_of[(j, k)], 1.0), (index_of[(k, i)], 1.0)],
            -np.inf,
            2.0,
        )
        add_constraint(
            [(index_of[(i, k)], 1.0), (index_of[(k, j)], 1.0), (index_of[(j, i)], 1.0)],
            -np.inf,
            2.0,
        )

    constraint_matrix = sparse.csr_matrix(
        (data, (rows, cols)), shape=(len(lower), n_vars)
    )
    linear_constraint = LinearConstraint(constraint_matrix, lower, upper)

    result = milp(
        cost,
        constraints=[linear_constraint],
        bounds=Bounds(0, 1),
        integrality=np.ones(n_vars),
    )
    if not result.success:
        raise RuntimeError(f"Kemeny consensus MILP failed to solve: {result.message}")

    # Nombre de « victoires » (candidats placés après) : rang consensus local
    solution = np.round(result.x)
    wins = np.zeros(n)
    for (i, _j), value in zip(pairs, solution):
        wins[i] += value
    consensus_local = stats.rankdata(-wins, method="ordinal")

    final_rank = np.empty(n_total)
    final_rank[preselected] = consensus_local
    final_rank[remainder] = n + np.arange(1, len(remainder) + 1)
    return final_rank


# ──────────────────────────────────────────────────────────────────────
# Rapport composite de cohérence
# ──────────────────────────────────────────────────────────────────────

# Rapport de cohérence entre méthodes d'agrégation
@dataclass
class CoherenceReport:
    """Composite comparison-and-robustness report of a set of methods.

    Attributes:
        tau_matrix: Pairwise Kendall τ_b between methods (Q1).
        weighted_tau_matrix: Pairwise weighted Kendall τ between methods,
            hyperbolically weighted towards the top of the ranking (Q2,
            A-06) — the parameter-free complement of the RBO.
        kendall_w: Kendall's coefficient of concordance across methods (Q1).
        violation_rate: :class:`ViolationRates` per method — strict
            inversions and ties told apart (Q3, M-10).
        front_rank: Per method, the ``(median_rank, max_rank)`` of the Pareto
            front members (:func:`front_rank_summary`).
        consensus_rank: Borda-consensus rank, indexed like the input scores
            (§6.6).
        disputed_ids: Ids whose rank spread across methods exceeds
            ``dispute_threshold`` — the note's own most informative output,
            not a weakness: it names exactly the products needing individual
            human review.
        dispute_threshold: Rank spread actually used to flag a product,
            derived from the requested fraction of the group size.
    """
    tau_matrix: pd.DataFrame = field(default_factory=pd.DataFrame)
    weighted_tau_matrix: pd.DataFrame = field(default_factory=pd.DataFrame)
    kendall_w: float = float("nan")
    violation_rate: Dict[str, ViolationRates] = field(default_factory=dict)
    front_rank: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    consensus_rank: pd.Series = field(default_factory=pd.Series)
    disputed_ids: List[Any] = field(default_factory=list)
    dispute_threshold: int = 0

    # Mise en forme des indicateurs numériques (style VulnerabilityReport.to_metrics)
    def to_metrics(self, prefix: str = "aggregation.coherence") -> Dict[str, float]:
        """Flatten the numeric diagnostics into a dotted metric mapping.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats (``NaN`` and
            infinite values dropped, e.g. for an MLflow tracker).

        Examples:
            >>> rates = ViolationRates(strict=0.0, tie=0.25, n_pairs=4)
            >>> report = CoherenceReport(kendall_w=0.8, violation_rate={"HHI": rates})
            >>> report.to_metrics()["aggregation.coherence.kendall_w"]
            0.8
            >>> report.to_metrics()["aggregation.coherence.violation_tie_HHI"]
            0.25
        """
        metrics: Dict[str, float] = {}
        if np.isfinite(self.kendall_w):
            metrics[f"{prefix}.kendall_w"] = float(self.kendall_w)
        for name, rates in self.violation_rate.items():
            if np.isfinite(rates.strict):
                metrics[f"{prefix}.violation_strict_{name}"] = float(rates.strict)
            if np.isfinite(rates.tie):
                metrics[f"{prefix}.violation_tie_{name}"] = float(rates.tie)
            metrics[f"{prefix}.violation_n_pairs_{name}"] = float(rates.n_pairs)
        for name, (median_rank, max_rank) in self.front_rank.items():
            if np.isfinite(median_rank):
                metrics[f"{prefix}.front_rank_median_{name}"] = float(median_rank)
            if np.isfinite(max_rank):
                metrics[f"{prefix}.front_rank_max_{name}"] = float(max_rank)
        metrics[f"{prefix}.n_disputed"] = float(len(self.disputed_ids))
        metrics[f"{prefix}.dispute_threshold"] = float(self.dispute_threshold)
        return metrics


# Fonction d'assemblage du rapport de cohérence
def compute_coherence_report(
    scores_by_method: Mapping[str, pd.Series],
    X: np.ndarray,
    *,
    dispute_fraction: float = 0.01,
    front_mask: Optional[np.ndarray] = None,
    large_n_threshold: int = 20_000,
    n_pairs_sample: int = 1_000_000,
    random_state: Optional[int] = None,
) -> CoherenceReport:
    """Assemble the coherence dashboard of algorithm 6 from a set of scores.

    Args:
        scores_by_method: Mapping of method name to its score series, every
            series sharing the same index (product identity) and order.
        X: Metric matrix of shape ``(n, d)``, positive polarity, aligned row
            for row with the scores — the basis of the dominance-violation
            check (Q3) and of the Pareto front.
        dispute_fraction: Fraction of the group size above which a rank
            spread flags a product as disputed. Expressed as a fraction and
            not as an absolute number of ranks, groups ranging from 27
            (reporters) to 2·10⁵ rows (global) — a 50-rank spread means
            everything in the first and nothing in the second (M-10). The
            effective threshold is ``max(1, round(dispute_fraction · n))``.
        front_mask: Boolean mask of the non-dominated products; computed with
            :func:`~macroforecast.trade.aggregation.pareto.pareto_front_sweep`
            when omitted, and worth passing whenever the caller has already
            computed the front.
        large_n_threshold: Forwarded to :func:`dominance_violation_rate`.
        n_pairs_sample: Forwarded to :func:`dominance_violation_rate`.
        random_state: Forwarded to :func:`dominance_violation_rate`.

    Returns:
        The :class:`CoherenceReport`.

    Examples:
        >>> import numpy as np
        >>> import pandas as pd
        >>> index = pd.Index(["a", "b", "c"])
        >>> scores = {
        ...     "sum": pd.Series([0.9, 0.5, 0.1], index=index),
        ...     "geo": pd.Series([0.8, 0.6, 0.2], index=index),
        ... }
        >>> X = np.array([[0.9, 0.9], [0.5, 0.5], [0.1, 0.1]])
        >>> report = compute_coherence_report(scores, X)
        >>> report.kendall_w
        1.0
        >>> report.violation_rate["sum"].strict
        0.0
        >>> report.front_rank["sum"]
        (1.0, 1.0)
    """
    index = next(iter(scores_by_method.values())).index
    arrays = {
        name: series.reindex(index).to_numpy()
        for name, series in scores_by_method.items()
    }
    X = np.asarray(X, dtype=float)
    if front_mask is None:
        front_mask = pareto_front_sweep(X)

    tau_matrix = kendall_tau_b_matrix(arrays)
    weighted_tau = weighted_tau_matrix(arrays)
    w = kendall_w(arrays)
    violation_rate = {
        name: dominance_violation_rate(
            X,
            scores,
            large_n_threshold=large_n_threshold,
            n_pairs_sample=n_pairs_sample,
            random_state=random_state,
        )
        for name, scores in arrays.items()
    }
    front_rank = {
        name: front_rank_summary(front_mask, scores) for name, scores in arrays.items()
    }

    consensus = borda_rank(arrays)
    consensus_series = pd.Series(consensus, index=index, name="consensus_rank")

    ranks = np.column_stack(
        [stats.rankdata(-scores, method="average") for scores in arrays.values()]
    )
    # Seuil de litige relatif à la taille du groupe, plancher d'un rang (I-08)
    dispute_threshold = max(1, int(round(dispute_fraction * len(index))))
    spread = ranks.max(axis=1) - ranks.min(axis=1)
    disputed_ids = list(index[spread > dispute_threshold])

    return CoherenceReport(
        tau_matrix=tau_matrix,
        weighted_tau_matrix=weighted_tau,
        kendall_w=w,
        violation_rate=violation_rate,
        front_rank=front_rank,
        consensus_rank=consensus_series,
        disputed_ids=disputed_ids,
        dispute_threshold=dispute_threshold,
    )
