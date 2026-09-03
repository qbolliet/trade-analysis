"""Comparison and robustness protocol, and consensus rankings.

Implements §6 of the methodological note: nothing guarantees that the
scores produced by different weighting/aggregation choices agree, and that
disagreement carries the most useful information — it delimits what the
result owes to the data from what it owes to the methodological choice. Five
questions structure the protocol:

* Q1 — global concordance between rankings (:func:`kendall_tau_b_matrix`,
  :func:`kendall_w`, :func:`cluster_methods`);
* Q2 — concordance at the top of the ranking (:func:`rank_biased_overlap`,
  :func:`topk_overlap`);
* Q3 — coherence with Pareto dominance, the one hard test
  (:func:`dominance_violation_rate`);
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
from .pareto import pareto_dominance_matrix
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

# Fonction de calcul du taux de violation de la dominance
def dominance_violation_rate(X: np.ndarray, scores: np.ndarray) -> float:
    """Compute the dominance-violation rate ``V(s)`` of a score.

    The one control resting on no convention, capable of invalidating a
    method on its own: ``V(s) > 0`` reverses at least one pair on which
    *every* metric agrees.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        scores: Score vector of shape ``(n,)``, higher meaning more
            vulnerable.

    Returns:
        Share of Pareto-comparable pairs the score inverts, ``NaN`` when no
        pair is comparable (an empty dominance relation).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0]])
        >>> dominance_violation_rate(X, np.array([1.0, 2.0]))
        1.0
        >>> dominance_violation_rate(X, np.array([2.0, 1.0]))
        0.0
    """
    dominance = pareto_dominance_matrix(X)
    n_pairs = dominance.sum()
    if n_pairs == 0:
        return float("nan")
    scores = np.asarray(scores)
    violations = np.sum(dominance & (scores[:, None] <= scores[None, :]))
    return float(violations / n_pairs)


# ──────────────────────────────────────────────────────────────────────
# Q4 — Stabilité
# ──────────────────────────────────────────────────────────────────────

# Fonction de bootstrap de la stabilité des rangs
def bootstrap_rank_stability(
    df_data: pd.DataFrame,
    config: AggregationConfig,
    pipeline_factory: Callable[[AggregationConfig], Any],
    *,
    n_boot: int = 200,
    ci: float = 0.9,
    random_state: Optional[int] = None,
) -> pd.DataFrame:
    """Resample products with replacement and collect their rank distribution.

    The full chain is refit on every draw — *including the weights*, since
    that they are estimated is precisely the point (algorithm/§6.4 of the
    note). A product whose 90% interval spans ``[3, 412]`` should never be
    reported as "third most vulnerable".

    Args:
        df_data: Wide metric table (``config.id_columns`` +
            ``config.metric_columns``).
        config: Column conventions.
        pipeline_factory: Zero-state factory building a fresh, unfitted
            estimator exposing ``fit(X).predict(X)`` (a
            ``sklearn.pipeline.Pipeline`` or a
            :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`),
            given the (unchanged, here) configuration.
        n_boot: Number of bootstrap draws.
        ci: Width of the reported rank interval (0.9 → the 5th-95th
            percentile).
        random_state: Seed of the resampling generator.

    Returns:
        ``DataFrame`` indexed by product id, with ``rank_median``,
        ``rank_low``, ``rank_high`` and ``n_draws_observed`` (how many draws
        actually included that id, resampling with replacement leaving some
        ids absent from a given draw).

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
        ['n_draws_observed', 'rank_high', 'rank_low', 'rank_median']
    """
    rng = np.random.default_rng(random_state)
    X_full, index_full = split_frame(df_data, config)
    n = X_full.shape[0]
    alpha = (1.0 - ci) / 2.0

    rank_samples: Dict[Any, List[float]] = defaultdict(list)
    for _ in range(n_boot):
        draw = rng.integers(0, n, size=n)
        X_boot = X_full[draw]
        ids_boot = index_full[draw]
        estimator = pipeline_factory(config)
        scores = estimator.fit(X_boot).predict(X_boot)
        ranks = stats.rankdata(-scores, method="average")
        for identifier, rank in zip(ids_boot, ranks):
            rank_samples[identifier].append(rank)

    records = []
    for identifier, ranks in rank_samples.items():
        ranks_array = np.asarray(ranks)
        records.append(
            {
                "id": identifier,
                "rank_median": float(np.median(ranks_array)),
                "rank_low": float(np.quantile(ranks_array, alpha)),
                "rank_high": float(np.quantile(ranks_array, 1.0 - alpha)),
                "n_draws_observed": len(ranks),
            }
        )
    return pd.DataFrame.from_records(records).set_index("id")


# Résultat de l'exploration SMAA
@dataclass
class SmaaResult:
    """Stochastic multicriteria acceptability analysis result.

    Attributes:
        rank_acceptability: Array of shape ``(n, n)``; entry ``[i, r]`` is
            the share of weight draws under which product ``i`` ranked
            ``r + 1``.
        central_weight: Array of shape ``(n, d)``; row ``i`` is the average
            weight vector among the draws where product ``i`` ranked first
            (``NaN`` row when it never did).
        confidence_factor: Array of shape ``(n,)``; share of weight draws
            under which product ``i`` ranked within the top ``k`` — the
            statement fit for an administrative report ("in the top 50
            under 94% of admissible weightings").
    """
    rank_acceptability: np.ndarray
    central_weight: np.ndarray
    confidence_factor: np.ndarray


# Fonction d'exploration SMAA sur le simplexe des poids
def smaa_rank_acceptability(
    X: np.ndarray,
    aggregation_fn: Callable[..., np.ndarray],
    *,
    aggregation_params: Optional[Dict[str, Any]] = None,
    k: int = 50,
    n_draws: int = 10_000,
    random_state: Optional[int] = None,
) -> SmaaResult:
    """Explore the whole admissible weight simplex instead of picking one vector.

    The exact formalisation of "unable to rank the criteria": ``n_draws``
    weight vectors are drawn uniformly on the simplex
    (:func:`~macroforecast.trade.aggregation.weights.dirichlet_weights`), the
    induced ranking is computed for each, and the rank distribution of every
    product is accumulated (algorithm 5 of the note).

    Args:
        X: Metric matrix of shape ``(n, d)``, already preprocessed
            (oriented, winsorised, normalised).
        aggregation_fn: A weight-taking aggregation function of
            :mod:`~macroforecast.trade.aggregation.functions` (e.g.
            :func:`~macroforecast.trade.aggregation.functions.weighted_sum_score`).
        aggregation_params: Extra keyword arguments forwarded to
            ``aggregation_fn``.
        k: Rank depth of the reported confidence factor.
        n_draws: Number of Dirichlet weight draws.
        random_state: Seed of the weight sampler.

    Returns:
        The :class:`SmaaResult` of the exploration.

    Examples:
        >>> import numpy as np
        >>> from macroforecast.trade.aggregation.functions import weighted_sum_score
        >>> X = np.array([[0.8, 0.2], [0.1, 0.9], [0.5, 0.5]])
        >>> result = smaa_rank_acceptability(
        ...     X, weighted_sum_score, k=1, n_draws=200, random_state=0)
        >>> result.confidence_factor.shape
        (3,)
    """
    X = np.asarray(X, dtype=float)
    n, d = X.shape
    params = aggregation_params or {}
    weight_draws = dirichlet_weights(d, n_draws, random_state=random_state)

    rank_counts = np.zeros((n, n), dtype=np.int64)
    weight_sum_top1 = np.zeros((n, d))
    n_top1 = np.zeros(n, dtype=np.int64)
    n_within_k = np.zeros(n, dtype=np.int64)

    for weight in weight_draws:
        scores = aggregation_fn(X, weight, **params)
        ranks = stats.rankdata(-scores, method="ordinal").astype(int)
        rank_counts[np.arange(n), ranks - 1] += 1
        n_within_k += ranks <= k
        top1_index = int(np.flatnonzero(ranks == 1)[0])
        weight_sum_top1[top1_index] += weight
        n_top1[top1_index] += 1

    rank_acceptability = rank_counts / n_draws
    confidence_factor = n_within_k / n_draws
    with np.errstate(invalid="ignore", divide="ignore"):
        central_weight = weight_sum_top1 / n_top1[:, None]
    central_weight[n_top1 == 0] = np.nan
    return SmaaResult(rank_acceptability, central_weight, confidence_factor)


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
def copeland_rank(scores_by_method: Mapping[str, np.ndarray]) -> np.ndarray:
    """Rank products by net pairwise-majority wins (Copeland consensus).

    For every ordered pair, count the methods placing ``i`` ahead of ``k``;
    ``i`` wins the duel on majority. The Copeland score is the count of duels
    won minus duels lost — more robust to extreme rank values than Borda.

    Args:
        scores_by_method: Mapping of method name to its score vector.

    Returns:
        Rank array of shape ``(n,)``, ``1`` = most vulnerable.

    Examples:
        >>> import numpy as np
        >>> scores = {"a": np.array([3.0, 1.0, 2.0]), "b": np.array([3.0, 2.0, 1.0])}
        >>> copeland_rank(scores)
        array([1. , 2.5, 2.5])
    """
    methods = [np.asarray(s) for s in scores_by_method.values()]
    n = methods[0].shape[0]
    votes_i_over_k = np.zeros((n, n))
    for scores in methods:
        votes_i_over_k += (scores[:, None] > scores[None, :]).astype(int)

    majority = votes_i_over_k > (len(methods) / 2.0)
    net_wins = majority.sum(axis=1).astype(float) - majority.T.sum(axis=1).astype(float)
    return stats.rankdata(-net_wins, method="average")


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
        kendall_w: Kendall's coefficient of concordance across methods (Q1).
        violation_rate: Dominance-violation rate ``V(s)`` per method (Q3).
        consensus_rank: Borda-consensus rank, indexed like the input scores
            (§6.6).
        disputed_ids: Ids whose rank spread across methods exceeds the
            configured threshold — the note's own most informative output,
            not a weakness: it names exactly the products needing individual
            human review.
    """
    tau_matrix: pd.DataFrame = field(default_factory=pd.DataFrame)
    kendall_w: float = float("nan")
    violation_rate: Dict[str, float] = field(default_factory=dict)
    consensus_rank: pd.Series = field(default_factory=pd.Series)
    disputed_ids: List[Any] = field(default_factory=list)

    # Mise en forme des indicateurs numériques (style VulnerabilityReport.to_metrics)
    def to_metrics(self, prefix: str = "aggregation.coherence") -> Dict[str, float]:
        """Flatten the numeric diagnostics into a dotted metric mapping.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats (``NaN`` and
            infinite values dropped, e.g. for an MLflow tracker).

        Examples:
            >>> report = CoherenceReport(kendall_w=0.8, violation_rate={"HHI": 0.0})
            >>> report.to_metrics()["aggregation.coherence.kendall_w"]
            0.8
        """
        metrics: Dict[str, float] = {}
        if np.isfinite(self.kendall_w):
            metrics[f"{prefix}.kendall_w"] = float(self.kendall_w)
        for name, value in self.violation_rate.items():
            if np.isfinite(value):
                metrics[f"{prefix}.violation_rate_{name}"] = float(value)
        metrics[f"{prefix}.n_disputed"] = float(len(self.disputed_ids))
        return metrics


# Fonction d'assemblage du rapport de cohérence
def compute_coherence_report(
    scores_by_method: Mapping[str, pd.Series],
    X: np.ndarray,
    *,
    dispute_threshold: int = 50,
) -> CoherenceReport:
    """Assemble the coherence dashboard of algorithm 6 from a set of scores.

    Args:
        scores_by_method: Mapping of method name to its score series, every
            series sharing the same index (product identity) and order.
        X: Metric matrix of shape ``(n, d)``, positive polarity, aligned row
            for row with the scores — the basis of the dominance-violation
            check (Q3).
        dispute_threshold: Rank-spread above which a product is flagged as
            disputed.

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
    """
    index = next(iter(scores_by_method.values())).index
    arrays = {name: series.reindex(index).to_numpy() for name, series in scores_by_method.items()}

    tau_matrix = kendall_tau_b_matrix(arrays)
    w = kendall_w(arrays)
    violation_rate = {
        name: dominance_violation_rate(X, scores) for name, scores in arrays.items()
    }

    consensus = borda_rank(arrays)
    consensus_series = pd.Series(consensus, index=index, name="consensus_rank")

    ranks = np.column_stack(
        [stats.rankdata(-scores, method="average") for scores in arrays.values()]
    )
    spread = ranks.max(axis=1) - ranks.min(axis=1)
    disputed_ids = list(index[spread > dispute_threshold])

    return CoherenceReport(
        tau_matrix=tau_matrix,
        kendall_w=w,
        violation_rate=violation_rate,
        consensus_rank=consensus_series,
        disputed_ids=disputed_ids,
    )
