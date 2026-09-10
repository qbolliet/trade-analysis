"""Pareto dominance : ordering without a single coefficient.

Implements §3 of the methodological note — the only construction entirely
free of assumption. Two products are only ordered when *every* metric
agrees; the salient object is the Pareto front, the set of products no other
product dominates on every metric at once.

Every function expects ``X`` already oriented in positive polarity (see
:class:`~macroforecast.trade.aggregation.preprocessing.PolarityOrienter`):
dominance is only meaningful once "higher = more vulnerable" holds uniformly.

Choosing an implementation (M-18, A-07): each pair of routines below returns
*identical* results, only the cost differs.

======================  =========================  ==============================  ================
Quantity                Exact reference            Large-``n`` variant             Variant memory
======================  =========================  ==============================  ================
Pairwise relation       :func:`pareto_dominance_matrix`  —                         ``O(n^2)``
Front ``F1``            :func:`pareto_front`       :func:`pareto_front_sweep`      ``O(n)``
Layers ``F_l``          :func:`non_dominated_sort`  :func:`non_dominated_sort_sweep`  ``O(n)``
Dominance count         :func:`dominance_count`    :func:`dominance_count_chunked`  ``O(chunk * n)``
======================  =========================  ==============================  ================

The matrix-based routines are the readable reference and stay usable up to
``n ~ 10**4``; past that the boolean matrix alone weighs ``n^2`` bytes
(``2 * 10**10`` at the global level of the pipeline) and only the sweep and
chunked variants are viable. Sorting sweeps remove the quadratic memory
outright for the front and the layers; the dominance count is inherently
``O(n^2 d)`` in time and is merely streamed block by block.

The ε-relation is *not* a widened dominance cone — that relation is symmetric
and excludes the global maximum from its own front (M-02). It selects a
representative subset of the exact front in the sense of Laumanns et al.
(2002); see :func:`epsilon_pareto_set`.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
# Modules de manipulation de données
import numpy as np
from scipy import stats
from scipy.cluster import hierarchy
from scipy.spatial.distance import squareform
from sklearn.base import BaseEstimator, TransformerMixin, clone
from sklearn.utils.validation import check_array, check_is_fitted
from tqdm.auto import tqdm
# Modules du package
from .preprocessing import spearman_correlation_matrix


# ──────────────────────────────────────────────────────────────────────
# Matrice de dominance par paires
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul de la matrice de dominance de Pareto
def pareto_dominance_matrix(X: np.ndarray) -> np.ndarray:
    """Compute the pairwise Pareto-dominance boolean matrix.

    ``D[i, k]`` is ``True`` when product ``i`` dominates product ``k``:
    at least as vulnerable on every metric, strictly more on at least one
    (definition 1.4 of the note).

    The relation is exact; there is no ε variant here. Widening the cone into
    ``x_i >= x_k - eps`` makes it symmetric for any two points closer than
    ``eps``, so both are excluded from their own front (M-02). The ε
    construction lives in :func:`epsilon_pareto_set` instead.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.

    Returns:
        Boolean matrix of shape ``(n, n)``, diagonal ``False`` (a product
        never dominates itself).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> pareto_dominance_matrix(X)
        array([[False,  True,  True],
               [False, False, False],
               [False,  True, False]])
    """
    X = check_array(X)
    # Comparaisons par paires : ge[i, k, j] = X[i, j] >= X[k, j]
    at_least_as_good = np.all(X[:, None, :] >= X[None, :, :], axis=2)
    strictly_better_somewhere = np.any(X[:, None, :] > X[None, :, :], axis=2)
    dominance = at_least_as_good & strictly_better_somewhere
    np.fill_diagonal(dominance, False)
    return dominance


# ──────────────────────────────────────────────────────────────────────
# Front de Pareto
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul du front de Pareto
def pareto_front(X: np.ndarray) -> np.ndarray:
    """Flag the non-dominated products (the Pareto front ``F1``).

    Reference implementation, ``O(n^2)`` in memory. See
    :func:`pareto_front_sweep` for the identical result at ``O(n)`` memory.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.

    Returns:
        Boolean mask of shape ``(n,)``, ``True`` for a product on the front.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> pareto_front(X)
        array([ True, False, False])
    """
    dominance = pareto_dominance_matrix(X)
    # Non dominé : aucune ligne ne le domine (aucune colonne à True)
    return ~np.any(dominance, axis=0)


# Fonction de calcul du front exact par balayage (grands n)
def pareto_front_sweep(X: np.ndarray) -> np.ndarray:
    """Compute the exact Pareto front by a sorting sweep (A-07).

    Strict dominance strictly increases the sum of the coordinates, so a point
    can only be dominated by a point of *strictly greater* sum: sorting by
    decreasing sum and comparing each point to the front accumulated so far is
    exact. Costs ``O(n log n + n |F| d)`` in time and ``O(n)`` in memory,
    against ``O(n^2)`` memory for :func:`pareto_front` — the only viable
    variant at the global level of the pipeline (``n ~ 2 * 10**5``).

    Duplicated rows all stay on the front, exactly as with
    :func:`pareto_front`: strict dominance requires a strict gain somewhere,
    which two identical rows cannot offer each other.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.

    Returns:
        Boolean mask of shape ``(n,)``, identical to ``pareto_front(X)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> pareto_front_sweep(X)
        array([ True, False, False])
        >>> rng = np.random.default_rng(0)
        >>> Y = rng.random((200, 3))
        >>> bool(np.array_equal(pareto_front_sweep(Y), pareto_front(Y)))
        True
    """
    X = check_array(X)
    n = X.shape[0]
    mask = np.zeros(n, dtype=bool)
    if n == 0:
        return mask

    # Tri décroissant par somme : un dominateur a nécessairement une somme supérieure
    order = np.argsort(-X.sum(axis=1), kind="stable")
    # Tampon prélloué du front courant : au pire tous les points sont non dominés
    front_rows = np.empty_like(X, dtype=float)
    n_front = 0
    for i in order:
        row = X[i]
        current = front_rows[:n_front]
        dominated = bool(
            np.any(np.all(current >= row, axis=1) & np.any(current > row, axis=1))
        )
        if not dominated:
            front_rows[n_front] = row
            n_front += 1
            mask[i] = True
    return mask


# ──────────────────────────────────────────────────────────────────────
# Sous-ensemble ε-représentatif (Laumanns et al., 2002)
# ──────────────────────────────────────────────────────────────────────

# Registre des critères de tri du glouton, point d'entrée piloté par configuration
SORT_KEY_REGISTRY: Dict[str, Callable[[np.ndarray], np.ndarray]] = {
    "sum": lambda X: X.sum(axis=1),
    "max": lambda X: X.max(axis=1),
    "min": lambda X: X.min(axis=1),
}


# Fonction de diffusion d'un epsilon scalaire ou vectoriel
def _broadcast_epsilon(
    epsilon: Union[float, np.ndarray], n_features: int
) -> np.ndarray:
    """Broadcast a scalar or per-metric ``epsilon`` to a ``(d,)`` float array.

    Args:
        epsilon: Scalar tolerance, or one tolerance per metric.
        n_features: Expected number of metrics ``d``.

    Returns:
        Float array of shape ``(d,)``.

    Raises:
        ValueError: If ``epsilon`` is neither scalar nor of shape ``(d,)``, or
            if any entry is negative.

    Examples:
        >>> _broadcast_epsilon(0.5, 3)
        array([0.5, 0.5, 0.5])
    """
    values = np.asarray(epsilon, dtype=float)
    if values.ndim == 0:
        values = np.full(n_features, float(values))
    elif values.shape != (n_features,):
        raise ValueError(
            f"epsilon has shape {values.shape}, expected a scalar or ({n_features},)."
        )
    if np.any(values < 0):
        raise ValueError("epsilon must be non-negative.")
    return values


# Fonction de sélection gloutonne du sous-ensemble ε-représentatif
def epsilon_pareto_set(
    X: np.ndarray,
    epsilon: Union[float, np.ndarray],
    *,
    sort_key: str = "sum",
    counts: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Select a representative ε-subset of the exact front (Laumanns et al., 2002).

    The relation is ``x_k ≽_ε x_i ⟺ ∀j, x_kj + ε_j ≥ x_ij`` — an approximate
    *covering*, not a widened dominance cone. The greedy sweep restricts to
    the exact front, orders it by decreasing ``sort_key`` (ties broken by
    ``counts``, then by index for determinism) and keeps a point when no
    already-kept point ε-dominates it. Every row of ``X`` is then ε-dominated
    by some element of the result.

    Properties, all covered by ``tests/aggregation/test_pareto.py``:

    * the result is a subset of ``pareto_front(X)``, by construction;
    * with ``sort_key="sum"`` the row maximising the coordinate sum is
      processed first and is therefore always kept — the failure mode of the
      relation this replaces (M-02), which excluded the global maximum;
    * ``epsilon = 0`` recovers the exact front, up to duplicated rows: at
      ``epsilon = 0`` the relation degenerates into *weak* dominance, so two
      identical front rows eliminate each other whereas strict dominance keeps
      both;
    * ``|F_ε|`` decreases as ``epsilon`` grows. Set *inclusion* however does
      **not** hold: the greedy walks a fixed order, so a point rejected at a
      small ``epsilon`` by a neighbour that itself disappears at a larger one
      reappears. Counter-example with all three rows on the exact front,
      ``[[20, 0], [11, 3], [9, 4]]``: rows ``{0, 1}`` at ``ε = 1``, rows
      ``{0, 2}`` at ``ε = 3``.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        epsilon: Scalar tolerance, or a ``(d,)`` array of per-metric
            tolerances. Expressed in the units of ``X``, hence normally
            applied to normalised data (``ε_j = c · s_j`` with ``s_j`` a robust
            scale of the column — see :class:`ParetoScorer`).
        sort_key: Key of :data:`SORT_KEY_REGISTRY` ordering the greedy sweep.
        counts: Optional tie-breaker of shape ``(n,)``, typically
            :func:`dominance_count`; the higher, the earlier.

    Returns:
        Boolean mask of shape ``(n,)``, ``True`` for a kept representative.

    Raises:
        ValueError: If ``sort_key`` is unknown, if ``epsilon`` has an
            incompatible shape or a negative entry, or if ``counts`` does not
            have shape ``(n,)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[3.0, 3.0], [2.0, 2.0], [1.0, 1.0]])
        >>> epsilon_pareto_set(X, epsilon=1.5)
        array([ True, False, False])
        >>> Y = np.array([[2.0, 0.0], [1.0, 1.0], [0.0, 2.0]])
        >>> epsilon_pareto_set(Y, epsilon=0.0)
        array([ True,  True,  True])
        >>> epsilon_pareto_set(Y, epsilon=1.0)
        array([ True, False,  True])
    """
    X = check_array(X)
    n, n_features = X.shape
    if sort_key not in SORT_KEY_REGISTRY:
        raise ValueError(
            f"Unknown sort_key {sort_key!r}. Available: {sorted(SORT_KEY_REGISTRY)}."
        )
    tolerance = _broadcast_epsilon(epsilon, n_features)

    mask = np.zeros(n, dtype=bool)
    if n == 0:
        return mask

    # Restriction au front exact : le résultat en est un sous-ensemble par construction
    front_indices = np.flatnonzero(pareto_front_sweep(X))
    key = np.asarray(SORT_KEY_REGISTRY[sort_key](X), dtype=float)[front_indices]

    # Clés décroissantes, l'indice croissant assurant un résultat déterministe
    if counts is None:
        order = front_indices[np.lexsort((front_indices, -key))]
    else:
        counts = np.asarray(counts)
        if counts.shape != (n,):
            raise ValueError(f"counts has shape {counts.shape}, expected ({n},).")
        tie = counts[front_indices].astype(float)
        order = front_indices[np.lexsort((front_indices, -tie, -key))]

    # Balayage glouton : conservation si aucun représentant retenu n'ε-domine le point
    kept_rows = np.empty((order.shape[0], n_features), dtype=float)
    n_kept = 0
    for i in order:
        row = X[i]
        current = kept_rows[:n_kept]
        covered = bool(np.any(np.all(current + tolerance >= row, axis=1)))
        if not covered:
            kept_rows[n_kept] = row
            n_kept += 1
            mask[i] = True
    return mask


# Fonction alias du sous-ensemble ε-représentatif
def epsilon_pareto_front(
    X: np.ndarray,
    epsilon: Union[float, np.ndarray],
    *,
    sort_key: str = "sum",
    counts: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Documented alias of :func:`epsilon_pareto_set`.

    Kept for the public API of the package. Note the change of meaning against
    earlier releases (M-02 / I-02): this is no longer a front computed under a
    widened dominance cone — which was not an order and excluded the global
    maximum — but a representative *subset* of the exact front.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        epsilon: Scalar or per-metric tolerance.
        sort_key: Key of :data:`SORT_KEY_REGISTRY` ordering the greedy sweep.
        counts: Optional tie-breaker of shape ``(n,)``.

    Returns:
        Boolean mask of shape ``(n,)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[3.0, 3.0], [2.0, 2.0], [1.0, 1.0]])
        >>> epsilon_pareto_front(X, epsilon=1.5)
        array([ True, False, False])
    """
    return epsilon_pareto_set(X, epsilon, sort_key=sort_key, counts=counts)


# ──────────────────────────────────────────────────────────────────────
# Tri non dominé et comptage de dominance
# ──────────────────────────────────────────────────────────────────────

# Fonction de tri non dominé par couches (NSGA-II)
def non_dominated_sort(X: np.ndarray) -> np.ndarray:
    """Peel off successive Pareto fronts, assigning a layer to every product.

    Layer 1 is the Pareto front itself; layer 2 is the front of what
    remains once layer 1 is removed, and so on (algorithm 2 of the note,
    NSGA-II's sorting procedure).

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.

    Returns:
        Integer array of shape ``(n,)``, 1-based layer index per product.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[3.0], [2.0], [1.0]])
        >>> non_dominated_sort(X)
        array([1, 2, 3])
    """
    dominance = pareto_dominance_matrix(X)
    n = X.shape[0]
    # Nombre de dominateurs restants par produit, décrémenté couche par couche
    n_dominators = dominance.sum(axis=0).astype(int)
    layers = np.zeros(n, dtype=int)

    layer = 1
    remaining = n_dominators == 0
    while np.any(remaining):
        layers[remaining] = layer
        # Retrait de la couche courante : décrément du nombre de dominateurs
        # restants de chaque produit encore non affecté
        n_dominators -= dominance[remaining, :].sum(axis=0)
        layer += 1
        remaining = (layers == 0) & (n_dominators <= 0)
    return layers


# Fonction de tri non dominé par balayages successifs (grands n)
def non_dominated_sort_sweep(
    X: np.ndarray, max_layers: Optional[int] = None
) -> np.ndarray:
    """Peel off Pareto fronts by iterated sweeps (A-07).

    Same layers as :func:`non_dominated_sort` without the ``O(n^2)`` matrix:
    each layer is a :func:`pareto_front_sweep` over the rows not yet assigned.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        max_layers: Stop after that many layers; deeper rows keep layer ``0``,
            the module's convention for "not computed". ``None`` peels the
            whole cloud.

    Returns:
        Integer array of shape ``(n,)``, 1-based layer index, ``0`` beyond
        ``max_layers``.

    Raises:
        ValueError: If ``max_layers`` is not strictly positive.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[3.0], [2.0], [1.0]])
        >>> non_dominated_sort_sweep(X)
        array([1, 2, 3])
        >>> non_dominated_sort_sweep(X, max_layers=2)
        array([1, 2, 0])
    """
    X = check_array(X)
    if max_layers is not None and max_layers < 1:
        raise ValueError(f"max_layers must be >= 1, got {max_layers}.")
    n = X.shape[0]
    layers = np.zeros(n, dtype=int)

    # Épluchage itératif : le front des lignes restantes constitue la couche courante
    remaining = np.arange(n)
    layer = 1
    while remaining.size and (max_layers is None or layer <= max_layers):
        front = pareto_front_sweep(X[remaining])
        layers[remaining[front]] = layer
        remaining = remaining[~front]
        layer += 1
    return layers


# Fonction de comptage de dominance
def dominance_count(X: np.ndarray) -> np.ndarray:
    """Compute Goldberg's dominance count ``δ(i) = #dominated - #dominators``.

    A finer-grained, weight-free refinement of the partial order: if
    ``x_i ≻ x_k`` then ``δ(i) > δ(k)``, so this score satisfies the
    monotonicity property by construction (property 1.5 of the note) and can
    be used directly as a Pareto-consistent baseline aggregation.

    Reference implementation, ``O(n^2)`` in memory. See
    :func:`dominance_count_chunked` for the identical result at
    ``O(chunk_size * n)`` memory.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.

    Returns:
        Integer array of shape ``(n,)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> dominance_count(X)
        array([ 2, -2,  0])
    """
    dominance = pareto_dominance_matrix(X)
    n_dominated = dominance.sum(axis=1)
    n_dominators = dominance.sum(axis=0)
    return (n_dominated - n_dominators).astype(int)


# Fonction de comptage de dominance par blocs de lignes (grands n)
def dominance_count_chunked(
    X: np.ndarray, chunk_size: int = 2048, progress: bool = False
) -> np.ndarray:
    """Compute the dominance count block by block (A-07).

    The count is inherently ``O(n^2 d)`` in time — every pair must be compared
    — but it need not be *stored*: each block of ``chunk_size`` rows is
    compared to all ``n`` columns, its contribution accumulated, then
    discarded. The comparison is itself accumulated column by column over the
    ``d`` metrics, so the ``d`` axis is never materialised and the peak memory
    is two boolean ``(chunk_size, n)`` buffers.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        chunk_size: Number of rows per block — the memory/overhead trade-off.
        progress: Whether to display a ``tqdm`` progress bar over the blocks,
            useful at the global level where the run takes minutes.

    Returns:
        Integer array of shape ``(n,)``, identical to ``dominance_count(X)``.

    Raises:
        ValueError: If ``chunk_size`` is not strictly positive.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> dominance_count_chunked(X, chunk_size=2)
        array([ 2, -2,  0])
        >>> rng = np.random.default_rng(0)
        >>> Y = rng.random((300, 4))
        >>> bool(np.array_equal(dominance_count_chunked(Y, 64), dominance_count(Y)))
        True
    """
    X = check_array(X)
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}.")
    n, n_features = X.shape
    n_dominated = np.zeros(n, dtype=np.int64)
    n_dominators = np.zeros(n, dtype=np.int64)

    blocks = tqdm(
        range(0, n, chunk_size), desc="dominance count", disable=not progress
    )
    for start in blocks:
        stop = min(start + chunk_size, n)
        block = X[start:stop]
        # Accumulation colonne par colonne : l'axe des métriques n'est jamais matérialisé
        at_least_as_good = np.ones((stop - start, n), dtype=bool)
        strictly_better = np.zeros((stop - start, n), dtype=bool)
        for j in range(n_features):
            column = X[:, j]
            at_least_as_good &= block[:, j, None] >= column[None, :]
            strictly_better |= block[:, j, None] > column[None, :]
        # Diagonale fausse d'office : un point n'est jamais strictement meilleur que lui-même
        dominance = at_least_as_good & strictly_better
        n_dominated[start:stop] = dominance.sum(axis=1)
        n_dominators += dominance.sum(axis=0)
    return (n_dominated - n_dominators).astype(int)


# Fonction de profondeur de dominance normalisée
def normalized_dominance_depth(X: np.ndarray) -> np.ndarray:
    """Rescale :func:`dominance_count` to ``[-1, 1]``.

    ``δ(i) / (n - 1)``, directly comparable across datasets of different
    size.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.

    Returns:
        Float array of shape ``(n,)``.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> normalized_dominance_depth(X)
        array([ 1., -1.,  0.])
    """
    n = X.shape[0]
    if n <= 1:
        return np.zeros(n)
    return dominance_count(X) / (n - 1)


# ──────────────────────────────────────────────────────────────────────
# Réduction préalable de la dimension
# ──────────────────────────────────────────────────────────────────────

# Fonction de rang-normalisation par colonne
def _rank_normalize(X: np.ndarray) -> np.ndarray:
    """Replace every column by its within-sample ranks rescaled to ``(0, 1]``.

    Args:
        X: Metric matrix of shape ``(n, d)``.

    Returns:
        Float array of shape ``(n, d)``, ties averaged.

    Examples:
        >>> import numpy as np
        >>> _rank_normalize(np.array([[10.0], [30.0], [20.0]]))
        array([[0.33333333],
               [1.        ],
               [0.66666667]])
    """
    n = X.shape[0]
    if n == 0:
        return X.astype(float)
    return stats.rankdata(X, axis=0) / n


# Transformateur de regroupement des métriques redondantes
class MetricReducer(BaseEstimator, TransformerMixin):
    """Group redundant metrics and replace each group by a sub-index (M-19).

    Hierarchical clustering on the dissimilarity ``1 - |ρ^S|`` built from the
    Spearman correlation matrix, then each group is replaced by the mean of
    its **rank-normalised** columns. Ranking before averaging is what makes
    the reduction invariant under a strictly increasing transformation applied
    coordinate by coordinate — the very invariance Pareto dominance enjoys, so
    the reduction smuggles in no scale assumption. (Contrast with
    :class:`~macroforecast.trade.aggregation.preprocessing.RankScaler`, which
    ranks against the training sample; here the ranks are recomputed on the
    matrix being transformed.)

    Dominance property, which is what justifies reducing ``d`` before
    computing a front: the mean is increasing in each of its arguments, so
    ``x_i ≻ x_k`` on the columns implies ``x_i ≻ x_k`` on the group means.
    Dominance on the means is therefore *richer* than dominance on the
    columns, and ``F1(reduced) ⊆ F1(full)`` — the front can only shrink, which
    is the point when ``d`` is large enough for the front to absorb most of
    the nomenclature.

    Args:
        n_groups: Fixed number of groups. Takes precedence over ``threshold``
            when both are set.
        threshold: Dissimilarity cut of the dendrogram — two metrics with
            ``1 - |ρ^S| < threshold`` land in the same group. Used when
            ``n_groups`` is ``None``.
        linkage: Linkage method forwarded to ``scipy.cluster.hierarchy``.

    Examples:
        >>> import numpy as np
        >>> rng = np.random.default_rng(0)
        >>> base = rng.random((50, 1))
        >>> X = np.hstack([base, base * 2.0 + 0.1, rng.random((50, 1))])
        >>> reducer = MetricReducer(threshold=0.3).fit(X)
        >>> reducer.groups_
        [(0, 1), (2,)]
        >>> reducer.transform(X).shape
        (50, 2)
    """

    # Initialisation
    def __init__(
        self,
        n_groups: Optional[int] = None,
        threshold: float = 0.3,
        linkage: str = "average",
    ) -> None:
        self.n_groups = n_groups
        self.threshold = threshold
        self.linkage = linkage

    # Ajustement : classification ascendante hiérarchique sur 1 - |rho de Spearman|
    def fit(self, X: np.ndarray, y: None = None) -> "MetricReducer":
        """Cluster the metrics into groups of redundant columns.

        Args:
            X: Metric matrix of shape ``(n, d)``.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``, with ``labels_`` and ``groups_`` fitted.

        Raises:
            ValueError: If neither ``n_groups`` nor ``threshold`` is set, or if
                ``n_groups`` falls outside ``[1, d]``.
        """
        X = check_array(X)
        n_features = X.shape[1]
        self.n_features_in_ = n_features

        if self.n_groups is None and self.threshold is None:
            raise ValueError("One of n_groups or threshold must be set.")
        if self.n_groups is not None and not 1 <= self.n_groups <= n_features:
            raise ValueError(
                f"n_groups must lie in [1, {n_features}], got {self.n_groups}."
            )

        # Une seule métrique : la CAH est indéfinie, le groupe unique est trivial
        if n_features == 1:
            self.labels_ = np.ones(1, dtype=int)
        else:
            correlation = spearman_correlation_matrix(X)
            # Colonne constante : corrélation indéfinie, traitée en dissimilarité maximale
            correlation = np.nan_to_num(np.asarray(correlation, dtype=float), nan=0.0)
            dissimilarity = 1.0 - np.abs(correlation)
            # Symétrisation et diagonale nulle exigées par squareform
            dissimilarity = np.clip((dissimilarity + dissimilarity.T) / 2.0, 0.0, None)
            np.fill_diagonal(dissimilarity, 0.0)
            linkage_matrix = hierarchy.linkage(
                squareform(dissimilarity, checks=False), method=self.linkage
            )
            if self.n_groups is not None:
                self.labels_ = hierarchy.fcluster(
                    linkage_matrix, t=self.n_groups, criterion="maxclust"
                )
            else:
                self.labels_ = hierarchy.fcluster(
                    linkage_matrix, t=self.threshold, criterion="distance"
                )

        # Groupes ordonnés par premier indice, pour une sortie reproductible
        groups: Dict[int, List[int]] = {}
        for index, label in enumerate(self.labels_):
            groups.setdefault(int(label), []).append(index)
        self.groups_: List[Tuple[int, ...]] = sorted(
            (tuple(indices) for indices in groups.values()), key=lambda group: group[0]
        )
        return self

    # Transformation : moyenne des colonnes rang-normalisées de chaque groupe
    def transform(self, X: np.ndarray) -> np.ndarray:
        """Replace every group by the mean of its rank-normalised columns.

        Args:
            X: Metric matrix of shape ``(n, d)``.

        Returns:
            Reduced matrix of shape ``(n, d')`` where ``d' = len(groups_)``.
        """
        check_is_fitted(self, "groups_")
        X = check_array(X)
        ranks = _rank_normalize(X)
        return np.column_stack(
            [ranks[:, list(group)].mean(axis=1) for group in self.groups_]
        )


# ──────────────────────────────────────────────────────────────────────
# Estimateur sklearn de la dominance de Pareto
# ──────────────────────────────────────────────────────────────────────

# Registre des échelles robustes de calibrage d'epsilon, piloté par configuration
EPSILON_SCALE_REGISTRY: Dict[str, Callable[[np.ndarray], Any]] = {
    "mad": lambda X: stats.median_abs_deviation(X, axis=0, scale=1.0),
    "iqr": lambda X: stats.iqr(X, axis=0),
    "std": lambda X: X.std(axis=0),
}

# Quantités exposées par predict
SCORE_REGISTRY = frozenset({"dominance_depth", "dominance_count"})


# Estimateur sklearn du front, des couches et du comptage de dominance
class ParetoScorer(BaseEstimator):
    """Pareto front, ε-front, layers and dominance count as one estimator (D-03).

    The weight-free, assumption-free first step of the workflow (§7 of the
    note), packaged with the large-``n`` algorithms of A-07: above
    ``large_n_threshold`` rows the dominance count switches to
    :func:`dominance_count_chunked` automatically, and the front is always
    computed by sweep. Every threshold is a constructor argument — nothing is
    hard-coded — so a configuration file drives the behaviour end to end.

    Args:
        score: Quantity returned by :meth:`predict`, one of
            ``"dominance_depth"`` (``δ / (n - 1)``) or ``"dominance_count"``.
        epsilon: ``None`` disables the ε-front, and :meth:`alert` then returns
            the exact front. A scalar is a *multiplier* of the robust column
            scale selected by ``epsilon_scale`` (D-03 recommends
            ``0.1 · MAD``). A ``(d',)`` array is used as an absolute
            per-metric tolerance, ``d'`` being the reduced dimension.
        epsilon_scale: Key of :data:`EPSILON_SCALE_REGISTRY`, used when
            ``epsilon`` is a scalar.
        reduce: ``None``, or a :class:`MetricReducer` cloned and fitted in
            :meth:`fit` and applied before every computation.
        compute_layers: Whether :meth:`layers` peels the cloud; ``False``
            returns zeros — the default at the global level, where the peeling
            is the expensive part. Named ``compute_layers`` rather than
            ``layers`` so as not to shadow the :meth:`layers` method.
        chunk_size: Block size of :func:`dominance_count_chunked`.
        sort_key: Key of :data:`SORT_KEY_REGISTRY` ordering the ε-greedy.
        large_n_threshold: Row count above which the chunked count is used and
            the dominance-count tie-break of the ε-greedy is dropped (it would
            itself cost ``O(n^2)``).

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> scorer = ParetoScorer().fit(X)
        >>> scorer.front(X)
        array([ True, False, False])
        >>> scorer.layers(X)
        array([1, 3, 2])
        >>> scorer.predict(X)
        array([ 1., -1.,  0.])
        >>> ParetoScorer(epsilon=0.5).fit(X).alert(X)
        array([ True, False, False])
    """

    # Initialisation
    def __init__(
        self,
        score: str = "dominance_depth",
        epsilon: Optional[Union[float, np.ndarray]] = None,
        epsilon_scale: str = "mad",
        reduce: Optional[MetricReducer] = None,
        compute_layers: bool = True,
        chunk_size: int = 2048,
        sort_key: str = "sum",
        large_n_threshold: int = 20_000,
    ) -> None:
        self.score = score
        self.epsilon = epsilon
        self.epsilon_scale = epsilon_scale
        self.reduce = reduce
        self.compute_layers = compute_layers
        self.chunk_size = chunk_size
        self.sort_key = sort_key
        self.large_n_threshold = large_n_threshold

    # Ajustement : réducteur de métriques et calibrage d'epsilon
    def fit(self, X: np.ndarray, y: None = None) -> "ParetoScorer":
        """Fit the metric reducer and calibrate ``epsilon``.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            ``self``, with ``reducer_``, ``epsilon_`` and
            ``n_features_reduced_`` fitted.

        Raises:
            ValueError: If ``score``, ``sort_key`` or ``epsilon_scale`` names
                an unknown option, or if a vector ``epsilon`` does not match
                the reduced dimension.
        """
        X = check_array(X)
        self.n_features_in_ = X.shape[1]

        if self.score not in SCORE_REGISTRY:
            raise ValueError(
                f"Unknown score {self.score!r}. Available: {sorted(SCORE_REGISTRY)}."
            )
        if self.sort_key not in SORT_KEY_REGISTRY:
            raise ValueError(
                f"Unknown sort_key {self.sort_key!r}. "
                f"Available: {sorted(SORT_KEY_REGISTRY)}."
            )

        # Réduction de dimension : clonage pour ne jamais muter l'objet de l'appelant
        self.reducer_ = None if self.reduce is None else clone(self.reduce).fit(X)
        X_reduced = self._reduce(X)
        self.n_features_reduced_ = X_reduced.shape[1]

        if self.epsilon is None:
            self.epsilon_ = None
        elif np.ndim(self.epsilon) == 0:
            if self.epsilon_scale not in EPSILON_SCALE_REGISTRY:
                raise ValueError(
                    f"Unknown epsilon_scale {self.epsilon_scale!r}. "
                    f"Available: {sorted(EPSILON_SCALE_REGISTRY)}."
                )
            # Multiplicateur d'une échelle robuste, colonne par colonne (M-02)
            scale = np.atleast_1d(
                np.asarray(
                    EPSILON_SCALE_REGISTRY[self.epsilon_scale](X_reduced), dtype=float
                )
            )
            self.epsilon_ = _broadcast_epsilon(
                float(self.epsilon) * scale, self.n_features_reduced_
            )
        else:
            self.epsilon_ = _broadcast_epsilon(self.epsilon, self.n_features_reduced_)
        return self

    # Application de la réduction de dimension ajustée
    def _reduce(self, X: np.ndarray) -> np.ndarray:
        """Apply the fitted reducer, if any.

        Args:
            X: Metric matrix of shape ``(n, d)``.

        Returns:
            Matrix of shape ``(n, d')``, ``X`` itself when ``reduce`` is
            ``None``.
        """
        X = check_array(X)
        return X if self.reducer_ is None else self.reducer_.transform(X)

    # Front exact, toujours par balayage
    def front(self, X: np.ndarray) -> np.ndarray:
        """Flag the rows on the exact Pareto front.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Boolean mask of shape ``(n,)``.
        """
        check_is_fitted(self, "n_features_reduced_")
        return pareto_front_sweep(self._reduce(X))

    # Sous-ensemble ε-représentatif du front
    def epsilon_front(self, X: np.ndarray) -> np.ndarray:
        """Flag the rows of the representative ε-subset of the front.

        Falls back to :meth:`front` when ``epsilon`` is ``None``.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Boolean mask of shape ``(n,)``, a subset of ``front(X)``.
        """
        check_is_fitted(self, "n_features_reduced_")
        if self.epsilon_ is None:
            return self.front(X)
        X_reduced = self._reduce(X)
        # Départage par comptage de dominance, sauf à grand n où il coûterait O(n²)
        counts = (
            None
            if X_reduced.shape[0] > self.large_n_threshold
            else self.dominance_count(X)
        )
        return epsilon_pareto_set(
            X_reduced, self.epsilon_, sort_key=self.sort_key, counts=counts
        )

    # Couches de dominance
    def layers(self, X: np.ndarray) -> np.ndarray:
        """Assign a non-dominated-sort layer to every row.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Integer array of shape ``(n,)``, 1-based; all zeros when
            ``compute_layers`` is ``False``.
        """
        check_is_fitted(self, "n_features_reduced_")
        X_reduced = self._reduce(X)
        if not self.compute_layers:
            return np.zeros(X_reduced.shape[0], dtype=int)
        return non_dominated_sort_sweep(X_reduced)

    # Comptage de dominance, par blocs au-delà du seuil
    def dominance_count(self, X: np.ndarray) -> np.ndarray:
        """Compute Goldberg's dominance count of every row.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Integer array of shape ``(n,)``.
        """
        check_is_fitted(self, "n_features_reduced_")
        X_reduced = self._reduce(X)
        if X_reduced.shape[0] > self.large_n_threshold:
            return dominance_count_chunked(X_reduced, chunk_size=self.chunk_size)
        return dominance_count(X_reduced)

    # Prédiction : profondeur de dominance normalisée
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Score every row with the quantity selected by ``score``.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Array of shape ``(n,)``: normalised depth ``δ / (n - 1)`` by
            default, the raw count when ``score="dominance_count"``.
        """
        count = self.dominance_count(X)
        if self.score == "dominance_count":
            return count
        n = count.shape[0]
        if n <= 1:
            return np.zeros(n)
        return count / (n - 1)

    # Alias sklearn conventionnel : ajustement puis prédiction sur les mêmes données
    def fit_predict(self, X: np.ndarray, y: None = None) -> np.ndarray:
        """Fit on ``X`` then predict on the same matrix.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.
            y: Ignored, present for sklearn API compatibility.

        Returns:
            Score vector of shape ``(n,)``.
        """
        return self.fit(X, y).predict(X)

    # Alerte : appartenance au front ε (ou au front exact sans epsilon)
    def alert(self, X: np.ndarray) -> np.ndarray:
        """Flag the rows raising an alert.

        Membership of the ε-front, or of the exact front when ``epsilon`` is
        ``None`` — the weight-free alert rule of the workflow.

        Args:
            X: Metric matrix of shape ``(n, d)``, positive polarity.

        Returns:
            Boolean mask of shape ``(n,)``.
        """
        return self.epsilon_front(X)
