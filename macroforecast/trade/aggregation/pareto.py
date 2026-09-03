"""Pareto dominance : ordering without a single coefficient.

Implements §3 of the methodological note — the only construction entirely
free of assumption. Two products are only ordered when *every* metric
agrees; the salient object is the Pareto front, the set of products no other
product dominates on every metric at once.

Every function expects ``X`` already oriented in positive polarity (see
:class:`~macroforecast.trade.aggregation.preprocessing.PolarityOrienter`):
dominance is only meaningful once "higher = more vulnerable" holds uniformly.

Complexity note: the pairwise dominance matrix is ``O(n^2 d)`` in time and
``O(n^2)`` in memory (one boolean matrix, reused across the ``d`` columns) —
the note itself flags ``n ~ 10**4`` as the practical ceiling of this
vectorised approach; a larger ``n`` needs a smarter data structure (e.g. a
divide-and-conquer skyline algorithm) not implemented here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Optional, Union
# Modules de manipulation de données
import numpy as np
from sklearn.utils.validation import check_array


# ──────────────────────────────────────────────────────────────────────
# Matrice de dominance par paires
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul de la matrice de dominance de Pareto
def pareto_dominance_matrix(
    X: np.ndarray, epsilon: Optional[Union[float, np.ndarray]] = None
) -> np.ndarray:
    """Compute the pairwise Pareto-dominance boolean matrix.

    ``D[i, k]`` is ``True`` when product ``i`` dominates product ``k``:
    at least as vulnerable on every metric, strictly more on at least one.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        epsilon: ``None`` for the exact relation of the note (definition 1.4).
            A scalar or a ``(d,)`` array switches to ε-dominance
            (``x_i ≽ε x_k ⟺ ∀j, x_ij ≥ x_kj - ε_j``), which widens the
            dominance cone and shrinks the front — the note's remedy against
            a front that absorbs most of the nomenclature when ``d`` is
            large.

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
    n = X.shape[0]
    # Comparaisons par paires : ge[i, k, j] = X[i, j] >= X[k, j] (+ epsilon)
    threshold = X if epsilon is None else X - np.asarray(epsilon)
    at_least_as_good = np.all(
        X[:, None, :] >= threshold[None, :, :], axis=2
    )
    strictly_better_somewhere = np.any(
        X[:, None, :] > threshold[None, :, :], axis=2
    )
    dominance = at_least_as_good & strictly_better_somewhere
    np.fill_diagonal(dominance, False)
    return dominance


# ──────────────────────────────────────────────────────────────────────
# Front de Pareto
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul du front de Pareto
def pareto_front(X: np.ndarray, epsilon: Optional[Union[float, np.ndarray]] = None) -> np.ndarray:
    """Flag the non-dominated products (the Pareto front ``F1``).

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        epsilon: See :func:`pareto_dominance_matrix`.

    Returns:
        Boolean mask of shape ``(n,)``, ``True`` for a product on the front.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[2.0, 2.0], [1.0, 1.0], [2.0, 1.0]])
        >>> pareto_front(X)
        array([ True, False, False])
    """
    dominance = pareto_dominance_matrix(X, epsilon=epsilon)
    # Non dominé : aucune ligne ne le domine (aucune colonne à True)
    return ~np.any(dominance, axis=0)


# Fonction de calcul du front élargi (ε-dominance)
def epsilon_pareto_front(X: np.ndarray, epsilon: Union[float, np.ndarray]) -> np.ndarray:
    """Convenience alias computing the ε-dominance front.

    Args:
        X: Metric matrix of shape ``(n, d)``, positive polarity.
        epsilon: Scalar or per-metric widening of the dominance cone.

    Returns:
        Boolean mask of shape ``(n,)``, ``True`` for a product on the
        ε-front.

    Examples:
        >>> import numpy as np
        >>> X = np.array([[3.0, 3.0], [2.0, 2.0], [1.0, 1.0]])
        >>> epsilon_pareto_front(X, epsilon=0.5)
        array([ True, False, False])
    """
    return pareto_front(X, epsilon=epsilon)


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


# Fonction de comptage de dominance
def dominance_count(X: np.ndarray) -> np.ndarray:
    """Compute Goldberg's dominance count ``δ(i) = #dominated - #dominators``.

    A finer-grained, weight-free refinement of the partial order: if
    ``x_i ≻ x_k`` then ``δ(i) > δ(k)``, so this score satisfies the
    monotonicity property by construction (property 1.5 of the note) and can
    be used directly as a Pareto-consistent baseline aggregation.

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
