"""Shared column conventions for the metric-aggregation module.

Holds :class:`AggregationConfig`, the only piece of shared state the module
needs: which columns identify a row and which columns are the metrics to
aggregate. Every other hyperparameter (winsorisation quantile, weighting
scheme, aggregation function, bootstrap draws...) lives on the sklearn
estimator that uses it, following the sklearn convention of self-contained,
independently configurable estimators rather than one large configuration
object threaded through every function — appropriate here since each brick
(:mod:`~macroforecast.trade.aggregation.preprocessing`,
:mod:`~macroforecast.trade.aggregation.estimators`) is already an
``sklearn.base.BaseEstimator`` with its own constructor arguments.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
from typing import Mapping, Tuple
# Modules de manipulation de données
import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────────────
# Configuration partagée
# ──────────────────────────────────────────────────────────────────────

# Configuration des conventions de colonnes
@dataclass(frozen=True)
class AggregationConfig:
    """Column conventions shared by every function of the module.

    Attributes:
        id_columns: Columns identifying a row (a product, a reporter x
            product pair...), carried through untouched and never fed to a
            transformer or estimator.
        metric_columns: Columns holding the ``d`` vulnerability metrics to
            aggregate, i.e. the columns of the matrix ``X`` of the
            methodological note.
        polarities: Mapping of metric column to ``+1`` (high value = high
            vulnerability) or ``-1`` (high value = low vulnerability). A
            metric absent from the mapping defaults to ``+1``; only negative
            entries need to be listed.
        high_score_threshold: Score above which a cell is reported as "highly
            concentrated" in the coherence diagnostics (0.5 in the
            literature).
        shares_tolerance: Absolute tolerance used when a score or a share is
            compared to 1 (float equality is never exact).

    Examples:
        >>> config = AggregationConfig(
        ...     id_columns=("product",),
        ...     metric_columns=("HHI", "CDI2"),
        ...     polarities={"CDI2": -1},
        ... )
        >>> polarity_vector(config)
        array([ 1, -1])
    """
    id_columns: Tuple[str, ...]
    metric_columns: Tuple[str, ...]
    polarities: Mapping[str, int] = field(default_factory=dict)
    high_score_threshold: float = 0.5
    shares_tolerance: float = 1e-9


# ──────────────────────────────────────────────────────────────────────
# Conversion DataFrame <-> matrice
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction du vecteur de polarités
def polarity_vector(config: AggregationConfig) -> np.ndarray:
    """Build the ``±1`` polarity vector aligned on ``config.metric_columns``.

    Args:
        config: Column conventions.

    Returns:
        Integer array of shape ``(d,)``, one entry per metric column, ``+1``
        unless the metric is listed in ``config.polarities`` with ``-1``.

    Examples:
        >>> config = AggregationConfig(
        ...     id_columns=(), metric_columns=("HHI", "CDI2", "CDI3"),
        ...     polarities={"CDI2": -1},
        ... )
        >>> polarity_vector(config)
        array([ 1, -1,  1])
    """
    return np.array(
        [config.polarities.get(column, 1) for column in config.metric_columns]
    )


# Fonction de découpage d'un DataFrame en matrice de métriques + index d'identification
def split_frame(
    df_data: pd.DataFrame, config: AggregationConfig
) -> Tuple[np.ndarray, pd.Index]:
    """Split a wide metric table into a numeric matrix and its row identity.

    Every estimator of the module operates on a plain ``(n, d)`` array (the
    sklearn convention); this is the single point where the identifying
    columns of the caller's DataFrame are set aside, so they can be
    reattached later by :func:`attach_scores` without every estimator having
    to know about them.

    Args:
        df_data: Wide table (e.g. the output of
            :func:`~macroforecast.trade.vulnerabilities.run_vulnerabilities`)
            carrying ``config.id_columns`` and ``config.metric_columns``.
        config: Column conventions.

    Returns:
        Tuple ``(X, index)``: the ``(n, d)`` metric matrix, float64, and the
        row index built from ``config.id_columns`` (a single column becomes
        that column's own index; several are combined into a ``MultiIndex``).

    Raises:
        KeyError: If an identifying or metric column is absent from
            ``df_data``.

    Examples:
        >>> df = pd.DataFrame({"product": ["a", "b"], "HHI": [0.5, 0.2]})
        >>> config = AggregationConfig(id_columns=("product",), metric_columns=("HHI",))
        >>> X, index = split_frame(df, config)
        >>> X
        array([[0.5],
               [0.2]])
        >>> list(index)
        ['a', 'b']
    """
    # Vérification des colonnes attendues
    missing = [
        column
        for column in (*config.id_columns, *config.metric_columns)
        if column not in df_data.columns
    ]
    if missing:
        raise KeyError(
            f"Column(s) {missing} are absent from the frame. "
            f"Available columns: {list(df_data.columns)}."
        )

    # Index d'identification : une colonne devient directement l'index,
    # plusieurs sont combinées en MultiIndex
    if len(config.id_columns) == 1:
        index = pd.Index(df_data[config.id_columns[0]].to_numpy())
    else:
        index = pd.MultiIndex.from_frame(df_data[list(config.id_columns)])

    # Matrice numérique des métriques
    values = df_data[list(config.metric_columns)].to_numpy(dtype=float)
    return values, index


# Fonction de ré-attachement d'un vecteur de scores à l'index d'identification
def attach_scores(values: np.ndarray, index: pd.Index, name: str) -> pd.Series:
    """Reattach a score vector to the row identity extracted by :func:`split_frame`.

    Args:
        values: Score array of shape ``(n,)``.
        index: Row identity, as returned by :func:`split_frame`.
        name: Name of the resulting series.

    Returns:
        A ``pandas.Series`` of ``values`` indexed by ``index``.

    Examples:
        >>> index = pd.Index(["a", "b"])
        >>> attach_scores(np.array([0.5, 0.2]), index, "score")
        a    0.5
        b    0.2
        Name: score, dtype: float64
    """
    return pd.Series(np.asarray(values, dtype=float), index=index, name=name)
