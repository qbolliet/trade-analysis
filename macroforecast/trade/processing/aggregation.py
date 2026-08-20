"""Measure-aware re-aggregation of trade declarations.

Holds the single re-aggregation primitive shared by the two places where a trade
table has to be collapsed onto a coarser key:

* :func:`~macroforecast.trade.processing.baci._aggregate_side`, which folds the
  customs/mode sub-dimensions of one declaration side onto the flow grain
  ``(exporter, importer, product, year)``;
* :class:`~macroforecast.trade.processing.classification.HsHarmonizer`, which
  folds the ``n:1`` collisions produced by a downward HS conversion.

Both face the same trap, which is why the logic lives here rather than being
written twice: **aggregation is not symmetric across measures**. Values and net
weights are additive and simply sum, but quantities are not — they carry a unit
code, and summing quantities expressed in litres, items and tonnes would produce
a number that means nothing while looking perfectly healthy. The convention
retained (and used by CEPII BACI) is to sum quantities *per unit code* and keep
the unit carrying the largest cumulated quantity.

Notes:
    Every ``pandas.DataFrame`` — argument, attribute or local variable — carries
    the ``df_`` prefix, following the module-wide convention.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import List, Optional, Sequence
# Modules de manipulation de données
import pandas as pd


# Fonction d'agrégation d'une table de déclarations sur une clé donnée
def aggregate_measures(
    df_data: pd.DataFrame,
    keys: Sequence[str],
    *,
    sum_cols: Sequence[str] = (),
    qty_col: Optional[str] = None,
    qty_unit_col: Optional[str] = None,
    dropna: bool = True,
) -> pd.DataFrame:
    """Collapse a trade table onto ``keys``, one rule per kind of measure.

    Additive measures (values, weights) are summed. The quantity is handled
    apart: it is summed **per unit code**, and the unit carrying the largest
    cumulated quantity is kept for the group — summing heterogeneous units would
    be a silent, severe bug.

    Columns of ``sum_cols``, ``qty_col`` and ``qty_unit_col`` that are absent
    from ``df_data`` are ignored, so callers may pass their full column
    convention without checking what the extraction actually projected.

    Args:
        df_data: Declarations to aggregate.
        keys: Columns forming the target grain.
        sum_cols: Additive measure columns, summed over each group.
        qty_col: Column with the declared quantity; ``None`` disables the
            quantity branch.
        qty_unit_col: Column with the quantity-unit code; ``None`` disables the
            quantity branch.
        dropna: Whether groups with a missing key value are dropped. ``True``
            matches the mirror-flow construction, where an incomplete key makes
            the row unusable; ``False`` must be used whenever the aggregation is
            expected to conserve the aggregate value.

    Returns:
        One row per distinct combination of ``keys``, with the retained
        ``sum_cols``, and — when the quantity branch is active — the quantity
        summed over its dominant unit plus that unit code. Groups holding no
        exploitable quantity carry ``NaN`` in both.

    Raises:
        KeyError: If a column of ``keys`` is absent from ``df_data``.

    Examples:
        >>> df_data = pd.DataFrame(
        ...     {
        ...         "product": ["010121"] * 3,
        ...         "primaryValue": [10.0, 20.0, 5.0],
        ...         "qty": [100.0, 40.0, 30.0],
        ...         "qtyUnitCode": [8, 21, 21],
        ...     }
        ... )
        >>> df_out = aggregate_measures(
        ...     df_data,
        ...     ["product"],
        ...     sum_cols=["primaryValue"],
        ...     qty_col="qty",
        ...     qty_unit_col="qtyUnitCode",
        ... )
        >>> df_out.to_dict("records")
        [{'product': '010121', 'primaryValue': 35.0, 'qtyUnitCode': 8, 'qty': 100.0}]
    """
    # Vérification des clés
    keys = list(keys)
    missing = [key for key in keys if key not in df_data.columns]
    if missing:
        raise KeyError(
            f"Aggregation keys {missing} are absent from the frame. "
            f"Available columns: {list(df_data.columns)}."
        )

    # Résolution des colonnes réellement présentes
    retained_sum_cols: List[str] = [
        col for col in sum_cols if col in df_data.columns and col not in keys
    ]
    has_qty = (
        qty_col is not None
        and qty_unit_col is not None
        and qty_col in df_data.columns
        and qty_unit_col in df_data.columns
    )

    # Agrégation des mesures additives (ou, à défaut, clé seule dédoublonnée)
    if retained_sum_cols:
        df_base = (
            df_data.groupby(keys, dropna=dropna, observed=True)
            .agg({col: "sum" for col in retained_sum_cols})
            .reset_index()
        )
    else:
        df_base = (
            df_data.groupby(keys, dropna=dropna, observed=True)
            .size()
            .reset_index()
            .drop(columns=0)
        )

    # Sortie anticipée en l'absence de branche quantité
    if not has_qty:
        return df_base

    # Quantité : somme par code unité, puis unité de plus grande quantité cumulée
    df_qty = (
        df_data.groupby(keys + [qty_unit_col], dropna=dropna, observed=True)[qty_col]
        .sum()
        .reset_index()
    )
    df_qty = df_qty.sort_values(qty_col, ascending=False).drop_duplicates(subset=keys)

    # Jointure gauche : un groupe sans quantité exploitable ressort en NaN
    return df_base.merge(df_qty, on=keys, how="left")
