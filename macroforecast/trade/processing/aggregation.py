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
retained is to sum quantities *per unit code* and keep the unit carrying the
largest cumulated quantity.

Warning:

    Two known limitations follow, documented at the point where the arbitration
    happens in :func:`aggregate_measures` and reported through a warning:

    1. the quantities of the units *not* retained are abandoned, while the value
       keeps being summed over all of them — so the unit value ``V / Q`` of an
       affected group is overstated;
    2. the arbitration ranks raw cumulated quantities *across* unit codes, which
       are not commensurable: the unit with the largest numeric scale tends to
       win, which biases the choice against the tonne — the very unit the
       downstream chain converges to.

    The practical impact is bounded, because ``netWgt`` is summed over every
    sub-line and :class:`~macroforecast.trade.processing.baci.TonnageConverter`
    prefers it (``prefer_netwgt=True``): the retained ``(quantity, unit)`` pair
    mostly acts as a fallback when the net weight is missing, and as calibration
    sample for the conversion rates.

Notes:
    Every ``pandas.DataFrame`` — argument, attribute or local variable — carries
    the ``df_`` prefix, following the module-wide convention.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
from typing import List, Optional, Sequence
# Modules de manipulation de données
import pandas as pd

# Initialisation du logger
logger = logging.getLogger(__name__)


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

    Warning:
        Groups carrying several unit codes are the only ones where that
        arbitration discards anything, and the two limitations it entails (a
        partial quantity against a complete value, and a ranking across
        incommensurable units) are stated in the module docstring. They are
        **reported, not corrected**: every such group raises a warning naming the
        number of groups concerned and the units met, so their frequency becomes
        observable on real extractions.

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
    df_qty = df_qty.sort_values(qty_col, ascending=False)
    df_dominant = df_qty.drop_duplicates(subset=keys)

    # Arbitrage entre unités concurrentes — seul cas où le dédoublonnage ci-dessus
    # écarte effectivement une quantité, et seul endroit où les deux limites
    # documentées en tête de module se matérialisent :
    #   1. les quantités des unités non retenues sont abandonnées alors que la
    #      valeur, elle, reste sommée sur toutes : la valeur unitaire V/Q du
    #      groupe s'en trouve surestimée ;
    #   2. le classement porte sur des quantités cumulées exprimées dans des
    #      unités incommensurables ; l'unité de plus grande échelle numérique
    #      (articles, litres) tend donc à l'emporter sur la tonne, que la chaîne
    #      aval cherche pourtant à atteindre.
    # Le cas est signalé plutôt que corrigé : le redressement ne s'en trouve pas modifié, 
    # mais sa fréquence devient observable.
    n_discarded = len(df_qty) - len(df_dominant)
    if n_discarded:
        # Calcul du nombre de groupes pour lesquels il y a plusieurs unités
        is_contested = df_qty.duplicated(subset=keys, keep=False)
        n_groups_contested = len(df_qty.loc[is_contested].drop_duplicates(subset=keys))
        units_contested = sorted(
            df_qty.loc[is_contested, qty_unit_col].dropna().unique().tolist(),
            key=str,
        )
        # Logging
        logger.warning(
            f"{n_groups_contested} of {len(df_dominant)} groups carry several "
            f"{qty_unit_col!r} values (units met: {units_contested[:10]}): "
            f"{n_discarded} unit-level quantities are discarded by keeping the "
            f"dominant unit only, while {qty_col!r} stays partial and the summed "
            f"measures stay complete: the unit values of these groups are "
            f"therefore overstated. Ranking cumulated quantities across "
            f"incommensurable units also biases the choice towards the unit of "
            f"largest numeric scale."
        )

    # Jointure gauche : un groupe sans quantité exploitable ressort en NaN
    return df_base.merge(df_dominant, on=keys, how="left")
