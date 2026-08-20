"""BACI reconstruction of bilateral trade flows from raw COMTRADE data.

Implements, step by step, the CEPII *BACI* reconciliation methodology described in
``BACI - Méthodologie détaillée succinte.tex``. Starting from the two mirror
declarations of each elementary flow ``(exporter i, importer j, product k, year t)``
— the exporter's FOB declaration and the importer's CIF declaration — the pipeline
produces a single reconciled **value** and **quantity** on an FOB basis.

The chain follows the note's synthesis order:

1. :class:`TonnageConverter` — convert heterogeneous quantities to tonnes.
2. :class:`CifGravityModel` — estimate CIF (freight) rates by a gravity equation.
3. :class:`Fobizer` — strip the estimated freight from CIF imports.
4. :class:`ReportingQualityModel` — score each country's declaration reliability.
5. :class:`MirrorReconciler` — weighted average of the two mirror declarations.
6. :class:`AreaNesReallocator` — reallocate "Areas NES" flows (optional).

The public entry point :func:`run_baci` applies the whole pipeline to an
already-loaded COMTRADE fact table and returns the reconciled flows. Every
function and class here consumes eager dataframes, column names and parameter
values only — all I/O (DuckLake catalogs, CEPII files, YAML configuration)
belongs to the caller (see ``scripts/process_baci.py``).

Each methodological step takes its parameters as keyword-only arguments with
explicit defaults, following the scikit-learn convention: ``__init__`` stores every
argument unchanged under the same name, derivations belong to ``fit`` and fitted
attributes carry a trailing underscore. :class:`BaciConfig` is only a configuration
façade, consumed by :func:`run_baci`, which distributes its values step by step.

Notes:
    Unlike the ``vulnerabilities`` module (backend-agnostic via narwhals), this
    module works on eager pandas frames throughout: the econometric backends
    (``statsmodels``, ``linearmodels``) are pandas/numpy bound, so a single native
    backend keeps the estimation code straightforward.

    Every ``pandas.DataFrame`` — argument, attribute or local variable — carries the
    ``df_`` prefix; ``pandas.Series`` keep a bare name.

    :class:`TonnageConverter` re-implements, on the mirror flows, the CEPII BACI
    approach to heterogeneous quantity units (implicit rates estimated from
    mirror flows, validated by ``n >= 10`` and ``std < 2.5``). UN Statistics
    separately documents its own procedures for
    `quantity estimation and imputation <https://unstats.un.org/wiki/spaces/I2CG/pages/6325204/E.+Estimation+and+imputation+of+quantity+data>`_
    and for
    `conversion to standard units of quantity <https://unstats.un.org/wiki/spaces/I2CG/pages/6325197/C.+Factors+with+which+to+convert+from+non-standard+to+standard+units+of+quantity>`_,
    sometimes already applied **upstream** of COMTRADE publication. The two
    retreatments can therefore overlap: a quantity converted here may already
    have been estimated or reconciled by UN Statistics before reaching COMTRADE.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
import logging
import math
from typing import Dict, List, Literal, Mapping, Optional, Sequence, Tuple
# Modules de manipulation de données
import numpy as np
import pandas as pd
# Modules économétriques
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import OLSInfluence
from linearmodels.iv import AbsorbingLS

# Initialisation du logger
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

# Conventions de schéma des sources (COMTRADE et CEPII)
@dataclass(frozen=True)
class ComtradeSchema:
    """Column conventions of the COMTRADE fact table and of the CEPII gravity files.

    Carries naming and encoding assumptions only: switching to a differently named
    data source touches this dataclass, never the methodology (see
    :class:`BaciConfig`).

    Attributes:
        flow_col: COMTRADE column holding the trade-flow code.
        import_code: Flow code identifying import declarations (CIF).
        export_code: Flow code identifying export declarations (FOB).
        reporter_iso_col: Column with the reporter ISO-3 code.
        partner_iso_col: Column with the partner ISO-3 code.
        partner_code_col: Column with the numeric (M49) partner code.
        product_col: Column with the product (HS6) code.
        period_col: Column with the period (year as text).
        value_col: Column with the primary trade value (thousands USD).
        qty_col: Column with the declared quantity.
        qty_unit_col: Column with the quantity-unit code.
        netwgt_col: Column with the net weight, expressed in **kilograms** in UN
            Comtrade (see the
            `supplementary quantity units <https://uncomtrade.org/docs/supplementary-quantity-units/>`_
            page and the
            `WCO standard units of quantity overview <https://unstats.un.org/wiki/spaces/I2CG/pages/6325189/A.+An+overview+of+the+World+Customs+Organization+standard+units+of+quantity>`_).
        cif_value_col: Column with the CIF import value, filled only when the
            reporter values its imports CIF (zero otherwise).
        fob_value_col: Column with the FOB value, filled on export declarations
            and on import declarations of reporters valuing their imports FOB
            (or FAS) — zero on CIF import declarations.
        classification_col: Column with the HS classification vintage code
            (e.g. ``"H5"``, ``"H6"``). Used only by
            :func:`_assert_homogeneous_classification` to check that
            ``df_comtrade`` carries a single vintage before any estimation runs
            — :func:`run_baci` never converts between vintages itself (see
            :class:`macroforecast.trade.processing.classification.HsHarmonizer`).
        dist_iso_o_col: CEPII distance column with the origin ISO-3 code.
        dist_iso_d_col: CEPII distance column with the destination ISO-3 code.
        distance_column: CEPII distance column to use (population-weighted).
        contig_col: CEPII contiguity indicator column.
        geo_iso_col: CEPII geography column with the ISO-3 code.
        landlocked_col: CEPII geography landlocked indicator column.

    Examples:
        >>> ComtradeSchema().product_col
        'cmdCode'
        >>> ComtradeSchema(product_col="hs6").product_col
        'hs6'
    """
    # Colonnes et codes COMTRADE
    flow_col: str = "flowCode"
    import_code: str = "M"
    export_code: str = "X"
    reporter_iso_col: str = "reporterISO"
    partner_iso_col: str = "partnerISO"
    partner_code_col: str = "partnerCode"
    product_col: str = "cmdCode"
    period_col: str = "period"
    value_col: str = "primaryValue"
    qty_col: str = "qty"
    qty_unit_col: str = "qtyUnitCode"
    netwgt_col: str = "netWgt"
    cif_value_col: str = "cifvalue"
    fob_value_col: str = "fobvalue"
    classification_col: str = "classificationCode"
    # Colonnes CEPII
    dist_iso_o_col: str = "iso_o"
    dist_iso_d_col: str = "iso_d"
    distance_column: str = "distw"
    contig_col: str = "contig"
    geo_iso_col: str = "iso3"
    landlocked_col: str = "landlocked"


# Paramètres méthodologiques du redressement BACI
@dataclass(frozen=True)
class BaciConfig:
    """Methodological parameters of the BACI pipeline.

    Configuration façade of the pipeline: :func:`run_baci` reads it and hands the
    relevant values to each step as explicit keyword arguments. The steps
    themselves never see this object, so any of them can be reused on its own.

    Attributes:
        schema: Column conventions of the COMTRADE and CEPII sources.
        tonne_conversion_factors: Mapping from COMTRADE quantity-unit code to the
            multiplicative factor converting it to tonnes (e.g. ``{8: 1e-3, 21:
            1.0}``). Quantities in any other unit without a validated conversion
            rate are abandoned (tonnage ``NaN``) while their value is kept.
        min_mirror_flows: Minimum mirror observations to validate a conversion
            rate (``n >= 10``).
        max_conversion_std: Maximum std of the ratios to validate a rate
            (``std < 2.5``).
        prefer_netwgt: When ``True``, use ``netWgt`` as the primary tonnage source
            and fall back to unit conversion only when it is missing.
        cook_factor: Cook's-distance cutoff factor; observations with
            ``cook > cook_factor / n`` are dropped before the final gravity fit.
        cif_share_threshold: CIF share above which an importer is deemed to
            value its imports CIF, in :func:`infer_import_valuation_regime`.
        regime_granularity: Grain of the valuation-regime inference, either
            ``"country_year"`` (dated regime changes) or ``"country"`` (a single
            regime per importer over the whole period).
        fas_countries: Importer ISO-3 codes declaring FAS (freight stripped only
            when it reduces the mirror gap). Kept in configuration because FAS is
            not distinguishable from FOB in the COMTRADE columns, while the
            methodology reserves it a conditional correction.
        excluded_pairs: Country pairs whose internal flows are dropped.
        period_start: First year (included) kept from ``df_comtrade``, applied
            in :func:`build_mirror_flows` before any estimation; ``None`` means
            no lower bound. See :func:`run_baci` for the filtering rationale.
        period_end: Last year (included) kept from ``df_comtrade``; ``None``
            means no upper bound.
        world_partner_code: Numeric partner code of the *World* aggregate
            (rows dropped upfront).
        nes_partner_codes: Numeric partner codes of the "Areas NES" aggregates
            eligible for reallocation.
        nes_skip_codes: Numeric partner codes of aggregates left untouched and
            excluded from the data (e.g. "Other Asia, nes").
        primary_keys: Primary-key columns of the reconciled result table.

    Examples:
        >>> BaciConfig().schema.value_col
        'primaryValue'
        >>> BaciConfig(cook_factor=3.0).cook_factor
        3.0
    """
    # Conventions de schéma des sources
    schema: ComtradeSchema = field(default_factory=ComtradeSchema)
    # Table des facteurs multiplicatifs des unités de poids vers la tonne
    # Défaut mutable interdit dans une dataclass : fabrique dédiée
    tonne_conversion_factors: Mapping[int, float] = field(
        default_factory=lambda: {8: 1e-3, 21: 1.0}
    )
    min_mirror_flows: int = 10
    max_conversion_std: float = 2.5
    prefer_netwgt: bool = True
    # Équation de gravité
    cook_factor: float = 4.0
    # Inférence du régime de valorisation des importations (CAF/FAB)
    cif_share_threshold: float = 0.5
    regime_granularity: Literal["country", "country_year"] = "country_year"
    # Listes de pays (ISO-3)
    fas_countries: Tuple[str, ...] = ("CAN",)
    excluded_pairs: Tuple[Tuple[str, str], ...] = (("BEL", "LUX"),)
    # Périmètre temporel (années incluses) ; None = pas de borne
    period_start: Optional[int] = None
    period_end: Optional[int] = None
    # Zones non spécifiées / agrégats
    world_partner_code: int = 0
    nes_partner_codes: Tuple[int, ...] = (899,)  # 899 = "Areas, nes"
    nes_skip_codes: Tuple[int, ...] = (490,)  # 490 = "Other Asia, nes"
    # Clés primaires du résultat
    primary_keys: Tuple[str, ...] = ("exporter", "importer", "product", "year")


# Configuration par défaut (schéma COMTRADE tariffline + CEPII dist/geo)
DEFAULT_CONFIG = BaciConfig()

# Noms de colonnes canoniques de la table de flux miroirs
_EXP, _IMP, _PROD, _YEAR = "exporter", "importer", "product", "year"

# Libellés des régimes de valorisation des importations
_CIF_REGIME, _FOB_REGIME = "CIF", "FOB"

# ──────────────────────────────────────────────────────────────────────
# Chargement des données (gravité CEPII)
# ──────────────────────────────────────────────────────────────────────

# Fonction de chargement des variables de gravité CEPII
def build_gravity_data(
    df_dist: pd.DataFrame,
    df_geo: pd.DataFrame,
    *,
    dist_iso_o_col: str = "iso_o",
    dist_iso_d_col: str = "iso_d",
    distance_column: str = "distw",
    contig_col: str = "contig",
    geo_iso_col: str = "iso3",
    landlocked_col: str = "landlocked",
) -> pd.DataFrame:
    """Assemble the bilateral CEPII gravity variables.

    Joins the ``dist_cepii`` bilateral table (distance, contiguity) with the
    per-country ``geo_cepii`` landlocked indicator (merged twice, for the origin
    and the destination). Whatever the source column names, the returned frame
    always carries the canonical names ``iso_o``, ``iso_d``, ``distw`` and
    ``contig``.

    Args:
        df_dist: Raw ``dist_cepii`` table, already loaded (e.g. via
            :class:`macroforecast.storage2.Loader`).
        df_geo: Raw ``geo_cepii`` table, already loaded.
        dist_iso_o_col: Distance-table column with the origin ISO-3 code.
        dist_iso_d_col: Distance-table column with the destination ISO-3 code.
        distance_column: Distance-table column to use (population-weighted).
        contig_col: Distance-table contiguity indicator column.
        geo_iso_col: Geography-table column with the ISO-3 code.
        landlocked_col: Geography-table landlocked indicator column.

    Returns:
        A bilateral gravity frame with columns ``iso_o``, ``iso_d``, ``distw``,
        ``contig``, ``landlocked_o`` and ``landlocked_d``.

    Raises:
        KeyError: If an expected CEPII column is absent.

    Examples:
        >>> df_dist = pd.DataFrame(
        ...     {"iso_o": ["FRA"], "iso_d": ["DEU"], "distw": [500.0], "contig": [1]}
        ... )
        >>> df_geo = pd.DataFrame({"iso3": ["FRA", "DEU"], "landlocked": [0, 0]})
        >>> build_gravity_data(df_dist, df_geo).columns.tolist()
        ['iso_o', 'iso_d', 'distw', 'contig', 'landlocked_o', 'landlocked_d']
    """
    # Distance bilatérale + contiguïté
    dist_cols = [
        dist_iso_o_col,
        dist_iso_d_col,
        distance_column,
        contig_col,
    ]
    df_dist = df_dist[dist_cols].copy()

    # Enclavement par pays : géographie dédupliquée (une ligne par ISO-3, la
    # table CEPII listant plusieurs villes par pays)
    df_geo_unique = (
        df_geo[[geo_iso_col, landlocked_col]]
        .drop_duplicates(subset=[geo_iso_col])
        .rename(columns={geo_iso_col: "iso"})
    )

    # Jointure de l'enclavement de l'origine puis de la destination
    df_merged = df_dist.rename(
        columns={
            dist_iso_o_col: "iso_o",
            dist_iso_d_col: "iso_d",
            distance_column: "distw",
            contig_col: "contig",
        }
    )
    df_merged = df_merged.merge(
        df_geo_unique.rename(columns={"iso": "iso_o", landlocked_col: "landlocked_o"}),
        on="iso_o",
        how="left",
    )
    df_merged = df_merged.merge(
        df_geo_unique.rename(columns={"iso": "iso_d", landlocked_col: "landlocked_d"}),
        on="iso_d",
        how="left",
    )

    # Coercition numérique : les fichiers CEPII notent les manquants par un "."
    # (colonnes alors de type object) — conversion en flottant, manquants → NaN.
    for col in ("distw", "contig", "landlocked_o", "landlocked_d"):
        df_merged[col] = pd.to_numeric(df_merged[col], errors="coerce")
    return df_merged


# ──────────────────────────────────────────────────────────────────────
# Construction des flux miroirs
# ──────────────────────────────────────────────────────────────────────

# Fonction d'agrégation d'un côté de déclaration à la maille du flux
def _aggregate_side(
    df_side: pd.DataFrame,
    *,
    exporter_col: str,
    importer_col: str,
    suffix: str,
    product_col: str = "cmdCode",
    value_col: str = "primaryValue",
    qty_col: str = "qty",
    qty_unit_col: str = "qtyUnitCode",
    netwgt_col: str = "netWgt",
) -> pd.DataFrame:
    """Aggregate one declaration side to the ``(i, j, k, t)`` grain.

    Sums value and net weight over the customs/mode sub-dimensions and keeps, for
    the quantity, the unit code carrying the largest summed quantity.

    Args:
        df_side: Declarations of a single flow direction (export or import).
        exporter_col: Column to use as the exporter identity.
        importer_col: Column to use as the importer identity.
        suffix: Suffix appended to the produced value columns (``"x"`` or ``"m"``).
        product_col: Column with the product (HS6) code.
        value_col: Column with the primary trade value.
        qty_col: Column with the declared quantity.
        qty_unit_col: Column with the quantity-unit code.
        netwgt_col: Column with the net weight (kilograms).

    Returns:
        One row per ``(exporter, importer, product, year)`` with value, quantity,
        unit code and net weight columns suffixed by ``suffix``.
    """
    keys = [exporter_col, importer_col, product_col, "_year"]

    # Agrégation valeur + poids net par flux
    df_base = (
        df_side.groupby(keys, dropna=True)
        .agg(
            **{
                f"v_{suffix}": (value_col, "sum"),
                f"nw_{suffix}": (netwgt_col, "sum"),
            }
        )
        .reset_index()
    )

    # Quantité : unité portant la plus grande quantité cumulée par flux
    df_qty = (
        df_side.groupby(keys + [qty_unit_col], dropna=True)[qty_col]
        .sum()
        .reset_index()
    )
    df_qty = df_qty.sort_values(qty_col, ascending=False).drop_duplicates(subset=keys)
    df_qty = df_qty.rename(
        columns={qty_col: f"q_{suffix}", qty_unit_col: f"unit_{suffix}"}
    )

    df_merged = df_base.merge(df_qty, on=keys, how="left")
    # Renommage des identités vers les colonnes canoniques
    return df_merged.rename(
        columns={
            exporter_col: _EXP,
            importer_col: _IMP,
            product_col: _PROD,
            "_year": _YEAR,
        }
    )


# Fonction de construction de la table des flux miroirs
def build_mirror_flows(
    df_comtrade: pd.DataFrame,
    valid_iso: Sequence[str],
    *,
    flow_col: str = "flowCode",
    import_code: str = "M",
    export_code: str = "X",
    reporter_iso_col: str = "reporterISO",
    partner_iso_col: str = "partnerISO",
    partner_code_col: str = "partnerCode",
    product_col: str = "cmdCode",
    period_col: str = "period",
    value_col: str = "primaryValue",
    qty_col: str = "qty",
    qty_unit_col: str = "qtyUnitCode",
    netwgt_col: str = "netWgt",
    world_partner_code: int = 0,
    nes_partner_codes: Tuple[int, ...] = (899,),
    nes_skip_codes: Tuple[int, ...] = (490,),
    excluded_pairs: Tuple[Tuple[str, str], ...] = (("BEL", "LUX"),),
    period_start: Optional[int] = None,
    period_end: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Reshape COMTRADE declarations into a mirror-flow table.

    Keeps only import/export declarations between individual countries, applies
    the geographic exclusions of the note (re-exports of Hong Kong/USA are
    already excluded by keeping only ``M``/``X`` flows; *World* and
    ``nes_skip_codes`` aggregates dropped upfront; internal ``excluded_pairs``
    dropped), and pivots each direction so both mirror declarations of a flow
    ``(exporter i, importer j, product k, year t)`` sit on the same row.
    Quantities are kept whatever their unit: those that cannot be converted to
    tonnes are abandoned later by :class:`TonnageConverter` (value preserved).

    Args:
        df_comtrade: Raw COMTRADE fact-table rows.
        valid_iso: ISO-3 codes of individual countries (from ``geo_cepii``);
            partners outside this set are treated as aggregates.
        flow_col: Column holding the trade-flow code.
        import_code: Flow code identifying import declarations (CIF).
        export_code: Flow code identifying export declarations (FOB).
        reporter_iso_col: Column with the reporter ISO-3 code.
        partner_iso_col: Column with the partner ISO-3 code.
        partner_code_col: Column with the numeric (M49) partner code.
        product_col: Column with the product (HS6) code.
        period_col: Column with the period (year as text).
        value_col: Column with the primary trade value.
        qty_col: Column with the declared quantity.
        qty_unit_col: Column with the quantity-unit code.
        netwgt_col: Column with the net weight (kilograms).
        world_partner_code: Numeric partner code of the *World* aggregate.
        nes_partner_codes: Numeric partner codes of the "Areas NES" aggregates
            eligible for reallocation.
        nes_skip_codes: Numeric partner codes of aggregates dropped upfront.
        excluded_pairs: Country pairs whose internal flows are dropped.
        period_start: First year (included) kept from ``df_comtrade``; ``None``
            means no lower bound.
        period_end: Last year (included) kept from ``df_comtrade``; ``None``
            means no upper bound.

    Returns:
        A tuple ``(df_mirror, df_nes)``:

        * ``df_mirror``: one row per ``(exporter, importer, product, year)`` with
          columns ``v_x, q_x, unit_x, nw_x`` (export/FOB side) and
          ``v_m, q_m, unit_m, nw_m`` (import/CIF side), outer-joined.
        * ``df_nes``: import/export declarations whose partner is an "Areas NES"
          aggregate (``nes_partner_codes``), kept aside for the reallocation step.

    Raises:
        ValueError: If the ``period_start``/``period_end`` filter empties
            ``df_comtrade``.
    """
    # Copiée indépendante du jeu de données
    df_data = df_comtrade.copy()
    # Année entière dérivée de la période (chaîne "YYYY")
    df_data["_year"] = df_data[period_col].astype(str).str[:4].astype(int)

    # Filtre du périmètre temporel — appliqué avant toute estimation (taux de
    # conversion, équation de gravité), sans quoi ils seraient calés sur des
    # années finalement exclues du résultat
    if period_start is not None:
        df_data = df_data[df_data["_year"] >= period_start]
    if period_end is not None:
        df_data = df_data[df_data["_year"] <= period_end]
    if (period_start is not None or period_end is not None) and df_data.empty:
        raise ValueError(
            f"Temporal filter period_start={period_start!r}, "
            f"period_end={period_end!r} emptied df_comtrade before any BACI "
            "estimation could run."
        )

    # Exclusion explicite des agrégats non traités : (ex. Monde, « Other Asia, nes », ni réallouées ni pays)
    df_data = df_data[df_data[partner_code_col] != world_partner_code]
    if nes_skip_codes:
        df_data = df_data[~df_data[partner_code_col].isin(list(nes_skip_codes))]

    # Séparation des déclarations d'export et d'import
    is_export = df_data[flow_col] == export_code
    is_import = df_data[flow_col] == import_code

    valid = set(valid_iso)

    # Flux NES : partenaire agrégé « Areas nes » (avant filtre pays individuels)
    nes_mask = df_data[partner_code_col].isin(list(nes_partner_codes))
    df_nes = df_data[(is_export | is_import) & nes_mask].copy()

    # Restriction aux pays individuels (reporter et partenaire valides ISO-3)
    individual = (
        df_data[reporter_iso_col].isin(valid)
        & df_data[partner_iso_col].isin(valid)
    )
    df_exports = df_data[is_export & individual].copy()
    df_imports = df_data[is_import & individual].copy()

    # Agrégation de chaque côté à la maille du flux
    # Export : exportateur = reporter, importateur = partenaire
    df_x_side = _aggregate_side(
        df_exports,
        exporter_col=reporter_iso_col,
        importer_col=partner_iso_col,
        suffix="x",
        product_col=product_col,
        value_col=value_col,
        qty_col=qty_col,
        qty_unit_col=qty_unit_col,
        netwgt_col=netwgt_col,
    )
    # Import : importateur = reporter, exportateur = partenaire
    df_m_side = _aggregate_side(
        df_imports,
        exporter_col=partner_iso_col,
        importer_col=reporter_iso_col,
        suffix="m",
        product_col=product_col,
        value_col=value_col,
        qty_col=qty_col,
        qty_unit_col=qty_unit_col,
        netwgt_col=netwgt_col,
    )

    # Jointure externe des deux côtés sur la maille du flux
    df_mirror = df_x_side.merge(df_m_side, on=[_EXP, _IMP, _PROD, _YEAR], how="outer")

    # Retrait des paires exclues (flux internes instables, ex. BEL-LUX)
    for a, b in excluded_pairs:
        drop = (
            ((df_mirror[_EXP] == a) & (df_mirror[_IMP] == b))
            | ((df_mirror[_EXP] == b) & (df_mirror[_IMP] == a))
        )
        df_mirror = df_mirror[~drop]

    return df_mirror.reset_index(drop=True), df_nes.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────────────
# Inférence du régime de valorisation des importations (CAF / FAB)
# ──────────────────────────────────────────────────────────────────────

# Détecteur du régime de valorisation des importations
def infer_import_valuation_regime(
    df_comtrade: pd.DataFrame,
    *,
    flow_col: str = "flowCode",
    import_code: str = "M",
    reporter_iso_col: str = "reporterISO",
    period_col: str = "period",
    cif_value_col: str = "cifvalue",
    fob_value_col: str = "fobvalue",
    cif_share_threshold: float = 0.5,
    regime_granularity: Literal["country", "country_year"] = "country_year",
) -> pd.DataFrame:
    """Infer, per importer and year, whether imports are declared CIF or FOB.

    On an import declaration COMTRADE fills either ``cifvalue`` or ``fobvalue``
    and leaves the other at zero, so the valuation regime of a reporter is
    derivable from the data instead of being frozen in a country list. The share
    is weighted **by value** rather than by a row count, so that a handful of
    marginal declarations cannot flip the regime of a reporter::

        cif_share = Σ cifvalue / (Σ cifvalue + Σ fobvalue)
        regime    = "CIF" if cif_share >= cif_share_threshold else "FOB"

    Must be called **before** :func:`build_mirror_flows`, which aggregates the
    two valuation columns away.

    Args:
        df_comtrade: Raw COMTRADE fact-table rows (both flow directions).
        flow_col: Column holding the trade-flow code.
        import_code: Flow code identifying import declarations.
        reporter_iso_col: Column with the reporter ISO-3 code.
        period_col: Column with the period (year as text).
        cif_value_col: Column with the CIF import value.
        fob_value_col: Column with the FOB value.
        cif_share_threshold: CIF share above which the importer is deemed CIF.
        regime_granularity: ``"country_year"`` for a dated regime, ``"country"``
            for a single regime per importer (value sums cumulated over the whole
            period, then broadcast over each observed year).

    Returns:
        One row per observed ``(importer, year)`` with columns ``importer``,
        ``year``, ``cif_share``, ``n_declarations`` and ``regime``. When no
        valuation information is available (``Σ cifvalue + Σ fobvalue == 0``),
        ``cif_share`` is ``NaN`` and the regime is ``"CIF"``: this is BACI's
        historical behaviour, and missing information must not silently disable
        the fobization.

    Raises:
        KeyError: If one of the expected COMTRADE columns is absent.
        ValueError: If ``regime_granularity`` is neither ``"country"`` nor
            ``"country_year"``.

    Examples:
        >>> df_comtrade = pd.DataFrame(
        ...     {
        ...         "flowCode": ["M", "M", "M", "X"],
        ...         "reporterISO": ["AAA", "BBB", "CCC", "AAA"],
        ...         "period": ["2020", "2020", "2020", "2020"],
        ...         "cifvalue": [100.0, 0.0, 0.0, 0.0],
        ...         "fobvalue": [0.0, 100.0, 0.0, 500.0],
        ...     }
        ... )
        >>> infer_import_valuation_regime(df_comtrade)
          importer  year  cif_share  n_declarations regime
        0      AAA  2020        1.0               1    CIF
        1      BBB  2020        0.0               1    FOB
        2      CCC  2020        NaN               1    CIF
    """
    # Vérification de la présence des colonnes nécessaires
    needed = list(
        dict.fromkeys(
            [flow_col, reporter_iso_col, period_col, cif_value_col, fob_value_col]
        )
    )
    missing = [col for col in needed if col not in df_comtrade.columns]
    if missing:
        raise KeyError(
            "Missing COMTRADE columns for import valuation-regime inference: "
            f"{missing}"
        )
    # Vérification de la granularité demandée
    if regime_granularity not in ("country", "country_year"):
        raise ValueError(
            "regime_granularity must be 'country' or 'country_year', "
            f"got {regime_granularity!r}"
        )

    # Restriction aux déclarations d'importation : elles seules portent le régime
    df_imports = df_comtrade.loc[df_comtrade[flow_col] == import_code, needed].copy()

    # Jeu sans déclaration d'importation : sortie vide au schéma attendu
    if df_imports.empty:
        return pd.DataFrame(
            {
                _IMP: pd.Series(dtype=object),
                _YEAR: pd.Series(dtype=int),
                "cif_share": pd.Series(dtype=float),
                "n_declarations": pd.Series(dtype=int),
                "regime": pd.Series(dtype=object),
            }
        )

    # Année entière dérivée de la période (chaîne "YYYY"), convention partagée
    # avec build_mirror_flows
    df_imports[_YEAR] = df_imports[period_col].astype(str).str[:4].astype(int)
    # Coercition numérique des valeurs déclarées : manquants comptés pour zéro
    for col in (cif_value_col, fob_value_col):
        df_imports[col] = pd.to_numeric(df_imports[col], errors="coerce").fillna(0.0)

    # Agrégation par (importateur, année) : pondération par la valeur déclarée
    df_regime = (
        df_imports.groupby([reporter_iso_col, _YEAR], dropna=True)
        .agg(
            _cif=(cif_value_col, "sum"),
            _fob=(fob_value_col, "sum"),
            n_declarations=(flow_col, "size"),
        )
        .reset_index()
        .rename(columns={reporter_iso_col: _IMP})
    )

    # Granularité pays : cumul sur l'ensemble des années, puis diffusion des
    # valeurs pays sur chaque année observée (schéma de sortie inchangé)
    if regime_granularity == "country":
        df_country = df_regime.groupby(_IMP, as_index=False)[
            ["_cif", "_fob", "n_declarations"]
        ].sum()
        df_regime = df_regime[[_IMP, _YEAR]].merge(df_country, on=_IMP, how="left")

    # Part CIF de la valeur déclarée ; total nul → aucune information (NaN)
    total = df_regime["_cif"] + df_regime["_fob"]
    df_regime["cif_share"] = df_regime["_cif"].div(total.where(total > 0))

    # Régime : CIF au-dessus du seuil, et CIF également en l'absence
    # d'information (ne pas désactiver silencieusement la fobisation)
    is_cif = (df_regime["cif_share"] >= cif_share_threshold) | df_regime[
        "cif_share"
    ].isna()
    df_regime["regime"] = np.where(is_cif, _CIF_REGIME, _FOB_REGIME)

    return (
        df_regime[[_IMP, _YEAR, "cif_share", "n_declarations", "regime"]]
        .sort_values([_IMP, _YEAR])
        .reset_index(drop=True)
    )


# Fonction d'alignement du régime inféré sur les flux miroirs
def _resolve_regimes(df_mirror: pd.DataFrame, df_regime: pd.DataFrame) -> pd.Series:
    """Align the inferred valuation regime on each mirror flow.

    Falls back, for an ``(importer, year)`` pair absent from the inference, on
    the importer's modal regime, and on ``"CIF"`` when the importer is unknown
    altogether — missing information never disables the fobization.

    Args:
        df_mirror: Mirror-flow table, keyed by ``importer`` and ``year``.
        df_regime: Inference produced by :func:`infer_import_valuation_regime`.

    Returns:
        The valuation regime of each mirror flow, indexed like ``df_mirror``.
    """
    # Inférence vide : comportement historique de BACI (tous CIF)
    if df_regime is None or df_regime.empty:
        return pd.Series(_CIF_REGIME, index=df_mirror.index, dtype=object)

    # Jointure sur (importateur, année) — clés dédupliquées par précaution
    df_keys = df_regime[[_IMP, _YEAR, "regime"]].drop_duplicates(subset=[_IMP, _YEAR])
    df_joined = df_mirror[[_IMP, _YEAR]].merge(df_keys, on=[_IMP, _YEAR], how="left")
    regime = pd.Series(
        df_joined["regime"].to_numpy(), index=df_mirror.index, dtype=object
    )

    # Repli sur le régime modal du pays pour les années non renseignées
    # (égalité tranchée vers CAF, régime par défaut)
    if regime.isna().any():
        modal = df_regime.groupby(_IMP)["regime"].agg(
            lambda regimes: _CIF_REGIME
            if (regimes == _CIF_REGIME).sum() >= (regimes == _FOB_REGIME).sum()
            else _FOB_REGIME
        )
        regime = regime.where(regime.notna(), df_mirror[_IMP].map(modal))

    # Importateur totalement absent de l'inférence : CAF par défaut
    return regime.fillna(_CIF_REGIME)



# ──────────────────────────────────────────────────────────────────────
# Étape 1 — Conversion des quantités en tonnes
# ──────────────────────────────────────────────────────────────────────

# Code unité COMTRADE de la tonne (annoté 21 dans la documentation publique,
# qui n'expose que les codes 1 à 13 ; NON CONFIRMÉ contre la codelist vivante
# ComtradeClient().get_metadata(category="qtyunit"), qui exige une clé
# d'abonnement — cf. DEC-2 dans ARCHITECTURE.md).
_TONNE_UNIT_CODE = 21



# Estimateur des taux de conversion vers la tonne
class TonnageConverter:
    """Convert heterogeneous declared quantities to tonnes.

    Estimates, per ``(product, source unit)``, an implicit conversion rate from
    mirror flows where one partner declares in tonnes and the other in the source
    unit — the CEPII BACI methodology. A rate is validated only when at least
    ``min_mirror_flows`` observations are available and their std is below
    ``max_conversion_std``.

    Args:
        tonne_conversion_factors: Mapping from COMTRADE quantity-unit code to
            the multiplicative factor converting it to tonnes (e.g. ``{8: 1e-3,
            21: 1.0}``). Every code present in
            this table is excluded from :meth:`fit`'s ratio collection: a
            quantity already known in a target unit must never serve as the
            numerator estimating a rate towards that same unit.
        min_mirror_flows: Minimum mirror observations to validate a conversion
            rate (``n >= 10``).
        max_conversion_std: Maximum std of the ratios to validate a rate
            (``std < 2.5``).
        prefer_netwgt: When ``True``, use ``netWgt`` as the primary tonnage source
            and fall back to unit conversion only when it is missing.

    Attributes:
        conversion_rates_: Mapping ``(product, unit) -> rate`` (tonnes per unit),
            populated by :meth:`fit`.
    """

    # Initialisation
    def __init__(
        self,
        *,
        tonne_conversion_factors: Optional[Mapping[int, float]] = { 8 : 1e-3 , 21 : 1 },
        min_mirror_flows: int = 10,
        max_conversion_std: float = 2.5,
        prefer_netwgt: bool = True,
    ) -> None:
        # Initialisation des attributs
        self.tonne_conversion_factors = dict(tonne_conversion_factors)
        self.min_mirror_flows = min_mirror_flows
        self.max_conversion_std = max_conversion_std
        self.prefer_netwgt = prefer_netwgt

    # Méthode auxiliaire : quantité en tonnes déjà connue (poids ou unité tonne)
    def _tonnes_from_weight(self, qty: pd.Series, unit: pd.Series, nw: pd.Series) -> pd.Series:
        """Return the tonnage known without any estimated rate.

        Args:
            qty: Declared quantities.
            unit: Declared quantity-unit codes.
            nw: Net weights (kilograms).

        Returns:
            Tonnage from net weight (preferred) or from quantities already
            expressed in a unit covered by ``tonne_conversion_factors``
            (weight units, or the tonne unit itself); ``NaN`` where neither is
            available.
        """
        # Initialisation de la série des valeurs en tonnes
        tonnes = pd.Series(np.nan, index=qty.index, dtype="float64")
        # Repli/priorité sur le poids net (kg → tonnes)
        if self.prefer_netwgt:
            tonnes = tonnes.where(~(nw > 0), nw * 1e-3)
        # Quantités déjà exprimées dans une unité couverte par la table de facteurs
        # (poids en kg, tonnes elles-mêmes, ou tout autre code y figurant)
        factor = unit.map(self.tonne_conversion_factors)
        tonnes = tonnes.where(~(tonnes.isna() & factor.notna()), qty * factor)
        return tonnes

    # Estimation des taux de conversion
    def fit(self, df_mirror: pd.DataFrame) -> "TonnageConverter":
        """Estimate per-``(product, unit)`` conversion rates from mirror flows.

        Args:
            df_mirror: Mirror-flow table from :func:`build_mirror_flows`.

        Returns:
            The fitted converter (``self``).
        """
        # Initialisation du dictionnaire des taux de conversion
        rates: Dict[Tuple[str, int], float] = {}

        # Tonnage connu (poids) de chaque côté, sans taux estimé
        t_x = self._tonnes_from_weight(df_mirror["q_x"], df_mirror["unit_x"], df_mirror["nw_x"])
        t_m = self._tonnes_from_weight(df_mirror["q_m"], df_mirror["unit_m"], df_mirror["nw_m"])

        # Observations du taux : un côté connu en tonnes, l'autre en unité source
        records: List[Tuple[str, int, float]] = []
        # Côté export en tonnes, import en unité source
        _collect_ratio(
            records,
            df_mirror["q_m"],
            df_mirror["unit_m"],
            t_x,
            df_mirror[_PROD],
            tonne_conversion_factors=self.tonne_conversion_factors,
        )
        # Côté import en tonnes, export en unité source
        _collect_ratio(
            records,
            df_mirror["q_x"],
            df_mirror["unit_x"],
            t_m,
            df_mirror[_PROD],
            tonne_conversion_factors=self.tonne_conversion_factors,
        )

        if records:
            df_ratios = pd.DataFrame(records, columns=[_PROD, "unit", "ratio"])
            # Filtres de validité : n ≥ 10 et écart-type < 2,5
            grouped = df_ratios.groupby([_PROD, "unit"])["ratio"]
            df_stats = grouped.agg(["mean", "std", "count"]).reset_index()
            df_valid = df_stats[
                (df_stats["count"] >= self.min_mirror_flows)
                & (df_stats["std"] < self.max_conversion_std)
            ]
            rates = {
                (row[_PROD], int(row["unit"])): row["mean"]
                for _, row in df_valid.iterrows()
            }

        # Mise à jour des taux de conversion
        self.conversion_rates_ = rates

        # Logging
        logger.info("TonnageConverter: %d taux de conversion validés", len(rates))

        return self

    # Application des taux : ajout des quantités en tonnes
    def transform(self, df_mirror: pd.DataFrame) -> pd.DataFrame:
        """Add tonne-denominated quantities ``q_x_t`` and ``q_m_t``.

        Args:
            df_mirror: Mirror-flow table.

        Returns:
            The frame with two added columns ``q_x_t`` and ``q_m_t`` (tonnes),
            ``NaN`` when no source (weight or validated rate) is available.

        Raises:
            AttributeError: If called before :meth:`fit`.
        """
        # Copie indépendante du jeu de données
        df_out = df_mirror.copy()
        # Ajout de colonnes exprimant les quantités en tonnes (à l'import et à l'export)
        df_out["q_x_t"] = self._to_tonnes(
            df_out["q_x"], df_out["unit_x"], df_out["nw_x"], df_out[_PROD]
        )
        df_out["q_m_t"] = self._to_tonnes(
            df_out["q_m"], df_out["unit_m"], df_out["nw_m"], df_out[_PROD]
        )

        return df_out

    # Méthode auxiliaire : conversion complète d'un côté vers la tonne
    def _to_tonnes(
        self, qty: pd.Series, unit: pd.Series, nw: pd.Series, product: pd.Series
    ) -> pd.Series:
        """Convert one side to tonnes, using weight then estimated rates.

        Args:
            qty: Declared quantities.
            unit: Quantity-unit codes.
            nw: Net weights (kilograms).
            product: Product codes (for the rate lookup).

        Returns:
            Tonnage series (``NaN`` where no source is available).
        """
        # Tonnage connu (poids net ou unité de poids)
        tonnes = self._tonnes_from_weight(qty, unit, nw)
        # Complément par les taux estimés (product, unit)
        missing = tonnes.isna() & qty.notna() & unit.notna()
        # /!\ LOG MLFLOW : Logger la proportion totale d'observations converties en tonnes depuis d'autres unités (que la tonne ou le kilogramme)
        if missing.any() and self.conversion_rates_:
            keys = list(zip(product[missing], unit[missing].astype("Int64")))
            rate = np.array(
                [self.conversion_rates_.get((p, int(u)) if pd.notna(u) else None, np.nan)
                 for p, u in keys],
                dtype="float64",
            )
            tonnes.loc[missing] = qty[missing].to_numpy() * rate
        return tonnes


# Fonction auxiliaire : collecte des rapports tonnes/unité source
def _collect_ratio(
    records: List[Tuple[str, int, float]],
    qty_unit: pd.Series,
    unit_unit: pd.Series,
    tonnes_other: pd.Series,
    product: pd.Series,
    *,
    tonne_conversion_factors: Mapping[int, float],
) -> None:
    """Append ``(product, unit, ratio)`` observations to ``records`` in place.

    An observation exists when the *other* side is known in tonnes and the current
    side is in a unit with no known factor to tonnes: ``ratio = tonnes_other /
    qty_unit``.

    Args:
        records: Accumulator list mutated in place.
        qty_unit: Quantities declared in a heterogeneous (source) unit.
        unit_unit: Unit codes of ``qty_unit``.
        tonnes_other: Tonnage of the mirror partner (the tonnes side).
        product: Product codes.
        tonne_conversion_factors: Quantity-unit codes with an already-known
            factor to tonnes (weight units, and the tonne unit itself). All of
            them are excluded from the ratio collection — a quantity already
            expressed in (or convertible to) tonnes must never serve as the
            numerator estimating a rate towards that same target unit.
    """
    # Côté source dépourvu de facteur connu vers la tonne et quantité strictement positive
    is_source = (~unit_unit.isin(list(tonne_conversion_factors))) & (qty_unit > 0)
    mask = is_source & (tonnes_other > 0)
    if not mask.any():
        return
    # Ajout des ratios calculés à la liste en entrée
    ratio = tonnes_other[mask] / qty_unit[mask]
    for p, u, r in zip(product[mask], unit_unit[mask], ratio):
        if pd.notna(u) and np.isfinite(r):
            records.append((p, int(u), float(r)))


# ──────────────────────────────────────────────────────────────────────
# Étape 2 — Estimation des taux CAF par équation de gravité
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul des valeurs unitaires médianes mondiales par produit
def world_median_unit_values(df_mirror: pd.DataFrame) -> pd.Series:
    """Compute the world median unit value ``UV^k`` per product.

    Uses the export (FOB) unit value ``v_x / q_x_t`` across all flows, a proxy of
    the product's transportability.

    Args:
        df_mirror: Mirror-flow table with tonne quantities (``q_x_t``).

    Returns:
        Series of median unit values indexed by product.
    """
    # Valeur unitaire export sur flux exploitables
    uv = df_mirror["v_x"] / df_mirror["q_x_t"]
    # Extraction des valeurs unitaires valides non nulles
    df_valid = df_mirror.loc[(df_mirror["v_x"] > 0) & (df_mirror["q_x_t"] > 0), [_PROD]].copy()
    df_valid["uv"] = uv[df_valid.index]
    # Calcul de la médiane par produit
    return df_valid.groupby(_PROD)["uv"].median()


# Estimateur de l'équation de gravité des taux CAF
# /!\ LOG MLFLOW : Logguer le taux de fret moyen ou peut-être faire cela après le retraitement dans "run_baci" et logger "mean_freight_rate". pourquoi cette ligne de retraitement est-t-elle faite hors de cette fonction d'ailleurs ? Ne serait-il pas préférable de l'inclure ici ?
class CifGravityModel:
    """Estimate CIF (freight) rates by a weighted gravity equation.

    The dependent variable is ``ln(UVm / UVx)`` on complete mirror flows;
    regressors are ``ln distw``, ``(ln distw)^2``, contiguity, exporter/importer
    landlocked indicators, ``ln UV^k`` and year dummies. Estimation is a weighted
    least squares (weight ``min(Q)/max(Q)``), robustified by dropping influential
    observations flagged by Cook's distance — computed on the ``√w``-whitened
    model so the weights are accounted for — before the final fit. The predicted
    value ``exp(X β̂)`` estimates ``1 + τ`` (see :meth:`predict`).

    Args:
        cook_factor: Cook's-distance cutoff factor; observations with
            ``cook > cook_factor / n`` are dropped before the final fit.

    Attributes:
        result_: Fitted ``statsmodels`` WLS results.
        design_columns_: Ordered design-matrix columns (excluding the constant).
        uv_world_: World median unit values used at fit time.
    """

    # Initialisation
    def __init__(self, *, cook_factor: float = 4.0) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.cook_factor = cook_factor

    # Construction de la matrice de design (variables explicatives)
    def _design(self, df_flows: pd.DataFrame, df_gravity: pd.DataFrame) -> pd.DataFrame:
        """Build the gravity design frame (regressors only).

        Args:
            df_flows: Flows with tonne quantities and the mirror identities.
            df_gravity: Bilateral gravity frame from :func:`build_gravity_data`.

        Returns:
            A design frame indexed like ``df_flows`` with the gravity regressors
            and year dummies; rows with missing distance are dropped.
        """
        # Jointure des variables de gravité (exportateur = origine, importateur = destination)
        df_merged = df_flows.merge(
            df_gravity.rename(columns={"iso_o": _EXP, "iso_d": _IMP}),
            on=[_EXP, _IMP],
            how="left",
        )
        # Valeur unitaire médiane mondiale du produit
        df_merged["uv_world"] = df_merged[_PROD].map(self.uv_world_)

        # Variables transformées (logarithmes protégés)
        df_design = pd.DataFrame(index=df_merged.index)
        df_design["ln_dist"] = np.log(df_merged["distw"])
        df_design["ln_dist2"] = df_design["ln_dist"] ** 2
        df_design["contig"] = df_merged["contig"].astype("float64")
        df_design["landlocked_i"] = df_merged["landlocked_o"].astype("float64")
        df_design["landlocked_j"] = df_merged["landlocked_d"].astype("float64")
        df_design["ln_uv"] = np.log(df_merged["uv_world"])
        # Indicatrices d'année (référence = première année)
        df_year_dummies = pd.get_dummies(
            df_merged[_YEAR], prefix="year", drop_first=True
        ).astype("float64")
        df_design = pd.concat([df_design, df_year_dummies], axis=1)
        return df_design

    # Estimation du modèle
    def fit(self, df_mirror: pd.DataFrame, df_gravity: pd.DataFrame) -> "CifGravityModel":
        """Estimate the gravity equation on complete mirror flows.

        Args:
            df_mirror: Mirror-flow table with tonne quantities.
            df_gravity: Bilateral gravity frame.

        Returns:
            The fitted model (``self``).

        Raises:
            ValueError: If no usable observation remains after filtering.
        """
        # Valeurs unitaires mondiales calées sur l'échantillon
        self.uv_world_ = world_median_unit_values(df_mirror)

        # Échantillon d'estimation : flux miroirs complets exploitables
        df_sample = df_mirror[
            (df_mirror["v_x"] > 0)
            & (df_mirror["v_m"] > 0)
            & (df_mirror["q_x_t"] > 0)
            & (df_mirror["q_m_t"] > 0)
        ].copy()

        # Variable dépendante : log du rapport des valeurs unitaires (CAF/FAB)
        uv_x = df_sample["v_x"] / df_sample["q_x_t"]
        uv_m = df_sample["v_m"] / df_sample["q_m_t"]
        y = np.log(uv_m / uv_x)

        # Pondération : rapport des quantités miroirs min/max ∈ (0, 1]
        q_min = np.minimum(df_sample["q_x_t"], df_sample["q_m_t"])
        q_max = np.maximum(df_sample["q_x_t"], df_sample["q_m_t"])
        weights = (q_min / q_max).to_numpy()

        # Matrice de design
        # Variable explicatives
        df_design = self._design(df_sample, df_gravity)
        # Ajout de la variable dépendante
        df_design["_y"] = y.to_numpy()
        # Ajout des poids
        df_design["_w"] = weights
        # Retrait des lignes non exploitables (gravité/UV manquants, y non fini)
        df_design = df_design.replace([np.inf, -np.inf], np.nan).dropna()
        if df_design.empty:
            raise ValueError("No usable observation for the gravity estimation")

        # Extraction de la variable dépendante
        y_fit = df_design.pop("_y")
        # Extraction des poids
        w_fit = df_design.pop("_w")
        # Enumération des colonnes correspondant aux variables explicatives
        self.design_columns_ = list(df_design.columns)
        # Ajout d'une constante
        df_x = sm.add_constant(df_design, has_constant="add")

        # Première estimation WLS
        model = sm.WLS(y_fit, df_x, weights=w_fit)
        res = model.fit()

        # Robustesse : retrait des observations influentes (distance de Cook)
        try:
            # Influence calculée sur le modèle blanchi (OLS sur données
            # transformées par √w, équivalent exact du WLS) afin que la
            # distance de Cook tienne compte des poids
            sqrt_w = np.sqrt(np.asarray(w_fit, dtype="float64"))
            whitened = sm.OLS(
                np.asarray(y_fit, dtype="float64") * sqrt_w,
                df_x.to_numpy(dtype="float64") * sqrt_w[:, None],
            ).fit()
            # Calcul de a distance de cook
            cook = OLSInfluence(whitened).cooks_distance[0]
            # Détermination du seuil de décision
            cutoff = self.cook_factor / len(cook)
            # Conservation des observations qui satisfont le seuil
            keep = cook < cutoff
            if keep.sum() > df_x.shape[1] and (~keep).any():
                res = sm.WLS(y_fit[keep], df_x[keep], weights=w_fit[keep]).fit()
                # Logging
                logger.info(
                    "CifGravityModel: %d/%d observations influentes retirées (Cook)",
                    int((~keep).sum()), len(cook),
                )
        except Exception as exc:  # pragma: no cover - robustesse numérique
            # Logging
            logger.warning("Cook's distance skipped: %s", exc)

        # Sauvegarde du résultat
        self.result_ = res
        # Logging
        logger.info(
            "CifGravityModel: fit sur %d observations, taux de fret moyen %.3f",
            int(res.nobs), float(np.expm1(res.fittedvalues).clip(lower=0).mean()),
        )
        return self

    # Prédiction du taux de fret
    def predict(self, df_mirror: pd.DataFrame, df_gravity: pd.DataFrame) -> pd.Series:
        """Predict the freight rate ``exp(X β̂)`` for each flow.

        Args:
            df_mirror: Mirror-flow table with tonne quantities.
            df_gravity: Bilateral gravity frame.

        Returns:
            Series of estimated freight rates ``τ̂`` aligned on ``df_mirror``
            (``NaN`` where gravity variables are missing). The dependent variable
            is ``ln(UVm/UVx) = ln(1 + τ)``, so the freight rate is
            ``τ̂ = exp(X β̂) − 1`` and the fobisation divides by ``1 + τ̂``.

        Raises:
            AttributeError: If called before :meth:`fit`.
        """
        # Construction du design puis alignement sur les colonnes d'estimation
        df_design = self._design(df_mirror, df_gravity)
        df_design = df_design.reindex(columns=self.design_columns_, fill_value=0.0)
        df_x = sm.add_constant(df_design, has_constant="add")
        pred = self.result_.predict(df_x)
        # La régression prédit ln(1 + τ) : le taux de fret est exp(prédiction) − 1
        return np.expm1(pred)


# ──────────────────────────────────────────────────────────────────────
# Étape 3 — Retrait du fret des importations (fobisation)
# ──────────────────────────────────────────────────────────────────────

# Transformateur de fobisation des valeurs d'importation
# LOG MLFLOW : Logger la part du nombre total de flux qui est in fine corrigé ainsi que la part des flux d'importation auxquels est appliquée une correction
class Fobizer:
    """Strip the estimated freight from CIF import values.

    Computes ``V_m_fob = V_m / (1 + cif_rate)`` under the note's safeguards, in
    strict priority order: conditional correction for FAS importers (kept only
    when it reduces the mirror gap), no correction for importers whose inferred
    valuation regime is FOB, unconditional correction floored at zero otherwise.

    The regime itself is no longer a parameter: it is inferred from the data by
    :func:`infer_import_valuation_regime` and handed to :meth:`transform`.

    Args:
        fas_countries: Importer ISO-3 codes declaring FAS (freight stripped only
            when it reduces the mirror gap). FAS is not distinguishable from FOB
            in the COMTRADE valuation columns — a FAS reporter fills
            ``fobvalue`` — while the methodology reserves it a *conditional*
            correction rather than no correction at all, hence this list stays in
            configuration and takes priority over the inferred regime.
    """

    # Initialisation
    def __init__(
        self,
        *,
        fas_countries: Tuple[str, ...] = ("CAN",),
    ) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.fas_countries = fas_countries

    # Application de la fobisation
    def transform(
        self,
        df_mirror: pd.DataFrame,
        cif_rate: pd.Series,
        df_regime: pd.DataFrame,
    ) -> pd.DataFrame:
        """Add the FOB-equivalent import value ``v_m_fob``.

        Args:
            df_mirror: Mirror-flow table.
            cif_rate: Estimated freight rate per flow (from
                :meth:`CifGravityModel.predict`).
            df_regime: Inferred import valuation regime, as returned by
                :func:`infer_import_valuation_regime`. An empty frame means no
                inference is available and every importer is treated as CIF.

        Returns:
            The frame with an added ``v_m_fob`` column.
        """
        # Copie indépendante des données
        df_out = df_mirror.copy()
        # Extraction de la valeur CIF (qui correspond généralement aux données d'importation)
        v_m = df_out["v_m"]

        # Valeur fobisée candidate (plancher à zéro), taux manquant → pas de correction
        rate = cif_rate.reindex(df_out.index)
        v_m_fob = np.where(rate.notna(), v_m / (1.0 + rate), v_m)
        v_m_fob = np.clip(v_m_fob, a_min=0.0, a_max=None)
        candidate = pd.Series(v_m_fob, index=df_out.index)

        # Régime de valorisation inféré, aligné sur les flux miroirs
        regime = _resolve_regimes(df_out, df_regime)

        # Priorité 1 — importateurs FAS : traitement conditionnel appliqué quel
        # que soit le régime inféré (FAS n'est pas distinguable de FAB dans les
        # colonnes COMTRADE, alors que la note lui réserve ce traitement)
        fas = df_out[_IMP].isin(list(self.fas_countries))

        # Priorité 2 — régime inféré FAB hors pays FAS : aucune correction
        # Priorité 3 — régime CAF : correction inconditionnelle (valeur candidate)
        no_correction = ~fas & (regime == _FOB_REGIME)
        candidate = candidate.where(~no_correction, v_m)

        # Correction FAS conservée seulement si elle réduit l'écart miroir
        if fas.any():
            has_mirror = (df_out["v_x"] > 0) & (v_m > 0)
            gap_before = np.abs(np.log(df_out["v_x"] / v_m))
            gap_after = np.abs(np.log(df_out["v_x"] / candidate.replace(0.0, np.nan)))
            # Ne garder la correction FAS que lorsqu'elle réduit l'écart
            revert = fas & has_mirror & ~(gap_after < gap_before)
            candidate = candidate.where(~revert, v_m)

        df_out["v_m_fob"] = candidate
        return df_out


# ──────────────────────────────────────────────────────────────────────
# Étape 4 — Évaluation de la qualité de déclaration (ANOVA)
# ──────────────────────────────────────────────────────────────────────

# Résultat d'une estimation de qualité : variances par pays
@dataclass
class QualityResult:
    """Per-country reporting-quality variances.

    Attributes:
        sigma_export: Estimated ``σ̂`` per country acting as exporter (indexed by
            ISO-3 code).
        sigma_import: Estimated ``σ̂`` per country acting as importer.
    """
    sigma_export: pd.Series
    sigma_import: pd.Series

# /!\ MLFLOW LOG : Logger la part des observations ayant deux flux miroir (peut-être le logger dans run_baci àau niveau de cette étape ou d'une autre)
# Estimateur de la qualité de déclaration par ANOVA
class ReportingQualityModel:
    """Estimate reporting quality by a weighted ANOVA.

    Decomposes the reporting distance ``RD = |ln(V_i / V_j)|`` into additive
    exporter, importer and year fixed effects, the ~5000-modality product
    dimension being absorbed by a within transformation (``linearmodels``'
    ``AbsorbingLS``). A single fit yields both the exporter and importer
    effects, re-expressed in sum-to-zero coding with proper contrast standard
    errors. Observations are weighted by ``ln(V_i + V_j)`` computed on the
    declared *values* for both targets, as in the note. Per-country marginal
    means are turned into standard deviations ``σ̂``.

    The step works on the canonical mirror-flow columns only, so it carries no
    methodological parameter.
    """

    # Estimation pour une cible (valeurs ou quantités)
    def fit(self, df_mirror: pd.DataFrame, target: str) -> QualityResult:
        """Estimate per-country variances for a target quantity.

        Args:
            df_mirror: Mirror-flow table with ``v_x``, ``v_m_fob`` (and tonne
                quantities ``q_x_t``, ``q_m_t``).
            target: ``"value"`` (uses ``v_x`` vs ``v_m_fob``) or ``"quantity"``
                (uses ``q_x_t`` vs ``q_m_t``).

        Returns:
            A :class:`QualityResult` with exporter and importer ``σ̂`` series.

        Raises:
            ValueError: If ``target`` is not ``"value"`` or ``"quantity"``.
        """
        # Sélection des colonnes de flux selon la cible
        if target == "value":
            v_i, v_j = df_mirror["v_x"], df_mirror["v_m_fob"]
        elif target == "quantity":
            v_i, v_j = df_mirror["q_x_t"], df_mirror["q_m_t"]
        else:
            raise ValueError("target must be 'value' or 'quantity'")

        # Flux miroirs complets et strictement positifs
        df_sample = df_mirror.assign(_vi=v_i, _vj=v_j)
        df_sample = df_sample[(df_sample["_vi"] > 0) & (df_sample["_vj"] > 0)].copy()

        # Distance de déclaration ; pondération par le log de la somme des
        # valeurs déclarées (s = ln(V_i + V_j)), y compris pour la
        # qualité estimée sur les quantités
        df_sample["_rd"] = np.abs(np.log(df_sample["_vi"] / df_sample["_vj"]))
        df_sample["_w"] = np.log(
            df_sample["v_x"].fillna(0.0) + df_sample["v_m_fob"].fillna(0.0)
        )
        df_sample = df_sample[(df_sample["_w"] > 0) & np.isfinite(df_sample["_rd"])]

        # Effets exportateur et importateur extraits d'une même ANOVA absorbée
        effects = _absorbed_anova_effects(df_sample)
        ls_exp, se_exp = effects[_EXP]
        ls_imp, se_imp = effects[_IMP]

        return QualityResult(
            sigma_export=_ls_mean_to_sigma(ls_exp, se_exp),
            sigma_import=_ls_mean_to_sigma(ls_imp, se_imp),
        )


# Fonction d'estimation des effets des dimensions pays par ANOVA absorbée
def _absorbed_anova_effects(
    df_sample: pd.DataFrame,
) -> Dict[str, Tuple[pd.Series, pd.Series]]:
    """Estimate exporter and importer effects of ``RD`` via one absorbed ANOVA.

    Fits ``RD ~ exporter + importer + year`` weighted by ``ln(V_i + V_j)``,
    absorbing the product dimension, then re-expresses each country dimension in
    the note's sum-to-zero coding (eq. 11): a country's effect is its dummy
    coefficient minus the mean coefficient of its dimension (the reference
    level counting as zero), and its standard error is that of the
    corresponding contrast, derived from the full coefficient covariance —
    including a proper (non-zero) standard error for the reference level.

    Args:
        df_sample: Prepared frame with ``_rd`` (dependent), ``_w`` (weight), the
            entity columns and ``year``/``product``.

    Returns:
        Mapping ``{dimension: (effects, std_errors)}`` for the ``exporter`` and
        ``importer`` dimensions, each pair being pandas Series indexed by
        country code.
    """
    # Variables explicatives : indicatrices exportateur + importateur + année.
    # Pas de constante explicite : absorbée par les effets fixes produit (elle
    # deviendrait colinéaire après transformation within).
    df_exog = pd.get_dummies(
        df_sample[[_EXP, _IMP, _YEAR]].astype(str), drop_first=True
    ).astype("float64")

    # Dimension produit absorbée (transformation within)
    df_absorb = df_sample[[_PROD]].astype("category")

    # Estimation absorbée pondérée : un seul ajustement pour les deux dimensions
    res = AbsorbingLS(
        df_sample["_rd"], df_exog, absorb=df_absorb, weights=df_sample["_w"]
    ).fit()
    params = res.params
    cov = res.cov

    # Initialisation du dictionnaire résultat
    out: Dict[str, Tuple[pd.Series, pd.Series]] = {}
    # Parcours des colonnes d'importation et d'exportation
    for entity_col in (_EXP, _IMP):
        levels = sorted(df_sample[entity_col].astype(str).unique())
        n_levels = len(levels)
        # Coefficients en codage de référence (modalité de référence : zéro)
        coefs = pd.Series(
            {e: float(params.get(f"{entity_col}_{e}", 0.0)) for e in levels}
        )
        # Recentrage somme-nulle (les écarts entre pays sont préservés)
        effects = coefs - coefs.mean()

        # Écart-type de chaque effet recentré : contraste c = e_i − (1/L)·1 sur
        # les coefficients estimés (la part de la modalité de référence, sans
        # coefficient, est nulle dans le contraste)
        est_cols = [
            c for c in (f"{entity_col}_{e}" for e in levels) if c in params.index
        ]
        col_pos = {c: p for p, c in enumerate(est_cols)}
        v_mat = cov.loc[est_cols, est_cols].to_numpy()
        se: Dict[str, float] = {}
        for e in levels:
            contrast = np.full(len(est_cols), -1.0 / n_levels)
            name = f"{entity_col}_{e}"
            if name in col_pos:
                contrast[col_pos[name]] += 1.0
            se[e] = float(np.sqrt(max(contrast @ v_mat @ contrast, 0.0)))
        out[entity_col] = (effects, pd.Series(se))
    return out


# Fonction de conversion des moyennes marginales en écarts-types σ̂
def _ls_mean_to_sigma(ls_mean: pd.Series, std_error: pd.Series) -> pd.Series:
    """Turn least-square means of ``RD`` into per-country ``σ̂`` (eq. 12–13).

    Applies ``K_i = min_i LS_RD + 2·stderr_i`` and
    ``σ̂_i = (π/2)·(LS_RD_i - K_i)``, floored at zero so the best declarant carries
    the smallest variance.

    Args:
        ls_mean: Least-square mean of ``RD`` per country.
        std_error: Standard error of each country's effect.

    Returns:
        Series of ``σ̂`` per country (``>= 0``).
    """
    # Calage sur le meilleur déclarant (plus petite moyenne marginale)
    min_ls = float(ls_mean.min())
    k = min_ls + 2.0 * std_error.reindex(ls_mean.index).fillna(0.0)
    sigma = (math.pi / 2.0) * (ls_mean - k)
    return sigma.clip(lower=0.0)


# ──────────────────────────────────────────────────────────────────────
# Étape 5 — Réconciliation : moyenne pondérée des flux miroirs
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul du poids optimal à partir des variances log-normales
def _optimal_weight(sigma_i: np.ndarray, sigma_j: np.ndarray) -> np.ndarray:
    """Return the variance-minimising weight ``w`` on declaration ``i`` (eq. 10).

    Uses the log-normal error variance ``Var(E) = e^{σ²}(e^{σ²} - 1)``; when both
    variances are zero (perfect declarants) the weight defaults to ``0.5``.

    Args:
        sigma_i: Exporter declaration standard deviations.
        sigma_j: Importer declaration standard deviations.

    Returns:
        Array of weights ``w ∈ [0, 1]`` on the exporter declaration.
    """
    # Variances des erreurs log-normales
    var_i = np.exp(sigma_i ** 2) * (np.exp(sigma_i ** 2) - 1.0)
    var_j = np.exp(sigma_j ** 2) * (np.exp(sigma_j ** 2) - 1.0)
    denom = var_i + var_j
    # Poids optimal (plus de poids au déclarant le plus fiable) ; 0.5 si dégénéré
    with np.errstate(invalid="ignore", divide="ignore"):
        w = np.where(denom > 0, var_j / denom, 0.5)
    return w


# Transformateur de réconciliation des flux miroirs
class MirrorReconciler:
    """Reconcile the two mirror declarations into single FOB values.

    When both declarations exist, the reconciled value is the convex combination
    ``w·V_i + (1-w)·V_j`` where ``V_i`` is the export (FOB) declaration, ``V_j``
    the fobized import declaration, and ``w`` the optimal weight derived from the
    estimated variances. When a single declaration exists it is kept as is; when
    none exists the flow is absent. Values and quantities are reconciled with
    their respective (value/quantity) quality variances.

    The step works on the canonical mirror-flow columns only, so it carries no
    methodological parameter.
    """

    # Réconciliation des valeurs et des quantités
    def transform(
        self,
        df_mirror: pd.DataFrame,
        quality_value: QualityResult,
        quality_qty: QualityResult,
    ) -> pd.DataFrame:
        """Produce the reconciled value and quantity per flow.

        Args:
            df_mirror: Mirror-flow table with ``v_x``, ``v_m_fob``, ``q_x_t``,
                ``q_m_t``.
            quality_value: Per-country variances estimated on values.
            quality_qty: Per-country variances estimated on quantities.

        Returns:
            A frame keyed by ``(exporter, importer, product, year)`` with
            ``reconciled_value`` and ``reconciled_quantity``.
        """
        # Copie indépendante des données
        df_out = df_mirror.copy()

        # Réconciliation des valeurs (export FAB vs import fobisé)
        df_out["reconciled_value"] = _reconcile_pair(
            df_out, "v_x", "v_m_fob", quality_value
        )
        # Réconciliation des quantités (tonnes des deux côtés)
        df_out["reconciled_quantity"] = _reconcile_pair(
            df_out, "q_x_t", "q_m_t", quality_qty
        )

        # Restriction aux colonnes d'intérêt
        cols = [_EXP, _IMP, _PROD, _YEAR, "reconciled_value", "reconciled_quantity"]
        return df_out[cols]


# Fonction de réconciliation d'un couple de colonnes miroirs
def _reconcile_pair(
    df_mirror: pd.DataFrame, col_i: str, col_j: str, quality: QualityResult
) -> pd.Series:
    """Reconcile one mirror pair (value or quantity) into a single series.

    Args:
        df_mirror: Mirror-flow table.
        col_i: Exporter-side column (``V_i``).
        col_j: Importer-side column (``V_j``, already FOB-comparable).
        quality: Per-country variances for the reconciled quantity.

    Returns:
        Reconciled series (both-flow convex combination, single-flow passthrough,
        ``NaN`` when neither side exists).
    """
    # Extraction des valeurs d'intérêt des données
    v_i = df_mirror[col_i]
    v_j = df_mirror[col_j]
    # Extraction d'une indicatrice indiquant les valeurs positives
    has_i = v_i > 0
    has_j = v_j > 0

    # Écarts-types propres aux déclarants du flux ; pays absents de
    # l'estimation de qualité : fiabilité médiane (plutôt que parfaite) pour ne
    # pas leur accorder un poids indu
    # Initialisation des valeurs médianes par défaut
    default_i = float(quality.sigma_export.median()) if len(quality.sigma_export) else 0.0
    default_j = float(quality.sigma_import.median()) if len(quality.sigma_import) else 0.0
    # Calcul des écarts-types
    sigma_i = (
        df_mirror[_EXP].map(quality.sigma_export).astype("float64").fillna(default_i).to_numpy()
    )
    sigma_j = (
        df_mirror[_IMP].map(quality.sigma_import).astype("float64").fillna(default_j).to_numpy()
    )
    # Calcul du poids
    w = _optimal_weight(sigma_i, sigma_j)

    # Combinaison convexe lorsque les deux flux existent
    both = (has_i & has_j).to_numpy()
    reconciled = pd.Series(np.nan, index=df_mirror.index, dtype="float64")
    combo = w * v_i.fillna(0.0).to_numpy() + (1.0 - w) * v_j.fillna(0.0).to_numpy()
    reconciled[both] = combo[both]
    # Un seul flux présent : on le conserve
    only_i = (has_i & ~has_j).to_numpy()
    only_j = (~has_i & has_j).to_numpy()
    reconciled[only_i] = v_i[only_i]
    reconciled[only_j] = v_j[only_j]
    return reconciled


# ──────────────────────────────────────────────────────────────────────
# Étape 6 — Traitement des zones non spécifiées (Areas NES)
# ──────────────────────────────────────────────────────────────────────
# /!\ Rétablir le paramètre "apply_nes" dans la configuration
# /!\ MLFLOW LOG : Logger la part des flux finaux concernés par ce traitement
# Réallocateur des flux « Areas NES »
class AreaNesReallocator:
    """Reallocate "Areas NES" export flows to identified partners.

    For each ``(exporter i, product k, year t)`` carrying an "Areas NES" export
    declaration, compares the sum of the exporter's declared exports on
    *complete* mirror flows with the sum of the corresponding mirror imports.
    When the exporter under-declares, the shortfall ``Σ V_m - Σ V_x`` (capped by
    the NES value) is distributed across partners in proportion to the
    per-partner missing imports and added to the reconciled flows — but only
    when this reduces the group's overall mirror gap (safeguard). The residual
    NES value is then confronted with the imports declared *without* an export
    mirror, following the note's double-counting rule.

    This is the most heuristic step of the methodology and is therefore optional
    (``apply_nes`` in :func:`run_baci`). "Other Asia, nes" partners are excluded
    upfront by :func:`build_mirror_flows` (``nes_skip_codes``).

    The complete list of Areas not elsewhere specified is available at https://uncomtrade.org/docs/areas-not-elsewhere-specified/

    Args:
        flow_col: Column holding the trade-flow code.
        export_code: Flow code identifying export declarations (FOB).
        reporter_iso_col: Column with the reporter ISO-3 code.
        product_col: Column with the product (HS6) code.
        period_col: Column with the period (year as text).
        value_col: Column with the primary trade value.
    """

    # Initialisation
    def __init__(
        self,
        *,
        flow_col: str = "flowCode",
        export_code: str = "X",
        reporter_iso_col: str = "reporterISO",
        product_col: str = "cmdCode",
        period_col: str = "period",
        value_col: str = "primaryValue",
    ) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.flow_col = flow_col
        self.export_code = export_code
        self.reporter_iso_col = reporter_iso_col
        self.product_col = product_col
        self.period_col = period_col
        self.value_col = value_col

    # Réallocation des flux NES sur les partenaires identifiés
    def transform(
        self,
        df_reconciled: pd.DataFrame,
        df_mirror: pd.DataFrame,
        df_nes: pd.DataFrame,
    ) -> pd.DataFrame:
        """Add reallocated NES value to the reconciled flows.

        Implements the three parts of the methodology :

        1. **Reallocation** — for each ``(exporter, product, year)`` where the
           exporter under-declares on complete mirror flows (``Σ V_x < Σ V_m``),
           the shortfall (capped by the NES value) is distributed across
           partners proportionally to the per-partner missing imports.
        2. **Safeguard** — a group's reallocation is kept only when it reduces
           the group's total mirror gap ``Σ |ln(V_x / V_m)|``.
        3. **Residual** — the NES value left after reallocation is compared to
           the imports declared *without* an export mirror: when smaller, it is
           considered already counted there and dropped to avoid double
           counting; otherwise only the excess remains and, having no
           identifiable partner, is discarded (logged).

        Args:
            df_reconciled: Reconciled flows from :meth:`MirrorReconciler.transform`.
            df_mirror: Mirror-flow table (for per-partner declared/mirror sums).
            df_nes: "Areas NES" declarations kept aside by
                :func:`build_mirror_flows`.

        Returns:
            The reconciled frame with NES value distributed across identified
            partners (unchanged when there is nothing to reallocate; missing
            reconciled values stay missing).
        """
        # Initialisation des clés
        keys3 = [_EXP, _PROD, _YEAR]
        keys4 = [_EXP, _IMP, _PROD, _YEAR]
        if df_nes.empty:
            return df_reconciled

        # Valeur NES exportée par (exportateur, produit, année)
        df_nes_exp = df_nes[df_nes[self.flow_col] == self.export_code].copy()

        # Vérification que le DataFrame est non vide
        if df_nes_exp.empty:
            return df_reconciled

        # Extraction de la date
        df_nes_exp["_year"] = df_nes_exp[self.period_col].astype(str).str[:4].astype(int)
        # Somme par pays, produit et année
        df_nes_value = (
            df_nes_exp.groupby(
                [self.reporter_iso_col, self.product_col, "_year"]
            )[self.value_col]
            .sum()
            .rename("v_nes")
            .reset_index()
            .rename(
                columns={
                    self.reporter_iso_col: _EXP,
                    self.product_col: _PROD,
                    "_year": _YEAR,
                }
            )
        )

        # Découpage des flux de chaque exportateur : miroirs complets (les deux
        # déclarations existent) vs imports déclarés sans miroir export
        df_detail = df_mirror[keys4 + ["v_x", "v_m_fob"]].copy()
        has_x = df_detail["v_x"] > 0
        has_m = df_detail["v_m_fob"] > 0
        df_complete = df_detail[has_x & has_m].copy()

        # Étape 1 — sous-déclaration mesurée sur les miroirs complets :
        # Σ V_x vs Σ V_m des déclarations miroirs correspondantes
        df_sums = (
            df_complete.groupby(keys3)
            .agg(sum_vx=("v_x", "sum"), sum_vm=("v_m_fob", "sum"))
            .reset_index()
        )
        df_alloc = df_nes_value.merge(df_sums, on=keys3, how="left")
        df_alloc[["sum_vx", "sum_vm"]] = df_alloc[["sum_vx", "sum_vm"]].fillna(0.0)
        # Valeur réallouable : min(V_nes, Σ V_m − Σ V_x) si l'exportateur sous-déclare
        df_alloc["shortfall"] = (df_alloc["sum_vm"] - df_alloc["sum_vx"]).clip(lower=0.0)
        df_alloc["realloc"] = np.minimum(df_alloc["v_nes"], df_alloc["shortfall"])

        # Répartition proportionnelle au manque par partenaire (V_m − V_x > 0)
        df_complete["missing"] = (df_complete["v_m_fob"] - df_complete["v_x"]).clip(lower=0.0)
        df_shares = df_complete.merge(
            df_alloc.loc[df_alloc["realloc"] > 0, keys3 + ["realloc"]],
            on=keys3,
            how="inner",
        )

        # Initialisation du jeu de données des valeurs à ajouter
        df_add: Optional[pd.DataFrame] = None
        if not df_shares.empty:
            group_missing = df_shares.groupby(keys3)["missing"].transform("sum")
            df_shares["share"] = np.where(
                group_missing > 0, df_shares["missing"] / group_missing, 0.0
            )
            df_shares["add_value"] = df_shares["realloc"] * df_shares["share"]

            # Étape 2 — garde-fou : réallocation d'un groupe conservée
            # seulement si elle réduit son écart miroir global Σ |ln(V_x/V_m)|
            df_shares["_gap_before"] = np.abs(np.log(df_shares["v_x"] / df_shares["v_m_fob"]))
            df_shares["_gap_after"] = np.abs(
                np.log((df_shares["v_x"] + df_shares["add_value"]) / df_shares["v_m_fob"])
            )
            df_gaps = (
                df_shares.groupby(keys3)
                .agg(gap_before=("_gap_before", "sum"), gap_after=("_gap_after", "sum"))
                .reset_index()
            )
            df_improving = df_gaps.loc[df_gaps["gap_after"] < df_gaps["gap_before"], keys3]
            df_shares = df_shares.merge(df_improving, on=keys3, how="inner")
            if not df_shares.empty:
                df_add = df_shares[keys4 + ["add_value"]]

        # Étape 3 — résidu V_nes' confronté aux imports sans miroir V_m' :
        # inférieur → déjà compté dans V_m' (ramené à zéro, pas de double
        # compte) ; sinon seul l'excédent subsiste et, sans partenaire
        # identifiable, il est écarté
        if df_add is not None:
            df_allocated = (
                df_add.groupby(keys3)["add_value"].sum().rename("allocated").reset_index()
            )
            df_residual = df_alloc.merge(df_allocated, on=keys3, how="left")
        else:
            df_residual = df_alloc.copy()
            df_residual["allocated"] = 0.0
        df_residual["allocated"] = df_residual["allocated"].fillna(0.0)
        df_residual["v_nes_prime"] = (
            df_residual["v_nes"] - df_residual["allocated"]
        ).clip(lower=0.0)
        df_vm_prime = (
            df_detail[has_m & ~has_x]
            .groupby(keys3)["v_m_fob"]
            .sum()
            .rename("vm_prime")
            .reset_index()
        )
        df_residual = df_residual.merge(df_vm_prime, on=keys3, how="left")
        df_residual["vm_prime"] = df_residual["vm_prime"].fillna(0.0)
        df_residual["unallocated"] = np.where(
            df_residual["v_nes_prime"] < df_residual["vm_prime"],
            0.0,
            df_residual["v_nes_prime"] - df_residual["vm_prime"],
        )

        if df_add is None:
            # Logging
            logger.info(
                "AreaNesReallocator: aucune réallocation (%.1f de valeur NES, "
                "%.1f écartés faute de partenaire identifiable)",
                float(df_residual["v_nes"].sum()), float(df_residual["unallocated"].sum()),
            )
            return df_reconciled

        # Ajout de la valeur réallouée aux flux réconciliés ; les flux sans
        # réallocation (dont les valeurs réconciliées manquantes) restent intacts
        df_out = df_reconciled.merge(df_add, on=keys4, how="left")
        add_value = df_out.pop("add_value").fillna(0.0)
        df_out["reconciled_value"] = df_out["reconciled_value"] + add_value

        # Logging
        logger.info(
            "AreaNesReallocator: %d flux enrichis (%.1f réalloués, %.1f absorbés "
            "par les imports sans miroir, %.1f écartés faute de partenaire)",
            int((add_value > 0).sum()),
            float(df_residual["allocated"].sum()),
            float((df_residual["v_nes_prime"] - df_residual["unallocated"]).sum()),
            float(df_residual["unallocated"].sum()),
        )
        return df_out


# ──────────────────────────────────────────────────────────────────────
# Orchestration de bout en bout
# ──────────────────────────────────────────────────────────────────────

# Rapport d'exécution du redressement BACI
@dataclass
class BaciReport:
    """Summary of a BACI reconstruction run.

    Attributes:
        flows: Number of reconciled flows produced.
        mean_freight_rate: Mean estimated freight rate over the mirror flows.
        regime_country_years: Number of ``(importer, year)`` pairs whose import
            valuation regime was inferred.
        regime_fob_country_years: Number of those pairs inferred FOB (imports
            left uncorrected, save for the FAS importers).
        regime_no_information: Number of those pairs carrying no valuation
            information at all (both value columns summing to zero), defaulted to
            CIF.
        classification_code: HS classification vintage code shared by every
            observation in the run (``None`` when the classification column was
            absent from ``df_comtrade``, see
            :func:`_assert_homogeneous_classification`).
        period_start: First year (included) kept from ``df_comtrade`` for this
            run; ``None`` means no lower bound was applied.
        period_end: Last year (included) kept from ``df_comtrade`` for this
            run; ``None`` means no upper bound was applied.
        created: Whether the result schema was created (vs. upserted); left
            ``False`` by :func:`run_baci`, set by the caller after persisting.
    """
    flows: int = 0
    mean_freight_rate: float = float("nan")
    regime_country_years: int = 0
    regime_fob_country_years: int = 0
    regime_no_information: int = 0
    classification_code: Optional[str] = None
    period_start: Optional[int] = None
    period_end: Optional[int] = None
    created: bool = False


# Colonnes COMTRADE nécessaires au redressement
def required_columns(config: BaciConfig = DEFAULT_CONFIG) -> List[str]:
    """Return the COMTRADE columns the pipeline reads.

    Lets the caller project only the needed columns when loading the source
    fact table before handing it to :func:`run_baci`.

    Args:
        config: Column and methodological conventions (only ``config.schema`` is
            read here).

    Returns:
        Ordered, de-duplicated list of source columns to project.

    Examples:
        >>> required_columns()[:3]
        ['flowCode', 'reporterISO', 'partnerISO']
    """
    return list(
        dict.fromkeys(
            [
                config.schema.flow_col,
                config.schema.reporter_iso_col,
                config.schema.partner_iso_col,
                config.schema.partner_code_col,
                config.schema.product_col,
                config.schema.period_col,
                config.schema.value_col,
                config.schema.qty_col,
                config.schema.qty_unit_col,
                config.schema.netwgt_col,
                config.schema.cif_value_col,
                config.schema.fob_value_col,
            ]
        )
    )


# Fonction de vérification de l'homogénéité de la nomenclature HS
def _assert_homogeneous_classification(
    df_comtrade: pd.DataFrame, classification_col: str
) -> Optional[str]:
    """Return the single HS classification code present in ``df_comtrade``, or raise.

    :func:`run_baci` only checks homogeneity — it never converts between HS
    vintages, that conversion belongs to
    :class:`macroforecast.trade.processing.classification.HsHarmonizer`, run
    upstream by the caller.

    Args:
        df_comtrade: Raw COMTRADE fact-table rows.
        classification_col: Column holding the HS classification vintage code
            (e.g. ``"H5"``, ``"H6"``).

    Returns:
        The single classification code found, or ``None`` when
        ``classification_col`` is absent from ``df_comtrade`` — a warning is
        emitted in that case, for backward compatibility with extractions that
        do not project this column.

    Raises:
        ValueError: If more than one classification code coexists in
            ``df_comtrade``.
    """
    # Rétrocompatibilité : colonne absente des extractions ne la projetant pas
    if classification_col not in df_comtrade.columns:
        logger.warning(
            "Column %r absent from df_comtrade: skipping the HS classification "
            "homogeneity check.",
            classification_col,
        )
        return None

    codes = sorted(df_comtrade[classification_col].dropna().unique().tolist())
    if len(codes) > 1:
        raise ValueError(
            f"Heterogeneous HS classifications found in df_comtrade: {codes}. "
            "Run macroforecast.trade.processing.classification.HsHarmonizer "
            "first to convert every observation to a single vintage (the "
            "oldest one is unambiguous)."
        )
    return codes[0] if codes else None


# Fonction d'orchestration : flux COMTRADE chargés → flux réconciliés
def run_baci(
    df_comtrade: pd.DataFrame,
    df_dist: pd.DataFrame,
    df_geo: pd.DataFrame,
    *,
    config: BaciConfig = DEFAULT_CONFIG,
    apply_nes: bool = True,
    period_start: Optional[int] = None,
    period_end: Optional[int] = None,
) -> Tuple[pd.DataFrame, BaciReport]:
    """Run the BACI reconstruction end to end on already-loaded data.

    Assembles the CEPII gravity variables, builds the mirror-flow table, applies
    the six methodological steps in order, and returns the reconciled value and
    quantity per ``(exporter, importer, product, year)``. The function performs
    no I/O: reading the COMTRADE fact table and persisting the result belong to
    the caller (see ``scripts/process_baci.py``).

    This is the only place where :class:`BaciConfig` is read: each step receives
    the values it needs as explicit keyword arguments.

    The import valuation regime (CIF or FOB) is inferred from the data by
    :func:`infer_import_valuation_regime` **before** the mirror flows are built.

    The HS classification of ``df_comtrade`` is checked for homogeneity (never
    converted — see :func:`_assert_homogeneous_classification`), and the
    temporal scope is filtered in :func:`build_mirror_flows`, right after the
    year is derived from ``period_col`` and before any estimation runs: the
    conversion rates and the gravity equation must never be calibrated on years
    later excluded from the result. Absent ``period_start``/``period_end``
    bounds, the scope is whatever ``df_comtrade`` already carries — filtering
    upstream (in SQL, in the caller script) remains preferable for volumetry.

    Args:
        df_comtrade: Raw COMTRADE fact-table rows, holding at least the columns
            returned by :func:`required_columns`.
        df_dist: Raw ``dist_cepii`` table, already loaded.
        df_geo: Raw ``geo_cepii`` table, already loaded.
        config: Column and methodological conventions. Its own
            ``period_start``/``period_end`` are used whenever the
            same-named argument below is left ``None``.
        apply_nes: Whether to apply the "Areas NES" reallocation step.
        period_start: First year (included) kept from ``df_comtrade``; falls
            back to ``config.period_start`` when ``None``.
        period_end: Last year (included) kept from ``df_comtrade``; falls back
            to ``config.period_end`` when ``None``.

    Returns:
        A tuple ``(df_reconciled, report)``: the reconciled flows and the
        :class:`BaciReport` of the run (``created`` is left ``False``; the
        caller sets it once the result is persisted).

    Raises:
        ValueError: If the HS classification is heterogeneous, if the temporal
            filter empties ``df_comtrade``, or if the reconciliation produces no
            flow.
    """
    # Périmètre temporel effectif : l'argument explicite prime sur la config
    effective_period_start = (
        period_start if period_start is not None else config.period_start
    )
    effective_period_end = (
        period_end if period_end is not None else config.period_end
    )

    # Vérification de l'homogénéité de la nomenclature HS
    classification_code = _assert_homogeneous_classification(
        df_comtrade, config.schema.classification_col
    )

    # Assemblage des données du CEPII servant à estimer les modèles de gravité
    df_gravity = build_gravity_data(
        df_dist,
        df_geo,
        dist_iso_o_col=config.schema.dist_iso_o_col,
        dist_iso_d_col=config.schema.dist_iso_d_col,
        distance_column=config.schema.distance_column,
        contig_col=config.schema.contig_col,
        geo_iso_col=config.schema.geo_iso_col,
        landlocked_col=config.schema.landlocked_col,
    )
    # Extraction des codes pays valides
    valid_iso = sorted(set(df_gravity["iso_o"]) | set(df_gravity["iso_d"]))

    # Inférence du régime de valorisation des importations — impérativement avant
    # la construction des flux miroirs, qui agrège les colonnes de valorisation
    df_regime = infer_import_valuation_regime(
        df_comtrade,
        flow_col=config.schema.flow_col,
        import_code=config.schema.import_code,
        reporter_iso_col=config.schema.reporter_iso_col,
        period_col=config.schema.period_col,
        cif_value_col=config.schema.cif_value_col,
        fob_value_col=config.schema.fob_value_col,
        cif_share_threshold=config.cif_share_threshold,
        regime_granularity=config.regime_granularity,
    )

    # Construction des flux miroirs (+ flux NES mis de côté)
    df_mirror, df_nes = build_mirror_flows(
        df_comtrade,
        valid_iso,
        flow_col=config.schema.flow_col,
        import_code=config.schema.import_code,
        export_code=config.schema.export_code,
        reporter_iso_col=config.schema.reporter_iso_col,
        partner_iso_col=config.schema.partner_iso_col,
        partner_code_col=config.schema.partner_code_col,
        product_col=config.schema.product_col,
        period_col=config.schema.period_col,
        value_col=config.schema.value_col,
        qty_col=config.schema.qty_col,
        qty_unit_col=config.schema.qty_unit_col,
        netwgt_col=config.schema.netwgt_col,
        world_partner_code=config.world_partner_code,
        nes_partner_codes=config.nes_partner_codes,
        nes_skip_codes=config.nes_skip_codes,
        excluded_pairs=config.excluded_pairs,
        period_start=effective_period_start,
        period_end=effective_period_end,
    )

    # Étape 1 — conversion des quantités en tonnes
    converter = TonnageConverter(
        tonne_conversion_factors=config.tonne_conversion_factors,
        min_mirror_flows=config.min_mirror_flows,
        max_conversion_std=config.max_conversion_std,
        prefer_netwgt=config.prefer_netwgt,
    ).fit(df_mirror)
    df_mirror = converter.transform(df_mirror)

    # Étape 2 — estimation des taux CAF par équation de gravité
    gravity_model = CifGravityModel(cook_factor=config.cook_factor).fit(
        df_mirror, df_gravity
    )
    cif_rate = gravity_model.predict(df_mirror, df_gravity)
    mean_freight_rate = float(cif_rate.replace([np.inf, -np.inf], np.nan).dropna().mean())
    print(mean_freight_rate)

    # Étape 3 — fobisation des importations
    df_mirror = Fobizer(fas_countries=config.fas_countries).transform(
        df_mirror, cif_rate, df_regime
    )

    # Étape 4 — qualité de déclaration (valeurs puis quantités)
    quality_model = ReportingQualityModel()
    quality_value = quality_model.fit(df_mirror, target="value")
    quality_qty = quality_model.fit(df_mirror, target="quantity")

    # Étape 5 — réconciliation des flux miroirs
    df_reconciled = MirrorReconciler().transform(df_mirror, quality_value, quality_qty)

    # Étape 6 — réallocation des zones non spécifiées (optionnelle)
    if apply_nes:
        df_reconciled = AreaNesReallocator(
            flow_col=config.schema.flow_col,
            export_code=config.schema.export_code,
            reporter_iso_col=config.schema.reporter_iso_col,
            product_col=config.schema.product_col,
            period_col=config.schema.period_col,
            value_col=config.schema.value_col,
        ).transform(df_reconciled, df_mirror, df_nes)

    # Nettoyage : flux réconciliés exploitables (valeur non manquante)
    df_reconciled = df_reconciled[
        df_reconciled["reconciled_value"].notna()
    ].reset_index(drop=True)
    if df_reconciled.empty:
        raise ValueError("No reconciled flow produced")

    report = BaciReport(
        flows=len(df_reconciled),
        mean_freight_rate=mean_freight_rate,
        regime_country_years=len(df_regime),
        regime_fob_country_years=int((df_regime["regime"] == _FOB_REGIME).sum()),
        regime_no_information=int(df_regime["cif_share"].isna().sum()),
        classification_code=classification_code,
        period_start=effective_period_start,
        period_end=effective_period_end,
    )

    return df_reconciled, report
