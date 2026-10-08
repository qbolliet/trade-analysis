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

Diagnostics are **data, not logs**: every step produces a report dataclass
(:class:`TonnageReport`, :class:`GravityReport`, :class:`FobisationReport`,
:class:`MirrorReport`, :class:`QualityReport`, :class:`NesReport`), returned
alongside its result or exposed as a fitted ``report_`` attribute, and gathered
by :func:`run_baci` into the composite :class:`BaciReport`. Logging and
experiment tracking *consume* those reports; they never compute them.

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
from dataclasses import dataclass, field, fields
import json
import logging
import math
import time
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)
# Modules de manipulation de données
import numpy as np
import pandas as pd
# Modules du package
from ...tracking import NULL_TRACKER, RunTracker, flatten_metrics, run_params
from .aggregation import aggregate_measures
from .streaming import (
    AbsorbedWLSAccumulator,
    CookFilter,
    WelfordGroupStats,
    WeightedLeastSquaresAccumulator,
    WlsSolution,
)

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
        min_sigma_ratio: Strictly positive floor applied to the reporting-quality
            ``σ̂``, as a fraction of the median strictly positive ``σ̂`` of the
            fit (see :class:`ReportingQualityModel`).
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
        apply_nes: Whether to apply the "Areas NES" reallocation step (see
            :class:`AreaNesReallocator`). Distinct from ``nes_partner_codes``:
            an empty ``nes_partner_codes`` means "no NES code declared", not
            "do not reallocate".
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
    # Qualité de déclaration : plancher relatif des σ̂ (évite le poids tout-ou-rien)
    min_sigma_ratio: float = 0.1
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
    # Active la réallocation (étape 6) ; distinct de nes_partner_codes, qui ne
    # fait que déclarer les codes éligibles quand la réallocation est active
    apply_nes: bool = True
    world_partner_code: int = 0
    nes_partner_codes: Tuple[int, ...] = (899,)  # 899 = "Areas, nes"
    nes_skip_codes: Tuple[int, ...] = (490,)  # 490 = "Other Asia, nes"
    # Clés primaires du résultat
    primary_keys: Tuple[str, ...] = ("exporter", "importer", "product", "year")


# Configuration par défaut (schéma COMTRADE tariffline + CEPII dist/geo)
DEFAULT_CONFIG = BaciConfig()

# Champs de BaciConfig exclus de l'empreinte méthodologique : aucun. Tous
# changent les flux réconciliés écrits, y compris les conventions de schéma
# (schema.distance_column choisit la distance de la gravité) et le périmètre
# temporel (period_start / period_end bornent l'échantillon d'estimation)
BACI_FINGERPRINT_EXCLUDED: frozenset = frozenset()

# Noms de colonnes canoniques de la table de flux miroirs
_EXP, _IMP, _PROD, _YEAR = "exporter", "importer", "product", "year"

# Colonne transitoire portant le poids de la déclaration exportatrice dans la
# réconciliation des valeurs : produite par MirrorReconciler, consommée par
# AreaNesReallocator, retirée par run_baci avant restitution du résultat
_VALUE_WEIGHT = "_value_weight"

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
            :class:`macroforecast.storage.Loader`).
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
    # Une paire orientée par ligne : un doublon dupliquerait les flux lors de la
    # jointure de gravité et fausserait l'estimation comme la prédiction
    df_dist = df_dist[dist_cols].drop_duplicates(
        subset=[dist_iso_o_col, dist_iso_d_col]
    ).copy()

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

    # Agrégation par mesure : somme des valeurs et poids, unité dominante pour la
    # quantité. Règle partagée avec HsHarmonizer (voir aggregation.py) : une clé
    # incomplète rend la déclaration inexploitable, d'où dropna=True.
    df_merged = aggregate_measures(
        df_side,
        keys,
        sum_cols=[value_col, netwgt_col],
        qty_col=qty_col,
        qty_unit_col=qty_unit_col,
        dropna=True,
    )

    # Renommage des mesures et des identités vers les colonnes canoniques
    return df_merged.rename(
        columns={
            value_col: f"v_{suffix}",
            netwgt_col: f"nw_{suffix}",
            qty_col: f"q_{suffix}",
            qty_unit_col: f"unit_{suffix}",
            exporter_col: _EXP,
            importer_col: _IMP,
            product_col: _PROD,
            "_year": _YEAR,
        }
    )


# Diagnostics de la construction des flux miroirs
@dataclass
class MirrorReport:
    """Diagnostics of the mirror-flow construction.

    Attributes:
        n_flows: Number of ``(exporter, importer, product, year)`` flows built.
        share_both_declarations: Share of flows declared by both partners.
        share_export_only: Share of flows declared by the exporter only.
        share_import_only: Share of flows declared by the importer only.
    """
    n_flows: int = 0
    share_both_declarations: float = float("nan")
    share_export_only: float = float("nan")
    share_import_only: float = float("nan")


# Compteurs additifs de la construction des flux miroirs
@dataclass
class _MirrorTally:
    """Additive counts behind :class:`MirrorReport`.

    The report's shares are ratios, which do not add up over chunks; their
    numerators and denominator do. Summing the tallies of every chunk then
    calling :meth:`report` yields the report of the whole data.

    Attributes:
        n_flows: Number of mirror flows.
        n_both: Flows declared by both partners.
        n_export_only: Flows declared by the exporter only.
        n_import_only: Flows declared by the importer only.

    Examples:
        >>> df = pd.DataFrame({"v_x": [1.0, None, 2.0], "v_m": [1.0, 3.0, None]})
        >>> tally = _MirrorTally.from_mirror(df) + _MirrorTally.from_mirror(df)
        >>> tally.report().share_both_declarations
        0.3333333333333333
    """
    # Initialisation des attributs
    n_flows: int = 0
    n_both: int = 0
    n_export_only: int = 0
    n_import_only: int = 0

    # Compteurs d'une table de flux miroirs
    @classmethod
    def from_mirror(cls, df_mirror: pd.DataFrame) -> "_MirrorTally":
        """Count the declaration coverage of a mirror-flow table.

        Args:
            df_mirror: Mirror-flow table (``v_x``, ``v_m``).

        Returns:
            The tally of the table.
        """
        has_x = df_mirror["v_x"].notna()
        has_m = df_mirror["v_m"].notna()
        return cls(
            n_flows=int(len(df_mirror)),
            n_both=int((has_x & has_m).sum()),
            n_export_only=int((has_x & ~has_m).sum()),
            n_import_only=int((~has_x & has_m).sum()),
        )

    # Somme de deux tallies
    def __add__(self, other: "_MirrorTally") -> "_MirrorTally":
        return _add_tallies(self, other)

    # Rapport correspondant
    def report(self) -> MirrorReport:
        """Return the :class:`MirrorReport` of the counted flows.

        Returns:
            The report (shares ``NaN`` when no flow was counted).
        """
        denominator = float(self.n_flows) if self.n_flows else float("nan")
        return MirrorReport(
            n_flows=self.n_flows,
            share_both_declarations=float(self.n_both) / denominator,
            share_export_only=float(self.n_export_only) / denominator,
            share_import_only=float(self.n_import_only) / denominator,
        )


# Fonction auxiliaire : somme champ à champ de deux tallies de même type
def _add_tallies(left, right):
    """Sum two tallies of the same dataclass field by field.

    Booleans are combined with ``or`` (a flag raised on any chunk is raised on the
    whole data), numbers are added.

    Args:
        left: First tally.
        right: Second tally, of the same type.

    Returns:
        A new tally of the same type.

    Raises:
        TypeError: If the two tallies are of different types.
    """
    if type(left) is not type(right):
        raise TypeError(f"Cannot add {type(left).__name__} and {type(right).__name__}")
    values = {}
    for item in fields(left):
        a, b = getattr(left, item.name), getattr(right, item.name)
        values[item.name] = (a or b) if isinstance(a, bool) else a + b
    return type(left)(**values)


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
) -> Tuple[pd.DataFrame, pd.DataFrame, MirrorReport]:
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
        A tuple ``(df_mirror, df_nes, report)``:

        * ``df_mirror``: one row per ``(exporter, importer, product, year)`` with
          columns ``v_x, q_x, unit_x, nw_x`` (export/FOB side) and
          ``v_m, q_m, unit_m, nw_m`` (import/CIF side), outer-joined.
        * ``df_nes``: import/export declarations whose partner is an "Areas NES"
          aggregate (``nes_partner_codes``), kept aside for the reallocation step.
        * ``report``: :class:`MirrorReport` describing the double-declaration
          coverage of the built table.

    Raises:
        ValueError: If the ``period_start``/``period_end`` filter empties
            ``df_comtrade``, or if ``partner_code_col`` holds no numeric M49
            code at all (the aggregates could then not be identified).
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

    # Coercition numérique du code partenaire M49 : les catalogues le livrent
    # tantôt en entier, tantôt en texte, et une comparaison texte/entier
    # laisserait passer les agrégats sans le moindre signal
    partner_code = pd.to_numeric(df_data[partner_code_col], errors="coerce")
    if partner_code.isna().all() and len(df_data):
        raise ValueError(
            f"Column {partner_code_col!r} holds no numeric M49 code: the World "
            "and 'Areas nes' aggregates cannot be identified."
        )

    # Exclusion explicite des agrégats non traités : (ex. Monde, « Other Asia, nes », ni réallouées ni pays)
    keep = partner_code != world_partner_code
    if nes_skip_codes:
        keep &= ~partner_code.isin(list(nes_skip_codes))
    df_data = df_data[keep]
    partner_code = partner_code[keep]

    # Séparation des déclarations d'export et d'import
    is_export = df_data[flow_col] == export_code
    is_import = df_data[flow_col] == import_code

    valid = set(valid_iso)

    # Flux NES : partenaire agrégé « Areas nes » (avant filtre pays individuels)
    nes_mask = partner_code.isin(list(nes_partner_codes))
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

    df_mirror = df_mirror.reset_index(drop=True)

    # Diagnostics : couverture des deux déclarations, connue dès le pivot et
    # avant toute estimation (compteurs additifs, partagés avec le traitement par
    # passes)
    report = _MirrorTally.from_mirror(df_mirror).report()

    # Logging
    logger.info(
        "build_mirror_flows: %d flux miroirs, %.1f%% à double déclaration",
        report.n_flows,
        100.0 * report.share_both_declarations,
    )

    return df_mirror, df_nes.reset_index(drop=True), report


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
    # Vérification de la granularité demandée, puis sommes additives et décision
    _check_regime_granularity(regime_granularity)
    df_sums = _regime_sums(
        df_comtrade,
        flow_col=flow_col,
        import_code=import_code,
        reporter_iso_col=reporter_iso_col,
        period_col=period_col,
        cif_value_col=cif_value_col,
        fob_value_col=fob_value_col,
    )
    return _regime_from_sums(
        df_sums,
        cif_share_threshold=cif_share_threshold,
        regime_granularity=regime_granularity,
    )


# Fonction de vérification de la granularité du régime
def _check_regime_granularity(regime_granularity: str) -> None:
    """Reject an unknown valuation-regime granularity.

    Args:
        regime_granularity: ``"country"`` or ``"country_year"``.

    Raises:
        ValueError: On any other value.
    """
    if regime_granularity not in ("country", "country_year"):
        raise ValueError(
            "regime_granularity must be 'country' or 'country_year', "
            f"got {regime_granularity!r}"
        )


# Colonnes des sommes de valorisation par (importateur, année)
_REGIME_SUM_COLUMNS = [_IMP, _YEAR, "_cif", "_fob", "n_declarations"]


# Fonction de calcul des sommes de valorisation par (importateur, année)
def _regime_sums(
    df_comtrade: pd.DataFrame,
    *,
    flow_col: str = "flowCode",
    import_code: str = "M",
    reporter_iso_col: str = "reporterISO",
    period_col: str = "period",
    cif_value_col: str = "cifvalue",
    fob_value_col: str = "fobvalue",
) -> pd.DataFrame:
    """Sum the CIF and FOB import values per ``(importer, year)``.

    These sums are the sufficient statistics of the valuation-regime inference:
    additive over any partition of the declarations (see
    :func:`_merge_regime_sums`), whichever the granularity finally applied.

    Args:
        df_comtrade: Raw COMTRADE rows (both flow directions).
        flow_col: Column holding the trade-flow code.
        import_code: Flow code identifying import declarations.
        reporter_iso_col: Column with the reporter ISO-3 code.
        period_col: Column with the period (year as text).
        cif_value_col: Column with the CIF import value.
        fob_value_col: Column with the FOB value.

    Returns:
        Frame with columns ``importer``, ``year``, ``_cif``, ``_fob`` and
        ``n_declarations`` (one row per observed pair).

    Raises:
        KeyError: If one of the expected COMTRADE columns is absent.

    Examples:
        >>> df = pd.DataFrame({"flowCode": ["M", "M"], "reporterISO": ["AAA", "AAA"],
        ...                    "period": ["2020", "2020"], "cifvalue": [1.0, 2.0],
        ...                    "fobvalue": [0.0, 0.0]})
        >>> _regime_sums(df).to_dict("records")
        [{'importer': 'AAA', 'year': 2020, '_cif': 3.0, '_fob': 0.0, 'n_declarations': 2}]
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

    # Restriction aux déclarations d'importation : elles seules portent le régime
    df_imports = df_comtrade.loc[df_comtrade[flow_col] == import_code, needed].copy()
    if df_imports.empty:
        return _empty_regime_sums()

    # Année entière dérivée de la période (chaîne "YYYY"), convention partagée
    # avec build_mirror_flows
    df_imports[_YEAR] = df_imports[period_col].astype(str).str[:4].astype(int)
    # Coercition numérique des valeurs déclarées : manquants comptés pour zéro
    for col in (cif_value_col, fob_value_col):
        df_imports[col] = pd.to_numeric(df_imports[col], errors="coerce").fillna(0.0)

    # Agrégation par (importateur, année) : pondération par la valeur déclarée
    return (
        df_imports.groupby([reporter_iso_col, _YEAR], dropna=True)
        .agg(
            _cif=(cif_value_col, "sum"),
            _fob=(fob_value_col, "sum"),
            n_declarations=(flow_col, "size"),
        )
        .reset_index()
        .rename(columns={reporter_iso_col: _IMP})[_REGIME_SUM_COLUMNS]
    )


# Fonction de construction d'une table de sommes de valorisation vide
def _empty_regime_sums() -> pd.DataFrame:
    """Return an empty frame with the schema of :func:`_regime_sums`.

    Returns:
        Empty frame with columns ``importer``, ``year``, ``_cif``, ``_fob`` and
        ``n_declarations``.
    """
    return pd.DataFrame(
        {
            _IMP: pd.Series(dtype=object),
            _YEAR: pd.Series(dtype="int64"),
            "_cif": pd.Series(dtype=float),
            "_fob": pd.Series(dtype=float),
            "n_declarations": pd.Series(dtype="int64"),
        }
    )


# Fonction de fusion de sommes de valorisation calculées sur des tranches
def _merge_regime_sums(df_left: pd.DataFrame, df_right: pd.DataFrame) -> pd.DataFrame:
    """Add two frames of valuation sums (:func:`_regime_sums`) key by key.

    Args:
        df_left: Sums of a first set of declarations.
        df_right: Sums of a second, disjoint set of declarations.

    Returns:
        The sums of the union, sorted by ``(importer, year)``.

    Examples:
        >>> a = pd.DataFrame({"importer": ["AAA"], "year": [2020], "_cif": [1.0],
        ...                   "_fob": [0.0], "n_declarations": [1]})
        >>> _merge_regime_sums(a, a)["_cif"].tolist()
        [2.0]
    """
    frames = [df for df in (df_left, df_right) if df is not None and not df.empty]
    if not frames:
        return _empty_regime_sums()
    df_all = pd.concat(frames, ignore_index=True)
    return (
        df_all.groupby([_IMP, _YEAR], as_index=False)[["_cif", "_fob", "n_declarations"]]
        .sum()
        .sort_values([_IMP, _YEAR])
        .reset_index(drop=True)
    )


# Fonction de décision du régime à partir des sommes de valorisation
def _regime_from_sums(
    df_sums: pd.DataFrame,
    *,
    cif_share_threshold: float = 0.5,
    regime_granularity: Literal["country", "country_year"] = "country_year",
) -> pd.DataFrame:
    """Turn the valuation sums into the inferred regime table.

    Args:
        df_sums: Valuation sums per ``(importer, year)`` (:func:`_regime_sums`).
        cif_share_threshold: CIF share above which the importer is deemed CIF.
        regime_granularity: ``"country_year"`` or ``"country"`` (sums cumulated
            over every year, then broadcast over each observed year).

    Returns:
        The table documented in :func:`infer_import_valuation_regime`.

    Raises:
        ValueError: If ``regime_granularity`` is unknown.
    """
    _check_regime_granularity(regime_granularity)
    # Jeu sans déclaration d'importation : sortie vide au schéma attendu
    if df_sums is None or df_sums.empty:
        return pd.DataFrame(
            {
                _IMP: pd.Series(dtype=object),
                _YEAR: pd.Series(dtype=int),
                "cif_share": pd.Series(dtype=float),
                "n_declarations": pd.Series(dtype=int),
                "regime": pd.Series(dtype=object),
            }
        )
    df_regime = df_sums[_REGIME_SUM_COLUMNS].copy()

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


# Fonction auxiliaire : décompte des importateurs d'un régime inféré
def _count_importers(df_regime: pd.DataFrame, regime: Optional[str]) -> int:
    """Count the distinct importers carrying a given inferred regime.

    Args:
        df_regime: Inferred regime table from
            :func:`infer_import_valuation_regime`; may be empty.
        regime: Regime to count (``"CIF"``/``"FOB"``), or ``None`` to count the
            importers for which no valuation information was available at all
            (``cif_share`` missing).

    Returns:
        Number of distinct importers matching the criterion, ``0`` when the
        inference produced nothing.

    Examples:
        >>> df_regime = pd.DataFrame(
        ...     {"importer": ["ZAF", "ZAF", "FRA"], "year": [2019, 2020, 2019],
        ...      "cif_share": [0.1, float("nan"), 0.9], "regime": ["FOB", "CIF", "CIF"]}
        ... )
        >>> _count_importers(df_regime, "FOB"), _count_importers(df_regime, None)
        (1, 1)
    """
    # Cas où le jeu de données est vide ou non renseigné
    if df_regime is None or df_regime.empty:
        return 0
    # Absence d'information : part CAF non calculable (dénominateur nul)
    if regime is None:
        mask = df_regime["cif_share"].isna()
    # Extraction des lignes associées au régime
    else:
        mask = df_regime["regime"] == regime
    return int(df_regime.loc[mask, _IMP].nunique())



# ──────────────────────────────────────────────────────────────────────
# Étape 1 — Conversion des quantités en tonnes
# ──────────────────────────────────────────────────────────────────────

# Origines possibles du tonnage d'une déclaration
_SOURCE_NETWGT, _SOURCE_FACTOR = "netwgt", "unit_factor"
_SOURCE_RATE, _SOURCE_MISSING = "rate", "missing"


# Diagnostics de la conversion des quantités en tonnes
@dataclass
class TonnageReport:
    """Diagnostics of the tonne conversion step.

    Shares are computed over the *declared side observations*: each mirror flow
    contributes its export side and its import side, but only those carrying a
    quantity or a net weight. Sides that were never declared are excluded — they
    say nothing about the conversion, and the double-declaration coverage they do
    measure is already reported by :class:`MirrorReport`.

    Attributes:
        n_validated_rates: Number of ``(product, unit)`` rates that passed the
            count and dispersion filters.
        n_candidate_pairs: Number of ``(product, unit)`` pairs observed before
            those filters.
        share_converted_from_other_units: Share of declared side observations
            whose tonnage comes from an estimated rate, i.e. from a unit other
            than the tonne or a weight unit.
        share_tonnage_from_netwgt: Share of declared side observations whose
            tonnage comes from the declared net weight.
        share_tonnage_missing: Share of declared side observations left without
            tonnage — a genuine conversion failure (no weight, and no validated
            rate for the ``(product, unit)`` pair).
    """
    n_validated_rates: int = 0
    n_candidate_pairs: int = 0
    share_converted_from_other_units: float = float("nan")
    share_tonnage_from_netwgt: float = float("nan")
    share_tonnage_missing: float = float("nan")


# Compteurs additifs des origines du tonnage
@dataclass
class _TonnageTally:
    """Additive counts behind the shares of :class:`TonnageReport`.

    Attributes:
        n_sides: Declared side observations (quantity or net weight present).
        n_rate: Of which converted with an estimated rate.
        n_netwgt: Of which taken from the net weight.
        n_missing: Of which left without tonnage.
    """
    n_sides: int = 0
    n_rate: int = 0
    n_netwgt: int = 0
    n_missing: int = 0

    # Somme de deux tallies
    def __add__(self, other: "_TonnageTally") -> "_TonnageTally":
        return _add_tallies(self, other)


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
        report_: :class:`TonnageReport` of the step; counts are populated by
            :meth:`fit`, shares by :meth:`transform`.
    """

    # Initialisation
    def __init__(
        self,
        *,
        tonne_conversion_factors: Optional[Mapping[int, float]] = None,
        min_mirror_flows: int = 10,
        max_conversion_std: float = 2.5,
        prefer_netwgt: bool = True,
    ) -> None:
        # Initialisation des attributs ; repli sur BaciConfig (source unique de
        # la table de facteurs, évite toute duplication en dur de ce paramètre
        # méthodologique)
        self.tonne_conversion_factors = dict(
            tonne_conversion_factors
            if tonne_conversion_factors is not None
            else DEFAULT_CONFIG.tonne_conversion_factors
        )
        self.min_mirror_flows = min_mirror_flows
        self.max_conversion_std = max_conversion_std
        self.prefer_netwgt = prefer_netwgt

    # Méthode auxiliaire : quantité en tonnes déjà connue (poids ou unité tonne)
    def _tonnes_from_weight(
        self, qty: pd.Series, unit: pd.Series, nw: pd.Series
    ) -> Tuple[pd.Series, pd.Series]:
        """Return the tonnage known without any estimated rate, and its origin.

        Args:
            qty: Declared quantities.
            unit: Declared quantity-unit codes.
            nw: Net weights (kilograms).

        Returns:
            A tuple ``(tonnes, source)``: the tonnage from net weight
            (preferred) or from quantities already expressed in a unit covered
            by ``tonne_conversion_factors`` (weight units, or the tonne unit
            itself), ``NaN`` where neither is available; and the origin of each
            value (``"netwgt"``, ``"unit_factor"`` or ``"missing"``).
        """
        # Initialisation de la série des valeurs en tonnes
        tonnes = pd.Series(np.nan, index=qty.index, dtype="float64")
        # Initialisation de la série des origines du tonnage
        source = pd.Series(_SOURCE_MISSING, index=qty.index, dtype=object)
        # Repli/priorité sur le poids net (kg → tonnes)
        if self.prefer_netwgt:
            from_netwgt = nw > 0
            tonnes = tonnes.where(~from_netwgt, nw * 1e-3)
            source = source.where(~from_netwgt, _SOURCE_NETWGT)
        # Quantités déjà exprimées dans une unité couverte par la table de facteurs
        # (poids en kg, tonnes elles-mêmes, ou tout autre code y figurant)
        factor = unit.map(self.tonne_conversion_factors)
        # Quantité manquante exclue : le produit resterait NaN, l'origine aussi
        from_factor = tonnes.isna() & factor.notna() & qty.notna()
        tonnes = tonnes.where(~from_factor, qty * factor)
        source = source.where(~from_factor, _SOURCE_FACTOR)
        return tonnes, source

    # Observations des taux d'une table de flux miroirs
    def _ratio_frame(self, df_mirror: pd.DataFrame) -> pd.DataFrame:
        """Collect the ``(product, unit, ratio)`` observations of a mirror table.

        An observation exists when one side is known in tonnes (net weight or a
        unit of ``tonne_conversion_factors``) and the other side declares a
        strictly positive quantity in a unit absent from that table:
        ``ratio = tonnes / quantity``.

        Args:
            df_mirror: Mirror-flow table (``q_*``, ``unit_*``, ``nw_*``, product).

        Returns:
            Frame with columns ``product``, ``unit`` (integer code) and ``ratio``.
        """
        # Tonnage connu (poids) de chaque côté, sans taux estimé
        t_x, _ = self._tonnes_from_weight(
            df_mirror["q_x"], df_mirror["unit_x"], df_mirror["nw_x"]
        )
        t_m, _ = self._tonnes_from_weight(
            df_mirror["q_m"], df_mirror["unit_m"], df_mirror["nw_m"]
        )
        # Côté export en tonnes et import en unité source, puis l'inverse
        return pd.concat(
            [
                _ratio_observations(
                    df_mirror["q_m"], df_mirror["unit_m"], t_x, df_mirror[_PROD],
                    tonne_conversion_factors=self.tonne_conversion_factors,
                ),
                _ratio_observations(
                    df_mirror["q_x"], df_mirror["unit_x"], t_m, df_mirror[_PROD],
                    tonne_conversion_factors=self.tonne_conversion_factors,
                ),
            ],
            ignore_index=True,
        )

    # Accumulation des observations d'une tranche
    def partial_fit(self, df_mirror: pd.DataFrame) -> "TonnageConverter":
        """Fold the ratio observations of one chunk of mirror flows.

        The rates are pooled over **every** chunk (all the years of a vintage):
        per ``(product, unit)``, only the count, mean and sum of squared
        deviations of the ratios are kept (:class:`WelfordGroupStats`), which is
        exact whatever the partition.

        Args:
            df_mirror: Chunk of the mirror-flow table.

        Returns:
            The converter (``self``); call :meth:`finalize` once every chunk
            has been seen.
        """
        if getattr(self, "_ratio_stats", None) is None:
            self._ratio_stats = WelfordGroupStats(ddof=1)
        self._ratio_stats.partial_fit(
            self._ratio_frame(df_mirror), by=[_PROD, "unit"], value="ratio"
        )
        return self

    # Validation des taux à partir des moments accumulés
    def finalize(self) -> "TonnageConverter":
        """Validate the rates from the accumulated moments.

        A rate is the mean ratio of a ``(product, unit)`` pair, kept when the
        pair counts at least ``min_mirror_flows`` observations and the sample
        standard deviation (``ddof=1``) of its ratios is strictly below
        ``max_conversion_std`` (an undefined deviation rejects the pair).

        Returns:
            The fitted converter (``self``).
        """
        if getattr(self, "_ratio_stats", None) is None:
            self._ratio_stats = WelfordGroupStats(ddof=1)
        df_stats = self._ratio_stats.finalize().stats_
        n_candidate_pairs = int(len(df_stats))
        # Filtres de validité : n ≥ 10 et écart-type < 2,5
        df_valid = df_stats[
            (df_stats["count"] >= self.min_mirror_flows)
            & (df_stats["std"] < self.max_conversion_std)
        ]
        rates: Dict[Tuple[str, int], float] = {
            (product, int(unit)): float(mean)
            for (product, unit), mean in df_valid["mean"].items()
        }

        # Mise à jour des taux de conversion
        self.conversion_rates_ = rates
        # Diagnostics de l'ajustement, complétés par transform
        self.report_ = TonnageReport(
            n_validated_rates=len(rates), n_candidate_pairs=n_candidate_pairs
        )

        # Logging
        logger.info(
            "TonnageConverter: %d taux de conversion validés sur %d couples "
            "(produit, unité) candidats",
            self.report_.n_validated_rates,
            self.report_.n_candidate_pairs,
        )
        return self

    # Estimation des taux de conversion
    def fit(self, df_mirror: pd.DataFrame) -> "TonnageConverter":
        """Estimate per-``(product, unit)`` conversion rates from mirror flows.

        Single-chunk case of :meth:`partial_fit` followed by :meth:`finalize`.

        Args:
            df_mirror: Mirror-flow table from :func:`build_mirror_flows`.

        Returns:
            The fitted converter (``self``).
        """
        # Réinitialisation : fit repart toujours d'un état vide
        self._ratio_stats = WelfordGroupStats(ddof=1)
        return self.partial_fit(df_mirror).finalize()

    # Application des taux : ajout des quantités en tonnes
    def transform(self, df_mirror: pd.DataFrame) -> pd.DataFrame:
        """Add tonne-denominated quantities ``q_x_t`` and ``q_m_t``.

        Also stores the additive counts of the call in ``tally_`` and refreshes
        the shares of ``report_`` from them.

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
        df_out["q_x_t"], source_x = self._to_tonnes(
            df_out["q_x"], df_out["unit_x"], df_out["nw_x"], df_out[_PROD]
        )
        df_out["q_m_t"], source_m = self._to_tonnes(
            df_out["q_m"], df_out["unit_m"], df_out["nw_m"], df_out[_PROD]
        )

        # Diagnostics : parts par origine du tonnage, sur les seuls côtés
        # effectivement déclarés (export et import confondus). Compter les côtés
        # absents ferait de share_tonnage_missing une mesure de la couverture des
        # déclarations — déjà portée par MirrorReport — et non de l'échec de
        # conversion, et rendrait share_converted_from_other_units incomparable
        # aux 8,5 % de la note.
        declared_x = self._is_declared(df_out["q_x"], df_out["nw_x"])
        declared_m = self._is_declared(df_out["q_m"], df_out["nw_m"])
        source = pd.concat([source_x, source_m], ignore_index=True)
        declared = pd.concat([declared_x, declared_m], ignore_index=True)
        source = source[declared]
        self.tally_ = _TonnageTally(
            n_sides=int(len(source)),
            n_rate=int((source == _SOURCE_RATE).sum()),
            n_netwgt=int((source == _SOURCE_NETWGT).sum()),
            n_missing=int((source == _SOURCE_MISSING).sum()),
        )
        self._apply_tally(self.tally_)
        return df_out

    # Mise à jour des parts du rapport à partir de compteurs
    def _apply_tally(self, tally: _TonnageTally) -> None:
        """Set the shares of ``report_`` from additive counts.

        Args:
            tally: Counts of one call of :meth:`transform`, or their sum over
                every chunk.
        """
        denominator = float(tally.n_sides) if tally.n_sides else float("nan")
        self.report_.share_converted_from_other_units = float(tally.n_rate) / denominator
        self.report_.share_tonnage_from_netwgt = float(tally.n_netwgt) / denominator
        self.report_.share_tonnage_missing = float(tally.n_missing) / denominator

        # Logging
        logger.info(
            "TonnageConverter: %.1f%% des observations converties depuis une "
            "autre unité, %.1f%% sans tonnage",
            100.0 * self.report_.share_converted_from_other_units,
            100.0 * self.report_.share_tonnage_missing,
        )

    # Méthode auxiliaire : présence d'une déclaration exploitable d'un côté
    @staticmethod
    def _is_declared(qty: pd.Series, nw: pd.Series) -> pd.Series:
        """Flag the sides carrying a quantity or a net weight.

        Args:
            qty: Declared quantities of one side.
            nw: Net weights of the same side (kilograms).

        Returns:
            Boolean series, ``True`` where the side declared something a tonnage
            could have been derived from.
        """
        # Côté déclaré : quantité renseignée ou poids net strictement positif
        return qty.notna() | (nw > 0)

    # Méthode auxiliaire : conversion complète d'un côté vers la tonne
    def _to_tonnes(
        self, qty: pd.Series, unit: pd.Series, nw: pd.Series, product: pd.Series
    ) -> Tuple[pd.Series, pd.Series]:
        """Convert one side to tonnes, using weight then estimated rates.

        Args:
            qty: Declared quantities.
            unit: Quantity-unit codes.
            nw: Net weights (kilograms).
            product: Product codes (for the rate lookup).

        Returns:
            A tuple ``(tonnes, source)``: the tonnage series (``NaN`` where no
            source is available) and the origin of each value (``"netwgt"``,
            ``"unit_factor"``, ``"rate"`` or ``"missing"``).
        """
        # Tonnage connu (poids net ou unité de poids)
        tonnes, source = self._tonnes_from_weight(qty, unit, nw)
        # Complément par les taux estimés (product, unit)
        missing = tonnes.isna() & qty.notna() & unit.notna()
        if missing.any() and self.conversion_rates_:
            keys = list(zip(product[missing], unit[missing].astype("Int64")))
            rate = np.array(
                [self.conversion_rates_.get((p, int(u)) if pd.notna(u) else None, np.nan)
                 for p, u in keys],
                dtype="float64",
            )
            tonnes.loc[missing] = qty[missing].to_numpy() * rate
            # Origine « taux estimé » réservée aux conversions abouties
            source.loc[missing] = np.where(
                np.isfinite(rate), _SOURCE_RATE, _SOURCE_MISSING
            )
        return tonnes, source


# Fonction auxiliaire : observations des rapports tonnes/unité source
def _ratio_observations(
    qty_unit: pd.Series,
    unit_unit: pd.Series,
    tonnes_other: pd.Series,
    product: pd.Series,
    *,
    tonne_conversion_factors: Mapping[int, float],
) -> pd.DataFrame:
    """Return the ``(product, unit, ratio)`` observations of one side.

    An observation exists when the *other* side is known in tonnes and the current
    side is in a unit with no known factor to tonnes: ``ratio = tonnes_other /
    qty_unit``. Missing unit codes and non-finite ratios are discarded.

    Args:
        qty_unit: Quantities declared in a heterogeneous (source) unit.
        unit_unit: Unit codes of ``qty_unit``.
        tonnes_other: Tonnage of the mirror partner (the tonnes side).
        product: Product codes.
        tonne_conversion_factors: Quantity-unit codes with an already-known
            factor to tonnes (weight units, and the tonne unit itself). All of
            them are excluded from the ratio collection — a quantity already
            expressed in (or convertible to) tonnes must never serve as the
            numerator estimating a rate towards that same target unit.

    Returns:
        Frame with columns ``product``, ``unit`` (integer) and ``ratio``.

    Examples:
        >>> _ratio_observations(
        ...     pd.Series([2.0, 1.0]), pd.Series([5.0, 8.0]), pd.Series([4.0, 4.0]),
        ...     pd.Series(["010121", "010121"]), tonne_conversion_factors={8: 1e-3},
        ... ).to_dict("records")
        [{'product': '010121', 'unit': 5, 'ratio': 2.0}]
    """
    # Côté source dépourvu de facteur connu vers la tonne et quantité strictement positive
    is_source = (~unit_unit.isin(list(tonne_conversion_factors))) & (qty_unit > 0)
    mask = is_source & (tonnes_other > 0)
    ratio = (tonnes_other[mask] / qty_unit[mask]).astype("float64")
    unit = unit_unit[mask]
    # Unité renseignée et rapport fini seulement
    keep = unit.notna() & np.isfinite(ratio)
    return pd.DataFrame(
        {
            _PROD: product[mask][keep].to_numpy(),
            "unit": unit[keep].astype("int64").to_numpy(),
            "ratio": ratio[keep].to_numpy(),
        }
    )


# ──────────────────────────────────────────────────────────────────────
# Étape 2 — Estimation des taux CAF par équation de gravité
# ──────────────────────────────────────────────────────────────────────

# Fonction d'empilement des valeurs unitaires exploitables des deux côtés
def _stacked_unit_values(df_mirror: pd.DataFrame) -> pd.DataFrame:
    """Stack the usable export and import unit values of a mirror table.

    Args:
        df_mirror: Mirror-flow table with tonne quantities (``q_x_t``, ``q_m_t``).

    Returns:
        Frame with columns ``product`` and ``uv``: one row per side whose value
        and tonnage are both strictly positive (missing values excluded).
    """
    frames = []
    for value_col, qty_col in (("v_x", "q_x_t"), ("v_m", "q_m_t")):
        usable = (df_mirror[value_col] > 0) & (df_mirror[qty_col] > 0)
        df_side = df_mirror.loc[usable, [_PROD]].copy()
        df_side["uv"] = df_mirror.loc[usable, value_col] / df_mirror.loc[usable, qty_col]
        frames.append(df_side)
    return pd.concat(frames, ignore_index=True)


# Fonction de calcul des valeurs unitaires médianes mondiales par produit
def world_median_unit_values(df_mirror: pd.DataFrame) -> pd.Series:
    """Compute the world median unit value ``UV^k`` per product.

    Pools the export unit values ``v_x / q_x_t`` **and** the import unit values
    ``v_m / q_m_t``, a proxy of the product's transportability. Both sides are
    used on purpose: ``ln UV^k`` is a regressor of the gravity equation whose
    dependent variable is ``ln(UV^m) - ln(UV^x)``, so a median built on the
    export side alone would correlate mechanically with the dependent variable on
    thin products — where the world median is close to the very ``UV^x`` of the
    flow — and bias the estimated coefficient ``η``.

    The median is taken over **every** year of the frame (``UV^k`` carries no time
    index in the methodology). It is not additive over chunks; on a vintage too
    large for memory, the same statistic is computed by the SQL query of
    :func:`world_median_unit_values_sql`, which this function serves as oracle
    for.

    Args:
        df_mirror: Mirror-flow table with tonne quantities (``q_x_t``, ``q_m_t``).

    Returns:
        Series of median unit values indexed by product (the mean of the two
        central values on an even count).

    Examples:
        >>> df_mirror = pd.DataFrame(
        ...     {
        ...         "product": ["100001", "100001"],
        ...         "v_x": [10.0, float("nan")], "q_x_t": [1.0, float("nan")],
        ...         "v_m": [float("nan"), 30.0], "q_m_t": [float("nan"), 1.0],
        ...     }
        ... )
        >>> float(world_median_unit_values(df_mirror).loc["100001"])
        20.0
    """
    # Calcul de la médiane par produit sur les deux côtés empilés
    return _stacked_unit_values(df_mirror).groupby(_PROD)["uv"].median()


# Fonction auxiliaire : condition SQL « valeur renseignée » au sens de pandas
def _sql_present(expr: str) -> str:
    """SQL test equivalent to pandas ``notna`` (``NULL`` and ``NaN`` excluded)."""
    return f"({expr} IS NOT NULL AND NOT isnan(CAST({expr} AS DOUBLE)))"


# Fonction auxiliaire : condition SQL « strictement positif » au sens de pandas
def _sql_positive(expr: str) -> str:
    """SQL test equivalent to pandas ``> 0`` (``NaN`` compares greater in DuckDB)."""
    return f"({expr} > 0 AND NOT isnan(CAST({expr} AS DOUBLE)))"


# Fonction de génération du SQL équivalent à world_median_unit_values
def world_median_unit_values_sql(
    mirror_relation: str,
    rates_relation: str,
    *,
    tonne_conversion_factors: Mapping[int, float],
    prefer_netwgt: bool = True,
    product_range: bool = False,
) -> str:
    """Build the DuckDB query computing ``UV^k`` on mirror flows stored out of core.

    Reproduces, in SQL, :meth:`TonnageConverter.transform` followed by
    :func:`world_median_unit_values`, on the **raw** mirror columns (``v_*``,
    ``q_*``, ``unit_*``, ``nw_*``):

    1. tonnage of each side, first source available: net weight in tonnes
       (``nw · 0.001``) when ``prefer_netwgt`` and ``nw > 0``; quantity times the
       unit factor when the unit appears in ``tonne_conversion_factors`` and the
       quantity is present; quantity times the estimated rate of
       ``(product, unit)`` otherwise (missing when no rate);
    2. unit value ``v / q_t`` of each side with ``v > 0`` and ``q_t > 0``;
    3. ``median()`` per product — DuckDB's interpolated median, which averages
       the two central values like pandas.

    Two DuckDB traps are neutralised: decimal literals are read as ``DECIMAL``
    (constants are cast to ``DOUBLE``), and ``NaN`` compares *greater* than any
    number (every comparison is guarded by ``isnan``). The query builds text
    only; executing it belongs to the caller. The median being per product, the
    query is exact on any partition of the products, which bounds the memory
    of the engine (``product_range=True``).

    Args:
        mirror_relation: SQL relation of the mirror flows (a table name or e.g.
            ``read_parquet([...])``).
        rates_relation: SQL relation of the validated rates, with columns
            ``product`` (text), ``unit`` (integer) and ``rate`` (double).
        tonne_conversion_factors: Mapping unit code → factor to tonnes.
        prefer_netwgt: Whether the net weight takes precedence.
        product_range: When ``True``, restricts the products to
            ``BETWEEN ? AND ?`` (two bound parameters, repeated once per side:
            ``[low, high, low, high]``).

    Returns:
        The SQL text, yielding columns ``product`` and ``uv`` sorted by product.

    Examples:
        >>> sql = world_median_unit_values_sql("mirror", "rates",
        ...                                    tonne_conversion_factors={8: 1e-3})
        >>> "median(v / q_t)" in sql and "CAST('0.001' AS DOUBLE)" in sql
        True
    """
    # Table des facteurs de conversion (au moins une ligne factice pour le schéma)
    if tonne_conversion_factors:
        rows = ", ".join(
            f"(CAST('{float(unit)!r}' AS DOUBLE), CAST('{float(factor)!r}' AS DOUBLE))"
            for unit, factor in tonne_conversion_factors.items()
        )
        factors_cte = f"factors(unit, factor) AS (VALUES {rows})"
    else:
        factors_cte = (
            "factors AS (SELECT CAST(NULL AS DOUBLE) AS unit, "
            "CAST(NULL AS DOUBLE) AS factor WHERE false)"
        )
    where = ' WHERE "product" BETWEEN ? AND ?' if product_range else ""
    prefer = "TRUE" if prefer_netwgt else "FALSE"
    return f"""
WITH {factors_cte},
sides AS (
    SELECT "product", v_x AS v, q_x AS q, unit_x AS unit, nw_x AS nw FROM {mirror_relation}{where}
    UNION ALL
    SELECT "product", v_m AS v, q_m AS q, unit_m AS unit, nw_m AS nw FROM {mirror_relation}{where}
),
tonnes AS (
    SELECT
        s."product",
        s.v,
        CASE
            WHEN {prefer} AND {_sql_positive('s.nw')} THEN s.nw * CAST('0.001' AS DOUBLE)
            WHEN f.factor IS NOT NULL AND {_sql_present('s.q')} THEN s.q * f.factor
            WHEN {_sql_present('s.q')} AND {_sql_present('s.unit')} THEN s.q * r.rate
        END AS q_t
    FROM sides AS s
    LEFT JOIN factors AS f ON CAST(s.unit AS DOUBLE) = f.unit
    LEFT JOIN {rates_relation} AS r
        ON s."product" = r."product"
        AND {_sql_present('s.unit')}
        AND CAST(s.unit AS BIGINT) = r.unit
)
SELECT "product", median(v / q_t) AS uv
FROM tonnes
WHERE {_sql_positive('v')} AND {_sql_positive('q_t')}
GROUP BY "product"
ORDER BY "product"
"""


# Diagnostics de l'équation de gravité
@dataclass
class GravityReport:
    """Diagnostics of the CIF gravity estimation.

    Attributes:
        n_obs: Observations retained by the final weighted estimation.
        n_cook_dropped: Influential observations removed by Cook's distance.
        r_squared: ``R²`` of the final estimation.
        mean_freight_rate: Mean estimated freight rate over the mirror flows.
        median_freight_rate: Median estimated freight rate.
        p10_freight_rate: First decile of the estimated freight rate.
        p90_freight_rate: Last decile of the estimated freight rate.
        share_flows_without_prediction: Share of mirror flows left without a
            freight-rate prediction (missing gravity variables).
        coefficients: Estimated coefficients of the gravity equation.
    """
    n_obs: int = 0
    n_cook_dropped: int = 0
    r_squared: float = float("nan")
    mean_freight_rate: float = float("nan")
    median_freight_rate: float = float("nan")
    p10_freight_rate: float = float("nan")
    p90_freight_rate: float = float("nan")
    share_flows_without_prediction: float = float("nan")
    coefficients: Dict[str, float] = field(default_factory=dict)


# Probabilités des quantiles de taux de fret publiés (p10, médiane, p90)
FREIGHT_QUANTILES: Tuple[float, float, float] = (0.1, 0.5, 0.9)


# Compteurs additifs de la distribution des taux de fret
@dataclass
class _FreightTally:
    """Additive part of the freight-rate distribution of :class:`GravityReport`.

    The mean and the share without prediction add up over chunks; the
    quantiles do not and are supplied separately (:meth:`apply`).

    Attributes:
        n_flows: Mirror flows predicted.
        n_finite: Of which with a finite freight rate.
        sum_finite: Sum of the finite freight rates.
    """
    n_flows: int = 0
    n_finite: int = 0
    sum_finite: float = 0.0

    # Compteurs d'une série de taux de fret
    @classmethod
    def from_rates(cls, rate: pd.Series) -> "_FreightTally":
        """Count the predicted freight rates of a chunk.

        Args:
            rate: Freight rates ``τ̂`` of the chunk's flows.

        Returns:
            The tally.
        """
        finite = rate.replace([np.inf, -np.inf], np.nan).dropna()
        return cls(n_flows=int(len(rate)), n_finite=int(len(finite)), sum_finite=float(finite.sum()))

    # Somme de deux tallies
    def __add__(self, other: "_FreightTally") -> "_FreightTally":
        return _add_tallies(self, other)

    # Report des statistiques de taux de fret dans le rapport
    def apply(self, report: GravityReport, quantiles: Sequence[float]) -> None:
        """Fill the freight-rate fields of a gravity report.

        Args:
            report: Report to complete in place.
            quantiles: p10, median and p90 of the finite freight rates (in the
                order of :data:`FREIGHT_QUANTILES`).
        """
        has_rates = self.n_finite > 0
        p10, median, p90 = (float(q) for q in quantiles)
        report.mean_freight_rate = self.sum_finite / self.n_finite if has_rates else float("nan")
        report.median_freight_rate = median if has_rates else float("nan")
        report.p10_freight_rate = p10 if has_rates else float("nan")
        report.p90_freight_rate = p90 if has_rates else float("nan")
        report.share_flows_without_prediction = (
            float(self.n_flows - self.n_finite) / float(self.n_flows)
            if self.n_flows
            else float("nan")
        )


# Résultat de l'ajustement final de la gravité
@dataclass
class GravityFit:
    """Final weighted fit of the gravity equation (the former ``statsmodels`` results).

    Attributes:
        params: Coefficients, indexed by design column (``const`` first).
        bse: Standard errors (non-robust WLS, ``√diag(MSE · (XᵀWX)⁻¹)``).
        rsquared: ``R²`` of the weighted fit (centred, the design holding a
            constant).
        nobs: Number of observations of the fit.
        df_resid: Residual degrees of freedom.
        mse: Mean squared residual ``RSS / df_resid``.
    """
    params: pd.Series
    bse: pd.Series
    rsquared: float
    nobs: int
    df_resid: int
    mse: float

    # Construction depuis une solution des moindres carrés pondérés
    @classmethod
    def from_solution(cls, solution: WlsSolution) -> "GravityFit":
        """Wrap a :class:`~macroforecast.trade.processing.streaming.WlsSolution`.

        Args:
            solution: Weighted least-squares solution.

        Returns:
            The fit.
        """
        return cls(
            params=pd.Series(solution.params, index=solution.columns, dtype="float64"),
            bse=pd.Series(solution.bse, index=solution.columns, dtype="float64"),
            rsquared=float(solution.r_squared),
            nobs=int(solution.nobs),
            df_resid=int(solution.df_resid),
            mse=float(solution.mse),
        )


# Régresseurs continus de l'équation de gravité, dans l'ordre du design
_GRAVITY_REGRESSORS: Tuple[str, ...] = (
    "ln_dist", "ln_dist2", "contig", "landlocked_i", "landlocked_j", "ln_uv",
)
# Colonne de constante du design
_CONST = "const"
# Préfixe des indicatrices d'année (nommage de pandas.get_dummies)
_YEAR_PREFIX = "year_"
# Taille des sous-blocs de lignes du design dense (indicatrices d'année comprises)
_GRAVITY_BLOCK_ROWS = 1_000_000


# Estimateur de l'équation de gravité des taux CAF
class CifGravityModel:
    """Estimate CIF (freight) rates by a weighted gravity equation.

    The dependent variable is ``ln(UVm / UVx)`` on complete mirror flows;
    regressors are ``ln distw``, ``(ln distw)^2``, contiguity, exporter/importer
    landlocked indicators, ``ln UV^k`` and year dummies. Estimation is a weighted
    least squares (weight ``min(Q)/max(Q)``), robustified by dropping influential
    observations flagged by Cook's distance — computed on the ``√w``-whitened
    model so the weights are accounted for — before the final fit. The predicted
    value ``exp(X β̂)`` estimates ``1 + τ`` (see :meth:`predict`).

    The estimation is **pooled over every year** of the data (methodology:
    pooled OLS over the period of validity of a nomenclature, with year
    dummies). It is therefore expressed by sufficient statistics accumulated
    chunk by chunk, in two phases over the same chunks:

    1. ``phase="fit"`` (:meth:`partial_fit` then :meth:`finalize`): first fit,
       from the QR factor of ``[√w·X | √w·y]``
       (:class:`~macroforecast.trade.processing.streaming.WeightedLeastSquaresAccumulator`);
    2. ``phase="cook"``: Cook's distance of each observation from the first fit
       (:class:`~macroforecast.trade.processing.streaming.CookFilter`), cut-off
       ``cook_factor / N``; the kept observations feed the final fit. The final
       fit replaces the first only when more observations than design columns
       remain and at least one was dropped.

    Year dummies follow ``pandas.get_dummies(drop_first=True)`` on the estimation
    sample (complete mirror flows with positive values and tonnages), **before**
    the removal of non-finite rows: the reference is the first year present in
    that sample, and a year whose rows are all removed afterwards keeps a null
    column (zero coefficient, minimum-norm solution). The dummies of every
    year of the universe are accumulated, the selection happening at
    :meth:`finalize`.

    Args:
        cook_factor: Cook's-distance cutoff factor; observations with
            ``cook >= cook_factor / n`` are dropped before the final fit.

    Attributes:
        result_: :class:`GravityFit` of the final estimation.
        design_columns_: Ordered design-matrix columns (excluding the constant).
        uv_world_: World median unit values used at fit time.
        years_: Year universe of the dummies.
        cif_rate_: Freight rates predicted on the frame passed to :meth:`fit`;
            callers reuse it instead of calling :meth:`predict` again (set by
            :meth:`fit` only, not by the chunked path).
        report_: :class:`GravityReport` of the estimation; :meth:`fit` also fills
            the freight-rate distribution.
    """

    # Initialisation
    def __init__(self, *, cook_factor: float = 4.0) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.cook_factor = cook_factor

    # Régresseurs continus d'une table de flux
    def _regressors(self, df_flows: pd.DataFrame, df_gravity: pd.DataFrame) -> pd.DataFrame:
        """Build the continuous gravity regressors of each flow.

        Args:
            df_flows: Flows carrying the mirror identities and the product.
            df_gravity: Bilateral gravity frame from :func:`build_gravity_data`.

        Returns:
            Frame indexed like ``df_flows`` with the columns of
            ``_GRAVITY_REGRESSORS`` (``NaN``/``inf`` where an input is missing).

        Raises:
            ValueError: If ``df_gravity`` carries duplicate ``(iso_o, iso_d)``
                pairs (the join would duplicate flows).
        """
        # Jointure des variables de gravité (exportateur = origine, importateur =
        # destination) sur les seules clés : pas de copie des autres colonnes
        df_merged = df_flows[[_EXP, _IMP]].merge(
            df_gravity.rename(columns={"iso_o": _EXP, "iso_d": _IMP}),
            on=[_EXP, _IMP],
            how="left",
        )
        if len(df_merged) != len(df_flows):
            raise ValueError(
                f"The gravity join duplicated rows ({len(df_flows)} flows in, "
                f"{len(df_merged)} out): df_gravity carries duplicate "
                "(iso_o, iso_d) pairs."
            )
        # Restauration de l'index d'entrée : une jointure sur colonnes renvoie un
        # RangeIndex, ce qui désalignerait silencieusement la série de predict de
        # df_mirror (et, en aval, annulerait la fobisation sans erreur)
        df_merged.index = df_flows.index
        df_design = pd.DataFrame(index=df_flows.index)
        df_design["ln_dist"] = np.log(df_merged["distw"])
        df_design["ln_dist2"] = df_design["ln_dist"] ** 2
        df_design["contig"] = df_merged["contig"].astype("float64")
        df_design["landlocked_i"] = df_merged["landlocked_o"].astype("float64")
        df_design["landlocked_j"] = df_merged["landlocked_d"].astype("float64")
        # Valeur unitaire médiane mondiale du produit
        df_design["ln_uv"] = np.log(df_flows[_PROD].map(self.uv_world_).astype("float64"))
        return df_design

    # Échantillon d'estimation d'une tranche
    def _sample(
        self, df_mirror: pd.DataFrame, df_gravity: pd.DataFrame
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Return the estimation sample of a chunk.

        Args:
            df_mirror: Chunk of mirror flows with tonne quantities.
            df_gravity: Bilateral gravity frame.

        Returns:
            A tuple ``(regressors, year_position, y, w, valid)``: continuous
            regressors and year positions of the complete mirror flows, the
            dependent variable ``ln(UVm/UVx)``, the weights ``min(Q)/max(Q)`` and
            the mask of the finite rows (the rows actually fitted).

        Raises:
            ValueError: If a year lies outside the year universe.
        """
        # Échantillon d'estimation : flux miroirs complets exploitables
        df_sample = df_mirror[
            (df_mirror["v_x"] > 0)
            & (df_mirror["v_m"] > 0)
            & (df_mirror["q_x_t"] > 0)
            & (df_mirror["q_m_t"] > 0)
        ]
        # Variable dépendante : log du rapport des valeurs unitaires (CAF/FAB)
        uv_x = df_sample["v_x"] / df_sample["q_x_t"]
        uv_m = df_sample["v_m"] / df_sample["q_m_t"]
        y = np.log(uv_m / uv_x).to_numpy(dtype="float64")
        # Pondération : rapport des quantités miroirs min/max ∈ (0, 1]
        q_min = np.minimum(df_sample["q_x_t"], df_sample["q_m_t"])
        q_max = np.maximum(df_sample["q_x_t"], df_sample["q_m_t"])
        w = (q_min / q_max).to_numpy(dtype="float64")
        regressors = self._regressors(df_sample, df_gravity).to_numpy(dtype="float64")
        # Position de l'année dans l'univers des indicatrices
        year_position = pd.Index(self.years_).get_indexer(
            df_sample[_YEAR].astype("int64").to_numpy()
        )
        if (year_position < 0).any():
            unknown = sorted(set(df_sample[_YEAR][year_position < 0].astype(int)))
            raise ValueError(f"Years outside the gravity year universe: {unknown}")
        # Lignes non exploitables retirées (gravité/UV manquants, y ou poids non finis)
        valid = np.isfinite(regressors).all(axis=1) & np.isfinite(y) & np.isfinite(w)
        return regressors, year_position, y, w, valid

    # Matrice de design dense d'un sous-bloc de lignes
    def _design_block(self, regressors: np.ndarray, year_position: np.ndarray) -> np.ndarray:
        """Assemble constant, regressors and year dummies of every year.

        Args:
            regressors: Continuous regressors, shape ``(n, 6)``.
            year_position: Position of each row's year in ``years_``.

        Returns:
            Dense design, columns ``[const, regressors…, year_<y> for y in years_]``.
        """
        n = len(regressors)
        X = np.zeros((n, 1 + len(_GRAVITY_REGRESSORS) + len(self.years_)))
        X[:, 0] = 1.0
        X[:, 1 : 1 + len(_GRAVITY_REGRESSORS)] = regressors
        X[np.arange(n), 1 + len(_GRAVITY_REGRESSORS) + year_position] = 1.0
        return X

    # Accumulation d'une tranche (ajustement initial ou filtre de Cook)
    def partial_fit(
        self,
        df_mirror: pd.DataFrame,
        df_gravity: pd.DataFrame,
        *,
        uv_world: Optional[pd.Series] = None,
        years: Optional[Sequence[int]] = None,
        phase: Literal["fit", "cook"] = "fit",
    ) -> "CifGravityModel":
        """Fold one chunk of mirror flows into the current phase.

        Args:
            df_mirror: Chunk of mirror flows with tonne quantities.
            df_gravity: Bilateral gravity frame.
            uv_world: World median unit values per product, pooled over every
                year (:func:`world_median_unit_values`); required on the first
                ``"fit"`` call.
            years: Year universe of the dummies (every year any chunk may
                carry); required on the first ``"fit"`` call.
            phase: ``"fit"`` (first fit) or ``"cook"`` (Cook filter and final
                fit, after :meth:`finalize` closed the first phase).

        Returns:
            The model (``self``).

        Raises:
            ValueError: If the phase is out of order, if ``uv_world``/``years``
                are missing on the first call, or if a year lies outside the
                universe.
        """
        state = getattr(self, "_phase", None)
        if phase == "fit":
            # Premier appel : univers des années et médianes figés pour la passe
            if state != "fit":
                if uv_world is None or years is None:
                    raise ValueError("uv_world and years are required on the first 'fit' call.")
                self.uv_world_ = uv_world
                self.years_ = sorted(int(y) for y in years)
                columns = (
                    [_CONST]
                    + list(_GRAVITY_REGRESSORS)
                    + [f"{_YEAR_PREFIX}{y}" for y in self.years_]
                )
                self._acc_fit = WeightedLeastSquaresAccumulator(columns)
                self._sample_year_counts = np.zeros(len(self.years_), dtype="int64")
                self._phase = "fit"
            regressors, year_position, y, w, valid = self._sample(df_mirror, df_gravity)
            # Effectifs de l'échantillon par année AVANT retrait des lignes non finies
            self._sample_year_counts += np.bincount(year_position, minlength=len(self.years_))
            for start in range(0, len(y), _GRAVITY_BLOCK_ROWS):
                part = slice(start, start + _GRAVITY_BLOCK_ROWS)
                keep = valid[part]
                X = self._design_block(regressors[part][keep], year_position[part][keep])
                self._acc_fit.partial_fit(X, y[part][keep], w[part][keep])
        elif phase == "cook":
            if state != "cook":
                raise ValueError("Call finalize() on the 'fit' phase before the 'cook' phase.")
            regressors, year_position, y, w, valid = self._sample(df_mirror, df_gravity)
            for start in range(0, len(y), _GRAVITY_BLOCK_ROWS):
                part = slice(start, start + _GRAVITY_BLOCK_ROWS)
                keep = valid[part]
                X = self._design_block(regressors[part][keep], year_position[part][keep])
                X = X[:, self._selected_idx]
                y_part, w_part = y[part][keep], w[part][keep]
                kept = self._cook.keep_mask(X, y_part, w_part)
                self._n_cook_seen += len(y_part)
                self._acc_cook.partial_fit(X[kept], y_part[kept], w_part[kept])
        else:
            raise ValueError(f"phase must be 'fit' or 'cook', got {phase!r}")
        return self

    # Clôture de la phase courante
    def finalize(self) -> "CifGravityModel":
        """Close the current phase.

        After ``"fit"``: solves the first fit on the selected columns and prepares
        the Cook filter. After ``"cook"``: keeps the final fit (or the first one,
        see the class docstring) and builds ``result_`` and ``report_``.

        Returns:
            The model (``self``).

        Raises:
            ValueError: If no usable observation remains, or if no phase is open.
        """
        state = getattr(self, "_phase", None)
        if state == "fit":
            if self._acc_fit.n_obs_ == 0:
                raise ValueError("No usable observation for the gravity estimation")
            # Indicatrices des années présentes dans l'échantillon, référence = la première
            present = [
                year for year, count in zip(self.years_, self._sample_year_counts) if count > 0
            ]
            selected = (
                [_CONST]
                + list(_GRAVITY_REGRESSORS)
                + [f"{_YEAR_PREFIX}{year}" for year in present[1:]]
            )
            position = {name: i for i, name in enumerate(self._acc_fit.columns)}
            self._selected_idx = np.array([position[name] for name in selected], dtype="int64")
            self.design_columns_ = selected[1:]
            self._first_fit = self._acc_fit.solve(selected)
            # Distance de Cook : N et nombre de colonnes de l'ajustement initial
            self._cook = CookFilter(
                self._first_fit,
                cook_factor=self.cook_factor,
                n_obs=self._first_fit.nobs,
                k_vars=len(selected),
            )
            self._acc_cook = WeightedLeastSquaresAccumulator(selected)
            self._n_cook_seen = 0
            self._phase = "cook"
        elif state == "cook":
            n_kept = int(self._acc_cook.n_obs_)
            n_dropped = int(self._n_cook_seen - n_kept)
            # Second ajustement seulement s'il reste plus d'observations que de
            # colonnes et qu'au moins une observation a été retirée
            if n_kept > len(self._first_fit.columns) and n_dropped > 0:
                solution = self._acc_cook.solve()
                n_cook_dropped = n_dropped
                # Logging
                logger.info(
                    "CifGravityModel: %d/%d observations influentes retirées (Cook)",
                    n_cook_dropped, self._n_cook_seen,
                )
            else:
                solution = self._first_fit
                n_cook_dropped = 0
            # Sauvegarde du résultat
            self.result_ = GravityFit.from_solution(solution)
            # Diagnostics de l'estimation (distribution des taux de fret à part)
            self.report_ = GravityReport(
                n_obs=int(solution.nobs),
                n_cook_dropped=n_cook_dropped,
                r_squared=float(solution.r_squared),
                coefficients={
                    str(name): float(value) for name, value in self.result_.params.items()
                },
            )
            self._phase = "done"
        else:
            raise ValueError("No open phase to finalize: call partial_fit first.")
        return self

    # Estimation du modèle
    def fit(self, df_mirror: pd.DataFrame, df_gravity: pd.DataFrame) -> "CifGravityModel":
        """Estimate the gravity equation on complete mirror flows.

        Single-chunk case of the two phases of :meth:`partial_fit`; also predicts
        ``cif_rate_`` on ``df_mirror`` and fills the freight-rate distribution of
        ``report_``.

        Args:
            df_mirror: Mirror-flow table with tonne quantities.
            df_gravity: Bilateral gravity frame.

        Returns:
            The fitted model (``self``).

        Raises:
            ValueError: If no usable observation remains after filtering.
        """
        # Réinitialisation : fit repart toujours d'un état vide
        self._phase = None
        # Valeurs unitaires mondiales calées sur l'échantillon, univers des années
        uv_world = world_median_unit_values(df_mirror)
        years = sorted(int(year) for year in df_mirror[_YEAR].dropna().unique())
        self.partial_fit(df_mirror, df_gravity, uv_world=uv_world, years=years, phase="fit")
        self.finalize()
        self.partial_fit(df_mirror, df_gravity, phase="cook")
        self.finalize()

        # Taux de fret prédits sur l'ensemble des flux miroirs : le calcul
        # appartient au modèle, seul détenteur du design et des coefficients
        self.cif_rate_ = self.predict(df_mirror, df_gravity)
        finite = self.cif_rate_.replace([np.inf, -np.inf], np.nan).dropna()
        quantiles = (
            [float(finite.quantile(q)) for q in FREIGHT_QUANTILES]
            if len(finite)
            else [float("nan")] * len(FREIGHT_QUANTILES)
        )
        _FreightTally.from_rates(self.cif_rate_).apply(self.report_, quantiles)

        # Logging
        logger.info(
            "CifGravityModel: fit sur %d observations, taux de fret moyen %.3f",
            self.report_.n_obs, self.report_.mean_freight_rate,
        )
        return self

    # Diagnostic : observations conservées par le filtre de Cook
    def cook_keep_mask(self, df_mirror: pd.DataFrame, df_gravity: pd.DataFrame) -> pd.Series:
        """Tell, for each estimation observation of a chunk, whether Cook kept it.

        Evaluates the Cook cut-off of the first fit on the rows of ``df_mirror``
        that enter the estimation (complete mirror flows with finite design);
        usable on any chunk once the ``"fit"`` phase is finalized.

        Args:
            df_mirror: Mirror flows with tonne quantities.
            df_gravity: Bilateral gravity frame.

        Returns:
            Boolean Series indexed like the estimation rows of ``df_mirror``,
            ``True`` for the observations kept in the final fit's sample.

        Raises:
            AttributeError: If the ``"fit"`` phase has not been finalized.
        """
        if not hasattr(self, "_cook"):
            raise AttributeError("The 'fit' phase must be finalized before evaluating Cook's filter.")
        complete = (
            (df_mirror["v_x"] > 0)
            & (df_mirror["v_m"] > 0)
            & (df_mirror["q_x_t"] > 0)
            & (df_mirror["q_m_t"] > 0)
        )
        regressors, year_position, y, w, valid = self._sample(df_mirror, df_gravity)
        X = self._design_block(regressors[valid], year_position[valid])[:, self._selected_idx]
        index = df_mirror.index[complete.to_numpy()][valid]
        return pd.Series(self._cook.keep_mask(X, y[valid], w[valid]), index=index)

    # Prédiction du taux de fret
    def predict(self, df_mirror: pd.DataFrame, df_gravity: pd.DataFrame) -> pd.Series:
        """Predict the freight rate ``exp(X β̂) − 1`` for each flow.

        Works on any chunk: the prediction of a flow depends only on its own
        regressors and year, and on the pooled coefficients.

        Args:
            df_mirror: Mirror-flow table with tonne quantities.
            df_gravity: Bilateral gravity frame.

        Returns:
            Series of estimated freight rates ``τ̂`` aligned on ``df_mirror``
            (``NaN`` where gravity variables are missing). The dependent variable
            is ``ln(UVm/UVx) = ln(1 + τ)``, so the freight rate is
            ``τ̂ = exp(X β̂) − 1`` and the fobisation divides by ``1 + τ̂``. A year
            without dummy (reference year, or year absent from the estimation)
            takes the reference level.

        Raises:
            AttributeError: If called before the model is fitted.
        """
        params = self.result_.params
        regressors = self._regressors(df_mirror, df_gravity)
        pred = params[_CONST] + regressors.to_numpy(dtype="float64") @ params[
            list(_GRAVITY_REGRESSORS)
        ].to_numpy(dtype="float64")
        # Effet d'année : coefficient de l'indicatrice, nul pour la référence
        year_effect = {
            int(name[len(_YEAR_PREFIX):]): float(value)
            for name, value in params.items()
            if str(name).startswith(_YEAR_PREFIX)
        }
        effect = df_mirror[_YEAR].map(year_effect).fillna(0.0).to_numpy(dtype="float64")
        # La régression prédit ln(1 + τ) : le taux de fret est exp(prédiction) − 1
        return pd.Series(np.expm1(pred + effect), index=df_mirror.index, dtype="float64")


# ──────────────────────────────────────────────────────────────────────
# Étape 3 — Retrait du fret des importations (fobisation)
# ──────────────────────────────────────────────────────────────────────

# Diagnostics de la fobisation
@dataclass
class FobisationReport:
    """Diagnostics of the import fobisation step.

    Attributes:
        share_flows_corrected: Share of all mirror flows whose import value was
            actually corrected.
        share_import_flows_corrected: Share of the flows carrying an import
            declaration that were corrected.
        share_skipped_non_cif: Share of flows left untouched because the
            importer's inferred regime is FOB.
        share_reverted_fas: Share of flows whose FAS correction was abandoned,
            for either reason — it widened the mirror gap, or no mirror
            declaration allowed it to be tested at all.
        share_fas_unverifiable: Subset of the above left uncorrected purely for
            lack of a mirror declaration. The note conditions the FAS correction
            on it *reducing* the mirror gap, which cannot be established without
            a mirror; a value close to ``share_reverted_fas`` means the FAS
            treatment is driven by untestable cases rather than by measured
            deterioration.
        share_clipped_to_zero: Share of flows whose corrected value hit the zero
            floor.
        mean_correction_ratio: Mean ``v_m_fob / v_m`` over the corrected flows.
        n_importers_detected_fob: Importer-year cells inferred as FOB.
        n_importers_regime_unknown: Importer-year cells carrying no valuation
            information at all.
    """
    share_flows_corrected: float = float("nan")
    share_import_flows_corrected: float = float("nan")
    share_skipped_non_cif: float = float("nan")
    share_reverted_fas: float = float("nan")
    share_fas_unverifiable: float = float("nan")
    share_clipped_to_zero: float = float("nan")
    mean_correction_ratio: float = float("nan")
    n_importers_detected_fob: int = 0
    n_importers_regime_unknown: int = 0


# Compteurs additifs de la fobisation
@dataclass
class _FobisationTally:
    """Additive counts behind :class:`FobisationReport`.

    Attributes:
        n_flows: Mirror flows seen.
        n_imports: Flows carrying an import declaration.
        n_corrected: Import values actually corrected.
        n_skipped_non_cif: Import flows left untouched (FOB regime).
        n_reverted_fas: FAS corrections abandoned.
        n_fas_unverifiable: Of which for lack of a mirror declaration.
        n_clipped: Corrected values floored at zero.
        sum_ratio: Sum of the finite ``v_m_fob / v_m`` ratios of corrected flows.
        n_ratio: Number of those finite ratios.
    """
    n_flows: int = 0
    n_imports: int = 0
    n_corrected: int = 0
    n_skipped_non_cif: int = 0
    n_reverted_fas: int = 0
    n_fas_unverifiable: int = 0
    n_clipped: int = 0
    sum_ratio: float = 0.0
    n_ratio: int = 0

    # Somme de deux tallies
    def __add__(self, other: "_FobisationTally") -> "_FobisationTally":
        return _add_tallies(self, other)

    # Rapport correspondant
    def report(self, df_regime: pd.DataFrame) -> FobisationReport:
        """Return the :class:`FobisationReport` of the counted flows.

        Args:
            df_regime: Inferred valuation regimes (importer counts).

        Returns:
            The report.
        """
        def share(count: int, total: int) -> float:
            return float(count) / float(total) if total else float("nan")

        return FobisationReport(
            share_flows_corrected=share(self.n_corrected, self.n_flows),
            share_import_flows_corrected=share(self.n_corrected, self.n_imports),
            share_skipped_non_cif=share(self.n_skipped_non_cif, self.n_flows),
            share_reverted_fas=share(self.n_reverted_fas, self.n_flows),
            share_fas_unverifiable=share(self.n_fas_unverifiable, self.n_flows),
            share_clipped_to_zero=share(self.n_clipped, self.n_flows),
            mean_correction_ratio=(
                self.sum_ratio / self.n_ratio
                if self.n_corrected and self.n_ratio
                else float("nan")
            ),
            n_importers_detected_fob=_count_importers(df_regime, _FOB_REGIME),
            n_importers_regime_unknown=_count_importers(df_regime, None),
        )


# Transformateur de fobisation des valeurs d'importation
class Fobizer:
    """Strip the estimated freight from CIF import values.

    Computes ``V_m_fob = V_m / (1 + cif_rate)`` under the note's safeguards, in
    strict priority order: conditional correction for FAS importers (kept only
    when it *demonstrably* reduces the mirror gap), no correction for importers
    whose inferred valuation regime is FOB, unconditional correction floored at
    zero otherwise.

    The FAS condition is read strictly: an import declaration with no export
    mirror offers nothing to compare, so the correction is abandoned rather than
    presumed beneficial. Applying it there would mean correcting precisely where
    the note's own criterion cannot be evaluated — and on a reporter the note
    singles out because the correction is *not* systematically an improvement.

    The regime itself is no longer a parameter: it is inferred from the data by
    :func:`infer_import_valuation_regime` and handed to :meth:`transform`.

    Args:
        fas_countries: Importer ISO-3 codes declaring FAS (freight stripped only
            when it reduces the mirror gap, and only where that can be checked).
            FAS is not distinguishable from FOB in the COMTRADE valuation
            columns — a FAS reporter fills ``fobvalue`` — while the methodology
            reserves it a *conditional* correction rather than no correction at
            all, hence this list stays in configuration and takes priority over
            the inferred regime.

    Attributes:
        report_: :class:`FobisationReport` of the step, populated by
            :meth:`transform`.
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

        Raises:
            ValueError: If ``cif_rate`` is not aligned on ``df_mirror``.
        """
        # Copie indépendante des données
        df_out = df_mirror.copy()
        # Extraction de la valeur CIF (qui correspond généralement aux données d'importation)
        v_m = df_out["v_m"]

        # Alignement exigé plutôt que subi : un désalignement rendrait le taux
        # manquant partout, et la fobisation serait sautée sans le moindre signal
        if not cif_rate.index.equals(df_out.index):
            raise ValueError(
                "cif_rate is not aligned on df_mirror: expected the index of "
                "the mirror-flow table, got a different one. Pass the series "
                "returned by CifGravityModel.predict on this very frame."
            )

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
        revert = pd.Series(False, index=df_out.index)
        unverifiable = pd.Series(False, index=df_out.index)
        if fas.any():
            has_mirror = (df_out["v_x"] > 0) & (v_m > 0)
            gap_before = np.abs(np.log(df_out["v_x"] / v_m))
            gap_after = np.abs(np.log(df_out["v_x"] / candidate.replace(0.0, np.nan)))
            # Ne garder la correction FAS que lorsqu'elle réduit l'écart. Faute
            # de flux miroir, le critère de la note est intestable : la
            # correction est alors abandonnée plutôt que présumée bénéfique —
            # « que si elle réduit l'écart » exclut d'agir sans pouvoir vérifier.
            unverifiable = fas & ~has_mirror
            revert = fas & (~has_mirror | ~(gap_after < gap_before))
            candidate = candidate.where(~revert, v_m)

        df_out["v_m_fob"] = candidate

        # Diagnostics : masques de correction, connus de cette étape seule, réduits
        # à des compteurs additifs (partagés avec le traitement par passes)
        has_import = v_m.notna()
        corrected = has_import & candidate.notna() & (candidate != v_m)
        ratio = (candidate[corrected] / v_m[corrected]).replace([np.inf, -np.inf], np.nan)
        ratio = ratio.dropna()
        self.tally_ = _FobisationTally(
            n_flows=int(len(df_out)),
            n_imports=int(has_import.sum()),
            n_corrected=int(corrected.sum()),
            n_skipped_non_cif=int((no_correction & has_import).sum()),
            n_reverted_fas=int(revert.sum()),
            n_fas_unverifiable=int(unverifiable.sum()),
            n_clipped=int((corrected & (candidate <= 0.0)).sum()),
            sum_ratio=float(ratio.sum()),
            n_ratio=int(len(ratio)),
        )
        self.report_ = self.tally_.report(df_regime)

        # Logging
        logger.info(
            "Fobizer: %.1f%% des flux corrigés, %.1f%% des flux d'importation",
            100.0 * self.report_.share_flows_corrected,
            100.0 * self.report_.share_import_flows_corrected,
        )

        return df_out


# ──────────────────────────────────────────────────────────────────────
# Étape 4 — Évaluation de la qualité de déclaration (ANOVA)
# ──────────────────────────────────────────────────────────────────────

# Diagnostics d'une estimation de qualité de déclaration
@dataclass
class QualityReport:
    """Diagnostics of one reporting-quality estimation.

    Attributes:
        n_countries_scored: Number of countries carrying a ``σ̂`` estimate.
        sigma_floor: Strictly positive floor applied to ``σ̂``, derived from the
            fit (see :class:`ReportingQualityModel`). Countries sitting on it
            are those equation (13) drove below zero; comparing it to
            ``sigma_*_median`` says how much of the ranking it flattens.
        sigma_export_median: Median ``σ̂`` of the exporter dimension.
        sigma_import_median: Median ``σ̂`` of the importer dimension.
        sigma_export_max: Largest ``σ̂`` of the exporter dimension.
        sigma_import_max: Largest ``σ̂`` of the importer dimension.
    """
    n_countries_scored: int = 0
    sigma_floor: float = float("nan")
    sigma_export_median: float = float("nan")
    sigma_import_median: float = float("nan")
    sigma_export_max: float = float("nan")
    sigma_import_max: float = float("nan")


# Résultat d'une estimation de qualité : variances par pays
@dataclass
class QualityResult:
    """Per-country reporting-quality variances.

    Attributes:
        sigma_export: Estimated ``σ̂`` per country acting as exporter (indexed by
            ISO-3 code).
        sigma_import: Estimated ``σ̂`` per country acting as importer.
        report: Diagnostics of the estimation. It travels with the result rather
            than as a fitted attribute because a single model instance serves
            both the value and the quantity targets.
    """
    sigma_export: pd.Series
    sigma_import: pd.Series
    report: QualityReport = field(default_factory=QualityReport)


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

    Args:
        min_sigma_ratio: Floor applied to ``σ̂``, expressed as a fraction of the
            median strictly positive ``σ̂`` of the fit (so it follows the scale
            the data actually produces instead of hard-coding one). Equation (13)
            of the note, ``K_i = min_i LS_RD_i + 2·stderr_i``, drives every
            country within two standard errors of the best declarant below zero;
            flooring them at exactly zero gives them a null error variance, hence
            a weight of exactly ``1`` against any partner with ``σ̂ > 0``. A
            strictly positive floor removes that mass point. It only *softens*
            the asymmetry — the note's optimal weight behaves like
            ``σ_j² / (σ_i² + σ_j²)``, so raising the ratio is what makes the
            reconciliation less winner-takes-all; the value belongs in
            configuration and is worth revisiting once the ``σ̂`` distribution is
            observed on real data (see ``quality/sigma_by_country.csv`` and the
            ``sigma_*_median`` metrics).

    Attributes:
        sigma_floor_: Floor value derived by the last :meth:`fit`, in ``σ̂``
            units.
    """

    # Initialisation
    def __init__(self, *, min_sigma_ratio: float = 0.1) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.min_sigma_ratio = min_sigma_ratio

    # Échantillon d'estimation d'une cible
    @staticmethod
    def _sample(df_mirror: pd.DataFrame, target: str) -> pd.DataFrame:
        """Return the estimation sample of a target, with ``_rd`` and ``_w``.

        Args:
            df_mirror: Mirror-flow table with ``v_x``, ``v_m_fob`` (and tonne
                quantities ``q_x_t``, ``q_m_t``).
            target: ``"value"`` (uses ``v_x`` vs ``v_m_fob``) or ``"quantity"``
                (uses ``q_x_t`` vs ``q_m_t``).

        Returns:
            Complete, strictly positive mirror flows with the reporting distance
            ``_rd = |ln(V_i / V_j)|`` and the weight ``_w = ln(v_x + v_m_fob)``
            (strictly positive weights and finite distances only).

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
        keep = (v_i > 0) & (v_j > 0)
        df_sample = df_mirror.loc[keep, [_EXP, _IMP, _PROD, _YEAR, "v_x", "v_m_fob"]].copy()

        # Distance de déclaration ; pondération par le log de la somme des
        # valeurs déclarées (s = ln(V_i + V_j)), y compris pour la
        # qualité estimée sur les quantités
        df_sample["_rd"] = np.abs(np.log(v_i[keep] / v_j[keep]))
        df_sample["_w"] = np.log(
            df_sample["v_x"].fillna(0.0) + df_sample["v_m_fob"].fillna(0.0)
        )
        return df_sample[(df_sample["_w"] > 0) & np.isfinite(df_sample["_rd"])]

    # Accumulation d'une tranche pour une cible
    def partial_fit(
        self,
        df_mirror: pd.DataFrame,
        *,
        target: str,
        phase: Literal["fit", "cov"] = "fit",
        exporters: Optional[Sequence[str]] = None,
        importers: Optional[Sequence[str]] = None,
        years: Optional[Sequence[int]] = None,
    ) -> "ReportingQualityModel":
        """Fold one chunk of fobized mirror flows into the ANOVA of a target.

        The ANOVA ``RD ~ exporter + importer + year`` (product absorbed) is pooled
        over **every** chunk, in two passes over the same chunks:
        ``phase="fit"`` accumulates the cross-products and the per-product sums
        (:class:`~macroforecast.trade.processing.streaming.AbsorbedWLSAccumulator`),
        :meth:`finalize` solves the coefficients, ``phase="cov"`` accumulates the
        robust meat and the second :meth:`finalize` derives ``σ̂``.

        Args:
            df_mirror: Chunk with ``v_x``, ``v_m_fob``, ``q_x_t``, ``q_m_t``.
            target: ``"value"`` or ``"quantity"`` (independent estimations).
            phase: ``"fit"`` then ``"cov"``.
            exporters: Universe of exporter codes; required on the first ``"fit"``
                call of the target.
            importers: Universe of importer codes; same.
            years: Universe of years; same.

        Returns:
            The model (``self``).

        Raises:
            ValueError: On an unknown target or phase, an out-of-order phase,
                missing universes, or a level outside them.
        """
        states = self.__dict__.setdefault("_states", {})
        state = states.get(target)
        sample = self._sample(df_mirror, target)
        factors = sample[[_EXP, _IMP, _YEAR]].astype(str)
        if phase == "fit":
            if state is None or state["phase"] != "fit":
                if exporters is None or importers is None or years is None:
                    raise ValueError(
                        "exporters, importers and years are required on the first 'fit' call."
                    )
                # Univers des modalités, ordre lexicographique de chaîne (celui de
                # pandas.get_dummies) : la référence en dépend
                universe = {
                    _EXP: sorted({str(code) for code in exporters}),
                    _IMP: sorted({str(code) for code in importers}),
                    _YEAR: sorted({str(int(year)) for year in years}),
                }
                state = {"phase": "fit", "acc": AbsorbedWLSAccumulator(universe)}
                states[target] = state
            state["acc"].partial_fit(factors, sample["_rd"], sample["_w"], sample[_PROD])
        elif phase == "cov":
            if state is None or state["phase"] != "cov":
                raise ValueError("Call finalize(target) on the 'fit' phase before the 'cov' phase.")
            state["acc"].robust_meat(factors, sample["_rd"], sample["_w"], sample[_PROD])
        else:
            raise ValueError(f"phase must be 'fit' or 'cov', got {phase!r}")
        return self

    # Clôture de la phase courante d'une cible
    def finalize(self, target: str) -> "ReportingQualityModel":
        """Close the current phase of a target.

        After ``"fit"``: keeps, per country dimension and for the years, the
        levels present in the sample, drops the first one of each (reference
        level of ``get_dummies(drop_first=True)``) and solves the demeaned
        weighted least squares. After ``"cov"``: derives the robust covariance,
        the sum-to-zero effects and their contrast standard errors, then the
        floored ``σ̂`` (``results_[target]``).

        Args:
            target: ``"value"`` or ``"quantity"``.

        Returns:
            The model (``self``).

        Raises:
            ValueError: If no phase is open for the target.
        """
        state = self.__dict__.get("_states", {}).get(target)
        if state is None:
            raise ValueError(f"No open phase to finalize for target {target!r}.")
        acc: AbsorbedWLSAccumulator = state["acc"]
        if state["phase"] == "fit":
            # Modalités présentes dans l'échantillon de la cible, référence retirée
            counts = acc.level_counts()
            present = {
                name: [level for level, count in series.items() if count > 0]
                for name, series in counts.items()
            }
            selected = [
                f"{name}_{level}"
                for name in (_EXP, _IMP, _YEAR)
                for level in present[name][1:]
            ]
            acc.solve(selected)
            state["levels"] = present
            state["phase"] = "cov"
        elif state["phase"] == "cov":
            effects = _absorbed_anova_effects(
                acc.params_, acc.covariance(), {dim: state["levels"][dim] for dim in (_EXP, _IMP)}
            )
            state["phase"] = "done"
            self.results_ = {**getattr(self, "results_", {}), target: self._result(effects, target)}
        else:
            raise ValueError(f"The estimation of target {target!r} is already complete.")
        return self

    # Construction du résultat d'une cible à partir des effets estimés
    def _result(
        self, effects: Dict[str, Tuple[pd.Series, pd.Series]], target: str
    ) -> QualityResult:
        """Turn the estimated effects into floored ``σ̂`` and their report.

        Args:
            effects: Sum-to-zero effects and standard errors per dimension.
            target: Target name (for the log).

        Returns:
            The :class:`QualityResult`.
        """
        ls_exp, se_exp = effects[_EXP]
        ls_imp, se_imp = effects[_IMP]

        # Calcul des métriques de diagnostic
        sigma_export = _ls_mean_to_sigma(ls_exp, se_exp)
        sigma_import = _ls_mean_to_sigma(ls_imp, se_imp)

        # Plancher strictement positif : l'équation (13) ramène sous zéro tout
        # pays situé à moins de deux écarts-types du meilleur déclarant, et une
        # variance d'erreur exactement nulle lui vaudrait un poids de 1 face à
        # n'importe quel partenaire. Calé sur l'échelle des σ̂ observés.
        self.sigma_floor_ = _sigma_floor(
            sigma_export, sigma_import, ratio=self.min_sigma_ratio
        )
        sigma_export = sigma_export.clip(lower=self.sigma_floor_)
        sigma_import = sigma_import.clip(lower=self.sigma_floor_)

        # Diagnostics : dispersion des σ̂ estimés, déjà portés par le résultat
        report = QualityReport(
            sigma_floor=self.sigma_floor_,
            n_countries_scored=int(
                len(set(sigma_export.index) | set(sigma_import.index))
            ),
            sigma_export_median=float(sigma_export.median()) if len(sigma_export) else float("nan"),
            sigma_import_median=float(sigma_import.median()) if len(sigma_import) else float("nan"),
            sigma_export_max=float(sigma_export.max()) if len(sigma_export) else float("nan"),
            sigma_import_max=float(sigma_import.max()) if len(sigma_import) else float("nan"),
        )

        # Logging
        logger.info(
            "ReportingQualityModel (%s): %d pays notés, sigma médian export "
            "%.3f / import %.3f (plancher %.4g)",
            target,
            report.n_countries_scored,
            report.sigma_export_median,
            report.sigma_import_median,
            report.sigma_floor,
        )

        return QualityResult(
            sigma_export=sigma_export,
            sigma_import=sigma_import,
            report=report,
        )

    # Estimation pour une cible (valeurs ou quantités)
    def fit(self, df_mirror: pd.DataFrame, target: str) -> QualityResult:
        """Estimate per-country variances for a target quantity.

        Single-chunk case of the two phases of :meth:`partial_fit`, the level
        universes being read from ``df_mirror``.

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
        # Réinitialisation de la cible : fit repart toujours d'un état vide
        self.__dict__.setdefault("_states", {}).pop(target, None)
        self.partial_fit(
            df_mirror,
            target=target,
            phase="fit",
            exporters=df_mirror[_EXP].dropna().unique(),
            importers=df_mirror[_IMP].dropna().unique(),
            years=df_mirror[_YEAR].dropna().unique(),
        )
        self.finalize(target)
        self.partial_fit(df_mirror, target=target, phase="cov")
        self.finalize(target)
        return self.results_[target]


# Fonction d'estimation des effets des dimensions pays à partir de l'ANOVA absorbée
def _absorbed_anova_effects(
    params: pd.Series,
    cov: pd.DataFrame,
    levels: Mapping[str, Sequence[str]],
) -> Dict[str, Tuple[pd.Series, pd.Series]]:
    """Re-express the exporter and importer coefficients in sum-to-zero coding.

    The ANOVA ``RD ~ exporter + importer + year`` (weights ``ln(V_i + V_j)``,
    product absorbed) is estimated in reference coding: a country's coefficient
    is that of its dummy, the reference level (the first in string order)
    counting as zero. Following the note's sum-to-zero coding (eq. 11), a
    country's effect is its coefficient minus the mean coefficient of its
    dimension, and its standard error is that of the corresponding contrast
    ``c = e_i − (1/L)·1``, derived from the full (robust) covariance — including
    a proper, non-zero standard error for the reference level.

    Args:
        params: Coefficients in reference coding, indexed ``"<dimension>_<level>"``.
        cov: Their covariance, same index and columns.
        levels: Levels of each dimension present in the sample, sorted as
            strings (``{"exporter": [...], "importer": [...]}``).

    Returns:
        Mapping ``{dimension: (effects, std_errors)}`` for the ``exporter`` and
        ``importer`` dimensions, each pair being pandas Series indexed by
        country code.

    Examples:
        >>> params = pd.Series({"exporter_B": 1.0})
        >>> cov = pd.DataFrame([[0.04]], index=["exporter_B"], columns=["exporter_B"])
        >>> effects, se = _absorbed_anova_effects(params, cov, {"exporter": ["A", "B"]})["exporter"]
        >>> effects.to_dict(), se.round(3).to_dict()
        ({'A': -0.5, 'B': 0.5}, {'A': 0.1, 'B': 0.1})
    """
    # Initialisation du dictionnaire résultat
    out: Dict[str, Tuple[pd.Series, pd.Series]] = {}
    # Parcours des dimensions pays
    for entity_col, entity_levels in levels.items():
        entity_levels = list(entity_levels)
        n_levels = len(entity_levels)
        # Coefficients en codage de référence (modalité de référence : zéro)
        coefs = pd.Series(
            {e: float(params.get(f"{entity_col}_{e}", 0.0)) for e in entity_levels},
            dtype="float64",
        )
        # Recentrage somme-nulle (les écarts entre pays sont préservés)
        effects = coefs - coefs.mean()

        # Écart-type de chaque effet recentré : contraste c = e_i − (1/L)·1 sur
        # les coefficients estimés (la part de la modalité de référence, sans
        # coefficient, est nulle dans le contraste)
        est_cols = [
            c for c in (f"{entity_col}_{e}" for e in entity_levels) if c in params.index
        ]
        col_pos = {c: p for p, c in enumerate(est_cols)}
        v_mat = cov.loc[est_cols, est_cols].to_numpy()
        se: Dict[str, float] = {}
        for e in entity_levels:
            contrast = np.full(len(est_cols), -1.0 / n_levels)
            name = f"{entity_col}_{e}"
            if name in col_pos:
                contrast[col_pos[name]] += 1.0
            se[e] = float(np.sqrt(max(contrast @ v_mat @ contrast, 0.0)))
        out[entity_col] = (effects, pd.Series(se, dtype="float64"))
    return out


# Fonction de conversion des moyennes marginales en écarts-types σ̂
def _ls_mean_to_sigma(ls_mean: pd.Series, std_error: pd.Series) -> pd.Series:
    """Turn least-square means of ``RD`` into per-country ``σ̂`` (eq. 12–13).

    Applies ``K_i = min_i LS_RD + 2·stderr_i`` and
    ``σ̂_i = (π/2)·(LS_RD_i - K_i)``, floored at zero so the best declarant carries
    the smallest variance. The strictly positive floor that keeps a null variance
    from turning into a weight of exactly ``1`` is applied afterwards, by
    :meth:`ReportingQualityModel.fit`, because it needs both dimensions to set
    its scale.

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


# Fonction de calage du plancher des σ̂ sur l'échelle observée
def _sigma_floor(
    sigma_export: pd.Series, sigma_import: pd.Series, *, ratio: float
) -> float:
    """Derive the strictly positive floor applied to ``σ̂``.

    Scales the floor on the data rather than on a hard-coded constant: it is
    ``ratio`` times the median of the strictly positive ``σ̂`` pooled over both
    country dimensions. Falls back to ``0`` when the fit produced no strictly
    positive ``σ̂`` at all — there is then no scale to speak of, and every weight
    degenerates to ``0.5`` anyway.

    Args:
        sigma_export: Raw exporter ``σ̂``, before flooring.
        sigma_import: Raw importer ``σ̂``, before flooring.
        ratio: Fraction of the median strictly positive ``σ̂`` to use as floor.

    Returns:
        The floor, in ``σ̂`` units.

    Examples:
        >>> exp = pd.Series({"AAA": 0.0, "BBB": 0.4})
        >>> imp = pd.Series({"AAA": 0.6, "BBB": 0.0})
        >>> _sigma_floor(exp, imp, ratio=0.1)
        0.05
        >>> _sigma_floor(pd.Series({"AAA": 0.0}), pd.Series({"AAA": 0.0}), ratio=0.1)
        0.0
    """
    # Échelle des σ̂ effectivement estimés (les planchers à zéro sont écartés)
    pooled = pd.concat([sigma_export, sigma_import], ignore_index=True)
    positive = pooled[pooled > 0]
    if positive.empty:
        return 0.0
    return float(ratio) * float(positive.median())


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
            ``reconciled_value``, ``reconciled_quantity`` and the transient
            column :data:`_VALUE_WEIGHT` holding the weight ``w`` borne by the
            exporter declaration in the value reconciliation. That column is
            consumed by :class:`AreaNesReallocator` and dropped by
            :func:`run_baci`; it is not part of the persisted schema.
        """
        # Copie indépendante des données
        df_out = df_mirror.copy()

        # Réconciliation des valeurs (export FAB vs import fobisé) ; le poids
        # accompagne le résultat, l'étape NES en ayant besoin pour ajouter de la
        # valeur du côté exportateur d'un flux déjà réconcilié
        df_out["reconciled_value"], value_weight = _reconcile_pair(
            df_out, "v_x", "v_m_fob", quality_value
        )
        df_out[_VALUE_WEIGHT] = value_weight
        # Réconciliation des quantités (tonnes des deux côtés)
        df_out["reconciled_quantity"], _ = _reconcile_pair(
            df_out, "q_x_t", "q_m_t", quality_qty
        )

        # Restriction aux colonnes d'intérêt
        cols = [
            _EXP, _IMP, _PROD, _YEAR,
            "reconciled_value", "reconciled_quantity", _VALUE_WEIGHT,
        ]
        return df_out[cols]


# Fonction de réconciliation d'un couple de colonnes miroirs
def _reconcile_pair(
    df_mirror: pd.DataFrame, col_i: str, col_j: str, quality: QualityResult
) -> Tuple[pd.Series, pd.Series]:
    """Reconcile one mirror pair (value or quantity) into a single series.

    Args:
        df_mirror: Mirror-flow table.
        col_i: Exporter-side column (``V_i``).
        col_j: Importer-side column (``V_j``, already FOB-comparable).
        quality: Per-country variances for the reconciled quantity.

    Returns:
        A tuple ``(reconciled, weight)``: the reconciled series (both-flow convex
        combination, single-flow passthrough, ``NaN`` when neither side exists)
        and the weight ``w`` actually borne by the exporter declaration — the
        optimal weight when both flows exist, ``1`` when only the exporter
        declared, ``0`` when only the importer did, ``NaN`` when neither did.
        The weight is returned rather than kept private because a later step may
        have to add value *to the exporter declaration* of an already reconciled
        flow (see :class:`AreaNesReallocator`), which is only correct on the
        ``w`` scale.
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

    # Poids effectivement porté par la déclaration de l'exportateur : le poids
    # optimal sur les miroirs complets, 1 ou 0 sur les flux à déclaration unique
    weight = pd.Series(np.nan, index=df_mirror.index, dtype="float64")
    weight[both] = w[both]
    weight[only_i] = 1.0
    weight[only_j] = 0.0
    return reconciled, weight


# ──────────────────────────────────────────────────────────────────────
# Étape 6 — Traitement des zones non spécifiées (Areas NES)
# ──────────────────────────────────────────────────────────────────────


# Diagnostics de la réallocation des zones non spécifiées
@dataclass
class NesReport:
    """Diagnostics of the "Areas NES" reallocation step.

    Attributes:
        share_final_flows_affected: Share of the final reconciled flows that
            received reallocated value.
        n_flows_enriched: Number of flows that received reallocated value.
        value_reallocated: NES value distributed across identified partners,
            i.e. the amount added to the *exporter declarations* and removed
            from the NES pool before the residual step.
        value_added_to_flows: Value actually added to ``reconciled_value``,
            i.e. ``Σ w·add`` — smaller than ``value_reallocated``, the
            reconciled value retaining only the fraction ``w`` of the exporter
            declaration.
        value_absorbed: NES value considered already counted in the imports
            declared without an export mirror.
        value_discarded: NES value left without any identifiable partner.
        share_nes_value_reallocated: Share of the total NES value that was
            reallocated.
    """
    share_final_flows_affected: float = float("nan")
    n_flows_enriched: int = 0
    value_added_to_flows: float = float("nan")
    value_reallocated: float = float("nan")
    value_absorbed: float = float("nan")
    value_discarded: float = float("nan")
    share_nes_value_reallocated: float = float("nan")


# Compteurs additifs de la réallocation NES
@dataclass
class _NesTally:
    """Additive totals behind :class:`NesReport`.

    The reallocation works per ``(exporter, product, year)`` group, so it is exact
    on any chunk holding whole groups; only its report needs these totals to be
    summed, the report's branches (nothing to reallocate, nothing reallocated,
    reallocation) being decided on the whole data by the two flags.

    Attributes:
        any_export_nes: Whether some "Areas NES" export value was found.
        any_reallocation: Whether some value was actually reallocated.
        n_flows: Reconciled flows handed to the step.
        n_enriched: Flows that received reallocated value.
        value_reallocated: Value added to the exporter declarations.
        value_added: Value actually added to the reconciled values.
        value_absorbed: NES value deemed counted in the imports without mirror.
        value_discarded: NES value without identifiable partner.
        total_nes: Total "Areas NES" export value.
    """
    any_export_nes: bool = False
    any_reallocation: bool = False
    n_flows: int = 0
    n_enriched: int = 0
    value_reallocated: float = 0.0
    value_added: float = 0.0
    value_absorbed: float = 0.0
    value_discarded: float = 0.0
    total_nes: float = 0.0

    # Somme de deux tallies
    def __add__(self, other: "_NesTally") -> "_NesTally":
        return _add_tallies(self, other)

    # Rapport correspondant
    def report(self) -> NesReport:
        """Return the :class:`NesReport` of the counted totals.

        Returns:
            The report, following the branch the whole data falls into.
        """
        # Aucune valeur NES exportée : rapport neutre
        if not self.any_export_nes:
            return NesReport(
                share_final_flows_affected=0.0,
                value_reallocated=0.0,
                value_added_to_flows=0.0,
                value_absorbed=0.0,
                value_discarded=0.0,
                share_nes_value_reallocated=0.0,
            )
        # Aucune réallocation : seul le résidu est renseigné
        if not self.any_reallocation:
            return NesReport(
                share_final_flows_affected=0.0,
                n_flows_enriched=0,
                value_reallocated=0.0,
                value_added_to_flows=0.0,
                value_absorbed=self.value_absorbed,
                value_discarded=self.value_discarded,
                share_nes_value_reallocated=0.0 if self.total_nes else float("nan"),
            )
        return NesReport(
            share_final_flows_affected=(
                float(self.n_enriched) / float(self.n_flows) if self.n_flows else float("nan")
            ),
            n_flows_enriched=self.n_enriched,
            value_reallocated=self.value_reallocated,
            value_added_to_flows=self.value_added,
            value_absorbed=self.value_absorbed,
            value_discarded=self.value_discarded,
            share_nes_value_reallocated=(
                self.value_reallocated / self.total_nes if self.total_nes else float("nan")
            ),
        )


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

    The note also asks for "Commodities NES" to be left aside. That exclusion is
    **not** enforced here: it happens further upstream, at extraction time, via
    the ``products.exclude`` list of the ``comtrade`` parameters (HS code
    ``999999``, alongside the ``00``/``0090``/``009000`` aggregates). Should that
    extraction filter change, the exclusion has to be reinstated — either there or
    in this step.

    The complete list of Areas not elsewhere specified is available at https://uncomtrade.org/docs/areas-not-elsewhere-specified/

    Args:
        flow_col: Column holding the trade-flow code.
        export_code: Flow code identifying export declarations (FOB).
        reporter_iso_col: Column with the reporter ISO-3 code.
        product_col: Column with the product (HS6) code.
        period_col: Column with the period (year as text).
        value_col: Column with the primary trade value.

    Attributes:
        report_: :class:`NesReport` of the step, populated by :meth:`transform`
            whichever branch is taken.
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

        Steps 1 and 2 both reason on the **exporter declaration**: the value is
        allocated as ``V^x' = V^x + add`` and the safeguard is evaluated on that
        scale. Since this step runs *after* the reconciliation, only the fraction
        ``w`` of ``add`` borne by the exporter declaration reaches
        ``reconciled_value`` — hence the :data:`_VALUE_WEIGHT` column carried by
        ``df_reconciled``. Adding ``add`` unweighted would over-allocate by
        ``(1 - w)·add``.

        Args:
            df_reconciled: Reconciled flows from :meth:`MirrorReconciler.transform`,
                carrying the :data:`_VALUE_WEIGHT` column. Absent that column
                every weight defaults to ``1``, which reproduces the unweighted
                (over-allocating) behaviour.
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
        # Diagnostics : compteurs neutres, renseignés dans chacune des branches
        self.tally_ = _NesTally(n_flows=int(len(df_reconciled)))
        self.report_ = self.tally_.report()
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

        # Valeur NES totale, dénominateur des parts du rapport
        total_nes = float(df_residual["v_nes"].sum())

        if df_add is None:
            # Diagnostics : aucune réallocation, seul le résidu est renseigné
            self.tally_ = _NesTally(
                any_export_nes=True,
                n_flows=int(len(df_reconciled)),
                value_absorbed=float(
                    (df_residual["v_nes_prime"] - df_residual["unallocated"]).sum()
                ),
                value_discarded=float(df_residual["unallocated"].sum()),
                total_nes=total_nes,
            )
            self.report_ = self.tally_.report()
            # Logging
            logger.info(
                "AreaNesReallocator: aucune réallocation (%.1f de valeur NES, "
                "%.1f écartés faute de partenaire identifiable)",
                total_nes, self.report_.value_discarded,
            )
            return df_reconciled

        # Ajout de la valeur réallouée aux flux réconciliés ; les flux sans
        # réallocation (dont les valeurs réconciliées manquantes) restent intacts
        df_out = df_reconciled.merge(df_add, on=keys4, how="left")
        add_value = df_out.pop("add_value").fillna(0.0)
        # Pondération par w : la réallocation abonde la déclaration de
        # l'exportateur (V^x' = V^x + add, échelle sur laquelle le garde-fou
        # ci-dessus a été évalué), et la valeur réconciliée n'en retient que la
        # fraction w. Ajouter add tel quel sur-allouerait de (1 − w)·add, soit
        # près du double de la valeur voulue lorsque w ≈ 0,5.
        weight = df_out[_VALUE_WEIGHT].fillna(1.0) if _VALUE_WEIGHT in df_out else 1.0
        add_value = add_value * weight
        df_out["reconciled_value"] = df_out["reconciled_value"] + add_value

        # Diagnostics : ampleur du traitement sur les flux finaux
        n_enriched = int((add_value > 0).sum())
        n_flows = len(df_out)
        self.tally_ = _NesTally(
            any_export_nes=True,
            any_reallocation=True,
            n_flows=int(n_flows),
            n_enriched=n_enriched,
            value_reallocated=float(df_residual["allocated"].sum()),
            value_added=float(add_value.sum()),
            value_absorbed=float(
                (df_residual["v_nes_prime"] - df_residual["unallocated"]).sum()
            ),
            value_discarded=float(df_residual["unallocated"].sum()),
            total_nes=total_nes,
        )
        self.report_ = self.tally_.report()

        # Logging
        logger.info(
            "AreaNesReallocator: %d flux enrichis, soit %.1f%% des flux finaux "
            "(%.1f réalloués côté exportateur, dont %.1f effectivement ajoutés "
            "aux valeurs réconciliées ; %.1f absorbés par les imports sans "
            "miroir, %.1f écartés faute de partenaire)",
            self.report_.n_flows_enriched,
            100.0 * self.report_.share_final_flows_affected,
            self.report_.value_reallocated,
            self.report_.value_added_to_flows,
            self.report_.value_absorbed,
            self.report_.value_discarded,
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
        n_input_declarations: Number of COMTRADE rows handed to the run.
        total_reconciled_value: Total reconciled value produced.
        tonnage: Diagnostics of the tonne conversion step.
        gravity: Diagnostics of the gravity estimation (carries
            ``mean_freight_rate``, the only place it is reported).
        fobisation: Diagnostics of the fobisation step.
        mirror: Diagnostics of the mirror-flow construction.
        quality_value: Diagnostics of the reporting quality estimated on values.
        quality_quantity: Diagnostics of the reporting quality estimated on
            quantities.
        nes: Diagnostics of the "Areas NES" reallocation.
    """
    flows: int = 0
    regime_country_years: int = 0
    regime_fob_country_years: int = 0
    regime_no_information: int = 0
    classification_code: Optional[str] = None
    period_start: Optional[int] = None
    period_end: Optional[int] = None
    created: bool = False
    # Contexte de l'exécution
    n_input_declarations: int = 0
    total_reconciled_value: float = float("nan")
    # Rapports d'étape (principe P3 : les diagnostics sont des données)
    tonnage: TonnageReport = field(default_factory=TonnageReport)
    gravity: GravityReport = field(default_factory=GravityReport)
    fobisation: FobisationReport = field(default_factory=FobisationReport)
    mirror: MirrorReport = field(default_factory=MirrorReport)
    quality_value: QualityReport = field(default_factory=QualityReport)
    quality_quantity: QualityReport = field(default_factory=QualityReport)
    nes: NesReport = field(default_factory=NesReport)

    # Mise en forme des métriques (la seule à connaître les contraintes MLflow)
    def to_metrics(self, prefix: str = "baci") -> Dict[str, float]:
        """Flatten every numeric field into a dotted metric mapping.

        Walks the step reports recursively, producing keys such as
        ``baci.tonnage.share_converted_from_other_units``. ``NaN`` and infinite
        values are dropped, MLflow rejecting them. Keeping this formatting here
        rather than in :func:`run_baci` leaves the report usable on its own.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> report = BaciReport(flows=12)
            >>> report.to_metrics()["baci.flows"]
            12.0
            >>> "baci.gravity.mean_freight_rate" in report.to_metrics()
            False
        """
        return flatten_metrics(self, prefix=prefix)


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
        >>> required_columns()[-1]
        'classificationCode'
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
                # Sans cette colonne, _assert_homogeneous_classification se
                # contente d'un avertissement : le garde-fou d'homogénéité HS
                # serait un no-op chez tout appelant projetant exactement cette
                # liste (cas de scripts/process_baci.py)
                config.schema.classification_col,
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
    apply_nes: Optional[bool] = None,
    period_start: Optional[int] = None,
    period_end: Optional[int] = None,
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
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
            ``period_start``/``period_end``/``apply_nes`` are used whenever the
            same-named argument below is left ``None``.
        apply_nes: Whether to apply the "Areas NES" reallocation step; falls
            back to ``config.apply_nes`` when ``None``.
        period_start: First year (included) kept from ``df_comtrade``; falls
            back to ``config.period_start`` when ``None``.
        period_end: Last year (included) kept from ``df_comtrade``; falls back
            to ``config.period_end`` when ``None``.
        tracker: Experiment tracker receiving the run parameters and artifacts.
            Defaults to the null tracker, so an unconfigured run behaves exactly
            as before. The *metrics* are left to the caller, which sends
            ``report.to_metrics()`` once the report is complete.
        log_artifacts: Whether to build and send the step artifacts (conversion
            rates, gravity coefficients, inferred regimes, per-country ``σ̂``).

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
    # Idem pour l'activation de la réallocation "Areas NES"
    effective_apply_nes = apply_nes if apply_nes is not None else config.apply_nes

    # Vérification de l'homogénéité de la nomenclature HS
    classification_code = _assert_homogeneous_classification(
        df_comtrade, config.schema.classification_col
    )

    # Paramètres de l'exécution : configuration aplatie et contexte
    tracker.log_params(
        run_params(
            config,
            {
                "apply_nes": effective_apply_nes,
                "period_start": effective_period_start,
                "period_end": effective_period_end,
                "classification_code": classification_code,
                "n_input_declarations": len(df_comtrade),
            },
        )
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
    df_mirror, df_nes, mirror_report = build_mirror_flows(
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

    # Étape 2 — estimation des taux CAF par équation de gravité. Les taux
    # prédits sont ajustés par le modèle : le recalcul par un second appel à
    # predict serait redondant.
    gravity_model = CifGravityModel(cook_factor=config.cook_factor).fit(
        df_mirror, df_gravity
    )
    cif_rate = gravity_model.cif_rate_

    # Étape 3 — fobisation des importations
    fobizer = Fobizer(fas_countries=config.fas_countries)
    df_mirror = fobizer.transform(df_mirror, cif_rate, df_regime)

    # Étape 4 — qualité de déclaration (valeurs puis quantités)
    quality_model = ReportingQualityModel(min_sigma_ratio=config.min_sigma_ratio)
    quality_value = quality_model.fit(df_mirror, target="value")
    quality_qty = quality_model.fit(df_mirror, target="quantity")

    # Étape 5 — réconciliation des flux miroirs
    df_reconciled = MirrorReconciler().transform(df_mirror, quality_value, quality_qty)

    # Étape 6 — réallocation des zones non spécifiées (optionnelle)
    nes_report = NesReport()
    if effective_apply_nes:
        reallocator = AreaNesReallocator(
            flow_col=config.schema.flow_col,
            export_code=config.schema.export_code,
            reporter_iso_col=config.schema.reporter_iso_col,
            product_col=config.schema.product_col,
            period_col=config.schema.period_col,
            value_col=config.schema.value_col,
        )
        df_reconciled = reallocator.transform(df_reconciled, df_mirror, df_nes)
        nes_report = reallocator.report_

    # Nettoyage : flux réconciliés exploitables (valeur non manquante) et
    # retrait du poids de réconciliation, transitoire (il n'a servi qu'à mettre
    # la réallocation NES à l'échelle de la valeur réconciliée)
    df_reconciled = df_reconciled[
        df_reconciled["reconciled_value"].notna()
    ].reset_index(drop=True)
    df_reconciled = df_reconciled.drop(columns=[_VALUE_WEIGHT], errors="ignore")
    if df_reconciled.empty:
        raise ValueError("No reconciled flow produced")

    # Rapport composite : contexte de l'exécution et rapports d'étape. Le taux
    # de fret moyen n'est porté que par GravityReport (report.gravity), qui le
    # calcule ; BaciReport ne le duplique pas.
    report = BaciReport(
        flows=len(df_reconciled),
        regime_country_years=len(df_regime),
        regime_fob_country_years=int((df_regime["regime"] == _FOB_REGIME).sum()),
        regime_no_information=int(df_regime["cif_share"].isna().sum()),
        classification_code=classification_code,
        period_start=effective_period_start,
        period_end=effective_period_end,
        n_input_declarations=len(df_comtrade),
        total_reconciled_value=float(df_reconciled["reconciled_value"].sum()),
        tonnage=converter.report_,
        gravity=gravity_model.report_,
        fobisation=fobizer.report_,
        mirror=mirror_report,
        quality_value=quality_value.report,
        quality_quantity=quality_qty.report,
        nes=nes_report,
    )

    # Artefacts d'étape : tables et coefficients réutilisables, auditables
    if log_artifacts:
        _log_artifacts(
            tracker,
            converter=converter,
            gravity_model=gravity_model,
            df_regime=df_regime,
            quality_value=quality_value,
            quality_qty=quality_qty,
        )

    return df_reconciled, report


# ──────────────────────────────────────────────────────────────────────
# Orchestration par passes sur des tranches (mémoire bornée à une tranche)
# ──────────────────────────────────────────────────────────────────────

# Clé d'une tranche de données
@dataclass(frozen=True, order=True)
class ChunkKey:
    """Identity of one chunk of a vintage: a year, possibly split into blocks.

    Every chunk holds the declarations of **one** year; a year too large for
    memory is split into blocks of whole products (no target product, hence no
    ``(exporter, product, year)`` group of the NES step, straddles two blocks).

    Attributes:
        year: Year of the chunk.
        block: Block number within the year (``0`` when the year is whole).

    Examples:
        >>> sorted([ChunkKey(2021, 1), ChunkKey(2020), ChunkKey(2021, 0)])
        [ChunkKey(year=2020, block=0), ChunkKey(year=2021, block=0), ChunkKey(year=2021, block=1)]
        >>> ChunkKey(2021, 1).path
        'year=2021/block=1'
    """
    year: int
    block: int = 0

    # Chemin relatif d'une tranche dans un espace de travail partitionné
    @property
    def path(self) -> str:
        """Hive-style relative path of the chunk (``year=<y>/block=<b>``)."""
        return f"year={int(self.year)}/block={int(self.block)}"


# Protocole des entrées / sorties du traitement par passes
class BaciPassIO(Protocol):
    """Input/output callbacks of :func:`run_baci_passes`, supplied by the caller.

    The methodology never touches a file: reading the source declarations,
    storing and re-reading the intermediate mirror flows, the two statistics
    computed out of core (median unit values and freight-rate quantiles) and the
    year-by-year persistence of the result all go through this object. A DuckDB
    implementation lives in ``kedro_pipeline.steps.baci``; :class:`InMemoryPassIO`
    keeps everything in memory (tests, small data).

    Kinds of intermediate data (``kind``): ``"mirror"`` (mirror flows of a
    chunk), ``"nes"`` (its "Areas NES" declarations) and ``"freight"`` (its
    predicted freight rates, column ``cif_rate``).
    """

    def comtrade_chunks(self, columns: Sequence[str]) -> Iterator[Tuple[ChunkKey, pd.DataFrame]]:
        """Yield the source declarations (already harmonised to the vintage), chunk by chunk."""
        ...

    def spill(self, kind: str, key: ChunkKey, df: pd.DataFrame) -> None:
        """Store the intermediate data of a chunk."""
        ...

    def read(self, kind: str, key: ChunkKey, columns: Optional[Sequence[str]] = None) -> pd.DataFrame:
        """Read back the intermediate data of a chunk (optionally projected)."""
        ...

    def median_uv(
        self,
        df_rates: pd.DataFrame,
        *,
        tonne_conversion_factors: Mapping[int, float],
        prefer_netwgt: bool,
    ) -> pd.Series:
        """Return ``UV^k`` per product over every stored mirror chunk (see
        :func:`world_median_unit_values_sql`)."""
        ...

    def quantiles(self, kind: str, column: str, probs: Sequence[float]) -> List[float]:
        """Return linear-interpolation quantiles of a column over every chunk (missing skipped)."""
        ...

    def write_year(self, year: int, blocks: Iterator[pd.DataFrame]) -> None:
        """Persist the reconciled flows of one year, consuming every block in one transaction."""
        ...

    def load_p0_state(self) -> Optional[Mapping[str, pd.DataFrame]]:
        """Return the saved state of the preparation pass, if a resumable one exists."""
        ...

    def save_p0_state(self, frames: Mapping[str, pd.DataFrame]) -> None:
        """Save the state of the preparation pass (resumption of an interrupted run)."""
        ...


# État de la passe de préparation (P0)
@dataclass
class BaciPassState:
    """Statistics gathered by the preparation pass, enough to resume without it.

    Attributes:
        regime_sums: Valuation sums per ``(importer, year)`` (:func:`_regime_sums`).
        tonnage_stats: Moments of the conversion ratios per ``(product, unit)``
            (:meth:`WelfordGroupStats.to_frame`).
        mirror: Coverage counts of the mirror flows.
        n_input_declarations: Source declarations read.
        classification_code: HS classification shared by every chunk.
        chunk_keys: Chunks whose mirror flows were stored.
    """
    regime_sums: pd.DataFrame
    tonnage_stats: pd.DataFrame
    mirror: _MirrorTally
    n_input_declarations: int
    classification_code: Optional[str]
    chunk_keys: List[ChunkKey]

    # Sérialisation en tables (persistance par l'appelant)
    def to_frames(self) -> Dict[str, pd.DataFrame]:
        """Serialise the state into tables.

        Returns:
            Mapping ``name -> frame`` (``regime_sums``, ``tonnage_stats``,
            ``summary``).

        Examples:
            >>> state = BaciPassState(_empty_regime_sums(), pd.DataFrame(), _MirrorTally(3, 1, 1, 1),
            ...                       10, "H5", [ChunkKey(2020)])
            >>> BaciPassState.from_frames(state.to_frames()).chunk_keys
            [ChunkKey(year=2020, block=0)]
        """
        summary = pd.DataFrame(
            [
                {
                    "n_input_declarations": int(self.n_input_declarations),
                    "classification_code": self.classification_code,
                    "mirror": json.dumps(
                        {item.name: getattr(self.mirror, item.name) for item in fields(self.mirror)}
                    ),
                    "chunk_keys": json.dumps([[int(k.year), int(k.block)] for k in self.chunk_keys]),
                }
            ]
        )
        return {
            "regime_sums": self.regime_sums.reset_index(drop=True),
            "tonnage_stats": self.tonnage_stats.reset_index(drop=True),
            "summary": summary,
        }

    # Restauration depuis les tables sérialisées
    @classmethod
    def from_frames(cls, frames: Mapping[str, pd.DataFrame]) -> "BaciPassState":
        """Rebuild the state from :meth:`to_frames`' output.

        Args:
            frames: Mapping ``name -> frame``.

        Returns:
            The state.
        """
        summary = frames["summary"].iloc[0]
        code = summary["classification_code"]
        return cls(
            regime_sums=frames["regime_sums"],
            tonnage_stats=frames["tonnage_stats"],
            mirror=_MirrorTally(**json.loads(summary["mirror"])),
            n_input_declarations=int(summary["n_input_declarations"]),
            classification_code=None if code is None or pd.isna(code) else str(code),
            chunk_keys=[ChunkKey(int(y), int(b)) for y, b in json.loads(summary["chunk_keys"])],
        )


# Cibles de la qualité de déclaration
_QUALITY_TARGETS: Tuple[str, str] = ("value", "quantity")


# Passe de préparation : déclarations sources → flux miroirs stockés
def _preparation_pass(
    io: BaciPassIO,
    *,
    config: BaciConfig,
    valid_iso: Sequence[str],
    period_start: Optional[int],
    period_end: Optional[int],
) -> BaciPassState:
    """Run the preparation pass (P0) over the source chunks.

    Per chunk: homogeneity of the classification, valuation sums (on every
    declaration, as :func:`run_baci` infers the regime before any temporal
    filter), mirror flows, conversion-ratio moments, storage of the mirror and
    NES flows.

    Args:
        io: Input/output callbacks.
        config: Methodological configuration.
        valid_iso: ISO-3 codes of the individual countries.
        period_start: First year kept (``None``: no bound).
        period_end: Last year kept (``None``: no bound).

    Returns:
        The state of the pass.

    Raises:
        ValueError: On heterogeneous classifications across chunks, or when the
            temporal filter leaves no declaration at all.
    """
    schema = config.schema
    regime_sums = _empty_regime_sums()
    ratio_stats = WelfordGroupStats(ddof=1)
    converter = TonnageConverter(
        tonne_conversion_factors=config.tonne_conversion_factors,
        prefer_netwgt=config.prefer_netwgt,
    )
    converter._ratio_stats = ratio_stats
    mirror_tally = _MirrorTally()
    n_input = 0
    classification_code: Optional[str] = None
    keys: List[ChunkKey] = []

    for key, df_chunk in io.comtrade_chunks(required_columns(config)):
        n_input += int(len(df_chunk))
        # Une seule nomenclature pour toutes les tranches du millésime
        chunk_code = _assert_homogeneous_classification(df_chunk, schema.classification_col)
        if chunk_code is not None:
            if classification_code is not None and chunk_code != classification_code:
                raise ValueError(
                    "Heterogeneous HS classifications found across chunks: "
                    f"{sorted([classification_code, chunk_code])}. Harmonise every chunk "
                    "to the same vintage first."
                )
            classification_code = chunk_code
        # Sommes de valorisation : avant tout filtre, comme run_baci
        regime_sums = _merge_regime_sums(
            regime_sums,
            _regime_sums(
                df_chunk,
                flow_col=schema.flow_col,
                import_code=schema.import_code,
                reporter_iso_col=schema.reporter_iso_col,
                period_col=schema.period_col,
                cif_value_col=schema.cif_value_col,
                fob_value_col=schema.fob_value_col,
            ),
        )
        # Tranche hors du périmètre temporel : rien à redresser
        years = df_chunk[schema.period_col].astype(str).str[:4].astype(int)
        in_scope = pd.Series(True, index=df_chunk.index)
        if period_start is not None:
            in_scope &= years >= period_start
        if period_end is not None:
            in_scope &= years <= period_end
        if not in_scope.any():
            continue
        df_mirror, df_nes, _ = build_mirror_flows(
            df_chunk,
            valid_iso,
            flow_col=schema.flow_col,
            import_code=schema.import_code,
            export_code=schema.export_code,
            reporter_iso_col=schema.reporter_iso_col,
            partner_iso_col=schema.partner_iso_col,
            partner_code_col=schema.partner_code_col,
            product_col=schema.product_col,
            period_col=schema.period_col,
            value_col=schema.value_col,
            qty_col=schema.qty_col,
            qty_unit_col=schema.qty_unit_col,
            netwgt_col=schema.netwgt_col,
            world_partner_code=config.world_partner_code,
            nes_partner_codes=config.nes_partner_codes,
            nes_skip_codes=config.nes_skip_codes,
            excluded_pairs=config.excluded_pairs,
            period_start=period_start,
            period_end=period_end,
        )
        mirror_tally += _MirrorTally.from_mirror(df_mirror)
        converter.partial_fit(df_mirror)
        io.spill("mirror", key, df_mirror)
        io.spill("nes", key, df_nes)
        keys.append(key)

    if not keys and (period_start is not None or period_end is not None):
        raise ValueError(
            f"Temporal filter period_start={period_start!r}, "
            f"period_end={period_end!r} emptied df_comtrade before any BACI "
            "estimation could run."
        )
    return BaciPassState(
        regime_sums=regime_sums,
        tonnage_stats=ratio_stats.to_frame(),
        mirror=mirror_tally,
        n_input_declarations=n_input,
        classification_code=classification_code,
        chunk_keys=sorted(keys),
    )


# Chronométrage d'une passe
class _PassClock:
    """Accumulate the wall-clock duration of each pass (``passes/<name>/seconds``)."""

    def __init__(self) -> None:
        self.seconds: Dict[str, float] = {}
        self._name: Optional[str] = None
        self._start = 0.0

    def start(self, name: str) -> None:
        """Start timing pass ``name``."""
        self._name, self._start = name, time.perf_counter()

    def stop(self) -> None:
        """Stop the current pass and record its duration."""
        if self._name is not None:
            self.seconds[self._name] = time.perf_counter() - self._start
            # Logging
            logger.info("Passe BACI %s : %.1f s", self._name, self.seconds[self._name])
        self._name = None


# Fonction d'orchestration : redressement BACI par passes sur des tranches
def run_baci_passes(
    io: BaciPassIO,
    df_dist: pd.DataFrame,
    df_geo: pd.DataFrame,
    *,
    config: BaciConfig = DEFAULT_CONFIG,
    apply_nes: Optional[bool] = None,
    period_start: Optional[int] = None,
    period_end: Optional[int] = None,
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
    memory_probe: Optional[Callable[[], float]] = None,
) -> Tuple[BaciReport, pd.DataFrame]:
    """Run the BACI reconstruction pass by pass, memory bounded by one chunk.

    Gives the same result as :func:`run_baci` on the concatenation of the
    chunks — no approximation: the four estimations pooled over every year of a
    vintage are expressed by sufficient statistics accumulated chunk by chunk,
    and those that need the whole sample (Cook's distance, robust covariance)
    read the chunks a second time. Passes:

    * **P0 preparation** — source chunks → valuation sums, conversion-ratio
      moments, coverage counts; mirror and NES flows stored (``io.spill``);
      skipped when ``io.load_p0_state()`` returns a resumable state;
    * **S1** — ``UV^k`` per product over every stored chunk (``io.median_uv``);
    * **P1 / P2** — gravity: first fit, then Cook filter and final fit;
    * **P3** — freight rates (stored), fobisation, first pass of the reporting
      quality ANOVA (both targets);
    * **S2** — freight-rate quantiles of the report (``io.quantiles``);
    * **P4** — robust covariance of the ANOVA, ``σ̂`` per country;
    * **P5** — reconciliation and NES reallocation, written year by year
      (``io.write_year``, one call per year with every block of the year).

    Args:
        io: Input/output callbacks (:class:`BaciPassIO`).
        df_dist: Raw ``dist_cepii`` table, already loaded.
        df_geo: Raw ``geo_cepii`` table, already loaded.
        config: Column and methodological conventions.
        apply_nes: Whether to apply the "Areas NES" reallocation; falls back to
            ``config.apply_nes``.
        period_start: First year kept; falls back to ``config.period_start``.
        period_end: Last year kept; falls back to ``config.period_end``.
        tracker: Experiment tracker (parameters, artifacts and the per-pass
            and per-year metrics ``passes/<name>/seconds``, ``timing/seconds``,
            ``output/rows``, ``memory/peak_mb``).
        log_artifacts: Whether to send the step artifacts.
        memory_probe: Optional callable returning the peak memory of the
            process in megabytes (logged as ``memory/peak_mb``).

    Returns:
        A tuple ``(report, rows_by_year)``: the :class:`BaciReport` of the run
        (identical to :func:`run_baci`'s) and the rows written per year
        (columns ``year``, ``rows``).

    Raises:
        ValueError: On heterogeneous classifications, an empty temporal scope,
            no usable gravity observation, or no reconciled flow (raised before
            any year is written).
        RuntimeError: If ``io.write_year`` does not consume every block of a year.

    Examples:
        >>> chunks = [(ChunkKey(year), df_comtrade[df_comtrade["period"] == str(year)])
        ...           for year in (2019, 2020, 2021)]  # doctest: +SKIP
        >>> io = InMemoryPassIO(chunks)  # doctest: +SKIP
        >>> report, rows_by_year = run_baci_passes(io, df_dist, df_geo)  # doctest: +SKIP
        >>> io.result().equals(run_baci(df_comtrade, df_dist, df_geo)[0])  # à l'arrondi près  # doctest: +SKIP
    """
    # Paramètres effectifs : l'argument explicite prime sur la config
    effective_period_start = period_start if period_start is not None else config.period_start
    effective_period_end = period_end if period_end is not None else config.period_end
    effective_apply_nes = apply_nes if apply_nes is not None else config.apply_nes
    schema = config.schema
    clock = _PassClock()

    def probe() -> Dict[str, float]:
        if memory_probe is None:
            return {}
        value = memory_probe()
        return {"memory/peak_mb": float(value)} if value is not None and np.isfinite(value) else {}

    # Variables de gravité du CEPII et pays individuels
    df_gravity = build_gravity_data(
        df_dist,
        df_geo,
        dist_iso_o_col=schema.dist_iso_o_col,
        dist_iso_d_col=schema.dist_iso_d_col,
        distance_column=schema.distance_column,
        contig_col=schema.contig_col,
        geo_iso_col=schema.geo_iso_col,
        landlocked_col=schema.landlocked_col,
    )
    valid_iso = sorted(set(df_gravity["iso_o"]) | set(df_gravity["iso_d"]))

    # P0 — préparation, sautée si un état réutilisable existe (reprise)
    frames = io.load_p0_state()
    if frames is None:
        clock.start("P0")
        state = _preparation_pass(
            io,
            config=config,
            valid_iso=valid_iso,
            period_start=effective_period_start,
            period_end=effective_period_end,
        )
        io.save_p0_state(state.to_frames())
        clock.stop()
    else:
        state = BaciPassState.from_frames(frames)
        # Logging
        logger.info("Passe P0 reprise depuis l'état sauvegardé (%d tranches)", len(state.chunk_keys))
    keys = sorted(state.chunk_keys)
    years = sorted({key.year for key in keys})

    # Paramètres de l'exécution : configuration aplatie et contexte
    tracker.log_params(
        run_params(
            config,
            {
                "apply_nes": effective_apply_nes,
                "period_start": effective_period_start,
                "period_end": effective_period_end,
                "classification_code": state.classification_code,
                "n_input_declarations": state.n_input_declarations,
            },
        )
    )

    # Taux de conversion validés et régimes de valorisation (statistiques de P0)
    converter = TonnageConverter(
        tonne_conversion_factors=config.tonne_conversion_factors,
        min_mirror_flows=config.min_mirror_flows,
        max_conversion_std=config.max_conversion_std,
        prefer_netwgt=config.prefer_netwgt,
    )
    converter._ratio_stats = WelfordGroupStats.from_frame(state.tonnage_stats, by=[_PROD, "unit"])
    converter.finalize()
    df_regime = _regime_from_sums(
        state.regime_sums,
        cif_share_threshold=config.cif_share_threshold,
        regime_granularity=config.regime_granularity,
    )

    # Lecture d'une tranche miroir convertie en tonnes
    def converted(key: ChunkKey) -> pd.DataFrame:
        return converter.transform(io.read("mirror", key))

    # S1 — médianes mondiales des valeurs unitaires (hors mémoire, par l'appelant)
    clock.start("S1")
    df_rates = pd.DataFrame(
        [(product, int(unit), float(rate)) for (product, unit), rate in converter.conversion_rates_.items()],
        columns=[_PROD, "unit", "rate"],
    ).astype({"unit": "int64", "rate": "float64"})
    uv_world = io.median_uv(
        df_rates,
        tonne_conversion_factors=config.tonne_conversion_factors,
        prefer_netwgt=config.prefer_netwgt,
    )
    clock.stop()

    # P1 / P2 — gravité : ajustement initial puis filtre de Cook et ajustement final
    gravity_model = CifGravityModel(cook_factor=config.cook_factor)
    for name, phase in (("P1", "fit"), ("P2", "cook")):
        clock.start(name)
        for key in keys:
            gravity_model.partial_fit(
                converted(key), df_gravity, uv_world=uv_world, years=years, phase=phase
            )
        gravity_model.finalize()
        clock.stop()

    # Fobisation d'une tranche (taux de fret prédits par le modèle ajusté)
    fobizer = Fobizer(fas_countries=config.fas_countries)

    def fobized(key: ChunkKey) -> Tuple[pd.DataFrame, pd.Series]:
        df_chunk = converted(key)
        rate = gravity_model.predict(df_chunk, df_gravity)
        return fobizer.transform(df_chunk, rate, df_regime), rate

    # P3 — taux de fret, fobisation, première passe de l'ANOVA de qualité
    clock.start("P3")
    quality_model = ReportingQualityModel(min_sigma_ratio=config.min_sigma_ratio)
    tonnage_tally, freight_tally, fob_tally = _TonnageTally(), _FreightTally(), _FobisationTally()
    n_reconcilable = 0
    for key in keys:
        df_chunk, rate = fobized(key)
        tonnage_tally += converter.tally_
        fob_tally += fobizer.tally_
        freight_tally += _FreightTally.from_rates(rate)
        io.spill(
            "freight",
            key,
            pd.DataFrame({"cif_rate": rate.replace([np.inf, -np.inf], np.nan).to_numpy()}),
        )
        # Flux réconciliables : au moins une déclaration strictement positive
        n_reconcilable += int(((df_chunk["v_x"] > 0) | (df_chunk["v_m_fob"] > 0)).sum())
        for target in _QUALITY_TARGETS:
            quality_model.partial_fit(
                df_chunk, target=target, phase="fit",
                exporters=valid_iso, importers=valid_iso, years=years,
            )
    if n_reconcilable == 0:
        raise ValueError("No reconciled flow produced")
    for target in _QUALITY_TARGETS:
        quality_model.finalize(target)
    clock.stop()

    # S2 — quantiles des taux de fret (hors mémoire, par l'appelant)
    clock.start("S2")
    quantiles = (
        io.quantiles("freight", "cif_rate", FREIGHT_QUANTILES)
        if freight_tally.n_finite
        else [float("nan")] * len(FREIGHT_QUANTILES)
    )
    freight_tally.apply(gravity_model.report_, quantiles)
    clock.stop()

    # P4 — covariance robuste de l'ANOVA, σ̂ par pays
    clock.start("P4")
    for key in keys:
        df_chunk, _ = fobized(key)
        for target in _QUALITY_TARGETS:
            quality_model.partial_fit(df_chunk, target=target, phase="cov")
    for target in _QUALITY_TARGETS:
        quality_model.finalize(target)
    quality_value = quality_model.results_["value"]
    quality_qty = quality_model.results_["quantity"]
    clock.stop()

    # P5 — réconciliation, réallocation NES et écriture année par année
    clock.start("P5")
    reconciler = MirrorReconciler()
    reallocator = AreaNesReallocator(
        flow_col=schema.flow_col,
        export_code=schema.export_code,
        reporter_iso_col=schema.reporter_iso_col,
        product_col=schema.product_col,
        period_col=schema.period_col,
        value_col=schema.value_col,
    )
    totals = {"flows": 0, "value": 0.0, "blocks": 0}
    nes_tally = _NesTally()
    rows_by_year: List[Tuple[int, int]] = []

    def reconciled_blocks(year_keys: Sequence[ChunkKey]) -> Iterator[pd.DataFrame]:
        nonlocal nes_tally
        for key in year_keys:
            df_chunk, _ = fobized(key)
            df_reconciled = reconciler.transform(df_chunk, quality_value, quality_qty)
            if effective_apply_nes:
                df_reconciled = reallocator.transform(df_reconciled, df_chunk, io.read("nes", key))
                nes_tally += reallocator.tally_
            # Nettoyage : valeurs réconciliées exploitables, poids transitoire retiré
            df_reconciled = df_reconciled[df_reconciled["reconciled_value"].notna()].reset_index(drop=True)
            df_reconciled = df_reconciled.drop(columns=[_VALUE_WEIGHT], errors="ignore")
            totals["flows"] += int(len(df_reconciled))
            totals["value"] += float(df_reconciled["reconciled_value"].sum())
            totals["blocks"] += 1
            yield df_reconciled

    for year in years:
        year_keys = [key for key in keys if key.year == year]
        started, flows_before, blocks_before = time.perf_counter(), totals["flows"], totals["blocks"]
        io.write_year(year, reconciled_blocks(year_keys))
        if totals["blocks"] - blocks_before != len(year_keys):
            raise RuntimeError(
                f"write_year({year}) consumed {totals['blocks'] - blocks_before} of "
                f"{len(year_keys)} blocks: the year would be written incompletely."
            )
        rows = totals["flows"] - flows_before
        rows_by_year.append((year, rows))
        tracker.log_metrics(
            {"timing/seconds": time.perf_counter() - started, "output/rows": float(rows), **probe()},
            step=int(year),
        )
    clock.stop()

    # Parts du tonnage sur l'ensemble des tranches (les appels par tranche les écrasent)
    converter._apply_tally(tonnage_tally)

    # Rapport composite : identique à celui de run_baci
    report = BaciReport(
        flows=totals["flows"],
        regime_country_years=len(df_regime),
        regime_fob_country_years=int((df_regime["regime"] == _FOB_REGIME).sum()),
        regime_no_information=int(df_regime["cif_share"].isna().sum()),
        classification_code=state.classification_code,
        period_start=effective_period_start,
        period_end=effective_period_end,
        n_input_declarations=state.n_input_declarations,
        total_reconciled_value=totals["value"],
        tonnage=converter.report_,
        gravity=gravity_model.report_,
        fobisation=fob_tally.report(df_regime),
        mirror=state.mirror.report(),
        quality_value=quality_value.report,
        quality_quantity=quality_qty.report,
        nes=nes_tally.report() if effective_apply_nes else NesReport(),
    )

    # Métriques de passes et artefacts d'étape
    tracker.log_metrics(
        {f"passes/{name}/seconds": seconds for name, seconds in clock.seconds.items()} | probe()
    )
    if log_artifacts:
        _log_artifacts(
            tracker,
            converter=converter,
            gravity_model=gravity_model,
            df_regime=df_regime,
            quality_value=quality_value,
            quality_qty=quality_qty,
        )
    df_rows_by_year = pd.DataFrame(rows_by_year, columns=["year", "rows"])
    return report, df_rows_by_year


# Implémentation en mémoire des entrées / sorties du traitement par passes
class InMemoryPassIO:
    """In-memory :class:`BaciPassIO` (tests, small data, doctests).

    Holds the source chunks, the intermediate data and the written years in
    dictionaries; the out-of-core statistics are computed with pandas on the
    concatenation of the stored chunks, as :func:`world_median_unit_values`
    does on a single frame.

    Args:
        comtrade: Source chunks ``(key, declarations)``, already harmonised.

    Attributes:
        store: Intermediate data by kind and chunk.
        written: Reconciled flows written, by year.
        p0_state: Saved state of the preparation pass.

    Examples:
        >>> io = InMemoryPassIO([])
        >>> io.spill("freight", ChunkKey(2020), pd.DataFrame({"cif_rate": [0.1, 0.3]}))
        >>> io.quantiles("freight", "cif_rate", [0.5])
        [0.2]
    """

    def __init__(self, comtrade: Iterable[Tuple[ChunkKey, pd.DataFrame]]) -> None:
        self.comtrade = list(comtrade)
        self.store: Dict[str, Dict[ChunkKey, pd.DataFrame]] = {}
        self.written: Dict[int, pd.DataFrame] = {}
        self.p0_state: Optional[Dict[str, pd.DataFrame]] = None

    def comtrade_chunks(self, columns: Sequence[str]) -> Iterator[Tuple[ChunkKey, pd.DataFrame]]:
        """Yield the source chunks, projected on the available requested columns."""
        for key, df_chunk in self.comtrade:
            yield key, df_chunk[[col for col in columns if col in df_chunk.columns]].copy()

    def spill(self, kind: str, key: ChunkKey, df: pd.DataFrame) -> None:
        """Store a copy of the intermediate data of a chunk."""
        self.store.setdefault(kind, {})[key] = df.copy()

    def read(self, kind: str, key: ChunkKey, columns: Optional[Sequence[str]] = None) -> pd.DataFrame:
        """Return a copy of the intermediate data of a chunk."""
        df = self.store[kind][key]
        return (df if columns is None else df[list(columns)]).copy()

    def median_uv(
        self,
        df_rates: pd.DataFrame,
        *,
        tonne_conversion_factors: Mapping[int, float],
        prefer_netwgt: bool,
    ) -> pd.Series:
        """Median unit value per product over every stored mirror chunk."""
        converter = TonnageConverter(
            tonne_conversion_factors=tonne_conversion_factors, prefer_netwgt=prefer_netwgt
        )
        converter.conversion_rates_ = {
            (str(product), int(unit)): float(rate)
            for product, unit, rate in df_rates[[_PROD, "unit", "rate"]].itertuples(index=False)
        }
        converter.report_ = TonnageReport()
        frames = [
            _stacked_unit_values(converter.transform(df))
            for df in self.store.get("mirror", {}).values()
        ]
        if not frames:
            return pd.Series(dtype="float64", name="uv")
        return pd.concat(frames, ignore_index=True).groupby(_PROD)["uv"].median()

    def quantiles(self, kind: str, column: str, probs: Sequence[float]) -> List[float]:
        """Linear-interpolation quantiles of a column over every stored chunk."""
        values = pd.concat(
            [df[column] for df in self.store.get(kind, {}).values()], ignore_index=True
        ).dropna()
        return [float(values.quantile(prob)) for prob in probs]

    def write_year(self, year: int, blocks: Iterator[pd.DataFrame]) -> None:
        """Concatenate and keep the blocks of a year (replacing a previous write)."""
        frames = list(blocks)
        self.written[int(year)] = (
            pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        )

    def load_p0_state(self) -> Optional[Mapping[str, pd.DataFrame]]:
        """Return the saved state of the preparation pass, if any."""
        return self.p0_state

    def save_p0_state(self, frames: Mapping[str, pd.DataFrame]) -> None:
        """Keep a copy of the state of the preparation pass."""
        self.p0_state = {name: df.copy() for name, df in frames.items()}

    def result(self) -> pd.DataFrame:
        """Return every written year, concatenated in year order."""
        frames = [self.written[year] for year in sorted(self.written)]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────
# Alimentation du suivi d'exécution
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : artefacts d'une exécution
def _log_artifacts(
    tracker: RunTracker,
    *,
    converter: TonnageConverter,
    gravity_model: CifGravityModel,
    df_regime: pd.DataFrame,
    quality_value: QualityResult,
    quality_qty: QualityResult,
) -> None:
    """Send the five auditable by-products of a run to the tracker.

    Args:
        tracker: Tracker receiving the artifacts.
        converter: Fitted tonne converter (validated conversion rates).
        gravity_model: Fitted gravity model (coefficients and standard errors).
        df_regime: Inferred import valuation regimes.
        quality_value: Reporting quality estimated on values.
        quality_qty: Reporting quality estimated on quantities.
    """
    # Taux de conversion validés, réutilisables tels quels
    df_rates = pd.DataFrame(
        [
            {"product": product, "unit": unit, "rate": rate}
            for (product, unit), rate in converter.conversion_rates_.items()
        ],
        columns=["product", "unit", "rate"],
    )
    tracker.log_table(df_rates, "tonnage/conversion_rates.csv")

    # Coefficients de gravité et écarts-types : contrôle de signe
    result = gravity_model.result_
    tracker.log_dict(
        {
            "coefficients": gravity_model.report_.coefficients,
            "std_errors": {
                str(name): float(value) for name, value in result.bse.items()
            },
            "r_squared": float(result.rsquared),
            "n_obs": int(result.nobs),
        },
        "gravity/coefficients.json",
    )

    # Régimes de valorisation inférés : validation du chantier C
    tracker.log_table(df_regime, "fobisation/valuation_regimes.csv")

    # σ̂ par pays et par cible : classement des déclarants
    df_sigma = pd.concat(
        [
            quality_value.sigma_export.rename("sigma_export_value"),
            quality_value.sigma_import.rename("sigma_import_value"),
            quality_qty.sigma_export.rename("sigma_export_quantity"),
            quality_qty.sigma_import.rename("sigma_import_quantity"),
        ],
        axis=1,
    )
    df_sigma.index.name = "country"
    tracker.log_table(df_sigma.reset_index(), "quality/sigma_by_country.csv")
