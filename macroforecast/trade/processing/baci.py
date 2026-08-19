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

Notes:
    Unlike the ``vulnerabilities`` module (backend-agnostic via narwhals), this
    module works on eager pandas frames throughout: the econometric backends
    (``statsmodels``, ``linearmodels``) are pandas/numpy bound, so a single native
    backend keeps the estimation code straightforward.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
import logging
import math
from typing import Dict, List, Optional, Sequence, Tuple
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

# Configuration des conventions de schéma et des paramètres méthodologiques
@dataclass(frozen=True)
class BaciConfig:
    """Schema conventions and methodological parameters for the BACI pipeline.

    Centralises every assumption the pipeline makes about the COMTRADE and CEPII
    schemas, together with the numeric thresholds and country lists of the CEPII
    methodology, so the same code can be reused on differently named datasets by
    passing another configuration.

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
        netwgt_col: Column with the net weight (kilograms).
        weight_unit_codes: Quantity-unit codes already expressed as a weight in
            kilograms (converted to tonnes by ``kg_to_tonne``). Quantities in
            any other unit without a validated conversion rate are abandoned
            (tonnage ``NaN``) while their value is kept.
        kg_to_tonne: Multiplicative factor from kilograms to tonnes.
        min_mirror_flows: Minimum mirror observations to validate a conversion
            rate (``n >= 10``).
        max_conversion_std: Maximum std of the ratios to validate a rate
            (``std < 2.5``).
        prefer_netwgt: When ``True``, use ``netWgt`` as the primary tonnage source
            and fall back to unit conversion only when it is missing.
        distance_column: CEPII distance column to use (population-weighted).
        cook_factor: Cook's-distance cutoff factor; observations with
            ``cook > cook_factor / n`` are dropped before the final gravity fit.
        non_cif_countries: Importer ISO-3 codes that do not declare CIF (freight
            never stripped).
        fas_countries: Importer ISO-3 codes declaring FAS (freight stripped only
            when it reduces the mirror gap).
        excluded_pairs: Country pairs whose internal flows are dropped.
        world_partner_code: Numeric partner code of the *World* aggregate
            (rows dropped upfront).
        nes_partner_codes: Numeric partner codes of the "Areas NES" aggregates
            eligible for reallocation.
        nes_skip_codes: Numeric partner codes of aggregates left untouched and
            excluded from the data (e.g. "Other Asia, nes").
        dist_iso_o_col: CEPII distance column with the origin ISO-3 code.
        dist_iso_d_col: CEPII distance column with the destination ISO-3 code.
        contig_col: CEPII contiguity indicator column.
        geo_iso_col: CEPII geography column with the ISO-3 code.
        landlocked_col: CEPII geography landlocked indicator column.
        primary_keys: Primary-key columns of the reconciled result table.
    """
    # Colonnes COMTRADE
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
    # Conversion des quantités en tonnes
    weight_unit_codes: Tuple[int, ...] = (8,)  # 8 = poids en kilogrammes (COMTRADE)
    kg_to_tonne: float = 1e-3
    min_mirror_flows: int = 10
    max_conversion_std: float = 2.5
    prefer_netwgt: bool = True
    # Équation de gravité
    distance_column: str = "distw"
    cook_factor: float = 4.0
    # Listes de pays (ISO-3)
    non_cif_countries: Tuple[str, ...] = (
        "DZA", "GEO", "ZAF", "BWA", "LSO", "NAM", "SWZ",
    )
    fas_countries: Tuple[str, ...] = ("CAN",)
    excluded_pairs: Tuple[Tuple[str, str], ...] = (("BEL", "LUX"),)
    # Zones non spécifiées / agrégats
    world_partner_code: int = 0
    nes_partner_codes: Tuple[int, ...] = (899,)  # 899 = "Areas, nes"
    nes_skip_codes: Tuple[int, ...] = (490,)  # 490 = "Other Asia, nes"
    # Colonnes CEPII
    dist_iso_o_col: str = "iso_o"
    dist_iso_d_col: str = "iso_d"
    contig_col: str = "contig"
    geo_iso_col: str = "iso3"
    landlocked_col: str = "landlocked"
    # Clés primaires du résultat
    primary_keys: Tuple[str, ...] = ("exporter", "importer", "product", "year")


# Configuration par défaut (schéma COMTRADE tariffline + CEPII dist/geo)
DEFAULT_CONFIG = BaciConfig()

# Noms de colonnes canoniques de la table de flux miroirs
_EXP, _IMP, _PROD, _YEAR = "exporter", "importer", "product", "year"

# /!\ Plutôt que de passer la config en argument de chaque étape, on pourrait ne la mettre en argument que dans run_baci et passer un à un les valeurs pertinentes à chaque étape à travers des arguments distincts

# ──────────────────────────────────────────────────────────────────────
# Chargement des données (gravité CEPII)
# ──────────────────────────────────────────────────────────────────────

# Fonction de chargement des variables de gravité CEPII
def build_gravity_data(
    dist: pd.DataFrame,
    geo: pd.DataFrame,
    config: BaciConfig = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """Assemble the bilateral CEPII gravity variables.

    Joins the ``dist_cepii`` bilateral table (distance, contiguity) with the
    per-country ``geo_cepii`` landlocked indicator (merged twice, for the origin
    and the destination).

    Args:
        dist: Raw ``dist_cepii`` table, already loaded (e.g. via
            :class:`macroforecast.storage2.Loader`).
        geo: Raw ``geo_cepii`` table, already loaded.
        config: Column conventions.

    Returns:
        A bilateral gravity frame with columns ``iso_o``, ``iso_d``, ``distw``,
        ``contig``, ``landlocked_o`` and ``landlocked_d``.

    Raises:
        KeyError: If an expected CEPII column is absent.
    """
    # Distance bilatérale + contiguïté
    dist_cols = [
        config.dist_iso_o_col,
        config.dist_iso_d_col,
        config.distance_column,
        config.contig_col,
    ]
    dist = dist[dist_cols].copy()

    # Enclavement par pays : géographie dédupliquée (une ligne par ISO-3, la
    # table CEPII listant plusieurs villes par pays)
    geo_unique = (
        geo[[config.geo_iso_col, config.landlocked_col]]
        .drop_duplicates(subset=[config.geo_iso_col])
        .rename(columns={config.geo_iso_col: "iso"})
    )

    # Jointure de l'enclavement de l'origine puis de la destination
    merged = dist.rename(
        columns={
            config.dist_iso_o_col: "iso_o",
            config.dist_iso_d_col: "iso_d",
            config.distance_column: "distw",
            config.contig_col: "contig",
        }
    )
    merged = merged.merge(
        geo_unique.rename(columns={"iso": "iso_o", config.landlocked_col: "landlocked_o"}),
        on="iso_o",
        how="left",
    )
    merged = merged.merge(
        geo_unique.rename(columns={"iso": "iso_d", config.landlocked_col: "landlocked_d"}),
        on="iso_d",
        how="left",
    )

    # Coercition numérique : les fichiers CEPII notent les manquants par un "."
    # (colonnes alors de type object) — conversion en flottant, manquants → NaN.
    for col in ("distw", "contig", "landlocked_o", "landlocked_d"):
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    return merged


# ──────────────────────────────────────────────────────────────────────
# Construction des flux miroirs
# ──────────────────────────────────────────────────────────────────────

# Fonction d'agrégation d'un côté de déclaration à la maille du flux
def _aggregate_side(
    df: pd.DataFrame,
    config: BaciConfig,
    exporter_col: str,
    importer_col: str,
    suffix: str,
) -> pd.DataFrame:
    """Aggregate one declaration side to the ``(i, j, k, t)`` grain.

    Sums value and net weight over the customs/mode sub-dimensions and keeps, for
    the quantity, the unit code carrying the largest summed quantity.

    Args:
        df: Declarations of a single flow direction (export or import).
        config: Column conventions.
        exporter_col: Column to use as the exporter identity.
        importer_col: Column to use as the importer identity.
        suffix: Suffix appended to the produced value columns (``"x"`` or ``"m"``).

    Returns:
        One row per ``(exporter, importer, product, year)`` with value, quantity,
        unit code and net weight columns suffixed by ``suffix``.
    """
    keys = [exporter_col, importer_col, config.product_col, "_year"]

    # Agrégation valeur + poids net par flux
    base = (
        df.groupby(keys, dropna=True)
        .agg(
            **{
                f"v_{suffix}": (config.value_col, "sum"),
                f"nw_{suffix}": (config.netwgt_col, "sum"),
            }
        )
        .reset_index()
    )

    # Quantité : unité portant la plus grande quantité cumulée par flux
    qty = (
        df.groupby(keys + [config.qty_unit_col], dropna=True)[config.qty_col]
        .sum()
        .reset_index()
    )
    qty = qty.sort_values(config.qty_col, ascending=False).drop_duplicates(subset=keys)
    qty = qty.rename(
        columns={config.qty_col: f"q_{suffix}", config.qty_unit_col: f"unit_{suffix}"}
    )

    merged = base.merge(qty, on=keys, how="left")
    # Renommage des identités vers les colonnes canoniques
    return merged.rename(
        columns={
            exporter_col: _EXP,
            importer_col: _IMP,
            config.product_col: _PROD,
            "_year": _YEAR,
        }
    )


# Fonction de construction de la table des flux miroirs
def build_mirror_flows(
    df_comtrade: pd.DataFrame,
    valid_iso: Sequence[str],
    config: BaciConfig = DEFAULT_CONFIG,
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
        config: Column conventions.

    Returns:
        A tuple ``(mirror, nes)``:

        * ``mirror``: one row per ``(exporter, importer, product, year)`` with
          columns ``v_x, q_x, unit_x, nw_x`` (export/FOB side) and
          ``v_m, q_m, unit_m, nw_m`` (import/CIF side), outer-joined.
        * ``nes``: import/export declarations whose partner is an "Areas NES"
          aggregate (:attr:`BaciConfig.nes_partner_codes`), kept aside for the
          reallocation step.
    """
    # Copiée indépendante du jeu de données
    df = df_comtrade.copy()
    # Année entière dérivée de la période (chaîne "YYYY")
    df["_year"] = df[config.period_col].astype(str).str[:4].astype(int)

    # Exclusion explicite des agrégats non traités : (ex. Monde, « Other Asia, nes », ni réallouées ni pays)
    df = df[df[config.partner_code_col] != config.world_partner_code]
    if config.nes_skip_codes:
        df = df[~df[config.partner_code_col].isin(list(config.nes_skip_codes))]

    # Séparation des déclarations d'export et d'import
    is_export = df[config.flow_col] == config.export_code
    is_import = df[config.flow_col] == config.import_code

    valid = set(valid_iso)

    # Flux NES : partenaire agrégé « Areas nes » (avant filtre pays individuels)
    nes_mask = df[config.partner_code_col].isin(list(config.nes_partner_codes))
    df_nes = df[(is_export | is_import) & nes_mask].copy()

    # Restriction aux pays individuels (reporter et partenaire valides ISO-3)
    individual = (
        df[config.reporter_iso_col].isin(valid)
        & df[config.partner_iso_col].isin(valid)
    )
    df_exports = df[is_export & individual].copy()
    df_imports = df[is_import & individual].copy()

    # Agrégation de chaque côté à la maille du flux
    # Export : exportateur = reporter, importateur = partenaire
    df_x_side = _aggregate_side(
        df_exports, config, config.reporter_iso_col, config.partner_iso_col, "x"
    )
    # Import : importateur = reporter, exportateur = partenaire
    df_m_side = _aggregate_side(
        df_imports, config, config.partner_iso_col, config.reporter_iso_col, "m"
    )

    # Jointure externe des deux côtés sur la maille du flux
    df_mirror = df_x_side.merge(df_m_side, on=[_EXP, _IMP, _PROD, _YEAR], how="outer")

    # Retrait des paires exclues (flux internes instables, ex. BEL-LUX)
    for a, b in config.excluded_pairs:
        drop = (
            ((df_mirror[_EXP] == a) & (df_mirror[_IMP] == b))
            | ((df_mirror[_EXP] == b) & (df_mirror[_IMP] == a))
        )
        df_mirror = df_mirror[~drop]

    return df_mirror.reset_index(drop=True), df_nes.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────────────
# Étape 1 — Conversion des quantités en tonnes
# ──────────────────────────────────────────────────────────────────────

# Estimateur des taux de conversion vers la tonne
# /!\ Dans la nomenclature des codes disponibles sur UN Comtrade Database, il apparait que la 21 correspond aux tonnes, hors je ne vois aucun traitement particulier pour ce code
# /!\ Spécifier dans la docstring que le netWeight est renseigné en kilogrammes dans UN Comtrade (https://uncomtrade.org/docs/supplementary-quantity-units/ et https://unstats.un.org/wiki/spaces/I2CG/pages/6325189/A.+An+overview+of+the+World+Customs+Organization+standard+units+of+quantity)
# /!\ Mentionner également quelque part que l'on utilise la méthodologie de traitement et de conversion qui est utilisée par BACI mais que UnStatistics mentionne également https://unstats.un.org/wiki/spaces/I2CG/pages/6325204/E.+Estimation+and+imputation+of+quantity+data et https://unstats.un.org/wiki/spaces/I2CG/pages/6325197/C.+Factors+with+which+to+convert+from+non-standard+to+standard+units+of+quantity qui sont parfois utilisés comme retraitements préalables à la diffusion des données COMTRADE
class TonnageConverter:
    """Convert heterogeneous declared quantities to tonnes.

    Estimates, per ``(product, source unit)``, an implicit conversion rate from
    mirror flows where one partner declares in tonnes and the other in the source
    unit. A rate is validated only when at least :attr:`BaciConfig.min_mirror_flows`
    observations are available and their std is below
    :attr:`BaciConfig.max_conversion_std`.

    Args:
        config: Column and threshold conventions.

    Attributes:
        conversion_rates_: Mapping ``(product, unit) -> rate`` (tonnes per unit),
            populated by :meth:`fit`.
    """

    # Initialisation
    def __init__(self, config: BaciConfig = DEFAULT_CONFIG) -> None:
        # Intialisation des attributs
        # Stockage de la config
        self.config = config
        # Dictionnaire des taux de conversion
        self.conversion_rates_: Dict[Tuple[str, int], float] = {}

    # Méthode auxiliaire : quantité en tonnes déjà connue (poids)
    def _tonnes_from_weight(self, qty: pd.Series, unit: pd.Series, nw: pd.Series) -> pd.Series:
        """Return the tonnage known without any estimated rate.

        Args:
            qty: Declared quantities.
            unit: Declared quantity-unit codes.
            nw: Net weights (kilograms).

        Returns:
            Tonnage from net weight (preferred) or from weight-unit quantities;
            ``NaN`` where neither is available.
        """
        # Extraction de la config
        cfg = self.config
        # Initialisation de la série des valeurs en tonnes
        tonnes = pd.Series(np.nan, index=qty.index, dtype="float64")
        # Repli/priorité sur le poids net (kg → tonnes)
        if cfg.prefer_netwgt:
            tonnes = tonnes.where(~(nw > 0), nw * cfg.kg_to_tonne)
        # Quantités déjà exprimées en unité de poids (kg → tonnes)
        weight_unit = unit.isin(list(cfg.weight_unit_codes))
        tonnes = tonnes.where(~(tonnes.isna() & weight_unit), qty * cfg.kg_to_tonne)
        return tonnes

    # Estimation des taux de conversion
    def fit(self, mirror: pd.DataFrame) -> "TonnageConverter":
        """Estimate per-``(product, unit)`` conversion rates from mirror flows.

        Args:
            mirror: Mirror-flow table from :func:`build_mirror_flows`.

        Returns:
            The fitted converter (``self``).
        """
        # Extraction de la configuration
        cfg = self.config
        # Initialisation du dictionnaire des taux de conversion
        rates: Dict[Tuple[str, int], float] = {}

        # Tonnage connu (poids) de chaque côté, sans taux estimé
        t_x = self._tonnes_from_weight(mirror["q_x"], mirror["unit_x"], mirror["nw_x"])
        t_m = self._tonnes_from_weight(mirror["q_m"], mirror["unit_m"], mirror["nw_m"])

        # Observations du taux : un côté connu en tonnes, l'autre en unité source
        records: List[Tuple[str, int, float]] = []
        # Côté export en tonnes, import en unité source
        _collect_ratio(records, mirror["q_m"], mirror["unit_m"], t_x, mirror[_PROD], cfg)
        # Côté import en tonnes, export en unité source
        _collect_ratio(records, mirror["q_x"], mirror["unit_x"], t_m, mirror[_PROD], cfg)

        if records:
            ratios = pd.DataFrame(records, columns=[_PROD, "unit", "ratio"])
            # Filtres de validité : n ≥ 10 et écart-type < 2,5
            grouped = ratios.groupby([_PROD, "unit"])["ratio"]
            stats = grouped.agg(["mean", "std", "count"]).reset_index()
            valid = stats[
                (stats["count"] >= cfg.min_mirror_flows)
                & (stats["std"] < cfg.max_conversion_std)
            ]
            rates = {
                (row[_PROD], int(row["unit"])): row["mean"]
                for _, row in valid.iterrows()
            }

        # Mise à jour des taux de conversion
        self.conversion_rates_ = rates

        # Logging
        logger.info("TonnageConverter: %d taux de conversion validés", len(rates))

        return self

    # Application des taux : ajout des quantités en tonnes
    def transform(self, mirror: pd.DataFrame) -> pd.DataFrame:
        """Add tonne-denominated quantities ``q_x_t`` and ``q_m_t``.

        Args:
            mirror: Mirror-flow table.

        Returns:
            The frame with two added columns ``q_x_t`` and ``q_m_t`` (tonnes),
            ``NaN`` when no source (weight or validated rate) is available.
        """
        # Copie indépendante du jeu de données
        out = mirror.copy()
        # Ajout de colonnes exprimant les quantités en tonnes (à l'import et à l'export)
        out["q_x_t"] = self._to_tonnes(out["q_x"], out["unit_x"], out["nw_x"], out[_PROD])
        out["q_m_t"] = self._to_tonnes(out["q_m"], out["unit_m"], out["nw_m"], out[_PROD])

        return out

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
    config: BaciConfig,
) -> None:
    """Append ``(product, unit, ratio)`` observations to ``records`` in place.

    An observation exists when the *other* side is known in tonnes and the current
    side is a non-weight unit: ``ratio = tonnes_other / qty_unit``.

    Args:
        records: Accumulator list mutated in place.
        qty_unit: Quantities declared in a heterogeneous (source) unit.
        unit_unit: Unit codes of ``qty_unit``.
        tonnes_other: Tonnage of the mirror partner (the tonnes side).
        product: Product codes.
        config: Column conventions.
    """
    # Côté source non exprimé en poids et quantité strictement positive
    is_source = (~unit_unit.isin(list(config.weight_unit_codes))) & (qty_unit > 0)
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
        mirror: Mirror-flow table with tonne quantities (``q_x_t``).

    Returns:
        Series of median unit values indexed by product.
    """
    # Valeur unitaire export sur flux exploitables
    uv = df_mirror["v_x"] / df_mirror["q_x_t"]
    # Extraction des valeurs unitaires valides non nulles
    valid = df_mirror.loc[(df_mirror["v_x"] > 0) & (df_mirror["q_x_t"] > 0), [_PROD]].copy()
    valid["uv"] = uv[valid.index]
    # Calcul de la médiane par produit
    return valid.groupby(_PROD)["uv"].median()


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
        config: Column and threshold conventions.

    Attributes:
        result_: Fitted ``statsmodels`` WLS results.
        design_columns_: Ordered design-matrix columns (excluding the constant).
        uv_world_: World median unit values used at fit time.
    """

    # Initialisation
    def __init__(self, config: BaciConfig = DEFAULT_CONFIG) -> None:
        # Initialisation des attributs
        # Initialisation de la configuration
        self.config = config
        # Initialisation des attributs résultats
        self.result_ = None
        self.design_columns_: List[str] = []
        self.uv_world_: Optional[pd.Series] = None

    # Construction de la matrice de design (variables explicatives)
    def _design(self, df: pd.DataFrame, gravity: pd.DataFrame) -> pd.DataFrame:
        """Build the gravity design frame (regressors only).

        Args:
            df: Flows with tonne quantities and the mirror identities.
            gravity: Bilateral gravity frame from :func:`build_gravity_data`.

        Returns:
            A design frame indexed like ``df`` with the gravity regressors and
            year dummies; rows with missing distance are dropped.
        """
        # Jointure des variables de gravité (exportateur = origine, importateur = destination)
        merged = df.merge(
            gravity.rename(columns={"iso_o": _EXP, "iso_d": _IMP}),
            on=[_EXP, _IMP],
            how="left",
        )
        # Valeur unitaire médiane mondiale du produit
        merged["uv_world"] = merged[_PROD].map(self.uv_world_)

        # Variables transformées (logarithmes protégés)
        design = pd.DataFrame(index=merged.index)
        design["ln_dist"] = np.log(merged["distw"])
        design["ln_dist2"] = design["ln_dist"] ** 2
        design["contig"] = merged["contig"].astype("float64")
        design["landlocked_i"] = merged["landlocked_o"].astype("float64")
        design["landlocked_j"] = merged["landlocked_d"].astype("float64")
        design["ln_uv"] = np.log(merged["uv_world"])
        # Indicatrices d'année (référence = première année)
        year_dummies = pd.get_dummies(merged[_YEAR], prefix="year", drop_first=True).astype("float64")
        design = pd.concat([design, year_dummies], axis=1)
        return design

    # Estimation du modèle
    def fit(self, mirror: pd.DataFrame, gravity: pd.DataFrame) -> "CifGravityModel":
        """Estimate the gravity equation on complete mirror flows.

        Args:
            mirror: Mirror-flow table with tonne quantities.
            gravity: Bilateral gravity frame.

        Returns:
            The fitted model (``self``).

        Raises:
            ValueError: If no usable observation remains after filtering.
        """
        # Extraction de la configuration
        cfg = self.config
        # Valeurs unitaires mondiales calées sur l'échantillon
        self.uv_world_ = world_median_unit_values(mirror)

        # Échantillon d'estimation : flux miroirs complets exploitables
        m = mirror[
            (mirror["v_x"] > 0)
            & (mirror["v_m"] > 0)
            & (mirror["q_x_t"] > 0)
            & (mirror["q_m_t"] > 0)
        ].copy()

        # Variable dépendante : log du rapport des valeurs unitaires (CAF/FAB)
        uv_x = m["v_x"] / m["q_x_t"]
        uv_m = m["v_m"] / m["q_m_t"]
        y = np.log(uv_m / uv_x)

        # Pondération : rapport des quantités miroirs min/max ∈ (0, 1]
        q_min = np.minimum(m["q_x_t"], m["q_m_t"])
        q_max = np.maximum(m["q_x_t"], m["q_m_t"])
        weights = (q_min / q_max).to_numpy()

        # Matrice de design
        # Variable explicatives
        design = self._design(m, gravity)
        # Ajout de la variable dépendante
        design["_y"] = y.to_numpy()
        # Ajout des poids
        design["_w"] = weights
        # Retrait des lignes non exploitables (gravité/UV manquants, y non fini)
        design = design.replace([np.inf, -np.inf], np.nan).dropna()
        if design.empty:
            raise ValueError("No usable observation for the gravity estimation")

        # Extraction de la variable dépendante
        y_fit = design.pop("_y")
        # Extraction des poids
        w_fit = design.pop("_w")
        # Enumération des colonnes correspondant aux variables explicatives
        self.design_columns_ = list(design.columns)
        # Ajout d'une constante
        X = sm.add_constant(design, has_constant="add")

        # Première estimation WLS
        model = sm.WLS(y_fit, X, weights=w_fit)
        res = model.fit()

        # Robustesse : retrait des observations influentes (distance de Cook)
        try:
            

            # Influence calculée sur le modèle blanchi (OLS sur données
            # transformées par √w, équivalent exact du WLS) afin que la
            # distance de Cook tienne compte des poids
            sqrt_w = np.sqrt(np.asarray(w_fit, dtype="float64"))
            whitened = sm.OLS(
                np.asarray(y_fit, dtype="float64") * sqrt_w,
                X.to_numpy(dtype="float64") * sqrt_w[:, None],
            ).fit()
            # Calcul de a distance de cook
            cook = OLSInfluence(whitened).cooks_distance[0]
            # Détermination du seuil de décision
            cutoff = cfg.cook_factor / len(cook)
            # Conservation des observations qui satisfont le seuil
            keep = cook < cutoff
            if keep.sum() > X.shape[1] and (~keep).any():
                res = sm.WLS(y_fit[keep], X[keep], weights=w_fit[keep]).fit()
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
    def predict(self, mirror: pd.DataFrame, gravity: pd.DataFrame) -> pd.Series:
        """Predict the freight rate ``exp(X β̂)`` for each flow.

        Args:
            mirror: Mirror-flow table with tonne quantities.
            gravity: Bilateral gravity frame.

        Returns:
            Series of estimated freight rates ``τ̂`` aligned on ``mirror`` (``NaN``
            where gravity variables are missing). The dependent variable is
            ``ln(UVm/UVx) = ln(1 + τ)``, so the freight rate is
            ``τ̂ = exp(X β̂) − 1`` and the fobisation divides by ``1 + τ̂``.
        """
        # Construction du design puis alignement sur les colonnes d'estimation
        design = self._design(mirror, gravity)
        design = design.reindex(columns=self.design_columns_, fill_value=0.0)
        X = sm.add_constant(design, has_constant="add")
        pred = self.result_.predict(X)
        # La régression prédit ln(1 + τ) : le taux de fret est exp(prédiction) − 1
        return np.expm1(pred)


# ──────────────────────────────────────────────────────────────────────
# Étape 3 — Retrait du fret des importations (fobisation)
# ──────────────────────────────────────────────────────────────────────

# Transformateur de fobisation des valeurs d'importation
# LOG MLFLOW : Logger la part du nombre total de flux qui est in fine corrigé ainsi que la part des flux d'importation auxquels est appliquée une correction
class Fobizer:
    """Strip the estimated freight from CIF import values.

    Computes ``V_m_fob = V_m / (1 + cif_rate)`` under the note's safeguards: no
    correction for non-CIF importers, conditional correction for FAS importers
    (only when it reduces the mirror gap), and a floor at zero.

    Args:
        config: Country-list conventions.
    """

    # Initialisation
    def __init__(self, config: BaciConfig = DEFAULT_CONFIG) -> None:
        # Initialisation des attributs
        # Instanciation de la configuration
        self.config = config

    # Application de la fobisation
    def transform(self, mirror: pd.DataFrame, cif_rate: pd.Series) -> pd.DataFrame:
        """Add the FOB-equivalent import value ``v_m_fob``.

        Args:
            mirror: Mirror-flow table.
            cif_rate: Estimated freight rate per flow (from
                :meth:`CifGravityModel.predict`).

        Returns:
            The frame with an added ``v_m_fob`` column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Copie indépendante des données
        out = mirror.copy()
        # Extraction de la valeur CIF (qui correspond généralement aux données d'importation)
        v_m = out["v_m"]

        # Valeur fobisée candidate (plancher à zéro), taux manquant → pas de correction
        rate = cif_rate.reindex(out.index)
        v_m_fob = np.where(rate.notna(), v_m / (1.0 + rate), v_m)
        v_m_fob = np.clip(v_m_fob, a_min=0.0, a_max=None)
        candidate = pd.Series(v_m_fob, index=out.index)

        # Importateurs ne déclarant pas en CAF : aucune correction
        # /!\ A déterminer à partir des données et non plus à partir de la configuration
        non_cif = out[_IMP].isin(list(cfg.non_cif_countries))
        candidate = candidate.where(~non_cif, v_m)

        # Importateurs FAS : correction conservée seulement si elle réduit l'écart miroir
        fas = out[_IMP].isin(list(cfg.fas_countries))
        if fas.any():
            has_mirror = (out["v_x"] > 0) & (v_m > 0)
            gap_before = np.abs(np.log(out["v_x"] / v_m))
            gap_after = np.abs(np.log(out["v_x"] / candidate.replace(0.0, np.nan)))
            # Ne garder la correction FAS que lorsqu'elle réduit l'écart
            revert = fas & has_mirror & ~(gap_after < gap_before)
            candidate = candidate.where(~revert, v_m)

        out["v_m_fob"] = candidate
        return out


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

    Args:
        config: Column conventions.
    """

    # Initialisation
    def __init__(self, config: BaciConfig = DEFAULT_CONFIG) -> None:
        # Initialisation des attributs
        # Instanciation de la configuration
        self.config = config

    # Estimation pour une cible (valeurs ou quantités)
    def fit(self, mirror: pd.DataFrame, target: str) -> QualityResult:
        """Estimate per-country variances for a target quantity.

        Args:
            mirror: Mirror-flow table with ``v_x``, ``v_m_fob`` (and tonne
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
            v_i, v_j = mirror["v_x"], mirror["v_m_fob"]
        elif target == "quantity":
            v_i, v_j = mirror["q_x_t"], mirror["q_m_t"]
        else:
            raise ValueError("target must be 'value' or 'quantity'")

        # Flux miroirs complets et strictement positifs
        m = mirror.assign(_vi=v_i, _vj=v_j)
        m = m[(m["_vi"] > 0) & (m["_vj"] > 0)].copy()

        # Distance de déclaration ; pondération par le log de la somme des
        # valeurs déclarées (s = ln(V_i + V_j)), y compris pour la
        # qualité estimée sur les quantités
        m["_rd"] = np.abs(np.log(m["_vi"] / m["_vj"]))
        m["_w"] = np.log(m["v_x"].fillna(0.0) + m["v_m_fob"].fillna(0.0))
        m = m[(m["_w"] > 0) & np.isfinite(m["_rd"])]

        # Effets exportateur et importateur extraits d'une même ANOVA absorbée
        effects = _absorbed_anova_effects(m, self.config)
        ls_exp, se_exp = effects[_EXP]
        ls_imp, se_imp = effects[_IMP]

        return QualityResult(
            sigma_export=_ls_mean_to_sigma(ls_exp, se_exp),
            sigma_import=_ls_mean_to_sigma(ls_imp, se_imp),
        )


# Fonction d'estimation des effets des dimensions pays par ANOVA absorbée
def _absorbed_anova_effects(
    m: pd.DataFrame, config: BaciConfig
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
        m: Prepared frame with ``_rd`` (dependent), ``_w`` (weight), the entity
            columns and ``year``/``product``.
        config: Column conventions.

    Returns:
        Mapping ``{dimension: (effects, std_errors)}`` for the ``exporter`` and
        ``importer`` dimensions, each pair being pandas Series indexed by
        country code.
    """
    # Variables explicatives : indicatrices exportateur + importateur + année.
    # Pas de constante explicite : absorbée par les effets fixes produit (elle
    # deviendrait colinéaire après transformation within).
    exog = pd.get_dummies(
        m[[_EXP, _IMP, _YEAR]].astype(str), drop_first=True
    ).astype("float64")

    # Dimension produit absorbée (transformation within)
    absorb = m[[_PROD]].astype("category")

    # Estimation absorbée pondérée : un seul ajustement pour les deux dimensions
    res = AbsorbingLS(m["_rd"], exog, absorb=absorb, weights=m["_w"]).fit()
    params = res.params
    cov = res.cov

    # Initialisation du dictionnaire résultat
    out: Dict[str, Tuple[pd.Series, pd.Series]] = {}
    # Parcours des colonnes d'importation et d'exportation
    for entity_col in (_EXP, _IMP):
        levels = sorted(m[entity_col].astype(str).unique())
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

    Args:
        config: Column conventions.
    """

    # Initialisation
    def __init__(self, config: BaciConfig = DEFAULT_CONFIG) -> None:
        self.config = config

    # Réconciliation des valeurs et des quantités
    def transform(
        self,
        mirror: pd.DataFrame,
        quality_value: QualityResult,
        quality_qty: QualityResult,
    ) -> pd.DataFrame:
        """Produce the reconciled value and quantity per flow.

        Args:
            mirror: Mirror-flow table with ``v_x``, ``v_m_fob``, ``q_x_t``,
                ``q_m_t``.
            quality_value: Per-country variances estimated on values.
            quality_qty: Per-country variances estimated on quantities.

        Returns:
            A frame keyed by ``(exporter, importer, product, year)`` with
            ``reconciled_value`` and ``reconciled_quantity``.
        """
        # Copie indépendante des données
        out = mirror.copy()

        # Réconciliation des valeurs (export FAB vs import fobisé)
        out["reconciled_value"] = _reconcile_pair(
            out, "v_x", "v_m_fob", quality_value
        )
        # Réconciliation des quantités (tonnes des deux côtés)
        out["reconciled_quantity"] = _reconcile_pair(
            out, "q_x_t", "q_m_t", quality_qty
        )
        
        # Restriction aux colonnes d'intérêt
        cols = [_EXP, _IMP, _PROD, _YEAR, "reconciled_value", "reconciled_quantity"]
        return out[cols]


# Fonction de réconciliation d'un couple de colonnes miroirs
def _reconcile_pair(
    df: pd.DataFrame, col_i: str, col_j: str, quality: QualityResult
) -> pd.Series:
    """Reconcile one mirror pair (value or quantity) into a single series.

    Args:
        df: Mirror-flow table.
        col_i: Exporter-side column (``V_i``).
        col_j: Importer-side column (``V_j``, already FOB-comparable).
        quality: Per-country variances for the reconciled quantity.

    Returns:
        Reconciled series (both-flow convex combination, single-flow passthrough,
        ``NaN`` when neither side exists).
    """
    # Extraction des valeurs d'intérêt des données
    v_i = df[col_i]
    v_j = df[col_j]
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
    sigma_i = df[_EXP].map(quality.sigma_export).astype("float64").fillna(default_i).to_numpy()
    sigma_j = df[_IMP].map(quality.sigma_import).astype("float64").fillna(default_j).to_numpy()
    # Calcul du poids
    w = _optimal_weight(sigma_i, sigma_j)

    # Combinaison convexe lorsque les deux flux existent
    both = (has_i & has_j).to_numpy()
    reconciled = pd.Series(np.nan, index=df.index, dtype="float64")
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
        config: Column and NES-code conventions.
    """

    # Initialisation
    def __init__(self, config: BaciConfig = DEFAULT_CONFIG) -> None:
        # Initialisation des attributs
        # Instanciation de la configuration
        self.config = config

    # Réallocation des flux NES sur les partenaires identifiés
    def transform(
        self, reconciled: pd.DataFrame, mirror: pd.DataFrame, nes: pd.DataFrame
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
            reconciled: Reconciled flows from :meth:`MirrorReconciler.transform`.
            mirror: Mirror-flow table (for per-partner declared/mirror sums).
            nes: "Areas NES" declarations kept aside by :func:`build_mirror_flows`.

        Returns:
            The reconciled frame with NES value distributed across identified
            partners (unchanged when there is nothing to reallocate; missing
            reconciled values stay missing).
        """
        # Extraction de la configuration
        cfg = self.config
        # Initialisation des clés
        keys3 = [_EXP, _PROD, _YEAR]
        keys4 = [_EXP, _IMP, _PROD, _YEAR]
        if nes.empty:
            return reconciled

        # Valeur NES exportée par (exportateur, produit, année)
        nes_exp = nes[nes[cfg.flow_col] == cfg.export_code].copy()

        # Vérification que le DataFrame est non vide
        if nes_exp.empty:
            return reconciled
        
        # Extraction de la date
        nes_exp["_year"] = nes_exp[cfg.period_col].astype(str).str[:4].astype(int)
        # Somme par pays, produit et année
        nes_value = (
            nes_exp.groupby([cfg.reporter_iso_col, cfg.product_col, "_year"])[cfg.value_col]
            .sum()
            .rename("v_nes")
            .reset_index()
            .rename(
                columns={cfg.reporter_iso_col: _EXP, cfg.product_col: _PROD, "_year": _YEAR}
            )
        )

        # Découpage des flux de chaque exportateur : miroirs complets (les deux
        # déclarations existent) vs imports déclarés sans miroir export
        detail = mirror[keys4 + ["v_x", "v_m_fob"]].copy()
        has_x = detail["v_x"] > 0
        has_m = detail["v_m_fob"] > 0
        complete = detail[has_x & has_m].copy()

        # Étape 1 — sous-déclaration mesurée sur les miroirs complets :
        # Σ V_x vs Σ V_m des déclarations miroirs correspondantes
        sums = (
            complete.groupby(keys3)
            .agg(sum_vx=("v_x", "sum"), sum_vm=("v_m_fob", "sum"))
            .reset_index()
        )
        alloc = nes_value.merge(sums, on=keys3, how="left")
        alloc[["sum_vx", "sum_vm"]] = alloc[["sum_vx", "sum_vm"]].fillna(0.0)
        # Valeur réallouable : min(V_nes, Σ V_m − Σ V_x) si l'exportateur sous-déclare
        alloc["shortfall"] = (alloc["sum_vm"] - alloc["sum_vx"]).clip(lower=0.0)
        alloc["realloc"] = np.minimum(alloc["v_nes"], alloc["shortfall"])

        # Répartition proportionnelle au manque par partenaire (V_m − V_x > 0)
        complete["missing"] = (complete["v_m_fob"] - complete["v_x"]).clip(lower=0.0)
        shares = complete.merge(
            alloc.loc[alloc["realloc"] > 0, keys3 + ["realloc"]],
            on=keys3,
            how="inner",
        )

        # Initialisation du jeu de données des valeurs à ajouter
        add: Optional[pd.DataFrame] = None
        if not shares.empty:
            group_missing = shares.groupby(keys3)["missing"].transform("sum")
            shares["share"] = np.where(
                group_missing > 0, shares["missing"] / group_missing, 0.0
            )
            shares["add_value"] = shares["realloc"] * shares["share"]

            # Étape 2 — garde-fou : réallocation d'un groupe conservée
            # seulement si elle réduit son écart miroir global Σ |ln(V_x/V_m)|
            shares["_gap_before"] = np.abs(np.log(shares["v_x"] / shares["v_m_fob"]))
            shares["_gap_after"] = np.abs(
                np.log((shares["v_x"] + shares["add_value"]) / shares["v_m_fob"])
            )
            gaps = (
                shares.groupby(keys3)
                .agg(gap_before=("_gap_before", "sum"), gap_after=("_gap_after", "sum"))
                .reset_index()
            )
            improving = gaps.loc[gaps["gap_after"] < gaps["gap_before"], keys3]
            shares = shares.merge(improving, on=keys3, how="inner")
            if not shares.empty:
                add = shares[keys4 + ["add_value"]]

        # Étape 3 — résidu V_nes' confronté aux imports sans miroir V_m' :
        # inférieur → déjà compté dans V_m' (ramené à zéro, pas de double
        # compte) ; sinon seul l'excédent subsiste et, sans partenaire
        # identifiable, il est écarté
        if add is not None:
            allocated = (
                add.groupby(keys3)["add_value"].sum().rename("allocated").reset_index()
            )
            residual = alloc.merge(allocated, on=keys3, how="left")
        else:
            residual = alloc.copy()
            residual["allocated"] = 0.0
        residual["allocated"] = residual["allocated"].fillna(0.0)
        residual["v_nes_prime"] = (residual["v_nes"] - residual["allocated"]).clip(lower=0.0)
        vm_prime = (
            detail[has_m & ~has_x]
            .groupby(keys3)["v_m_fob"]
            .sum()
            .rename("vm_prime")
            .reset_index()
        )
        residual = residual.merge(vm_prime, on=keys3, how="left")
        residual["vm_prime"] = residual["vm_prime"].fillna(0.0)
        residual["unallocated"] = np.where(
            residual["v_nes_prime"] < residual["vm_prime"],
            0.0,
            residual["v_nes_prime"] - residual["vm_prime"],
        )

        if add is None:
            # Logging
            logger.info(
                "AreaNesReallocator: aucune réallocation (%.1f de valeur NES, "
                "%.1f écartés faute de partenaire identifiable)",
                float(residual["v_nes"].sum()), float(residual["unallocated"].sum()),
            )
            return reconciled

        # Ajout de la valeur réallouée aux flux réconciliés ; les flux sans
        # réallocation (dont les valeurs réconciliées manquantes) restent intacts
        out = reconciled.merge(add, on=keys4, how="left")
        add_value = out.pop("add_value").fillna(0.0)
        out["reconciled_value"] = out["reconciled_value"] + add_value
        
        # Logging
        logger.info(
            "AreaNesReallocator: %d flux enrichis (%.1f réalloués, %.1f absorbés "
            "par les imports sans miroir, %.1f écartés faute de partenaire)",
            int((add_value > 0).sum()),
            float(residual["allocated"].sum()),
            float((residual["v_nes_prime"] - residual["unallocated"]).sum()),
            float(residual["unallocated"].sum()),
        )
        return out


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
        created: Whether the result schema was created (vs. upserted); left
            ``False`` by :func:`run_baci`, set by the caller after persisting.
    """
    flows: int = 0
    mean_freight_rate: float = float("nan")
    created: bool = False


# Colonnes COMTRADE nécessaires au redressement
def required_columns(config: BaciConfig = DEFAULT_CONFIG) -> List[str]:
    """Return the COMTRADE columns the pipeline reads.

    Lets the caller project only the needed columns when loading the source
    fact table before handing it to :func:`run_baci`.

    Args:
        config: Column conventions.

    Returns:
        Ordered, de-duplicated list of source columns to project.
    """
    return list(
        dict.fromkeys(
            [
                config.flow_col,
                config.reporter_iso_col,
                config.partner_iso_col,
                config.partner_code_col,
                config.product_col,
                config.period_col,
                config.value_col,
                config.qty_col,
                config.qty_unit_col,
                config.netwgt_col,
            ]
        )
    )


# Fonction d'orchestration : flux COMTRADE chargés → flux réconciliés
# /!\ Soit spécifier dans la documentation de run_baci qu'il faut que df_comtrade contienne le périmètre temporel souhaité, soit ajouter des paramètres dans la config qui spécifient le début et la fin de la période d'intérêt (None pourrait signifier que l'on garde l'ensemble des données).
# /!\ Spécifier également que l'ensemble des données doivent être dans une même nomenclature (classificationCode contient cette information dans les données comtrade), on vérifiera qu'il y a bien une homogénéité de la classification utilisée, sinon on effectuera la conversion vers la plus ancienne (car c'est le seul sens où il n'y a pas d'ambiguité). Les tables de correspondance peuvent être trouvées ici (https://unstats.un.org/unsd/classifications/econ) et j'en avais déjà téléchargées en utilisant le code dans lookup.py qui utilise les chemins dans un_lookup.json. S'il y a une manière plus élégante de récupérer ces données, je suis preneur de tes suggestions. De plus je souhaite stocker ces tables de correspondances (qui sont invariantes) et je ne souhaite pas les retélécharger à chaque conversion. Je souhaite, comme dans les autres fichiers, que la logique de ne pas télécharger si existe déjà se fasse dans les scripts (ou l'on spécifiera également le chemin) et non dans le package, ainsi que la définition de la classification que l'on utilise. Dans run_baci, on s'assurera seulement que cette dernière est homogène. La logique de conversion pourra être dnas un nouveau fichier dans trade/processing
# Ajouter "df_" comme préfixe devant les noms de variables pour les pandas.DataFrame (faire de même pour les noms des arguments des classes, fonctions, et méthodes)
def run_baci(
    df_comtrade: pd.DataFrame,
    dist: pd.DataFrame,
    geo: pd.DataFrame,
    *,
    config: BaciConfig = DEFAULT_CONFIG,
    apply_nes: bool = True,
) -> Tuple[pd.DataFrame, BaciReport]:
    """Run the BACI reconstruction end to end on already-loaded data.

    Assembles the CEPII gravity variables, builds the mirror-flow table, applies
    the six methodological steps in order, and returns the reconciled value and
    quantity per ``(exporter, importer, product, year)``. The function performs
    no I/O: reading the COMTRADE fact table and persisting the result belong to
    the caller (see ``scripts/process_baci.py``).

    Args:
        df_comtrade: Raw COMTRADE fact-table rows, holding at least the columns
            returned by :func:`required_columns`.
        dist: Raw ``dist_cepii`` table, already loaded.
        geo: Raw ``geo_cepii`` table, already loaded.
        config: Column and methodological conventions.
        apply_nes: Whether to apply the "Areas NES" reallocation step.

    Returns:
        A tuple ``(reconciled, report)``: the reconciled flows and the
        :class:`BaciReport` of the run (``created`` is left ``False``; the
        caller sets it once the result is persisted).

    Raises:
        ValueError: If the reconciliation produces no flow.
    """
    # Assemblage des données du CEPII servant à estimer les modèles de gravité
    gravity = build_gravity_data(dist, geo, config)
    # Extraction des codes pays valides
    valid_iso = sorted(set(gravity["iso_o"]) | set(gravity["iso_d"]))

    # Construction des flux miroirs (+ flux NES mis de côté)
    df_mirror, df_nes = build_mirror_flows(df_comtrade, valid_iso, config)
    
    # Étape 1 — conversion des quantités en tonnes
    converter = TonnageConverter(config).fit(df_mirror)
    df_mirror = converter.transform(df_mirror)

    # Étape 2 — estimation des taux CAF par équation de gravité
    gravity_model = CifGravityModel(config).fit(df_mirror, gravity)
    cif_rate = gravity_model.predict(df_mirror, gravity)
    mean_freight_rate = float(cif_rate.replace([np.inf, -np.inf], np.nan).dropna().mean())
    print(mean_freight_rate)

    # Étape 3 — fobisation des importations
    df_mirror = Fobizer(config).transform(df_mirror, cif_rate)

    # Étape 4 — qualité de déclaration (valeurs puis quantités)
    quality_model = ReportingQualityModel(config)
    quality_value = quality_model.fit(df_mirror, target="value")
    quality_qty = quality_model.fit(df_mirror, target="quantity")

    # Étape 5 — réconciliation des flux miroirs
    reconciled = MirrorReconciler(config).transform(df_mirror, quality_value, quality_qty)

    # Étape 6 — réallocation des zones non spécifiées (optionnelle)
    if apply_nes:
        reconciled = AreaNesReallocator(config).transform(reconciled, df_mirror, df_nes)

    # Nettoyage : flux réconciliés exploitables (valeur non manquante)
    reconciled = reconciled[reconciled["reconciled_value"].notna()].reset_index(drop=True)
    if reconciled.empty:
        raise ValueError("No reconciled flow produced")

    report = BaciReport(flows=len(reconciled), mean_freight_rate=mean_freight_rate)

    return reconciled, report
