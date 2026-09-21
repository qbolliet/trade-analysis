"""Synthetic UN Comtrade tariffline rows and a network-free client.

Builds, from the "true" flows of a :class:`~kedro_pipeline.synthetic.world.SyntheticWorld`,
what the reporters would declare to UN Comtrade: export declarations (valued FOB)
and import declarations (valued CIF, or FOB for a few reporters), each with its own
reporting noise, possibly missing, plus "Areas, nes" and "World" lines. Mirror
flows therefore disagree the way real ones do, which is what the BACI
reconstruction is designed to reconcile.

:class:`SyntheticComtradeClient` plugs this into the unchanged download machinery
of ``statflows``: only the HTTP call that fetches tariffline rows is replaced, so
query planning, primary keys, upsert and the download registry behave exactly as
for a real download.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Modules de manipulation de données
import numpy as np
import pandas as pd

# Client Comtrade dont seul l'appel réseau des données est remplacé
from statflows import ComtradeClient

from kedro_pipeline.synthetic.io import align_to_dtypes
from kedro_pipeline.synthetic.world import (
    SyntheticWorld,
    _TAG_REPORTER_YEAR,
    _TAG_REPORTING,
    _by_prefix,
)

# Initialisation du logger
logger = logging.getLogger(__name__)

# Codes Comtrade des agrégats de partenaires
WORLD_CODE, WORLD_ISO, WORLD_DESC = 0, "W00", "World"
NES_CODE, NES_ISO, NES_DESC = 899, "X1", "Areas, nes"
# Libellés des unités de quantité (code → abréviation)
_UNIT_ABBR = {8: "kg", 5: "u", -1: "N/A"}
# Ordre des colonnes d'une ligne tariffline
TARIFFLINE_COLUMNS = (
    "typeCode", "freqCode", "refPeriodId", "refYear", "refMonth", "period",
    "reporterCode", "reporterDesc", "reporterISO",
    "flowCode", "flowDesc",
    "partnerCode", "partnerDesc", "partnerISO",
    "partner2Code", "partner2Desc", "partner2ISO",
    "classificationCode", "cmdCode", "cmdDesc",
    "customsCode", "customsDesc", "mosCode", "motCode", "motDesc",
    "qtyUnitCode", "qtyUnitAbbr", "altQtyUnitCode", "altQtyUnitAbbr",
    "qty", "altQty", "netWgt", "grossWgt", "cifvalue", "fobvalue", "primaryValue",
    "isReported", "isAggregate",
)


# Classe des pays de référence (codes M49, ISO2, libellés)
@dataclass(frozen=True)
class CountryReference:
    """Codes and labels of the simulated countries, from the Comtrade codelist.

    Attributes:
        table: DataFrame indexed by ISO3 with columns ``m49`` (int), ``iso2``
            and ``name``.
    """

    table: pd.DataFrame

    # Construction depuis la codelist de référence `reporter` de Comtrade
    @classmethod
    def from_codelist(cls, codelist: pd.DataFrame, iso3_codes: Sequence[str]) -> "CountryReference":
        """Build the reference of ``iso3_codes`` from the Comtrade ``reporter`` codelist.

        Args:
            codelist: Output of ``ComtradeClient.get_metadata("reporter")``
                (columns ``reporterCode``, ``reporterDesc``,
                ``reporterCodeIsoAlpha2``, ``reporterCodeIsoAlpha3``).
            iso3_codes: ISO3 codes of the simulated universe.

        Returns:
            The country reference.

        Raises:
            ValueError: If some ISO3 codes are absent from the codelist.

        Examples:
            >>> codelist = pd.DataFrame({
            ...     "reporterCode": [251], "reporterDesc": ["France"],
            ...     "reporterCodeIsoAlpha2": ["FR"], "reporterCodeIsoAlpha3": ["FRA"]})
            >>> CountryReference.from_codelist(codelist, ["FRA"]).m49("FRA")
            251
        """
        table = (
            codelist.rename(
                columns={
                    "reporterCode": "m49",
                    "reporterDesc": "name",
                    "reporterCodeIsoAlpha2": "iso2",
                    "reporterCodeIsoAlpha3": "iso3",
                }
            )[["m49", "name", "iso2", "iso3"]]
            .drop_duplicates("iso3")
            .set_index("iso3")
        )
        missing = sorted(set(iso3_codes) - set(table.index))
        if missing:
            raise ValueError(f"ISO3 codes absent from the Comtrade reporter codelist: {missing}")
        table = table.loc[list(iso3_codes)].copy()
        table["m49"] = table["m49"].astype(int)
        return cls(table)

    # Code M49 d'un pays
    def m49(self, iso3: str) -> int:
        """Return the M49 numeric code of a country."""
        return int(self.table.at[iso3, "m49"])

    # Code ISO2 d'un pays
    def iso2(self, iso3: str) -> str:
        """Return the ISO2 code of a country."""
        return str(self.table.at[iso3, "iso2"])


# Classe des paramètres de déclaration
@dataclass(frozen=True)
class ReportingConfig:
    """Reporting-error parameters (``REPORTING`` section of the YAML).

    Attributes:
        report_probability: Probability that a declaration exists.
        reporter_missing_year: Probability that a reporter misses a whole year.
        value_noise_sigma: Relative std of the declared value noise.
        weight_noise_sigma: Relative std of the declared weight noise.
        cif_markup_mean: Mean relative CIF markup of import declarations.
        cif_markup_sigma: Std of the CIF markup.
        fob_import_reporters: ISO3 of reporters valuing imports FOB.
        no_quantity_share: Share of declarations without quantity.
        nes_share: Share of a reporter's total booked under "Areas, nes".
        world_rows: Whether to emit "World" aggregate lines.
        classification_code: Declared HS classification code (e.g. ``"H5"``).
        quantity_unit_default: Default quantity-unit code.
        quantity_unit_by_prefix: Quantity-unit code by HS prefix.
    """

    report_probability: float = 0.9
    reporter_missing_year: float = 0.03
    value_noise_sigma: float = 0.06
    weight_noise_sigma: float = 0.10
    cif_markup_mean: float = 0.06
    cif_markup_sigma: float = 0.02
    fob_import_reporters: Tuple[str, ...] = ()
    no_quantity_share: float = 0.1
    nes_share: float = 0.02
    world_rows: bool = True
    classification_code: str = "H5"
    quantity_unit_default: int = 8
    quantity_unit_by_prefix: Mapping[str, int] = field(default_factory=dict)

    # Construction depuis la section `REPORTING` du fichier YAML
    @classmethod
    def from_mapping(cls, section: Mapping[str, Any]) -> "ReportingConfig":
        """Build the configuration from the parsed ``REPORTING`` YAML section.

        Args:
            section: Mapping under the ``REPORTING`` key.

        Returns:
            The reporting configuration (missing keys keep their defaults).

        Examples:
            >>> ReportingConfig.from_mapping({"NES_SHARE": 0.0}).nes_share
            0.0
        """
        units = section.get("QUANTITY_UNIT") or {}
        return cls(
            report_probability=float(section.get("REPORT_PROBABILITY", cls.report_probability)),
            reporter_missing_year=float(
                section.get("REPORTER_MISSING_YEAR", cls.reporter_missing_year)
            ),
            value_noise_sigma=float(section.get("VALUE_NOISE_SIGMA", cls.value_noise_sigma)),
            weight_noise_sigma=float(section.get("WEIGHT_NOISE_SIGMA", cls.weight_noise_sigma)),
            cif_markup_mean=float(section.get("CIF_MARKUP_MEAN", cls.cif_markup_mean)),
            cif_markup_sigma=float(section.get("CIF_MARKUP_SIGMA", cls.cif_markup_sigma)),
            fob_import_reporters=tuple(section.get("FOB_IMPORT_REPORTERS") or ()),
            no_quantity_share=float(section.get("NO_QUANTITY_SHARE", cls.no_quantity_share)),
            nes_share=float(section.get("NES_SHARE", cls.nes_share)),
            world_rows=bool(section.get("WORLD_ROWS", cls.world_rows)),
            classification_code=str(section.get("CLASSIFICATION_CODE", cls.classification_code)),
            quantity_unit_default=int(units.get("DEFAULT", cls.quantity_unit_default)),
            quantity_unit_by_prefix={
                str(k): int(v) for k, v in (units.get("BY_PREFIX") or {}).items()
            },
        )


# Fonction de construction des déclarations d'un produit et d'une année
def _declarations(
    world: SyntheticWorld,
    reporting: ReportingConfig,
    product: str,
    year: int,
) -> Dict[str, np.ndarray]:
    """Draw the declarations (exports and imports) of one product-year.

    Args:
        world: Simulated world.
        reporting: Reporting-error parameters.
        product: HS6 product code (digits).
        year: Simulated year.

    Returns:
        Dict of aligned arrays: ``reporter`` and ``partner`` (indices into
        ``world.iso3``), ``flow`` (``"M"``/``"X"``), ``cif``, ``fob``,
        ``netwgt`` and ``qty`` values, and ``unit`` (quantity-unit code).
    """
    n = len(world.iso3)
    value, weight = world.matrices(product, year)
    exporter, importer = np.nonzero(value)
    true_value, true_weight = value[exporter, importer], weight[exporter, importer]
    n_flows = len(true_value)

    # Tirages du couple produit-année, dans un ordre fixe (reproductibilité)
    rng = world.rng(_TAG_REPORTING, product, year)
    report_x = rng.random(n_flows) < reporting.report_probability
    report_m = rng.random(n_flows) < reporting.report_probability
    value_noise_x = reporting.value_noise_sigma * rng.standard_normal(n_flows)
    value_noise_m = reporting.value_noise_sigma * rng.standard_normal(n_flows)
    weight_noise_x = reporting.weight_noise_sigma * rng.standard_normal(n_flows)
    weight_noise_m = reporting.weight_noise_sigma * rng.standard_normal(n_flows)
    markup = np.clip(
        reporting.cif_markup_mean + reporting.cif_markup_sigma * rng.standard_normal(n_flows),
        0.0,
        None,
    )
    no_qty_x = rng.random(n_flows) < reporting.no_quantity_share
    no_qty_m = rng.random(n_flows) < reporting.no_quantity_share

    # Déclarants absents une année entière (indépendants du produit)
    present = (
        world.rng(_TAG_REPORTER_YEAR, "0", year).random(n) >= reporting.reporter_missing_year
    )

    # Valorisation des importations : FOB pour certains déclarants, CIF sinon
    fob_importers = np.isin(world.iso3[importer], list(reporting.fob_import_reporters))
    export_value = true_value * (1 + value_noise_x)
    import_value = true_value * (1 + value_noise_m)
    import_cif = np.where(fob_importers, 0.0, import_value * (1 + markup))
    import_fob = np.where(fob_importers, import_value, 0.0)

    # Quantité : unité déclarée du produit, ou absence de quantité
    unit = int(_by_prefix(product, reporting.quantity_unit_default, reporting.quantity_unit_by_prefix))
    factor = world.items_per_kg(product) if unit == 5 else 1.0

    sel_x = report_x & present[exporter]
    sel_m = report_m & present[importer]
    weight_x = true_weight[sel_x] * (1 + weight_noise_x[sel_x])
    weight_m = true_weight[sel_m] * (1 + weight_noise_m[sel_m])
    n_x, n_m = int(sel_x.sum()), int(sel_m.sum())
    return {
        "reporter": np.concatenate([exporter[sel_x], importer[sel_m]]),
        "partner": np.concatenate([importer[sel_x], exporter[sel_m]]),
        "flow": np.array(["X"] * n_x + ["M"] * n_m, dtype="U1"),
        "cif": np.concatenate([np.zeros(n_x), import_cif[sel_m]]),
        "fob": np.concatenate([export_value[sel_x], import_fob[sel_m]]),
        "netwgt": np.concatenate([weight_x, weight_m]),
        "qty": np.concatenate(
            [
                np.where(no_qty_x[sel_x], 0.0, weight_x * factor),
                np.where(no_qty_m[sel_m], 0.0, weight_m * factor),
            ]
        ),
        "unit": np.concatenate(
            [
                np.where(no_qty_x[sel_x], -1, unit),
                np.where(no_qty_m[sel_m], -1, unit),
            ]
        ),
    }


# Fonction d'ajout des lignes « Areas, nes » et « Monde »
def _with_aggregates(
    rows: pd.DataFrame, reporting: ReportingConfig, rng: np.random.Generator
) -> pd.DataFrame:
    """Append the "Areas, nes" and "World" lines of each reporter-flow.

    Args:
        rows: Declarations with numeric partner index in ``partner`` (-1 unused).
        reporting: Reporting-error parameters.
        rng: Generator used to pick the reporter-flows that book a "nes" line.

    Returns:
        ``rows`` extended with the aggregate lines (partner index ``-2`` for
        "Areas, nes" and ``-3`` for "World").
    """
    if rows.empty:
        return rows
    value_cols = ["cif", "fob", "netwgt", "qty"]
    totals = rows.groupby(["reporter", "flow"], as_index=False)[value_cols].sum()
    extra: List[pd.DataFrame] = []

    # « Areas, nes » : une part du total d'une partie des déclarants-flux
    if reporting.nes_share > 0:
        chosen = totals[rng.random(len(totals)) < 0.5].copy()
        chosen[value_cols] = chosen[value_cols] * reporting.nes_share
        chosen["partner"], chosen["unit"] = -2, -1
        chosen["qty"] = 0.0
        extra.append(chosen)

    # « Monde » : somme de toutes les lignes déclarées, « nes » incluses
    if reporting.world_rows:
        base = pd.concat([totals[["reporter", "flow", *value_cols]]] + [
            e[["reporter", "flow", *value_cols]] for e in extra
        ])
        world_rows = base.groupby(["reporter", "flow"], as_index=False)[value_cols].sum()
        world_rows["partner"], world_rows["unit"] = -3, -1
        world_rows["qty"] = 0.0
        extra.append(world_rows)

    return pd.concat([rows, *extra], ignore_index=True)


# Fonction de construction des lignes tariffline d'une période
def build_tariffline(
    world: SyntheticWorld,
    reference: CountryReference,
    reporting: ReportingConfig,
    year: int,
    products: Sequence[str],
    *,
    reporters: Optional[Sequence[int]] = None,
    partners: Optional[Sequence[int]] = None,
    flows: Sequence[str] = ("M", "X"),
    product_labels: Optional[Mapping[str, str]] = None,
) -> pd.DataFrame:
    """Build the tariffline rows declared for some products in a year.

    Args:
        world: Simulated world.
        reference: Codes and labels of the simulated countries.
        reporting: Reporting-error parameters.
        year: Simulated year.
        products: HS6 product codes (digits).
        reporters: M49 codes of the reporters to keep (``None`` = all).
        partners: M49 codes of the partners to keep (``None`` = all).
        flows: Flow codes to keep (``"M"``, ``"X"``).
        product_labels: Optional ``code -> label`` mapping for ``cmdDesc``.

    Returns:
        DataFrame with the columns of :data:`TARIFFLINE_COLUMNS` (empty when the
        year is outside the simulated range or nothing is declared).

    Examples:
        >>> # Voir tests/test_synthetic_comtrade.py
    """
    if year not in world.config.years:
        return pd.DataFrame(columns=list(TARIFFLINE_COLUMNS))
    labels = product_labels or {}
    iso3 = world.iso3
    m49 = np.array([reference.m49(c) for c in iso3])
    names = [str(reference.table.at[c, "name"]) for c in iso3]

    frames: List[pd.DataFrame] = []
    for product in products:
        declared = _declarations(world, reporting, product, year)
        rows = pd.DataFrame(declared)
        rng = world.rng(_TAG_REPORTING + 100, product, year)
        rows = _with_aggregates(rows, reporting, rng)
        if rows.empty:
            continue
        rows["product"] = product
        frames.append(rows)
    if not frames:
        return pd.DataFrame(columns=list(TARIFFLINE_COLUMNS))
    rows = pd.concat(frames, ignore_index=True)

    # Codes numériques du déclarant et du partenaire (indices négatifs = agrégats)
    reporter_index = rows["reporter"].to_numpy()
    partner_index = rows["partner"].to_numpy()
    reporter_code = m49[reporter_index]
    partner_code = np.select(
        [partner_index == -2, partner_index == -3],
        [NES_CODE, WORLD_CODE],
        default=m49[np.clip(partner_index, 0, None)],
    )

    # Filtres de la requête (flux, déclarants, partenaires), en un seul masque
    keep = rows["flow"].isin(list(flows)).to_numpy()
    if reporters is not None:
        keep &= np.isin(reporter_code, list(reporters))
    if partners is not None:
        keep &= np.isin(partner_code, list(partners))
    if not keep.any():
        return pd.DataFrame(columns=list(TARIFFLINE_COLUMNS))
    rows = rows[keep]
    reporter_index, partner_index = reporter_index[keep], partner_index[keep]
    reporter_code, partner_code = reporter_code[keep], partner_code[keep]

    is_world = partner_index == -3
    unit = rows["unit"].to_numpy().astype(int)
    primary = np.where(rows["cif"] > 0, rows["cif"], rows["fob"])
    partner_desc = np.array(
        [
            NES_DESC if i == -2 else WORLD_DESC if i == -3 else names[i]
            for i in partner_index
        ]
    )

    out = pd.DataFrame(
        {
            "typeCode": "C",
            "freqCode": "A",
            "refPeriodId": int(f"{year}0101"),
            "refYear": year,
            "refMonth": 52,
            "period": year,
            "reporterCode": reporter_code.astype("int64"),
            "reporterDesc": [names[i] for i in reporter_index],
            "reporterISO": iso3[reporter_index],
            "flowCode": rows["flow"].to_numpy(),
            "flowDesc": np.where(rows["flow"] == "M", "Import", "Export"),
            "partnerCode": partner_code.astype("int64"),
            "partnerDesc": partner_desc,
            "partnerISO": np.select(
                [partner_index == -2, is_world],
                [NES_ISO, WORLD_ISO],
                default=iso3[np.clip(partner_index, 0, None)],
            ),
            "partner2Code": 0,
            "partner2Desc": WORLD_DESC,
            "partner2ISO": WORLD_ISO,
            "classificationCode": reporting.classification_code,
            "cmdCode": rows["product"].to_numpy(),
            "cmdDesc": [labels.get(p, "") for p in rows["product"]],
            "customsCode": "C00",
            "customsDesc": "TOTAL CPC",
            "mosCode": "0",
            "motCode": 0,
            "motDesc": "TOTAL MOT",
            "qtyUnitCode": unit,
            "qtyUnitAbbr": [_UNIT_ABBR.get(u, "N/A") for u in unit],
            "altQtyUnitCode": -1,
            "altQtyUnitAbbr": "N/A",
            "qty": rows["qty"].to_numpy(),
            "altQty": 0.0,
            "netWgt": rows["netwgt"].to_numpy(),
            "grossWgt": rows["netwgt"].to_numpy() * 1.02,
            "cifvalue": rows["cif"].to_numpy(),
            "fobvalue": rows["fob"].to_numpy(),
            "primaryValue": primary,
            "isReported": ~is_world,
            "isAggregate": is_world,
        }
    )
    return out[list(TARIFFLINE_COLUMNS)].reset_index(drop=True)


# Fonction d'analyse d'une liste de codes transmise à l'API
def _parse_codes(value: Optional[str]) -> Optional[List[str]]:
    """Split a comma-separated code string (``None`` stays ``None``).

    Args:
        value: Comma-separated codes as prepared by ``ComtradeClient``.

    Returns:
        List of stripped codes, or ``None``.

    Examples:
        >>> _parse_codes("251, 276")
        ['251', '276']
        >>> _parse_codes(None) is None
        True
    """
    if value is None:
        return None
    return [code.strip() for code in str(value).split(",") if code.strip()]


# Client Comtrade fictif
class SyntheticComtradeClient(ComtradeClient):
    """``ComtradeClient`` whose tariffline calls are answered by the simulated world.

    Only the network call fetching tariffline rows is replaced: reference
    codelists (``get_metadata``), valid periods, structures, primary keys and the
    aggregation of duplicates stay those of the real client, so the download
    script and the BACI completeness gate run unchanged.

    Args:
        world: Simulated world.
        reference: Codes and labels of the simulated countries.
        reporting: Reporting-error parameters.
        product_labels: Optional ``code -> label`` mapping for ``cmdDesc``.
        product_universe: HS6 codes returned for a query that does not restrict
            the products (``None`` = every product of the codelist); without it
            such a query is refused.
        force: When ``True``, queries already downloaded are regenerated too
            (otherwise a second run is a no-op).
        table_dtypes: Column → dtype of the existing fact table, when there is
            one: the generated rows are aligned on it before being written.
        **kwargs: Forwarded to ``ComtradeClient`` (``subscription_key`` is
            forced to ``None``: no API key is ever needed).
    """

    # Initialisation
    def __init__(
        self,
        world: SyntheticWorld,
        reference: CountryReference,
        reporting: ReportingConfig,
        *,
        product_labels: Optional[Mapping[str, str]] = None,
        product_universe: Optional[Sequence[str]] = None,
        force: bool = False,
        table_dtypes: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        kwargs["subscription_key"] = None
        super().__init__(**kwargs)
        self._world = world
        self._reference = reference
        self._reporting = reporting
        self._labels = dict(product_labels or {})
        self._product_universe = list(product_universe) if product_universe is not None else None
        self._force = force
        self._table_dtypes = table_dtypes

    # Appel de données remplacé : réponse du monde simulé, période par période
    def _fetch_tariffline(
        self, api_kwargs: Dict[str, Optional[str]], **params: Any
    ) -> Tuple[pd.DataFrame, bool]:
        """Answer a tariffline request from the simulated world.

        Args:
            api_kwargs: Flow dimensions keyed by API argument name, as built by
                ``ComtradeClient.get_data``.
            **params: Ignored request parameters (type, frequency…).

        Returns:
            Tuple ``(DataFrame, truncated)``; never truncated.
        """
        periods = _parse_codes(api_kwargs.get("period")) or []
        products = _parse_codes(api_kwargs.get("cmdCode"))
        reporters = _parse_codes(api_kwargs.get("reporterCode"))
        partners = _parse_codes(api_kwargs.get("partnerCode"))
        flows = _parse_codes(api_kwargs.get("flowCode")) or ["M", "X"]
        if products is None:
            # Requête sans restriction de produits : tout l'univers HS6 déclaré
            if self._product_universe is None:
                raise ValueError(
                    "Unrestricted product selection: pass product_universe to the synthetic client"
                )
            products = self._product_universe
        frames = [
            build_tariffline(
                self._world, self._reference, self._reporting, int(str(period)[:4]), products,
                reporters=[int(r) for r in reporters] if reporters is not None else None,
                partners=[int(p) for p in partners] if partners is not None else None,
                flows=flows, product_labels=self._labels,
            )
            for period in periods
        ]
        frames = [frame for frame in frames if not frame.empty]
        if not frames:
            return pd.DataFrame(), False
        return pd.concat(frames, ignore_index=True), False

    # Téléchargement incrémental : une requête déjà écrite n'est pas régénérée
    def fetch_updates(
        self, query: Any, since: Optional[datetime], n_observations: int = 10
    ) -> pd.DataFrame:
        """Generate a query, or skip it when it was already downloaded.

        Args:
            query: Comtrade query request.
            since: Instant of the previous download, ``None`` if never downloaded.
            n_observations: Ignored (interface conformity).

        Returns:
            The generated rows, empty for an already downloaded query unless
            ``force`` is set.
        """
        if since is not None and not self._force:
            return pd.DataFrame()
        return align_to_dtypes(self.execute_query(query), self._table_dtypes)
