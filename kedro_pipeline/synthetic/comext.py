"""Synthetic Eurostat Comext rows, to complete a slow or partial download.

Builds, from the "true" flows of a :class:`~kedro_pipeline.synthetic.world.SyntheticWorld`,
the rows a Comext (DS-045409) query would return for one reporter and one product:
partner-level values and quantities plus the ``WORLD`` / extra-EU / intra-EU
aggregates the vulnerability metrics divide by.

The column set and the conventions (data types, aggregate partner codes, extra
columns) are **learned from the rows already present in the catalog**
(:class:`ComextTemplate`) so that the synthetic rows are upserted into the very
same table without any schema guess; an empty table falls back to the dimensions
of the query itself.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence

# Modules de manipulation de données
import numpy as np
import pandas as pd

from kedro_pipeline.synthetic.world import SyntheticWorld

# Initialisation du logger
logger = logging.getLogger(__name__)

# Étiquette de la graine de répartition d'un code SH6 entre ses lignes NC8
_TAG_NC8 = 7
# Nom des colonnes de temps et de valeur d'une observation Comext
TIME_COLUMN = "TIME_PERIOD"
VALUE_COLUMN = "OBS_VALUE"
# Colonnes observées d'une table Comext qui ne sont pas des dimensions constantes
_OBSERVATION_COLUMNS = (TIME_COLUMN, VALUE_COLUMN)


# Classe des paramètres du complément Comext
@dataclass(frozen=True)
class ComextConfig:
    """Parameters of the Comext completion (``COMEXT`` section of the YAML).

    Attributes:
        usd_to_eur: USD → EUR exchange rate applied to values.
        world_prefix: Prefix of the "all partners" aggregate code.
        extra_eu_prefix: Prefix of the extra-EU aggregate code.
        intra_eu_prefix: Prefix of the intra-EU aggregate code.
        union_reporter: Code of the aggregated "European Union" reporter.
        eu_region: Region label of the member states in the world universe.
        products_per_write: Products written per DuckLake commit.
        nc8_share_min: Lower bound of the share of an HS6 code carried by one
            of its NC8 lines.
    """

    usd_to_eur: float = 0.92
    world_prefix: str = "WORLD"
    extra_eu_prefix: str = "EXT_EU"
    intra_eu_prefix: str = "INT_EU"
    union_reporter: str = "EU27_2020"
    eu_region: str = "EU"
    products_per_write: int = 5
    nc8_share_min: float = 0.2

    # Construction depuis la section `COMEXT` du fichier YAML
    @classmethod
    def from_mapping(cls, section: Mapping[str, Any]) -> "ComextConfig":
        """Build the configuration from the parsed ``COMEXT`` YAML section.

        Args:
            section: Mapping under the ``COMEXT`` key.

        Returns:
            The configuration (missing keys keep their defaults).

        Examples:
            >>> ComextConfig.from_mapping({"AGGREGATE_PREFIXES": {"WORLD": "W"}}).world_prefix
            'W'
        """
        prefixes = section.get("AGGREGATE_PREFIXES") or {}
        return cls(
            usd_to_eur=float(section.get("USD_TO_EUR", cls.usd_to_eur)),
            world_prefix=str(prefixes.get("WORLD", cls.world_prefix)),
            extra_eu_prefix=str(prefixes.get("EXTRA_EU", cls.extra_eu_prefix)),
            intra_eu_prefix=str(prefixes.get("INTRA_EU", cls.intra_eu_prefix)),
            union_reporter=str(section.get("UNION_REPORTER", cls.union_reporter)),
            eu_region=str(section.get("EU_REGION", cls.eu_region)),
            products_per_write=int(section.get("PRODUCTS_PER_WRITE", cls.products_per_write)),
            nc8_share_min=float(section.get("NC8_SHARE_MIN", cls.nc8_share_min)),
        )


# Classe du modèle de table appris sur les lignes existantes
@dataclass(frozen=True)
class ComextTemplate:
    """Schema and code conventions learned from existing Comext rows.

    Attributes:
        dtypes: Column → pandas dtype of the existing table (empty when the
            table has no row yet).
        constants: Values of the non-dimension extra columns (copied from the
            first existing row), e.g. dataflow label or observation flags.
        partner_codes: Partner codes present in the table.
    """

    dtypes: Mapping[str, Any] = field(default_factory=dict)
    constants: Mapping[str, Any] = field(default_factory=dict)
    partner_codes: Sequence[str] = ()

    # Construction depuis un échantillon de la table existante
    @classmethod
    def from_sample(
        cls, sample: Optional[pd.DataFrame], partner_codes: Sequence[str] = ()
    ) -> "ComextTemplate":
        """Learn the template from a sample of the existing fact table.

        Args:
            sample: Some rows of the table (``None`` or empty when absent).
            partner_codes: Distinct partner codes of the table.

        Returns:
            The template (empty when there is no sample).

        Examples:
            >>> ComextTemplate.from_sample(None).dtypes
            {}
        """
        if sample is None or sample.empty:
            return cls(partner_codes=tuple(partner_codes))
        # Colonnes hors dimensions de découpage : constantes recopiées de la 1re ligne
        first = sample.iloc[0]
        constants = {
            col: first[col]
            for col in sample.columns
            if col not in _OBSERVATION_COLUMNS
        }
        return cls(
            dtypes={col: sample[col].dtype for col in sample.columns},
            constants=constants,
            partner_codes=tuple(partner_codes),
        )

    # Code d'agrégat réellement utilisé dans la table
    def aggregate_code(self, prefix: str) -> str:
        """Return the aggregate partner code starting with ``prefix``.

        Args:
            prefix: Configured prefix (e.g. ``"EXT_EU"``).

        Returns:
            The table's own code when one starts with ``prefix`` (the shortest
            wins), else ``prefix`` itself.

        Examples:
            >>> ComextTemplate(partner_codes=("EXT_EU27_2020", "CN")).aggregate_code("EXT_EU")
            'EXT_EU27_2020'
            >>> ComextTemplate().aggregate_code("WORLD")
            'WORLD'
        """
        matches = sorted(
            (code for code in self.partner_codes if str(code).startswith(prefix)), key=len
        )
        return str(matches[0]) if matches else prefix


# Fonction de mise en liste d'une valeur de dimension
def _as_list(value: Any) -> List[Any]:
    """Return a dimension value as a list (scalar → singleton, ``None`` → empty).

    Args:
        value: Scalar, list, tuple or ``None``.

    Returns:
        A list.

    Examples:
        >>> _as_list("A"), _as_list(["1", "2"]), _as_list(None)
        (['A'], ['1', '2'], [])
    """
    if value is None:
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]


# Fonction de calcul de la part NC8 d'un code produit
def _nc8_share(world: SyntheticWorld, product: str, minimum: float) -> float:
    """Share of the HS6 flows carried by a (possibly longer) product code.

    Args:
        world: Simulated world (provides the seed).
        product: Comext product code (HS6, or NC8 / longer).
        minimum: Lower bound of the share for a code longer than HS6.

    Returns:
        1.0 for an HS6 code, else a deterministic share in ``[minimum, 1]``.

    Examples:
        >>> # Un code SH6 porte la totalité des flux
        >>> _nc8_share(None, "810510", 0.2)
        1.0
    """
    if len(product) <= 6:
        return 1.0
    draw = world.rng(_TAG_NC8, product).random()
    return float(minimum + (1.0 - minimum) * draw)


# Fonction de construction des lignes Comext d'un reporter et d'un produit
def build_comext(
    world: SyntheticWorld,
    iso2_by_iso3: Mapping[str, str],
    config: ComextConfig,
    template: ComextTemplate,
    *,
    reporter: str,
    product: str,
    dimensions: Mapping[str, Any],
    start_period: Optional[str] = None,
) -> pd.DataFrame:
    """Build the Comext rows of one reporter and one product (every year).

    Args:
        world: Simulated world.
        iso2_by_iso3: ISO3 → ISO2 mapping of the universe (Comext partner codes).
        config: Completion parameters.
        template: Schema and code conventions of the existing table.
        reporter: Comext reporter code (an ISO2 member state, or the union code).
        product: Comext product code (HS6 or NC8).
        dimensions: Fixed dimensions of the query (``freq``, ``partner``,
            ``flow``, ``indicators``…); lists are expanded.
        start_period: First year kept (``None`` = every simulated year).

    Returns:
        DataFrame of observations (one row per partner, flow, indicator and
        year with a positive value), with the template's columns and dtypes;
        empty when the reporter is unknown or nothing is traded.

    Examples:
        >>> # Voir tests/test_synthetic_comext.py
    """
    iso3 = world.iso3
    iso2 = np.array([iso2_by_iso3[c] for c in iso3])
    region_eu = np.array([c.region == config.eu_region for c in world.config.countries])
    is_union = reporter == config.union_reporter
    if not is_union and reporter not in set(iso2):
        return pd.DataFrame()

    hs6 = product[:6]
    share = _nc8_share(world, product, config.nc8_share_min)
    first_year = int(start_period) if start_period else world.config.year_start
    years = [y for y in world.config.years if y >= first_year]

    flows = _as_list(dimensions.get("flow"))
    indicators = _as_list(dimensions.get("indicators"))
    world_code = template.aggregate_code(config.world_prefix)
    extra_code = template.aggregate_code(config.extra_eu_prefix)
    intra_code = template.aggregate_code(config.intra_eu_prefix)

    # Observations accumulées par listes de colonnes (une seule construction finale)
    partner_col: List[str] = []
    flow_col: List[int] = []
    measure_col: List[str] = []
    year_col: List[int] = []
    amount_col: List[float] = []
    for year in years:
        value, weight = world.matrices(hs6, year)
        for measure_name, matrix in (("value", value * config.usd_to_eur), ("weight", weight / 100.0)):
            # Flux du reporter : importations (colonne = reporter) et exportations (ligne)
            if is_union:
                # Union : somme des États membres, vis-à-vis des seuls partenaires hors Union
                imports = matrix[:, region_eu].sum(axis=1)
                exports = matrix[region_eu, :].sum(axis=0)
                partners = ~region_eu
            else:
                index = int(np.flatnonzero(iso2 == reporter)[0])
                imports, exports = matrix[:, index], matrix[index, :]
                partners = np.arange(len(iso3)) != index
            for flow_code, series in ((1, imports), (2, exports)):
                vals = np.where(partners, series, 0.0) * share
                if vals.sum() <= 0:
                    continue
                # Partenaires individuels, puis agrégats (Monde, hors UE, intra-UE)
                codes = [str(c) for c in iso2[vals > 0]]
                amounts = [float(v) for v in vals[vals > 0]]
                codes += [world_code, extra_code]
                amounts += [float(vals.sum()), float(vals[~region_eu].sum())]
                if not is_union:
                    codes.append(intra_code)
                    amounts.append(float(vals[region_eu].sum()))
                keep = [k for k, amount in enumerate(amounts) if amount > 0]
                partner_col += [codes[k] for k in keep]
                amount_col += [amounts[k] for k in keep]
                flow_col += [flow_code] * len(keep)
                measure_col += [measure_name] * len(keep)
                year_col += [year] * len(keep)
    if not partner_col:
        return pd.DataFrame()
    frames = pd.DataFrame(
        {
            "partner": partner_col,
            "flow": flow_col,
            "measure": measure_col,
            TIME_COLUMN: year_col,
            VALUE_COLUMN: amount_col,
        }
    )
    return _assemble(frames, reporter, product, dimensions, flows, indicators, template)


# Fonction d'assemblage du DataFrame final aux conventions de la table
def _assemble(
    obs: pd.DataFrame,
    reporter: str,
    product: str,
    dimensions: Mapping[str, Any],
    flows: Sequence[Any],
    indicators: Sequence[Any],
    template: ComextTemplate,
) -> pd.DataFrame:
    """Turn raw observations into rows following the template's conventions.

    Args:
        obs: Raw observations (``partner``, ``flow`` 1/2, ``measure``
            value/weight, ``TIME_PERIOD``, ``OBS_VALUE``).
        reporter: Reporter code.
        product: Product code.
        dimensions: Fixed dimensions of the query.
        flows: Flow codes of the query, in the query's own spelling.
        indicators: Indicator codes of the query.
        template: Template learned from the existing table.

    Returns:
        Observations keyed by the dimensions of the query.
    """
    # Correspondance flux (1 = importation, 2 = exportation) → code de la requête
    flow_map = {1: next((f for f in flows if str(f) == "1"), 1),
                2: next((f for f in flows if str(f) == "2"), 2)}
    # Indicateur de la requête portant sur la valeur ou sur la quantité
    indicator_map = {
        "value": next((i for i in indicators if "VALUE" in str(i).upper()), None),
        "weight": next((i for i in indicators if "QUANTITY" in str(i).upper()), None),
    }
    obs = obs[obs["measure"].map(indicator_map).notna()].copy()
    obs["indicators"] = obs["measure"].map(indicator_map)
    obs["flow"] = obs["flow"].map(flow_map)
    obs["reporter"], obs["product"] = reporter, product
    # Dimensions fixes restantes (fréquence…), hors partenaire déjà renseigné
    for name, val in dimensions.items():
        if name not in obs.columns and not isinstance(val, list):
            obs[name] = val
    obs = obs.drop(columns=["measure"])

    # Colonnes du modèle : mêmes noms, mêmes types, constantes recopiées
    if template.dtypes:
        for col, dtype in template.dtypes.items():
            if col not in obs.columns:
                obs[col] = template.constants.get(col)
        obs = obs[list(template.dtypes)]
        for col, dtype in template.dtypes.items():
            try:
                obs[col] = obs[col].astype(dtype)
            except (TypeError, ValueError) as exc:
                logger.warning("Colonne %s non convertie en %s : %s", col, dtype, exc)
    else:
        obs[TIME_COLUMN] = obs[TIME_COLUMN].astype(str)
    return obs.reset_index(drop=True)
