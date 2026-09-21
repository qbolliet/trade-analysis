"""Simulated world of international trade (synthetic data).

Pure module (no I/O, no network, no Kedro): generates the "true" bilateral flows
of a fictitious world by product and year, from a gravity model whose parameters
all come from the configuration (``config/profiles/demo/synthetic.yaml``). The
reporting layers (UN Comtrade tariffline rows with mirror errors, Comext rows)
are built on top of these flows in :mod:`kedro_pipeline.synthetic.comtrade` and
:mod:`kedro_pipeline.synthetic.comext`.

Simulated data have **no statistical value**: they only exercise the
downstream steps of the pipeline.

Determinism: every random draw is keyed by ``(seed, tag, product[, year])``, so
the same configuration always yields the same flows, whatever the way queries
are batched.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Modules de manipulation de données
import numpy as np
import pandas as pd

# Étiquettes des flux de graines aléatoires (un flux indépendant par usage)
_TAG_STRUCTURE = 1
_TAG_REPORTING = 2
_TAG_REPORTER_YEAR = 3


# Classe décrivant un pays de l'univers simulé
@dataclass(frozen=True)
class CountrySpec:
    """A country of the simulated universe.

    Attributes:
        iso3: ISO 3166-1 alpha-3 code.
        size: Relative economic weight (any positive scale).
        region: Region label; trade between regions is penalised.
        iso2: ISO 3166-1 alpha-2 code (Comext partner code).
    """

    iso3: str
    size: float
    region: str
    iso2: str = ""


# Fonction de résolution d'un paramètre indexé par préfixe de code
def _by_prefix(
    product: str, default: Any, overrides: Mapping[str, Any]
) -> Any:
    """Resolve a per-product parameter from HS-prefix overrides.

    Args:
        product: Product code (HS6 or longer).
        default: Value used when no prefix matches.
        overrides: Mapping ``prefix -> value``; the longest matching prefix wins.

    Returns:
        The value of the longest prefix matching ``product``, else ``default``.

    Examples:
        >>> _by_prefix("810510", 1.2, {"8105": 2.8, "81": 2.0})
        2.8
        >>> _by_prefix("999999", 1.2, {"8105": 2.8})
        1.2
    """
    matches = [prefix for prefix in overrides if str(product).startswith(str(prefix))]
    if not matches:
        return default
    return overrides[max(matches, key=len)]


# Classe portant les paramètres du monde simulé
@dataclass(frozen=True)
class WorldConfig:
    """Parameters of the simulated world (``synthetic`` section of the YAML).

    Attributes:
        seed: Reproducibility seed.
        year_start: First simulated year (included).
        year_end: Last simulated year (included).
        countries: Universe of countries (reporters and partners).
        density: Probability that an exporter → importer pair trades a product.
        supply_size_exponent: Elasticity of supply to the exporter's size.
        demand_size_exponent: Elasticity of demand to the importer's size.
        region_penalty: Log-penalty of trade between different regions.
        pair_sigma: Std of the time-invariant pair effect (log).
        flow_sigma: Std of the yearly noise of each flow (log).
        supply_drift_sigma: Std of the yearly random walk of supply (log).
        trend_growth: Mean yearly growth of a product's world trade (log).
        year_shocks: Common yearly shocks (log), ``year -> shock``.
        world_trade_median: Median world trade of a product (USD).
        world_trade_sigma: Dispersion of world trade across products (log).
        unit_price_median: Median unit price (USD/kg).
        unit_price_sigma: Dispersion of unit prices across products (log).
        min_value: Minimal flow value (USD); thinner flows are dropped.
        concentration_default: Supply concentration when no prefix matches.
        concentration_by_prefix: Supply concentration by HS prefix.
        producer_share_default: Share of exporting countries by default.
        producer_share_by_prefix: Share of exporting countries by HS prefix.
        supplier_bias: Additive log-advantage of countries, by HS prefix.
    """

    seed: int
    year_start: int
    year_end: int
    countries: Tuple[CountrySpec, ...]
    density: float = 0.4
    supply_size_exponent: float = 0.8
    demand_size_exponent: float = 0.9
    region_penalty: float = 1.2
    pair_sigma: float = 0.3
    flow_sigma: float = 0.15
    supply_drift_sigma: float = 0.12
    trend_growth: float = 0.03
    year_shocks: Mapping[int, float] = field(default_factory=dict)
    world_trade_median: float = 1.5e8
    world_trade_sigma: float = 1.4
    unit_price_median: float = 6.0
    unit_price_sigma: float = 1.5
    min_value: float = 2000.0
    concentration_default: float = 1.2
    concentration_by_prefix: Mapping[str, float] = field(default_factory=dict)
    producer_share_default: float = 0.6
    producer_share_by_prefix: Mapping[str, float] = field(default_factory=dict)
    supplier_bias: Mapping[str, Mapping[str, float]] = field(default_factory=dict)

    # Construction depuis la section `synthetic` du fichier YAML
    @classmethod
    def from_mapping(cls, section: Mapping[str, Any]) -> "WorldConfig":
        """Build the configuration from the parsed ``synthetic`` YAML section.

        Args:
            section: Mapping under the ``synthetic`` root key (``SEED``,
                ``YEARS``, ``COUNTRIES``, ``MODEL``).

        Returns:
            The world configuration.

        Raises:
            KeyError: If a mandatory key is missing.
            ValueError: If the universe has fewer than two countries or the
                year range is empty.

        Examples:
            >>> cfg = WorldConfig.from_mapping({
            ...     "SEED": 1, "YEARS": {"START": 2020, "END": 2021},
            ...     "COUNTRIES": [{"iso3": "FRA", "size": 1.0, "region": "EU"},
            ...                   {"iso3": "DEU", "size": 2.0, "region": "EU"}],
            ...     "MODEL": {},
            ... })
            >>> cfg.years
            [2020, 2021]
        """
        model = section.get("MODEL") or {}
        concentration = model.get("CONCENTRATION") or {}
        producers = model.get("PRODUCER_SHARE") or {}
        countries = tuple(
            CountrySpec(
                str(c["iso3"]), float(c["size"]), str(c["region"]), str(c.get("iso2", ""))
            )
            for c in section["COUNTRIES"]
        )
        config = cls(
            seed=int(section["SEED"]),
            year_start=int(section["YEARS"]["START"]),
            year_end=int(section["YEARS"]["END"]),
            countries=countries,
            density=float(model.get("DENSITY", cls.density)),
            supply_size_exponent=float(
                model.get("SUPPLY_SIZE_EXPONENT", cls.supply_size_exponent)
            ),
            demand_size_exponent=float(
                model.get("DEMAND_SIZE_EXPONENT", cls.demand_size_exponent)
            ),
            region_penalty=float(model.get("REGION_PENALTY", cls.region_penalty)),
            pair_sigma=float(model.get("PAIR_SIGMA", cls.pair_sigma)),
            flow_sigma=float(model.get("FLOW_SIGMA", cls.flow_sigma)),
            supply_drift_sigma=float(
                model.get("SUPPLY_DRIFT_SIGMA", cls.supply_drift_sigma)
            ),
            trend_growth=float(model.get("TREND_GROWTH", cls.trend_growth)),
            year_shocks={int(y): float(s) for y, s in (model.get("YEAR_SHOCKS") or {}).items()},
            world_trade_median=float(
                model.get("WORLD_TRADE_MEDIAN", cls.world_trade_median)
            ),
            world_trade_sigma=float(
                model.get("WORLD_TRADE_SIGMA", cls.world_trade_sigma)
            ),
            unit_price_median=float(
                model.get("UNIT_PRICE_MEDIAN", cls.unit_price_median)
            ),
            unit_price_sigma=float(model.get("UNIT_PRICE_SIGMA", cls.unit_price_sigma)),
            min_value=float(model.get("MIN_VALUE", cls.min_value)),
            concentration_default=float(
                concentration.get("DEFAULT", cls.concentration_default)
            ),
            concentration_by_prefix={
                str(k): float(v) for k, v in (concentration.get("BY_PREFIX") or {}).items()
            },
            producer_share_default=float(
                producers.get("DEFAULT", cls.producer_share_default)
            ),
            producer_share_by_prefix={
                str(k): float(v) for k, v in (producers.get("BY_PREFIX") or {}).items()
            },
            supplier_bias={
                str(prefix): {str(iso): float(b) for iso, b in biases.items()}
                for prefix, biases in (model.get("SUPPLIER_BIAS") or {}).items()
            },
        )
        # Vérification de la cohérence des bornes et de l'univers
        if len(config.countries) < 2:
            raise ValueError("The simulated universe needs at least two countries")
        if config.year_end < config.year_start:
            raise ValueError("YEARS.END must not precede YEARS.START")
        return config

    # Années simulées
    @property
    def years(self) -> List[int]:
        """Simulated years, increasing."""
        return list(range(self.year_start, self.year_end + 1))


# Classe du monde simulé
class SyntheticWorld:
    """Fictitious world producing "true" bilateral flows by product and year.

    Flows follow a gravity model ``X_ij = K · supply_i · demand_j · resistance_ij``
    with a persistent existence mask, a supplier-specific concentration and
    yearly drifts; they are normalised to a world total per product and year.

    Args:
        config: World parameters.

    Examples:
        >>> from kedro_pipeline.synthetic.world import WorldConfig, SyntheticWorld
        >>> cfg = WorldConfig.from_mapping({
        ...     "SEED": 1, "YEARS": {"START": 2020, "END": 2021},
        ...     "COUNTRIES": [{"iso3": "FRA", "size": 3.0, "region": "EU"},
        ...                   {"iso3": "DEU", "size": 4.0, "region": "EU"},
        ...                   {"iso3": "CHN", "size": 18.0, "region": "ASIA"}],
        ...     "MODEL": {"DENSITY": 1.0, "MIN_VALUE": 0},
        ... })
        >>> flows = SyntheticWorld(cfg).flows("280519", 2020)
        >>> sorted(flows.columns)
        ['exporter', 'importer', 'product', 'value', 'weight', 'year']
        >>> bool((flows["exporter"] != flows["importer"]).all())
        True
    """

    # Initialisation
    def __init__(self, config: WorldConfig) -> None:
        self.config = config
        self._iso3 = np.array([c.iso3 for c in config.countries])
        self._size = np.array([c.size for c in config.countries], dtype=float)
        self._region = np.array([c.region for c in config.countries])
        # Cache des trajectoires simulées, une par produit (toutes années)
        self._paths: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}

    # Codes ISO3 des pays de l'univers, dans l'ordre des tableaux internes
    @property
    def iso3(self) -> np.ndarray:
        """ISO3 codes of the universe, in the order of the internal arrays."""
        return self._iso3

    # Générateur aléatoire indépendant, indexé par usage, produit et année
    def rng(self, tag: int, product: str, year: Optional[int] = None) -> np.random.Generator:
        """Return a deterministic generator keyed by ``(seed, tag, product, year)``.

        Args:
            tag: Usage tag (independent random streams).
            product: Product code (digits only).
            year: Year, or ``None`` for time-invariant draws.

        Returns:
            A NumPy ``Generator``.
        """
        key = [self.config.seed, tag, int(product)]
        if year is not None:
            key.append(int(year))
        return np.random.default_rng(key)

    # Simulation des trajectoires d'un produit sur toutes les années
    def _simulate(self, product: str) -> Tuple[np.ndarray, np.ndarray]:
        """Simulate the flow values and weights of a product over all years.

        Args:
            product: HS6 product code (digits).

        Returns:
            Tuple ``(value, weight)`` of arrays of shape ``(T, N, N)``
            (year, exporter, importer), value in USD, weight in kg. Zero where
            no flow exists.
        """
        cfg = self.config
        n_countries, years = len(self._iso3), cfg.years
        rng = self.rng(_TAG_STRUCTURE, product)

        # Paramètres du produit (préfixe de code le plus long)
        gamma = _by_prefix(product, cfg.concentration_default, cfg.concentration_by_prefix)
        producer_share = _by_prefix(
            product, cfg.producer_share_default, cfg.producer_share_by_prefix
        )
        bias = _by_prefix(product, {}, cfg.supplier_bias)

        # Pays exportateurs du produit : au moins deux, les biais désignent des producteurs
        is_producer = rng.random(n_countries) < producer_share
        for iso in bias:
            is_producer |= self._iso3 == iso
        if is_producer.sum() < 2:
            is_producer[rng.choice(n_countries, size=2, replace=False)] = True

        # Capacité d'exportation et demande (log)
        log_supply = (
            cfg.supply_size_exponent * np.log(self._size)
            + gamma * rng.standard_normal(n_countries)
            + np.array([bias.get(iso, 0.0) for iso in self._iso3])
        )
        log_demand = cfg.demand_size_exponent * np.log(self._size) + 0.5 * rng.standard_normal(
            n_countries
        )
        # Existence structurelle des flux et effet propre à chaque couple
        exists = rng.random((n_countries, n_countries)) < cfg.density
        np.fill_diagonal(exists, False)
        exists &= is_producer[:, None]
        pair_effect = cfg.pair_sigma * rng.standard_normal((n_countries, n_countries))
        # Résistance : pénalité d'un échange inter-régional
        resistance = -cfg.region_penalty * (self._region[:, None] != self._region[None, :])

        # Trajectoires annuelles : marche aléatoire de l'offre, bruit propre aux flux
        n_years = len(years)
        supply_path = log_supply[None, :] + cfg.supply_drift_sigma * np.cumsum(
            rng.standard_normal((n_years, n_countries)), axis=0
        )
        noise = cfg.flow_sigma * rng.standard_normal((n_years, n_countries, n_countries))
        log_flow = (
            supply_path[:, :, None]
            + log_demand[None, None, :]
            + (resistance + pair_effect)[None, :, :]
            + noise
        )
        raw = np.exp(log_flow) * exists[None, :, :]

        # Normalisation au commerce mondial du produit, croissant avec la tendance
        base_trade = cfg.world_trade_median * np.exp(cfg.world_trade_sigma * rng.standard_normal())
        totals = np.array(
            [
                base_trade
                * np.exp(cfg.trend_growth * (year - cfg.year_start) + cfg.year_shocks.get(year, 0.0))
                for year in years
            ]
        )
        value = raw * (totals / raw.sum(axis=(1, 2)))[:, None, None]
        # Suppression des flux trop faibles
        value = np.where(value >= cfg.min_value, value, 0.0)

        # Poids : prix unitaire du produit, avec un bruit propre à chaque flux
        unit_price = cfg.unit_price_median * np.exp(cfg.unit_price_sigma * rng.standard_normal())
        weight = value / unit_price * np.exp(0.15 * rng.standard_normal(value.shape))
        return value, weight

    # Trajectoires d'un produit (calculées une seule fois)
    def paths(self, product: str) -> Tuple[np.ndarray, np.ndarray]:
        """Return (and cache) the flow arrays of a product, all years.

        Args:
            product: HS6 product code (digits).

        Returns:
            Tuple ``(value, weight)`` of shape ``(T, N, N)``.
        """
        if product not in self._paths:
            self._paths[product] = self._simulate(product)
        return self._paths[product]

    # Flux vrais d'un produit et d'une année, sous forme de matrices
    def matrices(self, product: str, year: int) -> Tuple[np.ndarray, np.ndarray]:
        """Return the value and weight matrices of a product-year.

        Args:
            product: HS6 product code (digits).
            year: Simulated year.

        Returns:
            Tuple ``(value, weight)`` of shape ``(N, N)`` (exporter, importer).

        Raises:
            ValueError: If ``year`` is outside the simulated range.
        """
        if year not in self.config.years:
            raise ValueError(
                f"Year {year} outside the simulated range "
                f"[{self.config.year_start}, {self.config.year_end}]"
            )
        value, weight = self.paths(product)
        index = year - self.config.year_start
        return value[index], weight[index]

    # Flux vrais d'un produit et d'une année, sous forme de table
    def flows(self, product: str, year: int) -> pd.DataFrame:
        """Return the true bilateral flows of a product-year.

        Args:
            product: HS6 product code (digits).
            year: Simulated year.

        Returns:
            DataFrame with columns ``year``, ``product``, ``exporter``,
            ``importer`` (ISO3), ``value`` (USD) and ``weight`` (kg); one row
            per existing flow.
        """
        value, weight = self.matrices(product, year)
        exporter, importer = np.nonzero(value)
        return pd.DataFrame(
            {
                "year": year,
                "product": product,
                "exporter": self._iso3[exporter],
                "importer": self._iso3[importer],
                "value": value[exporter, importer],
                "weight": weight[exporter, importer],
            }
        )

    # Paramètre de quantité d'un produit (articles par kilogramme)
    def items_per_kg(self, product: str) -> float:
        """Number of articles per kilogram of a product (for count-based units).

        Args:
            product: HS6 product code (digits).

        Returns:
            Articles per kg (log-normal across products, deterministic).
        """
        return float(np.exp(3.0 + 1.0 * self.rng(_TAG_STRUCTURE + 100, product).standard_normal()))
