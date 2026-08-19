"""Vulnerability metric abstraction.

Defines the shared contract for trade-vulnerability metrics. Each metric scores
the supply vulnerability of a good for a given
``date x nomenclature x indicator x flow x reporter`` cell, looking at the link
between the reporter country and its trading partners.

All metrics share a uniform, backend-agnostic API (narwhals) so that new metrics
can be added with minimal boilerplate and the whole registry can be iterated over
a dataset uniformly. :class:`VulnerabilityMetric` is the abstract parent; concrete
metrics live in :mod:`macroforecast.trade.vulnerabilities.metrics`.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Set, Tuple
# Module de manipulation de données
import narwhals as nw


# ──────────────────────────────────────────────────────────────────────
# Configuration partagée
# ──────────────────────────────────────────────────────────────────────

# Configuration des conventions de colonnes et de codes partenaires
@dataclass(frozen=True)
class VulnerabilityConfig:
    """Column names and partner-code conventions shared by all metrics.

    Centralises every assumption a metric makes about the input schema so that
    the same metric classes can be reused on differently named datasets simply
    by passing another configuration.

    Attributes:
        key_columns: Columns identifying an output cell (the iteration grid),
            i.e. ``date x nomenclature x indicator x flow x reporter`` plus
            frequency. A metric returns exactly one value per distinct
            combination of these columns.
        partner_col: Column holding the partner country/aggregate code.
        value_col: Column holding the observation value (mass or value).
        flow_col: Column holding the trade-flow code.
        world_code: Partner code of the *total* (all-partners) aggregate.
        extra_eu_code: Partner code of the *extra-EU* aggregate (current EU
            composition).
        import_flow: Flow code of imports.
        export_flow: Flow code of exports.
        aggregate_codes: Explicit partner codes treated as aggregates (excluded
            from individual-country shares).
        exclude_underscore_partners: When ``True``, any partner code containing
            an underscore (e.g. ``EXT_EU``, ``INT_EU27_2020``) is treated as an
            aggregate. This cleanly drops every regional aggregate while keeping
            genuine two-letter country codes such as ``QA`` (Qatar).
    """
    # Grille d'itération (cellule de sortie)
    key_columns: Tuple[str, ...] = (
        "freq",
        "reporter",
        "product",
        "flow",
        "indicators",
        "TIME_PERIOD",
    )
    # Colonnes du schéma source
    partner_col: str = "partner"
    value_col: str = "OBS_VALUE"
    flow_col: str = "flow"
    # Codes partenaires agrégés
    world_code: str = "WORLD"
    extra_eu_code: str = "EXT_EU"
    # Codes de flux
    import_flow: int = 1
    export_flow: int = 2
    # Règles d'identification des agrégats (exclus des parts par pays)
    aggregate_codes: Tuple[str, ...] = ("WORLD", "QW")
    exclude_underscore_partners: bool = True


# Configuration par défaut (schéma Eurostat Comext DS-045409)
DEFAULT_CONFIG = VulnerabilityConfig()


# ──────────────────────────────────────────────────────────────────────
# Classe parente abstraite
# ──────────────────────────────────────────────────────────────────────

# Classe parente normalisant l'API des métriques de vulnérabilité
class VulnerabilityMetric(ABC):
    """Abstract base class for trade-vulnerability metrics.

    Subclasses implement :meth:`compute`, which scores a whole dataset at once
    and returns a narwhals frame keyed by :attr:`VulnerabilityConfig.key_columns`
    with a single value column named after the metric (:attr:`name`). Operating
    on the full frame (rather than cell by cell) keeps the implementation
    vectorised and lets cross-flow metrics (e.g. CDI3) join imports to exports.

    The class provides reusable narwhals helpers shared by the concrete metrics
    so that adding a new metric usually amounts to a few narwhals expressions.

    Args:
        config: Column and partner-code conventions. Defaults to
            :data:`DEFAULT_CONFIG`.

    Attributes:
        name: Output column name of the metric (class attribute).
    """

    # Nom de la métrique (colonne de sortie) — défini par chaque sous-classe
    name: ClassVar[str]

    # Initialisation
    def __init__(self, config: VulnerabilityConfig = DEFAULT_CONFIG) -> None:
        # Configuration des conventions de colonnes/codes
        self.config = config

    # Méthode abstraite de calcul de la métrique
    @abstractmethod
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the metric over an entire dataset.

        Args:
            data: Narwhals frame of partner-level observations, exposing at least
                the configured key columns plus ``partner_col`` and
                ``value_col``.

        Returns:
            Narwhals frame with the configured ``key_columns`` and a single
            additional column named :attr:`name`, holding one value per cell.
        """
        raise NotImplementedError

    # Méthode déclarant les colonnes d'entrée requises par la métrique
    def required_columns(self) -> Set[str]:
        """Return the input columns the metric needs to be computable.

        Derived from :attr:`config`: the output-grid keys plus the partner,
        value and flow columns. Overridable so that a custom metric requiring an
        extra column can extend the set; the runner validates the union of every
        metric's requirements against the source frame before computing, turning
        a missing column into a clear error instead of a deep narwhals failure.

        Returns:
            Set of column names that must be present in the input frame.
        """
        # Colonnes dérivées des conventions de configuration
        return set(self.config.key_columns) | {
            self.config.partner_col,
            self.config.value_col,
            self.config.flow_col,
        }

    # ──────────────────────────────────────────────────────────────────
    # Helpers narwhals partagés
    # ──────────────────────────────────────────────────────────────────

    # Méthode auxiliaire : expression de filtre des partenaires individuels
    def _individual_partner_expr(self) -> nw.Expr:
        """Build a boolean narwhals expression selecting individual countries.

        Excludes every aggregate partner code: the explicit ones listed in
        :attr:`VulnerabilityConfig.aggregate_codes` and, when enabled, any code
        containing an underscore (all regional aggregates such as ``EXT_EU``).

        Returns:
            A narwhals boolean expression usable in ``filter``.
        """
        # Colonne des partenaires
        partner = nw.col(self.config.partner_col)
        # Exclusion des partenaires nuls (jamais des pays individuels)
        expr = ~partner.is_null()
        # Exclusion des codes agrégés explicites (fill_null : un nul n'est pas agrégé)
        expr = expr & ~partner.is_in(list(self.config.aggregate_codes)).fill_null(False)
        # Exclusion des agrégats régionaux (codes contenant un underscore) ;
        # fill_null(True) traite un partenaire nul comme exclu sans casser l'opérateur ~.
        if self.config.exclude_underscore_partners:
            expr = expr & ~partner.str.contains("_", literal=True).fill_null(True)
        return expr

    # Méthode auxiliaire : valeurs d'un partenaire donné, par cellule
    def _partner_values(
        self,
        data: nw.DataFrame,
        partner_code: str,
        value_alias: str,
        *,
        keys: Tuple[str, ...] | None = None,
    ) -> nw.DataFrame:
        """Extract one aggregate partner's value per cell.

        Args:
            data: Source frame.
            partner_code: Partner code to keep (e.g. ``WORLD``).
            value_alias: Name of the resulting value column.
            keys: Key columns to retain. Defaults to
                :attr:`VulnerabilityConfig.key_columns`.

        Returns:
            Narwhals frame with ``keys`` and a single ``value_alias`` column.
        """
        # Clés conservées (grille complète par défaut)
        key_cols = list(keys) if keys is not None else list(self.config.key_columns)
        # Filtre sur le partenaire et projection valeur → alias
        return data.filter(
            nw.col(self.config.partner_col) == partner_code
        ).select(*key_cols, nw.col(self.config.value_col).alias(value_alias))
