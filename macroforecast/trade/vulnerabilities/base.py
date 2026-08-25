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
from typing import ClassVar, Optional, Set, Tuple
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
        reporter_col: Column holding the declaring country, used for the input
            cardinality diagnostics.
        product_col: Column holding the product code, same purpose.
        period_col: Column holding the time period, same purpose.
        high_score_threshold: Score above which a cell is reported as "highly
            concentrated" (0.5 in the literature).
        unit_score_threshold: Score above which a ratio-type index exceeds
            parity (1.0, e.g. extra-EU imports exceeding total exports).
        shares_lower_bound: Lower bound below which the individual partner
            shares of a cell are reported as under-covering its total.
        shares_tolerance: Absolute tolerance used when comparing a sum of
            shares to 1, and a score to 1 (float equality is never exact).
        drift_relative_change: Relative move above which a cell's score is
            counted as changed between two runs.
        psi_n_bins: Number of bins of the population stability index.
        metric_alert_thresholds: Per-metric alert thresholds, as
            ``(metric_name, threshold)`` pairs. A metric absent from the
            mapping falls back to ``high_score_threshold``.
        ranking_metric: Metric the "most vulnerable cells" artifact is sorted
            on. ``None`` selects the first concentration metric of the registry
            (see :attr:`VulnerabilityMetric.reciprocal_is_effective_count`).
        artifact_top_n: Number of cells kept in the top-vulnerability artifact.
        artifact_max_rows: Maximum number of rows of a tabular artifact; beyond
            it the table is truncated (the truncation is reported).
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
    # Dimensions dont la cardinalité est rapportée (diagnostics de volumétrie)
    reporter_col: str = "reporter"
    product_col: str = "product"
    period_col: str = "TIME_PERIOD"
    # Seuils d'interprétation des scores
    high_score_threshold: float = 0.5
    unit_score_threshold: float = 1.0
    # Seuils de contrôle de cohérence des agrégats
    shares_lower_bound: float = 0.9
    shares_tolerance: float = 1e-9
    # Paramètres de la comparaison inter-exécutions
    drift_relative_change: float = 0.1
    psi_n_bins: int = 10
    # Paramètres des artefacts de synthèse
    metric_alert_thresholds: Tuple[Tuple[str, float], ...] = (
        ("HHI", 0.5),
        ("CDI2", 0.5),
        ("CDI3", 1.0),
    )
    ranking_metric: Optional[str] = None
    artifact_top_n: int = 50
    artifact_max_rows: int = 10_000


# Configuration par défaut (schéma Eurostat Comext DS-045409)
DEFAULT_CONFIG = VulnerabilityConfig()


# ──────────────────────────────────────────────────────────────────────
# Expressions partagées
# ──────────────────────────────────────────────────────────────────────

# Expression de filtre des partenaires individuels (hors agrégats)
def individual_partner_expr(config: VulnerabilityConfig = DEFAULT_CONFIG) -> nw.Expr:
    """Build a boolean narwhals expression selecting individual countries.

    Excludes every aggregate partner code: the explicit ones listed in
    :attr:`VulnerabilityConfig.aggregate_codes` and, when enabled, any code
    containing an underscore (all regional aggregates such as ``EXT_EU``).

    Deliberately a module-level function rather than a metric method: the
    aggregate-coherence diagnostics must evaluate the *very same* rule as the
    metrics, since what they check is precisely whether that heuristic still
    catches every aggregate of the source nomenclature.

    Args:
        config: Column and partner-code conventions.

    Returns:
        A narwhals boolean expression usable in ``filter``.

    Examples:
        >>> expr = individual_partner_expr()
        >>> isinstance(expr, nw.Expr)
        True
    """
    # Colonne des partenaires
    partner = nw.col(config.partner_col)
    # Exclusion des partenaires nuls (jamais des pays individuels)
    expr = ~partner.is_null()
    # Exclusion des codes agrégés explicites (fill_null : un nul n'est pas agrégé)
    expr = expr & ~partner.is_in(list(config.aggregate_codes)).fill_null(False)
    # Exclusion des agrégats régionaux (codes contenant un underscore) ;
    # fill_null(True) traite un partenaire nul comme exclu sans casser l'opérateur ~.
    if config.exclude_underscore_partners:
        expr = expr & ~partner.str.contains("_", literal=True).fill_null(True)
    return expr


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
        reciprocal_is_effective_count: Whether ``1 / score`` reads as an
            effective number of suppliers — true for a concentration index such
            as the HHI, false for a ratio. Drives the
            ``effective_suppliers_median`` diagnostic, which is left ``NaN``
            for the metrics that do not declare it.
    """

    # Nom de la métrique (colonne de sortie) — défini par chaque sous-classe
    name: ClassVar[str]
    # Interprétation de l'inverse du score : nombre effectif de fournisseurs.
    # Vraie pour un indice de concentration (HHI), fausse pour un ratio.
    reciprocal_is_effective_count: ClassVar[bool] = False

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

        Thin delegation to :func:`individual_partner_expr`, which the
        aggregate-coherence diagnostics reuse verbatim.

        Returns:
            A narwhals boolean expression usable in ``filter``.
        """
        # Délégation à l'expression partagée (métriques et diagnostics)
        return individual_partner_expr(self.config)

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
