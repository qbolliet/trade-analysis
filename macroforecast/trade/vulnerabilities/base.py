"""Vulnerability metric abstraction.

Defines the shared contract for trade-vulnerability metrics. Each metric scores
the vulnerability of a good for a given
``date x nomenclature x indicator x flow x reporter`` cell, looking at the link
between the reporter country and its trading partners.

All metrics share a uniform, backend-agnostic API (narwhals) so that new metrics
can be added with minimal boilerplate and the whole registry can be iterated over
a dataset uniformly. :class:`VulnerabilityMetric` is the abstract parent; concrete
metrics live in :mod:`macroforecast.trade.vulnerabilities.metrics`.

A second, sibling family scores the *world trade graph of a product* rather than
the sourcing of one declaring country: :class:`NetworkVulnerabilityMetric` and
:class:`NetworkVulnerabilityConfig`, whose cell is a
``nomenclature x product x year`` triple over the BACI reconciled flows, with
concrete metrics in :mod:`macroforecast.trade.vulnerabilities.network_metrics`.
The two families share no partner-aggregate machinery, only the
:class:`ScoreConfig` fields the metric-agnostic diagnostics read.

Both families also share one framework for the **direction** of exposure,
independent from their **scale** (one country or the world graph, carried by the
class): every metric instance takes a ``flow`` hyperparameter, ``"import"`` (the
country is exposed to its suppliers) or ``"export"`` (the country is exposed to
its outlets). The shared configuration is never altered by the direction: a
partner metric resolves the flow into its own and opposite flow codes
(:attr:`VulnerabilityMetric.own_flow_code`), a network metric into the roles of
the two edge columns (:attr:`NetworkVulnerabilityMetric.counterpart_col`).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Dict,
    FrozenSet,
    Literal,
    Optional,
    Protocol,
    Set,
    Tuple,
    get_args,
)
# Module de manipulation de données
import narwhals as nw
# Module du package
from ..methodology import methodology_params


# ──────────────────────────────────────────────────────────────────────
# Sens du flux
# ──────────────────────────────────────────────────────────────────────

# Sens d'exposition d'un pays : à ses fournisseurs (import) ou à ses débouchés (export)
Flow = Literal["import", "export"]
# Sens reconnus, dans l'ordre de la définition du type
FLOW_NAMES: Tuple[str, ...] = get_args(Flow)


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
            mapping falls back to ``high_score_threshold``. Each is also
            materialised as a boolean ``{metric}_ALERT`` column in the result
            table, next to the continuous score, so a downstream synthetic
            indicator can rank the nomenclatures without re-deriving them.
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
    # Seuils d'alerte par métrique, matérialisés en colonnes booléennes
    # « {métrique}_ALERT » à côté du score continu dans la table résultat
    # (cf. append_alert_flags). Tous issus de la littérature ici.
    metric_alert_thresholds: Tuple[Tuple[str, float], ...] = (
        ("HHI", 0.5),   # littérature (seuil de forte concentration)
        ("CDI2", 0.5),  # littérature (seuil de forte concentration)
        ("CDI3", 1.0),  # littérature (parité extra-UE / exportations)
    )
    ranking_metric: Optional[str] = None
    artifact_top_n: int = 50
    artifact_max_rows: int = 10_000


# Configuration par défaut (schéma Eurostat Comext DS-045409)
DEFAULT_CONFIG = VulnerabilityConfig()

# Champs de VulnerabilityConfig exclus de l'empreinte méthodologique : ils ne
# pilotent que les diagnostics, la comparaison inter-exécutions et les artefacts,
# jamais une valeur écrite en table. Les seuils d'alerte (metric_alert_thresholds)
# et high_score_threshold, qui sert de repli à une alerte non déclarée, restent
# dans l'empreinte : ils changent les colonnes booléennes « {métrique}_ALERT »
VULNERABILITY_FINGERPRINT_EXCLUDED: frozenset = frozenset(
    {
        "unit_score_threshold",
        "shares_lower_bound",
        "shares_tolerance",
        "drift_relative_change",
        "psi_n_bins",
        "ranking_metric",
        "artifact_top_n",
        "artifact_max_rows",
    }
)


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
# Cadre commun du sens
# ──────────────────────────────────────────────────────────────────────

# Correspondance unique sens → code de flux, pour les deux familles
def flow_code_map(config: Any) -> Dict[str, int]:
    """Map each flow direction to its flow code.

    The single place where a direction name becomes a code. Both
    configurations carry ``import_flow`` / ``export_flow``: the partner one
    reads them in the source table, the network one writes them in its result.

    Args:
        config: :class:`VulnerabilityConfig` or
            :class:`NetworkVulnerabilityConfig` (any object exposing
            ``import_flow`` and ``export_flow``).

    Returns:
        Mapping ``{"import": import_flow, "export": export_flow}``.

    Examples:
        >>> flow_code_map(VulnerabilityConfig())
        {'import': 1, 'export': 2}
        >>> flow_code_map(VulnerabilityConfig(import_flow=10, export_flow=20))["export"]
        20
    """
    return {"import": config.import_flow, "export": config.export_flow}


# Fonction de lecture et de validation d'une liste de sens (paramètre FLOWS)
def parse_flows(value: Any) -> Tuple[str, ...]:
    """Read and validate a list of flow directions.

    Args:
        value: Directions, as a list or tuple of names, or a single name.

    Returns:
        The directions, duplicates removed, in their original order.

    Raises:
        ValueError: If the list is empty or names an unknown direction.

    Examples:
        >>> parse_flows(["import", "export", "import"])
        ('import', 'export')
        >>> parse_flows("export")
        ('export',)
    """
    # Normalisation : un nom isolé vaut une liste d'un élément
    items = [value] if isinstance(value, str) else list(value or ())
    flows = tuple(dict.fromkeys(str(item) for item in items))
    # Vérification des arguments
    if not flows:
        raise ValueError("At least one flow direction must be requested.")
    unknown = [flow for flow in flows if flow not in FLOW_NAMES]
    if unknown:
        raise ValueError(
            f"Unknown flow direction(s) {unknown}: expected among {list(FLOW_NAMES)}."
        )
    return flows


# Fonction de validation d'un sens au regard des sens supportés par une classe
def _validate_flow(cls: type, flow: str) -> None:
    """Check that a metric class supports a flow direction.

    Args:
        cls: Metric class.
        flow: Requested direction.

    Raises:
        ValueError: If ``flow`` is not a known direction or not one of
            ``cls.supported_flows``.

    Examples:
        >>> _validate_flow(type("M", (), {"supported_flows": frozenset({"import"})}), "import")
    """
    # Vérification du sens demandé (vocabulaire, puis support par la classe)
    if flow not in FLOW_NAMES:
        raise ValueError(
            f"Unknown flow {flow!r} for {cls.__name__}: expected one of {list(FLOW_NAMES)}."
        )
    if flow not in cls.supported_flows:
        raise ValueError(
            f"{cls.__name__} does not support flow {flow!r}: supported flows are "
            f"{sorted(cls.supported_flows)}."
        )


# Classe parente commune aux deux familles : hyperparamètre de sens et empreinte
class _DirectedMetric(ABC):
    """Shared direction framework of the partner and network metric families.

    Carries everything the two families have in common about the direction of
    exposure, so that it is written once: the ``flow`` hyperparameter and its
    validation, the class-level declarations of the supported directions and of
    the orientation invariance, and the methodological fingerprint of an
    instance. The scale (one country or the world graph) is carried by the
    concrete subclass, never by this hyperparameter.

    Args:
        config: Configuration dataclass of the family.
        flow: Direction of exposure, ``"import"`` (suppliers) or ``"export"``
            (outlets). Stored as given, following the scikit-learn convention.

    Raises:
        ValueError: If ``flow`` is not in :attr:`supported_flows`.

    Attributes:
        name: Output column name of the metric (class attribute). Two instances
            of a class in two directions share it: the direction is carried by
            the flow column of the result table.
        supported_flows: Directions the class can compute. Defaults to the
            import direction only — the safe choice for a metric whose export
            reading has not been defined.
        orientation_invariant: Whether the value does not depend on the
            direction (a measure of the symmetrised graph): the runner then
            computes it once and copies it onto every direction.
        reciprocal_is_effective_count: Whether ``1 / score`` reads as an
            effective number of counterparts — true for a concentration index,
            false for a ratio or a topological measure. Drives the
            ``effective_suppliers_median`` diagnostic.
    """

    # Nom de la métrique (colonne de sortie) — défini par chaque sous-classe
    name: ClassVar[str]
    # Sens calculables par la classe (import seul par défaut : choix sûr)
    supported_flows: ClassVar[FrozenSet[str]] = frozenset({"import"})
    # Valeur indépendante du sens (graphe symétrisé) : calcul unique, recopie
    orientation_invariant: ClassVar[bool] = False
    # Interprétation de l'inverse du score : nombre effectif de contreparties
    reciprocal_is_effective_count: ClassVar[bool] = False
    # Champs de configuration exclus de l'empreinte — définis par chaque famille
    _fingerprint_excluded: ClassVar[FrozenSet[str]] = frozenset()

    # Initialisation
    def __init__(self, config: Any, *, flow: Flow = "import") -> None:
        # Vérification du sens, puis stockage tel quel des hyperparamètres
        _validate_flow(type(self), flow)
        self.config = config
        self.flow = flow

    # Représentation textuelle (journaux, messages d'erreur)
    def __repr__(self) -> str:
        return f"{type(self).__name__}(flow={self.flow!r})"

    # Clé de l'empreinte dans un registre de fraîcheur
    @property
    def fingerprint_key(self) -> str:
        """Key of the instance's fingerprint in a freshness registry.

        Two instances of a class share a column name, so the key qualifies the
        name by the direction: adding a direction then only makes that
        direction stale.

        Returns:
            ``"<name>/<flow>"``.

        Examples:
            >>> from macroforecast.trade.vulnerabilities import SinglePointOfFailureRisk
            >>> SinglePointOfFailureRisk(flow="export").fingerprint_key
            'SPOF/export'
        """
        return f"{self.name}/{self.flow}"

    # Paramètres de l'empreinte méthodologique
    def fingerprint_params(self) -> Dict[str, Any]:
        """Parameters digested by the methodological fingerprint.

        Returns:
            The configuration fields that shape the written values (the
            family's diagnostic-only fields excluded) plus the ``flow``
            hyperparameter.

        Examples:
            >>> from macroforecast.trade.vulnerabilities import HerfindahlHirschmanIndex
            >>> params = HerfindahlHirschmanIndex(flow="export").fingerprint_params()
            >>> params["flow"], "artifact_top_n" in params
            ('export', False)
        """
        params = methodology_params(self.config, self._fingerprint_excluded)
        params["flow"] = self.flow
        return params

    # Méthode abstraite de calcul de la métrique
    @abstractmethod
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the metric over an entire dataset (see the families)."""
        raise NotImplementedError


# ──────────────────────────────────────────────────────────────────────
# Classe parente abstraite
# ──────────────────────────────────────────────────────────────────────

# Classe parente normalisant l'API des métriques de vulnérabilité
class VulnerabilityMetric(_DirectedMetric):
    """Abstract base class for trade-vulnerability metrics.

    Subclasses implement :meth:`compute`, which scores a whole dataset at once
    and returns a narwhals frame keyed by :attr:`VulnerabilityConfig.key_columns`
    with a single value column named after the metric (:attr:`name`). Operating
    on the full frame (rather than cell by cell) keeps the implementation
    vectorised and lets cross-flow metrics (e.g. CDI3) join both flows.

    An instance only returns rows of its **own** flow
    (:attr:`own_flow_code`); a cross-flow metric reads the opposite one
    (:attr:`other_flow_code`) as an input. The configuration is the same object
    in both directions.

    The class provides reusable narwhals helpers shared by the concrete metrics
    so that adding a new metric usually amounts to a few narwhals expressions.

    Args:
        config: Column and partner-code conventions. Defaults to
            :data:`DEFAULT_CONFIG`.
        flow: Direction of exposure, ``"import"`` or ``"export"``.

    Raises:
        ValueError: If ``flow`` is not in :attr:`supported_flows`.

    Attributes:
        name: Output column name of the metric (class attribute).
        reciprocal_is_effective_count: Whether ``1 / score`` reads as an
            effective number of suppliers — true for a concentration index such
            as the HHI, false for a ratio. Drives the
            ``effective_suppliers_median`` diagnostic, which is left ``NaN``
            for the metrics that do not declare it.

    Examples:
        >>> from macroforecast.trade.vulnerabilities import HerfindahlHirschmanIndex
        >>> metric = HerfindahlHirschmanIndex(flow="export")
        >>> metric.own_flow_code, metric.other_flow_code, metric.fingerprint_key
        (2, 1, 'HHI/export')
    """

    # Champs de configuration exclus de l'empreinte méthodologique
    _fingerprint_excluded: ClassVar[FrozenSet[str]] = VULNERABILITY_FINGERPRINT_EXCLUDED

    # Initialisation
    def __init__(
        self, config: VulnerabilityConfig = DEFAULT_CONFIG, *, flow: Flow = "import"
    ) -> None:
        super().__init__(config, flow=flow)

    # Code du flux propre de l'instance
    @property
    def own_flow_code(self) -> int:
        """Flow code of the instance's direction (rows it returns)."""
        return flow_code_map(self.config)[self.flow]

    # Code du flux opposé
    @property
    def other_flow_code(self) -> int:
        """Flow code of the opposite direction (read by cross-flow metrics)."""
        other = next(name for name in FLOW_NAMES if name != self.flow)
        return flow_code_map(self.config)[other]

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
            additional column named :attr:`name`, holding one value per cell
            of the instance's own flow (:attr:`own_flow_code`).
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


# ──────────────────────────────────────────────────────────────────────
# Protocole partagé des configurations de score
# ──────────────────────────────────────────────────────────────────────

# Dénominateur commun des configurations, consommé par les diagnostics génériques
class ScoreConfig(Protocol):
    """Structural contract satisfied by every vulnerability configuration.

    Names the fields the *metric-agnostic* diagnostics actually read (volumetry,
    coverage, distributions, run-to-run drift and summary artifacts), so that
    those functions can serve the partner-level family
    (:class:`VulnerabilityConfig`) and the network family
    (:class:`NetworkVulnerabilityConfig`) without either depending on the other.
    A typing aid only: nothing is enforced at runtime, and both dataclasses
    satisfy it by construction.

    Attributes:
        key_columns: Columns identifying an output cell (the iteration grid).
        reporter_col: Column whose cardinality is reported as "reporters".
        product_col: Column whose cardinality is reported as "products".
        partner_col: Column whose cardinality is reported as "partners".
        period_col: Column whose cardinality is reported as "periods".
        high_score_threshold: Score above which a cell is flagged as high.
        unit_score_threshold: Score above which a ratio-type index exceeds parity.
        shares_tolerance: Absolute tolerance of the float comparisons to 1.
        drift_relative_change: Relative move above which a score counts as
            changed between two runs.
        psi_n_bins: Number of bins of the population stability index.
        metric_alert_thresholds: Per-metric alert thresholds, as
            ``(metric_name, threshold)`` pairs, read both by the persisted
            ``{metric}_ALERT`` columns and the ``alerts_summary`` artifact.
        ranking_metric: Metric the top-vulnerability artifact is sorted on.
        artifact_top_n: Number of cells kept in that artifact.
        artifact_max_rows: Maximum number of rows of a tabular artifact.
    """
    # Grille d'itération
    key_columns: Tuple[str, ...]
    # Dimensions dont la cardinalité est rapportée (diagnostics de volumétrie)
    reporter_col: str
    product_col: str
    partner_col: str
    period_col: str
    # Seuils d'interprétation des scores
    high_score_threshold: float
    unit_score_threshold: float
    shares_tolerance: float
    # Paramètres de la comparaison inter-exécutions
    drift_relative_change: float
    psi_n_bins: int
    # Paramètres des artefacts de synthèse
    metric_alert_thresholds: Tuple[Tuple[str, float], ...]
    ranking_metric: Optional[str]
    artifact_top_n: int
    artifact_max_rows: int


# ──────────────────────────────────────────────────────────────────────
# Configuration des métriques de réseau
# ──────────────────────────────────────────────────────────────────────

# Configuration des conventions de colonnes des flux bilatéraux réconciliés
@dataclass(frozen=True)
class NetworkVulnerabilityConfig:
    """Column names and thresholds shared by the trade-network metrics.

    Sister of :class:`VulnerabilityConfig` for the metrics scoring the *world
    trade graph of a product* rather than the sourcing of one declaring country.
    The input is the BACI reconciled-flow table — one row per
    ``exporter x importer x product x year`` — and a cell of the output grid is
    a ``nomenclature x product x year`` triple: those metrics have no reporter
    and no partner aggregate, hence a configuration of their own.

    Satisfies :class:`ScoreConfig`: ``reporter_col`` and ``partner_col`` are
    deliberately aliased onto the exporter and importer columns, so that the
    shared volumetry diagnostics report the dimensions of the graph itself.

    Attributes:
        key_columns: Columns identifying an output cell, i.e.
            ``nomenclature x product x year``. A metric returns exactly one
            value per distinct combination of these columns.
        classification_col: Column holding the BACI HS vintage (``"HS2017"``…),
            the primary key the partner-level result table does not carry.
        exporter_col: Column holding the exporting country (edge origin).
        importer_col: Column holding the importing country (edge destination).
        value_col: Column holding the reconciled flow value (edge weight).
        product_col: Column holding the product code.
        period_col: Column holding the year.
        flow_col: Column holding the flow code in the *result* table. The BACI
            matrix has no flow: each direction is computed in its own pass and
            this column is added to its output, then joins the primary key.
        import_flow: Flow code written on the import-direction rows. Must equal
            :attr:`VulnerabilityConfig.import_flow`, so that the synthesis joins
            the two families with a plain equality on the flow.
        export_flow: Flow code written on the export-direction rows, same
            constraint with :attr:`VulnerabilityConfig.export_flow`.
        reporter_col: Alias of :attr:`exporter_col` for the shared diagnostics.
        partner_col: Alias of :attr:`importer_col` for the shared diagnostics.
        centrality_risk_threshold: Centrality-risk value above which a product
            is flagged (2.5 in the literature — roughly the situation where the
            world's first exporter supplies two thirds of world exports; read by
            symmetry in the export direction, where the world's first importer
            absorbs about two thirds of world imports).
        high_score_threshold: Score above which a cell is reported as high
            (0.5, the "highly concentrated" threshold of the literature).
        unit_score_threshold: Score above which a ratio-type index exceeds
            parity (1.0).
        shares_tolerance: Absolute tolerance used when comparing a sum of
            shares to 1, and a score to 1 (float equality is never exact).
        min_graph_nodes: Minimum number of countries below which the topological
            metrics (clustering, diameter) are left undefined rather than
            computed on a degenerate graph.
        spof_rank_keys: Columns within which the SPOF ranks are taken. Products
            are ranked against the products of the *same* nomenclature and year,
            never across vintages.
        spof_n_quantiles: Number of quantile groups the SPOF risk is discretised
            into (deciles in the literature).
        drift_relative_change: Relative move above which a cell's score is
            counted as changed between two runs.
        psi_n_bins: Number of bins of the population stability index.
        metric_alert_thresholds: Per-metric alert thresholds, as
            ``(metric_name, threshold)`` pairs. A metric absent from the mapping
            falls back to ``high_score_threshold``. Each threshold is also
            materialised as a boolean ``{metric}_ALERT`` column in the result
            table, next to the continuous score.
        ranking_metric: Metric the "most vulnerable products" artifact is sorted
            on. Defaults to the aggregate SPOF risk, which is precisely the
            measure combining the others; ``None`` would fall back to the first
            concentration index of the registry.
        artifact_top_n: Number of cells kept in the top-vulnerability artifact.
        artifact_max_rows: Maximum number of rows of a tabular artifact; beyond
            it the table is truncated (the truncation is reported).

    Examples:
        >>> NetworkVulnerabilityConfig().key_columns
        ('classification', 'product', 'year')
        >>> flow_code_map(NetworkVulnerabilityConfig())
        {'import': 1, 'export': 2}
        >>> NetworkVulnerabilityConfig(value_col="v").value_col
        'v'
    """
    # Grille d'itération (cellule de sortie)
    key_columns: Tuple[str, ...] = ("classification", "product", "year")
    # Colonnes du schéma source (flux réconciliés BACI)
    classification_col: str = "classification"
    exporter_col: str = "exporter"
    importer_col: str = "importer"
    value_col: str = "reconciled_value"
    product_col: str = "product"
    period_col: str = "year"
    # Colonne et codes du sens ajoutés en sortie (la table BACI n'a pas de flux) :
    # mêmes codes que les partenaires, condition d'une jointure de synthèse par égalité
    flow_col: str = "flow"
    import_flow: int = 1
    export_flow: int = 2
    # Alias satisfaisant ScoreConfig : les diagnostics de volumétrie partagés
    # rapportent ici les cardinalités des deux extrémités des arêtes
    reporter_col: str = "exporter"
    partner_col: str = "importer"
    # Seuils d'interprétation des scores
    centrality_risk_threshold: float = 2.5
    high_score_threshold: float = 0.5
    unit_score_threshold: float = 1.0
    shares_tolerance: float = 1e-9
    # Taille minimale d'un graphe exploitable (clustering, diamètre)
    min_graph_nodes: int = 3
    # Paramètres du risque de point de défaillance unique (SPOF)
    spof_rank_keys: Tuple[str, ...] = ("classification", "year")
    spof_n_quantiles: int = 10
    # Paramètres de la comparaison inter-exécutions
    drift_relative_change: float = 0.1
    psi_n_bins: int = 10
    # Seuils d'alerte par métrique, matérialisés en colonnes booléennes
    # « {métrique}_ALERT » à côté du score continu (cf. append_alert_flags).
    # Sans eux, l'alerte retomberait sur high_score_threshold, que le diamètre
    # et le décile dépassent par construction. L'appartenance de chaque seuil à
    # la littérature est précisée en commentaire.
    metric_alert_thresholds: Tuple[Tuple[str, float], ...] = (
        ("CENTRALITY_RISK", 2.5),  # littérature (1er exportateur ~2/3 du monde)
        ("CLUSTERING_W", 0.5),  # hors littérature (convention)
        ("DIAMETER", 4.0),  # hors littérature (convention)
        ("WORLD_HHI", 0.5),  # littérature (seuil de forte concentration)
        ("SPOF", 0.9),  # hors littérature (convention)
        ("SPOF_DECILE", 9.0),  # hors littérature (dernier décile sur 10)
    )
    ranking_metric: Optional[str] = "SPOF"
    artifact_top_n: int = 50
    artifact_max_rows: int = 10_000


# Configuration par défaut (schéma des flux réconciliés BACI)
DEFAULT_NETWORK_CONFIG = NetworkVulnerabilityConfig()

# Champs de NetworkVulnerabilityConfig exclus de l'empreinte méthodologique
# (diagnostics, dérive et artefacts seulement). centrality_risk_threshold,
# high_score_threshold et metric_alert_thresholds restent dans l'empreinte :
# ils changent des valeurs ou des colonnes « {métrique}_ALERT » écrites
NETWORK_FINGERPRINT_EXCLUDED: frozenset = frozenset(
    {
        "unit_score_threshold",
        "shares_tolerance",
        "drift_relative_change",
        "psi_n_bins",
        "ranking_metric",
        "artifact_top_n",
        "artifact_max_rows",
    }
)


# ──────────────────────────────────────────────────────────────────────
# Classe parente abstraite des métriques de réseau
# ──────────────────────────────────────────────────────────────────────

# Classe parente normalisant l'API des métriques de réseau
class NetworkVulnerabilityMetric(_DirectedMetric):
    """Abstract base class for trade-network vulnerability metrics.

    Same contract as :class:`VulnerabilityMetric` — score a whole dataset at
    once, return a narwhals frame keyed by
    :attr:`NetworkVulnerabilityConfig.key_columns` with a single value column
    named after the metric — so that the runner iterates both families
    identically.

    What it deliberately does not inherit is the partner-aggregate machinery
    (:meth:`VulnerabilityMetric._partner_values`,
    :meth:`VulnerabilityMetric._individual_partner_expr`): a BACI flow table
    carries no ``WORLD`` / ``EXT_EU`` row, every row being a pair of individual
    countries. Hence a sibling hierarchy rather than a subclass.

    The direction is resolved into the **roles** of the two edge columns, which
    the oriented formulas use instead of the raw exporter / importer columns:
    :attr:`counterpart_col` (the side a country is exposed to: exporters in the
    import direction, importers in the export direction) and
    :attr:`exposed_col` (the other side). Computing a metric in the export
    direction is therefore computing it on the **transposed** BACI matrix,
    without ever altering the configuration. A column used for another reason
    than its role in the flow is still named explicitly.

    Args:
        config: Column conventions and thresholds. Defaults to
            :data:`DEFAULT_NETWORK_CONFIG`.
        flow: Direction of exposure, ``"import"`` or ``"export"``.

    Raises:
        ValueError: If ``flow`` is not in :attr:`supported_flows`.

    Attributes:
        name: Output column name of the metric (class attribute).
        reciprocal_is_effective_count: Whether ``1 / score`` reads as an
            effective number of counterparts — true for a concentration index
            such as the world HHI, false for a topological measure. Drives the
            ``effective_suppliers_median`` diagnostic.

    Examples:
        >>> from macroforecast.trade.vulnerabilities import WorldExportConcentration
        >>> metric = WorldExportConcentration(flow="export")
        >>> metric.counterpart_col, metric.exposed_col
        ('importer', 'exporter')
    """

    # Champs de configuration exclus de l'empreinte méthodologique
    _fingerprint_excluded: ClassVar[FrozenSet[str]] = NETWORK_FINGERPRINT_EXCLUDED

    # Initialisation
    def __init__(
        self,
        config: NetworkVulnerabilityConfig = DEFAULT_NETWORK_CONFIG,
        *,
        flow: Flow = "import",
    ) -> None:
        super().__init__(config, flow=flow)

    # Correspondance unique sens → rôles des deux colonnes d'arête
    def _roles(self) -> Tuple[str, str]:
        """Resolve the direction into ``(counterpart_col, exposed_col)``.

        Returns:
            At the import, the exporters are the counterparts (suppliers) and
            the importers the exposed side; at the export, the reverse.

        Examples:
            >>> from macroforecast.trade.vulnerabilities import WorldExportConcentration
            >>> WorldExportConcentration()._roles()
            ('exporter', 'importer')
        """
        cfg = self.config
        roles = {
            "import": (cfg.exporter_col, cfg.importer_col),
            "export": (cfg.importer_col, cfg.exporter_col),
        }
        return roles[self.flow]

    # Colonne des contreparties
    @property
    def counterpart_col(self) -> str:
        """Edge column of the counterparts: exporters at the import, importers at the export."""
        return self._roles()[0]

    # Colonne du côté exposé
    @property
    def exposed_col(self) -> str:
        """Edge column of the exposed side: importers at the import, exporters at the export."""
        return self._roles()[1]

    # Méthode abstraite de calcul de la métrique
    @abstractmethod
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the metric over an entire dataset.

        Args:
            data: Narwhals frame of reconciled bilateral flows, exposing at
                least the configured key columns plus ``exporter_col``,
                ``importer_col`` and ``value_col``.

        Returns:
            Narwhals frame with the configured ``key_columns`` and a single
            additional column named :attr:`name`, holding one value per cell
            of the instance's direction (no flow column: the runner adds it).
        """
        raise NotImplementedError

    # Méthode déclarant les colonnes d'entrée requises par la métrique
    def required_columns(self) -> Set[str]:
        """Return the input columns the metric needs to be computable.

        Derived from :attr:`config`: the output-grid keys plus the two edge
        endpoints and the edge weight. Overridable so that a custom metric
        requiring an extra column can extend the set; the runner validates the
        union of every metric's requirements against the source frame before
        computing, turning a missing column into a clear error instead of a deep
        narwhals failure.

        Returns:
            Set of column names that must be present in the input frame.
        """
        # Colonnes dérivées des conventions de configuration
        return set(self.config.key_columns) | {
            self.config.exporter_col,
            self.config.importer_col,
            self.config.value_col,
        }
