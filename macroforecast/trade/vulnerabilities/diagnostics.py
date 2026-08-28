"""Structured diagnostics of a vulnerability run.

Holds the composite :class:`VulnerabilityReport` and the narwhals computations
feeding it:

- volumetry and coverage (:func:`compute_input_report`,
  :func:`compute_coverage_report`),
- aggregate coherence (:func:`compute_quality_report`) — the module's most
  valuable check, since every metric joins its ``WORLD`` / ``EXT_EU`` aggregate
  with an *inner* join: a cell deprived of its aggregate silently vanishes from
  the result,
- score distributions (:func:`compute_distribution_reports`),
- run-to-run stability (:func:`compute_drift_report`),
- business artifacts (:func:`log_vulnerability_artifacts`).

The volumetry, coverage, distribution and drift computations are **metric- and
family-agnostic**: they read a configuration only through the
:class:`~macroforecast.trade.vulnerabilities.base.ScoreConfig` fields, so the
network family reuses them verbatim. What is family-specific is the coherence
check — :class:`AggregateQualityReport` for the partner aggregates,
:class:`GraphQualityReport` for the shape of the trade graphs — and the summary
artifacts, whose common bricks are shared and whose family-specific listing is
not (:func:`log_vulnerability_artifacts` versus
:func:`log_network_vulnerability_artifacts`).

Every computation is expressed in narwhals and **never converts to pandas**:
the sub-package is deliberately backend-agnostic, and converting would forfeit
lazy execution on DuckDB/Polars volumes. Only aggregation results — single rows
of a handful of columns — are materialised, plus the artifacts, whose size is
bounded by :attr:`VulnerabilityConfig.artifact_max_rows`.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
import logging
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
# Module de manipulation de données
import narwhals as nw
import pandas as pd
# Modules du package
from ...tracking import RunTracker, flatten_metrics
from .base import (
    DEFAULT_CONFIG,
    DEFAULT_NETWORK_CONFIG,
    NetworkVulnerabilityConfig,
    NetworkVulnerabilityMetric,
    ScoreConfig,
    VulnerabilityConfig,
    VulnerabilityMetric,
    individual_partner_expr,
)
from .graph import GraphReport

# Initialisation du logger
logger = logging.getLogger(__name__)

# Valeur des métriques non calculables (écartée par flatten_metrics)
_NAN = float("nan")
# Colonnes de travail des diagnostics (préfixées, jamais rendues à l'appelant)
_SUM_INDIVIDUAL = "_diag_sum_individual"
_WORLD = "_diag_world"
_RATIO = "_diag_ratio"
_MISSING = "missing_aggregate"
_PREVIOUS_SUFFIX = "__previous"
_UNSCORED = "unscored_metric"
# Suffixe des colonnes booléennes d'alerte, persistées à côté du score continu
_ALERT_SUFFIX = "_ALERT"
# Dossiers d'artefacts, un par famille de métriques
_PARTNER_PREFIX = "vulnerabilities"
_NETWORK_PREFIX = "network_vulnerabilities"

# Métrique de l'une ou l'autre famille : les diagnostics ci-dessous ne lisent
# d'une métrique que son nom et son interprétation, jamais sa mécanique de calcul
AnyMetric = Union[VulnerabilityMetric, NetworkVulnerabilityMetric]


# ──────────────────────────────────────────────────────────────────────
# Utilitaires
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : concaténation de deux fragments de clé
def _join(prefix: str, name: str) -> str:
    """Join a metric prefix and a name with a dot.

    Args:
        prefix: Prefix, possibly empty.
        name: Name to append.

    Returns:
        The dotted key, or ``name`` alone when ``prefix`` is empty.

    Examples:
        >>> _join("vulnerabilities", "quality")
        'vulnerabilities.quality'
        >>> _join("", "quality")
        'quality'
    """
    return f"{prefix}.{name}" if prefix else name


# Fonction auxiliaire : matérialisation d'une ligne d'agrégation
def _aggregate_row(frame: nw.DataFrame) -> Dict[str, Any]:
    """Materialise the single row of an aggregation frame.

    The only materialisation this module performs: a one-row, few-column
    aggregate. It is a ``collect`` of aggregates, not a backend switch — the
    computation itself stays expressed in narwhals.

    Args:
        frame: Narwhals frame holding at most one row.

    Returns:
        Mapping of column name to value, empty when the frame has no row.
    """
    # Extraction des lignes nommées (une seule attendue)
    rows = frame.rows(named=True)
    return dict(rows[0]) if rows else {}


# Fonction auxiliaire : conversion en flottant tolérante aux nuls
def _as_float(value: Any) -> float:
    """Convert an aggregate value to float, mapping missing values to ``NaN``.

    Args:
        value: Value read from an aggregation row.

    Returns:
        The value as a float, ``NaN`` when it is missing or not numeric.

    Examples:
        >>> _as_float(None)
        nan
        >>> _as_float(3)
        3.0
    """
    # Absence de valeur (agrégation sur un frame vide)
    if value is None:
        return _NAN
    try:
        return float(value)
    except (TypeError, ValueError):
        return _NAN


# Fonction auxiliaire : conversion en entier tolérante aux nuls
def _as_int(value: Any) -> int:
    """Convert an aggregate value to int, mapping missing values to ``0``.

    Args:
        value: Value read from an aggregation row.

    Returns:
        The value as an int, ``0`` when it is missing or not numeric.

    Examples:
        >>> _as_int(None), _as_int(4.0)
        (0, 4)
    """
    # Valeur flottante intermédiaire (les sommes booléennes peuvent l'être)
    numeric = _as_float(value)
    return int(numeric) if math.isfinite(numeric) else 0


# Fonction auxiliaire : rapport protégé contre le dénominateur nul
def _share(numerator: Any, denominator: Any) -> float:
    """Compute a share, returning ``NaN`` on an empty denominator.

    Args:
        numerator: Counted items.
        denominator: Total items.

    Returns:
        ``numerator / denominator``, ``NaN`` when the denominator is zero or
        missing — an undefined share is never reported as ``0``.

    Examples:
        >>> _share(1, 4)
        0.25
        >>> _share(0, 0)
        nan
    """
    # Dénominateur nul ou absent : la part n'est pas définie
    total = _as_float(denominator)
    if not math.isfinite(total) or total == 0:
        return _NAN
    return _as_float(numerator) / total


# Fonction auxiliaire : expression de score effectivement calculé
def _scored_expr(column: str) -> nw.Expr:
    """Build the boolean expression "this cell carries a usable score".

    A cell is scored when its value is neither null nor non-finite: dividing by
    a zero aggregate yields ``inf``, which is not a score.

    Args:
        column: Metric column name.

    Returns:
        A narwhals boolean expression, never null.
    """
    # is_finite écarte NaN et ±inf ; fill_null neutralise les cellules non scorées
    return nw.col(column).is_finite().fill_null(False)


# Fonction auxiliaire : comparaison booléenne jamais nulle
def _flag(expr: nw.Expr) -> nw.Expr:
    """Turn a comparison into a null-free boolean expression.

    Args:
        expr: Boolean narwhals expression, possibly null-producing.

    Returns:
        The same expression with nulls mapped to ``False``, so that summing it
        counts occurrences instead of propagating nulls.
    """
    return expr.fill_null(False)


# Fonction auxiliaire : noms des métriques présentes dans un frame
def _present_metrics(
    frame: nw.DataFrame,
    metrics: Sequence[AnyMetric],
) -> List[AnyMetric]:
    """Keep the metrics whose output column exists in a frame.

    Args:
        frame: Frame to inspect.
        metrics: Candidate metric instances.

    Returns:
        The metrics whose :attr:`VulnerabilityMetric.name` is a column of the
        frame — a metric absent from a previous run never raises here.
    """
    # Colonnes disponibles
    available = set(frame.columns)
    return [metric for metric in metrics if metric.name in available]


# Fonction auxiliaire : seuil d'alerte d'une métrique
def metric_alert_threshold(config: ScoreConfig, metric: AnyMetric) -> float:
    """Return the alert threshold a metric's score is compared against.

    The per-metric value configured in
    :attr:`~macroforecast.trade.vulnerabilities.base.ScoreConfig.metric_alert_thresholds`,
    or :attr:`~macroforecast.trade.vulnerabilities.base.ScoreConfig.high_score_threshold`
    when the metric is absent from that mapping. Single source of truth shared by
    the persisted alert columns (:func:`append_alert_flags`) and the
    ``alerts_summary`` artifact (:func:`_log_alerts`), so the two can never
    disagree on where a metric's alert starts.

    Args:
        config: Threshold conventions.
        metric: Metric instance whose threshold is looked up.

    Returns:
        The threshold as a float.

    Examples:
        >>> from macroforecast.trade.vulnerabilities import (
        ...     HerfindahlHirschmanIndex, DEFAULT_CONFIG)
        >>> metric_alert_threshold(DEFAULT_CONFIG, HerfindahlHirschmanIndex())
        0.5
    """
    # Valeur configurée pour la métrique, défaut = seuil de forte concentration
    return dict(config.metric_alert_thresholds).get(
        metric.name, config.high_score_threshold
    )


# ──────────────────────────────────────────────────────────────────────
# Drapeaux d'alerte persistés
# ──────────────────────────────────────────────────────────────────────

# Fonction d'ajout des colonnes booléennes d'alerte au résultat
def append_alert_flags(
    df_result: nw.DataFrame,
    metrics: Sequence[AnyMetric],
    config: ScoreConfig = DEFAULT_CONFIG,
) -> nw.DataFrame:
    """Append one boolean ``{metric}_ALERT`` column per metric to the result.

    Materialises, next to each continuous score, whether the cell exceeds the
    metric's alert threshold (:func:`metric_alert_threshold`) — the literature
    thresholds and the tunable conventions alike. Persisting the flag *and* the
    value lets a downstream synthetic indicator rank every nomenclature without
    re-deriving the thresholds, and keeps the discretisation an auditable
    property of the run rather than a convention rebuilt by every consumer.

    An infinite score (division by a zero aggregate) is a data defect, not a
    business alert — it is reported by the quality diagnostics — so its flag
    stays ``False``; a cell a metric leaves unscored carries a ``False`` flag
    too.

    Args:
        df_result: Result frame (grid keys plus one column per metric).
        metrics: Metric instances applied to the run.
        config: Threshold conventions.

    Returns:
        The frame with one appended ``{name}_ALERT`` boolean column per metric
        present, in registry order. Returned unchanged when no metric column is
        present.

    Examples:
        >>> import pandas as pd
        >>> from macroforecast.trade.vulnerabilities import (
        ...     HerfindahlHirschmanIndex, VulnerabilityConfig)
        >>> config = VulnerabilityConfig(key_columns=("flow",))
        >>> df = pd.DataFrame({"flow": [1, 2], "HHI": [0.7, 0.2]})
        >>> frame = append_alert_flags(
        ...     nw.from_native(df, eager_only=True),
        ...     [HerfindahlHirschmanIndex(config)], config)
        >>> list(frame.to_native()["HHI_ALERT"])
        [True, False]
    """
    # Métriques effectivement présentes dans le résultat
    present = _present_metrics(df_result, metrics)
    if not present:
        return df_result

    # Un booléen de dépassement de seuil par métrique, score infini neutralisé
    exprs = [
        (
            _flag(nw.col(metric.name) > metric_alert_threshold(config, metric))
            & _scored_expr(metric.name)
        ).alias(f"{metric.name}{_ALERT_SUFFIX}")
        for metric in present
    ]
    return df_result.with_columns(*exprs)


# ──────────────────────────────────────────────────────────────────────
# Rapports d'étape
# ──────────────────────────────────────────────────────────────────────

# Volumétrie et cardinalité des dimensions d'entrée
@dataclass
class InputReport:
    """Volumetry of the observations handed to the run.

    Attributes:
        n_observations: Number of input rows.
        n_reporters: Number of distinct declaring countries.
        n_products: Number of distinct products.
        n_partners: Number of distinct partner codes (aggregates included).
        n_periods: Number of distinct time periods.
    """
    n_observations: int = 0
    n_reporters: int = 0
    n_products: int = 0
    n_partners: int = 0
    n_periods: int = 0


# Couverture des scores sur la grille de sortie
@dataclass
class CoverageReport:
    """Share of cells actually scored, per metric.

    Attributes:
        share_non_null: Mapping of metric name to the share of grid cells
            carrying a finite score. A share well below 1 signals cells lost to
            the inner joins on the partner aggregates.
    """
    share_non_null: Dict[str, float] = field(default_factory=dict)


# Cohérence des agrégats partenaires
@dataclass
class AggregateQualityReport:
    """Coherence of the partner aggregates the metrics rely on.

    The most valuable group of the report: it covers the module's most likely
    and least visible failure mode — a cell silently dropped by the inner join
    on its ``WORLD`` / ``EXT_EU`` aggregate — and directly checks that
    :func:`~macroforecast.trade.vulnerabilities.base.individual_partner_expr`
    still filters out every aggregate of the source nomenclature.

    Attributes:
        share_rows_dropped_null: Share of input rows dropped for a null partner
            or value.
        share_cells_missing_world: Share of grid cells with no ``WORLD``
            partner — those cells carry no concentration score at all.
        share_cells_missing_extra_eu: Share of *import* cells with no extra-EU
            aggregate — those cells carry neither CDI2 nor CDI3.
        share_cells_shares_gt_1: Share of cells whose individual partner shares
            sum above ``1``. Anything above zero means an aggregate code got
            through the individual-partner filter and is double-counted.
        share_cells_shares_lt_0_9: Share of cells whose individual partner
            shares sum below the configured lower bound — partner
            under-coverage.
        n_cells_world_le_zero: Number of cells whose ``WORLD`` total is zero or
            negative, i.e. a denominator producing ``inf``.
        n_cells_with_world: Number of cells carrying a strictly positive
            ``WORLD`` total — the denominator of the two share statistics
            above, an undefined ratio being excluded rather than counted as 0.
    """
    share_rows_dropped_null: float = _NAN
    share_cells_missing_world: float = _NAN
    share_cells_missing_extra_eu: float = _NAN
    share_cells_shares_gt_1: float = _NAN
    share_cells_shares_lt_0_9: float = _NAN
    n_cells_world_le_zero: int = 0
    n_cells_with_world: int = 0


# Forme et cohérence des graphes commerciaux
@dataclass
class GraphQualityReport:
    """Shape and coherence of the graphs the network metrics are computed on.

    Network counterpart of :class:`AggregateQualityReport`, which has no meaning
    on a BACI flow table: there is no ``WORLD`` / ``EXT_EU`` aggregate to lose a
    cell to. What can silently distort a topological score here is the *shape*
    of the graph — a network too small to close a triangle, or fragmented into
    components between which no path exists — so that is what is reported.

    Attributes:
        n_graphs: Number of graphs built, i.e. of output cells.
        median_n_nodes: Median number of countries per graph.
        median_n_edges: Median number of undirected trade links per graph.
        median_density: Median density, ``2 m / (n (n - 1))``.
        share_graphs_disconnected: Share of graphs made of more than one
            connected component. The diameter being measured on the largest
            component only, this says how representative it is.
        share_graphs_below_min_nodes: Share of graphs too small for the
            topological metrics, left undefined rather than computed on a
            degenerate graph.
        share_rows_dropped_non_positive: Share of input rows dropped for a
            non-positive value — such a row describes no trade link.
        share_rows_dropped_null: Share of input rows dropped upstream for a null
            endpoint or value.
    """
    n_graphs: int = 0
    median_n_nodes: float = _NAN
    median_n_edges: float = _NAN
    median_density: float = _NAN
    share_graphs_disconnected: float = _NAN
    share_graphs_below_min_nodes: float = _NAN
    share_rows_dropped_non_positive: float = _NAN
    share_rows_dropped_null: float = _NAN

    # Construction depuis le rapport de la passe de graphe
    @classmethod
    def from_graph_report(
        cls,
        report: GraphReport,
        *,
        share_rows_dropped_null: float = _NAN,
    ) -> "GraphQualityReport":
        """Build the report from a graph pass, adding the upstream drop share.

        Args:
            report: Report of the graph-building pass (see
                :func:`~macroforecast.trade.vulnerabilities.graph.compute_graph_features`).
            share_rows_dropped_null: Share of input rows dropped upstream for a
                null endpoint or value, measured around the runner's
                ``drop_nulls``.

        Returns:
            The :class:`GraphQualityReport` of the run.

        Examples:
            >>> from macroforecast.trade.vulnerabilities.graph import GraphReport
            >>> GraphQualityReport.from_graph_report(GraphReport(n_graphs=7)).n_graphs
            7
        """
        # Reprise des champs homonymes du rapport de passe
        return cls(
            n_graphs=report.n_graphs,
            median_n_nodes=report.median_n_nodes,
            median_n_edges=report.median_n_edges,
            median_density=report.median_density,
            share_graphs_disconnected=report.share_graphs_disconnected,
            share_graphs_below_min_nodes=report.share_graphs_below_min_nodes,
            share_rows_dropped_non_positive=report.share_rows_dropped_non_positive,
            share_rows_dropped_null=share_rows_dropped_null,
        )


# Distribution d'un score sur la grille
@dataclass
class ScoreDistributionReport:
    """Distribution of one metric over the scored cells.

    Attributes:
        mean: Mean score.
        median: Median score.
        p10: First decile.
        p90: Last decile.
        std: Standard deviation.
        share_above_0_5: Share of scored cells above
            :attr:`VulnerabilityConfig.high_score_threshold` (0.5 by default,
            the "highly concentrated" threshold of the literature).
        share_above_1: Share of scored cells above
            :attr:`VulnerabilityConfig.unit_score_threshold` (1.0 by default).
        n_cells_equal_1: Number of cells scoring 1 — single-source supply for a
            concentration index — compared within
            :attr:`VulnerabilityConfig.shares_tolerance`.
        effective_suppliers_median: Median of ``1 / score``, i.e. the effective
            number of suppliers. Left ``NaN`` for metrics not declaring
            :attr:`VulnerabilityMetric.reciprocal_is_effective_count`.
        n_scored: Number of cells carrying a finite score — the denominator of
            the shares above.
    """
    mean: float = _NAN
    median: float = _NAN
    p10: float = _NAN
    p90: float = _NAN
    std: float = _NAN
    share_above_0_5: float = _NAN
    share_above_1: float = _NAN
    n_cells_equal_1: int = 0
    effective_suppliers_median: float = _NAN
    n_scored: int = 0


# Stabilité inter-exécutions
@dataclass
class DriftReport:
    """Comparison with the previous run.

    A drop of ``spearman`` without any change of scope is the surest signal of
    an upstream regression (nomenclature change, source revision).

    Attributes:
        n_new_cells: Cells present in this run and absent from the previous one.
        n_disappeared_cells: Cells of the previous run absent from this one.
        n_common_cells: Cells present in both.
        spearman: Rank correlation with the previous run, per metric, over the
            common cells scored in both runs.
        share_cells_changed_gt_10pct: Share of common cells whose score moved by
            more than :attr:`VulnerabilityConfig.drift_relative_change`, per
            metric.
        psi: Population stability index of each metric's distribution, computed
            on both runs as a whole (bins are the previous run's deciles).
    """
    n_new_cells: int = 0
    n_disappeared_cells: int = 0
    n_common_cells: int = 0
    spearman: Dict[str, float] = field(default_factory=dict)
    share_cells_changed_gt_10pct: Dict[str, float] = field(default_factory=dict)
    psi: Dict[str, float] = field(default_factory=dict)

    # Mise en forme des métriques : suffixe par métrique
    def to_metrics(self, prefix: str = "drift") -> Dict[str, float]:
        """Flatten the drift diagnostics into dotted metric keys.

        Per-metric mappings are rendered with an underscore — ``spearman_HHI``,
        ``psi_CDI2``.

        Args:
            prefix: Prefix prepended to every key.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> metrics = DriftReport(n_new_cells=2, spearman={"HHI": 0.97}).to_metrics()
            >>> metrics["drift.n_new_cells"], metrics["drift.spearman_HHI"]
            (2.0, 0.97)
        """
        # Compteurs de périmètre
        metrics = flatten_metrics(
            {
                "n_new_cells": self.n_new_cells,
                "n_disappeared_cells": self.n_disappeared_cells,
                "n_common_cells": self.n_common_cells,
            },
            prefix=prefix,
        )
        # Indicateurs par métrique, aplatis avec un suffixe
        per_metric = (
            ("spearman", self.spearman),
            ("share_cells_changed_gt_10pct", self.share_cells_changed_gt_10pct),
            ("psi", self.psi),
        )
        for label, values in per_metric:
            for metric_name, value in values.items():
                metrics.update(
                    flatten_metrics({f"{label}_{metric_name}": value}, prefix=prefix)
                )
        return metrics


# ──────────────────────────────────────────────────────────────────────
# Rapport composite
# ──────────────────────────────────────────────────────────────────────

# Structure résumant l'exécution du calcul des vulnérabilités
@dataclass
class VulnerabilityReport:
    """Summary of a vulnerability run.

    Composite report built on the ``BaciReport`` model: nested step reports plus
    a :meth:`to_metrics` knowing the MLflow formatting, so that the report stays
    usable on its own.

    Attributes:
        cells: Number of output cells (distinct key combinations) scored.
        metrics: Names of the metrics computed.
        created: Whether the result schema was created (vs. upserted).
        input: Volumetry of the input observations.
        coverage: Share of cells actually scored, per metric.
        quality: Coherence of the partner aggregates.
        distributions: Distribution of each metric, keyed by metric name.
        drift: Comparison with the previous run, ``None`` when no previous
            result was supplied.
    """
    cells: int = 0
    metrics: Optional[List[str]] = None
    created: bool = False
    # Rapports d'étape (principe P3 : les diagnostics sont des données)
    input: InputReport = field(default_factory=InputReport)
    coverage: CoverageReport = field(default_factory=CoverageReport)
    quality: AggregateQualityReport = field(default_factory=AggregateQualityReport)
    distributions: Dict[str, ScoreDistributionReport] = field(default_factory=dict)
    drift: Optional[DriftReport] = None

    # Mise en forme des métriques (la seule à connaître les contraintes MLflow)
    def to_metrics(self, prefix: str = "vulnerabilities") -> Dict[str, float]:
        """Flatten every numeric field into a dotted metric mapping.

        Produces the nomenclature : ``…cells.n_total``, ``…input.*``,
        ``…coverage.share_non_null.HHI``, ``…quality.*``, ``…HHI.median`` (each
        metric prefixes its own distribution) and ``…drift.spearman_HHI``.
        ``NaN`` and infinite values are dropped, MLflow rejecting them. Keeping
        this formatting here rather than in the runner leaves the report usable
        on its own.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> report = VulnerabilityReport(cells=12)
            >>> report.to_metrics()["vulnerabilities.cells.n_total"]
            12.0
            >>> "vulnerabilities.quality.share_cells_shares_gt_1" in report.to_metrics()
            False
        """
        metrics: Dict[str, float] = {}
        # Volumétrie de la grille de sortie et issue de l'écriture
        metrics.update(
            flatten_metrics({"n_total": self.cells}, prefix=_join(prefix, "cells"))
        )
        metrics.update(flatten_metrics({"created": self.created}, prefix=prefix))
        # Rapports d'étape à nomenclature directe
        metrics.update(flatten_metrics(self.input, prefix=_join(prefix, "input")))
        metrics.update(flatten_metrics(self.coverage, prefix=_join(prefix, "coverage")))
        metrics.update(flatten_metrics(self.quality, prefix=_join(prefix, "quality")))
        # Distributions : le nom de la métrique sert de préfixe
        for name, distribution in self.distributions.items():
            metrics.update(flatten_metrics(distribution, prefix=_join(prefix, name)))
        # Dérive : absente si aucune exécution précédente n'a été fournie
        if self.drift is not None:
            metrics.update(self.drift.to_metrics(prefix=_join(prefix, "drift")))
        return metrics


# Structure résumant l'exécution du calcul des vulnérabilités de réseau
@dataclass
class NetworkVulnerabilityReport:
    """Summary of a network-vulnerability run.

    Same composite shape as :class:`VulnerabilityReport` — nested step reports
    plus a :meth:`to_metrics` knowing the MLflow formatting — with the graph
    diagnostics in place of the partner-aggregate ones, which do not apply to a
    BACI flow table.

    Attributes:
        cells: Number of output cells (distinct ``nomenclature x product x year``
            triples) scored.
        metrics: Names of the metrics computed.
        classification: HS vintage the run covers, carried so that a report read
            on its own says which slice it describes.
        created: Whether the result schema was created (vs. upserted).
        input: Volumetry of the input flows.
        coverage: Share of cells actually scored, per metric.
        graph: Shape and coherence of the graphs built.
        distributions: Distribution of each metric, keyed by metric name.
        drift: Comparison with the previous run, ``None`` when no previous
            result was supplied.
    """
    cells: int = 0
    metrics: Optional[List[str]] = None
    classification: Optional[str] = None
    created: bool = False
    # Rapports d'étape
    input: InputReport = field(default_factory=InputReport)
    coverage: CoverageReport = field(default_factory=CoverageReport)
    graph: GraphQualityReport = field(default_factory=GraphQualityReport)
    distributions: Dict[str, ScoreDistributionReport] = field(default_factory=dict)
    drift: Optional[DriftReport] = None

    # Mise en forme des métriques (la seule à connaître les contraintes MLflow)
    def to_metrics(self, prefix: str = "network_vulnerabilities") -> Dict[str, float]:
        """Flatten every numeric field into a dotted metric mapping.

        Produces the nomenclature ``…cells.n_total``, ``…input.*``,
        ``…coverage.share_non_null.SPOF``, ``…graph.*``, ``…SPOF.median`` (each
        metric prefixes its own distribution) and ``…drift.spearman_SPOF``.
        ``NaN`` and infinite values are dropped, MLflow rejecting them.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> report = NetworkVulnerabilityReport(cells=12)
            >>> report.to_metrics()["network_vulnerabilities.cells.n_total"]
            12.0
            >>> "network_vulnerabilities.graph.median_n_nodes" in report.to_metrics()
            False
        """
        metrics: Dict[str, float] = {}
        # Volumétrie de la grille de sortie et issue de l'écriture
        metrics.update(
            flatten_metrics({"n_total": self.cells}, prefix=_join(prefix, "cells"))
        )
        metrics.update(flatten_metrics({"created": self.created}, prefix=prefix))
        # Rapports d'étape à nomenclature directe
        metrics.update(flatten_metrics(self.input, prefix=_join(prefix, "input")))
        metrics.update(flatten_metrics(self.coverage, prefix=_join(prefix, "coverage")))
        metrics.update(flatten_metrics(self.graph, prefix=_join(prefix, "graph")))
        # Distributions : le nom de la métrique sert de préfixe
        for name, distribution in self.distributions.items():
            metrics.update(flatten_metrics(distribution, prefix=_join(prefix, name)))
        # Dérive : absente si aucune exécution précédente n'a été fournie
        if self.drift is not None:
            metrics.update(self.drift.to_metrics(prefix=_join(prefix, "drift")))
        return metrics


# ──────────────────────────────────────────────────────────────────────
# G.1 — Couverture et volumétrie
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul de la volumétrie d'entrée
def compute_input_report(
    data: nw.DataFrame,
    config: ScoreConfig = DEFAULT_CONFIG,
) -> InputReport:
    """Measure the volumetry and dimension cardinalities of the input.

    A single aggregation pass; dimensions absent from the frame are simply left
    at zero rather than raising, the column names being configuration-driven.

    Args:
        data: Narwhals frame of partner-level observations.
        config: Column conventions (``reporter_col``, ``product_col``,
            ``partner_col``, ``period_col``).

    Returns:
        The :class:`InputReport` of the run.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"reporter": ["FR", "FR"], "product": ["01", "02"],
        ...                    "partner": ["CN", "CN"], "TIME_PERIOD": [2020, 2020]})
        >>> compute_input_report(nw.from_native(df, eager_only=True)).n_products
        2
    """
    # Dimensions rapportées, appariées à leur colonne de configuration
    dimensions = (
        ("n_reporters", config.reporter_col),
        ("n_products", config.product_col),
        ("n_partners", config.partner_col),
        ("n_periods", config.period_col),
    )
    # Colonnes effectivement disponibles
    available = set(data.columns)
    # Expressions d'agrégation : volumétrie puis cardinalités
    exprs = [nw.len().alias("n_observations")]
    exprs.extend(
        nw.col(column).n_unique().alias(alias)
        for alias, column in dimensions
        if column in available
    )

    # Matérialisation de la ligne unique d'agrégation
    row = _aggregate_row(data.select(*exprs))
    return InputReport(
        n_observations=_as_int(row.get("n_observations")),
        n_reporters=_as_int(row.get("n_reporters")),
        n_products=_as_int(row.get("n_products")),
        n_partners=_as_int(row.get("n_partners")),
        n_periods=_as_int(row.get("n_periods")),
    )


# Fonction de calcul de la couverture des scores
def compute_coverage_report(
    df_result: nw.DataFrame,
    metrics: Sequence[AnyMetric],
) -> CoverageReport:
    """Measure the share of grid cells actually scored, per metric.

    Args:
        df_result: Result frame (grid keys plus one column per metric).
        metrics: Metric instances applied to the run.

    Returns:
        The :class:`CoverageReport` of the run.
    """
    # Métriques effectivement présentes dans le résultat
    present = _present_metrics(df_result, metrics)
    if not present:
        return CoverageReport()

    # Comptage des cellules scorées, une expression par métrique
    row = _aggregate_row(
        df_result.select(
            nw.len().alias("_n_cells"),
            *(
                _scored_expr(metric.name).sum().alias(metric.name)
                for metric in present
            ),
        )
    )
    # Part de cellules scorées par métrique
    return CoverageReport(
        share_non_null={
            metric.name: _share(row.get(metric.name), row.get("_n_cells"))
            for metric in present
        }
    )


# ──────────────────────────────────────────────────────────────────────
# G.2 — Cohérence des agrégats
# ──────────────────────────────────────────────────────────────────────

# Fonction de repérage des cellules privées d'un agrégat partenaire
def _cells_missing_partner(
    data: nw.DataFrame,
    df_grid: nw.DataFrame,
    config: VulnerabilityConfig,
    partner_code: str,
    *,
    import_only: bool,
) -> Tuple[nw.DataFrame, int]:
    """Locate the grid cells deprived of a given partner aggregate.

    Implemented with an **anti-join**, the exact complement of the ``inner``
    join the metrics perform: what this returns is precisely what silently
    disappears from the result.

    Args:
        data: Narwhals frame of partner-level observations.
        df_grid: Canonical grid of output cells.
        config: Column and partner-code conventions.
        partner_code: Aggregate partner code to look for (e.g. ``WORLD``).
        import_only: Whether to restrict the perimeter to the import flow — the
            extra-EU aggregate only matters for the import-only metrics.

    Returns:
        Tuple ``(missing_cells, n_perimeter)``: the cells lacking the aggregate,
        and the size of the perimeter they are counted against.
    """
    # Clés de la grille de sortie
    keys = list(config.key_columns)
    # Périmètre considéré : grille entière ou cellules d'import
    perimeter = df_grid
    holders = data.filter(nw.col(config.partner_col) == partner_code)
    if import_only and config.flow_col in df_grid.columns:
        flow_is_import = nw.col(config.flow_col) == config.import_flow
        perimeter = perimeter.filter(flow_is_import)
        holders = holders.filter(flow_is_import)

    # Cellules disposant de l'agrégat, dédoublonnées
    holder_cells = holders.select(*keys).unique()
    # Complément de la jointure interne des métriques
    return perimeter.join(holder_cells, on=keys, how="anti"), len(perimeter)


# Fonction de repérage de toutes les cellules privées d'un agrégat
def missing_aggregate_cells(
    data: nw.DataFrame,
    df_grid: nw.DataFrame,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
) -> nw.DataFrame:
    """List every cell deprived of the ``WORLD`` or extra-EU aggregate.

    Feeds both the ``quality.share_cells_missing_*`` metrics and the
    ``missing_aggregates.csv`` artifact, so that counts and listing can never
    diverge.

    Args:
        data: Narwhals frame of partner-level observations.
        df_grid: Canonical grid of output cells.
        config: Column and partner-code conventions.

    Returns:
        Narwhals frame of the grid keys plus a ``missing_aggregate`` column
        holding the missing partner code.
    """
    # Agrégats contrôlés : WORLD sur toute la grille, extra-UE sur les imports
    checks = (
        (config.world_code, False),
        (config.extra_eu_code, True),
    )
    frames = []
    for partner_code, import_only in checks:
        missing, _ = _cells_missing_partner(
            data, df_grid, config, partner_code, import_only=import_only
        )
        frames.append(missing.with_columns(nw.lit(partner_code).alias(_MISSING)))
    # Empilement des deux diagnostics
    return nw.concat(frames, how="vertical")


# Fonction de calcul de la somme des parts par cellule
def compute_shares_frame(
    data: nw.DataFrame,
    df_grid: nw.DataFrame,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
) -> nw.DataFrame:
    """Sum the individual partner shares of every cell.

    Deliberately joins the aggregate with a **left** join, unlike the metrics:
    an ``inner`` join here would hide the very cells the diagnostic looks for.
    The individual-partner filter is
    :func:`~macroforecast.trade.vulnerabilities.base.individual_partner_expr`
    itself, so that what is checked is the rule the metrics actually apply.

    Args:
        data: Narwhals frame of partner-level observations.
        df_grid: Canonical grid of output cells.
        config: Column and partner-code conventions.

    Returns:
        Narwhals frame of the grid keys plus the summed individual value, the
        ``WORLD`` total (null when absent) and their ratio.
    """
    # Clés de la grille de sortie
    keys = list(config.key_columns)

    # Somme des valeurs des partenaires individuels, par cellule
    individuals = data.filter(individual_partner_expr(config))
    sums = individuals.group_by(keys).agg(
        nw.col(config.value_col).sum().alias(_SUM_INDIVIDUAL)
    )
    # Total WORLD par cellule ; agrégé pour rester robuste à une source dupliquée
    world = (
        data.filter(nw.col(config.partner_col) == config.world_code)
        .group_by(keys)
        .agg(nw.col(config.value_col).sum().alias(_WORLD))
    )

    # Jointures gauches sur la grille : aucune cellule ne peut disparaître ici
    frame = df_grid.join(sums, on=keys, how="left").join(world, on=keys, how="left")
    # Somme absente = aucun partenaire individuel déclaré, soit une part nulle
    return frame.with_columns(
        (nw.col(_SUM_INDIVIDUAL).fill_null(0.0) / nw.col(_WORLD)).alias(_RATIO)
    )


# Fonction de calcul du rapport de cohérence des agrégats
def compute_quality_report(
    data: nw.DataFrame,
    df_grid: nw.DataFrame,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    *,
    share_rows_dropped_null: float = _NAN,
) -> AggregateQualityReport:
    """Check the coherence of the partner aggregates the metrics rely on.

    Args:
        data: Narwhals frame of partner-level observations, after the null
            partner/value rows were dropped.
        df_grid: Canonical grid of output cells.
        config: Column, partner-code and threshold conventions.
        share_rows_dropped_null: Share of input rows dropped upstream for a null
            partner or value, measured around the ``drop_nulls`` of the runner.

    Returns:
        The :class:`AggregateQualityReport` of the run.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     "flow": [1, 1, 1], "partner": ["CN", "US", "WORLD"],
        ...     "OBS_VALUE": [60.0, 40.0, 100.0],
        ... })
        >>> config = VulnerabilityConfig(key_columns=("flow",))
        >>> data = nw.from_native(df, eager_only=True)
        >>> grid = data.select("flow").unique()
        >>> compute_quality_report(data, grid, config).share_cells_shares_gt_1
        0.0
    """
    # Cellules privées de leur agrégat : complément exact des jointures internes
    missing_world, n_cells = _cells_missing_partner(
        data, df_grid, config, config.world_code, import_only=False
    )
    missing_extra, n_import_cells = _cells_missing_partner(
        data, df_grid, config, config.extra_eu_code, import_only=True
    )

    # Somme des parts par cellule (jointure gauche : rien ne disparaît)
    shares = compute_shares_frame(data, df_grid, config)
    # Cellules dont le dénominateur est exploitable
    positive_world = _flag(nw.col(_WORLD) > 0)
    row = _aggregate_row(
        shares.select(
            (
                _flag(nw.col(_RATIO) > 1 + config.shares_tolerance) & positive_world
            ).sum().alias("n_gt_1"),
            (
                _flag(nw.col(_RATIO) < config.shares_lower_bound) & positive_world
            ).sum().alias("n_lt_lower"),
            _flag(nw.col(_WORLD) <= 0).sum().alias("n_world_le_zero"),
            positive_world.sum().alias("n_with_world"),
        )
    )

    # Nombre de cellules au dénominateur exploitable
    n_with_world = _as_int(row.get("n_with_world"))
    return AggregateQualityReport(
        share_rows_dropped_null=share_rows_dropped_null,
        share_cells_missing_world=_share(len(missing_world), n_cells),
        share_cells_missing_extra_eu=_share(len(missing_extra), n_import_cells),
        share_cells_shares_gt_1=_share(row.get("n_gt_1"), n_with_world),
        share_cells_shares_lt_0_9=_share(row.get("n_lt_lower"), n_with_world),
        n_cells_world_le_zero=_as_int(row.get("n_world_le_zero")),
        n_cells_with_world=n_with_world,
    )


# ──────────────────────────────────────────────────────────────────────
# G.3 — Distribution des scores
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul des distributions de scores
def compute_distribution_reports(
    df_result: nw.DataFrame,
    metrics: Sequence[AnyMetric],
    config: ScoreConfig = DEFAULT_CONFIG,
) -> Dict[str, ScoreDistributionReport]:
    """Describe the distribution of every metric over the scored cells.

    Statistics are computed on the **scored** cells only: a cell divided by a
    zero aggregate carries ``inf``, which would otherwise swallow the mean, the
    standard deviation and every threshold share. The cells left out are counted
    by ``quality.n_cells_world_le_zero`` and by the coverage report.

    Quantiles go through ``nw.col(...).quantile(q, interpolation="linear")``,
    available on every targeted backend.

    Args:
        df_result: Result frame (grid keys plus one column per metric).
        metrics: Metric instances applied to the run.
        config: Threshold conventions.

    Returns:
        Mapping of metric name to its :class:`ScoreDistributionReport`.
    """
    # Distributions par métrique effectivement présente
    distributions: Dict[str, ScoreDistributionReport] = {}
    for metric in _present_metrics(df_result, metrics):
        name = metric.name
        column = nw.col(name)
        # Restriction aux cellules effectivement scorées : un score infini
        # (dénominateur nul) n'est pas une valeur de la distribution
        scored = df_result.filter(_scored_expr(name))
        # Agrégation unique : moments, quantiles et comptages de dépassement
        row = _aggregate_row(
            scored.select(
                column.mean().alias("mean"),
                column.median().alias("median"),
                column.quantile(0.1, interpolation="linear").alias("p10"),
                column.quantile(0.9, interpolation="linear").alias("p90"),
                column.std().alias("std"),
                _flag(column > config.high_score_threshold).sum().alias("n_above_high"),
                _flag(column > config.unit_score_threshold).sum().alias("n_above_unit"),
                _flag((column - 1.0).abs() <= config.shares_tolerance)
                .sum()
                .alias("n_equal_1"),
                nw.len().alias("n_scored"),
            )
        )
        # Nombre de cellules scorées : dénominateur des parts
        n_scored = _as_int(row.get("n_scored"))

        # Nombre effectif de fournisseurs : seules les métriques le déclarant
        effective_median = _NAN
        if metric.reciprocal_is_effective_count:
            # Restriction aux scores strictement positifs (1/0 n'est pas défini)
            effective_median = _as_float(
                _aggregate_row(
                    scored.filter(_flag(column > 0)).select(
                        (nw.lit(1.0) / column).median().alias("effective")
                    )
                ).get("effective")
            )

        distributions[name] = ScoreDistributionReport(
            mean=_as_float(row.get("mean")),
            median=_as_float(row.get("median")),
            p10=_as_float(row.get("p10")),
            p90=_as_float(row.get("p90")),
            std=_as_float(row.get("std")),
            share_above_0_5=_share(row.get("n_above_high"), n_scored),
            share_above_1=_share(row.get("n_above_unit"), n_scored),
            n_cells_equal_1=_as_int(row.get("n_equal_1")),
            effective_suppliers_median=effective_median,
            n_scored=n_scored,
        )
    return distributions


# ──────────────────────────────────────────────────────────────────────
# G.4 — Stabilité inter-exécutions
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : bornes de classes issues des déciles d'une exécution
def _quantile_edges(
    frame: nw.DataFrame,
    column: str,
    n_bins: int,
) -> List[float]:
    """Compute the inner bin edges of a distribution.

    Args:
        frame: Frame holding the reference distribution.
        column: Metric column.
        n_bins: Number of bins (``n_bins - 1`` inner edges).

    Returns:
        Sorted, de-duplicated finite edges; empty when the distribution is
        degenerate (constant or fully missing), in which case no PSI is defined.
    """
    # Quantiles intérieurs demandés
    quantiles = [index / n_bins for index in range(1, max(n_bins, 1))]
    if not quantiles:
        return []
    # Agrégation unique : toutes les bornes en une passe
    row = _aggregate_row(
        frame.select(
            *(
                nw.col(column)
                .quantile(quantile, interpolation="linear")
                .alias(f"q{index}")
                for index, quantile in enumerate(quantiles)
            )
        )
    )
    # Bornes finies, ordonnées et dédoublonnées (distribution dégénérée → vide)
    edges = {
        _as_float(value)
        for value in row.values()
        if math.isfinite(_as_float(value))
    }
    return sorted(edges)


# Fonction auxiliaire : effectifs par classe
def _bin_counts(
    frame: nw.DataFrame,
    column: str,
    edges: Sequence[float],
) -> List[int]:
    """Count the observations falling in each bin.

    Args:
        frame: Frame to bin.
        column: Metric column.
        edges: Inner bin edges, sorted.

    Returns:
        Counts per bin, of length ``len(edges) + 1``. Null and non-finite
        scores fall in no bin, comparisons being null-free.
    """
    # Expressions d'appartenance : (-inf, e0], (e0, e1], …, (elast, +inf)
    column_expr = nw.col(column)
    exprs = []
    for index, edge in enumerate(edges):
        membership = column_expr <= edge
        if index > 0:
            membership = (column_expr > edges[index - 1]) & membership
        exprs.append(_flag(membership).sum().alias(f"b{index}"))
    exprs.append(_flag(column_expr > edges[-1]).sum().alias(f"b{len(edges)}"))

    # Agrégation unique : tous les effectifs en une passe
    row = _aggregate_row(frame.select(*exprs))
    return [_as_int(row.get(f"b{index}")) for index in range(len(edges) + 1)]


# Fonction de calcul de l'indice de stabilité de population
def _population_stability_index(
    df_current: nw.DataFrame,
    df_previous: nw.DataFrame,
    column: str,
    config: ScoreConfig,
) -> float:
    """Compute the population stability index of one metric.

    ``PSI = Σ (pᵢ - qᵢ) · ln(pᵢ / qᵢ)`` over bins defined by the previous run's
    deciles, each population being taken as a whole (standard definition), not
    restricted to the common cells.

    Args:
        df_current: Result of this run.
        df_previous: Result of the previous run.
        column: Metric column.
        config: Binning conventions (``psi_n_bins``).

    Returns:
        The index, ``NaN`` when the reference distribution is degenerate or a
        population is empty.
    """
    # Bornes de classes issues de l'exécution de référence
    edges = _quantile_edges(df_previous, column, config.psi_n_bins)
    if not edges:
        return _NAN

    # Effectifs par classe des deux populations
    current_counts = _bin_counts(df_current, column, edges)
    previous_counts = _bin_counts(df_previous, column, edges)
    n_current, n_previous = sum(current_counts), sum(previous_counts)
    if n_current == 0 or n_previous == 0:
        return _NAN

    # Plancher de fréquence : une classe vide rendrait le logarithme infini
    floor = 1.0 / max(n_current, n_previous)
    index = 0.0
    for current, previous in zip(current_counts, previous_counts):
        share_current = max(current / n_current, floor)
        share_previous = max(previous / n_previous, floor)
        index += (share_current - share_previous) * math.log(
            share_current / share_previous
        )
    return index


# Fonction de calcul du rapport de dérive
def compute_drift_report(
    df_result: nw.DataFrame,
    df_previous: nw.DataFrame,
    metrics: Sequence[AnyMetric],
    config: ScoreConfig = DEFAULT_CONFIG,
) -> DriftReport:
    """Compare a run with its predecessor.

    Args:
        df_result: Result of this run (grid keys plus one column per metric).
        df_previous: Result of the previous run, same schema.
        metrics: Metric instances applied to the run.
        config: Key and threshold conventions.

    Returns:
        The :class:`DriftReport` of the run.
    """
    # Clés de la grille de sortie
    keys = list(config.key_columns)
    # Métriques comparables : présentes dans les deux exécutions
    present = [
        metric
        for metric in _present_metrics(df_result, metrics)
        if metric.name in set(df_previous.columns)
    ]

    # Évolution du périmètre : cellules apparues et disparues
    current_cells = df_result.select(*keys)
    previous_cells = df_previous.select(*keys)
    report = DriftReport(
        n_new_cells=len(current_cells.join(previous_cells, on=keys, how="anti")),
        n_disappeared_cells=len(
            previous_cells.join(current_cells, on=keys, how="anti")
        ),
    )
    if not present:
        return report

    # Cellules communes, scores de l'exécution précédente suffixés
    previous_scores = df_previous.select(
        *keys,
        *(
            nw.col(metric.name).alias(f"{metric.name}{_PREVIOUS_SUFFIX}")
            for metric in present
        ),
    )
    common = df_result.join(previous_scores, on=keys, how="inner")
    report.n_common_cells = len(common)

    # Indicateurs par métrique
    for metric in present:
        name = metric.name
        previous_name = f"{name}{_PREVIOUS_SUFFIX}"
        # Paires exploitables : score fini dans les deux exécutions
        pairs = common.filter(
            _scored_expr(name) & _scored_expr(previous_name)
        ).select(
            nw.col(name).alias("_current"),
            nw.col(previous_name).alias("_previous"),
        )
        n_pairs = len(pairs)

        # Corrélation de rang : indéfinie en deçà de deux paires
        if n_pairs >= 2:
            report.spearman[name] = _as_float(
                _aggregate_row(
                    pairs.select(
                        nw.corr("_current", "_previous", method="spearman").alias(
                            "spearman"
                        )
                    )
                ).get("spearman")
            )

        # Part de cellules dont le score bouge de plus du seuil relatif
        moved = pairs.filter(_flag(nw.col("_previous") != 0))
        row = _aggregate_row(
            moved.select(
                _flag(
                    (nw.col("_current") - nw.col("_previous")).abs()
                    / nw.col("_previous").abs()
                    > config.drift_relative_change
                )
                .sum()
                .alias("n_changed"),
                nw.len().alias("n_comparable"),
            )
        )
        report.share_cells_changed_gt_10pct[name] = _share(
            row.get("n_changed"), row.get("n_comparable")
        )

        # Indice de stabilité de population sur les deux distributions entières
        report.psi[name] = _population_stability_index(
            df_result, df_previous, name, config
        )
    return report


# ──────────────────────────────────────────────────────────────────────
# G.5 — Artefacts de synthèse : auxiliaires et briques communes
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : métrique servant de critère de classement
def _ranking_metric_name(
    metrics: Sequence[AnyMetric],
    config: ScoreConfig,
) -> Optional[str]:
    """Pick the metric the top-vulnerability artifact is sorted on.

    Args:
        metrics: Metric instances applied to the run.
        config: Conventions (``ranking_metric`` takes precedence when set).

    Returns:
        The metric name, or ``None`` when no metric is available.
    """
    # Choix explicite de la configuration
    if config.ranking_metric:
        return config.ranking_metric
    # À défaut, le premier indice de concentration du registre
    for metric in metrics:
        if metric.reciprocal_is_effective_count:
            return metric.name
    return metrics[0].name if metrics else None


# Fonction auxiliaire : valeur numérique sérialisable en JSON
def _json_number(value: Any) -> Optional[float]:
    """Render a numeric diagnostic as a JSON-serialisable value.

    Args:
        value: Numeric value, possibly ``NaN`` or infinite.

    Returns:
        The value as a float, ``None`` when it is not finite — ``NaN`` is not
        valid JSON.

    Examples:
        >>> _json_number(float("nan")) is None
        True
        >>> _json_number(2)
        2.0
    """
    # Valeurs non finies : rendues absentes plutôt que non sérialisables
    numeric = _as_float(value)
    return numeric if math.isfinite(numeric) else None


# Fonction auxiliaire : troncature d'un artefact tabulaire
def _bounded_table(frame: nw.DataFrame, max_rows: int, artifact: str) -> pd.DataFrame:
    """Materialise a bounded table for artifact logging.

    Converting to pandas is unavoidable here — ``RunTracker.log_table`` takes a
    pandas frame — but only ever applies to a table capped at ``max_rows``, the
    truncation being reported.

    Args:
        frame: Narwhals frame to export.
        max_rows: Maximum number of rows kept.
        artifact: Artifact name, used in the truncation warning.

    Returns:
        The (possibly truncated) table as a pandas DataFrame.
    """
    # Signalement explicite de la troncature
    n_rows = len(frame)
    if n_rows > max_rows:
        # Logging
        logger.warning(
            f"Artifact '{artifact}': {n_rows} rows truncated to {max_rows}"
        )
        frame = frame.head(max_rows)
    return frame.to_pandas()


# Fonction d'envoi de l'artefact des cellules les plus vulnérables
def _log_top_cells(
    tracker: RunTracker,
    df_result: nw.DataFrame,
    present: Sequence[AnyMetric],
    config: ScoreConfig,
    *,
    prefix: str,
) -> None:
    """Send the top-vulnerability table to the tracker.

    Args:
        tracker: Tracker receiving the artifact.
        df_result: Result frame (grid keys plus one column per metric).
        present: Metrics actually present in the result.
        config: Ranking and artifact conventions.
        prefix: Artifact directory, naming the metric family.
    """
    # Métrique servant de critère de tri
    ranking = _ranking_metric_name(present, config)
    if not ranking:
        return
    # Cellules effectivement scorées, par score décroissant
    top = (
        df_result.filter(_scored_expr(ranking))
        .sort(ranking, descending=True)
        .head(config.artifact_top_n)
    )
    tracker.log_table(top.to_pandas(), f"{prefix}/top_vulnerable_products.csv")


# Fonction d'envoi de l'artefact des comptes d'alertes
def _log_alerts(
    tracker: RunTracker,
    df_result: nw.DataFrame,
    present: Sequence[AnyMetric],
    config: ScoreConfig,
    *,
    prefix: str,
) -> None:
    """Send the per-metric alert counts to the tracker.

    Args:
        tracker: Tracker receiving the artifact.
        df_result: Result frame (grid keys plus one column per metric).
        present: Metrics actually present in the result.
        config: Threshold conventions.
        prefix: Artifact directory, naming the metric family.
    """
    # Comptes d'alertes par métrique, seuils partagés avec les colonnes
    # booléennes persistées via metric_alert_threshold
    alerts: Dict[str, Any] = {"n_cells": len(df_result), "rules": {}}
    combined = None
    for metric in present:
        threshold = metric_alert_threshold(config, metric)
        # Un score infini est un défaut de donnée, pas une alerte métier :
        # il est rapporté par les diagnostics de qualité.
        rule = _flag(nw.col(metric.name) > threshold) & _scored_expr(metric.name)
        alerts["rules"][metric.name] = {
            "threshold": threshold,
            "n_cells": _as_int(
                _aggregate_row(df_result.select(rule.sum().alias("n"))).get("n")
            ),
        }
        # Conjonction des règles : cellules cumulant toutes les alertes
        combined = rule if combined is None else combined & rule
    if combined is not None:
        alerts["n_cells_all_rules"] = _as_int(
            _aggregate_row(df_result.select(combined.sum().alias("n"))).get("n")
        )
    tracker.log_dict(alerts, f"{prefix}/alerts_summary.json")


# Fonction d'envoi de l'artefact des distributions et déciles
def _log_metric_distributions(
    tracker: RunTracker,
    df_result: nw.DataFrame,
    present: Sequence[AnyMetric],
    distributions: Dict[str, ScoreDistributionReport],
    *,
    prefix: str,
) -> None:
    """Send the per-metric deciles and distribution summaries to the tracker.

    Args:
        tracker: Tracker receiving the artifact.
        df_result: Result frame (grid keys plus one column per metric).
        present: Metrics actually present in the result.
        distributions: Distribution reports of the completed run report.
        prefix: Artifact directory, naming the metric family.
    """
    # Déciles de chaque métrique
    deciles = {
        metric.name: {
            key: _json_number(value)
            for key, value in _aggregate_row(
                df_result.filter(_scored_expr(metric.name)).select(
                    *(
                        nw.col(metric.name)
                        .quantile(index / 10, interpolation="linear")
                        .alias(f"d{index}")
                        for index in range(1, 10)
                    )
                )
            ).items()
        }
        for metric in present
    }
    tracker.log_dict(
        {
            "deciles": deciles,
            "distributions": {
                name: {
                    field_name: _json_number(value)
                    for field_name, value in vars(distribution).items()
                }
                for name, distribution in distributions.items()
            },
        },
        f"{prefix}/metric_distributions.json",
    )


# Fonction de repérage des cellules laissées sans score
def unscored_cells(
    df_result: nw.DataFrame,
    metrics: Sequence[AnyMetric],
    config: ScoreConfig,
) -> nw.DataFrame:
    """List every cell a metric left unscored, with the metric that skipped it.

    Complement of the coverage report, which only counts: this says *which*
    cells are missing a score, so a coverage below 1 can be audited instead of
    guessed at. For the network family this is the direct reading of a graph too
    small or too fragmented to carry a topological measure.

    Args:
        df_result: Result frame (grid keys plus one column per metric).
        metrics: Metric instances applied to the run.
        config: Key conventions.

    Returns:
        Narwhals frame of the grid keys plus an ``unscored_metric`` column
        holding the name of the metric that left the cell unscored. Empty when
        every cell of every metric is scored.
    """
    # Clés de la grille de sortie
    keys = list(config.key_columns)
    # Métriques effectivement présentes dans le résultat
    present = _present_metrics(df_result, metrics)

    # Un fragment par métrique : ses cellules non scorées, estampillées
    frames = [
        df_result.filter(~_scored_expr(metric.name))
        .select(*keys)
        .with_columns(nw.lit(metric.name).alias(_UNSCORED))
        for metric in present
    ]
    # Aucune métrique présente : grille vide au schéma attendu
    if not frames:
        return df_result.select(*keys).head(0).with_columns(
            nw.lit(None).cast(nw.String()).alias(_UNSCORED)
        )
    return nw.concat(frames, how="vertical")


# ──────────────────────────────────────────────────────────────────────
# G.6 — Artefacts de synthèse des métriques partenaires
# ──────────────────────────────────────────────────────────────────────

# Fonction d'envoi des artefacts de synthèse au tracker
def log_vulnerability_artifacts(
    tracker: RunTracker,
    *,
    data: nw.DataFrame,
    df_grid: nw.DataFrame,
    df_result: nw.DataFrame,
    report: VulnerabilityReport,
    metrics: Sequence[VulnerabilityMetric],
    config: VulnerabilityConfig = DEFAULT_CONFIG,
) -> None:
    """Send the four business artifacts of a run to the tracker.

    Args:
        tracker: Tracker receiving the artifacts.
        data: Narwhals frame of partner-level observations.
        df_grid: Canonical grid of output cells.
        df_result: Result frame (grid keys plus one column per metric).
        report: Completed run report (its distributions feed the deciles).
        metrics: Metric instances applied to the run.
        config: Column, threshold and artifact conventions.
    """
    # Métriques effectivement présentes dans le résultat
    present = _present_metrics(df_result, metrics)

    # Cellules les plus vulnérables, par score de concentration décroissant
    _log_top_cells(tracker, df_result, present, config, prefix=_PARTNER_PREFIX)
    # Comptes d'alertes par métrique
    _log_alerts(tracker, df_result, present, config, prefix=_PARTNER_PREFIX)

    # Cellules privées d'un agrégat : liste auditable des disparitions silencieuses
    missing = missing_aggregate_cells(data, df_grid, config)
    tracker.log_table(
        _bounded_table(missing, config.artifact_max_rows, "missing_aggregates.csv"),
        f"{_PARTNER_PREFIX}/missing_aggregates.csv",
    )

    # Déciles et distributions de chaque métrique
    _log_metric_distributions(
        tracker, df_result, present, report.distributions, prefix=_PARTNER_PREFIX
    )


# ──────────────────────────────────────────────────────────────────────
# G.7 — Artefacts de synthèse des métriques de réseau
# ──────────────────────────────────────────────────────────────────────

# Fonction d'envoi des artefacts de synthèse du calcul de réseau
def log_network_vulnerability_artifacts(
    tracker: RunTracker,
    *,
    df_result: nw.DataFrame,
    report: NetworkVulnerabilityReport,
    metrics: Sequence[NetworkVulnerabilityMetric],
    config: NetworkVulnerabilityConfig = DEFAULT_NETWORK_CONFIG,
) -> None:
    """Send the four business artifacts of a network run to the tracker.

    Same three artifacts as the partner-level family — most vulnerable products,
    alert counts, deciles and distributions — with the unscored-cell listing in
    place of the missing-aggregate one: what makes a network cell disappear is a
    graph too small or too fragmented to measure, not a missing partner
    aggregate.

    Takes no source frame: unlike the partner-level artifacts, none of these is
    computed against the input observations, only against the scores.

    Args:
        tracker: Tracker receiving the artifacts.
        df_result: Result frame (grid keys plus one column per metric).
        report: Completed run report (its distributions feed the deciles).
        metrics: Metric instances applied to the run.
        config: Column, threshold and artifact conventions.
    """
    # Métriques effectivement présentes dans le résultat
    present = _present_metrics(df_result, metrics)

    # Produits les plus exposés, par score décroissant
    _log_top_cells(tracker, df_result, present, config, prefix=_NETWORK_PREFIX)
    # Comptes d'alertes par métrique
    _log_alerts(tracker, df_result, present, config, prefix=_NETWORK_PREFIX)

    # Cellules laissées sans score : graphes dégénérés ou fragmentés, liste
    # auditable de ce que la couverture ne fait que compter
    unscored = unscored_cells(df_result, present, config)
    tracker.log_table(
        _bounded_table(unscored, config.artifact_max_rows, "unscored_cells.csv"),
        f"{_NETWORK_PREFIX}/unscored_cells.csv",
    )

    # Déciles et distributions de chaque métrique
    _log_metric_distributions(
        tracker, df_result, present, report.distributions, prefix=_NETWORK_PREFIX
    )
