"""Trade-network vulnerability metrics.

Implements the network dimensions of the methodology, which score the **world
trade graph of a product** rather than the sourcing of one declaring country:

- :class:`WeightedOutdegreeCentralityRisk` — presence of central
  exporters, measured by the dispersion of the weighted outdegree centrality.
- :class:`WeightedClusteringCoefficient` — tendency of partners to
  trade among themselves, Barrat's weighted local clustering.
- :class:`NetworkDiameter` — how many steps separate the two most
  distant countries of the network.
- :class:`WorldExportConcentration` — concentration of world exports
  by country, the HHI component of the SPOF risk.
- :class:`SinglePointOfFailureRisk` and :class:`SinglePointOfFailureDecile`
  — combination of the centrality and concentration ranks, and its
  discretisation into quantile groups.

The input is the BACI reconciled-flow table (one row per
``exporter x importer x product x year``) and a cell of the output grid is a
``nomenclature x product x year`` triple. All metrics share the
:class:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityMetric`
API, so the registry is iterated exactly like the partner-level one.

Four of the six metrics are pure narwhals and therefore run on any supported
backend. Only the two topological measures — clustering and diameter — delegate
to :mod:`macroforecast.trade.vulnerabilities.graph`, which materialises each
cell as a dense NumPy matrix and hands the result back **in the caller's
backend** (see that module's ``Notes:`` for why a graph library would be slower
here). They each pay for their own scan: building the adjacency matrices is
negligible next to the matrix products they do not share.
"""
# Importation des modules
from __future__ import annotations
# Module de base
from typing import ClassVar, List
# Module de manipulation de données
import narwhals as nw
# Modules du package
from .base import (
    DEFAULT_NETWORK_CONFIG,
    NetworkVulnerabilityConfig,
    NetworkVulnerabilityMetric,
)
from .graph import CLUSTERING_W, DIAMETER, compute_graph_features


# ──────────────────────────────────────────────────────────────────────
# 1.1.4 — Centralité dans les réseaux commerciaux mondiaux
# ──────────────────────────────────────────────────────────────────────

# Risque de centralité : dispersion de la centralité weighted outdegree
class WeightedOutdegreeCentralityRisk(NetworkVulnerabilityMetric):
    """Dispersion of the weighted outdegree centrality across exporters.

    Measures the presence of central producers in the world trade network of a
    product — the risk that a shock hitting one major exporter propagates to
    every importer. For an exporter *i*::

        C_i^out = Σ_j w_ij / <w_j>

    where ``w_ij`` is the value exported by *i* to *j* and ``<w_j>`` the mean
    import value of country *j* for that product and year. The score of the cell
    is the **standard deviation of ``C_i^out`` over all its exporters**: a
    concentrated network has a handful of very central players and therefore a
    wide dispersion. The literature flags a centrality risk above 2.5, which
    corresponds to the world's first exporter supplying about two thirds of
    world exports (see
    :attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityConfig.centrality_risk_threshold`).

    Expressed entirely in narwhals: normalising by ``<w_j>`` is a join, and both
    sums are group-bys. No graph is built.

    A cell with a single exporter has no dispersion to measure and is left null
    (the sample standard deviation of one observation is undefined), rather than
    scored zero — which would read as "perfectly diversified".

    Examples:
        >>> metric = WeightedOutdegreeCentralityRisk()
        >>> metric.name
        'CENTRALITY_RISK'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "CENTRALITY_RISK"

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the centrality risk per cell.

        Args:
            data: Narwhals frame of reconciled bilateral flows.

        Returns:
            Narwhals frame keyed by ``key_columns`` with a ``CENTRALITY_RISK``
            column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)

        # Valeur moyenne des importations de chaque pays destinataire : <w_j>
        mean_imports = data.group_by(keys + [cfg.importer_col]).agg(
            nw.col(cfg.value_col).mean().alias("_mean_imports")
        )

        # Normalisation de chaque flux par les importations moyennes de sa
        # destination : w_ij / <w_j>
        normalised = data.join(
            mean_imports, on=keys + [cfg.importer_col], how="inner"
        ).with_columns(
            (nw.col(cfg.value_col) / nw.col("_mean_imports")).alias("_normalised")
        )

        # Centralité weighted outdegree de chaque exportateur : C_i^out
        centrality = normalised.group_by(keys + [cfg.exporter_col]).agg(
            nw.col("_normalised").sum().alias("_centrality")
        )

        # Dispersion des centralités sur les exportateurs de la cellule
        return centrality.group_by(keys).agg(
            nw.col("_centrality").std().alias(self.name)
        )


# ──────────────────────────────────────────────────────────────────────
# 1.1.5 — Tendance au clustering dans les réseaux commerciaux
# ──────────────────────────────────────────────────────────────────────

# Coefficient de clustering local moyen pondéré
class WeightedClusteringCoefficient(NetworkVulnerabilityMetric):
    """Average weighted local clustering coefficient of the trade network.

    Quantifies how likely a country's trading partners are to also trade with
    one another for the same product::

        CC_i^w = 1 / (k_i (k_i - 1)) * Σ_{j,k} (1 / <w_i>)
                 * (w_ij + w_ik) / 2 * a_ij a_ik a_jk

    with ``k_i`` the number of partners of *i* and ``a_ij`` the existence of a
    trade link. The score of the cell is the mean of ``CC_i^w`` over the
    countries having at least two partners. Read together with
    :class:`NetworkDiameter`: a high clustering *and* a high diameter signal
    distinct trade clusters, hence little room for diversification after a
    shock.

    The graph is undirected, its weight being total trade between the two
    countries; cells with fewer than
    :attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityConfig.min_graph_nodes`
    countries are left null rather than scored on a degenerate graph.

    Examples:
        >>> metric = WeightedClusteringCoefficient()
        >>> metric.name
        'CLUSTERING_W'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "CLUSTERING_W"

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the average weighted clustering coefficient per cell.

        Args:
            data: Narwhals frame of reconciled bilateral flows.

        Returns:
            Narwhals frame keyed by ``key_columns`` with a ``CLUSTERING_W``
            column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Passe de graphe restreinte au seul trait utile à cette métrique ; le
        # rapport est celui de cette passe, l'appelant tenant le sien
        frame, _ = compute_graph_features(
            data,
            keys=list(cfg.key_columns),
            exporter_col=cfg.exporter_col,
            importer_col=cfg.importer_col,
            value_col=cfg.value_col,
            features=(CLUSTERING_W,),
            min_nodes=cfg.min_graph_nodes,
        )
        # Alignement du nom du trait sur celui de la métrique
        return frame.rename({CLUSTERING_W: self.name})


# Diamètre du réseau commercial
class NetworkDiameter(NetworkVulnerabilityMetric):
    """Diameter of the product's world trade network.

    The maximum number of steps needed to link the two most distant countries of
    the network. A high diameter combined with a high clustering coefficient
    (:class:`WeightedClusteringCoefficient`) indicates distinct trade clusters
    and limited diversification potential in case of a shock.

    The trade network of a product is regularly fragmented, which would make the
    diameter of the whole graph infinite; the value reported is therefore that
    of the **largest connected component**, the fragmentation being carried
    alongside by
    :attr:`~macroforecast.trade.vulnerabilities.graph.GraphReport.share_graphs_disconnected`.

    Examples:
        >>> metric = NetworkDiameter()
        >>> metric.name
        'DIAMETER'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "DIAMETER"

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the network diameter per cell.

        Args:
            data: Narwhals frame of reconciled bilateral flows.

        Returns:
            Narwhals frame keyed by ``key_columns`` with a ``DIAMETER`` column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Passe de graphe restreinte au seul trait utile à cette métrique
        frame, _ = compute_graph_features(
            data,
            keys=list(cfg.key_columns),
            exporter_col=cfg.exporter_col,
            importer_col=cfg.importer_col,
            value_col=cfg.value_col,
            features=(DIAMETER,),
            min_nodes=cfg.min_graph_nodes,
        )
        # Alignement du nom du trait sur celui de la métrique
        return frame.rename({DIAMETER: self.name})


# ──────────────────────────────────────────────────────────────────────
# 1.1.8 — Risque de points de défaillance uniques (SPOF)
# ──────────────────────────────────────────────────────────────────────

# Concentration des exportations mondiales
class WorldExportConcentration(NetworkVulnerabilityMetric):
    """Herfindahl-Hirschman index of world exports by country.

    ``HHI = Σ_i s_i²`` where ``s_i`` is the share of country *i* in the world
    exports of the product for that year. Distinct from
    :class:`~macroforecast.trade.vulnerabilities.metrics.HerfindahlHirschmanIndex`,
    which measures the sourcing concentration *of one importer*: what is
    measured here is how concentrated **world production** is, whoever buys it —
    the second component of the SPOF risk.

    Expressed entirely in narwhals.

    Examples:
        >>> metric = WorldExportConcentration()
        >>> metric.name, metric.reciprocal_is_effective_count
        ('EXPORT_HHI', True)
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "EXPORT_HHI"
    # 1/HHI se lit comme un nombre effectif d'exportateurs mondiaux
    reciprocal_is_effective_count: ClassVar[bool] = True

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the world-export HHI per cell.

        Args:
            data: Narwhals frame of reconciled bilateral flows.

        Returns:
            Narwhals frame keyed by ``key_columns`` with an ``EXPORT_HHI``
            column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)

        # Exportations totales de chaque pays, toutes destinations confondues
        exports = data.group_by(keys + [cfg.exporter_col]).agg(
            nw.col(cfg.value_col).sum().alias("_exports")
        )
        # Exportations mondiales de la cellule, dénominateur des parts
        world = exports.group_by(keys).agg(nw.col("_exports").sum().alias("_world"))

        # Carré de la part de chaque exportateur : s_i² = (X_i / ΣX)²
        shares = exports.join(world, on=keys, how="inner").with_columns(
            ((nw.col("_exports") / nw.col("_world")) ** 2).alias("_sq_share")
        )

        # Somme des parts carrées par cellule
        return shares.group_by(keys).agg(
            nw.col("_sq_share").sum().alias(self.name)
        )


# Préfixe des colonnes de percentile intermédiaires
_PERCENTILE_PREFIX = "_pct_"


# Fonction de conversion d'un score en percentile dans son groupe de rang
def _rank_percentile(
    frame: nw.DataFrame,
    column: str,
    *,
    keys: List[str],
    rank_keys: List[str],
) -> nw.DataFrame:
    """Turn one score into its percentile within its ranking group.

    Reproduces ``rank(method="average") / count()`` **exactly** — ties included —
    without a windowed ``rank().over()``: the frame is sorted by ranking group
    then by score, its rows are numbered, the first row and the size of each
    group are joined back, and the ordinal ranks of tied values are averaged.
    Sorts, joins and elementary aggregations are all any narwhals backend
    implements, whereas windowed ranking is not: PyArrow, for one, restricts
    ``.over`` to elementary aggregations. Keeping the SPOF metrics free of it is
    what keeps the whole registry backend-agnostic.

    Cells whose score is null or infinite carry no rank and are dropped, so they
    come back null through the left join of :func:`_spof_frame` rather than
    being ranked as if they were zero.

    Args:
        frame: Frame holding ``keys`` and the score column.
        column: Score column to rank.
        keys: Columns identifying an output cell.
        rank_keys: Columns within which the ranking is done.

    Returns:
        Narwhals frame of ``keys`` plus a percentile column in ``]0, 1]``, named
        after the score column.
    """
    # Nom de la colonne de percentile produite
    alias = f"{_PERCENTILE_PREFIX}{column}"

    # Cellules effectivement scorées : un score absent ou infini n'a pas de rang
    scored = frame.filter(nw.col(column).is_finite().fill_null(False))
    # Aucun score : grille vide au schéma attendu
    if len(scored) == 0:
        return frame.select(*keys).head(0).with_columns(
            nw.lit(None).cast(nw.Float64()).alias(alias)
        )

    # Tri par groupe de rang puis par score croissant : les lignes d'un groupe
    # deviennent contiguës et ordonnées, condition du rang par numérotation
    scored = scored.sort(*rank_keys, column).with_row_index("_row")

    # Première ligne et effectif de chaque groupe de rang
    bounds = scored.group_by(rank_keys).agg(
        nw.col("_row").min().alias("_first"), nw.len().alias("_size")
    )
    # Rang ordinal : position dans le groupe, à partir de 1
    scored = scored.join(bounds, on=rank_keys, how="left").with_columns(
        (nw.col("_row") - nw.col("_first") + 1).alias("_ordinal")
    )

    # Moyenne des rangs ordinaux au sein d'un ex aequo : rang « average »
    ties = scored.group_by(rank_keys + [column]).agg(
        nw.col("_ordinal").mean().alias("_rank")
    )
    scored = scored.join(ties, on=rank_keys + [column], how="left")

    # Percentile : rang rapporté à l'effectif du groupe
    return scored.with_columns(
        (nw.col("_rank") / nw.col("_size")).alias(alias)
    ).select(*keys, alias)


# Fonction de combinaison des rangs de centralité et de concentration
def _spof_frame(config: NetworkVulnerabilityConfig, data: nw.DataFrame) -> nw.DataFrame:
    """Combine the centrality and export-concentration ranks into a SPOF score.

    Shared by :class:`SinglePointOfFailureRisk` and
    :class:`SinglePointOfFailureDecile`, which are the same measure at two
    granularities and must never be able to disagree.

    Each component is turned into a **percentile within its ranking group**
    (:attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityConfig.spof_rank_keys`,
    i.e. products are ranked against the products of the same nomenclature and
    year, never across vintages), the two percentiles being then averaged.
    Combining percentiles rather than raw ranks keeps the score comparable
    between years of unequal product counts, and neutralises the fact that the
    two components do not score exactly the same set of cells.

    Args:
        config: Column conventions and SPOF parameters.
        data: Narwhals frame of reconciled bilateral flows.

    Returns:
        Narwhals frame keyed by ``key_columns`` with a ``SPOF`` column in
        ``]0, 1]``, null wherever either component is.
    """
    # Extraction des colonnes de clés et des clés de rang
    keys = list(config.key_columns)
    rank_keys = list(config.spof_rank_keys)

    # Composantes du risque : centralité des exportateurs et concentration des
    # exportations mondiales
    centrality_name = WeightedOutdegreeCentralityRisk.name
    concentration_name = WorldExportConcentration.name
    combined = WeightedOutdegreeCentralityRisk(config).compute(data).join(
        WorldExportConcentration(config).compute(data), on=keys, how="inner"
    )

    # Percentile de chaque composante dans son groupe de rang
    percentiles = combined.select(*keys)
    for name in (centrality_name, concentration_name):
        percentiles = percentiles.join(
            _rank_percentile(combined, name, keys=keys, rank_keys=rank_keys),
            on=keys,
            how="left",
        )

    # Moyenne des deux percentiles ; un percentile absent rend le risque absent
    return percentiles.with_columns(
        (
            (
                nw.col(f"{_PERCENTILE_PREFIX}{centrality_name}")
                + nw.col(f"{_PERCENTILE_PREFIX}{concentration_name}")
            )
            / 2
        ).alias(SinglePointOfFailureRisk.name)
    ).select(*keys, SinglePointOfFailureRisk.name)


# Risque agrégé de point de défaillance unique
class SinglePointOfFailureRisk(NetworkVulnerabilityMetric):
    """Aggregate single-point-of-failure risk of a product.

    Combines, by ranks, the two indicators of the methodology:

    * the **centrality risk** (:class:`WeightedOutdegreeCentralityRisk`), and
    * the **concentration of world exports**
      (:class:`WorldExportConcentration`).

    Each is turned into a percentile within its ranking group and the two are
    averaged, giving a score in ``]0, 1]`` where 1 is the most exposed product.
    A product ranking high on both has a very concentrated world production *and*
    central exporters, which makes diversification structurally difficult — see
    :class:`SinglePointOfFailureDecile` for the grouping the literature reads it
    through.

    Expressed entirely in narwhals; both components are recomputed here rather
    than read back from the result, so the metric stays valid whichever subset
    of the registry a run enables.

    Examples:
        >>> metric = SinglePointOfFailureRisk()
        >>> metric.name
        'SPOF'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "SPOF"

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the aggregate SPOF risk per cell.

        Args:
            data: Narwhals frame of reconciled bilateral flows.

        Returns:
            Narwhals frame keyed by ``key_columns`` with a ``SPOF`` column.
        """
        # Combinaison des rangs des deux composantes
        return _spof_frame(self.config, data)


# Décile de risque de point de défaillance unique
class SinglePointOfFailureDecile(NetworkVulnerabilityMetric):
    """Quantile group of the single-point-of-failure risk.

    Discretisation of :class:`SinglePointOfFailureRisk` into
    :attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityConfig.spof_n_quantiles`
    groups (deciles in the literature), from 1 (least exposed) to
    ``spof_n_quantiles`` (most exposed). Products in the top groups are those
    the methodology designates as vulnerable at world level.

    Stored alongside the continuous risk rather than derived downstream: it is
    the form the methodology reads, and materialising it keeps the grouping
    (and its number of groups) an auditable property of the run rather than a
    convention rebuilt by every consumer.

    Kept as a float so that a cell whose risk is undefined stays null, instead
    of being forced into an arbitrary group.

    Examples:
        >>> metric = SinglePointOfFailureDecile()
        >>> metric.name
        'SPOF_DECILE'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "SPOF_DECILE"

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the SPOF quantile group per cell.

        Args:
            data: Narwhals frame of reconciled bilateral flows.

        Returns:
            Narwhals frame keyed by ``key_columns`` with a ``SPOF_DECILE``
            column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)
        # Nombre de groupes de quantiles
        n_quantiles = cfg.spof_n_quantiles

        # Risque continu, puis découpage en groupes de quantiles. Le percentile
        # valant 1 pour le produit le plus exposé, le plafond évite qu'il forme
        # à lui seul un groupe supplémentaire.
        risk = _spof_frame(cfg, data)
        return risk.with_columns(
            (nw.col(SinglePointOfFailureRisk.name) * n_quantiles)
            .ceil()
            .clip(1, n_quantiles)
            .alias(self.name)
        ).select(*keys, self.name)


# ──────────────────────────────────────────────────────────────────────
# Registre des métriques de réseau
# ──────────────────────────────────────────────────────────────────────

# Classes de métriques de réseau activées par défaut (itérables et extensibles)
DEFAULT_NETWORK_METRIC_CLASSES = (
    WeightedOutdegreeCentralityRisk,
    WeightedClusteringCoefficient,
    NetworkDiameter,
    WorldExportConcentration,
    SinglePointOfFailureRisk,
    SinglePointOfFailureDecile,
)


# Fabrique des métriques de réseau par défaut, instanciées avec une configuration
def default_network_metrics(
    config: NetworkVulnerabilityConfig = DEFAULT_NETWORK_CONFIG,
) -> List[NetworkVulnerabilityMetric]:
    """Instantiate the default network-metric registry with a configuration.

    Args:
        config: Column conventions and thresholds shared by the metrics.

    Returns:
        List of metric instances, one per class in
        :data:`DEFAULT_NETWORK_METRIC_CLASSES`.

    Examples:
        >>> [metric.name for metric in default_network_metrics()]
        ['CENTRALITY_RISK', 'CLUSTERING_W', 'DIAMETER', 'EXPORT_HHI', 'SPOF', 'SPOF_DECILE']
    """
    # Instanciation de chaque classe avec la configuration commune
    return [cls(config) for cls in DEFAULT_NETWORK_METRIC_CLASSES]
