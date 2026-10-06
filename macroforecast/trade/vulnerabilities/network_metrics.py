"""Trade-network vulnerability metrics.

Implements the network dimensions of the methodology, which score the **world
trade graph of a product** rather than the sourcing of one declaring country:

- :class:`WeightedOutdegreeCentralityRisk` — presence of central
  counterparts, measured by the dispersion of their weighted degree centrality.
- :class:`WeightedClusteringCoefficient` — tendency of partners to
  trade among themselves, Barrat's weighted local clustering.
- :class:`NetworkDiameter` — how many steps separate the two most
  distant countries of the network.
- :class:`WorldExportConcentration` (``WORLD_HHI``) — concentration of world
  exports (or imports) by country, the HHI component of the SPOF risk.
- :class:`SinglePointOfFailureRisk` and :class:`SinglePointOfFailureDecile`
  — combination of the centrality and concentration ranks, and its
  discretisation into quantile groups.

The input is the BACI reconciled-flow table (one row per
``exporter x importer x product x year``) and a cell of the output grid is a
``nomenclature x product x year`` triple. All metrics share the
:class:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityMetric`
API, so the registry is iterated exactly like the partner-level one.

**Two directions.** The metrics score a product, never a country, but four of
them are **oriented**: as written in the literature, ``CENTRALITY_RISK``,
``WORLD_HHI``, ``SPOF`` and ``SPOF_DECILE`` describe how concentrated the world
**supply** is — an import reading (a shock on a major exporter propagates to
every importer). With ``flow="export"`` they are computed on the **transposed**
BACI matrix and describe how concentrated the world **demand** is (central
buyers, concentrated world imports, single point of failure on the demand
side), which is what an exporting country is exposed to. The supply reading is
deliberately not reused at the export: for a country that is itself the
dominant exporter, a concentrated world supply is market power, not a
vulnerability. These export mirrors are **proposed by analogy, without
anchoring in the literature**. The polarity is the same in both directions —
higher means more vulnerable. The transposition never touches the
configuration: the oriented formulas name their columns by role
(:attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityMetric.counterpart_col`,
:attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityMetric.exposed_col`).
``CLUSTERING_W`` and ``DIAMETER`` are measures of the symmetrised graph
(``w_ij + w_ji``), hence identical in both directions: they declare
``orientation_invariant`` and the runner computes them once.

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
from typing import ClassVar, FrozenSet, List, Sequence
# Module de manipulation de données
import narwhals as nw
# Modules du package
from .base import (
    DEFAULT_NETWORK_CONFIG,
    NetworkVulnerabilityConfig,
    NetworkVulnerabilityMetric,
)
from .graph import CLUSTERING_W, DIAMETER, compute_graph_features

# Sens calculables par toutes les métriques de réseau
_BOTH_FLOWS: FrozenSet[str] = frozenset({"import", "export"})


# ──────────────────────────────────────────────────────────────────────
# Centralité dans les réseaux commerciaux mondiaux
# ──────────────────────────────────────────────────────────────────────

# Risque de centralité : dispersion de la centralité pondérée des contreparties
class WeightedOutdegreeCentralityRisk(NetworkVulnerabilityMetric):
    """Dispersion of the weighted degree centrality across the counterparts.

    ``flow="import"`` (graph as is) — presence of central **suppliers**: the
    risk that a shock hitting one major exporter propagates to every importer.
    For an exporter *i*, the weighted outdegree centrality is::

        C_i^out = Σ_j w_ij / <w_j>

    where ``w_ij`` is the value exported by *i* to *j* and ``<w_j>`` the mean
    import value of country *j* for that product and year.

    ``flow="export"`` (transposed graph) — presence of central **buyers**. For
    an importer *j*, the weighted indegree centrality is::

        C_j^in = Σ_i w_ij / <w_i>

    with ``<w_i>`` the mean export value of country *i*.

    The score of the cell is the **standard deviation of the centralities over
    all the counterparts** (exporters at the import, importers at the export):
    a concentrated network has a handful of very central players and therefore
    a wide dispersion. The literature flags a centrality risk above 2.5, which
    corresponds to the world's first exporter supplying about two thirds of
    world exports (see
    :attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityConfig.centrality_risk_threshold`);
    at the export the same threshold is read by symmetry — the world's first
    importer absorbing about two thirds of world imports — without a
    calibration of its own in the literature.

    Expressed entirely in narwhals: normalising by ``<w_j>`` is a join, and both
    sums are group-bys. No graph is built.

    A cell with a single counterpart has no dispersion to measure and is left null
    (the sample standard deviation of one observation is undefined), rather than
    scored zero — which would read as "perfectly diversified".

    Examples:
        >>> metric = WeightedOutdegreeCentralityRisk()
        >>> metric.name
        'CENTRALITY_RISK'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "CENTRALITY_RISK"
    # Sens calculables
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS

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

        # Rôles des extrémités d'arête selon le sens (contreparties, côté exposé)
        counterpart, exposed = self.counterpart_col, self.exposed_col

        # Valeur moyenne des flux de chaque pays du côté exposé : <w_j> à
        # l'import (importations moyennes), <w_i> à l'export (exportations moyennes)
        mean_exposed = data.group_by(keys + [exposed]).agg(
            nw.col(cfg.value_col).mean().alias("_mean_exposed")
        )

        # Normalisation de chaque flux par la moyenne de son extrémité exposée
        normalised = data.join(
            mean_exposed, on=keys + [exposed], how="inner"
        ).with_columns(
            (nw.col(cfg.value_col) / nw.col("_mean_exposed")).alias("_normalised")
        )

        # Centralité pondérée de chaque contrepartie : C_i^out à l'import,
        # C_j^in à l'export
        centrality = normalised.group_by(keys + [counterpart]).agg(
            nw.col("_normalised").sum().alias("_centrality")
        )

        # Dispersion des centralités sur les contreparties de la cellule
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
    countries (``w_ij + w_ji``): the measure is identical in both directions
    (``orientation_invariant``), and the runner computes it once and copies it
    onto the import and export rows. Cells with fewer than
    :attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityConfig.min_graph_nodes`
    countries are left null rather than scored on a degenerate graph.

    Examples:
        >>> metric = WeightedClusteringCoefficient()
        >>> metric.name
        'CLUSTERING_W'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "CLUSTERING_W"
    # Sens calculables, valeur identique dans les deux (graphe symétrisé)
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS
    orientation_invariant: ClassVar[bool] = True

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
        # rapport est celui de cette passe, l'appelant tenant le sien. Colonnes
        # exportateur / importateur désignées explicitement, et non par rôle :
        # le graphe est symétrisé, leur orientation est sans effet
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

    Measured on the undirected graph, hence identical in both directions
    (``orientation_invariant``): computed once and copied onto both flows.

    Examples:
        >>> metric = NetworkDiameter()
        >>> metric.name
        'DIAMETER'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "DIAMETER"
    # Sens calculables, valeur identique dans les deux (graphe non orienté)
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS
    orientation_invariant: ClassVar[bool] = True

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
        # Passe de graphe restreinte au seul trait utile à cette métrique ;
        # colonnes d'arête explicites (graphe non orienté, sens sans effet)
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
# Risque de points de défaillance uniques (SPOF)
# ──────────────────────────────────────────────────────────────────────

# Concentration mondiale des contreparties (exportations ou importations)
class WorldExportConcentration(NetworkVulnerabilityMetric):
    """Herfindahl-Hirschman index of world trade by counterpart country.

    ``WORLD_HHI = Σ_i s_i²`` where ``s_i`` is the share of counterpart *i* in
    the world trade of the product for that year:

    - ``flow="import"``: shares in world **exports** — how concentrated the
      world supply is, whoever buys it;
    - ``flow="export"``: shares in world **imports** — how concentrated the
      world demand is (monopsony risk), whoever sells.

    Distinct from
    :class:`~macroforecast.trade.vulnerabilities.metrics.HerfindahlHirschmanIndex`,
    which measures the concentration of the partners *of one country*: the
    scale differs (the world graph of a product), hence two classes, while the
    direction is the same ``flow`` hyperparameter. Second component of the SPOF
    risk. The class keeps its historical name; its output column is
    ``WORLD_HHI``, the name holding in both directions.

    Expressed entirely in narwhals.

    Examples:
        >>> metric = WorldExportConcentration()
        >>> metric.name, metric.reciprocal_is_effective_count
        ('WORLD_HHI', True)
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "WORLD_HHI"
    # Sens calculables
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS
    # 1/HHI se lit comme un nombre effectif de contreparties mondiales
    reciprocal_is_effective_count: ClassVar[bool] = True

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the world HHI per cell.

        Args:
            data: Narwhals frame of reconciled bilateral flows.

        Returns:
            Narwhals frame keyed by ``key_columns`` with a ``WORLD_HHI``
            column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)

        # Flux totaux de chaque contrepartie, toutes extrémités opposées
        # confondues : exportations à l'import, importations à l'export
        totals = data.group_by(keys + [self.counterpart_col]).agg(
            nw.col(cfg.value_col).sum().alias("_totals")
        )
        # Total mondial de la cellule, dénominateur des parts
        world = totals.group_by(keys).agg(nw.col("_totals").sum().alias("_world"))

        # Carré de la part de chaque contrepartie : s_i² = (X_i / ΣX)²
        shares = totals.join(world, on=keys, how="inner").with_columns(
            ((nw.col("_totals") / nw.col("_world")) ** 2).alias("_sq_share")
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
def _spof_frame(metric: NetworkVulnerabilityMetric, data: nw.DataFrame) -> nw.DataFrame:
    """Combine the centrality and world-concentration ranks into a SPOF score.

    Shared by :class:`SinglePointOfFailureRisk` and
    :class:`SinglePointOfFailureDecile`, which are the same measure at two
    granularities and must never be able to disagree. Both components are
    computed in the direction of ``metric``, so the score is a supply-side
    single point of failure at the import and a demand-side one at the export.
    The ranks are taken within a single direction: a product scored at the
    import is never ranked against a product scored at the export.

    Each component is turned into a **percentile within its ranking group**
    (:attr:`~macroforecast.trade.vulnerabilities.base.NetworkVulnerabilityConfig.spof_rank_keys`,
    i.e. products are ranked against the products of the same nomenclature and
    year, never across vintages), the two percentiles being then averaged.
    Combining percentiles rather than raw ranks keeps the score comparable
    between years of unequal product counts, and neutralises the fact that the
    two components do not score exactly the same set of cells.

    Args:
        metric: SPOF metric instance (configuration and direction).
        data: Narwhals frame of reconciled bilateral flows.

    Returns:
        Narwhals frame keyed by ``key_columns`` with a ``SPOF`` column in
        ``]0, 1]``, null wherever either component is.
    """
    # Configuration et sens de l'instance
    config, flow = metric.config, metric.flow
    # Extraction des colonnes de clés et des clés de rang
    keys = list(config.key_columns)
    rank_keys = list(config.spof_rank_keys)

    # Composantes du risque, dans le sens de l'instance : centralité des
    # contreparties et concentration mondiale
    centrality_name = WeightedOutdegreeCentralityRisk.name
    concentration_name = WorldExportConcentration.name
    combined = WeightedOutdegreeCentralityRisk(config, flow=flow).compute(data).join(
        WorldExportConcentration(config, flow=flow).compute(data), on=keys, how="inner"
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
    * the **world concentration** (:class:`WorldExportConcentration`).

    Each is turned into a percentile within its ranking group and the two are
    averaged, giving a score in ``]0, 1]`` where 1 is the most exposed product.

    - ``flow="import"``: supply-side single point of failure — a product
      ranking high on both has a very concentrated world production *and*
      central exporters, which makes diversifying the suppliers structurally
      difficult;
    - ``flow="export"``: demand-side single point of failure — very
      concentrated world imports and central buyers, which makes diversifying
      the outlets difficult (mirror proposed by analogy, without anchoring in
      the literature).

    The ranks are taken within one direction (one pass per flow) — see
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
    # Sens calculables
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the aggregate SPOF risk per cell.

        Args:
            data: Narwhals frame of reconciled bilateral flows.

        Returns:
            Narwhals frame keyed by ``key_columns`` with a ``SPOF`` column.
        """
        # Combinaison des rangs des deux composantes, dans le sens de l'instance
        return _spof_frame(self, data)


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
    # Sens calculables
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS

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
        risk = _spof_frame(self, data)
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
    flows: Sequence[str] = ("import",),
) -> List[NetworkVulnerabilityMetric]:
    """Instantiate the default network-metric registry with a configuration.

    One instance per (direction, class) pair the class supports, the
    configuration being shared as is by every instance (the direction lives in
    the ``flow`` hyperparameter only).

    Args:
        config: Column conventions and thresholds shared by the metrics.
        flows: Directions to instantiate (``"import"``, ``"export"``).

    Returns:
        List of metric instances, grouped by direction in the order of
        ``flows``, then in the order of :data:`DEFAULT_NETWORK_METRIC_CLASSES`.

    Examples:
        >>> [metric.name for metric in default_network_metrics()]
        ['CENTRALITY_RISK', 'CLUSTERING_W', 'DIAMETER', 'WORLD_HHI', 'SPOF', 'SPOF_DECILE']
        >>> len(default_network_metrics(flows=("import", "export")))
        12
    """
    # Instanciation de chaque classe, pour chaque sens qu'elle supporte
    return [
        cls(config, flow=flow)
        for flow in flows
        for cls in DEFAULT_NETWORK_METRIC_CLASSES
        if flow in cls.supported_flows
    ]
