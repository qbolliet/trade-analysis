"""Trade-vulnerability metrics.

Implements the three partner-level vulnerability measures, each computable in
both directions of exposure (``flow="import"``: the reporter's suppliers;
``flow="export"``: its outlets):

- :class:`HerfindahlHirschmanIndex` (HHI) — geographic concentration of the
  suppliers (import) or of the outlets (export).
- :class:`ConcentrationDependencyIndex2` (CDI2) — extra-regional dependence.
- :class:`ConcentrationDependencyIndex3` (CDI3) — domestic substitutability
  (import) or extra-EU exposure relative to the absorption capacity (export).

The export definitions are the formal mirrors of the import ones, obtained by
swapping the roles of imports (M) and exports (X). The polarity is unchanged —
a higher value means a more vulnerable cell in both directions — and so are the
alert thresholds (0.5 for HHI and CDI2, parity 1.0 for CDI3).

All metrics share the :class:`~macroforecast.trade.vulnerabilities.base.VulnerabilityMetric`
API and are expressed in narwhals, so they run on any supported backend.
"""
# Importation des modules
from __future__ import annotations
# Module de base
from typing import ClassVar, FrozenSet, List, Sequence
# Module de manipulation de données
import narwhals as nw
# Module du package
from .base import DEFAULT_CONFIG, VulnerabilityConfig, VulnerabilityMetric

# Sens calculables par les trois métriques partenaires
_BOTH_FLOWS: FrozenSet[str] = frozenset({"import", "export"})


# ──────────────────────────────────────────────────────────────────────
# Concentration géographique (HHI)
# ──────────────────────────────────────────────────────────────────────

# Indice de Herfindahl-Hirschman
class HerfindahlHirschmanIndex(VulnerabilityMetric):
    """Herfindahl-Hirschman index of geographic concentration.

    Computes ``HHI = Σ sᵢ²`` where ``sᵢ`` is the share of partner *i* in the
    cell's total, taken as ``OBS_VALUE(partnerᵢ) / OBS_VALUE(WORLD)`` on the
    instance's own flow. The sum runs over individual partner countries only
    (aggregate partner codes are excluded). A value close to 1 signals extreme
    concentration; the literature flags ``HHI > 0.5`` as highly concentrated.

    - ``flow="import"``: concentration of the reporter's **suppliers** — the
      more concentrated, the more exposed to a supply shock;
    - ``flow="export"``: concentration of the reporter's **outlets** — the more
      concentrated, the more exposed to a demand shock.

    Each instance only scores the cells of its own flow.

    Examples:
        >>> # Two suppliers with shares 0.6 and 0.4 → HHI = 0.36 + 0.16 = 0.52
        >>> metric = HerfindahlHirschmanIndex()
        >>> HerfindahlHirschmanIndex(flow="export").own_flow_code
        2
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "HHI"
    # Sens calculables
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS
    # 1/HHI se lit comme un nombre effectif de fournisseurs (ou de débouchés)
    reciprocal_is_effective_count: ClassVar[bool] = True

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the HHI per cell of the instance's own flow.

        Args:
            data: Narwhals frame of partner-level observations.

        Returns:
            Narwhals frame keyed by ``key_columns`` (own flow only) with an
            ``HHI`` column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)

        # Restriction au flux propre de l'instance
        own = data.filter(nw.col(cfg.flow_col) == self.own_flow_code)

        # Valeur totale (WORLD) par cellule, servant de dénominateur des parts
        world = self._partner_values(own, cfg.world_code, "_world")

        # Restriction aux partenaires individuels (hors agrégats)
        individuals = own.filter(self._individual_partner_expr())

        # Jointure des parts au total de leur cellule
        shares = individuals.join(world, on=keys, how="inner")
        # Carré de la part de chaque partenaire : sᵢ² = (vᵢ / WORLD)²
        shares = shares.with_columns(
            ((nw.col(cfg.value_col) / nw.col("_world")) ** 2).alias("_sq_share")
        )

        # Somme des parts carrées par cellule
        return shares.group_by(keys).agg(
            nw.col("_sq_share").sum().alias(self.name)
        )


# ──────────────────────────────────────────────────────────────────────
# Dépendance aux sources extra-régionales (CDI2)
# ──────────────────────────────────────────────────────────────────────

# Indice de dépendance extra-régionale
class ConcentrationDependencyIndex2(VulnerabilityMetric):
    """Extra-regional dependence index CDI2.

    ``CDI2 = extra_EU(own) / world(own)`` per cell, using the extra-EU and
    WORLD partner aggregates of the instance's own flow:

    - ``flow="import"``: extra-EU imports / total imports — a value above 0.5
      means more than half of the imports come from outside the EU;
    - ``flow="export"``: extra-EU exports / total exports — the share of the
      outlets located outside the internal market, i.e. the dependence on third
      markets.

    For the aggregated reporter ``EU27_2020`` (the Union as a whole), every
    flow is extra-EU by construction, so ``CDI2`` equals 1 in both directions:
    that reporter is excluded from the synthesis by its context filter, not
    from the metrics.

    Examples:
        >>> metric = ConcentrationDependencyIndex2(flow="export")
        >>> metric.fingerprint_key
        'CDI2/export'
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "CDI2"
    # Sens calculables
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute CDI2 per cell of the instance's own flow.

        Args:
            data: Narwhals frame of partner-level observations.

        Returns:
            Narwhals frame keyed by ``key_columns`` (own flow only) with a
            ``CDI2`` column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)

        # Restriction au flux propre de l'instance
        own = data.filter(nw.col(cfg.flow_col) == self.own_flow_code)

        # Valeurs extra-UE et totales du flux propre, par cellule
        extra_eu = self._partner_values(own, cfg.extra_eu_code, "_extra_eu")
        total = self._partner_values(own, cfg.world_code, "_total")

        # Ratio extra-UE / total
        ratio = extra_eu.join(total, on=keys, how="inner")
        return ratio.with_columns(
            (nw.col("_extra_eu") / nw.col("_total")).alias(self.name)
        ).select(*keys, self.name)


# ──────────────────────────────────────────────────────────────────────
# Substituabilité par la production domestique (CDI3)
# ──────────────────────────────────────────────────────────────────────

# Indice de substituabilité domestique
class ConcentrationDependencyIndex3(VulnerabilityMetric):
    """Cross-flow dependence index CDI3.

    ``CDI3 = extra_EU(own) / world(other)``: the numerator is the extra-EU
    partner aggregate on the instance's own flow, the denominator the WORLD
    partner aggregate on the opposite flow, joined on every cell key except the
    flow. The result is attached to the cell of the own flow.

    - ``flow="import"``: extra-EU imports / total exports — a ratio above 1
      means extra-EU imports exceed total exports, suggesting limited
      substitutability by domestic production;
    - ``flow="export"``: extra-EU exports / total imports — the extra-EU
      exposure of the outlets relative to the country's absorption capacity
      (its imports). Formal mirror of the import definition, **proposed by
      analogy, without anchoring in the literature**.

    Examples:
        >>> metric = ConcentrationDependencyIndex3(flow="export")
        >>> metric.own_flow_code, metric.other_flow_code
        (2, 1)
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "CDI3"
    # Sens calculables
    supported_flows: ClassVar[FrozenSet[str]] = _BOTH_FLOWS

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute CDI3 per cell of the instance's own flow.

        Args:
            data: Narwhals frame of partner-level observations, both flows
                included (the denominator is read on the opposite flow).

        Returns:
            Narwhals frame keyed by ``key_columns`` (own flow only) with a
            ``CDI3`` column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)
        # Clés de jointure cross-flux : toutes les clés sauf le flux
        join_keys = [k for k in keys if k != cfg.flow_col]

        # Valeur extra-UE du flux propre (numérateur), clés hors flux
        own_extra = self._partner_values(
            data.filter(nw.col(cfg.flow_col) == self.own_flow_code),
            cfg.extra_eu_code,
            "_own_extra",
            keys=tuple(join_keys),
        )
        # Valeur totale du flux opposé (dénominateur), clés hors flux
        other_total = self._partner_values(
            data.filter(nw.col(cfg.flow_col) == self.other_flow_code),
            cfg.world_code,
            "_other_total",
            keys=tuple(join_keys),
        )

        # Ratio extra-UE (flux propre) / total (flux opposé), rattaché à la
        # cellule du flux propre
        ratio = own_extra.join(other_total, on=join_keys, how="inner")
        ratio = ratio.with_columns(
            (nw.col("_own_extra") / nw.col("_other_total")).alias(self.name),
            nw.lit(self.own_flow_code).alias(cfg.flow_col),
        )
        return ratio.select(*keys, self.name)


# ──────────────────────────────────────────────────────────────────────
# Registre des métriques
# ──────────────────────────────────────────────────────────────────────

# Classes de métriques activées par défaut (itérables et extensibles)
DEFAULT_METRIC_CLASSES = (
    HerfindahlHirschmanIndex,
    ConcentrationDependencyIndex2,
    ConcentrationDependencyIndex3,
)


# Fabrique des métriques par défaut, instanciées avec une configuration
def default_metrics(
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    flows: Sequence[str] = ("import",),
) -> List[VulnerabilityMetric]:
    """Instantiate the default metric registry with a configuration.

    One instance per (direction, class) pair the class supports, the
    configuration being shared as is by every instance.

    Args:
        config: Column and partner-code conventions shared by the metrics.
        flows: Directions to instantiate (``"import"``, ``"export"``).

    Returns:
        List of metric instances, grouped by direction in the order of
        ``flows``, then in the order of :data:`DEFAULT_METRIC_CLASSES`.

    Examples:
        >>> [repr(m) for m in default_metrics(flows=("import", "export"))][:2]
        ["HerfindahlHirschmanIndex(flow='import')", "ConcentrationDependencyIndex2(flow='import')"]
        >>> len(default_metrics(flows=("import", "export")))
        6
    """
    # Instanciation de chaque classe, pour chaque sens qu'elle supporte
    return [
        cls(config, flow=flow)
        for flow in flows
        for cls in DEFAULT_METRIC_CLASSES
        if flow in cls.supported_flows
    ]
