"""Trade-vulnerability metrics.

Implements the three import-vulnerability measures :

- :class:`HerfindahlHirschmanIndex` (HHI) — geographic concentration of sources.
- :class:`ConcentrationDependencyIndex2` (CDI2) — extra-regional dependence.
- :class:`ConcentrationDependencyIndex3` (CDI3) — domestic substitutability.

All metrics share the :class:`~macroforecast.trade.vulnerabilities.base.VulnerabilityMetric`
API and are expressed in narwhals, so they run on any supported backend.
"""
# Importation des modules
from __future__ import annotations
# Module de base
from typing import ClassVar, List
# Module de manipulation de données
import narwhals as nw
# Module du package
from .base import DEFAULT_CONFIG, VulnerabilityConfig, VulnerabilityMetric


# ──────────────────────────────────────────────────────────────────────
# 1.1.1 — Concentration géographique (HHI)
# ──────────────────────────────────────────────────────────────────────

# Indice de Herfindahl-Hirschman
class HerfindahlHirschmanIndex(VulnerabilityMetric):
    """Herfindahl-Hirschman index of geographic concentration.

    Computes ``HHI = Σ sᵢ²`` where ``sᵢ`` is the share of partner *i* in the
    cell's total, taken as ``OBS_VALUE(partnerᵢ) / OBS_VALUE(WORLD)``. The sum
    runs over individual partner countries only (aggregate partner codes are
    excluded). A value close to 1 signals extreme concentration; the literature
    flags ``HHI > 0.5`` as highly concentrated.

    The index is computed for every flow (import sourcing as well as export
    destinations).

    Examples:
        >>> # Two suppliers with shares 0.6 and 0.4 → HHI = 0.36 + 0.16 = 0.52
        >>> metric = HerfindahlHirschmanIndex()
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "HHI"

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute the HHI per cell.

        Args:
            data: Narwhals frame of partner-level observations.

        Returns:
            Narwhals frame keyed by ``key_columns`` with an ``HHI`` column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)

        # Valeur totale (WORLD) par cellule, servant de dénominateur des parts
        world = self._partner_values(data, cfg.world_code, "_world")

        # Restriction aux partenaires individuels (hors agrégats)
        individuals = data.filter(self._individual_partner_expr())

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
# 1.1.2 — Dépendance aux sources extra-régionales (CDI2)
# ──────────────────────────────────────────────────────────────────────

# Indice de dépendance extra-régionale
class ConcentrationDependencyIndex2(VulnerabilityMetric):
    """Extra-regional import-dependence index CDI2.

    ``CDI2 = value(extra-EU imports) / value(total imports)`` per cell, using
    the extra-EU and WORLD partner aggregates. A value above 0.5 means more than
    half of imports come from outside the EU. As an import-specific vulnerability
    it is only computed for the import flow (``null`` otherwise).

    Examples:
        >>> metric = ConcentrationDependencyIndex2()
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "CDI2"

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute CDI2 per import cell.

        Args:
            data: Narwhals frame of partner-level observations.

        Returns:
            Narwhals frame keyed by ``key_columns`` (import flow only) with a
            ``CDI2`` column.
        """
        # Extration de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)

        # Restriction au flux d'importation
        imports = data.filter(nw.col(cfg.flow_col) == cfg.import_flow)

        # Imports extra-UE et imports totaux par cellule
        extra_eu = self._partner_values(imports, cfg.extra_eu_code, "_extra_eu")
        total = self._partner_values(imports, cfg.world_code, "_total")

        # Ratio extra-UE / total
        ratio = extra_eu.join(total, on=keys, how="inner")
        return ratio.with_columns(
            (nw.col("_extra_eu") / nw.col("_total")).alias(self.name)
        ).select(*keys, self.name)


# ──────────────────────────────────────────────────────────────────────
# 1.1.3 — Substituabilité par la production domestique (CDI3)
# ──────────────────────────────────────────────────────────────────────

# Indice de substituabilité domestique
class ConcentrationDependencyIndex3(VulnerabilityMetric):
    """Domestic-substitutability index CDI3.

    ``CDI3 = value(extra-EU imports) / value(total exports)``. The numerator is
    the extra-EU partner aggregate on the import flow; the denominator is the
    WORLD partner aggregate on the export flow, joined on every cell key except
    the flow. A ratio above 1 means extra-EU imports exceed total exports,
    suggesting limited domestic-production substitutability. The result is
    attached to the import cell (``null`` for other flows).

    Examples:
        >>> metric = ConcentrationDependencyIndex3()
    """

    # Nom de la colonne de sortie
    name: ClassVar[str] = "CDI3"

    # Calcul de l'indice
    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        """Compute CDI3 per import cell.

        Args:
            data: Narwhals frame of partner-level observations.

        Returns:
            Narwhals frame keyed by ``key_columns`` (import flow only) with a
            ``CDI3`` column.
        """
        # Extraction de la configuration
        cfg = self.config
        # Extraction des colonnes de clés
        keys = list(cfg.key_columns)
        # Clés de jointure cross-flux : toutes les clés sauf le flux
        join_keys = [k for k in keys if k != cfg.flow_col]

        # Imports extra-UE (numérateur), clés hors flux
        imports_extra = self._partner_values(
            data.filter(nw.col(cfg.flow_col) == cfg.import_flow),
            cfg.extra_eu_code,
            "_imports_extra",
            keys=tuple(join_keys),
        )
        # Exports totaux (dénominateur), clés hors flux
        exports_total = self._partner_values(
            data.filter(nw.col(cfg.flow_col) == cfg.export_flow),
            cfg.world_code,
            "_exports_total",
            keys=tuple(join_keys),
        )

        # Ratio imports extra-UE / exports totaux, rattaché à la cellule d'import
        ratio = imports_extra.join(exports_total, on=join_keys, how="inner")
        ratio = ratio.with_columns(
            (nw.col("_imports_extra") / nw.col("_exports_total")).alias(self.name),
            nw.lit(cfg.import_flow).alias(cfg.flow_col),
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
) -> List[VulnerabilityMetric]:
    """Instantiate the default metric registry with a configuration.

    Args:
        config: Column and partner-code conventions shared by the metrics.

    Returns:
        List of metric instances, one per class in
        :data:`DEFAULT_METRIC_CLASSES`.
    """
    # Instanciation de chaque classe avec la configuration commune
    return [cls(config) for cls in DEFAULT_METRIC_CLASSES]
