"""Invalidation des empreintes d'un registre de fraîcheur (recalcul à la passe suivante).

Procédure à suivre après la **correction de l'implémentation** d'une métrique,
d'une étape BACI, d'une méthode de synthèse ou d'une statistique de cohérence.
Les empreintes méthodologiques ne portent que le nom et les paramètres de
configuration, jamais le code : une correction de formule ne se voit donc pas
d'elle-même, il faut la signaler. Ce script supprime l'empreinte enregistrée des
noms corrigés (ou de l'étape) sur les unités choisies ; la prochaine exécution
planifiée de l'étape trouve l'empreinte manquante et recalcule ces unités, avec
la raison ``fingerprint``, que les étapes aval traitent comme un changement
complet de leur entrée (cascade vers la synthèse et la cohérence).

Seule l'empreinte est supprimée, pas l'entrée : l'historique de l'unité (dernier
calcul, watermark amont) est conservé. L'invalidation est persistée : si
l'exécution qui recalcule échoue, les unités restent invalides et sont reprises
à l'exécution suivante (contrairement à un forçage par paramètres d'exécution,
perdu avec l'exécution forcée).

À ne pas lancer pendant que l'étape tourne : un pod qui a déjà chargé un
fragment le réécrirait avec les anciennes empreintes, et l'invalidation serait
perdue sans erreur. Lancer entre deux exécutions (ou sous le même mutex Argo).

Exemples::

    # HHI corrigé : recalcul de HHI (et des autres métriques) sur toutes les unités
    invalidate-freshness-script --step partners --metrics HHI
    # Aperçu, sans écriture, restreint à deux reporters
    invalidate-freshness-script --step partners --metrics HHI --reporters "FR;DE" --dry-run
    # Un seul sens de flux : nom qualifié par le sens
    invalidate-freshness-script --step partners --metrics HHI/export
    # Étape BACI corrigée : réestimation du millésime HS2017
    invalidate-freshness-script --step baci --vintages HS2017
    # Méthode de synthèse corrigée : recalcul de toute la synthèse
    invalidate-freshness-script --step synthesis

Les registres sont lus et écrits avec la configuration de l'environnement
(``VULNERABILITIES_CONFIG_PATH``, ``EUROSTAT_CONFIG_PATH``, ``BACI_CONFIG_PATH``,
``SYNTHESIS_CONFIG_PATH``, ``RUNTIME_CONFIG_PATH``), comme les étapes elles-mêmes.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import argparse
import logging
from typing import Any, Callable, FrozenSet, List, Mapping, Optional, Sequence, Tuple

# Registres de fraîcheur v2
from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    Unit,
    parse_force_list,
    split_qualified,
)
# Métriques calculées par les étapes de vulnérabilité
from macroforecast.trade.vulnerabilities import (
    DEFAULT_METRIC_CLASSES,
    DEFAULT_NETWORK_METRIC_CLASSES,
    FLOW_NAMES,
)

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Étapes dotées d'un registre de fraîcheur
STEPS: Tuple[str, ...] = ("partners", "network", "baci", "synthesis", "coherence")
# Empreinte des tables de passage des unités partenaires historiques (même nom
# que dans scripts/compute_trade_vulnerabilities.py, importé paresseusement ici)
_CONCORDANCE_FINGERPRINT = "concordance"

# Dimensions des unités de chaque étape (contrôle des filtres de périmètre)
STEP_DIMENSIONS: Mapping[str, FrozenSet[str]] = {
    "partners": frozenset({"classification", "reporter", "product"}),
    "network": frozenset({"vintage"}),
    "baci": frozenset({"vintage"}),
    "synthesis": frozenset({"scope"}),
    "coherence": frozenset({"scope"}),
}

# Dimensions auxquelles s'applique chaque filtre de périmètre
_FILTER_DIMENSIONS: Mapping[str, FrozenSet[str]] = {
    "reporters": frozenset({"reporter"}),
    "products": frozenset({"product"}),
    "periods": frozenset({"period", "year", "TIME_PERIOD"}),
    "vintages": frozenset({"vintage", "classification"}),
}

# Nombre maximal d'unités énumérées dans le journal
_LOG_UNITS = 20


# Fonction de listage des noms invalidables d'une étape
def step_names(step: str) -> Tuple[str, ...]:
    """Names whose fingerprint a step records.

    Partner and network steps record one fingerprint per metric; the
    historical partner units also record the fingerprint of the
    correspondence tables they were converted with (``concordance``); BACI,
    the synthesis and the coherence record a single fingerprint named after
    the step.

    Args:
        step: Step name (one of :data:`STEPS`).

    Returns:
        The names, in registry order.

    Raises:
        ValueError: If the step is unknown.

    Examples:
        >>> step_names("partners")
        ('HHI', 'CDI2', 'CDI3', 'concordance')
        >>> step_names("baci")
        ('baci',)
    """
    if step == "partners":
        return (*(cls.name for cls in DEFAULT_METRIC_CLASSES), _CONCORDANCE_FINGERPRINT)
    if step == "network":
        return tuple(cls.name for cls in DEFAULT_NETWORK_METRIC_CLASSES)
    if step in STEPS:
        return (step,)
    raise ValueError(f"Unknown step '{step}', expected one of {list(STEPS)}")


# Fonction de validation des noms à invalider
def resolve_names(step: str, names: Sequence[str]) -> Optional[FrozenSet[str]]:
    """Validate the names to invalidate.

    A typo would otherwise silently invalidate nothing. The partner and network
    steps record one fingerprint per metric and flow direction
    (``HHI/import``): a plain metric name invalidates every direction, a name
    qualified by a direction (``HHI/export``) only that one.

    Args:
        step: Step name.
        names: Requested names (empty: every fingerprint of the units).

    Returns:
        The names, or ``None`` for every fingerprint.

    Raises:
        ValueError: If a name is not recorded by the step.

    Examples:
        >>> resolve_names("partners", ["HHI"])
        frozenset({'HHI'})
        >>> resolve_names("partners", ["HHI/export"])
        frozenset({'HHI/export'})
        >>> resolve_names("partners", []) is None
        True
    """
    if not names:
        return None

    # Nom connu de l'étape, éventuellement qualifié par un sens de flux connu
    def _known(name: str) -> bool:
        base, qualifier = split_qualified(name)
        return base in step_names(step) and (qualifier is None or qualifier in FLOW_NAMES)

    unknown = sorted(name for name in set(names) if not _known(name))
    if unknown:
        raise ValueError(
            f"Unknown name(s) {unknown} for step '{step}', expected among {list(step_names(step))}"
        )
    return frozenset(names)


# Fonction de construction du périmètre d'invalidation
def build_scope(
    step: str,
    *,
    reporters: Any = None,
    products: Any = None,
    periods: Any = None,
    vintages: Any = None,
) -> Optional[ForceSpec]:
    """Build the unit filter of an invalidation.

    A filter on a dimension the units of the step do not have is rejected,
    rather than ignored as for a forcing: ignoring it would invalidate every
    unit of the step instead of the intended subset.

    Args:
        step: Step name.
        reporters: Reporter codes (``,`` or ``;`` separated).
        products: Product code prefixes.
        periods: Periods.
        vintages: Vintages or classifications.

    Returns:
        The filter, or ``None`` when no filter is given (every unit).

    Raises:
        ValueError: If a filter does not apply to the units of the step.

    Examples:
        >>> build_scope("partners", reporters="FR;DE").reporters
        ('FR', 'DE')
        >>> build_scope("synthesis") is None
        True
    """
    values = {
        "reporters": parse_force_list(reporters),
        "products": parse_force_list(products),
        "periods": parse_force_list(periods),
        "vintages": parse_force_list(vintages),
    }
    for name, given in values.items():
        if given and not (_FILTER_DIMENSIONS[name] & STEP_DIMENSIONS[step]):
            raise ValueError(
                f"Filter '{name}' does not apply to step '{step}' "
                f"(unit dimensions: {sorted(STEP_DIMENSIONS[step])})"
            )
    if not any(values.values()):
        return None
    return ForceSpec(**values)


# Fonction de construction du registre de fraîcheur d'une étape
def build_registry(step: str) -> FreshnessRegistry:
    """Build the freshness registry of a step from the environment configuration.

    The very same builders as the steps are used, so the paths, buckets and
    version-1 fallbacks are identical.

    Args:
        step: Step name.

    Returns:
        The registry.

    Raises:
        ValueError: If the step is unknown.
    """
    # Imports paresseux : chaque étape ne charge que ses propres dépendances
    if step == "partners":
        from scripts.compute_trade_vulnerabilities import (
            load_eurostat_config,
            load_vulnerability_config,
            partner_classification,
            partner_registry,
        )
        from scripts.download_comtrade import load_runtime_config

        dataflow = load_eurostat_config()["DATAFLOW"]
        block = load_vulnerability_config()["VULNERABILITIES"][dataflow]
        classification = partner_classification(load_runtime_config()["NOMENCLATURES"]["HS"])
        return partner_registry(block, classification)
    if step == "network":
        from scripts.compute_network_vulnerabilities import (
            load_vulnerability_config,
            network_registry,
        )

        return network_registry(load_vulnerability_config()["NETWORK_VULNERABILITIES"])
    if step == "baci":
        from scripts.process_baci_hs import baci_registry, load_baci_config

        return baci_registry(load_baci_config())
    if step in {"synthesis", "coherence"}:
        from scripts.compute_synthetic_scores import global_registry, load_synthesis_config

        synthesis_file = load_synthesis_config()
        block = synthesis_file[step.upper()]
        return global_registry(
            block["PATHS"]["LAST_COMPUTATION_PATH"],
            synthesis_file["SYNTHESIS"]["BUCKET"],
            step,
            step.upper(),
        )
    raise ValueError(f"Unknown step '{step}', expected one of {list(STEPS)}")


# Fonction d'invalidation d'une étape
def invalidate_step(
    registry: FreshnessRegistry,
    names: Optional[FrozenSet[str]],
    scope: Optional[ForceSpec],
    *,
    dry_run: bool = False,
) -> List[Unit]:
    """Invalidate the fingerprints of a step, then persist them (unless dry run).

    Args:
        registry: Freshness registry of the step.
        names: Names whose fingerprint is deleted (``None``: all).
        scope: Unit filter (``None``: every unit).
        dry_run: When true, nothing is written.

    Returns:
        The units invalidated (or that would be, in a dry run).
    """
    units = registry.invalidate(names, scope)
    if not dry_run:
        registry.save()
    return units


# Fonction de lecture des arguments de la ligne de commande
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse the command line.

    Args:
        argv: Arguments (``sys.argv[1:]`` by default).

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Invalidate recorded fingerprints so that the next run of the step "
            "recomputes the units (procedure after fixing an implementation)."
        )
    )
    parser.add_argument("--step", required=True, choices=STEPS, help="Step whose registry is invalidated.")
    parser.add_argument(
        "--metrics", default="",
        help="Names whose fingerprint is deleted (',' or ';' separated); default: all.",
    )
    parser.add_argument("--reporters", default="", help="Reporter codes (partners).")
    parser.add_argument("--products", default="", help="Product code prefixes (partners).")
    parser.add_argument("--periods", default="", help="Periods (units carrying a period).")
    parser.add_argument("--vintages", default="", help="Vintages or classifications.")
    parser.add_argument("--dry-run", action="store_true", help="List the units without writing.")
    return parser.parse_args(argv)


# Fonction principale
def main(
    argv: Optional[Sequence[str]] = None,
    registry_factory: Callable[[str], FreshnessRegistry] = build_registry,
) -> int:
    """CLI entry point of the invalidation.

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` by default).
        registry_factory: Builder of the step registry (the configuration-based
            one by default; replaced in tests).

    Returns:
        Exit code: ``0`` on success, ``2`` on an invalid argument.
    """
    args = parse_args(argv)
    try:
        names = resolve_names(args.step, parse_force_list(args.metrics))
        scope = build_scope(
            args.step,
            reporters=args.reporters,
            products=args.products,
            periods=args.periods,
            vintages=args.vintages,
        )
    except ValueError as exc:
        logger.error(str(exc))
        return 2

    units = invalidate_step(registry_factory(args.step), names, scope, dry_run=args.dry_run)

    # Logging
    shown = ", ".join(unit.key for unit in units[:_LOG_UNITS])
    more = f" (+{len(units) - _LOG_UNITS})" if len(units) > _LOG_UNITS else ""
    logger.info(
        "%s%d unité(s) invalidée(s) pour l'étape '%s' : %s%s",
        "[aperçu, rien n'est écrit] " if args.dry_run else "",
        len(units), args.step, shown or "aucune", more,
    )
    if units and not args.dry_run:
        logger.info("Elles seront recalculées à la prochaine exécution de l'étape '%s'.", args.step)
    return 0


# Exécution du script principal
if __name__ == "__main__":
    raise SystemExit(main())
