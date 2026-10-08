"""Script de calcul/mise à jour des indicateurs de vulnérabilité de réseau.

Recalcule, pour chaque millésime de nomenclature HS du redressement BACI, les
indicateurs portant sur le graphe mondial des échanges d'un produit — risque de
centralité, clustering pondéré, diamètre, concentration des exportations
mondiales et risque de point de défaillance unique (cf.
`macroforecast.trade.vulnerabilities.network_metrics`). La cellule de sortie est
un quadruplet `nomenclature x produit x année x flux`, le millésime étant la clé
primaire supplémentaire que la table des indicateurs partenaires ne porte pas.

Chaque sens de `NETWORK_VULNERABILITIES.FLOWS` est calculé sur la même matrice
BACI : tel quel à l'import (concentration de l'offre mondiale), transposé à
l'export (concentration de la demande mondiale). La colonne `flow` porte les
mêmes codes que la table partenaires, ce qui ramène la jointure de synthèse à
une égalité. Les empreintes sont tenues par métrique et par sens
(`SPOF/export`) : ajouter un sens ne recalcule que ce sens. Les métriques MLflow
sont préfixées par sens (`network/import/...`, `network/export/...`).

Script distinct de `compute_trade_vulnerabilities.py`, et non une étape de plus
dans celui-ci : les deux familles n'ont ni la même source (flux réconciliés BACI
dans le catalogue COMTRADE contre flux Eurostat Comext), ni la même clé de
sortie, ni la même dépendance amont (`process_baci_hs.py` contre
`download_eurostat_comext.py`), ni le même registre de fraîcheur. Deux nœuds
Argo ordonnançables indépendamment, deux domaines d'échec, deux runs MLflow.

Le périmètre recalculé est décidé par le registre de fraîcheur fragmenté de cette
étape (`STATE.PATH_TEMPLATE`, un fichier par millésime), confronté au registre
BACI (lecture seule, `STATE` de `baci.yaml`) : un millésime est recalculé s'il ne
l'a jamais été, si sa dernière passe BACI terminée est postérieure à son dernier
calcul, si l'empreinte d'une métrique a changé ou en cas de forçage. Un millésime
dont la passe BACI est interrompue n'est pas scoré (sa table mélange deux
ajustements). La maille est le millésime entier, et non l'année : une passe BACI
réestime la gravité et la qualité des déclarants sur toute sa tranche
temporelle, donc toutes ses années bougent ensemble — prétendre à une
granularité annuelle serait faux.

Comme `process_baci_hs.py`, l'échec d'un millésime n'interrompt pas les autres :
chaque échec est capturé et journalisé, et le script ne sort en erreur qu'en fin
de parcours. Seuls les millésimes réussis voient leur date de calcul avancer.

Le suivi d'exécution MLflow est piloté par le bloc
`NETWORK_VULNERABILITIES.MLFLOW` de `config/vulnerabilities.yaml` : sans
`TRACKING_URI` (ou sans serveur joignable), `get_tracker` retourne un objet nul
et l'exécution est strictement inchangée. La relecture du résultat précédent,
qui alimente les diagnostics de dérive, est faite ici — jamais par le module de
calcul.

Chaque ligne porte `in_force` : vrai quand le millésime de la ligne est celui en
vigueur l'année de la ligne (`runtime.NOMENCLATURES.HS`). Les lignes d'un
millésime ancien sur des années postérieures sont la version « historique » des
métriques, jointe aux lignes partenaires historiques de même millésime.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import fields, replace
from datetime import datetime
import logging
import os
from pathlib import Path
from functools import partial
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
import yaml

# Modules de manipulation de données
import narwhals as nw

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    DuckLakeTable,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
    workflow_run_id,
)
# Module d'utilitaires de téléchargement
from statflows.core.download import _now, _parse_iso, _schema_name
# Registres de fraîcheur v2 (fragments, empreintes, forçage)
from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    LegacySource,
    RegistryEntry,
    Unit,
    UnitPlan,
    adopt_legacy_flag,
    fingerprint,
    legacy_entry,
    plan_metrics,
    qualifiers_to_compute,
    units_to_compute,
)
# Registre BACI (amont, lecture seule) et complétude d'une passe
from scripts.process_baci_hs import baci_registry, pass_is_complete
# Paramètres d'exécution partagés (forçage ponctuel)
from scripts.download_comtrade import load_runtime_config
# Millésime SH en vigueur une année donnée
from kedro_pipeline.config import vintage_in_force

# Module de suivi d'exécution (MLflow optionnel, objet nul par défaut)
from macroforecast.tracking import CapturingTracker, get_tracker
from macroforecast.tracking.figures import (
    key_figures_network_vulnerabilities,
    sections_network_vulnerabilities,
)
from macroforecast.tracking.report import Units
from scripts._run_report import RunScope, flow_run_metrics, guarded_run, run_name
# Lecture des sens de flux calculés (règle commune aux deux familles)
from scripts.compute_trade_vulnerabilities import load_flows
# Module de calcul des indicateurs
from macroforecast.trade.vulnerabilities import (
    DEFAULT_NETWORK_CONFIG,
    DEFAULT_NETWORK_METRIC_CLASSES,
    NetworkVulnerabilityConfig,
)
from macroforecast.trade.vulnerabilities.runner import (
    read_previous_network_result,
    run_network_vulnerabilities,
)

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé racine du registre JSON des dates de dernier traitement BACI (écrit par
# scripts/process_baci_hs.py, lu seulement ici)
_PROCESSING_ROOT = "BACI"
# Clé racine du registre JSON des dates de dernier calcul, tenu par ce script
_REGISTRY_ROOT = "NETWORK_VULNERABILITIES"
# Bloc de configuration dédié aux indicateurs de réseau
_CONFIG_ROOT = "NETWORK_VULNERABILITIES"
# Clé YAML portant le backend de calcul narwhals (hors NetworkVulnerabilityConfig)
_BACKEND_KEY = "BACKEND"
# Préfixe des métriques MLflow de l'étape (suivi du sens : network/import/...)
_METRICS_FAMILY = "network"
# Colonne du drapeau « millésime en vigueur l'année de la ligne » (fait de schéma,
# lu par la couche de service)
_IN_FORCE_COL = "in_force"


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

# Fonction de chargement de la configuration associée à la base comtrade
def load_comtrade_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load configuration from file.

    Args:
        config_path: Path to config file. If None, uses default location or
            the COMTRADE_CONFIG_PATH environment variable.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        # Priorité 1 : variable d'environnement (pour flexibilité Kubernetes)
        config_path = os.environ.get(
            "COMTRADE_CONFIG_PATH", "config/datasets/comtrade.yaml"
        )

    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de chargement de la configuration du redressement BACI
def load_baci_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load configuration from file.

    Read for two things only: the HS vintages to score
    (``CLASSIFICATIONS.TARGETS``, i.e. which source schemas exist) and the path
    of the BACI processing registry (``PATHS.LAST_PROCESSING_PATH``). No
    methodological BACI parameter is used here.

    Args:
        config_path: Path to config file. If None, uses default location or
            the BACI_CONFIG_PATH environment variable.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        # Priorité 1 : variable d'environnement (pour flexibilité Kubernetes)
        config_path = os.environ.get("BACI_CONFIG_PATH", "config/baci.yaml")

    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de chargement de la configuration dédiée au calcul des vulnérabilités
def load_vulnerability_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the vulnerability-computation configuration from file.

    Same file as ``compute_trade_vulnerabilities.py`` — the two families write
    into the same DuckLake catalog — but a block of its own
    (``NETWORK_VULNERABILITIES``), so neither script can be perturbed by the
    other's settings.

    Args:
        config_path: Path to config file. If None, uses default location
                     or VULNERABILITIES_CONFIG_PATH environment variable.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        config_path = os.environ.get(
            "VULNERABILITIES_CONFIG_PATH", "config/vulnerabilities.yaml"
        )
    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de construction de la configuration méthodologique des métriques de réseau
def network_config_from_params(params: Optional[Dict]) -> NetworkVulnerabilityConfig:
    """Build a ``NetworkVulnerabilityConfig`` from the YAML ``PARAMETERS`` section.

    Generic construction, twin of ``vulnerability_config_from_params``: every key
    matching a ``NetworkVulnerabilityConfig`` field name overrides the dataclass
    default; unknown keys are ignored with a warning. YAML lists are coerced to
    the tuple types the frozen dataclass expects, nested pairs included
    (``metric_alert_thresholds``). The ``BACKEND`` key is skipped: it drives the
    narwhals execution backend, not the methodology.

    Args:
        params: The ``NETWORK_VULNERABILITIES.PARAMETERS`` mapping of
            ``config/vulnerabilities.yaml`` (or ``None``, meaning the default
            BACI conventions).

    Returns:
        A ``NetworkVulnerabilityConfig`` reflecting the configured overrides.
    """
    # Aucune surcharge : configuration par défaut
    if not params:
        return DEFAULT_NETWORK_CONFIG

    # Surcharge générique champ à champ, avec coercition listes → tuples
    valid = {field.name for field in fields(NetworkVulnerabilityConfig)}
    overrides: Dict[str, Any] = {}
    for key, value in params.items():
        # Backend d'exécution : lu à part, pas un paramètre méthodologique
        if key == _BACKEND_KEY:
            continue
        if key not in valid:
            # Logging
            logger.warning(f"Paramètre de vulnérabilité réseau inconnu ignoré : {key}")
            continue
        default = getattr(DEFAULT_NETWORK_CONFIG, key)
        if isinstance(default, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(item) if isinstance(item, (list, tuple)) else item
                for item in value
            )
        overrides[key] = value

    return replace(DEFAULT_NETWORK_CONFIG, **overrides)


# ──────────────────────────────────────────────────────────────────────
# Registres de fraîcheur : traitement BACI (lecture) et calcul (lecture/écriture)
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : extraction des dates d'un registre indexé par schéma
def _dates_by_schema(
    registry: Mapping[str, Mapping[str, Any]],
    field_name: str,
) -> Dict[str, datetime]:
    """Index the dated field of a registry by result schema.

    Both registries confronted here share the same shape — one entry per result
    schema, carrying an ISO instant — so they share the same reader; only the
    name of the dated field changes.

    Args:
        registry: Entries of the registry, keyed by result schema.
        field_name: Name of the field holding the ISO instant.

    Returns:
        Mapping ``result_schema -> instant`` (UTC-aware datetime), entries
        without a parsable instant being dropped.

    Examples:
        >>> registry = {"baci_hs2017": {"last_processed": "2026-01-02T03:04:05+00:00"}}
        >>> _dates_by_schema(registry, "last_processed")["baci_hs2017"].year
        2026
        >>> _dates_by_schema({"baci_hs2017": {}}, "last_processed")
        {}
    """
    # Parcours des entrées, dates non exploitables écartées
    dates: Dict[str, datetime] = {}
    for schema, entry in registry.items():
        when = _parse_iso(entry.get(field_name))
        if when is not None:
            dates[schema] = when
    return dates


# Fonction de lecture des dates de dernier traitement BACI, par schéma résultat
def load_last_processing_dates(
    last_processing_path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Dict[str, datetime]:
    """Read the BACI processing registry (empty when it does not exist yet).

    Read-only: this script never writes into the registry of the BACI step, and
    the BACI step never reads this one — the coupling between the two is that
    single JSON file.

    Args:
        last_processing_path: Path to the ``LAST_PROCESSING_PATH`` registry
            (cf. ``baci.yaml`` / ``process_baci_hs.py``).
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        Mapping ``result_schema -> last_processed`` (UTC-aware datetime).
    """
    # Lecture du registre (racine "BACI") et indexation par schéma résultat
    registry = (
        loader.load(last_processing_path, bucket=bucket, missing_ok=True) or {}
    ).get(_PROCESSING_ROOT, {})
    return _dates_by_schema(registry, "last_processed")


# Fonction de lecture des dates de dernier calcul, par schéma source
def load_last_computation_dates(
    last_computation_path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Dict[str, datetime]:
    """Read the network-computation registry (empty if it does not exist yet).

    Args:
        last_computation_path: Path to the network-computation registry.
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        Mapping ``source_schema -> last_computed`` (UTC-aware datetime).
    """
    # Lecture du registre et indexation par schéma source
    registry = (
        loader.load(last_computation_path, bucket=bucket, missing_ok=True) or {}
    ).get(_REGISTRY_ROOT, {})
    return _dates_by_schema(registry, "last_computed")


# Fonction de fusion et de sauvegarde des dates de calcul mises à jour
def save_last_computation_dates(
    last_computation_path: Path,
    entries: Mapping[str, Dict[str, Any]],
    loader: Loader,
    saver: Saver,
    bucket: Optional[str],
) -> None:
    """Merge the recomputed vintages into the registry and persist it.

    Args:
        last_computation_path: Path to the network-computation registry.
        entries: Registry entries of the vintages just computed, keyed by source
            schema. Merged into the existing registry; every other entry is
            preserved untouched.
        loader: ``Loader`` instance (to load the existing registry before merging).
        saver: ``Saver`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
    """
    # Fusion avec le registre existant : seules les entrées recalculées bougent
    registry = (
        loader.load(last_computation_path, bucket=bucket, missing_ok=True) or {}
    ).get(_REGISTRY_ROOT, {})
    registry.update(entries)
    # Écriture du registre mis à jour
    saver.save(
        last_computation_path,
        {_REGISTRY_ROOT: registry},
        bucket=bucket,
        indent=2,
        ensure_ascii=False,
    )
    # Logging
    logger.info(
        f"{len(entries)} date(s) de calcul mise(s) à jour dans "
        f"'{last_computation_path}'"
    )


# ──────────────────────────────────────────────────────────────────────
# Détermination des millésimes à (re)calculer
# ──────────────────────────────────────────────────────────────────────

# Fonction de sélection des millésimes dont les scores sont périmés
def vintages_to_recompute(
    targets: Mapping[str, str],
    last_processed: Mapping[str, datetime],
    last_computed: Mapping[str, datetime],
) -> List[Tuple[str, str]]:
    """Select the HS vintages whose network scores are stale.

    A vintage is selected when its BACI slice was never scored, or scored before
    its most recent BACI pass. A vintage absent from the BACI registry has never
    been produced and is skipped — there is nothing to read for it — rather than
    scored on a table that may not exist.

    Args:
        targets: Configured vintages, mapping the label (``"HS2017"``) to its
            BACI result schema (``"baci_hs2017"``).
        last_processed: Last BACI processing date per source schema.
        last_computed: Last network computation date per source schema.

    Returns:
        Sorted list of ``(label, source_schema)`` pairs to (re)compute.

    Examples:
        >>> from datetime import datetime, timezone
        >>> old = datetime(2026, 1, 1, tzinfo=timezone.utc)
        >>> new = datetime(2026, 6, 1, tzinfo=timezone.utc)
        >>> targets = {"HS2017": "baci_hs2017", "HS2022": "baci_hs2022"}
        >>> vintages_to_recompute(targets, {"baci_hs2017": new}, {})
        [('HS2017', 'baci_hs2017')]
        >>> vintages_to_recompute(
        ...     targets, {"baci_hs2017": old}, {"baci_hs2017": new})
        []
    """
    # Millésimes configurés mais jamais produits par BACI : rien à lire
    unknown = sorted(label for label, schema in targets.items() if schema not in last_processed)
    if unknown:
        # Logging
        logger.warning(
            f"Millésime(s) absent(s) du registre de traitement BACI, ignoré(s) : "
            f"{unknown}"
        )

    # Millésimes jamais calculés ou calculés avant la dernière passe BACI
    return sorted(
        (label, schema)
        for label, schema in targets.items()
        if schema in last_processed
        and (
            schema not in last_computed
            or last_computed[schema] < last_processed[schema]
        )
    )


# ──────────────────────────────────────────────────────────────────────
# Registre de fraîcheur v2 : un fragment par millésime
# ──────────────────────────────────────────────────────────────────────

# Nom de l'étape (forçage FORCE_STEPS, champ « step » des fragments)
STEP = "network"


# Fonction de calcul des empreintes demandées des métriques de réseau
def network_requested(
    config: NetworkVulnerabilityConfig,
    flows: Sequence[str] = ("import",),
    metric_classes: Sequence[type] = DEFAULT_NETWORK_METRIC_CLASSES,
) -> Dict[str, str]:
    """Current methodological fingerprint of every network metric and direction.

    Keyed by ``"<metric>/<flow>"``, like the partner step: two instances share
    a column name, and adding a direction must only make that direction stale.
    The orientation-invariant metrics (clustering, diameter) also get one key
    per direction, their value being written on the rows of each.

    Args:
        config: Methodological configuration of the network metrics.
        flows: Directions computed by the step.
        metric_classes: Metric classes computed by the step (each in the
            directions it supports).

    Returns:
        Mapping ``"<metric>/<flow>" -> fingerprint`` (name, result-shaping
        configuration fields and direction).

    Examples:
        >>> "SPOF/import" in network_requested(NetworkVulnerabilityConfig())
        True
        >>> "SPOF/export" in network_requested(NetworkVulnerabilityConfig(), ("import", "export"))
        True
    """
    metrics = [
        cls(config, flow=flow)
        for flow in flows
        for cls in metric_classes
        if flow in cls.supported_flows
    ]
    return {
        metric.fingerprint_key: fingerprint(metric.name, metric.fingerprint_params())
        for metric in metrics
    }


# Fonction de lecture du registre v1 du réseau en entrées héritées
def _parse_legacy_network(data: Mapping[str, Any]) -> Iterator[RegistryEntry]:
    """Turn the version-1 network registry (keyed by source schema) into legacy entries.

    Args:
        data: Version-1 document (``{"NETWORK_VULNERABILITIES": {schema: {...}}}``).

    Yields:
        One legacy entry per vintage recorded.
    """
    for item in (data.get(_REGISTRY_ROOT) or {}).values():
        if not isinstance(item, Mapping) or not item.get("vintage"):
            continue
        yield legacy_entry(
            Unit.of(vintage=item["vintage"]),
            item.get("last_computed"),
            n_cells=item.get("n_cells"),
        )


# Fonction de construction du registre de fraîcheur du réseau
def network_registry(
    network_config: Mapping[str, Any],
    *,
    loader: Optional[Loader] = None,
    saver: Optional[Saver] = None,
) -> FreshnessRegistry:
    """Build the network freshness registry (one fragment per vintage).

    Args:
        network_config: ``NETWORK_VULNERABILITIES`` block of
            ``config/vulnerabilities.yaml`` (``BUCKET``, ``STATE``,
            ``PATHS.LAST_COMPUTATION_PATH`` read as the version-1 fallback).
        loader: JSON loader (a fresh one by default).
        saver: JSON saver (a fresh one by default).

    Returns:
        The registry.

    Raises:
        KeyError: If the block has no ``STATE.PATH_TEMPLATE``.
    """
    bucket = network_config.get("BUCKET")
    legacy_path = (network_config.get("PATHS") or {}).get("LAST_COMPUTATION_PATH")
    return FreshnessRegistry(
        network_config["STATE"]["PATH_TEMPLATE"],
        bucket,
        STEP,
        shard_of=lambda unit: unit.get("vintage"),
        legacy=LegacySource(legacy_path, bucket, _parse_legacy_network) if legacy_path else None,
        loader=loader,
        saver=saver,
    )


# Fonction de construction des unités réseau et de leur watermark amont
def network_upstream(
    baci: FreshnessRegistry,
    targets: Mapping[str, str],
) -> Dict[Unit, datetime]:
    """Vintages that can be scored, with the last BACI computation as watermark.

    A vintage is left out (with a warning) when BACI never produced it, or
    when its last BACI pass is incomplete (``years_written`` differs from
    ``years_scope``): its table then mixes two estimation passes and must not
    be scored before the pass is resumed.

    Args:
        baci: BACI freshness registry (read only).
        targets: Configured vintages, label -> BACI result schema.

    Returns:
        Mapping ``Unit(vintage) -> last BACI computation``.
    """
    units: Dict[Unit, datetime] = {}
    skipped: List[str] = []
    for label in targets:
        entry = baci.get(Unit.of(vintage=label))
        if entry is None or entry.last_computed is None or not pass_is_complete(entry):
            skipped.append(label)
            continue
        units[Unit.of(vintage=label)] = entry.last_computed
    if skipped:
        # Logging
        logger.warning(
            f"Millésime(s) sans passe BACI terminée, ignoré(s) : {sorted(skipped)}"
        )
    return units


# Fonction de décision des millésimes à (re)calculer
def plan_network_units(
    registry: FreshnessRegistry,
    units: Mapping[Unit, datetime],
    requested: Mapping[str, str],
    force: ForceSpec,
    *,
    adopt_legacy_fingerprints: bool = False,
) -> Dict[Unit, UnitPlan]:
    """Decide which vintages to (re)score.

    Args:
        registry: Network freshness registry.
        units: Scorable vintages and their last BACI computation
            (:func:`network_upstream`).
        requested: Current metric fingerprints (:func:`network_requested`).
        force: One-off forcing.
        adopt_legacy_fingerprints: Deployment migration flag.

    Returns:
        Mapping ``unit -> plan``; every metric of a planned vintage is
        recomputed.
    """
    return units_to_compute(
        units, registry, units, requested, force,
        step=STEP, adopt_legacy_fingerprints=adopt_legacy_fingerprints,
    )


# ──────────────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────

# Préfixe du nœud du rapport de run : un run par millésime (clé « compute_network_vulnerabilities* »
# de config/tracking.yaml)
NODE = "compute_network_vulnerabilities"


# Fonction d'ajout du drapeau « millésime en vigueur » aux scores de réseau
def annotate_network_in_force(
    result: nw.DataFrame,
    *,
    nomenclatures: Mapping[str, int],
    config: NetworkVulnerabilityConfig,
) -> nw.DataFrame:
    """Add ``in_force``: whether the row's vintage is the one in force its year.

    Args:
        result: Network scores of one vintage (classification and year columns).
        nomenclatures: Mapping vintage label -> entry-into-force year.
        config: Column conventions (``classification_col``, ``period_col``).

    Returns:
        The scores with a boolean ``in_force`` column.

    Examples:
        >>> import pandas as pd
        >>> frame = nw.from_native(pd.DataFrame({"classification": "HS2017", "year": [2019, 2023]}),
        ...                        eager_only=True)
        >>> annotate_network_in_force(frame, nomenclatures={"HS2017": 2017, "HS2022": 2022},
        ...                           config=NetworkVulnerabilityConfig()).to_native()["in_force"].tolist()
        [True, False]
    """
    years = result.get_column(config.period_col).cast(nw.Int64)
    mapping = {year: vintage_in_force(year, nomenclatures) for year in years.unique().to_list()}
    in_force_vintage = years.replace_strict(mapping, return_dtype=nw.String)
    return result.with_columns(
        (nw.col(config.classification_col) == in_force_vintage).alias(_IN_FORCE_COL)
    )


# Fonction principale de calcul des vulnérabilités de réseau
def main() -> None:
    """CLI entry point for the incremental network-vulnerability computation.

    Raises:
        RuntimeError: If at least one vintage failed, once every stale vintage
            has been attempted.
    """
    # Chargement des configurations : catalogue source, millésimes BACI, et
    # paramètres propres au calcul des indicateurs de réseau
    comtrade_config = load_comtrade_config()
    baci_config = load_baci_config()
    vulnerability_config = load_vulnerability_config()
    network_config = vulnerability_config[_CONFIG_ROOT]
    runtime_config = load_runtime_config()

    # Construction des paramètres méthodologiques (seuils, conventions de colonnes)
    parameters = network_config.get("PARAMETERS") or {}
    network_parameters = network_config_from_params(parameters)
    backend = parameters.get(_BACKEND_KEY, "pandas")
    # Sens de flux calculés (bloc réseau, même valeur que celle des partenaires)
    flows = load_flows(network_config)

    # Options de suivi d'exécution (un run par millésime, construit dans la boucle)
    mlflow_config = network_config.get("MLFLOW") or {}
    log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))
    measure_drift = bool(mlflow_config.get("DRIFT", True))

    # Dataflow COMTRADE dont sont issus les flux redressés
    DATAFLOW = comtrade_config["DATAFLOW"]

    # Millésimes configurés : label → schéma source (résultat du redressement BACI)
    targets = {
        label: _schema_name(target_cfg["RESULT_SCHEMA"])
        for label, target_cfg in baci_config["CLASSIFICATIONS"]["TARGETS"].items()
    }

    # Unités candidates : millésimes dont la dernière passe BACI est terminée,
    # avec son dernier calcul comme watermark amont (lecture seule)
    units = network_upstream(baci_registry(baci_config), targets)

    # Registre de fraîcheur fragmenté, empreintes courantes et forçage ponctuel
    registry = network_registry(network_config)
    requested = network_requested(network_parameters, flows)
    force = ForceSpec.from_runtime(runtime_config)
    plans = plan_network_units(
        registry, units, requested, force,
        adopt_legacy_fingerprints=adopt_legacy_flag(network_config.get("STATE")),
    )
    stale = sorted((unit.get("vintage"), targets[unit.get("vintage")]) for unit in plans)

    # Logging
    logger.info(
        f"{len(stale)} millésime(s) à recalculer : "
        f"{ {unit.get('vintage'): plan.reason for unit, plan in plans.items()} }"
    )

    # Sortie anticipée : rien à recalculer (entrées v1 adoptées écrites malgré tout)
    if not stale:
        registry.save()
        logger.info("Nothing to recompute, stop.")
        return

    # Instant de référence capturé avant le calcul : la date enregistrée
    # correspond au début du traitement, jamais après, pour ne pas rater une
    # mise à jour survenue pendant le calcul
    computed_at = _now()

    # Identifiants du catalogue et du stockage (lus une fois dans l'environnement)
    pg_credentials = pg_credentials_from_env()
    s3_credentials = s3_credentials_from_env()

    # Connecteur DuckLake aux flux redressés (catalogue COMTRADE, un schéma par
    # millésime — cf. scripts/process_baci_hs.py)
    source_connector = build_connector(
        DuckLakeLocation(
            dbname=comtrade_config["DOWNLOADS"]["DBNAME"],
            catalog_alias=comtrade_config["DOWNLOADS"]["CATALOG_ALIAS"],
            schema=targets[stale[0][0]],
            bucket=comtrade_config["DOWNLOADS"][DATAFLOW]["BUCKET"],
            data_path=comtrade_config["DOWNLOADS"][DATAFLOW]["PATHS"]["DATA_PATH"],
        ),
        pg=pg_credentials,
        s3=s3_credentials,
    )

    # Schéma résultat, commun à la relecture et à l'écriture
    result_schema = _schema_name(network_config["RESULT_SCHEMA"])

    # Connecteur DuckLake résultat : catalogue des vulnérabilités, partagé avec
    # les indicateurs partenaires, positionné sur le schéma dédié au réseau
    result_connector = build_connector(
        DuckLakeLocation(
            dbname=vulnerability_config["VULNERABILITIES"]["DBNAME"],
            catalog_alias=vulnerability_config["VULNERABILITIES"]["CATALOG_ALIAS"],
            schema=result_schema,
            bucket=network_config["BUCKET"],
            data_path=network_config["PATHS"]["DATA_PATH"],
        ),
        pg=pg_credentials,
        s3=s3_credentials,
    )

    # Ouverture des connexions : leur cycle de vie appartient au script, le
    # runner ne les ouvre ni ne les ferme (cf. `run_network_vulnerabilities`)
    source_conn = source_connector.connect()
    failures: Dict[str, Exception] = {}
    try:
        result_conn = result_connector.connect()
        try:
            # Un millésime après l'autre : l'échec de l'un n'emporte pas les autres
            for label, source_schema in stale:
                try:
                    # Un run par millésime, taggé, comme dans process_baci_hs.py
                    node = f"{NODE}_{label}"
                    tracker = CapturingTracker(
                        get_tracker(
                            tracking_uri=mlflow_config.get("TRACKING_URI"),
                            experiment=mlflow_config.get(
                                "EXPERIMENT", "trade-03-vulnerabilities"
                            ),
                            run_name=run_name(
                                f"network-vulnerabilities-{label}-{datetime.now():%Y%m%d-%H%M}", node
                            ),
                            tags={"vintage": label},
                        )
                    )
                    scope = RunScope(node)
                    unit = Unit.of(vintage=label)
                    plan = plans[unit]
                    # Sens à recalculer pour ce millésime : ceux que nomme le plan
                    computed_flows = qualifiers_to_compute({unit: plan}, flows)
                    with tracker, guarded_run(scope, tracker):
                        # Fraîcheur : décision du millésime et tag de forçage
                        tracker.log_metrics(plan_metrics({unit: plan}, n_candidates=len(units)))
                        tracker.set_tags({"freshness_reason": plan.reason})
                        if force.forces_step(STEP, requested):
                            tracker.set_tags({"forced": force.describe()})

                        # Résultat de l'exécution précédente sur ce millésime :
                        # la lecture appartient au script (principe P4), et son
                        # absence désactive simplement la mesure de dérive
                        df_previous = (
                            read_previous_network_result(
                                result_conn,
                                result_connector.catalog_alias,
                                result_schema,
                                classification=label,
                                config=network_parameters,
                            )
                            if measure_drift
                            else None
                        )

                        # Calcul du millésime et upsert dans le schéma résultat
                        report = run_network_vulnerabilities(
                            source_conn,
                            source_catalog_alias=source_connector.catalog_alias,
                            source_schema=source_schema,
                            classification=label,
                            result_schema=result_schema,
                            result_conn=result_conn,
                            result_catalog_alias=result_connector.catalog_alias,
                            config=network_parameters,
                            flows=computed_flows,
                            backend=backend,
                            tracker=tracker,
                            log_artifacts=log_artifacts,
                            df_previous=df_previous,
                            writer=DuckLakeTable(
                                result_conn, result_connector.catalog_alias, result_schema,
                                label=label,
                            ).writer(run_id=workflow_run_id(), commit_message=f"{NODE} {label}"),
                            annotate=partial(
                                annotate_network_in_force,
                                nomenclatures=runtime_config["NOMENCLATURES"]["HS"],
                                config=network_parameters,
                            ),
                        )

                        # Envoi des métriques, préfixées par sens (le rapport
                        # connaît sa mise en forme, le préfixe appartient à
                        # l'appelant). Les paramètres sont journalisés par le
                        # runner lui-même ; seuls les tags propres au script restent ici.
                        tracker.log_metrics(flow_run_metrics(report, _METRICS_FAMILY))
                        tracker.set_tags(
                            {
                                "dataflow": DATAFLOW,
                                "source_schema": source_schema,
                                "result_schema": result_schema,
                                "created": str(report.created),
                                "n_cells": str(report.cells),
                                "flows": ",".join(report.flows),
                            }
                        )

                        # Rapport de run du millésime : contrôles, chiffres clés, sections,
                        # publiés avant toute sortie en erreur
                        scope.step = "rapport de run"
                        run_report = scope.build(
                            metrics=tracker.metrics,
                            units=Units(planned=1, succeeded=1, planned_label=f"1 millésime ({label})"),
                            key_figures=key_figures_network_vulnerabilities,
                            sections=lambda m: sections_network_vulnerabilities(m, tracker.tables),
                        )
                        scope.publish(tracker, run_report)

                    # Entrée de registre du millésime calculé, écrite après succès
                    # du calcul et de l'écriture seulement (jamais de date avancée
                    # à tort) ; un fragment par millésime. La raison est conservée
                    # pour la cascade vers la synthèse
                    registry.upsert(
                        RegistryEntry(
                            unit=unit,
                            last_computed=computed_at,
                            upstream_watermark=units[unit],
                            fingerprints=dict(requested),
                            reason=plan.reason,
                            extra={
                                "source_schema": source_schema,
                                "result_schema": result_schema,
                                "n_cells": int(report.cells),
                            },
                        )
                    )
                    registry.save()

                    # Logging
                    logger.info(
                        f"Vulnérabilités de réseau calculées pour {label} : {report}"
                    )
                except Exception as exc:
                    # Journalisation de l'échec, poursuite avec les autres millésimes
                    logger.exception(
                        f"Échec du calcul des vulnérabilités de réseau pour "
                        f"le millésime {label}"
                    )
                    failures[label] = exc
        finally:
            result_conn.close()
    finally:
        source_conn.close()

    # Échec global si au moins un millésime a échoué, une fois tous tentés
    if failures:
        raise RuntimeError(
            f"{len(failures)} millésime(s) en échec sur {len(stale)} : "
            f"{sorted(failures)}"
        ) from next(iter(failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
