"""Script de calcul/mise à jour des indicateurs de vulnérabilité de réseau.

Recalcule, pour chaque millésime de nomenclature HS du redressement BACI, les
indicateurs portant sur le graphe mondial des échanges d'un produit — risque de
centralité, clustering pondéré, diamètre, concentration des exportations
mondiales et risque de point de défaillance unique (cf.
`macroforecast.trade.vulnerabilities.network_metrics`). La cellule de sortie est
un triplet `nomenclature x produit x année`, le millésime étant la clé primaire
supplémentaire que la table des indicateurs partenaires ne porte pas.

Script distinct de `compute_trade_vulnerabilities.py`, et non une étape de plus
dans celui-ci : les deux familles n'ont ni la même source (flux réconciliés BACI
dans le catalogue COMTRADE contre flux Eurostat Comext), ni la même clé de
sortie, ni la même dépendance amont (`process_baci_hs.py` contre
`download_eurostat_comext.py`), ni le même registre de fraîcheur. Deux nœuds
Argo ordonnançables indépendamment, deux domaines d'échec, deux runs MLflow.

Le périmètre recalculé est déterminé par confrontation de deux registres JSON :
la date de dernier traitement de chaque millésime, tenue par `process_baci_hs.py`
(`LAST_PROCESSING_PATH` de `baci.yaml`), et la date de dernier calcul, tenue ici.
Un millésime n'est recalculé que si son BACI a été réécrit depuis. La maille est
le millésime entier, et non l'année : une passe BACI réestime la gravité et la
qualité des déclarants sur toute sa tranche temporelle, donc toutes ses années
bougent ensemble — prétendre à une granularité annuelle serait faux.

Comme `process_baci_hs.py`, l'échec d'un millésime n'interrompt pas les autres :
chaque échec est capturé et journalisé, et le script ne sort en erreur qu'en fin
de parcours. Seuls les millésimes réussis voient leur date de calcul avancer.

Le suivi d'exécution MLflow est piloté par le bloc
`NETWORK_VULNERABILITIES.MLFLOW` de `config/vulnerabilities.yaml` : sans
`TRACKING_URI` (ou sans serveur joignable), `get_tracker` retourne un objet nul
et l'exécution est strictement inchangée. La relecture du résultat précédent,
qui alimente les diagnostics de dérive, est faite ici — jamais par le module de
calcul.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import fields, replace
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple
import yaml

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from macroforecast.storage import Loader, Saver
# Module de connexion à la base de données
from dt_ducklake_manager import DuckLakeConnector
# Module d'utilitaires de téléchargement
from macroforecast.datasets.core.download import _now, _parse_iso, _schema_name

# Module de suivi d'exécution (MLflow optionnel, objet nul par défaut)
from macroforecast.tracking import get_tracker
# Module de calcul des indicateurs
from macroforecast.trade.vulnerabilities import (
    DEFAULT_NETWORK_CONFIG,
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
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────

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

    # Construction des paramètres méthodologiques (seuils, conventions de colonnes)
    parameters = network_config.get("PARAMETERS") or {}
    network_parameters = network_config_from_params(parameters)
    backend = parameters.get(_BACKEND_KEY, "pandas")

    # Options de suivi d'exécution (un run par millésime, construit dans la boucle)
    mlflow_config = network_config.get("MLFLOW") or {}
    log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))
    measure_drift = bool(mlflow_config.get("DRIFT", True))

    # Dataflow COMTRADE dont sont issus les flux redressés
    DATAFLOW = "C_A_HS"

    # Millésimes configurés : label → schéma source (résultat du redressement BACI)
    targets = {
        label: _schema_name(target_cfg["RESULT_SCHEMA"])
        for label, target_cfg in baci_config["CLASSIFICATIONS"]["TARGETS"].items()
    }

    # Initialisation des loaders et savers
    loader = Loader()
    saver = Saver()

    # Dates de dernier traitement BACI (lecture seule du registre du redressement)
    last_processed = load_last_processing_dates(
        last_processing_path=Path(baci_config["PATHS"]["LAST_PROCESSING_PATH"]),
        loader=loader,
        bucket=baci_config["BUCKET"],
    )
    # Dates de dernier calcul des indicateurs de réseau, par millésime
    last_computed = load_last_computation_dates(
        last_computation_path=Path(network_config["PATHS"]["LAST_COMPUTATION_PATH"]),
        loader=loader,
        bucket=network_config["BUCKET"],
    )

    # Sélection des millésimes jamais calculés ou périmés
    stale = vintages_to_recompute(targets, last_processed, last_computed)

    # Logging
    logger.info(f"{len(stale)} millésime(s) à recalculer : {[l for l, _ in stale]}")

    # Sortie anticipée : rien à recalculer
    if not stale:
        logger.info("Nothing to recompute, stop.")
        return

    # Instant de référence capturé avant le calcul : la date enregistrée
    # correspond au début du traitement, jamais après, pour ne pas rater une
    # mise à jour survenue pendant le calcul
    computed_at = _now()

    # Connecteur DuckLake aux flux redressés (catalogue COMTRADE, un schéma par
    # millésime — cf. scripts/process_baci_hs.py)
    source_connector = DuckLakeConnector.from_postgres(
        data_path=f"s3://{comtrade_config['DOWNLOADS'][DATAFLOW]['BUCKET']}/{comtrade_config['DOWNLOADS'][DATAFLOW]['PATHS']['DATA_PATH']}",
        dbname=comtrade_config["DOWNLOADS"]["DBNAME"],
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        create_db_if_missing=True,
        admin_dbname=os.environ["PGDATABASE"],
        admin_user="postgres",
        admin_password=os.environ["PGPASSWORD"],
        catalog_alias=comtrade_config["DOWNLOADS"]["CATALOG_ALIAS"],
        schema=targets[stale[0][0]],
        s3_endpoint=os.environ["AWS_S3_ENDPOINT"],
        s3_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        s3_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        s3_session_token=os.environ["AWS_SESSION_TOKEN"],
    )

    # Schéma résultat, commun à la relecture et à l'écriture
    result_schema = _schema_name(network_config["RESULT_SCHEMA"])

    # Connecteur DuckLake résultat : catalogue des vulnérabilités, partagé avec
    # les indicateurs partenaires, positionné sur le schéma dédié au réseau
    result_connector = DuckLakeConnector.from_postgres(
        data_path=f"s3://{network_config['BUCKET']}/{network_config['PATHS']['DATA_PATH']}",
        dbname=vulnerability_config["VULNERABILITIES"]["DBNAME"],
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        create_db_if_missing=True,
        admin_dbname=os.environ["PGDATABASE"],
        admin_user="postgres",
        admin_password=os.environ["PGPASSWORD"],
        catalog_alias=vulnerability_config["VULNERABILITIES"]["CATALOG_ALIAS"],
        schema=result_schema,
        s3_endpoint=os.environ["AWS_S3_ENDPOINT"],
        s3_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        s3_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        s3_session_token=os.environ["AWS_SESSION_TOKEN"],
    )

    # Ouverture des connexions : leur cycle de vie appartient au script, le
    # runner ne les ouvre ni ne les ferme (cf. `run_network_vulnerabilities`)
    source_conn = source_connector.connect()
    computed: Dict[str, Dict[str, Any]] = {}
    failures: Dict[str, Exception] = {}
    try:
        result_conn = result_connector.connect()
        try:
            # Un millésime après l'autre : l'échec de l'un n'emporte pas les autres
            for label, source_schema in stale:
                try:
                    # Un run par millésime, taggé, comme dans process_baci_hs.py
                    tracker = get_tracker(
                        tracking_uri=mlflow_config.get("TRACKING_URI"),
                        experiment=mlflow_config.get(
                            "EXPERIMENT", "network-vulnerabilities"
                        ),
                        run_name=f"network-vulnerabilities-{label}-{datetime.now():%Y%m%d-%H%M}",
                        tags={"vintage": label},
                    )
                    with tracker:
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
                            backend=backend,
                            tracker=tracker,
                            log_artifacts=log_artifacts,
                            df_previous=df_previous,
                        )

                        # Envoi des métriques : le rapport connaît sa mise en
                        # forme. Les paramètres sont journalisés par le runner
                        # lui-même ; seuls les tags propres au script restent ici.
                        tracker.log_metrics(report.to_metrics())
                        tracker.set_tags(
                            {
                                "dataflow": DATAFLOW,
                                "source_schema": source_schema,
                                "result_schema": result_schema,
                                "created": str(report.created),
                                "n_cells": str(report.cells),
                            }
                        )

                    # Entrée de registre du millésime effectivement calculé
                    computed[source_schema] = {
                        "vintage": label,
                        "source_schema": source_schema,
                        "result_schema": result_schema,
                        "last_computed": computed_at.isoformat(),
                        "n_cells": int(report.cells),
                    }

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

    # Mise à jour du registre des dates de calcul, uniquement pour les millésimes
    # dont le calcul et l'écriture ont réussi (cohérence : jamais de date
    # avancée à tort, qui ferait sauter un recalcul nécessaire)
    if computed:
        save_last_computation_dates(
            Path(network_config["PATHS"]["LAST_COMPUTATION_PATH"]),
            computed,
            loader,
            saver,
            network_config["BUCKET"],
        )

    # Échec global si au moins un millésime a échoué, une fois tous tentés
    if failures:
        raise RuntimeError(
            f"{len(failures)} millésime(s) en échec sur {len(stale)} : "
            f"{sorted(failures)}"
        ) from next(iter(failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
