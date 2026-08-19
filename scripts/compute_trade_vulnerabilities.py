"""Script de calcul/mise à jour des indicateurs de vulnérabilité commerciale.

Recalcule les indicateurs (HHI, CDI2, CDI3 — cf. `macroforecast.trade.vulnerabilities`)
pour chaque couple reporter x produit dont la date de dernier calcul est absente
ou antérieure à la date de dernier téléchargement de la série correspondante.
Cette dernière est lue directement dans le registre JSON `LAST_DOWNLOAD_PATH`
tenu par `download_eurostat_comext.py` / `SDMXDownloader` (aucune modification
du script de téléchargement n'est nécessaire).

Une date de dernier calcul est tenue par couple dans un registre JSON dédié au
calcul des vulnérabilités (même principe que le registre de téléchargement,
— cf. `vulnerabilities.yaml`).

Source et résultat vivent dans deux schémas du même catalogue Postgres
(`DuckLakeConnector.from_postgres`, cf. `run_vulnerabilities_incremental`).

Prévu comme étape Argo s'exécutant après le téléchargement (dépendance directe,
mais couplage faible : ce script ne lit que le registre JSON produit par le
téléchargement, il ne dépend d'aucun état en mémoire de ce dernier).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from datetime import datetime
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple
import yaml

from botocore.exceptions import ClientError

# Module de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from macroforecast.storage import Loader, Saver
# Module de connexion à la base de données
from macroforecast.storage2 import DuckLakeConnector
# Module d'utilitaires de téléchargement
from macroforecast.datasets.core.download import _now, _parse_iso, _schema_name

# Module du package : calcul des indicateurs (mode incrémental, catalogue Postgres partagé)
from macroforecast.trade.vulnerabilities.runner import run_vulnerabilities_incremental

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé racine du registre JSON des dates de dernier calcul (miroir de la
# racine "DOWNLOADS" du registre de téléchargement)
_REGISTRY_ROOT = "VULNERABILITIES"


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

# Fonction de chargement de la configuration (fichier dédié à eurostat)
def load_eurostat_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load configuration from file.
    
    Args:
        config_path: Path to config file. If None, uses default location
                     or CONFIG_PATH environment variable.
    
    Returns:
        dict: Configuration dictionary
    """
    # Détermination du chemin de configuration
    if config_path is None:
        # Priorité 1 : variable d'environnement (pour flexibilité Kubernetes)
        config_path = os.environ.get('EUROSTAT_CONFIG_PATH', 'config/datasets/eurostat.yaml')
    
    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de chargement de la configuration (fichier dédié au calcul des vulnérabilités)
def load_vulnerability_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the vulnerability-computation configuration from file.

    Deliberately a separate config file from the download step's
    ``eurostat.yaml`` (own env var, own default path).

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


# ──────────────────────────────────────────────────────────────────────
# Accès aux registres JSON (local ou S3) — même pattern que SDMXDownloader
# ──────────────────────────────────────────────────────────────────────

# Fonction de lecture d'un registre JSON (local ou S3)
# /!\ Voir si cela ne pourrait pas être mutualisé avec la méthode "_read_json" de SDMXDownloader dans macroforecast/datasets/core/download.py
def _read_json(
    path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Read a JSON registry from local storage or S3.

    Mirrors ``SDMXDownloader._read_json`` : a missing file/object returns
    ``None`` rather than raising.

    Args:
        path: Registry path (local path or S3 key).
        loader: ``Loader`` instance.
        bucket: S3 bucket, or ``None`` to read a local path.

    Returns:
        The deserialised JSON mapping, or ``None`` if absent.
    """
    # Cas S3 : l'absence d'objet se détecte via l'exception du client
    if bucket is not None:
        try:
            return loader.load(path.as_posix(), bucket=bucket)
        except ClientError:
            return None
    # Cas local : court-circuit si le fichier n'existe pas encore
    if not path.exists():
        return None
    return loader.load(str(path))


# Fonction d'écriture d'un registre JSON (local ou S3), atomique en local
# /!\ Voir si cela ne pourrait pas être mutualisé avec la méthode "_read_json" de SDMXDownloader dans macroforecast/datasets/core/download.py
def _write_json(
    path: Path,
    obj: Any,
    saver: Saver,
    bucket: Optional[str],
) -> None:
    """Write a JSON registry to local storage or S3, atomically when local.

    Mirrors ``SDMXDownloader._write_json``.

    Args:
        path: Registry path (local path or S3 key).
        obj: JSON-serialisable object to persist.
        saver: ``Saver`` instance.
        bucket: S3 bucket, or ``None`` to write a local path.
    """
    # Cas S3 : écriture directe (PUT d'objet atomique)
    if bucket is not None:
        saver.save(path.as_posix(), obj, bucket=bucket, indent=2, ensure_ascii=False)
        return

    # Cas local : écriture atomique via fichier temporaire + remplacement
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".json")
    os.close(fd)
    try:
        saver.save(tmp_name, obj, indent=2, ensure_ascii=False)
        os.replace(tmp_name, str(path))
    except Exception:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


# ──────────────────────────────────────────────────────────────────────
# Registre de téléchargement (lecture seule) : dates par reporter x produit
# ──────────────────────────────────────────────────────────────────────

# Fonction d'indexation des dates de dernier téléchargement par couple reporter x produit
def load_last_download_dates(
    last_download_path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Dict[Tuple[str, str], datetime]:
    """Read the download registry and index last-download dates by (reporter, product).

    Args:
        last_download_path: Path to the ``LAST_DOWNLOAD_PATH`` registry
            (cf. ``eurostat.yaml`` / ``SDMXDownloader``).
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        Mapping ``(reporter, product) -> last_download`` (UTC-aware datetime).
    """
    # Lecture du registre (racine "DOWNLOADS", cf. _REGISTRY_ROOT de download.py)
    data = _read_json(last_download_path, loader, bucket) or {}
    registry = data.get("DOWNLOADS", {})

    # Extraction du couple et de la date par entrée
    dates: Dict[Tuple[str, str], datetime] = {}
    # Parcours des entrées du registre
    for entry in registry.values():
        # Extraction des paramètres de requêtes
        params = entry.get("params", {})
        # Extraction des dimensions
        dims = params.get("dimensions", params)
        # Extraction du reporter et du produit
        reporter = dims.get("reporter")
        product = dims.get("product")
        # Extraction de la date de dernier téléchargement
        last_download = _parse_iso(entry.get("last_download"))
        # Association de la date de dernier téléchargement au couple
        if reporter and product and last_download is not None:
            dates[(reporter, product)] = last_download
    return dates


# ──────────────────────────────────────────────────────────────────────
# Registre de calcul des vulnérabilités (lecture/écriture)
# ──────────────────────────────────────────────────────────────────────

# Fonction de lecture des dates de dernier calcul par couple reporter x produit
def load_last_computation_dates(
    last_computation_path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Dict[Tuple[str, str], datetime]:
    """Read the vulnerability-computation registry (empty if it does not exist yet).

    Args:
        last_computation_path: Path to the vulnerability-computation registry.
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        Mapping ``(reporter, product) -> last_computed`` (UTC-aware datetime).
    """
    # Importation des données
    data = _read_json(last_computation_path, loader, bucket) or {}
    # Extraction du registre
    registry = data.get(_REGISTRY_ROOT, {})
    # Initialisation du dictionnaire associant au tuple reporter X produit la date du dernier calcul de vulnérabilité
    dates: Dict[Tuple[str, str], datetime] = {}
    # Parcours des entrées du registre
    for entry in registry.values():
        # Extraction du reporter et du produit
        reporter = entry.get("reporter")
        product = entry.get("product")
        # Extraction de la date de dernier téléchargement
        last_computed = _parse_iso(entry.get("last_computed"))
        # Association de la date de dernier téléchargement au couple
        if reporter and product and last_computed is not None:
            dates[(reporter, product)] = last_computed
    return dates


# Fonction de fusion et de sauvegarde des dates de calcul mises à jour
def save_last_computation_dates(
    last_computation_path: Path,
    dates: Dict[Tuple[str, str], datetime],
    loader: Loader,
    saver: Saver,
    bucket: Optional[str],
) -> None:
    """Merge new (reporter, product) computation dates into the registry and persist it.

    Args:
        last_computation_path: Path to the vulnerability-computation registry.
        dates: Pairs and their new ``last_computed`` instant. Merged into the
            existing registry; every other entry is preserved untouched.
        loader: ``Loader`` instance (to load the existing registry before merging).
        saver: ``Saver`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
    """
    # Fusion avec le registre existant : seules les entrées recalculées bougent
    data = _read_json(last_computation_path, loader, bucket) or {}
    registry = data.get(_REGISTRY_ROOT, {})

    # Mise à jour des dates
    for (reporter, product), when in dates.items():
        registry[f"{reporter}|{product}"] = {
            "reporter": reporter,
            "product": product,
            "last_computed": when.isoformat(),
        }

    # Ecriture du fichier json mis à jour
    _write_json(last_computation_path, {_REGISTRY_ROOT: registry}, saver, bucket)
    # Logging
    logger.info(
        f"{len(dates)} date(s) de calcul mise(s) à jour dans '{last_computation_path}'"
    )


# ──────────────────────────────────────────────────────────────────────
# Détermination des couples à (re)calculer
# ──────────────────────────────────────────────────────────────────────

# Fonction de sélection des couples dont le score de vulnérabilité est périmé
def pairs_to_recompute(
    last_download: Dict[Tuple[str, str], datetime],
    last_computed: Dict[Tuple[str, str], datetime],
) -> Set[Tuple[str, str]]:
    """Select (reporter, product) pairs whose vulnerability score is stale.

    A pair is selected when it was never scored, or when it was last scored
    before its most recent download.

    Args:
        last_download: Last-download date per pair (source registry).
        last_computed: Last-computation date per pair (vulnerability registry).

    Returns:
        Set of pairs to (re)compute.
    """
    return {
        pair
        for pair, downloaded_at in last_download.items()
        if pair not in last_computed or last_computed[pair] < downloaded_at
    }


# ──────────────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────

# Fonction principale de calcul des vulnérabilités
def main() -> None:
    """CLI entry point for the incremental vulnerability computation script."""
    # Chargement de la configuration dédiée aux données eurostat qui servent de source au calcul des vulnérabilités
    eurostat_config = load_eurostat_config()
    # Chargement de la configuration dédiée au calcul des vulnérabilités
    vulnerability_config = load_vulnerability_config()

    # Initialisation du Dataflow sur lequel sont calculées les métriques de vulnérabilité
    DATAFLOW = "DS-045409"

    # Initialisation des loaders et savers
    loader = Loader()
    saver = Saver()

    # Dates de dernier téléchargement, par couple reporter x produit (lecture
    # seule du registre du téléchargement, aucune écriture de ce côté)
    last_download = load_last_download_dates(
        last_download_path=Path(eurostat_config["DOWNLOADS"][DATAFLOW]["PATHS"]["LAST_DOWNLOAD_PATH"]),
        loader=loader,
        bucket=eurostat_config["DOWNLOADS"][DATAFLOW]["BUCKET"]
    )
    # Dates de dernier calcul de vulnérabilité, par couple
    last_computed = load_last_computation_dates(
        last_computation_path=Path(vulnerability_config["VULNERABILITIES"][DATAFLOW]["PATHS"]["LAST_COMPUTATION_PATH"]),
        loader=loader,
        bucket=vulnerability_config["VULNERABILITIES"][DATAFLOW]["BUCKET"]
    )

    # Sélection des couples jamais calculés ou périmés (calcul antérieur au
    # dernier téléchargement de la série)
    reporters_products = pairs_to_recompute(last_download, last_computed)
    logger.info(
        f"{len(reporters_products)} couple(s) reporter x produit to recompute"
    )

    # Sortie anticipée : rien à recalculer
    if not reporters_products:
        logger.info("Nothing to recompute, stop.")
        return

    # Instant de référence capturé avant le calcul : la date enregistrée
    # correspond au début du traitement, jamais après, pour ne pas rater une
    # mise à jour survenue pendant le calcul)
    computed_at = _now()

    # Connecteur DuckLake aux données sources
    source_connector = DuckLakeConnector.from_postgres(
            data_path=f"s3://{eurostat_config['DOWNLOADS'][DATAFLOW]['BUCKET']}/{eurostat_config['DOWNLOADS'][DATAFLOW]['PATHS']['DATA_PATH']}",
            dbname=eurostat_config['DOWNLOADS']['DBNAME'],
            host=os.environ['PGHOST'],
            port=os.environ['PGPORT'],
            user=os.environ['PGUSER'],
            password=os.environ['PGPASSWORD'],
            admin_dbname=os.environ['PGDATABASE'],
            admin_user="postgres",
            admin_password=os.environ["PGPASSWORD"],
            catalog_alias=eurostat_config['DOWNLOADS']['CATALOG_ALIAS'],
            schema=_schema_name(DATAFLOW),  # re.sub(r'[^a-zA-Z0-9]', '', DATAFLOW),
            s3_endpoint=os.environ["AWS_S3_ENDPOINT"],
            s3_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            s3_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            s3_session_token=os.environ["AWS_SESSION_TOKEN"],
        )

    # Connecteur DuckLake résultat : catalogue Postgres positionné sur le schéma résultat des vulnérabilités.
    result_connector = DuckLakeConnector.from_postgres(
        data_path=f"s3://{vulnerability_config['VULNERABILITIES'][DATAFLOW]['BUCKET']}/{vulnerability_config['VULNERABILITIES'][DATAFLOW]['PATHS']['DATA_PATH']}",
        dbname=vulnerability_config['VULNERABILITIES']["DBNAME"],
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        admin_dbname=os.environ['PGDATABASE'],
        admin_user="postgres",
        admin_password=os.environ["PGPASSWORD"],
        catalog_alias=vulnerability_config['VULNERABILITIES']['CATALOG_ALIAS'],
        schema=_schema_name(vulnerability_config['VULNERABILITIES'][DATAFLOW]["RESULT_SCHEMA"]),
        s3_endpoint=os.environ["AWS_S3_ENDPOINT"],
        s3_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        s3_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        s3_session_token=os.environ["AWS_SESSION_TOKEN"],
    )

    # Calcul incrémental et upsert dans le schéma résultat
    report = run_vulnerabilities_incremental(
        connector=source_connector,
        reporters_products=reporters_products,
        result_connector=result_connector,
        source_schema=_schema_name(DATAFLOW),
        result_schema=_schema_name(vulnerability_config['VULNERABILITIES'][DATAFLOW]["RESULT_SCHEMA"]),
    )

    # Logging
    logger.info(f"Vulnerability computation complete : {report}")

    # Mise à jour du registre des dates de calcul, uniquement après succès du
    # calcul et de l'écriture (cohérence : jamais de date avancée à tort)
    save_last_computation_dates(
        Path(vulnerability_config['VULNERABILITIES'][DATAFLOW]["PATHS"]["LAST_COMPUTATION_PATH"]),
        {pair: computed_at for pair in reporters_products},
        loader,
        saver,
        vulnerability_config["VULNERABILITIES"][DATAFLOW]["BUCKET"],
    )


# Exécution du script principal
if __name__ == "__main__":
    main()
