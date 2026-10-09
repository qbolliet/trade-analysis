"""Script de téléchargement des données Comext (commerce extérieur) Eurostat.

Enveloppe CLI de la fonction d'étape ``kedro_pipeline.steps.downloads.run_download``
(source ``eurostat``) : le dataflow configuré (``DATAFLOW``, DS-045409) est
téléchargé depuis l'API SDMX 3.0 d'Eurostat en scindant les requêtes par code
produit x pays reporter, toutes les années dans une même requête à partir de
``runtime.ANALYSIS_START_YEAR.eurostat``, dans un ordre **produit-majeur** (un
produit complet pour tous les reporters avant le suivant, maille des
indicateurs partenaires). ``download_updates`` traite d'abord les requêtes
jamais téléchargées : l'ordre de la liste fait foi pour le rattrapage. Les
codelists sont publiées en référentiels et la couverture de la source est
auditée en fin de téléchargement.

Configuration lue : blocs ``eurostat`` et ``runtime`` des paramètres Kedro
(``config/<env>/parameters_*.yml``), environnement choisi par ``KEDRO_ENV``
(``local`` par défaut, ``demo`` pour le périmètre de démonstration).
"""
# Importation des modules
# Modules de base
import os
import logging
from typing import Any, Optional

# Importation des modules du package
from statflows import EurostatClient

# Module de suivi d'exécution
from macroforecast.tracking import CapturingTracker, get_tracker
from kedro_pipeline.config import experiment_name, load_parameters, read_config_file
from kedro_pipeline.io.ducklake import (
    DuckLakeTable,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
)
from kedro_pipeline.steps._config import download_location
# Fonctions de planification et étape de téléchargement (ré-exportées)
from kedro_pipeline.steps.downloads import (  # noqa: F401
    _PRODUCT_DIM,
    _ordered_codes,
    build_eurostat_queries as build_split_queries,
    cap_queries,
    eurostat_shard_key as registry_shard_key,
    fetch_eurostat_codelist as fetch_dimension_codelists,
    run_download,
)
from scripts._run_report import RunScope, guarded_run, run_name, timestamp


# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)


# Fabrique de connecteur : identifiants lus dans l'environnement à la première connexion
def _connector(location: Any, pg: Any = None, s3: Any = None, **kwargs: Any) -> Any:
    """Build the connector of a table, reading the credentials only when connecting."""
    return build_connector(location, pg_credentials_from_env(), s3_credentials_from_env(), **kwargs)

# Nœud du rapport de run (clé de tracking.CHECKS)
NODE = "download_eurostat"


# Fonction de chargement de la configuration
def load_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the Eurostat Comext download configuration.

    Args:
        config_path: Explicit YAML file (``eurostat`` root key or historical
            format without it). If None, the ``eurostat`` block of the Kedro
            parameters of the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary
    """
    if config_path is None:
        return load_parameters()["eurostat"]
    return read_config_file(config_path, "eurostat")


# Fonction de chargement de la configuration d'exécution partagée
def load_runtime_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the shared runtime configuration (``runtime`` block).

    Args:
        config_path: Explicit YAML file with a ``runtime`` root key. If None,
            the ``runtime`` block of the Kedro parameters of the ``KEDRO_ENV``
            environment.

    Returns:
        dict: The mapping under the ``runtime`` root key.
    """
    if config_path is None:
        return load_parameters()["runtime"]
    return read_config_file(config_path, "runtime")


# Fonction principale de téléchargement
def main() -> None:
    """CLI entry point for the Comext download script.

    Raises:
        DownloadFailureError: If the share of failed queries exceeds ``MAX_ERROR_RATIO``,
            after the run report was published.
    """
    config, runtime_config = load_config(), load_runtime_config()
    table = DuckLakeTable.lazy(download_location(config), None, None, connector_factory=_connector)
    # Suivi d'exécution : sans URI MLflow, get_tracker retourne un tracker inerte ; le run
    # est ouvert dès le début pour que tout échec, y compris de planification, porte son rapport
    tracker = CapturingTracker(
        get_tracker(
            tracking_uri=None,
            experiment=experiment_name("downloads"),
            run_name=run_name(f"{config['DATAFLOW']}-{timestamp()}", NODE),
        )
    )
    scope = RunScope(NODE)
    with tracker, guarded_run(scope, tracker):
        result = run_download(
            EurostatClient, table, source="eurostat", params=config,
            runtime=runtime_config, tracker=tracker, progress=scope,
        )
        scope.publish_result(tracker, result)
    # Statut de sortie : contrôle après la clôture du run, le rapport étant publié
    result.raise_if_failed()


# Exécution du script principal
if __name__ == "__main__":
    main()
