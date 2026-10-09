"""Script de téléchargement des données tariffline UN Comtrade.

Enveloppe CLI de la fonction d'étape ``kedro_pipeline.steps.downloads.run_download``
(source ``comtrade``) : requêtes scindées par année x lot de produits HS6, dans un
ordre **année-majeur** (toutes les requêtes d'une année précèdent celles de
l'année suivante, pour qu'une année soit complète — et donc utilisable par la
porte de complétude BACI — le plus tôt possible), mise à jour incrémentale
déléguée à ``download_updates``, référentiels publiés depuis les codelists,
puis audit de couverture de la source.

Ce script charge les paramètres, construit la poignée de la table, le client
(clé d'abonnement lue dans l'environnement) et le suivi d'exécution, appelle
l'étape, publie le rapport de run et sort en erreur si la part de requêtes en
échec dépasse ``MAX_ERROR_RATIO``. Les fonctions de planification restent
importables d'ici (scripts de données fictives, tests).

Configuration lue : blocs ``comtrade`` et ``runtime`` des paramètres Kedro
(``config/<env>/parameters_*.yml``), environnement choisi par ``KEDRO_ENV``
(``local`` par défaut, ``demo`` pour le périmètre de démonstration).
"""
# Importation des modules
# Modules de base
import os
from dataclasses import replace
import logging
from typing import Any, Optional

# Importation des modules du package
from statflows import ComtradeClient

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
    _PERIODS_KEY,
    _PERIODS_ORDERS,
    build_comtrade_queries as build_split_queries,
    cap_queries,
    comtrade_shard_key as registry_shard_key,
    fetch_comtrade_codelist as fetch_dimension_codelists,
    plan_comtrade_queries as plan_queries,
    resolve_periods,
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

# Variable d'environnement de la clé d'API Comtrade (abonnement premium)
_SUBSCRIPTION_KEY_ENV = "COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY"
# Nœud du rapport de run (clé de tracking.CHECKS)
NODE = "download_comtrade"


# Fonction de chargement de la configuration
def load_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the UN Comtrade download configuration.

    Args:
        config_path: Explicit YAML file (``comtrade`` root key or historical
            format without it). If None, the ``comtrade`` block of the Kedro
            parameters of the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary (``DATAFLOW``, ``DOWNLOADS``, ``split_filters``…).
    """
    if config_path is None:
        return load_parameters()["comtrade"]
    return read_config_file(config_path, "comtrade")


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


# Fonction de construction du client Comtrade (clé d'abonnement de l'environnement)
def comtrade_client() -> ComtradeClient:
    """Build the UN Comtrade client with the subscription key of the environment.

    Returns:
        The client.
    """
    return ComtradeClient(subscription_key=os.environ.get(_SUBSCRIPTION_KEY_ENV))


# Fonction principale de téléchargement
def main() -> None:
    """CLI entry point for the Comtrade download script.

    Raises:
        DownloadFailureError: If the share of failed queries exceeds ``MAX_ERROR_RATIO``,
            after the run report was published.
    """
    config, runtime_config = load_config(), load_runtime_config()
    # Table téléchargée : catalogue nommé comme le fournisseur Comtrade
    location = replace(download_location(config), catalog_alias=ComtradeClient.PROVIDER_CONFIG_NAME)
    table = DuckLakeTable.lazy(location, None, None, connector_factory=_connector)
    # Suivi d'exécution : sans URI MLflow, get_tracker retourne un tracker inerte ; le run
    # est ouvert dès le début pour que tout échec porte son rapport
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
            comtrade_client, table, source="comtrade", params=config,
            runtime=runtime_config, tracker=tracker, progress=scope,
        )
        scope.publish_result(tracker, result)
    # Statut de sortie : au-delà de la part tolérée de requêtes en échec, après le rapport
    result.raise_if_failed()


# Exécution du script principal
if __name__ == "__main__":
    main()
