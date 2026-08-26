"""Script de téléchargement des données Comext (commerce extérieur) Eurostat.

Télécharge le dataflow DS-045409 depuis l'API SDMX 3.0 d'Eurostat en scindant
les requêtes par pays reporter x code produit. Peut être ordonnancé (Argo, cron)
ou intégré directement comme nœud Kedro via les fonctions exportées.
"""
# Importation des modules
# Modules de base
import os
from datetime import datetime, timedelta
import itertools
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import yaml

# Modules de manipulation de données
import pandas as pd

# Importation des modules du package
from macroforecast.datasets import (
    EurostatClient,
    StructureResourceType,
    DataflowStructure,
    EurostatQueryRequestV30
)
from macroforecast.datasets.sources.eurostat.parsing import parse_codelist_response
from macroforecast.datasets.utils import (
    filter_codes,
)
from macroforecast.datasets.core.download import download_updates, _schema_name
from macroforecast.datasets.core.reports import QueryReport

# Module de suivi d'exécution
from macroforecast.tracking import get_tracker

# Module de connexion à la base de données
from macroforecast.storage2 import DuckLakeConnector
    

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)


# Fonction de chargement de la configuration
def load_config(config_path: Optional[os.PathLike] = None) -> dict:
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


# Fonction récupération des listes de codes associées à une dimension d'un dataflow
def fetch_dimension_codelists(
    structure: DataflowStructure,
    dimension: str,
    client: Optional[EurostatClient] = None,
) -> pd.DataFrame:
    """Fetch the codelist of one dimension of a Comext dataflow.

    Deduces the codelist identifier of the requested dimension from the
    dataflow's Data Structure Definition (DSD), then downloads and parses it.

    Args:
        structure: Resolved dataflow structure (carries the dimension →
            codelist mapping).
        dimension: Name of the dimension to fetch the codelist of (e.g.
            ``"reporter"``, ``"product"``).
        client: ``EurostatClient`` instance; a new one is created if ``None``.

    Returns:
        DataFrame with columns ``(code, name)`` for the requested dimension.

    Examples:
        >>> structure = client.get_dataflow_structure(
        ...     dataflow="DS-045409",
        ... )  # doctest: +SKIP
        >>> reporter_codes = fetch_dimension_codelists(
        ...     structure, "reporter",
        ... )  # doctest: +SKIP
        >>> "FR" in reporter_codes["code"].values  # doctest: +SKIP
        True
    """
    # Initialisation du client s'il n'est pas spécifié
    if client is None:
        client = EurostatClient()

    # Déduction des codelists des informations de la structure
    codelists = {d.name: d.codelist for d in structure.dimensions}

    # Extraction de la liste des codes liée à la dimension
    dimension_codelist = codelists[dimension]
    # Logging
    logger.info("Codes related to '%s' : %s", dimension, dimension_codelist)

    # Requête des codes associés à la dimension
    dimension_xml = client.get_structure(StructureResourceType.CODELIST, dimension_codelist)
    # Parsing du XML de réponse
    dimension_codes = parse_codelist_response(dimension_xml)
    # Logging
    logger.info("%d codes %s", len(dimension_codes), dimension)

    return dimension_codes


# Fonction de construction des requêtes
def build_split_queries(
    dataflow: str,
    dims_codes: Dict[str, pd.DataFrame],
    fixed_dims: Dict[str, Union[List[str], str]] = {},
    split_filters: Dict[str, Dict[str, Union[List[str], str]]] = {}
) -> List[Any]:
    """Build the split queries for a Comext dataflow.

    Applies the include/exclude filters declared in the YAML configuration to
    the reporter and product codelists, then returns one query per
    (reporter, product) pair in the cartesian product.

    Args:
        dataflow: Eurostat dataflow identifier (e.g. ``"DS-045409"``).
        dims_codes: Mapping of split-dimension name to its codelist DataFrame
            (column ``code``), as returned by :func:`fetch_dimension_codelists`.
        fixed_dims: Dimensions shared by every query (e.g. freq=A, partner=*,
            flow=1, indicators=QUANTITY_IN_100KG), read from the YAML
            ``fixed_dims`` section.
        split_filters: Per split-dimension include/exclude filters (forwarded
            to :func:`~macroforecast.datasets.utils.filter_codes`), read from
            the YAML ``split_filters`` section. Must share the same keys as
            ``dims_codes``.

    Returns:
        List of ``EurostatQueryRequestV30`` objects, one per combination of
        the cartesian product of the filtered split-dimension codes.

    Raises:
        ValueError: If ``split_filters`` and ``dims_codes`` do not share the
            same keys.

    Examples:
        >>> queries = build_split_queries(
        ...     "DS-045409",
        ...     dims_codes={"reporter": reporter_codes, "product": product_codes},
        ...     fixed_dims={"freq": "A"},
        ...     split_filters={"reporter": {}, "product": {}},
        ... )  # doctest: +SKIP
        >>> len(queries) > 0
        True
    """
    
    # Vérification que 'split_filters' et 'dims_codes' partagent les mêmes clés
    if set(split_filters.keys()) != set(dims_codes.keys()):
        raise ValueError(f"'split_filters' and 'dims_codes' should have similar keys. Found {split_filters.keys()} for 'split_filters' and {dims_codes.keys()} for 'dims_codes'")

    # Construction des codes associés aux dimensions splitées à croiser
    split_dims_codes = [filter_codes(dims_codes[split_dim]["code"], **split_filters[split_dim]) for split_dim in split_filters.keys()]

    # Construction des requêtes
    queries = [
        EurostatQueryRequestV30(
            dataflow=dataflow,
            dimensions={**fixed_dims, **{key: value for key, value in zip(split_filters.keys(), dim_values)}},
        )
        for dim_values in itertools.product(*split_dims_codes)
    ]

    # Logging
    logger.info(f"Successfully built {len(queries)} splitted queries.")

    return queries


# Fonction principale de téléchargement
def main() -> None:
    """CLI entry point for the Comext download script."""
    # Chargement de la configuration
    config = load_config()
    # Spécification du dataflow que l'on souhaite télécharger
    DATAFLOW = "DS-045409"

    # Initialisation du client eurostat
    client = EurostatClient()
    try:
        # Téléchargement de la structure
        structure = client.get_dataflow_structure(dataflow=DATAFLOW)
        # Extraction des codes associés au reporter et au produit (qui sont les dimensions selon lesquelles on souhaite scinder les requêtes)
        dims_codes = {split_dim: fetch_dimension_codelists(structure=structure, dimension=split_dim, client=client) for split_dim in config["split_filters"][DATAFLOW].keys()}

        # Construction des requêtes selon les dimensions souhaitées
        queries = build_split_queries(
            dataflow=DATAFLOW,
            dims_codes=dims_codes,
            fixed_dims=config["fixed_dims"][DATAFLOW],
            split_filters=config["split_filters"][DATAFLOW]
        )
        # Restriction de test à 5 requêtes
        queries = queries[:5]  # queries[:5]

        # Initialisation du connecteur au catalogue
        connector = DuckLakeConnector.from_postgres(
            data_path=f"s3://{config['DOWNLOADS'][DATAFLOW]['BUCKET']}/{config['DOWNLOADS'][DATAFLOW]['PATHS']['DATA_PATH']}",
            dbname=config['DOWNLOADS']['DBNAME'],
            host=os.environ['PGHOST'],
            port=os.environ['PGPORT'],
            user=os.environ['PGUSER'],
            password=os.environ['PGPASSWORD'],
            admin_dbname=os.environ['PGDATABASE'],
            admin_user="postgres",
            admin_password=os.environ["PGPASSWORD"],
            catalog_alias=config['DOWNLOADS']['CATALOG_ALIAS'],
            schema=_schema_name(DATAFLOW),
            s3_endpoint=os.environ["AWS_S3_ENDPOINT"],
            s3_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            s3_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            s3_session_token=os.environ["AWS_SESSION_TOKEN"],
        )

        # Construction du suivi d'exécution : sans URI (ou sans MLflow installé,
        # ou serveur injoignable), get_tracker retourne un tracker inerte et
        # l'exécution est strictement inchangée
        mlflow_config = config.get("MLFLOW") or {}
        tracker = get_tracker(
            tracking_uri=mlflow_config.get("TRACKING_URI"),
            experiment=mlflow_config.get("EXPERIMENT", "eurostat-download"),
            run_name=f"{DATAFLOW}-{datetime.now():%Y%m%d-%H%M}",
        )
        log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))

        with tracker:
            # Suivi requête par requête : le paquet datasets ignore tout de
            # MLflow, le rappel est le seul point de contact
            def stream_query_metrics(query_report: QueryReport) -> None:
                """Send one query's diagnostics to the tracker."""
                tracker.log_metrics(
                    query_report.to_metrics(), step=len(streamed_queries)
                )
                streamed_queries.append(query_report)

            streamed_queries: list[QueryReport] = []

            # Téléchargement des données
            report = download_updates(
                client=client,
                queries=queries,
                connector=connector,
                structures_path=config["DOWNLOADS"][DATAFLOW]["PATHS"]["STRUCTURES_PATH"],
                last_download_path=config["DOWNLOADS"][DATAFLOW]["PATHS"]["LAST_DOWNLOAD_PATH"],
                n_observations=config["DOWNLOADS"][DATAFLOW]["N_LAST_OBSERVATIONS"],
                fresh_registry=False,
                max_runtime=timedelta(
                    weeks=config["DOWNLOADS"][DATAFLOW]["MAX_RUNTIME"]["WEEKS"],
                    days=config["DOWNLOADS"][DATAFLOW]["MAX_RUNTIME"]["DAYS"],
                    hours=config["DOWNLOADS"][DATAFLOW]["MAX_RUNTIME"]["HOURS"],
                    minutes=config["DOWNLOADS"][DATAFLOW]["MAX_RUNTIME"]["MINUTES"],
                    seconds=config["DOWNLOADS"][DATAFLOW]["MAX_RUNTIME"]["SECONDS"]
                ),
                categorical_threshold=None,  # A supprimer avec la nouvelle version de la base de données
                bucket=config['DOWNLOADS'][DATAFLOW]['BUCKET'],
                storage_options=None,
                on_query_complete=stream_query_metrics,
            )

            # Métriques de run : le rapport connaît sa mise en forme
            tracker.log_metrics(report.to_metrics())
            tracker.set_tags(
                {
                    "dataflow": DATAFLOW,
                    "stopped_early": str(report.stopped_early),
                    "n_queries_planned": str(report.n_queries_planned),
                }
            )
            # Détail par requête : table auditable de ce que le run a fait
            if log_artifacts and report.queries:
                tracker.log_table(report.to_frame(), "download/queries.csv")

        # Logging
        logger.info(f"Téléchargement terminé : {report.to_metrics()}")
    finally:
        client.close()

# Exécution du script principal
if __name__ == "__main__":
    main()
