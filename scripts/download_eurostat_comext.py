"""Script de téléchargement des données Comext (commerce extérieur) Eurostat.

Télécharge le dataflow configuré (``DATAFLOW``, DS-045409) depuis l'API SDMX 3.0
d'Eurostat en scindant les requêtes par code produit x pays reporter, toutes les
années dans une même requête à partir de
``runtime.ANALYSIS_START_YEAR.eurostat`` (PD-07, PD-08). La liste est
**produit-majeure** (PS-12.2) : un produit est complet pour tous les reporters
avant le suivant, maille utile aux indicateurs partenaires (couple reporter x
produit). ``download_updates`` trie de façon stable les requêtes jamais
téléchargées en tête : l'ordre de la liste fait donc foi pour le rattrapage.

Fichiers de configuration lus : ``EUROSTAT_CONFIG_PATH`` (défaut
``config/datasets/eurostat.yaml``) et ``RUNTIME_CONFIG_PATH`` (défaut
``config/runtime.yaml``). Peut être ordonnancé (Argo, cron) ou intégré
directement comme nœud Kedro via les fonctions exportées.
"""
# Importation des modules
# Modules de base
import os
from datetime import datetime, timedelta
import itertools
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, TypeVar, Union
import yaml

# Modules de manipulation de données
import pandas as pd

# Importation des modules du package
from statflows import (
    EurostatClient,
    StructureResourceType,
    DataflowStructure,
    EurostatQueryRequestV30
)
from statflows.sources.eurostat.parsing import parse_codelist_response
from statflows.core.factory import (
    filter_codes,
)
from statflows.core.download import download_updates, _schema_name
from statflows.core.reports import QueryReport

# Module de suivi d'exécution
from macroforecast.tracking import get_tracker

# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.download_report import check_download_report
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
)


# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Dimension découpée en boucle externe (ordre produit-majeur, PS-12.2)
_PRODUCT_DIM = "product"

T = TypeVar("T")


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


# Fonction de chargement de la configuration d'exécution partagée
def load_runtime_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the shared runtime configuration (``runtime`` root key, PS-04.1).

    Args:
        config_path: Path to config file. If None, uses the
            RUNTIME_CONFIG_PATH environment variable or ``config/runtime.yaml``.

    Returns:
        dict: The mapping under the ``runtime`` root key.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        config_path = os.environ.get("RUNTIME_CONFIG_PATH", "config/runtime.yaml")

    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)["runtime"]


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


# Fonction de plafonnement du nombre de requêtes
def cap_queries(queries: Sequence[T], max_queries: Optional[int]) -> List[T]:
    """Keep at most ``max_queries`` queries, in list order (C-01).

    Args:
        queries: Ordered queries.
        max_queries: Maximum number of queries; ``None`` means no cap.

    Returns:
        The (possibly truncated) list of queries.

    Examples:
        >>> cap_queries([1, 2, 3], None)
        [1, 2, 3]
        >>> cap_queries([1, 2, 3], 1)
        [1]
    """
    return list(itertools.islice(queries, max_queries))


# Fonction de filtrage d'une codelist en conservant l'ordre de la liste d'inclusion
def _ordered_codes(codes: pd.DataFrame, filters: Mapping[str, Any]) -> List[str]:
    """Filter a codelist, keeping the order of the ``include`` list when given.

    ``filter_codes`` returns sorted codes; the configured ``include`` order is
    the query order of the dimension (e.g. reporters), so it is restored here.

    Args:
        codes: Codelist DataFrame with a ``code`` column.
        filters: include/exclude filters forwarded to ``filter_codes``.

    Returns:
        Selected codes, in ``include`` order if set, else in natural order.
    """
    selected = filter_codes(codes["code"], **filters)
    include = filters.get("include")
    # Pas de liste d'inclusion : ordre naturel des codes
    if include is None:
        return selected
    # Ordre de la liste d'inclusion (codes absents déjà signalés par filter_codes)
    kept = set(selected)
    return [code for code in dict.fromkeys(str(c) for c in include) if code in kept]


# Fonction de construction des requêtes
def build_split_queries(
    dataflow: str,
    dims_codes: Dict[str, pd.DataFrame],
    fixed_dims: Dict[str, Union[List[str], str]] = {},
    split_filters: Dict[str, Dict[str, Union[List[str], str]]] = {},
    products_step: int = 1,
    period_windows: Optional[Sequence[Sequence[Optional[int]]]] = None,
    start_period: Optional[str] = None,
) -> List[EurostatQueryRequestV30]:
    """Build the split queries for a Comext dataflow, product-major.

    Applies the include/exclude filters declared in the YAML configuration to
    the split-dimension codelists, then returns one query per product and per
    combination of the other split dimensions (typically reporters), every
    period in a single query. Order (PS-12.2): outer loop over the products in
    natural code order, inner loop over the other dimensions in the order of
    their ``include`` list (natural order when unset).

    Args:
        dataflow: Eurostat dataflow identifier (e.g. ``"DS-045409"``).
        dims_codes: Mapping of split-dimension name to its codelist DataFrame
            (column ``code``), as returned by :func:`fetch_dimension_codelists`.
            Must contain ``"product"``.
        fixed_dims: Dimensions shared by every query (e.g. freq=A, partner=*,
            flow=1, indicators=QUANTITY_IN_100KG), read from the YAML
            ``fixed_dims`` section.
        split_filters: Per split-dimension include/exclude filters (forwarded
            to :func:`~statflows.core.factory.filter_codes`), read from
            the YAML ``split_filters`` section. Must share the same keys as
            ``dims_codes``.
        products_step: Number of products per query; only ``1`` is supported
            (product batches are an open risk, PR-03).
        period_windows: Optional ordered period windows (PD-07); only ``None``
            is supported.
        start_period: First period requested (``startPeriod``, sent as
            ``c[TIME_PERIOD]=ge:<start>`` by the SDMX 3.0 client); ``None``
            requests every available period.

    Returns:
        List of ``EurostatQueryRequestV30`` objects, product-major.

    Raises:
        ValueError: If ``split_filters`` and ``dims_codes`` do not share the
            same keys, or ``"product"`` is not a split dimension.
        NotImplementedError: If ``products_step > 1`` or ``period_windows`` is
            set.

    Examples:
        >>> queries = build_split_queries(
        ...     "DS-045409",
        ...     dims_codes={"reporter": pd.DataFrame({"code": ["DE", "FR"]}),
        ...                 "product": pd.DataFrame({"code": ["01", "02"]})},
        ...     fixed_dims={"freq": "A"},
        ...     split_filters={"reporter": {"include": ["FR", "DE"]}, "product": {}},
        ... )
        >>> [(q.dimensions["product"], q.dimensions["reporter"]) for q in queries]
        [('01', 'FR'), ('01', 'DE'), ('02', 'FR'), ('02', 'DE')]
    """
    # Paramètres non encore supportés (granularité inchangée tant que PR-03 est ouvert)
    if products_step is not None and products_step > 1:
        raise NotImplementedError(
            f"products_step={products_step} is not supported yet: product batches "
            "require measuring the Eurostat API limits first (PR-03). Use 1."
        )
    if period_windows is not None:
        raise NotImplementedError(
            "period_windows is not supported yet (PD-07): set it to null."
        )

    # Vérification que 'split_filters' et 'dims_codes' partagent les mêmes clés
    if set(split_filters.keys()) != set(dims_codes.keys()):
        raise ValueError(f"'split_filters' and 'dims_codes' should have similar keys. Found {split_filters.keys()} for 'split_filters' and {dims_codes.keys()} for 'dims_codes'")
    if _PRODUCT_DIM not in split_filters:
        raise ValueError(f"'{_PRODUCT_DIM}' must be a split dimension (product-major order)")

    # Codes des dimensions scindées, dans l'ordre des requêtes
    split_dims_codes = {
        split_dim: (
            filter_codes(dims_codes[split_dim]["code"], **filters)
            if split_dim == _PRODUCT_DIM
            else _ordered_codes(dims_codes[split_dim], filters)
        )
        for split_dim, filters in split_filters.items()
    }
    # Dimensions de la boucle interne (ordre de la configuration)
    inner_dims = [dim for dim in split_filters if dim != _PRODUCT_DIM]

    # Construction produit-majeure (l'ordre d'insertion des dimensions suit la
    # configuration, comme auparavant)
    queries = [
        EurostatQueryRequestV30(
            dataflow=dataflow,
            dimensions={
                **fixed_dims,
                **{
                    dim: (product if dim == _PRODUCT_DIM else inner[inner_dims.index(dim)])
                    for dim in split_filters
                },
            },
            start_period=start_period,
        )
        for product in split_dims_codes[_PRODUCT_DIM]
        for inner in itertools.product(*(split_dims_codes[dim] for dim in inner_dims))
    ]

    # Logging
    logger.info(f"Successfully built {len(queries)} splitted queries.")

    return queries


# Fonction principale de téléchargement
def main() -> None:
    """CLI entry point for the Comext download script."""
    # Chargement des configurations
    config = load_config()
    runtime_config = load_runtime_config()
    # Dataflow à télécharger (C-05)
    DATAFLOW = config["DATAFLOW"]
    parameters = config["parameters"][DATAFLOW]
    downloads_config = config["DOWNLOADS"][DATAFLOW]

    # Initialisation du client eurostat
    client = EurostatClient()
    try:
        # Téléchargement de la structure
        structure = client.get_dataflow_structure(dataflow=DATAFLOW)
        # Extraction des codes associés au reporter et au produit (qui sont les dimensions selon lesquelles on souhaite scinder les requêtes)
        dims_codes = {split_dim: fetch_dimension_codelists(structure=structure, dimension=split_dim, client=client) for split_dim in config["split_filters"][DATAFLOW].keys()}

        # Construction des requêtes produit-majeures, toutes années depuis la
        # première année de l'analyse (PD-08)
        queries = build_split_queries(
            dataflow=DATAFLOW,
            dims_codes=dims_codes,
            fixed_dims=config["fixed_dims"][DATAFLOW],
            split_filters=config["split_filters"][DATAFLOW],
            products_step=parameters.get("products_step", 1),
            period_windows=parameters.get("period_windows"),
            start_period=str(runtime_config["ANALYSIS_START_YEAR"]["eurostat"]),
        )
        # Plafond optionnel (null = aucun, C-01)
        queries = cap_queries(queries, parameters.get("max_queries"))

        # Initialisation du connecteur au catalogue
        connector = build_connector(
            DuckLakeLocation(
                dbname=config["DOWNLOADS"]["DBNAME"],
                catalog_alias=config["DOWNLOADS"]["CATALOG_ALIAS"],
                schema=_schema_name(DATAFLOW),
                bucket=downloads_config["BUCKET"],
                data_path=downloads_config["PATHS"]["DATA_PATH"],
            ),
            pg=pg_credentials_from_env(),
            s3=s3_credentials_from_env(),
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
            # Suivi requête par requête : statflows ignore tout de MLflow,
            # le rappel est le seul point de contact
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
                structures_path=downloads_config["PATHS"]["STRUCTURES_PATH"],
                last_download_path=downloads_config["PATHS"]["LAST_DOWNLOAD_PATH"],
                n_observations=downloads_config["N_LAST_OBSERVATIONS"],
                fresh_registry=False,
                max_runtime=timedelta(
                    weeks=downloads_config["MAX_RUNTIME"]["WEEKS"],
                    days=downloads_config["MAX_RUNTIME"]["DAYS"],
                    hours=downloads_config["MAX_RUNTIME"]["HOURS"],
                    minutes=downloads_config["MAX_RUNTIME"]["MINUTES"],
                    seconds=downloads_config["MAX_RUNTIME"]["SECONDS"]
                ),
                categorical_threshold=None,  # A supprimer avec la nouvelle version de la base de données
                bucket=downloads_config['BUCKET'],
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
        # Statut de sortie : contrôle après la clôture du tracker, pour que les métriques et la
        # table de diagnostic des requêtes en échec soient publiées avant l'échec de l'étape
        check_download_report(report, downloads_config.get("MAX_ERROR_RATIO"))
    finally:
        client.close()

# Exécution du script principal
if __name__ == "__main__":
    main()
