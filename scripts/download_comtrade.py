"""Script de téléchargement des données tariffline UN Comtrade.

Télécharge les données de commerce international depuis l'API UN Comtrade en
scindant les requêtes par année x lot de produits HS6 (PD-07), dans un ordre
**année-majeur** (PD-06, PS-12.1) : toutes les requêtes d'une année précèdent
celles de l'année suivante, pour qu'une année soit complète — et donc
utilisable par la porte de complétude BACI — le plus tôt possible. La mise à
jour incrémentale est déléguée à ``download_updates`` : le client Comtrade
encode dans sa méthode ``fetch_updates`` la règle « une période n'est
re-téléchargée que si sa date de publication (``lastReleased``) est
postérieure au dernier téléchargement enregistré », et ``download_updates``
traite d'abord, par tri stable, les requêtes jamais téléchargées : l'ordre de
la liste construite ici fait donc foi pour le rattrapage.

Fichiers de configuration lus : ``COMTRADE_CONFIG_PATH`` (défaut
``config/datasets/comtrade.yaml``) et ``RUNTIME_CONFIG_PATH`` (défaut
``config/runtime.yaml``).
"""
# Importation des modules
# Modules de base
import os
import itertools
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, TypeVar, Union

import yaml

# Modules de manipulation de données
import pandas as pd

# Importation des modules du package
from statflows import ComtradeClient, ComtradeQueryRequest
from statflows.core.factory import filter_codes
from statflows.core.download import download_updates, _schema_name

# Module de suivi d'exécution
from macroforecast.tracking import CapturingTracker, get_tracker, rekey_metrics
from statflows.core.reports import QueryReport

# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.download_report import check_download_report
from scripts._run_report import RunScope, build_download_report, guarded_run, run_name
from kedro_pipeline.steps.reference import publish_reference
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

# Clé des bornes de périodes dans les filtres de découpage (hors codelists)
_PERIODS_KEY = "periods"
# Ordres admis de la boucle sur les années
_PERIODS_ORDERS = ("desc", "asc")
# Variable d'environnement de la clé d'API Comtrade (abonnement premium)
_SUBSCRIPTION_KEY_ENV = "COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY"

T = TypeVar("T")


# Fonction de chargement de la configuration
def load_config(config_path: Optional[os.PathLike] = None) -> dict:
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
        config_path = os.environ.get("COMTRADE_CONFIG_PATH", "config/datasets/comtrade.yaml")

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


# Colonne portant le code dans les métadonnées de référence Comtrade, par catégorie
_REFERENCE_CODE_COLUMNS = {"reporter": "reporterCode", "partner": "PartnerCode"}


# Fonction de récupération de la liste des codes d'une catégorie de référence
def fetch_dimension_codelists(
    dimension: str,
    client: Optional[ComtradeClient] = None,
) -> pd.DataFrame:
    """Fetch the codelist of a UN Comtrade reference dimension, with its labels.

    Generic counterpart of the Comext ``fetch_dimension_codelists`` helper:
    UN Comtrade exposes no Data Structure Definition, so valid codes are read
    directly from the reference metadata of ``dimension`` rather than deduced
    from a dataflow structure. The metadata rows of the valid codes are kept
    (labels, ISO codes, ``isGroup``, ``parent``…) so the reference tables can
    be published without a second network call (PS-28.4).

    Args:
        dimension: Reference dimension (e.g. ``"reporter"`` or ``"cmd:HS"``).
        client: ComtradeClient instance; a new one is created if ``None``.

    Returns:
        DataFrame with a leading ``code`` column (valid codes, as text, in the
        order of the metadata) followed by the metadata columns.

    Examples:
        >>> reporter_codes = fetch_dimension_codelists("reporter")  # doctest: +SKIP
        >>> reporter_codes[["code", "reporterDesc"]].head(1)  # doctest: +SKIP
    """
    from statflows.sources.comtrade.parsing import extract_codes

    # Initialisation du client s'il n'est pas spécifié
    if client is None:
        client = ComtradeClient()

    # Métadonnées de la catégorie (un seul appel) puis codes valides
    metadata = client.get_metadata(category=dimension)
    codes = {str(code) for code in extract_codes(metadata, dimension)}
    code_column = _REFERENCE_CODE_COLUMNS.get(dimension, "id")
    codelist = metadata[metadata[code_column].astype(str).isin(codes)].copy()
    codelist.insert(0, "code", codelist[code_column].astype(str))

    # Logging
    logger.info("%d codes for dimension '%s'", len(codelist), dimension)

    return codelist.reset_index(drop=True)


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
        >>> cap_queries([1, 2, 3], 2)
        [1, 2]
    """
    return list(itertools.islice(queries, max_queries))


# Fonction de résolution des périodes à télécharger
def resolve_periods(
    client: Any,
    periods_filter: Optional[Mapping[str, Any]],
    default_start: int,
    frequency: str = "annual",
) -> List[str]:
    """Resolve the list of periods to download, bounds included.

    ``ComtradeClient.get_valid_periods`` drops the last period of its range
    (the current, incomplete one when no end is given — but also an explicit
    ``period_end``): it is therefore called without end, and the upper bound
    is applied here, inclusively.

    Args:
        client: Object exposing ``get_valid_periods`` (a ``ComtradeClient``;
            the method is local, no API call).
        periods_filter: ``{"start": int | None, "end": int | None}`` bounds
            (years, included); ``start`` falls back to ``default_start``.
        default_start: First year of the analysis
            (``runtime.ANALYSIS_START_YEAR.comtrade``).
        frequency: ``"annual"`` or ``"monthly"``.

    Returns:
        Periods in API format (``YYYY`` or ``YYYYMM``), increasing.

    Examples:
        >>> resolve_periods(client, {"start": 2015, "end": None}, 1994)  # doctest: +SKIP
        ['2015', ..., '2025']
    """
    # Bornes configurées (start nul → première année de l'analyse)
    periods_filter = periods_filter or {}
    start = periods_filter.get("start")
    start = default_start if start is None else start
    end = periods_filter.get("end")

    # Périodes valides depuis la borne basse (l'année en cours est exclue)
    periods = client.get_valid_periods(period_start=str(start), frequency=frequency)
    # Borne haute incluse, appliquée sur l'année
    if end is not None:
        periods = [p for p in periods if int(str(p)[:4]) <= int(end)]
    return [str(p) for p in periods]


# Fonction de résolution des valeurs d'une dimension scindée
def _dimension_values(
    codes: pd.DataFrame, filters: Mapping[str, Any]
) -> Optional[List[str]]:
    """Filter a codelist, or return ``None`` when no filter narrows it.

    Args:
        codes: Codelist DataFrame with a ``code`` column.
        filters: include/exclude filters forwarded to ``filter_codes``.

    Returns:
        ``None`` (every code, left to the API) when all filters are null,
        else the sorted list of selected codes.
    """
    # Aucun filtre : la dimension n'est pas restreinte (valeur None à l'appel)
    if all(value is None for value in filters.values()):
        return None
    return filter_codes(codes["code"], **filters)


# Fonction de construction des requêtes
def build_split_queries(
    dataflow: str,
    dims_codes: Dict[str, pd.DataFrame],
    fixed_dims: Dict[str, Any],
    split_filters: Dict[str, Dict[str, Union[List[str], str, None]]],
    periods: Sequence[str],
    products_step: Optional[int] = 10,
    periods_order: str = "desc",
) -> List[ComtradeQueryRequest]:
    """Build the split Comtrade queries, one per (period, product batch).

    The include/exclude filters of the YAML configuration are applied to the
    ``reporters`` and ``products`` codelists. A dimension whose filters are all
    null is not restricted: it is sent as ``None`` (every code, e.g. all
    reporters) in a single query. Otherwise the filtered reporters are sent as
    a single list, and the filtered products are grouped once, in natural code
    order, into batches of ``products_step``.

    The returned list is **period-major** (PS-12.1): the outer loop runs over
    the periods (``periods_order``), the inner loop over the product batches.
    ``download_updates`` sorts never-downloaded queries first with a stable
    sort, so a period is complete before the next one starts.

    Args:
        dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).
        dims_codes: Mapping ``{"reporters": df, "products": df}`` of the
            codelists to filter, each a DataFrame with a ``code`` column.
        fixed_dims: Fixed query fields shared by every query (``flows``,
            ``type_code``, ``classification``, ``frequency``). Must not carry
            ``periods``, ``products`` or ``reporters``.
        split_filters: Per-dimension include/exclude filters, keyed like
            ``dims_codes``; an optional ``"periods"`` entry (bounds, resolved
            by :func:`resolve_periods`) is ignored here.
        periods: Periods to download (``YYYY`` or ``YYYYMM``).
        products_step: Number of products per query; ``None`` or ``0`` sends
            the whole product selection in each query.
        periods_order: ``"desc"`` (recent periods first) or ``"asc"``.

    Returns:
        List of ``ComtradeQueryRequest`` objects, period-major.

    Raises:
        ValueError: If ``split_filters`` and ``dims_codes`` do not share the
            same keys, or ``periods_order`` is invalid.

    Examples:
        >>> queries = build_split_queries(
        ...     "C_A_HS",
        ...     {"reporters": pd.DataFrame({"code": ["251"]}),
        ...      "products": pd.DataFrame({"code": ["010121", "010129", "854140"]})},
        ...     {"frequency": "annual"},
        ...     {"reporters": {"include": None}, "products": {"include": None}},
        ...     periods=["2023", "2024"], products_step=2,
        ... )
        >>> [(q.periods, q.products) for q in queries]
        [('2024', ['010121', '010129']), ('2024', ['854140']), ('2023', ['010121', '010129']), ('2023', ['854140'])]
    """
    # Filtres de codelists (les bornes de périodes sont résolues séparément)
    code_filters = {k: v for k, v in split_filters.items() if k != _PERIODS_KEY}

    # Vérification que les filtres et les codelists partagent les mêmes clés
    if set(code_filters.keys()) != set(dims_codes.keys()):
        raise ValueError(
            f"'split_filters' and 'dims_codes' should have similar keys. "
            f"Found {code_filters.keys()} for 'split_filters' and "
            f"{dims_codes.keys()} for 'dims_codes'"
        )
    # Vérification de l'ordre des périodes
    if periods_order not in _PERIODS_ORDERS:
        raise ValueError(
            f"Invalid periods_order: {periods_order!r}. Should be in {_PERIODS_ORDERS}"
        )

    # Valeurs des dimensions non découpées en lots (une seule valeur par requête)
    other_dims = {
        dim: _dimension_values(dims_codes[dim], filters)
        for dim, filters in code_filters.items()
        if dim != "products"
    }

    # Lots de produits formés une fois, identiques pour toutes les années
    products = (
        _dimension_values(dims_codes["products"], code_filters["products"])
        if "products" in code_filters
        else None
    )
    if products is not None and products_step:
        batches: List[Optional[List[str]]] = [
            products[i: i + products_step] for i in range(0, len(products), products_step)
        ]
    else:
        # Pas de découpage produit : toute la sélection (ou None) dans chaque requête
        batches = [products]

    # Ordre des années : boucle externe (récentes d'abord par défaut)
    ordered_periods = sorted(
        (str(p) for p in periods), key=int, reverse=(periods_order == "desc")
    )

    # Construction année-majeure : toutes les requêtes d'une année, puis la suivante
    queries = [
        ComtradeQueryRequest(
            dataflow=dataflow,
            periods=period,
            products=batch,
            **other_dims,
            **fixed_dims,
        )
        for period in ordered_periods
        for batch in batches
    ]

    # Logging
    logger.info(
        "Successfully built %d splitted queries (%d period(s) x %d product batch(es)).",
        len(queries), len(ordered_periods), len(batches),
    )

    return queries


# Fonction de planification de la liste complète des requêtes d'un dataflow
def plan_queries(
    config: Mapping[str, Any],
    runtime_config: Mapping[str, Any],
    client: Any,
    dims_codes: Optional[Dict[str, pd.DataFrame]] = None,
) -> List[ComtradeQueryRequest]:
    """Plan the full, ordered list of Comtrade queries of the configured dataflow.

    Single source of the planned queries: used by the download script and by
    the BACI completeness gate (``scripts/process_baci_hs.py``), which must
    reason on exactly the same batches.

    Args:
        config: Parsed ``comtrade.yaml`` (``DATAFLOW``, ``parameters``,
            ``fixed_dims``, ``split_filters``).
        runtime_config: Parsed ``runtime`` mapping (``ANALYSIS_START_YEAR``).
        client: ``ComtradeClient`` (codelists and valid periods).
        dims_codes: Codelists ``{"reporters": df, "products": df}``; fetched
            from the API when ``None``.

    Returns:
        Ordered list of ``ComtradeQueryRequest`` (uncapped).
    """
    # Dataflow et sections de configuration associées
    dataflow = config["DATAFLOW"]
    parameters = config["parameters"][dataflow]
    fixed_dims = config["fixed_dims"][dataflow]
    split_filters = config["split_filters"][dataflow]

    # Codelists des dimensions scindées
    if dims_codes is None:
        dims_codes = {
            "reporters": fetch_dimension_codelists("reporter", client=client),
            "products": fetch_dimension_codelists("cmd:HS", client=client),
        }

    # Périodes : bornes configurées, première année de l'analyse par défaut (PD-08)
    periods = resolve_periods(
        client,
        split_filters.get(_PERIODS_KEY),
        default_start=runtime_config["ANALYSIS_START_YEAR"]["comtrade"],
        frequency=fixed_dims.get("frequency", "annual"),
    )

    return build_split_queries(
        dataflow=dataflow,
        dims_codes=dims_codes,
        fixed_dims=fixed_dims,
        split_filters=split_filters,
        periods=periods,
        products_step=parameters["products_step"],
        periods_order=parameters.get("periods_order", "desc"),
    )


# Nœud du rapport de run (clé de config/tracking.yaml)
NODE = "download_comtrade"


# Fonction principale de téléchargement
def main() -> None:
    """CLI entry point for the Comtrade download script.

    Raises:
        DownloadFailureError: If the share of failed queries exceeds ``MAX_ERROR_RATIO``,
            after the run report was published.
    """
    # Chargement des configurations
    config = load_config()
    runtime_config = load_runtime_config()
    # Dataflow à télécharger (C-05)
    DATAFLOW = config["DATAFLOW"]
    downloads_config = config["DOWNLOADS"][DATAFLOW]

    # Construction du suivi d'exécution : sans URI (ou sans MLflow installé, ou serveur
    # injoignable), get_tracker retourne un tracker inerte et l'exécution est strictement
    # inchangée. Le run est ouvert dès le début pour que tout échec porte son rapport.
    mlflow_config = config.get("MLFLOW") or {}
    tracker = CapturingTracker(
        get_tracker(
            tracking_uri=mlflow_config.get("TRACKING_URI"),
            experiment=mlflow_config.get("EXPERIMENT", "comtrade-download"),
            run_name=run_name(f"{DATAFLOW}-{datetime.now():%Y%m%d-%H%M}", NODE),
        )
    )
    scope = RunScope(NODE)
    with tracker, guarded_run(scope, tracker):
        report = _download(config, runtime_config, tracker, scope)

    # Statut de sortie : statflows isole les erreurs par requête, le script doit donc
    # échouer explicitement au-delà du seuil toléré (sinon l'étape resterait « réussie »),
    # après la publication du rapport de run
    check_download_report(report, downloads_config.get("MAX_ERROR_RATIO"))


# Planification, téléchargement et rapport de run
def _download(config: dict, runtime_config: dict, tracker: CapturingTracker, scope: RunScope):
    """Plan the queries, download them and publish the run report.

    Args:
        config: Comtrade download configuration.
        runtime_config: Shared runtime configuration.
        tracker: Capturing tracker of the open run.
        scope: Report scope of the run.

    Returns:
        The ``statflows`` download report.
    """
    DATAFLOW = config["DATAFLOW"]
    downloads_config = config["DOWNLOADS"][DATAFLOW]
    mlflow_config = config.get("MLFLOW") or {}

    # Initialisation du client comtrade
    scope.step = "planification des requêtes"
    client = ComtradeClient(subscription_key=os.environ.get(_SUBSCRIPTION_KEY_ENV))
    try:
        # Codelists des dimensions scindées (libellés compris, réutilisés par les référentiels)
        dims_codes = {
            "reporters": fetch_dimension_codelists("reporter", client=client),
            "products": fetch_dimension_codelists("cmd:HS", client=client),
        }
        # Construction de la liste ordonnée des requêtes (année-majeure)
        queries = plan_queries(config, runtime_config, client, dims_codes=dims_codes)
        # Plafond optionnel (null = aucun, C-01)
        queries = cap_queries(queries, config["parameters"][DATAFLOW].get("max_queries"))

        # Initialisation du connecteur au catalogue (catalog fixé à PROVIDER_CONFIG_NAME)
        connector = build_connector(
            DuckLakeLocation(
                dbname=config["DOWNLOADS"]["DBNAME"],
                catalog_alias=client.PROVIDER_CONFIG_NAME,
                schema=_schema_name(DATAFLOW),
                bucket=downloads_config["BUCKET"],
                data_path=downloads_config["PATHS"]["DATA_PATH"],
            ),
            pg=pg_credentials_from_env(),
            s3=s3_credentials_from_env(),
        )

        # Référentiels (libellés des déclarants et des produits, PS-28.4) depuis les
        # codelists déjà récupérées ; non bloquant pour le téléchargement
        reference = publish_reference(
            {"reporter": dims_codes["reporters"], "cmd:HS": dims_codes["products"]},
            connector,
            source="comtrade",
            params={
                **config["DOWNLOADS"]["REFERENCE"],
                "NOMENCLATURES": runtime_config["NOMENCLATURES"]["HS"],
                "YEAR": datetime.now().year,
            },
        )
        logger.info(f"Référentiels Comtrade : {reference['rows']} ; échecs : {reference['failures']}")

        # Suivi requête par requête : statflows ignore tout de MLflow, le rappel est le
        # seul point de contact
        streamed_queries: List[QueryReport] = []

        def stream_query_metrics(query_report: QueryReport) -> None:
            """Send one query's diagnostics to the tracker."""
            tracker.log_metrics(rekey_metrics(query_report.to_metrics()), step=len(streamed_queries))
            streamed_queries.append(query_report)

        # Téléchargement des données (mise à jour incrémentale via fetch_updates)
        scope.step = "téléchargement"
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
                seconds=downloads_config["MAX_RUNTIME"]["SECONDS"],
            ),
            categorical_threshold=None,  # A supprimer avec la nouvelle version de la base de données
            bucket=downloads_config["BUCKET"],
            storage_options=None,
            on_query_complete=stream_query_metrics,
        )
        # Rapport de run : métriques, table par requête, contrôles, description
        scope.step = "rapport de run"
        run_report = build_download_report(
            scope, tracker, report, downloads_config.get("MAX_ERROR_RATIO"),
            log_artifacts=bool(mlflow_config.get("LOG_ARTIFACTS", True)),
            tags={
                "dataflow": DATAFLOW,
                "stopped_early": str(report.stopped_early),
                "n_queries_planned": str(report.n_queries_planned),
            },
        )
        scope.publish(tracker, run_report)
        # Logging
        logger.info("Téléchargement terminé : %s", report.to_metrics())
        return report
    finally:
        client.close()


# Exécution du script principal
if __name__ == "__main__":
    main()
