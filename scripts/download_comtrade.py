"""Script de téléchargement des données tariffline UN Comtrade.

Télécharge les données de commerce international depuis l'API UN Comtrade en
scindant les requêtes par période x lot de produits. La mise à jour
incrémentale est déléguée à ``download_updates`` : le client Comtrade encode
dans sa méthode ``fetch_updates`` la règle « une période n'est re-téléchargée
que si sa date de publication (``lastReleased``) est postérieure au dernier
téléchargement enregistré ».
"""
# Importation des modules
# Modules de base
import os
import itertools
import re
import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

import yaml

# Modules de manipulation de données
import pandas as pd

# Importation des modules du package
from statflows import ComtradeClient, ComtradeQueryRequest
from statflows.core.factory import filter_codes
from statflows.core.download import download_updates, _schema_name

# Module de connexion à la base de données
from dt_ducklake_manager import DuckLakeConnector


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
        config_path: Path to config file. If None, uses default location or
            the CONFIG_PATH environment variable.

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


# Fonction de récupération de la liste des codes d'une catégorie de référence
def fetch_dimension_codelists(
    dimension: str,
    client: Optional[ComtradeClient] = None,
) -> pd.DataFrame:
    """Fetch the codelist of a UN Comtrade reference dimension.

    Generic counterpart of the Comext ``fetch_dimension_codelists`` helper:
    UN Comtrade exposes no Data Structure Definition, so valid codes are read
    directly from the reference metadata of ``dimension`` rather than deduced
    from a dataflow structure.

    Args:
        dimension: Reference dimension (e.g. ``"reporter"`` or ``"cmd:HS"``).
        client: ComtradeClient instance; a new one is created if ``None``.
        code_length: When set, keep only the codes of that exact length (e.g.
            ``6`` to restrict the HS nomenclature to 6-digit sub-headings, a
            request on an aggregated HS code already covering its children).

    Returns:
        DataFrame with a single ``code`` column.

    Examples:
        >>> reporter_codes = fetch_codes("reporter")  # doctest: +SKIP
        >>> product_codes = fetch_codes("cmd:HS", code_length=6)  # doctest: +SKIP
    """
    # Initialisation du client s'il n'est pas spécifié
    if client is None:
        client = ComtradeClient()

    # Extraction des codes valides de la catégorie
    codes = [str(code) for code in client._extract_codes(dimension)]

    # Logging
    logger.info("%d codes for dimension '%s'", len(codes), dimension)

    return pd.DataFrame({"code": codes})


# Fonction de construction des requêtes
def build_split_queries(
    dataflow: str,
    dims_codes: Dict[str, pd.DataFrame],
    fixed_dims: Dict[str, Any] = {},
    split_filters: Dict[str, Dict[str, Union[List[str], str]]] = {},
    products_step: int = 10,
) -> List[ComtradeQueryRequest]:
    """Build the split queries for a Comtrade dataflow.

    Mirrors the Comext ``build_split_queries``: the include/exclude filters
    declared in the YAML configuration are applied to the ``reporter`` and
    ``product`` codelists before crossing the split dimensions. Two Comtrade
    specificities are preserved: products are grouped into batches of
    ``products_step`` (rather than one value per query) and reporters are sent
    as a single value — ``None`` (all reporters) when the configuration does
    not narrow them — to keep the number of API calls small.

    Args:
        dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).
        dims_codes: Mapping ``{"reporter": df, "product": df}`` of the
            codelists to filter, each a DataFrame with a ``code`` column.
        fixed_dims: Fixed query fields shared by every query (e.g. ``flows``,
            ``type_code``, ``classification``), splatted into every
            ``ComtradeQueryRequest``. Must not carry a key already set
            explicitly below (``dataflow``, ``reporters``, ``products``,
            ``periods``, ``frequency``).
        split_filters: Per-dimension include/exclude filters, keyed like
            ``dims_codes``.
        products_step: Number of products per query.
        frequency: Data frequency (``"annual"`` or ``"monthly"``).
        periods: Explicit list of periods; computed from the client when
            omitted.
        period_start: Start period (used when ``periods`` is omitted).
        period_end: End period (used when ``periods`` is omitted).
        client: ComtradeClient instance; a new one is created if ``None``.

    Returns:
        List of ``ComtradeQueryRequest`` objects, one per (period, product
        batch) pair.

    Raises:
        ValueError: If ``split_filters`` and ``dims_codes`` do not share the
            same keys.

    Examples:
        >>> queries = build_split_queries(
        ...     "C_A_HS", dims_codes, fixed_dims, split_filters
        ... )  # doctest: +SKIP
        >>> len(queries) > 0
        True
    """
    # Vérification que 'split_filters' et 'dims_codes' partagent les mêmes clés
    if set(split_filters.keys()) != set(dims_codes.keys()):
        raise ValueError(
            f"'split_filters' and 'dims_codes' should have similar keys. "
            f"Found {split_filters.keys()} for 'split_filters' and "
            f"{dims_codes.keys()} for 'dims_codes'"
        )

    # Construction d'une requête par (période, lot de produits)
    if ("products" in split_filters.keys()) and (products_step > 0):
        # Construction des codes associés aux dimensions splitées à croiser
        split_dims_codes = [filter_codes(dims_codes[split_dim]["code"], **split_filters[split_dim]) for split_dim in split_filters.keys() if split_dim != "products"]
        # Extraction des produits
        products = filter_codes(dims_codes["products"]["code"], **split_filters["products"])
        # Construction des requêtes
        queries = [
            ComtradeQueryRequest(
                dataflow=dataflow,
                products=products[i: i + products_step],
                **fixed_dims,
                **{key: value for key, value in zip(split_filters.keys(), dim_values)}
            )
            for i in range(0, len(products), products_step)
            for dim_values in itertools.product(*split_dims_codes) 
        ]
    else:
        # Construction des codes associés aux dimensions splitées à croiser
        split_dims_codes = [filter_codes(dims_codes[split_dim]["code"], **split_filters[split_dim]) for split_dim in split_filters.keys()]
        # Construction des requêtes
        queries = [
            ComtradeQueryRequest(
                dataflow=dataflow,
                **fixed_dims,
                **{key: value for key, value in zip(split_filters.keys(), dim_values)}
            )
            for i in range(0, len(products), products_step)
            for dim_values in itertools.product(*split_dims_codes) 
        ]

    # Logging
    logger.info("Successfully built %d splitted queries.", len(queries))

    return queries


# Fonction principale de téléchargement
def main() -> None:
    """CLI entry point for the Comtrade download script."""
    # Chargement de la configuration
    config = load_config()
    # Spécification du dataflow que l'on souhaite télécharger
    DATAFLOW = "C_A_HS"

    # Initialisation du client comtrade
    client = ComtradeClient(subscription_key=os.environ.get('COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY'))
    try:
        # Extraction des codes des dimensions selon lesquelles scinder les requêtes
        dims_codes = {
            "reporters": fetch_dimension_codelists("reporter", client=client),
            "products": fetch_dimension_codelists("cmd:HS", client=client),
        }

        # Construction des requêtes selon les dimensions souhaitées
        queries = build_split_queries(
            dataflow=DATAFLOW,
            dims_codes=dims_codes,
            fixed_dims=config["fixed_dims"][DATAFLOW],
            split_filters=config["split_filters"][DATAFLOW],
            products_step=config["parameters"][DATAFLOW]["products_step"],
        )
        # Restriction de test à 5 requêtes
        queries = queries[:10]

        # Initialisation du connecteur au catalogue
        connector = DuckLakeConnector.from_postgres(
            data_path=f"s3://{config['DOWNLOADS'][DATAFLOW]['BUCKET']}/{config['DOWNLOADS'][DATAFLOW]['PATHS']['DATA_PATH']}",
            dbname=config["DOWNLOADS"]["DBNAME"],
            host=os.environ["PGHOST"],
            port=os.environ["PGPORT"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            create_db_if_missing=True,
            admin_dbname=os.environ['PGDATABASE'],
            admin_user="postgres",
            admin_password=os.environ["PGPASSWORD"],
            catalog_alias=client.PROVIDER_CONFIG_NAME,  # catalog fixé à PROVIDER_CONFIG_NAME
            schema=_schema_name(DATAFLOW),  # re.sub(r"[^a-zA-Z0-9]", "", DATAFLOW),
            s3_endpoint=os.environ["AWS_S3_ENDPOINT"],
            s3_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            s3_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            s3_session_token=os.environ["AWS_SESSION_TOKEN"],
        )

        # Téléchargement des données (mise à jour incrémentale via fetch_updates)
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
                seconds=config["DOWNLOADS"][DATAFLOW]["MAX_RUNTIME"]["SECONDS"],
            ),
            categorical_threshold=None,  # A supprimer avec la nouvelle version de la base de données
            bucket=config["DOWNLOADS"][DATAFLOW]["BUCKET"],
            storage_options=None,
        )
    finally:
        client.close()


# Exécution du script principal
if __name__ == "__main__":
    main()