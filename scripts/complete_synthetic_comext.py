"""Script de complément de la table Comext par des données FICTIVES.

Comble les trous d'un téléchargement Comext trop lent : planifie exactement les
mêmes requêtes que ``scripts/download_eurostat_comext.py`` (produit × reporter),
écarte celles que le registre ``LAST_DOWNLOAD_PATH`` déclare déjà téléchargées
(données RÉELLES, jamais touchées), et simule les autres à partir du monde fictif
de ``config/profiles/demo/synthetic.yaml``. Chaque requête simulée est inscrite
au registre (marquée ``"synthetic": true``) : l'étape des vulnérabilités
partenaires la considère donc comme n'importe quelle série téléchargée.

Le schéma (colonnes, types, codes des agrégats de partenaires) est appris sur les
lignes DÉJÀ présentes dans la table, pour que l'écriture par clé primaire se fasse
dans la même table sans hypothèse. Les écritures sont regroupées par lot de
produits (``COMEXT.PRODUCTS_PER_WRITE``) plutôt qu'une par requête.

ATTENTION : données simulées, sans valeur statistique. Un téléchargement réel
ultérieur ne remplace que les dernières observations de chaque série (mise à jour
incrémentale) : pour revenir aux vraies données, vider le catalogue (cf.
``kubernetes/transition/README.md``).

Fichiers de configuration lus : ``EUROSTAT_CONFIG_PATH``, ``RUNTIME_CONFIG_PATH``
et ``SYNTHETIC_CONFIG_PATH``.
"""
# Importation des modules
# Modules de base
from collections import defaultdict
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any, Dict, List

# Modules de manipulation de données
import pandas as pd

# Importation des modules du package
from statflows import EurostatClient
from statflows.core.download import _primary_keys, _schema_name
from statflows.storage.ducklake.tables import write_dataframe
from statflows.storage.json import Saver

# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
)
from kedro_pipeline.synthetic.comext import (
    ComextConfig,
    ComextTemplate,
    build_comext,
)
from kedro_pipeline.synthetic.io import (
    SYNTHETIC_FLAG,
    _REGISTRY_ROOT,
    ensure_isolated_catalog,
    load_registry,
    load_synthetic_config,
    read_distinct_codes,
    read_table_sample,
)
from kedro_pipeline.synthetic.world import SyntheticWorld, WorldConfig
# Planification des requêtes Comext : même liste que le téléchargement réel
from scripts.download_eurostat_comext import (
    build_split_queries,
    cap_queries,
    fetch_dimension_codelists,
    load_config,
    load_runtime_config,
)

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Dimensions de découpage des requêtes Comext
_PRODUCT_DIM = "product"
_REPORTER_DIM = "reporter"


# Fonction de regroupement des requêtes par lot de produits
def group_by_product(queries: List[Any], products_per_write: int) -> List[List[Any]]:
    """Group queries into batches covering ``products_per_write`` distinct products.

    Args:
        queries: Queries exposing ``dimensions["product"]``, in planned order.
        products_per_write: Distinct products per batch (at least 1).

    Returns:
        Batches of queries, keeping the planned (product-major) order.

    Examples:
        >>> class Q:  # doctest: +SKIP
        ...     def __init__(self, p): self.dimensions = {"product": p}
        >>> [len(b) for b in group_by_product([Q("a"), Q("a"), Q("b")], 1)]  # doctest: +SKIP
        [2, 1]
    """
    by_product: Dict[str, List[Any]] = defaultdict(list)
    for query in queries:
        by_product[str(query.dimensions[_PRODUCT_DIM])].append(query)
    products = list(by_product)
    step = max(1, int(products_per_write))
    return [
        [q for product in products[i: i + step] for q in by_product[product]]
        for i in range(0, len(products), step)
    ]


# Fonction principale de complément
def main() -> None:
    """CLI entry point of the synthetic Comext completion script."""
    # Chargement des configurations
    config = load_config()
    runtime_config = load_runtime_config()
    synthetic = load_synthetic_config()
    DATAFLOW = config["DATAFLOW"]
    parameters = config["parameters"][DATAFLOW]
    downloads_config = config["DOWNLOADS"][DATAFLOW]
    schema = _schema_name(DATAFLOW)
    # Garde d'isolation, avant toute connexion ou appel réseau
    ensure_isolated_catalog(config["DOWNLOADS"]["DBNAME"], synthetic.get("SAFETY"))

    # Monde simulé et paramètres du complément
    world = SyntheticWorld(WorldConfig.from_mapping(synthetic))
    comext_config = ComextConfig.from_mapping(synthetic.get("COMEXT") or {})
    iso2_by_iso3 = {c.iso3: c.iso2 for c in world.config.countries}
    if not all(iso2_by_iso3.values()):
        raise ValueError("Chaque pays de synthetic.COUNTRIES doit porter un code iso2 (Comext)")

    # Planification des mêmes requêtes que le téléchargement réel
    client = EurostatClient()
    try:
        structure = client.get_dataflow_structure(dataflow=DATAFLOW)
        dims_codes = {
            dim: fetch_dimension_codelists(structure=structure, dimension=dim, client=client)
            for dim in config["split_filters"][DATAFLOW].keys()
        }
        queries = build_split_queries(
            dataflow=DATAFLOW,
            dims_codes=dims_codes,
            fixed_dims=config["fixed_dims"][DATAFLOW],
            split_filters=config["split_filters"][DATAFLOW],
            products_step=parameters.get("products_step", 1),
            period_windows=parameters.get("period_windows"),
            start_period=str(runtime_config["ANALYSIS_START_YEAR"]["eurostat"]),
        )
        queries = cap_queries(queries, parameters.get("max_queries"))
    finally:
        client.close()

    # Requêtes jamais téléchargées : seules candidates au complément
    last_download_path = downloads_config["PATHS"]["LAST_DOWNLOAD_PATH"]
    bucket = downloads_config["BUCKET"]
    registry = load_registry(last_download_path, bucket)
    missing = [q for q in queries if q.identity_key() not in registry]
    logger.info(
        "%d requête(s) planifiée(s), %d déjà téléchargée(s) (intactes), %d à simuler",
        len(queries), len(queries) - len(missing), len(missing),
    )
    if not missing:
        logger.info("Rien à compléter.")
        return

    # Connexion au catalogue et apprentissage du schéma de la table existante
    connector = build_connector(
        DuckLakeLocation(
            dbname=config["DOWNLOADS"]["DBNAME"],
            catalog_alias=config["DOWNLOADS"]["CATALOG_ALIAS"],
            schema=schema,
            bucket=bucket,
            data_path=downloads_config["PATHS"]["DATA_PATH"],
        ),
        pg=pg_credentials_from_env(),
        s3=s3_credentials_from_env(),
    )
    conn = connector.connect()
    written_rows, written_queries = 0, 0
    try:
        template = ComextTemplate.from_sample(
            read_table_sample(conn, connector.catalog_alias, schema),
            partner_codes=read_distinct_codes(
                conn, connector.catalog_alias, schema, "partner",
                [comext_config.world_prefix, comext_config.extra_eu_prefix,
                 comext_config.intra_eu_prefix],
            ),
        )
        logger.info(
            "Schéma appris sur la table existante : %s colonne(s), agrégats %s",
            len(template.dtypes) or "aucune (table vide)", list(template.partner_codes),
        )

        started_at = datetime.now(timezone.utc)
        for batch in group_by_product(missing, comext_config.products_per_write):
            frames = [
                build_comext(
                    world, iso2_by_iso3, comext_config, template,
                    reporter=str(q.dimensions[_REPORTER_DIM]),
                    product=str(q.dimensions[_PRODUCT_DIM]),
                    dimensions=q.dimensions, start_period=q.start_period,
                )
                for q in batch
            ]
            frames = [frame for frame in frames if not frame.empty]
            if frames:
                data = pd.concat(frames, ignore_index=True)
                write_dataframe(
                    conn, data, _primary_keys(structure, list(data.columns)),
                    catalog_alias=connector.catalog_alias, schema=schema, label=DATAFLOW,
                )
                written_rows += len(data)
            # Registre écrit APRÈS les données (même ordre que statflows) et marqué fictif
            for q in batch:
                registry[q.identity_key()] = {
                    "agency": q.agency,
                    "dataflow": q.dataflow,
                    "params": _json_params(q),
                    "last_download": datetime.now(timezone.utc).isoformat(),
                    SYNTHETIC_FLAG: True,
                }
            Saver().save(
                Path(last_download_path), {_REGISTRY_ROOT: registry},
                bucket=bucket, indent=2, ensure_ascii=False,
            )
            written_queries += len(batch)
            logger.info(
                "Complément fictif : %d/%d requête(s), %d ligne(s) écrite(s)",
                written_queries, len(missing), written_rows,
            )
    finally:
        conn.close()
    logger.info(
        "Complément terminé : %d requête(s) simulée(s), %d ligne(s)", written_queries, written_rows
    )


# Fonction de sérialisation des paramètres d'une requête pour le registre
def _json_params(query: Any) -> Dict[str, Any]:
    """Return a query's parameters in the registry's JSON-safe form.

    Args:
        query: Provider query object (exposes ``to_dict()``).

    Returns:
        JSON-serialisable parameters, as ``statflows`` writes them.
    """
    from statflows.core.download import _json_safe

    return _json_safe(query.to_dict())


# Exécution du script principal
if __name__ == "__main__":
    main()
