"""Script de remplissage de la table Comtrade par des données FICTIVES.

Remplace ``scripts/download_comtrade.py`` quand le fournisseur ne répond pas : il
exécute exactement le même téléchargement (mêmes requêtes planifiées par
``plan_queries``, même écriture DuckLake par ``download_updates``, même registre
``LAST_DOWNLOAD_PATH``), à une différence près — les lignes tariffline ne sont
pas demandées à l'API mais simulées par ``SyntheticComtradeClient`` à partir du
monde fictif décrit dans ``config/profiles/demo/synthetic.yaml``. La porte de
complétude de ``scripts/process_baci_hs.py`` et le redressement BACI s'exécutent
donc ensuite sans aucune adaptation.

Seules les codelists de référence (pays, produits) sont lues auprès de l'API
Comtrade : ce sont des fichiers statiques, distincts de l'API de données en
panne.

ATTENTION : données simulées, sans valeur statistique. Les entrées de registre
écrites sont marquées ``"synthetic": true``. Une requête déjà téléchargée
(réellement ou non) n'est pas régénérée, sauf option ``--force``.

Fichiers de configuration lus : ``COMTRADE_CONFIG_PATH``, ``RUNTIME_CONFIG_PATH``
et ``SYNTHETIC_CONFIG_PATH``.
"""
# Importation des modules
# Modules de base
import argparse
from datetime import datetime, timedelta, timezone
import logging
from typing import Optional, Sequence

# Importation des modules du package
from statflows import ComtradeClient
from statflows.core.download import download_updates, _schema_name

# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.download_report import check_download_report
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
)
from kedro_pipeline.synthetic.comtrade import (
    CountryReference,
    ReportingConfig,
    SyntheticComtradeClient,
)
from kedro_pipeline.synthetic.io import (
    load_synthetic_config,
    mark_synthetic_entries,
    read_table_sample,
)
from kedro_pipeline.synthetic.world import SyntheticWorld, WorldConfig
# Planification des requêtes Comtrade : même liste que le téléchargement réel
from scripts.download_comtrade import (
    cap_queries,
    fetch_dimension_codelists,
    load_config,
    load_runtime_config,
    plan_queries,
)

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)


# Fonction d'analyse des arguments de la ligne de commande
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse the command-line arguments.

    Args:
        argv: Arguments (``sys.argv[1:]`` when ``None``).

    Returns:
        The parsed namespace (``force``).
    """
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--force",
        action="store_true",
        help="regénère aussi les requêtes déjà téléchargées (mise à jour par clé primaire)",
    )
    return parser.parse_args(argv)


# Fonction principale de remplissage
def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point of the synthetic Comtrade seeding script."""
    args = parse_args(argv)
    # Chargement des configurations
    config = load_config()
    runtime_config = load_runtime_config()
    synthetic = load_synthetic_config()
    # Dataflow à remplir (C-05)
    DATAFLOW = config["DATAFLOW"]
    downloads_config = config["DOWNLOADS"][DATAFLOW]

    # Monde simulé et paramètres de déclaration
    world = SyntheticWorld(WorldConfig.from_mapping(synthetic))
    reporting = ReportingConfig.from_mapping(synthetic.get("REPORTING") or {})

    # Codelists de référence (fichiers statiques de l'API Comtrade)
    reference_client = ComtradeClient(subscription_key=None)
    try:
        reporters_codelist = reference_client.get_metadata("reporter")
        products_codelist = reference_client.get_metadata("cmd:HS")
        dims_codes = {
            "reporters": fetch_dimension_codelists("reporter", client=reference_client),
            "products": fetch_dimension_codelists("cmd:HS", client=reference_client),
        }
    finally:
        reference_client.close()
    reference = CountryReference.from_codelist(reporters_codelist, list(world.iso3))
    labels = dict(zip(products_codelist["id"].astype(str), products_codelist["text"].astype(str)))
    # Univers des produits d'une requête non restreinte : codes SH6 (feuilles de la nomenclature)
    product_universe = sorted(
        code for code in products_codelist["id"].astype(str) if code.isdigit() and len(code) == 6
    )

    # Connecteur au catalogue (même emplacement que le téléchargement réel)
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
    # Types de la table existante (le cas échéant) : les lignes fictives s'y alignent
    conn = connector.connect()
    try:
        sample = read_table_sample(conn, connector.catalog_alias, _schema_name(DATAFLOW))
    finally:
        conn.close()
    table_dtypes = dict(sample.dtypes) if sample is not None and not sample.empty else None

    # Client fictif : seules les requêtes de données sont simulées
    client = SyntheticComtradeClient(
        world, reference, reporting, product_labels=labels,
        product_universe=product_universe, force=args.force,
        table_dtypes=table_dtypes,
    )
    started_at = datetime.now(timezone.utc)
    try:
        queries = plan_queries(config, runtime_config, client, dims_codes)
        queries = cap_queries(queries, config["parameters"][DATAFLOW].get("max_queries"))
        logger.info(
            "Remplissage FICTIF de %s : %d requête(s) planifiée(s), %d pays simulés, %d–%d",
            DATAFLOW, len(queries), len(world.iso3),
            world.config.year_start, world.config.year_end,
        )
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
            categorical_threshold=None,
            bucket=downloads_config["BUCKET"],
            storage_options=None,
        )
        logger.info("Remplissage terminé : %s", report.to_metrics())
        # Marquage des entrées de registre écrites par ce run
        mark_synthetic_entries(
            downloads_config["PATHS"]["LAST_DOWNLOAD_PATH"], downloads_config["BUCKET"], started_at
        )
        check_download_report(report, downloads_config.get("MAX_ERROR_RATIO"))
    finally:
        client.close()


# Exécution du script principal
if __name__ == "__main__":
    main()
