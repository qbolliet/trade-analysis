"""Script de calcul/mise à jour des indicateurs de vulnérabilité commerciale.

Enveloppe CLI de la fonction d'étape
``kedro_pipeline.steps.partners.run_partner_vulnerabilities`` : indicateurs
partenaires (HHI, CDI2, CDI3) de chaque sens de ``FLOWS``, écrits dans une table
dont la clé porte la nomenclature (``classification``) — lignes en vigueur (codes
tels que déclarés dans Comext) et lignes historiques (flux SH6 convertis vers
chaque millésime de ``VINTAGES`` par les tables de passage UNSD partagées avec
BACI). Fraîcheur par unité (classification x reporter x produit), registre
fragmenté par classification x reporter, écrit après succès du calcul et de
l'écriture.

Ce script charge les paramètres, construit les poignées des tables source et
résultat (identifiants lus à la première connexion), le registre de fraîcheur, la
vue du registre de téléchargement et un run MLflow par passe (en vigueur, puis
un par millésime historique), appelle l'étape, puis sort en erreur si une passe
a échoué. Les fonctions de l'étape restent importables d'ici (tests, outils de
migration).

Configuration lue : blocs ``eurostat``, ``vulnerabilities``, ``baci`` et
``runtime`` des paramètres Kedro, environnement choisi par ``KEDRO_ENV``.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from functools import partial
import logging
import os
from typing import Any, Optional

# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    DuckLakeTable,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
    workflow_run_id,
)
from kedro_pipeline.io.freshness import ForceSpec, adopt_legacy_flag
from kedro_pipeline.io.registry_views import DownloadRegistryView
from kedro_pipeline.config import experiment_name, load_parameters, read_config_file
from kedro_pipeline.steps._config import download_location, schema_name, vulnerabilities_location
# Étape partenaires (ré-exportée : fraîcheur, millésimes, calcul)
from kedro_pipeline.steps.partners import (  # noqa: F401
    CLASSIFICATION_COL,
    CONCORDANCE_FINGERPRINT,
    HS_VINTAGE_COL,
    IN_FORCE_COL,
    NODE,
    PROVISIONAL_COL,
    STEP,
    HistoricalUnits,
    PartnerPass,
    PartnerPlan,
    PartnerStepResult,
    annotate_nomenclature,
    compute_partner_units,
    ensure_nomenclature_key,
    historical_conversions,
    historical_requested,
    historical_source_predicate,
    historical_units,
    load_flows,
    load_last_computation_dates,
    load_last_download_dates,
    load_partner_concordances,
    nomenclature_config,
    pairs_to_recompute,
    partner_classification,
    partner_registry,
    partner_requested,
    partner_units,
    plan_partner_passes,
    plan_partner_units,
    prepare_vintage_flows,
    record_computed_units,
    run_partner_step,
    run_partner_vulnerabilities,
    save_last_computation_dates,
    vulnerability_config_from_params,
)
# Paramètres d'exécution partagés (même lecteur que le téléchargement)
from scripts.download_comtrade import load_runtime_config
from scripts._run_report import script_runs, timestamp

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)


# Fabrique de connecteur : identifiants lus dans l'environnement à la première connexion
def _connector(location: DuckLakeLocation, pg: Any = None, s3: Any = None, **kwargs: Any) -> Any:
    """Build the connector of a table, reading the credentials only when connecting."""
    return build_connector(location, pg_credentials_from_env(), s3_credentials_from_env(), **kwargs)


# Fonction de chargement de la configuration (bloc dédié à eurostat)
def load_eurostat_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the Eurostat Comext download configuration (source of the metrics).

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

# Fonction de chargement de la configuration (bloc dédié au calcul des vulnérabilités)
def load_vulnerability_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the vulnerability-computation configuration.

    Deliberately a separate block from the download step's ``eurostat`` block.

    Args:
        config_path: Explicit YAML file (``vulnerabilities`` root key or
            historical format without it). If None, the ``vulnerabilities``
            block of the Kedro parameters of the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary.
    """
    if config_path is None:
        return load_parameters()["vulnerabilities"]
    return read_config_file(config_path, "vulnerabilities")

# Fonction de chargement de la configuration BACI (cache des tables de passage)
def load_baci_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the BACI configuration, which locates the correspondence-table cache.

    Args:
        config_path: Explicit YAML file (``baci`` root key or historical format
            without it). If None, the ``baci`` block of the Kedro parameters of
            the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary.
    """
    if config_path is None:
        return load_parameters()["baci"]
    return read_config_file(config_path, "baci")



# Fonction principale de calcul des vulnérabilités
def main() -> None:
    """CLI entry point for the incremental vulnerability computation script.

    Raises:
        RuntimeError: If at least one pass failed, once every pass has been
            attempted.
    """
    eurostat_config, vulnerability_config = load_eurostat_config(), load_vulnerability_config()
    runtime_config = load_runtime_config()
    dataflow = eurostat_config["DATAFLOW"]
    block = vulnerability_config["VULNERABILITIES"][dataflow]
    downloads = eurostat_config["DOWNLOADS"][dataflow]
    nomenclatures = runtime_config["NOMENCLATURES"]["HS"]
    # Poignées source et résultat (connexions ouvertes par l'étape, seulement si besoin)
    source = DuckLakeTable.lazy(download_location(eurostat_config), None, None, connector_factory=_connector)
    result = DuckLakeTable.lazy(
        vulnerabilities_location(
            vulnerability_config, schema=schema_name(block["RESULT_SCHEMA"]),
            bucket=block["BUCKET"], data_path=block["PATHS"]["DATA_PATH"],
        ),
        None, None, connector_factory=_connector,
    )
    # Un run MLflow par passe (lignes en vigueur, puis chaque millésime historique)
    runs = script_runs(
        node_of=lambda label: NODE,
        run_name_of=lambda label: f"vulnerabilities-{label}-{timestamp()}",
        experiment=experiment_name("vulnerabilities"),
    )
    outcome = run_partner_vulnerabilities(
        source, result, partner_registry(block, partner_classification(nomenclatures)),
        DownloadRegistryView(downloads["PATHS"]["LAST_DOWNLOAD_PATH"], downloads["BUCKET"]),
        params=vulnerability_config, runtime=runtime_config, dataflow=dataflow, runs=runs,
        concordances_loader=partial(load_partner_concordances, baci_config=load_baci_config()),
        force=ForceSpec.from_runtime(runtime_config),
        adopt_legacy_fingerprints=adopt_legacy_flag(block.get("STATE")), run_id=workflow_run_id(),
    )
    outcome.raise_if_failed()


# Exécution du script principal
if __name__ == "__main__":
    main()
