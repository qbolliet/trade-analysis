"""Script de calcul/mise à jour des indicateurs de vulnérabilité de réseau.

Enveloppe CLI de la fonction d'étape
``kedro_pipeline.steps.network.run_network_vulnerabilities`` : indicateurs portant
sur le graphe mondial des échanges d'un produit (risque de centralité,
clustering pondéré, diamètre, concentration mondiale, point de défaillance
unique), pour chaque millésime HS du redressement BACI et chaque sens de
``NETWORK_VULNERABILITIES.FLOWS`` (matrice transposée à l'export). Fraîcheur par
millésime, confrontée au registre BACI (lecture seule) ; calcul dans des
processus de travail, écriture par le seul processus parent, un run MLflow par
millésime.

Ce script charge les paramètres, construit les poignées des tables BACI (une par
millésime configuré) et de la table résultat (identifiants lus à la première
connexion), les registres, la fabrique de runs et le nombre de processus
(``N_JOBS``, à défaut ``NUM_CPU``), appelle l'étape puis sort en erreur si un
millésime a échoué.

Configuration lue : blocs ``comtrade``, ``baci``, ``vulnerabilities`` et
``runtime`` des paramètres Kedro, environnement choisi par ``KEDRO_ENV``.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
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
# Parallélisme intra-pod (résolution de n_jobs)
from kedro_pipeline.parallel import resolve_n_jobs
from kedro_pipeline.config import experiment_name, load_parameters, read_config_file
from kedro_pipeline.steps._config import (  # noqa: F401
    baci_target_schemas,
    download_location,
    network_config_from_params,
    schema_name,
    vulnerabilities_location,
)
# Registre BACI (amont, lecture seule) et complétude d'une passe
from kedro_pipeline.steps.baci import baci_registry, pass_is_complete  # noqa: F401
# Étape réseau (ré-exportée : fraîcheur, tâches des workers, annotation)
from kedro_pipeline.steps.network import (  # noqa: F401
    NODE,
    STEP,
    VintageOutcome,
    VintageTask,
    _CONFIG_ROOT,
    _dates_by_schema,
    annotate_network_in_force,
    compute_vintage_task,
    load_last_computation_dates,
    load_last_processing_dates,
    network_registry,
    network_requested,
    network_upstream,
    plan_network_units,
    run_network_vulnerabilities,
    save_last_computation_dates,
    vintages_to_recompute,
)
# Paramètres d'exécution partagés (forçage ponctuel)
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


# Fonction de chargement de la configuration associée à la base comtrade
def load_comtrade_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the UN Comtrade configuration (catalog holding the BACI schemas).

    Args:
        config_path: Explicit YAML file (``comtrade`` root key or historical
            format without it). If None, the ``comtrade`` block of the Kedro
            parameters of the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary.
    """
    if config_path is None:
        return load_parameters()["comtrade"]
    return read_config_file(config_path, "comtrade")

# Fonction de chargement de la configuration du redressement BACI
def load_baci_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the BACI configuration.

    Read for two things only: the HS vintages to score
    (``CLASSIFICATIONS.TARGETS``, i.e. which source schemas exist) and the path
    of the BACI processing registry (``PATHS.LAST_PROCESSING_PATH``). No
    methodological BACI parameter is used here.

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

# Fonction de chargement de la configuration dédiée au calcul des vulnérabilités
def load_vulnerability_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the vulnerability-computation configuration.

    Same block as ``compute_trade_vulnerabilities.py`` — the two families write
    into the same DuckLake catalog — but a sub-block of its own
    (``NETWORK_VULNERABILITIES``), so neither script can be perturbed by the
    other's settings.

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



# Fonction principale de calcul des vulnérabilités de réseau
def main() -> None:
    """CLI entry point for the incremental network-vulnerability computation.

    Raises:
        RuntimeError: If at least one vintage failed, once every stale vintage
            has been attempted.
    """
    comtrade_config, baci_config = load_comtrade_config(), load_baci_config()
    vulnerability_config, runtime_config = load_vulnerability_config(), load_runtime_config()
    network_config = vulnerability_config[_CONFIG_ROOT]
    # Tables BACI des millésimes configurés (même catalogue que Comtrade) et table résultat
    baci = {
        label: DuckLakeTable.lazy(
            download_location(comtrade_config, schema=schema), None, None, connector_factory=_connector
        )
        for label, schema in baci_target_schemas(baci_config).items()
    }
    result = DuckLakeTable.lazy(
        vulnerabilities_location(
            vulnerability_config, schema=schema_name(network_config["RESULT_SCHEMA"]),
            bucket=network_config["BUCKET"], data_path=network_config["PATHS"]["DATA_PATH"],
        ),
        None, None, connector_factory=_connector,
    )
    # Un run MLflow par millésime
    runs = script_runs(
        node_of=lambda label: f"{NODE}_{label}",
        run_name_of=lambda label: f"network-vulnerabilities-{label}-{timestamp()}",
        experiment=experiment_name("vulnerabilities"),
    )
    outcome = run_network_vulnerabilities(
        baci, result, network_registry(network_config), baci_registry(baci_config),
        params=vulnerability_config, runtime=runtime_config, dataflow=comtrade_config["DATAFLOW"],
        runs=runs, n_jobs=resolve_n_jobs(network_config.get("N_JOBS")),
        force=ForceSpec.from_runtime(runtime_config),
        adopt_legacy_fingerprints=adopt_legacy_flag(network_config.get("STATE")),
        run_id=workflow_run_id(),
    )
    outcome.raise_if_failed()


# Exécution du script principal
if __name__ == "__main__":
    main()
