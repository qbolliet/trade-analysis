"""Script de redressement BACI multi-millésimes des flux de commerce international.

Enveloppe CLI des fonctions d'étape ``kedro_pipeline.steps.baci.prepare_baci`` et
``run_baci_vintages``. Pour chaque millésime cible ``V`` de la nomenclature HS
(``CLASSIFICATIONS.TARGETS``), les déclarations COMTRADE d'année
``>= START_YEAR[V]`` sont harmonisées vers ``V`` (tables de passage UNSD), la
méthodologie BACI leur est appliquée par passes sur des tranches annuelles
(mémoire bornée, résultat identique au traitement monobloc) et le résultat est
écrit année par année dans un schéma DuckLake dédié au millésime
(``baci_hs2022``, ``baci_hs2017``, …) du catalogue Comtrade. Un schéma par
millésime plutôt qu'une colonne : les millésimes se recouvrent (une même année
figure dans plusieurs cibles), et les mélanger inviterait au double compte.

Porte de complétude : la liste PLANIFIÉE des requêtes Comtrade (même fonction que
le téléchargement) est confrontée au registre de téléchargement ; seules les
années suffisamment téléchargées sont redressées. Fraîcheur : un fragment de
registre par millésime (``STATE.PATH_TEMPLATE``), l'unité étant le millésime
entier ; son ``last_computed`` est ce que lit l'étape réseau.

Ce script charge les paramètres, planifie les requêtes Comtrade (client et clé
d'abonnement de l'environnement), construit la poignée du catalogue, le registre
et un run MLflow par millésime, puis sort en erreur une fois tous les millésimes
tentés si l'un d'eux a échoué. ``--targets HS2017,HS1992`` (ou la variable
``BACI_TARGETS``) restreint les millésimes traités — un pod par millésime en
production.

Configuration lue : blocs ``baci``, ``comtrade`` et ``runtime`` des paramètres
Kedro (``config/<env>/parameters_*.yml``), environnement choisi par ``KEDRO_ENV``
(``local`` par défaut, ``demo`` pour le périmètre de démonstration).
"""
# Importation des modules
# Modules de base
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional, Sequence

# Configuration du projet (paramètres Kedro, expériences MLflow)
from kedro_pipeline.config import experiment_name, load_parameters, read_config_file
# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeTable,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
    workflow_run_id,
)
from kedro_pipeline.io.freshness import ForceSpec, adopt_legacy_flag
from kedro_pipeline.io.registry_views import DownloadRegistryView
from kedro_pipeline.steps._config import (  # noqa: F401
    baci_config_from_params,
    comtrade_schema_from_params,
    download_location,
    resolve_target_start_years,
)
from kedro_pipeline.steps.reference import publish_hs_reference
# Étape BACI (ré-exportée : fraîcheur, porte de complétude, passes)
from kedro_pipeline.steps.baci import (  # noqa: F401
    STEP,
    _PROCESSING_ROOT,
    _parse_legacy_baci,
    _read_comtrade_fact_table,
    _share_min,
    BaciScope,
    baci_is_new_data,
    baci_registry,
    baci_requested,
    baci_unit,
    completed_entry,
    completeness_by_year,
    compute_fit_id,
    coverage_metrics,
    eligible_years,
    is_provisional_scope,
    pass_is_complete,
    plan_baci_vintages,
    prepare_baci,
    prepare_concordances,
    run_baci_vintage,
    run_baci_vintages,
    select_targets,
    started_entry,
    vintage_scopes,
    vintage_watermark,
    with_year_written,
)
# Planification des requêtes Comtrade : même liste que le téléchargement
from scripts.download_comtrade import (
    _SUBSCRIPTION_KEY_ENV,
    fetch_dimension_codelists,
    load_runtime_config,
    plan_queries,
)
from statflows import ComtradeClient, UNSDClient
# Modules de chargement/sauvegarde de données (xls/parquet)
from macroforecast.storage import Loader as TableLoader, Saver as TableSaver
# Module d'implémentation du traitement BACI
from macroforecast.trade.processing import run_baci_passes
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
def _connector(location: Any, pg: Any = None, s3: Any = None, **kwargs: Any) -> Any:
    """Build the connector of a table, reading the credentials only when connecting."""
    return build_connector(location, pg_credentials_from_env(), s3_credentials_from_env(), **kwargs)


# Fonction de chargement de la configuration associée à la base comtrade
def load_comtrade_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the UN Comtrade configuration (source catalog of BACI).

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

# Fonction de chargement de la configuration
def load_baci_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the BACI configuration.

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

# Variable d'environnement restreignant les millésimes traités
TARGETS_ENV = "BACI_TARGETS"

# Fonction de lecture des millésimes demandés (ligne de commande ou environnement)
def requested_targets(
    argv: Optional[Sequence[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[List[str]]:
    """Read the vintages this run is restricted to, if any.

    ``--targets HS2017,HS1992`` takes precedence over the ``BACI_TARGETS``
    environment variable; both hold comma-separated labels. Unknown arguments
    are ignored (the script also runs under test runners).

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` when ``None``).
        environ: Environment mapping (``os.environ`` when ``None``).

    Returns:
        The requested labels, or ``None`` when no restriction is given.

    Examples:
        >>> requested_targets(["--targets", "HS2017, HS1992"], {})
        ['HS2017', 'HS1992']
        >>> requested_targets([], {"BACI_TARGETS": "HS2022"})
        ['HS2022']
        >>> requested_targets([], {}) is None
        True
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--targets", default=None)
    args, _ = parser.parse_known_args(list(sys.argv[1:] if argv is None else argv))
    env = os.environ if environ is None else environ
    raw = args.targets if args.targets is not None else env.get(TARGETS_ENV)
    if raw is None or not str(raw).strip():
        return None
    return [label.strip() for label in str(raw).split(",") if label.strip()]



# Fonction de planification des requêtes Comtrade (porte de complétude)
def plan_comtrade(comtrade_config: Mapping, runtime_config: Mapping):
    """Plan the Comtrade queries exactly as the download step does.

    Args:
        comtrade_config: The ``comtrade`` parameters.
        runtime_config: The ``runtime`` parameters.

    Returns:
        Tuple ``(planned queries, product codelist)``.
    """
    client = ComtradeClient(subscription_key=os.environ.get(_SUBSCRIPTION_KEY_ENV))
    try:
        dims_codes = {
            "reporters": fetch_dimension_codelists("reporter", client=client),
            "products": fetch_dimension_codelists("cmd:HS", client=client),
        }
        return plan_queries(comtrade_config, runtime_config, client, dims_codes), dims_codes["products"]["code"]
    finally:
        client.close()


# Fonction principale de redressement BACI multi-millésimes
def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point for the multi-vintage BACI reconstruction script.

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` when ``None``).

    Raises:
        RuntimeError: If at least one vintage failed, once every vintage has
            been attempted.
        ValueError: If a requested target is not configured.
    """
    comtrade_config, baci_config, runtime_config = load_comtrade_config(), load_baci_config(), load_runtime_config()
    targets = requested_targets(argv)
    # Instant de référence capturé avant tout traitement (jamais après une mise à jour)
    now = datetime.now(timezone.utc)
    planned, products = plan_comtrade(comtrade_config, runtime_config)
    downloads = comtrade_config["DOWNLOADS"][comtrade_config["DATAFLOW"]]
    view = DownloadRegistryView(
        downloads["PATHS"]["LAST_DOWNLOAD_PATH"], bucket=downloads["BUCKET"], dataflow=comtrade_config["DATAFLOW"]
    )
    comtrade = DuckLakeTable.lazy(download_location(comtrade_config), None, None, connector_factory=_connector)
    state = baci_registry(baci_config)
    scope = prepare_baci(
        comtrade, state, view, planned, products, params=baci_config, comtrade_params=comtrade_config,
        runtime=runtime_config, targets=targets, force=ForceSpec.from_runtime(runtime_config),
        adopt_legacy_fingerprints=adopt_legacy_flag(baci_config.get("STATE")), now=now,
        loader=TableLoader(), saver=TableSaver(), concordance_client_factory=UNSDClient,
        concordances_loader=prepare_concordances, reference_publisher=publish_hs_reference,
    )
    if not scope.targets:
        return
    # Un run MLflow par millésime ; une seule connexion pour tous les millésimes
    runs = script_runs(
        node_of=lambda label: f"process_baci_{label}",
        run_name_of=lambda label: f"baci-{label}-{timestamp()}",
        experiment=experiment_name("baci"),
    )
    with comtrade.connect():
        result = run_baci_vintages(
            scope, comtrade, state, params=baci_config, runs=runs,
            run_id=workflow_run_id(), passes_runner=run_baci_passes,
        )
    result.raise_if_failed()


# Exécution du script principal
if __name__ == "__main__":
    main()
