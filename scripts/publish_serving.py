"""Script de publication de la couche de service (catalogue DuckLake ``serving``).

Enveloppe transitoire (phase 0, PD-02) de la fonction d'étape
``kedro_pipeline.steps.serving.publish_serving`` : il charge les configurations,
construit la poignée ``ServingCatalog`` (catalogue ``serving`` en écriture, catalogues
``eurostat``, ``comtrade`` et ``vulnerabilities`` en lecture seule) et le suivi
d'exécution, puis publie toutes les tables de ``serving.TABLES`` en une transaction.
Superset lit directement ce catalogue (PD-21, PS-30.1).

Place dans le pipeline (workflow de transition) : après ``coherence`` et ``partners``
(réussis ou en échec), sérialisé par le mutex ``trade-serving`` (écrivain unique).

Configuration lue : blocs ``serving``, ``eurostat``, ``comtrade``, ``vulnerabilities``,
``synthesis`` et ``runtime`` des paramètres Kedro, environnement choisi par
``KEDRO_ENV`` (``local`` par défaut, ``demo`` pour le périmètre de démonstration).
Identifiants : ``trade-postgres-credentials`` et
``trade-s3-credentials`` (aucun secret propre à la couche de service, PD-18).

Le suivi MLflow est piloté par ``MLFLOW_TRACKING_URI`` (expérience :
``experiments.yml``) : sans URI, ``get_tracker`` renvoie un objet nul. Code de sortie non nul si la publication
échoue (elle a alors été entièrement annulée : Superset garde l'état précédent).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import argparse
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

# Modules internes
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    pg_credentials_from_env,
    s3_credentials_from_env,
)
from kedro_pipeline.config import experiment_name, load_parameters
from kedro_pipeline.io.serving import ServingCatalog
from kedro_pipeline.steps.serving import publish_serving, source_tables
from macroforecast.tracking import CapturingTracker, get_tracker
from macroforecast.tracking.figures import key_figures_serving, sections_serving
from macroforecast.tracking.report import Units
from scripts._run_report import RunScope, guarded_run, run_name

# Logger
logger = logging.getLogger(__name__)

# Blocs de paramètres lus par l'étape de service
_CONFIG_BLOCKS = ("serving", "eurostat", "comtrade", "vulnerabilities", "synthesis", "runtime")


# Fonction de chargement des configurations
def load_configs() -> Dict[str, Dict[str, Any]]:
    """Load every configuration block read by the serving step.

    The blocks come from the Kedro parameters of the ``KEDRO_ENV`` environment
    (``local`` by default, ``demo`` for the demonstration scope), so that the
    serving queries target the very schemas the upstream steps wrote.

    Returns:
        Mapping ``name -> block`` for ``serving``, ``eurostat``, ``comtrade``,
        ``vulnerabilities``, ``synthesis`` and ``runtime``.

    Raises:
        KeyError: If a block is missing from the parameters.
    """
    parameters = load_parameters()
    return {name: parameters[name] for name in _CONFIG_BLOCKS}


# Fonction d'analyse des arguments de ligne de commande
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse the command line (overrides of ``serving.MODE`` / ``serving.YEARS``).

    Args:
        argv: Arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="serving-script",
        description=(
            "Publish the dashboard tables into the DuckLake 'serving' catalog "
            "(one transaction), read directly by Superset."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=["full", "by_year"],
        default=None,
        help="Publication mode (default: serving.MODE of the configuration).",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help="Years republished in by_year mode (default: serving.YEARS).",
    )
    return parser.parse_args(argv)


# Nœud du rapport de run (clé de tracking.CHECKS)
NODE = "publish_serving"


# Fonction principale
def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point of ``serving-script``.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Raises:
        SystemExit: With code 1 when the publication failed (rolled back).
    """
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Chargement des configurations et surcharges de la ligne de commande
    configs = load_configs()
    params = dict(configs["serving"])
    if args.mode is not None:
        params["MODE"] = args.mode
    if args.years is not None:
        params["YEARS"] = args.years

    # Catalogues sources (schémas du profil) et poignée de service (aucune connexion ici)
    locations, tables = source_tables(
        eurostat=configs["eurostat"],
        comtrade=configs["comtrade"],
        vulnerabilities=configs["vulnerabilities"],
        synthesis=configs["synthesis"],
    )
    catalog = ServingCatalog(
        DuckLakeLocation(
            dbname=params["DBNAME"],
            catalog_alias=params["CATALOG_ALIAS"],
            schema=params["SCHEMA"],
            bucket=params["BUCKET"],
            data_path=params["DATA_PATH"],
        ),
        pg=pg_credentials_from_env(),
        s3=s3_credentials_from_env(),
        sources=locations,
    )

    # Suivi d'exécution : objet nul sans URI MLflow
    tracker = CapturingTracker(
        get_tracker(
            tracking_uri=None,
            experiment=experiment_name("serving"),
            run_name=run_name(f"serving-{params['SCHEMA']}-{datetime.now():%Y%m%d-%H%M}", NODE),
        )
    )
    scope = RunScope(NODE)
    with tracker, guarded_run(scope, tracker):
        result = publish_serving(
            tables, catalog, params=params, runtime=configs["runtime"], tracker=tracker
        )
        tracker.set_tags(
            {
                "schema": str(params["SCHEMA"]),
                "mode": result["mode"],
                "missing_sources": ",".join(result["missing_sources"]),
            }
        )

        # Rapport de run, publié avant la sortie en erreur : la publication est atomique
        # (tout ou rien), une panne annule donc toutes les tables
        scope.step = "rapport de run"
        planned = len(params["TABLES"])
        failed = len(result["failures"])
        scope.publish(
            tracker,
            scope.build(
                metrics=tracker.metrics,
                units=Units(
                    planned=planned,
                    succeeded=0 if failed else len(result["tables"]),
                    failed=failed,
                    planned_label=f"{planned} tables (mode {result['mode']})",
                ),
                failures=result["failures"],
                key_figures=key_figures_serving,
                sections=lambda m: sections_serving(m, tracker.tables),
            ),
        )

    # Compte rendu
    for table, rows in result["rows"].items():
        logger.info(f"{params['SCHEMA']}.{table} : {rows} ligne(s).")
    if result["failures"]:
        failures: List[str] = [f"{name}: {message}" for name, message in result["failures"].items()]
        logger.error("Publication annulée :\n" + "\n".join(failures))
        sys.exit(1)
    logger.info(f"Publication réussie ({result['mode']}) de {len(result['tables'])} table(s).")


if __name__ == "__main__":
    main()
