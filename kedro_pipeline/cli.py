"""Project commands of the trade pipeline, exposed by Kedro as ``kedro trade …``.

Kedro loads the ``cli`` group of ``<package>.cli`` and adds its commands to the
project commands of the ``kedro`` CLI.
"""
# Importation des modules
# Modules de base
from typing import Optional

# CLI
import click


# Groupe des commandes projet (chargé par Kedro)
@click.group(name="trade-analysis")
def cli() -> None:
    """Project commands of the trade pipeline."""


# Groupe des commandes du pipeline commercial
@cli.group(name="trade")
def trade() -> None:
    """Commands of the trade vulnerability pipeline."""


# Commande de contrôle de la configuration
@trade.command(name="config-check")
@click.option(
    "--env", "-e", "env", default=None,
    help="Kedro environment to check (KEDRO_ENV, else local, when omitted).",
)
def config_check(env: Optional[str]) -> None:
    """Check that the catalog designates the tables and registries of the parameters.

    Loads the project configuration of the environment (no connection, no
    secret needed), instantiates the catalog and compares every DuckLake
    dataset, freshness registry and the serving catalog to the locations the
    steps read in the parameters.

    Args:
        env: Kedro environment.

    Raises:
        click.ClickException: If at least one dataset differs from the parameters.

    Examples:
        $ kedro trade config-check --env demo
    """
    from kedro.framework.session import KedroSession
    from kedro.framework.startup import bootstrap_project

    from kedro_pipeline.config import conf_source_path, resolve_env
    from kedro_pipeline.config_check import catalog_mismatches, expected_datasets

    project_path = conf_source_path().parent
    bootstrap_project(project_path)
    environment = resolve_env(env)
    with KedroSession.create(project_path=project_path, env=environment) as session:
        context = session.load_context()
        parameters = context.config_loader["parameters"]
        mismatches = catalog_mismatches(context.catalog, parameters)
    if mismatches:
        raise click.ClickException(
            f"{len(mismatches)} catalog / parameters mismatch(es) in environment "
            f"'{environment}':\n  " + "\n  ".join(mismatches)
        )
    click.echo(
        f"Environment '{environment}': {len(expected_datasets(parameters))} datasets "
        "consistent with the parameters."
    )
