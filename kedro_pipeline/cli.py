"""Project commands of the trade pipeline, exposed by Kedro as ``kedro trade …``.

Kedro loads the ``cli`` group of ``<package>.cli`` and adds its commands to the
project commands of the ``kedro`` CLI.

The module also defines the project's ``run`` command, executed by
``python -m kedro_pipeline`` (Kedro's ``find_run_command`` looks for a ``run``
attribute here). ``kedro run`` itself always resolves to the command of
argo-kedro: Kedro gives the global commands of the plugins precedence over the
project commands, and that command ignores ``--params``. The pods of the Argo
workflow therefore call ``python -m kedro_pipeline --pipeline … --nodes …
--env … --params …``.
"""
# Importation des modules
# Modules de base
from pathlib import Path
from typing import Any, Dict, List, Optional

# CLI
import click
from kedro.framework.cli.utils import _split_params, split_node_names


# Commande d'exécution du projet (python -m kedro_pipeline, kedro trade run)
@click.command(name="run")
@click.option("--pipeline", "-p", "pipeline", default="__default__", help="Name of the pipeline to run.")
@click.option("--env", "-e", "env", default=None, help="Kedro environment (KEDRO_ENV, else local, when omitted).")
@click.option(
    "--nodes", "-n", "node_names", type=str, default="", callback=split_node_names,
    help="Comma-separated names of the nodes (or fused tasks) to run.",
)
@click.option(
    "--params", "params", type=click.UNPROCESSED, default="", callback=_split_params,
    help=(
        "Runtime parameters 'key=value' separated by ','; inside a value, separate the "
        "items with ';' (e.g. runtime.FORCE_STEPS=partners;synthesis)."
    ),
)
def run(pipeline: str, env: Optional[str], node_names: List[str], params: Dict[str, Any]) -> None:
    """Run a pipeline (or some of its tasks) with the fused-task runner of argo-kedro.

    Same behaviour as ``kedro run`` of argo-kedro (a fused download task runs
    its inner nodes in one session), plus the runtime parameters of
    ``--params``, given to the session so that the nodes and the hooks read
    them (one-off forcing, tag ``forced`` of the MLflow run).

    Args:
        pipeline: Name of the registered pipeline.
        env: Kedro environment.
        node_names: Names of the nodes to run (all when empty).
        params: Runtime parameters, as a nested mapping.

    Examples:
        $ python -m kedro_pipeline --pipeline __default__ --nodes maintain_ducklake --env test
        $ kedro trade run --nodes compute_synthetic_scores --params "runtime.FORCE_STEPS=synthesis"
    """
    from argo_kedro.runners import FusedRunner
    from kedro.framework.session import KedroSession
    from kedro.framework.startup import bootstrap_project

    from kedro_pipeline.config import conf_source_path, resolve_env

    project_path = conf_source_path().parent
    bootstrap_project(project_path)
    with KedroSession.create(
        project_path=project_path, env=resolve_env(env), runtime_params=params or None
    ) as session:
        # Réglages du runner lus dans argo.yml (hook d'argo-kedro)
        context = session.load_context()
        runner = FusedRunner(
            pipeline_name=pipeline, use_memory_datasets=context.argo.runner.use_memory_datasets
        )
        session.run(pipeline_names=[pipeline], node_names=node_names or None, runner=runner)


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


# Commande projet équivalente à python -m kedro_pipeline
trade.add_command(run)


# Commande de rendu des manifestes Argo
@trade.command(name="render-argo")
@click.option(
    "--env", "-e", "env", default="cloud", show_default=True,
    help="Kedro environment the manifests are rendered for (fixed as KEDRO_ENV in the pods).",
)
@click.option(
    "--image-tag", "image_tag", default=None,
    help="Default image tag of the workflow (deployment.tag of argo.yml when omitted).",
)
@click.option(
    "--output-dir", "output_dir", default=None, type=click.Path(file_okay=False, path_type=Path),
    help="Destination folder (kubernetes/generated at the repository root when omitted).",
)
@click.option(
    "--check", is_flag=True, default=False,
    help="Write nothing; fail with the diff if the files differ from the rendering.",
)
def render_argo(env: str, image_tag: Optional[str], output_dir: Optional[Path], check: bool) -> None:
    """Render the Argo manifests of the scheduled pipelines (WorkflowTemplate + CronWorkflows).

    The DAG of each entry point (``daily``, ``weekly``) is computed from the
    Kedro pipelines of ``env``; the deployment settings come from ``argo.yml``.
    The output is deterministic, so that the versioned manifests can be
    checked against the code (``--check``, run in CI).

    Args:
        env: Kedro environment.
        image_tag: Default image tag.
        output_dir: Destination folder.
        check: Compare instead of writing.

    Raises:
        click.ClickException: With ``--check``, if a manifest differs or is missing.

    Examples:
        $ kedro trade render-argo --image-tag sha-1a2b3c4
        $ kedro trade render-argo --check
        $ kedro trade render-argo --env demo --output-dir /tmp/argo-demo
    """
    from kedro_pipeline.config import conf_source_path
    from kedro_pipeline.deploy.render import DEFAULT_OUTPUT_DIR, diff_manifests, render_manifests, write_manifests

    folder = output_dir if output_dir is not None else conf_source_path().parent / DEFAULT_OUTPUT_DIR
    manifests = render_manifests(env, image_tag=image_tag)
    if check:
        differences = diff_manifests(manifests, folder)
        if differences:
            for diff in differences.values():
                click.echo(diff)
            raise click.ClickException(
                f"{len(differences)} Argo manifest(s) out of date in {folder}: "
                f"run `kedro trade render-argo --env {env}` and commit the result."
            )
        click.echo(f"{len(manifests)} Argo manifests up to date in {folder}.")
        return
    for path in write_manifests(manifests, folder):
        click.echo(f"Written {path}")


# Commande de génération de la page de référence des paramètres
@trade.command(name="docs-config")
@click.option(
    "--source-dir", "source_dir", default=None, type=click.Path(file_okay=False, path_type=Path),
    help="Folder holding the parameters_*.yml files (config/base at the repository root when omitted).",
)
@click.option(
    "--output", "output", default=None, type=click.Path(dir_okay=False, path_type=Path),
    help="Markdown file to write (docs/configuration.md at the repository root when omitted).",
)
@click.option(
    "--max-value-length", "max_value_length", default=160, show_default=True, type=click.IntRange(min=10),
    help="Maximum number of characters displayed for a value (longer values are cut).",
)
def docs_config(source_dir: Optional[Path], output: Optional[Path], max_value_length: int) -> None:
    """Generate the parameter reference page of the documentation site.

    Reads the ``parameters_*.yml`` files of the base configuration and writes
    a Markdown page with one table per root key: dotted key, value and the YAML
    comment written above the key. Needs the ``docs`` extra (``ruamel.yaml``,
    to read the comments).

    Args:
        source_dir: Folder of the parameter files.
        output: Markdown file to write.
        max_value_length: Maximum displayed length of a value.

    Raises:
        click.ClickException: If the folder holds no parameter file or ``ruamel.yaml`` is missing.

    Examples:
        $ kedro trade docs-config
        $ kedro trade docs-config --source-dir config/demo --output /tmp/configuration.md
    """
    from kedro_pipeline.config import conf_source_path
    from kedro_pipeline.docs_config import build_configuration_page

    root = conf_source_path().parent
    folder = source_dir if source_dir is not None else conf_source_path() / "base"
    target = output if output is not None else root / "docs" / "configuration.md"
    try:
        page = build_configuration_page(
            folder, max_value_length=max_value_length, source_label=_relative_label(folder, root)
        )
    except (FileNotFoundError, ImportError) as error:
        raise click.ClickException(str(error)) from error
    target.parent.mkdir(parents=True, exist_ok=True)
    # Fins de ligne LF : sortie identique sous Windows et sous Linux
    target.write_text(page, encoding="utf-8", newline="\n")
    click.echo(f"Written {target}")


# Libellé relatif d'un dossier (affichage dans la page générée)
def _relative_label(folder: Path, root: Path) -> str:
    """Return ``folder`` relative to ``root`` (POSIX separators), else as given.

    Args:
        folder: Folder to display.
        root: Repository root.

    Returns:
        The displayed folder path.

    Examples:
        >>> _relative_label(Path("/repo/config/base"), Path("/repo"))
        'config/base'
    """
    try:
        return folder.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return folder.as_posix()
