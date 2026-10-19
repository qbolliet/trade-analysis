"""Pipeline ``downloads``: one fused task per source (download, references, coverage audit).

Eurostat and Comtrade run in parallel, in two pods: different APIs, different
quotas, independent sources. Inside a source, nothing is parallelised: the API
is rate-limited and the download registry and fact table have a single writer.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Mapping, Optional

# Kedro et argo-kedro
from argo_kedro.pipeline import FusedPipeline, Node
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.pipelines._common import (
    CADENCE_DAILY,
    EXPERIMENT_DOWNLOADS,
    DOWNLOAD,
    reporting_outputs,
)
from kedro_pipeline.pipelines.downloads.nodes import (
    audit_source_coverage,
    fetch_node,
    publish_source_reference,
)
from kedro_pipeline.steps._config import reference_tables

# Sources téléchargées : nom de la source -> dataset de la table téléchargée
SOURCES = {"eurostat": "eurostat.comext", "comtrade": "comtrade.tariffline"}


# Fonction de construction de la tâche fusionnée d'une source
def source_task(source: str, table: str, params: Mapping[str, Any]) -> FusedPipeline:
    """Build the fused download task of one source.

    Args:
        source: ``"eurostat"`` or ``"comtrade"`` (also the parameter block and
            the catalog alias of its reference tables).
        table: Dataset of the downloaded table.
        params: Parameter block of the source (reference tables published).

    Returns:
        The fused pipeline ``download_<source>``.
    """
    task = f"download_{source}"
    plan = f"{source}.download_plan"
    references = {name: f"reference.{source}.{name}" for name in reference_tables(params)}
    return FusedPipeline(
        [
            Node(
                fetch_node(source),
                inputs={
                    "client_factory": f"clients.{source}",
                    "catalogs": "ducklake.catalogs",
                    "params": f"params:{source}",
                    "runtime": "params:runtime",
                },
                outputs={"table": table, "plan": plan, **reporting_outputs(task)},
                name=f"fetch_{source}",
            ),
            Node(
                publish_source_reference,
                inputs={
                    "plan": plan,
                    "table": table,
                    "catalogs": "ducklake.catalogs",
                    "params": f"params:{source}",
                    "runtime": "params:runtime",
                },
                outputs={**references, "metrics": reporting_outputs(f"publish_reference_{source}")["metrics"]},
                name=f"publish_reference_{source}",
            ),
            Node(
                audit_source_coverage,
                inputs={
                    "plan": plan,
                    "table": table,
                    "params": f"params:{source}",
                    "runtime": "params:runtime",
                    "tracking": "params:tracking",
                    **references,
                },
                outputs=reporting_outputs(f"coverage_{source}"),
                name=f"audit_coverage_{source}",
            ),
        ],
        name=task,
        tags=[EXPERIMENT_DOWNLOADS, CADENCE_DAILY],
        machine_type=DOWNLOAD,
    )


# Fonction de construction du pipeline
def create_pipeline(parameters: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Pipeline:
    """Create the ``downloads`` pipeline: two fused tasks, one per source.

    Args:
        parameters: Project parameters (reference tables of each source);
            those of the environment named by ``KEDRO_ENV`` when ``None``.
        **kwargs: Ignored (Kedro pipeline-creation convention).

    Returns:
        The pipeline, whose nodes are the two fused tasks.
    """
    from kedro_pipeline.config import load_parameters

    parameters = parameters if parameters is not None else load_parameters()
    return Pipeline([source_task(source, table, parameters[source]) for source, table in SOURCES.items()])
