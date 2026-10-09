"""Node of the serving layer: publication of the dashboard tables in one transaction."""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Dict, Mapping

# Modules du package
from kedro_pipeline.io.datasets import DuckLakeCatalogs
from kedro_pipeline.pipelines._common import finish_step, node_reporting
from kedro_pipeline.steps._config import serving_location
from kedro_pipeline.steps.serving import publish_serving, source_tables


# Nœud : publication de la couche de service
def publish_serving_node(
    catalogs: DuckLakeCatalogs,
    serving: Mapping[str, Any],
    eurostat: Mapping[str, Any],
    comtrade: Mapping[str, Any],
    vulnerabilities: Mapping[str, Any],
    synthesis: Mapping[str, Any],
    runtime: Mapping[str, Any],
    tracking: Mapping[str, Any],
    **upstream: Any,
) -> Dict[str, Any]:
    """Publish every serving table into the ``serving`` catalog, in one transaction.

    The source catalogs are attached read-only and the serving queries target
    the schemas the parameters of the environment designate; a failure rolls
    the whole publication back (the dashboard keeps the previous state).

    Args:
        catalogs: Factory of the written handles.
        serving: The ``serving`` parameters.
        eurostat: The ``eurostat`` parameters.
        comtrade: The ``comtrade`` parameters.
        vulnerabilities: The ``vulnerabilities`` parameters.
        synthesis: The ``synthesis`` parameters.
        runtime: ``runtime`` parameters.
        tracking: ``tracking`` parameters (report of the run).
        **upstream: Handles of every source table (results, raw tables,
            reference tables), received for the lineage: in the daily run the
            weekly tables are read as they are.

    Returns:
        ``catalog`` (handle of the serving catalog), ``metrics`` and ``artifacts``.

    Raises:
        Exception: The publication failure, once the metrics are saved.
    """
    reporting = node_reporting("publish_serving", tracking)
    locations, tables = source_tables(
        eurostat=eurostat, comtrade=comtrade, vulnerabilities=vulnerabilities, synthesis=synthesis
    )
    catalog = catalogs.serving(serving_location(serving), locations)
    result = publish_serving(tables, catalog, params=serving, runtime=runtime, tracker=reporting.step_tracker)
    return finish_step(result, outputs={"catalog": catalog}, reporting=reporting)
