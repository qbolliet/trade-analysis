"""Pipeline ``serving``: publication of the DuckLake ``serving`` catalog read by Superset."""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Dict, Mapping, Optional

# Kedro et argo-kedro
from argo_kedro.pipeline import Node
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.pipelines._common import (
    CADENCE_DAILY,
    CADENCE_WEEKLY,
    COMPUTE_MEDIUM,
    EXPERIMENT_SERVING,
    MUTEX_SERVING,
    reporting_outputs,
)
from kedro_pipeline.pipelines.serving.nodes import publish_serving_node
from kedro_pipeline.steps._config import HS_REFERENCE_TABLES, reference_tables

# Tables sources de la publication (hors référentiels) : argument -> dataset
SOURCES = {
    "partners": "vulnerabilities.partners",
    "network": "vulnerabilities.network",
    "scores": "synthesis.scores",
    "diagnostics": "synthesis.diagnostics",
    "comext": "eurostat.comext",
}


# Fonction des référentiels lus par la publication
def reference_inputs(parameters: Mapping[str, Any]) -> Dict[str, str]:
    """Return the reference datasets read by the publication, keyed by argument name.

    Args:
        parameters: Project parameters (reference tables of each source).

    Returns:
        ``{"<source>_<table>": "reference.<source>.<table>"}``.

    Examples:
        >>> params = {s: {"DOWNLOADS": {"REFERENCE": {"DIMENSIONS": {"r": "reporters"}}}}
        ...           for s in ("eurostat", "comtrade")}
        >>> sorted(reference_inputs(params))[:2]
        ['comtrade_hs_concordance', 'comtrade_hs_vintages']
    """
    tables = {
        "eurostat": reference_tables(parameters["eurostat"]),
        "comtrade": reference_tables(parameters["comtrade"]) + list(HS_REFERENCE_TABLES),
    }
    return {
        f"{source}_{table}": f"reference.{source}.{table}"
        for source, names in tables.items()
        for table in names
    }


# Fonction de construction du pipeline
def create_pipeline(parameters: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Pipeline:
    """Create the ``serving`` pipeline (both cadences, serialised by a mutex).

    Args:
        parameters: Project parameters (reference tables); those of
            ``KEDRO_ENV`` when ``None``.
        **kwargs: Ignored (Kedro pipeline-creation convention).

    Returns:
        The pipeline.
    """
    from kedro_pipeline.config import load_parameters

    parameters = parameters if parameters is not None else load_parameters()
    return Pipeline(
        [
            Node(
                publish_serving_node,
                inputs={
                    "catalogs": "ducklake.catalogs",
                    "serving": "params:serving",
                    "eurostat": "params:eurostat",
                    "comtrade": "params:comtrade",
                    "vulnerabilities": "params:vulnerabilities",
                    "synthesis": "params:synthesis",
                    "runtime": "params:runtime",
                    **SOURCES,
                    **reference_inputs(parameters),
                },
                outputs={"catalog": "serving.tables", **reporting_outputs("publish_serving")},
                name="publish_serving",
                tags=[EXPERIMENT_SERVING, CADENCE_DAILY, CADENCE_WEEKLY, MUTEX_SERVING],
                machine_type=COMPUTE_MEDIUM,
            )
        ]
    )
