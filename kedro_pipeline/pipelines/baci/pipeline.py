"""Pipeline ``baci``: preparation, then one task per HS target vintage (fan-out)."""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, List, Mapping, Optional

# Kedro et argo-kedro
from argo_kedro.pipeline import Node
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.config import active_targets
from kedro_pipeline.pipelines._common import (
    BACI_LARGE,
    CADENCE_WEEKLY,
    COMPUTE_MEDIUM,
    EXPERIMENT_BACI,
    reporting_outputs,
)
from kedro_pipeline.pipelines.baci.nodes import prepare_baci_node, vintage_node
from kedro_pipeline.steps._config import HS_REFERENCE_TABLES

# Tags communs aux tâches BACI
TAGS = [EXPERIMENT_BACI, CADENCE_WEEKLY]


# Fonction des millésimes cibles
def target_vintages(parameters: Mapping[str, Any]) -> List[str]:
    """Return the enabled BACI target vintages, in configuration order.

    Args:
        parameters: Project parameters (``baci.CLASSIFICATIONS.TARGETS``).

    Returns:
        The vintage labels.

    Examples:
        >>> target_vintages({"baci": {"CLASSIFICATIONS": {"TARGETS": {"HS2022": None, "HS2017": {}}}}})
        ['HS2017']
    """
    return list(active_targets(parameters["baci"]["CLASSIFICATIONS"]["TARGETS"]))


# Fonction de construction du pipeline
def create_pipeline(parameters: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Pipeline:
    """Create the ``baci`` pipeline: ``prepare_baci`` then ``process_baci_<vintage>``.

    One processing node per enabled target of ``baci.CLASSIFICATIONS.TARGETS``,
    read in the parameters of the environment named by ``KEDRO_ENV`` (the
    pipelines are built before any session: this variable must name the
    environment of the run for the nodes to match its targets; a node of a
    vintage the run does not target simply has nothing to do).

    Args:
        parameters: Project parameters; those of ``KEDRO_ENV`` when ``None``.
        **kwargs: Ignored (Kedro pipeline-creation convention).

    Returns:
        The pipeline.
    """
    from kedro_pipeline.config import load_parameters

    parameters = parameters if parameters is not None else load_parameters()
    references = {name: f"reference.comtrade.{name}" for name in HS_REFERENCE_TABLES}
    nodes = [
        Node(
            prepare_baci_node,
            inputs={
                "comtrade": "comtrade.tariffline",
                "state": "state.baci",
                "comtrade_client": "clients.comtrade",
                "unsd_client": "clients.unsd",
                "catalogs": "ducklake.catalogs",
                "baci": "params:baci",
                "comtrade_params": "params:comtrade",
                "runtime": "params:runtime",
                "tracking": "params:tracking",
            },
            outputs={
                "scope": "baci.scope",
                "concordances": "baci.concordances",
                **references,
                "metrics": reporting_outputs("prepare_baci")["metrics"],
            },
            name="prepare_baci",
            tags=TAGS,
            machine_type=COMPUTE_MEDIUM,
        )
    ]
    for vintage in target_vintages(parameters):
        name = f"process_baci_{vintage.lower()}"
        nodes.append(
            Node(
                vintage_node(vintage),
                inputs={
                    "scope": "baci.scope",
                    "concordances": "baci.concordances",
                    "comtrade": "comtrade.tariffline",
                    "state": "state.baci",
                    "catalogs": "ducklake.catalogs",
                    "baci": "params:baci",
                    "comtrade_params": "params:comtrade",
                    "runtime": "params:runtime",
                    "tracking": "params:tracking",
                },
                outputs={"table": f"baci.{vintage.lower()}", **reporting_outputs(name)},
                name=name,
                tags=TAGS,
                machine_type=BACI_LARGE,
            )
        )
    return Pipeline(nodes)
