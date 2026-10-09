"""Pipeline ``vulnerabilities``: partner metrics (daily) and network metrics (weekly)."""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Mapping, Optional

# Kedro et argo-kedro
from argo_kedro.pipeline import Node
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.pipelines._common import (
    CADENCE_DAILY,
    CADENCE_WEEKLY,
    COMPUTE_MEDIUM,
    EXPERIMENT_VULNERABILITIES,
    reporting_outputs,
)
from kedro_pipeline.pipelines.baci.pipeline import target_vintages
from kedro_pipeline.pipelines.vulnerabilities.nodes import (
    compute_network_vulnerabilities,
    compute_partner_vulnerabilities,
)


# Fonction de construction du pipeline
def create_pipeline(parameters: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Pipeline:
    """Create the ``vulnerabilities`` pipeline.

    The partner metrics only read Comext (daily, in parallel with the BACI
    block); the network metrics read every BACI vintage (weekly).

    Args:
        parameters: Project parameters (BACI targets); those of ``KEDRO_ENV``
            when ``None``.
        **kwargs: Ignored (Kedro pipeline-creation convention).

    Returns:
        The pipeline.
    """
    from kedro_pipeline.config import load_parameters

    parameters = parameters if parameters is not None else load_parameters()
    vintages = {label: f"baci.{label.lower()}" for label in target_vintages(parameters)}
    return Pipeline(
        [
            Node(
                compute_partner_vulnerabilities,
                inputs={
                    "comext": "eurostat.comext",
                    "concordances": "baci.concordances",
                    "state": "state.partners",
                    "unsd_client": "clients.unsd",
                    "catalogs": "ducklake.catalogs",
                    "eurostat": "params:eurostat",
                    "vulnerabilities": "params:vulnerabilities",
                    "baci": "params:baci",
                    "runtime": "params:runtime",
                },
                outputs={
                    "table": "vulnerabilities.partners",
                    **reporting_outputs("compute_partner_vulnerabilities"),
                },
                name="compute_partner_vulnerabilities",
                tags=[EXPERIMENT_VULNERABILITIES, CADENCE_DAILY],
                machine_type=COMPUTE_MEDIUM,
            ),
            Node(
                compute_network_vulnerabilities,
                inputs={
                    "baci_state": "state.baci",
                    "state": "state.network",
                    "catalogs": "ducklake.catalogs",
                    "comtrade": "params:comtrade",
                    "baci": "params:baci",
                    "vulnerabilities": "params:vulnerabilities",
                    "runtime": "params:runtime",
                    **vintages,
                },
                outputs={
                    "table": "vulnerabilities.network",
                    **reporting_outputs("compute_network_vulnerabilities"),
                },
                name="compute_network_vulnerabilities",
                tags=[EXPERIMENT_VULNERABILITIES, CADENCE_WEEKLY],
                machine_type=COMPUTE_MEDIUM,
            ),
        ]
    )
