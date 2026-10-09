"""Pipeline ``maintenance``: the end-of-workflow task (run whatever the workflow outcome)."""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Mapping, Optional

# Kedro et argo-kedro
from argo_kedro.pipeline import Node
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.pipelines._common import (
    EXPERIMENT_MAINTENANCE,
    IO_SMALL,
    MUTEX_MAINTENANCE,
    ON_EXIT,
    reporting_outputs,
)
from kedro_pipeline.pipelines.maintenance.nodes import maintain_ducklake


# Fonction de construction du pipeline
def create_pipeline(parameters: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Pipeline:
    """Create the ``maintenance`` pipeline.

    Its single node carries no cadence tag: it belongs to neither scheduled
    entry point but runs as their exit handler (``onexit`` tag), serialised by
    a mutex since two maintenances must never run on one catalog at once.

    Args:
        parameters: Unused (same signature as the other pipelines).
        **kwargs: Ignored (Kedro pipeline-creation convention).

    Returns:
        The pipeline.
    """
    return Pipeline(
        [
            Node(
                maintain_ducklake,
                inputs={"maintenance": "params:maintenance", "tracking": "params:tracking"},
                outputs={"metrics": reporting_outputs("maintain_ducklake")["metrics"]},
                name="maintain_ducklake",
                tags=[EXPERIMENT_MAINTENANCE, ON_EXIT, MUTEX_MAINTENANCE],
                machine_type=IO_SMALL,
            )
        ]
    )
