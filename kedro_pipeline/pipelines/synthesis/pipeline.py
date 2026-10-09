"""Pipeline ``synthesis``: synthetic scores, then coherence diagnostics (weekly)."""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Mapping, Optional

# Kedro et argo-kedro
from argo_kedro.pipeline import Node
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.pipelines._common import (
    CADENCE_WEEKLY,
    EXPERIMENT_VULNERABILITIES,
    SYNTHESIS_CPU,
    reporting_outputs,
)
from kedro_pipeline.pipelines.synthesis.nodes import (
    compute_synthesis_coherence,
    compute_synthetic_scores,
)

# Tags communs
TAGS = [EXPERIMENT_VULNERABILITIES, CADENCE_WEEKLY]


# Fonction de construction du pipeline
def create_pipeline(parameters: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Pipeline:
    """Create the ``synthesis`` pipeline.

    Args:
        parameters: Unused (same signature as the other pipelines).
        **kwargs: Ignored (Kedro pipeline-creation convention).

    Returns:
        The pipeline.
    """
    return Pipeline(
        [
            Node(
                compute_synthetic_scores,
                inputs={
                    "partners": "vulnerabilities.partners",
                    "network": "vulnerabilities.network",
                    "partners_state": "state.partners",
                    "network_state": "state.network",
                    "state": "state.synthesis",
                    "catalogs": "ducklake.catalogs",
                    "synthesis": "params:synthesis",
                    "vulnerabilities": "params:vulnerabilities",
                    "eurostat": "params:eurostat",
                    "runtime": "params:runtime",
                },
                outputs={"table": "synthesis.scores", **reporting_outputs("compute_synthetic_scores")},
                name="compute_synthetic_scores",
                tags=TAGS,
                machine_type=SYNTHESIS_CPU,
            ),
            Node(
                compute_synthesis_coherence,
                inputs={
                    "scores": "synthesis.scores",
                    "partners": "vulnerabilities.partners",
                    "network": "vulnerabilities.network",
                    "synthesis_state": "state.synthesis",
                    "state": "state.coherence",
                    "catalogs": "ducklake.catalogs",
                    "synthesis": "params:synthesis",
                    "vulnerabilities": "params:vulnerabilities",
                    "runtime": "params:runtime",
                },
                outputs={"table": "synthesis.diagnostics", **reporting_outputs("compute_synthesis_coherence")},
                name="compute_synthesis_coherence",
                tags=TAGS,
                machine_type=SYNTHESIS_CPU,
            ),
        ]
    )
