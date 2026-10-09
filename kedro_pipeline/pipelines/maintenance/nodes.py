"""Node of the end-of-workflow maintenance of the DuckLake catalogs."""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Dict, Mapping

# Modules du package
from kedro_pipeline.pipelines._common import finish_step
from kedro_pipeline.steps.maintenance import run_maintenance


# Nœud : maintenance des catalogues
def maintain_ducklake(maintenance: Mapping[str, Any], tracking: Mapping[str, Any]) -> Dict[str, Any]:
    """Maintain the DuckLake catalogs listed in the parameters.

    The node has no data input: it runs at the end of every workflow, whether
    the other tasks succeeded or not.

    Args:
        maintenance: The ``maintenance`` parameters.
        tracking: The ``tracking`` parameters (closing of the runs left open).

    Returns:
        ``metrics``.

    Examples:
        >>> maintain_ducklake({"CATALOGS": []}, {})["metrics"]["units/planned"]
        0.0
    """
    result = run_maintenance(params=maintenance)
    return finish_step(result, artifacts=False)
