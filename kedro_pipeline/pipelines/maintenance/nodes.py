"""Node of the end-of-workflow maintenance of the DuckLake catalogs."""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import replace
from datetime import timedelta
from typing import Any, Dict, Mapping

# Modules du package
from kedro_pipeline.io.tracking import (
    CONTEXT_KEY,
    DEFAULT_STALE_DESCRIPTION,
    active_run_id,
    close_stale_runs,
)
from kedro_pipeline.pipelines._common import finish_step, node_reporting
from kedro_pipeline.steps.maintenance import run_maintenance

# Métrique du nombre de runs orphelins clos par la maintenance
STALE_RUNS_METRIC = "mlflow/stale_runs_closed"


# Nœud : maintenance des catalogues
def maintain_ducklake(maintenance: Mapping[str, Any], tracking: Mapping[str, Any]) -> Dict[str, Any]:
    """Maintain the DuckLake catalogs listed in the parameters, then close the stale runs.

    The node has no data input: it runs at the end of every workflow, whether
    the other tasks succeeded or not. Every other task of the workflow is over
    by then, so a run of the workflow still ``RUNNING`` belongs to a killed pod
    (out of memory, deadline): it is closed ``FAILED`` with the description
    ``tracking.STALE_RUNS.DESCRIPTION``, in the experiments
    ``tracking.STALE_RUNS.EXPERIMENTS`` (the run of the maintenance itself
    excepted).

    Args:
        maintenance: The ``maintenance`` parameters.
        tracking: The ``tracking`` parameters (``STALE_RUNS``; the workflow
            identifier in the ``CONTEXT`` injected by the project hook).

    Returns:
        ``metrics``.

    Examples:
        >>> maintain_ducklake({"CATALOGS": []}, {})["metrics"]["units/planned"]
        0.0
    """
    reporting = node_reporting("maintain_ducklake", tracking)
    result = run_maintenance(params=maintenance, tracker=reporting.step_tracker)

    # Runs orphelins de l'exécution : tâches tuées avant d'avoir fermé leur run
    stale = dict((tracking or {}).get("STALE_RUNS") or {})
    closed = close_stale_runs(
        ((tracking or {}).get(CONTEXT_KEY) or {}).get("workflow_id"),
        list(stale.get("EXPERIMENTS") or []),
        timedelta(minutes=float(stale.get("MIN_AGE_MINUTES", 0))),
        description=str(stale.get("DESCRIPTION") or DEFAULT_STALE_DESCRIPTION),
        exclude_run_id=active_run_id(),
    )
    metrics = {STALE_RUNS_METRIC: float(closed)}
    reporting.step_tracker.log_metrics(metrics)
    result = replace(result, metrics={**result.metrics, **metrics}, reportable=True)
    return finish_step(result, artifacts=False, reporting=reporting)
