"""Maintenance step of the DuckLake catalogs, run at the end of every workflow.

Placeholder of the daily maintenance (compaction of the tables written
recently, expiry of old snapshots, clean-up of orphan files, closing of the
tracking runs left open by a killed task): the step exists so that the
pipeline already carries its end-of-workflow task, with its parameters and its
result; the operations themselves are added later without touching the node.

No environment variable and no YAML path are read here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
from typing import Any, Mapping, Optional, Sequence

# Modules du package
from kedro_pipeline.steps.result import StepResult, capturing
from macroforecast.tracking import RunTracker

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de l'étape (mise en page du rapport de run)
STEP = "maintenance"


# Fonction d'étape : maintenance des catalogues
def run_maintenance(
    tables: Sequence[Any] = (),
    *,
    params: Mapping[str, Any],
    tracker: Optional[RunTracker] = None,
) -> StepResult:
    """Maintain the DuckLake catalogs (no operation yet).

    Args:
        tables: Handles of the tables to maintain (none needed yet: the
            catalogs are listed in ``params``).
        params: The ``maintenance`` parameters (``CATALOGS``, thresholds,
            weekly day).
        tracker: Tracker of the open run (never entered here).

    Returns:
        An empty result: no unit planned, nothing reported.

    Examples:
        >>> result = run_maintenance(params={"CATALOGS": []})
        >>> result.step, result.n_units_planned, result.reportable
        ('maintenance', 0, False)
    """
    capturing(tracker)
    # Logging
    logger.info(
        "Maintenance DuckLake : aucune opération (%d catalogue(s) configuré(s)).",
        len(params.get("CATALOGS") or []),
    )
    return StepResult(step=STEP, reportable=False)
