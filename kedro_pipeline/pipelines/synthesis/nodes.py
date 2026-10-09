"""Nodes of the multi-criteria synthesis: synthetic scores, then their coherence diagnostics.

Both steps run by context (classification, frequency, flow, indicator,
period) and recompute the stale contexts only. The cadence is carried by the
weekly schedule: the minimum-interval check of the scripts is never requested
here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Dict, Mapping

# Modules du package
from kedro_pipeline.io.datasets import DuckLakeCatalogs
from kedro_pipeline.io.freshness import FreshnessRegistry
from kedro_pipeline.pipelines._common import finish_step, force_spec, n_jobs_of, run_id
from kedro_pipeline.steps._config import schema_name, vulnerabilities_location
from kedro_pipeline.steps.coherence import run_coherence_step
from kedro_pipeline.steps.synthesis import run_synthesis_step


# Fonction de la poignée d'une table de la synthèse
def synthesis_table(
    catalogs: DuckLakeCatalogs,
    vulnerabilities: Mapping[str, Any],
    synthesis: Mapping[str, Any],
    block_name: str,
) -> Any:
    """Return the handle of the scores (``SYNTHESIS``) or diagnostics (``COHERENCE``) table.

    Both tables live in the ``vulnerabilities`` catalog, with the data of the
    synthesis bucket.

    Args:
        catalogs: Factory of the written handles.
        vulnerabilities: The ``vulnerabilities`` parameters (catalog identity).
        synthesis: The ``synthesis`` parameters.
        block_name: ``"SYNTHESIS"`` or ``"COHERENCE"``.

    Returns:
        The lazy handle.
    """
    block = synthesis[block_name]
    return catalogs.table(
        vulnerabilities_location(
            vulnerabilities, schema=schema_name(block["RESULT_SCHEMA"]),
            bucket=synthesis["SYNTHESIS"]["BUCKET"], data_path=block["PATHS"]["DATA_PATH"],
        )
    )


# Nœud : scores synthétiques
def compute_synthetic_scores(
    partners: Any,
    network: Any,
    partners_state: FreshnessRegistry,
    network_state: FreshnessRegistry,
    state: FreshnessRegistry,
    catalogs: DuckLakeCatalogs,
    synthesis: Mapping[str, Any],
    vulnerabilities: Mapping[str, Any],
    eurostat: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> Dict[str, Any]:
    """Compute the synthetic scores of the stale contexts.

    The step also writes the ``fit`` family of the diagnostics table; that
    table is not declared as an output here, since the coherence node outputs
    it (Kedro allows one producer per dataset, and declaring it here would
    also make the coherence depend on itself).

    Args:
        partners: Handle of the partner metrics (upstream).
        network: Handle of the network metrics (upstream).
        partners_state: Partners freshness registry (read only).
        network_state: Network freshness registry (read only).
        state: Synthesis freshness registry.
        catalogs: Factory of the written handles.
        synthesis: The ``synthesis`` parameters (``SYNTHESIS``, ``COHERENCE``).
        vulnerabilities: The ``vulnerabilities`` parameters.
        eurostat: The ``eurostat`` parameters (default recent periods).
        runtime: ``runtime`` parameters (``N_JOBS`` resolved).

    Returns:
        ``table`` (handle of the scores), ``metrics`` and ``artifacts``.

    Raises:
        RuntimeError: If a context failed, once the registry and metrics are saved.
    """
    scores = synthesis_table(catalogs, vulnerabilities, synthesis, "SYNTHESIS")
    diagnostics = synthesis_table(catalogs, vulnerabilities, synthesis, "COHERENCE")
    result = run_synthesis_step(
        scores, diagnostics, state, [partners_state], [network_state],
        params=synthesis, vulnerability_params=vulnerabilities, runtime=runtime,
        n_jobs=n_jobs_of(synthesis["SYNTHESIS"], runtime), eurostat=eurostat,
        force=force_spec(runtime), cadence_check=False, run_id=run_id(),
    )
    return finish_step(result, state, outputs={"table": scores})


# Nœud : diagnostics de cohérence
def compute_synthesis_coherence(
    scores: Any,
    partners: Any,
    network: Any,
    synthesis_state: FreshnessRegistry,
    state: FreshnessRegistry,
    catalogs: DuckLakeCatalogs,
    synthesis: Mapping[str, Any],
    vulnerabilities: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> Dict[str, Any]:
    """Compute the coherence diagnostics of the contexts synthesised since their last diagnosis.

    Args:
        scores: Handle of the scores table (metrics and scores are read there);
            its input makes the coherence run after the synthesis.
        partners: Handle of the partner metrics (upstream lineage).
        network: Handle of the network metrics (upstream lineage).
        synthesis_state: Synthesis freshness registry (read only).
        state: Coherence freshness registry.
        catalogs: Factory of the written handles.
        synthesis: The ``synthesis`` parameters.
        vulnerabilities: The ``vulnerabilities`` parameters.
        runtime: ``runtime`` parameters (``N_JOBS`` resolved).

    Returns:
        ``table`` (handle of the diagnostics), ``metrics`` and ``artifacts``.

    Raises:
        RuntimeError: If a context failed, once the registry and metrics are saved.
    """
    diagnostics = synthesis_table(catalogs, vulnerabilities, synthesis, "COHERENCE")
    result = run_coherence_step(
        scores, diagnostics, state, synthesis_state,
        params=synthesis, vulnerability_params=vulnerabilities, runtime=runtime,
        n_jobs=n_jobs_of(synthesis["COHERENCE"], runtime), force=force_spec(runtime),
        cadence_check=False, run_id=run_id(),
    )
    return finish_step(result, state, outputs={"table": diagnostics})
