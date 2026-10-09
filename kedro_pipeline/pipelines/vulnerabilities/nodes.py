"""Nodes of the vulnerability metrics: partner concentration and trade-network metrics."""
# Importation des modules
from __future__ import annotations
# Modules de base
from functools import partial
from typing import Any, Callable, Dict, Mapping

# Modules du package
from kedro_pipeline.io.datasets import ConcordanceCache, DuckLakeCatalogs
from kedro_pipeline.io.freshness import FreshnessRegistry
from kedro_pipeline.io.registry_views import DownloadRegistryView
from kedro_pipeline.pipelines._common import adopt_legacy, finish_step, force_spec, n_jobs_of, run_id
from kedro_pipeline.steps._config import (
    baci_target_schemas,
    download_location,
    schema_name,
    vulnerabilities_location,
)
from kedro_pipeline.steps.network import run_network_vulnerabilities
from kedro_pipeline.steps.partners import load_partner_concordances, run_partner_vulnerabilities


# Fonction de la poignée d'une table résultat du catalogue des vulnérabilités
def result_table(catalogs: DuckLakeCatalogs, vulnerabilities: Mapping[str, Any], block: Mapping[str, Any]) -> Any:
    """Return the handle of a result table described by its step block.

    Args:
        catalogs: Factory of the written handles.
        vulnerabilities: The ``vulnerabilities`` parameters (catalog identity).
        block: Step block (``RESULT_SCHEMA``, ``BUCKET``, ``PATHS.DATA_PATH``).

    Returns:
        The lazy handle.
    """
    return catalogs.table(
        vulnerabilities_location(
            vulnerabilities, schema=schema_name(block["RESULT_SCHEMA"]),
            bucket=block["BUCKET"], data_path=block["PATHS"]["DATA_PATH"],
        )
    )


# Nœud : métriques de concentration des partenaires
def compute_partner_vulnerabilities(
    comext: Any,
    concordances: ConcordanceCache,
    state: FreshnessRegistry,
    unsd_client: Callable[[], Any],
    catalogs: DuckLakeCatalogs,
    eurostat: Mapping[str, Any],
    vulnerabilities: Mapping[str, Any],
    baci: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> Dict[str, Any]:
    """Compute the partner metrics of the stale units (rows in force, then each historical vintage).

    Args:
        comext: Handle of the Comext fact table.
        concordances: Cache of the correspondence tables (received to run after
            the BACI preparation when both run; read through the parameters).
        state: Partners freshness registry.
        unsd_client: Factory of the UNSD client (missing correspondence tables).
        catalogs: Factory of the written handles.
        eurostat: The ``eurostat`` parameters (dataflow, download registry).
        vulnerabilities: The ``vulnerabilities`` parameters.
        baci: The ``baci`` parameters (correspondence-table cache).
        runtime: ``runtime`` parameters.

    Returns:
        ``table`` (handle of the partner metrics), ``metrics`` and ``artifacts``.

    Raises:
        RuntimeError: If a pass failed, once the registry and metrics are saved.
    """
    dataflow = eurostat["DATAFLOW"]
    block = vulnerabilities["VULNERABILITIES"][dataflow]
    downloads = eurostat["DOWNLOADS"][dataflow]
    result = result_table(catalogs, vulnerabilities, block)
    outcome = run_partner_vulnerabilities(
        comext, result, state,
        DownloadRegistryView(downloads["PATHS"]["LAST_DOWNLOAD_PATH"], downloads["BUCKET"]),
        params=vulnerabilities, runtime=runtime, dataflow=dataflow,
        concordances_loader=partial(load_partner_concordances, baci_config=baci, client_factory=unsd_client),
        force=force_spec(runtime), adopt_legacy_fingerprints=adopt_legacy(block), run_id=run_id(),
    )
    return finish_step(outcome, state, outputs={"table": result})


# Nœud : métriques de réseau des millésimes BACI
def compute_network_vulnerabilities(
    baci_state: FreshnessRegistry,
    state: FreshnessRegistry,
    catalogs: DuckLakeCatalogs,
    comtrade: Mapping[str, Any],
    baci: Mapping[str, Any],
    vulnerabilities: Mapping[str, Any],
    runtime: Mapping[str, Any],
    **vintages: Any,
) -> Dict[str, Any]:
    """Compute the network metrics of the stale BACI vintages.

    Args:
        baci_state: BACI freshness registry (read only: upstream watermark).
        state: Network freshness registry.
        catalogs: Factory of the written handles.
        comtrade: The ``comtrade`` parameters (catalog of the BACI schemas).
        baci: The ``baci`` parameters (target vintages).
        vulnerabilities: The ``vulnerabilities`` parameters.
        runtime: ``runtime`` parameters (``N_JOBS`` resolved).
        **vintages: Handles of the BACI tables, received to run after every
            vintage task; the step reads the targets of the parameters.

    Returns:
        ``table`` (handle of the network metrics), ``metrics`` and ``artifacts``.

    Raises:
        RuntimeError: If a vintage failed, once the registry and metrics are saved.
    """
    network = vulnerabilities["NETWORK_VULNERABILITIES"]
    tables = {
        label: catalogs.table(download_location(comtrade, schema=schema))
        for label, schema in baci_target_schemas(baci).items()
    }
    result = result_table(catalogs, vulnerabilities, network)
    outcome = run_network_vulnerabilities(
        tables, result, state, baci_state,
        params=vulnerabilities, runtime=runtime, dataflow=comtrade["DATAFLOW"],
        n_jobs=n_jobs_of(network, runtime), force=force_spec(runtime),
        adopt_legacy_fingerprints=adopt_legacy(network), run_id=run_id(),
    )
    return finish_step(outcome, state, outputs={"table": result})
