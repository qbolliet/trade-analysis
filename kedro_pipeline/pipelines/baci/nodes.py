"""Nodes of the BACI reconstruction: one preparation, then one node per HS vintage.

The preparation decides, once for every vintage, which ones are due (download
completeness gate, freshness registry) and prepares their shared inputs
(correspondence tables, CEPII gravity files, HS reference tables). Each
vintage is then re-estimated by its own node — its own pod in production —
which writes its own schema and its own registry fragment: no shared writer,
and an out-of-memory failure only takes one vintage down.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from datetime import datetime, timezone
import logging
from typing import Any, Callable, Dict, Mapping

# Modules de manipulation de données
import pandas as pd

# Modules du package
from kedro_pipeline.config import active_targets
from kedro_pipeline.io.datasets import ConcordanceCache, DuckLakeCatalogs
from kedro_pipeline.io.freshness import FreshnessRegistry
from kedro_pipeline.io.registry_views import DownloadRegistryView
from kedro_pipeline.pipelines._common import adopt_legacy, finish_step, force_spec, node_reporting, run_id
from kedro_pipeline.steps._config import (
    HS_REFERENCE_TABLES,
    download_location,
    reference_location,
    schema_name,
)
from kedro_pipeline.steps.baci import BaciScope, prepare_baci, run_baci_vintage
from kedro_pipeline.steps.downloads import plan_download
from kedro_pipeline.steps.result import StepResult, failure_message

# Initialisation du logger
logger = logging.getLogger(__name__)


# Fonction de l'emplacement du cache des tables de correspondance
def concordance_cache(baci: Mapping[str, Any]) -> ConcordanceCache:
    """Return the location of the correspondence-table cache read in the parameters.

    Args:
        baci: The ``baci`` parameters (``CLASSIFICATIONS.CONCORDANCE_PATH``,
            ``BUCKET``).

    Returns:
        The cache location.
    """
    return ConcordanceCache(str(baci["CLASSIFICATIONS"]["CONCORDANCE_PATH"]), baci.get("BUCKET"))


# Fonction du schéma résultat d'un millésime
def vintage_schema(vintage: str, baci: Mapping[str, Any]) -> str:
    """Return the BACI result schema of a vintage.

    Args:
        vintage: Vintage label (``"HS2017"``).
        baci: The ``baci`` parameters.

    Returns:
        The configured ``RESULT_SCHEMA`` (sanitised) of an enabled target, else
        the default ``baci_<vintage>`` of the catalog (a vintage disabled in
        the running environment keeps a valid handle, never written).
    """
    target = active_targets(baci["CLASSIFICATIONS"]["TARGETS"]).get(vintage)
    if target is None:
        return f"baci_{vintage.lower()}"
    return schema_name(target["RESULT_SCHEMA"])


# Nœud : porte de complétude, plans des millésimes et entrées communes
def prepare_baci_node(
    comtrade: Any,
    state: FreshnessRegistry,
    comtrade_client: Callable[[], Any],
    unsd_client: Callable[[], Any],
    catalogs: DuckLakeCatalogs,
    baci: Mapping[str, Any],
    comtrade_params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    tracking: Mapping[str, Any],
) -> Dict[str, Any]:
    """Decide which vintages are due and prepare their shared inputs.

    The Comtrade queries are planned exactly as the download task plans them
    (same function, same client class), then confronted with the download
    registry: only complete years are re-estimated.

    Args:
        comtrade: Handle of the Comtrade fact table.
        state: BACI freshness registry.
        comtrade_client: Factory of the Comtrade client (planning).
        unsd_client: Factory of the UNSD client (missing correspondence tables).
        catalogs: Factory of the written handles.
        baci: The ``baci`` parameters.
        comtrade_params: The ``comtrade`` parameters.
        runtime: ``runtime`` parameters.
        tracking: ``tracking`` parameters (report of the run).

    Returns:
        ``scope`` (vintages due and their inputs), ``concordances`` (cache
        location), the HS reference handles and ``metrics``.
    """
    # Instant de référence capturé avant tout traitement
    now = datetime.now(timezone.utc)
    reporting = node_reporting("prepare_baci", tracking)
    client = comtrade_client()
    try:
        plan = plan_download(client, source="comtrade", params=comtrade_params, runtime=runtime)
    finally:
        client.close()
    dataflow = comtrade_params["DATAFLOW"]
    downloads = comtrade_params["DOWNLOADS"][dataflow]
    view = DownloadRegistryView(
        downloads["PATHS"]["LAST_DOWNLOAD_PATH"], bucket=downloads["BUCKET"], dataflow=dataflow
    )
    scope = prepare_baci(
        comtrade, state, view, plan.planned, plan.reference_codes["cmd:HS"]["code"],
        params=baci, comtrade_params=comtrade_params, runtime=runtime,
        force=force_spec(runtime), adopt_legacy_fingerprints=adopt_legacy(baci), now=now,
        concordance_client_factory=unsd_client,
    )
    metrics = {
        "completeness/years_eligible": float(len(scope.years_eligible)),
        "completeness/share_min": float(min(scope.shares.values(), default=0.0)),
        "freshness/vintages_due": float(len(scope.targets)),
    }
    reporting.step_tracker.log_metrics(metrics)
    result = StepResult(
        "baci_prepare",
        n_units_planned=len(scope.targets),
        n_units_succeeded=len(scope.targets),
        metrics=metrics,
        units_label=f"{len(scope.targets)} millésime(s) à réestimer",
        report_tables={"scope": scope_frame(scope)},
    )
    references = {
        name: catalogs.table(reference_location(comtrade_params, name)) for name in HS_REFERENCE_TABLES
    }
    return finish_step(
        result, state,
        outputs={"scope": scope, "concordances": concordance_cache(baci), **references},
        artifacts=False,
        reporting=reporting,
    )


# Fonction de la table du périmètre décidé par la préparation
def scope_frame(scope: BaciScope) -> pd.DataFrame:
    """Tabulate the scope of the BACI preparation: years to re-estimate, by vintage.

    Args:
        scope: Scope of the preparation.

    Returns:
        One row per vintage due: ``vintage``, ``n_years``, ``first_year``,
        ``last_year`` and ``reason`` (freshness reason of its plan).
    """
    rows = []
    for vintage in scope.targets:
        years = sorted(scope.scopes.get(vintage) or [])
        rows.append(
            {
                "vintage": vintage,
                "n_years": len(years),
                "first_year": years[0] if years else None,
                "last_year": years[-1] if years else None,
                "reason": getattr(scope.plans.get(vintage), "reason", None),
            }
        )
    return pd.DataFrame(rows, columns=["vintage", "n_years", "first_year", "last_year", "reason"])


# Nœud : redressement d'un millésime
def process_baci_vintage(
    scope: BaciScope,
    concordances: ConcordanceCache,
    comtrade: Any,
    state: FreshnessRegistry,
    catalogs: DuckLakeCatalogs,
    baci: Mapping[str, Any],
    comtrade_params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    tracking: Mapping[str, Any],
    *,
    vintage: str,
) -> Dict[str, Any]:
    """Re-estimate one HS vintage, when the preparation planned it.

    Args:
        scope: Scope of the preparation.
        concordances: Cache of the correspondence tables (received to run after
            the preparation; the tables themselves are in ``scope``).
        comtrade: Handle of the Comtrade fact table.
        state: BACI freshness registry (this vintage's fragment only is written).
        catalogs: Factory of the written handles.
        baci: The ``baci`` parameters.
        comtrade_params: The ``comtrade`` parameters (catalog of the result).
        runtime: ``runtime`` parameters.
        tracking: ``tracking`` parameters (report of the run).
        vintage: Vintage label.

    Returns:
        ``table`` (handle of the vintage's schema), ``metrics`` and ``artifacts``.

    Raises:
        Exception: The failure of the re-estimation, once the registry and the
            metrics are saved.
    """
    reporting = node_reporting(f"process_baci_{vintage.lower()}", tracking)
    if vintage in scope.targets and scope.scopes.get(vintage):
        try:
            result = run_baci_vintage(
                vintage, scope, comtrade, state, params=baci, run_id=run_id(),
                tracker=reporting.step_tracker, progress=reporting,
            )
        except Exception as exc:
            # Échec du millésime : point de reprise déjà écrit par l'étape
            logger.exception("Échec du redressement BACI pour le millésime %s", vintage)
            result = StepResult(
                "baci", 1, 0, failures={vintage: failure_message(exc)}, failure_exception=exc
            )
    else:
        # Millésime à jour, ou sans année complète : rien à redresser
        logger.info("Millésime %s : aucun redressement planifié.", vintage)
        result = StepResult("baci", reportable=False)
    table = catalogs.table(download_location(comtrade_params, schema=vintage_schema(vintage, baci)))
    return finish_step(result, state, outputs={"table": table}, reporting=reporting)


# Fonction de construction du nœud d'un millésime
def vintage_node(vintage: str) -> Callable[..., Dict[str, Any]]:
    """Return the processing node function of one vintage, named ``process_baci_<vintage>``.

    A named function rather than a ``functools.partial``: Kedro reads the
    signature and type hints of the node function.

    Args:
        vintage: Vintage label (``"HS2017"``).

    Returns:
        The node function (arguments of :func:`process_baci_vintage` without
        ``vintage``).

    Examples:
        >>> vintage_node("HS2017").__name__
        'process_baci_hs2017'
    """

    def process(
        scope: BaciScope,
        concordances: ConcordanceCache,
        comtrade: Any,
        state: FreshnessRegistry,
        catalogs: DuckLakeCatalogs,
        baci: Mapping[str, Any],
        comtrade_params: Mapping[str, Any],
        runtime: Mapping[str, Any],
        tracking: Mapping[str, Any],
    ) -> Dict[str, Any]:
        return process_baci_vintage(
            scope, concordances, comtrade, state, catalogs, baci, comtrade_params, runtime, tracking,
            vintage=vintage,
        )

    process.__name__ = process.__qualname__ = f"process_baci_{vintage.lower()}"
    process.__doc__ = f"Re-estimate the BACI vintage {vintage} (see process_baci_vintage)."
    return process
