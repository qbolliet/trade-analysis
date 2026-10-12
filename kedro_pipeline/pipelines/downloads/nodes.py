"""Nodes of the download task of one source (Eurostat Comext or UN Comtrade).

The three nodes run in the same pod, fused into one task:

1. :func:`fetch_source` plans the queries and downloads them with one API
   client (its codelists and structures cached for the whole task);
2. :func:`publish_source_reference` publishes the reference tables from the
   codelists already fetched (never blocking);
3. :func:`audit_source_coverage` audits the coverage of the source,
   publishes the report of the task's MLflow run (download and audit
   together, under the task name ``download_<source>``), then raises the
   download failure, if any: the references, the audit and the report of a
   failed download are therefore recorded before the task fails.

The three nodes log into the same MLflow run (one run per task); only the
last one publishes the report.

The plan travels between them in memory, which is only allowed inside a fused
task (an in-memory dataset is not shared between pods).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Callable, Dict, Mapping

# Modules du package
from kedro_pipeline.io.datasets import DuckLakeCatalogs
from kedro_pipeline.pipelines._common import finish_step, node_reporting
from kedro_pipeline.steps._config import download_location, reference_location, reference_tables
from kedro_pipeline.steps.downloads import (
    DownloadPlan,
    audit_download,
    download_planned,
    download_report_result,
    download_result,
    plan_download,
    publish_download_reference,
)
from kedro_pipeline.steps.result import StepResult


# Nœud : planification et téléchargement d'une source
def fetch_source(
    client_factory: Callable[[], Any],
    catalogs: DuckLakeCatalogs,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    *,
    source: str,
) -> Dict[str, Any]:
    """Plan and download the queries of a source.

    The node never raises for failed queries: the failure (share of failed
    queries above ``MAX_ERROR_RATIO``) is kept in the plan and raised by the
    last node of the task, once the coverage is reported.

    Args:
        client_factory: Builds the API client (closed here).
        catalogs: Factory of the written handles.
        params: Parameter block of the source.
        runtime: ``runtime`` parameters.
        source: ``"eurostat"`` or ``"comtrade"``.

    Returns:
        ``table`` (handle of the downloaded table), ``plan`` (planned queries,
        codelists, report and result), ``metrics`` and ``artifacts``.
    """
    # Suivi dans le run de la tâche (le rapport est publié par l'audit, dernier nœud)
    reporting = node_reporting(task_of(source), None, name=f"fetch_{source}")
    table = catalogs.table(download_location(params))
    client = client_factory()
    try:
        plan = plan_download(client, source=source, params=params, runtime=runtime)
        download_planned(
            client, plan, table, params=params, tracker=reporting.step_tracker, progress=reporting
        )
    finally:
        client.close()
    plan.result = download_result(plan, params=params, tracker=reporting.step_tracker, progress=reporting)
    return finish_step(plan.result, outputs={"table": table, "plan": plan}, failure=None)


# Fonction du nom de la tâche de téléchargement d'une source
def task_of(source: str) -> str:
    """Return the name of the download task of a source (its MLflow run and checks).

    Args:
        source: ``"eurostat"`` or ``"comtrade"``.

    Returns:
        ``download_<source>``.

    Examples:
        >>> task_of("comtrade")
        'download_comtrade'
    """
    return f"download_{source}"


# Fonction de construction du nœud de téléchargement d'une source
def fetch_node(source: str) -> Callable[..., Dict[str, Any]]:
    """Return the download node function of one source, named ``fetch_<source>``.

    A named function rather than a ``functools.partial``: Kedro reads the
    signature and type hints of the node function.

    Args:
        source: ``"eurostat"`` or ``"comtrade"``.

    Returns:
        The node function (arguments of :func:`fetch_source` without ``source``).

    Examples:
        >>> fetch_node("eurostat").__name__
        'fetch_eurostat'
    """

    def fetch(
        client_factory: Callable[[], Any],
        catalogs: DuckLakeCatalogs,
        params: Mapping[str, Any],
        runtime: Mapping[str, Any],
    ) -> Dict[str, Any]:
        return fetch_source(client_factory, catalogs, params, runtime, source=source)

    fetch.__name__ = fetch.__qualname__ = f"fetch_{source}"
    fetch.__doc__ = f"Plan and download the queries of the '{source}' source (see fetch_source)."
    return fetch


# Nœud : référentiels de la source
def publish_source_reference(
    plan: DownloadPlan,
    table: Any,
    catalogs: DuckLakeCatalogs,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> Dict[str, Any]:
    """Publish the reference tables of a source from its planning codelists.

    Never blocking: a failed table is only reported.

    Args:
        plan: Plan of the source.
        table: Handle of the downloaded table (its catalog holds the references).
        catalogs: Factory of the written handles.
        params: Parameter block of the source.
        runtime: ``runtime`` parameters.

    Returns:
        One handle per reference table (keyed by table name) and ``metrics``.
    """
    result = publish_download_reference(plan, table, params=params, runtime=runtime)
    handles = {name: catalogs.table(reference_location(params, name)) for name in reference_tables(params)}
    return finish_step(result, outputs=handles, artifacts=False, failure=None)


# Nœud : audit de couverture, puis échec éventuel du téléchargement
def audit_source_coverage(
    plan: DownloadPlan,
    table: Any,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    tracking: Mapping[str, Any],
    **references: Any,
) -> Dict[str, Any]:
    """Audit the coverage, publish the report of the task, then raise the download failure.

    Args:
        plan: Downloaded plan (report and result set).
        table: Handle of the downloaded table.
        params: Parameter block of the source.
        runtime: ``runtime`` parameters.
        tracking: ``tracking`` parameters (checks and report of the task).
        **references: Reference handles, received only so that this node runs
            after the reference publication.

    Returns:
        ``metrics`` and ``artifacts`` of the audit.

    Raises:
        DownloadFailureError: If the share of failed queries exceeds
            ``MAX_ERROR_RATIO``, once the audit outputs and the report of the
            task are recorded.
    """
    reporting = node_reporting(task_of(plan.source), tracking, name=f"audit_coverage_{plan.source}")
    coverage = audit_download(plan, table, params=params, runtime=runtime, tracker=reporting.step_tracker)
    reported = download_report_result(plan.result, coverage)
    if coverage is None:
        coverage = StepResult("coverage", reportable=False)
    return finish_step(coverage, failure=plan.result, reporting=reporting, reported=reported)
