"""Publication of the run report to the experiment tracker (ARCH PS-31, PD-13).

The report itself is built by pure code (``macroforecast.tracking.report``); this
module only sends it to a :class:`~macroforecast.tracking.RunTracker`: the
description in the *Overview* tab, the tags, the ``checks/*`` metrics and the
artifacts (``report/summary.md``, ``report/report.html``, ``report/checks.csv``,
``failures.csv``, ``tables/*.csv``). It is meant to be reused as is by the Kedro
nodes (K-13).

Every write is guarded: a tracking failure is a WARNING, never an exception, so an
unreachable MLflow server cannot interrupt a computation nor mask its own error.
"""
# Importation des modules
# Modules de base
import logging
import os
import re
from typing import Any, Callable, Dict, Mapping, Optional
# Modules de manipulation des données
import pandas as pd
# Modules du package
from macroforecast.tracking import RunTracker
from macroforecast.tracking.report import (
    RunReport,
    count_results,
    failed_labels,
    failure_markdown,
)

# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé de tag MLflow portant la description d'un run (onglet « Overview »)
DESCRIPTION_TAG = "mlflow.note.content"
# Valeurs par défaut de la section `REPORT` de la configuration (la valeur de production
# vit dans config/tracking.yaml)
_DEFAULT_MAX_DESCRIPTION_CHARS = 7500
_DEFAULT_MAX_TABLE_ROWS = 50


# Fonction auxiliaire : exécution protégée d'une écriture
def _guarded(action: str, call: Callable[[], Any]) -> None:
    """Run one tracking write, warning instead of raising.

    Args:
        action: Short description of the write, used in the warning.
        call: Zero-argument callable performing the write.
    """
    try:
        call()
    except Exception as exc:
        # Logging
        logger.warning("Run report: %s failed (%s: %s)", action, type(exc).__name__, exc)


# Fonction auxiliaire : nom d'artefact sûr
def _artifact_name(name: str) -> str:
    """Turn a table name into a safe artifact file name.

    Args:
        name: Table name, e.g. ``"coverage by year"``.

    Returns:
        Name restricted to alphanumerics and ``_-.``, without directory part.

    Examples:
        >>> _artifact_name("coverage by/year")
        'coverage_by_year'
    """
    return re.sub(r"[^0-9a-zA-Z_.\-]", "_", os.path.basename(name.replace("/", "_")))


# Pic de mémoire du processus
def peak_memory_mb() -> Optional[float]:
    """Return the peak resident memory of the current process.

    Returns:
        Peak resident set size in MB, ``None`` when the platform does not
        provide it.

    Examples:
        >>> value = peak_memory_mb()
        >>> value is None or value > 0
        True
    """
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (ImportError, OSError, ValueError):
        return None
    # ru_maxrss est en kilooctets sous Linux (en octets sous macOS)
    import sys

    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


# Métriques de durée et de ressources d'un run
def run_metrics(context: Mapping[str, Any]) -> Dict[str, float]:
    """Extract the ``run/*`` metrics from the execution context of a report.

    Args:
        context: ``RunReport.context``; ``duration_s`` and ``peak_memory_mb``
            are used when present.

    Returns:
        ``run/duration_seconds`` and ``run/peak_memory_mb`` when known.

    Examples:
        >>> run_metrics({"duration_s": 12.5, "env": "demo"})
        {'run/duration_seconds': 12.5}
    """
    metrics: Dict[str, float] = {}
    if context.get("duration_s") is not None:
        metrics["run/duration_seconds"] = float(context["duration_s"])
    if context.get("peak_memory_mb") is not None:
        metrics["run/peak_memory_mb"] = float(context["peak_memory_mb"])
    return metrics


# Publication du rapport d'un run
def publish_run_report(
    tracker: RunTracker,
    report: RunReport,
    params: Mapping[str, Any],
) -> None:
    """Publish the report of a run to the tracker.

    Must be called inside the tracker's ``with`` block, after the step has
    finished and **before** any exit on error (an aggregated ``RuntimeError``,
    ``sys.exit``…): a failed run carries its report too (PS-08, invariant 3).

    Sent, each write guarded:

    * tag ``mlflow.note.content``: the description (:meth:`RunReport.to_markdown`,
      truncated to ``REPORT.MAX_DESCRIPTION_CHARS``);
    * tags ``health``, ``node``, ``checks_failed`` (labels, truncated),
      ``workflow_id`` (report context, else the ``WORKFLOW_ID`` variable);
    * metrics ``checks/n_passed``, ``checks/n_warnings``, ``checks/n_failed``,
      ``checks/n_skipped`` and the ``run/*`` metrics of the context;
    * artifacts ``report/summary.md`` (untruncated description),
      ``report/report.html`` (if ``REPORT.HTML``), ``report/checks.csv``,
      ``failures.csv`` (if any unit failed) and ``tables/<name>.csv``.

    Args:
        tracker: Tracker of the open run.
        report: Report to publish.
        params: The ``tracking`` configuration block (``REPORT`` keys
            ``HTML``, ``PLOTLY_JS``, ``MAX_DESCRIPTION_CHARS``, ``MAX_TABLE_ROWS``).

    Examples:
        >>> from macroforecast.tracking import CapturingTracker
        >>> from macroforecast.tracking.report import RunReport, Units
        >>> tracker = CapturingTracker()
        >>> publish_run_report(tracker, RunReport(node="n", units=Units(1, 1, 0)), {"REPORT": {"HTML": False}})
        >>> tracker.tags["health"], sorted(tracker.texts)
        ('ok', ['report/summary.md'])
    """
    options = dict(params.get("REPORT") or {}) if params else {}
    max_chars = int(options.get("MAX_DESCRIPTION_CHARS", _DEFAULT_MAX_DESCRIPTION_CHARS))
    max_rows = int(options.get("MAX_TABLE_ROWS", _DEFAULT_MAX_TABLE_ROWS))
    workflow_id = report.context.get("workflow_id") or os.environ.get("WORKFLOW_ID")

    # Étiquettes : verdict et description
    tags: Dict[str, str] = {"health": report.health, "node": report.node}
    if workflow_id:
        tags["workflow_id"] = str(workflow_id)
    labels = failed_labels(report.checks)
    if labels:
        tags["checks_failed"] = labels
    _guarded("set_tags(health)", lambda: tracker.set_tags(tags))
    _guarded(
        "set_tags(description)",
        lambda: tracker.set_tags({DESCRIPTION_TAG: report.to_markdown(max_chars=max_chars)}),
    )

    # Métriques de contrôle et de ressources
    counts = count_results(report.checks)
    metrics = {
        "checks/n_passed": float(counts["passed"]),
        "checks/n_warnings": float(counts["warning"]),
        "checks/n_failed": float(counts["failed"]),
        "checks/n_skipped": float(counts["skipped"]),
        **run_metrics(report.context),
    }
    _guarded("log_metrics(checks)", lambda: tracker.log_metrics(metrics))

    # Artefacts : rapport, contrôles, échecs, tables
    _guarded("log_text(report/summary.md)", lambda: tracker.log_text(report.to_markdown(), "report/summary.md"))
    if options.get("HTML", True):
        _guarded(
            "log_text(report/report.html)",
            lambda: tracker.log_text(
                report.to_html(plotly_js=str(options.get("PLOTLY_JS", "inline")), max_table_rows=max_rows),
                "report/report.html",
            ),
        )
    _guarded("log_table(report/checks.csv)", lambda: tracker.log_table(report.checks_frame(), "report/checks.csv"))
    if report.failures:
        _guarded("log_table(failures.csv)", lambda: tracker.log_table(report.failures_frame(), "failures.csv"))
    for name, table in report.tables.items():
        _guarded(
            f"log_table(tables/{name})",
            lambda name=name, table=table: tracker.log_table(
                pd.DataFrame(table), f"tables/{_artifact_name(name)}.csv"
            ),
        )


# Publication de la description réduite d'un run interrompu
def publish_failure(
    tracker: RunTracker,
    node: str,
    exc: BaseException,
    params: Mapping[str, Any],
    *,
    step: Optional[str] = None,
    context: Optional[Mapping[str, Any]] = None,
) -> None:
    """Publish the reduced description of a run aborted by an uncaught exception.

    Args:
        tracker: Tracker of the open run.
        node: Node (script) name.
        exc: The uncaught exception.
        params: The ``tracking`` configuration block.
        step: Last step reached, when known.
        context: Execution context (``workflow_id``, ``links``…).

    Examples:
        >>> from macroforecast.tracking import CapturingTracker
        >>> tracker = CapturingTracker()
        >>> publish_failure(tracker, "n", ValueError("boom"), {})
        >>> tracker.tags["health"], tracker.tags[DESCRIPTION_TAG].splitlines()[0]
        ('failed', '### ❌ n — échec')
    """
    options = dict(params.get("REPORT") or {}) if params else {}
    max_chars = int(options.get("MAX_DESCRIPTION_CHARS", _DEFAULT_MAX_DESCRIPTION_CHARS))
    context = dict(context or {})
    workflow_id = context.get("workflow_id") or os.environ.get("WORKFLOW_ID")
    tags = {
        "health": "failed",
        "node": node,
        DESCRIPTION_TAG: failure_markdown(node, exc, step=step, context=context, max_chars=max_chars),
    }
    if workflow_id:
        tags["workflow_id"] = str(workflow_id)
    _guarded("set_tags(failure)", lambda: tracker.set_tags(tags))
