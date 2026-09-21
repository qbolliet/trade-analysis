"""Glue between the pipeline scripts and the run report (ARCH PS-31, K-03d).

Transitional module: it holds what only scripts do (read ``TRACKING_CONFIG_PATH`` and
the environment, measure the run, wrap the body of ``main()``). The pure logic lives in
``macroforecast.tracking`` and the publication in ``kedro_pipeline.io.tracking``, both
reused as is by the Kedro nodes (K-13), which will make this module obsolete (K-18).

Typical use in a script::

    tracker = CapturingTracker(get_tracker(..., run_name=run_name(default, node)))
    scope = RunScope(node)
    with tracker, guarded_run(scope, tracker):
        ...  # the computation, logging through ``tracker``
        report = scope.build(metrics=tracker.metrics, units=Units(...), ...)
        scope.publish(tracker, report)
    # exits on error (aggregated RuntimeError, sys.exit...) come AFTER the ``with``
"""
# Importation des modules
# Modules de base
from contextlib import contextmanager
import logging
import os
import time
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Union
# Modules de manipulation des données
import pandas as pd
import yaml
# Modules du package
from kedro_pipeline.io.download_report import (
    DownloadFailureError,
    check_download_report,
    download_run_metrics,
)
from kedro_pipeline.io.tracking import peak_memory_mb, publish_failure, publish_run_report, run_metrics
from macroforecast.tracking import RunTracker
from macroforecast.tracking.figures import key_figures_downloads, sections_downloads
from macroforecast.tracking.report import (
    Check,
    RunReport,
    Section,
    Units,
    build_report,
    checks_for_node,
)

# Initialisation du logger
logger = logging.getLogger(__name__)

# Chemin par défaut de la configuration du rapport de run
DEFAULT_TRACKING_CONFIG_PATH = "config/tracking.yaml"
# Libellé affiché pour les liens de la configuration
_LINK_LABELS = {"ARGO_WORKFLOW": "DAG Argo"}


# Chargement de la configuration du rapport de run
def load_tracking_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load the ``tracking`` block of the run-report configuration.

    Args:
        config_path: Path of the YAML file. Defaults to the
            ``TRACKING_CONFIG_PATH`` environment variable, then to
            ``config/tracking.yaml``.

    Returns:
        The ``tracking`` block; empty (with a WARNING) when the file is missing,
        so a missing report configuration never stops a computation.
    """
    path = config_path or os.environ.get("TRACKING_CONFIG_PATH", DEFAULT_TRACKING_CONFIG_PATH)
    try:
        with open(path, "r", encoding="utf-8") as file:
            return dict((yaml.safe_load(file) or {}).get("tracking") or {})
    except OSError as exc:
        # Logging
        logger.warning("Run report configuration %s unreadable (%s): default report.", path, exc)
        return {}


# Nom du run MLflow
def run_name(default: str, node: str) -> str:
    """Return the name of the MLflow run.

    Args:
        default: Name used outside a workflow (timestamped, as before).
        node: Node (script) name.

    Returns:
        ``<node>-<WORKFLOW_ID>`` when the ``WORKFLOW_ID`` variable exists
        (ARCH PD-13), ``default`` otherwise.

    Examples:
        >>> os.environ.pop("WORKFLOW_ID", None) and None
        >>> run_name("baci-HS2017-20260921-1030", "process_baci_HS2017")
        'baci-HS2017-20260921-1030'
        >>> os.environ["WORKFLOW_ID"] = "wf-7k2qd"
        >>> run_name("baci-HS2017-20260921-1030", "process_baci_HS2017")
        'process_baci_HS2017-wf-7k2qd'
        >>> del os.environ["WORKFLOW_ID"]
    """
    workflow_id = os.environ.get("WORKFLOW_ID")
    return f"{node}-{workflow_id}" if workflow_id else default


# Périmètre d'un run : configuration, chronomètre, contexte
class RunScope:
    """Configuration, clock and context of one run being reported.

    Args:
        node: Node (script) name, e.g. ``"process_baci_HS2017"``; it selects the
            configured checks (:func:`~macroforecast.tracking.report.checks_for_node`).
        params: The ``tracking`` block; read from ``TRACKING_CONFIG_PATH`` when ``None``.
        step: Last step reached; scripts update it as they progress so a failure
            report can name it.

    Attributes:
        published: Whether the report of this run was already published.
    """

    # Initialisation
    def __init__(
        self,
        node: str,
        params: Optional[Mapping[str, Any]] = None,
        step: Optional[str] = None,
    ) -> None:
        # Stockage tel quel (convention sklearn) puis attributs d'exécution
        self.node = node
        self.params = dict(params) if params is not None else load_tracking_config()
        self.step = step
        self.started = time.monotonic()
        self.published = False

    # Contexte d'exécution
    def context(self) -> Dict[str, Any]:
        """Return the execution context of the run.

        Returns:
            ``workflow_id`` (``WORKFLOW_ID``), ``env`` (``PROFILE``), ``image``
            (``IMAGE_TAG``), ``duration_s``, ``peak_memory_mb`` and ``links``,
            each present when known.
        """
        workflow_id = os.environ.get("WORKFLOW_ID")
        links = {
            _LINK_LABELS.get(name, name): template.format(workflow_id=workflow_id or "")
            for name, template in (self.params.get("LINKS") or {}).items()
            if template and (workflow_id or "{workflow_id}" not in template)
        }
        context: Dict[str, Any] = {
            "workflow_id": workflow_id,
            "env": os.environ.get("PROFILE"),
            "image": os.environ.get("IMAGE_TAG"),
            "duration_s": time.monotonic() - self.started,
            "peak_memory_mb": peak_memory_mb(),
            "links": links,
        }
        return {key: value for key, value in context.items() if value not in (None, {}, "")}

    # Contrôles configurés du nœud
    def checks(self) -> List[Check]:
        """Return the checks configured for the node.

        Returns:
            Checks of :func:`~macroforecast.tracking.report.checks_for_node`.
        """
        return checks_for_node(self.node, self.params)

    # Construction du rapport
    def build(
        self,
        *,
        metrics: Mapping[str, float],
        units: Units,
        failures: Optional[Mapping[str, str]] = None,
        key_figures: Union[Sequence[str], Callable[[Mapping[str, float]], Sequence[str]], None] = None,
        sections: Union[Sequence[Section], Callable[[Mapping[str, float]], Sequence[Section]], None] = None,
        tables: Optional[Mapping[str, pd.DataFrame]] = None,
        title: str = "",
    ) -> RunReport:
        """Evaluate the checks and assemble the report of the run.

        The ``run/*`` metrics (duration, memory peak) are added to ``metrics`` so
        the checks, the key figures and the sections can use them.

        Args:
            metrics: Metrics logged by the run (typically ``CapturingTracker.metrics``).
            units: Units planned, succeeded and failed.
            failures: Failed units, unit -> message.
            key_figures: Key figures, or a function of the completed metrics
                (``key_figures_<step>``) returning them.
            sections: Sections of the HTML report, or a function of the completed
                metrics (``lambda m: sections_<step>(m, artifacts)``) returning them.
            tables: ``tables/`` artifacts.
            title: Heading; the node name when empty.

        Returns:
            The :class:`~macroforecast.tracking.report.RunReport`.
        """
        context = self.context()
        all_metrics = {**metrics, **run_metrics(context)}
        if callable(key_figures):
            key_figures = key_figures(all_metrics)
        if callable(sections):
            sections = sections(all_metrics)
        return build_report(
            self.node,
            metrics=all_metrics,
            checks=self.checks(),
            units=units,
            failures=failures,
            title=title,
            context=context,
            key_figures=key_figures,
            sections=sections,
            tables=tables,
            max_failures_listed=int((self.params.get("REPORT") or {}).get("MAX_FAILURES_LISTED", 20)),
        )

    # Publication du rapport
    def publish(self, tracker: RunTracker, report: RunReport) -> None:
        """Publish the report through the tracker, once.

        Args:
            tracker: Tracker of the open run.
            report: Report to publish.
        """
        publish_run_report(tracker, report, self.params)
        self.published = True


# Garde du corps d'un script
@contextmanager
def guarded_run(scope: RunScope, tracker: RunTracker) -> Iterator[RunScope]:
    """Publish the reduced failure description when the body raises, then re-raise.

    Must be nested inside the tracker's ``with`` block, so the run is still open.
    Only ``Exception`` is caught: a ``KeyboardInterrupt`` (soft time budget) or a
    ``SystemExit`` passes through untouched. When the report was already published,
    the exception is left alone (the full report is more informative).

    Args:
        scope: Scope of the run.
        tracker: Tracker of the open run.

    Yields:
        The scope.
    """
    try:
        yield scope
    except Exception as exc:
        if not scope.published:
            publish_failure(
                tracker, scope.node, exc, scope.params, step=scope.step, context=scope.context()
            )
        raise


# Rapport d'un run de téléchargement (Eurostat, Comtrade)
def build_download_report(
    scope: RunScope,
    tracker: Any,
    report: Any,
    max_error_ratio: Optional[float],
    *,
    log_artifacts: bool = True,
    tags: Optional[Mapping[str, str]] = None,
) -> RunReport:
    """Log the run-level metrics of a download and build its report.

    Units are the queries. Failed queries only make the run *fail* when their share
    exceeds ``max_error_ratio`` (the very condition on which the process exits with an
    error); below it they are tolerated and listed in ``tables/errors.csv`` and in the
    ``download/error_share`` check.

    Args:
        scope: Report scope of the run.
        tracker: Capturing tracker of the open run.
        report: ``statflows`` download report.
        max_error_ratio: Tolerated share of failed queries (``MAX_ERROR_RATIO``).
        log_artifacts: Whether to log the per-query table ``download/queries.csv``.
        tags: Tags attached to the run.

    Returns:
        The report to publish with :meth:`RunScope.publish`.
    """
    metrics = download_run_metrics(report)
    tracker.log_metrics(metrics)
    if tags:
        tracker.set_tags(tags)
    frame = report.to_frame()
    if log_artifacts and report.queries:
        tracker.log_table(frame, "download/queries.csv")

    # Échec du run : seulement au-delà de la part tolérée
    failures: Dict[str, str] = {}
    try:
        check_download_report(report, max_error_ratio)
    except DownloadFailureError as exc:
        failures = {"téléchargement": str(exc)}
    errors = frame[frame["error_type"].notna()] if "error_type" in frame else frame.iloc[0:0]
    return scope.build(
        metrics=metrics,
        units=Units(
            planned=int(report.n_queries_planned),
            succeeded=len(report.queries) - int(report.errors),
            failed=int(report.errors),
            planned_label=f"{int(report.n_queries_planned)} requêtes",
        ),
        failures=failures,
        key_figures=key_figures_downloads,
        sections=lambda m: sections_downloads(m, {"download/queries.csv": frame}),
        tables={"errors": errors} if len(errors) else {},
    )
