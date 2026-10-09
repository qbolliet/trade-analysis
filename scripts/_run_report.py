"""Glue between the pipeline scripts and the MLflow run report.

Transitional module: it holds what only scripts do (read the ``tracking`` parameters and
the environment, measure the run, wrap the body of ``main()``). The pure logic lives in
``macroforecast.tracking`` and the publication in ``kedro_pipeline.io.tracking``, both
reused as is by the Kedro nodes, which will make this module obsolete with the scripts.

Typical use in a script::

    tracker = CapturingTracker(get_tracker(..., run_name=run_name(default, node)))
    scope = RunScope(node)
    with tracker, guarded_run(scope, tracker):
        result = run_step(..., tracker=tracker, progress=scope)  # a StepResult
        scope.publish_result(tracker, result)
    result.raise_if_failed()  # exits on error come AFTER the ``with``

Steps opening one run per unit (pass, vintage) receive :func:`script_runs` instead.
"""
# Importation des modules
# Modules de base
from contextlib import contextmanager
from datetime import datetime
import logging
import os
import time
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Union
# Modules de manipulation des données
import pandas as pd
# Modules du package
from kedro_pipeline.config import load_parameters, read_config_file
from kedro_pipeline.io.tracking import (  # noqa: F401  (flow_run_metrics ré-exportée)
    build_step_report,
    flow_run_metrics,
    peak_memory_mb,
    publish_failure,
    publish_run_report,
    run_metrics,
)
from kedro_pipeline.steps.result import StepResult, UnitRun, UnitRuns
from macroforecast.tracking import CapturingTracker, RunTracker, get_tracker
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

# Libellé affiché pour les liens de la configuration
_LINK_LABELS = {"ARGO_WORKFLOW": "DAG Argo"}


# Chargement de la configuration du rapport de run
def load_tracking_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load the ``tracking`` block of the run-report configuration.

    Args:
        config_path: Explicit YAML file with a ``tracking`` root key. Defaults to
            the ``tracking`` block of the Kedro parameters of the ``KEDRO_ENV``
            environment.

    Returns:
        The ``tracking`` block; empty (with a WARNING) when it cannot be read,
        so a missing report configuration never stops a computation.
    """
    try:
        if config_path is None:
            return dict(load_parameters().get("tracking") or {})
        return dict(read_config_file(config_path, "tracking"))
    except Exception as exc:
        # Configuration illisible (fichier absent, environnement inconnu) : rapport par défaut
        logger.warning(
            "Run report configuration %s unreadable (%s): default report.",
            config_path or "tracking", exc,
        )
        return {}


# Nom du run MLflow
def run_name(default: str, node: str) -> str:
    """Return the name of the MLflow run.

    Args:
        default: Name used outside a workflow (timestamped, as before).
        node: Node (script) name.

    Returns:
        ``<node>-<WORKFLOW_ID>`` when the ``WORKFLOW_ID`` variable exists
        (runs of one workflow share it), ``default`` otherwise.

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
        params: The ``tracking`` block; read from the Kedro parameters when ``None``.
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
            # Profil du workflow de transition, à défaut l'environnement Kedro
            "env": os.environ.get("PROFILE") or os.environ.get("KEDRO_ENV"),
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

    # Garde d'échec du run (description réduite si une exception traverse le bloc)
    def guard(self, tracker: RunTracker) -> Any:
        """Return the failure guard of the run (see :func:`guarded_run`).

        Args:
            tracker: Tracker of the open run.

        Returns:
            The context manager.
        """
        return guarded_run(self, tracker)

    # Publication du rapport construit depuis le résultat d'une étape
    def publish_result(self, tracker: RunTracker, result: StepResult) -> None:
        """Build the report of the run from a step result, then publish it, once.

        Nothing is published when the result is not reportable (nothing was
        stale).

        Args:
            tracker: Tracker of the open run.
            result: Result returned by the step function.
        """
        if not result.reportable:
            return
        self.step = "rapport de run"
        context = self.context()
        report = build_step_report(result, node=self.node, params=self.params, context=context)
        self.publish(tracker, report)


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


# Fabrique des runs d'unités des scripts : un run MLflow par passe ou millésime
def script_runs(
    *,
    node_of: Callable[[str], str],
    run_name_of: Callable[[str], str],
    experiment: str,
    params: Optional[Mapping[str, Any]] = None,
) -> UnitRuns:
    """Return the factory opening one MLflow run, with its report, per unit.

    Each unit gets a capturing tracker over ``get_tracker`` (a null tracker
    without ``MLFLOW_TRACKING_URI``), a :class:`RunScope` and the failure guard:
    an exception crossing the block publishes the reduced description and ends
    the run failed.

    Args:
        node_of: Node name of a unit label (``"process_baci_HS2017"``…).
        run_name_of: Default run name (outside a workflow) of a unit label.
        experiment: MLflow experiment of the runs.
        params: The ``tracking`` block; read from the Kedro parameters when ``None``.

    Returns:
        The factory.
    """
    # Configuration lue une fois pour toutes les unités
    tracking = dict(params) if params is not None else load_tracking_config()

    @contextmanager
    def runs(label: str, tags: Mapping[str, str]) -> Iterator[UnitRun]:
        node = node_of(label)
        tracker = CapturingTracker(
            get_tracker(
                tracking_uri=None,
                experiment=experiment,
                run_name=run_name(run_name_of(label), node),
                tags=dict(tags),
            )
        )
        scope = RunScope(node, tracking)
        with tracker, guarded_run(scope, tracker):
            yield UnitRun(
                tracker=tracker,
                progress=scope,
                publish=lambda result: scope.publish_result(tracker, result),
            )

    return runs


# Horodatage des noms de run par défaut (hors workflow)
def timestamp() -> str:
    """Return the current local time as ``YYYYmmdd-HHMM`` (default run names)."""
    return f"{datetime.now():%Y%m%d-%H%M}"
