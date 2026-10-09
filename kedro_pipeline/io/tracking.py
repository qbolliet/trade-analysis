"""Run tracking of the pipeline: run report, trackers of the Kedro nodes, stale runs.

The report itself is built by pure code (``macroforecast.tracking.report``); this
module sends it to a :class:`~macroforecast.tracking.RunTracker`: the
description in the *Overview* tab, the tags, the ``checks/*`` metrics and the
artifacts (``report/summary.md``, ``report/report.html``, ``report/checks.csv``,
``failures.csv``, ``tables/*.csv``). It is shared by the transitional scripts
and the Kedro nodes.

For the Kedro nodes, the MLflow run is opened by the kedro-mlflow hook (one run
per ``kedro run``, i.e. per Argo task); this module also provides:

* :func:`build_tracker` — the tracker of that active run (a null tracker when
  tracking is off);
* :func:`node_unit_runs` — the per-unit factory of the steps that compute
  several HS vintages in one node: every unit logs into the node's run, its
  metrics keeping their names and taking the vintage year as MLflow step;
* :func:`build_node_report` — the report of a node from its step result, one
  set of checks per unit when the step computed several units;
* :func:`close_stale_runs` — closing of the runs a killed task left
  ``RUNNING``.

Every write is guarded: a tracking failure is a WARNING, never an exception, so an
unreachable MLflow server cannot interrupt a computation nor mask its own error.
"""
# Importation des modules
# Modules de base
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
import logging
import os
import re
import time
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
# Modules de manipulation des données
import pandas as pd
# Modules du package
from macroforecast.tracking import NULL_TRACKER, ActiveRunTracker, RunTracker
from macroforecast.tracking.report import (
    CheckResult,
    RunReport,
    Section,
    Units,
    build_report,
    checks_for_node,
    count_results,
    evaluate_checks,
    failed_labels,
    failure_markdown,
    health,
)

# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé de tag MLflow portant la description d'un run (onglet « Overview »)
DESCRIPTION_TAG = "mlflow.note.content"
# Valeurs par défaut de la section `REPORT` de la configuration (la valeur de production
# vit dans les paramètres tracking)
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


# ──────────────────────────────────────────────────────────────────────
# Construction du rapport d'un run depuis le résultat d'une étape
# ──────────────────────────────────────────────────────────────────────

# Fonction des métriques d'un calcul de vulnérabilités, préfixées par famille et par sens
def flow_run_metrics(report: Any, family: str) -> Dict[str, float]:
    """MLflow metrics of a vulnerability run, prefixed by family and direction.

    The vulnerability runners diagnose each flow direction on its own
    (``report.flows``); the prefix is applied here, by the caller, so that the
    import and export distributions are never mixed:
    ``partners/import/HHI/mean``, ``network/export/SPOF/mean``.

    Args:
        report: ``VulnerabilityReport`` or ``NetworkVulnerabilityReport`` whose
            ``flows`` holds one sub-report per direction.
        family: Family prefix (``"partners"`` or ``"network"``).

    Returns:
        Slash-separated metric names mapped to their values.

    Examples:
        >>> from macroforecast.trade.vulnerabilities import VulnerabilityReport
        >>> report = VulnerabilityReport(flows={"export": VulnerabilityReport(cells=3)})
        >>> flow_run_metrics(report, "partners")["partners/export/cells/n_total"]
        3.0
    """
    from macroforecast.tracking import rekey_metrics

    metrics: Dict[str, float] = {}
    for flow, sub in report.flows.items():
        metrics.update(rekey_metrics(sub.to_metrics(prefix=f"{family}.{flow}")))
    return metrics


# Fonction de choix des chiffres clés et des sections d'une étape
def report_layout(step: str) -> Tuple[Optional[Callable[..., Any]], Optional[Callable[..., Any]]]:
    """Return the key-figure and section builders of a step's run report.

    Args:
        step: :attr:`StepResult.step` (``"download"``, ``"baci"``,
            ``"partners"``, ``"network"``, ``"synthesis"``, ``"coherence"``,
            ``"serving"``).

    Returns:
        Tuple ``(key_figures, sections)``; ``(None, None)`` for a step
        without dedicated layout.

    Examples:
        >>> report_layout("baci")[0].__name__
        'key_figures_baci'
        >>> report_layout("unknown")
        (None, None)
    """
    from macroforecast.tracking import figures

    layouts = {
        "download": (figures.key_figures_downloads, figures.sections_downloads),
        "baci": (figures.key_figures_baci, figures.sections_baci),
        "partners": (
            figures.key_figures_partner_vulnerabilities,
            figures.sections_partner_vulnerabilities,
        ),
        "network": (
            figures.key_figures_network_vulnerabilities,
            figures.sections_network_vulnerabilities,
        ),
        "synthesis": (figures.key_figures_synthesis, figures.sections_synthesis),
        "coherence": (figures.key_figures_coherence, figures.sections_coherence),
        "serving": (figures.key_figures_serving, figures.sections_serving),
    }
    return layouts.get(step, (None, None))


# Fonction de construction du rapport d'un run à partir du résultat de son étape
def build_step_report(
    result: Any,
    *,
    node: str,
    params: Mapping[str, Any],
    context: Optional[Mapping[str, Any]] = None,
    title: str = "",
) -> RunReport:
    """Evaluate the checks of a node and assemble its report from a step result.

    Single construction of the run report, shared by the scripts and the Kedro
    nodes. The ``run/*`` metrics of the context (duration, memory peak) are
    added to the metrics of the step, so that the checks, the key figures and
    the sections can use them.

    Args:
        result: :class:`~kedro_pipeline.steps.result.StepResult` of the run.
        node: Node name; it selects the configured checks.
        params: The ``tracking`` parameter block (``CHECKS``, ``REPORT``).
        context: Execution context (``workflow_id``, ``env``, ``image``,
            ``duration_s``, ``peak_memory_mb``, ``links``), read by the caller.
        title: Heading; the node name when empty.

    Returns:
        The :class:`~macroforecast.tracking.report.RunReport`.
    """
    from macroforecast.tracking.report import Units, build_report, checks_for_node

    context = dict(context or {})
    metrics = {**result.metrics, **run_metrics(context)}
    key_figures, sections = report_layout(result.step)
    return build_report(
        node,
        metrics=metrics,
        checks=checks_for_node(node, params),
        units=Units(
            planned=int(result.n_units_planned),
            succeeded=int(result.n_units_succeeded),
            failed=int(result.n_failed),
            planned_label=result.units_label,
        ),
        failures=dict(result.failures),
        title=title,
        context=context,
        key_figures=key_figures(metrics) if key_figures is not None else None,
        sections=sections(metrics, result.artifacts) if sections is not None else None,
        tables=dict(result.report_tables),
        max_failures_listed=int((params.get("REPORT") or {}).get("MAX_FAILURES_LISTED", 20)),
    )


# ──────────────────────────────────────────────────────────────────────
# Trackers des nœuds Kedro : run actif de kedro-mlflow
# ──────────────────────────────────────────────────────────────────────

# Fonction de l'identifiant du run actif
def active_run_id() -> Optional[str]:
    """Return the identifier of the active MLflow run.

    Returns:
        The run identifier, ``None`` when no run is active or ``mlflow`` is missing.

    Examples:
        >>> active_run_id() is None
        True
    """
    try:
        import mlflow

        run = mlflow.active_run()
    except Exception:
        return None
    return None if run is None else run.info.run_id


# Fonction du tracker du run actif
def build_tracker(*, log_tables: bool = True) -> RunTracker:
    """Return the tracker of the MLflow run opened for this task, or the null tracker.

    The run is opened by the kedro-mlflow hook before the first node; without
    an active run (tracking disabled, server unreachable, ``mlflow`` missing)
    the null tracker keeps every call inert.

    Args:
        log_tables: Whether the tracker writes the tables it receives. The
            steps of a node receive ``False``: their tables are outputs of the
            node, written by its artifacts dataset.

    Returns:
        An :class:`~macroforecast.tracking.ActiveRunTracker`, or
        :data:`~macroforecast.tracking.NULL_TRACKER`.

    Examples:
        >>> build_tracker() is NULL_TRACKER  # no active run
        True
    """
    if active_run_id() is None:
        return NULL_TRACKER
    return ActiveRunTracker(log_tables=log_tables)


# Type d'expérience lu par l'interface MLflow 3 : sans lui, une expérience s'ouvre en
# mode « GenAI » sur l'onglet Traces, et un lien filtré vers la liste des runs perd son
# filtre ; « custom_model_development » l'ouvre sur la liste des runs (« Model training »)
EXPERIMENT_KIND_TAG = "mlflow.experimentKind"
EXPERIMENT_KIND = "custom_model_development"


# Fonction de typage de l'expérience du run actif
def tag_active_experiment() -> None:
    """Mark the experiment of the active run as a model-training experiment, once.

    The MLflow 3 interface opens an experiment without this tag in its GenAI
    mode (traces), which drops the run filter of a shared link; tagged, it
    opens on the run list. Idempotent and guarded: a failure is a WARNING.

    Examples:
        >>> tag_active_experiment()  # no active run: nothing done
    """
    try:
        import mlflow

        run = mlflow.active_run()
        if run is None:
            return
        client = mlflow.tracking.MlflowClient()
        experiment = client.get_experiment(run.info.experiment_id)
        if experiment.tags.get(EXPERIMENT_KIND_TAG) != EXPERIMENT_KIND:
            client.set_experiment_tag(run.info.experiment_id, EXPERIMENT_KIND_TAG, EXPERIMENT_KIND)
    except Exception as exc:
        # Logging
        logger.warning(f"Type de l'expérience MLflow non posé : {exc}")


# Motif d'un libellé de millésime (« HS2017 ») : lettres puis année sur quatre chiffres
_VINTAGE_LABEL = re.compile(r"^[A-Za-z]+(\d{4})$")


# Fonction du step MLflow d'une unité
def vintage_step(label: str) -> Optional[int]:
    """Return the MLflow step of a unit labelled by an HS vintage: its year.

    Args:
        label: Unit label (``"HS2017"``).

    Returns:
        The year, ``None`` when the label is not a vintage.

    Examples:
        >>> vintage_step("HS2017"), vintage_step("coverage")
        (2017, None)
    """
    match = _VINTAGE_LABEL.match(str(label))
    return int(match.group(1)) if match else None


# Tracker d'une unité dans le run du nœud
class UnitTracker:
    """Tracker of one unit (an HS vintage) inside the run of its node.

    Several units of a node share its run. Their metrics keep their names and
    take the unit's step (the vintage year) when they carry none, so that each
    metric draws one point per vintage instead of being overwritten. Tags,
    parameters and artifact paths are prefixed with the unit label: a tag would
    be overwritten, and MLflow refuses to change a logged parameter.

    Args:
        inner: Tracker of the node's run.
        label: Unit label (``"HS2017"``).
        step: Step of the unit's metrics; ``None`` keeps the step of each call.

    Examples:
        >>> from macroforecast.tracking import CapturingTracker
        >>> inner = CapturingTracker()
        >>> unit = UnitTracker(inner, "HS2017", 2017)
        >>> unit.set_tags({"created": "True"})
        >>> unit.log_metrics({"network/import/cells/n_total": 3.0})
        >>> inner.tags, inner.metrics
        ({'HS2017/created': 'True'}, {'network/import/cells/n_total': 3.0})
    """

    # Initialisation
    def __init__(self, inner: RunTracker, label: str, step: Optional[int]) -> None:
        # Stockage tel quel (convention sklearn)
        self.inner = inner
        self.label = label
        self.step = step

    # Préfixe des clés propres à l'unité
    def _key(self, name: str) -> str:
        return f"{self.label}/{name}"

    def log_params(self, params: Mapping[str, Any]) -> None:
        """Forward the parameters, prefixed with the unit label.

        Args:
            params: Mapping of parameter names to values.
        """
        self.inner.log_params({self._key(k): v for k, v in params.items()})

    def log_metrics(self, metrics: Mapping[str, float], step: Optional[int] = None) -> None:
        """Forward the metrics, at the unit's step when the call gives none.

        Args:
            metrics: Mapping of metric names to values.
            step: Step of the call; the unit's step when ``None``.
        """
        self.inner.log_metrics(metrics, step=self.step if step is None else step)

    def log_dict(self, obj: Mapping[str, Any], artifact_file: str) -> None:
        """Forward a mapping artifact under the unit's folder.

        Args:
            obj: Mapping to serialise.
            artifact_file: Artifact path.
        """
        self.inner.log_dict(obj, self._key(artifact_file))

    def log_table(self, df_table: pd.DataFrame, artifact_file: str) -> None:
        """Forward a table artifact under the unit's folder.

        Args:
            df_table: Table to serialise.
            artifact_file: Artifact path.
        """
        self.inner.log_table(df_table, self._key(artifact_file))

    def log_text(self, text: str, artifact_file: str) -> None:
        """Forward a text artifact under the unit's folder.

        Args:
            text: Text to record.
            artifact_file: Artifact path.
        """
        self.inner.log_text(text, self._key(artifact_file))

    def set_tags(self, tags: Mapping[str, str]) -> None:
        """Forward the tags, prefixed with the unit label.

        Args:
            tags: Mapping of tag names to values.
        """
        self.inner.set_tags({self._key(k): v for k, v in tags.items()})

    def __enter__(self) -> "UnitTracker":
        """Return the tracker (the node's run is already open)."""
        return self

    def __exit__(self, *exc: Any) -> None:
        """Close nothing and never swallow an exception."""


# Fabrique des runs d'unités d'un nœud Kedro : toutes les unités dans le run du nœud
def node_unit_runs(tracker: Optional[RunTracker] = None) -> Any:
    """Return the unit-run factory of a Kedro node (one run per node, not per unit).

    Every unit (a pass of the partner metrics, an HS vintage of the network
    metrics) logs into the node's run through a :class:`UnitTracker` (step =
    vintage year). No report is published per unit: the node publishes one
    report, with one set of checks per unit (:func:`build_node_report`). An
    exception raised inside the block crosses it; the step records the failed
    unit and goes on.

    Args:
        tracker: Tracker of the node's run; the null tracker when ``None``.

    Returns:
        The :class:`~kedro_pipeline.steps.result.UnitRuns` factory.

    Examples:
        >>> from macroforecast.tracking import CapturingTracker
        >>> inner = CapturingTracker()
        >>> with node_unit_runs(inner)("HS2022", {"vintage": "HS2022"}) as run:
        ...     run.tracker.log_metrics({"network/import/cells/n_total": 1.0})
        >>> inner.tags
        {'HS2022/vintage': 'HS2022'}
    """
    from kedro_pipeline.steps.result import UnitRun

    target = NULL_TRACKER if tracker is None else tracker

    @contextmanager
    def runs(label: str, tags: Mapping[str, str]) -> Iterator[Any]:
        unit = UnitTracker(target, label, vintage_step(label))
        unit.set_tags(dict(tags))
        yield UnitRun(tracker=unit)

    return runs


# ──────────────────────────────────────────────────────────────────────
# Rapport de run d'un nœud Kedro
# ──────────────────────────────────────────────────────────────────────

# Clé du contexte d'exécution injecté dans les paramètres `tracking` par le hook projet
CONTEXT_KEY = "CONTEXT"
# Chiffre clé d'un run qui n'avait rien à recalculer
IDLE_FIGURE = "rien à recalculer : toutes les unités sont à jour"
# Libellés affichés pour les liens de la configuration
LINK_LABELS = {"ARGO_WORKFLOW": "DAG Argo"}


# Fonction de rendu des liens de la description d'un run
def render_links(links: Optional[Mapping[str, Optional[str]]], workflow_id: Optional[str]) -> Dict[str, str]:
    """Render the configured links of a run description.

    Args:
        links: ``tracking.LINKS``, name -> URL template (``{workflow_id}`` is
            replaced); a null template is skipped.
        workflow_id: Identifier of the workflow; a template that needs it is
            skipped when it is unknown.

    Returns:
        Display label -> URL.

    Examples:
        >>> render_links({"ARGO_WORKFLOW": "https://argo/wf/{workflow_id}"}, "wf-1")
        {'DAG Argo': 'https://argo/wf/wf-1'}
        >>> render_links({"ARGO_WORKFLOW": "https://argo/wf/{workflow_id}"}, None)
        {}
    """
    return {
        LINK_LABELS.get(name, name): template.format(workflow_id=workflow_id or "")
        for name, template in (links or {}).items()
        if template and (workflow_id or "{workflow_id}" not in template)
    }


# Fonction du contexte d'exécution du rapport d'un nœud
def node_context(tracking: Mapping[str, Any], started_at: float) -> Dict[str, Any]:
    """Return the execution context of a node's report.

    The project hook injects the context of the task (workflow, environment,
    image, links, start of the task) in the ``tracking`` parameters; the node
    adds its duration and the memory peak of the process.

    Args:
        tracking: The ``tracking`` parameters (``CONTEXT`` injected by the hook).
        started_at: Start of the node (``time.time()``), used when the hook gave
            no task start.

    Returns:
        ``workflow_id``, ``env``, ``image``, ``duration_s``, ``peak_memory_mb``
        and ``links``, each present when known.

    Examples:
        >>> context = node_context({"CONTEXT": {"env": "test", "started_at": 0.0}}, 0.0)
        >>> context["env"], context["duration_s"] > 0
        ('test', True)
    """
    injected = dict((tracking or {}).get(CONTEXT_KEY) or {})
    started = injected.get("started_at", started_at)
    context: Dict[str, Any] = {
        "workflow_id": injected.get("workflow_id"),
        "env": injected.get("env"),
        "image": injected.get("image"),
        "duration_s": time.time() - float(started),
        "peak_memory_mb": peak_memory_mb(),
        "links": dict(injected.get("links") or {}),
    }
    return {key: value for key, value in context.items() if value not in (None, {}, "")}


# Fonction d'un résultat d'étape sans rien à recalculer
def _is_idle(result: Any) -> bool:
    """Whether a step result is an idle run: nothing stale, nothing failed."""
    return not result.reportable and not result.children and not result.failures


# Fonction de construction du rapport d'un nœud à partir du résultat de son étape
def build_node_report(
    result: Any,
    *,
    node: str,
    tracking: Mapping[str, Any],
    context: Optional[Mapping[str, Any]] = None,
) -> RunReport:
    """Build the report of a Kedro node (one run per node) from its step result.

    * a result without unit runs is reported as by the scripts
      (:func:`build_step_report`);
    * a result whose units ran in the node's run (``children``: passes, HS
      vintages) is reported once: the configured checks are evaluated on the
      metrics of **each** unit (labelled ``"<check> — <unit>"``), the implicit
      checks on the units of the node, the verdict is the worst result, and
      the key figures, sections and tables of each unit are prefixed with its
      label;
    * an idle result (nothing was stale) still gets a report, with the
      implicit checks only: the configured checks would flag the absence of
      computation as a defect.

    Args:
        result: :class:`~kedro_pipeline.steps.result.StepResult` of the node.
        node: Node (task) name; it selects the configured checks.
        tracking: The ``tracking`` parameters (``CHECKS``, ``REPORT``).
        context: Execution context (:func:`node_context`).

    Returns:
        The :class:`~macroforecast.tracking.report.RunReport`.

    Examples:
        >>> from kedro_pipeline.steps.result import StepResult
        >>> report = build_node_report(StepResult("synthesis", reportable=False), node="n", tracking={})
        >>> report.health, report.key_figures
        ('ok', ['rien à recalculer : toutes les unités sont à jour'])
    """
    context = dict(context or {})
    max_failures = int((tracking.get("REPORT") or {}).get("MAX_FAILURES_LISTED", 20))
    units = Units(
        planned=int(result.n_units_planned),
        succeeded=int(result.n_units_succeeded),
        failed=int(result.n_failed),
        planned_label=result.units_label,
    )

    # Run inactif : contrôles implicites seuls
    if _is_idle(result):
        return build_report(
            node, metrics={**result.metrics, **run_metrics(context)}, checks=[], units=units,
            failures=dict(result.failures), context=context, key_figures=[IDLE_FIGURE],
            max_failures_listed=max_failures,
        )
    children = {label: child for label, child in result.children.items() if child is not None}
    if not children:
        return build_step_report(result, node=node, params=tracking, context=context)

    # Unités dans le run du nœud : contrôles implicites du nœud, puis contrôles par unité
    parent = build_report(
        node, metrics={**result.metrics, **run_metrics(context)}, checks=[], units=units,
        failures=dict(result.failures), context=context, max_failures_listed=max_failures,
    )
    configured = checks_for_node(node, tracking)
    checks: List[CheckResult] = list(parent.checks)
    key_figures: List[str] = []
    sections: List[Section] = []
    tables: Dict[str, pd.DataFrame] = {}
    for label, child in children.items():
        child_report = build_step_report(child, node=node, params=tracking, context=context)
        # Contrôles configurés seuls : les implicites portent sur les unités du nœud
        unit_results = evaluate_checks(
            {**child.metrics, **run_metrics(context)}, configured,
            n_planned=0, n_succeeded=0, failures={},
        )[len(parent.checks):]
        checks.extend(
            CheckResult(replace(item.check, label=f"{item.check.label} — {label}"), item.value, item.status)
            for item in unit_results
        )
        key_figures.extend(f"{label} : {figure}" for figure in child_report.key_figures)
        sections.extend(replace(section, title=f"{label} · {section.title}") for section in child_report.sections)
        tables.update({f"{label}_{name}": table for name, table in child_report.tables.items()})
    return replace(
        parent, health=health(checks), checks=checks, key_figures=key_figures,
        sections=sections, tables={**dict(result.report_tables), **tables},
    )


# ──────────────────────────────────────────────────────────────────────
# Runs orphelins : tâches tuées avant d'avoir fermé leur run
# ──────────────────────────────────────────────────────────────────────

# Description d'un run clos faute d'avoir été fermé par sa tâche
DEFAULT_STALE_DESCRIPTION = "tâche interrompue — voir Argo"


# Fonction de clôture des runs restés ouverts d'une exécution
def close_stale_runs(
    workflow_id: Optional[str],
    experiments: Sequence[str],
    min_age: timedelta = timedelta(0),
    *,
    description: str = DEFAULT_STALE_DESCRIPTION,
    exclude_run_id: Optional[str] = None,
    client: Any = None,
) -> int:
    """Mark ``FAILED`` the runs of a workflow still ``RUNNING`` (task killed).

    A pod killed by an out-of-memory error or a deadline cannot close its run,
    which would stay ``RUNNING`` without a report. Run by the end-of-workflow
    maintenance, once every other task is over, this function searches the runs
    tagged with the workflow in the given experiments, then gives each one the
    description ``❌ <node> — échec`` followed by ``description``, the tag
    ``health=failed`` and the status ``FAILED``. Never raises: a tracking error
    is a WARNING.

    Args:
        workflow_id: Identifier of the workflow (tag ``workflow_id``); nothing
            is done when empty (local run).
        experiments: Names of the experiments to search.
        min_age: Minimum age of a run to be closed (since its start).
        description: Text explaining the closure.
        exclude_run_id: Run never closed (the run of the maintenance itself).
        client: ``MlflowClient`` to use; one on the current tracking URI when
            ``None``.

    Returns:
        The number of runs closed.

    Examples:
        >>> close_stale_runs(None, ["trade-00-maintenance"])
        0
    """
    if not workflow_id or not experiments:
        return 0
    closed = 0
    try:
        if client is None:
            from mlflow.tracking import MlflowClient

            client = MlflowClient()
        ids = [
            experiment.experiment_id
            for experiment in (client.get_experiment_by_name(name) for name in experiments)
            if experiment is not None
        ]
        if not ids:
            return 0
        runs = client.search_runs(
            ids,
            filter_string=f"tags.workflow_id = '{workflow_id}' and attributes.status = 'RUNNING'",
            max_results=1000,
        )
        now_ms = time.time() * 1000.0
        for run in runs:
            run_id = run.info.run_id
            age_ms = now_ms - float(run.info.start_time or 0)
            if run_id == exclude_run_id or age_ms < min_age.total_seconds() * 1000.0:
                continue
            node = run.data.tags.get("node") or run.info.run_name or run_id
            try:
                client.set_tag(run_id, DESCRIPTION_TAG, f"### ❌ {node} — échec\n{description}")
                client.set_tag(run_id, "health", "failed")
                client.set_terminated(run_id, status="FAILED")
                closed += 1
            except Exception as exc:
                # Logging
                logger.warning(f"Run orphelin {run_id} ({node}) non clos : {exc}")
    except Exception as exc:
        # Aucun échec de suivi n'interrompt la maintenance
        logger.warning(f"Recherche des runs orphelins de {workflow_id} impossible : {exc}")
    if closed:
        # Logging
        logger.info(f"{closed} run(s) orphelin(s) de {workflow_id} clos en FAILED.")
    return closed
