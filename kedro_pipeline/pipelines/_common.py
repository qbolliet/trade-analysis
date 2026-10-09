"""Shared plumbing of the pipeline nodes: reporting outputs, failure order, run settings.

Every node is a thin wrapper around one step function of
:mod:`kedro_pipeline.steps` (the same functions the transitional scripts
call). The wrappers share the conventions gathered here:

* **reporting outputs** — each node outputs its metrics to
  ``mlflow.metrics.<node>`` and its artifact tables to
  ``mlflow.artifacts.<node>`` (local JSON / CSV files for now, MLflow datasets
  later, under the same names);
* **failure order** — a step never stops at its first failed unit; the node
  saves the freshness registries itself, then raises the step failure. Kedro
  saves the outputs of a node only once its function returned, in an order it
  does not guarantee, and not at all when the function raises: the registries
  are therefore saved inside the node (they are inputs only — Kedro refuses a
  dataset that is both an input and an output of one node), and the
  reporting outputs of a failed node are saved by the project hook
  ``TradeRunHooks.on_node_error`` from the result attached to the exception;
* **run settings** — the number of worker processes comes from the
  ``runtime`` parameters, resolved by the project hook (a step block's own
  ``N_JOBS`` taking precedence); the one-off forcing is read from the
  ``runtime`` parameters only, never from the environment.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from typing import Any, Dict, Iterable, Mapping, Optional

# Modules de manipulation de données
import pandas as pd

# Modules du package
from kedro_pipeline.io.freshness import ForceSpec, adopt_legacy_flag
from kedro_pipeline.io.ducklake import workflow_run_id
from kedro_pipeline.steps.result import StepResult

# Préfixes des datasets de suivi d'un nœud
METRICS_PREFIX = "mlflow.metrics."
ARTIFACTS_PREFIX = "mlflow.artifacts."
# Clés des sorties de suivi dans le dictionnaire renvoyé par un nœud
METRICS_KEY = "metrics"
ARTIFACTS_KEY = "artifacts"
# Extension des artefacts tabulaires (ajoutée par le dataset partitionné)
_CSV_SUFFIX = ".csv"

# Tags des expériences de suivi, des cadences, des sections critiques et du nœud de fin.
# Forme « <famille>.<valeur> » : Kedro n'admet dans un tag que lettres, chiffres, « - »,
# « _ » et « . » (le séparateur « : » est refusé à la création du nœud)
TAG_SEPARATOR = "."
EXPERIMENT_DOWNLOADS = "experiment.trade-01-downloads"
EXPERIMENT_BACI = "experiment.trade-02-baci"
EXPERIMENT_VULNERABILITIES = "experiment.trade-03-vulnerabilities"
EXPERIMENT_SERVING = "experiment.trade-04-serving"
EXPERIMENT_MAINTENANCE = "experiment.trade-00-maintenance"
CADENCE_DAILY = "cadence.daily"
CADENCE_WEEKLY = "cadence.weekly"
MUTEX_SERVING = "mutex.trade-serving"
MUTEX_MAINTENANCE = "mutex.trade-maintenance"
ON_EXIT = "onexit"

# Types de machine des tâches (définis dans la configuration d'ordonnancement)
IO_SMALL = "io-small"
COMPUTE_MEDIUM = "compute-medium"
BACI_LARGE = "baci-large"
SYNTHESIS_CPU = "synthesis-cpu"


# Fonction des noms des datasets de suivi d'un nœud
def reporting_outputs(node: str) -> Dict[str, str]:
    """Return the reporting outputs of a node, keyed as the node returns them.

    Args:
        node: Node name.

    Returns:
        ``{"metrics": "mlflow.metrics.<node>", "artifacts": "mlflow.artifacts.<node>"}``.

    Examples:
        >>> reporting_outputs("publish_serving")["metrics"]
        'mlflow.metrics.publish_serving'
    """
    return {METRICS_KEY: f"{METRICS_PREFIX}{node}", ARTIFACTS_KEY: f"{ARTIFACTS_PREFIX}{node}"}


# Fonction des métriques d'un résultat d'étape
def metrics_of(result: StepResult) -> Dict[str, float]:
    """Flatten the metrics of a step result, with its unit counts.

    The metrics of every per-unit run (``children``: a pass, a vintage) are
    prefixed with the unit label, so that two units never share a name.
    ``units/planned``, ``units/succeeded`` and ``units/failed`` count the units
    of work the execution planned (``units/planned`` is 0 when nothing was
    stale).

    Args:
        result: Step result.

    Returns:
        Metric name -> float value.

    Examples:
        >>> child = StepResult("baci", 1, 1, metrics={"gravity/r2": 0.5})
        >>> metrics_of(StepResult("baci", 1, 1, children={"HS2017": child}))
        {'units/planned': 1.0, 'units/succeeded': 1.0, 'units/failed': 0.0, 'HS2017/gravity/r2': 0.5}
    """
    metrics: Dict[str, float] = {
        "units/planned": float(result.n_units_planned),
        "units/succeeded": float(result.n_units_succeeded),
        "units/failed": float(result.n_failed),
    }
    metrics.update({name: _as_float(value) for name, value in result.metrics.items()})
    for label, child in result.children.items():
        if child is None:
            continue
        metrics.update({f"{label}/{name}": _as_float(value) for name, value in child.metrics.items()})
    return metrics


# Fonction de conversion d'une valeur de métrique
def _as_float(value: Any) -> float:
    """Convert a metric value to ``float`` (``NaN`` when not numeric)."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


# Fonction des artefacts tabulaires d'un résultat d'étape
def artifacts_of(result: StepResult) -> Dict[str, pd.DataFrame]:
    """Collect the artifact tables of a step result, keyed by partition.

    The key is the artifact path without its ``.csv`` extension (the
    partitioned dataset adds it back), prefixed with the unit label for the
    artifacts of per-unit runs.

    Args:
        result: Step result.

    Returns:
        Partition id -> table.

    Examples:
        >>> artifacts_of(StepResult("x", artifacts={"download/queries.csv": pd.DataFrame()}))
        {'download/queries': Empty DataFrame
        Columns: []
        Index: []}
    """
    tables: Dict[str, pd.DataFrame] = {}

    def add(prefix: str, artifacts: Mapping[str, Any]) -> None:
        for path, table in artifacts.items():
            if isinstance(table, pd.DataFrame):
                key = path[: -len(_CSV_SUFFIX)] if path.endswith(_CSV_SUFFIX) else path
                tables[f"{prefix}{key}"] = table

    add("", result.artifacts)
    for label, child in result.children.items():
        if child is not None:
            add(f"{label}/", child.artifacts)
    return tables


# Fonction des valeurs de suivi destinées aux sorties d'un nœud
def reporting_values(outputs: Iterable[str], result: StepResult) -> Dict[str, Any]:
    """Map the reporting outputs of a node to the values of a step result.

    Used by the failure hook: only the reporting datasets the node declares
    are returned.

    Args:
        outputs: Output dataset names of the node.
        result: Step result attached to the node failure.

    Returns:
        Dataset name -> value to save.

    Examples:
        >>> sorted(reporting_values(["mlflow.metrics.n", "x"], StepResult("s")))
        ['mlflow.metrics.n']
    """
    values: Dict[str, Any] = {}
    for name in outputs:
        if name.startswith(METRICS_PREFIX):
            values[name] = metrics_of(result)
        elif name.startswith(ARTIFACTS_PREFIX):
            values[name] = artifacts_of(result)
    return values


# Marqueur : l'échec levé est celui du résultat rapporté
_SAME = object()


# Fonction de fin d'un nœud : registres sauvegardés, puis sorties ou échec
def finish_step(
    result: StepResult,
    *registries: Any,
    outputs: Optional[Mapping[str, Any]] = None,
    artifacts: bool = True,
    failure: Any = _SAME,
) -> Dict[str, Any]:
    """Save the registries, then return the node outputs or raise the step failure.

    Args:
        result: Step result reported by the node.
        *registries: Freshness registries written by the step, saved first
            (``save`` writes the modified fragments only).
        outputs: Other outputs of the node (handles), keyed as declared.
        artifacts: Whether the node declares an artifacts output.
        failure: Result whose failure the node raises: ``result`` itself by
            default, ``None`` for a never-blocking step (reference tables), or
            another result (the download failure raised by the last node of
            the download task, once its coverage audit is reported).

    Returns:
        ``outputs`` plus ``metrics`` (and ``artifacts``).

    Raises:
        BaseException: The failure of ``failure`` (``raise_if_failed()``), with
            ``result`` attached as ``step_result`` for the failure hook.

    Examples:
        >>> sorted(finish_step(StepResult("s"), outputs={"table": 1}))
        ['artifacts', 'metrics', 'table']
    """
    for registry in registries:
        if registry is not None:
            registry.save()
    failing = result if failure is _SAME else failure
    try:
        if failing is not None:
            failing.raise_if_failed()
    except BaseException as exc:
        # Résultat joint à l'exception : le hook d'erreur sauvegarde les métriques
        try:
            exc.step_result = result  # type: ignore[attr-defined]
        except AttributeError:
            pass
        raise
    values = dict(outputs or {})
    values[METRICS_KEY] = metrics_of(result)
    if artifacts:
        values[ARTIFACTS_KEY] = artifacts_of(result)
    return values


# Fonction du forçage ponctuel d'une exécution
def force_spec(runtime: Mapping[str, Any]) -> ForceSpec:
    """Read the one-off forcing from the ``runtime`` parameters only.

    Args:
        runtime: ``runtime`` parameters (``FORCE_STEPS``, ``FORCE_METRICS``,
            ``FORCE_METHODS``, ``FORCE_SCOPE``).

    Returns:
        The forcing (the environment is never read: a pod receives its
        forcing as parameters).
    """
    return ForceSpec.from_runtime(runtime, environ={})


# Fonction de l'adoption des empreintes héritées
def adopt_legacy(block: Optional[Mapping[str, Any]]) -> bool:
    """Read the legacy-fingerprint adoption flag of a step block (``STATE``).

    Args:
        block: Parameter block of the step.

    Returns:
        Whether the legacy entries adopt the current fingerprints.
    """
    return adopt_legacy_flag((block or {}).get("STATE"), environ={})


# Fonction du nombre de processus d'une étape
def n_jobs_of(block: Optional[Mapping[str, Any]], runtime: Mapping[str, Any]) -> int:
    """Return the number of worker processes of a step.

    Args:
        block: Step block (its own ``N_JOBS`` first, when positive).
        runtime: ``runtime`` parameters, ``N_JOBS`` resolved by the project hook.

    Returns:
        The number of processes, at least 1.

    Examples:
        >>> n_jobs_of({"N_JOBS": 2}, {"N_JOBS": 8}), n_jobs_of({}, {"N_JOBS": 8})
        (2, 8)
    """
    from kedro_pipeline.parallel import resolve_n_jobs

    configured = (block or {}).get("N_JOBS")
    if configured is not None and int(configured) > 0:
        return int(configured)
    return resolve_n_jobs(runtime.get("N_JOBS"))


# Fonction de l'identifiant d'exécution enregistré sur les instantanés DuckLake
def run_id() -> Optional[str]:
    """Return the workflow identifier recorded on the DuckLake snapshots.

    Returns:
        The Argo workflow id, ``None`` for a local run.
    """
    return workflow_run_id()
