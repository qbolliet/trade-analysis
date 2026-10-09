"""Project hooks of the trade vulnerability pipeline.

The MLflow run of a task is opened by :class:`~kedro_pipeline.mlflow_hook.GuardedMlflowHook`
(kedro-mlflow behind a guard), registered just after these hooks in the
settings: pluggy calls the hooks registered last first, so the run is open
when these hooks tag it. The hook of argo-kedro registers itself through its
entry point once enabled.
"""
# Importation des modules
# Modules de base
import logging
import os
import time
from types import SimpleNamespace
from typing import Any, Dict, Mapping, Optional

# Kedro
from kedro.framework.hooks import hook_impl

# Initialisation du logger
logger = logging.getLogger(__name__)

# Entrées des paramètres d'exécution partagés et du suivi
RUNTIME_INPUT = "params:runtime"
TRACKING_INPUT = "params:tracking"

# Variables d'environnement décrivant l'exécution (posées dans les pods par le rendu Argo
# et dans l'image) : identifiant du workflow, SHA du commit et étiquette de l'image
WORKFLOW_ID_VARIABLE = "WORKFLOW_ID"
GIT_SHA_VARIABLE = "GIT_SHA"
IMAGE_TAG_VARIABLE = "IMAGE_TAG"
# Nom de run hors workflow (exécution locale)
LOCAL_RUN_SUFFIX = "local"


# Hooks propres au projet
class TradeRunHooks:
    """Project-level Kedro hooks (run tags, run context, parallelism, failures).

    * ``after_context_created``: gives the context the runner settings that
      the ``kedro run`` command of argo-kedro reads (``context.argo``), as long
      as the argo-kedro hook is disabled, and remembers the environment;
    * ``before_pipeline_run`` (after the run is opened): tags the MLflow run of
      the task — ``workflow_id``, ``git_sha``, ``image_tag``, ``kedro_env``,
      ``node``, ``forced`` —, names it ``<task>-<WORKFLOW_ID>`` and marks its
      experiment as a model-training one (opened on the run list by MLflow 3);
    * ``before_node_run``: injects the resolved ``N_JOBS`` in the ``runtime``
      parameters and the context of the task (workflow, environment, image,
      links, start) in the ``tracking`` parameters, so that the nodes never
      read the environment;
    * ``on_node_error``: saves the reporting outputs of a node that failed
      after computing its result and, when the node raised before publishing
      its report, gives the run the reduced failure description
      (``❌ <task> — échec``, ``health=failed``).

    Examples:
        >>> from kedro_pipeline.settings import HOOKS
        >>> any(isinstance(hook, TradeRunHooks) for hook in HOOKS)
        True
    """

    # Initialisation : état de la session (une tâche par session)
    def __init__(self) -> None:
        self._env: Optional[str] = None
        self._task: Optional[str] = None
        self._context: Dict[str, Any] = {}
        self._tracking: Dict[str, Any] = {}

    # Paramètres du runner d'argo-kedro, tant que son hook est désactivé
    @hook_impl
    def after_context_created(self, context: Any) -> None:
        """Remember the environment and set ``context.argo`` when absent.

        The ``kedro run`` command of argo-kedro (it replaces Kedro's own and
        runs the fused tasks) reads ``context.argo.runner.use_memory_datasets``.
        The hook of argo-kedro sets it from ``argo.yml``, but it is disabled
        until that file exists; the defaults are then given here.

        Args:
            context: The Kedro context just created.
        """
        self._env = getattr(context, "env", None)
        if hasattr(context, "argo"):
            return
        try:
            from argo_kedro.config.kedro_argo_config import RunnerConfig
        except ImportError:
            return
        context.__setattr__("argo", SimpleNamespace(runner=RunnerConfig()))

    # Étiquetage du run de la tâche, une fois ouvert par kedro-mlflow
    @hook_impl(trylast=True)
    def before_pipeline_run(self, run_params: Dict[str, Any], pipeline: Any, catalog: Any) -> None:
        """Name and tag the MLflow run of the task, and prepare the report context.

        Declared ``trylast``: it runs after the kedro-mlflow hook has opened the
        run. Without an active run (tracking off), only the context is prepared.

        Args:
            run_params: Parameters of the run (``node_names``, ``pipeline_names``, ``env``).
            pipeline: Pipeline about to run.
            catalog: Catalog of the run (``params:tracking``, ``params:runtime``).
        """
        from kedro_pipeline.io.tracking import build_tracker, tag_active_experiment

        self._task = task_name(run_params)
        self._tracking = _load_parameter(catalog, TRACKING_INPUT)
        runtime = _load_parameter(catalog, RUNTIME_INPUT)
        env = self._env or run_params.get("env")
        self._context = run_context(env, self._tracking, os.environ, started_at=time.time())
        tags = run_tags(self._task, env, runtime, os.environ)
        build_tracker().set_tags(tags)
        tag_active_experiment()

    # Résolution du nombre de processus et contexte du rapport
    @hook_impl
    def before_node_run(self, node: Any, inputs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Inject ``N_JOBS`` in the ``runtime`` parameters and the task context in ``tracking``.

        ``runtime.N_JOBS`` when positive, else the CPUs allocated to the pod
        (``NUM_CPU``), else the CPUs of the machine. A step block with its own
        ``N_JOBS`` still takes precedence in the node.

        Args:
            node: Node about to run.
            inputs: Loaded inputs of the node.

        Returns:
            The updated ``params:runtime`` / ``params:tracking`` inputs, or
            ``None`` when the node reads neither.
        """
        from kedro_pipeline.io.tracking import CONTEXT_KEY

        updates: Dict[str, Any] = {}
        runtime = inputs.get(RUNTIME_INPUT)
        if isinstance(runtime, Mapping):
            updates[RUNTIME_INPUT] = with_resolved_n_jobs(runtime)
        tracking = inputs.get(TRACKING_INPUT)
        if isinstance(tracking, Mapping):
            updates[TRACKING_INPUT] = {**dict(tracking), CONTEXT_KEY: dict(self._context)}
        return updates or None

    # Échec d'un nœud : sorties de suivi et description réduite
    @hook_impl
    def on_node_error(self, error: BaseException, node: Any, catalog: Any) -> None:
        """Save the reporting outputs of a failed node and describe the failure.

        A node raises its step failure only after saving its registries and
        publishing its report; the exception then carries the step result,
        whose metrics and artifacts are saved here into the node's own
        reporting datasets. An exception raised before the report gets the
        reduced description instead. Nothing here ever masks the node's error.

        Args:
            error: Exception raised by the node.
            node: The failed node.
            catalog: The data catalog of the run.
        """
        from kedro_pipeline.io.tracking import build_tracker, publish_failure
        from kedro_pipeline.pipelines._common import progress_of, reporting_values

        result = getattr(error, "step_result", None)
        if result is not None:
            for name, value in reporting_values(node.outputs, result).items():
                try:
                    catalog.save(name, value)
                except Exception as exc:
                    # Le suivi ne masque jamais l'erreur du nœud
                    logger.warning(f"Sortie de suivi '{name}' du nœud en échec non sauvegardée : {exc}")
        if getattr(error, "report_published", False):
            return
        try:
            publish_failure(
                build_tracker(), self._task or node.name, error, self._tracking,
                step=progress_of(node.name), context=self._context,
            )
        except Exception as exc:
            # Logging
            logger.warning(f"Description de l'échec non publiée : {exc}")


# Fonction de lecture protégée d'un paramètre du catalogue
def _load_parameter(catalog: Any, name: str) -> Dict[str, Any]:
    """Load a parameter block from the catalog, empty when absent.

    Args:
        catalog: Catalog of the run.
        name: Dataset name (``params:<block>``).

    Returns:
        The block, ``{}`` when it is not a mapping or cannot be loaded.
    """
    try:
        value = catalog.load(name)
    except Exception:
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


# Fonction du nom de la tâche exécutée
def task_name(run_params: Mapping[str, Any]) -> str:
    """Return the name of the task a ``kedro run`` executes.

    A pod runs a single task (``--nodes <task>``, the fused download task
    included); otherwise the pipeline name stands for the run.

    Args:
        run_params: Parameters of the run.

    Returns:
        The single node name, else the pipeline name(s) joined by ``+``.

    Examples:
        >>> task_name({"node_names": ["download_eurostat"]})
        'download_eurostat'
        >>> task_name({"node_names": [], "pipeline_names": ["daily"]})
        'daily'
        >>> task_name({})
        '__default__'
    """
    nodes = list(run_params.get("node_names") or [])
    if len(nodes) == 1:
        return str(nodes[0])
    pipelines = run_params.get("pipeline_names") or run_params.get("pipeline_name") or ["__default__"]
    return "+".join([pipelines] if isinstance(pipelines, str) else list(pipelines))


# Fonction du contexte d'exécution d'une tâche
def run_context(
    env: Optional[str],
    tracking: Mapping[str, Any],
    environ: Mapping[str, str],
    *,
    started_at: float,
) -> Dict[str, Any]:
    """Return the context of the task, injected in the ``tracking`` parameters of its nodes.

    Args:
        env: Kedro environment.
        tracking: The ``tracking`` parameters (``LINKS``).
        environ: Environment of the pod (``WORKFLOW_ID``, ``IMAGE_TAG``, ``GIT_SHA``).
        started_at: Start of the task (``time.time()``).

    Returns:
        ``workflow_id``, ``env``, ``image``, ``git_sha``, ``started_at`` and
        ``links``, each present when known.

    Examples:
        >>> context = run_context("cloud", {}, {"WORKFLOW_ID": "wf-1"}, started_at=0.0)
        >>> context["workflow_id"], context["env"]
        ('wf-1', 'cloud')
    """
    from kedro_pipeline.io.tracking import render_links

    workflow_id = environ.get(WORKFLOW_ID_VARIABLE) or None
    context = {
        "workflow_id": workflow_id,
        "env": env,
        "image": environ.get(IMAGE_TAG_VARIABLE) or None,
        "git_sha": environ.get(GIT_SHA_VARIABLE) or None,
        "started_at": started_at,
        "links": render_links(tracking.get("LINKS"), workflow_id),
    }
    return {key: value for key, value in context.items() if value not in (None, {}, "")}


# Fonction des tags de regroupement du run d'une tâche
def run_tags(
    task: str,
    env: Optional[str],
    runtime: Mapping[str, Any],
    environ: Mapping[str, str],
) -> Dict[str, str]:
    """Return the tags of the MLflow run of a task, its name included.

    Every run of one workflow carries ``workflow_id``: the list of runs filtered
    on it shows a whole execution. The run is named ``<task>-<WORKFLOW_ID>``
    (``<task>-local`` outside a workflow).

    Args:
        task: Task name.
        env: Kedro environment.
        runtime: The ``runtime`` parameters (one-off forcing).
        environ: Environment of the pod.

    Returns:
        Tag name -> value (``mlflow.runName`` included).

    Examples:
        >>> tags = run_tags("publish_serving", "cloud", {}, {"WORKFLOW_ID": "wf-1", "GIT_SHA": "6f24c6c"})
        >>> tags["mlflow.runName"], tags["git_sha"], tags["forced"]
        ('publish_serving-wf-1', '6f24c6c', 'none')
    """
    from kedro_pipeline.io.freshness import ForceSpec

    workflow_id = environ.get(WORKFLOW_ID_VARIABLE) or None
    try:
        forced = ForceSpec.from_runtime(runtime, environ={}).describe() or "none"
    except Exception:
        forced = "none"
    tags = {
        "workflow_id": workflow_id,
        "git_sha": environ.get(GIT_SHA_VARIABLE) or None,
        "image_tag": environ.get(IMAGE_TAG_VARIABLE) or None,
        "kedro_env": env,
        "node": task,
        "forced": forced,
        "mlflow.runName": f"{task}-{workflow_id or LOCAL_RUN_SUFFIX}",
    }
    return {key: str(value) for key, value in tags.items() if value}


# Fonction de résolution du nombre de processus dans les paramètres d'exécution
def with_resolved_n_jobs(runtime: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a copy of the ``runtime`` parameters with ``N_JOBS`` resolved.

    Args:
        runtime: ``runtime`` parameters (``N_JOBS`` null, absent or positive).

    Returns:
        The copy, ``N_JOBS`` an integer of at least 1.

    Examples:
        >>> with_resolved_n_jobs({"N_JOBS": 3})["N_JOBS"]
        3
        >>> with_resolved_n_jobs({"N_JOBS": None})["N_JOBS"] >= 1
        True
    """
    from kedro_pipeline.parallel import resolve_n_jobs

    return {**dict(runtime), "N_JOBS": resolve_n_jobs(runtime.get("N_JOBS"))}
