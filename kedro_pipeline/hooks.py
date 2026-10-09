"""Project hooks of the trade vulnerability pipeline.

The hooks of ``kedro-mlflow`` and ``argo-kedro`` register themselves through
their entry points: they must not be repeated here.
"""
# Importation des modules
# Modules de base
import logging
from types import SimpleNamespace
from typing import Any, Dict, Mapping, Optional

# Kedro
from kedro.framework.hooks import hook_impl

# Initialisation du logger
logger = logging.getLogger(__name__)

# Entrée des paramètres d'exécution partagés
RUNTIME_INPUT = "params:runtime"


# Hooks propres au projet
class TradeRunHooks:
    """Project-level Kedro hooks (intra-pod parallelism, failed-node reporting).

    * ``after_context_created``: gives the context the runner settings that
      the ``kedro run`` command of argo-kedro reads (``context.argo``), as long
      as the argo-kedro hook — which normally sets them from ``argo.yml`` — is
      disabled;
    * ``before_node_run``: resolves the number of worker processes of the pod
      and injects it in the ``runtime`` parameters of the node, so that the
      nodes never read the environment;
    * ``on_node_error``: saves the metrics and artifacts outputs of a node
      that failed after computing its result, which Kedro would otherwise
      drop with the other outputs.

    Examples:
        >>> from kedro_pipeline.settings import HOOKS
        >>> any(isinstance(hook, TradeRunHooks) for hook in HOOKS)
        True
    """

    # Paramètres du runner d'argo-kedro, tant que son hook est désactivé
    @hook_impl
    def after_context_created(self, context: Any) -> None:
        """Set ``context.argo`` to the default runner settings when absent.

        The ``kedro run`` command of argo-kedro (it replaces Kedro's own and
        runs the fused tasks) reads ``context.argo.runner.use_memory_datasets``.
        The hook of argo-kedro sets it from ``argo.yml``, but it is disabled
        until that file exists; the defaults are then given here.

        Args:
            context: The Kedro context just created.
        """
        if hasattr(context, "argo"):
            return
        try:
            from argo_kedro.config.kedro_argo_config import RunnerConfig
        except ImportError:
            return
        context.__setattr__("argo", SimpleNamespace(runner=RunnerConfig()))

    # Résolution du nombre de processus du pod
    @hook_impl
    def before_node_run(self, node: Any, inputs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Inject the resolved ``N_JOBS`` into the ``runtime`` parameters of the node.

        ``runtime.N_JOBS`` when positive, else the CPUs allocated to the pod
        (``NUM_CPU``), else the CPUs of the machine. A step block with its own
        ``N_JOBS`` still takes precedence in the node.

        Args:
            node: Node about to run.
            inputs: Loaded inputs of the node.

        Returns:
            ``{"params:runtime": ...}`` with ``N_JOBS`` resolved, or ``None``
            when the node does not read the ``runtime`` parameters.
        """
        runtime = inputs.get(RUNTIME_INPUT)
        if not isinstance(runtime, Mapping):
            return None
        return {RUNTIME_INPUT: with_resolved_n_jobs(runtime)}

    # Sauvegarde des métriques d'un nœud en échec
    @hook_impl
    def on_node_error(self, error: BaseException, node: Any, catalog: Any) -> None:
        """Save the reporting outputs of a node whose step failed.

        A node raises its step failure only after saving its registries; the
        exception carries the step result, whose metrics and artifacts are
        saved here into the node's own reporting datasets.

        Args:
            error: Exception raised by the node.
            node: The failed node.
            catalog: The data catalog of the run.
        """
        from kedro_pipeline.pipelines._common import reporting_values

        result = getattr(error, "step_result", None)
        if result is None:
            return
        for name, value in reporting_values(node.outputs, result).items():
            try:
                catalog.save(name, value)
            except Exception as exc:
                # Le suivi ne masque jamais l'erreur du nœud
                logger.warning(f"Sortie de suivi '{name}' du nœud en échec non sauvegardée : {exc}")


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
