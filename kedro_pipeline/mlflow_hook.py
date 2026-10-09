"""Guarded kedro-mlflow hook: tracking never interrupts a computation.

kedro-mlflow opens one MLflow run per ``kedro run`` (one per Argo task), logs
the parameters of every node and closes the run at the end. Its own hook,
however, lets every tracking error propagate: an unreachable server makes the
creation of the Kedro context fail (the experiment is looked up as soon as the
context exists), and a server lost during the run makes the next node fail
(the run is reopened and the parameters logged before each node). The
pipeline must on the contrary finish its computation with the same exit code,
tracking or not.

:class:`GuardedMlflowHook` is therefore registered by the project instead of
the plugin's hook (whose entry point is disabled in the settings):

* the tracking URI is probed first, with short timeouts; without a URI, or
  when the server does not answer, tracking is disabled for the session with a
  WARNING, and the MLflow datasets of the catalog are switched off;
* every hook of kedro-mlflow is called inside a guard: an exception is logged
  as a WARNING and disables tracking for the rest of the session, it never
  reaches the runner.

Without a configured URI (``server.mlflow_tracking_uri`` of ``mlflow.yml``,
else the ``MLFLOW_TRACKING_URI`` variable), tracking is off: kedro-mlflow would
otherwise fall back on a local ``mlruns`` file store, which MLflow 3 refuses.
"""
# Importation des modules
# Modules de base
import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

# Kedro et kedro-mlflow
from kedro.config import MissingConfigException
from kedro.framework.hooks import hook_impl
from kedro_mlflow.config.kedro_mlflow_config import _validate_uri
from kedro_mlflow.framework.hooks.mlflow_hook import MlflowHook
from kedro_mlflow.io.catalog.switch_catalog_logging import switch_catalog_logging

# Initialisation du logger
logger = logging.getLogger(__name__)

# Délais de la sonde de disponibilité du serveur : sans eux, un URI injoignable
# bloquerait plusieurs minutes en reprises HTTP avant le premier nœud
PROBE_ENVIRONMENT = {"MLFLOW_HTTP_REQUEST_TIMEOUT": "5", "MLFLOW_HTTP_REQUEST_MAX_RETRIES": "0"}


# Gestionnaire de contexte : variables d'environnement posées le temps de la sonde
@contextmanager
def _temporary_environment(values: Dict[str, str]) -> Iterator[None]:
    """Set environment variables for the duration of the block, unless already set.

    Args:
        values: Variable -> value; a variable the user already set is kept.

    Yields:
        Nothing.
    """
    added = {name: value for name, value in values.items() if name not in os.environ}
    os.environ.update(added)
    try:
        yield
    finally:
        for name in added:
            os.environ.pop(name, None)


# Fonction de sonde d'un serveur de suivi
def probe_tracking_uri(uri: str) -> Optional[str]:
    """Check that an MLflow tracking URI answers, with short timeouts.

    Args:
        uri: Tracking URI (server, database or file store).

    Returns:
        ``None`` when the store answers, else the error message.

    Examples:
        >>> probe_tracking_uri("http://127.0.0.1:9") is not None
        True
    """
    try:
        from mlflow.tracking import MlflowClient

        with _temporary_environment(PROBE_ENVIRONMENT):
            MlflowClient(tracking_uri=uri).search_experiments(max_results=1)
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


# Hook kedro-mlflow dont aucune erreur n'atteint le runner
class GuardedMlflowHook(MlflowHook):
    """kedro-mlflow hook whose errors never interrupt the pipeline.

    Same behaviour as kedro-mlflow's ``MlflowHook`` (one run per session,
    parameters of the nodes logged, run closed at the end, ``FAILED`` on a
    pipeline error) when the tracking store is reachable; otherwise a WARNING
    and a session without tracking.

    Attributes:
        tracking_enabled: Whether tracking is active for the session.

    Examples:
        >>> GuardedMlflowHook().tracking_enabled
        True
    """

    # Initialisation
    def __init__(self) -> None:
        super().__init__()
        self.tracking_enabled = True

    # Méthode auxiliaire : désactivation du suivi pour la session
    def _disable(self, reason: str) -> None:
        """Disable tracking for the rest of the session.

        Args:
            reason: Cause, written in the WARNING.
        """
        if self.tracking_enabled:
            # Logging
            logger.warning(f"Suivi MLflow désactivé pour cette exécution : {reason}")
        self.tracking_enabled = False
        self._is_mlflow_enabled = False
        self.run_id = None

    # Méthode auxiliaire : appel protégé d'un hook de kedro-mlflow
    def _guarded(self, hook: str, call: Any) -> None:
        """Call a kedro-mlflow hook, disabling tracking instead of raising.

        Args:
            hook: Name of the hook, written in the WARNING.
            call: Zero-argument callable running the hook.
        """
        if not self.tracking_enabled:
            return
        try:
            call()
        except Exception as exc:
            self._disable(f"{hook} a échoué ({type(exc).__name__}: {exc})")

    # Méthode auxiliaire : URI de suivi configuré pour la session
    @staticmethod
    def _configured_uri(context: Any) -> Optional[str]:
        """Return the tracking URI of the session, normalised as kedro-mlflow does.

        Args:
            context: The Kedro context.

        Returns:
            ``server.mlflow_tracking_uri`` of ``mlflow.yml``, else the
            ``MLFLOW_TRACKING_URI`` variable; ``None`` when neither is set.
        """
        try:
            server = (context.config_loader["mlflow"] or {}).get("server") or {}
        except MissingConfigException:
            server = {}
        uri = server.get("mlflow_tracking_uri") or os.environ.get("MLFLOW_TRACKING_URI")
        return _validate_uri(project_path=context.project_path, uri=str(uri)) if uri else None

    @hook_impl
    def after_context_created(self, context: Any) -> None:
        """Probe the tracking store, then configure MLflow as kedro-mlflow does.

        Args:
            context: The Kedro context just created.
        """
        self.tracking_enabled = True
        try:
            uri = self._configured_uri(context)
        except Exception as exc:
            self._disable(f"configuration mlflow.yml illisible ({type(exc).__name__}: {exc})")
            return
        if uri is None:
            self._disable("aucun MLFLOW_TRACKING_URI ni server.mlflow_tracking_uri")
            return
        error = probe_tracking_uri(uri)
        if error is not None:
            self._disable(f"serveur {uri} injoignable ({error})")
            return
        self._guarded("after_context_created", lambda: super(GuardedMlflowHook, self).after_context_created(context))

    @hook_impl
    def after_catalog_created(  # noqa: PLR0913
        self,
        catalog: Any,
        conf_catalog: Dict[str, Any],
        conf_creds: Dict[str, Any],
        parameters: Dict[str, Any],
        save_version: str,
        load_versions: Dict[str, str],
    ) -> None:
        """Run kedro-mlflow's catalog hook when tracking is active.

        Args:
            catalog: The catalog of the session.
            conf_catalog: Catalog configuration.
            conf_creds: Credentials configuration.
            parameters: Parameters of the session.
            save_version: Save version.
            load_versions: Load versions.
        """
        self._guarded(
            "after_catalog_created",
            lambda: super(GuardedMlflowHook, self).after_catalog_created(
                catalog, conf_catalog, conf_creds, parameters, save_version, load_versions
            ),
        )

    @hook_impl
    def before_pipeline_run(self, run_params: Dict[str, Any], pipeline: Any, catalog: Any) -> None:
        """Open the run of the session, or switch the MLflow datasets off.

        Args:
            run_params: Parameters of the run.
            pipeline: Pipeline about to run.
            catalog: Catalog of the run.
        """
        self._guarded(
            "before_pipeline_run",
            lambda: super(GuardedMlflowHook, self).before_pipeline_run(run_params, pipeline, catalog),
        )
        if not self.tracking_enabled:
            try:
                switch_catalog_logging(catalog, False)
            except Exception as exc:
                # Logging
                logger.warning(f"Datasets MLflow non désactivés : {exc}")

    @hook_impl
    def before_node_run(self, node: Any, catalog: Any, inputs: Dict[str, Any], is_async: bool) -> None:
        """Log the parameters of the node into the run of the session.

        kedro-mlflow reopens the run before every node (``start_run(run_id=…,
        nested=True)``), for the runners that execute nodes in other threads.
        In the thread that already holds the run, that call pushes the same run
        once more on MLflow's stack and starts one more system-metrics monitor
        per node: the samples are duplicated, and the extra stack entries
        outlive the session, so that a later session of the same process would
        log into this run. The reopening is therefore skipped when the run is
        already active in the current thread. A failure is a WARNING and does
        not disable tracking: the next nodes still log.

        Args:
            node: Node about to run.
            catalog: Catalog of the run.
            inputs: Loaded inputs of the node.
            is_async: Whether the node runs asynchronously.
        """
        if not self.tracking_enabled:
            return
        run_id = self.run_id
        try:
            import mlflow

            active = mlflow.active_run()
            if active is not None and active.info.run_id == run_id:
                # Run déjà actif dans ce fil : paramètres seuls, sans réouverture
                self.run_id = None
            super().before_node_run(node, catalog, inputs, is_async)
        except Exception as exc:
            # Logging
            logger.warning(f"Paramètres du nœud {node.name} non journalisés dans MLflow : {exc}")
        finally:
            self.run_id = run_id

    def _log_param(self, name: str, value: Any) -> None:
        """Log one parameter, a rejected value being a WARNING for that parameter only.

        Args:
            name: Parameter name.
            value: Parameter value.
        """
        try:
            super()._log_param(name, value)
        except Exception as exc:
            # Logging
            logger.warning(f"Paramètre MLflow '{name}' non journalisé : {exc}")

    @hook_impl
    def after_pipeline_run(self, run_params: Dict[str, Any], pipeline: Any, catalog: Any) -> None:
        """Close the run of the session.

        Args:
            run_params: Parameters of the run.
            pipeline: Pipeline that ran.
            catalog: Catalog of the run.
        """
        self._guarded(
            "after_pipeline_run",
            lambda: super(GuardedMlflowHook, self).after_pipeline_run(run_params, pipeline, catalog),
        )

    @hook_impl
    def on_pipeline_error(self, error: Exception, run_params: Dict[str, Any], pipeline: Any, catalog: Any) -> None:
        """Close the run of the session as ``FAILED``.

        Args:
            error: Exception of the pipeline.
            run_params: Parameters of the run.
            pipeline: Pipeline that failed.
            catalog: Catalog of the run.
        """
        self._guarded(
            "on_pipeline_error",
            lambda: super(GuardedMlflowHook, self).on_pipeline_error(error, run_params, pipeline, catalog),
        )
