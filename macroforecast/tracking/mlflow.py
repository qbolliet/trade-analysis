# Importation des modules
# Modules de base
from __future__ import annotations
import importlib
import logging
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping, Optional
# Modules de manipulation des données
import pandas as pd
# Modules du package
from .base import NULL_TRACKER, RunTracker

# Initialisation du logger
logger = logging.getLogger(__name__)

# Délais par défaut appliqués à la sonde de disponibilité : sans eux, un URI
# injoignable bloque plusieurs minutes en reprises HTTP
_DEFAULT_HTTP_TIMEOUT = "5"
_DEFAULT_HTTP_MAX_RETRIES = "1"


# ──────────────────────────────────────────────────────────────────────
# Implémentation MLflow du protocole de suivi
# ──────────────────────────────────────────────────────────────────────

# Suivi d'exécution adossé à MLflow
class MlflowTracker:
    """:class:`~macroforecast.tracking.base.RunTracker` backed by MLflow.

    ``mlflow`` is imported **lazily**, inside :meth:`__enter__`, so importing
    ``macroforecast`` never requires the optional dependency. Every write is
    guarded: a failure warns and the tracker degrades to a no-op for the rest
    of the run rather than interrupting the computation.

    Args:
        tracking_uri: URI of the MLflow tracking server or store.
        experiment: Experiment name, created on the fly when absent.
        run_name: Name given to the opened run.
        tags: Tags attached to the run at creation.

    Examples:
        >>> tracker = MlflowTracker(tracking_uri="file:///tmp/mlruns")
        >>> tracker.tracking_uri
        'file:///tmp/mlruns'
    """

    # Initialisation
    def __init__(
        self,
        *,
        tracking_uri: str,
        experiment: Optional[str] = None,
        run_name: Optional[str] = None,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.tracking_uri = tracking_uri
        self.experiment = experiment
        self.run_name = run_name
        self.tags = tags
        # Attributs d'exécution : module importé paresseusement et état du run
        self._mlflow: Any = None
        self._active: bool = False

    # Ouverture du run
    def __enter__(self) -> "MlflowTracker":
        """Open the MLflow run, degrading to a no-op on any failure.

        Returns:
            The tracker itself, active or degraded.
        """
        try:
            # Import paresseux : MLflow est une dépendance optionnelle
            self._mlflow = importlib.import_module("mlflow")
            self._mlflow.set_tracking_uri(self.tracking_uri)
            if self.experiment:
                self._mlflow.set_experiment(self.experiment)
            self._mlflow.start_run(run_name=self.run_name, tags=dict(self.tags or {}))
            self._active = True
        except Exception as exc:
            # Aucun échec de suivi n'interrompt un traitement
            logger.warning(
                "MLflow run could not be started on %s (%s): tracking disabled "
                "for this run.",
                self.tracking_uri,
                exc,
            )
            self._active = False
        return self

    # Fermeture du run
    def __exit__(self, *exc: Any) -> None:
        """Close the MLflow run without swallowing a business exception.

        Args:
            *exc: Exception triple of the ``with`` block, propagated as is.
        """
        if not self._active:
            return
        try:
            self._mlflow.end_run()
        except Exception as end_exc:
            # Logging
            logger.warning("MLflow run could not be closed: %s", end_exc)
        finally:
            self._active = False

    # Méthode auxiliaire : exécution protégée d'une écriture
    def _guard(self, action: str, call: Any) -> None:
        """Run a tracking write, warning instead of raising.

        Args:
            action: Short description of the write, used in the warning.
            call: Zero-argument callable performing the write.
        """
        if not self._active:
            return
        try:
            call()
        except Exception as exc:
            # Logging
            logger.warning("MLflow %s failed: %s", action, exc)

    # Enregistrement des paramètres
    def log_params(self, params: Mapping[str, Any]) -> None:
        """Record the parameters of the run.

        Args:
            params: Mapping of parameter names to values.
        """
        self._guard("log_params", lambda: self._mlflow.log_params(dict(params)))

    # Enregistrement des métriques
    def log_metrics(self, metrics: Mapping[str, float], step: Optional[int] = None) -> None:
        """Record numeric metrics, dropping the non-finite ones.

        Args:
            metrics: Mapping of metric names to values.
            step: Optional step index.
        """
        # Filtrage des valeurs non finies, rejetées par MLflow
        finite = {
            name: float(value)
            for name, value in metrics.items()
            if isinstance(value, (int, float)) and math.isfinite(float(value))
        }
        dropped = len(metrics) - len(finite)
        if dropped:
            # Logging
            logger.warning("MLflow log_metrics: %d non-finite metrics dropped", dropped)
        if not finite:
            return
        self._guard("log_metrics", lambda: self._mlflow.log_metrics(finite, step=step))

    # Enregistrement d'un dictionnaire en artefact
    def log_dict(self, obj: Mapping[str, Any], artifact_file: str) -> None:
        """Record a mapping as a JSON artifact.

        Args:
            obj: Mapping to serialise.
            artifact_file: Artifact path, e.g. ``"gravity/coefficients.json"``.
        """
        self._guard(
            f"log_dict({artifact_file})",
            lambda: self._mlflow.log_dict(dict(obj), artifact_file),
        )

    # Enregistrement d'une table en artefact
    def log_table(self, df_table: pd.DataFrame, artifact_file: str) -> None:
        """Record a table as a CSV artifact.

        Args:
            df_table: Table to serialise.
            artifact_file: Artifact path, e.g. ``"quality/sigma_by_country.csv"``.
        """
        def _write() -> None:
            # Écriture dans un fichier temporaire, MLflow ne versant que des fichiers
            artifact_path = str(Path(artifact_file).parent).replace("\\", "/")
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / Path(artifact_file).name
                df_table.to_csv(local_path, index=False, encoding="utf-8")
                self._mlflow.log_artifact(
                    str(local_path),
                    artifact_path=None if artifact_path in (".", "") else artifact_path,
                )

        self._guard(f"log_table({artifact_file})", _write)

    # Attachement des tags
    def set_tags(self, tags: Mapping[str, str]) -> None:
        """Attach tags to the run.

        Args:
            tags: Mapping of tag names to values.
        """
        self._guard("set_tags", lambda: self._mlflow.set_tags(dict(tags)))


# ──────────────────────────────────────────────────────────────────────
# Fabrique : tracker MLflow ou objet nul
# ──────────────────────────────────────────────────────────────────────

# Fabrique du tracker adapté à l'environnement
def get_tracker(
    tracking_uri: Optional[str] = None,
    experiment: Optional[str] = None,
    run_name: Optional[str] = None,
    tags: Optional[Mapping[str, str]] = None,
) -> RunTracker:
    """Return an MLflow-backed tracker, or a no-op one.

    Falls back to :data:`~macroforecast.tracking.base.NULL_TRACKER` when
    ``tracking_uri`` is empty, when the ``mlflow`` package is not installed, or
    when the server is unreachable. The ``MLFLOW_TRACKING_URI`` environment
    variable **overrides** the argument, following the MLflow convention and
    easing Kubernetes deployment.

    Args:
        tracking_uri: URI of the MLflow tracking server or store; ``None`` or
            empty disables tracking altogether.
        experiment: Experiment name, created on the fly when absent.
        run_name: Name given to the opened run.
        tags: Tags attached to the run at creation.

    Returns:
        An :class:`MlflowTracker` when tracking is both requested and usable,
        the shared null tracker otherwise.

    Examples:
        >>> from macroforecast.tracking import NullTracker
        >>> isinstance(get_tracker(tracking_uri=None), NullTracker)
        True
    """
    # Surcharge par l'environnement (convention MLflow)
    uri = os.environ.get("MLFLOW_TRACKING_URI") or tracking_uri
    # Absence d'URI : suivi désactivé, exécution inchangée et silencieuse
    if not uri:
        return NULL_TRACKER

    # Délais de la sonde : bornés si l'utilisateur ne les a pas fixés
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", _DEFAULT_HTTP_TIMEOUT)
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", _DEFAULT_HTTP_MAX_RETRIES)

    # Import paresseux : dépendance optionnelle (extra « tracking »)
    try:
        tracking = importlib.import_module("mlflow.tracking")
    except Exception as exc:
        # Logging
        logger.warning(
            "MLflow tracking requested (%s) but the 'mlflow' package is not "
            "importable (%s): install the 'tracking' extra to enable it. "
            "Continuing without tracking.",
            uri,
            exc,
        )
        return NULL_TRACKER

    # Sonde de disponibilité : un serveur injoignable ne doit pas faire échouer
    # le traitement, seulement désactiver le suivi
    try:
        tracking.MlflowClient(tracking_uri=uri).search_experiments(max_results=1)
    except Exception as exc:
        # Logging
        logger.warning(
            "MLflow tracking server %s is unreachable (%s): continuing without "
            "tracking.",
            uri,
            exc,
        )
        return NULL_TRACKER

    return MlflowTracker(
        tracking_uri=uri, experiment=experiment, run_name=run_name, tags=tags
    )
