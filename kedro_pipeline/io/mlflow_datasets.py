"""Kedro datasets of the tracking outputs of the nodes, written to the active MLflow run.

Each reporting node outputs its metrics to ``mlflow.metrics.<node>`` and its
artifact tables to ``mlflow.artifacts.<node>``. The run is the one the
kedro-mlflow hook opened for the task. Two rules hold:

* **no duplicate**: the steps already log their metrics while they run (series
  included: one point per year, per query, per vintage); the metrics dataset
  only writes what the run does not carry yet. The tables, on the contrary, are
  written by the dataset only (the steps of a node do not log them);
* **never blocking**: a tracking failure (server unreachable, rejected value)
  is a WARNING; a node never fails because of its tracking outputs. Without an
  active run (tracking disabled), nothing is written.

This module imports ``kedro_mlflow`` (hence MLflow): it is only loaded when the
catalog declares one of its datasets.
"""
# Importation des modules
# Modules de base
import logging
import math
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

# Kedro et kedro-mlflow
from kedro.io import AbstractDataset, DatasetError
from kedro_mlflow.io.metrics import MlflowMetricsHistoryDataset

# Modules de manipulation de données
import pandas as pd

# Modules du package
from kedro_pipeline.io.tracking import active_run_id, vintage_step
from macroforecast.tracking import ActiveRunTracker

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nombre maximal de métriques par requête acceptée par le serveur MLflow
_MAX_METRICS_PER_BATCH = 1000


# Fonction de lecture d'une valeur de métrique (nombre, ou format historique de kedro-mlflow)
def metric_points(value: Any) -> List[Tuple[float, int]]:
    """Return the finite ``(value, step)`` points of a metric value.

    Args:
        value: A number (step 0), ``{"value": v, "step": s}`` or a list of them.

    Returns:
        The finite points; empty for a non-numeric value.

    Examples:
        >>> metric_points(2), metric_points([{"value": 1.0, "step": 3}]), metric_points(float("nan"))
        ([(2.0, 0)], [(1.0, 3)], [])
    """
    items = value if isinstance(value, list) else [value]
    points: List[Tuple[float, int]] = []
    for item in items:
        if isinstance(item, Mapping):
            number, step = float(item["value"]), int(item.get("step", 0))
        elif isinstance(item, (int, float)) and not isinstance(item, bool):
            number, step = float(item), 0
        else:
            continue
        if math.isfinite(number):
            points.append((number, step))
    return points


# Fonction du nom et du step MLflow d'une métrique de la sortie d'un nœud
def metric_target(key: str) -> Tuple[str, Optional[int]]:
    """Return the MLflow name and step of a metric key of a node output.

    The metrics of a unit run are named ``<vintage label>/<name>`` in the node
    output (two units never share a key there); in the run, they are the metric
    ``<name>`` at the vintage-year step.

    Args:
        key: Key of the node output (``"HS2017/network/import/cells/n_total"``).

    Returns:
        ``(name, step)``; the step is ``None`` for a metric of the node itself.

    Examples:
        >>> metric_target("HS2017/network/import/cells/n_total")
        ('network/import/cells/n_total', 2017)
        >>> metric_target("units/planned")
        ('units/planned', None)
    """
    label, _, rest = str(key).partition("/")
    step = vintage_step(label) if rest else None
    return (rest, step) if step is not None else (str(key), None)


# Métriques d'un nœud : seules celles que le run ne porte pas encore
class MlflowRunMetricsDataset(MlflowMetricsHistoryDataset):
    """Metrics output of a node, written to the active MLflow run without duplicates.

    The node output repeats the metrics its steps already logged (so that
    Kedro sees them as data); this dataset writes only the names the run does
    not carry yet — in practice the unit counts of the node (``units/*``), or
    every metric when the direct logging failed. A unit metric keyed
    ``<vintage label>/<name>`` is written as ``<name>`` at the vintage-year
    step (see :func:`metric_target`). Declare ``prefix: ""`` in the catalog:
    with a null prefix, kedro-mlflow would prefix every name with the dataset
    name.

    Args:
        prefix: Prefix of the metric names (joined by ``.``); none when empty.
        run_id: Run written to; the active run when ``None``.
        metadata: Free metadata, ignored by Kedro.

    Examples:
        >>> MlflowRunMetricsDataset(prefix="").save({"units/planned": 1.0})  # no active run
    """

    def __init__(
        self,
        prefix: Optional[str] = "",
        run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(run_id=run_id, prefix=prefix, metadata=metadata)

    def _save(self, data: Mapping[str, Any]) -> None:
        """Write the metrics the run does not carry yet; never raises.

        Args:
            data: Metric name -> value (number, or kedro-mlflow history format).
        """
        run_id = self._run_id or active_run_id()
        if run_id is None or not self._logging_activated:
            return
        try:
            from mlflow.entities import Metric
            from mlflow.tracking import MlflowClient

            client = MlflowClient()
            present = set(client.get_run(run_id).data.metrics)
            timestamp = int(time.time() * 1000)
            missing = []
            for key, value in (data or {}).items():
                name, unit_step = metric_target(key)
                name = f"{self._prefix}.{name}" if self._prefix else name
                # Métrique déjà journalisée en direct par l'étape : rien à écrire
                if name in present:
                    continue
                missing.extend(
                    Metric(name, number, timestamp, unit_step if unit_step is not None else step)
                    for number, step in metric_points(value)
                )
            for start in range(0, len(missing), _MAX_METRICS_PER_BATCH):
                client.log_batch(run_id, metrics=missing[start:start + _MAX_METRICS_PER_BATCH])
        except Exception as exc:
            # Le suivi n'interrompt jamais un nœud
            logger.warning(f"Métriques du nœud non journalisées dans MLflow : {exc}")


# Tables d'un nœud : artefacts CSV du run actif
class MlflowTablesDataset(AbstractDataset[Dict[str, pd.DataFrame], None]):
    """Artifact tables of a node, written as CSV files of the active MLflow run.

    The node returns its tables keyed by artifact path without extension
    (``download/queries``, ``HS2017/output/rows_by_year``); each one is written
    to ``<artifact_path>/<key>.csv`` — the very paths the transitional scripts
    logged directly. Write-only.

    Args:
        artifact_path: Folder of the artifacts in the run; the root when empty.
        metadata: Free metadata, ignored by Kedro.

    Examples:
        >>> MlflowTablesDataset().save({"download/queries": pd.DataFrame({"a": [1]})})  # no active run
    """

    def __init__(self, *, artifact_path: str = "", metadata: Optional[Dict[str, Any]] = None) -> None:
        self._artifact_path = str(artifact_path or "").strip("/")
        self.metadata = metadata

    def load(self) -> Dict[str, pd.DataFrame]:
        """Refuse the read: the tables are only written.

        Raises:
            DatasetError: Always.
        """
        raise DatasetError("MlflowTablesDataset is write-only: read the artifacts in MLflow")

    def save(self, data: Mapping[str, Any]) -> None:
        """Write each table as a CSV artifact of the active run; never raises.

        Args:
            data: Artifact key (path without ``.csv``) -> table; values that are
                not DataFrames are ignored.
        """
        if active_run_id() is None:
            return
        # Écritures gardées une à une : un échec n'empêche pas les tables suivantes
        tracker = ActiveRunTracker()
        for key, table in (data or {}).items():
            if not isinstance(table, pd.DataFrame):
                continue
            path = str(key) if str(key).endswith(".csv") else f"{key}.csv"
            tracker.log_table(table, f"{self._artifact_path}/{path}" if self._artifact_path else path)

    def _describe(self) -> Dict[str, Any]:
        """Describe the dataset."""
        return {"artifact_path": self._artifact_path}
