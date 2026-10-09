"""Project pipelines: six business pipelines, their sum and the two scheduled cadences."""
# Importation des modules
# Modules de base
from typing import Any, Dict, Mapping, Optional

# Kedro et argo-kedro
from argo_kedro.pipeline import sum_pipelines
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.pipelines import baci, downloads, maintenance, serving, synthesis, vulnerabilities
from kedro_pipeline.pipelines._common import CADENCE_DAILY, CADENCE_WEEKLY

# Pipelines métier, dans l'ordre du flux de données
BUSINESS_PIPELINES = {
    "downloads": downloads,
    "baci": baci,
    "vulnerabilities": vulnerabilities,
    "synthesis": synthesis,
    "serving": serving,
    "maintenance": maintenance,
}


# Fonction d'enregistrement des pipelines
def register_pipelines(parameters: Optional[Mapping[str, Any]] = None) -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    * the six business pipelines (``downloads``, ``baci``,
      ``vulnerabilities``, ``synthesis``, ``serving``, ``maintenance``);
    * ``__default__``, their sum. It is built from the *tasks* of each
      pipeline (``sum_pipelines``), not with ``+``: Kedro's addition unpacks
      the fused download tasks into their inner nodes, which would then become
      separate pods;
    * ``daily`` and ``weekly``, the nodes of ``__default__`` tagged with the
      cadence. The filter adds no upstream node: an input produced by a node of
      the other cadence (e.g. the synthesis scores read by the daily serving
      publication) is read from the catalog as it is — every exchanged dataset
      is a catalog handle, never an in-memory result.

    Args:
        parameters: Project parameters (BACI targets, reference tables); those
            of the environment named by ``KEDRO_ENV`` when ``None``.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.

    Examples:
        >>> sorted(register_pipelines())  # doctest: +NORMALIZE_WHITESPACE
        ['__default__', 'baci', 'daily', 'downloads', 'maintenance', 'serving',
         'synthesis', 'vulnerabilities', 'weekly']
    """
    from kedro_pipeline.config import load_parameters

    parameters = parameters if parameters is not None else load_parameters()
    pipelines: Dict[str, Pipeline] = {
        name: module.create_pipeline(parameters) for name, module in BUSINESS_PIPELINES.items()
    }
    default = sum_pipelines(pipelines.values())
    pipelines["__default__"] = default
    pipelines["daily"] = default.only_nodes_with_tags(CADENCE_DAILY)
    pipelines["weekly"] = default.only_nodes_with_tags(CADENCE_WEEKLY)
    return pipelines
