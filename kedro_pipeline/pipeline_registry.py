"""Project pipelines."""
# Importation des modules
from kedro.pipeline import Pipeline


# Fonction d'enregistrement des pipelines
def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    No business pipeline exists yet: the default pipeline is empty, so that the
    project, its configuration and its catalog can already be loaded and checked.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.

    Examples:
        >>> list(register_pipelines())
        ['__default__']
    """
    return {"__default__": Pipeline([])}
