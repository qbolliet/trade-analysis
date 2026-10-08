"""Project hooks of the trade vulnerability pipeline.

The hooks of ``kedro-mlflow`` and ``argo-kedro`` register themselves through
their entry points: they must not be repeated here.
"""


# Hooks propres au projet
class TradeRunHooks:
    """Project-level Kedro hooks (run tags, intra-pod parallelism).

    Empty for now: the class is registered in ``settings.HOOKS`` so that the
    run-level behaviour (MLflow tags such as the workflow id, translation of the
    pod CPU count into ``n_jobs``) can be added without touching the settings.

    Examples:
        >>> from kedro_pipeline.settings import HOOKS
        >>> any(isinstance(hook, TradeRunHooks) for hook in HOOKS)
        True
    """
