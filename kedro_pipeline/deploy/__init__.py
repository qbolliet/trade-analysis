"""Rendering of the production Argo manifests from the Kedro pipelines.

``kedro trade render-argo`` turns the ``daily`` and ``weekly`` pipelines into a
``WorkflowTemplate`` (one DAG template per entry point, exit handler, mutexes)
and two ``CronWorkflow`` (one per entry point), written to
``kubernetes/generated/`` and checked against the code in CI.
"""
