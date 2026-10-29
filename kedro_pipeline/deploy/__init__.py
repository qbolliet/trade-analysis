"""Rendering of the Argo DAG data of the ``trade-pipeline`` Helm chart.

``kedro trade render-argo`` turns the ``daily`` and ``weekly`` pipelines of each
Kedro environment into a data file (one DAG per entry point, exit handler,
mutexes), written to ``helm/trade-pipeline/files/`` and checked against the
code in CI. The chart templates turn it into a ``WorkflowTemplate`` and its
``CronWorkflow`` resources.
"""
