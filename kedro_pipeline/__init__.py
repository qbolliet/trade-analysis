"""Kedro project of the trade vulnerability pipeline.

The package holds the Kedro project (settings, pipeline registry, hooks, project
commands), the I/O layer (DuckLake handles and Kedro datasets, freshness
registries, serving catalog) and the step logic. The methodology itself stays in
``macroforecast``, which never depends on Kedro.
"""
