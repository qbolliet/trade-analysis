"""Step functions of the pipeline, free of any Kedro dependency.

Each function receives resolved objects (parameter blocks, table handles, freshness
registries, tracker) and returns a ``StepResult``; it reads neither a YAML file nor an
environment variable, so the scripts and the Kedro nodes share one implementation.
"""
