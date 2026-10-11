"""Pipeline et configuration jouets du rendu Argo.

Quatre tâches couvrant chaque règle du rendu :

* ``download_toy`` : tâche fusionnée de deux nœuds, cadence quotidienne, sans reprise ;
* ``compute_toy`` : cadence hebdomadaire, lit la sortie du téléchargement (dépendance hors
  du point d'entrée, donc absente du DAG hebdomadaire) ;
* ``publish_toy`` : partagée par les deux cadences, sérialisée par un mutex, dépendance
  stricte envers ``compute_toy`` ;
* ``cleanup_toy`` : nœud de fin (``onexit``), sérialisé par un second mutex.
"""

from __future__ import annotations

from argo_kedro.pipeline import FusedPipeline, Node, sum_pipelines
from kedro.pipeline import Pipeline

from kedro_pipeline.deploy.config import EntryPointSettings, MachineSpec, RenderConfig, SecretKey


def _identity(*args):
    return args[0] if args else 0


def toy_pipeline() -> Pipeline:
    """Pipeline complet jouet (équivalent de ``__default__``)."""
    download = FusedPipeline(
        [
            Node(_identity, None, "toy_intermediate", name="fetch_toy"),
            Node(_identity, "toy_intermediate", "toy_raw", name="publish_reference_toy"),
        ],
        name="download_toy",
        tags=["experiment.toy-01-downloads", "cadence.daily"],
        machine_type="small",
    )
    compute = Pipeline(
        [
            Node(
                _identity, "toy_raw", "toy_scores", name="compute_toy",
                tags=["experiment.toy-02-compute", "cadence.weekly"], machine_type="large",
            )
        ]
    )
    publish = Pipeline(
        [
            Node(
                _identity, ["toy_raw", "toy_scores"], "toy_served", name="publish_toy",
                tags=["experiment.toy-03-serving", "cadence.daily", "cadence.weekly", "mutex.toy-serving"],
            )
        ]
    )
    cleanup = Pipeline(
        [
            Node(
                _identity, "params:cleanup", "toy_cleaned", name="cleanup_toy",
                tags=["experiment.toy-00-maintenance", "onexit", "mutex.toy-maintenance"],
            )
        ]
    )
    return sum_pipelines([download, compute, publish, cleanup])


def toy_config(**overrides) -> RenderConfig:
    """Configuration jouet du rendu."""
    values = dict(
        env="toy",
        namespace="toy-namespace",
        image="registry.example/toy",
        image_tag="sha-0000000",
        machine_types={"small": MachineSpec(cpu=1, mem=2), "large": MachineSpec(cpu=4, mem=32)},
        default_machine_type="small",
        name="toy-pipeline",
        argo_version="3.6.10",
        service_account="toy-sa",
        labels={"app": "toy"},
        parallelism=3,
        image_pull_policy="IfNotPresent",
        timezone="Europe/Paris",
        node_retries={"download_*": 0},
        strict_dependencies={"publish_*": ("compute_toy",)},
        entry_points=(
            EntryPointSettings("daily", "cadence.daily", 15, 1, False, 3600, 600, 3),
            EntryPointSettings("weekly", "cadence.weekly", 0, 2, True, 7200, 900, 2),
        ),
        environment={"PLAIN": "value", "EMPTY": ""},
        secret_environment={"TOKEN": SecretKey("toy-secret", "TOKEN")},
        secret_environment_from=("toy-env",),
        system_metrics_enabled=False,
        system_metrics_sampling_seconds=10,
        weekly_day=0,
    )
    values.update(overrides)
    return RenderConfig(**values)
