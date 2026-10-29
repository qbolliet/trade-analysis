"""Configuration of the DAG data rendering, read from ``argo.yml`` and the parameters.

The ``argo`` configuration section holds two parts: the one validated by
argo-kedro (namespace, image, machine types, runner) and the project's
``workflow`` block, restricted to what shapes the DAG (entry points and their
cadence tags, default schedule, retries, strict dependencies, environments
rendered for the Helm chart). Everything that belongs to the deployment
(image tag, namespace, service account, deadlines, parallelism, environment
and secrets of the pods) is a value of the ``trade-pipeline`` Helm chart.

Two values come from the parameters instead, since the pipeline steps read
them too: the weekly day (``runtime.WEEKLY_DAY``) and the MLflow system
metrics settings (``tracking.SYSTEM_METRICS``).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Tuple


# Ressources d'un type de machine
@dataclass(frozen=True)
class MachineSpec:
    """Resources of a machine type (one pod).

    Attributes:
        cpu: CPU count (requests = limits).
        mem: Memory in GiB (requests = limits, so that an out-of-memory kill is predictable).
        num_gpu: GPU count (kept for argo-kedro, not rendered).
        emph_storage: Ephemeral storage in GiB (kept for argo-kedro, not rendered).

    Examples:
        >>> MachineSpec(cpu=4, mem=32).mem
        32
    """

    cpu: int
    mem: int
    num_gpu: int = 0
    emph_storage: int = 10


# Réglages d'un point d'entrée
@dataclass(frozen=True)
class EntryPointSettings:
    """Settings of one scheduled entry point (a DAG template and its CronWorkflow).

    Attributes:
        name: Entry point name (``daily``, ``weekly``): DAG template name and suffix of
            the CronWorkflow name.
        tag: Cadence tag of the Kedro nodes it runs (``cadence.daily``).
        minute: Minute of the default scheduled start.
        hour: Hour of the default scheduled start, in the workflow timezone.
        weekly: Whether the schedule is weekly (day taken from ``runtime.WEEKLY_DAY``)
            rather than daily.

    Examples:
        >>> EntryPointSettings("daily", "cadence.daily", 0, 1, False).weekly
        False
    """

    name: str
    tag: str
    minute: int = 0
    hour: int = 0
    weekly: bool = False


# Configuration complète du rendu
@dataclass(frozen=True)
class RenderConfig:
    """Everything the DAG rendering needs, independent of the Kedro session.

    Attributes:
        env: Kedro environment the DAG is computed for (the BACI fan-out depends on the
            targets of this environment).
        machine_types: Machine types by name.
        default_machine_type: Machine type of a node without one.
        default_retries: Retries on infrastructure error of a task.
        node_retries: Retries by node name pattern (``fnmatch``), first match wins.
        strict_dependencies: Upstream nodes a node (by name pattern) requires to
            have succeeded; any other dependency tolerates an upstream failure.
        entry_points: Entry points, in rendering order.
        system_metrics_enabled: Whether MLflow logs the pod's system metrics.
        system_metrics_sampling_seconds: Sampling interval of the system metrics.
        weekly_day: Day of the weekly cadence (0 = Monday … 6 = Sunday).

    Examples:
        >>> config = RenderConfig(env="cloud", machine_types={"small": MachineSpec(1, 2)},
        ...     default_machine_type="small")
        >>> config.default_retries
        1
    """

    env: str
    machine_types: Mapping[str, MachineSpec]
    default_machine_type: str
    default_retries: int = 1
    node_retries: Mapping[str, int] = field(default_factory=dict)
    strict_dependencies: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)
    entry_points: Tuple[EntryPointSettings, ...] = ()
    system_metrics_enabled: bool = True
    system_metrics_sampling_seconds: int = 30
    weekly_day: int = 5

    # Construction depuis les sections de configuration
    @classmethod
    def from_sections(cls, argo: Mapping[str, Any], parameters: Mapping[str, Any], env: str) -> "RenderConfig":
        """Build the configuration from the ``argo`` section and the parameters.

        Args:
            argo: The ``argo`` configuration section (``argo.yml`` merged for ``env``).
            parameters: The project parameters (``runtime.WEEKLY_DAY``,
                ``tracking.SYSTEM_METRICS``).
            env: Kedro environment the DAG is computed for.

        Returns:
            The rendering configuration.

        Raises:
            KeyError: If a required key is missing.
            ValueError: If the default machine type is not defined.

        Examples:
            >>> argo = {"machine_types": {"small": {"cpu": 1, "mem": 2, "num_gpu": 0}},
            ...         "default_machine_type": "small",
            ...         "workflow": {"entry_points": {"daily": {"tag": "cadence.daily", "hour": 1}}}}
            >>> RenderConfig.from_sections(argo, {"runtime": {"WEEKLY_DAY": 5}}, "cloud").entry_points[0].hour
            1
        """
        workflow = dict(argo.get("workflow") or {})
        machine_types = {
            name: MachineSpec(
                cpu=int(spec["cpu"]),
                mem=int(spec["mem"]),
                num_gpu=int(spec.get("num_gpu", 0)),
                emph_storage=int(spec.get("emph_storage", 10)),
            )
            for name, spec in argo["machine_types"].items()
        }
        default_machine_type = argo["default_machine_type"]
        # Vérification du type de machine par défaut
        if default_machine_type not in machine_types:
            raise ValueError(f"Default machine type '{default_machine_type}' is not defined")
        retries = dict(workflow.get("retries") or {})
        system_metrics = dict((parameters.get("tracking") or {}).get("SYSTEM_METRICS") or {})
        entry_points = tuple(
            EntryPointSettings(
                name=name,
                tag=str(spec["tag"]),
                minute=int(spec.get("minute", 0)),
                hour=int(spec.get("hour", 0)),
                weekly=bool(spec.get("weekly", False)),
            )
            for name, spec in (workflow.get("entry_points") or {}).items()
        )
        return cls(
            env=env,
            machine_types=machine_types,
            default_machine_type=default_machine_type,
            default_retries=int(retries.get("default", cls.default_retries)),
            node_retries={str(k): int(v) for k, v in (retries.get("nodes") or {}).items()},
            strict_dependencies={
                str(pattern): tuple(str(node) for node in nodes)
                for pattern, nodes in (workflow.get("strict_dependencies") or {}).items()
            },
            entry_points=entry_points,
            system_metrics_enabled=bool(system_metrics.get("ENABLED", cls.system_metrics_enabled)),
            system_metrics_sampling_seconds=int(
                system_metrics.get("SAMPLING_SECONDS", cls.system_metrics_sampling_seconds)
            ),
            weekly_day=int((parameters.get("runtime") or {}).get("WEEKLY_DAY", cls.weekly_day)),
        )


# Chargement de la configuration du rendu pour un environnement
def load_render_config(env: str) -> RenderConfig:
    """Load the rendering configuration of a Kedro environment.

    Args:
        env: Kedro environment (``cloud`` for production, ``demo`` for the acceptance runs).

    Returns:
        The rendering configuration.

    Examples:
        >>> load_render_config("cloud").env  # doctest: +SKIP
        'cloud'
    """
    from kedro_pipeline.config import load_config_section, load_parameters

    return RenderConfig.from_sections(load_config_section("argo", env), load_parameters(env), env)


# Environnements rendus pour le chart
def chart_environments(env: Optional[str] = None) -> Tuple[str, ...]:
    """Return the Kedro environments whose DAG is rendered for the Helm chart.

    Args:
        env: Kedro environment whose ``argo.yml`` is read (``base`` merged with it).

    Returns:
        The environments listed in ``workflow.chart_environments`` (``("cloud",)``
        when absent).

    Examples:
        >>> chart_environments()  # doctest: +SKIP
        ('cloud', 'demo')
    """
    from kedro_pipeline.config import load_config_section

    workflow = load_config_section("argo", env).get("workflow") or {}
    return tuple(str(name) for name in workflow.get("chart_environments") or ("cloud",))


__all__ = [
    "EntryPointSettings",
    "MachineSpec",
    "RenderConfig",
    "chart_environments",
    "load_render_config",
]
