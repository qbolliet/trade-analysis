"""Configuration of the Argo manifests rendering, read from ``argo.yml`` and the parameters.

The ``argo`` configuration section holds two parts: the one validated by
argo-kedro (namespace, image, machine types, runner) and the project's
``workflow`` block (service account, schedules, deadlines, retries, strict
dependencies, environment and secrets of the pods). Two values come from the
parameters instead, since the pipeline steps read them too: the weekly day
(``runtime.WEEKLY_DAY``) and the MLflow system metrics settings
(``tracking.SYSTEM_METRICS``).
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
        minute: Minute of the scheduled start.
        hour: Hour of the scheduled start, in the workflow timezone.
        weekly: Whether the schedule is weekly (day taken from ``runtime.WEEKLY_DAY``)
            rather than daily.
        active_deadline_seconds: Maximum duration of one execution.
        starting_deadline_seconds: Delay after which a missed schedule is skipped.
        history_limit: Successful and failed executions kept by the CronWorkflow.

    Examples:
        >>> EntryPointSettings("daily", "cadence.daily", 0, 1, False, 82800, 3600, 7).weekly
        False
    """

    name: str
    tag: str
    minute: int
    hour: int
    weekly: bool
    active_deadline_seconds: int
    starting_deadline_seconds: int
    history_limit: int


# Variable tirée d'une clé de secret
@dataclass(frozen=True)
class SecretKey:
    """Reference to one key of a Kubernetes secret.

    Attributes:
        secret: Secret name.
        key: Key inside the secret.

    Examples:
        >>> SecretKey("trade-s3-credentials", "S3_ACCESS_KEY").secret
        'trade-s3-credentials'
    """

    secret: str
    key: str


# Configuration complète du rendu
@dataclass(frozen=True)
class RenderConfig:
    """Everything the rendering needs, independent of the Kedro session.

    Attributes:
        env: Kedro environment the manifests are rendered for (fixed as ``KEDRO_ENV``
            in the pods: the BACI fan-out depends on the targets of this environment).
        namespace: Kubernetes namespace.
        image: Image repository.
        image_tag: Default of the ``image-tag`` workflow parameter.
        machine_types: Machine types by name.
        default_machine_type: Machine type of a node without one.
        name: Name of the ``WorkflowTemplate``.
        argo_version: Version of the deployed Argo Workflows controller.
        service_account: Service account of the pods.
        labels: Labels of the rendered resources.
        parallelism: Maximum simultaneous pods of one execution.
        ttl_seconds_after_success: Retention of a successful workflow.
        ttl_seconds_after_failure: Retention of a failed workflow.
        pod_gc: Pod garbage-collection strategy.
        image_pull_policy: Image pull policy of the pods.
        timezone: Timezone of the schedules.
        concurrency_policy: Concurrency policy of the CronWorkflows.
        run_mutex: Name of the mutex held by a whole run, so that runs of different
            entry points never overlap; ``None`` lets them overlap.
        default_retries: Retries on infrastructure error of a task.
        node_retries: Retries by node name pattern (``fnmatch``), first match wins.
        strict_dependencies: Upstream nodes a node (by name pattern) requires to
            have succeeded; any other dependency tolerates an upstream failure.
        entry_points: Entry points, in rendering order.
        environment: Plain environment variables of the pods.
        secret_environment: Environment variables read from a secret key.
        secret_environment_from: Secrets exposed whole (one variable per key).
        system_metrics_enabled: Whether MLflow logs the pod's system metrics.
        system_metrics_sampling_seconds: Sampling interval of the system metrics.
        weekly_day: Day of the weekly cadence (0 = Monday … 6 = Sunday).

    Examples:
        >>> config = RenderConfig(env="cloud", namespace="ns", image="img", image_tag="t",
        ...     machine_types={"small": MachineSpec(1, 2)}, default_machine_type="small",
        ...     name="pipeline")
        >>> config.parallelism
        8
    """

    env: str
    namespace: str
    image: str
    image_tag: str
    machine_types: Mapping[str, MachineSpec]
    default_machine_type: str
    name: str
    argo_version: str = "3.6.10"
    service_account: str = "default"
    labels: Mapping[str, str] = field(default_factory=dict)
    parallelism: int = 8
    ttl_seconds_after_success: int = 259200
    ttl_seconds_after_failure: int = 604800
    pod_gc: str = "OnPodSuccess"
    image_pull_policy: str = "IfNotPresent"
    timezone: str = "UTC"
    concurrency_policy: str = "Forbid"
    run_mutex: Optional[str] = None
    default_retries: int = 1
    node_retries: Mapping[str, int] = field(default_factory=dict)
    strict_dependencies: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)
    entry_points: Tuple[EntryPointSettings, ...] = ()
    environment: Mapping[str, str] = field(default_factory=dict)
    secret_environment: Mapping[str, SecretKey] = field(default_factory=dict)
    secret_environment_from: Tuple[str, ...] = ()
    system_metrics_enabled: bool = True
    system_metrics_sampling_seconds: int = 30
    weekly_day: int = 5

    # Construction depuis les sections de configuration
    @classmethod
    def from_sections(
        cls,
        argo: Mapping[str, Any],
        parameters: Mapping[str, Any],
        env: str,
        image_tag: Optional[str] = None,
    ) -> "RenderConfig":
        """Build the configuration from the ``argo`` section and the parameters.

        Args:
            argo: The ``argo`` configuration section (``argo.yml`` merged for ``env``).
            parameters: The project parameters (``runtime.WEEKLY_DAY``,
                ``tracking.SYSTEM_METRICS``).
            env: Kedro environment the manifests are rendered for.
            image_tag: Default image tag; ``deployment.tag`` of ``argo.yml`` when ``None``.

        Returns:
            The rendering configuration.

        Raises:
            KeyError: If a required key is missing.
            ValueError: If the default machine type is not defined.

        Examples:
            >>> argo = {"namespace": "ns", "deployment": {"image": "img", "tag": "t"},
            ...         "machine_types": {"small": {"cpu": 1, "mem": 2, "num_gpu": 0}},
            ...         "default_machine_type": "small", "workflow": {"name": "p"}}
            >>> RenderConfig.from_sections(argo, {"runtime": {"WEEKLY_DAY": 5}}, "cloud").image_tag
            't'
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
                hour=int(spec["hour"]),
                weekly=bool(spec.get("weekly", False)),
                active_deadline_seconds=int(spec["active_deadline_seconds"]),
                starting_deadline_seconds=int(spec["starting_deadline_seconds"]),
                history_limit=int(spec["history_limit"]),
            )
            for name, spec in (workflow.get("entry_points") or {}).items()
        )
        return cls(
            env=env,
            namespace=argo["namespace"],
            image=argo["deployment"]["image"],
            image_tag=str(image_tag if image_tag is not None else argo["deployment"].get("tag", "latest")),
            machine_types=machine_types,
            default_machine_type=default_machine_type,
            name=workflow["name"],
            argo_version=str(workflow.get("argo_version", cls.argo_version)),
            service_account=workflow.get("service_account", cls.service_account),
            labels={str(k): str(v) for k, v in (workflow.get("labels") or {}).items()},
            parallelism=int(workflow.get("parallelism", cls.parallelism)),
            ttl_seconds_after_success=int(
                workflow.get("ttl_seconds_after_success", cls.ttl_seconds_after_success)
            ),
            ttl_seconds_after_failure=int(
                workflow.get("ttl_seconds_after_failure", cls.ttl_seconds_after_failure)
            ),
            pod_gc=workflow.get("pod_gc", cls.pod_gc),
            image_pull_policy=workflow.get("image_pull_policy", cls.image_pull_policy),
            timezone=workflow.get("timezone", cls.timezone),
            concurrency_policy=workflow.get("concurrency_policy", cls.concurrency_policy),
            run_mutex=workflow.get("run_mutex", cls.run_mutex),
            default_retries=int(retries.get("default", cls.default_retries)),
            node_retries={str(k): int(v) for k, v in (retries.get("nodes") or {}).items()},
            strict_dependencies={
                str(pattern): tuple(str(node) for node in nodes)
                for pattern, nodes in (workflow.get("strict_dependencies") or {}).items()
            },
            entry_points=entry_points,
            # Valeurs converties en texte : une variable d'environnement est une chaîne
            environment={str(k): "" if v is None else str(v) for k, v in (workflow.get("environment") or {}).items()},
            secret_environment={
                str(name): SecretKey(secret=str(ref["secret"]), key=str(ref["key"]))
                for name, ref in (workflow.get("secret_environment") or {}).items()
            },
            secret_environment_from=tuple(str(s) for s in workflow.get("secret_environment_from") or ()),
            system_metrics_enabled=bool(system_metrics.get("ENABLED", cls.system_metrics_enabled)),
            system_metrics_sampling_seconds=int(
                system_metrics.get("SAMPLING_SECONDS", cls.system_metrics_sampling_seconds)
            ),
            weekly_day=int((parameters.get("runtime") or {}).get("WEEKLY_DAY", cls.weekly_day)),
        )


# Chargement de la configuration du rendu pour un environnement
def load_render_config(env: str, image_tag: Optional[str] = None) -> RenderConfig:
    """Load the rendering configuration of a Kedro environment.

    Args:
        env: Kedro environment (``cloud`` for the production manifests).
        image_tag: Default image tag; ``deployment.tag`` of ``argo.yml`` when ``None``.

    Returns:
        The rendering configuration.

    Examples:
        >>> load_render_config("cloud").name  # doctest: +SKIP
        'trade-pipeline'
    """
    from kedro_pipeline.config import load_config_section, load_parameters

    return RenderConfig.from_sections(
        load_config_section("argo", env), load_parameters(env), env, image_tag=image_tag
    )


__all__ = [
    "EntryPointSettings",
    "MachineSpec",
    "RenderConfig",
    "SecretKey",
    "load_render_config",
]
