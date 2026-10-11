"""Rendering of the production Argo manifests from the Kedro pipelines.

The Argo DAG of each entry point is computed by argo-kedro (``get_argo_dag``,
called in :func:`build_dag` only, the single place depending on its API), then
enriched from the tags of the nodes and the ``workflow`` block of ``argo.yml``:

* **experiment** — the ``experiment.<name>`` tag gives the MLflow experiment of
  the task (``MLFLOW_EXPERIMENT_NAME`` of its pod);
* **resources** — the ``machine_type`` of the node (requests = limits);
* **dependencies** — tolerant to an upstream failure (``(x.Succeeded ||
  x.Failed)``): every step reads committed data only and decides by its
  freshness registries what to recompute; a dependency is strict
  (``x.Succeeded``) only where configured (the BACI vintages need a valid
  scope). The dependencies towards nodes outside the entry point disappear:
  the DAG is computed on the filtered pipeline, the tables being read as they are;
* **retries** — on infrastructure error, by node name pattern (none for the
  downloads, whose retry would exceed their time budget);
* **mutex** — the ``mutex.<name>`` tag serialises the task across concurrent
  executions (the serving publication shared by both cadences);
* **exit handler** — the node tagged ``onexit`` (DuckLake maintenance) leaves
  the DAGs and runs as the ``onExit`` handler, whatever the outcome.

The output is deterministic (stable task order, no timestamp, no commit SHA),
so that ``kedro trade render-argo --check`` can compare it with the versioned
manifests.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
import difflib
from fnmatch import fnmatchcase
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

# Rendu des gabarits et contrôle du YAML produit
import jinja2
import yaml

# Kedro
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.deploy.config import EntryPointSettings, RenderConfig, load_render_config
from kedro_pipeline.pipelines._common import ON_EXIT, TAG_SEPARATOR

# Familles de tags lues par le rendu (forme « <famille>.<valeur> »)
EXPERIMENT_FAMILY = "experiment"
MUTEX_FAMILY = "mutex"
# Pipeline exécuté par chaque pod : la somme de tous les pipelines, filtrée par `--nodes`
POD_PIPELINE = "__default__"
# Template conteneur générique et préfixe des enveloppes sérialisées par un mutex
KEDRO_TEMPLATE = "kedro"
LOCKED_TEMPLATE_PREFIX = "locked-"
EXIT_TEMPLATE = "exit-handler"
# Fichiers produits
WORKFLOW_TEMPLATE_FILE = "workflowtemplate.yaml"
CRON_FILE_TEMPLATE = "cronworkflow-{entry_point}.yaml"
# Dossier des gabarits Jinja et dossier de sortie par défaut (relatif à la racine du dépôt)
TEMPLATES_DIR = Path(__file__).parent / "templates"
DEFAULT_OUTPUT_DIR = Path("kubernetes") / "generated"
# Version d'Argo Workflows à partir de laquelle un CronWorkflow accepte une liste de
# planifications (`schedules`) ; avant, une seule (`schedule`)
SCHEDULES_LIST_VERSION = (3, 6)


# Dépendance d'une tâche
@dataclass(frozen=True)
class Dependency:
    """Dependency of an Argo task on an upstream task.

    Attributes:
        task: Argo name of the upstream task.
        strict: Whether the upstream task must have succeeded; otherwise its
            failure is tolerated.

    Examples:
        >>> Dependency("prepare-baci", strict=True).expression
        'prepare-baci.Succeeded'
        >>> Dependency("download-eurostat", strict=False).expression
        '(download-eurostat.Succeeded || download-eurostat.Failed)'
    """

    task: str
    strict: bool

    @property
    def expression(self) -> str:
        """Argo ``depends`` expression of the dependency."""
        if self.strict:
            return f"{self.task}.Succeeded"
        return f"({self.task}.Succeeded || {self.task}.Failed)"


# Tâche Argo enrichie
@dataclass(frozen=True)
class TaskSpec:
    """Argo task rendered from one Kedro node (or fused pipeline).

    Attributes:
        name: Argo task name (node name with ``_`` turned into ``-``).
        node: Kedro node name, passed to ``--nodes``.
        experiment: MLflow experiment of the task.
        machine_type: Machine type name.
        cpu: CPU count of the pod.
        mem: Memory of the pod, in GiB.
        dependencies: Upstream tasks, sorted by name.
        retries: Retries on infrastructure error.
        mutex: Name of the mutex serialising the task, if any.

    Examples:
        >>> task = TaskSpec("publish-serving", "publish_serving", "trade-04-serving",
        ...                 "compute-medium", 4, 16, (Dependency("a", False),), 1, "trade-serving")
        >>> task.template, task.depends
        ('locked-trade-serving', '(a.Succeeded || a.Failed)')
    """

    name: str
    node: str
    experiment: str
    machine_type: str
    cpu: int
    mem: int
    dependencies: Tuple[Dependency, ...]
    retries: int
    mutex: Optional[str] = None

    @property
    def template(self) -> str:
        """Name of the Argo template running the task (a locked envelope for a mutex)."""
        return KEDRO_TEMPLATE if self.mutex is None else f"{LOCKED_TEMPLATE_PREFIX}{self.mutex}"

    @property
    def depends(self) -> str:
        """Argo ``depends`` expression of the task (empty without dependency)."""
        return " && ".join(dependency.expression for dependency in self.dependencies)


# Point d'entrée rendu
@dataclass(frozen=True)
class EntryPointSpec:
    """A scheduled entry point with its tasks and its cron expression.

    Attributes:
        settings: Settings of the entry point.
        tasks: Tasks of its DAG, in topological then alphabetical order.
        schedule: Cron expression of its CronWorkflow.

    Examples:
        >>> from kedro_pipeline.deploy.config import EntryPointSettings
        >>> settings = EntryPointSettings("daily", "cadence.daily", 0, 1, False, 82800, 3600, 7)
        >>> EntryPointSpec(settings, (), "0 1 * * *").settings.name
        'daily'
    """

    settings: EntryPointSettings
    tasks: Tuple[TaskSpec, ...]
    schedule: str


# Nom Argo d'un nœud
def argo_name(node_name: str) -> str:
    """Return the Argo name of a Kedro node (argo-kedro's cleaning rule).

    Every run of non-alphanumeric characters (``_`` included) becomes ``-``,
    leading and trailing dashes are removed.

    Args:
        node_name: Kedro node name.

    Returns:
        The Argo task name.

    Examples:
        >>> argo_name("process_baci_hs2017")
        'process-baci-hs2017'
    """
    from argo_kedro.argo import clean_name

    return clean_name(node_name)


# Valeurs d'une famille de tags
def tag_values(tags: Iterable[str], family: str) -> List[str]:
    """Return the values of the ``<family>.<value>`` tags, sorted.

    Args:
        tags: Tags of a node.
        family: Tag family (``experiment``, ``mutex``).

    Returns:
        The values of that family.

    Examples:
        >>> tag_values({"experiment.trade-02-baci", "cadence.weekly"}, "experiment")
        ['trade-02-baci']
    """
    prefix = f"{family}{TAG_SEPARATOR}"
    return sorted(tag[len(prefix):] for tag in tags if tag.startswith(prefix))


# Valeur unique d'une famille de tags
def _single_tag_value(node: Any, family: str, required: bool) -> Optional[str]:
    """Return the single value of a tag family on a node (``None`` if absent and optional)."""
    values = tag_values(node.tags, family)
    if len(values) > 1:
        raise ValueError(f"Node '{node.name}' has several '{family}' tags: {values}")
    if not values:
        if required:
            raise ValueError(f"Node '{node.name}' has no '{family}{TAG_SEPARATOR}<name>' tag")
        return None
    return values[0]


# Nombre de reprises d'un nœud
def node_retries(node_name: str, config: RenderConfig) -> int:
    """Return the retries on infrastructure error of a node (first matching pattern).

    Args:
        node_name: Kedro node name.
        config: Rendering configuration (``node_retries``, ``default_retries``).

    Returns:
        The number of retries.

    Examples:
        >>> from kedro_pipeline.deploy.config import MachineSpec
        >>> config = RenderConfig(env="cloud", namespace="ns", image="i", image_tag="t",
        ...     machine_types={"s": MachineSpec(1, 2)}, default_machine_type="s", name="p",
        ...     node_retries={"download_*": 0})
        >>> node_retries("download_comtrade", config), node_retries("prepare_baci", config)
        (0, 1)
    """
    for pattern, retries in config.node_retries.items():
        if fnmatchcase(node_name, pattern):
            return retries
    return config.default_retries


# Caractère strict d'une dépendance
def is_strict(node_name: str, upstream_name: str, config: RenderConfig) -> bool:
    """Tell whether a node requires its upstream node to have succeeded.

    Args:
        node_name: Kedro name of the downstream node.
        upstream_name: Kedro name of the upstream node.
        config: Rendering configuration (``strict_dependencies``).

    Returns:
        ``True`` for a strict dependency, ``False`` for a failure-tolerant one.

    Examples:
        >>> from kedro_pipeline.deploy.config import MachineSpec
        >>> config = RenderConfig(env="cloud", namespace="ns", image="i", image_tag="t",
        ...     machine_types={"s": MachineSpec(1, 2)}, default_machine_type="s", name="p",
        ...     strict_dependencies={"process_baci_*": ("prepare_baci",)})
        >>> is_strict("process_baci_hs2017", "prepare_baci", config)
        True
        >>> is_strict("compute_network_vulnerabilities", "process_baci_hs2017", config)
        False
    """
    return any(
        fnmatchcase(node_name, pattern) and upstream_name in upstreams
        for pattern, upstreams in config.strict_dependencies.items()
    )


# Tâche d'un nœud, hors dépendances
def _task_of(node: Any, config: RenderConfig, dependencies: Tuple[Dependency, ...]) -> TaskSpec:
    """Build the task of a node from its tags, machine type and the configuration."""
    machine_type = getattr(node, "machine_type", None) or config.default_machine_type
    if machine_type not in config.machine_types:
        raise KeyError(f"Machine type '{machine_type}' not found for node '{node.name}'")
    machine = config.machine_types[machine_type]
    return TaskSpec(
        name=argo_name(node.name),
        node=node.name,
        experiment=_single_tag_value(node, EXPERIMENT_FAMILY, required=True),
        machine_type=machine_type,
        cpu=machine.cpu,
        mem=machine.mem,
        dependencies=dependencies,
        retries=node_retries(node.name, config),
        mutex=_single_tag_value(node, MUTEX_FAMILY, required=False),
    )


# Construction du DAG d'un point d'entrée
def build_dag(pipeline: Pipeline, config: RenderConfig) -> List[TaskSpec]:
    """Compute the Argo tasks of a pipeline with argo-kedro, then enrich them.

    The single caller of argo-kedro's ``get_argo_dag``: should its API change,
    only this function adapts. A fused pipeline is one task. The parents are
    computed within ``pipeline`` only, so a dependency on a node of another
    entry point is dropped (its tables are read as they are). The node tagged
    ``onexit`` is left out (it becomes the exit handler, see :func:`exit_task`).

    Args:
        pipeline: Pipeline of one entry point (nodes filtered by a cadence tag).
        config: Rendering configuration.

    Returns:
        The tasks, sorted by topological level, then by name.

    Raises:
        KeyError: If a node names an undefined machine type.
        ValueError: If a node lacks an experiment tag, has several experiment
            or mutex tags, or if two nodes share an Argo name.

    Examples:
        >>> from argo_kedro.pipeline import Node
        >>> from kedro.pipeline import Pipeline
        >>> from kedro_pipeline.deploy.config import MachineSpec
        >>> config = RenderConfig(env="cloud", namespace="ns", image="i", image_tag="t",
        ...     machine_types={"s": MachineSpec(1, 2)}, default_machine_type="s", name="p")
        >>> pipe = Pipeline([Node(lambda: 1, None, "a", name="make_a", tags=["experiment.x"]),
        ...                  Node(lambda a: a, "a", "b", name="use_a", tags=["experiment.x"])])
        >>> [(task.name, task.depends) for task in build_dag(pipe, config)]
        [('make-a', ''), ('use-a', '(make-a.Succeeded || make-a.Failed)')]
    """
    # Dépendance à argo-kedro isolée ici : calcul du DAG (fusion, parents par datasets)
    from argo_kedro.argo import get_argo_dag
    from argo_kedro.config.kedro_argo_config import MachineType, TemplateConfig

    nodes = [node for node in pipeline.nodes if ON_EXIT not in node.tags]
    machine_types = {
        name: MachineType(mem=spec.mem, cpu=spec.cpu, num_gpu=spec.num_gpu, emph_storage=spec.emph_storage)
        for name, spec in config.machine_types.items()
    }
    argo_tasks = get_argo_dag(
        Pipeline(nodes),
        machine_types=machine_types,
        default_machine_type=config.default_machine_type,
        template_config=TemplateConfig(),
    )

    # Correspondance nom Argo → nom Kedro (dépendances strictes exprimées en noms Kedro)
    kedro_names: Dict[str, str] = {}
    for node_name in argo_tasks:
        name = argo_name(node_name)
        if name in kedro_names:
            raise ValueError(f"Nodes '{kedro_names[name]}' and '{node_name}' share the Argo name '{name}'")
        kedro_names[name] = node_name

    # Enrichissement, dans l'ordre topologique fourni par argo-kedro
    tasks: Dict[str, TaskSpec] = {}
    levels: Dict[str, int] = {}
    for node_name, argo_task in argo_tasks.items():
        parents = sorted(set(argo_task.deps))
        dependencies = tuple(
            Dependency(parent, is_strict(node_name, kedro_names[parent], config)) for parent in parents
        )
        task = _task_of(argo_task.node, config, dependencies)
        tasks[task.name] = task
        levels[task.name] = 1 + max((levels[parent] for parent in parents), default=-1)

    # Ordre stable : niveau topologique, puis nom
    return sorted(tasks.values(), key=lambda task: (levels[task.name], task.name))


# Tâche de fin de workflow
def exit_task(pipeline: Pipeline, config: RenderConfig) -> Optional[TaskSpec]:
    """Return the exit-handler task: the node tagged ``onexit``, if any.

    Args:
        pipeline: Pipeline holding every node (``__default__``).
        config: Rendering configuration.

    Returns:
        The exit task (no dependency), or ``None`` without ``onexit`` node.

    Raises:
        ValueError: If several nodes are tagged ``onexit``.

    Examples:
        >>> from argo_kedro.pipeline import Node
        >>> from kedro.pipeline import Pipeline
        >>> from kedro_pipeline.deploy.config import MachineSpec
        >>> config = RenderConfig(env="cloud", namespace="ns", image="i", image_tag="t",
        ...     machine_types={"s": MachineSpec(1, 2)}, default_machine_type="s", name="p")
        >>> pipe = Pipeline([Node(lambda: 1, None, "m", name="clean", tags=["onexit", "experiment.x"])])
        >>> exit_task(pipe, config).node
        'clean'
    """
    nodes = sorted((node for node in pipeline.nodes if ON_EXIT in node.tags), key=lambda node: node.name)
    if len(nodes) > 1:
        raise ValueError(f"Several nodes are tagged '{ON_EXIT}': {[node.name for node in nodes]}")
    return _task_of(nodes[0], config, ()) if nodes else None


# Expression cron d'un point d'entrée
def cron_schedule(minute: int, hour: int, weekly_day: Optional[int] = None) -> str:
    """Return the cron expression of a daily or weekly schedule.

    The weekly day follows the project convention (0 = Monday … 6 = Sunday,
    as Python's ``weekday()``); cron counts from Sunday (0 = Sunday … 6 =
    Saturday), hence the shift by one.

    Args:
        minute: Minute of the start.
        hour: Hour of the start.
        weekly_day: Day of the week (0 = Monday), ``None`` for a daily schedule.

    Returns:
        The cron expression.

    Raises:
        ValueError: If a value is out of range.

    Examples:
        >>> cron_schedule(0, 1)
        '0 1 * * *'
        >>> cron_schedule(0, 2, weekly_day=5)   # samedi
        '0 2 * * 6'
        >>> cron_schedule(30, 3, weekly_day=6)  # dimanche
        '30 3 * * 0'
    """
    # Vérification des bornes
    if not (0 <= minute < 60 and 0 <= hour < 24):
        raise ValueError(f"Invalid time {hour}:{minute}")
    if weekly_day is None:
        return f"{minute} {hour} * * *"
    if not 0 <= weekly_day <= 6:
        raise ValueError(f"Invalid weekly day {weekly_day} (0 = Monday … 6 = Sunday)")
    return f"{minute} {hour} * * {(weekly_day + 1) % 7}"


# Champ de planification selon la version d'Argo
def schedule_field(argo_version: str) -> str:
    """Return the CronWorkflow schedule field supported by an Argo version.

    Args:
        argo_version: Version of the Argo Workflows controller (``3.6.10``, ``v3.5.2``).

    Returns:
        ``"schedules"`` (list, Argo ≥ 3.6) or ``"schedule"`` (single string).

    Examples:
        >>> schedule_field("3.6.10"), schedule_field("v3.5.8")
        ('schedules', 'schedule')
    """
    major, minor = (int(part) for part in argo_version.lstrip("v").split(".")[:2])
    return "schedules" if (major, minor) >= SCHEDULES_LIST_VERSION else "schedule"


# Chaîne YAML sûre
def _quote(value: Any) -> str:
    """Quote a scalar as a YAML (JSON) string."""
    return json.dumps(str(value), ensure_ascii=False)


# Environnement Jinja des gabarits
def _jinja_environment(templates_dir: Path) -> jinja2.Environment:
    """Return the Jinja environment of the manifest templates.

    Délimiteurs en crochets : les accolades doubles appartiennent aux expressions
    Argo (`{{workflow.parameters.…}}`), recopiées telles quelles dans les manifestes.
    """
    environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(templates_dir)),
        block_start_string="[%",
        block_end_string="%]",
        variable_start_string="[[",
        variable_end_string="]]",
        comment_start_string="[#",
        comment_end_string="#]",
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
        undefined=jinja2.StrictUndefined,
        autoescape=False,
    )
    environment.filters["q"] = _quote
    return environment


# Points d'entrée d'un pipeline complet
def entry_point_specs(default: Pipeline, config: RenderConfig) -> List[EntryPointSpec]:
    """Build the DAG of every entry point from the complete pipeline.

    Each entry point runs the nodes of ``default`` carrying its cadence tag
    (no upstream node is added).

    Args:
        default: Complete pipeline (``__default__``).
        config: Rendering configuration.

    Returns:
        The entry points, in configuration order.

    Raises:
        ValueError: If there is no entry point, or one selects no task.
    """
    if not config.entry_points:
        raise ValueError("No entry point configured (argo.yml, workflow.entry_points)")
    specs = []
    for settings in config.entry_points:
        tasks = tuple(build_dag(default.only_nodes_with_tags(settings.tag), config))
        if not tasks:
            raise ValueError(f"Entry point '{settings.name}' selects no node (tag '{settings.tag}')")
        weekly_day = config.weekly_day if settings.weekly else None
        specs.append(EntryPointSpec(settings, tasks, cron_schedule(settings.minute, settings.hour, weekly_day)))
    return specs


# Rendu des manifestes d'un pipeline complet
def render_pipeline_manifests(
    default: Pipeline, config: RenderConfig, templates_dir: Path = TEMPLATES_DIR
) -> Dict[str, str]:
    """Render the WorkflowTemplate and the CronWorkflows of a complete pipeline.

    Args:
        default: Complete pipeline (``__default__``): every scheduled node and
            the exit node.
        config: Rendering configuration.
        templates_dir: Folder of the Jinja templates.

    Returns:
        The manifest texts by file name (``workflowtemplate.yaml``,
        ``cronworkflow-<entry point>.yaml``), each a valid YAML document.

    Raises:
        ValueError: If the DAG cannot be built or a rendered manifest is not valid YAML.
    """
    environment = _jinja_environment(templates_dir)
    entry_points = entry_point_specs(default, config)
    exit_spec = exit_task(default, config)
    # Mutex des tâches des DAG, une enveloppe sérialisée par nom
    mutexes = sorted({task.mutex for spec in entry_points for task in spec.tasks if task.mutex})
    context = {
        "config": config,
        "entry_points": entry_points,
        "exit": exit_spec,
        "mutexes": mutexes,
        "pod_pipeline": POD_PIPELINE,
        "kedro_template": KEDRO_TEMPLATE,
        "locked_prefix": LOCKED_TEMPLATE_PREFIX,
        "exit_template": EXIT_TEMPLATE,
        "schedule_field": schedule_field(config.argo_version),
        # Borne du template : la plus longue des durées maximales (chaque CronWorkflow
        # impose la sienne)
        "active_deadline_seconds": max(spec.settings.active_deadline_seconds for spec in entry_points),
    }
    manifests = {WORKFLOW_TEMPLATE_FILE: environment.get_template("workflowtemplate.yaml.j2").render(**context)}
    cron = environment.get_template("cronworkflow.yaml.j2")
    for spec in entry_points:
        manifests[CRON_FILE_TEMPLATE.format(entry_point=spec.settings.name)] = cron.render(entry=spec, **context)
    # Contrôle de validité : chaque manifeste est un document YAML
    for name, text in manifests.items():
        try:
            yaml.safe_load(text)
        except yaml.YAMLError as error:
            raise ValueError(f"Rendered manifest '{name}' is not valid YAML: {error}") from error
    return manifests


# Rendu des manifestes d'un environnement
def render_manifests(env: str = "cloud", image_tag: Optional[str] = None) -> Dict[str, str]:
    """Render the production manifests of a Kedro environment.

    The pipelines are those of ``env`` (the BACI fan-out follows its targets)
    and ``KEDRO_ENV`` is fixed to ``env`` in the pods.

    Args:
        env: Kedro environment (``cloud`` for production).
        image_tag: Default image tag; ``deployment.tag`` of ``argo.yml`` when ``None``.

    Returns:
        The manifest texts by file name.

    Examples:
        >>> sorted(render_manifests("cloud"))  # doctest: +SKIP
        ['cronworkflow-daily.yaml', 'cronworkflow-weekly.yaml', 'workflowtemplate.yaml']
    """
    from kedro_pipeline.config import load_parameters
    from kedro_pipeline.pipeline_registry import register_pipelines

    config = load_render_config(env, image_tag=image_tag)
    default = register_pipelines(load_parameters(env))[POD_PIPELINE]
    return render_pipeline_manifests(default, config)


# Écriture des manifestes
def write_manifests(manifests: Mapping[str, str], output_dir: Path) -> List[Path]:
    """Write the manifests (LF line endings, whatever the platform).

    Args:
        manifests: Manifest texts by file name.
        output_dir: Destination folder (created if needed).

    Returns:
        The written paths, sorted.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name in sorted(manifests):
        path = output_dir / name
        # Fins de ligne LF : fichiers identiques sous Windows et en CI (Linux)
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(manifests[name])
        paths.append(path)
    return paths


# Écarts entre les manifestes rendus et les fichiers existants
def diff_manifests(manifests: Mapping[str, str], output_dir: Path) -> Dict[str, str]:
    """Return the unified diff of every manifest differing from its file.

    Line endings are normalised before comparison. A missing file differs
    from any manifest.

    Args:
        manifests: Rendered manifest texts by file name.
        output_dir: Folder of the versioned manifests.

    Returns:
        The diff text by file name, for the differing files only.

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as folder:
        ...     _ = write_manifests({"a.yaml": "x: 1\\n"}, Path(folder))
        ...     sorted(diff_manifests({"a.yaml": "x: 1\\n", "b.yaml": "y: 2\\n"}, Path(folder)))
        ['b.yaml']
    """
    differences = {}
    for name in sorted(manifests):
        path = output_dir / name
        current = path.read_text(encoding="utf-8").replace("\r\n", "\n") if path.is_file() else ""
        expected = manifests[name].replace("\r\n", "\n")
        if current != expected:
            differences[name] = "".join(
                difflib.unified_diff(
                    current.splitlines(keepends=True),
                    expected.splitlines(keepends=True),
                    fromfile=f"{path.as_posix()} (versioned)" if path.is_file() else f"{path.as_posix()} (missing)",
                    tofile=f"{path.as_posix()} (rendered)",
                )
            )
    return differences
