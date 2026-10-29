"""Rendering of the Argo DAG data of the ``trade-pipeline`` Helm chart.

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

Helm cannot compute this DAG, and Kedro should not know the deployment: the
rendering writes one data file per Kedro environment
(``helm/trade-pipeline/files/dag-<env>.yaml``), which the chart templates turn
into a ``WorkflowTemplate`` and its ``CronWorkflow`` resources, adding the
deployment settings (image, namespace, service account, schedules, deadlines,
secrets). The output is deterministic (stable task order, no timestamp, no
commit SHA), so that ``kedro trade render-argo --check`` can compare it with
the versioned files.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
import difflib
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# Sérialisation des fichiers de données
import yaml

# Kedro
from kedro.pipeline import Pipeline

# Modules du package
from kedro_pipeline.deploy.config import RenderConfig, chart_environments, load_render_config
from kedro_pipeline.pipelines._common import ON_EXIT, TAG_SEPARATOR

# Familles de tags lues par le rendu (forme « <famille>.<valeur> »)
EXPERIMENT_FAMILY = "experiment"
MUTEX_FAMILY = "mutex"
# Pipeline exécuté par chaque pod : la somme de tous les pipelines, filtrée par `--nodes`
POD_PIPELINE = "__default__"
# Template conteneur générique et préfixe des enveloppes sérialisées par un mutex (noms
# repris par les gabarits du chart)
KEDRO_TEMPLATE = "kedro"
LOCKED_TEMPLATE_PREFIX = "locked-"
# Fichier de données d'un environnement Kedro
DAG_FILE_TEMPLATE = "dag-{env}.yaml"
# Dossier de sortie par défaut (relatif à la racine du dépôt) : fichiers du chart
DEFAULT_OUTPUT_DIR = Path("helm") / "trade-pipeline" / "files"
# Version du format des fichiers de données, vérifiée par le chart
DAG_FORMAT_VERSION = 1


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
        >>> settings = EntryPointSettings("daily", "cadence.daily", 0, 1, False)
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
        >>> config = RenderConfig(env="cloud", machine_types={"s": MachineSpec(1, 2)},
        ...     default_machine_type="s", node_retries={"download_*": 0})
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
        >>> config = RenderConfig(env="cloud", machine_types={"s": MachineSpec(1, 2)},
        ...     default_machine_type="s", strict_dependencies={"process_baci_*": ("prepare_baci",)})
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
        >>> config = RenderConfig(env="cloud", machine_types={"s": MachineSpec(1, 2)},
        ...     default_machine_type="s")
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
        >>> config = RenderConfig(env="cloud", machine_types={"s": MachineSpec(1, 2)},
        ...     default_machine_type="s")
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


# Données d'une tâche
def task_data(task: TaskSpec) -> Dict[str, Any]:
    """Return the data of a task, as read by the chart templates.

    Args:
        task: Rendered task.

    Returns:
        The task fields; ``depends`` and ``mutex`` only when set.

    Examples:
        >>> task = TaskSpec("publish-serving", "publish_serving", "trade-04-serving",
        ...                 "compute-medium", 4, 16, (), 1, "trade-serving")
        >>> task_data(task)["mutex"], "depends" in task_data(task)
        ('trade-serving', False)
    """
    data: Dict[str, Any] = {
        "name": task.name,
        "node": task.node,
        "experiment": task.experiment,
        "cpu": task.cpu,
        "mem": task.mem,
        "retries": task.retries,
    }
    if task.depends:
        data["depends"] = task.depends
    if task.mutex:
        data["mutex"] = task.mutex
    return data


# Données du DAG d'un pipeline complet
def render_dag_data(default: Pipeline, config: RenderConfig) -> Dict[str, Any]:
    """Compute the DAG data of a complete pipeline, as read by the chart templates.

    Args:
        default: Complete pipeline (``__default__``): every scheduled node and
            the exit node.
        config: Rendering configuration.

    Returns:
        The data: format version, Kedro environment, pipeline run by the pods,
        MLflow system metrics settings, entry points (name, tag, default cron,
        tasks), mutexes of the tasks and exit task.

    Raises:
        ValueError: If the DAG cannot be built.
    """
    entry_points = entry_point_specs(default, config)
    exit_spec = exit_task(default, config)
    return {
        "formatVersion": DAG_FORMAT_VERSION,
        "kedroEnv": config.env,
        "podPipeline": POD_PIPELINE,
        "systemMetrics": {
            "enabled": config.system_metrics_enabled,
            "samplingSeconds": config.system_metrics_sampling_seconds,
        },
        "entryPoints": [
            {
                "name": spec.settings.name,
                "tag": spec.settings.tag,
                "cron": spec.schedule,
                "tasks": [task_data(task) for task in spec.tasks],
            }
            for spec in entry_points
        ],
        # Mutex des tâches des DAG, une enveloppe sérialisée par nom
        "mutexes": sorted({task.mutex for spec in entry_points for task in spec.tasks if task.mutex}),
        "exit": task_data(exit_spec) if exit_spec else None,
    }


# Texte d'un fichier de données
def dump_dag_data(data: Mapping[str, Any]) -> str:
    """Serialise DAG data as a commented, deterministic YAML text.

    Args:
        data: DAG data (:func:`render_dag_data`).

    Returns:
        The YAML text, preceded by a header naming its origin; one line per task.

    Raises:
        ValueError: If the text does not read back as the same data.

    Examples:
        >>> text = dump_dag_data({"kedroEnv": "cloud", "entryPoints": []})
        >>> text.splitlines()[0].startswith("# Fichier généré")
        True
    """
    header = (
        f"# Fichier généré par `kedro trade render-argo` (environnement {data['kedroEnv']}) : "
        "ne pas modifier à la main.\n"
        "# Source : pipelines Kedro (tags des nœuds) et config/base/argo.yml (bloc `workflow`).\n"
        "# Lu par les gabarits du chart trade-pipeline (WorkflowTemplate, CronWorkflow).\n"
    )
    # Collections terminales en style compact (une tâche par ligne), sans repli de ligne
    body = yaml.safe_dump(dict(data), sort_keys=False, default_flow_style=None, width=100_000, allow_unicode=True)
    text = header + body
    # Contrôle de relecture : le texte se relit en données identiques
    if yaml.safe_load(text) != dict(data):
        raise ValueError("DAG data does not read back identically")
    return text


# Rendu des fichiers d'un pipeline complet
def render_chart_files(default: Pipeline, config: RenderConfig) -> Dict[str, str]:
    """Render the DAG data file of a complete pipeline.

    Args:
        default: Complete pipeline (``__default__``).
        config: Rendering configuration.

    Returns:
        The file text by file name (``dag-<env>.yaml``).
    """
    return {DAG_FILE_TEMPLATE.format(env=config.env): dump_dag_data(render_dag_data(default, config))}


# Rendu des fichiers des environnements
def render_files(environments: Optional[Sequence[str]] = None) -> Dict[str, str]:
    """Render the DAG data files of the chart, one per Kedro environment.

    The pipelines are those of each environment (the BACI fan-out follows its
    targets).

    Args:
        environments: Kedro environments; ``workflow.chart_environments`` of
            ``argo.yml`` when ``None``.

    Returns:
        The file texts by file name.

    Examples:
        >>> sorted(render_files())  # doctest: +SKIP
        ['dag-cloud.yaml', 'dag-demo.yaml']
    """
    from kedro_pipeline.config import load_parameters
    from kedro_pipeline.pipeline_registry import register_pipelines

    files: Dict[str, str] = {}
    for env in environments or chart_environments():
        config = load_render_config(env)
        default = register_pipelines(load_parameters(env))[POD_PIPELINE]
        files.update(render_chart_files(default, config))
    return files


# Écriture des fichiers
def write_files(files: Mapping[str, str], output_dir: Path) -> List[Path]:
    """Write the files (LF line endings, whatever the platform).

    Args:
        files: File texts by file name.
        output_dir: Destination folder (created if needed).

    Returns:
        The written paths, sorted.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name in sorted(files):
        path = output_dir / name
        # Fins de ligne LF : fichiers identiques sous Windows et en CI (Linux)
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(files[name])
        paths.append(path)
    return paths


# Écarts entre les fichiers rendus et les fichiers existants
def diff_files(files: Mapping[str, str], output_dir: Path) -> Dict[str, str]:
    """Return the unified diff of every file differing from its versioned copy.

    Line endings are normalised before comparison. A missing file differs
    from any rendering.

    Args:
        files: Rendered file texts by file name.
        output_dir: Folder of the versioned files.

    Returns:
        The diff text by file name, for the differing files only.

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as folder:
        ...     _ = write_files({"a.yaml": "x: 1\\n"}, Path(folder))
        ...     sorted(diff_files({"a.yaml": "x: 1\\n", "b.yaml": "y: 2\\n"}, Path(folder)))
        ['b.yaml']
    """
    differences = {}
    for name in sorted(files):
        path = output_dir / name
        current = path.read_text(encoding="utf-8").replace("\r\n", "\n") if path.is_file() else ""
        expected = files[name].replace("\r\n", "\n")
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
