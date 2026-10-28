"""Network-metrics step: vulnerability of the world trade graph of each product.

Recomputes, for every HS vintage of the BACI reconstruction, the metrics of the
world graph of a product's trade — centrality risk, weighted clustering,
diameter, concentration of world exports and single point of failure risk (see
``tradestat.trade.vulnerabilities.network_metrics``). The output cell is a
quadruplet ``nomenclature x product x year x flow``, the vintage being an extra
primary-key column the partner table does not carry.

Every direction of ``NETWORK_VULNERABILITIES.FLOWS`` is computed on the same
BACI matrix: as is for the import (concentration of world supply), transposed
for the export (concentration of world demand). The ``flow`` column carries the
codes of the partner table, so that the synthesis joins on an equality.
Fingerprints are kept per metric and per direction (``SPOF/export``): adding a
direction only recomputes that direction. Run metrics are prefixed by direction
(``network/import/...``).

The stale vintages are decided by the fragmented registry of this step (one
fragment per vintage), confronted with the BACI registry (read only): a vintage
is recomputed when it never was, when its last completed BACI pass is more
recent than its last computation, when a metric fingerprint changed, or when it
is forced. A vintage whose BACI pass is interrupted is not scored (its table
mixes two estimations). The unit is the whole vintage, not the year: a BACI pass
re-estimates gravity and reporter quality over its whole time span, so every
year moves together.

The vintages are computed in worker processes (read and compute only) and
written one after the other by the parent process, each in its own tracked
run; the failure of one vintage does not stop the others and only the
successful ones see their date advance. Every row carries ``in_force``: true
when the vintage of the row is the one in force in its year.

No environment variable and no YAML path are read here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
from datetime import datetime
import logging
import time
from functools import partial
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Modules de manipulation de données
import narwhals as nw

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Poignées DuckLake
from kedro_pipeline.io.ducklake import DuckLakeTable, attached_catalog_alias
# Révision méthodologique du verrou (recalcul après correction d'une formule)
from kedro_pipeline.methodology import revised
# Parallélisme intra-pod (carte parallèle, erreurs transmissibles)
from kedro_pipeline.parallel import parallel_map, serialisable_exception
# Module d'utilitaires de téléchargement
from statflows.core.download import _now
# Registres de fraîcheur v2 (fragments, empreintes, forçage)
from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    RegistryEntry,
    Unit,
    UnitPlan,
    fingerprint,
    plan_metrics,
    qualifiers_to_compute,
    units_to_compute,
)
# Complétude d'une passe BACI (amont, lecture seule)
from kedro_pipeline.steps.baci import pass_is_complete
# Millésime SH en vigueur une année donnée
from kedro_pipeline.config import vintage_in_force
# Fabriques de configuration
from kedro_pipeline.steps._config import _BACKEND_KEY, load_flows, network_config_from_params
# Résultat d'étape et runs d'unités
from kedro_pipeline.steps.result import (
    StepResult,
    UnitRuns,
    capturing,
    failure_message,
    shared_runs,
)

# Module de suivi d'exécution
from kedro_pipeline.io.tracking import flow_run_metrics
from tradestat.tracking import RecordingTracker
# Module de calcul des indicateurs
from tradestat.trade.vulnerabilities import (
    DEFAULT_NETWORK_METRIC_CLASSES,
    NetworkVulnerabilityConfig,
)
from tradestat.trade.vulnerabilities.runner import (
    compute_network_vintage,
    read_previous_network_result,
    write_network_vintage,
)

# Initialisation du logger
logger = logging.getLogger(__name__)

# Bloc de configuration dédié aux indicateurs de réseau
_CONFIG_ROOT = "NETWORK_VULNERABILITIES"

# Préfixe des métriques MLflow de l'étape (suivi du sens : network/import/...)
_METRICS_FAMILY = "network"

# Colonne du drapeau « millésime en vigueur l'année de la ligne » (fait de schéma,
# lu par la couche de service)
_IN_FORCE_COL = "in_force"

# Préfixe du nœud du rapport de run : un run par millésime, nœud « <NODE>_<millésime> »
NODE = "compute_network_vulnerabilities"



# Nom de l'étape (forçage FORCE_STEPS, champ « step » des fragments)
STEP = "network"

# Fonction de calcul des empreintes demandées des métriques de réseau
def network_requested(
    config: NetworkVulnerabilityConfig,
    flows: Sequence[str] = ("import",),
    metric_classes: Sequence[type] = DEFAULT_NETWORK_METRIC_CLASSES,
) -> Dict[str, str]:
    """Current methodological fingerprint of every network metric and direction.

    Keyed by ``"<metric>/<flow>"``, like the partner step: two instances share
    a column name, and adding a direction must only make that direction stale.
    The orientation-invariant metrics (clustering, diameter) also get one key
    per direction, their value being written on the rows of each.

    Args:
        config: Methodological configuration of the network metrics.
        flows: Directions computed by the step.
        metric_classes: Metric classes computed by the step (each in the
            directions it supports).

    Returns:
        Mapping ``"<metric>/<flow>" -> fingerprint`` (name, result-shaping
        configuration fields and direction).

    Examples:
        >>> "SPOF/import" in network_requested(NetworkVulnerabilityConfig())
        True
        >>> "SPOF/export" in network_requested(NetworkVulnerabilityConfig(), ("import", "export"))
        True
    """
    metrics = [
        cls(config, flow=flow)
        for flow in flows
        for cls in metric_classes
        if flow in cls.supported_flows
    ]
    return {
        metric.fingerprint_key: fingerprint(
            metric.name, revised(metric.name, metric.fingerprint_params())
        )
        for metric in metrics
    }

# Fonction de construction du registre de fraîcheur du réseau
def network_registry(
    network_config: Mapping[str, Any],
    *,
    loader: Optional[Loader] = None,
    saver: Optional[Saver] = None,
) -> FreshnessRegistry:
    """Build the network freshness registry (one fragment per vintage).

    Args:
        network_config: ``NETWORK_VULNERABILITIES`` block of
            the ``vulnerabilities`` parameters (``BUCKET``, ``STATE``).
        loader: JSON loader (a fresh one by default).
        saver: JSON saver (a fresh one by default).

    Returns:
        The registry.

    Raises:
        KeyError: If the block has no ``STATE.PATH_TEMPLATE``.
    """
    return FreshnessRegistry(
        network_config["STATE"]["PATH_TEMPLATE"],
        network_config.get("BUCKET"),
        STEP,
        shard_of=lambda unit: unit.get("vintage"),
        loader=loader,
        saver=saver,
    )

# Fonction de construction des unités réseau et de leur watermark amont
def network_upstream(
    baci: FreshnessRegistry,
    targets: Mapping[str, str],
) -> Dict[Unit, datetime]:
    """Vintages that can be scored, with the last BACI computation as watermark.

    A vintage is left out (with a warning) when BACI never produced it, or
    when its last BACI pass is incomplete (``years_written`` differs from
    ``years_scope``): its table then mixes two estimation passes and must not
    be scored before the pass is resumed.

    Args:
        baci: BACI freshness registry (read only).
        targets: Configured vintages, label -> BACI result schema.

    Returns:
        Mapping ``Unit(vintage) -> last BACI computation``.
    """
    units: Dict[Unit, datetime] = {}
    skipped: List[str] = []
    for label in targets:
        entry = baci.get(Unit.of(vintage=label))
        if entry is None or entry.last_computed is None or not pass_is_complete(entry):
            skipped.append(label)
            continue
        units[Unit.of(vintage=label)] = entry.last_computed
    if skipped:
        # Logging
        logger.warning(
            f"Millésime(s) sans passe BACI terminée, ignoré(s) : {sorted(skipped)}"
        )
    return units

# Fonction de décision des millésimes à (re)calculer
def plan_network_units(
    registry: FreshnessRegistry,
    units: Mapping[Unit, datetime],
    requested: Mapping[str, str],
    force: ForceSpec,
) -> Dict[Unit, UnitPlan]:
    """Decide which vintages to (re)score.

    Args:
        registry: Network freshness registry.
        units: Scorable vintages and their last BACI computation
            (:func:`network_upstream`).
        requested: Current metric fingerprints (:func:`network_requested`).
        force: One-off forcing.

    Returns:
        Mapping ``unit -> plan``; every metric of a planned vintage is
        recomputed.
    """
    return units_to_compute(units, registry, units, requested, force, step=STEP)

# Tâche de calcul d'un millésime, envoyée à un worker
@dataclass(frozen=True)
class VintageTask:
    """Everything a worker needs to score one HS vintage.

    The worker opens the connections of its own through the readers and never
    writes: the parent process is the only writer.

    Attributes:
        label: HS vintage label (``"HS2017"``).
        source_schema: BACI schema holding the reconciled flows of the vintage.
        source_catalog_alias: Alias of the BACI catalog.
        source_reader: Opens the connection on the BACI catalog.
        result_reader: Opens the connection on the result catalog (previous
            result read for the drift); ``None`` when the drift is not measured.
        result_catalog_alias: Alias of the result catalog.
        result_schema: Schema of the network scores.
        config: Network methodological configuration.
        flows: Directions to recompute.
        backend: Narwhals native backend.
        log_artifacts: Whether the artifacts are recorded for the parent.
        nomenclatures: Vintage label -> entry-into-force year (``in_force`` flag).
    """
    label: str
    source_schema: str
    source_catalog_alias: str
    source_reader: Any
    result_reader: Any
    result_catalog_alias: str
    result_schema: str
    config: NetworkVulnerabilityConfig
    flows: Tuple[str, ...]
    backend: str
    log_artifacts: bool
    nomenclatures: Mapping[str, int]

# Résultat du calcul d'un millésime, renvoyé au processus parent
@dataclass
class VintageOutcome:
    """Outcome of one :class:`VintageTask`, picklable.

    Attributes:
        label: HS vintage label.
        result: Scores as a native frame (``None`` on failure).
        report: Network report of the vintage.
        recorded: Tracker calls recorded by the worker.
        cpu_seconds: CPU time spent by the task.
        wall_seconds: Elapsed time of the task.
        error: Serialisable exception of a failed vintage.
    """
    label: str
    result: Any = None
    report: Any = None
    recorded: Optional[RecordingTracker] = None
    cpu_seconds: float = 0.0
    wall_seconds: float = 0.0
    error: Optional[BaseException] = None

# Fonction de calcul d'un millésime dans un worker (lecture et calcul, aucune écriture)
def compute_vintage_task(task: VintageTask) -> VintageOutcome:
    """Read, score and annotate one vintage with connections of its own.

    Module-level function so that it can be sent to a worker process. The
    failure is returned, not raised: one vintage must not stop the others.

    Args:
        task: Vintage to compute.

    Returns:
        The outcome of the task.
    """
    started_cpu, started_wall = time.process_time(), time.perf_counter()
    outcome = VintageOutcome(label=task.label)
    try:
        recorder = RecordingTracker()
        with task.source_reader() as source_conn:
            # Résultat précédent du millésime pour la dérive : son absence ou la
            # désactivation de la mesure la neutralise
            df_previous = None
            if task.result_reader is not None:
                with task.result_reader() as result_conn:
                    df_previous = read_previous_network_result(
                        result_conn, task.result_catalog_alias, task.result_schema,
                        classification=task.label, config=task.config,
                    )
            result, report = compute_network_vintage(
                source_conn,
                source_catalog_alias=task.source_catalog_alias,
                source_schema=task.source_schema,
                classification=task.label,
                result_schema=task.result_schema,
                config=task.config,
                flows=task.flows,
                backend=task.backend,
                tracker=recorder,
                log_artifacts=task.log_artifacts,
                df_previous=df_previous,
                annotate=partial(
                    annotate_network_in_force, nomenclatures=task.nomenclatures,
                    config=task.config,
                ),
            )
        outcome.result = result.to_native()
        outcome.report = report
        outcome.recorded = recorder
    except Exception as exc:
        logger.exception(
            f"Échec du calcul des vulnérabilités de réseau pour le millésime {task.label}"
        )
        outcome.error = serialisable_exception(exc)
    outcome.cpu_seconds = time.process_time() - started_cpu
    outcome.wall_seconds = time.perf_counter() - started_wall
    return outcome

# Fonction d'ajout du drapeau « millésime en vigueur » aux scores de réseau
def annotate_network_in_force(
    result: nw.DataFrame,
    *,
    nomenclatures: Mapping[str, int],
    config: NetworkVulnerabilityConfig,
) -> nw.DataFrame:
    """Add ``in_force``: whether the row's vintage is the one in force its year.

    Args:
        result: Network scores of one vintage (classification and year columns).
        nomenclatures: Mapping vintage label -> entry-into-force year.
        config: Column conventions (``classification_col``, ``period_col``).

    Returns:
        The scores with a boolean ``in_force`` column.

    Examples:
        >>> import pandas as pd
        >>> frame = nw.from_native(pd.DataFrame({"classification": "HS2017", "year": [2019, 2023]}),
        ...                        eager_only=True)
        >>> annotate_network_in_force(frame, nomenclatures={"HS2017": 2017, "HS2022": 2022},
        ...                           config=NetworkVulnerabilityConfig()).to_native()["in_force"].tolist()
        [True, False]
    """
    years = result.get_column(config.period_col).cast(nw.Int64)
    mapping = {year: vintage_in_force(year, nomenclatures) for year in years.unique().to_list()}
    in_force_vintage = years.replace_strict(mapping, return_dtype=nw.String)
    return result.with_columns(
        (nw.col(config.classification_col) == in_force_vintage).alias(_IN_FORCE_COL)
    )



# ──────────────────────────────────────────────────────────────────────
# Fonction d'étape
# ──────────────────────────────────────────────────────────────────────

# Fonction d'étape : métriques de réseau, un run par millésime
def run_network_vulnerabilities(
    baci: Mapping[str, Any],
    result: Any,
    state: FreshnessRegistry,
    baci_state: FreshnessRegistry,
    *,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    dataflow: str,
    runs: Optional[UnitRuns] = None,
    n_jobs: int = 1,
    force: Optional[ForceSpec] = None,
    run_id: Optional[str] = None,
) -> StepResult:
    """Score the stale BACI vintages, one tracked run per vintage.

    Args:
        baci: BACI result table of every configured vintage, keyed by label
            (:class:`~kedro_pipeline.io.ducklake.DuckLakeTable`; a lazy handle
            gives each worker a connection of its own).
        result: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            network metrics (``network_indicators``).
        state: Network freshness registry (one fragment per vintage).
        baci_state: BACI freshness registry, read only (upstream watermark and
            completeness of the last pass).
        params: The ``vulnerabilities`` parameters (``NETWORK_VULNERABILITIES``).
        runtime: The ``runtime`` parameters (``NOMENCLATURES.HS``).
        dataflow: Comtrade dataflow of the reconciled flows (tag of the runs).
        runs: Factory of the run of each vintage (tag ``vintage``); every
            vintage in a null run when ``None``.
        n_jobs: Worker processes computing the vintages (resolved by the caller).
        force: One-off forcing; read from ``runtime`` alone when ``None``.
        run_id: Run identifier recorded on the DuckLake snapshots.

    Returns:
        The step result: one unit per stale vintage, ``children`` holding the
        result of every scored vintage; ``failures`` raised as
        ``RuntimeError("<n> millésime(s) en échec sur <m> : [...]")`` chained
        to the first error.
    """
    runs = runs if runs is not None else shared_runs()
    network_config = params[_CONFIG_ROOT]
    parameters = network_config.get("PARAMETERS") or {}
    network_parameters = network_config_from_params(parameters)
    backend = parameters.get(_BACKEND_KEY, "pandas")
    flows = load_flows(network_config)
    tracking = network_config.get("TRACKING") or {}
    log_artifacts = bool(tracking.get("LOG_ARTIFACTS", True))
    measure_drift = bool(tracking.get("DRIFT", True))
    force = force if force is not None else ForceSpec.from_runtime(runtime, environ={})

    # Unités candidates : millésimes dont la dernière passe BACI est terminée, avec
    # son dernier calcul comme watermark amont (lecture seule)
    targets = {label: table.schema for label, table in baci.items()}
    units = network_upstream(baci_state, targets)
    requested = network_requested(network_parameters, flows)
    plans = plan_network_units(state, units, requested, force)
    stale = sorted((unit.get("vintage"), targets[unit.get("vintage")]) for unit in plans)
    # Logging
    logger.info(
        f"{len(stale)} millésime(s) à recalculer : "
        f"{ {unit.get('vintage'): plan.reason for unit, plan in plans.items()} }"
    )
    if not stale:
        logger.info("Nothing to recompute, stop.")
        return StepResult(step="network", reportable=False)

    # Instant de référence capturé avant le calcul : la date enregistrée correspond au
    # début du traitement, pour ne pas rater une mise à jour survenue pendant le calcul
    computed_at = _now()
    result_schema = result.schema
    result_alias = attached_catalog_alias(result)
    # Tâches des workers : chacun ouvre ses propres connexions (jamais partagées)
    result_reader = result.reader() if measure_drift else None
    tasks = [
        VintageTask(
            label=label, source_schema=source_schema,
            source_catalog_alias=attached_catalog_alias(baci[label]),
            source_reader=baci[label].reader(),
            result_reader=result_reader,
            result_catalog_alias=result_alias,
            result_schema=result_schema, config=network_parameters,
            flows=tuple(qualifiers_to_compute({Unit.of(vintage=label): plans[Unit.of(vintage=label)]}, flows)),
            backend=backend, log_artifacts=log_artifacts,
            nomenclatures=runtime["NOMENCLATURES"]["HS"],
        )
        for label, source_schema in stale
    ]
    schemas = dict(stale)

    # Connexion d'écriture : le processus parent est le seul écrivain
    children: Dict[str, StepResult] = {}
    errors: Dict[str, BaseException] = {}
    with result.connect() as result_conn:
        # Un millésime après l'autre à l'arrivée : l'échec de l'un n'emporte pas les autres
        for _, outcome in parallel_map(compute_vintage_task, tasks, n_jobs):
            label = outcome.label
            try:
                unit = Unit.of(vintage=label)
                plan = plans[unit]
                source_schema = schemas[label]
                with runs(label, {"vintage": label}) as run:
                    tracker = capturing(run.tracker)
                    # Échec du worker : relevé dans le run du millésime puis propagé
                    if outcome.error is not None:
                        raise outcome.error
                    # Fraîcheur : décision du millésime et tag de forçage
                    tracker.log_metrics(plan_metrics({unit: plan}, n_candidates=len(units)))
                    tracker.set_tags({"freshness_reason": plan.reason})
                    if force.forces_step(STEP, requested):
                        tracker.set_tags({"forced": force.describe()})
                    # Paramètres et artefacts enregistrés par le worker
                    outcome.recorded.replay(tracker)

                    # Écriture du millésime et de l'issue sur ses rapports
                    report = outcome.report
                    write_network_vintage(
                        nw.from_native(outcome.result, eager_only=True),
                        report,
                        classification=label,
                        config=network_parameters,
                        result_conn=result_conn,
                        result_catalog_alias=result_alias,
                        result_schema=result_schema,
                        writer=DuckLakeTable(
                            result_conn, result_alias, result_schema, label=label,
                        ).writer(run_id=run_id, commit_message=f"{NODE} {label}"),
                    )

                    # Métriques préfixées par sens (le préfixe appartient à l'appelant) ;
                    # les paramètres sont journalisés par le calcul lui-même
                    tracker.log_metrics(flow_run_metrics(report, _METRICS_FAMILY))
                    tracker.log_metrics(
                        {
                            "timing/wall_seconds": outcome.wall_seconds,
                            "timing/cpu_seconds_sum": outcome.cpu_seconds,
                            "parallel/n_jobs": float(n_jobs),
                        }
                    )
                    tracker.set_tags(
                        {
                            "dataflow": dataflow,
                            "source_schema": source_schema,
                            "result_schema": result_schema,
                            "created": str(report.created),
                            "n_cells": str(report.cells),
                            "flows": ",".join(report.flows),
                        }
                    )
                    child = StepResult(
                        step="network",
                        n_units_planned=1,
                        n_units_succeeded=1,
                        metrics=dict(tracker.metrics),
                        artifacts=dict(tracker.tables),
                        tags=dict(tracker.tags),
                        units_label=f"1 millésime ({label})",
                        outputs={"report": report},
                    )
                    # Rapport de run du millésime, publié avant toute sortie en erreur
                    run.publish(child)

                # Entrée de registre écrite après succès du calcul et de l'écriture
                # seulement (jamais de date avancée à tort) ; la raison est conservée
                # pour la cascade vers la synthèse
                state.upsert(
                    RegistryEntry(
                        unit=unit,
                        last_computed=computed_at,
                        upstream_watermark=units[unit],
                        fingerprints=dict(requested),
                        reason=plan.reason,
                        extra={
                            "source_schema": source_schema,
                            "result_schema": result_schema,
                            "n_cells": int(report.cells),
                        },
                    )
                )
                state.save()
                children[label] = child

                # Logging
                logger.info(f"Vulnérabilités de réseau calculées pour {label} : {report}")
            except Exception as exc:
                # Journalisation de l'échec, poursuite avec les autres millésimes
                logger.exception(
                    f"Échec du calcul des vulnérabilités de réseau pour le millésime {label}"
                )
                errors[label] = exc

    failure: Optional[BaseException] = None
    if errors:
        failure = RuntimeError(
            f"{len(errors)} millésime(s) en échec sur {len(stale)} : {sorted(errors)}"
        )
        failure.__cause__ = next(iter(errors.values()))
    return StepResult(
        step="network",
        n_units_planned=len(stale),
        n_units_succeeded=len(children),
        failures={label: failure_message(exc) for label, exc in errors.items()},
        children=children,
        reportable=False,
        failure_exception=failure,
    )
