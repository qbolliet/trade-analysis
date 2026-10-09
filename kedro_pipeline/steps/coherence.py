"""Coherence step: diagnostics of the coherence of the synthesis, by context.

Second step of the synthesis, modelled on :mod:`.synthesis`. It reads two
tables of the ``vulnerabilities`` catalog — the combined metric table (same
query as the synthesis) and the long table of the scores written by the
synthesis (``SYNTHESIS.RESULT_SCHEMA``) — calls the pure runner
``macroforecast.trade.aggregation.run_coherence`` and writes the long table of
the coherence diagnostics into ``COHERENCE.RESULT_SCHEMA``
(``synthesis_diagnostics``): ``metrics`` family (coherence of the metrics with
each other) and ``methods`` family (coherence of the syntheses); the ``fit``
family of the same table is written by the synthesis.

The queries, the read, the configuration, the per-context registry, the budget
and the cadence are those of :mod:`.synthesis`, owner of the ``SOURCES`` /
``FILTERS`` read contract: no duplication.

Freshness, by context. A context is a candidate once synthesised; it is
recomputed when it was never analysed, when its synthesis was recomputed since
its last analysis (watermark = ``last_computed`` of its synthesis entry), when
the fingerprint of the coherence configuration changes (global fingerprint: no
per-statistic granularity), or when it is forced (``FORCE`` of the block or
forced step ``coherence``). Same three stages as the synthesis, same catch-up
budget and cadence check; the recorded instant is captured before the
computation. The failure of a context does not interrupt the others.

No environment variable and no YAML path are read here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import replace
from datetime import datetime
from contextlib import nullcontext
import logging
from pathlib import Path
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Poignée d'écriture des tables DuckLake (remplacement transactionnel, traçabilité)
from kedro_pipeline.io.ducklake import BorrowedReader, DuckLakeTable, attached_catalog_alias
# Parallélisme intra-pod (carte parallèle, erreurs transmissibles)
from kedro_pipeline.parallel import parallel_map, serialisable_exception
# Module d'utilitaires de téléchargement (instants, parsing ISO)
from statflows.core.download import _now, _parse_iso

# Module de manipulation de données
import pandas as pd

# Module de suivi d'exécution (objet nul par défaut)
from macroforecast.tracking import (
    ArtifactCollector,
    CapturingTracker,
    RecordingTracker,
    RunTracker,
    rekey_metrics,
)
# Méthodologie de cohérence (fonction pure) et configuration de la synthèse
from macroforecast.trade.aggregation import (
    CoherenceConfig,
    CoherenceLevelReport,
    CoherenceRunReport,
    SynthesisConfig,
    run_coherence,
)
# Fabriques de configuration
from kedro_pipeline.steps._config import (  # noqa: F401
    coherence_config_from_params,
    incremental_settings,
    load_synthesis_flows,
    load_synthesis_vintages,
    synthesis_config_from_params,
)
# Helpers partagés avec l'étape de synthèse (cf. docstring)
from kedro_pipeline.steps.synthesis import (
    STEP as SYNTHESIS_STEP,  # noqa: F401  (ré-exportée par le script)
    IncrementalOutcome,
    IncrementalSettings,
    _run_result,
    _sql_literal,  # noqa: F401  (ré-exportée par le script)
    build_contexts_query,
    build_scores_query,
    build_source_query,
    context_freshness_tags,
    context_unit,
    last_computation,
    read_contexts,
    read_source_metrics,
    replacement_predicate,
    select_contexts,
    skip_by_cadence,
    skipped_by_cadence,
)
# Registres de fraîcheur v2 (empreinte, forçage, cascade)
from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    RegistryEntry,
    Unit,
    UnitPlan,
    fingerprint,
    plan_metrics,
    units_to_compute,
)
from kedro_pipeline.steps.result import StepResult, capturing
from macroforecast.trade.methodology import methodology_params
# Macros SQL de nomenclature (référentiel des millésimes)
from kedro_pipeline.config import nomenclature_macros_sql

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nœud du rapport de run (clé des contrôles des paramètres tracking ; message de commit)
NODE = "compute_synthesis_coherence"

# Clé racine du registre JSON des dates de dernier calcul de cohérence, tenu ici
_REGISTRY_ROOT = "COHERENCE"

# Nom de l'étape (forçage FORCE_STEPS, champ « step » du registre)
STEP = "coherence"



# Fonction de calcul de l'empreinte globale de la cohérence
def coherence_requested(config: CoherenceConfig) -> Dict[str, str]:
    """Current methodological fingerprint of the coherence diagnostics (global).

    There is no per-statistic granularity: any change of the coherence
    configuration recomputes every context. A fix in the implementation of a
    statistic is signalled by invalidating the recorded fingerprint
    (``scripts/invalidate_freshness.py --step coherence``).

    Args:
        config: Coherence configuration.

    Returns:
        ``{"coherence": fingerprint}``.

    Examples:
        >>> list(coherence_requested(CoherenceConfig()))
        ['coherence']
    """
    return {STEP: fingerprint(STEP, methodology_params(config))}

# Fonction d'extraction des contextes distincts de la table des métriques
def distinct_contexts(
    df_metrics: pd.DataFrame, context_columns: Sequence[str]
) -> List[Tuple[Any, ...]]:
    """Return the distinct context tuples of the metric table, in first-seen order.

    Args:
        df_metrics: Combined metric table read by :func:`read_source_metrics`.
        context_columns: Ordered context key columns
            (``SynthesisConfig.context_columns``).

    Returns:
        One tuple per distinct context, each value in the order of
        ``context_columns``.

    Examples:
        >>> frame = pd.DataFrame(
        ...     {"freq": ["A", "A", "A"], "TIME_PERIOD": ["2023", "2023", "2024"]}
        ... )
        >>> distinct_contexts(frame, ["freq", "TIME_PERIOD"])
        [('A', '2023'), ('A', '2024')]
    """
    unique = df_metrics.loc[:, list(context_columns)].drop_duplicates()
    return [tuple(row) for row in unique.itertuples(index=False, name=None)]

# Fonction de lecture de l'instant de dernier calcul de cohérence
def load_coherence_computation_date(
    last_computation_path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Optional[datetime]:
    """Read the coherence registry (empty if it does not exist yet).

    Args:
        last_computation_path: Path to the coherence registry.
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        The last coherence computation instant (UTC-aware), or ``None``.
    """
    # Lecture de l'entrée globale (racine "COHERENCE")
    entry = (
        loader.load(last_computation_path, bucket=bucket, missing_ok=True) or {}
    ).get(_REGISTRY_ROOT, {})
    return (
        _parse_iso(entry.get("last_computed"))
        if isinstance(entry, Mapping)
        else None
    )

# Fonction d'écriture de l'entrée de registre de cohérence
def save_coherence_computation_date(
    last_computation_path: Path,
    entry: Mapping[str, Any],
    saver: Saver,
    bucket: Optional[str],
) -> None:
    """Persist the coherence registry entry (single global entry, first registry format).

    Args:
        last_computation_path: Path to the coherence registry.
        entry: Registry payload (``last_computed``, ``n_groups``,
            ``n_contexts``).
        saver: ``Saver`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
    """
    # Écriture de l'entrée sous la racine "COHERENCE" (aucune fusion : entrée unique)
    saver.save(
        last_computation_path,
        {_REGISTRY_ROOT: dict(entry)},
        bucket=bucket,
        indent=2,
        ensure_ascii=False,
    )
    # Logging
    logger.info(
        f"Registre de cohérence mis à jour dans '{last_computation_path}'"
    )

# Fonction d'agrégation des rapports de contexte en un rapport d'exécution
def _aggregate_reports(
    reports: Sequence[CoherenceRunReport],
) -> CoherenceRunReport:
    """Merge the per-context :class:`CoherenceRunReport` into a run-level one.

    Only the counters are summed. The per-level medians (``kendall_w``,
    ``disputed_share``, ``mean_abs_rho``, ``violation_strict``) are left at their
    ``NaN`` default and dropped by ``to_metrics``: a run-level median cannot be
    recovered from per-context medians, and holding the isolation of a
    ``run_coherence`` call per context is the priority (isolation of the failures).

    Args:
        reports: One report per successfully analysed context.

    Returns:
        A single :class:`CoherenceRunReport` describing the whole run.
    """
    total = CoherenceRunReport()
    for report in reports:
        total.n_contexts += report.n_contexts
        total.n_groups += report.n_groups
        for level, level_report in report.levels.items():
            merged = total.levels.setdefault(level, CoherenceLevelReport())
            merged.n_groups += level_report.n_groups
    return total

# Familles de la table longue écrites par cette étape (la famille « fit » l'est
# par l'étape de synthèse)
_COHERENCE_FAMILIES: Tuple[str, ...] = ("metrics", "methods")

# Fonction de calcul d'un contexte (aucune lecture ni écriture)
def compute_coherence_context(
    df_metrics: pd.DataFrame,
    df_scores: pd.DataFrame,
    synthesis_config: SynthesisConfig,
    coherence_config: CoherenceConfig,
    *,
    tracker: Any = None,
    log_artifacts: bool = False,
) -> Tuple[pd.DataFrame, CoherenceRunReport]:
    """Measure the coherence of one context: pure computation, no read, no write.

    The middle step of an incremental run (planning → computation of each
    context → batched writes), callable in a separate process.

    Args:
        df_metrics: Metric rows of a single context.
        df_scores: Score rows of the same context (one row per cell and per
            method).
        synthesis_config: Configuration the scores were produced with.
        coherence_config: Coherence configuration.
        tracker: Experiment tracker receiving the artifacts (null by default).
        log_artifacts: Whether to log the artifacts.

    Returns:
        ``(df_diagnostics, report)`` of the context.
    """
    from macroforecast.tracking import NULL_TRACKER

    return run_coherence(
        df_metrics,
        df_scores,
        synthesis_config,
        coherence_config,
        tracker=tracker if tracker is not None else NULL_TRACKER,
        log_artifacts=log_artifacts,
    )

# Fonction d'écriture d'un lot de contextes analysés
def write_coherence_batch(
    frames: Sequence[pd.DataFrame],
    contexts: Sequence[Tuple[Any, ...]],
    synthesis_config: SynthesisConfig,
    *,
    table: DuckLakeTable,
    run_id: Optional[str] = None,
    commit_message: Optional[str] = None,
) -> bool:
    """Replace the ``metrics`` and ``methods`` diagnostics of a batch of contexts.

    In one transaction, the stored rows of these two families for the
    contexts of the batch are deleted, then the new rows are upserted: a
    statistic the previous run emitted and this one does not (a group grown
    past a threshold, a method removed) disappears. The ``fit`` rows written
    by the synthesis are left untouched.

    Args:
        frames: Diagnostic rows, one frame per context.
        contexts: Typed values of the contexts of the batch.
        synthesis_config: Synthesis configuration (context and cell columns).
        table: Handle of the long diagnostic table.
        run_id: Run identifier recorded on the snapshot.
        commit_message: Commit message recorded on the snapshot.

    Returns:
        Whether the table was created by this write.
    """
    context_columns = list(synthesis_config.context_columns)
    keys = [
        *context_columns, "level", synthesis_config.reporter_col,
        synthesis_config.product_col, "family", "statistic", "item_a", "item_b",
    ]
    non_empty = [frame for frame in frames if not frame.empty]
    where = replacement_predicate(context_columns, {_COHERENCE_FAMILIES: list(contexts)}, "family")
    if not non_empty and not table.exists():
        return False
    return table.upsert_many(
        [pd.concat(non_empty, ignore_index=True)] if non_empty else [], keys,
        delete_where=where, run_id=run_id, commit_message=commit_message,
    )

# Tâche de calcul de la cohérence d'un contexte, envoyée à un worker
@dataclass(frozen=True)
class CoherenceTask:
    """Everything a worker needs to analyse the coherence of one context.

    Attributes:
        rank: Position of the context in the run plan.
        unit: Freshness unit of the context.
        context: Typed context values.
        synthesis_config: Configuration the scores were produced with.
        coherence_config: Coherence configuration.
        reader: Opens the read connection of the worker.
        sources: ``SYNTHESIS.SOURCES`` block.
        filters: ``SYNTHESIS.FILTERS`` block.
        catalog_alias: DuckLake catalog alias.
        scores_schema: Schema of the scores.
        flow_codes: Flow codes of the synthesised directions.
        vintages: ``SYNTHESIS.VINTAGES``.
        log_artifacts: Whether the artifacts are recorded for the parent.
    """
    rank: int
    unit: Unit
    context: Tuple[Any, ...]
    synthesis_config: SynthesisConfig
    coherence_config: CoherenceConfig
    reader: Any
    sources: Sequence[Mapping[str, Any]]
    filters: Mapping[str, Any]
    catalog_alias: str
    scores_schema: str
    flow_codes: Optional[Sequence[int]]
    vintages: Optional[str]
    log_artifacts: bool

# Résultat d'une tâche de cohérence, renvoyé au processus parent
@dataclass
class CoherenceOutcome:
    """Outcome of one :class:`CoherenceTask`, picklable.

    Attributes:
        rank: Rank of the context in the plan.
        unit: Freshness unit of the context.
        diagnostics: Diagnostic rows of the context (``None`` on failure).
        report: Coherence report of the context.
        recorded: Tracker calls recorded by the worker.
        cpu_seconds: CPU time spent by the task.
        error: Serialisable exception of a failed context.
        missing: Whether the context was absent from the read.
    """
    rank: int
    unit: Unit
    diagnostics: Optional[pd.DataFrame] = None
    report: Optional[CoherenceRunReport] = None
    recorded: Optional[RecordingTracker] = None
    cpu_seconds: float = 0.0
    error: Optional[BaseException] = None
    missing: bool = False

# Fonction de calcul de la cohérence d'un contexte dans un worker
def compute_coherence_task(task: CoherenceTask) -> CoherenceOutcome:
    """Read the metrics and scores of one context and analyse their coherence.

    Module-level function so that it can be sent to a worker process; the
    worker opens a read connection of its own and never writes. Errors are
    returned, not raised.

    Args:
        task: Context to analyse.

    Returns:
        The outcome of the task.
    """
    started = time.process_time()
    outcome = CoherenceOutcome(rank=task.rank, unit=task.unit)
    context_columns = list(task.synthesis_config.context_columns)
    cell_columns = (task.synthesis_config.reporter_col, task.synthesis_config.product_col)
    try:
        with task.reader() as conn:
            df_metrics = read_source_metrics(
                conn,
                build_source_query(
                    task.sources, task.filters, task.catalog_alias, task.flow_codes,
                    task.vintages, contexts=[task.context], context_columns=context_columns,
                ),
                cell_columns,
            )
            df_scores = read_source_metrics(
                conn,
                build_scores_query(
                    task.catalog_alias, task.scores_schema, context_columns, [task.context]
                ),
                cell_columns,
            )
        if df_metrics.empty:
            # Contexte disparu de la grille entre la planification et la lecture
            outcome.missing = True
            return outcome
        recorder = RecordingTracker() if task.log_artifacts else None
        outcome.diagnostics, outcome.report = compute_coherence_context(
            df_metrics, df_scores, task.synthesis_config, task.coherence_config,
            tracker=recorder, log_artifacts=task.log_artifacts,
        )
        outcome.recorded = recorder
    except Exception as exc:
        logger.exception(f"Échec de la cohérence pour le contexte {task.unit}")
        outcome.error = serialisable_exception(exc)
    outcome.cpu_seconds = time.process_time() - started
    return outcome

# Fonction de décision des contextes de cohérence à (re)calculer
def plan_coherence_contexts(
    units: Sequence[Unit],
    registry: FreshnessRegistry,
    synthesis_registry: FreshnessRegistry,
    requested: Mapping[str, str],
    force: ForceSpec,
    *,
    adopt_legacy_fingerprints: bool = False,
) -> Tuple[Dict[Unit, UnitPlan], Dict[Unit, datetime]]:
    """Decide which contexts the coherence recomputes.

    A context is a candidate once its synthesis exists; it is recomputed when
    never analysed (``first``), forced, synthesised again since its last
    analysis (``new_data``: the ``last_computed`` of its synthesis entry is
    more recent than the recorded watermark), or when the fingerprint of the
    coherence configuration changed (``fingerprint``, global: no granularity
    per statistic).

    Args:
        units: Contexts of the filtered grid.
        registry: Per-context registry of the coherence.
        synthesis_registry: Per-context registry of the synthesis.
        requested: Current fingerprint (:func:`coherence_requested`).
        force: One-off forcing.
        adopt_legacy_fingerprints: Deployment migration flag.

    Returns:
        ``(plans, watermarks)``: the plans of the stale contexts and the
        synthesis instant of every candidate context.
    """
    watermarks: Dict[Unit, datetime] = {}
    for unit in units:
        entry = synthesis_registry.get(unit)
        if entry is not None and entry.last_computed is not None:
            watermarks[unit] = entry.last_computed
    candidates = [unit for unit in units if unit in watermarks]
    plans = units_to_compute(
        candidates, registry, watermarks, requested, force, step=STEP,
        adopt_legacy_fingerprints=adopt_legacy_fingerprints,
    )
    return plans, watermarks

# Fonction d'exécution de la partie « lecture -> run_coherence -> écriture »
def run_from_connections(
    read_conn: Any,
    diagnostics_conn: Any,
    source_query: str,
    scores_schema: str,
    synthesis_config: SynthesisConfig,
    coherence_config: CoherenceConfig,
    *,
    catalog_alias: str,
    diagnostics_schema: str,
    tracker: Any = None,
    log_artifacts: bool = True,
    scope: Any = None,
    freshness_metrics: Optional[Mapping[str, float]] = None,
    freshness_tags: Optional[Mapping[str, str]] = None,
    run_id: Optional[str] = None,
) -> Tuple[List[CoherenceRunReport], Dict[str, Exception], bool, int]:
    """Read the metrics and scores, run the coherence per context, and write diagnostics.

    Complete (non incremental) analysis of every context of ``source_query``;
    the incremental runs go through :func:`run_incremental_coherence`.
    Isolates the DB-bound core — the metrics read, the context-restricted
    scores read and the context-by-context call to ``run_coherence``
    writing the long diagnostic table — from connection setup and freshness
    bookkeeping, so it is callable on any pair of already-open connections,
    tests included. The failure of one context does not interrupt the others.

    Args:
        read_conn: Open connection positioned to read the metrics
            (``source_query``) and the scores (``scores_schema``).
        diagnostics_conn: Open connection to write the ``diagnostics_schema``
            fact table (``metrics`` and ``methods`` families).
        source_query: Metrics query built by
            :func:`~kedro_pipeline.steps.synthesis.build_source_query`
            (same as the synthesis step).
        scores_schema: Schema the score table lives in.
        synthesis_config: Configuration the scores were produced with.
        coherence_config: Coherence methodological configuration.
        catalog_alias: DuckLake catalog alias both connections are attached to.
        diagnostics_schema: Target schema of the coherence diagnostics.
        tracker: Experiment tracker; the null tracker by default.
        log_artifacts: Whether to log the coherence artifacts (rank-correlation
            matrices, leave-one-metric-out tables) to the tracker.
        scope: Run-report scope exposing ``guard(tracker)`` and
            ``publish_result(tracker, result)`` (the scripts' ``RunScope``).
            When given, the run report (checks, key figures, sections) is built
            from the step result and published inside the tracker's run, after
            the metrics and before returning, and an uncaught exception
            publishes the reduced failure description.
        freshness_metrics: Metrics of the freshness decision
            (``freshness/*``), logged in the run.
        freshness_tags: Tags of the freshness decision (``forced``…), set on
            the run.
        run_id: Run identifier recorded on the DuckLake snapshots.

    Returns:
        Tuple ``(reports, failures, created_any, n_contexts)``: one
        :class:`~macroforecast.trade.aggregation.CoherenceRunReport` per
        successfully analysed context, the per-context exceptions keyed by
        their string representation, whether the diagnostics schema was
        created on this call, and the number of contexts attempted.
    """
    if tracker is None:
        from macroforecast.tracking import NULL_TRACKER

        tracker = NULL_TRACKER
    # Enregistrement de ce qui est journalisé : source des contrôles et du rapport de run
    tracker = CapturingTracker(tracker)

    context_columns = list(synthesis_config.context_columns)
    table = DuckLakeTable(diagnostics_conn, catalog_alias, diagnostics_schema)

    reports: List[CoherenceRunReport] = []
    failures: Dict[str, Exception] = {}
    created_any = False
    n_contexts = 0

    with tracker, (scope.guard(tracker) if scope is not None else nullcontext()):
        # Décision de fraîcheur de l'exécution (métriques et tag de forçage)
        if freshness_metrics:
            tracker.log_metrics(dict(freshness_metrics))
        if freshness_tags:
            tracker.set_tags(dict(freshness_tags))
        # Lecture de la table des métriques combinées (une seule requête)
        df_metrics = read_source_metrics(
            read_conn, source_query, (synthesis_config.reporter_col, synthesis_config.product_col)
        )
        contexts = distinct_contexts(df_metrics, context_columns)
        logger.info(
            f"{len(df_metrics)} ligne(s) de métriques lue(s), "
            f"{len(contexts)} contexte(s)."
        )

        # Lecture des scores, restreinte aux contextes lus (jamais la table entière)
        scores_query = build_scores_query(
            catalog_alias, scores_schema, context_columns, contexts
        )
        logger.info(f"Requête des scores :\n{scores_query}")
        df_scores = read_source_metrics(
            read_conn, scores_query, (synthesis_config.reporter_col, synthesis_config.product_col)
        )
        logger.info(f"{len(df_scores)} ligne(s) de scores lue(s).")

        # Un contexte après l'autre : l'échec de l'un n'emporte pas les autres
        for context_value, df_context in df_metrics.groupby(
            context_columns, sort=False, observed=True
        ):
            n_contexts += 1
            context = (
                context_value
                if isinstance(context_value, tuple)
                else (context_value,)
            )
            try:
                # Calcul pur des diagnostics de cohérence du contexte
                df_diagnostics, report = compute_coherence_context(
                    df_context,
                    _context_rows(df_scores, context_columns, context),
                    synthesis_config,
                    coherence_config,
                    tracker=tracker,
                    log_artifacts=log_artifacts,
                )

                # Écriture : les familles « metrics » et « methods » du contexte
                # sont remplacées (schéma « synthesis_diagnostics », clé contexte x
                # niveau x groupe x famille x statistique x objets)
                created = write_coherence_batch(
                    [df_diagnostics], [context], synthesis_config,
                    table=table,
                    run_id=run_id,
                    commit_message=(
                        f"compute_synthesis_coherence {'/'.join(str(value) for value in context)}"
                    ),
                )
                created_any = created_any or created
                reports.append(report)

                # Logging
                logger.info(
                    f"Contexte {context} analysé : {report.n_groups} "
                    f"groupe(s), {len(df_diagnostics)} ligne(s) de diagnostic."
                )
            except Exception as exc:
                # Journalisation de l'échec, poursuite avec les autres contextes
                logger.exception(
                    f"Échec de la cohérence pour le contexte {context}"
                )
                failures[str(context)] = exc

        _log_run(tracker, reports, created_any, diagnostics_schema=diagnostics_schema)
        if scope is not None:
            scope.publish_result(
                tracker, _run_result(tracker, reports, failures, n_contexts, step=STEP)
            )

    return reports, failures, created_any, n_contexts

# Fonction d'extraction des lignes d'un contexte
def _context_rows(
    df: pd.DataFrame, context_columns: Sequence[str], context: Sequence[Any]
) -> pd.DataFrame:
    """Return the rows of ``df`` belonging to one context.

    Args:
        df: Table carrying the context columns.
        context_columns: Ordered context key columns.
        context: Values of the context.

    Returns:
        The rows of the context.
    """
    mask = pd.Series(True, index=df.index)
    for column, value in zip(context_columns, context):
        mask &= df[column] == value
    return df.loc[mask]

# Fonction de journalisation des métriques et tags agrégés d'une exécution
def _log_run(
    tracker: Any,
    reports: Sequence[CoherenceRunReport],
    created_any: bool,
    *,
    diagnostics_schema: str,
) -> None:
    """Log the aggregated metrics and tags of a run (every successful context).

    Args:
        tracker: Capturing tracker of the run.
        reports: One report per analysed context.
        created_any: Whether the schema was created.
        diagnostics_schema: Target schema of the diagnostics.
    """
    aggregate = _aggregate_reports(reports)
    aggregate.created = created_any
    tracker.log_metrics(rekey_metrics(aggregate.to_metrics()))
    tracker.set_tags(
        {
            "result_schema": diagnostics_schema,
            "n_contexts": str(len(reports)),
            "created": str(created_any),
        }
    )


# Fonction d'exécution incrémentale de la cohérence (planification, calcul, écriture)
def run_incremental_coherence(
    read_conn: Any,
    diagnostics_conn: Any,
    synthesis_config: SynthesisConfig,
    coherence_config: CoherenceConfig,
    *,
    sources: Sequence[Mapping[str, Any]],
    filters: Mapping[str, Any],
    catalog_alias: str,
    scores_schema: str,
    diagnostics_schema: str,
    registry: FreshnessRegistry,
    synthesis_registry: FreshnessRegistry,
    requested: Mapping[str, str],
    force: ForceSpec,
    settings: IncrementalSettings,
    flow_codes: Optional[Sequence[int]] = None,
    vintages: Optional[str] = None,
    adopt_legacy_fingerprints: bool = False,
    computed_at: Optional[datetime] = None,
    run_id: Optional[str] = None,
    tracker: Any = None,
    log_artifacts: bool = True,
    reader: Any = None,
) -> IncrementalOutcome:
    """Analyse the stale contexts, batch by batch.

    Same model as the synthesis: planning (contexts of the filtered grid
    already synthesised, :func:`plan_coherence_contexts`, most recent periods
    first, catch-up budget), pure computation of each context
    (:func:`compute_coherence_task`, in ``settings.n_jobs`` worker processes
    each reading with a connection of its own), then batched writes
    (:func:`write_coherence_batch`) and registry entries. The watermark of a
    context is the ``last_computed`` of its synthesis entry.

    Args:
        read_conn: Open connection reading the metrics and the scores.
        diagnostics_conn: Open connection writing the diagnostics.
        synthesis_config: Configuration the scores were produced with.
        coherence_config: Coherence configuration.
        sources: ``SYNTHESIS.SOURCES`` block.
        filters: ``SYNTHESIS.FILTERS`` block.
        catalog_alias: DuckLake catalog alias.
        scores_schema: Schema of the scores.
        diagnostics_schema: Target schema of the diagnostics.
        registry: Per-context registry of the coherence.
        synthesis_registry: Per-context registry of the synthesis.
        requested: Current fingerprint.
        force: One-off forcing.
        settings: Incremental settings.
        flow_codes: Flow codes of the synthesised directions.
        vintages: ``SYNTHESIS.VINTAGES``.
        adopt_legacy_fingerprints: Deployment migration flag.
        computed_at: Instant recorded as ``last_computed``.
        run_id: Run identifier recorded on the snapshots.
        tracker: Tracker of the caller's open run (never entered here); the
            null tracker by default.
        log_artifacts: Whether to log the artifacts.
        reader: Opens the read connection of a worker (a
            :class:`~kedro_pipeline.io.ducklake.ConnectionReader`). ``None``
            lends ``read_conn``, which only works with one process.

    Returns:
        The outcome of the run.

    Raises:
        ValueError: If several processes are requested without a ``reader``.
    """
    if tracker is None:
        from macroforecast.tracking import NULL_TRACKER

        tracker = NULL_TRACKER
    tracker = CapturingTracker(tracker)
    computed_at = computed_at or _now()
    if reader is None:
        if settings.n_jobs > 1:
            raise ValueError("n_jobs > 1 requires a `reader` (connector factory), not a shared connection.")
        reader = BorrowedReader(read_conn)
    context_columns = list(synthesis_config.context_columns)
    cell_columns = (synthesis_config.reporter_col, synthesis_config.product_col)
    table = DuckLakeTable(diagnostics_conn, catalog_alias, diagnostics_schema)
    outcome = IncrementalOutcome()

    # 1. Planification : contextes de la grille déjà synthétisés, plans, budget
    contexts = read_contexts(
        read_conn,
        build_contexts_query(sources, filters, catalog_alias, context_columns, flow_codes, vintages),
    )
    typed = {context_unit(context_columns, context): context for context in contexts}
    units = list(typed)
    outcome.plans, watermarks = plan_coherence_contexts(
        units, registry, synthesis_registry, requested, force,
        adopt_legacy_fingerprints=adopt_legacy_fingerprints,
    )
    selected, outcome.backlog = select_contexts(
        outcome.plans, units, settings.period_column, settings.max_contexts
    )
    outcome.n_selected = len(selected)
    tracker.log_metrics(
        {
            **plan_metrics(outcome.plans, n_candidates=len(watermarks)),
            "freshness/skipped_by_cadence": 0.0,
            "freshness/units_waiting_synthesis": float(len(units) - len(watermarks)),
            f"{STEP}/contexts_planned": float(len(outcome.plans)),
            f"{STEP}/contexts_run": float(len(selected)),
            f"{STEP}/contexts_budget_left": float(outcome.backlog),
        }
    )
    tracker.set_tags(context_freshness_tags(outcome.plans, force, requested, STEP))
    # Logging
    logger.info(
        f"{len(units)} contexte(s) dans la grille dont {len(watermarks)} synthétisé(s), "
        f"{len(outcome.plans)} périmé(s), {len(selected)} analysé(s) dans cette "
        f"exécution, {outcome.backlog} reporté(s)."
    )

    # 2-3. Calcul dans les workers, écriture par lots dans ce processus
    tasks = [
        CoherenceTask(
            rank=rank, unit=unit, context=typed[unit],
            synthesis_config=synthesis_config, coherence_config=coherence_config,
            reader=reader, sources=sources, filters=filters, catalog_alias=catalog_alias,
            scores_schema=scores_schema, flow_codes=flow_codes, vintages=vintages,
            log_artifacts=log_artifacts,
        )
        for rank, unit in enumerate(selected)
    ]
    artifacts = ArtifactCollector()
    cpu_seconds = 0.0
    wall_started = time.perf_counter()

    # Écriture d'un lot, puis avancement du registre (jamais avant l'écriture)
    def flush(done: List[CoherenceOutcome]) -> None:
        if not done:
            return
        try:
            created = write_coherence_batch(
                [item.diagnostics for item in done], [typed[item.unit] for item in done],
                synthesis_config,
                table=table,
                run_id=run_id,
                commit_message=f"{NODE} {len(done)} contexte(s) "
                               f"{done[0].unit.key} .. {done[-1].unit.key}",
            )
        except Exception as exc:
            logger.exception(f"Échec de l'écriture d'un lot de {len(done)} contexte(s)")
            for item in done:
                outcome.failures[item.unit.key] = exc
            return
        outcome.created_any = outcome.created_any or created
        for item in done:
            registry.upsert(
                RegistryEntry(
                    unit=item.unit,
                    last_computed=computed_at,
                    upstream_watermark=watermarks[item.unit],
                    fingerprints=dict(requested),
                    reason=outcome.plans[item.unit].reason,
                    extra={"n_groups": int(item.report.n_groups)},
                )
            )
            outcome.reports.append(item.report)
            outcome.computed.append(item.unit)
        registry.save()

    pending: List[CoherenceOutcome] = []
    for _, result in parallel_map(compute_coherence_task, tasks, settings.n_jobs):
        cpu_seconds += result.cpu_seconds
        if result.error is not None:
            outcome.failures[result.unit.key] = result.error
            continue
        if result.missing:
            logger.warning(f"Contexte {result.unit} absent de la lecture des métriques, ignoré.")
            continue
        if result.recorded is not None:
            artifacts.add(result.rank, result.recorded)
        pending.append(result)
        if len(pending) >= settings.write_batch_contexts:
            flush(pending)
            pending = []
    flush(pending)
    # Artefacts des workers rejoués dans le run, comme un calcul séquentiel
    artifacts.replay(tracker)
    tracker.log_metrics(
        {
            "timing/wall_seconds": time.perf_counter() - wall_started,
            "timing/cpu_seconds_sum": cpu_seconds,
            "parallel/n_jobs": float(settings.n_jobs),
        }
    )

    # Rapport de run seulement si un contexte était périmé : une exécution
    # sans rien à recalculer ne doit pas lever le contrôle « contextes calculés »
    if outcome.plans:
        _log_run(
            tracker, outcome.reports, outcome.created_any, diagnostics_schema=diagnostics_schema,
        )
    registry.save()
    return outcome


# ──────────────────────────────────────────────────────────────────────
# Fonction d'étape
# ──────────────────────────────────────────────────────────────────────

# Fonction d'étape : diagnostics de cohérence incrémentaux
def run_coherence_step(
    scores: Any,
    diagnostics: Any,
    state: FreshnessRegistry,
    synthesis_state: FreshnessRegistry,
    *,
    params: Mapping[str, Any],
    vulnerability_params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    tracker: Optional[RunTracker] = None,
    n_jobs: int = 1,
    force: Optional[ForceSpec] = None,
    cadence_check: bool = False,
    run_id: Optional[str] = None,
) -> StepResult:
    """Analyse the stale synthesised contexts and return the outcome of the run.

    Args:
        scores: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the score
            table (``synthesis``), whose catalog also holds the source metrics
            (read connection; a lazy handle gives each worker its own).
        diagnostics: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            diagnostics table (``synthesis_diagnostics``).
        state: Per-context registry of the coherence.
        synthesis_state: Per-context registry of the synthesis (read only).
        params: The ``synthesis`` parameters (``SYNTHESIS`` and ``COHERENCE``).
        vulnerability_params: The ``vulnerabilities`` parameters (flow codes).
        runtime: The ``runtime`` parameters (``NOMENCLATURES.HS``, forcing).
        tracker: Tracker of the caller's open run (never entered here).
        n_jobs: Worker processes computing the contexts (resolved by the caller).
        force: One-off forcing; read from ``runtime`` alone when ``None``. The
            ``COHERENCE.FORCE`` flag forces the step too.
        cadence_check: Whether to skip the run when the step ran less than
            ``CADENCE.MIN_INTERVAL_DAYS`` ago and nothing forces it.
        run_id: Run identifier recorded on the DuckLake snapshots.

    Returns:
        The step result (see
        :func:`~kedro_pipeline.steps.synthesis.run_synthesis_step`).
    """
    tracker = capturing(tracker)
    synthesis_block, block = params["SYNTHESIS"], params["COHERENCE"]
    # Configuration méthodologique de la synthèse (les scores ont été produits avec
    # elle) et de la cohérence ; mêmes sens et même périmètre de nomenclature
    synthesis_config = synthesis_config_from_params(synthesis_block.get("PARAMETERS") or {})
    coherence_config = coherence_config_from_params(block.get("PARAMETERS") or {})
    _, flow_codes = load_synthesis_flows(synthesis_block, vulnerability_params, synthesis_config)
    vintages = load_synthesis_vintages(synthesis_block, synthesis_config)
    settings = incremental_settings(block, n_jobs=n_jobs)
    log_artifacts = bool((block.get("TRACKING") or {}).get("LOG_ARTIFACTS", True))

    # Fraîcheur : empreinte globale de la configuration de cohérence, forçage
    requested = coherence_requested(coherence_config)
    force = force if force is not None else ForceSpec.from_runtime(runtime, environ={})
    if block.get("FORCE", False):
        force = replace(force, steps=force.steps | {STEP})

    # Cadence : sortie immédiate si la dernière exécution est trop récente
    if cadence_check:
        last = last_computation(state)
        if skipped_by_cadence(last, _now(), settings.min_interval_days,
                              force.forces_step(STEP, requested)):
            skip_by_cadence(tracker, STEP, last)
            return StepResult(
                step=STEP, metrics=dict(tracker.metrics), tags=dict(tracker.tags),
                reportable=False, outputs={"skipped_by_cadence": True},
            )

    # Instant de référence capturé avant le calcul
    computed_at = _now()
    # Macros de nomenclature de chaque session (conditions de jointure de la requête)
    macros = nomenclature_macros_sql(runtime["NOMENCLATURES"]["HS"])
    reader = scores.reader(macros)
    with scores.connect() as read_conn:
        for statement in macros:
            read_conn.execute(statement)
        with diagnostics.connect() as diagnostics_conn:
            outcome = run_incremental_coherence(
                read_conn,
                diagnostics_conn,
                synthesis_config,
                coherence_config,
                sources=synthesis_block["SOURCES"],
                filters=synthesis_block.get("FILTERS") or {},
                catalog_alias=attached_catalog_alias(scores),
                scores_schema=scores.schema,
                diagnostics_schema=diagnostics.schema,
                registry=state,
                synthesis_registry=synthesis_state,
                requested=requested,
                force=force,
                settings=settings,
                flow_codes=flow_codes,
                vintages=vintages,
                computed_at=computed_at,
                run_id=run_id,
                tracker=tracker,
                log_artifacts=log_artifacts,
                reader=reader,
            )
    result = _run_result(
        tracker, outcome.reports, outcome.failures, outcome.n_selected,
        step=STEP, reportable=bool(outcome.plans),
    )
    result.outputs["outcome"] = outcome
    return result
