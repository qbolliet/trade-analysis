"""Script de calcul/mise à jour des diagnostics de cohérence de la synthèse.

Second script de la synthèse, calqué sur ``scripts/compute_synthetic_scores.py``.
Il lit deux tables du catalogue ``vulnerabilities`` — la table des métriques
combinées (même requête que le script de synthèse) et la table longue des
scores produite par ce dernier (``SYNTHESIS.RESULT_SCHEMA``) — appelle le
runner pur ``macroforecast.trade.aggregation.run_coherence`` (aucun I/O) et
écrit la table longue des diagnostics de cohérence dans
``COHERENCE.RESULT_SCHEMA`` (schéma ``synthesis_diagnostics`` du même
catalogue). Familles ``metrics`` (cohérence des métriques entre elles) et
``methods`` (cohérence des synthèses) ; la famille ``fit`` de la même table est
écrite par le script de synthèse.

Place dans le pipeline (ordre Argo) : après ``compute_synthetic_scores.py``
(dépendance directe, couplage faible : seul son registre JSON de fraîcheur est
lu).

Réutilisation du script de synthèse. Les requêtes (``build_source_query``,
``build_contexts_query``, ``build_scores_query``), la lecture, la
configuration, le registre par contexte, la planification par budget et la
cadence sont importés tels quels depuis ``scripts.compute_synthetic_scores``,
propriétaire naturel du contrat de lecture ``SOURCES`` / ``FILTERS`` : aucune
duplication.

Fraîcheur, par contexte. Même registre par contexte que la synthèse
(``COHERENCE.STATE.PATH_TEMPLATE``). Un contexte n'est candidat qu'une fois
synthétisé ; il est recalculé s'il n'a jamais été analysé, si sa synthèse a été
recalculée depuis sa dernière analyse (watermark = ``last_computed`` de son
entrée de synthèse), si l'empreinte de la configuration de cohérence change
(empreinte globale : pas de granularité par statistique), ou en cas de forçage
(``FORCE`` du YAML ou ``runtime.FORCE_STEPS=coherence``). Mêmes trois temps que
la synthèse (planification, calcul pur, écriture par lots remplaçant les
familles ``metrics`` et ``methods`` des contextes), même budget de rattrapage
et même contrôle de cadence. La date écrite est capturée avant le calcul,
jamais après.

Erreurs. Comme le script de synthèse : un appel ``run_coherence`` par contexte,
l'échec de l'un n'emporte pas les autres, chaque échec est capturé et journalisé,
seuls les contextes réussis sont écrits, et le script ne sort en erreur qu'en fin
de parcours.

Le suivi d'exécution MLflow est piloté par le bloc ``COHERENCE.MLFLOW`` de
``config/synthesis.yaml`` : sans ``TRACKING_URI`` (ou sans serveur joignable),
``get_tracker`` retourne un objet nul et l'exécution est strictement inchangée.
Un seul run par exécution.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import fields, replace
from datetime import datetime
from contextlib import nullcontext
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Poignée d'écriture des tables DuckLake (remplacement transactionnel, traçabilité)
from kedro_pipeline.io.ducklake import DuckLakeTable, workflow_run_id
# Module d'utilitaires de téléchargement (instants, parsing ISO, noms de schéma)
from statflows.core.download import _now, _parse_iso, _schema_name

# Module de manipulation de données
import pandas as pd

# Module de suivi d'exécution (MLflow optionnel, objet nul par défaut)
from macroforecast.tracking import CapturingTracker, get_tracker, rekey_metrics
from macroforecast.tracking.figures import key_figures_coherence, sections_coherence
from macroforecast.tracking.report import Units
from scripts._run_report import RunScope, guarded_run, run_name
# Méthodologie de cohérence (fonction pure) et configuration de la synthèse
from macroforecast.trade.aggregation import (
    CoherenceConfig,
    CoherenceLevelReport,
    CoherenceRunReport,
    SynthesisConfig,
    run_coherence,
)
# Helpers d'I/O partagés avec le premier script de la synthèse (cf. docstring)
from scripts.compute_synthetic_scores import (
    _PARTNERS_ROOT,
    STEP as SYNTHESIS_STEP,
    IncrementalOutcome,
    IncrementalSettings,
    _result_connector,
    _sql_literal,
    build_contexts_query,
    build_scores_query,
    build_source_query,
    cadence_check_requested,
    chunks,
    context_freshness_tags,
    context_registry,
    context_unit,
    incremental_settings,
    last_computation,
    load_synthesis_config,
    load_synthesis_flows,
    load_synthesis_vintages,
    load_vulnerability_config,
    log_cadence_skip,
    parse_args,
    read_contexts,
    read_source_metrics,
    replacement_predicate,
    select_contexts,
    skipped_by_cadence,
    synthesis_config_from_params,
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
from macroforecast.trade.methodology import methodology_params
# Paramètres d'exécution partagés (forçage ponctuel)
from scripts.download_comtrade import load_runtime_config
# Macros SQL de nomenclature (référentiel des millésimes)
from kedro_pipeline.config import nomenclature_macros_sql

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

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


# ──────────────────────────────────────────────────────────────────────
# Configuration méthodologique de la cohérence
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction de la configuration méthodologique de la cohérence
def coherence_config_from_params(
    params: Optional[Mapping[str, Any]],
) -> CoherenceConfig:
    """Build a ``CoherenceConfig`` from the YAML ``PARAMETERS`` section.

    Generic construction, twin of ``synthesis_config_from_params``: every key
    matching a ``CoherenceConfig`` field name overrides the frozen dataclass
    default; unknown keys are dropped with a warning. YAML lists are coerced to
    the tuple types the dataclass expects (``topk_depths``, ``lomo_methods``).

    Args:
        params: The ``COHERENCE.PARAMETERS`` mapping of
            ``config/synthesis.yaml`` (or ``None``, meaning the defaults of
            :class:`~macroforecast.trade.aggregation.CoherenceConfig`).

    Returns:
        A ``CoherenceConfig`` reflecting the configured overrides.

    Examples:
        >>> coherence_config_from_params(None) == CoherenceConfig()
        True
        >>> coherence_config_from_params({"topk_depths": [5, 25]}).topk_depths
        (5, 25)
    """
    # Aucune surcharge : configuration par défaut
    default = CoherenceConfig()
    if not params:
        return default

    # Surcharge générique champ à champ
    valid = {field.name for field in fields(CoherenceConfig)}
    overrides: Dict[str, Any] = {}
    for key, value in params.items():
        if key not in valid:
            # Logging
            logger.warning(f"Paramètre de cohérence inconnu ignoré : {key}")
            continue
        # Coercition listes → tuples pour les champs tuple du dataclass gelé
        current = getattr(default, key)
        if isinstance(current, tuple) and isinstance(value, (list, tuple)):
            value = tuple(value)
        overrides[key] = value

    return replace(default, **overrides)


# ──────────────────────────────────────────────────────────────────────
# Sélection des contextes et requête des scores (fonctions pures)
# ──────────────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────────────
# Registre de fraîcheur de la cohérence (lecture/écriture, racine « COHERENCE »)
# ──────────────────────────────────────────────────────────────────────

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
    """Persist the coherence registry entry (single global entry, S-2.1 v1).

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


# ──────────────────────────────────────────────────────────────────────
# Agrégation des rapports (un rapport par contexte -> un rapport d'exécution)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'agrégation des rapports de contexte en un rapport d'exécution
def _aggregate_reports(
    reports: Sequence[CoherenceRunReport],
) -> CoherenceRunReport:
    """Merge the per-context :class:`CoherenceRunReport` into a run-level one.

    Only the counters are summed. The per-level medians (``kendall_w``,
    ``disputed_share``, ``mean_abs_rho``, ``violation_strict``) are left at their
    ``NaN`` default and dropped by ``to_metrics``: a run-level median cannot be
    recovered from per-context medians, and holding the isolation of a
    ``run_coherence`` call per context is the priority (S-2.1 errors).

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


# ──────────────────────────────────────────────────────────────────────
# Calcul d'un contexte (fonction pure) et écriture par lots
# ──────────────────────────────────────────────────────────────────────

# Familles de la table longue écrites par ce script (la famille « fit » l'est
# par le script de synthèse)
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


# ──────────────────────────────────────────────────────────────────────
# Fraîcheur par contexte : planification
# ──────────────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────────────
# Orchestration lecture -> run_coherence -> écriture
# ──────────────────────────────────────────────────────────────────────

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
    scope: Optional[RunScope] = None,
    freshness_metrics: Optional[Mapping[str, float]] = None,
    freshness_tags: Optional[Mapping[str, str]] = None,
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
            :func:`scripts.compute_synthetic_scores.build_source_query`
            (same as the synthesis script).
        scores_schema: Schema the score table lives in.
        synthesis_config: Configuration the scores were produced with.
        coherence_config: Coherence methodological configuration.
        catalog_alias: DuckLake catalog alias both connections are attached to.
        diagnostics_schema: Target schema of the coherence diagnostics.
        tracker: Experiment tracker; the null tracker by default.
        log_artifacts: Whether to log the coherence artifacts (rank-correlation
            matrices, leave-one-metric-out tables) to the tracker.
        scope: Run-report scope. When given, the run report (checks, key figures,
            sections) is built and published inside the tracker's run, after the
            metrics and before returning, and an uncaught exception publishes the
            reduced failure description.
        freshness_metrics: Metrics of the freshness decision
            (``freshness/*``), logged in the run.
        freshness_tags: Tags of the freshness decision (``forced``…), set on
            the run.

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

    with tracker, (guarded_run(scope, tracker) if scope is not None else nullcontext()):
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

        # Lecture des scores, restreinte aux contextes lus (S-2.4)
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
                    run_id=workflow_run_id(),
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

        _publish_run(
            tracker, scope, reports, failures, created_any, n_contexts,
            diagnostics_schema=diagnostics_schema,
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


# Fonction de publication des métriques et du rapport d'une exécution
def _publish_run(
    tracker: Any,
    scope: Optional[RunScope],
    reports: Sequence[CoherenceRunReport],
    failures: Mapping[str, Exception],
    created_any: bool,
    n_contexts: int,
    *,
    diagnostics_schema: str,
) -> None:
    """Log the aggregated metrics and tags of a run, then publish its report.

    Args:
        tracker: Capturing tracker of the run.
        scope: Run-report scope, or ``None``.
        reports: One report per analysed context.
        failures: Per-context exceptions.
        created_any: Whether the schema was created.
        n_contexts: Number of contexts attempted.
        diagnostics_schema: Target schema of the diagnostics.
    """
    # Métriques et tags de l'exécution : un rapport agrégé sur tous les contextes réussis
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

    # Rapport de run : contrôles, chiffres clés, sections, publiés avant toute sortie
    # en erreur (les contextes en échec sont listés, ils ne l'interrompent pas)
    if scope is not None:
        scope.step = "rapport de run"
        scope.publish(
            tracker,
            scope.build(
                metrics=tracker.metrics,
                units=Units(
                    planned=n_contexts,
                    succeeded=len(reports),
                    failed=len(failures),
                    planned_label=f"{n_contexts} contextes",
                ),
                failures={
                    unit: f"{type(exc).__name__}: {exc}" for unit, exc in failures.items()
                },
                key_figures=key_figures_coherence,
                sections=lambda m: sections_coherence(m, tracker.tables),
            ),
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
    scope: Optional[RunScope] = None,
) -> IncrementalOutcome:
    """Analyse the stale contexts, batch by batch.

    Same model as the synthesis: planning (contexts of the filtered grid
    already synthesised, :func:`plan_coherence_contexts`, most recent periods
    first, catch-up budget), pure computation of each context
    (:func:`compute_coherence_context`), then batched writes
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
        tracker: Experiment tracker; the null tracker by default.
        log_artifacts: Whether to log the artifacts.
        scope: Run-report scope, or ``None``.

    Returns:
        The outcome of the run.
    """
    if tracker is None:
        from macroforecast.tracking import NULL_TRACKER

        tracker = NULL_TRACKER
    tracker = CapturingTracker(tracker)
    computed_at = computed_at or _now()
    context_columns = list(synthesis_config.context_columns)
    cell_columns = (synthesis_config.reporter_col, synthesis_config.product_col)
    table = DuckLakeTable(diagnostics_conn, catalog_alias, diagnostics_schema)
    outcome = IncrementalOutcome()

    with tracker, (guarded_run(scope, tracker) if scope is not None else nullcontext()):
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

        for batch in chunks(selected, settings.write_batch_contexts):
            batch_contexts = [typed[unit] for unit in batch]
            # Lecture des métriques et des scores du lot (listes IN des contextes)
            df_metrics = read_source_metrics(
                read_conn,
                build_source_query(
                    sources, filters, catalog_alias, flow_codes, vintages,
                    contexts=batch_contexts, context_columns=context_columns,
                ),
                cell_columns,
            )
            df_scores = read_source_metrics(
                read_conn,
                build_scores_query(catalog_alias, scores_schema, context_columns, batch_contexts),
                cell_columns,
            )
            metric_frames = {
                context_unit(context_columns, key): frame
                for key, frame in df_metrics.groupby(context_columns, sort=False, observed=True)
            }
            score_frames = {
                context_unit(context_columns, key): frame
                for key, frame in df_scores.groupby(context_columns, sort=False, observed=True)
            }

            # Calcul de chaque contexte : l'échec de l'un n'emporte pas les autres
            done: List[Tuple[Unit, pd.DataFrame, CoherenceRunReport]] = []
            for unit in batch:
                df_context = metric_frames.get(unit)
                if df_context is None:
                    logger.warning(f"Contexte {unit} absent de la lecture des métriques, ignoré.")
                    continue
                try:
                    df_diagnostics, report = compute_coherence_context(
                        df_context,
                        score_frames.get(unit, df_scores.iloc[0:0]),
                        synthesis_config,
                        coherence_config,
                        tracker=tracker,
                        log_artifacts=log_artifacts,
                    )
                    done.append((unit, df_diagnostics, report))
                except Exception as exc:
                    logger.exception(f"Échec de la cohérence pour le contexte {unit}")
                    outcome.failures[unit.key] = exc
            if not done:
                continue

            # Écriture du lot, puis avancement du registre (jamais avant l'écriture)
            try:
                created = write_coherence_batch(
                    [frame for _, frame, _ in done], [typed[unit] for unit, _, _ in done],
                    synthesis_config,
                    table=table,
                    run_id=run_id,
                    commit_message=f"{NODE} {len(done)} contexte(s) "
                                   f"{done[0][0].key} .. {done[-1][0].key}",
                )
            except Exception as exc:
                logger.exception(f"Échec de l'écriture d'un lot de {len(done)} contexte(s)")
                for unit, _, _ in done:
                    outcome.failures[unit.key] = exc
                continue
            outcome.created_any = outcome.created_any or created
            for unit, _, report in done:
                registry.upsert(
                    RegistryEntry(
                        unit=unit,
                        last_computed=computed_at,
                        upstream_watermark=watermarks[unit],
                        fingerprints=dict(requested),
                        reason=outcome.plans[unit].reason,
                        extra={"n_groups": int(report.n_groups)},
                    )
                )
                outcome.reports.append(report)
                outcome.computed.append(unit)
            registry.save()

        # Rapport de run seulement si un contexte était périmé : une exécution
        # sans rien à recalculer ne doit pas lever le contrôle « contextes calculés »
        if outcome.plans:
            _publish_run(
                tracker, scope, outcome.reports, outcome.failures, outcome.created_any,
                len(selected), diagnostics_schema=diagnostics_schema,
            )
    registry.save()
    return outcome


# ──────────────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────

# Nœud du rapport de run (clé de config/tracking.yaml)
NODE = "compute_synthesis_coherence"


# Fonction principale de calcul des diagnostics de cohérence
def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point for the incremental coherence-diagnostics computation.

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` when ``None``).

    Raises:
        RuntimeError: If at least one context failed, once every context has
            been attempted.
    """
    args = parse_args(argv)
    # Chargement des configurations : synthèse (méthodologie, sources, filtres,
    # registre amont) et vulnérabilités (identité du catalogue)
    synthesis_file = load_synthesis_config()
    vulnerability_config = load_vulnerability_config()
    synthesis_config_block = synthesis_file["SYNTHESIS"]
    coherence_config_block = synthesis_file["COHERENCE"]

    # Configuration méthodologique de la synthèse (les scores ont été produits
    # avec elle) et de la cohérence
    synthesis_config = synthesis_config_from_params(
        synthesis_config_block.get("PARAMETERS") or {}
    )
    coherence_config = coherence_config_from_params(
        coherence_config_block.get("PARAMETERS") or {}
    )
    # Sens synthétisés et leurs codes, mêmes règles que le script de synthèse
    _, flow_codes = load_synthesis_flows(
        synthesis_config_block, vulnerability_config, synthesis_config
    )
    # Même périmètre de nomenclature que la synthèse dont on contrôle les scores
    vintages = load_synthesis_vintages(synthesis_config_block, synthesis_config)
    sources = synthesis_config_block["SOURCES"]
    filters = synthesis_config_block.get("FILTERS") or {}
    settings = incremental_settings(coherence_config_block)

    # Options de suivi d'exécution (un seul run par exécution, D-14)
    mlflow_config = coherence_config_block.get("MLFLOW") or {}
    log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))
    tracker = get_tracker(
        tracking_uri=mlflow_config.get("TRACKING_URI"),
        experiment=mlflow_config.get("EXPERIMENT", "trade-03-vulnerabilities"),
        run_name=run_name(f"vulnerabilities-coherence-{datetime.now():%Y%m%d-%H%M}", NODE),
    )
    scope = RunScope(NODE)

    # Identité du catalogue partagé et schémas résultat
    catalog = vulnerability_config[_PARTNERS_ROOT]
    catalog_alias = catalog["CATALOG_ALIAS"]
    scores_schema = _schema_name(synthesis_config_block["RESULT_SCHEMA"])
    diagnostics_schema = _schema_name(coherence_config_block["RESULT_SCHEMA"])

    # Fraîcheur : registres par contexte de la cohérence et de la synthèse,
    # empreinte globale de la configuration de cohérence, forçage
    runtime_config = load_runtime_config()
    bucket = synthesis_config_block["BUCKET"]
    context_columns = synthesis_config.context_columns
    registry = context_registry(coherence_config_block, bucket, STEP, context_columns)
    synthesis_registry = context_registry(
        synthesis_config_block, bucket, SYNTHESIS_STEP, context_columns
    )
    requested = coherence_requested(coherence_config)
    force = ForceSpec.from_runtime(runtime_config)
    if coherence_config_block.get("FORCE", False):
        force = replace(force, steps=force.steps | {STEP})

    # Cadence : sortie immédiate si la dernière exécution est trop récente
    if cadence_check_requested(args.cadence_check):
        last = last_computation(registry)
        if skipped_by_cadence(last, _now(), settings.min_interval_days,
                              force.forces_step(STEP, requested)):
            log_cadence_skip(tracker, STEP, last)
            return

    # Instant de référence capturé avant le calcul : la date enregistrée
    # correspond au début du traitement, jamais après, pour ne pas rater une
    # mise à jour survenue pendant le calcul
    computed_at = _now()

    # Connecteurs DuckLake : un pour la lecture (métriques + scores, schéma des
    # scores), un pour l'écriture des diagnostics ; tous deux sur le catalogue
    # partagé « vulnerabilities », bucket de la synthèse
    read_connector = _result_connector(
        vulnerability_config,
        bucket=bucket,
        data_path=synthesis_config_block["PATHS"]["DATA_PATH"],
        schema=scores_schema,
    )
    diagnostics_connector = _result_connector(
        vulnerability_config,
        bucket=bucket,
        data_path=coherence_config_block["PATHS"]["DATA_PATH"],
        schema=diagnostics_schema,
    )

    # Ouverture des connexions : leur cycle de vie appartient au script
    read_conn = read_connector.connect()
    try:
        # Macros de nomenclature de la session (conditions de jointure de la requête)
        for statement in nomenclature_macros_sql(runtime_config["NOMENCLATURES"]["HS"]):
            read_conn.execute(statement)
        diagnostics_conn = diagnostics_connector.connect()
        try:
            outcome = run_incremental_coherence(
                read_conn,
                diagnostics_conn,
                synthesis_config,
                coherence_config,
                sources=sources,
                filters=filters,
                catalog_alias=catalog_alias,
                scores_schema=scores_schema,
                diagnostics_schema=diagnostics_schema,
                registry=registry,
                synthesis_registry=synthesis_registry,
                requested=requested,
                force=force,
                settings=settings,
                flow_codes=flow_codes,
                vintages=vintages,
                computed_at=computed_at,
                run_id=workflow_run_id(),
                tracker=tracker,
                log_artifacts=log_artifacts,
                scope=scope,
            )
        finally:
            diagnostics_conn.close()
    finally:
        read_conn.close()

    # Échec global si au moins un contexte a échoué, une fois tous tentés
    if outcome.failures:
        raise RuntimeError(
            f"{len(outcome.failures)} contexte(s) en échec sur {len(outcome.computed) + len(outcome.failures)} : "
            f"{sorted(outcome.failures)}"
        ) from next(iter(outcome.failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
