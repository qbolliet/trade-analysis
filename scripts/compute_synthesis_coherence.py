"""Script de calcul/mise à jour des diagnostics de cohérence de la synthèse.

Enveloppe CLI de la fonction d'étape ``kedro_pipeline.steps.coherence.run_coherence_step`` :
diagnostics de cohérence des métriques entre elles (famille ``metrics``) et des
méthodes de synthèse entre elles (famille ``methods``), par contexte déjà
synthétisé, écrits dans la table longue ``synthesis_diagnostics``. Mêmes
requêtes, même registre par contexte, même budget et même contrôle de cadence
que la synthèse.

Ce script charge les paramètres, construit les poignées des tables de scores
(lecture des métriques et des scores) et de diagnostics (identifiants lus à la
première connexion), les registres de la cohérence et de la synthèse, le nombre
de processus et un seul run MLflow, appelle l'étape, publie le rapport de run
puis sort en erreur si un contexte a échoué.

Configuration lue : blocs ``synthesis``, ``vulnerabilities`` et ``runtime`` des
paramètres Kedro, environnement choisi par ``KEDRO_ENV``.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
from typing import Optional, Sequence

from kedro_pipeline.io.ducklake import workflow_run_id
from kedro_pipeline.io.freshness import ForceSpec
from kedro_pipeline.parallel import resolve_n_jobs
from kedro_pipeline.config import experiment_name
# Étape de cohérence (ré-exportée : configuration, requêtes, calcul, écriture)
from kedro_pipeline.steps.coherence import (  # noqa: F401
    _aggregate_reports,
    build_contexts_query,
    build_scores_query,
    build_source_query,
    coherence_config_from_params,
    coherence_requested,
    CoherenceOutcome,
    CoherenceTask,
    compute_coherence_context,
    compute_coherence_task,
    context_freshness_tags,
    _context_rows,
    context_unit,
    distinct_contexts,
    incremental_settings,
    IncrementalOutcome,
    IncrementalSettings,
    last_computation,
    load_coherence_computation_date,
    load_synthesis_flows,
    load_synthesis_vintages,
    _log_run,
    NODE,
    plan_coherence_contexts,
    read_contexts,
    read_source_metrics,
    _REGISTRY_ROOT,
    replacement_predicate,
    run_coherence_step,
    run_from_connections,
    run_incremental_coherence,
    save_coherence_computation_date,
    select_contexts,
    skipped_by_cadence,
    _sql_literal,
    STEP,
    synthesis_config_from_params,
    SYNTHESIS_STEP,
    write_coherence_batch,
)
# Lecture des paramètres et poignées partagées avec le script de synthèse
from scripts.compute_synthetic_scores import (  # noqa: F401
    _PARTNERS_ROOT,
    context_registry,
    load_synthesis_config,
    load_vulnerability_config,
    log_cadence_skip,
    parse_args,
    cadence_check_requested,
    result_table,
)
# Paramètres d'exécution partagés (forçage ponctuel)
from scripts.download_comtrade import load_runtime_config
from macroforecast.tracking import CapturingTracker, get_tracker
from scripts._run_report import RunScope, guarded_run, run_name, timestamp

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)


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
    synthesis_file, vulnerability_config = load_synthesis_config(), load_vulnerability_config()
    runtime_config = load_runtime_config()
    synthesis_block, block = synthesis_file["SYNTHESIS"], synthesis_file["COHERENCE"]
    bucket = synthesis_block["BUCKET"]
    # Lecture des métriques et des scores (schéma des scores), écriture des diagnostics
    scores = result_table(vulnerability_config, synthesis_block["RESULT_SCHEMA"], bucket, synthesis_block["PATHS"]["DATA_PATH"])
    diagnostics = result_table(vulnerability_config, block["RESULT_SCHEMA"], bucket, block["PATHS"]["DATA_PATH"])
    # Registres par contexte de la cohérence et de la synthèse (lecture seule)
    context_columns = synthesis_config_from_params(synthesis_block.get("PARAMETERS") or {}).context_columns
    state = context_registry(block, bucket, STEP, context_columns)
    synthesis_state = context_registry(synthesis_block, bucket, SYNTHESIS_STEP, context_columns)
    # Un seul run par exécution, tous diagnostics confondus
    tracker = CapturingTracker(
        get_tracker(
            tracking_uri=None,
            experiment=experiment_name("vulnerabilities"),
            run_name=run_name(f"vulnerabilities-coherence-{timestamp()}", NODE),
        )
    )
    scope = RunScope(NODE)
    with tracker, guarded_run(scope, tracker):
        result = run_coherence_step(
            scores, diagnostics, state, synthesis_state, params=synthesis_file,
            vulnerability_params=vulnerability_config, runtime=runtime_config, tracker=tracker,
            n_jobs=resolve_n_jobs(block.get("N_JOBS")), force=ForceSpec.from_runtime(runtime_config),
            cadence_check=cadence_check_requested(args.cadence_check), run_id=workflow_run_id(),
        )
        scope.publish_result(tracker, result)
    result.raise_if_failed()


# Exécution du script principal
if __name__ == "__main__":
    main()
