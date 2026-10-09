"""Script de calcul/mise à jour des scores synthétiques de vulnérabilité.

Enveloppe CLI de la fonction d'étape ``kedro_pipeline.steps.synthesis.run_synthesis_step`` :
combinaison des indices partenaires (``indicators``) et de réseau
(``network_indicators``) en scores synthétiques, une ligne par cellule
``reporter x product`` d'un contexte ``(hs_vintage, freq, flow, indicators,
TIME_PERIOD)`` et par méthode d'agrégation, avec un couple ``(score, rang)`` par
niveau de comparaison. Fraîcheur par contexte et par méthode ; calcul des
contextes dans des processus de travail, écriture par lots dans le processus
parent ; diagnostics d'ajustement (famille ``fit``) dans ``synthesis_diagnostics``.

Ce script charge les paramètres, construit les poignées des tables de scores et
de diagnostics (identifiants lus à la première connexion), les registres (synthèse
en lecture/écriture, partenaires et réseau en lecture seule), le nombre de
processus (``N_JOBS``, à défaut ``NUM_CPU``) et un seul run MLflow, appelle
l'étape, publie le rapport de run puis sort en erreur si un contexte a échoué.
``--cadence-check`` (ou ``CADENCE_CHECK=1``) fait sortir le script sans rien
calculer si la dernière exécution date de moins de ``CADENCE.MIN_INTERVAL_DAYS``
jours.

Configuration lue : blocs ``synthesis``, ``vulnerabilities``, ``eurostat`` et
``runtime`` des paramètres Kedro, environnement choisi par ``KEDRO_ENV``.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import argparse
import logging
import os
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    DuckLakeTable,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
    workflow_run_id,
)
from kedro_pipeline.io.freshness import ForceSpec
from kedro_pipeline.parallel import resolve_n_jobs
from kedro_pipeline.config import experiment_name, load_parameters, read_config_file
from kedro_pipeline.steps._config import schema_name, vulnerabilities_location
from kedro_pipeline.steps.partners import partner_classification
# Étape de synthèse (ré-exportée : requêtes, fraîcheur, calcul, écriture)
from kedro_pipeline.steps.synthesis import (  # noqa: F401
    _aggregate_reports,
    build_contexts_query,
    build_scores_query,
    build_source_query,
    chunks,
    compute_context_task,
    compute_synthesis_context,
    CONSENSUS_FINGERPRINT,
    context_entry,
    context_freshness_tags,
    context_in_predicate,
    context_registry,
    context_unit,
    ContextResult,
    contexts_to_recompute,
    default_recent_periods,
    expand_synthesis_names,
    _FAMILY_COLUMN,
    _FLOW_COLUMN,
    freshness_tags,
    _FULL_CHANGE_REASONS,
    global_entry,
    global_legacy_parser,
    global_registry,
    GLOBAL_UNIT,
    _grid_where_parts,
    _IN_FORCE_COLUMN,
    incremental_settings,
    IncrementalOutcome,
    IncrementalSettings,
    INPUTS_FINGERPRINT,
    _ITEM_A_COLUMN,
    iter_upstream_registries,
    last_computation,
    load_max_upstream_computation,
    load_synthesis_computation_date,
    load_synthesis_flows,
    load_synthesis_vintages,
    _log_run,
    _merge_phase_reports,
    _METHOD_COLUMN,
    method_fingerprint_params,
    _NETWORK_ROOT,
    NODE,
    _PARTNERS_ROOT,
    _PERIOD_COLUMN,
    plan_global_unit,
    plan_methods,
    plan_synthesis_contexts,
    read_contexts,
    read_source_metrics,
    recent_period_values,
    _REGISTRY_ROOT,
    replacement_predicate,
    run_from_connections,
    run_incremental_synthesis,
    _run_result,
    run_synthesis_step,
    save_synthesis_computation_date,
    select_contexts,
    skip_by_cadence,
    skipped_by_cadence,
    _sql_list,
    _sql_literal,
    STEP,
    summarize_registries,
    synthesis_config_from_params,
    synthesis_context_requested,
    _SYNTHESIS_FINGERPRINT_EXCLUDED,
    synthesis_requested,
    _synthesise_contexts,
    SynthesisTask,
    TaskOutcome,
    upstream_registries,
    upstream_registry_groups,
    UpstreamMarks,
    validate_flow_context,
    _VINTAGE_COLUMN,
    VINTAGE_MODES,
    write_synthesis_batch,
)
# Paramètres d'exécution partagés (nomenclatures, forçage ponctuel)
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


# Fabrique de connecteur : identifiants lus dans l'environnement à la première connexion
def _connector(location: DuckLakeLocation, pg: Any = None, s3: Any = None, **kwargs: Any) -> Any:
    """Build the connector of a table, reading the credentials only when connecting."""
    return build_connector(location, pg_credentials_from_env(), s3_credentials_from_env(), **kwargs)


# Fonction de construction de la poignée d'une table du catalogue des vulnérabilités
def result_table(
    vulnerability_config: Mapping[str, Any], result_schema: str, bucket: Optional[str], data_path: str
) -> DuckLakeTable:
    """Lazy handle of a result table of the shared ``vulnerabilities`` catalog.

    Args:
        vulnerability_config: The ``vulnerabilities`` parameters (catalog identity).
        result_schema: Configured result schema.
        bucket: S3 bucket of the data.
        data_path: Data path of the table.

    Returns:
        The handle (credentials read at the first connection).
    """
    location = vulnerabilities_location(
        vulnerability_config, schema=schema_name(result_schema), bucket=bucket, data_path=data_path
    )
    return DuckLakeTable.lazy(location, None, None, connector_factory=_connector)


# Fonction de chargement de la configuration de synthèse
def load_synthesis_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the synthesis configuration.

    Args:
        config_path: Explicit YAML file (``synthesis`` root key or historical
            format without it). If ``None``, the ``synthesis`` block of the
            Kedro parameters of the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary (blocks ``SYNTHESIS`` and ``COHERENCE``).
    """
    if config_path is None:
        return load_parameters()["synthesis"]
    return read_config_file(config_path, "synthesis")

# Fonction de chargement de la configuration dédiée au calcul des vulnérabilités
def load_vulnerability_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the vulnerability-computation configuration.

    Read for three things only: the catalog identity shared by the two upstream
    families (``VULNERABILITIES.DBNAME`` / ``VULNERABILITIES.CATALOG_ALIAS``, the
    same catalog the synthesis writes into), and the paths of the two upstream
    freshness registries (partners and network). No methodological parameter of
    the metric computation is used here.

    Args:
        config_path: Explicit YAML file (``vulnerabilities`` root key or
            historical format without it). If ``None``, the ``vulnerabilities``
            block of the Kedro parameters of the ``KEDRO_ENV`` environment.

    Returns:
        dict: Configuration dictionary.
    """
    if config_path is None:
        return load_parameters()["vulnerabilities"]
    return read_config_file(config_path, "vulnerabilities")

# Fonction de lecture de la demande de contrôle de cadence
def cadence_check_requested(flag: bool = False, environ: Optional[Mapping[str, str]] = None) -> bool:
    """Tell whether the run must honour the minimum interval between two runs.

    Args:
        flag: The ``--cadence-check`` command-line flag.
        environ: Environment (``os.environ`` by default); ``CADENCE_CHECK``
            set to ``1`` / ``true`` / ``yes`` / ``on`` requests the check.

    Returns:
        Whether the cadence is checked.

    Examples:
        >>> cadence_check_requested(False, {"CADENCE_CHECK": "1"})
        True
        >>> cadence_check_requested(False, {})
        False
    """
    environ = os.environ if environ is None else environ
    value = (environ.get("CADENCE_CHECK") or "").strip().lower()
    return bool(flag) or value in {"1", "true", "yes", "on"}

# Fonction de lecture des arguments de la ligne de commande
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse the command line of the per-context steps (synthesis, coherence).

    Args:
        argv: Arguments (``sys.argv[1:]`` when ``None``).

    Returns:
        Namespace with ``cadence_check``.

    Examples:
        >>> parse_args(["--cadence-check"]).cadence_check
        True
    """
    parser = argparse.ArgumentParser(
        description="Incremental per-context step of the synthesis (scores or coherence)."
    )
    parser.add_argument(
        "--cadence-check", action="store_true",
        help="exit at once (code 0) when the step ran less than "
             "CADENCE.MIN_INTERVAL_DAYS ago and no recomputation is forced "
             "(also requested by CADENCE_CHECK=1)",
    )
    return parser.parse_args(argv)

# Fonction de lecture optionnelle de la configuration de téléchargement Eurostat
def _optional_eurostat_config() -> Optional[Mapping[str, Any]]:
    """Read the Eurostat download configuration, ``None`` when absent.

    It only provides the default of ``RECENT_PERIODS`` (depth of the
    incremental download).
    """
    from scripts.compute_trade_vulnerabilities import load_eurostat_config

    try:
        return load_eurostat_config()
    except (FileNotFoundError, KeyError):
        # Bloc eurostat absent des paramètres de l'environnement
        return None

# Fonction de journalisation d'une exécution sautée par cadence
def log_cadence_skip(tracker: Any, step: str, last: Optional[datetime]) -> None:
    """Log the metric and tag of a run skipped by the cadence check.

    Args:
        tracker: Experiment tracker of the run.
        step: Step name.
        last: Most recent computation of the step.
    """
    with tracker:
        tracker.log_metrics({"freshness/skipped_by_cadence": 1.0})
        tracker.set_tags({"freshness_reasons": "cadence"})
    # Logging
    logger.info(f"Étape '{step}' calculée le {last} : exécution sautée (cadence).")



# Fonction principale de calcul des scores synthétiques
def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point for the incremental synthetic-score computation.

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` when ``None``).

    Raises:
        RuntimeError: If at least one context failed, once every context has
            been attempted.
    """
    args = parse_args(argv)
    synthesis_file, vulnerability_config = load_synthesis_config(), load_vulnerability_config()
    runtime_config = load_runtime_config()
    block, coherence_block = synthesis_file["SYNTHESIS"], synthesis_file["COHERENCE"]
    scores = result_table(vulnerability_config, block["RESULT_SCHEMA"], block["BUCKET"], block["PATHS"]["DATA_PATH"])
    diagnostics = result_table(
        vulnerability_config, coherence_block["RESULT_SCHEMA"], block["BUCKET"], coherence_block["PATHS"]["DATA_PATH"]
    )
    # Registres : synthèse par contexte (fragment = période), amont par famille (lecture seule)
    context_columns = synthesis_config_from_params(block.get("PARAMETERS") or {}).context_columns
    state = context_registry(block, block["BUCKET"], STEP, context_columns)
    partners, network = upstream_registry_groups(
        vulnerability_config, partner_classification(runtime_config["NOMENCLATURES"]["HS"])
    )
    # Un seul run par exécution, toutes méthodes confondues
    tracker = CapturingTracker(
        get_tracker(
            tracking_uri=None,
            experiment=experiment_name("vulnerabilities"),
            run_name=run_name(f"vulnerabilities-synthesis-{timestamp()}", NODE),
        )
    )
    scope = RunScope(NODE)
    with tracker, guarded_run(scope, tracker):
        result = run_synthesis_step(
            scores, diagnostics, state, partners, network, params=synthesis_file,
            vulnerability_params=vulnerability_config, runtime=runtime_config, tracker=tracker,
            n_jobs=resolve_n_jobs(block.get("N_JOBS")), eurostat=_optional_eurostat_config(),
            force=ForceSpec.from_runtime(runtime_config),
            cadence_check=cadence_check_requested(args.cadence_check), run_id=workflow_run_id(),
        )
        scope.publish_result(tracker, result)
    result.raise_if_failed()


# Exécution du script principal
if __name__ == "__main__":
    main()
