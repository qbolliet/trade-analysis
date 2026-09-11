"""Script de calcul/mise à jour des diagnostics de cohérence de la synthèse.

Second script de la synthèse, calqué sur ``scripts/compute_synthetic_scores.py``
(S-2.1). Il lit deux tables du catalogue ``vulnerabilities`` — la table des
métriques combinées (même requête que le script de synthèse, S-2.3) et la table
longue des scores produite par ce dernier (``SYNTHESIS.RESULT_SCHEMA``,
S-2.4) — appelle le runner pur
``macroforecast.trade.aggregation.run_coherence`` (D-17, aucun I/O) et écrit la
table longue des diagnostics de cohérence dans ``COHERENCE.RESULT_SCHEMA``
(schéma ``synthesis_diagnostics`` du même catalogue, D-02). Familles ``metrics``
(S-2.5.a) et ``methods`` (S-2.5.b) ; la famille ``fit`` de la même table est
écrite par le script de synthèse.

Place dans le pipeline (ordre Argo) : après ``compute_synthetic_scores.py``
(dépendance directe, couplage faible : seul son registre JSON de fraîcheur est
lu).

Réutilisation du script de synthèse. ``build_source_query``,
``synthesis_config_from_params``, ``read_source_metrics``, ``_result_connector``,
``load_synthesis_computation_date`` et ``contexts_to_recompute`` sont importés
tels quels depuis ``scripts.compute_synthetic_scores``. Ces helpers sont déjà
figés par ``tests/test_scripts_synthesis.py`` (contrat de non-régression) et le
script de synthèse est le propriétaire naturel du contrat de lecture
``SOURCES`` / ``FILTERS`` ; les importer n'introduit aucune duplication. Un
module ``scripts/_synthesis_common.py`` forcerait au contraire des retouches sur
cette suite figée et sur ``pyproject`` pour aucun gain de comportement : il n'est
pas créé.

Fraîcheur (S-2.1, v1). Même règle que la synthèse, appliquée cette fois contre le
registre de synthèse (racine ``SYNTHESIS``) : si son ``last_computed`` est
postérieur au ``last_computed`` du registre de cohérence (racine ``COHERENCE``)
— ou si ``FORCE`` est vrai — tous les contextes sélectionnés sont recalculés,
sinon rien. La date écrite dans le registre de cohérence est capturée avant le
calcul, jamais après.

Erreurs. Comme le script de synthèse : un appel ``run_coherence`` par contexte,
l'échec de l'un n'emporte pas les autres, chaque échec est capturé et journalisé,
seuls les contextes réussis sont écrits, et le script ne sort en erreur qu'en fin
de parcours.

Le suivi d'exécution MLflow est piloté par le bloc ``COHERENCE.MLFLOW`` de
``config/synthesis.yaml`` : sans ``TRACKING_URI`` (ou sans serveur joignable),
``get_tracker`` retourne un objet nul et l'exécution est strictement inchangée.
Un seul run par exécution (D-14).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import fields, replace
from datetime import datetime
import logging
import numbers
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Module d'écriture des tables de faits DuckLake (upsert par clé primaire)
from statflows.storage.ducklake.tables import FACT_TABLE, write_dataframe
# Module d'utilitaires de téléchargement (instants, parsing ISO, noms de schéma)
from statflows.core.download import _now, _parse_iso, _schema_name

# Module de manipulation de données
import numpy as np
import pandas as pd

# Module de suivi d'exécution (MLflow optionnel, objet nul par défaut)
from macroforecast.tracking import get_tracker
# Méthodologie de cohérence (fonction pure) et configuration de la synthèse
from macroforecast.trade.aggregation import (
    CoherenceConfig,
    CoherenceLevelReport,
    CoherenceRunReport,
    run_coherence,
)
# Helpers d'I/O partagés avec le premier script de la synthèse (cf. docstring)
from scripts.compute_synthetic_scores import (
    _PARTNERS_ROOT,
    _result_connector,
    build_source_query,
    contexts_to_recompute,
    load_synthesis_computation_date,
    load_synthesis_config,
    load_vulnerability_config,
    read_source_metrics,
    synthesis_config_from_params,
)

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


# Fonction de rendu d'un littéral SQL à partir d'une valeur scalaire
def _sql_literal(value: Any) -> str:
    """Render one scalar as a DuckDB SQL literal.

    Strings are single-quoted with quote doubling; booleans become ``TRUE`` /
    ``FALSE``; integers and floats are emitted verbatim; anything else falls
    back to its quoted string form. ``numpy`` scalars are handled through the
    ``numbers`` ABCs.

    Args:
        value: Scalar drawn from a context tuple.

    Returns:
        The SQL literal.

    Examples:
        >>> _sql_literal("VALUE_IN_EUROS")
        "'VALUE_IN_EUROS'"
        >>> _sql_literal(1)
        '1'
        >>> _sql_literal("d'Ivoire")
        "'d''Ivoire'"
    """
    # Chaîne : guillemets simples, apostrophes doublées
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    # Booléen avant l'entier (bool est sous-classe de int)
    if isinstance(value, (bool, np.bool_)):
        return "TRUE" if value else "FALSE"
    # Entier puis réel, via les ABC `numbers` (couvre les scalaires numpy)
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, numbers.Real):
        return repr(float(value))
    # Repli : forme chaîne échappée
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


# Fonction de construction de la requête de lecture des scores
def build_scores_query(
    catalog_alias: str,
    schema: str,
    context_columns: Sequence[str],
    contexts: Sequence[Sequence[Any]],
) -> str:
    """Build the query reading the score table, restricted to ``contexts`` (S-2.4).

    Pure function, no database connection. The table is read whole
    (``SELECT *``) — the runner projects the columns it needs — and filtered by
    a row-value ``IN`` list on the context key, so a single query covers every
    selected context. An empty ``contexts`` yields a ``WHERE FALSE`` guard
    rather than an unfiltered scan.

    Args:
        catalog_alias: DuckLake catalog alias the fact table lives in.
        schema: Result schema of the scores (``SYNTHESIS.RESULT_SCHEMA``).
        context_columns: Ordered context key columns.
        contexts: Context tuples to keep, each in the order of
            ``context_columns``.

    Returns:
        The SQL query as a string.

    Examples:
        >>> print(build_scores_query(
        ...     "vulnerabilities", "synthesis", ["freq", "flow"],
        ...     [("A", 1), ("A", 2)],
        ... ))
        SELECT * FROM "vulnerabilities"."synthesis"."fact_table"
        WHERE ("freq", "flow") IN (
            ('A', 1),
            ('A', 2)
          )
    """
    table = f'"{catalog_alias}"."{schema}"."{FACT_TABLE}"'
    base = f"SELECT * FROM {table}"
    # Aucun contexte : garde-fou explicite plutôt qu'un balayage complet
    if not contexts:
        return f"{base}\nWHERE FALSE"
    columns = ", ".join(f'"{column}"' for column in context_columns)
    tuples = ",\n    ".join(
        "(" + ", ".join(_sql_literal(value) for value in context) + ")"
        for context in contexts
    )
    return f"{base}\nWHERE ({columns}) IN (\n    {tuples}\n  )"


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
) -> Tuple[List[CoherenceRunReport], Dict[str, Exception], bool, int]:
    """Read the metrics and scores, run the coherence per context, and write diagnostics.

    Isolates the DB-bound core of :func:`main` — the metrics read, the
    context-restricted scores read (S-2.4) and the context-by-context call to
    ``run_coherence`` writing the S-2.6 table — from connection setup
    (``DuckLakeConnector.from_postgres``, environment variables) and freshness
    bookkeeping, so it is callable on any pair of already-open connections,
    tests included. As in :func:`main`, the failure of one context does not
    interrupt the others.

    Args:
        read_conn: Open connection positioned to read the metrics
            (``source_query``) and the scores (``scores_schema``).
        diagnostics_conn: Open connection to write the ``diagnostics_schema``
            fact table (``metrics`` and ``methods`` families).
        source_query: Metrics query built by
            :func:`scripts.compute_synthetic_scores.build_source_query`
            (S-2.3, same as the synthesis script).
        scores_schema: Schema the score table (S-2.4) lives in.
        synthesis_config: Configuration the scores were produced with.
        coherence_config: Coherence methodological configuration.
        catalog_alias: DuckLake catalog alias both connections are attached to.
        diagnostics_schema: Target schema of the coherence diagnostics.
        tracker: Experiment tracker; the null tracker by default.
        log_artifacts: Whether to log the S-2.7 artifacts to the tracker.

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

    context_columns = list(synthesis_config.context_columns)
    diagnostics_keys = [
        *context_columns,
        "level",
        synthesis_config.reporter_col,
        synthesis_config.product_col,
        "family",
        "statistic",
        "item_a",
        "item_b",
    ]

    # Lecture de la table des métriques combinées (une seule requête)
    df_metrics = read_source_metrics(read_conn, source_query)
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
    df_scores = read_source_metrics(read_conn, scores_query)
    logger.info(f"{len(df_scores)} ligne(s) de scores lue(s).")

    reports: List[CoherenceRunReport] = []
    failures: Dict[str, Exception] = {}
    created_any = False
    n_contexts = 0

    with tracker:
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
            # Sous-ensemble des scores du contexte courant
            mask = pd.Series(True, index=df_scores.index)
            for column, value in zip(context_columns, context):
                mask &= df_scores[column] == value
            df_scores_context = df_scores.loc[mask]

            try:
                # Calcul pur des diagnostics de cohérence du contexte
                df_diagnostics, report = run_coherence(
                    df_context,
                    df_scores_context,
                    synthesis_config,
                    coherence_config,
                    tracker=tracker,
                    log_artifacts=log_artifacts,
                )

                # Écriture de la table longue (schéma « synthesis_diagnostics »,
                # familles « metrics » et « methods », clé S-2.6)
                created = write_dataframe(
                    diagnostics_conn,
                    df_diagnostics,
                    diagnostics_keys,
                    catalog_alias=catalog_alias,
                    schema=diagnostics_schema,
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

        # Métriques et tags de l'exécution : un rapport agrégé sur tous
        # les contextes réussis
        aggregate = _aggregate_reports(reports)
        aggregate.created = created_any
        tracker.log_metrics(aggregate.to_metrics())
        tracker.set_tags(
            {
                "result_schema": diagnostics_schema,
                "n_contexts": str(len(reports)),
                "created": str(created_any),
            }
        )

    return reports, failures, created_any, n_contexts


# ──────────────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────

# Fonction principale de calcul des diagnostics de cohérence
def main() -> None:
    """CLI entry point for the incremental coherence-diagnostics computation.

    Raises:
        RuntimeError: If at least one context failed, once every context has
            been attempted.
    """
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

    # Options de suivi d'exécution (un seul run par exécution, D-14)
    mlflow_config = coherence_config_block.get("MLFLOW") or {}
    log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))
    tracker = get_tracker(
        tracking_uri=mlflow_config.get("TRACKING_URI"),
        experiment=mlflow_config.get("EXPERIMENT", "vulnerabilities-coherence"),
        run_name=f"vulnerabilities-coherence-{datetime.now():%Y%m%d-%H%M}",
    )

    # Identité du catalogue partagé et schémas résultat
    catalog = vulnerability_config[_PARTNERS_ROOT]
    catalog_alias = catalog["CATALOG_ALIAS"]
    scores_schema = _schema_name(synthesis_config_block["RESULT_SCHEMA"])
    diagnostics_schema = _schema_name(coherence_config_block["RESULT_SCHEMA"])

    # Initialisation des loaders et savers
    loader = Loader()
    saver = Saver()

    # Fraîcheur : instant de calcul de synthèse (amont) contre instant de cohérence
    last_upstream = load_synthesis_computation_date(
        Path(synthesis_config_block["PATHS"]["LAST_COMPUTATION_PATH"]),
        loader=loader,
        bucket=synthesis_config_block["BUCKET"],
    )
    last_coherence = load_coherence_computation_date(
        Path(coherence_config_block["PATHS"]["LAST_COMPUTATION_PATH"]),
        loader=loader,
        bucket=synthesis_config_block["BUCKET"],
    )
    force = bool(coherence_config_block.get("FORCE", False))

    # Sortie anticipée : registre de synthèse inchangé depuis la dernière cohérence
    if not contexts_to_recompute(last_upstream, last_coherence, force):
        logger.info(
            "Registre de synthèse inchangé depuis la dernière cohérence "
            f"(synthèse={last_upstream}, cohérence={last_coherence}), "
            "rien à recalculer."
        )
        return

    # Instant de référence capturé avant le calcul : la date enregistrée
    # correspond au début du traitement, jamais après, pour ne pas rater une
    # mise à jour survenue pendant le calcul
    computed_at = _now()

    # Requête source : même combinaison SQL que le script de synthèse (S-2.3)
    source_query = build_source_query(
        synthesis_config_block["SOURCES"],
        synthesis_config_block.get("FILTERS") or {},
        catalog_alias,
    )
    # Logging
    logger.info(f"Requête des métriques :\n{source_query}")

    # Connecteurs DuckLake : un pour la lecture (métriques + scores, schéma des
    # scores), un pour l'écriture des diagnostics ; tous deux sur le catalogue
    # partagé « vulnerabilities », bucket de la synthèse
    read_connector = _result_connector(
        vulnerability_config,
        bucket=synthesis_config_block["BUCKET"],
        data_path=synthesis_config_block["PATHS"]["DATA_PATH"],
        schema=scores_schema,
    )
    diagnostics_connector = _result_connector(
        vulnerability_config,
        bucket=synthesis_config_block["BUCKET"],
        data_path=coherence_config_block["PATHS"]["DATA_PATH"],
        schema=diagnostics_schema,
    )

    # Ouverture des connexions : leur cycle de vie appartient au script, le
    # runner ne les ouvre ni ne les ferme (`run_from_connections` orchestre
    # lecture / calcul / écriture sur des connexions déjà ouvertes)
    read_conn = read_connector.connect()
    try:
        diagnostics_conn = diagnostics_connector.connect()
        try:
            reports, failures, created_any, n_contexts = run_from_connections(
                read_conn,
                diagnostics_conn,
                source_query,
                scores_schema,
                synthesis_config,
                coherence_config,
                catalog_alias=catalog_alias,
                diagnostics_schema=diagnostics_schema,
                tracker=tracker,
                log_artifacts=log_artifacts,
            )
        finally:
            diagnostics_conn.close()
    finally:
        read_conn.close()

    # Mise à jour du registre de cohérence, uniquement si au moins un contexte a
    # réussi (cohérence : jamais de date avancée à tort)
    if reports:
        save_coherence_computation_date(
            Path(coherence_config_block["PATHS"]["LAST_COMPUTATION_PATH"]),
            {
                "last_computed": computed_at.isoformat(),
                "n_groups": int(sum(report.n_groups for report in reports)),
                "n_contexts": int(len(reports)),
            },
            saver,
            synthesis_config_block["BUCKET"],
        )

    # Échec global si au moins un contexte a échoué, une fois tous tentés
    if failures:
        raise RuntimeError(
            f"{len(failures)} contexte(s) en échec sur {n_contexts} : "
            f"{sorted(failures)}"
        ) from next(iter(failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
