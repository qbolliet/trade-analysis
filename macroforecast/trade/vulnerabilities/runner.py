"""Vulnerability computation runner.

Iterates the registered vulnerability metrics over a trade DuckLake fact table —
whole, or restricted to a set of reporter x product pairs — and writes the
scores to a result schema: one column per metric, keyed by
``date x nomenclature x indicator x flow x reporter`` (plus frequency).

Source and result are reached through DuckDB connections opened by the caller.
The result schema is built on
first encounter and upserted afterwards, through the shared
:mod:`macroforecast.storage2.tables` helpers also used by
:mod:`macroforecast.datasets.core.download`.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
from typing import Any, Collection, Iterable, Optional, Sequence, Tuple
# Modules de manipulation de données
import duckdb
import narwhals as nw
import pandas as pd
# Modules de gestion des tables DuckLake
from ...storage2.tables import FACT_TABLE, fact_table_exists, write_dataframe
# Modules du package
from ...tracking import NULL_TRACKER, RunTracker, run_params
from .base import DEFAULT_CONFIG, VulnerabilityConfig, VulnerabilityMetric
from .diagnostics import (
    VulnerabilityReport,
    compute_coverage_report,
    compute_distribution_reports,
    compute_drift_report,
    compute_input_report,
    compute_quality_report,
    log_vulnerability_artifacts,
)
from .metrics import default_metrics

# Initialisation du logger
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Calcul (backend-agnostique, narwhals)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'application de toutes les métriques sur un jeu de données
def compute_vulnerabilities(
    data: nw.DataFrame,
    metrics: Sequence[VulnerabilityMetric],
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    *,
    df_previous: Optional[nw.DataFrame] = None,
) -> Tuple[nw.DataFrame, VulnerabilityReport]:
    """Compute every metric and assemble a one-column-per-metric frame.

    Builds the canonical grid of distinct cells and left-joins each metric's
    output onto it, so cells a metric does not score (e.g. CDI2/CDI3 on non-import
    flows) carry a null value. Alongside the scores, the run is diagnosed
    (volumetry, coverage, aggregate coherence, distributions and — when a
    previous result is supplied — drift), the diagnostics being data rather than
    logs.

    Args:
        data: Narwhals frame of partner-level observations.
        metrics: Metric instances to apply.
        config: Column conventions (its ``key_columns`` define the grid).
        df_previous: Result of the previous run, same schema, enabling the
            run-to-run stability diagnostics. Reading it belongs to the caller
            (see :func:`read_previous_result`).

    Returns:
        Tuple ``(df_result, report)``: the scores — ``config.key_columns`` plus
        one column per metric — and the :class:`VulnerabilityReport` of the run
        (``created`` is left ``False``; the caller sets it once the result is
        persisted).

    Raises:
        ValueError: If the input frame is missing any column required by one of
            the metrics (see :meth:`VulnerabilityMetric.required_columns`).

    Examples:
        >>> import pandas as pd
        >>> from macroforecast.trade.vulnerabilities import (
        ...     HerfindahlHirschmanIndex, VulnerabilityConfig)
        >>> df = pd.DataFrame({
        ...     "flow": [1, 1, 1], "partner": ["CN", "US", "WORLD"],
        ...     "OBS_VALUE": [60.0, 40.0, 100.0],
        ... })
        >>> config = VulnerabilityConfig(key_columns=("flow",))
        >>> data = nw.from_native(df, eager_only=True)
        >>> scores, report = compute_vulnerabilities(
        ...     data, [HerfindahlHirschmanIndex(config)], config)
        >>> round(float(scores.to_native()["HHI"][0]), 2), report.cells
        (0.52, 1)
    """
    # Clés de la grille de sortie
    keys = list(config.key_columns)

    # Validation de schéma (fail-fast) : union des colonnes exigées par les métriques,
    # confrontée aux colonnes disponibles
    available = set(data.columns)
    required = set().union(*(metric.required_columns() for metric in metrics))
    missing = required - available
    if missing:
        # Métriques concernées par au moins une colonne manquante
        culprits = sorted(
            metric.name
            for metric in metrics
            if metric.required_columns() & missing
        )
        raise ValueError(
            f"Missing required column(s) {sorted(missing)} for metric(s) "
            f"{culprits}. Available columns: {sorted(available)}."
        )

    # Volumétrie d'entrée, mesurée avant tout filtrage
    report = VulnerabilityReport(metrics=[metric.name for metric in metrics])
    report.input = compute_input_report(data, config)
    n_input = report.input.n_observations

    # Suppression des observations inexploitables (partenaire ou valeur nuls) :
    # un partenaire nul fausse les masques booléens du filtre des pays individuels.
    data = data.drop_nulls(subset=[config.partner_col, config.value_col])
    # Effet du filtrage, rapporté plutôt que silencieux
    share_rows_dropped_null = (
        (n_input - len(data)) / n_input if n_input else float("nan")
    )

    # Grille canonique : cellules distinctes de la base
    df_grid = data.select(*keys).unique()
    result = df_grid

    # Jointure gauche de la sortie de chaque métrique sur la grille
    for metric in metrics:
        # Calcul de la métrique (frame indexé par les clés + colonne metric.name)
        scored = metric.compute(data)
        result = result.join(scored, on=keys, how="left")

    # Diagnostics de l'exécution
    report.cells = len(result)
    report.coverage = compute_coverage_report(result, metrics)
    report.quality = compute_quality_report(
        data, df_grid, config, share_rows_dropped_null=share_rows_dropped_null
    )
    report.distributions = compute_distribution_reports(result, metrics, config)
    # Dérive : seulement si l'exécution précédente a été relue par l'appelant
    if df_previous is not None:
        report.drift = compute_drift_report(result, df_previous, metrics, config)

    return result, report

# ──────────────────────────────────────────────────────────────────────
# Lecture de la source / relecture du résultat (DuckLake)
# ──────────────────────────────────────────────────────────────────────

# Fonction de mise en forme d'un littéral SQL
def _sql_literal(value: str) -> str:
    """Return a single-quoted SQL literal, embedded quotes doubled.

    Args:
        value: Raw value to quote.

    Returns:
        The value as a SQL string literal.

    Examples:
        >>> _sql_literal("FR")
        "'FR'"
        >>> _sql_literal("O'Brien")
        "'O''Brien'"
    """
    # Doublement des quotes simples : seul échappement admis par DuckDB
    return "'" + str(value).replace("'", "''") + "'"


# Fonction de construction du prédicat SQL sur les couples reporter x produit
def _reporter_product_predicate(
    reporter_col: str,
    product_col: str,
    reporters_products: Iterable[Tuple[str, str]],
) -> str:
    """Build the SQL predicate restricting a query to reporter x product pairs.

    Shared by the source read and the previous-result read, which must scope
    themselves to exactly the same perimeter for the drift diagnostics to
    compare comparable things.

    Args:
        reporter_col: Name of the reporter column.
        product_col: Name of the product column.
        reporters_products: Reporter x product pairs to restrict to.

    Returns:
        A SQL boolean expression of the form ``("reporter", "product") IN (...)``.

    Examples:
        >>> print(_reporter_product_predicate("reporter", "product", [("FR", "27")]))
        ("reporter", "product") IN (('FR', '27'))
    """
    # Liste des couples ciblés, littéraux échappés
    values_clause = ", ".join(
        f"({_sql_literal(reporter)}, {_sql_literal(product)})"
        for reporter, product in reporters_products
    )
    return f'("{reporter_col}", "{product_col}") IN ({values_clause})'


# Fonction de lecture de la table de faits source
def _read_source_fact_table(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    source_schema: str,
    columns: Sequence[str],
    *,
    reporter_col: str,
    product_col: str,
    reporters_products: Optional[Sequence[Tuple[str, str]]] = None,
) -> pd.DataFrame:
    """Read the projected source fact table, optionally restricted to a perimeter.

    Uses a connection already attached to the catalog, whatever its backend: the
    source schema is reached by qualified name, no extra ``ATTACH`` is required.

    Args:
        conn: Open DuckLake connection, owned by the caller.
        catalog_alias: Alias under which the catalog is attached.
        source_schema: Schema holding the source ``fact_table``.
        columns: Columns to project.
        reporter_col: Name of the reporter column.
        product_col: Name of the product column.
        reporters_products: Reporter x product pairs to restrict the read to.
            ``None`` reads the whole fact table.

    Returns:
        A pandas DataFrame of the projected (and possibly filtered) fact table.
    """
    # Projection des colonnes demandées
    col_list = ", ".join(f'"{c}"' for c in columns)
    query = (
        f'SELECT {col_list} '
        f'FROM "{catalog_alias}"."{source_schema}"."{FACT_TABLE}"'
    )
    # Restriction éventuelle au périmètre recalculé
    if reporters_products:
        query += " WHERE " + _reporter_product_predicate(
            reporter_col, product_col, reporters_products
        )
    return conn.execute(query).df()


# Fonction de lecture du résultat de l'exécution précédente (diagnostics de dérive)
def read_previous_result(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    result_schema: str,
    *,
    reporters_products: Optional[Sequence[Tuple[str, str]]] = None,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
) -> Optional[nw.DataFrame]:
    """Read the previous run's scores, for the run-to-run drift diagnostics.

    Reading belongs to the caller: the runner never decides on its own to
    re-read the result table. This helper only spares the callers the
    duplication of the SQL, and degrades gracefully — a result table that does
    not exist yet returns ``None``, which disables the drift diagnostics rather
    than failing the run.

    Args:
        conn: Open DuckLake connection on the result catalog, owned by the
            caller.
        catalog_alias: Alias under which the result catalog is attached.
        result_schema: Schema holding the result ``fact_table``.
        reporters_products: Reporter x product pairs to restrict the read to,
            mirroring the perimeter of an incremental recomputation. ``None``
            reads the whole table.
        config: Column conventions (``reporter_col`` / ``product_col`` name the
            filtered columns).

    Returns:
        Narwhals frame of the previous scores, or ``None`` when no result table
        exists yet.
    """
    # Première exécution : aucune table résultat, donc aucune dérive mesurable
    if not fact_table_exists(conn, catalog_alias, result_schema):
        # Logging
        logger.info(
            f"No result table in '{result_schema}' yet: drift diagnostics skipped"
        )
        return None

    # Projection intégrale : la table résultat est étroite (clés + métriques)
    query = f'SELECT * FROM "{catalog_alias}"."{result_schema}"."{FACT_TABLE}"'
    # Restriction éventuelle au périmètre recalculé
    if reporters_products:
        query += " WHERE " + _reporter_product_predicate(
            config.reporter_col, config.product_col, reporters_products
        )
    # Exécution de la requête
    previous_pdf = conn.execute(query).df()

    # Logging
    logger.info(f"Read {len(previous_pdf)} previous result rows from '{result_schema}'")
    return nw.from_native(previous_pdf, eager_only=True)


# ──────────────────────────────────────────────────────────────────────
# Orchestration de bout en bout
# ──────────────────────────────────────────────────────────────────────

# Fonction de bascule vers un backend eager natif
def _to_native(source_pdf: pd.DataFrame, backend: str) -> Any:
    """Convert a pandas frame to the requested native eager backend.

    Args:
        source_pdf: Frame read from the catalog.
        backend: ``"pandas"``, ``"polars"`` or ``"pyarrow"``.

    Returns:
        The frame in its native form for the requested backend.
    """
    # Imports paresseux : polars et pyarrow sont des dépendances optionnelles
    if backend == "polars":
        import polars as pl

        return pl.from_pandas(source_pdf)
    if backend == "pyarrow":
        import pyarrow as pa

        return pa.Table.from_pandas(source_pdf)
    return source_pdf


# Fonction d'orchestration : table de faits source → métriques → schéma résultat
def run_vulnerabilities(
    source_conn: duckdb.DuckDBPyConnection,
    *,
    source_catalog_alias: str,
    source_schema: str,
    result_schema: str,
    result_conn: Optional[duckdb.DuckDBPyConnection] = None,
    result_catalog_alias: Optional[str] = None,
    reporters_products: Optional[Collection[Tuple[str, str]]] = None,
    metrics: Optional[Sequence[VulnerabilityMetric]] = None,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    backend: str = "pandas",
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
    df_previous: Optional[nw.DataFrame] = None,
) -> VulnerabilityReport:
    """Compute trade-vulnerability metrics and write them to a result schema.

    Reads the source fact table — whole, or restricted to a set of
    reporter x product pairs — applies every metric, and persists the scores
    (one column per metric, keyed by ``config.key_columns``) into the result
    schema. Initialisation and incremental update are the same call: the result
    schema is built on first encounter and upserted by primary key afterwards
    (see :func:`macroforecast.storage2.write_dataframe`).

    Connections are passed in and are **never opened or closed here**: their
    lifecycle belongs to the caller. Nothing assumes a particular catalog
    backend — a local ``.ducklake`` file and a PostgreSQL-backed catalog are
    driven identically. Source and result may live in two schemas of the same
    catalog (a single connection, the default) or in two distinct catalogs
    (``result_conn`` and ``result_catalog_alias``).

    Args:
        source_conn: Open DuckLake connection on the source catalog.
        source_catalog_alias: Alias under which the source catalog is attached.
        source_schema: Schema holding the source ``fact_table``.
        result_schema: Schema to create or upsert the scores into.
        result_conn: Open connection on the result catalog, when it differs from
            the source one. Defaults to ``source_conn``.
        result_catalog_alias: Alias of the result catalog. Required whenever
            ``result_conn`` is supplied; defaults to ``source_catalog_alias``.
        reporters_products: Reporter x product pairs to (re)compute. ``None``
            recomputes the whole fact table (initialisation, notebooks); an
            empty collection is a no-op (nothing is read or written).
        metrics: Metric instances to apply. Defaults to
            :func:`~macroforecast.trade.vulnerabilities.metrics.default_metrics`.
        config: Column and partner-code conventions.
        backend: Native eager backend for narwhals computation (``"pandas"``
            or, when installed, ``"polars"``/``"pyarrow"``).
        tracker: Experiment tracker receiving the run parameters and artifacts.
            Defaults to the null tracker, so an unconfigured run behaves exactly
            as before. The *metrics* are left to the caller, which sends
            ``report.to_metrics()`` once the report is complete.
        log_artifacts: Whether to build and send the business artifacts (top
            vulnerable cells, alert counts, missing aggregates, deciles).
        df_previous: Result of the previous run over the same perimeter,
            enabling the drift diagnostics (see :func:`read_previous_result`).

    Returns:
        A :class:`VulnerabilityReport` summarising the run.

    Raises:
        ValueError: If ``result_conn`` is supplied without
            ``result_catalog_alias``, or if the input frame is missing a column
            required by one of the metrics.
    """
    # Initialisation de la liste des métriques
    metric_list = list(metrics) if metrics is not None else default_metrics(config)

    # Périmètre vide (distinct de None, qui vaut « tout le catalogue ») :
    # rien à recalculer, sortie anticipée sans toucher à la base
    if reporters_products is not None and not reporters_products:
        logger.info("No reporter-product pairs to recalculate; early termination.")
        return VulnerabilityReport(
            cells=0, metrics=[metric.name for metric in metric_list], created=False
        )

    # Catalogue résultat : partagé avec la source par défaut. Une connexion
    # distincte impose de nommer son alias, qui n'est pas déductible.
    if result_conn is not None and result_catalog_alias is None:
        raise ValueError(
            "result_catalog_alias is required when result_conn is supplied"
        )
    result_conn = result_conn if result_conn is not None else source_conn
    result_catalog_alias = result_catalog_alias or source_catalog_alias

    # Paramètres de l'exécution : configuration aplatie et contexte
    tracker.log_params(
        run_params(
            config,
            {
                "source_schema": source_schema,
                "result_schema": result_schema,
                "backend": backend,
                "metrics": [metric.name for metric in metric_list],
                "n_reporter_product_pairs": (
                    len(reporters_products) if reporters_products is not None else None
                ),
            },
        )
    )

    # Colonnes nécessaires : clés de la grille + partenaire + valeur
    required = list(
        dict.fromkeys([*config.key_columns, config.partner_col, config.value_col])
    )

    # Lecture de la table de faits source (pandas), filtrée le cas échéant
    source_pdf = _read_source_fact_table(
        source_conn,
        source_catalog_alias,
        source_schema,
        required,
        reporter_col=config.reporter_col,
        product_col=config.product_col,
        reporters_products=(
            sorted(reporters_products) if reporters_products is not None else None
        ),
    )

    # Calcul des métriques via narwhals (agnostique du backend)
    data = nw.from_native(_to_native(source_pdf, backend), eager_only=True)
    result, report = compute_vulnerabilities(
        data, metric_list, config, df_previous=df_previous
    )

    # Artefacts de synthèse : la grille est la projection du résultat sur les clés
    if log_artifacts:
        log_vulnerability_artifacts(
            tracker,
            data=data,
            df_grid=result.select(*config.key_columns),
            df_result=result,
            report=report,
            metrics=metric_list,
            config=config,
        )

    # Écriture dans le schéma résultat : le frame narwhals est passé tel quel,
    # builder/updater de dt_ducklake_manager acceptant IntoDataFrame (aucune
    # reconversion pandas nécessaire).
    report.created = write_dataframe(
        result_conn,
        result,
        config.key_columns,
        catalog_alias=result_catalog_alias,
        schema=result_schema,
    )

    return report
