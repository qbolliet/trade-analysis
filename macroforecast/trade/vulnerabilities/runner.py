"""Vulnerability computation runner.

Iterates the registered vulnerability metrics over a trade DuckLake fact table —
whole, or restricted to a set of reporter x product pairs — and writes the
scores to a result schema: one column per metric, plus one boolean
``{metric}_ALERT`` column, keyed by
``date x nomenclature x indicator x flow x reporter`` (plus frequency).

Source and result are reached through DuckDB connections opened by the caller.
The result schema is built on
first encounter and upserted afterwards, through the shared
:mod:`statflows.storage.ducklake.tables` helpers also used by
:mod:`statflows.core.download`.

The same three-layer structure (compute / read previous / orchestrate) is
repeated for the **network** family, which scores the world trade graph of a
product over the BACI reconciled flows:
:func:`compute_network_vulnerabilities`, :func:`read_previous_network_result`
and :func:`run_network_vulnerabilities`. Its cell is a
``nomenclature x product x year`` triple, its perimeter one HS vintage at a
time, and its result a schema of its own — the two families share the DuckLake
plumbing, nothing else.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
from typing import TYPE_CHECKING, Any, Collection, Iterable, Optional, Sequence, Tuple
# Modules de manipulation de données
import narwhals as nw
import pandas as pd

# DuckDB : usage purement annotatif ici (les connexions sont ouvertes par
# l'appelant), donc importé au seul typage pour ne pas imposer l'extra `ducklake`
if TYPE_CHECKING:
    import duckdb
# Modules de gestion des tables DuckLake (fournis par statflows)
from statflows.storage.ducklake.tables import (
    FACT_TABLE,
    fact_table_exists,
    write_dataframe,
)
# Modules du package
from ...tracking import NULL_TRACKER, RunTracker, run_params
from .base import (
    DEFAULT_CONFIG,
    DEFAULT_NETWORK_CONFIG,
    NetworkVulnerabilityConfig,
    NetworkVulnerabilityMetric,
    VulnerabilityConfig,
    VulnerabilityMetric,
)
from .diagnostics import (
    GraphQualityReport,
    NetworkVulnerabilityReport,
    VulnerabilityReport,
    append_alert_flags,
    compute_coverage_report,
    compute_distribution_reports,
    compute_drift_report,
    compute_input_report,
    compute_quality_report,
    log_network_vulnerability_artifacts,
    log_vulnerability_artifacts,
)
from .graph import compute_graph_features
from .metrics import default_metrics
from .network_metrics import default_network_metrics

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
        Tuple ``(df_result, report)``: the scores — ``config.key_columns``, one
        column per metric, and one boolean ``{metric}_ALERT`` column flagging the
        cells above the metric's alert threshold (see
        :func:`~macroforecast.trade.vulnerabilities.diagnostics.append_alert_flags`)
        — and the :class:`VulnerabilityReport` of the run (``created`` is left
        ``False``; the caller sets it once the result is persisted).

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
        >>> bool(scores.to_native()["HHI_ALERT"][0])
        True
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

    # Drapeaux d'alerte persistés : un booléen de dépassement de seuil par
    # métrique, écrit à côté du score continu pour l'indicateur synthétique aval
    result = append_alert_flags(result, metrics, config)

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
    reporter_col: Optional[str] = None,
    product_col: Optional[str] = None,
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
        reporter_col: Name of the reporter column. Only used to build the
            perimeter predicate, hence optional.
        product_col: Name of the product column, same purpose.
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
    (one column per metric plus one boolean ``{metric}_ALERT`` column, keyed by
    ``config.key_columns``) into the result
    schema. Initialisation and incremental update are the same call: the result
    schema is built on first encounter and upserted by primary key afterwards
    (see :func:`statflows.storage.ducklake.tables.write_dataframe`).

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


# ──────────────────────────────────────────────────────────────────────
# Métriques de réseau
# ──────────────────────────────────────────────────────────────────────

# Fonction d'application de toutes les métriques de réseau sur un jeu de données
def compute_network_vulnerabilities(
    data: nw.DataFrame,
    metrics: Sequence[NetworkVulnerabilityMetric],
    config: NetworkVulnerabilityConfig = DEFAULT_NETWORK_CONFIG,
    *,
    df_previous: Optional[nw.DataFrame] = None,
) -> Tuple[nw.DataFrame, NetworkVulnerabilityReport]:
    """Compute every network metric and assemble a one-column-per-metric frame.

    Twin of :func:`compute_vulnerabilities` for the graph family: same canonical
    grid, same left join per metric so a cell a metric does not score (a graph
    too small to close a triangle, say) carries a null rather than vanishing,
    same principle of diagnostics being data rather than logs. What differs is
    the coherence check — the shape of the graphs
    (:class:`~macroforecast.trade.vulnerabilities.diagnostics.GraphQualityReport`)
    rather than the coherence of partner aggregates, which a BACI flow table
    does not carry.

    Args:
        data: Narwhals frame of reconciled bilateral flows, already carrying the
            classification column (see :func:`run_network_vulnerabilities`,
            which stamps it).
        metrics: Metric instances to apply.
        config: Column conventions (its ``key_columns`` define the grid).
        df_previous: Result of the previous run, same schema, enabling the
            run-to-run stability diagnostics. Reading it belongs to the caller
            (see :func:`read_previous_network_result`).

    Returns:
        Tuple ``(df_result, report)``: the scores — ``config.key_columns``, one
        column per metric, and one boolean ``{metric}_ALERT`` column flagging the
        cells above the metric's alert threshold (see
        :func:`~macroforecast.trade.vulnerabilities.diagnostics.append_alert_flags`)
        — and the :class:`NetworkVulnerabilityReport` of the run (``created`` is
        left ``False``; the caller sets it once the result is persisted).

    Raises:
        ValueError: If the input frame is missing any column required by one of
            the metrics (see
            :meth:`NetworkVulnerabilityMetric.required_columns`).

    Examples:
        >>> import pandas as pd
        >>> from macroforecast.trade.vulnerabilities import (
        ...     NetworkVulnerabilityConfig, WorldExportConcentration)
        >>> df = pd.DataFrame({
        ...     "product": ["01", "01"],
        ...     "exporter": ["CN", "US"], "importer": ["FR", "FR"],
        ...     "reconciled_value": [60.0, 40.0],
        ... })
        >>> config = NetworkVulnerabilityConfig(key_columns=("product",))
        >>> data = nw.from_native(df, eager_only=True)
        >>> scores, report = compute_network_vulnerabilities(
        ...     data, [WorldExportConcentration(config)], config)
        >>> round(float(scores.to_native()["EXPORT_HHI"][0]), 2), report.cells
        (0.52, 1)
        >>> bool(scores.to_native()["EXPORT_HHI_ALERT"][0])
        True
    """
    # Clés de la grille de sortie
    keys = list(config.key_columns)

    # Validation de schéma (fail-fast) : union des colonnes exigées par les
    # métriques, confrontée aux colonnes disponibles
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
    report = NetworkVulnerabilityReport(metrics=[metric.name for metric in metrics])
    report.input = compute_input_report(data, config)
    n_input = report.input.n_observations

    # Suppression des flux inexploitables : une arête sans extrémité ou sans
    # valeur n'existe pas, et fausserait degrés comme poids
    data = data.drop_nulls(
        subset=[config.exporter_col, config.importer_col, config.value_col]
    )
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

    # Drapeaux d'alerte persistés : un booléen de dépassement de seuil par
    # métrique, écrit à côté du score continu pour l'indicateur synthétique aval
    result = append_alert_flags(result, metrics, config)

    # Diagnostics de l'exécution
    report.cells = len(result)
    report.coverage = compute_coverage_report(result, metrics)
    # Forme des graphes : passe structurelle, sans aucune mesure topologique
    # (features vides), donc sans payer le prix des métriques déjà calculées
    _, graph_report = compute_graph_features(
        data,
        keys=keys,
        exporter_col=config.exporter_col,
        importer_col=config.importer_col,
        value_col=config.value_col,
        features=(),
        min_nodes=config.min_graph_nodes,
    )
    report.graph = GraphQualityReport.from_graph_report(
        graph_report, share_rows_dropped_null=share_rows_dropped_null
    )
    report.distributions = compute_distribution_reports(result, metrics, config)
    # Dérive : seulement si l'exécution précédente a été relue par l'appelant
    if df_previous is not None:
        report.drift = compute_drift_report(result, df_previous, metrics, config)

    return result, report


# ──────────────────────────────────────────────────────────────────────
# Métriques de réseau — relecture du résultat (DuckLake)
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction du prédicat SQL sur le millésime de nomenclature
def _classification_predicate(classification_col: str, classification: str) -> str:
    """Build the SQL predicate restricting a query to one HS vintage.

    Perimeter of an incremental network run: a BACI pass re-estimates gravity
    and reporting quality over its whole time slice, so what a rerun invalidates
    is one vintage in full — never a subset of its years.

    Args:
        classification_col: Name of the classification column.
        classification: HS vintage to restrict to (e.g. ``"HS2017"``).

    Returns:
        A SQL boolean expression of the form ``"classification" = 'HS2017'``.

    Examples:
        >>> print(_classification_predicate("classification", "HS2017"))
        "classification" = 'HS2017'
    """
    # Littéral échappé, même règle que le prédicat reporter x produit
    return f'"{classification_col}" = {_sql_literal(classification)}'


# Fonction de lecture du résultat de réseau de l'exécution précédente
def read_previous_network_result(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    result_schema: str,
    *,
    classification: str,
    config: NetworkVulnerabilityConfig = DEFAULT_NETWORK_CONFIG,
) -> Optional[nw.DataFrame]:
    """Read the previous run's network scores, for the drift diagnostics.

    Twin of :func:`read_previous_result`, scoped to a single HS vintage — the
    perimeter an incremental network run recomputes. Reading belongs to the
    caller: the runner never decides on its own to re-read the result table.
    Degrades gracefully — a result table that does not exist yet returns
    ``None``, which disables the drift diagnostics rather than failing the run.

    Args:
        conn: Open DuckLake connection on the result catalog, owned by the
            caller.
        catalog_alias: Alias under which the result catalog is attached.
        result_schema: Schema holding the result ``fact_table``.
        classification: HS vintage to restrict the read to.
        config: Column conventions (``classification_col`` names the filtered
            column).

    Returns:
        Narwhals frame of the previous scores, or ``None`` when no result table
        exists yet.
    """
    # Première exécution : aucune table résultat, donc aucune dérive mesurable
    if not fact_table_exists(conn, catalog_alias, result_schema):
        # Logging
        logger.info(
            f"No network result table in '{result_schema}' yet: "
            "drift diagnostics skipped"
        )
        return None

    # Projection intégrale : la table résultat est étroite (clés + métriques)
    query = (
        f'SELECT * FROM "{catalog_alias}"."{result_schema}"."{FACT_TABLE}" '
        "WHERE "
        + _classification_predicate(config.classification_col, classification)
    )
    # Exécution de la requête
    previous_pdf = conn.execute(query).df()

    # Logging
    logger.info(
        f"Read {len(previous_pdf)} previous network result rows for "
        f"'{classification}' from '{result_schema}'"
    )
    return nw.from_native(previous_pdf, eager_only=True)


# ──────────────────────────────────────────────────────────────────────
# Métriques de réseau — orchestration de bout en bout
# ──────────────────────────────────────────────────────────────────────

# Fonction d'orchestration : flux BACI d'un millésime → métriques → schéma résultat
def run_network_vulnerabilities(
    source_conn: duckdb.DuckDBPyConnection,
    *,
    source_catalog_alias: str,
    source_schema: str,
    classification: str,
    result_schema: str,
    result_conn: Optional[duckdb.DuckDBPyConnection] = None,
    result_catalog_alias: Optional[str] = None,
    metrics: Optional[Sequence[NetworkVulnerabilityMetric]] = None,
    config: NetworkVulnerabilityConfig = DEFAULT_NETWORK_CONFIG,
    backend: str = "pandas",
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
    df_previous: Optional[nw.DataFrame] = None,
) -> NetworkVulnerabilityReport:
    """Compute the network vulnerability metrics of one HS vintage and persist them.

    Reads the BACI reconciled-flow table of a vintage, **stamps the vintage onto
    every row** as the ``classification`` column, applies every metric, and
    upserts the scores (one column per metric plus one boolean
    ``{metric}_ALERT`` column) into the result schema keyed by
    ``config.key_columns`` — ``nomenclature x product x year``, the nomenclature
    being the primary key the partner-level result table does not carry.

    One vintage per call, deliberately: the BACI vintages live in one schema
    each, they overlap in time, and running them separately is what lets a
    failure on one leave the others alone (see
    ``scripts/compute_network_vulnerabilities.py``).

    Connections are passed in and are **never opened or closed here**: their
    lifecycle belongs to the caller. Source (the BACI catalog) and result (the
    vulnerabilities catalog) are normally two distinct catalogs, hence
    ``result_conn`` and ``result_catalog_alias``; two schemas of a single
    catalog work just as well.

    Args:
        source_conn: Open DuckLake connection on the source (BACI) catalog.
        source_catalog_alias: Alias under which the source catalog is attached.
        source_schema: Schema holding the vintage's reconciled ``fact_table``
            (e.g. ``baci_hs2017``).
        classification: HS vintage label stamped onto the result (e.g.
            ``"HS2017"``), and the perimeter the run covers.
        result_schema: Schema to create or upsert the scores into.
        result_conn: Open connection on the result catalog, when it differs from
            the source one. Defaults to ``source_conn``.
        result_catalog_alias: Alias of the result catalog. Required whenever
            ``result_conn`` is supplied; defaults to ``source_catalog_alias``.
        metrics: Metric instances to apply. Defaults to
            :func:`~macroforecast.trade.vulnerabilities.network_metrics.default_network_metrics`.
        config: Column conventions and thresholds.
        backend: Native eager backend for narwhals computation (``"pandas"``
            or, when installed, ``"polars"``/``"pyarrow"``).
        tracker: Experiment tracker receiving the run parameters and artifacts.
            Defaults to the null tracker, so an unconfigured run behaves exactly
            as before. The *metrics* are left to the caller, which sends
            ``report.to_metrics()`` once the report is complete.
        log_artifacts: Whether to build and send the business artifacts (most
            exposed products, alert counts, unscored cells, deciles).
        df_previous: Result of the previous run over the same vintage, enabling
            the drift diagnostics (see :func:`read_previous_network_result`).

    Returns:
        A :class:`NetworkVulnerabilityReport` summarising the run.

    Raises:
        ValueError: If ``result_conn`` is supplied without
            ``result_catalog_alias``, or if the source frame is missing a column
            required by one of the metrics.
    """
    # Initialisation de la liste des métriques
    metric_list = (
        list(metrics) if metrics is not None else default_network_metrics(config)
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
                "classification": classification,
                "backend": backend,
                "metrics": [metric.name for metric in metric_list],
            },
        )
    )

    # Colonnes à lire : clés de la grille hors millésime (estampillé ici, absent
    # de la table BACI) plus les deux extrémités et le poids des arêtes
    required = list(
        dict.fromkeys(
            [key for key in config.key_columns if key != config.classification_col]
            + [config.exporter_col, config.importer_col, config.value_col]
        )
    )

    # Lecture de la table de faits BACI du millésime (intégrale : une métrique de
    # graphe a besoin de tous les pays d'un produit, aucun périmètre partiel
    # n'aurait de sens)
    source_pdf = _read_source_fact_table(
        source_conn, source_catalog_alias, source_schema, required
    )

    # Calcul des métriques via narwhals (agnostique du backend), millésime
    # estampillé sur chaque ligne pour devenir clé primaire du résultat
    data = nw.from_native(_to_native(source_pdf, backend), eager_only=True).with_columns(
        nw.lit(classification).alias(config.classification_col)
    )
    result, report = compute_network_vulnerabilities(
        data, metric_list, config, df_previous=df_previous
    )
    report.classification = classification

    # Artefacts de synthèse
    if log_artifacts:
        log_network_vulnerability_artifacts(
            tracker,
            df_result=result,
            report=report,
            metrics=metric_list,
            config=config,
        )

    # Écriture dans le schéma résultat : le frame narwhals est passé tel quel,
    # builder/updater de dt_ducklake_manager acceptant IntoDataFrame.
    report.created = write_dataframe(
        result_conn,
        result,
        config.key_columns,
        catalog_alias=result_catalog_alias,
        schema=result_schema,
        label=classification,
    )

    return report
