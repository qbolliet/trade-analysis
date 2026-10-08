"""Vulnerability computation runner.

Iterates the registered vulnerability metrics over a trade DuckLake fact table —
whole, or restricted to a set of reporter x product pairs — and writes the
scores to a result schema: one column per metric, plus one boolean
``{metric}_ALERT`` column, keyed by
``date x nomenclature x indicator x flow x reporter`` (plus frequency).

Both families are computed for the requested **directions** (``flows``:
``"import"``, ``"export"``), each metric class being instantiated once per
direction it supports (``cls(config, flow=f)``). The run diagnostics are
produced per direction (:attr:`VulnerabilityReport.flows`), import and export
cells never being pooled.

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
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)
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
    flow_code_map,
    parse_flows,
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

# Métrique de l'une ou l'autre famille (helpers communs aux deux runners)
_Metric = TypeVar("_Metric", VulnerabilityMetric, NetworkVulnerabilityMetric)

# Écrivain d'une table résultat : (jeu de données, clé primaire) -> table créée
TableWriter = Callable[[Any, Sequence[str]], bool]


# Fonction d'écriture du résultat d'un runner
def _write_result(
    result: Any,
    primary_keys: Sequence[str],
    *,
    writer: Optional[TableWriter],
    conn: Any,
    catalog_alias: str,
    schema: str,
    write_options: Optional[Mapping[str, Any]],
) -> bool:
    """Write a runner result through the injected writer, or ``write_dataframe``.

    The methodology runners know neither the pipeline's write handle nor its
    options: the calling step injects a writer bound to them. Without writer,
    the historical path (``write_dataframe`` and its options) is kept.

    Args:
        result: Scores to write (any ``IntoDataFrame``).
        primary_keys: Primary-key columns of the result table.
        writer: Injected writer, or ``None``.
        conn: Open connection on the result catalog (``write_dataframe`` path).
        catalog_alias: Alias of the result catalog (``write_dataframe`` path).
        schema: Result schema (``write_dataframe`` path).
        write_options: Extra keyword arguments of ``write_dataframe``.

    Returns:
        Whether the result table was created by this write.
    """
    if writer is not None:
        return bool(writer(result, list(primary_keys)))
    return write_dataframe(
        conn,
        result,
        list(primary_keys),
        catalog_alias=catalog_alias,
        schema=schema,
        **(write_options or {}),
    )


# ──────────────────────────────────────────────────────────────────────
# Sens des flux : sélection des instances et vérifications communes
# ──────────────────────────────────────────────────────────────────────

# Fonction de sélection des instances des sens demandés
def _metrics_for_flows(metrics: Sequence[_Metric], flows: Sequence[str]) -> List[_Metric]:
    """Keep the metric instances whose direction is requested.

    Args:
        metrics: Metric instances, possibly of several directions.
        flows: Requested directions.

    Returns:
        The instances whose ``flow`` is in ``flows``, in their original order.

    Raises:
        ValueError: If ``flows`` is empty or names an unknown direction.

    Examples:
        >>> from macroforecast.trade.vulnerabilities import HerfindahlHirschmanIndex
        >>> both = [HerfindahlHirschmanIndex(flow="import"), HerfindahlHirschmanIndex(flow="export")]
        >>> [m.flow for m in _metrics_for_flows(both, ["export"])]
        ['export']
    """
    # Vérification des arguments
    requested = parse_flows(flows)
    return [metric for metric in metrics if metric.flow in requested]


# Fonction de dédoublonnage des instances par nom de colonne
def _unique_by_name(metrics: Sequence[_Metric]) -> List[_Metric]:
    """Keep one representative instance per output column.

    Two instances of a class in two directions share a column name: the
    column-wise steps (alert flags, top-level report) need it once.

    Args:
        metrics: Metric instances.

    Returns:
        The first instance of each name, in registry order.

    Examples:
        >>> from macroforecast.trade.vulnerabilities import HerfindahlHirschmanIndex
        >>> both = [HerfindahlHirschmanIndex(flow="import"), HerfindahlHirschmanIndex(flow="export")]
        >>> [m.flow for m in _unique_by_name(both)]
        ['import']
    """
    representatives: Dict[str, _Metric] = {}
    for metric in metrics:
        representatives.setdefault(metric.name, metric)
    return list(representatives.values())


# Fonction de validation du schéma d'entrée au regard des métriques
def _check_required_columns(data: nw.DataFrame, metrics: Sequence[Any]) -> None:
    """Fail fast when the input lacks a column required by a metric.

    Args:
        data: Input frame.
        metrics: Metric instances to apply.

    Raises:
        ValueError: If a required column is missing, naming the metrics
            concerned.

    Examples:
        >>> import pandas as pd
        >>> from macroforecast.trade.vulnerabilities import HerfindahlHirschmanIndex
        >>> frame = nw.from_native(pd.DataFrame({"partner": ["CN"]}), eager_only=True)
        >>> _check_required_columns(frame, [HerfindahlHirschmanIndex()])  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        ValueError: Missing required column(s) ...
    """
    # Union des colonnes exigées par les métriques, confrontée aux colonnes disponibles
    available = set(data.columns)
    required = set().union(*(metric.required_columns() for metric in metrics))
    missing = required - available
    if missing:
        # Métriques concernées par au moins une colonne manquante
        culprits = sorted(
            {metric.name for metric in metrics if metric.required_columns() & missing}
        )
        raise ValueError(
            f"Missing required column(s) {sorted(missing)} for metric(s) "
            f"{culprits}. Available columns: {sorted(available)}."
        )


# ──────────────────────────────────────────────────────────────────────
# Calcul (backend-agnostique, narwhals)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'application de toutes les métriques sur un jeu de données
def compute_vulnerabilities(
    data: nw.DataFrame,
    metrics: Sequence[VulnerabilityMetric],
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    *,
    flows: Sequence[str] = ("import",),
    flow_codes: Optional[Mapping[str, int]] = None,
    df_previous: Optional[nw.DataFrame] = None,
) -> Tuple[nw.DataFrame, VulnerabilityReport]:
    """Compute every metric and assemble a one-column-per-metric frame.

    Builds the canonical grid of distinct cells, **restricted to the flow codes
    of the requested directions**, and left-joins each metric's output onto
    it, so cells a metric does not score carry a null value. The instances of
    a class in several directions return disjoint rows (each scores its own
    flow only): they are stacked, then joined once, under their shared column.
    A metric not supported for a direction is therefore null on its rows. The
    metrics read the whole input, both flows included, since a cross-flow
    metric (CDI3) divides by the opposite flow.

    Alongside the scores, each direction is diagnosed on its own slice
    (volumetry, coverage, aggregate coherence, distributions and — when a
    previous result is supplied — drift), the diagnostics being data rather
    than logs.

    Args:
        data: Narwhals frame of partner-level observations.
        metrics: Metric instances to apply; those whose direction is not in
            ``flows`` are ignored.
        config: Column conventions (its ``key_columns`` define the grid).
        flows: Directions to compute (``"import"``, ``"export"``).
        flow_codes: Flow code of each direction. Defaults to the
            configuration's ``import_flow`` / ``export_flow``.
        df_previous: Result of the previous run, same schema, enabling the
            run-to-run stability diagnostics. Reading it belongs to the caller
            (see :func:`read_previous_result`).

    Returns:
        Tuple ``(df_result, report)``: the scores — ``config.key_columns``, one
        column per metric, and one boolean ``{metric}_ALERT`` column flagging the
        cells above the metric's alert threshold (see
        :func:`~macroforecast.trade.vulnerabilities.diagnostics.append_alert_flags`)
        — and the :class:`VulnerabilityReport` of the run, one sub-report per
        direction in ``report.flows`` (``created`` is left ``False``; the
        caller sets it once the result is persisted).

    Raises:
        ValueError: If ``flows`` is empty or unknown, or if the input frame is
            missing any column required by one of the metrics (see
            :meth:`VulnerabilityMetric.required_columns`).

    Examples:
        >>> import pandas as pd
        >>> from macroforecast.trade.vulnerabilities import (
        ...     HerfindahlHirschmanIndex, VulnerabilityConfig)
        >>> df = pd.DataFrame({
        ...     "flow": [1, 1, 1, 2, 2], "partner": ["CN", "US", "WORLD", "CN", "WORLD"],
        ...     "OBS_VALUE": [60.0, 40.0, 100.0, 10.0, 10.0],
        ... })
        >>> config = VulnerabilityConfig(key_columns=("flow",))
        >>> data = nw.from_native(df, eager_only=True)
        >>> scores, report = compute_vulnerabilities(
        ...     data, [HerfindahlHirschmanIndex(config)], config)
        >>> round(float(scores.to_native()["HHI"][0]), 2), report.cells
        (0.52, 1)
        >>> bool(scores.to_native()["HHI_ALERT"][0]), list(report.flows)
        (True, ['import'])
    """
    # Clés de la grille de sortie et codes des sens demandés
    keys = list(config.key_columns)
    codes = dict(flow_codes) if flow_codes is not None else flow_code_map(config)
    selected = _metrics_for_flows(metrics, flows)
    representatives = _unique_by_name(selected)

    # Validation de schéma (fail-fast)
    _check_required_columns(data, selected)

    # Volumétrie d'entrée, mesurée avant tout filtrage
    report = VulnerabilityReport(metrics=[metric.name for metric in representatives])
    report.input = compute_input_report(data, config)

    # Suppression des observations inexploitables (partenaire ou valeur nuls) :
    # un partenaire nul fausse les masques booléens du filtre des pays individuels.
    clean = data.drop_nulls(subset=[config.partner_col, config.value_col])

    # Grille canonique : cellules distinctes de la base, restreintes aux flux demandés
    flow_col = nw.col(config.flow_col)
    all_cells = clean.select(*keys).unique()
    df_grid = all_cells.filter(flow_col.is_in([codes[flow] for flow in flows]))
    # Grille vide sur une source non vide : codes de flux absents (typiquement une
    # colonne de flux en texte face à des codes entiers) : signalée, jamais silencieuse
    if len(df_grid) == 0 and len(all_cells) > 0:
        logger.warning(
            f"No cell of flow code(s) {[codes[flow] for flow in flows]} in column "
            f"'{config.flow_col}' (values found: "
            f"{sorted(map(str, all_cells.get_column(config.flow_col).unique().to_list()))}): "
            "check the flow codes and the column type."
        )
    result = df_grid

    # Une jointure gauche par colonne : sorties des instances (une par sens,
    # aux lignes disjointes) empilées puis jointes en une fois
    for representative in representatives:
        scored = nw.concat(
            [metric.compute(clean) for metric in selected if metric.name == representative.name],
            how="vertical",
        )
        result = result.join(scored, on=keys, how="left")

    # Drapeaux d'alerte persistés : un booléen de dépassement de seuil par
    # métrique, écrit à côté du score continu pour l'indicateur synthétique aval
    result = append_alert_flags(result, representatives, config)
    report.cells = len(result)

    # Diagnostics par sens, chacun sur sa tranche (jamais de distribution mêlant
    # import et export)
    for flow in flows:
        is_flow = flow_col == codes[flow]
        flow_metrics = [metric for metric in selected if metric.flow == flow]
        flow_result = result.filter(is_flow)
        flow_grid = df_grid.filter(is_flow)
        flow_input = data.filter(is_flow)
        n_flow_input = len(flow_input)
        n_flow_clean = len(clean.filter(is_flow))

        # Rapport du sens : volumétrie, couverture, cohérence, distributions
        sub = VulnerabilityReport(metrics=[metric.name for metric in flow_metrics])
        sub.input = compute_input_report(flow_input, config)
        sub.cells = len(flow_result)
        sub.coverage = compute_coverage_report(flow_result, flow_metrics)
        sub.quality = compute_quality_report(
            clean,
            flow_grid,
            config,
            share_rows_dropped_null=(
                (n_flow_input - n_flow_clean) / n_flow_input if n_flow_input else float("nan")
            ),
        )
        sub.distributions = compute_distribution_reports(flow_result, flow_metrics, config)
        # Dérive : seulement si l'exécution précédente a été relue par l'appelant
        if df_previous is not None and config.flow_col in df_previous.columns:
            sub.drift = compute_drift_report(
                flow_result, df_previous.filter(is_flow), flow_metrics, config
            )
        report.flows[flow] = sub

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
    where: Optional[str] = None,
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
        where: Additional SQL predicate, conjoined with the pair predicate
            (period bounds, code length…). ``None`` adds nothing.

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
    predicates = []
    if reporters_products:
        predicates.append(
            _reporter_product_predicate(reporter_col, product_col, reporters_products)
        )
    if where:
        predicates.append(f"({where})")
    if predicates:
        query += " WHERE " + " AND ".join(predicates)
    return conn.execute(query).df()


# Fonction publique de lecture des flux partenaires sources
def read_source_flows(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    source_schema: str,
    *,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    columns: Optional[Sequence[str]] = None,
    reporters_products: Optional[Sequence[Tuple[str, str]]] = None,
    where: Optional[str] = None,
) -> pd.DataFrame:
    """Read the partner flows a computation needs, restricted to a perimeter.

    The read a caller performs before transforming the flows itself (a
    nomenclature conversion, say) and handing them to
    :func:`run_vulnerabilities_on_frame`.

    Args:
        conn: Open DuckLake connection on the source catalog, owned by the
            caller.
        catalog_alias: Alias under which the source catalog is attached.
        source_schema: Schema holding the source ``fact_table``.
        config: Column conventions; the grid keys, partner and value columns
            are read by default.
        columns: Columns to read instead of the default ones.
        reporters_products: Reporter x product pairs to restrict the read to.
        where: Additional SQL predicate (e.g. a period bound).

    Returns:
        The projected and filtered flows.
    """
    required = (
        list(columns)
        if columns is not None
        else list(dict.fromkeys([*config.key_columns, config.partner_col, config.value_col]))
    )
    return _read_source_fact_table(
        conn,
        catalog_alias,
        source_schema,
        required,
        reporter_col=config.reporter_col,
        product_col=config.product_col,
        reporters_products=reporters_products,
        where=where,
    )


# Fonction de lecture du résultat de l'exécution précédente (diagnostics de dérive)
def read_previous_result(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    result_schema: str,
    *,
    reporters_products: Optional[Sequence[Tuple[str, str]]] = None,
    flow_codes: Optional[Collection[int]] = None,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    where: Optional[str] = None,
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
        flow_codes: Flow codes to restrict the read to, mirroring the
            directions recomputed. ``None`` reads every flow.
        config: Column conventions (``reporter_col`` / ``product_col`` /
            ``flow_col`` name the filtered columns).
        where: Additional SQL predicate restricting the read (e.g. to the
            rows of one nomenclature), so that the drift compares the rows the
            run recomputes and nothing else.

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
    # Restriction éventuelle au périmètre recalculé : couples et flux
    predicates = []
    if reporters_products:
        predicates.append(
            _reporter_product_predicate(
                config.reporter_col, config.product_col, reporters_products
            )
        )
    if flow_codes:
        codes = ", ".join(str(int(code)) for code in sorted(flow_codes))
        predicates.append(f'"{config.flow_col}" IN ({codes})')
    if where:
        predicates.append(f"({where})")
    if predicates:
        query += " WHERE " + " AND ".join(predicates)
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
    flows: Sequence[str] = ("import",),
    flow_codes: Optional[Mapping[str, int]] = None,
    backend: str = "pandas",
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
    df_previous: Optional[nw.DataFrame] = None,
    write_options: Optional[Mapping[str, Any]] = None,
    writer: Optional[TableWriter] = None,
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
            :func:`~macroforecast.trade.vulnerabilities.metrics.default_metrics`
            instantiated for ``flows``; explicit instances of another direction
            are ignored.
        config: Column and partner-code conventions.
        flows: Directions to compute (``"import"``, ``"export"``). Only the
            rows of these flows are written: the rows of another flow already
            in the result table are left untouched by the upsert.
        flow_codes: Flow code of each direction. Defaults to the
            configuration's ``import_flow`` / ``export_flow``.
        backend: Native eager backend for narwhals computation (``"pandas"``
            or, when installed, ``"polars"``/``"pyarrow"``).
        tracker: Experiment tracker receiving the run parameters and artifacts.
            Defaults to the null tracker, so an unconfigured run behaves exactly
            as before. The *metrics* are left to the caller, which sends the
            per-direction ``report.flows[flow].to_metrics()`` once the report is
            complete.
        log_artifacts: Whether to build and send the business artifacts (top
            vulnerable cells, alert counts, missing aggregates, deciles), one
            sub-directory per direction.
        df_previous: Result of the previous run over the same perimeter,
            enabling the drift diagnostics (see :func:`read_previous_result`).
        write_options: Extra keyword arguments forwarded to
            :func:`~statflows.storage.ducklake.tables.write_dataframe`
            (``update_options``, ``run_id``, ``commit_message``). ``None``
            keeps the library defaults.
        writer: Writer of the result table, ``(frame, primary_keys) -> created``
            (e.g. ``kedro_pipeline.io.ducklake.DuckLakeTable.writer``), which
            then carries the write options itself; ``write_options`` is
            ignored. ``None`` writes through
            :func:`~statflows.storage.ducklake.tables.write_dataframe`.

    Returns:
        A :class:`VulnerabilityReport` summarising the run, with one
        sub-report per direction in ``report.flows``.

    Raises:
        ValueError: If ``result_conn`` is supplied without
            ``result_catalog_alias``, if ``flows`` is empty or unknown, or if the
            input frame is missing a column required by one of the metrics.
    """
    # Initialisation de la liste des métriques, restreinte aux sens demandés
    metric_list = _metrics_for_flows(
        list(metrics) if metrics is not None else default_metrics(config, flows), flows
    )
    codes = dict(flow_codes) if flow_codes is not None else flow_code_map(config)

    # Périmètre vide (distinct de None, qui vaut « tout le catalogue ») :
    # rien à recalculer, sortie anticipée sans toucher à la base
    if reporters_products is not None and not reporters_products:
        logger.info("No reporter-product pairs to recalculate; early termination.")
        return VulnerabilityReport(
            cells=0,
            metrics=[metric.name for metric in _unique_by_name(metric_list)],
            created=False,
        )

    # Catalogue résultat : partagé avec la source par défaut. Une connexion
    # distincte impose de nommer son alias, qui n'est pas déductible.
    if result_conn is not None and result_catalog_alias is None:
        raise ValueError(
            "result_catalog_alias is required when result_conn is supplied"
        )
    result_conn = result_conn if result_conn is not None else source_conn
    result_catalog_alias = result_catalog_alias or source_catalog_alias

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

    # Calcul, artefacts et écriture : chemin commun à toute lecture
    return run_vulnerabilities_on_frame(
        source_pdf,
        result_conn=result_conn,
        result_catalog_alias=result_catalog_alias,
        result_schema=result_schema,
        metrics=metric_list,
        config=config,
        flows=flows,
        flow_codes=codes,
        backend=backend,
        tracker=tracker,
        log_artifacts=log_artifacts,
        df_previous=df_previous,
        write_options=write_options,
        writer=writer,
        params={
            "source_schema": source_schema,
            "n_reporter_product_pairs": (
                len(reporters_products) if reporters_products is not None else None
            ),
        },
    )


# Fonction de calcul et d'écriture des métriques sur des flux déjà lus
def run_vulnerabilities_on_frame(
    source_pdf: pd.DataFrame,
    *,
    result_conn: duckdb.DuckDBPyConnection,
    result_catalog_alias: str,
    result_schema: str,
    metrics: Optional[Sequence[VulnerabilityMetric]] = None,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    flows: Sequence[str] = ("import",),
    flow_codes: Optional[Mapping[str, int]] = None,
    backend: str = "pandas",
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
    df_previous: Optional[nw.DataFrame] = None,
    write_options: Optional[Mapping[str, Any]] = None,
    writer: Optional[TableWriter] = None,
    annotate: Optional[Callable[[nw.DataFrame], nw.DataFrame]] = None,
    params: Optional[Mapping[str, Any]] = None,
) -> VulnerabilityReport:
    """Compute the metrics on flows already read, then write them.

    The second half of :func:`run_vulnerabilities`, for a caller that reads
    or transforms the flows itself (a nomenclature conversion, say) before the
    computation. The grid, the primary key of the written table and the
    columns the metrics group by are ``config.key_columns``: a caller adding a
    key dimension (a classification column) extends them in the
    configuration and stamps the column on ``source_pdf``.

    Args:
        source_pdf: Partner-level flows, carrying ``config.key_columns``, the
            partner and the value columns.
        result_conn: Open connection on the result catalog, owned by the
            caller.
        result_catalog_alias: Alias of the result catalog.
        result_schema: Schema to create or upsert the scores into.
        metrics: Metric instances to apply. Defaults to
            :func:`~macroforecast.trade.vulnerabilities.metrics.default_metrics`
            instantiated for ``flows``.
        config: Column and partner-code conventions.
        flows: Directions to compute.
        flow_codes: Flow code of each direction. Defaults to the
            configuration's ``import_flow`` / ``export_flow``.
        backend: Native eager backend for narwhals computation.
        tracker: Experiment tracker receiving the parameters and artifacts.
        log_artifacts: Whether to build and send the business artifacts.
        df_previous: Result of the previous run over the same perimeter,
            enabling the drift diagnostics.
        write_options: Extra keyword arguments forwarded to
            :func:`~statflows.storage.ducklake.tables.write_dataframe`
            (``update_options``, ``build_options``, ``run_id``,
            ``commit_message``).
        writer: Writer of the result table, ``(frame, primary_keys) -> created``
            (e.g. ``kedro_pipeline.io.ducklake.DuckLakeTable.writer``), which
            then carries the write options itself; ``write_options`` is
            ignored. ``None`` writes through
            :func:`~statflows.storage.ducklake.tables.write_dataframe`.
        annotate: Function adding descriptive columns to the scores before
            the write (columns derived from the keys, outside the primary
            key). ``None`` writes the scores as computed.
        params: Context parameters logged with the flattened configuration.

    Returns:
        A :class:`VulnerabilityReport` summarising the run, with one
        sub-report per direction in ``report.flows``.

    Raises:
        ValueError: If ``flows`` is empty or unknown, or if the input frame is
            missing a column required by one of the metrics.

    Examples:
        >>> report = run_vulnerabilities_on_frame(
        ...     df, result_conn=conn, result_catalog_alias="v",
        ...     result_schema="indicators")  # doctest: +SKIP
    """
    # Initialisation de la liste des métriques, restreinte aux sens demandés
    metric_list = _metrics_for_flows(
        list(metrics) if metrics is not None else default_metrics(config, flows), flows
    )
    codes = dict(flow_codes) if flow_codes is not None else flow_code_map(config)

    # Paramètres de l'exécution : configuration aplatie et contexte
    tracker.log_params(
        run_params(
            config,
            {
                **dict(params or {}),
                "result_schema": result_schema,
                "backend": backend,
                "metrics": [metric.name for metric in _unique_by_name(metric_list)],
                "flows": list(flows),
            },
        )
    )

    # Calcul des métriques via narwhals (agnostique du backend)
    data = nw.from_native(_to_native(source_pdf, backend), eager_only=True)
    result, report = compute_vulnerabilities(
        data, metric_list, config, flows=flows, flow_codes=codes, df_previous=df_previous
    )

    # Artefacts de synthèse, un dossier par sens : la grille est la projection
    # de la tranche du résultat sur les clés
    if log_artifacts:
        for flow, sub in report.flows.items():
            flow_result = result.filter(nw.col(config.flow_col) == codes[flow])
            log_vulnerability_artifacts(
                tracker,
                data=data,
                df_grid=flow_result.select(*config.key_columns),
                df_result=flow_result,
                report=sub,
                metrics=[metric for metric in metric_list if metric.flow == flow],
                config=config,
                flow=flow,
            )

    # Colonnes descriptives dérivées des clés, ajoutées hors clé primaire
    if annotate is not None:
        result = annotate(result)

    # Écriture dans le schéma résultat : le frame narwhals est passé tel quel,
    # builder/updater de dt_ducklake_manager acceptant IntoDataFrame (aucune
    # reconversion pandas nécessaire).
    report.created = _write_result(
        result,
        config.key_columns,
        writer=writer,
        conn=result_conn,
        catalog_alias=result_catalog_alias,
        schema=result_schema,
        write_options=write_options,
    )
    # Issue de l'écriture reportée sur les rapports par sens
    for sub in report.flows.values():
        sub.created = report.created

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
    flows: Sequence[str] = ("import",),
    flow_codes: Optional[Mapping[str, int]] = None,
    df_previous: Optional[nw.DataFrame] = None,
) -> Tuple[nw.DataFrame, NetworkVulnerabilityReport]:
    """Compute every network metric, per direction, in a one-column-per-metric frame.

    Twin of :func:`compute_vulnerabilities` for the graph family: same canonical
    grid, same left join per metric so a cell a metric does not score (a graph
    too small to close a triangle, say) carries a null rather than vanishing,
    same principle of diagnostics being data rather than logs.

    The BACI matrix has no flow: **each direction is computed in its own
    pass** over the whole matrix, by the instances of that direction (the
    export pass reads the matrix transposed, through the role columns of the
    metrics). The orientation-invariant metrics (clustering, diameter) are
    computed once and their values copied onto every direction. Each pass
    yields the rows of one direction, to which the ``flow_col`` column is added
    last, so that the SPOF ranks are only ever taken within one direction. The
    passes are then stacked.

    What differs from the partner family is the coherence check — the shape of
    the graphs
    (:class:`~macroforecast.trade.vulnerabilities.diagnostics.GraphQualityReport`)
    rather than the coherence of partner aggregates, which a BACI flow table
    does not carry. It does not depend on the direction and is computed once.

    Args:
        data: Narwhals frame of reconciled bilateral flows, already carrying the
            classification column (see :func:`run_network_vulnerabilities`,
            which stamps it).
        metrics: Metric instances to apply; those whose direction is not in
            ``flows`` are ignored.
        config: Column conventions (its ``key_columns`` define the grid, its
            ``flow_col`` names the flow column added to the result).
        flows: Directions to compute (``"import"``, ``"export"``), in output
            order.
        flow_codes: Flow code of each direction. Defaults to the
            configuration's ``import_flow`` / ``export_flow``.
        df_previous: Result of the previous run, same schema, enabling the
            run-to-run stability diagnostics. Reading it belongs to the caller
            (see :func:`read_previous_network_result`). A previous result
            without flow column (written before the directions existed) is
            ignored.

    Returns:
        Tuple ``(df_result, report)``: the scores — ``config.key_columns``, one
        column per metric, one boolean ``{metric}_ALERT`` column per metric
        (see
        :func:`~macroforecast.trade.vulnerabilities.diagnostics.append_alert_flags`)
        and the ``flow_col`` column — and the
        :class:`NetworkVulnerabilityReport` of the run, one sub-report per
        direction in ``report.flows`` (``created`` is left ``False``; the
        caller sets it once the result is persisted).

    Raises:
        ValueError: If ``flows`` is empty or unknown, or if the input frame is
            missing any column required by one of the metrics (see
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
        >>> metrics = [WorldExportConcentration(config, flow=f) for f in ("import", "export")]
        >>> scores, report = compute_network_vulnerabilities(
        ...     data, metrics, config, flows=("import", "export"))
        >>> scores.to_native()[["flow", "WORLD_HHI"]].round(2).values.tolist()
        [[1.0, 0.52], [2.0, 1.0]]
        >>> bool(scores.to_native()["WORLD_HHI_ALERT"].iloc[0]), report.cells
        (True, 2)
    """
    # Clés de la grille de sortie et codes des sens demandés
    keys = list(config.key_columns)
    codes = dict(flow_codes) if flow_codes is not None else flow_code_map(config)
    selected = _metrics_for_flows(metrics, flows)

    # Validation de schéma (fail-fast)
    _check_required_columns(data, selected)

    # Volumétrie d'entrée, mesurée avant tout filtrage
    report = NetworkVulnerabilityReport(
        metrics=[metric.name for metric in _unique_by_name(selected)]
    )
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

    # Grille canonique : cellules distinctes de la base (commune aux deux sens)
    df_grid = data.select(*keys).unique()

    # Forme des graphes : passe structurelle, sans aucune mesure topologique
    # (features vides), indépendante du sens. Colonnes d'arête explicites : la
    # forme d'un graphe ne dépend pas de l'orientation de ses arêtes
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

    # Une passe par sens ; métriques invariantes calculées une seule fois
    invariant_scores: Dict[Tuple[str, NetworkVulnerabilityConfig], nw.DataFrame] = {}
    passes: List[nw.DataFrame] = []
    for flow in flows:
        flow_metrics = [metric for metric in selected if metric.flow == flow]
        flow_result = df_grid
        for metric in flow_metrics:
            if metric.orientation_invariant:
                cache_key = (metric.name, metric.config)
                if cache_key not in invariant_scores:
                    invariant_scores[cache_key] = metric.compute(data)
                scored = invariant_scores[cache_key]
            else:
                scored = metric.compute(data)
            flow_result = flow_result.join(scored, on=keys, how="left")

        # Drapeaux d'alerte du sens, à côté des scores continus
        flow_result = append_alert_flags(flow_result, flow_metrics, config)

        # Rapport du sens : diagnostics des scores sur sa seule tranche
        sub = NetworkVulnerabilityReport(metrics=[metric.name for metric in flow_metrics])
        sub.input = report.input
        sub.graph = report.graph
        sub.cells = len(flow_result)
        sub.coverage = compute_coverage_report(flow_result, flow_metrics)
        sub.distributions = compute_distribution_reports(flow_result, flow_metrics, config)
        # Dérive : seulement contre un résultat précédent qui distingue les sens
        if df_previous is not None and config.flow_col in df_previous.columns:
            sub.drift = compute_drift_report(
                flow_result,
                df_previous.filter(nw.col(config.flow_col) == codes[flow]),
                flow_metrics,
                config,
            )
        report.flows[flow] = sub

        # Colonne du flux ajoutée en dernier : les rangs ont été pris dans le sens
        passes.append(flow_result.with_columns(nw.lit(codes[flow]).alias(config.flow_col)))

    # Empilement des passes ; une métrique absente d'un sens y reste nulle
    result = passes[0] if len(passes) == 1 else nw.concat(passes, how="diagonal")
    report.cells = len(result)
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

# Fonction de calcul d'un millésime, sans écriture
def compute_network_vintage(
    source_conn: duckdb.DuckDBPyConnection,
    *,
    source_catalog_alias: str,
    source_schema: str,
    classification: str,
    result_schema: str,
    metrics: Optional[Sequence[NetworkVulnerabilityMetric]] = None,
    config: NetworkVulnerabilityConfig = DEFAULT_NETWORK_CONFIG,
    flows: Sequence[str] = ("import",),
    flow_codes: Optional[Mapping[str, int]] = None,
    backend: str = "pandas",
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
    df_previous: Optional[nw.DataFrame] = None,
    annotate: Optional[Callable[[nw.DataFrame], nw.DataFrame]] = None,
) -> Tuple[nw.DataFrame, NetworkVulnerabilityReport]:
    """Compute the network vulnerability metrics of one HS vintage, without writing.

    Everything :func:`run_network_vulnerabilities` does before the write: reads
    the BACI reconciled flows of the vintage, stamps the vintage onto every row,
    applies the metrics in each requested direction, sends the run parameters
    and the business artifacts to the tracker and annotates the scores. Having
    no write side effect, it can run in a worker process while the parent
    process remains the only writer.

    Args:
        source_conn: Open DuckLake connection on the source (BACI) catalog,
            owned by the caller.
        source_catalog_alias: Alias under which the source catalog is attached.
        source_schema: Schema holding the vintage's reconciled ``fact_table``.
        classification: HS vintage label stamped onto the result.
        result_schema: Schema the scores will be written into (logged as a
            run parameter only).
        metrics: Metric instances to apply (see
            :func:`run_network_vulnerabilities`).
        config: Column conventions and thresholds.
        flows: Directions to compute.
        flow_codes: Flow code of each direction.
        backend: Native eager backend for narwhals computation.
        tracker: Experiment tracker receiving the parameters and artifacts.
        log_artifacts: Whether to build and send the business artifacts.
        df_previous: Previous result of the vintage, enabling the drift
            diagnostics.
        annotate: Function adding descriptive columns to the scores.

    Returns:
        Tuple ``(result, report)``: the scores, ready to be written, and the
        :class:`NetworkVulnerabilityReport` (``created`` left ``False``).

    Raises:
        ValueError: If ``flows`` is empty or unknown, or if the source frame
            is missing a column required by one of the metrics.
    """
    # Initialisation de la liste des métriques, restreinte aux sens demandés
    metric_list = _metrics_for_flows(
        list(metrics) if metrics is not None else default_network_metrics(config, flows),
        flows,
    )
    codes = dict(flow_codes) if flow_codes is not None else flow_code_map(config)

    # Paramètres de l'exécution : configuration aplatie et contexte
    tracker.log_params(
        run_params(
            config,
            {
                "source_schema": source_schema,
                "result_schema": result_schema,
                "classification": classification,
                "backend": backend,
                "metrics": [metric.name for metric in _unique_by_name(metric_list)],
                "flows": list(flows),
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
        data, metric_list, config, flows=flows, flow_codes=codes, df_previous=df_previous
    )
    report.classification = classification

    # Artefacts de synthèse, un dossier par sens
    if log_artifacts:
        for flow, sub in report.flows.items():
            log_network_vulnerability_artifacts(
                tracker,
                df_result=result.filter(nw.col(config.flow_col) == codes[flow]),
                report=sub,
                metrics=[metric for metric in metric_list if metric.flow == flow],
                config=config,
                flow=flow,
            )

    # Colonnes descriptives dérivées des clés, ajoutées hors clé primaire
    if annotate is not None:
        result = annotate(result)

    return result, report


# Fonction d'écriture du résultat d'un millésime
def write_network_vintage(
    result: nw.DataFrame,
    report: NetworkVulnerabilityReport,
    *,
    classification: str,
    config: NetworkVulnerabilityConfig,
    result_conn: Optional[duckdb.DuckDBPyConnection],
    result_catalog_alias: Optional[str],
    result_schema: str,
    write_options: Optional[Mapping[str, Any]] = None,
    writer: Optional[TableWriter] = None,
) -> None:
    """Upsert the scores of a vintage and record the outcome on its reports.

    Args:
        result: Scores returned by :func:`compute_network_vintage`.
        report: Report returned by :func:`compute_network_vintage`, updated in
            place (``created`` and the vintage of each direction).
        classification: HS vintage label (write label).
        config: Column conventions (primary key).
        result_conn: Open connection on the result catalog (unused when
            ``writer`` is given).
        result_catalog_alias: Alias of the result catalog.
        result_schema: Schema to create or upsert the scores into.
        write_options: Extra keyword arguments of the library writer.
        writer: Writer of the result table, ``(frame, primary_keys) ->
            created``; ``write_options`` is ignored when it is given.
    """
    # Écriture dans le schéma résultat : le frame narwhals est passé tel quel,
    # builder/updater de dt_ducklake_manager acceptant IntoDataFrame. Le flux
    # entre dans la clé primaire (une ligne par sens et par cellule)
    report.created = _write_result(
        result,
        (*config.key_columns, config.flow_col),
        writer=writer,
        conn=result_conn,
        catalog_alias=result_catalog_alias,
        schema=result_schema,
        write_options={"label": classification, **(write_options or {})},
    )
    # Millésime et issue de l'écriture reportés sur les rapports par sens
    for sub in report.flows.values():
        sub.classification = classification
        sub.created = report.created


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
    flows: Sequence[str] = ("import",),
    flow_codes: Optional[Mapping[str, int]] = None,
    backend: str = "pandas",
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
    df_previous: Optional[nw.DataFrame] = None,
    write_options: Optional[Mapping[str, Any]] = None,
    writer: Optional[TableWriter] = None,
    annotate: Optional[Callable[[nw.DataFrame], nw.DataFrame]] = None,
) -> NetworkVulnerabilityReport:
    """Compute the network vulnerability metrics of one HS vintage and persist them.

    Reads the BACI reconciled-flow table of a vintage, **stamps the vintage onto
    every row** as the ``classification`` column, applies every metric in each
    requested direction, and upserts the scores (one column per metric plus one
    boolean ``{metric}_ALERT`` column) into the result schema keyed by
    ``config.key_columns`` plus ``config.flow_col`` — ``nomenclature x product
    x year x flow``, the nomenclature being the primary key the partner-level
    result table does not carry.

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
            :func:`~macroforecast.trade.vulnerabilities.network_metrics.default_network_metrics`
            instantiated for ``flows``; explicit instances of another direction
            are ignored.
        config: Column conventions and thresholds.
        flows: Directions to compute (``"import"``, ``"export"``). Only the
            rows of these flows are written: the rows of another flow already
            in the result table are left untouched by the upsert.
        flow_codes: Flow code of each direction. Defaults to the
            configuration's ``import_flow`` / ``export_flow``.
        backend: Native eager backend for narwhals computation (``"pandas"``
            or, when installed, ``"polars"``/``"pyarrow"``).
        tracker: Experiment tracker receiving the run parameters and artifacts.
            Defaults to the null tracker, so an unconfigured run behaves exactly
            as before. The *metrics* are left to the caller, which sends the
            per-direction ``report.flows[flow].to_metrics()`` once the report is
            complete.
        log_artifacts: Whether to build and send the business artifacts (most
            exposed products, alert counts, unscored cells, deciles), one
            sub-directory per direction.
        df_previous: Result of the previous run over the same vintage, enabling
            the drift diagnostics (see :func:`read_previous_network_result`).
        write_options: Extra keyword arguments forwarded to
            :func:`~statflows.storage.ducklake.tables.write_dataframe`
            (``update_options``, ``run_id``, ``commit_message``). ``None``
            keeps the library defaults.
        writer: Writer of the result table, ``(frame, primary_keys) -> created``
            (e.g. ``kedro_pipeline.io.ducklake.DuckLakeTable.writer``), which
            then carries the write options itself; ``write_options`` is
            ignored. ``None`` writes through
            :func:`~statflows.storage.ducklake.tables.write_dataframe`.
        annotate: Function adding descriptive columns to the scores before
            the write (e.g. whether the vintage is the one in force each
            year). ``None`` writes the scores as computed.

    Returns:
        A :class:`NetworkVulnerabilityReport` summarising the run, with one
        sub-report per direction in ``report.flows``.

    Raises:
        ValueError: If ``result_conn`` is supplied without
            ``result_catalog_alias``, if ``flows`` is empty or unknown, or if the
            source frame is missing a column required by one of the metrics.
    """
    # Liste des métriques et codes de flux : validés avant toute lecture
    codes = dict(flow_codes) if flow_codes is not None else flow_code_map(config)
    # Catalogue résultat : partagé avec la source par défaut. Une connexion
    # distincte impose de nommer son alias, qui n'est pas déductible.
    if result_conn is not None and result_catalog_alias is None:
        raise ValueError(
            "result_catalog_alias is required when result_conn is supplied"
        )
    result_conn = result_conn if result_conn is not None else source_conn
    result_catalog_alias = result_catalog_alias or source_catalog_alias

    # Calcul du millésime (lecture, métriques, artefacts, annotation)
    result, report = compute_network_vintage(
        source_conn,
        source_catalog_alias=source_catalog_alias,
        source_schema=source_schema,
        classification=classification,
        result_schema=result_schema,
        metrics=metrics,
        config=config,
        flows=flows,
        flow_codes=codes,
        backend=backend,
        tracker=tracker,
        log_artifacts=log_artifacts,
        df_previous=df_previous,
        annotate=annotate,
    )

    # Écriture dans le schéma résultat
    write_network_vintage(
        result,
        report,
        classification=classification,
        config=config,
        result_conn=result_conn,
        result_catalog_alias=result_catalog_alias,
        result_schema=result_schema,
        write_options=write_options,
        writer=writer,
    )
    return report
