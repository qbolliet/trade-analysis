"""Coverage audit of a downloaded source: what is planned, downloaded and stored.

Run at the end of a download, it confronts the planned queries with the
download registry (queries never downloaded, share downloaded, naive time to
completion) and reads the stored fact table through **one aggregated SQL
query** grouped by reporter (first and last year, number of products, number of
rows), never by loading the table. The reporters expected to cover the whole
history of the source (``COVERAGE.EXPECTED_FULL_HISTORY_REPORTERS``) whose first
stored year comes after the first year of the analysis are counted: a catch-up
still running, or a source that does not go back that far.

Published metrics (``coverage/*``): ``queries_total``,
``queries_never_downloaded``, ``share_downloaded`` (``1 - never / total``),
``min_period`` (first stored year), ``reporters_below_start`` and ``eta_days``
(never downloaded queries divided by the queries processed by this run, i.e. the
days left at the current pace). Artifacts: ``coverage/by_reporter.csv``
(reporter, min_period, max_period, n_products, n_rows) and, for the sources
whose queries are split by year (Comtrade), ``coverage/by_year.csv`` (year,
share_queries_downloaded). Comext queries carry every year at once, so their
download share does not vary by year and no yearly table is produced.

No environment variable and no YAML path are read here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
from typing import Any, Dict, Mapping, Optional, Sequence

# Modules de manipulation de données
import pandas as pd

# Modules du package
from kedro_pipeline.io.registry_views import period_year, products_key, split_codes
from kedro_pipeline.steps.baci import completeness_by_year
from kedro_pipeline.steps.result import StepResult, capturing
from macroforecast.tracking import RunTracker

# Initialisation du logger
logger = logging.getLogger(__name__)

# Sources dont les requêtes sont découpées par année (lots de produits x période)
_YEARLY_SOURCES = frozenset({"comtrade"})
# Colonnes par défaut des tables téléchargées, par source
DEFAULT_COLUMNS: Mapping[str, Mapping[str, str]] = {
    "eurostat": {"REPORTER": "reporter", "PERIOD": "TIME_PERIOD", "PRODUCT": "product"},
    "comtrade": {"REPORTER": "reporterCode", "PERIOD": "period", "PRODUCT": "cmdCode"},
}


# Fonction de test du téléchargement d'une requête planifiée
def query_downloaded(query: Any, source: str, downloaded: Mapping[Any, Any]) -> bool:
    """Tell whether a planned query was downloaded at least once.

    Args:
        query: Planned query: ``periods`` / ``products`` for Comtrade,
            ``dimensions["reporter"]`` / ``dimensions["product"]`` for Comext.
        source: ``"comtrade"`` or ``"eurostat"``.
        downloaded: Registry view content: ``year -> {products key -> date}``
            for Comtrade, ``(reporter, product) -> date`` for Comext.

    Returns:
        ``True`` when the registry covers the query (every reporter x product
        pair for Comext).

    Examples:
        >>> from types import SimpleNamespace as Query
        >>> query_downloaded(Query(dimensions={"reporter": "FR", "product": "01"}),
        ...                  "eurostat", {("FR", "01"): "x"})
        True
        >>> query_downloaded(Query(periods="2023", products=["01"]), "comtrade", {2023: {}})
        False
    """
    if source in _YEARLY_SOURCES:
        return products_key(query.products) in downloaded.get(period_year(query.periods), {})
    dims = getattr(query, "dimensions", None) or {}
    pairs = [
        (reporter, product)
        for reporter in split_codes(dims.get("reporter"))
        for product in split_codes(dims.get("product"))
    ]
    return bool(pairs) and all(pair in downloaded for pair in pairs)


# Fonction de construction de la requête agrégée par reporter
def coverage_query(qualified_name: str, columns: Mapping[str, str]) -> str:
    """Build the aggregated query of the stored coverage, one row per reporter.

    Args:
        qualified_name: Quoted qualified name of the fact table.
        columns: ``REPORTER``, ``PERIOD`` and ``PRODUCT`` column names.

    Returns:
        The SQL query (columns ``reporter``, ``min_period``, ``max_period``,
        ``n_products``, ``n_rows``).

    Examples:
        >>> print(coverage_query('"c"."s"."fact_table"',
        ...       {"REPORTER": "r", "PERIOD": "p", "PRODUCT": "k"}).splitlines()[0])
        SELECT CAST("r" AS VARCHAR) AS reporter,
    """
    year = f'CAST(substr(CAST("{columns["PERIOD"]}" AS VARCHAR), 1, 4) AS INTEGER)'
    return (
        f'SELECT CAST("{columns["REPORTER"]}" AS VARCHAR) AS reporter,\n'
        f"       min({year}) AS min_period,\n"
        f"       max({year}) AS max_period,\n"
        f'       count(DISTINCT "{columns["PRODUCT"]}") AS n_products,\n'
        f"       count(*) AS n_rows\n"
        f"FROM {qualified_name}\n"
        f"GROUP BY 1\n"
        f"ORDER BY 1"
    )


# Fonction de calcul des métriques de couverture (pure)
def coverage_metrics_of(
    *,
    n_planned: int,
    n_never: int,
    by_reporter: pd.DataFrame,
    expected_reporters: Sequence[str],
    start_year: Optional[int],
    n_processed: Optional[int],
) -> Dict[str, float]:
    """Compute the ``coverage/*`` metrics from the counts and the per-reporter table.

    A reporter expected to cover the whole history but absent from the table
    counts as below the start (it has no stored year at all).

    Args:
        n_planned: Number of planned queries.
        n_never: Planned queries never downloaded.
        by_reporter: Per-reporter coverage (:func:`coverage_query`).
        expected_reporters: Reporters expected from the first year on.
        start_year: First year of the analysis for the source.
        n_processed: Queries processed by the current run; ``eta_days`` is
            left out when it is unknown or zero while queries remain.

    Returns:
        The metrics.

    Examples:
        >>> table = pd.DataFrame({"reporter": ["FR"], "min_period": [1995], "max_period": [2024],
        ...                       "n_products": [3], "n_rows": [10]})
        >>> metrics = coverage_metrics_of(n_planned=4, n_never=1, by_reporter=table,
        ...                               expected_reporters=["FR", "DE"], start_year=1994,
        ...                               n_processed=1)
        >>> metrics["coverage/share_downloaded"], metrics["coverage/reporters_below_start"]
        (0.75, 2.0)
        >>> metrics["coverage/eta_days"]
        1.0
    """
    metrics: Dict[str, float] = {
        "coverage/queries_total": float(n_planned),
        "coverage/queries_never_downloaded": float(n_never),
        # Aucune requête planifiée : rien ne manque
        "coverage/share_downloaded": 1.0 - n_never / n_planned if n_planned else 1.0,
    }
    if len(by_reporter):
        metrics["coverage/min_period"] = float(by_reporter["min_period"].min())
    first_years = dict(zip(by_reporter["reporter"].astype(str), by_reporter["min_period"]))
    if start_year is not None:
        metrics["coverage/reporters_below_start"] = float(
            sum(
                1
                for reporter in expected_reporters
                if str(reporter) not in first_years or int(first_years[str(reporter)]) > int(start_year)
            )
        )
    if n_never == 0:
        metrics["coverage/eta_days"] = 0.0
    elif n_processed:
        metrics["coverage/eta_days"] = float(n_never) / float(n_processed)
    return metrics


# Fonction d'étape : audit de couverture d'une source téléchargée
def audit_coverage(
    table: Any,
    registry: Any,
    planned: Sequence[Any],
    *,
    source: str,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    n_processed: Optional[int] = None,
    tracker: Optional[RunTracker] = None,
) -> StepResult:
    """Audit the coverage of a downloaded source and log it to the tracker.

    Args:
        table: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            downloaded fact table (lazy or bound to an open connection).
        registry: :class:`~kedro_pipeline.io.registry_views.DownloadRegistryView`
            of the source.
        planned: Planned queries of the source (uncapped), in plan order.
        source: ``"eurostat"`` or ``"comtrade"``.
        params: Download parameter block of the source; its optional
            ``COVERAGE`` block holds ``EXPECTED_FULL_HISTORY_REPORTERS`` and
            ``COLUMNS`` (``REPORTER``, ``PERIOD``, ``PRODUCT``; defaults of
            :data:`DEFAULT_COLUMNS`).
        runtime: ``runtime`` parameters (``ANALYSIS_START_YEAR.<source>``).
        n_processed: Queries processed by the current download run.
        tracker: Tracker of the open run; metrics and artifacts logged there.

    Returns:
        The step result: one unit per planned query, ``metrics`` ``coverage/*``,
        ``artifacts`` ``coverage/by_reporter.csv`` (and
        ``coverage/by_year.csv`` for Comtrade).

    Raises:
        Exception: Any error of the registry read or of the SQL query; the
            download step catches it, the audit never fails a download.
    """
    tracker = capturing(tracker)
    coverage = dict(params.get("COVERAGE") or {})
    columns = {**DEFAULT_COLUMNS.get(source, {}), **(coverage.get("COLUMNS") or {})}
    start_year = (runtime.get("ANALYSIS_START_YEAR") or {}).get(source)

    # Requêtes jamais téléchargées (lecture du registre par sa vue)
    downloaded = (
        registry.batches_by_year() if source in _YEARLY_SOURCES else registry.pairs_last_download()
    )
    flags = [query_downloaded(query, source, downloaded) for query in planned]
    n_never = flags.count(False)

    # Couverture stockée : une seule requête agrégée par reporter
    by_reporter = table.query(coverage_query(table.qualified_name, columns))
    metrics = coverage_metrics_of(
        n_planned=len(planned),
        n_never=n_never,
        by_reporter=by_reporter,
        expected_reporters=list(coverage.get("EXPECTED_FULL_HISTORY_REPORTERS") or []),
        start_year=int(start_year) if start_year is not None else None,
        n_processed=n_processed,
    )
    tracker.log_metrics(metrics)
    tracker.log_table(by_reporter, "coverage/by_reporter.csv")
    if source in _YEARLY_SOURCES:
        shares = completeness_by_year(planned, downloaded)
        by_year = pd.DataFrame(
            sorted(shares.items()), columns=["year", "share_queries_downloaded"]
        )
        tracker.log_table(by_year, "coverage/by_year.csv")

    # Logging
    logger.info(
        f"Couverture {source} : {len(planned) - n_never}/{len(planned)} requête(s) "
        f"téléchargée(s), {len(by_reporter)} reporter(s) en base."
    )
    return StepResult(
        step="coverage",
        n_units_planned=len(planned),
        n_units_succeeded=len(planned) - n_never,
        metrics=dict(tracker.metrics),
        artifacts=dict(tracker.tables),
        units_label=f"{len(planned)} requêtes planifiées",
    )
