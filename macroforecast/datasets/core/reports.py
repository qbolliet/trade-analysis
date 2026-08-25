"""Structured diagnostics of a download run.

Applies the "diagnostics are data, not logs" principle to the download layer:
everything the run learns — per sub-request outcomes, dimension post-filtering,
duplicates, HTTP behaviour, rate-limit waits, structure cache hits — is carried
by the returned reports instead of only being rendered into log strings. A
consuming project can then feed an experiment tracker (MLflow, kedro-mlflow, …)
without parsing logs, and the logging itself stays what it is: the
human-readable counterpart.

The module is deliberately **self-contained**: it depends on nothing outside
``macroforecast.datasets``, so the sub-package stays extractable as a standalone,
shareable package. :func:`flatten_metrics` duplicates a couple of dozen lines of
``macroforecast.tracking.base`` for that very reason — the alternative would be a
cross-package dependency the extraction is meant to avoid.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import asdict, dataclass, field, fields, is_dataclass
import math
import re
from typing import Any, Dict, List, Mapping, Optional
# Module de manipulation de données
import pandas as pd

# Caractères admis par les serveurs de suivi dans un nom de métrique
_FORBIDDEN_KEY_CHARS = re.compile(r"[^0-9a-zA-Z_\-./ :]")


# ──────────────────────────────────────────────────────────────────────
# Aplatissement des rapports
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : parcours récursif d'un rapport
def _walk(payload: Any, prefix: str) -> Dict[str, Any]:
    """Flatten a dataclass, mapping or scalar into dotted keys.

    Args:
        payload: Dataclass instance, mapping or scalar to flatten.
        prefix: Key prefix already accumulated (may be empty).

    Returns:
        Mapping of dotted keys to leaf values.
    """
    # Rapport structuré : parcours de ses champs
    if is_dataclass(payload) and not isinstance(payload, type):
        walked: Dict[str, Any] = {}
        for report_field in fields(payload):
            key = f"{prefix}.{report_field.name}" if prefix else report_field.name
            walked.update(_walk(getattr(payload, report_field.name), key))
        return walked
    # Dictionnaire : parcours de ses entrées
    if isinstance(payload, Mapping):
        walked = {}
        for name, value in payload.items():
            key = f"{prefix}.{name}" if prefix else str(name)
            walked.update(_walk(value, key))
        return walked
    # Feuille
    return {prefix: payload}


# Fonction d'aplatissement d'un rapport en métriques
def flatten_metrics(payload: Any, prefix: str = "") -> Dict[str, float]:
    """Flatten every finite numeric field of a report into dotted metric keys.

    Walks dataclasses and mappings recursively. Booleans are cast to ``0``/``1``;
    strings, ``None``, sequences and pandas objects are dropped, as are ``NaN``
    and infinities — tracking servers reject them.

    Args:
        payload: Report (dataclass instance) or mapping to flatten.
        prefix: Prefix prepended to every key, e.g. ``"download"``.

    Returns:
        Mapping of dotted metric names to finite floats.

    Examples:
        >>> flatten_metrics(DownloadReport(processed=3), prefix="download")["download.processed"]
        3.0
    """
    # Initialisation du dictionnaire des métriques
    metrics: Dict[str, float] = {}
    for key, value in _walk(payload, prefix).items():
        # Exclusion des types non numériques (les booléens sont des entiers)
        if isinstance(value, bool):
            value = int(value)
        elif not isinstance(value, (int, float)):
            continue
        # Exclusion des valeurs non finies, rejetées par les serveurs de suivi
        numeric = float(value)
        if not math.isfinite(numeric):
            continue
        metrics[_FORBIDDEN_KEY_CHARS.sub("_", key)] = numeric
    return metrics


# ──────────────────────────────────────────────────────────────────────
# Compteurs des briques mutualisées
# ──────────────────────────────────────────────────────────────────────

# Compteurs de la couche HTTP
@dataclass
class HttpStats:
    """Counters of the HTTP layer, accumulated by :class:`APIClient`.

    What used to be visible only through ``logger.debug``/``logger.error`` lines
    around each request.

    Attributes:
        n_requests: Requests issued (retries handled inside ``urllib3`` are not
            counted separately).
        n_failures: Requests that ended in an exception.
        total_seconds: Cumulated wall-clock time spent in requests.
        total_bytes: Cumulated size of the response bodies.
        status_counts: Number of responses per HTTP status code.
    """
    n_requests: int = 0
    n_failures: int = 0
    total_seconds: float = 0.0
    total_bytes: int = 0
    status_counts: Dict[str, int] = field(default_factory=dict)

    # Enregistrement d'une réponse
    def record(self, status_code: Optional[int], seconds: float, n_bytes: int) -> None:
        """Record one completed request.

        Args:
            status_code: HTTP status code, ``None`` when no response was
                obtained (connection error).
            seconds: Wall-clock duration of the request.
            n_bytes: Size of the response body.
        """
        # Comptage de la requête et de son coût
        self.n_requests += 1
        self.total_seconds += seconds
        self.total_bytes += n_bytes
        # Histogramme des statuts ; clé textuelle pour rester sérialisable
        key = str(status_code) if status_code is not None else "error"
        self.status_counts[key] = self.status_counts.get(key, 0) + 1

    # Enregistrement d'un échec
    def record_failure(self, status_code: Optional[int], seconds: float) -> None:
        """Record one failed request.

        Args:
            status_code: HTTP status code when the failure carried a response.
            seconds: Wall-clock duration before the failure.
        """
        # Un échec reste une requête, comptée à part
        self.record(status_code, seconds, 0)
        self.n_failures += 1

    # Réinitialisation des compteurs
    def reset(self) -> None:
        """Zero every counter, e.g. between two queries."""
        # Remise à zéro sur place : les appelants gardent leur référence
        self.n_requests = 0
        self.n_failures = 0
        self.total_seconds = 0.0
        self.total_bytes = 0
        self.status_counts = {}

    # Copie figée des compteurs
    def snapshot(self) -> "HttpStats":
        """Return an independent copy of the current counters.

        Returns:
            A detached :class:`HttpStats`, safe to store in a per-query report.
        """
        return HttpStats(
            n_requests=self.n_requests,
            n_failures=self.n_failures,
            total_seconds=self.total_seconds,
            total_bytes=self.total_bytes,
            status_counts=dict(self.status_counts),
        )


# Compteurs du limiteur de débit
@dataclass
class RateLimitStats:
    """Counters of the rate limiter.

    The wait it imposes is the main hidden cost of a long download; it used to
    be visible only through ``logger.debug`` lines.

    Attributes:
        n_acquisitions: Number of acquisitions granted.
        total_wait_seconds: Cumulated time spent waiting for a slot.
        max_wait_seconds: Longest single wait.
        remaining_requests: Slots still available in the current window.
    """
    n_acquisitions: int = 0
    total_wait_seconds: float = 0.0
    max_wait_seconds: float = 0.0
    remaining_requests: int = 0


# ──────────────────────────────────────────────────────────────────────
# Rapport d'une récupération (niveau client)
# ──────────────────────────────────────────────────────────────────────

# Diagnostic d'une récupération de données par le client
@dataclass
class FetchReport:
    """Diagnostics of one data retrieval, gathered by the SDMX client.

    Filled by ``AbstractSDMXClient._execute_query_pipeline`` and exposed as
    ``client.last_fetch_report_``.

    Attributes:
        n_requests: Number of split sub-requests issued.
        n_request_errors: Sub-requests that genuinely failed.
        n_no_records: Sub-requests answered "no records" (a valid empty result,
            not an error).
        n_empty_responses: Sub-requests returning no row after post-filtering.
        rows_fetched: Rows returned by the provider, before dimension
            post-filtering.
        rows_after_filter: Rows left after post-filtering — the gap with
            ``rows_fetched`` is the earliest indicator of a silent data loss
            (a wildcard returning more or less than requested).
        n_duplicates: Duplicate rows detected on the key columns.
        structure_from_cache: Whether the dataflow structure came from the
            registry (``True``) or had to be fetched (``False``); ``None`` when
            no structure was resolved.
    """
    n_requests: int = 0
    n_request_errors: int = 0
    n_no_records: int = 0
    n_empty_responses: int = 0
    rows_fetched: int = 0
    rows_after_filter: int = 0
    n_duplicates: int = 0
    structure_from_cache: Optional[bool] = None


# ──────────────────────────────────────────────────────────────────────
# Rapport d'une requête (niveau orchestrateur)
# ──────────────────────────────────────────────────────────────────────

# Diagnostic du traitement d'une requête
@dataclass
class QueryReport:
    """Diagnostics of a single query processed by the download orchestrator.

    One row of the machine-readable twin of the run log: what was asked, what
    came back, what was written, how long it took and what failed.

    Attributes:
        identity_key: Identity key of the query in the download registry.
        agency: Provider agency.
        dataflow: Dataflow identifier.
        schema: DuckLake schema the data was written to.
        incremental: Whether the query was an incremental download (a previous
            download date existed) rather than a full one.
        rows_written: Rows written or upserted into DuckLake.
        table_created: Whether the schema was created (vs. upserted).
        empty: Whether the query returned no new data.
        duration_seconds: Wall-clock duration of the whole query processing.
        error_type: Exception class name when the query failed, ``None``
            otherwise.
        error_message: Exception message when the query failed.
        fetch: Diagnostics of the retrieval itself.
        http: HTTP counters accumulated while processing the query.
        rate_limit: Rate-limiter counters accumulated while processing it.
    """
    identity_key: str = ""
    agency: str = ""
    dataflow: str = ""
    schema: str = ""
    incremental: bool = False
    rows_written: int = 0
    table_created: bool = False
    empty: bool = False
    duration_seconds: float = 0.0
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    fetch: FetchReport = field(default_factory=FetchReport)
    http: HttpStats = field(default_factory=HttpStats)
    rate_limit: RateLimitStats = field(default_factory=RateLimitStats)

    # Mise en forme des métriques d'une requête
    def to_metrics(self, prefix: str = "query") -> Dict[str, float]:
        """Flatten the numeric diagnostics of the query.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.
        """
        return flatten_metrics(self, prefix=prefix)


# ──────────────────────────────────────────────────────────────────────
# Rapport d'exécution (niveau run)
# ──────────────────────────────────────────────────────────────────────

# Structure de données résumant l'exécution d'un téléchargement
@dataclass
class DownloadReport:
    """Summary of a download run.

    The five historical counters are kept as they were; the run now also
    carries its per-query detail, so a consuming project can log both run-level
    metrics and a per-query table.

    Attributes:
        processed: Number of queries processed.
        rows_written: Total rows written/upserted into DuckLake.
        empty: Number of queries that returned no new data.
        errors: Number of queries that failed.
        stopped_early: Whether the run stopped on the graceful-shutdown
            deadline before processing every query.
        n_queries_planned: Number of queries handed to the run.
        n_queries_remaining: Queries left unprocessed on an early stop.
        n_tables_created: Schemas created during the run (vs. upserted).
        n_structures_fetched: Dataflow structures downloaded (registry misses).
        duration_seconds: Wall-clock duration of the whole run.
        queries: Per-query diagnostics, in processing order.
    """
    processed: int = 0
    rows_written: int = 0
    empty: int = 0
    errors: int = 0
    stopped_early: bool = False
    # Contexte de l'exécution
    n_queries_planned: int = 0
    n_queries_remaining: int = 0
    n_tables_created: int = 0
    n_structures_fetched: int = 0
    duration_seconds: float = 0.0
    # Détail par requête (principe « les diagnostics sont des données »)
    queries: List[QueryReport] = field(default_factory=list)

    # Agrégats dérivés du détail par requête
    def aggregates(self) -> Dict[str, float]:
        """Sum the per-query diagnostics into run-level counters.

        Returns:
            Mapping of aggregate names to values: sub-requests issued and
            failed, no-record answers, rows fetched before and after
            post-filtering, duplicates, HTTP requests, failures, seconds and
            bytes, and rate-limit waits.

        Examples:
            >>> report = DownloadReport(queries=[QueryReport(rows_written=5)])
            >>> report.aggregates()["n_requests"]
            0.0
        """
        # Champs sommés, exprimés comme des chemins dans le rapport par requête
        paths = {
            "n_requests": ("fetch", "n_requests"),
            "n_request_errors": ("fetch", "n_request_errors"),
            "n_no_records": ("fetch", "n_no_records"),
            "n_empty_responses": ("fetch", "n_empty_responses"),
            "rows_fetched": ("fetch", "rows_fetched"),
            "rows_after_filter": ("fetch", "rows_after_filter"),
            "n_duplicates": ("fetch", "n_duplicates"),
            "n_http_requests": ("http", "n_requests"),
            "n_http_failures": ("http", "n_failures"),
            "http_seconds": ("http", "total_seconds"),
            "http_bytes": ("http", "total_bytes"),
            "rate_limit_wait_seconds": ("rate_limit", "total_wait_seconds"),
            "n_rate_limit_acquisitions": ("rate_limit", "n_acquisitions"),
        }
        # Somme sur l'ensemble des requêtes traitées
        return {
            name: float(
                sum(
                    getattr(getattr(query, group), attribute)
                    for query in self.queries
                )
            )
            for name, (group, attribute) in paths.items()
        }

    # Mise en forme des métriques du run
    def to_metrics(self, prefix: str = "download") -> Dict[str, float]:
        """Flatten the run diagnostics into dotted metric keys.

        The per-query detail is *not* expanded here — it belongs in
        :meth:`to_frame`, logged as a table — but its aggregates are.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> DownloadReport(processed=2).to_metrics()["download.processed"]
            2.0
        """
        # Compteurs de run, hors détail par requête
        summary = {
            report_field.name: getattr(self, report_field.name)
            for report_field in fields(self)
            if report_field.name != "queries"
        }
        metrics = flatten_metrics(summary, prefix=prefix)
        # Agrégats du détail par requête
        metrics.update(flatten_metrics(self.aggregates(), prefix=prefix))
        return metrics

    # Mise en forme tabulaire du détail par requête
    def to_frame(self) -> pd.DataFrame:
        """Render the per-query diagnostics as a table.

        Directly consumable by a tracker's ``log_table``, and readable on its
        own when investigating a partial run.

        Returns:
            One row per processed query, nested reports flattened into dotted
            column names.
        """
        # Aplatissement de chaque rapport de requête, valeurs textuelles incluses
        rows = [_walk(asdict(query), "") for query in self.queries]
        return pd.DataFrame(rows)
