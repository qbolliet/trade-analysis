"""Incremental SDMX → DuckLake download orchestrator.

Production entry point that, for a list of provider queries:

1. Initialises (or reuses) a per-provider DuckLake catalog of downloads,
2. Performs a first **full** download of missing series and an **incremental**
   download of series already present (only newly published observations),
3. Persists a JSON registry of the last-download date per query,
4. Exports the provider structure registry when a new structure was fetched,
5. Stops gracefully after a configurable runtime (e.g. 23 h),
6. Closes the catalog connection cleanly.

The orchestration is provider-agnostic: *how* to fetch the incremental data is
delegated to each client through
:meth:`~macroforecast.datasets.core.client.AbstractSDMXClient.fetch_updates`
"""
# Importation des modules
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from enum import Enum
import logging
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from botocore.exceptions import ClientError
import duckdb
import pandas as pd

# Importation des modules de connexion
from macroforecast.storage import Loader, Saver

from dt_ducklake_manager import (
    DatabaseUpdater,
    DuckLakeConnector,
    DuckLakeTablesBuilder,
)

from .client import AbstractSDMXClient
from .reports import DownloadReport, HttpStats, QueryReport, RateLimitStats
from .structures import DataflowStructure, DataflowStructureRegistry

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de la table de faits DuckLake (convention dt_ducklake_manager)
_FACT_TABLE = "fact_table"
# Nom de la dimension temporelle SDMX (incluse dans la clé primaire)
_TIME_COLUMN = "TIME_PERIOD"
# Clé racine du registre JSON des dates de dernier téléchargement
_REGISTRY_ROOT = "DOWNLOADS"


# ──────────────────────────────────────────────────────────────────────
# Fonctions utilitaires
# ──────────────────────────────────────────────────────────────────────

# Fonction de récupération de l'instant courant en UTC
def _now() -> datetime:
    """Return the current instant as a UTC-aware datetime."""
    return datetime.now(timezone.utc)


# Fonction de parsing d'une chaîne ISO en datetime UTC
def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string into a UTC-aware datetime.

    Args:
        value: ISO-8601 string (``Z`` suffix accepted) or ``None``.

    Returns:
        UTC-aware ``datetime`` or ``None`` when ``value`` is falsy/unparseable.
    """
    # Court-circuit si la valeur est absente
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.warning(f"Could not parse stored date '{value}'")
        return None
    # Normalisation en UTC (les datetimes naïfs sont interprétés comme UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# Fonction de normalisation d'un nom de dataflow en identifiant de schéma SQL
def _schema_name(dataflow: str) -> str:
    """Sanitise a dataflow identifier into a safe DuckLake schema name.

    Non-alphanumeric characters (e.g. ``@``, ``-``, ``.`` in
    ``"DSD_KEI@DF_KEI"``) are replaced by underscores, and a leading digit is
    prefixed so the result is a valid unquoted SQL identifier.

    Args:
        dataflow: Dataflow identifier.

    Returns:
        Sanitised schema name.

    Examples:
        >>> _schema_name("DSD_KEI@DF_KEI")
        'DSD_KEI_DF_KEI'
        >>> _schema_name("namq_10_gdp")
        'namq_10_gdp'
    """
    # Remplacement de tout caractère non alphanumérique par un underscore
    sanitized = "".join(c if c.isalnum() or c == "_" else "_" for c in dataflow)
    # Préfixe si l'identifiant commence par un chiffre (interdit en SQL non quoté)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"df_{sanitized}"
    return sanitized


# Fonction de calcul des clés primaires d'une table à partir de sa structure
def _primary_keys(
    structure: Optional[DataflowStructure],
    df_columns: List[str],
) -> List[str]:
    """Derive the primary-key columns of the DuckLake table.

    The primary key is the set of dataflow dimensions (which uniquely identify
    an observation) intersected with the columns actually present in the
    DataFrame, matched case-insensitively. The time dimension
    (``TIME_PERIOD``) is always appended when present, guaranteeing
    observation uniqueness even if it is not listed among the structure
    dimensions.

    Args:
        structure: Resolved dataflow structure (may be ``None``).
        df_columns: Columns of the retrieved DataFrame.

    Returns:
        Ordered list of primary-key column names (using the DataFrame's
        original casing).
    """
    # Index insensible à la casse : nom en minuscules → nom réel de la colonne
    cols_lower = {c.lower(): c for c in df_columns}
    primary_keys: List[str] = []

    # Dimensions de la structure présentes dans le DataFrame
    if structure is not None:
        for dim in structure.dimensions:
            actual = cols_lower.get(dim.name.lower())
            if actual is not None and actual not in primary_keys:
                primary_keys.append(actual)

    # Ajout systématique de la dimension temporelle si présente
    time_actual = cols_lower.get(_TIME_COLUMN.lower())
    if time_actual is not None and time_actual not in primary_keys:
        primary_keys.append(time_actual)

    return primary_keys


# Fonction de conversion d'une valeur en représentation JSON-sérialisable
def _json_safe(value: Any) -> Any:
    """Recursively convert enums and datetimes into JSON-serialisable values.

    Args:
        value: Arbitrary value (possibly nested) from a query ``to_dict()``.

    Returns:
        JSON-serialisable equivalent.
    """
    # Enum → valeur sous-jacente ; datetime → ISO ; conteneurs → récursion
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


# ──────────────────────────────────────────────────────────────────────
# Diagnostics d'exécution
# ──────────────────────────────────────────────────────────────────────
#
# `DownloadReport` et `QueryReport` vivent dans `reports.py` : ils constituent
# le pendant exploitable des journaux, réutilisable par un projet consommateur
# sans dépendre de l'orchestrateur. Ils sont réexportés ici par compatibilité.

# Fonction auxiliaire : sources de compteurs HTTP portées par un client
def _http_stats_sources(client: Any) -> List[HttpStats]:
    """Collect every HTTP counter object a client carries.

    Providers either *are* an ``APIClient`` (Comtrade, UNSD) or *hold* one or
    more (Eurostat, OECD, plus Eurostat's dedicated Comext client). Discovering
    them by attribute keeps the orchestrator provider-agnostic.

    Args:
        client: Provider client.

    Returns:
        The :class:`HttpStats` instances found, possibly empty.
    """
    # Compteurs portés par le client lui-même (héritage d'APIClient)
    sources: List[HttpStats] = []
    own = getattr(client, "stats_", None)
    if isinstance(own, HttpStats):
        sources.append(own)
    # Compteurs des clients HTTP détenus par composition
    for attribute in vars(client).values():
        stats = getattr(attribute, "stats_", None)
        if isinstance(stats, HttpStats):
            sources.append(stats)
    return sources


# Fonction auxiliaire : totalisation de compteurs HTTP
def _total_http_stats(sources: Iterable[HttpStats]) -> HttpStats:
    """Sum several HTTP counter objects into one.

    Args:
        sources: Counter objects to total.

    Returns:
        A detached :class:`HttpStats` holding the sums.
    """
    # Totalisation champ à champ, histogramme de statuts compris
    total = HttpStats()
    for stats in sources:
        total.n_requests += stats.n_requests
        total.n_failures += stats.n_failures
        total.total_seconds += stats.total_seconds
        total.total_bytes += stats.total_bytes
        for status, count in stats.status_counts.items():
            total.status_counts[status] = total.status_counts.get(status, 0) + count
    return total


# Fonction auxiliaire : consommation HTTP d'une requête (différence de compteurs)
def _http_delta(before: HttpStats, after: HttpStats) -> HttpStats:
    """Difference two HTTP snapshots to isolate one query's cost.

    Args:
        before: Snapshot taken before the query.
        after: Snapshot taken after it.

    Returns:
        A :class:`HttpStats` holding what the query alone consumed.
    """
    # Différence champ à champ ; les statuts absents avant valent zéro
    return HttpStats(
        n_requests=after.n_requests - before.n_requests,
        n_failures=after.n_failures - before.n_failures,
        total_seconds=after.total_seconds - before.total_seconds,
        total_bytes=after.total_bytes - before.total_bytes,
        status_counts={
            status: count - before.status_counts.get(status, 0)
            for status, count in after.status_counts.items()
            if count - before.status_counts.get(status, 0) > 0
        },
    )


# Fonction auxiliaire : consommation du limiteur de débit d'une requête
def _rate_limit_delta(
    before: RateLimitStats, after: RateLimitStats
) -> RateLimitStats:
    """Difference two rate-limit snapshots to isolate one query's waits.

    Args:
        before: Snapshot taken before the query.
        after: Snapshot taken after it.

    Returns:
        A :class:`RateLimitStats` holding what the query alone incurred.
    """
    # Attentes et acquisitions imputables à la requête
    return RateLimitStats(
        n_acquisitions=after.n_acquisitions - before.n_acquisitions,
        total_wait_seconds=after.total_wait_seconds - before.total_wait_seconds,
        max_wait_seconds=after.max_wait_seconds,
        remaining_requests=after.remaining_requests,
    )


# Fonction auxiliaire : instantané des compteurs d'un client
def _client_snapshot(client: Any) -> tuple[HttpStats, RateLimitStats]:
    """Snapshot the HTTP and rate-limit counters of a client.

    Args:
        client: Provider client.

    Returns:
        Tuple ``(http, rate_limit)`` of detached counter objects.
    """
    # Compteurs HTTP totalisés sur toutes les sessions du client
    http = _total_http_stats(_http_stats_sources(client))
    # Compteurs du limiteur de débit, absent chez certains providers
    limiter = getattr(client, "rate_limiter", None)
    rate_limit = limiter.get_stats() if limiter is not None else RateLimitStats()
    return http, rate_limit


# ──────────────────────────────────────────────────────────────────────
# Orchestrateur
# ──────────────────────────────────────────────────────────────────────

# Classe orchestrant le téléchargement incrémental vers DuckLake
class SDMXDownloader:
    """Incremental SDMX → DuckLake download orchestrator.

    Drives the full run for a single provider: prioritises the queries,
    fetches each one (full or incremental via
    :meth:`AbstractSDMXClient.fetch_updates`), writes the result into the
    provider's DuckLake catalog (one schema per dataflow), and maintains the
    JSON registry of last-download dates.

    The orchestrator owns the DuckLake connection (opened from the supplied
    connector and closed in a ``finally`` block) but **not** the SDMX client,
    whose lifetime is managed by the caller.

    Args:
        client: Provider SDMX client (e.g. ``EurostatClient``, ``OECDClient``).
        connector: DuckLake connector for the provider catalog. Opened once and
            closed by :meth:`run`; the schema is selected per dataflow.
        structures_path: Path to the JSON structure registry of the provider.
        last_download_path: Path to the JSON registry of last-download dates.
        n_observations: Number of most-recent observations to retrieve in
            incremental mode for providers without a per-observation filter
            (Eurostat).
        fresh_registry: When ``True``, start from an empty structure registry
            instead of loading ``structures_path``.
        max_runtime: Graceful-shutdown delay. The loop stops between two
            queries once this much wall-clock time has elapsed. ``None``
            disables the deadline.
        categorical_threshold: ``categorical_threshold`` forwarded to
            dt_ducklake_manager. ``None`` (default) disables dimension tables.
        bucket: Optional S3 bucket name. When provided, the structure and
            last-download JSON registries are read from and written to S3
            (the registry paths are used as object keys); when ``None``
            (default) they live on the local filesystem.
        storage_options: Optional keyword arguments forwarded to the
            ``Loader``/``Saver`` ``connect()`` method (e.g. ``endpoint_url``,
            ``aws_access_key_id``, ``aws_secret_access_key``, ``verify``). Only
            used when ``bucket`` is set; if omitted, the connection is
            established lazily from the standard AWS environment variables.
        on_query_complete: Callback invoked with the :class:`QueryReport` of
            every processed query, successful or not. The seam through which a
            consuming project streams per-query metrics to its experiment
            tracker without this package ever knowing about it. A failing
            callback warns and never interrupts the download.
    """

    # Initialisation
    def __init__(
        self,
        client: AbstractSDMXClient,
        connector: DuckLakeConnector,
        structures_path: Union[str, Path],
        last_download_path: Union[str, Path],
        *,
        n_observations: int = 10,
        fresh_registry: bool = False,
        max_runtime: Optional[timedelta] = timedelta(hours=23),
        categorical_threshold: Optional[int] = None,
        bucket: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        on_query_complete: Optional[Callable[[QueryReport], None]] = None,
    ) -> None:
        # Dépendances injectées
        self._client = client
        self._connector = connector
        self._structures_path = Path(structures_path)
        self._last_download_path = Path(last_download_path)

        # Paramètres d'exécution
        self._n_observations = n_observations
        self._max_runtime = max_runtime
        self._categorical_threshold = categorical_threshold

        # Accès au stockage des registres JSON (local ou S3 selon ``bucket``).
        # Instances réutilisables : la connexion S3 paresseuse est ainsi établie
        # une seule fois et partagée par toutes les lectures/écritures.
        self._bucket = bucket
        self._loader = Loader()
        self._saver = Saver()
        # Connexion explicite uniquement si des options sont fournies ; sinon la
        # connexion reste paresseuse (variables d'environnement AWS au 1er accès).
        if bucket is not None and storage_options:
            self._loader.connect(**storage_options)
            self._saver.connect(**storage_options)

        # Alias du catalogue DuckLake (pour les requêtes d'introspection)
        self._catalog_alias = connector.catalog_alias

        # Registre des dates de dernier téléchargement (identity_key → entrée)
        self._registry: Dict[str, Dict[str, Any]] = {}
        self._load_registry()

        # Registre des structures, injecté dans le client pour mutualiser le cache
        self._structure_registry = DataflowStructureRegistry()
        if not fresh_registry:
            structures_data = self._read_json(self._structures_path)
            if structures_data:
                self._structure_registry.load_from_dict(structures_data)
        self._client.structure_registry = self._structure_registry

        # Rappel de fin de requête (seam d'observabilité côté appelant)
        self._on_query_complete = on_query_complete

        # Instant de démarrage (renseigné dans run())
        self._t0: Optional[datetime] = None

    # Méthode principale d'exécution du téléchargement
    def run(
        self,
        queries: Union[Any, Iterable[Any]],
    ) -> DownloadReport:
        """Run the download for the provided queries.

        Args:
            queries: A single provider query or an iterable/iterator of
                queries. Each query must expose ``identity_key()``,
                ``to_dict()``, ``dataflow`` and ``agency``.

        Returns:
            A :class:`DownloadReport` summarising the run.
        """
        # Normalisation en liste (un objet requête isolé est accepté)
        query_list = self._as_query_list(queries)

        # Priorisation : jamais téléchargées d'abord, puis les plus anciennes
        query_list = self._prioritize(query_list)

        # Initialisation du chronomètre de graceful shutdown
        self._t0 = _now()

        # Initialisation du rapport de téléchargement
        report = DownloadReport(n_queries_planned=len(query_list))

        # Extraction des structures connues à l'initialisation : tout ajout 
        # (par fetch_updates ou par la résolution explicite) déclenchera 
        # l'export du registre en fin de run
        initial_structure_keys = set(self._structure_registry.list_structures())

        # Ouverture de la connexion au catalogue DuckLake
        conn = self._connector.connect()
        try:
            # Parcours des requêtes
            for index, query in enumerate(query_list):
                # Arrêt anticipé si le délai maximal est atteint
                if self._deadline_reached():
                    # Requêtes laissées de côté
                    report.n_queries_remaining = len(query_list) - index
                    # Logging
                    logger.warning(
                        "Graceful shutdown: max runtime reached, stopping "
                        f"after {report.processed} queries, "
                        f"{report.n_queries_remaining} left"
                    )
                    report.stopped_early = True
                    break

                # Diagnostic de la requête, renseigné qu'elle aboutisse ou non
                query_report = QueryReport(
                    identity_key=query.identity_key(),
                    agency=getattr(query, "agency", ""),
                    dataflow=getattr(query, "dataflow", ""),
                )
                # Traitement d'une requête (les erreurs sont isolées par requête)
                try:
                    self._process_query(conn, query, report, query_report)
                # Si échec : la date n'est pas mise à jour → la requête sera retentée
                except Exception as e:
                    # L'échec devient une donnée du rapport, pas seulement un log
                    query_report.error_type = type(e).__name__
                    query_report.error_message = str(e)[:500]
                    # Logging
                    logger.exception(
                        f"Query {query.identity_key()} failed: {e}"
                    )
                    # Incrément des erreurs
                    report.errors += 1
                finally:
                    # Publication du diagnostic de la requête
                    report.queries.append(query_report)
                    self._notify(query_report)
        finally:
            # Structures nouvellement téléchargées pendant le run
            report.n_structures_fetched = len(
                set(self._structure_registry.list_structures()) - initial_structure_keys
            )
            # Durée totale du run
            report.duration_seconds = (_now() - self._t0).total_seconds()
            # Export du registre des structures si une structure a été ajoutée
            if set(self._structure_registry.list_structures()) != initial_structure_keys:
                # Export (routé vers le local ou S3 selon ``bucket``)
                self._write_json(
                    self._structures_path,
                    self._structure_registry.to_dict(),
                )
                # Logging
                logger.info(f"Structure registry exported to {self._structures_path}")
            # Persistance finale du registre des dates (déjà persisté par requête)
            self._save_registry()
            # Fermeture propre de la connexion au catalogue
            conn.close()
            # Logging
            logger.info("DuckLake connection closed")

        return report

    # ──────────────────────────────────────────────────────────────────
    # Traitement d'une requête
    # ──────────────────────────────────────────────────────────────────

    # Méthode de traitement d'une requête unique
    def _process_query(
        self,
        conn: duckdb.DuckDBPyConnection,
        query: Any,
        report: DownloadReport,
        query_report: QueryReport,
    ) -> None:
        """Process a single query: fetch, write to DuckLake, update registry.

        The DuckLake write is committed **before** the JSON registry is
        updated, so the registry never references data absent from the
        database. The last-download date is updated whether or not new data
        was returned.

        Args:
            conn: Open DuckLake connection.
            query: Provider query object.
            report: Run report to update in place.
            query_report: Per-query diagnostics to fill in place — volumetry,
                sub-request outcomes, HTTP and rate-limit cost, duration.
        """
        # Clé identifiante de la requête
        key = query.identity_key()
        # Recherche de l'entrée associée dans le registre
        entry = self._registry.get(key)
        # Date du dernier téléchargement (None si jamais téléchargée)
        since = _parse_iso(entry.get("last_download")) if entry else None
        # Mode de téléchargement : complet ou incrémental
        query_report.incremental = since is not None

        # Instant capturé avant la requête : sert de nouvelle date de référence
        # (évite de manquer des observations publiées pendant le fetch)
        req_started = _now()
        # Compteurs du client avant la requête : la différence isole son coût
        http_before, rate_before = _client_snapshot(self._client)

        try:
            # Récupération des données (complète ou incrémentale selon ``since``)
            df = self._client.fetch_updates(query, since, self._n_observations)
        finally:
            # Coût de la requête, imputé même si la récupération a échoué
            http_after, rate_after = _client_snapshot(self._client)
            query_report.http = _http_delta(http_before, http_after)
            query_report.rate_limit = _rate_limit_delta(rate_before, rate_after)
            # Diagnostics de récupération tenus par le client
            fetch_report = getattr(self._client, "last_fetch_report_", None)
            if fetch_report is not None:
                query_report.fetch = fetch_report
            query_report.duration_seconds = (_now() - req_started).total_seconds()

        # Incrément des requêtes exécutées
        report.processed += 1
        # Extraction du nom du schéma
        schema = _schema_name(query.dataflow)
        query_report.schema = schema

        # Écriture en base uniquement si des données ont été récupérées
        if df is not None and not df.empty:
            # Résolution de la structure (pour les clés primaires) ; l'appel
            # enregistre aussi la structure dans le registre partagé, dont
            # l'ajout est détecté en fin de run pour déclencher l'export.
            structure = self._client.resolve_query_structure(query)
            query_report.table_created = self._write_dataframe(
                conn, df, structure, schema, query.dataflow
            )
            # Ajout du nombre de lignes écrites
            report.rows_written += len(df)
            report.n_tables_created += int(query_report.table_created)
            query_report.rows_written = len(df)
        else:
            # Incrément des requêtes vides
            report.empty += 1
            query_report.empty = True
            # Logging
            logger.info(f"{query.dataflow}: no new data")

        # ORDRE CRITIQUE : la base est écrite avant la mise à jour du JSON.
        # Mise à jour de la date que le DataFrame soit vide ou non.
        self._registry[key] = {
            "agency": query.agency,
            "dataflow": query.dataflow,
            "params": _json_safe(query.to_dict()),
            "last_download": req_started.isoformat(),
        }
        # Persistance atomique immédiate (rien n'est perdu en cas de shutdown)
        self._save_registry()

    # Méthode d'écriture d'un DataFrame dans le catalogue DuckLake
    def _write_dataframe(
        self,
        conn: duckdb.DuckDBPyConnection,
        df: pd.DataFrame,
        structure: Optional[DataflowStructure],
        schema: str,
        dataflow: str,
    ) -> bool:
        """Create or update the dataflow's DuckLake table with a DataFrame.

        Builds the schema on first encounter (``DuckLakeTablesBuilder``) or
        upserts by primary key on subsequent runs (``DatabaseUpdater``).

        Args:
            conn: Open DuckLake connection.
            df: Non-empty DataFrame to persist.
            structure: Resolved dataflow structure (for primary keys).
            schema: Target DuckLake schema (one per dataflow).
            dataflow: Dataflow identifier (for logging).

        Returns:
            ``True`` if the schema was created, ``False`` if it was upserted —
            an outcome that used to be readable only in the logs.

        Raises:
            ValueError: If no primary-key column can be resolved, or if the
                update operation reports failure.
        """
        # Calcul des clés primaires (dimensions + TIME_PERIOD)
        primary_keys = _primary_keys(structure, list(df.columns))
        if not primary_keys:
            raise ValueError(
                f"No primary-key column resolved for '{dataflow}'. "
                f"Columns: {list(df.columns)}"
            )

        # Distinction création / mise à jour selon l'existence de la fact table
        if self._fact_table_exists(conn, schema):
            # Mise à jour incrémentale (upsert par clé primaire lue des métadonnées)
            updater = DatabaseUpdater(
                connection=conn,
                categorical_threshold=self._categorical_threshold,
                ducklake_catalog_alias=self._catalog_alias,
                schema=schema,
            )
            success = updater.update_database(
                df,
                use_transaction=True,
                compact_after_update=True,
            )
            # Erreur
            if not success:
                raise ValueError(
                    f"DatabaseUpdater reported failure for '{dataflow}'"
                )
            # Logging
            logger.info(
                f"{dataflow}: updated {len(df)} rows in schema '{schema}'"
            )
            return False
        else:
            # Première rencontre : construction du schéma (métadonnées + fact table)
            builder = DuckLakeTablesBuilder(
                df,
                categorical_threshold=self._categorical_threshold,
                primary_keys=primary_keys,
                connection=conn,
                schema=schema,
            )
            builder.build_schema()
            # Logging
            logger.info(
                f"{dataflow}: created schema '{schema}' with {len(df)} rows "
                f"(primary keys: {primary_keys})"
            )
            return True

    # Méthode de détection de l'existence de la fact table d'un schéma
    def _fact_table_exists(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str,
    ) -> bool:
        """Return whether the fact table already exists in a schema.

        Args:
            conn: Open DuckLake connection.
            schema: Target DuckLake schema.

        Returns:
            ``True`` if ``{schema}.fact_table`` exists in the catalog.
        """
        # Introspection des tables du catalogue attaché
        row = conn.execute(
            "SELECT count(*) FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
            [self._catalog_alias, schema, _FACT_TABLE],
        ).fetchone()
        return bool(row and row[0] > 0)

    # Méthode de publication du diagnostic d'une requête
    def _notify(self, query_report: QueryReport) -> None:
        """Hand a per-query report to the caller's callback.

        No observability failure may interrupt a download: a raising callback
        is warned about and swallowed, exactly as a tracking failure is.

        Args:
            query_report: Diagnostics of the query just processed.
        """
        # Aucun rappel configuré : rien à faire
        if self._on_query_complete is None:
            return
        try:
            self._on_query_complete(query_report)
        except Exception as exc:
            # Logging
            logger.warning(f"on_query_complete callback failed: {exc}")

    # ──────────────────────────────────────────────────────────────────
    # Priorisation, deadline et registre des dates
    # ──────────────────────────────────────────────────────────────────

    # Méthode de normalisation des requêtes en liste
    @staticmethod
    def _as_query_list(queries: Union[Any, Iterable[Any]]) -> List[Any]:
        """Normalise the queries argument into a list.

        Args:
            queries: A single query object or an iterable of queries.

        Returns:
            List of query objects.
        """
        # Objet requête isolé (expose identity_key) → liste singleton
        if hasattr(queries, "identity_key"):
            return [queries]
        return list(queries)

    # Méthode de priorisation des requêtes
    def _prioritize(self, queries: List[Any]) -> List[Any]:
        """Order queries: never-downloaded first, then oldest first.

        Args:
            queries: Query objects.

        Returns:
            Sorted list of queries.
        """
        # Date « minimale » UTC pour départager les requêtes jamais téléchargées
        epoch = datetime.min.replace(tzinfo=timezone.utc)

        # Fonction de tri
        def sort_key(query: Any) -> tuple[int, datetime]:
            entry = self._registry.get(query.identity_key())
            last = _parse_iso(entry.get("last_download")) if entry else None
            # Jamais téléchargée (rang 0) avant déjà téléchargée (rang 1, plus ancienne d'abord)
            if last is None:
                return (0, epoch)
            return (1, last)

        return sorted(queries, key=sort_key)

    # Méthode de vérification du dépassement du délai maximal
    def _deadline_reached(self) -> bool:
        """Return whether the graceful-shutdown deadline has been reached."""
        # Pas de délai configuré → jamais d'arrêt anticipé
        if self._max_runtime is None or self._t0 is None:
            return False
        return (_now() - self._t0) >= self._max_runtime

    # Méthode de chargement du registre des dates de dernier téléchargement
    def _load_registry(self) -> None:
        """Load the last-download registry from the JSON file (empty if absent)."""
        # Lecture du registre JSON existant (None si absent)
        data = self._read_json(self._last_download_path) or {}
        self._registry = data.get(_REGISTRY_ROOT, {})
        # Logging
        logger.info(
            f"Loaded {len(self._registry)} download records from "
            f"{self._last_download_path}"
        )

    # Méthode de sauvegarde du registre des dates
    def _save_registry(self) -> None:
        """Persist the last-download registry through the configured storage."""
        self._write_json(
            self._last_download_path,
            {_REGISTRY_ROOT: self._registry},
        )

    # ──────────────────────────────────────────────────────────────────
    # Accès au stockage des registres JSON (local ou S3)
    # ──────────────────────────────────────────────────────────────────

    # Méthode de lecture d'un registre JSON (local ou S3)
    def _read_json(self, path: Path) -> Optional[Dict[str, Any]]:
        """Read a JSON registry from local storage or S3.

        Routing is driven by ``self._bucket``: when set, ``path`` is used as the
        S3 object key (POSIX form); otherwise it is a local filesystem path. A
        missing file/object is treated as "no registry yet" and returns
        ``None`` rather than raising.

        Args:
            path: Registry path (local path or S3 key).

        Returns:
            The deserialised JSON mapping, or ``None`` when the registry does
            not exist yet.
        """
        # Cas S3 : l'absence d'objet se détecte via l'exception du client
        if self._bucket is not None:
            try:
                return self._loader.load(path.as_posix(), bucket=self._bucket)
            except ClientError:
                # Objet inexistant (NoSuchKey/404) → registre vide
                return None
        # Cas local : court-circuit si le fichier n'existe pas encore
        if not path.exists():
            return None
        return self._loader.load(str(path))

    # Méthode d'écriture d'un registre JSON (local ou S3)
    def _write_json(self, path: Path, obj: Any) -> None:
        """Write a JSON registry to local storage or S3.

        On S3 the object PUT is atomic, so the payload is written directly. On
        the local filesystem the write is made atomic by writing to a temporary
        file in the destination directory and renaming it over the target, so a
        crash never leaves a half-written registry.

        Args:
            path: Registry path (local path or S3 key).
            obj: JSON-serialisable object to persist.
        """
        # Cas S3 : écriture directe (PUT d'objet atomique)
        if self._bucket is not None:
            self._saver.save(
                path.as_posix(),
                obj,
                bucket=self._bucket,
                indent=2,
                ensure_ascii=False,
            )
            return

        # Cas local : écriture atomique via fichier temporaire + remplacement
        # Création du dossier parent si nécessaire
        path.parent.mkdir(parents=True, exist_ok=True)
        # L'extension .json est nécessaire pour que Saver reconnaisse le format.
        fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".json")
        # Fermeture immédiate du descripteur : Saver ouvre le fichier lui-même
        os.close(fd)
        try:
            self._saver.save(
                tmp_name,
                obj,
                indent=2,
                ensure_ascii=False,
            )
            os.replace(tmp_name, str(path))
        except Exception:
            # Nettoyage du fichier temporaire en cas d'échec
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
            raise


# ──────────────────────────────────────────────────────────────────────
# Fonction de convenance
# ──────────────────────────────────────────────────────────────────────

# Fonction utilitaire enveloppant l'orchestrateur
def download_updates(
    client: AbstractSDMXClient,
    queries: Union[Any, Iterable[Any]],
    connector: DuckLakeConnector,
    structures_path: Union[str, Path],
    last_download_path: Union[str, Path],
    *,
    n_observations: int = 10,
    fresh_registry: bool = False,
    max_runtime: Optional[timedelta] = timedelta(hours=23),
    categorical_threshold: Optional[int] = None,
    bucket: Optional[str] = None,
    storage_options: Optional[Dict[str, Any]] = None,
    on_query_complete: Optional[Callable[[QueryReport], None]] = None,
) -> DownloadReport:
    """Run an incremental SDMX → DuckLake download.

    Thin wrapper around :class:`SDMXDownloader` for one-call usage (e.g. from a
    Kedro node). See :class:`SDMXDownloader` for the argument semantics.

    Returns:
        A :class:`DownloadReport` summarising the run.
    """
    # Construction de l'orchestrateur et exécution
    downloader = SDMXDownloader(
        client,
        connector,
        structures_path,
        last_download_path,
        n_observations=n_observations,
        fresh_registry=fresh_registry,
        max_runtime=max_runtime,
        categorical_threshold=categorical_threshold,
        bucket=bucket,
        storage_options=storage_options,
        on_query_complete=on_query_complete,
    )
    return downloader.run(queries)

