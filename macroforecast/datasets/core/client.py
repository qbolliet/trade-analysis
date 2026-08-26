"""Generic API client and abstract SDMX client base.

This module provides:
- ``APIClient``: generic HTTP client with retry logic.
- ``AbstractSDMXClient``: abstract base class mutualising logic shared by all
  SDMX provider clients (structure registry, duplicate checking, split-request
  execution, CSV parsing, context manager, etc.).
"""
# Importation des modules
from abc import ABC, abstractmethod
from datetime import datetime
from functools import reduce
from io import StringIO
import itertools
import json
import logging
import operator
from pathlib import Path
import time
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin
import warnings

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import runtime du rate limiter (rate_limiter.py n'a pas de dépendance interne
# au package, aucun risque de circularité)
from .reports import FetchReport, HttpStats
from .rate_limiter import CompositeRateLimiter, RateLimiter, build_rate_limiter

# Imports internes — éviter les imports circulaires en utilisant TYPE_CHECKING
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .sdmx import DuplicateHandling
    from .structures import DataflowStructure, DataflowStructureRegistry

# Initialisation du logger
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Client HTTP générique
# ──────────────────────────────────────────────────────────────────────

# Classe permettant d'effectuer des requêtes API avec 'requests'
class APIClient:
    """Generic HTTP client with retry logic and error handling.

    This class handles HTTP requests with automatic retry on failures,
    connection pooling, and timeout management.

    Args:
        base_url: Base URL for API requests.
        timeout: Request timeout in seconds (default: 30).
        max_retries: Maximum number of retry attempts (default: 3).
        backoff_factor: Backoff factor for retries (default: 0.5).
        headers: Additional headers to include in requests.

    Example:
        >>> client = APIClient("https://api.example.com")
        >>> response = client.get("/data", params={"key": "value"})
    """

    # Initialisation
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        # Initialisation des attributs
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = self._create_session(max_retries, backoff_factor)
        self.default_headers = {}
        if headers:
            self.default_headers.update(headers)

        # Compteurs HTTP : statuts, durées et volumes
        self.stats_ = HttpStats()

    # Méthode auxiliaire de création de la session 'request'
    def _create_session(self, max_retries: int, backoff_factor: float) -> requests.Session:
        """Create a session with retry configuration.

        Args:
            max_retries: Maximum number of retry attempts.
            backoff_factor: Backoff factor between retries.

        Returns:
            Configured requests Session object.
        """
        # Initialisation d'une session requests
        session = requests.Session()
        # Initialisation d'une stratégie de retry
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        # Ajout de la stratégie à la session
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    # Méthode de requête "GET"
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        """Make a GET request.

        Args:
            endpoint: API endpoint (relative to base_url).
            params: Query parameters.
            headers: Additional headers for this request.
            timeout: Request timeout in seconds. Overrides the instance-level
                timeout when provided.

        Returns:
            Response object.

        Raises:
            requests.exceptions.RequestException: On request failure.
        """
        # Création de l'URL de requête
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        # Création des headers
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)

        # Résolution du timeout : surcharge par requête ou valeur par défaut
        effective_timeout = timeout if timeout is not None else self.timeout

        # Logging
        logger.debug(f"GET request to {url} with params: {params}")
        # Instant de départ : la durée alimente les compteurs, quelle que soit l'issue
        started = time.monotonic()
        try:
            # Excution de la requête
            response = self.session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=effective_timeout,
            )
            # Statut de la requête
            response.raise_for_status()
            # Comptabilisation de la réponse (statut, durée, volume)
            self.stats_.record(
                response.status_code,
                time.monotonic() - started,
                len(response.content or b""),
            )
            return response
        except requests.exceptions.HTTPError as e:
            # Comptabilisation de l'échec, statut compris
            self.stats_.record_failure(
                e.response.status_code if e.response is not None else None,
                time.monotonic() - started,
            )
            # Logging
            logger.error(f"HTTP error: {e}")
            logger.error(f"Response content: {e.response.text[:500]}")
            raise
        except requests.exceptions.RequestException as e:
            # Comptabilisation de l'échec sans réponse (erreur de connexion)
            self.stats_.record_failure(None, time.monotonic() - started)
            # Logging
            logger.error(f"Request failed: {e}")
            raise

    # Méthode de fermeture de la session
    def close(self) -> None:
        """Close the session and clean up resources."""
        self.session.close()

    # Constructeur d'entrée comme context manager
    def __enter__(self) -> "APIClient":
        """Context manager entry."""
        return self

    # Constructeur de sortie comme contexte manager
    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Context manager exit."""
        self.close()


# ──────────────────────────────────────────────────────────────────────
# Client SDMX abstrait — logique mutualisée entre tous les providers
# ──────────────────────────────────────────────────────────────────────

class AbstractSDMXClient(ABC):
    """Abstract base class for SDMX provider clients.

    Provides the shared logic common to all SDMX provider clients :

    - Structure registry management (``register_structure``, ``_ensure_structure``)
    - Rate-limiter loading from ``parameters/{PROVIDER_CONFIG_NAME}.json``
      (``_load_rate_limiter``)
    - Query-object execution (``execute_query``)
    - Cartesian product of split dimensions (``_cartesian_split``)
    - Data-retrieval pipeline orchestration (``_execute_query_pipeline``)
    - Split-request execution loop (``_execute_split_requests``)
    - Post-request DataFrame filtering (``_filter_dataframe_by_dimensions``)
    - Duplicate detection (``_check_duplicates``)
    - CSV response parsing (``_parse_csv_response``)
    - Context manager protocol

    Subclasses must implement:
    - ``get_data``: provider-specific data retrieval entry point (typically a
      thin wrapper packing its arguments and delegating to
      ``_execute_query_pipeline``).
    - ``close``: release provider-specific resources.
    - ``_fetch_structure``: fetch a ``DataflowStructure`` from the provider API.
    - ``_execute_single_request``: execute one API request for a given
      dimension combination and return a parsed DataFrame.
    - ``_resolve_structure``: resolve/cache the structure for a query.
    - ``_prepare_requests``: build the split-request combinations and the
      execution keyword arguments.

    Subclasses must set :attr:`PROVIDER_CONFIG_NAME` to enable automatic
    rate-limiter loading, and may override ``_postprocess_dataframe`` to apply
    a provider-specific post-filter.

    Args:
        structure_registry: Pre-populated registry of dataflow structures.
            A new empty registry is created if ``None``.
        auto_fetch_structure: If ``True``, automatically query the provider
            API to retrieve dimension metadata when a dataflow structure is
            not yet in the registry.
        rate_limiter: Rate limiter instance. If ``None`` and
            ``auto_load_rate_limit`` is ``True``, the provider's JSON
            configuration file is read via ``_load_rate_limiter``.
        auto_load_rate_limit: Whether to attempt loading the rate limiter
            automatically from the provider config file.

    Example:
        >>> class MyClient(AbstractSDMXClient):
        ...     PROVIDER_CONFIG_NAME = "myprovider"
        ...     def get_data(self, ...): ...
        ...     def close(self): ...
        ...     def _fetch_structure(self, agency, dataflow, **kwargs): ...
        ...     def _execute_single_request(self, dims, **kwargs): ...
        ...     def _resolve_structure(self, params): ...
        ...     def _prepare_requests(self, structure, params): ...
    """

    # Nom du fichier de configuration du provider (sans extension) utilisé pour
    # le chargement automatique du rate limiter depuis parameters/{nom}.json.
    # Laissé à None dans la base ; surchargé par chaque client concret.
    PROVIDER_CONFIG_NAME: ClassVar[Optional[str]] = None

    # Initialisation
    def __init__(
        self,
        structure_registry: Optional["DataflowStructureRegistry"] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional["RateLimiter"] = None,
        auto_load_rate_limit: bool = True,
    ) -> None:
        # Import local pour éviter la circularité au niveau module
        from .structures import DataflowStructureRegistry as _Registry

        # Initialisation des attributs
        # Registre des structures de dataflows
        self.structure_registry: "DataflowStructureRegistry" = (
            structure_registry if structure_registry is not None else _Registry()
        )
        self.auto_fetch_structure = auto_fetch_structure

        # Chargement automatique du rate limiter si demandé
        if auto_load_rate_limit and rate_limiter is None:
            rate_limiter = self._load_rate_limiter()
        self.rate_limiter: Optional[Union[RateLimiter, CompositeRateLimiter]] = (
            rate_limiter
        )

        # Diagnostics de la dernière récupération (convention sklearn du dépôt :
        # attribut suffixé d'un underscore, renseigné à l'exécution)
        self.last_fetch_report_: Optional[FetchReport] = None
        # Nombre de structures effectivement téléchargées (défauts de cache)
        self.n_structures_fetched_: int = 0

    # Méthodes abstraites
    # Méthode abstraite de requête des données
    @abstractmethod
    def get_data(self, *args, **kwargs) -> pd.DataFrame:
        """Retrieve data from the provider API.

        Provider-specific signature. Implementations should handle dimension
        normalisation, structure auto-fetch, rate limiting, split requests,
        and duplicate checking.

        Returns:
            DataFrame with the retrieved data.
        """

    # Méthode abstraite de fermeture de la connexion
    @abstractmethod
    def close(self) -> None:
        """Release provider-specific resources (HTTP sessions, etc.)."""

    # Méthode de chargement du rate-limiter depuis le fichier de configuration
    def _load_rate_limiter(
        self,
    ) -> Optional[Union[RateLimiter, CompositeRateLimiter]]:
        """Load the rate limiter from ``parameters/{PROVIDER_CONFIG_NAME}.json``.

        Reads the ``RATE_LIMIT`` section of the provider configuration file and
        builds a limiter via :func:`build_rate_limiter`. The section may be a
        single ``{requests, unit, count}`` dict (→ :class:`RateLimiter`) or a
        list of such dicts (→ :class:`CompositeRateLimiter`). Subclasses only
        need to set :attr:`PROVIDER_CONFIG_NAME`; the lookup is skipped (and
        ``None`` returned) when it is left unset.

        Returns:
            ``RateLimiter`` / ``CompositeRateLimiter`` instance, or ``None`` if
            no configuration is found or loading fails.
        """
        # Aucun fichier de configuration déclaré → pas de rate limiting
        if not self.PROVIDER_CONFIG_NAME:
            return None
        try:
            # Construction du chemin vers parameters/{provider}.json (racine du repo)
            params_path = (
                Path(__file__).parents[3]
                / "parameters"
                / f"{self.PROVIDER_CONFIG_NAME}.json"
            )
            # Lecture et parsing du fichier de configuration si présent
            if params_path.exists():
                with open(params_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                # Extraction de la configuration du rate limiter
                if "RATE_LIMIT" in config:
                    # Logging
                    logger.info(
                        f"Loading rate limiter from "
                        f"parameters/{self.PROVIDER_CONFIG_NAME}.json"
                    )
                    return build_rate_limiter(config["RATE_LIMIT"])
            # Logging si aucune configuration de rate limit trouvée
            logger.debug("No RATE_LIMIT configuration found")
            return None
        # Échec non bloquant de chargement
        except Exception as e:
            # Logging
            logger.warning(f"Could not load rate limiter: {e}")
            return None

    # Méthode abstraire de requête de la structure d'un dataflow
    @abstractmethod
    def _fetch_structure(
        self,
        agency: str,
        dataflow: str,
        **kwargs,
    ) -> "DataflowStructure":
        """Fetch a dataflow structure from the provider API (no cache).

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            **kwargs: Provider-specific keyword arguments (e.g. ``version``
                for Eurostat).

        Returns:
            Parsed ``DataflowStructure``.

        Raises:
            Exception: On HTTP or parsing errors.
        """

    # Méthode abstraite d'exécution d'une requête
    @abstractmethod
    def _execute_single_request(
        self,
        dims_for_request: Dict,
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute one API request for a given dimension combination.

        Called by ``_execute_split_requests`` for each entry in the
        Cartesian product of split dimensions.

        Args:
            dims_for_request: Dimension values for this specific request
                (format is provider-specific: ``Dict[str, List[str]]`` for
                Eurostat, ``Dict[int, List[str]]`` for OECD).
            **request_kwargs: Additional provider-specific parameters
                forwarded from ``_execute_split_requests``.

        Returns:
            Parsed DataFrame (before post-filtering).
        """

    # Méthode abstraite de résolution de la structure d'une requête
    @abstractmethod
    def _resolve_structure(
        self, params: Dict[str, Any]
    ) -> Optional["DataflowStructure"]:
        """Resolve (and cache) the dataflow structure for a query.

        Called first by :meth:`_execute_query_pipeline`. Implementations read
        the relevant identifiers from ``params`` (e.g. ``dataflow``,
        ``agency``, ``version``) and return the structure, fetching it on
        demand when necessary.

        Args:
            params: Parameter dict assembled by the provider ``get_data``
                method (keys match its signature).

        Returns:
            ``DataflowStructure`` if available, ``None`` otherwise.
        """

    # Méthode abstraite de préparation des requêtes splitées
    @abstractmethod
    def _prepare_requests(
        self,
        structure: Optional["DataflowStructure"],
        params: Dict[str, Any],
    ) -> Tuple[List[Tuple[Dict, Dict]], Dict, Dict[str, Any]]:
        """Build the split-request combinations and execution arguments.

        Called by :meth:`_execute_query_pipeline` after structure resolution.
        Implementations normalise dimensions, generate the request
        combinations and assemble the keyword arguments forwarded to every
        ``_execute_single_request`` call.

        Args:
            structure: Resolved dataflow structure (may be ``None``).
            params: Parameter dict assembled by the provider ``get_data``
                method.

        Returns:
            Tuple ``(request_combinations, normalized_dimensions,
            execute_kwargs)``:

            - ``request_combinations``: list of ``(dims_for_request,
              dims_for_postfilter)`` tuples consumed by
              ``_execute_split_requests``.
            - ``normalized_dimensions``: dimensions used for duplicate
              checking.
            - ``execute_kwargs``: keyword arguments forwarded to every
              ``_execute_single_request`` call.
        """

    # Méthode abstraite de récupération incrémentale d'une requête
    @abstractmethod
    def fetch_updates(
        self,
        query: Any,
        since: Optional[datetime],
        n_observations: int = 10,
    ) -> pd.DataFrame:
        """Fetch the data for a query, incrementally when possible.

        Provider seam used by the download orchestrator
        (:class:`~macroforecast.datasets.core.download.SDMXDownloader`). This
        is where each provider encodes *how* to retrieve only the newly
        published observations:

        - First download (``since`` is ``None``) → retrieve the **full**
          series.
        - Subsequent download (``since`` set) → retrieve only what changed
          since that instant. OECD relies on the ``updated_after`` query
          parameter; Eurostat issues a ``dataconstraint`` structure request
          first and, when the data was updated after ``since``, pulls the last
          ``n_observations`` observations (to avoid leaving gaps).

        Args:
            query: Provider-specific query request exposing ``to_dict()``
                (e.g. ``OECDQueryRequest``, ``EurostatQueryRequest``).
            since: Instant of the previous successful download for this query,
                or ``None`` if it was never downloaded.
            n_observations: Number of most-recent observations to retrieve in
                incremental mode (providers without a per-observation update
                filter, e.g. Eurostat).

        Returns:
            DataFrame with the retrieved data. May be empty when nothing was
            published since ``since``.
        """

    # ──────────────────────────────────────────────────────────────────
    # Pipeline de récupération mutualisé
    # ──────────────────────────────────────────────────────────────────

    # Méthode d'exécution d'un objet requête provider
    def execute_query(self, query: Any) -> pd.DataFrame:
        """Execute a provider query object.

        Generic helper relying on the provider query dataclass exposing a
        ``to_dict()`` method whose keys match the provider ``get_data``
        parameters.

        Args:
            query: Provider-specific query request exposing ``to_dict()``
                (e.g. ``OECDQueryRequest``, ``EurostatQueryRequest``).

        Returns:
            DataFrame with the retrieved data.
        """
        # Délégation à get_data avec les paramètres de la requête
        return self.get_data(**query.to_dict())

    # Méthode de résolution de la structure associée à un objet requête
    def resolve_query_structure(
        self, query: Any
    ) -> Optional["DataflowStructure"]:
        """Resolve the dataflow structure backing a provider query object.

        Thin adapter delegating to the provider :meth:`_resolve_structure`
        hook with the query parameters. Used by the download orchestrator to
        derive the primary keys (the dataflow dimensions) of the target
        DuckLake table. Resolving the structure also registers it in
        :attr:`structure_registry` (side effect of ``_resolve_structure``),
        so the orchestrator can detect newly fetched structures.

        Args:
            query: Provider-specific query request exposing ``to_dict()``.

        Returns:
            Resolved ``DataflowStructure`` or ``None`` when unavailable.
        """
        # Délégation au hook provider avec les paramètres de la requête
        return self._resolve_structure(query.to_dict())

    # Méthode patron orchestrant la récupération des données
    def _execute_query_pipeline(self, params: Dict[str, Any]) -> pd.DataFrame:
        """Run the shared data-retrieval pipeline (template method).

        Orchestrates the steps common to every provider: structure
        resolution, request preparation, split-request execution, duplicate
        checking and optional post-processing. Providers customise the
        behaviour through :meth:`_resolve_structure`, :meth:`_prepare_requests`
        and :meth:`_postprocess_dataframe`.

        Every step feeds :attr:`last_fetch_report_`, so that a caller — the
        download orchestrator, or a consuming project — can read what the run
        did instead of parsing its logs.

        Args:
            params: Parameter dict assembled by the provider ``get_data``
                method (keys match its signature).

        Returns:
            Retrieved DataFrame.
        """
        # Rapport de la récupération, renseigné étape par étape
        report = FetchReport()
        self.last_fetch_report_ = report

        # Structures déjà téléchargées avant résolution : un compteur inchangé
        # signe un accès au cache
        n_fetched_before = self.n_structures_fetched_
        # Résolution de la structure du dataflow (spécifique au provider)
        structure = self._resolve_structure(params)
        if structure is not None:
            report.structure_from_cache = (
                self.n_structures_fetched_ == n_fetched_before
            )

        # Préparation des requêtes : combinaisons, dims normalisées, kwargs d'exécution
        request_combinations, normalized_dims, execute_kwargs = self._prepare_requests(
            structure, params
        )

        # Exécution mutualisée des sous-requêtes (rate limiting, concaténation)
        df = self._execute_split_requests(
            request_combinations, report=report, **execute_kwargs
        )

        # Vérification des doublons sauf si explicitement désactivée
        on_duplicate = params.get("on_duplicate", "warn")
        default_dimensions = params.get("default_dimensions", [])
        if on_duplicate != "ignore":
            report.n_duplicates = self._check_duplicates(
                df, normalized_dims, structure, on_duplicate, default_dimensions
            )

        # Post-traitement optionnel (post-filtre de repli côté provider)
        df = self._postprocess_dataframe(df, structure, normalized_dims, params)
        # Volumétrie finale, après l'éventuel post-filtre du provider
        report.rows_after_filter = len(df)
        return df

    # Hook de post-traitement du DataFrame (no-op par défaut)
    def _postprocess_dataframe(
        self,
        df: pd.DataFrame,
        structure: Optional["DataflowStructure"],
        normalized_dims: Dict,
        params: Dict[str, Any],
    ) -> pd.DataFrame:
        """Post-process the retrieved DataFrame (hook, no-op by default).

        Overridden by providers needing a client-side fallback filter (e.g.
        Eurostat when no structure is available).

        Args:
            df: DataFrame returned by the split requests.
            structure: Resolved dataflow structure (may be ``None``).
            normalized_dims: Normalised dimensions from
                :meth:`_prepare_requests`.
            params: Parameter dict from the provider ``get_data`` method.

        Returns:
            Possibly filtered DataFrame. The default implementation returns
            ``df`` unchanged.
        """
        return df

    # Méthode statique de génération du produit cartésien des dimensions à splitter
    @staticmethod
    def _cartesian_split(
        split_values: Dict[Any, List[str]],
        max_combinations: int,
    ) -> List[Dict[Any, str]]:
        """Build the cartesian product of split-dimension values.

        Args:
            split_values: Mapping of dimension key (name or position) to the
                list of values to split into separate requests.
            max_combinations: Maximum number of combinations allowed.

        Returns:
            List of ``{key: single_value}`` dicts — one per combination.
            Returns ``[{}]`` (a single empty combination) when
            ``split_values`` is empty.

        Raises:
            ValueError: If the cartesian product exceeds ``max_combinations``.
        """
        # Aucune dimension à splitter → une seule combinaison vide
        keys = list(split_values.keys())
        if not keys:
            return [{}]

        # Listes de valeurs dans l'ordre des clés
        value_lists = [split_values[k] for k in keys]

        # Contrôle du nombre de combinaisons avant génération
        num_combinations = reduce(operator.mul, (len(v) for v in value_lists), 1)
        if num_combinations > max_combinations:
            raise ValueError(
                f"Cartesian product would generate {num_combinations} requests, "
                f"exceeding max_split_combinations={max_combinations}. "
                f"Consider splitting fewer dimensions or filtering values."
            )

        # Construction des combinaisons {clé: valeur unique}
        return [dict(zip(keys, combo)) for combo in itertools.product(*value_lists)]

    # Structure registry
    # Méthode d'enregistrement d'une structure dans le registre
    def register_structure(self, structure: "DataflowStructure") -> None:
        """Register a dataflow structure for dimension name resolution.

        Args:
            structure: ``DataflowStructure`` to store in the registry.
        """
        # Enregistrement de la structure dans le registre
        self.structure_registry.register(structure)
        # Comptage des structures téléchargées : un accès au cache ne passe
        # jamais par ici, ce qui en fait le point de mesure des défauts de cache
        self.n_structures_fetched_ += 1
        # Logging
        logger.info(f"Registered structure for {structure.dataflow}")

    # Méthode d'extraction d'une structure d'un registre si elle existe et de téléchargement sinon
    def _ensure_structure(
        self,
        agency: str,
        dataflow: str,
        **kwargs,
    ) -> Optional["DataflowStructure"]:
        """Return the structure for a dataflow, fetching it if necessary.

        Checks the registry first. If not found and ``auto_fetch_structure``
        is ``True``, calls ``_fetch_structure`` and caches the result.

        Args:
            agency: Agency identifier used as the registry key.
            dataflow: Dataflow identifier used as the registry key.
            **kwargs: Forwarded to ``_fetch_structure`` (e.g. ``version``).

        Returns:
            ``DataflowStructure`` if available, ``None`` otherwise.
        """
        # Vérification du cache
        if self.structure_registry.has(agency, dataflow):
            return self.structure_registry.get(agency, dataflow)

        # Récupération automatique si activée
        if self.auto_fetch_structure:
            try:
                # Logging
                logger.info(f"Fetching structure for {agency}::{dataflow}")
                # Requête de la structure
                structure = self._fetch_structure(agency, dataflow, **kwargs)
                # Enregistrement
                self.register_structure(structure)
                return structure
            except Exception as e:
                # Logging
                logger.warning(
                    f"Failed to fetch structure for {agency}::{dataflow}: {e}"
                )

        return None

    # Méthode auxiliaire de détection d'une réponse SDMX « aucun enregistrement »
    @staticmethod
    def _is_no_records_error(exc: Exception) -> bool:
        """Return True if exc is an SDMX '404 NoRecordsFound' response.

        SDMX providers answer a valid query that matches no data with an HTTP
        404 whose body contains ``NoRecordsFound``. This is an empty result,
        not a genuine failure, and should be treated as such.

        Args:
            exc: Exception raised while executing a sub-request.

        Returns:
            ``True`` if ``exc`` is an HTTP 404 ``NoRecordsFound`` error,
            ``False`` otherwise.
        """
        # Filtre sur les seules erreurs HTTP porteuses d'une réponse
        if not isinstance(exc, requests.exceptions.HTTPError):
            return False
        response = exc.response
        # Statut 404 et corps signalant explicitement l'absence de données
        return (
            response is not None
            and response.status_code == 404
            and "NoRecordsFound" in (response.text or "")
        )

    #  Méthode auxiliaire d'exécution de requêtes multiples
    def _execute_split_requests(
        self,
        request_combinations: List[Tuple[Dict, Dict]],
        *,
        report: Optional[FetchReport] = None,
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute multiple API requests and concatenate the results.

        Iterates over the Cartesian product of split dimensions. For each
        combination, acquires the rate limiter, delegates the HTTP call to
        ``_execute_single_request``, applies post-request dimension filtering,
        and collects the resulting DataFrames.

        Counters are written into ``report`` as the loop goes — the same
        in-place style as ``SDMXDownloader._process_query`` — so that the
        sub-request outcomes survive the call instead of being only logged.

        Args:
            report: Fetch report to fill in place. A fresh one is used when
                ``None``, keeping the method usable on its own.
            request_combinations: List of ``(dims_for_request,
                dims_for_postfilter)`` tuples produced by
                ``_generate_request_combinations``.
                - ``dims_for_request``: passed verbatim to
                  ``_execute_single_request``.
                - ``dims_for_postfilter``: ``Dict[str, List[str]]``
                  (dimension name → allowed values) applied after retrieval.
            **request_kwargs: Additional keyword arguments forwarded to
                ``_execute_single_request`` for every sub-request.

        Returns:
            Concatenated DataFrame from all successful sub-requests.

        Raises:
            ValueError: If every sub-request failed or returned an empty
                DataFrame.
        """
        # Initialisation de la liste des jeux de données requêtés
        all_dataframes: List[pd.DataFrame] = []
        # Initialisation de la liste des erreurs
        errors: List[str] = []
        # Calcul du nombre de combinaisons
        n = len(request_combinations)

        # Rapport de récupération : rempli en place tout au long de la boucle
        report = report if report is not None else FetchReport()
        report.n_requests = n

        # Logging
        logger.info(f"Executing {n} split API requests")

        # Parcours des requêtes
        for i, (dims_for_request, dims_for_postfilter) in enumerate(request_combinations):
            # Application du rate limiter avant chaque sous-requête
            if self.rate_limiter:
                self.rate_limiter.acquire()

            # Logging de progression tous les 10 requêtes
            if (i + 1) % 10 == 0 or i == 0 or i == n - 1:
                logger.info(f"Processing request {i + 1}/{n}")

            try:
                # Exécution de la requête
                df = self._execute_single_request(dims_for_request, **request_kwargs)

                # Volumétrie brute, avant tout post-filtrage
                report.rows_fetched += len(df)

                # Post-filtrage par dimensions si nécessaire
                if not df.empty and dims_for_postfilter:
                    df = self._filter_dataframe_by_dimensions(df, dims_for_postfilter)

                # Ajout du DataFrame à la liste des jeux de données requêtés si non vide
                if not df.empty:
                    all_dataframes.append(df)
                else:
                    # Sous-requête sans donnée exploitable après filtrage
                    report.n_empty_responses += 1
                    # Logging
                    logger.debug(f"Request {i + 1} returned empty after filtering")

            except Exception as e:
                # 404 NoRecordsFound : requête valide sans donnée → résultat vide
                # (et non une erreur), on poursuit sans alimenter la liste d'erreurs
                if self._is_no_records_error(e):
                    # Requête valide sans donnée : comptée à part des erreurs
                    report.n_no_records += 1
                    # Logging
                    logger.info(f"Request {i + 1}/{n} returned no records (empty result)")
                    continue
                # Erreur véritable
                report.n_request_errors += 1
                # Construction du message d'erreur
                error_msg = f"Request {i + 1}/{n} failed: {e}"
                # Logging
                logger.error(error_msg)
                # Ajout de l'erreur à la liste
                errors.append(error_msg)

        # Aucun DataFrame collecté : distinction entre échec réel et résultat vide
        if not all_dataframes:
            # Vraies erreurs présentes → levée d'exception
            if errors:
                error_summary = "\n".join(errors)
                raise ValueError(
                    f"All {n} split requests failed or returned empty results.\n"
                    f"Errors:\n{error_summary}"
                )
            # Aucune erreur réelle : toutes les requêtes ont réussi mais ne renvoient
            # aucune donnée (no-records et/ou vide après post-filtrage) → DataFrame vide
            logger.info("All requests returned empty results; returning empty DataFrame")
            return pd.DataFrame()

        # Logging si les requêtes ont partiellement échoué
        if errors:
            # Logging
            logger.warning(
                f"{len(errors)} out of {n} requests failed. "
                f"Successfully retrieved {len(all_dataframes)} DataFrames."
            )

        # Logging
        logger.info(f"Concatenating {len(all_dataframes)} DataFrames")
        # Concaténation des jeux de données
        result = pd.concat(all_dataframes, ignore_index=True)
        # Logging
        logger.info(f"Split requests result: {len(result)} rows")

        return result

    # Méthode de filtrage post-requête
    @staticmethod
    def _filter_dataframe_by_dimensions(
        df: pd.DataFrame,
        dimension_filters: Dict[str, List[str]],
    ) -> pd.DataFrame:
        """Filter a DataFrame to retain only allowed dimension values.

        Applied after retrieval when wildcard dimensions would return more
        data than requested (e.g. OECD ``*`` wildcard, or multi-value
        non-split dimensions).

        Args:
            df: DataFrame to filter.
            dimension_filters: Mapping of dimension column name to allowed
                values (``{dim_name: [value, ...]})``).

        Returns:
            Filtered DataFrame (same columns, subset of rows).
        """
        # Vérification que le jeu de données est non vide et que des dimensions de filtre sont fournies
        if df.empty or not dimension_filters:
            return df

        # Masque cumulatif : toutes les conditions doivent être vraies
        mask = pd.Series([True] * len(df), index=df.index)

        # Parcours des filtres de dimension
        for dim_name, allowed_values in dimension_filters.items():
            # Vérification que la dimension de filtre est comprise dans le jeu de données
            if dim_name not in df.columns:
                # Logging
                logger.warning(
                    f"Dimension column '{dim_name}' not found in DataFrame. "
                    f"Available columns: {list(df.columns)}. Skipping filter."
                )
                continue
            # Mise à jour du masque
            mask &= df[dim_name].isin(allowed_values)

        # Filtre du jeu de données
        filtered_df = df[mask]

        # Logging des lignes filtrées
        if len(filtered_df) < len(df):
            logger.info(
                f"Filtered {len(df) - len(filtered_df)} rows by dimensions "
                f"{list(dimension_filters.keys())}"
            )

        return filtered_df

    # Méthode auxiliaire de détection des doublons
    @staticmethod
    def _check_duplicates(
        df: pd.DataFrame,
        dimensions: Union[Dict[int, Any], Dict[str, Any]],
        structure: Optional["DataflowStructure"],
        on_duplicate: "DuplicateHandling",
        default_dimensions: List[str] = [],
    ) -> int:
        """Detect and handle duplicate rows in the result DataFrame.

        Identifies the relevant key columns (filtered dimension columns plus
        ``TIME_PERIOD``) and checks for duplicate combinations. Duplicate
        rows usually indicate that wildcard dimensions returned unexpected
        extra dimension values that were not post-filtered.

        Args:
            df: DataFrame to check.
            dimensions: Dimension filter dict used in the query. Keys may be
                integer positions (OECD) or string names (Eurostat); the
                method handles both automatically.
            structure: Dataflow structure used to resolve int positions to
                column names. May be ``None`` if unavailable.
            on_duplicate: Strategy — ``"ignore"`` (no check), ``"warn"``
                (log a warning), or ``"raise"`` (raise ``ValueError``).
            default_dimensions: Default dimensions to check duplicates on.

        Returns:
            Number of duplicate rows found — the count was previously computed
            and thrown away, leaving it visible only in a warning.

        Raises:
            ValueError: If ``on_duplicate="raise"`` and duplicates are found.
        """
        # Vérification que le jeu de données est non vide
        if df.empty:
            return 0

        # Détermination des colonnes de vérification
        check_columns: List[str] = default_dimensions

        # Parcours des dimensions de filtre
        for key in dimensions.keys():
            if isinstance(key, int):
                # Clé positionnelle (OECD) → résolution en nom via structure
                if structure:
                    dim_name = structure.get_name(key)
                    if dim_name and dim_name in df.columns:
                        check_columns.append(dim_name)
            else:
                # Clé nominale (Eurostat)
                if key in df.columns:
                    check_columns.append(str(key))

        # Fallback : toutes les colonnes sauf la valeur observée
        if not check_columns:
            check_columns = [
                col for col in df.columns
                if col.lower() not in ("value", "obs_value", "obsvalue")
            ]

        # Détection des duplicats
        duplicates = df.duplicated(subset=check_columns, keep=False)
        # Comptage des duplicats
        num_duplicates = int(duplicates.sum())

        # Affichage des duplicats
        if num_duplicates > 0:
            # Extraction des 10 premières lignes
            dup_df = df[duplicates].sort_values(check_columns).head(10)
            # Construction du message
            message = (
                f"Found {num_duplicates} duplicate rows for columns "
                f"{check_columns}. This may indicate that undesired values "
                f"are included via wildcards (*). Examples:\n{dup_df.to_string()}"
            )
            # Affichage d'un message d'erreur
            if on_duplicate == "raise":
                raise ValueError(message)
            else:
                # Warning
                warnings.warn(message, UserWarning)
                # Logging
                logger.warning(message)

        return num_duplicates

    # Méthode de parsing du CSV de réponse
    @staticmethod
    def _parse_csv_response(text: str) -> pd.DataFrame:
        """Parse a CSV-formatted API response into a DataFrame.

        Args:
            text: Raw CSV text from the API response.

        Returns:
            DataFrame with the parsed data.

        Raises:
            ValueError: If the CSV cannot be parsed.
        """
        try:
            # Lecture du CSV
            df = pd.read_csv(StringIO(text))
            # Logging
            logger.info(f"Parsed {len(df)} rows from CSV")
            return df
        except Exception as e:
            # Logging
            logger.error(f"Failed to parse CSV response: {e}")
            # Erreur
            raise ValueError(f"Failed to parse CSV response: {e}") from e

    # Constructeur d'entrée dans le context manager
    def __enter__(self) -> "AbstractSDMXClient":
        """Context manager entry."""
        return self

    # Constructeur de sortie du contexte manager
    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Context manager exit."""
        self.close()
