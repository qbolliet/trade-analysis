"""Eurostat data client.

High-level client for querying Eurostat data through their SDMX API and
converting responses to pandas DataFrames. Both SDMX 3.0 (primary) and
SDMX 2.1 API versions are supported.
"""
# Importation des modules
from dataclasses import replace
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

import pandas as pd

# Utilitaires internes au package pour la requête de données au format SDMX
from ...core.client import AbstractSDMXClient, APIClient
from ...core.rate_limiter import RateLimiter
from ...core.sdmx import (
    DuplicateHandling,
    SDMXEndpointBuilder,
    SDMXVersion,
    StructureResourceType,
)
from ...core.structures import (
    DataflowStructure,
    DataflowStructureRegistry,
)
from . import parsing
from .endpoints import _ENDPOINT_BUILDERS
from .formats import (
    AGENCY_ID,
    DataDetail,
    EurostatResponseFormat,
    SUPPORTED_API_VERSIONS,
    StructureCompress,
    StructureDetail,
    StructureReferences,
)

if TYPE_CHECKING:
    from .queries import EurostatQueryRequest

# Initialisation du logger
logger = logging.getLogger(__name__)


# Fonction auxiliaire de normalisation d'un datetime en UTC
def _to_utc(value: datetime) -> datetime:
    """Return a UTC-aware copy of a datetime (naive values assumed UTC).

    Ensures the last-update / last-download comparison in
    :meth:`EurostatClient.fetch_updates` never raises on mixed
    aware/naive datetimes.

    Args:
        value: Datetime to normalise.

    Returns:
        UTC-aware ``datetime``.
    """
    # Datetime naïf → interprété comme UTC ; sinon conversion vers UTC
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# Initialisation du client haut niveau pour l'API SDMX Eurostat
class EurostatClient(AbstractSDMXClient):
    """High-level client for the Eurostat SDMX API.

    Supports SDMX 3.0 (default) and SDMX 2.1 API versions. The API version
    can be switched at construction time, and the client automatically routes
    Comext datasets (``DS-*`` prefix) to the dedicated Comext endpoint.

    Args:
        api_version: SDMX API version to use (default: 3.0).
        base_url: Override for the main API base URL. When *None* the
            standard Eurostat dissemination endpoint matching ``api_version``
            is used.
        timeout: Request timeout in seconds.
        structure_registry: Optional registry for dimension-name resolution.
        auto_fetch_structure: If *True*, fetch structure metadata on demand.
        rate_limiter: Optional rate limiter for API requests.
        auto_load_rate_limit: If *True*, load rate limiter from
            ``parameters/eurostat.json``.

    Example:
        >>> client = EurostatClient()
        >>> df = client.get_data(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR"], "FREQ": "Q"},
        ... )
    """

    # Nom du fichier de configuration pour le chargement du rate limiter
    PROVIDER_CONFIG_NAME = "eurostat"

    # URLs de base par défaut pour chaque version d'API
    _DEFAULT_BASE_URLS: Dict[SDMXVersion, str] = {
        SDMXVersion.V3: "https://ec.europa.eu/eurostat/api/dissemination",
        SDMXVersion.V2_1: "https://ec.europa.eu/eurostat/api/dissemination",
    }

    # URLs Comext par version d'API (pour les datasets DS-*)
    _COMEXT_BASE_URLS: Dict[SDMXVersion, str] = {
        SDMXVersion.V3: "https://ec.europa.eu/eurostat/api/comext/dissemination",
        SDMXVersion.V2_1: "https://ec.europa.eu/eurostat/api/comext/dissemination",
    }

    # Initialisation
    def __init__(
        self,
        api_version: SDMXVersion = SDMXVersion.V3,
        base_url: Optional[str] = None,
        timeout: int = 90,
        structure_registry: Optional[DataflowStructureRegistry] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional[RateLimiter] = None,
        auto_load_rate_limit: bool = True,
    ):
        # Validation du sous-ensemble de versions SDMX supportées par Eurostat
        if api_version not in SUPPORTED_API_VERSIONS:
            supported = ", ".join(v.name for v in SUPPORTED_API_VERSIONS)
            raise ValueError(
                f"Unsupported Eurostat API version {api_version!r}; "
                f"supported versions: {supported}"
            )

        # Initialisation de la base (structure_registry, auto_fetch_structure,
        # rate_limiter via _load_rate_limiter)
        super().__init__(
            structure_registry=structure_registry,
            auto_fetch_structure=auto_fetch_structure,
            rate_limiter=rate_limiter,
            auto_load_rate_limit=auto_load_rate_limit,
        )

        # Version d'API et builder d'endpoints associé
        self.api_version = api_version
        self.endpoint_builder: SDMXEndpointBuilder = _ENDPOINT_BUILDERS[api_version]

        # URL de base (résolution par défaut selon la version)
        self.base_url = base_url or self._DEFAULT_BASE_URLS[api_version]

        # Client HTTP principal
        self.api_client = APIClient(base_url=self.base_url, timeout=timeout)
        self._timeout = timeout

        # Client HTTP Comext (initialisation paresseuse)
        self._comext_client: Optional[APIClient] = None

    # ──────────────────────────────────────────────────────────────────
    # Méthodes publiques — Données
    # ──────────────────────────────────────────────────────────────────

    # Méthode publique principale de récupération de données
    def get_data(
        self,
        dataflow: str,
        version: str = "*",
        dimensions: Optional[Dict[str, Union[str, List[str]]]] = None,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        format: EurostatResponseFormat = EurostatResponseFormat.CSV,
        compress: bool = False,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        response_format_version: Optional[str] = None,
        dimension_at_observation: Optional[str] = None,
        detail: Optional[DataDetail] = None,
        on_duplicate: DuplicateHandling = "warn",
        split_dimensions: Optional[List[str]] = None,
        max_split_combinations: int = 100,
        default_dimensions: List[str] = ["TIME_PERIOD"],
    ) -> pd.DataFrame:
        """Retrieve data from Eurostat.

        The URL key and query parameters are built automatically from the
        dataflow structure (loaded from the registry or fetched on demand):

        - **SDMX 3.0**: dimensions with a single value are embedded in the
          positional URL key; dimensions with multiple values are passed as
          ``c[DIM]=val1,val2`` query parameters (server-side filtering,
          no client-side post-filter required).
        - **SDMX 2.1**: all dimensions are embedded in the positional URL
          key using ``val1+val2`` for multi-value positions.

        When no structure is available the method falls back to passing all
        dimensions as query parameters for SDMX 3.0 (existing behaviour) or
        using the ``"all"`` wildcard key for SDMX 2.1.

        Args:
            dataflow: Dataflow identifier (e.g., ``"namq_10_gdp"``).
            version: Dataflow version (``"*"`` for latest).
            dimensions: Dimension filters as ``{name: value_or_list}``.
            start_period: Start period in SDMX format (e.g., ``"2020-Q1"``).
            end_period: End period in SDMX format.
            last_n_observations: Number of most-recent observations.
            first_n_observations: Number of first observations.
            format: Response format (default: CSV).
            compress: Whether to request gzip compression.
            attributes: Attributes to include (SDMX 3.0 only).
            measures: Measures to include (SDMX 3.0 only).
            lang: Language code for label localisation (SDMX 3.0 only).
            labels: Label display mode (SDMX 3.0 only).
            response_format_version: Format version string (SDMX 3.0 only).
            dimension_at_observation: Dimension at observation level
                (SDMX 2.1 only).
            detail: Data detail level (SDMX 2.1 only).
            on_duplicate: Duplicate handling strategy.
            split_dimensions: Dimension names for which each value triggers
                a separate API request.  Use this to control the trade-off
                between response size and number of requests.
            max_split_combinations: Maximum split combinations allowed.
            default_dimensions: Default dimensions for duplicates checks

        Returns:
            DataFrame with retrieved data.

        Raises:
            ValueError: If data retrieval fails.

        Examples:
            >>> # SDMX 3.0 — GEO (single value) goes in URL key,
            >>> # unit (multi-value) goes in c[UNIT]=... query param
            >>> df = client.get_data(
            ...     dataflow="namq_10_gdp",
            ...     dimensions={"GEO": "FR", "unit": ["CLV10_MEUR", "CP_MEUR"]},
            ... )
            >>> # SDMX 3.0 — split GEO into separate requests
            >>> df = client.get_data(
            ...     dataflow="namq_10_gdp",
            ...     dimensions={"GEO": ["FR", "DE"]},
            ...     split_dimensions=["GEO"],
            ... )
        """
        # Empaquetage des paramètres et délégation au pipeline mutualisé
        # (résolution structure → préparation requêtes → exécution → doublons →
        # post-filtrage de repli via _postprocess_dataframe)
        params: Dict[str, Any] = {
            "dataflow": dataflow,
            "version": version,
            "dimensions": dimensions,
            "start_period": start_period,
            "end_period": end_period,
            "last_n_observations": last_n_observations,
            "first_n_observations": first_n_observations,
            "format": format,
            "compress": compress,
            "attributes": attributes,
            "measures": measures,
            "lang": lang,
            "labels": labels,
            "response_format_version": response_format_version,
            "dimension_at_observation": dimension_at_observation,
            "detail": detail,
            "on_duplicate": on_duplicate,
            "split_dimensions": split_dimensions,
            "max_split_combinations": max_split_combinations,
            "default_dimensions": default_dimensions
        }
        return self._execute_query_pipeline(params)

    # ──────────────────────────────────────────────────────────────────
    # Hooks du pipeline (AbstractSDMXClient._execute_query_pipeline)
    # ──────────────────────────────────────────────────────────────────

    # Implémentation du hook : résolution de la structure (non fatal en cas d'échec)
    def _resolve_structure(
        self, params: Dict[str, Any]
    ) -> Optional[DataflowStructure]:
        """Resolve the dataflow structure for a query.

        Tries the registry / auto-fetch path first, then falls back to a
        direct API fetch (populating the registry even when
        ``auto_fetch_structure`` is ``False``). Failure is non-fatal: the
        method returns ``None`` and the query proceeds without structure.

        Args:
            params: Parameter dict from :meth:`get_data`.

        Returns:
            Resolved ``DataflowStructure`` or ``None``.
        """
        dataflow: str = params["dataflow"]
        version: str = params.get("version", "*")

        # Chargement de la structure (non fatal en cas d'échec)
        structure = None
        try:
            structure = self._ensure_structure(dataflow, version)
        except Exception:
            # Fetch de secours : population du registry même si auto_fetch_structure=False
            try:
                structure = self.get_dataflow_structure(dataflow, version)
                self.register_structure(structure)
            except Exception as e2:
                logger.warning(f"Could not load structure for {dataflow}: {e2}")
        return structure

    # Implémentation du hook : préparation des requêtes splitées
    def _prepare_requests(
        self,
        structure: Optional[DataflowStructure],
        params: Dict[str, Any],
    ) -> Tuple[List[Tuple[Dict, Dict]], Dict, Dict[str, Any]]:
        """Normalise dimensions and build the request combinations.

        Args:
            structure: Resolved dataflow structure (may be ``None``).
            params: Parameter dict from :meth:`get_data`.

        Returns:
            Tuple ``(request_combinations, normalized_dims, execute_kwargs)``.
        """
        dataflow: str = params["dataflow"]

        # Normalisation des dimensions (conversion str → List[str], validation des noms)
        normalized_dims = self._normalize_dimensions(
            params.get("dimensions"), structure
        )

        # Booléen indiquant la version de l'API
        is_v21 = self.api_version == SDMXVersion.V2_1

        # Génération des combinaisons (dims_for_url, dims_for_params)
        raw_combinations = self._generate_request_combinations(
            dimensions=normalized_dims,
            split_dims=params.get("split_dimensions"),
            max_combinations=params.get("max_split_combinations", 100),
            is_v21=is_v21,
            structure=structure,
        )

        # Empaquetage au format attendu par AbstractSDMXClient :
        # tuple (dims_for_request, dims_for_postfilter). Eurostat n'utilise pas
        # le post-filtre par combinaison (côté serveur via c[DIM]=…), donc
        # postfilter est vide ; les deux sous-dicts sont passés ensemble à
        # _execute_single_request via la clé dims_for_request.
        request_combinations: List[Tuple[Dict, Dict]] = [
            (
                {"dims_for_url": dims_for_url, "dims_for_params": dims_for_params},
                {},
            )
            for dims_for_url, dims_for_params in raw_combinations
        ]

        # Logging
        logger.info(
            f"Fetching data from {dataflow} ({len(request_combinations)} request(s))"
        )

        # Arguments transmis à chaque _execute_single_request
        execute_kwargs: Dict[str, Any] = {
            "dataflow": dataflow,
            "version": params.get("version", "*"),
            "structure": structure,
            "response_format": params.get("format", EurostatResponseFormat.CSV),
            "compress": params.get("compress", False),
            "start_period": params.get("start_period"),
            "end_period": params.get("end_period"),
            "last_n_observations": params.get("last_n_observations"),
            "first_n_observations": params.get("first_n_observations"),
            "attributes": params.get("attributes"),
            "measures": params.get("measures"),
            "lang": params.get("lang"),
            "labels": params.get("labels"),
            "response_format_version": params.get("response_format_version"),
            "dimension_at_observation": params.get("dimension_at_observation"),
            "detail": params.get("detail"),
        }

        return request_combinations, normalized_dims, execute_kwargs

    # Implémentation du hook : post-filtrage de repli (sans structure)
    def _postprocess_dataframe(
        self,
        df: pd.DataFrame,
        structure: Optional[DataflowStructure],
        normalized_dims: Optional[Dict],
        params: Dict[str, Any],
    ) -> pd.DataFrame:
        """Apply the fallback client-side dimension filter.

        Without a structure all dimensions are sent as server-side query
        params, but the server may return broader data than requested; in
        that case only, the result is filtered client-side.

        Args:
            df: DataFrame returned by the split requests.
            structure: Resolved dataflow structure (may be ``None``).
            normalized_dims: Normalised dimensions (may be ``None``).
            params: Parameter dict from :meth:`get_data`.

        Returns:
            Possibly filtered DataFrame.
        """
        # Post-filtrage en fallback uniquement : sans structure toutes les dims
        # partent en query params server-side mais le serveur peut renvoyer
        # des valeurs plus larges que demandé
        if normalized_dims and not structure:
            df = self._filter_dataframe_by_dimensions(df, normalized_dims)

        # Logging
        logger.info(f"Retrieved {len(df)} rows from {params['dataflow']}")
        return df

    # ──────────────────────────────────────────────────────────────────
    # Méthodes publiques — Structure
    # ──────────────────────────────────────────────────────────────────

    # Méthode publique unifiée de requête d'artefacts structurels SDMX
    def get_structure(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str = AGENCY_ID,
        version: Optional[str] = "+",
        references: StructureReferences = "none",
        detail: StructureDetail = "full",
        format: Optional[str] = None,
        format_version: Optional[str] = None,
        compress: Optional[StructureCompress] = None,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> str:
        """Query an SDMX structure artefact and return raw XML.

        This is the single entry point for all structure queries. The
        ``resource_type`` parameter selects which endpoint is targeted
        (dataflow, datastructure, dataconstraint, conceptscheme, codelist).

        When the ``compress`` parameter is ``"true"`` (default for SDMX 3.0)
        or when the API returns gzip-compressed content, the response is
        transparently decompressed before being returned.

        Args:
            resource_type: Type of structure artefact to retrieve.
            resource_id: Artefact identifier (e.g., ``"namq_10_gdp"``), or
                ``"*"`` to retrieve all artefacts of the given type.
            agency: Maintaining agency (default: ``AGENCY_ID``), or ``"*"``
                for all agencies.
            version: Artefact version. Use ``"+"`` for latest (``"latest"``
                is used automatically when the 2.1 builder is active), ``"~"``
                for SDMX 3.0 latest-per-resource, ``"*"`` for all versions.
            detail: Level of detail (default: ``"full"``).
            references: Related artefacts to include (default: ``"none"``).
            format: Response format identifier (SDMX 3.0 only,
                default: ``"structure"``).
            format_version: Format version string (SDMX 3.0 only).
            compress: Whether to request gzip compression (SDMX 3.0 only,
                default: ``"true"``; the response is decompressed
                transparently).
            accept_encoding: Value for the ``Accept-Encoding`` request header.
            accept_language: Value for the ``Accept-Language`` request header.
            timeout: Request timeout in seconds. Overrides the client-level
                timeout for this call only. Useful for bulk harvest requests
                (``resource_id="*"``) that may take longer than the default.

        Returns:
            Raw XML response text.

        Raises:
            ValueError: If the request fails.

        Notes:
            Passing ``resource_id="*"`` triggers the Eurostat "special case"
            bulk endpoint that returns **all** artefacts of the given type in a
            single request.  This covers both documented special cases of the
            SDMX 3.0 API:

            - *Dataset listing*: ``resource_type=DATAFLOW``, ``resource_id="*"``
              → full catalogue of all dataflows.
              The documentation can be found here : https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started/sdmx3.0#APIGettingstartedwithSDMX3.0API-SpecialcaseofDatasetlisting

            - *Metadata harvesting*: any other ``resource_type`` with
              ``resource_id="*"`` → all codelists, DSDs, concept schemes, etc.
              The documentation can be found here : https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started/sdmx3.0#APIGettingstartedwithSDMX3.0API-SpecialcaseofMetadataharvesting

            Compression (``compress="true"``) is strongly recommended for these
            bulk queries because responses can be very large.

            Version token summary:

            - ``"*"``: any resource / any agency (wildcard).
            - ``"+"``: latest published version (SDMX 2.1 convention; mapped
              to ``"latest"`` by the 2.1 builder).
            - ``"~"``: latest version *per resource* (SDMX 3.0 only).

            For the dataflow catalogue specifically, prefer
            :meth:`list_all_dataflows`, which automatically selects the correct
            version token, sets optimal parameters (``detail="allstubs"``,
            ``references="none"``), and returns a parsed DataFrame.

        Examples:
            >>> # Requête d'un codelist spécifique
            >>> xml = client.get_structure(
            ...     StructureResourceType.CODELIST, "CL_GEO"
            ... )
            >>> # Requête d'un dataflow avec ses artefacts descendants
            >>> xml = client.get_structure(
            ...     StructureResourceType.DATAFLOW,
            ...     "namq_10_gdp",
            ...     references="descendants",
            ... )
            >>> # Metadata harvesting : tous les codelists Eurostat
            >>> xml = client.get_structure(
            ...     StructureResourceType.CODELIST,
            ...     resource_id="*",
            ...     agency="ESTAT",
            ...     compress="true",
            ... )
        """
        # Normalisation de la version pour le cas spécial "metadata harvesting"
        # (resource_id="*"). En SDMX 3.0 l'API Eurostat exige le segment de
        # version wildcard "*" : omettre le segment (None) ou demander "+"
        # (latest) renvoie un conteneur vide avec un HTTP 200 trompeur.
        if (
            resource_id == "*"
            and self.api_version == SDMXVersion.V3
            and version in (None, "+", "~")
        ):
            version = "*"

        # Construction de l'endpoint et des paramètres via le builder
        # Construction de l'endpoint
        endpoint = self.endpoint_builder.build_structure_endpoint(
            resource_type=resource_type,
            resource_id=resource_id,
            agency=agency,
            version=version,
        )
        # Construction des paramètres de requête
        params = self.endpoint_builder.build_structure_params(
            references=references,
            detail=detail,
            format=format,
            format_version=format_version,
            compress=compress,
        )
        # Construction des headers de requête
        headers = self.endpoint_builder.build_headers(
            accept_encoding=accept_encoding,
            accept_language=accept_language,
        )

        # Sélection du client API (Comext si nécessaire)
        client = self._get_api_client(resource_id)

        # Requête de l'artefact structurel
        try:
            response = client.get(endpoint, params=params, headers=headers, timeout=timeout)
            # Décompression si nécessaire (réponses gzip de l'API SDMX 3.0)
            content = parsing.decompress_response_bytes(response.content)
            return content.decode("utf-8")
        # Gestion des erreurs de requête
        except Exception as e:
            # Logging
            logger.error(
                f"Failed to fetch {resource_type.value}/{resource_id}: {e}"
            )
            # Erreur
            raise ValueError(
                f"Failed to fetch {resource_type.value} '{resource_id}': {e}"
            )

    # Méthode publique de récupération et parsing de la DSD d'un dataflow
    def get_dataflow_structure(
        self,
        dataflow: str,
        version: str = "+",
    ) -> DataflowStructure:
        """Retrieve and parse the DSD for a dataflow.

        Convenience wrapper around ``get_structure`` that returns a parsed
        ``DataflowStructure`` instead of raw XML.

        The version wildcards ``"*"`` and ``"~"`` are valid for data
        endpoints but cause HTTP 500 errors on Eurostat's structure endpoint.
        They are therefore remapped to ``"+"`` (latest version) before the
        structure request is issued.

        Args:
            dataflow: Dataflow identifier.
            version: Dataflow version (``"+"`` for latest). The wildcards
                ``"*"`` and ``"~"`` are automatically remapped to ``"+"``.

        Returns:
            Parsed ``DataflowStructure`` with dimension information.

        Raises:
            ValueError: If the structure cannot be retrieved or parsed.
        """
        # Remapping des wildcards "data" vers "+" (dernière version) pour la structure :
        # l'API Eurostat renvoie HTTP 500 pour "*" et "~" sur l'endpoint /structure/datastructure
        _UNSUPPORTED_STRUCTURE_VERSIONS = {"*", "~"}
        structure_version = "+" if version in _UNSUPPORTED_STRUCTURE_VERSIONS else version

        # Requête du XML brut via get_structure (endpoint datastructure, avec descendants)
        xml_text = self.get_structure(
            resource_type=StructureResourceType.DATASTRUCTURE,
            resource_id=dataflow,
            agency=AGENCY_ID,
            version=structure_version,
            references="descendants",
            compress="false"
        )
        # Parsing du XML et retour de la structure de dataflow
        return parsing.parse_structure_response(xml_text, dataflow)

    # Méthode publique d'extraction du catalogue de dataflows Eurostat
    def list_all_dataflows(
        self,
        agency: str = "ESTAT",
    ) -> pd.DataFrame:
        """Retrieve the full Eurostat dataflow catalogue.

        Convenience wrapper for the *Dataset listing* special case of the
        Eurostat SDMX API.  Fetches all dataflow definitions available on the
        configured API endpoint and returns them as a tidy DataFrame.

        Internally calls :meth:`get_structure` with ``resource_id="*"``,
        ``detail="allstubs"``, and ``references="none"``, which maps to the
        following bulk endpoints:

        - SDMX 3.0: ``/sdmx/3.0/structure/dataflow/{agency}/*``
        - SDMX 2.1: ``/sdmx/2.1/dataflow/{agency}/all/latest``

        For other structure types (codelists, DSDs, concept schemes), use
        :meth:`get_structure` directly with ``resource_id="*"``.  See the
        *Notes* section of :meth:`get_structure` for details on bulk /
        metadata-harvesting queries.

        Args:
            agency: Maintaining agency filter. Defaults to ``"ESTAT"`` (official
                Eurostat datasets). Use ``"*"`` for all agencies (mapped to
                ``"all"`` by the SDMX 2.1 builder).

        Returns:
            DataFrame with columns: ``id``, ``name``, ``version``,
            ``agency``.

        Raises:
            ValueError: If the catalogue cannot be retrieved or parsed.

        Examples:
            >>> # Catalogue des datasets officiels Eurostat (défaut)
            >>> catalogue = client.list_all_dataflows()
            >>> # Catalogue de toutes les agences
            >>> all_agencies = client.list_all_dataflows(agency="*")
        """
        # Sélection du token de version adapté à la version d'API :
        # SDMX 3.0 → "*" (wildcard) : le cas spécial Dataset listing exige le
        #   segment de version "*". Omettre le segment ou demander "+" renvoie
        #   un conteneur vide (HTTP 200 trompeur).
        # SDMX 2.1 → "+" converti en "latest" par le builder V2.1
        version: Optional[str] = "*" if self.api_version == SDMXVersion.V3 else "+"

        # Requête du catalogue via get_structure avec wildcards
        xml_text = self.get_structure(
            resource_type=StructureResourceType.DATAFLOW,
            resource_id="*",
            agency=agency,
            version=version,
            references="none",
            detail="allstubs",
            format="structure",
            format_version="3.0",
            compress="true",
        )

        # Parsing du XML et retour sous forme de DataFrame
        return parsing.parse_dataflow_list_response(xml_text)

    # ──────────────────────────────────────────────────────────────────
    # Seam de téléchargement incrémental (orchestrateur core.download)
    # ──────────────────────────────────────────────────────────────────

    # Méthode de récupération de la date de dernière mise à jour des données
    def get_data_last_update(
        self,
        dataflow: str,
        version: str = "*",
    ) -> Optional[datetime]:
        """Return when a dataflow's data was last updated.

        Eurostat has no per-observation ``updated_after`` filter, so the last
        data-update instant is read from the dataflow's *data constraint*
        structure (see :func:`parsing.parse_dataconstraint_last_update`). The
        download orchestrator compares it with the previous download date to
        decide whether to re-pull the most recent observations.

        Args:
            dataflow: Dataflow identifier (e.g. ``"STS_INPR_M"``).
            version: Dataflow version. The data wildcards ``"*"`` and ``"~"``
                are remapped to ``"+"`` (structure endpoint requirement).

        Returns:
            UTC-aware ``datetime`` of the last data update, or ``None`` when it
            cannot be determined (the caller then refreshes conservatively).
        """
        # Remapping des wildcards "data" vers "+" : l'endpoint /structure rejette
        # "*" et "~" (cf. get_dataflow_structure)
        _UNSUPPORTED = {"*", "~"}
        structure_version = "+" if version in _UNSUPPORTED else version

        try:
            # Requête de la contrainte de données (dataconstraint) en XML brut
            xml_text = self.get_structure(
                resource_type=StructureResourceType.DATACONSTRAINT,
                resource_id=dataflow,
                agency=AGENCY_ID,
                version=structure_version,
                references="none",
                compress="false",
            )
            # Extraction de la date de dernière mise à jour
            return parsing.parse_dataconstraint_last_update(xml_text)
        except Exception as e:
            # Échec non bloquant : le caller rafraîchira par précaution
            logger.warning(
                f"Could not fetch data last-update for {dataflow}: {e}"
            )
            return None

    # Implémentation de la récupération incrémentale via dataconstraint
    def fetch_updates(
        self,
        query: "EurostatQueryRequest",
        since: Optional[datetime],
        n_observations: int = 10,
    ) -> pd.DataFrame:
        """Fetch Eurostat data for a query, incrementally when possible.

        Eurostat offers no ``updated_after`` filter, so incremental download
        proceeds in two steps:

        1. Read the dataflow's last data-update instant from its data
           constraint (:meth:`get_data_last_update`).
        2. If the data changed after ``since``, pull the last
           ``n_observations`` observations (``last_n_observations``) — several,
           not just the latest, to avoid leaving gaps when multiple periods
           were revised. Otherwise return an empty DataFrame.

        A first download (``since`` is ``None``) retrieves the full series.
        When the last-update instant cannot be determined the method refreshes
        conservatively (pulls the last ``n_observations``).

        Args:
            query: Eurostat query request (``EurostatQueryRequestV30`` or
                ``EurostatQueryRequestV21``).
            since: Instant of the previous successful download, or ``None``
                for a first (full) download.
            n_observations: Number of most-recent observations to retrieve in
                incremental mode.

        Returns:
            DataFrame with the retrieved data; empty when nothing was
            published since ``since``.
        """
        # Premier téléchargement : récupération complète de la série
        if since is None:
            return self.execute_query(query)

        # Date de dernière mise à jour des données du dataflow
        last_update = self.get_data_last_update(query.dataflow, query.version)

        # Aucune nouvelle publication depuis le dernier téléchargement → vide.
        # Comparaison robuste : normalisation des deux instants en UTC.
        if last_update is not None and _to_utc(last_update) <= _to_utc(since):
            # Logging
            logger.info(
                f"{query.dataflow}: no update since {since} "
                f"(last update {last_update}); skipping data fetch"
            )
            return pd.DataFrame()

        # Récupération des n dernières observations (plusieurs pour éviter les trous)
        incremental_query = replace(query, last_n_observations=n_observations)
        return self.execute_query(incremental_query)

    # ──────────────────────────────────────────────────────────────────
    # Context manager et fermeture des ressources
    # ──────────────────────────────────────────────────────────────────

    # Méthode de fermeture des connexions HTTP
    def close(self) -> None:
        """Close API client connections."""
        # Fermeture du client API standard
        if self.api_client:
            self.api_client.close()
        # Fermeture du client Comext si initialisé
        if self._comext_client:
            self._comext_client.close()
        logger.info("Eurostat client closed")

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Sélection du client API
    # ──────────────────────────────────────────────────────────────────

    # Méthode statique de détection des artefacts Comext (préfixes DS-/CXT_)
    @staticmethod
    def _is_comext_dataset(resource_id: str) -> bool:
        """Detect if a resource belongs to the Comext database.

        Comext data flows are prefixed ``DS-`` and their structural artefacts
        (codelists, concept schemes, etc.) are prefixed ``CXT_``. Both are
        served only by the dedicated Comext endpoint, so structure queries for
        a ``CXT_*`` codelist (e.g. ``CXT_FREE_ISO`` for reporters, ``CXT_NC``
        for products) must be routed there too.

        Args:
            resource_id: Dataflow or structure-artefact identifier.

        Returns:
            True if the identifier starts with ``'DS-'`` or ``'CXT_'``.
        """
        # Détection des préfixes DS- (données) et CXT_ (artefacts) de Comext
        upper = resource_id.upper()
        return upper.startswith("DS-") or upper.startswith("CXT_")

    # Méthode d'accès au client Comext avec initialisation paresseuse
    def _get_comext_client(self) -> APIClient:
        """Get or create the Comext API client (lazy initialisation).

        Returns:
            ``APIClient`` instance for the Comext endpoint.
        """
        # Création du client Comext si non encore initialisé
        if self._comext_client is None:
            comext_url = self._COMEXT_BASE_URLS[self.api_version]
            self._comext_client = APIClient(
                base_url=comext_url, timeout=self._timeout
            )
        return self._comext_client

    # Méthode de sélection du client API approprié selon le dataflow
    def _get_api_client(self, dataflow: str) -> APIClient:
        """Return the appropriate API client for a given dataflow.

        Args:
            dataflow: Dataflow identifier.

        Returns:
            Standard or Comext ``APIClient``.
        """
        # Redirection vers le client Comext pour les datasets DS-*
        if self._is_comext_dataset(dataflow):
            return self._get_comext_client()
        return self.api_client

    # ──────────────────────────────────────────────────────────────────
    # Méthodes abstraites — Implémentations requises par AbstractSDMXClient
    # ──────────────────────────────────────────────────────────────────

    # Implémentation de l'abstraction : fetch de structure sans cache
    def _fetch_structure(
        self, agency: str, dataflow: str, version: str = "*"
    ) -> DataflowStructure:
        """Fetch structure from Eurostat API (no cache).

        Args:
            agency: Maintaining agency (unused — Eurostat always uses ESTAT).
            dataflow: Dataflow identifier.
            version: Dataflow version (``"*"`` for latest).

        Returns:
            Parsed ``DataflowStructure``.
        """
        # Délégation à get_dataflow_structure qui gère le remapping des wildcards
        return self.get_dataflow_structure(dataflow, version)

    # Implémentation de l'abstraction : exécution d'une seule requête de données
    def _execute_single_request(
        self,
        dims_for_request: Dict[str, Any],
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute a single Eurostat data request.

        For Eurostat the ``dims_for_request`` dict has two keys:
        ``dims_for_url`` (embedded in the positional key) and
        ``dims_for_params`` (passed as server-side ``c[DIM]=...`` filters).

        Args:
            dims_for_request: Dict with keys ``"dims_for_url"`` and
                ``"dims_for_params"``.
            **request_kwargs: Extra keyword arguments forwarded from
                :meth:`get_data` (``dataflow``, ``version``,
                ``response_format``, etc.).

        Returns:
            Parsed DataFrame for this single request.
        """
        # Extraction des dimensions URL et paramètres depuis le dict structuré
        dims_for_url: Dict[str, List[str]] = dims_for_request.get("dims_for_url", {})
        dims_for_params: Dict[str, List[str]] = dims_for_request.get("dims_for_params", {})

        dataflow: str = request_kwargs["dataflow"]
        version: str = request_kwargs.get("version", "*")
        structure: Optional[DataflowStructure] = request_kwargs.get("structure")
        response_format: EurostatResponseFormat = request_kwargs.get(
            "response_format", EurostatResponseFormat.CSV
        )
        is_v21 = self.api_version == SDMXVersion.V2_1

        # Construction de la clé positionnelle
        key_str: Optional[str] = None
        if dims_for_url and structure:
            key_str = self._build_key_string(dims_for_url, structure)
        elif is_v21:
            key_str = "all"

        # Construction de l'endpoint et des paramètres
        endpoint = self.endpoint_builder.build_data_endpoint(
            dataflow=dataflow,
            agency=AGENCY_ID,
            version=version,
            key=key_str,
        )
        params = self.endpoint_builder.build_data_params(
            dimensions=dims_for_params or None,
            start_period=request_kwargs.get("start_period"),
            end_period=request_kwargs.get("end_period"),
            last_n_observations=request_kwargs.get("last_n_observations"),
            first_n_observations=request_kwargs.get("first_n_observations"),
            compress=request_kwargs.get("compress", False),
            response_format=response_format,
            response_format_version=request_kwargs.get("response_format_version"),
            lang=request_kwargs.get("lang"),
            labels=request_kwargs.get("labels"),
            attributes=request_kwargs.get("attributes"),
            measures=request_kwargs.get("measures"),
            dimension_at_observation=request_kwargs.get("dimension_at_observation"),
            detail=request_kwargs.get("detail"),
        )

        # Exécution de la requête et parsing de la réponse
        client = self._get_api_client(dataflow)
        response = client.get(endpoint, params=params)
        raw_bytes = parsing.decompress_response_bytes(response.content)

        if response_format == EurostatResponseFormat.CSV:
            return self._parse_csv_response(raw_bytes.decode("utf-8"))
        elif response_format == EurostatResponseFormat.TSV:
            return parsing.parse_tsv_response(raw_bytes.decode("utf-8"))
        elif response_format == EurostatResponseFormat.JSON:
            return parsing.parse_json_response(json.loads(raw_bytes.decode("utf-8")))
        else:
            raise ValueError(f"Unsupported format: {response_format}")

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Normalisation, clé positionnelle, doublons
    # ──────────────────────────────────────────────────────────────────

    # Méthode de construction de la clé positionnelle pour l'URL
    def _build_key_string(
        self,
        dims: Dict[str, List[str]],
        structure: DataflowStructure,
    ) -> str:
        """Build a positional key string for the data endpoint URL.

        The key encodes dimension filters as a dot-separated sequence of
        values, one slot per dimension.  Each slot is either a single value,
        multiple values joined with ``+``, or a wildcard token.

        Wildcard tokens differ by API version:

        - SDMX 3.0: ``*``
        - SDMX 2.1: ``all``

        Args:
            dims: Dimension name → list of values to include in the URL key.
                Dimensions absent from this mapping receive the wildcard.
            structure: Dataflow structure used to resolve dimension names to
                their positional order.

        Returns:
            Positional key string (e.g. ``"FRA+DEU.*.Q"``).

        Examples:
            >>> key = client._build_key_string(
            ...     {"GEO": ["FR", "DE"], "FREQ": ["Q"]},
            ...     structure,
            ... )
            >>> # Returns e.g. "*.FR+DE.Q.*.*" depending on positions
        """
        # Jeton wildcard selon la version d'API
        wildcard = "*" if self.api_version == SDMXVersion.V3 else "all"
        # Index lowercase pour la comparaison insensible à la casse
        dims_lower = {k.lower(): v for k, v in dims.items()}
        # Itération sur les dimensions triées par position (positions XML 1-based)
        sorted_dims = sorted(structure.dimensions, key=lambda d: d.position)
        parts = []
        for dim_info in sorted_dims:
            values = dims_lower.get(dim_info.name.lower())
            if values:
                parts.append("+".join(values))
            else:
                parts.append(wildcard)
        return ".".join(parts)

    # Méthode statique de normalisation des dimensions (str → List[str])
    @staticmethod
    def _normalize_dimensions(
        dimensions: Optional[Dict[str, Union[str, List[str]]]],
        structure: Optional[DataflowStructure] = None,
    ) -> Optional[Dict[str, List[str]]]:
        """Normalize dimension values to ``Dict[str, List[str]]``.

        Args:
            dimensions: Input dimensions (may contain strings or lists).
            structure: Optional dataflow structure used to validate dimension
                names. Unknown names trigger a warning but are kept.

        Returns:
            Normalised dimensions or *None*.
        """
        # Retour immédiat si pas de dimensions à normaliser
        if not dimensions:
            return None
        # Conversion des valeurs scalaires en listes unitaires
        normalized = {
            k: [v] if isinstance(v, str) else list(v)
            for k, v in dimensions.items()
        }

        # Validation des noms contre la structure si disponible
        if structure:
            for name in normalized:
                if structure.get_position(name) is None:
                    logger.warning(
                        f"Dimension '{name}' not found in structure for this dataflow"
                    )
        return normalized

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Chargement de structure à la demande
    # ──────────────────────────────────────────────────────────────────

    # Méthode d'assurance de la disponibilité de la structure d'un dataflow
    def _ensure_structure(
        self, dataflow: str, version: str = "*"
    ) -> DataflowStructure:
        """Load and cache a dataflow structure.

        Args:
            dataflow: Dataflow identifier.
            version: Dataflow version.

        Returns:
            ``DataflowStructure`` instance.

        Raises:
            ValueError: If the structure cannot be retrieved.
        """
        # Vérification dans le cache avant tout appel API
        cached = self.structure_registry.get(AGENCY_ID, dataflow)
        if cached:
            return cached

        # Chargement via API et mise en cache si auto_fetch_structure est activé
        if self.auto_fetch_structure:
            structure = self.get_dataflow_structure(dataflow, version)
            self.register_structure(structure)
            return structure

        # Levée d'une erreur si la structure est introuvable et le fetch désactivé
        raise ValueError(f"Structure not found for {dataflow}::{version}")

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Requêtes splitées
    # ──────────────────────────────────────────────────────────────────

    # Méthode de génération des combinaisons de dimensions pour le split
    def _generate_request_combinations(
        self,
        dimensions: Optional[Dict[str, List[str]]],
        split_dims: Optional[List[str]],
        max_combinations: int,
        is_v21: bool,
        structure: Optional[DataflowStructure] = None,
    ) -> List[Tuple[Dict[str, List[str]], Dict[str, List[str]]]]:
        """Generate request combinations as ``(dims_for_url, dims_for_params)`` tuples.

        Each combination encodes which dimensions go into the positional URL
        key and which go into ``c[DIM]=...`` query parameters:

        - ``dims_for_url``: embedded in the URL key via
          :meth:`_build_key_string`.  For SDMX 3.0 these are the
          single-value dimensions; for SDMX 2.1 all dimensions are placed
          in the key.
        - ``dims_for_params``: passed to
          :meth:`EndpointBuilder.build_data_params` as ``c[DIM]=...``
          server-side filters (SDMX 3.0 only; empty dict for SDMX 2.1).

        When ``split_dims`` is given, one combination is produced per
        element of the cartesian product of the split-dimension values
        (built via :meth:`AbstractSDMXClient._cartesian_split`); each
        combination always contains a single value per split dimension, so
        that value always lands in ``dims_for_url``.

        Args:
            dimensions: Normalised dimensions, or *None*.
            split_dims: Dimension names to split into separate requests.
            max_combinations: Maximum allowed combinations.
            is_v21: Whether the target API version is SDMX 2.1.
            structure: Dataflow structure (reserved for future use,
                currently unused).

        Returns:
            List of ``(dims_for_url, dims_for_params)`` tuples.  Always
            contains at least one element.

        Raises:
            ValueError: If combinations exceed *max_combinations*.

        Examples:
            >>> # V3.0, no split — single-value dim to URL, multi-value to params
            >>> combos = client._generate_request_combinations(
            ...     {"GEO": ["FR"], "UNIT": ["CLV10_MEUR", "CP_MEUR"]},
            ...     split_dims=None, max_combinations=100,
            ...     is_v21=False, structure=None,
            ... )
            >>> combos[0]  # ({"GEO": ["FR"]}, {"UNIT": ["CLV10_MEUR", "CP_MEUR"]})
        """
        # Cas trivial : pas de dimensions → une seule combinaison vide
        if not dimensions:
            return [({}, {})]

        # Séparation des dimensions à splitter de celles à conserver intactes
        split_dims_set = set(split_dims) if split_dims else set()
        split_dict = {k: v for k, v in dimensions.items() if k in split_dims_set}
        keep_dict = {k: v for k, v in dimensions.items() if k not in split_dims_set}

        # Produit cartésien des valeurs des dimensions à splitter (helper mutualisé).
        # Renvoie [{}] s'il n'y a aucune dim à splitter → une seule combinaison.
        split_combos = self._cartesian_split(split_dict, max_combinations)

        # Construction des tuples (dims_for_url, dims_for_params) pour chaque combinaison
        result: List[Tuple[Dict[str, List[str]], Dict[str, List[str]]]] = []
        for combo in split_combos:
            # Fusion des dims non-splittées avec la valeur unique de chaque dim splittée
            combo_dims: Dict[str, List[str]] = keep_dict.copy()
            for k, val in combo.items():
                combo_dims[k] = [val]

            # Répartition entre URL key et query params selon la version SDMX
            if is_v21:
                # SDMX 2.1 : toutes les dims vont dans le key positionnel
                dims_for_url = combo_dims
                dims_for_params: Dict[str, List[str]] = {}
            else:
                # SDMX 3.0 : valeur unique → key positionnel, multi-valeurs → c[DIM]=...
                dims_for_url = {k: v for k, v in combo_dims.items() if len(v) == 1}
                dims_for_params = {k: v for k, v in combo_dims.items() if len(v) > 1}

            result.append((dims_for_url, dims_for_params))

        return result
