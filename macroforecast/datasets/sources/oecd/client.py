"""OECD data client.

High-level client for querying OECD data through their SDMX API and
converting responses to pandas DataFrames.
"""
# Importation des modules
from dataclasses import replace
from datetime import datetime
import logging
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, Union
import xml.etree.ElementTree as ET

import pandas as pd

# Utilitaires internes au package pour la requête de données au format SDMX
from ...core.client import AbstractSDMXClient, APIClient
from ...core.rate_limiter import RateLimiter
from ...core.sdmx import (
    DimensionAtObservation,
    DuplicateHandling,
    SDMXVersion,
)
from ...core.structures import (
    DataflowStructure,
    DataflowStructureRegistry,
)
from . import parsing
from .endpoints import OECDEndpointBuilder, _OECD_ENDPOINT_BUILDERS
from .formats import OECDResponseFormat

if TYPE_CHECKING:
    from .queries import OECDQueryRequest

# Initialisation du logger
logger = logging.getLogger(__name__)


# Initialisation du client pour la requête de données
class OECDClient(AbstractSDMXClient):
    """High-level client for OECD data API.

    This client handles data retrieval from OECD's SDMX API and provides
    convenient methods to query economic indicators and convert them to
    pandas DataFrames.

    Args:
        base_url: OECD API base URL (default: SDMX public endpoint).
        timeout: Request timeout in seconds.
        sdmx_version: SDMX API version to use (v1 or v2).
        structure_registry: Optional registry for dimension name resolution.
        auto_fetch_structure: If True, fetch structure metadata when needed.

    Example:
        >>> client = OECDClient()
        >>> df = client.get_data(
        ...     dataflow="DSD_KEI@DF_KEI",
        ...     agency="OECD.SDD.STES",
        ...     dimensions={"REF_AREA": ["FRA"], "MEASURE": ["PRINTO01"]},
        ... )
    """
    # Initialisation de l'URL par défaut
    DEFAULT_BASE_URL = "https://sdmx.oecd.org/public/rest"

    # Nom du fichier de configuration pour le chargement du rate limiter
    PROVIDER_CONFIG_NAME = "oecd"

    # Initialisation
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 60,
        sdmx_version: SDMXVersion = SDMXVersion.V2,
        structure_registry: Optional[DataflowStructureRegistry] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional[RateLimiter] = None,
        auto_load_rate_limit: bool = True,
    ):
        # Initialisation de la base (structure_registry, auto_fetch_structure,
        # rate_limiter via _load_rate_limiter)
        super().__init__(
            structure_registry=structure_registry,
            auto_fetch_structure=auto_fetch_structure,
            rate_limiter=rate_limiter,
            auto_load_rate_limit=auto_load_rate_limit,
        )

        # Attributs spécifiques OECD
        self.base_url = base_url
        self.sdmx_version = sdmx_version
        self.api_client = APIClient(base_url=base_url, timeout=timeout)
        # Sélection du builder versionné depuis le registre
        self.endpoint_builder: OECDEndpointBuilder = _OECD_ENDPOINT_BUILDERS[sdmx_version]

    # Méthode de requête des données
    def get_data(
        self,
        agency: str,
        dataflow: str,
        version: str = "+",
        dimensions: Optional[Dict[Union[int, str], Union[str, List[str]]]] = None,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        format: OECDResponseFormat = OECDResponseFormat.CSV_LABELS,
        dimension_at_observation: DimensionAtObservation = DimensionAtObservation.ALL_DIMENSIONS,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        on_duplicate: DuplicateHandling = "warn",
        split_dimensions: Optional[List[Union[int, str]]] = None,
        max_split_combinations: int = 100,
        updated_after: Optional[Union[str, datetime]] = None,
        default_dimensions: List[str] = ['TIME_PERIOD'],
    ) -> pd.DataFrame:
        """Retrieve data from OECD API.

        Args:
            agency: Agency identifier (e.g., "OECD.SDD.STES").
            dataflow: Dataflow identifier (e.g., "DSD_KEI@DF_KEI").
            version: Dataflow version (default: "+" for latest).
            dimensions: Dimension filters, can be:
                - Dict[int, str/List[str]]: dimension position -> values
                - Dict[str, str/List[str]]: dimension name -> values
            start_period: Start period (e.g., "2015", "2015-Q1").
            end_period: End period.
            last_n_observations: Number of recent observations.
            format: Response format (json, csv, csv_labels, xml).
            dimension_at_observation: How to group observations
                (AllDimensions for flat, TIME_PERIOD for series).
            attributes: Attributes to include ("dsd", "all", "none").
            measures: Measures to include ("all", "none").
            on_duplicate: How to handle duplicate rows when wildcards are used:
                - "ignore": Keep all rows without checking.
                - "warn": Log a warning if duplicates are found.
                - "raise": Raise an exception if duplicates are found.
            split_dimensions: List of dimensions (by position or name) for which multiple
                values should be handled in separate API requests. If None (default), all
                dimensions with multiple values will use wildcards (*) in the URL and be
                filtered post-retrieval. Use this parameter to trade off API calls vs
                response size (splitting reduces response size but increases API calls).
                Example: split_dimensions=["REF_AREA"] or split_dimensions=[0]
                NOTE: Internally converted to dimension NAMES for consistent filtering.
            max_split_combinations: Maximum number of requests allowed when splitting.
                Prevents accidental explosion of API calls. Default: 100
            updated_after: Incremental-sync threshold (SDMX-CSV v2 only). When
                set, the response only includes observations inserted, updated
                or deleted since that instant. Accepts an ISO-8601 dateTime
                string (with timezone) or a ``datetime`` object (naive datetimes
                are interpreted as UTC). Ignored when ``sdmx_version`` is V1.
            default_dimensions: Default dimensions to check duplicates on

        Returns:
            DataFrame with the retrieved data.

        Raises:
            ValueError: If dataflow is not specified or dimension resolution fails.
            DuplicateRowsError: If on_duplicate="raise" and duplicates are found.

        Example:
            >>> # By position
            >>> df = client.get_data(
            ...     agency="OECD.SDD.STES",
            ...     dataflow="DSD_KEI@DF_KEI",
            ...     dimensions={
            ...         0: ["FRA", "DEU"],
            ...         2: ["PRINTO01"],
            ...     },
            ... )

            >>> # By name (requires structure metadata)
            >>> df = client.get_data(
            ...     agency="OECD.SDD.STES",
            ...     dataflow="DSD_KEI@DF_KEI",
            ...     dimensions={
            ...         "REF_AREA": ["FRA", "DEU"],
            ...         "MEASURE": ["PRINTO01"],
            ...     },
            ... )
        """
        # Vérification de la validité du "dataflow"
        if dataflow is None:
            raise ValueError("dataflow is required")

        # Empaquetage des paramètres et délégation au pipeline mutualisé
        # (résolution structure → préparation requêtes → exécution → doublons)
        params: Dict[str, Any] = {
            "agency": agency,
            "dataflow": dataflow,
            "version": version,
            "dimensions": dimensions,
            "start_period": start_period,
            "end_period": end_period,
            "last_n_observations": last_n_observations,
            "format": format,
            "dimension_at_observation": dimension_at_observation,
            "attributes": attributes,
            "measures": measures,
            "on_duplicate": on_duplicate,
            "split_dimensions": split_dimensions,
            "max_split_combinations": max_split_combinations,
            "updated_after": updated_after,
            "default_dimensions": default_dimensions
        }
        return self._execute_query_pipeline(params)

    # ──────────────────────────────────────────────────────────────────
    # Seam de téléchargement incrémental (orchestrateur core.download)
    # ──────────────────────────────────────────────────────────────────

    # Implémentation de la récupération incrémentale via ``updated_after``
    def fetch_updates(
        self,
        query: "OECDQueryRequest",
        since: Optional[datetime],
        n_observations: int = 10,
    ) -> pd.DataFrame:
        """Fetch OECD data for a query, incrementally when possible.

        Uses the SDMX-CSV v2 ``updated_after`` filter, which returns only the
        observations inserted, updated or deleted since the given instant —
        the precise, server-side incremental mechanism for OECD. The
        ``n_observations`` argument is therefore unused (it exists for
        providers without ``updated_after``, e.g. Eurostat).

        Args:
            query: OECD query request (``OECDQueryRequest``).
            since: Instant of the previous successful download, or ``None``
                for a first (full) download.
            n_observations: Ignored for OECD (see above).

        Returns:
            DataFrame with the retrieved data; empty when nothing changed
            since ``since``.
        """
        # Premier téléchargement : récupération complète de la série
        if since is None:
            return self.execute_query(query)

        # Téléchargements suivants : seules les observations modifiées depuis
        # ``since`` via le paramètre serveur ``updated_after`` (SDMX-CSV v2).
        incremental_query = replace(query, updated_after=since)
        return self.execute_query(incremental_query)

    # ──────────────────────────────────────────────────────────────────
    # Hooks du pipeline (AbstractSDMXClient._execute_query_pipeline)
    # ──────────────────────────────────────────────────────────────────

    # Implémentation du hook : résolution de la structure
    def _resolve_structure(
        self, params: Dict[str, Any]
    ) -> Optional[DataflowStructure]:
        """Resolve the dataflow structure for a query (registry or API fetch).

        Args:
            params: Parameter dict from :meth:`get_data`.

        Returns:
            Resolved ``DataflowStructure`` or ``None``.
        """
        # Récupération de la structure du dataflow si nécessaire
        return self._ensure_structure(
            agency=params["agency"], dataflow=params["dataflow"]
        )

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
        agency: str = params["agency"]
        dataflow: str = params["dataflow"]

        # Normalisation des dimensions au format Dict[int, List[str]]
        normalized_dims = self._normalize_dimensions(
            dimensions=params.get("dimensions"),
            agency=agency,
            dataflow=dataflow,
            structure=structure,
        )

        # Normalisation de split_dimensions (convertit tout en NOMS)
        split_dimension_names = self._normalize_split_dimensions(
            params.get("split_dimensions"), structure, normalized_dims
        )

        # Génération des combinaisons (dims_for_url, dims_for_postfilter)
        request_combinations = self._generate_request_combinations(
            normalized_dims,
            split_dimension_names,
            structure,
            params.get("max_split_combinations", 100),
        )

        # Logging
        logger.info(
            f"Fetching data from {dataflow} ({len(request_combinations)} request(s))"
        )

        # Arguments transmis à chaque _execute_single_request
        execute_kwargs: Dict[str, Any] = {
            "agency": agency,
            "dataflow": dataflow,
            "version": params.get("version", "+"),
            "structure": structure,
            "format": params.get("format", OECDResponseFormat.CSV_LABELS),
            "dimension_at_observation": params.get(
                "dimension_at_observation", DimensionAtObservation.ALL_DIMENSIONS
            ),
            "start_period": params.get("start_period"),
            "end_period": params.get("end_period"),
            "last_n_observations": params.get("last_n_observations"),
            "attributes": params.get("attributes"),
            "measures": params.get("measures"),
            "updated_after": params.get("updated_after"),
        }

        return request_combinations, normalized_dims, execute_kwargs

    # Méthode auxiliaire de normalisation des dimensions du filtre sous la forme d'un dictionnaire position : valeur
    def _normalize_dimensions(
        self,
        dimensions: Optional[Dict[Union[int, str], Union[str, List[str]]]],
        agency: str,
        dataflow: str,
        structure: Optional[DataflowStructure] = None,
    ) -> Dict[int, List[str]]:
        """Normalize dimensions to Dict[int, List[str]] format.

        Args:
            dimensions: Input dimensions in various formats.
            agency: Agency identifier for dimension resolution.
            dataflow: Dataflow identifier for dimension resolution.
            structure: Optional structure for dimension resolution.

        Returns:
            Normalized dimensions with integer keys and list values.

        Raises:
            ValueError: If dimension names cannot be resolved.
        """
        # Cas où les dimensions ne sont pas spécifiées
        if dimensions is None:
            return {}

        # Vérification si des noms de dimensions sont utilisés
        has_string_keys = any(isinstance(k, str) for k in dimensions.keys())

        # Conversion en position
        if has_string_keys:
            # Utilisation du registre pour résoudre les noms
            return self.structure_registry.resolve_dimensions(
                agency,
                dataflow,
                dimensions,
            )

        # Cas où toutes les clés sont des entiers
        normalized = {}
        # Normalisation de la valeur sous forme de liste
        for key, value in dimensions.items():
            # Conversion de la valeur en liste si nécessaire
            if isinstance(value, str):
                normalized[key] = [value]
            else:
                normalized[key] = list(value)

        return normalized

    # Méthode auxiliaire de normalisation de split_dimensions en noms de dimensions
    def _normalize_split_dimensions(
        self,
        split_dimensions: Optional[List[Union[int, str]]],
        structure: Optional[DataflowStructure],
        dimensions: Dict[int, List[str]],
    ) -> List[str]:
        """Normalize split_dimensions to dimension NAMES and validate.

        Returns dimension names (not positions) because column positions
        in the output DataFrame vary by format (csvfile vs csvfilewithlabels),
        while dimension names remain consistent.

        Args:
            split_dimensions: List of dimension positions or names to split
            structure: Dataflow structure for name/position resolution
            dimensions: Normalized dimensions dict (position → values)

        Returns:
            List of unique dimension names to split

        Raises:
            ValueError: If dimension name/position cannot be resolved
            ValueError: If split dimension not in dimensions dict
            ValueError: If structure is None (required for name resolution)
        """
        # Cas où aucune dimension à split
        if split_dimensions is None:
            return []

        # Vérification de la structure
        if structure is None:
            raise ValueError(
                "Structure required when using split_dimensions. "
                "The structure could not be loaded for this dataflow."
            )

        # Initialisation de la liste des noms
        normalized_names: List[str] = []

        # Parcours des dimensions à split
        for dim_spec in split_dimensions:
            # Cas où c'est une position (int)
            if isinstance(dim_spec, int):
                # Conversion en nom via structure
                dim_name = structure.get_name(dim_spec)
                # Vérification que la position existe
                if dim_name is None:
                    raise ValueError(
                        f"Position {dim_spec} not found in structure. "
                        f"Valid positions: 0-{structure.num_dimensions-1}"
                    )
                # Vérification que la dimension est dans le filtre
                if dim_spec not in dimensions:
                    raise ValueError(
                        f"Dimension at position {dim_spec} ('{dim_name}') "
                        f"is not in the dimensions filter. "
                        f"Cannot split on dimension that is not filtered."
                    )
                normalized_names.append(dim_name)

            # Cas où c'est un nom (str)
            elif isinstance(dim_spec, str):
                # Vérification que le nom existe
                dim_position = structure.get_position(dim_spec)
                if dim_position is None:
                    # Liste des dimensions disponibles
                    available_names = [dim.name for dim in structure.dimensions]
                    raise ValueError(
                        f"Dimension name '{dim_spec}' not found in structure. "
                        f"Available dimensions: {available_names}"
                    )
                # Vérification que la dimension est dans le filtre
                if dim_position not in dimensions:
                    raise ValueError(
                        f"Dimension '{dim_spec}' (position {dim_position}) "
                        f"is not in the dimensions filter. "
                        f"Cannot split on dimension that is not filtered."
                    )
                normalized_names.append(dim_spec)

            # Type invalide
            else:
                raise ValueError(
                    f"Invalid type for split_dimensions element: {type(dim_spec)}. "
                    f"Expected int or str."
                )

        # Dédupliquer les noms (conversion en set puis list)
        unique_names = list(dict.fromkeys(normalized_names))  # Préserve l'ordre

        # Logging si déduplication
        if len(unique_names) < len(normalized_names):
            logger.debug(f"Deduplicated split_dimensions: {normalized_names} → {unique_names}")

        return unique_names

    # Méthode auxiliaire de génération des combinaisons de requêtes
    def _generate_request_combinations(
        self,
        dimensions: Dict[int, List[str]],
        split_dimension_names: List[str],
        structure: Optional[DataflowStructure],
        max_combinations: int = 100,
    ) -> List[Tuple[Dict[int, List[str]], Dict[str, List[str]]]]:
        """Generate request combinations and post-filter dimensions.

        Args:
            dimensions: Normalized dimensions dict (position → values)
            split_dimension_names: Dimension NAMES to split into separate requests
            structure: Dataflow structure for name/position mapping
            max_combinations: Maximum allowed combinations

        Returns:
            List of (dimensions_for_url, dimensions_for_postfilter) tuples
            - dimensions_for_url: Dict[int, List[str]] for URL construction
            - dimensions_for_postfilter: Dict[str, List[str]] for DataFrame filtering (by NAME)

        Raises:
            ValueError: If cartesian product exceeds max_combinations
        """
        # Conversion des noms en positions (structure garantie non-None si
        # split_dimension_names est non vide grâce à _normalize_split_dimensions)
        split_positions = (
            [structure.get_position(name) for name in split_dimension_names]
            if structure is not None
            else []
        )

        # Identification des dimensions à split (valeurs multiples et dans split_positions)
        split_dims: Dict[int, List[str]] = {
            pos: values
            for pos, values in dimensions.items()
            if pos in split_positions and len(values) > 1
        }

        # Identification des dimensions multi-valeurs à ne pas split (pour filtre ex post)
        postfilter_dims: Dict[int, List[str]] = {
            pos: values
            for pos, values in dimensions.items()
            if pos not in split_positions and len(values) > 1
        }

        # Conversion des positions postfilter en noms (skip si pas de structure :
        # le post-filtrage par nom de colonne n'est alors pas possible)
        postfilter_dims_by_name: Dict[str, List[str]] = {}
        if structure is not None:
            for pos, values in postfilter_dims.items():
                dim_name = structure.get_name(pos)
                if dim_name:
                    postfilter_dims_by_name[dim_name] = values

        # Produit cartésien des dimensions à splitter (helper mutualisé).
        # Renvoie [{}] s'il n'y a aucune dimension à splitter → une seule combinaison.
        split_combos = self._cartesian_split(split_dims, max_combinations)

        # Construction des combinaisons (dims_for_url, dims_for_postfilter)
        combinations: List[Tuple[Dict[int, List[str]], Dict[str, List[str]]]] = []
        for combo in split_combos:
            # Copie des dimensions de base
            dims_for_url = dimensions.copy()
            # Remplacement des dimensions splittées par leur valeur unique
            for pos, value in combo.items():
                dims_for_url[pos] = [value]
            # Remplacement des dimensions post-filtrées par wildcard
            for pos in postfilter_dims:
                dims_for_url[pos] = ["*"]
            # Copie défensive du dict de post-filtre (par NOM)
            combinations.append((dims_for_url, dict(postfilter_dims_by_name)))

        # Logging
        logger.info(
            f"Generated {len(combinations)} request combinations "
            f"for split dimensions: {split_dimension_names}"
        )

        return combinations

    # ──────────────────────────────────────────────────────────────────
    # Structure
    # ──────────────────────────────────────────────────────────────────

    # Méthode d'extraction de la structure des métadonnées associées à un flux
    def get_structure(
        self,
        agency: str,
        dataflow: str,
        version: str = "+",
        timeout: Optional[int] = None,
    ) -> DataflowStructure:
        """Retrieve dataflow structure metadata.

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            version: Dataflow version (default: "+" for latest).
            timeout: Request timeout in seconds. Overrides the client-level
                timeout for this call only.

        Returns:
            Structure metadata as dictionary.

        Raises:
            ValueError: If dataflow is not specified.
        """
        # Vérification que le flux de données est spécifié
        if dataflow is None:
            raise ValueError("dataflow is required")

        # Construction de l'URL et des paramètres de structure via le builder versionné
        endpoint = self.endpoint_builder.build_structure_endpoint(
            resource_type=None,
            resource_id=dataflow,
            agency=agency,
            version=version,
        )
        params = self.endpoint_builder.build_structure_params()

        # Construction des headers de requête (Accept dynamique selon la version)
        headers = {
            "Accept": self.endpoint_builder.get_structure_accept_header(),
        }

        # Exécution de la requête
        response = self.api_client.get(endpoint, params=params, headers=headers, timeout=timeout)
        return self.create_structure_from_api_response(
            agency=agency, dataflow=dataflow, api_response=response.json()
        )

    # Méthode publique de création d'une structure depuis une réponse API
    def create_structure_from_api_response(
        self,
        agency: str,
        dataflow: str,
        api_response: Dict[str, Any],
    ) -> DataflowStructure:
        """Create a DataflowStructure from OECD API structure response.

        Thin wrapper preserving the public API; delegates to
        :func:`parsing.create_structure_from_api_response`.

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            api_response: JSON response from structure API endpoint.

        Returns:
            DataflowStructure instance.

        Raises:
            ValueError: If the response cannot be parsed.
        """
        return parsing.create_structure_from_api_response(agency, dataflow, api_response)

    # Méthode de listing de tous les dataflows disponibles
    def list_all_dataflows(self) -> pd.DataFrame:
        """List all available OECD dataflows.

        Retrieves the complete list of dataflows from OECD SDMX API
        and parses them into a pandas DataFrame. The API always returns
        JSON regardless of the Accept header, using a SDMX v2 structure
        format where dataflows are keyed by URN in a ``references`` dict.

        Returns:
            DataFrame with columns: dataflow, agency, version, name

        Example:
            >>> client = OECDClient()
            >>> df = client.list_all_dataflows()
            >>> df.head()
        """
        # Endpoint pour lister tous les dataflows
        endpoint = "dataflow/all"

        # Headers
        headers = None

        # Application du rate limiter si configuré
        if self.rate_limiter:
            self.rate_limiter.acquire()

        # Logging
        logger.info("Fetching list of all OECD dataflows")

        # Exécution de la requête
        response = self.api_client.get(endpoint, headers=headers)

        # Parsing XML
        root = ET.fromstring(response.content)

        # Namespaces SDMX
        namespaces = {
            'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }

        # Extraction des dataflows
        dataflows = []
        for df in root.findall('.//str:Dataflow', namespaces):
            # Extraction des attributs
            dataflow_id = df.get('id')
            agency_id = df.get('agencyID')
            version = df.get('version')

            # Extraction du nom
            name_elem = df.find('.//com:Name', namespaces)
            name = name_elem.text if name_elem is not None else None

            # Ajout à la liste
            dataflows.append({
                'dataflow': dataflow_id,
                'agency': agency_id,
                'version': version,
                'name': name
            })

        # Conversion en DataFrame
        df_result = pd.DataFrame(dataflows)

        # Logging
        logger.info(f"Found {len(df_result)} dataflows")

        return df_result

    # ──────────────────────────────────────────────────────────────────
    # Méthodes abstraites — Implémentations requises par AbstractSDMXClient
    # ──────────────────────────────────────────────────────────────────

    # Implémentation de l'abstraction : fetch de structure sans cache
    def _fetch_structure(
        self, agency: str, dataflow: str, **kwargs
    ) -> DataflowStructure:
        """Fetch structure from OECD API (no cache).

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            **kwargs: Accepts ``version`` (ignored — OECD uses ``"+"``).

        Returns:
            Parsed ``DataflowStructure``.
        """
        # Délégation à get_structure qui gère l'appel API et le parsing
        return self.get_structure(agency, dataflow)

    # Implémentation de l'abstraction : exécution d'une seule requête de données
    def _execute_single_request(
        self,
        dims_for_request: Dict[int, List[str]],
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute a single OECD data request.

        Builds the data endpoint via :attr:`endpoint_builder`, executes the
        request, and parses the response.

        Args:
            dims_for_request: Dimension position → values for URL construction
                (``Dict[int, List[str]]``).
            **request_kwargs: Keyword arguments forwarded from
                ``_execute_split_requests`` or ``get_data``: ``agency``,
                ``dataflow``, ``version``, ``structure``, ``format``,
                ``dimension_at_observation``, ``start_period``,
                ``end_period``, ``last_n_observations``, ``attributes``,
                ``measures``.

        Returns:
            Parsed DataFrame for this single request.
        """
        agency: str = request_kwargs["agency"]
        dataflow: str = request_kwargs["dataflow"]
        version: str = request_kwargs.get("version", "+")
        structure: Optional[DataflowStructure] = request_kwargs.get("structure")
        fmt: OECDResponseFormat = request_kwargs.get("format", OECDResponseFormat.CSV_LABELS)
        dimension_at_observation: DimensionAtObservation = request_kwargs.get(
            "dimension_at_observation", DimensionAtObservation.ALL_DIMENSIONS
        )
        num_dimensions = structure.num_dimensions if structure else None

        # Construction du filtre de dimensions positionnel via le builder versionné
        dim_filter = self.endpoint_builder.build_dimension_filter(
            dims_for_request, num_dimensions
        )

        # Construction de l'endpoint, des paramètres et des headers
        endpoint = self.endpoint_builder.build_data_endpoint(
            dataflow=dataflow,
            agency=agency,
            version=version,
            key=dim_filter,
        )
        params = self.endpoint_builder.build_data_params(
            start_period=request_kwargs.get("start_period"),
            end_period=request_kwargs.get("end_period"),
            last_n_observations=request_kwargs.get("last_n_observations"),
            response_format=fmt,
            attributes=request_kwargs.get("attributes"),
            measures=request_kwargs.get("measures"),
            dimension_at_observation=(
                dimension_at_observation.value
                if isinstance(dimension_at_observation, DimensionAtObservation)
                else dimension_at_observation
            ),
            updated_after=request_kwargs.get("updated_after"),
        )
        headers = self.endpoint_builder.build_headers(response_format=fmt)

        # Exécution de la requête
        logger.debug(f"Single request: {endpoint}")
        response = self.api_client.get(endpoint, params=params, headers=headers)

        # Parsing de la réponse
        if fmt == OECDResponseFormat.JSON:
            return parsing.parse_json_response(response.json())
        elif fmt in (OECDResponseFormat.CSV, OECDResponseFormat.CSV_LABELS):
            return self._parse_csv_response(response.text)
        else:
            raise NotImplementedError(f"Format {fmt} not yet implemented")

    # Méthode de fermeture de la session
    def close(self) -> None:
        """Close the client and release resources."""
        self.api_client.close()
