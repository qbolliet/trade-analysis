"""Eurostat query request dataclasses.

Version-specific DTOs encapsulating the parameters of an
:meth:`EurostatClient.get_data` call. :class:`EurostatQueryRequest` holds the
parameters shared by both API versions; :class:`EurostatQueryRequestV30` and
:class:`EurostatQueryRequestV21` add the version-specific fields.
"""
# Importation des modules
# Modules de base
from dataclasses import dataclass
from typing import ClassVar, Dict, List, Optional, Type, Union
# Modules du package
from ...core.queries import SDMXQueryRequest
from ...core.sdmx import DuplicateHandling, SDMXResponseFormat
from .formats import AGENCY_ID, DataDetail, EurostatResponseFormat


# Classe de base contenant les paramètres communs aux deux versions d'API
@dataclass
class EurostatQueryRequest(SDMXQueryRequest):
    """Base class for Eurostat query requests.

    Contains all parameters shared by both SDMX 3.0 and 2.1 API versions.
    Use :class:`EurostatQueryRequestV30` or :class:`EurostatQueryRequestV21`
    directly to benefit from version-specific parameter typing.

    Attributes:
        dataflow: Dataflow identifier (e.g., ``"namq_10_gdp"``,
            ``"DS-045409"``).
        version: Dataflow version (default: ``"*"`` for latest).
        dimensions: Dimension filters as ``{name: value_or_list}``.
        start_period: Start period filter (ISO / SDMX format).
        end_period: End period filter.
        last_n_observations: Number of most-recent observations to return.
        first_n_observations: Number of first observations to return.
        format: Response format (default: CSV).
        compress: Whether to request gzip compression of the response.
        on_duplicate: Duplicate handling strategy.
        split_dimensions: Dimensions to split into separate sub-requests.
        max_split_combinations: Maximum allowed split combinations.
    """
    # Attributs communs aux deux versions
    dataflow: str
    version: str = "*"
    dimensions: Optional[Dict[str, Union[str, List[str]]]] = None
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    last_n_observations: Optional[int] = None
    first_n_observations: Optional[int] = None
    format: EurostatResponseFormat = EurostatResponseFormat.CSV
    compress: bool = False
    on_duplicate: DuplicateHandling = "warn"
    split_dimensions: Optional[List[str]] = None
    max_split_combinations: int = 100

    # Enum de format du provider (utilisé par SDMXQueryRequest.from_dict)
    _FORMAT_ENUM: ClassVar[Optional[Type[SDMXResponseFormat]]] = EurostatResponseFormat

    # Propriété d'agence (Eurostat publie toujours sous l'agence ESTAT)
    @property
    def agency(self) -> str:
        """Maintaining agency identifier (always ``ESTAT`` for Eurostat).

        Exposed for symmetry with :class:`OECDQueryRequest` so the download
        orchestrator can resolve the structure-registry key and the DuckLake
        schema uniformly across providers.

        Returns:
            The Eurostat agency identifier ``"ESTAT"``.
        """
        return AGENCY_ID

    # Surcharge de la clé de dataflow (Eurostat n'inclut pas l'agence, toujours ESTAT)
    def get_dataflow_key(self) -> str:
        """Get unique key for this dataflow.

        Overrides :meth:`SDMXQueryRequest.get_dataflow_key` to preserve the
        Eurostat-specific format (the agency is always ``ESTAT`` and is left out
        of the key).

        Returns:
            Key in format ``'dataflow::version'``.
        """
        return f"{self.dataflow}::{self.version}"

    # to_dict, from_dict et identity_key sont hérités de SDMXQueryRequest.


# Classe représentant une requête de données Eurostat via l'API SDMX 3.0
@dataclass
class EurostatQueryRequestV30(EurostatQueryRequest):
    """Query request for the Eurostat SDMX 3.0 API.

    Extends :class:`EurostatQueryRequest` with parameters specific to
    the SDMX 3.0 endpoint. Use this class when the client is configured with
    ``api_version=SDMXVersion.V3`` (the default).

    Attributes:
        attributes: Attribute selection string (e.g., ``"dataStructure"``).
        measures: Measure selection string (e.g., ``"OBS_VALUE"``).
        lang: Language code for label localisation (e.g., ``"en"``,
            ``"fr"``).
        labels: Label display mode (e.g., ``"name"``, ``"id"``).
        response_format_version: Format version string (e.g., ``"1.0"``).

    Example:
        >>> query = EurostatQueryRequestV30(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR", "DE"], "FREQ": "Q"},
        ...     lang="fr",
        ... )
        >>> df = client.execute_query(query)
    """
    # Attributs spécifiques SDMX 3.0
    attributes: Optional[str] = None
    measures: Optional[str] = None
    lang: Optional[str] = None
    labels: Optional[str] = None
    response_format_version: Optional[str] = None



# Classe représentant une requête de données Eurostat via l'API SDMX 2.1
@dataclass
class EurostatQueryRequestV21(EurostatQueryRequest):
    """Query request for the Eurostat SDMX 2.1 API (legacy).

    Extends :class:`EurostatQueryRequest` with parameters specific to
    the SDMX 2.1 endpoint. Use this class when the client is configured with
    ``api_version=SDMXVersion.V2_1``.

    Attributes:
        dimension_at_observation: Dimension serialised at observation
            level (e.g., ``"AllDimensions"`` for flat output,
            ``"TIME_PERIOD"`` for time series).
        detail: Data detail level (e.g., ``"dataonly"``,
            ``"serieskeysonly"``).

    Example:
        >>> query = EurostatQueryRequestV21(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR", "DE"], "FREQ": "Q"},
        ...     detail="dataonly",
        ... )
        >>> df = client_v21.execute_query(query)
    """
    # Attributs spécifiques SDMX 2.1
    dimension_at_observation: Optional[str] = None
    detail: Optional[DataDetail] = None

