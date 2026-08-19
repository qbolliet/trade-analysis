"""SDMX shared abstractions.

This module provides provider-agnostic SDMX abstractions usable across all
SDMX provider clients:

- ``SDMXVersion``: SDMX / OECD API version enum.
- ``DimensionAtObservation``: observation-level dimension options.
- ``StructureResourceType``: queryable SDMX artefact types.
- ``DuplicateHandling``: duplicate-row handling strategy literal type.
- ``SDMXResponseFormat``: marker base class for provider response-format enums.
- ``SDMXEndpointBuilder``: abstract base class for URL builders.

Provider-specific response format enums (``EurostatResponseFormat``,
``OECDResponseFormat``) inherit from :class:`SDMXResponseFormat` so that
generic abstractions can refer to them by a common type.
"""
# Importation des modules
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Literal, Optional
from enum import Enum


# ──────────────────────────────────────────────────────────────────────
# Éléments transversaux à tous les providers SDMX
# ──────────────────────────────────────────────────────────────────────

# Type pour la gestion des doublons (commun à tous les providers)
DuplicateHandling = Literal["ignore", "warn", "raise"]


# Fonction de construction d'une clé d'identité stable d'une requête
def build_identity_key(
    agency: str,
    dataflow: str,
    version: str,
    dimensions: Optional[Dict[Any, Any]] = None,
) -> str:
    """Build a deterministic identity key from query selection fields.

    Shared by the provider query DTOs (``OECDQueryRequest``,
    ``EurostatQueryRequest``) so that every download registry uses the same
    canonical format. Only the *selection* fields that determine which series
    is fetched are encoded (agency, dataflow, version, dimensions) — not
    presentation options such as ``format`` — so re-running the same logical
    query reuses its registry entry. Dimensions are normalised to sorted
    ``key=value1,value2`` segments, themselves sorted, making the result
    independent of insertion order.

    Args:
        agency: Maintaining agency identifier.
        dataflow: Dataflow identifier.
        version: Dataflow version token.
        dimensions: Dimension filters (values may be scalars or lists).

    Returns:
        Stable identity string ``"agency::dataflow::version::dims"``.

    Examples:
        >>> build_identity_key("ESTAT", "namq_10_gdp", "*",
        ...                     {"GEO": ["FR", "DE"], "FREQ": "Q"})
        'ESTAT::namq_10_gdp::*::FREQ=Q;GEO=DE,FR'
    """
    # Normalisation des dimensions en segments triés "clé=valeurs triées"
    dims_part = ""
    if dimensions:
        segments = []
        for key in sorted(dimensions, key=str):
            value = dimensions[key]
            values = [value] if isinstance(value, (str, int)) else list(value)
            values_str = ",".join(sorted(str(v) for v in values))
            segments.append(f"{key}={values_str}")
        dims_part = ";".join(segments)
    return f"{agency}::{dataflow}::{version}::{dims_part}"


# Classe spécifiant les types de versions de l'API SDMX
class SDMXVersion(str, Enum):
    """SDMX API version.

    Attributes:
        V1: OECD SDMX API v1 (legacy REST format).
        V2: OECD SDMX API v2 (current REST format).
        V2_1: SDMX 2.1 standard (used by Eurostat legacy endpoint).
        V3: SDMX 3.0 standard (used by Eurostat primary endpoint).
    """

    V1 = "v1"
    V2 = "v2"
    V2_1 = "v2_1"
    V3 = "v3"


# Classe spécifiant les valeurs possibles pour dimensionAtObservation
class DimensionAtObservation(str, Enum):
    """Dimension at observation level options.

    Attributes:
        ALL_DIMENSIONS: Flat representation of observations.
        TIME_PERIOD: Time series view with observations grouped by time.
    """

    ALL_DIMENSIONS = "AllDimensions"
    TIME_PERIOD = "TIME_PERIOD"


# Classe marker (sans membres) pour les enums de format de réponse SDMX
class SDMXResponseFormat(str, Enum):
    """Marker base class for provider-specific SDMX response-format enums.

    Subclassed (with no extra members on this base) by
    :class:`EurostatResponseFormat` and :class:`OECDResponseFormat`, so
    that provider-agnostic abstractions such as :class:`SDMXEndpointBuilder`
    can refer to response formats by a common static type.

    Note:
        A Python :class:`enum.Enum` can only be subclassed when it declares
        no members; this class is therefore intentionally empty. The actual
        format values (CSV, JSON, …) are defined in the provider-specific
        subclasses, where their string values map to the parameter values
        accepted by the corresponding API.
    """


# Énumération des types d'artefacts structurels SDMX interrogeables
class StructureResourceType(str, Enum):
    """SDMX structure resource types.

    Standard SDMX artefact types queryable via the structure endpoint.
    Applicable to any SDMX provider regardless of API version.

    Attributes:
        DATAFLOW: Dataflow definition.
        DATASTRUCTURE: Data Structure Definition (DSD).
        DATACONSTRAINT: Data constraint (valid dimension combinations).
        CONCEPTSCHEME: Concept scheme.
        CODELIST: Codelist (controlled vocabulary for a dimension).
    """

    DATAFLOW = "dataflow"
    DATASTRUCTURE = "datastructure"
    DATACONSTRAINT = "dataconstraint"
    CONCEPTSCHEME = "conceptscheme"
    CODELIST = "codelist"


# Classe abstraite de construction d'endpoints et de paramètres par version d'API
class SDMXEndpointBuilder(ABC):
    """Abstract base for version- and provider-specific URL construction.

    Subclasses implement the endpoint layout and query-parameter conventions
    for a given SDMX API version and provider.

    All method signatures span the **union** of parameters supported across
    SDMX versions and providers. Version-specific or provider-specific
    parameters that are not applicable to a given subclass are silently
    ignored by that subclass. Callers must always use keyword arguments so
    that optional parameters can be forwarded transparently.

    Examples:
        >>> builder = EurostatEndpointBuilderV30()
        >>> endpoint = builder.build_data_endpoint("nama_10_gdp", "ESTAT", "*")
        >>> params = builder.build_data_params(
        ...     start_period="2020",
        ...     dimensions={"GEO": ["FR", "DE"]},
        ... )
    """

    # Headers
    @abstractmethod
    def build_headers(
        self,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
        response_format: Optional["SDMXResponseFormat"] = None,
    ) -> Dict[str, str]:
        """Build HTTP request headers.

        Args:
            accept_encoding: Value for the ``Accept-Encoding`` header
                (e.g. ``"gzip, deflate"``).
            accept_language: Value for the ``Accept-Language`` header
                (e.g. ``"en"``).
            response_format: Desired response format, used by providers that
                communicate the format via the ``Accept`` header rather than a
                query parameter (e.g. OECD). Ignored by providers that use a
                fixed MIME type (e.g. Eurostat).

        Returns:
            HTTP headers dictionary including at minimum the ``Accept`` header.
        """

    # Data endpoints
    @abstractmethod
    def build_data_endpoint(
        self,
        dataflow: str,
        agency: str,
        version: str,
        key: Optional[str] = None,
    ) -> str:
        """Build the URL path for a data query.

        Args:
            dataflow: Dataflow identifier.
            agency: Maintaining agency.
            version: Dataflow version.
            key: Optional positional key for dimension filtering.
                Used by SDMX 2.1 (path-based filtering); typically unused
                in SDMX 3.0 where dimension filters are query parameters.

        Returns:
            URL path segment (without base URL).
        """

    # Data endpoints params
    @abstractmethod
    def build_data_params(
        self,
        *,
        # Commun à toutes les versions
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        # SDMX 3.0
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional["SDMXResponseFormat"] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        # SDMX 2.1
        dimension_at_observation: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build query parameters for a data request.

        All parameters are keyword-only to support transparent forwarding
        across versions without relying on positional ordering.

        Args:
            start_period: Start period filter.
            end_period: End period filter.
            last_n_observations: Number of most-recent observations to return.
            first_n_observations: Number of first observations to return.
            compress: Whether to request gzip compression.
            dimensions: Dimension filters as ``{name: [values]}``.
                Used by SDMX 3.0 (Eurostat). Ignored by SDMX 2.1 builders.
            response_format: Desired response format enum value.
            response_format_version: Format version string.
            lang: Language code for label localisation (e.g. ``"en"``).
            labels: Label display mode. SDMX 3.0 only.
            attributes: Attribute selection string.
            measures: Measure selection string.
            return_data: Return-data flag. SDMX 3.0 only.
            dimension_at_observation: Dimension serialised at observation
                level. SDMX 2.1 / OECD only.
            detail: Data detail level. SDMX 2.1 / OECD only.

        Returns:
            Query-parameter dictionary.
        """

    # Structure endpoints
    @abstractmethod
    def build_structure_endpoint(
        self,
        resource_type: "StructureResourceType",
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for a structure query.

        Args:
            resource_type: Type of structure artefact.
            resource_id: Artefact identifier, or ``"*"`` for all artefacts.
            agency: Maintaining agency, or ``"*"`` for all agencies.
            version: Artefact version. Use ``"+"`` for latest, ``"*"`` for
                all versions, or ``None`` to omit the version segment.

        Returns:
            URL path segment (without base URL).
        """

    # Structure endpoints params
    @abstractmethod
    def build_structure_params(
        self,
        references: Optional[str] = "none",
        detail: Optional[str] = "full",
        # SDMX 3.0 only
        format: Optional[str] = None,
        format_version: Optional[str] = None,
        compress: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build query parameters for a structure request.

        Args:
            references: Related artefacts to include (default: ``"none"``).
            detail: Level of detail (default: ``"full"``).
            format: Response format string. SDMX 3.0 only.
            format_version: Version of the response format. SDMX 3.0 only.
            compress: Whether to request gzip compression (``"true"`` /
                ``"false"``). SDMX 3.0 only.

        Returns:
            Query-parameter dictionary.
        """

