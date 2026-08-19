"""OECD SDMX endpoint builders.

Version-specific URL construction and query-parameter encoding for the OECD
SDMX REST API. The :class:`OECDEndpointBuilder` base implements the logic
shared by both API versions; :class:`OECDEndpointBuilderV1` and
:class:`OECDEndpointBuilderV2` implement the version-specific URL paths,
query parameters and positional dimension key.
"""
# Importation des modules
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from ...core.sdmx import (
    DimensionAtObservation,
    SDMXEndpointBuilder,
    SDMXVersion,
)
from .formats import OECDResponseFormat, _OECD_FORMAT_PARAM_MAP


# Classe de base abstraite pour les endpoint builders OCDE
class OECDEndpointBuilder(SDMXEndpointBuilder):
    """Base endpoint builder for the OECD SDMX API.

    Common logic (HTTP headers, Accept MIME types, structure query
    parameters) is implemented here; URL paths, query parameters and the
    positional dimension key are version-specific and implemented by the
    :class:`OECDEndpointBuilderV1` and :class:`OECDEndpointBuilderV2`
    subclasses.

    Attributes:
        sdmx_version: OECD SDMX API version associated with this builder.

    Note:
        Do not instantiate this base class directly — use the registry
        :data:`_OECD_ENDPOINT_BUILDERS` or the V1/V2 subclasses.
    """

    # Version d'API associée au builder (surchargée par les sous-classes)
    sdmx_version: SDMXVersion

    # Construction des headers HTTP (Accept basé sur le format et la version)
    def build_headers(
        self,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
        response_format: Optional[OECDResponseFormat] = None,
    ) -> Dict[str, str]:
        """Build HTTP request headers for the OECD API.

        Args:
            accept_encoding: Value for the ``Accept-Encoding`` header.
                Defaults to ``"gzip, deflate"`` when ``None``.
            accept_language: Value for the ``Accept-Language`` header.
            response_format: Desired response format. Controls the
                ``Accept`` header value.

        Returns:
            HTTP headers dictionary.
        """
        fmt = response_format or OECDResponseFormat.CSV_LABELS
        headers: Dict[str, str] = {
            "Accept": self.get_accept_header(fmt, self.sdmx_version),
            "Accept-Encoding": accept_encoding or "gzip, deflate",
        }
        if accept_language is not None:
            headers["Accept-Language"] = accept_language
        return headers

    # Construction des paramètres de requête de structure (identiques v1/v2)
    def build_structure_params(
        self,
        references: Optional[str] = "all",
        detail: Optional[str] = "referencepartial",
        format: Optional[str] = None,
        format_version: Optional[str] = None,
        compress: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build query parameters for an OECD structure request.

        Args:
            references: Related artefacts to embed (default: ``"all"``).
            detail: Level of detail (default: ``"referencepartial"``).
            format: Ignored — OECD uses the ``Accept`` header.
            format_version: Ignored.
            compress: Ignored.

        Returns:
            Query-parameter dictionary.
        """
        params: Dict[str, str] = {}
        if references is not None:
            params["references"] = references
        if detail is not None:
            params["detail"] = detail
        return params

    @staticmethod
    def get_accept_header(
        format: OECDResponseFormat,
        version: SDMXVersion,
    ) -> str:
        """Get appropriate Accept header for a data query format and SDMX version.

        Args:
            format: Desired response format.
            version: SDMX API version.

        Returns:
            ``Accept`` header value string.
        """
        if format == OECDResponseFormat.JSON:
            if version == SDMXVersion.V2:
                return "application/vnd.sdmx.data+json; charset=utf-8; version=2"
            return "application/vnd.sdmx.data+json; charset=utf-8; version=1.0"
        elif format in (OECDResponseFormat.CSV, OECDResponseFormat.CSV_LABELS):
            if version == SDMXVersion.V2:
                return "application/vnd.sdmx.data+csv; charset=utf-8; version=2"
            return "application/vnd.sdmx.data+csv; charset=utf-8"
        elif format == OECDResponseFormat.XML:
            return "application/vnd.sdmx.structurespecificdata+xml; charset=utf-8; version=2.1"
        return "application/json"

    @staticmethod
    def get_structure_accept_header() -> str:
        """Get the Accept header for an OECD structure JSON query.

        Returns:
            ``Accept`` header value string for the structure endpoint.
        """
        # Format SDMX-JSON de structure : seule la version 1.0 est proposée par
        # l'OCDE (cf. liste des types acceptés renvoyée dans les réponses 406),
        # indépendamment de la version v1/v2 de l'API REST (qui ne concerne que
        # le chemin d'URL). Demander version=2 provoque une erreur 406.
        return "application/vnd.sdmx.structure+json; charset=utf-8; version=1.0"

    # ── Méthodes spécifiques à la version, à implémenter par les sous-classes ──

    @staticmethod
    def build_dimension_filter(
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int] = None,
    ) -> str:
        """Build the positional dimension key string for the data URL.

        Implemented by version-specific subclasses.

        Args:
            dimensions: Dimension position → list of values.
            num_dimensions: Total dimension count (for wildcard padding).

        Returns:
            Dimension filter string (e.g., ``"FRA.M.LI"``).
        """
        raise NotImplementedError


# Builder concret pour l'API OCDE SDMX v1 (legacy REST)
class OECDEndpointBuilderV1(OECDEndpointBuilder):
    """Endpoint builder for the OECD SDMX v1 API (legacy REST format).

    URL patterns:
        data: ``/data/{agency},{dataflow},{version}/{key}``
        structure: ``/dataflow/{agency}/{dataflow}/{version}``

    The positional dimension key supports comma-separated multi-values
    inside a dimension (``"FRA,DEU.M.LI"``).
    """

    sdmx_version = SDMXVersion.V1

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
            agency: Agency identifier.
            version: Dataflow version.
            key: Pre-built positional dimension filter, or ``None`` for the
                ``"all"`` wildcard.

        Returns:
            URL path segment.
        """
        dim_filter = key or "all"
        return f"data/{agency},{dataflow},{version}/{dim_filter}"

    def build_data_params(
        self,
        *,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional[OECDResponseFormat] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        dimension_at_observation: Optional[str] = None,
        detail: Optional[str] = None,
        updated_after: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """Build query parameters for an OECD v1 data request.

        Recognised parameters: ``start_period``, ``end_period``,
        ``last_n_observations``, ``response_format``,
        ``dimension_at_observation``. Other arguments (including
        ``updated_after``, which is SDMX-CSV v2 only) are accepted for
        interface compatibility and silently ignored.

        Returns:
            Query-parameter dictionary.
        """
        fmt = response_format or OECDResponseFormat.CSV_LABELS
        params: Dict[str, Any] = {
            "dimensionAtObservation": (
                dimension_at_observation
                or DimensionAtObservation.ALL_DIMENSIONS.value
            ),
            "format": _OECD_FORMAT_PARAM_MAP[fmt],
        }
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if last_n_observations:
            params["lastNObservations"] = last_n_observations
        return params

    def build_structure_endpoint(
        self,
        resource_type: Any,
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for a structure query.

        Args:
            resource_type: Ignored — OECD v1 structure queries use a fixed
                ``dataflow`` path format.
            resource_id: Dataflow identifier.
            agency: Agency identifier.
            version: Dataflow version (defaults to ``"+"``).

        Returns:
            URL path segment.
        """
        v = version or "+"
        return f"dataflow/{agency}/{resource_id}/{v}"

    @staticmethod
    def build_dimension_filter(
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int] = None,
    ) -> str:
        """Build the positional dimension filter string for SDMX v1.

        Format: ``value1,value2.value3.value4`` (comma-separated multi-values
        within a dimension, dot-separated dimensions).

        Args:
            dimensions: Dimension position → list of values.
            num_dimensions: Total dimension count (for wildcard padding).

        Returns:
            Dimension filter string.
        """
        if not dimensions:
            if num_dimensions:
                return ".".join([""] * num_dimensions)
            return "all"
        max_dim = max(dimensions.keys())
        total_dims = num_dimensions if num_dimensions else max_dim + 1
        parts = [",".join(dimensions[i]) if i in dimensions else "" for i in range(total_dims)]
        return ".".join(parts)


# Builder concret pour l'API OCDE SDMX v2 (current REST)
class OECDEndpointBuilderV2(OECDEndpointBuilder):
    """Endpoint builder for the OECD SDMX v2 API (current REST format).

    URL patterns:
        data: ``/v2/data/dataflow/{agency}/{dataflow}/{version}/{key}``
        structure: ``/v2/structure/dataflow/{agency}/{dataflow}/{version}``

    SDMX v2 does not support comma-separated multi-values in the positional
    key. Use the ``split_dimensions`` parameter of
    :meth:`OECDClient.get_data` to iterate on multi-value dimensions.
    """

    sdmx_version = SDMXVersion.V2

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
            agency: Agency identifier.
            version: Dataflow version.
            key: Pre-built positional dimension filter, or ``None`` for the
                ``"*"`` wildcard.

        Returns:
            URL path segment.
        """
        dim_filter = key or "*"
        return f"v2/data/dataflow/{agency}/{dataflow}/{version}/{dim_filter}"

    def build_data_params(
        self,
        *,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional[OECDResponseFormat] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        dimension_at_observation: Optional[str] = None,
        detail: Optional[str] = None,
        updated_after: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """Build query parameters for an OECD v2 data request.

        v2 encodes the time-period filter via ``c[TIME_PERIOD]=ge:…+le:…``
        rather than ``startPeriod``/``endPeriod``, and additionally supports
        ``attributes`` and ``measures`` query parameters.

        Args:
            updated_after: Threshold instant, as an ISO-8601 dateTime string
                (with timezone) or a ``datetime`` object. When set, the
                response only includes observations inserted, updated or
                deleted since that instant (SDMX-CSV v2 ``updatedAfter``).
                A ``datetime`` is converted to ISO-8601: naive datetimes are
                interpreted as UTC (``…Z`` suffix), aware datetimes keep their
                offset.

        Returns:
            Query-parameter dictionary.
        """
        fmt = response_format or OECDResponseFormat.CSV_LABELS
        params: Dict[str, Any] = {
            "dimensionAtObservation": (
                dimension_at_observation
                or DimensionAtObservation.ALL_DIMENSIONS.value
            ),
            "format": _OECD_FORMAT_PARAM_MAP[fmt],
        }
        # Filtre temporel encodé dans c[TIME_PERIOD]
        if start_period and end_period:
            params["c[TIME_PERIOD]"] = f"ge:{start_period}+le:{end_period}"
        elif start_period:
            params["c[TIME_PERIOD]"] = f"ge:{start_period}"
        elif end_period:
            params["c[TIME_PERIOD]"] = f"le:{end_period}"
        if attributes:
            params["attributes"] = attributes
        if measures:
            params["measures"] = measures
        if last_n_observations:
            params["lastNObservations"] = last_n_observations
        # Synchronisation incrémentale : seules les observations modifiées depuis
        # cet instant sont renvoyées (SDMX-CSV v2 uniquement)
        if updated_after:
            # Conversion d'un datetime au format dateTime ISO-8601 attendu : un
            # datetime naïf est interprété comme UTC (suffixe "Z"), un datetime
            # avec fuseau conserve son offset. Une chaîne est utilisée telle quelle.
            if isinstance(updated_after, datetime):
                if updated_after.tzinfo is None:
                    updated_after = updated_after.strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    updated_after = updated_after.isoformat()
            params["updatedAfter"] = updated_after
        return params

    def build_structure_endpoint(
        self,
        resource_type: Any,
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for a structure query.

        Args:
            resource_type: Ignored — OECD v2 structure queries use a fixed
                ``dataflow`` path format.
            resource_id: Dataflow identifier.
            agency: Agency identifier.
            version: Dataflow version (defaults to ``"+"``).

        Returns:
            URL path segment.
        """
        v = version or "+"
        return f"v2/structure/dataflow/{agency}/{resource_id}/{v}"

    @staticmethod
    def build_dimension_filter(
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int] = None,
    ) -> str:
        """Build the positional dimension filter string for SDMX v2.

        Each dimension must carry exactly one value or be a wildcard
        (``"*"``).

        Args:
            dimensions: Dimension position → list of values (each list must
                contain exactly one element).
            num_dimensions: Total dimension count (for wildcard padding).

        Returns:
            Dimension filter string.

        Raises:
            ValueError: If any dimension has more than one value.
        """
        if not dimensions:
            if num_dimensions:
                return ".".join(["*"] * num_dimensions)
            return "*"
        for position, values in dimensions.items():
            if len(values) > 1:
                raise ValueError(
                    f"SDMX v2 does not support multiple values for a single dimension. "
                    f"Dimension at position {position} has {len(values)} values: {values}. "
                    f"Use split_dimensions parameter in get_data() to handle multiple values."
                )
        max_dim = max(dimensions.keys())
        total_dims = num_dimensions if num_dimensions else max_dim + 1
        parts = [dimensions[i][0] if i in dimensions else "*" for i in range(total_dims)]
        return ".".join(parts)


# Registre des builders par version d'API (à l'image du registre Eurostat)
_OECD_ENDPOINT_BUILDERS: Dict[SDMXVersion, OECDEndpointBuilder] = {
    SDMXVersion.V1: OECDEndpointBuilderV1(),
    SDMXVersion.V2: OECDEndpointBuilderV2(),
}
