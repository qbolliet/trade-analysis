"""Eurostat SDMX endpoint builders (strategy per API version).

URL construction and query-parameter encoding for the Eurostat SDMX API.
:class:`EurostatEndpointBuilderV30` implements the SDMX 3.0 conventions
(primary endpoint) and :class:`EurostatEndpointBuilderV21` the SDMX 2.1
conventions (legacy endpoint, Comext datasets).
"""
# Importation des modules
from typing import Any, Dict, List, Optional

from ...core.sdmx import (
    SDMXEndpointBuilder,
    SDMXVersion,
    StructureResourceType,
)
from .formats import (
    DataDetail,
    EurostatResponseFormat,
    StructureCompress,
    StructureDetail,
    StructureReferences,
)


# Constructeur d'endpoints pour l'API SDMX 3.0 (version principale)
class EurostatEndpointBuilderV30(SDMXEndpointBuilder):
    """Endpoint builder for the Eurostat SDMX 3.0 API.

    Implements URL construction and query-parameter encoding following the
    SDMX 3.0 conventions used by the Eurostat dissemination endpoint.

    URL patterns:
        data:
            ``/sdmx/3.0/data/dataflow/{agency}/{resource}/{version}[/{key}]``
        structure:
            ``/sdmx/3.0/structure/{type}/{agency}/{resource}/{version}``

    Attributes:
        ACCEPT_HEADER: MIME type sent in the ``Accept`` request header.

    Note:
        Dimension filtering is performed via query parameters
        (``c[DIM]=val1,val2``) rather than the URL path key, which is the
        primary difference from the SDMX 2.1 approach.
    """

    # Adresse de base du header
    ACCEPT_HEADER = "application/vnd.sdmx.structure+xml;version=3.0.0"

    # Mapping des formats de réponse vers les valeurs de paramètre API
    _FORMAT_PARAM: Dict[EurostatResponseFormat, str] = {
        EurostatResponseFormat.CSV: "csvdata",
        EurostatResponseFormat.TSV: "tsv",
        EurostatResponseFormat.JSON: "json",
        EurostatResponseFormat.XML: "structurespecificdata",
    }

    # Construction des headers pour SDMX 3.0
    def build_headers(
        self,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
        response_format: Optional[Any] = None,
    ) -> Dict[str, str]:
        """Build HTTP request headers for SDMX 3.0.

        Args:
            accept_encoding: Value for the ``Accept-Encoding`` header
                (e.g. ``"gzip"``).
            accept_language: Value for the ``Accept-Language`` header
                (e.g. ``"en"``).
            response_format: Ignored — Eurostat uses a fixed MIME type.

        Returns:
            HTTP headers dictionary with at minimum the ``Accept`` header
            set to the SDMX 3.0 structure MIME type.
        """
        # Initialisation du dictionnaire des headers
        headers = {"Accept": self.ACCEPT_HEADER}
        # Ajout des clés si spécifiées
        if accept_encoding is not None:
            headers["Accept-Encoding"] = accept_encoding
        if accept_language is not None:
            headers["Accept-Language"] = accept_language
        return headers

    # Construction de l'endpoint de données SDMX 3.0
    def build_data_endpoint(
        self,
        dataflow: str,
        agency: str,
        version: str,
        key: Optional[str] = None,
    ) -> str:
        """Build the URL path for an SDMX 3.0 data query.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx3-0/data-query#APIDetailedguidelinesSDMX3.0APIdataquery-Overview

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%203.0%20Data%20queries/get_sdmx_3_0_data_dataflow__agencyID___resourceID___version___key_

        Args:
            dataflow: Dataflow identifier (e.g. ``"namq_10_gdp"``).
            agency: Maintaining agency (e.g. ``"ESTAT"``).
            version: Dataflow version.  Use ``"*"`` for the latest version.
            key: Optional positional key for dimension filtering.
                When provided, appended as a trailing path segment.
                Not commonly used in SDMX 3.0 (prefer query-parameter
                filtering via :meth:`build_data_params`).

        Returns:
            URL path segment (without base URL).
        """
        # Construction du path de base
        path = f"/sdmx/3.0/data/dataflow/{agency}/{dataflow}/{version}"
        # Ajout de la clé si spécifiée
        if key is not None:
            path += f"/{key}"
        return path

    # Construction des paramètres de requête de données SDMX 3.0
    def build_data_params(
        self,
        *,
        # Commun aux deux versions
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        # Spécifique SDMX 3.0
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional[EurostatResponseFormat] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        # Spécifique SDMX 2.1 (ignoré par ce builder)
        dimension_at_observation: Optional[str] = None,
        detail: Optional[DataDetail] = None,
    ) -> Dict[str, str]:
        """Build query parameters for an SDMX 3.0 data request.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx3-0/data-query#APIDetailedguidelinesSDMX3.0APIdataquery-Overview

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%203.0%20Data%20queries/get_sdmx_3_0_data_dataflow__agencyID___resourceID___version___key_

        Args:
            start_period: Start period filter encoded as
                ``ge:<value>`` in ``c[TIME_PERIOD]``.
            end_period: End period filter encoded as
                ``le:<value>`` in ``c[TIME_PERIOD]``.
            last_n_observations: Number of most-recent observations.
            first_n_observations: Number of first observations.
            compress: Whether to request gzip compression via the
                ``compress`` query parameter.
            dimensions: Dimension filters as ``{name: [values]}``.
                Encoded as ``c[DIM]=val1,val2`` query parameters.
            response_format: Desired response format.  Mapped to the
                ``format`` query parameter via :attr:`_FORMAT_PARAM`.
            response_format_version: Format version string (``formatVersion``
                parameter, e.g. ``"1.0"`` for SDMX-CSV 1.0).
            lang: Language code for label localisation (``lang`` parameter,
                e.g. ``"en"``).
            labels: Label display mode (``labels`` parameter,
                e.g. ``"name"``).
            attributes: Attribute selection string (``attributes``
                parameter, e.g. ``"dsd"``, ``"none"``).
            measures: Measure selection string (``measures`` parameter).
            return_data: Return-data flag (``returnData`` parameter).
            dimension_at_observation: Ignored — SDMX 2.1 only.
            detail: Ignored — SDMX 2.1 only.

        Returns:
            Query-parameter dictionary suitable for use as ``params`` in
            an HTTP GET request.
        """
        # Initialisation du dictionnaire de paramètres
        params: Dict[str, str] = {}

        # Filtres de dimensions (c[DIM]=val1,val2)
        if dimensions:
            for dim_name, dim_values in dimensions.items():
                params[f"c[{dim_name.upper()}]"] = ",".join(dim_values)

        # Filtre de période temporelle (c[TIME_PERIOD]=ge:...+le:...)
        time_parts: list[str] = []
        if start_period:
            time_parts.append(f"ge:{start_period}")
        if end_period:
            time_parts.append(f"le:{end_period}")
        if time_parts:
            params["c[TIME_PERIOD]"] = "+".join(time_parts)

        # Paramètres d'observations
        if last_n_observations is not None:
            params["lastNObservations"] = str(last_n_observations)
        if first_n_observations is not None:
            params["firstNObservations"] = str(first_n_observations)

        # Attributs et mesures optionnels
        if attributes:
            params["attributes"] = attributes
        if measures:
            params["measures"] = measures

        # Format et version de format
        if response_format:
            params["format"] = self._FORMAT_PARAM[response_format]
        if response_format_version:
            params["formatVersion"] = response_format_version

        # Langue
        if lang:
            params["lang"] = lang

        # Labels
        if labels is not None:
            params["labels"] = labels

        # Compression
        params["compress"] = "true" if compress else "false"

        # Données
        if return_data:
            params["returnData"] = return_data

        return params

    # Construction de l'endpoint de structure SDMX 3.0
    def build_structure_endpoint(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for an SDMX 3.0 structure query.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx3-0/structure-queries

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%203.0%20Structure%20queries/get_sdmx_3_0_structure_dataflow__agencyID___resourceID_

        Args:
            resource_type: Type of structure artefact (e.g.
                :attr:`StructureResourceType.DATAFLOW`).
            resource_id: Artefact identifier, or ``"*"`` for all artefacts
                of the given type.
            agency: Maintaining agency, or ``"*"`` for all agencies.
            version: Artefact version.  Use ``"+"`` or ``"~"`` for the
                latest version, ``"*"`` for all versions, or ``None`` to omit
                the version segment.  Note: the bulk / metadata-harvesting
                special case (``resource_id="*"``) requires ``"*"`` here;
                omitting the segment or using ``"+"`` returns an empty
                container with a misleading HTTP 200.

        Returns:
            URL path segment (without base URL).
        """
        # Base path sans version
        path = f"/sdmx/3.0/structure/{resource_type.value}/{agency}/{resource_id}"
        # Ajout du segment de version uniquement si spécifié
        if version is not None:
            path += f"/{version}"
        return path

    # Construction des paramètres de requête de structure SDMX 3.0
    def build_structure_params(
        self,
        references: Optional[StructureReferences] = "none",
        detail: Optional[StructureDetail] = "full",
        format: Optional[str] = "structure",
        format_version: Optional[str] = "3.0",
        compress: Optional[StructureCompress] = "true",
    ) -> Dict[str, str]:
        """Build query parameters for an SDMX 3.0 structure request.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx3-0/structure-queries

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%203.0%20Structure%20queries/get_sdmx_3_0_structure_dataflow__agencyID___resourceID_

        Args:
            references: Related artefacts to embed in the response
                (default: ``"none"``).
            detail: Level of detail for each returned artefact
                (default: ``"full"``).
            format: Response format identifier (default: ``"structure"``).
            format_version: Version of the response format
                (default: ``"3.0"``).
            compress: Whether to request gzip compression of the response.
                Defaults to ``"true"`` because structure responses can be
                large; the client transparently decompresses the result.

        Returns:
            Query-parameter dictionary.
        """
        # Initialisation du dictionnaire des paramètres
        params: Dict[str, str] = {}

        # Ajout des clés quand elles sont non nulles
        if references is not None:
            params["references"] = references
        if detail is not None:
            params["detail"] = detail
        if format is not None:
            params["format"] = format
        if format_version is not None:
            params["formatVersion"] = format_version
        if compress is not None:
            params["compress"] = compress

        return params


# Constructeur d'endpoints pour l'API SDMX 2.1 (version legacy)
class EurostatEndpointBuilderV21(SDMXEndpointBuilder):
    """Endpoint builder for the Eurostat SDMX 2.1 API.

    Implements URL construction and query-parameter encoding following the
    SDMX 2.1 conventions.  This builder is kept for compatibility testing
    and for accessing Comext datasets that are not yet available on the
    SDMX 3.0 endpoint.

    URL patterns:
        data:
            ``/sdmx/2.1/data/{flow}[,{agency}[,{version}]]/{key}``
        structure:
            ``/sdmx/2.1/{type}/{agency}/{resource}/{version}``

    Attributes:
        ACCEPT_HEADER: MIME type sent in the ``Accept`` request header.

    Note:
        Dimension filtering uses a **positional key** embedded in the URL
        path (``/key`` segment), not query parameters.  The ``dimensions``
        and ``response_format`` parameters of :meth:`build_data_params`
        are accepted for interface compatibility but silently ignored.

        The wildcard tokens differ from SDMX 3.0:

        - ``"*"`` (all versions / all resources) → ``"all"`` in paths
        - ``"+"`` (latest version) → ``"latest"`` in paths
        - ``"dataconstraint"`` resource type → ``"contentconstraint"``
    """

    # Adresse de base du header
    ACCEPT_HEADER = "application/vnd.sdmx.structure+xml;version=2.1"

    # Mapping des formats de réponse vers les valeurs de paramètre API 2.1
    _FORMAT_PARAM: Dict[EurostatResponseFormat, str] = {
        EurostatResponseFormat.CSV: "SDMX-CSV",
        EurostatResponseFormat.TSV: "TSV",
        EurostatResponseFormat.JSON: "JSON",
        EurostatResponseFormat.XML: "SDMX_2.1_STRUCTURED",
    }

    # Mapping des types de structure 3.0 vers les types 2.1
    _RESOURCE_MAP: Dict[StructureResourceType, str] = {
        StructureResourceType.DATAFLOW: "dataflow",
        StructureResourceType.DATASTRUCTURE: "datastructure",
        StructureResourceType.DATACONSTRAINT: "contentconstraint",
        StructureResourceType.CONCEPTSCHEME: "conceptscheme",
        StructureResourceType.CODELIST: "codelist",
    }

    # Construction des headers pour SDMX 2.1
    def build_headers(
        self,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
        response_format: Optional[Any] = None,
    ) -> Dict[str, str]:
        """Build HTTP request headers for SDMX 2.1.

        Args:
            accept_encoding: Value for the ``Accept-Encoding`` header
                (e.g. ``"gzip"``).
            accept_language: Value for the ``Accept-Language`` header
                (e.g. ``"en"``).
            response_format: Ignored — Eurostat uses a fixed MIME type.

        Returns:
            HTTP headers dictionary with at minimum the ``Accept`` header
            set to the SDMX 2.1 structure MIME type.
        """
        # Initialisation du dictionnaire des headers
        headers = {"Accept": self.ACCEPT_HEADER}
        # Ajout des clés si spécifiées
        if accept_encoding is not None:
            headers["Accept-Encoding"] = accept_encoding
        if accept_language is not None:
            headers["Accept-Language"] = accept_language
        return headers

    # Construction de l'endpoint de données SDMX 2.1
    def build_data_endpoint(
        self,
        dataflow: str,
        agency: Optional[str],
        version: Optional[str],
        key: Optional[str] = "all",
    ) -> str:
        """Build the URL path for an SDMX 2.1 data query.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/data-query

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%202.1%20Data%20queries/get_sdmx_2_1_data__flow___key_

        In SDMX 2.1 the *flow* path parameter combines agency, dataflow ID
        and version in a single compound token:

        Examples::

            EXR               → dataflow ID only
            ECB,EXR           → agency + dataflow ID
            ECB,EXR,1.0       → agency + dataflow ID + version

        Args:
            dataflow: Dataflow identifier (e.g. ``"namq_10_gdp"``).
            agency: Maintaining agency.  When provided, prepended to the
                flow token (e.g. ``"ESTAT"``).
            version: Dataflow version.  When provided, appended to the
                flow token.
            key: Positional key for dimension filtering (default:
                ``"all"`` — no filtering).  Individual dimension values
                are separated by ``"."`` and multiple values within a
                dimension by ``"+"``.

        Returns:
            URL path segment (without base URL).
        """
        # Construction du flow composé
        flow = dataflow
        # Ajout de l'agency si précisé
        if agency is not None:
            flow = f"{agency}," + flow
        # Ajout de la version si spécifiée
        if version is not None:
            flow = flow + f",{version}"
        return f"/sdmx/2.1/data/{flow}/{key}"

    # Construction des paramètres de requête de données SDMX 2.1
    def build_data_params(
        self,
        *,
        # Commun aux deux versions
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        # Spécifique SDMX 3.0 (ignoré par ce builder)
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional[EurostatResponseFormat] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        # Spécifique SDMX 2.1
        dimension_at_observation: Optional[str] = None,
        detail: Optional[DataDetail] = None,
    ) -> Dict[str, str]:
        """Build query parameters for an SDMX 2.1 data request.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/data-query

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%202.1%20Data%20queries/get_sdmx_2_1_data__flow___key_

        Args:
            start_period: Start period filter (``startPeriod`` parameter).
            end_period: End period filter (``endPeriod`` parameter).
            last_n_observations: Number of most-recent observations
                (``lastNObservations`` parameter).
            first_n_observations: Number of first observations
                (``firstNObservations`` parameter).
            compress: Whether to request gzip compression
                (``compressed`` parameter).
            dimensions: Ignored — SDMX 3.0 only (use ``key`` in
                :meth:`build_data_endpoint` for 2.1 filtering).
            response_format: Ignored — SDMX 3.0 only.
            response_format_version: Ignored — SDMX 3.0 only.
            lang: Ignored — SDMX 3.0 only.
            labels: Ignored — SDMX 3.0 only.
            attributes: Ignored — SDMX 3.0 only.
            measures: Ignored — SDMX 3.0 only.
            return_data: Ignored — SDMX 3.0 only.
            dimension_at_observation: Dimension serialised at observation
                level (``dimensionAtObservation`` parameter,
                e.g. ``"AllDimensions"``).
            detail: Data detail level (``detail`` parameter,
                e.g. ``"full"``, ``"dataonly"``).

        Returns:
            Query-parameter dictionary.
        """
        # Initialisation du dictionnaire de paramètres
        params: Dict[str, str] = {}

        # Période temporelle (startPeriod / endPeriod)
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        # Paramètres d'observations
        if first_n_observations is not None:
            params["firstNObservations"] = str(first_n_observations)
        if last_n_observations is not None:
            params["lastNObservations"] = str(last_n_observations)

        # Paramètres spécifiques 2.1
        if dimension_at_observation:
            params["dimensionAtObservation"] = dimension_at_observation
        if detail:
            params["detail"] = detail

        # Compression (paramètre 2.1 : "compressed")
        params["compressed"] = "true" if compress else "false"

        return params

    # Construction de l'endpoint de structure SDMX 2.1
    def build_structure_endpoint(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for an SDMX 2.1 structure query.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/structure-queries#APIDetailedguidelinesSDMX2.1APIstructurequeries-Multiplevaluesandwildcardvaluesupport

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%202.1%20Structure%20queries/get_sdmx_2_1_dataflow__agencyID___resourceID___version_

        Wildcard token mapping (SDMX 3.0 → SDMX 2.1):

        - resource_id ``"*"`` → ``"all"``
        - agency ``"*"`` → ``"all"``
        - version ``"+"`` or ``"~"`` or ``None`` → ``"latest"``
        - version ``"*"`` → ``"all"``

        Args:
            resource_type: Type of structure artefact.  ``DATACONSTRAINT``
                is mapped to ``contentconstraint``.
            resource_id: Artefact identifier, or ``"*"`` for all artefacts
                (mapped to ``"all"``).
            agency: Maintaining agency, or ``"*"`` for all agencies
                (mapped to ``"all"``).
            version: Artefact version.  Use ``"+"`` or ``"~"`` for the
                latest version; ``"*"`` for all versions; ``None`` defaults
                to ``"latest"``.

        Returns:
            URL path segment (without base URL).
        """
        # Conversion du type de ressource vers la terminologie 2.1
        mapped_type = self._RESOURCE_MAP[resource_type]

        # Conversion des tokens de version vers les équivalents 2.1 (None → "latest")
        v21_version = (
            "latest" if version in ("+", "~", None)
            else ("all" if version == "*" else version)
        )

        # Conversion des wildcards d'agence et de ressource vers 2.1
        v21_agency = "all" if agency == "*" else agency
        v21_resource_id = "all" if resource_id == "*" else resource_id

        return (
            f"/sdmx/2.1/{mapped_type}"
            f"/{v21_agency}/{v21_resource_id}/{v21_version}"
        )

    # Construction des paramètres de requête de structure SDMX 2.1
    def build_structure_params(
        self,
        references: Optional[StructureReferences] = "none",
        detail: Optional[StructureDetail] = "full",
        # Spécifique SDMX 3.0 (ignoré par ce builder)
        format: Optional[str] = None,
        format_version: Optional[str] = None,
        compress: Optional[StructureCompress] = None,
    ) -> Dict[str, str]:
        """Build query parameters for an SDMX 2.1 structure request.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/structure-queries#APIDetailedguidelinesSDMX2.1APIstructurequeries-Multiplevaluesandwildcardvaluesupport

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%202.1%20Structure%20queries/get_sdmx_2_1_dataflow__agencyID___resourceID___version_

        Args:
            references: Related artefacts to embed in the response
                (default: ``"none"``).
            detail: Level of detail for each returned artefact
                (default: ``"full"``).
            format: Ignored — SDMX 3.0 only.
            format_version: Ignored — SDMX 3.0 only.
            compress: Ignored — SDMX 2.1 does not support this parameter.

        Returns:
            Query-parameter dictionary.
        """
        # Initialisation du dictionnaire de paramètres
        params: Dict[str, str] = {}
        # Ajout des clés supportées par l'API 2.1
        if detail is not None:
            params["detail"] = detail
        if references is not None:
            params["references"] = references
        return params


# Registre des builders par version d'API
_ENDPOINT_BUILDERS: Dict[SDMXVersion, SDMXEndpointBuilder] = {
    SDMXVersion.V3: EurostatEndpointBuilderV30(),
    SDMXVersion.V2_1: EurostatEndpointBuilderV21(),
}
