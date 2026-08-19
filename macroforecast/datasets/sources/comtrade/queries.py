"""UN Comtrade query request dataclass.

DTO encapsulating the parameters of a :meth:`ComtradeClient.get_data` call,
mirroring the SDMX provider query objects
(:class:`~macroforecast.datasets.sources.oecd.queries.OECDQueryRequest`,
:class:`~macroforecast.datasets.sources.eurostat.queries.EurostatQueryRequest`)
so the download orchestration can treat every provider uniformly.

UN Comtrade is not an SDMX provider: its data selection is expressed through
explicit fields (flows, reporters, partners, products, periods) rather than a
generic ``dimensions`` mapping. The :meth:`identity_key` override projects those
fields onto the canonical identity-key format shared with the SDMX DTOs.
"""
# Importation des modules
# Modules de base
from dataclasses import dataclass, field
from typing import ClassVar, List, Optional, Type, Union

# Modules du package
from ...core.queries import SDMXQueryRequest
from ...core.sdmx import SDMXResponseFormat, build_identity_key
from .formats import AGENCY_ID, ComtradeResponseFormat


# Classe représentant une requête de données tariffline UN Comtrade
@dataclass
class ComtradeQueryRequest(SDMXQueryRequest):
    """Query request for the UN Comtrade tariffline API.

    Attributes:
        dataflow: Logical dataflow identifier encoding the trade type,
            frequency and classification (e.g. ``"C_M_HS"`` for commodities,
            monthly, HS). Used as the DuckLake schema name and registry key.
        version: Dataflow version (``"*"`` for latest; Comtrade is unversioned
            but the field is kept for symmetry with the SDMX DTOs).
        flows: Trade flow codes (e.g. ``["M", "X"]``).
        reporters: Reporter country codes (M49 or ISO).
        partners: Partner country codes.
        partners2: Second partner / consignment codes.
        products: Commodity codes (HS).
        customs: Customs procedure codes.
        mot: Mode-of-transport codes.
        periods: Explicit periods (``YYYY`` or ``YYYYMM``).
        period_start: Start period (used when ``periods`` is not given).
        period_end: End period (used when ``periods`` is not given).
        type_code: Trade type (``"C"`` commodities or ``"S"`` services).
        classification: Classification code (``"HS"``, ``"SITC"``, …).
        frequency: Data frequency (``"annual"`` or ``"monthly"``).
        max_records: Maximum number of records returned per call.
        count_only: When ``True``, return only the record count.
        include_desc: Whether to include the variables' descriptions.
        format: Response format (default: JSON).

    Example:
        >>> query = ComtradeQueryRequest(
        ...     dataflow="C_A_HS",
        ...     reporters="FRA",
        ...     products=["010121"],
        ...     periods="2023",
        ...     frequency="annual",
        ... )
        >>> df = client.execute_query(query)  # doctest: +SKIP
    """
    # Identifiant logique du dataflow (typeCode_freqCode_clCode)
    dataflow: str
    version: str = "*"
    # Sélection des données (champs explicites propres à Comtrade)
    flows: Optional[Union[List[str], str]] = None
    reporters: Optional[Union[List[str], List[int], str, int]] = None
    partners: Optional[Union[List[str], List[int], str, int]] = None
    partners2: Optional[Union[List[str], List[int], str, int]] = None
    products: Optional[Union[List[str], List[int], str, int]] = None
    customs: Optional[Union[List[str], str]] = None
    mot: Optional[Union[List[str], str]] = None
    periods: Optional[Union[List[str], str]] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    # Paramètres de requête ancrés (défauts intelligents alignés sur get_data)
    type_code: str = "C"
    classification: str = "HS"
    frequency: str = "annual"
    max_records: Optional[int] = None
    count_only: Optional[bool] = None
    include_desc: bool = True
    format: ComtradeResponseFormat = ComtradeResponseFormat.JSON

    # Enum de format du provider (utilisé par SDMXQueryRequest.from_dict)
    _FORMAT_ENUM: ClassVar[Optional[Type[SDMXResponseFormat]]] = ComtradeResponseFormat

    # Propriété d'agence (Comtrade publie sous l'agence COMTRADE)
    @property
    def agency(self) -> str:
        """Maintaining agency identifier (always ``COMTRADE``).

        Exposed for symmetry with the SDMX DTOs so the download orchestration
        resolves the structure-registry key and DuckLake schema uniformly.

        Returns:
            The Comtrade agency identifier ``"COMTRADE"``.
        """
        return AGENCY_ID

    # Surcharge de la clé de dataflow (l'agence est toujours COMTRADE)
    def get_dataflow_key(self) -> str:
        """Get unique key for this dataflow.

        Returns:
            Key in format ``'dataflow::version'``.
        """
        return f"{self.dataflow}::{self.version}"

    # Surcharge de la clé d'identité (Comtrade n'utilise pas de dimensions SDMX)
    def identity_key(self) -> str:
        """Return a deterministic identity key for this query.

        Projects the explicit Comtrade selection fields (flows, reporters,
        partners, products, periods, frequency) onto the canonical
        ``agency::dataflow::version::dims`` format shared with the SDMX DTOs, so
        the download registry uses a single key layout across providers.

        Returns:
            Stable identity string.
        """
        # Projection des champs de sélection sous forme de pseudo-dimensions
        dimensions = {
            "flows": self.flows,
            "reporters": self.reporters,
            "partners": self.partners,
            "partners2": self.partners2,
            "products": self.products,
            "customs": self.customs,
            "mot": self.mot,
            "periods": self.periods,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "type_code": self.type_code,
            "classification": self.classification,
            "frequency": self.frequency,
            "max_records": self.max_records,
            "count_only": self.count_only,
            "include_desc": self.include_desc,
        }
        # Retrait des champs non renseignés pour une clé stable et compacte
        dimensions = {k: v for k, v in dimensions.items() if v is not None}
        return build_identity_key(
            self.agency, self.dataflow, self.version, dimensions
        )
