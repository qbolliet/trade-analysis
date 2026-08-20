"""UNSD correspondence request dataclass.

DTO encapsulating the parameters of a
:meth:`UNSDClient.get_correspondence` call, mirroring the SDMX provider query
objects
(:class:`~macroforecast.datasets.sources.oecd.queries.OECDQueryRequest`,
:class:`~macroforecast.datasets.sources.eurostat.queries.EurostatQueryRequest`)
so the pipeline can treat every provider uniformly.

UNSD is not an SDMX provider: a correspondence table is fully identified by a
source vintage, a target vintage and a kind of sheet, rather than by a generic
``dimensions`` mapping. ``dataflow`` is therefore derived from the vintage pair
— it is the ``TABLES`` catalogue key of ``parameters/unsd.json`` — and the
:meth:`identity_key` override projects the selection fields onto the canonical
identity-key format shared with the SDMX DTOs.
"""
# Importation des modules
# Modules de base
from dataclasses import dataclass
from typing import ClassVar, Optional, Type

# Modules du package
from ...core.queries import SDMXQueryRequest
from ...core.sdmx import SDMXResponseFormat, build_identity_key
from .formats import AGENCY_ID, UNSDResponseFormat, table_key


# Classe représentant une requête de table de correspondance UNSD
@dataclass
class UNSDCorrespondenceRequest(SDMXQueryRequest):
    """Query request for a UNSD classification correspondence table.

    Attributes:
        source: Source classification, the vintage being converted *from*
            (e.g. ``"HS2022"``).
        target: Target classification, the vintage being converted *to*
            (e.g. ``"HS2017"``). Only downward pairs are published for homogenous classfications.
        kind: Sheet to read. ``"conversion"`` (default) returns the functional
            correspondence, one row per source code — the one to convert data
            with. ``"correlation"`` returns the complete annotated
            correspondence, kept as a diagnostic of the information lost by the
            conversion.
        version: Table version (``"*"`` for latest; UNSD publishes unversioned
            files but the field is kept for symmetry with the SDMX DTOs).
        format: Published file format (default: XLSX). Informational: the
            actual extension is read from the catalogue.

    Example:
        >>> query = UNSDCorrespondenceRequest(source="HS2022", target="HS2017")
        >>> query.dataflow
        'HS2022-HS2017'
        >>> df_table = client.execute_query(query)  # doctest: +SKIP
    """
    # Sélection de la table (champs explicites propres à UNSD)
    source: str
    target: str
    kind: str = "conversion"
    version: str = "*"
    format: UNSDResponseFormat = UNSDResponseFormat.XLSX

    # Enum de format du provider (utilisé par SDMXQueryRequest.from_dict)
    _FORMAT_ENUM: ClassVar[Optional[Type[SDMXResponseFormat]]] = UNSDResponseFormat

    # Propriété d'agence (UNSD publie sous l'agence UNSD)
    @property
    def agency(self) -> str:
        """Maintaining agency identifier (always ``UNSD``).

        Exposed for symmetry with the SDMX DTOs so the orchestration resolves
        the registry key uniformly across providers.

        Returns:
            The UNSD agency identifier ``"UNSD"``.
        """
        return AGENCY_ID

    # Propriété de dataflow (clé du catalogue TABLES, dérivée de la paire)
    @property
    def dataflow(self) -> str:
        """Logical table identifier, the ``TABLES`` catalogue key.

        Derived from the vintage pair rather than stored, so it can never drift
        from ``source`` and ``target``.

        Returns:
            The catalogue key, e.g. ``"HS2022-HS2017"``.
        """
        return table_key(self.source, self.target)

    # Surcharge de la clé de dataflow (l'agence est toujours UNSD)
    def get_dataflow_key(self) -> str:
        """Get unique key for this table.

        Returns:
            Key in format ``'dataflow::version'``.
        """
        return f"{self.dataflow}::{self.version}"

    # Surcharge de la clé d'identité (UNSD n'utilise pas de dimensions SDMX)
    def identity_key(self) -> str:
        """Return a deterministic identity key for this query.

        Projects the explicit UNSD selection fields onto the canonical
        ``agency::dataflow::version::dims`` format shared with the SDMX DTOs, so
        a single key layout is used across providers.

        Returns:
            Stable identity string.

        Examples:
            >>> UNSDCorrespondenceRequest("HS2022", "HS2017").identity_key()
            'UNSD::HS2022-HS2017::*::kind=conversion;source=HS2022;target=HS2017'
        """
        # Projection des champs de sélection sous forme de pseudo-dimensions
        dimensions = {
            "source": self.source,
            "target": self.target,
            "kind": self.kind,
        }
        # Retrait des champs non renseignés pour une clé stable et compacte
        dimensions = {k: v for k, v in dimensions.items() if v is not None}
        return build_identity_key(
            self.agency, self.dataflow, self.version, dimensions
        )
