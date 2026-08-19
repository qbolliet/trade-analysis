"""Eurostat response formats, structure-query literal types and constants.

Centralises the small value types shared across the Eurostat submodules: the
response-format enum, the SDMX structure-query parameter literals, the set of
supported API versions and the maintaining-agency identifier.
"""
# Importation des modules
from typing import Literal

from ...core.sdmx import SDMXResponseFormat, SDMXVersion

# Identifiant de l'agence Eurostat
AGENCY_ID = "ESTAT"


# ──────────────────────────────────────────────────────────────────────
# Types pour les paramètres de requêtes de structure SDMX
# ──────────────────────────────────────────────────────────────────────

# Détails des structures
StructureDetail = Literal[
    "full",
    "allstubs",
    "referencestubs",
    "allcompletestubs",
    "referencecompletestubs",
    "referencepartial",
]
# Références des structures
StructureReferences = Literal[
    "none",
    "parents",
    "parentsandsiblings",
    "ancestors",
    "children",
    "descendants",
    "all",
]
# Compression des structures
StructureCompress = Literal["true", "false"]
# Type de détail des données
DataDetail = Literal[
    "full",
    "dataonly",
    "serieskeysonly",
    "nodata"
]

# Sous-ensemble des versions SDMX supportées par Eurostat (utilisé pour la validation)
SUPPORTED_API_VERSIONS: frozenset[SDMXVersion] = frozenset(
    {SDMXVersion.V2_1, SDMXVersion.V3}
)


# Énumération des formats de réponse pour les requêtes de données Eurostat
class EurostatResponseFormat(SDMXResponseFormat):
    """Response format for Eurostat SDMX data queries.

    Attributes:
        CSV: SDMX-CSV format (default).
        TSV: Tab-separated values format (legacy Eurostat).
        JSON: JSON-stat 2.0 format.
        XML: SDMX-ML XML format.
    """

    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    XML = "xml"
