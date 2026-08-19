"""OECD response formats and format/parameter mappings.

Defines the OECD-specific response format enum and the mapping from each
format to the corresponding OECD API ``format`` query-parameter value.
"""
# Importation des modules
from typing import Dict

from ...core.sdmx import SDMXResponseFormat


# Énumération des formats de réponse pour les requêtes de données OECD
class OECDResponseFormat(SDMXResponseFormat):
    """Response format options for the OECD API.

    Attributes:
        JSON: JSON format (jsondata parameter).
        CSV: CSV format without labels (csvfile parameter).
        CSV_LABELS: CSV format with labels (csvfilewithlabels parameter).
        XML: XML generic data format (genericdata parameter).
    """

    JSON = "json"
    CSV = "csv"
    CSV_LABELS = "csv_labels"
    XML = "xml"


# Mapping des formats vers les valeurs de paramètre API OECD
_OECD_FORMAT_PARAM_MAP: Dict[OECDResponseFormat, str] = {
    OECDResponseFormat.JSON: "jsondata",
    OECDResponseFormat.CSV: "csvfile",
    OECDResponseFormat.CSV_LABELS: "csvfilewithlabels",
    OECDResponseFormat.XML: "genericdata",
}
