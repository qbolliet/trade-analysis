"""UN Comtrade formats, constants and dimension mapping.

UN Comtrade does not follow the SDMX conventions of the Eurostat and OECD
APIs, so this module gathers the provider-specific constants (agency id,
response formats, valid frequencies) and the central dimension mapping that
bridges the three naming systems Comtrade exposes:

* the friendly names used as :meth:`ComtradeClient.get_data` arguments
  (``flows``, ``products``, ``reporters``…);
* the request-argument names of ``comtradeapicall._getTarifflineData``
  (``flowCode``, ``cmdCode``, ``reporterCode``…);
* the structure-dimension / response-column names (``flowDesc``, ``cmdCode``,
  ``reporterISO``…) declared in ``parameters/comtrade.json``.

Centralising this mapping keeps :mod:`client` free of scattered name
translations and lets the request subdivision iterate every flow-defining
dimension generically.
"""
# Importation des modules
from typing import Dict, List, Optional

# Module du package
from ...core.sdmx import SDMXResponseFormat


# Identifiant d'agence (par symétrie avec ESTAT / OECD)
AGENCY_ID = "COMTRADE"

# Fréquences métier valides acceptées par le client
VALID_FREQUENCIES: List[str] = ["annual", "monthly"]


# Format de réponse de l'API Comtrade
class ComtradeResponseFormat(SDMXResponseFormat):
    """Response formats supported by the UN Comtrade API.

    Attributes:
        JSON: JSON response (default for ``comtradeapicall``).
        CSV: CSV response.
    """
    JSON = "json"
    CSV = "csv"


# ──────────────────────────────────────────────────────────────────────
# Correspondance centrale des dimensions du flux
# ──────────────────────────────────────────────────────────────────────

# Mapping des dimensions subdivisibles : nom convivial (argument de get_data) →
#   - api_arg       : nom de l'argument correspondant de _getTarifflineData ;
#   - structure_dim : nom de la dimension de structure / colonne de réponse ;
#   - category      : catégorie de métadonnées pour récupérer les codes valides
#                     (None ⇒ codes non énumérables, ex. les périodes).
DIMENSIONS: Dict[str, Dict[str, Optional[str]]] = {
    "flows":     {"api_arg": "flowCode",     "structure_dim": "flowDesc",    "category": "flow"},
    "products":  {"api_arg": "cmdCode",      "structure_dim": "cmdCode",     "category": "cmd:HS"},
    "reporters": {"api_arg": "reporterCode", "structure_dim": "reporterISO", "category": "reporter"},
    "partners":  {"api_arg": "partnerCode",  "structure_dim": "partnerISO",  "category": "partner"},
    "partners2": {"api_arg": "partner2Code", "structure_dim": "partner2ISO", "category": "partner"},
    "customs":   {"api_arg": "customsCode",  "structure_dim": "customsCode", "category": None},
    "mot":       {"api_arg": "motCode",      "structure_dim": "motCode",     "category": None},
    "periods":   {"api_arg": "period",       "structure_dim": "period",      "category": None},
}

# Ordre de préférence des dimensions testées pour subdiviser une requête trop
# large (les flux d'abord, les périodes en dernier recours).
SUBDIVISION_ORDER: List[str] = [
    "flows",
    "products",
    "reporters",
    "partners",
    "partners2",
    "customs",
    "mot",
    "periods",
]


# Fonction d'extraction de l'argument API associé à un nom convivial
def api_arg_for(name: str) -> str:
    """Return the ``_getTarifflineData`` argument name of a friendly dimension.

    Args:
        name: Friendly dimension name (a key of :data:`DIMENSIONS`).

    Returns:
        The corresponding ``_getTarifflineData`` argument name.

    Raises:
        KeyError: If ``name`` is not a known dimension.

    Examples:
        >>> api_arg_for("reporters")
        'reporterCode'
    """
    return DIMENSIONS[name]["api_arg"]


# Fonction d'extraction de la catégorie de métadonnées d'un nom convivial
def category_for(name: str) -> Optional[str]:
    """Return the metadata category used to list a dimension's valid codes.

    Args:
        name: Friendly dimension name (a key of :data:`DIMENSIONS`).

    Returns:
        The metadata category (e.g. ``"reporter"``), or ``None`` when the
        dimension's codes cannot be enumerated (e.g. ``"periods"``).

    Raises:
        KeyError: If ``name`` is not a known dimension.

    Examples:
        >>> category_for("flows")
        'flow'
        >>> category_for("periods") is None
        True
    """
    return DIMENSIONS[name]["category"]
