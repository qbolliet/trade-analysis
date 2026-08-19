"""UN Comtrade parsing helpers.

Pure functions converting Comtrade reference/availability responses and the
parameter declarations into the package's shared structures. Kept free of any
HTTP or client state so they can be unit-tested in isolation, mirroring
``eurostat.parsing`` and ``oecd.parsing``.
"""
# Importation des modules
# Modules de base
import copy
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

# Modules du package
from ...core.structures import DataflowStructure

# Initialisation du logger
logger = logging.getLogger(__name__)


# Chargement des paramètres 
with open(Path(__file__).parents[4] / "parameters" / "comtrade.json", "r", encoding="utf-8") as f:
    PARAMETERS: Dict[str, Any] = json.load(f)


# Fonction de construction d'une structure de dataflow depuis les paramètres
def build_structure_from_parameters(
    agency: str,
    dataflow: str,
) -> Optional[DataflowStructure]:
    """Build a :class:`DataflowStructure` for a Comtrade dataflow.

    UN Comtrade exposes no structure endpoint, so the dataflow dimensions are
    declared in the module-level :data:`PARAMETERS` (``parameters/comtrade.json``).
    A ``STRUCTURES`` entry matching the requested dataflow is returned as-is;
    otherwise the canonical tariffline entry is cloned and re-stamped with the
    requested ``dataflow`` (every ``typeCode_freqCode_clCode`` dataflow shares
    the same dimension layout).

    Args:
        agency: Maintaining agency (``"COMTRADE"``).
        dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).

    Returns:
        A :class:`DataflowStructure` whose dimensions are the tariffline
        identifier columns.

    Raises:
        ValueError: If no ``STRUCTURES`` entry is declared at all.

    Examples:
        >>> structure = build_structure_from_parameters("COMTRADE", "C_A_HS")
        >>> structure.get_position("reporterISO")
        3
    """
    # Liste des structures
    structures: List[Dict[str, Any]] = PARAMETERS.get("STRUCTURES", [])

    # Recherche d'une entrée STRUCTURES correspondant exactement au dataflow
    for entry in structures:
        if entry.get("agency") == agency and entry.get("dataflow") == dataflow:
            return DataflowStructure.from_dict(entry)
    
    return None


# Fonction d'extraction des codes valides d'un jeu de métadonnées
def extract_codes(df, category: str) -> List[Any]:
    """Extract the valid codes of a reference category from its metadata.

    Args:
        df: Metadata DataFrame returned by ``ComtradeClient.get_metadata``.
        category: Reference category. One of ``"flow"``, ``"reporter"``,
            ``"partner"`` or ``"cmd:HS"``.

    Returns:
        List of valid codes for the category (expired entries are dropped for
        reporters and partners).

    Raises:
        ValueError: If ``category`` is not supported.

    Examples:
        >>> extract_codes(flow_metadata, "flow")  # doctest: +SKIP
        ['M', 'X', ...]
    """
    # Flux et nomenclature : codes dans la colonne "id"
    if category in ("flow", "cmd:HS"):
        return df["id"].tolist()
    # Reporters : codes des pays non expirés
    if category == "reporter":
        return df.loc[df["entryExpiredDate"].isna(), "reporterCode"].tolist()
    # Partenaires : codes des pays non expirés
    if category == "partner":
        return df.loc[df["entryExpiredDate"].isna(), "PartnerCode"].tolist()
    # Catégorie non supportée
    raise ValueError(
        f"Unsupported category '{category}'. "
        "Expected one of 'flow', 'reporter', 'partner', 'cmd:HS'."
    )


# Fonction d'extraction de la date de dernière publication d'une disponibilité
def parse_availability_last_released(
    availability,
) -> Dict[str, Optional[str]]:
    """Map each period to its ``lastReleased`` date from an availability frame.

    Reads the DataFrame returned by ``getFinalDataAvailability`` and produces a
    ``{period: lastReleased}`` mapping used by the download script to decide
    whether a (reporter, period) couple must be refreshed.

    Args:
        availability: DataFrame returned by
            ``ComtradeClient.get_final_data_availability`` (expects ``period``
            and ``lastReleased`` columns).

    Returns:
        Mapping of period (as ``str``) to its last-released date (``str`` or
        ``None``). Empty when the input is empty or lacks the columns.
    """
    # Court-circuit si le jeu de données est vide ou incomplet
    if (
        availability is None
        or availability.empty
        or "period" not in availability.columns
        or "lastReleased" not in availability.columns
    ):
        return {}

    # Construction du dictionnaire période → date de dernière publication
    return {
        str(period): (None if released is None or released != released else str(released))
        for period, released in zip(
            availability["period"], availability["lastReleased"]
        )
    }
