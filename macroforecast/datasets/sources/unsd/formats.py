"""UNSD formats, constants and workbook-reading conventions.

UNSD publishes its classification correspondence tables as plain Excel
workbooks on a static file server: there is neither a DSD nor a structure
endpoint, so this module plays the role that ``formats.py`` plays for
:mod:`~macroforecast.datasets.sources.comtrade` — it gathers every
provider-specific constant the client and the parser share:

* the response formats (the four file extensions UNSD publishes);
* the canonical output schema every vintage is normalised onto;
* the reading conventions that absorb the workbooks' irregularities — header
  detection pattern, per-extension pandas engine, partial-match marker values.

The *catalogue* itself (base URL, file names, sheet radicals, vintages) lives in
``parameters/unsd.json`` rather than here: it is data that changes when UNSD
publishes a new vintage, whereas this module holds the reading logic's
constants.
"""
# Importation des modules
# Modules de base
import re
from typing import Dict, List

# Module du package
from ...core.sdmx import SDMXResponseFormat


# Identifiant d'agence (par symétrie avec ESTAT / OECD / COMTRADE)
AGENCY_ID = "UNSD"

# Types de correspondance exposés par le client (radicaux des noms de feuilles)
CORRESPONDENCE_KINDS: List[str] = ["conversion", "correlation"]

# Schéma canonique de sortie, identique quel que soit le millésime
CANONICAL_COLUMNS: List[str] = [
    "source_classification",
    "source_code",
    "target_classification",
    "target_code",
    "relationship",
]

# Longueur des codes HS produits (sous-position à six chiffres)
HS_CODE_LENGTH: int = 6

# Relations valides de la feuille Correlation
VALID_RELATIONSHIPS: List[str] = ["1:1", "1:n", "n:1", "n:n"]


# Format de réponse du serveur de fichiers UNSD
class UNSDResponseFormat(SDMXResponseFormat):
    """File formats published by the UNSD classifications file server.

    Attributes:
        XLSX: Excel workbook, recent vintages (HS2017 and HS2022 sources).
        XLS: Legacy Excel workbook, older vintages (HS1996 to HS2012 sources).
        CSV: Comma-separated table (cross-classification tables).
        TXT: Text table (cross-classification tables).
    """
    XLSX = "xlsx"
    XLS = "xls"
    CSV = "csv"
    TXT = "txt"


# ──────────────────────────────────────────────────────────────────────
# Conventions de lecture des classeurs
# ──────────────────────────────────────────────────────────────────────

# Moteur pandas requis par extension : les .xls ne sont lisibles que par xlrd,
# les .xlsx que par openpyxl (les deux sont déclarés dans pyproject.toml).
EXCEL_ENGINES: Dict[str, str] = {
    ".xls": "xlrd",
    ".xlsx": "openpyxl",
}

# Motif d'identification d'une cellule d'en-tête de colonne de codes. Il couvre
# les trois libellés rencontrés ("HS2022", "HS 2012", "From HS 2017") et capture
# le millésime, qui sert à apparier chaque colonne à sa nomenclature.
HS_HEADER_PATTERN: str = r"HS\s*(\d{4})"
HS_HEADER_REGEX: re.Pattern = re.compile(HS_HEADER_PATTERN)

# Nombre minimal de cellules millésimées identifiant la ligne d'en-tête. Deux
# suffisent (source et cible) et écartent les lignes de titre des classeurs
# HS2012, qui ne portent le millésime que dans une seule cellule fusionnée.
MIN_HEADER_MATCHES: int = 2

# Nombre de lignes explorées à la recherche de l'en-tête. L'en-tête réel se
# trouve en 1re ligne (.xlsx), en 2e (.xls) ou en 7e (classeurs HS2012 à bloc
# de titre) ; la borne évite de parcourir un classeur entier en cas d'anomalie.
MAX_HEADER_SCAN_ROWS: int = 30

# Valeurs de la colonne de marqueur de correspondance partielle intercalée entre
# les colonnes de codes des .xls ("ex" ou "ex." selon les millésimes).
PARTIAL_MARKERS: List[str] = ["ex", "ex."]

# Fragments d'en-tête entraînant le rejet d'une colonne (modèle de lookup.py)
REJECTED_COLUMN_TOKENS: List[str] = ["partial"]

# Motif de normalisation des relations rédigées en toutes lettres : les
# classeurs HS2012 écrivent "n to 1" là où les autres écrivent "n:1".
RELATIONSHIP_SEPARATOR_PATTERN: str = r"\s+to\s+"


# Fonction d'extraction du moteur de lecture associé à une extension
def engine_for(extension: str) -> str:
    """Return the pandas Excel engine required by a workbook extension.

    Args:
        extension: File extension, with or without its leading dot and
            case-insensitive (e.g. ``".xls"``, ``"XLSX"``).

    Returns:
        The engine name to pass to ``pandas.read_excel``.

    Raises:
        ValueError: If the extension is not a supported workbook extension.

    Examples:
        >>> engine_for(".xls")
        'xlrd'
        >>> engine_for("xlsx")
        'openpyxl'
    """
    # Normalisation de l'extension (point optionnel, casse indifférente)
    normalised = extension.lower()
    if not normalised.startswith("."):
        normalised = f".{normalised}"

    # Résolution du moteur
    if normalised not in EXCEL_ENGINES:
        raise ValueError(
            f"Unsupported workbook extension '{extension}'. "
            f"Expected one of {sorted(EXCEL_ENGINES)}."
        )
    return EXCEL_ENGINES[normalised]


# Fonction de construction de la clé de catalogue d'une paire de millésimes
def table_key(source: str, target: str) -> str:
    """Build the ``TABLES`` catalogue key of a source/target vintage pair.

    Args:
        source: Source classification (e.g. ``"HS2022"``).
        target: Target classification (e.g. ``"HS2017"``).

    Returns:
        The catalogue key in ``'{source}-{target}'`` format.

    Examples:
        >>> table_key("HS2022", "HS2017")
        'HS2022-HS2017'
    """
    return f"{source}-{target}"


# Fonction d'extraction du millésime d'une nomenclature
def vintage_year(classification: str) -> str:
    """Extract the four-digit vintage of an HS classification identifier.

    The vintage is what pairs a workbook column to its classification: the
    column headers spell it inconsistently (``"HS2022"``, ``"HS 2012"``,
    ``"From HS 2017"``) but always carry it.

    Args:
        classification: Classification identifier (e.g. ``"HS2022"``).

    Returns:
        The four-digit vintage as a string.

    Raises:
        ValueError: If no vintage can be read from the identifier.

    Examples:
        >>> vintage_year("HS2022")
        '2022'
        >>> vintage_year("HS 1992")
        '1992'
    """
    # Recherche du millésime dans l'identifiant
    match = HS_HEADER_REGEX.search(classification)
    if match is None:
        raise ValueError(
            f"Could not read an HS vintage from classification "
            f"'{classification}'. Expected a form such as 'HS2022'."
        )
    return match.group(1)
