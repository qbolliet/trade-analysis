"""UNSD correspondence-table parsing helpers.

Pure functions normalising the UNSD correspondence workbooks onto the canonical
schema :data:`~macroforecast.datasets.sources.unsd.formats.CANONICAL_COLUMNS`.
Kept free of any HTTP or client state so they can be unit-tested from a local
workbook, mirroring ``comtrade.parsing``.

The workbooks are hand-maintained spreadsheets rather than an API payload, and
their layout drifts across vintages. Every function below neutralises one
documented discrepancy, verified against the 21 published tables:

1. **Heterogeneous sheet names** — ``"HS2022-HS2017 Conversions"`` (xlsx) vs
   ``"Conversion HS12-HS07"`` (xls) vs ``"Conversion Tables"`` (HS2007):
   :func:`select_sheet` matches the case-insensitive radical, never an exact
   name.
2. **Variable header row** — first row in the xlsx, second in most xls, and
   *seventh* in the HS2012 workbooks, which open on a title block:
   :func:`detect_header_row` locates the first row carrying at least two
   vintage-bearing cells.
3. **Lost leading zeros** — read without ``dtype=str`` the xlsx codes come back
   as ``int64`` and ``010121`` becomes ``10121``: :func:`read_sheet` reads
   everything as text and :func:`normalise_codes` re-pads to six digits.
4. **Spurious columns** — the xls interleave an ``ex`` / ``ex.`` partial-match
   marker column between source and target, and the HS2012 workbooks indent the
   table by an empty column: :func:`drop_marker_columns` rejects both, plus any
   ``partial`` column (the filter ``lookup.py`` already applied).
5. **Two engines** — ``.xls`` requires ``xlrd`` and ``.xlsx`` ``openpyxl``,
   resolved by :func:`~...formats.engine_for`.

Two further discrepancies surfaced on inspection and are handled by
:func:`normalise_relationships`: the HS2012 workbooks spell relationships
``"n to 1"`` instead of ``"n:1"``, and the HS1996/HS2002 workbooks prefix them
with a spreadsheet text-guard apostrophe (``"'n:n"``).
"""
# Importation des modules
# Modules de base
import json
import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Modules du package
from .formats import (
    CANONICAL_COLUMNS,
    HS_CODE_LENGTH,
    HS_HEADER_REGEX,
    MAX_HEADER_SCAN_ROWS,
    MIN_HEADER_MATCHES,
    PARTIAL_MARKERS,
    REJECTED_COLUMN_TOKENS,
    RELATIONSHIP_SEPARATOR_PATTERN,
    engine_for,
    vintage_year,
)

# Initialisation du logger
logger = logging.getLogger(__name__)


# Chargement des paramètres
with open(Path(__file__).parents[4] / "parameters" / "unsd.json", "r", encoding="utf-8") as f:
    PARAMETERS: Dict[str, Any] = json.load(f)


# Fonction de sélection de la feuille correspondant à un type de correspondance
def select_sheet(sheet_names: List[str], kind: str, filename: str) -> str:
    """Select the workbook sheet holding a given kind of correspondence.

    Matching is done on the case-insensitive radical declared in
    ``SHEET_PATTERNS`` (``"conversion"`` / ``"correlation"``) because the sheet
    names differ across vintages and no exact name is shared by all workbooks.

    Args:
        sheet_names: Sheet names of the workbook.
        kind: Correspondence kind, ``"conversion"`` or ``"correlation"``.
        filename: Source file name, used in error messages.

    Returns:
        The name of the single matching sheet.

    Raises:
        ValueError: If ``kind`` is unknown, or if the radical matches no sheet
            or several sheets.

    Examples:
        >>> select_sheet(
        ...     ["HS2022-HS2017 Conversions", "HS2022-HS2017 Correlations"],
        ...     "conversion",
        ...     "HS2022toHS2017ConversionAndCorrelationTables.xlsx",
        ... )
        'HS2022-HS2017 Conversions'
        >>> select_sheet(
        ...     ["Correlation HS2012-HS2007", "Conversion HS12-HS07"],
        ...     "conversion",
        ...     "HS 2012 to HS 2007 Correlation and conversion tables.xls",
        ... )
        'Conversion HS12-HS07'
    """
    # Extraction du radical déclaré pour le type demandé
    patterns: Dict[str, str] = PARAMETERS["SHEET_PATTERNS"]
    if kind not in patterns:
        raise ValueError(
            f"Unsupported correspondence kind '{kind}'. "
            f"Expected one of {sorted(patterns)}."
        )
    radical = patterns[kind].casefold()

    # Correspondance insensible à la casse sur le radical
    matches = [name for name in sheet_names if radical in name.casefold()]
    if not matches:
        raise ValueError(
            f"No '{kind}' sheet found in '{filename}'. "
            f"Available sheets: {sheet_names}."
        )
    if len(matches) > 1:
        raise ValueError(
            f"Ambiguous '{kind}' sheet in '{filename}': {matches}."
        )
    return matches[0]


# Fonction de détection de la ligne d'en-tête réelle d'une feuille
def detect_header_row(df_raw: pd.DataFrame, filename: str) -> int:
    """Locate the real header row of a correspondence sheet.

    The header is the first row carrying at least
    :data:`~...formats.MIN_HEADER_MATCHES` vintage-bearing cells. Requiring two
    such cells is what discriminates a header (``"HS 2012" | "HS 2007"``) from
    the merged title cell of the HS2012 workbooks
    (``"Conversion table HS 2012 to HS 2002"``), which carries both vintages in
    a *single* cell.

    Args:
        df_raw: Sheet read with ``header=None`` (positional columns).
        filename: Source file name, used in error messages.

    Returns:
        Positional index of the header row.

    Raises:
        ValueError: If no header row is found within the scanned range.

    Examples:
        >>> df_raw = pd.DataFrame([["From", "To"], ["HS 2012", "HS 2007"]])
        >>> detect_header_row(df_raw, "example.xls")
        1
    """
    # Parcours des premières lignes à la recherche des cellules millésimées
    for position in range(min(len(df_raw), MAX_HEADER_SCAN_ROWS)):
        n_matches = sum(
            1
            for value in df_raw.iloc[position]
            if isinstance(value, str) and HS_HEADER_REGEX.search(value)
        )
        if n_matches >= MIN_HEADER_MATCHES:
            return position

    raise ValueError(
        f"No header row found in the first {MAX_HEADER_SCAN_ROWS} rows of "
        f"'{filename}'. Expected a row with at least {MIN_HEADER_MATCHES} "
        f"cells matching the HS vintage pattern."
    )


# Fonction de lecture d'une feuille en texte brut
def read_sheet(content: bytes, extension: str, sheet: str) -> pd.DataFrame:
    """Read one workbook sheet as raw text, without header interpretation.

    Reading with ``header=None`` and ``dtype=str`` is what makes the downstream
    treatment identical for ``.xls`` and ``.xlsx``: the header row is located
    afterwards, and the codes never transit through ``int64`` — which would
    strip their leading zeros.

    Args:
        content: Raw workbook bytes.
        extension: Workbook extension (``".xls"`` or ``".xlsx"``), selecting
            the reading engine.
        sheet: Name of the sheet to read.

    Returns:
        DataFrame of the sheet's cells, positional columns, values as ``str``
        or ``NaN``.

    Raises:
        ValueError: If the extension has no associated engine.
    """
    # Lecture avec le moteur imposé par l'extension
    return pd.read_excel(
        BytesIO(content),
        engine=engine_for(extension),
        sheet_name=sheet,
        header=None,
        dtype=str,
    )


# Fonction de rejet des colonnes parasites d'une feuille
def drop_marker_columns(
    df_body: pd.DataFrame, headers: List[str]
) -> Tuple[pd.DataFrame, List[str]]:
    """Drop the marker, empty and partial-match columns of a sheet body.

    Three kinds of spurious columns coexist in the workbooks and are removed
    here: the ``ex`` / ``ex.`` partial-match markers interleaved between the
    code columns of the ``.xls``, the empty indentation columns of the HS2012
    workbooks, and any column whose header contains ``partial`` — the filter
    the legacy ``lookup.py`` already applied.

    Args:
        df_body: Sheet rows below the header, positional columns.
        headers: Header labels, positionally aligned with ``df_body``.

    Returns:
        Tuple ``(filtered body, filtered headers)``, still positionally
        aligned.

    Examples:
        >>> df_body = pd.DataFrame([["010121", "ex", "010110"]])
        >>> body, headers = drop_marker_columns(df_body, ["HS 2012", "", "HS 2007"])
        >>> headers
        ['HS 2012', 'HS 2007']
    """
    # Sélection des positions conservées
    kept_positions: List[int] = []
    kept_headers: List[str] = []
    for position, header in enumerate(headers):
        # Rejet sur le libellé d'en-tête (correspondance partielle déclarée)
        if any(token in header.casefold() for token in REJECTED_COLUMN_TOKENS):
            continue
        # Rejet sur le contenu : colonne vide ou entièrement composée de marqueurs
        values = (
            df_body.iloc[:, position].dropna().astype(str).str.strip().str.casefold()
        )
        if values.empty or values.isin(PARTIAL_MARKERS).all():
            continue
        kept_positions.append(position)
        kept_headers.append(header)

    return df_body.iloc[:, kept_positions], kept_headers


# Fonction de résolution des colonnes de codes et de relation
def resolve_columns(
    headers: List[str], source: str, target: str, filename: str
) -> Tuple[int, int, Optional[int]]:
    """Resolve the source, target and relationship column positions.

    Code columns are paired to their classification by the vintage their header
    carries, not by their position: the headers are spelled inconsistently
    (``"HS2022"``, ``"From HS 2017"``, ``"HS 2012"``) but always bear the
    vintage.

    Args:
        headers: Header labels of the retained columns.
        source: Source classification (e.g. ``"HS2022"``).
        target: Target classification (e.g. ``"HS2017"``).
        filename: Source file name, used in error messages.

    Returns:
        Tuple ``(source position, target position, relationship position)``, the
        last being ``None`` on a Conversion sheet.

    Raises:
        ValueError: If a vintage matches no header or several headers.

    Examples:
        >>> resolve_columns(
        ...     ["HS 2012", "HS 2007", "Relationship"],
        ...     "HS2012", "HS2007", "example.xls",
        ... )
        (0, 1, 2)
    """
    # Recherche de la colonne portant un millésime donné
    def _find(classification: str) -> int:
        year = vintage_year(classification)
        positions = [
            position
            for position, header in enumerate(headers)
            for match in [HS_HEADER_REGEX.search(header)]
            if match is not None and match.group(1) == year
        ]
        if not positions:
            raise ValueError(
                f"No column found for classification '{classification}' in "
                f"'{filename}'. Header row: {headers}."
            )
        if len(positions) > 1:
            raise ValueError(
                f"Ambiguous columns for classification '{classification}' in "
                f"'{filename}': positions {positions} of header row {headers}."
            )
        return positions[0]

    # Colonnes de codes source et cible
    source_position = _find(source)
    target_position = _find(target)

    # Colonne de relation (présente sur la seule feuille Correlation)
    relationship_positions = [
        position
        for position, header in enumerate(headers)
        if "relation" in header.casefold()
    ]
    relationship_position = (
        relationship_positions[0] if relationship_positions else None
    )

    return source_position, target_position, relationship_position


# Fonction de normalisation des codes HS
def normalise_codes(codes: pd.Series) -> pd.Series:
    """Normalise a column of HS codes to zero-padded six-digit strings.

    Args:
        codes: Column of codes read as text.

    Returns:
        Column of stripped, zero-padded codes, as pandas ``string`` dtype.

    Examples:
        >>> normalise_codes(pd.Series([" 10121 ", "010129"])).tolist()
        ['010121', '010129']
    """
    # Cadrage à six chiffres après retrait des espaces parasites
    return (
        codes.astype("string").str.strip().str.zfill(HS_CODE_LENGTH)
    )


# Fonction de normalisation des relations de la feuille Correlation
def normalise_relationships(relationships: pd.Series) -> pd.Series:
    """Normalise a relationship column onto the ``1:1`` / ``n:n`` convention.

    Absorbs the two spellings that coexist across vintages: the text-guard
    apostrophe of the HS1996/HS2002 workbooks (``"'n:n"``) and the spelled-out
    separator of the HS2012 ones (``"n to 1"``).

    Args:
        relationships: Column of relationships read as text.

    Returns:
        Column of normalised relationships, as pandas ``string`` dtype.

    Examples:
        >>> normalise_relationships(pd.Series(["1:1", "'n:n", "n to 1"])).tolist()
        ['1:1', 'n:n', 'n:1']
    """
    # Retrait de l'apostrophe de protection puis uniformisation du séparateur
    return (
        relationships.astype("string")
        .str.strip()
        .str.lstrip("'")
        .str.replace(RELATIONSHIP_SEPARATOR_PATTERN, ":", regex=True)
        .str.lower()
    )


# Fonction de normalisation d'une feuille de correspondance
def parse_correspondence(
    content: bytes,
    extension: str,
    source: str,
    target: str,
    kind: str,
    filename: str,
) -> pd.DataFrame:
    """Normalise one correspondence sheet onto the canonical schema.

    Args:
        content: Raw workbook bytes.
        extension: Workbook extension (``".xls"`` or ``".xlsx"``).
        source: Source classification (e.g. ``"HS2022"``).
        target: Target classification (e.g. ``"HS2017"``).
        kind: Correspondence kind, ``"conversion"`` or ``"correlation"``.
        filename: Source file name, used in error messages.

    Returns:
        DataFrame with the canonical columns ``source_classification``,
        ``source_code``, ``target_classification``, ``target_code`` and
        ``relationship``. The last is left ``pd.NA`` on a Conversion sheet,
        which carries no relationship annotation.

    Raises:
        ValueError: If the sheet, the header row or a code column cannot be
            resolved.
    """
    # Sélection de la feuille et lecture en texte brut
    sheet = select_sheet(
        pd.ExcelFile(BytesIO(content), engine=engine_for(extension)).sheet_names,
        kind=kind,
        filename=filename,
    )
    df_raw = read_sheet(content=content, extension=extension, sheet=sheet)

    # Découpe en-tête / corps à la ligne d'en-tête réelle
    header_position = detect_header_row(df_raw=df_raw, filename=filename)
    headers = [
        "" if pd.isna(value) else str(value).strip()
        for value in df_raw.iloc[header_position]
    ]
    df_body = df_raw.iloc[header_position + 1 :].reset_index(drop=True)

    # Rejet des colonnes parasites puis appariement aux nomenclatures
    df_body, headers = drop_marker_columns(df_body=df_body, headers=headers)
    source_position, target_position, relationship_position = resolve_columns(
        headers=headers, source=source, target=target, filename=filename
    )

    # Retrait des lignes incomplètes : une correspondance exige un code de part
    # et d'autre. Sont écartées les lignes de remplissage en fin de feuille et
    # les quelques lignes orphelines des classeurs HS2012 (un code cible sans
    # code source ni relation, appendues après la fin de la nomenclature).
    source_codes = df_body.iloc[:, source_position]
    target_codes = df_body.iloc[:, target_position]
    is_incomplete = source_codes.isna() | target_codes.isna()
    if is_incomplete.any():
        # Logging du volume écarté, pour tracer un éventuel dérapage du filtre
        logger.debug(
            f"Dropping {int(is_incomplete.sum())} incomplete rows from "
            f"'{filename}' ({kind} sheet)"
        )
        df_body = df_body.loc[~is_incomplete]

    # Construction du schéma canonique
    df_table = pd.DataFrame(
        {
            "source_classification": source,
            "source_code": normalise_codes(df_body.iloc[:, source_position]),
            "target_classification": target,
            "target_code": normalise_codes(df_body.iloc[:, target_position]),
        }
    )
    # Relation annotée sur la seule feuille Correlation
    if relationship_position is None:
        df_table["relationship"] = pd.Series(pd.NA, index=df_table.index, dtype="string")
    else:
        df_table["relationship"] = normalise_relationships(
            df_body.iloc[:, relationship_position]
        )

    return df_table[CANONICAL_COLUMNS].reset_index(drop=True)


# Fonction de validation d'une table de conversion
def validate_conversion(df_table: pd.DataFrame, filename: str) -> None:
    """Check that a parsed Conversion table is a well-formed function.

    The Conversion sheet is meant to be a genuine function — exactly one target
    code per source code — which is what makes a downward conversion
    unambiguous. A silent failure here would corrupt the whole downstream
    chain, so both invariants are enforced eagerly.

    Args:
        df_table: Parsed conversion table, canonical schema.
        filename: Source file name, named in the error message.

    Returns:
        None.

    Raises:
        ValueError: If a source code is duplicated, or if any code does not
            have exactly :data:`~...formats.HS_CODE_LENGTH` digits.

    Examples:
        >>> df_table = pd.DataFrame({
        ...     "source_classification": ["HS2022"],
        ...     "source_code": ["010121"],
        ...     "target_classification": ["HS2017"],
        ...     "target_code": ["010121"],
        ...     "relationship": [pd.NA],
        ... })
        >>> validate_conversion(df_table, "example.xlsx") is None
        True
    """
    # Unicité des codes sources (la Conversion doit être une fonction)
    duplicated = df_table.loc[df_table["source_code"].duplicated(keep=False), "source_code"]
    if not duplicated.empty:
        raise ValueError(
            f"Conversion table '{filename}' is not a function: "
            f"{duplicated.nunique()} source codes are duplicated "
            f"(e.g. {sorted(duplicated.unique())[:5]}). "
            f"Expected exactly one target code per source code."
        )

    # Longueur des codes de part et d'autre
    for column in ("source_code", "target_code"):
        invalid = df_table.loc[
            df_table[column].str.len() != HS_CODE_LENGTH, column
        ]
        if not invalid.empty:
            raise ValueError(
                f"Conversion table '{filename}' holds {len(invalid)} "
                f"'{column}' values that are not {HS_CODE_LENGTH} digits long "
                f"(e.g. {sorted(invalid.unique())[:5]})."
            )
