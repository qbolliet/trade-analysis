"""Eurostat response parsing helpers.

Pure functions parsing the various Eurostat API responses into the project's
data structures: gzip decompression, SDMX-CSV/TSV/JSON-stat data, and SDMX-ML
structure / dataflow-catalogue responses. They carry no client state and are
therefore exposed as module-level functions rather than methods.
"""
# Importation des modules
from datetime import datetime, timezone
import gzip
from io import StringIO
import logging
import re
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

import pandas as pd

from ...core.structures import DataflowStructure, DimensionInfo
from .formats import AGENCY_ID

# Initialisation du logger
logger = logging.getLogger(__name__)

# Namespaces XML SDMX 3.0
_SDMX3_NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common",
}

# Namespaces XML SDMX 2.1 (fallback)
_SDMX21_NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}


# Fonction de décompression transparente des réponses gzip
def decompress_response_bytes(content: bytes) -> bytes:
    """Transparently decompress response bytes if gzip-encoded.

    Some Eurostat API endpoints return gzip-compressed content when the
    ``compress=true`` query parameter is set, or by default for large
    structure responses.  This function inspects the magic bytes and
    decompresses only when necessary, so it is safe to call on any
    response regardless of whether compression was requested.

    Args:
        content: Raw response bytes, possibly gzip-compressed.

    Returns:
        Decompressed bytes, or the original bytes unchanged if the
        content is not gzip-compressed.
    """
    # Détection de la compression gzip par les octets magiques (0x1F 0x8B)
    if content[:2] == b"\x1f\x8b":
        return gzip.decompress(content)
    return content


# Fonction de parsing du format TSV Eurostat (format large avec flags)
def parse_tsv_response(text: str) -> pd.DataFrame:
    """Parse Eurostat TSV format (wide format with flags).

    The first column contains dimensions separated by commas, followed
    by tab-separated period columns.

    Args:
        text: TSV response text.

    Returns:
        Parsed DataFrame in long (tidy) format.

    Raises:
        ValueError: If TSV parsing fails.
    """
    try:
        # Lecture du TSV avec séparateur tabulation
        df = pd.read_csv(StringIO(text), sep="\t")
        index_col = df.columns[0]

        # Sélection des colonnes de périodes (contiennent des chiffres)
        period_cols = [
            col
            for col in df.columns[1:]
            if any(char.isdigit() for char in col)
        ]

        # Extraction des dimensions depuis la première colonne composite
        dimensions_split = df[index_col].str.split(",", expand=True)
        dim_names = [f"DIM_{i}" for i in range(len(dimensions_split.columns))]
        dimensions_split.columns = dim_names

        # Reconstruction du DataFrame en format large puis conversion en format long
        df_wide = pd.concat(
            [dimensions_split, df[period_cols].copy()], axis=1
        )
        df_long = df_wide.melt(
            id_vars=dim_names,
            var_name="TIME_PERIOD",
            value_name="value",
        )

        # Nettoyage des valeurs (suppression des flags et conversion numérique)
        df_long["value"] = df_long["value"].astype(str).str.strip()
        df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")

        return df_long
    # Gestion des erreurs de parsing
    except Exception as e:
        logger.error(f"TSV parsing failed: {e}")
        raise ValueError(f"Failed to parse TSV response: {e}")


# Fonction de parsing de réponse JSON-stat 2.0
def parse_json_response(data: Dict[str, Any]) -> pd.DataFrame:
    """Parse a JSON-stat 2.0 response.

    Eurostat serialises responses in JSON-stat 2.0:

    - ``id``: ordered list of dimension identifiers.
    - ``size``: number of categories per dimension, same order as ``id``.
    - ``dimension[dim_id].category.index``: either a ``{code: position}``
      mapping or an ordered list of codes.
    - ``value``: observations indexed by a single flat row-major position
      over the multi-dimensional array described by ``size``. May be
      serialised as a ``{position_string: value}`` dictionary (sparse)
      or as a plain list (dense, with ``None`` for missing values).

    Args:
        data: JSON-stat dictionary.

    Returns:
        Parsed DataFrame with one column per dimension (using the
        dimension code, e.g. ``"FR"``, ``"M"``, ``"2024-01"``) plus a
        ``value`` column. Empty when the response has no observations.

    Raises:
        ValueError: If JSON parsing fails.
    """
    try:
        # Récupération de l'ordre des dimensions, de leurs tailles et des valeurs
        dim_ids: List[str] = list(data.get("id", []))
        sizes: List[int] = list(data.get("size", []))
        dimensions: Dict[str, Any] = data.get("dimension", {})
        values = data.get("value", {})

        # Réponse vide ou mal formée
        if not dim_ids or not sizes or len(dim_ids) != len(sizes):
            return pd.DataFrame(columns=[*dim_ids, "value"])

        # Construction d'un mapping position -> code pour chaque dimension
        codes_by_dim: Dict[str, List[Optional[str]]] = {}
        for dim_id, dim_size in zip(dim_ids, sizes):
            cat_index = (
                dimensions.get(dim_id, {}).get("category", {}).get("index", {})
            )
            # Format objet {code: position} → inversion en liste ordonnée
            if isinstance(cat_index, dict):
                pos_to_code: List[Optional[str]] = [None] * dim_size
                for code, pos in cat_index.items():
                    pos_int = int(pos)
                    if 0 <= pos_int < dim_size:
                        pos_to_code[pos_int] = code
                codes_by_dim[dim_id] = pos_to_code
            # Format tableau : codes déjà ordonnés
            elif isinstance(cat_index, list):
                codes_by_dim[dim_id] = list(cat_index)
            else:
                codes_by_dim[dim_id] = [None] * dim_size

        # Normalisation des observations en itérable (index_plat, valeur)
        # JSON-stat 2.0 autorise un dict sparse ou une list dense
        if isinstance(values, dict):
            obs_items = ((int(k), v) for k, v in values.items())
        else:
            obs_items = (
                (i, v) for i, v in enumerate(values) if v is not None
            )

        # Décomposition row-major de l'index plat en indices multidimensionnels
        n_dims = len(sizes)
        rows: List[Dict[str, Any]] = []
        for flat_idx, value in obs_items:
            multi_idx = [0] * n_dims
            remainder = flat_idx
            for axis in range(n_dims - 1, -1, -1):
                multi_idx[axis] = remainder % sizes[axis]
                remainder //= sizes[axis]

            # Mapping de chaque indice de dimension vers son code
            row: Dict[str, Any] = {}
            for axis, dim_id in enumerate(dim_ids):
                codes = codes_by_dim[dim_id]
                idx = multi_idx[axis]
                row[dim_id] = codes[idx] if 0 <= idx < len(codes) else None
            row["value"] = value
            rows.append(row)

        return pd.DataFrame(rows, columns=[*dim_ids, "value"])
    # Gestion des erreurs de parsing
    except Exception as e:
        # Logging
        logger.error(f"JSON parsing failed: {e}")
        raise ValueError(f"Failed to parse JSON response: {e}")


# Fonction auxiliaire de parsing d'une date ISO en datetime UTC
def _parse_iso_datetime(text: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 date or datetime string into a UTC-aware datetime.

    Tolerant of the formats Eurostat uses in structure responses: bare dates
    (``"2024-03-15"``), datetimes with or without timezone, and the ``Z``
    suffix. Naive results are assumed to be UTC.

    Args:
        text: Candidate date/datetime string (may be ``None`` or noisy).

    Returns:
        UTC-aware ``datetime`` if a date could be extracted, else ``None``.
    """
    # Court-circuit si la chaîne est vide
    if not text:
        return None
    candidate = text.strip()
    # Normalisation du suffixe Z (UTC) accepté par datetime.fromisoformat récent
    normalized = candidate.replace("Z", "+00:00")
    # Tentative de parsing ISO complet (date ou datetime)
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        # Repli : extraction d'une date AAAA-MM-JJ noyée dans un texte
        match = re.search(r"\d{4}-\d{2}-\d{2}", candidate)
        if not match:
            return None
        try:
            parsed = datetime.fromisoformat(match.group(0))
        except ValueError:
            return None
    # Normalisation en UTC : les datetimes naïfs sont interprétés comme UTC
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# Fonction de parsing de la date de dernière mise à jour d'un dataconstraint
def parse_dataconstraint_last_update(xml_content: str) -> Optional[datetime]:
    """Extract the data last-update instant from a dataconstraint response.

    Eurostat exposes the date a dataset's *data* was last updated through an
    annotation on the data constraint (commonly typed ``UPDATE_DATA``),
    carrying the date in its ``AnnotationTitle`` or ``AnnotationText``. This
    function scans every annotation, preferring those whose type mentions an
    update of the *data*, and parses the embedded date. When no suitable
    annotation is found it falls back to the message ``Prepared`` header so
    callers always get a usable (if conservative) timestamp.

    The conservative fallback is deliberate: returning the response
    preparation time makes the download orchestrator re-pull the last *N*
    observations rather than risk missing a genuine update.

    Args:
        xml_content: Decompressed SDMX-ML dataconstraint response.

    Returns:
        UTC-aware ``datetime`` of the last data update, or ``None`` if the XML
        carries no parseable date at all.
    """
    try:
        # Parsing du document XML
        root = ET.parse(StringIO(xml_content)).getroot()
    except ET.ParseError as e:
        logger.warning(f"Could not parse dataconstraint XML: {e}")
        return None

    # Recherche des annotations dans les deux jeux de namespaces (3.0 puis 2.1)
    best_update: Optional[datetime] = None
    best_is_data = False
    for namespaces in (_SDMX3_NS, _SDMX21_NS):
        for annotation in root.findall(".//com:Annotation", namespaces):
            # Type d'annotation (ex. UPDATE_DATA, UPDATE_STRUCTURE)
            type_elem = annotation.find("com:AnnotationType", namespaces)
            ann_type = (type_elem.text or "").strip() if type_elem is not None else ""
            ann_type_upper = ann_type.upper()

            # Filtre sur les annotations relatives à une mise à jour
            if "UPDATE" not in ann_type_upper:
                continue

            # Extraction d'une date depuis le titre puis le texte de l'annotation
            date_value: Optional[datetime] = None
            for tag in ("com:AnnotationTitle", "com:AnnotationText"):
                elem = annotation.find(tag, namespaces)
                if elem is not None:
                    date_value = _parse_iso_datetime(elem.text)
                    if date_value is not None:
                        break
            if date_value is None:
                continue

            # Priorité aux annotations de mise à jour des données (UPDATE_DATA)
            is_data = "DATA" in ann_type_upper
            if best_update is None or (is_data and not best_is_data):
                best_update = date_value
                best_is_data = is_data
        # Arrêt dès qu'un jeu de namespaces a produit des annotations exploitables
        if best_update is not None:
            break

    # Annotation de mise à jour des données trouvée → date retournée
    if best_update is not None:
        return best_update

    # Repli conservateur : en-tête mes:Prepared du message
    for namespaces in (_SDMX3_NS, _SDMX21_NS):
        prepared = root.find(".//mes:Prepared", namespaces)
        if prepared is not None:
            parsed = _parse_iso_datetime(prepared.text)
            if parsed is not None:
                return parsed

    # Aucune date exploitable
    return None


# Fonction auxiliaire d'extraction de l'identifiant de codelist depuis une URN
def _codelist_id_from_urn(urn: str) -> Optional[str]:
    """Extract the codelist identifier from an SDMX codelist URN.

    The data structure references the codelist of each dimension through a URN
    of the form ``urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ESTAT:CXT_NC(11.0)``.
    This helper returns the bare codelist identifier (``CXT_NC``), which is the
    resource id to pass to
    ``EurostatClient.get_structure(StructureResourceType.CODELIST, ...)``.

    Args:
        urn: Codelist URN (the text of a ``str:Enumeration`` element).

    Returns:
        The codelist identifier, or ``None`` when the URN cannot be parsed.

    Examples:
        >>> _codelist_id_from_urn(
        ...     "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ESTAT:CXT_NC(11.0)"
        ... )
        'CXT_NC'
    """
    # Identifiant entre "Codelist=<agence>:" et la parenthèse de version
    match = re.search(r"Codelist=[^:]*:([^()]+)", urn)
    return match.group(1) if match else None


# Fonction de parsing d'une réponse SDMX-ML et d'extraction des dimensions
def parse_structure_response(
    xml_content: str, dataflow: str
) -> DataflowStructure:
    """Parse an SDMX-ML structure response and extract dimensions.

    Tries SDMX 3.0 namespaces first, then falls back to 2.1.

    Args:
        xml_content: XML response content.
        dataflow: Dataflow identifier.

    Returns:
        ``DataflowStructure`` instance.

    Raises:
        ValueError: If XML parsing fails.
    """
    try:
        # Parsing du document XML 
        root = ET.parse(StringIO(xml_content)).getroot()

        # Tentative avec les namespaces SDMX 3.0 puis fallback vers 2.1
        namespaces = _SDMX3_NS
        structure_elem = root.find(".//str:DataStructure", namespaces)
        if structure_elem is None:
            namespaces = _SDMX21_NS
            structure_elem = root.find(".//str:DataStructure", namespaces)

        # Vérification de la présence de l'élément DataStructure
        if structure_elem is None:
            raise ValueError(
                "DataStructure element not found in XML response"
            )

        # Construction d'un index id -> nom depuis les ConceptSchemes
        # (les dimensions ne portent pas de description directement :
        #  elles référencent un Concept via ConceptIdentity)
        concept_names: dict[str, str] = {}
        for concept in root.findall(".//str:Concept", namespaces):
            concept_id = concept.get("id")
            if not concept_id:
                continue
            # Extraction du nom
            name: str | None = None
            for name_elem in concept.findall("com:Name", namespaces):
                name = name_elem.text
            concept_names[concept_id] = name

        # Extraction de la liste des dimensions depuis le DSD
        dimensions: list[DimensionInfo] = []
        dimension_list = structure_elem.find(
            ".//str:DimensionList", namespaces
        )
        if dimension_list is not None:
            for i, dim in enumerate(
                dimension_list.findall("str:Dimension", namespaces)
            ):
                dim_id = dim.get("id")
                position = dim.get("position", str(i))

                # Résolution de la description via le ConceptScheme
                description = concept_names.get(dim_id)

                # Résolution de la codelist énumérant les valeurs de la dimension :
                # la représentation locale référence la codelist via une URN
                # (ex. "...Codelist=ESTAT:CXT_FREE_ISO(10.0)").
                enumeration = dim.find(
                    "str:LocalRepresentation/str:Enumeration", namespaces
                )
                codelist = None
                if enumeration is not None and enumeration.text:
                    codelist = _codelist_id_from_urn(enumeration.text)

                dimensions.append(
                    DimensionInfo(
                        name=dim_id,
                        position=int(position),
                        description=description,
                        codelist=codelist,
                    )
                )

        # Construction et retour de la structure de dataflow
        return DataflowStructure(
            agency=AGENCY_ID,
            dataflow=dataflow,
            num_dimensions=len(dimensions),
            dimensions=dimensions,
            description=None,
        )
    # Gestion des erreurs de parsing XML
    except Exception as e:
        logger.error(f"Structure XML parsing failed: {e}")
        raise ValueError(f"Failed to parse structure response: {e}")


# Fonction de parsing d'une réponse SDMX-ML contenant une liste de dataflows
def parse_dataflow_list_response(xml_content: str) -> pd.DataFrame:
    """Parse an SDMX-ML structure response containing multiple dataflows.

    Tries SDMX 3.0 namespaces first, then falls back to 2.1.
    Extracts the English name for each dataflow when available.

    Args:
        xml_content: XML response content (already decompressed).

    Returns:
        DataFrame with columns: ``id``, ``name``, ``version``,
        ``agency``.

    Raises:
        ValueError: If XML parsing or element extraction fails.
    """
    try:
        # Parsing du document XML 
        root = ET.parse(StringIO(xml_content)).getroot()

        # Tentative avec les namespaces SDMX 3.0 puis fallback 2.1
        namespaces = _SDMX3_NS
        dataflows = root.findall(".//str:Dataflow", namespaces)
        if not dataflows:
            namespaces = _SDMX21_NS
            dataflows = root.findall(".//str:Dataflow", namespaces)

        # Extraction des métadonnées de chaque dataflow
        rows = []
        for df_elem in dataflows:
            df_id = df_elem.get("id")
            df_agency = df_elem.get("agencyID")
            df_version = df_elem.get("version")

            # Extraction du nom anglais, ou première langue disponible
            name: Optional[str] = None
            for name_elem in df_elem.findall("com:Name", namespaces):
                lang = name_elem.get(
                    "{http://www.w3.org/XML/1998/namespace}lang", ""
                )
                if name is None or lang == "en":
                    name = name_elem.text

            rows.append(
                {
                    "id": df_id,
                    "name": name,
                    "version": df_version,
                    "agency": df_agency,
                }
            )

        # Logging
        logger.info(f"Parsed {len(rows)} dataflows from catalogue response")
        return pd.DataFrame(rows)
    # Gestion des erreurs de parsing XML
    except Exception as e:
        logger.error(f"Dataflow catalogue parsing failed: {e}")
        raise ValueError(f"Failed to parse dataflow catalogue: {e}")


# Fonction de parsing d'une réponse SDMX-ML contenant une liste de codes
def parse_codelist_response(xml_content: str) -> pd.DataFrame:
    """Parse an SDMX-ML codelist response into a ``(code, name)`` DataFrame.

    Tries SDMX 3.0 namespaces first, then falls back to 2.1. Extracts every
    code of every codelist in the response, with the English name when
    available (falling back to the first localised name). Useful to enumerate
    the allowed values of a dimension (e.g. reporters, products) before
    building split queries.

    Args:
        xml_content: XML response content (already decompressed), typically
            from ``EurostatClient.get_structure(StructureResourceType.CODELIST,
            ...)``.

    Returns:
        DataFrame with columns: ``code``, ``name``. Empty (with those columns)
        when the response carries no code.

    Raises:
        ValueError: If XML parsing or element extraction fails.
    """
    try:
        # Parsing du document XML
        root = ET.parse(StringIO(xml_content)).getroot()

        # Tentative avec les namespaces SDMX 3.0 puis fallback 2.1
        namespaces = _SDMX3_NS
        codes = root.findall(".//str:Codelist/str:Code", namespaces)
        if not codes:
            namespaces = _SDMX21_NS
            codes = root.findall(".//str:Codelist/str:Code", namespaces)

        # Extraction de l'identifiant et du nom de chaque code
        rows = []
        for code_elem in codes:
            code_id = code_elem.get("id")

            # Extraction du nom anglais, ou première langue disponible
            name: Optional[str] = None
            for name_elem in code_elem.findall("com:Name", namespaces):
                lang = name_elem.get(
                    "{http://www.w3.org/XML/1998/namespace}lang", ""
                )
                if name is None or lang == "en":
                    name = name_elem.text

            rows.append({"code": code_id, "name": name})

        # Logging
        logger.info(f"Parsed {len(rows)} codes from codelist response")
        return pd.DataFrame(rows, columns=["code", "name"])
    # Gestion des erreurs de parsing XML
    except Exception as e:
        logger.error(f"Codelist parsing failed: {e}")
        raise ValueError(f"Failed to parse codelist response: {e}")
