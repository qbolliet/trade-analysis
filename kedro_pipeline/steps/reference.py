"""Reference tables: product and country labels, HS concordances (PS-28.4, C-22).

The download steps already fetch the codelists of their dimensions; the BACI
step already caches the UNSD concordance tables. This module turns them into
normalised reference tables and upserts them, idempotently (primary keys),
into the catalog of each source through :func:`statflows.write_dataframe`.

``write_dataframe`` always writes a ``<schema>.fact_table``: each reference
table therefore lives in its own schema, ``<SCHEMA_PREFIX>_<table>`` (e.g.
``reference_products.fact_table``). The dashboard never reads these schemas:
the serving layer copies them under their plain names (PS-29.2).

Normalisation is pure (no I/O) and tolerant to the column names of each
provider: Eurostat SDMX codelists (``code``, ``name``) and UN Comtrade
reference metadata (``id``/``reporterCode``/``PartnerCode``, ``text``/
``reporterDesc``/``PartnerDesc``, ``…IsoAlpha3``, ``isGroup``, ``parent``).
"""
# Importation des modules
# Modules de base
import hashlib
import logging
import re
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence, Tuple

# Data manipulation
import pandas as pd

# Modules internes
from kedro_pipeline.config import classification_of, vintage_in_force

# Logger
logger = logging.getLogger(__name__)


# Colonnes (et types DuckDB) des tables de référence, clé primaire en tête
REFERENCE_COLUMNS: Dict[str, Dict[str, str]] = {
    "products": {
        "classification": "VARCHAR",
        "code": "VARCHAR",
        "label": "VARCHAR",
        "level": "INTEGER",
        "parent_code": "VARCHAR",
    },
    "reporters": {
        "source": "VARCHAR",
        "code": "VARCHAR",
        "label": "VARCHAR",
        "iso3": "VARCHAR",
        "m49": "VARCHAR",
        "is_aggregate": "BOOLEAN",
    },
    "partners": {
        "source": "VARCHAR",
        "code": "VARCHAR",
        "label": "VARCHAR",
        "iso3": "VARCHAR",
        "m49": "VARCHAR",
        "is_aggregate": "BOOLEAN",
    },
    "hs_concordance": {
        "source_classification": "VARCHAR",
        "source_code": "VARCHAR",
        "target_classification": "VARCHAR",
        "target_code": "VARCHAR",
        "relationship": "VARCHAR",
        "checksum": "VARCHAR",
    },
    "hs_vintages": {
        "classification": "VARCHAR",
        "entry_year": "INTEGER",
        "in_force_until": "INTEGER",
    },
}

# Clés primaires des tables de référence
REFERENCE_KEYS: Dict[str, Tuple[str, ...]] = {
    "products": ("classification", "code"),
    "reporters": ("source", "code"),
    "partners": ("source", "code"),
    "hs_concordance": (
        "source_classification",
        "source_code",
        "target_classification",
        "target_code",
    ),
    "hs_vintages": ("classification",),
}

# Colonnes candidates par rôle, dans l'ordre de préférence (Eurostat puis Comtrade)
_CODE_CANDIDATES = ("code", "reporterCode", "PartnerCode", "id")
_LABEL_CANDIDATES = ("name", "label", "reporterDesc", "PartnerDesc", "text")
_ISO3_CANDIDATES = ("iso3", "reporterCodeIsoAlpha3", "PartnerCodeIsoAlpha3")
# Colonnes portant le code M49 (métadonnées Comtrade)
_M49_CODE_COLUMNS = ("reporterCode", "PartnerCode")
# Code pays individuel au format ISO 3166-1 alpha-2 (convention Comext)
_ISO2_PATTERN = re.compile(r"^[A-Z]{2}$")


# Fonction de nommage du schéma d'une table de référence
def reference_schema(prefix: str, table: str) -> str:
    """Return the schema holding a reference table.

    Args:
        prefix: Schema prefix (``DOWNLOADS.REFERENCE.SCHEMA_PREFIX``).
        table: Reference table name (a key of :data:`REFERENCE_COLUMNS`).

    Returns:
        ``"<prefix>_<table>"``.

    Raises:
        KeyError: If ``table`` is not a reference table.

    Examples:
        >>> reference_schema("reference", "products")
        'reference_products'
    """
    if table not in REFERENCE_COLUMNS:
        raise KeyError(f"Unknown reference table '{table}'")
    return f"{prefix}_{table}"


# Fonction de sélection de la première colonne disponible
def _first_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """Return the first candidate column present in ``df``, or ``None``."""
    return next((column for column in candidates if column in df.columns), None)


# Fonction de mise en forme finale d'une table de référence
def _finalise(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Order the columns, drop duplicated keys and apply nullable dtypes.

    Args:
        df: Normalised table.
        table: Reference table name.

    Returns:
        The table restricted to :data:`REFERENCE_COLUMNS` ``[table]``, one row
        per primary key (first occurrence kept).
    """
    columns = REFERENCE_COLUMNS[table]
    df = df[list(columns)].drop_duplicates(list(REFERENCE_KEYS[table]), keep="first")
    # Types nullables : les colonnes entières ou booléennes peuvent être vides
    dtypes = {"VARCHAR": "string", "INTEGER": "Int64", "BOOLEAN": "boolean"}
    return df.astype({name: dtypes[kind] for name, kind in columns.items()}).reset_index(
        drop=True
    )


# Fonction de normalisation d'une codelist de produits
def normalize_products(
    codelist: pd.DataFrame,
    *,
    year: int,
    nomenclatures: Mapping[str, int],
) -> pd.DataFrame:
    """Normalise a product codelist into the ``products`` reference table.

    Codelists are not versioned by the providers: codes are attached to the
    classification in force in ``year`` (the download year) with
    :func:`~kedro_pipeline.config.classification_of` — HS vintage for 2/4/6
    digits, ``CN<year>`` for 8 digits. Non-numeric codes (``TOTAL``…) are
    attached to the HS vintage in force, with a ``NULL`` level.

    Args:
        codelist: Provider codelist (Eurostat ``code``/``name``, Comtrade
            ``id``/``text``/``parent``).
        year: Year the codelist describes.
        nomenclatures: Mapping vintage label -> entry-into-force year.

    Returns:
        DataFrame ``(classification, code, label, level, parent_code)``.

    Raises:
        KeyError: If no code column is found.

    Examples:
        >>> hs = {"HS2017": 2017, "HS2022": 2022}
        >>> df = pd.DataFrame({"id": ["85", "8541", "854110"],
        ...                    "text": ["85 - Electrical", "8541 - Diodes", "854110 - Diodes, other"],
        ...                    "parent": ["TOTAL", "85", "8541"]})
        >>> out = normalize_products(df, year=2024, nomenclatures=hs)
        >>> out[["classification", "label", "level", "parent_code"]].values.tolist()
        [['HS2022', 'Electrical', 2, <NA>], ['HS2022', 'Diodes', 4, '85'], ['HS2022', 'Diodes, other', 6, '8541']]
    """
    code_col = _first_column(codelist, _CODE_CANDIDATES)
    if code_col is None:
        raise KeyError(f"No code column in the product codelist: {list(codelist.columns)}")
    label_col = _first_column(codelist, _LABEL_CANDIDATES)

    codes = codelist[code_col].astype(str).str.strip()
    labels = (
        codelist[label_col].astype("string")
        if label_col is not None
        else pd.Series(pd.NA, index=codelist.index, dtype="string")
    )
    # Libellés Comtrade préfixés par le code (« 854110 - Diodes… ») : préfixe retiré
    labels = pd.Series(
        [
            re.sub(rf"^\s*{re.escape(code)}\s*-\s*", "", label) if isinstance(label, str) else label
            for code, label in zip(codes, labels)
        ],
        index=codelist.index,
        dtype="string",
    )
    is_numeric = codes.str.fullmatch(r"\d+")
    level = codes.str.len().where(is_numeric).astype("Int64")

    # Code parent : fourni par Comtrade, sinon déduit (8 → 6 → 4 → 2 chiffres)
    def _derived_parent(code: str) -> Optional[str]:
        if not code.isdigit() or len(code) <= 2:
            return None
        return code[: 6 if len(code) == 8 else len(code) - 2]

    parent = codes.map(_derived_parent)
    if "parent" in codelist.columns:
        given = codelist["parent"].astype("string").str.strip()
        # Parents non numériques (« TOTAL », « # ») : pas de code parent
        parent = given.where(given.str.fullmatch(r"\d+").fillna(False), None)

    classification = codes.map(lambda code: classification_of(code, year, nomenclatures))
    classification = classification.where(
        is_numeric, vintage_in_force(year, nomenclatures)
    )
    return _finalise(
        pd.DataFrame(
            {
                "classification": classification,
                "code": codes,
                "label": labels,
                "level": level,
                "parent_code": parent,
            }
        ),
        "products",
    )


# Fonction de normalisation d'une codelist de pays (déclarants ou partenaires)
def normalize_countries(
    codelist: pd.DataFrame,
    *,
    source: str,
    aggregate_codes: Sequence[str] = (),
    table: str = "reporters",
) -> pd.DataFrame:
    """Normalise a reporter or partner codelist into a country reference table.

    Comtrade metadata carry the M49 code (the code itself), the ISO3 code and
    an ``isGroup`` flag. Comext codelists carry only a label: ``iso3``/``m49``
    are ``NULL`` and a code is an aggregate when it is listed in
    ``aggregate_codes`` or is not a two-letter ISO code (``EXT_EU``,
    ``EU27_2020``…), the rule of ``VulnerabilityConfig``.

    Args:
        codelist: Provider codelist.
        source: Source name stored in the ``source`` key column.
        aggregate_codes: Codes always treated as aggregates.
        table: ``"reporters"`` or ``"partners"``.

    Returns:
        DataFrame ``(source, code, label, iso3, m49, is_aggregate)``.

    Raises:
        KeyError: If no code column is found.

    Examples:
        >>> df = pd.DataFrame({"code": ["FR", "EXT_EU", "WORLD"],
        ...                    "name": ["France", "Extra-EU", "World"]})
        >>> normalize_countries(df, source="eurostat", aggregate_codes=["WORLD"])[
        ...     "is_aggregate"].tolist()
        [False, True, True]
    """
    code_col = _first_column(codelist, _CODE_CANDIDATES)
    if code_col is None:
        raise KeyError(f"No code column in the country codelist: {list(codelist.columns)}")
    label_col = _first_column(codelist, _LABEL_CANDIDATES)
    iso3_col = _first_column(codelist, _ISO3_CANDIDATES)

    codes = codelist[code_col].astype(str).str.strip()
    m49_col = _first_column(codelist, _M49_CODE_COLUMNS)
    if "isGroup" in codelist.columns:
        is_aggregate = codelist["isGroup"].astype("boolean")
    else:
        is_aggregate = codes.isin(list(aggregate_codes)) | ~codes.str.fullmatch(
            _ISO2_PATTERN.pattern
        )
    return _finalise(
        pd.DataFrame(
            {
                "source": source,
                "code": codes,
                "label": codelist[label_col] if label_col is not None else None,
                "iso3": codelist[iso3_col] if iso3_col is not None else None,
                "m49": codelist[m49_col].astype(str) if m49_col is not None else None,
                "is_aggregate": is_aggregate,
            }
        ),
        table,
    )


# Fonction de calcul d'une somme de contrôle d'une table
def frame_checksum(df: pd.DataFrame) -> str:
    """Return the SHA-256 digest of a table's content (same rule as the BACI cache).

    Args:
        df: Table to hash.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    hashed = pd.util.hash_pandas_object(df, index=False)
    return hashlib.sha256(hashed.to_numpy().tobytes()).hexdigest()


# Fonction de construction de la table de passage HS
def concordance_table(
    concordances: Mapping[Tuple[str, str], pd.DataFrame],
) -> pd.DataFrame:
    """Stack the UNSD concordance tables into the ``hs_concordance`` table.

    ``relationship`` is derived from cardinalities within each pair: ``1:1``,
    ``n:1`` (several source codes merged into one target), ``1:n`` or
    ``n:n``; ``checksum`` is the content hash of the pair's table.

    Args:
        concordances: Mapping ``(source, target) -> table`` on the canonical
            schema of ``UNSDClient.get_correspondence``.

    Returns:
        DataFrame on :data:`REFERENCE_COLUMNS` ``["hs_concordance"]``.

    Examples:
        >>> table = pd.DataFrame({"source_classification": "HS2022",
        ...     "source_code": ["010121", "010129", "020110"],
        ...     "target_classification": "HS2017",
        ...     "target_code": ["010121", "010121", "020110"]})
        >>> concordance_table({("HS2022", "HS2017"): table})["relationship"].tolist()
        ['n:1', 'n:1', '1:1']
    """
    frames = []
    for (source, target), df_pair in concordances.items():
        df = df_pair.copy()
        df["source_classification"] = df.get("source_classification", source)
        df["target_classification"] = df.get("target_classification", target)
        df["source_code"] = df["source_code"].astype(str)
        df["target_code"] = df["target_code"].astype(str)
        # Cardinalités : sources par cible et cibles par source
        sources_per_target = df.groupby("target_code")["source_code"].transform("nunique")
        targets_per_source = df.groupby("source_code")["target_code"].transform("nunique")
        df["relationship"] = [
            f"{'n' if n_src > 1 else '1'}:{'n' if n_tgt > 1 else '1'}"
            for n_src, n_tgt in zip(sources_per_target, targets_per_source)
        ]
        df["checksum"] = frame_checksum(df_pair)
        frames.append(df)
    if not frames:
        return _finalise(
            pd.DataFrame(columns=list(REFERENCE_COLUMNS["hs_concordance"])), "hs_concordance"
        )
    return _finalise(pd.concat(frames, ignore_index=True), "hs_concordance")


# Fonction de construction de la table des millésimes
def vintages_table(nomenclatures: Mapping[str, int]) -> pd.DataFrame:
    """Build the ``hs_vintages`` table from ``runtime.NOMENCLATURES.HS``.

    Args:
        nomenclatures: Mapping vintage label -> entry-into-force year.

    Returns:
        DataFrame ``(classification, entry_year, in_force_until)``;
        ``in_force_until`` is the year before the next vintage, ``NULL`` for
        the vintage currently in force.

    Examples:
        >>> vintages_table({"HS2017": 2017, "HS2022": 2022}).values.tolist()
        [['HS2017', 2017, 2021], ['HS2022', 2022, <NA>]]
    """
    ordered = sorted(nomenclatures.items(), key=lambda item: int(item[1]))
    rows = [
        {
            "classification": label,
            "entry_year": int(entry),
            "in_force_until": int(ordered[i + 1][1]) - 1 if i + 1 < len(ordered) else None,
        }
        for i, (label, entry) in enumerate(ordered)
    ]
    return _finalise(pd.DataFrame(rows), "hs_vintages")


# Gestionnaire de contexte : connexion fournie ou ouverte sur le connecteur
@contextmanager
def _connection(table: Any, conn: Any) -> Iterator[Any]:
    """Yield ``conn`` if given, else a connection opened (and closed) on ``table``."""
    if conn is not None:
        yield conn
        return
    own = table.connect()
    try:
        yield own
    finally:
        own.close()


# Fonction d'écriture d'un ensemble de tables de référence
def _write_tables(
    frames: Mapping[str, pd.DataFrame],
    table: Any,
    *,
    step: str,
    prefix: str,
    conn: Any = None,
) -> Dict[str, Any]:
    """Upsert reference tables, one schema each; a failure does not stop the others.

    Args:
        frames: Mapping reference table name -> normalised table.
        table: Unconnected DuckLake connector of the source catalog (exposes
            ``connect()`` and ``catalog_alias``).
        step: Step name reported in the result.
        prefix: Schema prefix of the reference tables.
        conn: Already open connection on that catalog (reused, not closed).

    Returns:
        Result mapping ``{"step", "tables", "rows", "failures"}``; never
        raises (a connection failure is reported under ``"connection"``).
    """
    from statflows.storage.ducklake.tables import write_dataframe

    result: Dict[str, Any] = {"step": step, "tables": [], "rows": {}, "failures": {}}
    try:
        with _connection(table, conn) as connection:
            for name, df in frames.items():
                try:
                    if df.empty:
                        logger.warning(f"Référentiel '{name}' vide : non écrit.")
                        continue
                    write_dataframe(
                        connection,
                        df,
                        list(REFERENCE_KEYS[name]),
                        catalog_alias=table.catalog_alias,
                        schema=reference_schema(prefix, name),
                        label=f"reference/{name}",
                    )
                    result["tables"].append(name)
                    result["rows"][name] = int(len(df))
                except Exception as exc:  # isolation : un référentiel n'emporte pas les autres
                    logger.warning(f"Échec de l'écriture du référentiel '{name}' : {exc}")
                    result["failures"][name] = str(exc)[:500]
    except Exception as exc:  # connexion impossible : rien n'est écrit, l'appelant continue
        logger.warning(f"Référentiels non publiés (connexion) : {exc}")
        result["failures"]["connection"] = str(exc)[:500]
    return result


# Fonction de publication des référentiels d'une source (PS-28.4)
def publish_reference(
    codelists: Mapping[str, pd.DataFrame],
    table: Any,
    *,
    source: str,
    params: Mapping[str, Any],
    conn: Any = None,
) -> Dict[str, Any]:
    """Publish the product and country reference tables of a source.

    Args:
        codelists: Codelists already fetched by the download step, keyed by
            dimension (``"reporter"``, ``"partner"``, ``"product"``,
            ``"cmd:HS"``…).
        table: Unconnected DuckLake connector of the source catalog.
        source: Source name (``"eurostat"``, ``"comtrade"``).
        params: ``SCHEMA_PREFIX``; ``DIMENSIONS`` (dimension -> reference
            table among ``products``/``reporters``/``partners``);
            ``NOMENCLATURES`` (``runtime.NOMENCLATURES.HS``); ``YEAR`` (year
            the codelists describe); optional ``AGGREGATE_CODES``.
        conn: Already open connection on the catalog (reused, not closed).

    Returns:
        Result mapping ``{"step", "tables", "rows", "failures"}``; dimensions
        without a codelist are skipped silently.

    Raises:
        KeyError: If a dimension maps to an unknown reference table.

    Examples:
        >>> publish_reference(
        ...     {"reporter": df_reporters}, connector, source="eurostat",
        ...     params={"SCHEMA_PREFIX": "reference", "DIMENSIONS": {"reporter": "reporters"},
        ...             "NOMENCLATURES": {"HS2022": 2022}, "YEAR": 2026},
        ... )  # doctest: +SKIP
    """
    frames: Dict[str, pd.DataFrame] = {}
    for dimension, name in (params.get("DIMENSIONS") or {}).items():
        codelist = codelists.get(dimension)
        if codelist is None:
            continue
        if name == "products":
            frames[name] = normalize_products(
                codelist, year=int(params["YEAR"]), nomenclatures=params["NOMENCLATURES"]
            )
        elif name in ("reporters", "partners"):
            frames[name] = normalize_countries(
                codelist,
                source=source,
                aggregate_codes=params.get("AGGREGATE_CODES") or (),
                table=name,
            )
        else:
            raise KeyError(f"Dimension '{dimension}' maps to unknown reference table '{name}'")
    return _write_tables(
        frames, table, step=f"reference/{source}", prefix=params["SCHEMA_PREFIX"], conn=conn
    )


# Fonction de publication des référentiels de nomenclature (tables de passage, millésimes)
def publish_hs_reference(
    concordances: Mapping[Tuple[str, str], pd.DataFrame],
    table: Any,
    *,
    params: Mapping[str, Any],
    conn: Any = None,
) -> Dict[str, Any]:
    """Publish ``hs_concordance`` (UNSD cache) and ``hs_vintages`` (runtime).

    Args:
        concordances: Mapping ``(source, target) -> table`` returned by the
            concordance cache of the BACI step.
        table: Unconnected DuckLake connector of the Comtrade catalog.
        params: ``SCHEMA_PREFIX`` and ``NOMENCLATURES``.
        conn: Already open connection on the catalog (reused, not closed).

    Returns:
        Result mapping ``{"step", "tables", "rows", "failures"}``.
    """
    frames = {
        "hs_concordance": concordance_table(concordances),
        "hs_vintages": vintages_table(params["NOMENCLATURES"]),
    }
    return _write_tables(
        frames, table, step="reference/hs", prefix=params["SCHEMA_PREFIX"], conn=conn
    )
