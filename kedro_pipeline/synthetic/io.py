"""I/O helpers of the synthetic-data scripts (configuration, catalog, registry).

Everything that touches files, the DuckLake catalog or the download registry is
kept here so that :mod:`kedro_pipeline.synthetic.world`,
:mod:`kedro_pipeline.synthetic.comtrade` and :mod:`kedro_pipeline.synthetic.comext`
stay pure.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence
import yaml

# Modules de manipulation de données
import pandas as pd

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.core.download import _parse_iso
from statflows.storage.ducklake.tables import FACT_TABLE, fact_table_exists
from statflows.storage.json import Loader, Saver

# Initialisation du logger
logger = logging.getLogger(__name__)

# Variable d'environnement et valeur par défaut du fichier de configuration
SYNTHETIC_CONFIG_ENV = "SYNTHETIC_CONFIG_PATH"
DEFAULT_SYNTHETIC_CONFIG = "config/profiles/demo/synthetic.yaml"
# Racine du registre de téléchargement statflows
_REGISTRY_ROOT = "DOWNLOADS"
# Clé ajoutée aux entrées de registre écrites par ces scripts
SYNTHETIC_FLAG = "synthetic"


# Fonction de garde d'isolation des catalogues
def ensure_isolated_catalog(dbname: str, safety: Optional[Mapping[str, Any]]) -> None:
    """Refuse to write simulated data outside an isolated (demo) catalog.

    Simulated data must never reach a production catalog: the scripts call this
    guard before any connection or network access.

    Args:
        dbname: PostgreSQL database of the catalog the script is about to write to
            (``DOWNLOADS.DBNAME`` of the dataset configuration).
        safety: ``SAFETY`` section of the synthetic configuration; its
            ``REQUIRED_CATALOG_PREFIX`` is the prefix an isolated catalog must
            carry (e.g. ``"demo_"``).

    Raises:
        RuntimeError: If the prefix is not configured, or ``dbname`` does not
            start with it (typically: the production profile is selected).

    Examples:
        >>> ensure_isolated_catalog("demo_comtrade", {"REQUIRED_CATALOG_PREFIX": "demo_"})
        >>> ensure_isolated_catalog("comtrade", {"REQUIRED_CATALOG_PREFIX": "demo_"})
        Traceback (most recent call last):
            ...
        RuntimeError: Refus d'écrire des données FICTIVES dans le catalogue 'comtrade' : ...
    """
    prefix = (safety or {}).get("REQUIRED_CATALOG_PREFIX")
    if not prefix:
        raise RuntimeError(
            "synthetic.SAFETY.REQUIRED_CATALOG_PREFIX absent de la configuration : "
            "refus d'écrire des données fictives sans garde d'isolation"
        )
    if not str(dbname).startswith(str(prefix)):
        raise RuntimeError(
            f"Refus d'écrire des données FICTIVES dans le catalogue '{dbname}' : seuls les "
            f"catalogues préfixés '{prefix}' sont autorisés (profil de production sélectionné ?)"
        )


# Fonction de chargement de la configuration des données fictives
def load_synthetic_config(config_path: Optional[os.PathLike] = None) -> Dict[str, Any]:
    """Load the ``synthetic`` section of the configuration file.

    Args:
        config_path: Path of the YAML file. When ``None``, uses the
            ``SYNTHETIC_CONFIG_PATH`` environment variable, else
            ``config/profiles/demo/synthetic.yaml``.

    Returns:
        The mapping under the ``synthetic`` root key.

    Raises:
        FileNotFoundError: If the file does not exist.
        KeyError: If the ``synthetic`` root key is missing.
    """
    if config_path is None:
        config_path = os.environ.get(SYNTHETIC_CONFIG_ENV, DEFAULT_SYNTHETIC_CONFIG)
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)["synthetic"]


# Fonction de lecture d'un échantillon de la table de faits d'un schéma
def read_table_sample(
    conn: Any, catalog_alias: str, schema: str, limit: int = 20
) -> Optional[pd.DataFrame]:
    """Read a few rows of a schema's fact table (``None`` when it does not exist).

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias under which the catalog is attached.
        schema: Schema holding the fact table.
        limit: Number of rows read.

    Returns:
        The sample, or ``None`` when the fact table does not exist yet.
    """
    if not fact_table_exists(conn, catalog_alias, schema):
        return None
    return conn.execute(f'SELECT * FROM "{schema}".{FACT_TABLE} LIMIT {int(limit)}').df()


# Fonction de lecture des codes distincts d'une colonne, restreints par motif
def read_distinct_codes(
    conn: Any, catalog_alias: str, schema: str, column: str, prefixes: Sequence[str]
) -> Sequence[str]:
    """Distinct values of a column starting with one of ``prefixes``.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias under which the catalog is attached.
        schema: Schema holding the fact table.
        column: Column to read (a dimension name from the configuration).
        prefixes: Value prefixes to look for (e.g. aggregate partner codes).

    Returns:
        Distinct values found (empty when the table does not exist).
    """
    if not fact_table_exists(conn, catalog_alias, schema) or not prefixes:
        return ()
    condition = " OR ".join(f'"{column}" LIKE ?' for _ in prefixes)
    rows = conn.execute(
        f'SELECT DISTINCT "{column}" FROM "{schema}".{FACT_TABLE} WHERE {condition}',
        [f"{prefix}%" for prefix in prefixes],
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


# Fonction d'alignement d'un DataFrame sur les colonnes et types d'une table
def align_to_dtypes(frame: pd.DataFrame, dtypes: Optional[Mapping[str, Any]]) -> pd.DataFrame:
    """Align a frame on the columns and dtypes of an existing table.

    Columns of the table absent from ``frame`` are added (null), extra columns
    are dropped, and dtypes are cast when possible (a failed cast is logged and
    the column kept as is).

    Args:
        frame: Rows to write.
        dtypes: Column → dtype of the existing table (``None`` or empty: no
            alignment, ``frame`` is returned unchanged).

    Returns:
        The aligned frame.

    Examples:
        >>> frame = pd.DataFrame({"a": ["1"], "b": [2]})
        >>> align_to_dtypes(frame, {"a": "int64", "c": "float64"}).dtypes.astype(str).to_dict()
        {'a': 'int64', 'c': 'float64'}
    """
    if not dtypes or frame.empty:
        return frame
    out = frame.copy()
    for column in dtypes:
        if column not in out.columns:
            out[column] = None
    out = out[list(dtypes)]
    for column, dtype in dtypes.items():
        try:
            out[column] = out[column].astype(dtype)
        except (TypeError, ValueError) as exc:
            logger.warning("Colonne %s non convertie en %s : %s", column, dtype, exc)
    return out


# Fonction de lecture du registre de téléchargement
def load_registry(last_download_path: os.PathLike, bucket: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """Load the ``statflows`` download registry (empty when absent).

    Args:
        last_download_path: Registry path.
        bucket: S3 bucket, or ``None`` for a local file.

    Returns:
        Mapping ``identity_key -> entry``.
    """
    data = Loader().load(Path(last_download_path), bucket=bucket, missing_ok=True) or {}
    return data.get(_REGISTRY_ROOT, {})


# Fonction de marquage des entrées de registre écrites par ces scripts
def mark_synthetic_entries(
    last_download_path: os.PathLike, bucket: Optional[str], started_at: datetime
) -> int:
    """Flag the registry entries written since ``started_at`` as synthetic.

    The flag (``"synthetic": true``) lets a later clean-up tell simulated
    downloads from real ones. ``statflows`` only reads ``last_download`` and
    ``params``, so the extra key is harmless.

    Args:
        last_download_path: Registry path.
        bucket: S3 bucket, or ``None`` for a local file.
        started_at: Start instant of the seeding run (timezone-aware).

    Returns:
        Number of entries flagged.
    """
    registry = load_registry(last_download_path, bucket)
    flagged = 0
    for entry in registry.values():
        written = _parse_iso(entry.get("last_download"))
        if written is not None and written >= started_at and not entry.get(SYNTHETIC_FLAG):
            entry[SYNTHETIC_FLAG] = True
            flagged += 1
    if flagged:
        Saver().save(
            Path(last_download_path), {_REGISTRY_ROOT: registry},
            bucket=bucket, indent=2, ensure_ascii=False,
        )
    logger.info("%d entrée(s) de registre marquée(s) comme fictives", flagged)
    return flagged
