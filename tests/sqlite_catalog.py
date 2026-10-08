"""Catalogue DuckLake local à métadonnées SQLite, ouvrable par plusieurs processus.

Un catalogue DuckLake dont les métadonnées sont dans un fichier DuckDB n'admet qu'un
processus à la fois ; avec SQLite, les workers d'un test de parallélisme peuvent lire
pendant que le processus parent écrit.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

# Alias du catalogue attaché
ALIAS = "db"


def open_catalog(catalog_path: str, data_path: str, alias: str = ALIAS):
    """Open a DuckDB connection attached to the local SQLite-backed DuckLake catalog.

    Args:
        catalog_path: Path of the SQLite metadata file.
        data_path: Directory of the Parquet data files.
        alias: Alias of the attached catalog.

    Returns:
        The open connection (``pytest.skip`` when the extensions are unavailable).
    """
    conn = duckdb.connect(":memory:")
    try:
        for extension in ("ducklake", "sqlite"):
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")
    except duckdb.Error as exc:  # extension indisponible hors-ligne
        conn.close()
        pytest.skip(f"extension duckdb indisponible : {exc}")
    # Journal WAL et attente des verrous : lecteurs des workers et écrivain du parent
    # se croisent sur le même fichier SQLite
    conn.execute(
        f"ATTACH 'ducklake:sqlite:{catalog_path}' AS {alias} "
        f"(DATA_PATH '{data_path}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 60000)"
    )
    return conn


class SqliteCatalogConnector:
    """Picklable connector of the local catalog (``connect()`` opens a connection)."""

    def __init__(self, catalog_path: str, data_path: str, alias: str = ALIAS) -> None:
        self.catalog_path = catalog_path
        self.data_path = data_path
        self.alias = alias
        self.catalog_alias = alias

    def connect(self):
        return open_catalog(self.catalog_path, self.data_path, self.alias)


def catalog_paths(root: Path) -> tuple:
    """Return the ``(catalog_path, data_path)`` of the catalog under ``root``."""
    return (root / "catalog.sqlite").as_posix(), (root / "data").as_posix()
