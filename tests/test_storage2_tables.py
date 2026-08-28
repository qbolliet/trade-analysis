"""Tests de caractérisation — ``macroforecast.storage2.tables``.

Comportement figé : ``fact_table_exists`` sur un DuckDB en mémoire, et
``write_dataframe`` sur un catalogue ``.ducklake`` fichier temp (création au
premier appel → ``True``, upsert au second → ``False``, lignes préservées).
"""

from __future__ import annotations

import pandas as pd
import pytest

from macroforecast.storage2.tables import FACT_TABLE, fact_table_exists, write_dataframe


# ──────────────────────────────────────────────────────────────────────
# fact_table_exists (DuckDB en mémoire)
# ──────────────────────────────────────────────────────────────────────


def test_fact_table_exists_in_memory() -> None:
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        # Table absente → False
        assert fact_table_exists(conn, "memory", "main", table="t") is False
        # Défaut keyword-only : cherche "fact_table"
        assert FACT_TABLE == "fact_table"
        assert fact_table_exists(conn, "memory", "main") is False

        conn.execute("CREATE TABLE t (a INTEGER)")

        # Table présente → True
        assert fact_table_exists(conn, "memory", "main", table="t") is True
        # Mauvais catalogue / schéma → False
        assert fact_table_exists(conn, "autre", "main", table="t") is False
        assert fact_table_exists(conn, "memory", "autre", table="t") is False
    finally:
        conn.close()


def test_fact_table_exists_table_is_keyword_only() -> None:
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        with pytest.raises(TypeError):
            fact_table_exists(conn, "memory", "main", "t")  # type: ignore[misc]
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────
# write_dataframe (catalogue .ducklake fichier temp)
# ──────────────────────────────────────────────────────────────────────


def test_write_dataframe_create_then_upsert(ducklake_conn) -> None:
    conn, catalog_alias = ducklake_conn

    batch1 = pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
    batch2 = pd.DataFrame({"id": [2, 4], "val": [999, 40]})

    # Premier appel : la table de faits n'existe pas → création → True
    created = write_dataframe(
        conn, batch1, ["id"], catalog_alias=catalog_alias, schema="s1"
    )
    assert created is True
    assert fact_table_exists(conn, catalog_alias, "s1") is True

    # Second appel : la table existe → upsert → False
    upserted = write_dataframe(
        conn, batch2, ["id"], catalog_alias=catalog_alias, schema="s1"
    )
    assert upserted is False

    # Lignes préservées : id 1 et 3 intacts, id 2 mis à jour, id 4 inséré
    rows = conn.execute(
        f"SELECT id, val FROM {catalog_alias}.s1.{FACT_TABLE} ORDER BY id"
    ).fetchall()
    assert rows == [(1, 10), (2, 999), (3, 30), (4, 40)]
