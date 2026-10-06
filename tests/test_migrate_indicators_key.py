"""Tests de l'outil de recréation de ``indicators`` avec la clé de nomenclature.

Catalogue DuckLake fichier (fixture ``ducklake_conn``) : une table partenaires à
l'ancienne clé est recréée ; même nombre de lignes, clé unique, colonnes dérivées
du référentiel des millésimes, partition par classification, idempotence.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("dt_ducklake_manager")

pytestmark = pytest.mark.slow

# Chargement du module outil (dossier tools/ hors paquets)
_SPEC = importlib.util.spec_from_file_location(
    "migrate_indicators_key", Path(__file__).resolve().parents[1] / "tools" / "migrate_indicators_key.py"
)
migrate = importlib.util.module_from_spec(_SPEC)
# Module enregistré avant exécution : les dataclasses résolvent leur module
sys.modules[_SPEC.name] = migrate
_SPEC.loader.exec_module(migrate)

HS = {"HS1992": 1988, "HS2017": 2017, "HS2022": 2022}
OLD_KEYS = ["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD"]


def _legacy_indicators() -> pd.DataFrame:
    """Lignes partenaires à l'ancienne clé : codes SH6 et NC8 stockés en entiers."""
    rng = np.random.default_rng(1)
    rows = [
        {"freq": "A", "reporter": reporter, "product": product, "flow": flow,
         "indicators": "VALUE_IN_EUROS", "TIME_PERIOD": period,
         "HHI": float(rng.uniform()), "HHI_ALERT": False}
        for reporter in ("FR", "DE", "IT")
        for product in (854110, 10121, 85411000, 1012100)
        for flow in (1, 2)
        for period in ("2019", "2021", "2023")
    ]
    return pd.DataFrame(rows)


def test_migration_keeps_rows_and_derives_nomenclature(ducklake_conn) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    df_old = _legacy_indicators()
    write_dataframe(conn, df_old, OLD_KEYS, catalog_alias=alias, schema="indicators")

    # Aperçu sans écriture
    planned = migrate.migrate_indicators(
        conn, catalog_alias=alias, schema="indicators", nomenclatures=HS,
        key_columns=OLD_KEYS, is_provisional=True, dry_run=True,
    )
    assert planned.status == "planned" and planned.n_rows == len(df_old) and planned.n_batches == 3
    assert "classification" not in migrate.table_columns(conn, alias, "indicators")

    report = migrate.migrate_indicators(
        conn, catalog_alias=alias, schema="indicators", nomenclatures=HS,
        key_columns=OLD_KEYS, is_provisional=True,
    )
    assert report.status == "migrated" and report.backup_schema is None
    assert report.primary_keys == ["classification", *OLD_KEYS]
    assert migrate.primary_keys(conn, alias, "indicators") == ["classification", *OLD_KEYS]

    df_new = conn.execute(f'SELECT * FROM "{alias}"."indicators"."fact_table"').df()
    # Même nombre de lignes, clé unique, valeurs recopiées
    assert len(df_new) == len(df_old)
    assert not df_new.duplicated(["classification", *OLD_KEYS]).any()
    merged = df_old.merge(df_new, on=OLD_KEYS, suffixes=("_old", ""))
    assert len(merged) == len(df_old)
    np.testing.assert_allclose(merged["HHI_old"], merged["HHI"])
    # Colonnes dérivées du référentiel : millésime en vigueur, CN<année> pour NC8
    row = df_new.set_index(["reporter", "product", "flow", "TIME_PERIOD"]).sort_index()
    assert row.loc[("FR", 854110, 1, "2019"), "classification"] == "HS2017"
    assert row.loc[("FR", 10121, 1, "2023"), "classification"] == "HS2022"
    assert row.loc[("FR", 1012100, 1, "2021"), "classification"] == "CN2021"
    assert row.loc[("FR", 1012100, 1, "2021"), "hs_vintage"] == "HS2017"
    assert df_new["in_force"].all() and df_new["is_provisional"].all()
    # Sauvegarde supprimée
    assert migrate.schema_tables(conn, alias, "indicators__before_classification") == []

    # Idempotence : une seconde exécution ne touche à rien
    again = migrate.migrate_indicators(
        conn, catalog_alias=alias, schema="indicators", nomenclatures=HS,
        key_columns=OLD_KEYS, is_provisional=True,
    )
    assert again.status == "already_migrated"


def test_migration_partitions_by_classification(tmp_path: Path) -> None:
    """Sans inlining, les fichiers de la table recréée sont rangés par classification."""
    import duckdb
    from statflows.storage.ducklake.tables import write_dataframe

    conn = duckdb.connect()
    conn.execute("LOAD ducklake")
    conn.execute(
        f"ATTACH 'ducklake:{(tmp_path / 'c.ducklake').as_posix()}' AS db "
        f"(DATA_PATH '{(tmp_path / 'data').as_posix()}/', DATA_INLINING_ROW_LIMIT 0)"
    )
    write_dataframe(conn, _legacy_indicators(), OLD_KEYS, catalog_alias="db", schema="indicators")
    migrate.migrate_indicators(
        conn, catalog_alias="db", schema="indicators", nomenclatures=HS,
        key_columns=OLD_KEYS, is_provisional=False, keep_backup=True,
    )
    files = conn.execute(
        "SELECT data_file FROM ducklake_list_files('db', 'fact_table', schema => 'indicators')"
    ).df()["data_file"]
    assert files.str.contains("classification=").all()
    # Sauvegarde conservée sur demande
    assert "fact_table" in migrate.schema_tables(conn, "db", "indicators__before_classification")
    conn.close()


def test_backfill_network_in_force_and_drop_dependent(ducklake_conn) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    df_network = pd.DataFrame(
        {"classification": ["HS2017", "HS2017", "HS2022"], "product": ["854110"] * 3,
         "year": [2019, 2023, 2023], "flow": [1, 1, 1], "SPOF": [0.1, 0.2, 0.3]}
    )
    write_dataframe(conn, df_network, ["classification", "product", "year", "flow"],
                    catalog_alias=alias, schema="network_indicators")
    assert migrate.backfill_network_in_force(
        conn, catalog_alias=alias, schema="network_indicators", nomenclatures=HS
    ) == 3
    flags = conn.execute(
        f'SELECT classification, year, in_force FROM "{alias}"."network_indicators"."fact_table" '
        "ORDER BY ALL"
    ).fetchall()
    assert flags == [("HS2017", 2019, True), ("HS2017", 2023, False), ("HS2022", 2023, True)]
    # Idempotence
    assert migrate.backfill_network_in_force(
        conn, catalog_alias=alias, schema="network_indicators", nomenclatures=HS
    ) == 0

    # Tables dépendantes supprimées avec leur schéma
    write_dataframe(conn, df_network, ["classification", "product", "year", "flow"],
                    catalog_alias=alias, schema="synthesis")
    dropped = migrate.drop_schema_tables(conn, alias, "synthesis")
    assert "fact_table" in dropped
    assert migrate.schema_tables(conn, alias, "synthesis") == []
