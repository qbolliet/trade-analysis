"""Poignée d'écriture ``DuckLakeTable`` : évolution de schéma par l'API native.

Scénario complet sur un catalogue DuckLake fichier temporaire (fixture
``ducklake_conn``) : ajout d'une métrique (nouvelle colonne) par l'upsert,
upsert d'un DataFrame ne portant qu'un sous-ensemble des colonnes (constat
empirique : les valeurs stockées des colonnes absentes sont préservées sur les
lignes mises à jour, nulles sur les lignes insérées), diffusion d'une colonne
par ``add_columns``, remplacement transactionnel d'une tranche
(``upsert_many(delete_where=…)``) et traçabilité du snapshot.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("dt_ducklake_manager")

from kedro_pipeline.io.ducklake import DuckLakeTable, workflow_run_id  # noqa: E402

# Clé primaire de la table de test
KEYS = ["reporter", "product"]


def _read(conn, alias: str, schema: str = "s1") -> pd.DataFrame:
    """Relecture de la table de faits, triée par clé."""
    return (
        conn.execute(f'SELECT * FROM "{alias}"."{schema}"."fact_table"').df()
        .sort_values(KEYS).reset_index(drop=True)
    )


def _frame(reporters, products, **columns) -> pd.DataFrame:
    """Jeu de données à clé (reporter, product) et colonnes de valeurs données."""
    df = pd.DataFrame(
        [{"reporter": reporter, "product": product} for reporter in reporters for product in products]
    )
    for name, values in columns.items():
        df[name] = values
    return df


def test_new_metric_subset_upsert_and_add_columns(ducklake_conn) -> None:
    """Nouvelle colonne, upsert partiel, diffusion par add_columns."""
    conn, alias = ducklake_conn
    table = DuckLakeTable(conn, alias, "s1")

    # Création avec HHI seule
    first = _frame(["FR", "DE"], ["01", "02"], HHI=[0.1, 0.2, 0.3, 0.4])
    assert table.upsert(first, KEYS, run_id="wf-1", commit_message="create") is True
    assert table.exists()

    # Nouvelle métrique : colonne ajoutée, anciennes lignes nulles, nouvelles valorisées
    second = _frame(["IT"], ["01", "02"], HHI=[0.5, 0.6], NEW_METRIC=[1.0, 2.0])
    assert table.upsert(second, KEYS, run_id="wf-1", commit_message="new metric") is False
    stored = _read(conn, alias)
    assert "NEW_METRIC" in stored.columns
    assert stored.loc[stored["reporter"] != "IT", "NEW_METRIC"].isna().all()
    assert stored.loc[stored["reporter"] == "IT", "NEW_METRIC"].tolist() == [1.0, 2.0]

    # Upsert sans NEW_METRIC : valeurs stockées préservées sur les lignes mises à jour,
    # nulles sur la ligne insérée (constat empirique sur lequel repose la poignée)
    third = _frame(["IT", "ES"], ["01"], HHI=[0.9, 0.7])
    table.upsert(third, KEYS, run_id="wf-1", commit_message="subset")
    stored = _read(conn, alias).set_index(KEYS)
    assert stored.loc[("IT", "01"), "HHI"] == pytest.approx(0.9)
    assert stored.loc[("IT", "01"), "NEW_METRIC"] == pytest.approx(1.0)
    assert stored.loc[("IT", "02"), "NEW_METRIC"] == pytest.approx(2.0)
    assert np.isnan(stored.loc[("ES", "01"), "NEW_METRIC"])

    # Migration : MIGRATED sur la moitié des clés, les autres lignes nulles, aucun doublon
    keys = _read(conn, alias)[KEYS]
    half = keys.iloc[: len(keys) // 2].copy()
    half["MIGRATED"] = np.arange(len(half), dtype=float)
    table.add_columns(half, run_id="wf-1", commit_message="migration")
    stored = _read(conn, alias)
    assert len(stored) == len(keys)
    assert not stored.duplicated(subset=KEYS).any()
    migrated = stored.set_index(KEYS)["MIGRATED"]
    assert migrated.notna().sum() == len(half)
    # Colonne existante : refus sans overwrite
    with pytest.raises(ValueError):
        table.add_columns(half, run_id="wf-1")


def test_new_column_refused_without_allow_new_columns(ducklake_conn) -> None:
    conn, alias = ducklake_conn
    table = DuckLakeTable(conn, alias, "s1")
    table.upsert(_frame(["FR"], ["01"], HHI=[0.1]), KEYS)
    with pytest.raises(ValueError):
        table.upsert(_frame(["FR"], ["01"], HHI=[0.1], OTHER=[1.0]), KEYS, allow_new_columns=False)


def test_upsert_many_replaces_a_slice_atomically(ducklake_conn) -> None:
    """Suppression puis upsert dans une transaction : la tranche est remplacée."""
    conn, alias = ducklake_conn
    table = DuckLakeTable(conn, alias, "s1")
    table.upsert(_frame(["FR", "DE"], ["01", "02", "03"], HHI=np.arange(6, dtype=float)), KEYS)

    # Tranche FR remplacée par deux lots : FR/03 disparaît, FR/04 apparaît
    created = table.upsert_many(
        [_frame(["FR"], ["01"], HHI=[10.0]), _frame(["FR"], ["04"], HHI=[11.0])],
        KEYS, delete_where="\"reporter\" = 'FR'", run_id="wf-2", commit_message="replace FR",
    )
    assert created is False
    stored = _read(conn, alias)
    assert stored.loc[stored["reporter"] == "FR", "product"].tolist() == ["01", "04"]
    assert stored.loc[stored["reporter"] == "DE", "HHI"].tolist() == [3.0, 4.0, 5.0]

    # Échec au milieu (type incompatible) : annulation complète, tranche intacte
    before = _read(conn, alias)
    with pytest.raises(Exception):
        table.upsert_many(
            [_frame(["DE"], ["01"], HHI=[1.0]), _frame(["DE"], ["02"], HHI=["not a number"])],
            KEYS, delete_where="\"reporter\" = 'DE'",
        )
    pd.testing.assert_frame_equal(_read(conn, alias), before)


def test_upsert_with_delete_where_creates_missing_table(ducklake_conn) -> None:
    conn, alias = ducklake_conn
    table = DuckLakeTable(conn, alias, "s1")
    assert table.upsert(_frame(["FR"], ["01"], HHI=[0.1]), KEYS, delete_where="TRUE") is True
    assert len(_read(conn, alias)) == 1


def test_run_id_and_message_on_snapshot(ducklake_conn) -> None:
    """``run_id`` et ``commit_message`` figurent sur le snapshot DuckLake."""
    conn, alias = ducklake_conn
    table = DuckLakeTable(conn, alias, "s1")
    table.upsert(_frame(["FR"], ["01"], HHI=[0.1]), KEYS, run_id="wf-a", commit_message="first")
    table.upsert(_frame(["FR"], ["02"], HHI=[0.2]), KEYS, run_id="wf-b", commit_message="second")
    table.upsert_many(
        [_frame(["FR"], ["03"], HHI=[0.3])], KEYS, delete_where="FALSE",
        run_id="wf-c", commit_message="third",
    )
    snapshots = conn.execute(f"SELECT * FROM ducklake_snapshots('{alias}')").df()
    messages = set(snapshots["commit_message"].dropna())
    authors = set(snapshots["author"].dropna())
    assert {"first", "second", "third"} <= messages
    assert {"wf-a", "wf-b", "wf-c"} <= authors


def test_writer_binds_the_options(ducklake_conn) -> None:
    conn, alias = ducklake_conn
    write = DuckLakeTable(conn, alias, "s1").writer(run_id="wf-w", commit_message="bound")
    assert write(_frame(["FR"], ["01"], HHI=[0.1]), KEYS) is True
    assert write(_frame(["FR"], ["01"], HHI=[0.2]), KEYS) is False
    assert _read(conn, alias)["HHI"].tolist() == [0.2]


def test_workflow_run_id() -> None:
    assert workflow_run_id({"WORKFLOW_ID": "wf-7"}) == "wf-7"
    assert workflow_run_id({"WORKFLOW_ID": ""}) is None
