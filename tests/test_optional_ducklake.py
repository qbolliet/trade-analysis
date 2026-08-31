"""Tests du caractère optionnel de la dépendance ``dt_ducklake_manager``.

Seule l'écriture DuckLake (``write_dataframe``) requiert le paquet : le reste du
package doit s'importer et fonctionner sans lui. L'absence du module est simulée
en plaçant ``None`` dans ``sys.modules`` — la machinerie d'importation de CPython
lève alors ``ImportError`` sur toute tentative d'importation.
"""

from __future__ import annotations

import importlib
import sys

import pytest


# Fixture de simulation de l'absence de dt_ducklake_manager
@pytest.fixture
def ducklake_manager_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rend ``dt_ducklake_manager`` non importable pour la durée du test.

    ``monkeypatch`` restaure ``sys.modules`` au démontage, y compris si le paquet
    était réellement installé et déjà chargé.
    """
    monkeypatch.setitem(sys.modules, "dt_ducklake_manager", None)
    # Sous-modules éventuellement déjà en cache : sans purge, un
    # `from dt_ducklake_manager.x import y` continuerait de résoudre
    for name in [k for k in sys.modules if k.startswith("dt_ducklake_manager.")]:
        monkeypatch.delitem(sys.modules, name)


# ──────────────────────────────────────────────────────────────────────
# Importation du package
# ──────────────────────────────────────────────────────────────────────


def test_import_macroforecast_without_ducklake_manager(
    ducklake_manager_absent: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``import macroforecast`` réussit sans la dépendance optionnelle."""
    # Purge du cache : le package est déjà importé par le reste de la suite,
    # l'importation doit être rejouée à froid pour être significative.
    # `monkeypatch` réinstalle les modules d'origine au démontage.
    for name in [
        k for k in sys.modules if k == "macroforecast" or k.startswith("macroforecast.")
    ]:
        monkeypatch.delitem(sys.modules, name)

    module = importlib.import_module("macroforecast")

    # Les symboles de calcul restent exposés
    assert hasattr(module, "run_vulnerabilities")
    assert hasattr(module, "run_baci")

    # Loader / Saver tabulaires accessibles, helpers DuckLake sans écriture aussi
    storage = importlib.import_module("macroforecast.storage")
    assert storage.__all__ == ["Loader", "Saver"]

    tables = importlib.import_module("statflows.storage.ducklake.tables")
    assert tables.FACT_TABLE == "fact_table"
    assert callable(tables.fact_table_exists)


# ──────────────────────────────────────────────────────────────────────
# write_dataframe
# ──────────────────────────────────────────────────────────────────────


def test_write_dataframe_raises_explicit_import_error(
    ducklake_manager_absent: None,
) -> None:
    """``write_dataframe`` lève une ``ImportError`` nommant l'extra à installer."""
    from statflows.storage.ducklake.tables import write_dataframe

    # L'importation est en tête de corps : l'échec précède tout usage de la
    # connexion, `conn` et `data` peuvent donc valoir None.
    with pytest.raises(ImportError, match=r"statflows\[ducklake\]") as excinfo:
        write_dataframe(None, None, ["id"], catalog_alias="db", schema="s1")

    assert "dt-ducklake-manager" in str(excinfo.value)
