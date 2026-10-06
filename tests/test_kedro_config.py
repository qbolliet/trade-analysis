"""Tests des fonctions pures de millésimes (``kedro_pipeline/config.py``, PS-28.1).

Les macros SQL générées doivent rendre exactement le même résultat que les fonctions
Python sur toute la plage 1988 → 2030 : c'est ce qui garantit que la couche de service
dérive ``classification`` / ``hs_vintage`` comme le fera le pipeline (PS-29.3).
"""

from __future__ import annotations

import duckdb
import pytest

from kedro_pipeline.config import (
    classification_of,
    first_historical_year,
    historical_vintages,
    hs_vintage_of,
    nomenclature_macros_sql,
    product_code,
    requested_vintages,
    vintage_in_force,
)

from conftest import serving_configs

# Référentiel réel des millésimes (runtime.NOMENCLATURES.HS)
HS = serving_configs("base")["runtime"]["NOMENCLATURES"]["HS"]


def test_vintage_in_force_boundaries() -> None:
    assert vintage_in_force(1988, HS) == "HS1992"
    assert vintage_in_force(1995, HS) == "HS1992"
    assert vintage_in_force(1996, HS) == "HS1996"
    assert vintage_in_force(2021, HS) == "HS2017"
    assert vintage_in_force(2022, HS) == "HS2022"
    assert vintage_in_force(2030, HS) == "HS2022"


def test_vintage_in_force_before_first_vintage_raises() -> None:
    with pytest.raises(ValueError):
        vintage_in_force(1987, HS)
    with pytest.raises(ValueError):
        vintage_in_force(2000, {})


def test_historical_vintages_oldest_first_without_in_force() -> None:
    assert historical_vintages(2019, HS) == [
        "HS1992", "HS1996", "HS2002", "HS2007", "HS2012"
    ]
    assert historical_vintages(1990, HS) == []


def test_classification_of_hs_and_cn() -> None:
    assert classification_of("854110", 2019, HS) == "HS2017"
    assert classification_of("85", 2023, HS) == "HS2022"
    assert classification_of("85411000", 2019, HS) == "CN2019"
    # Code stocké en entier : zéro initial restitué avant de compter les chiffres
    assert classification_of(1012100, 2023, HS) == "CN2023"


def test_product_code_restores_leading_zero() -> None:
    assert product_code(1012100) == "01012100"
    assert product_code(10121) == "010121"
    assert product_code("854110") == "854110"
    assert product_code("TOTAL") == "TOTAL"


def test_sql_macros_match_python_functions() -> None:
    conn = duckdb.connect()
    for statement in nomenclature_macros_sql(HS):
        conn.execute(statement)
    for year in range(1988, 2031):
        sql_vintage, sql_hs6, sql_cn8 = conn.execute(
            "SELECT vintage_in_force(?), classification_of('854110', ?), "
            "classification_of(1012100, ?)",
            [year, year, year],
        ).fetchone()
        assert sql_vintage == vintage_in_force(year, HS)
        assert sql_hs6 == classification_of("854110", year, HS)
        assert sql_cn8 == classification_of(1012100, year, HS)
    # Année antérieure au premier millésime : NULL (la fonction Python lève)
    assert conn.execute("SELECT vintage_in_force(1980)").fetchone()[0] is None
    assert conn.execute("SELECT product_code(1012100), product_code('ALL')").fetchone() == (
        "01012100", "ALL"
    )


def test_requested_vintages_all_list_and_empty() -> None:
    # « all » : tous les millésimes sauf le plus récent, jamais historique
    assert requested_vintages("all", HS) == [
        "HS1992", "HS1996", "HS2002", "HS2007", "HS2012", "HS2017"
    ]
    # Liste : ordre du référentiel, pas celui de la configuration
    assert requested_vintages(["HS2017", "HS1992"], HS) == ["HS1992", "HS2017"]
    assert requested_vintages([], HS) == []
    assert requested_vintages(None, HS) == []


@pytest.mark.parametrize("spec", ["some", ["HS1900"], ["HS2022"]])
def test_requested_vintages_rejects_invalid(spec) -> None:
    with pytest.raises(ValueError):
        requested_vintages(spec, HS)


def test_first_historical_year_is_next_entry() -> None:
    assert first_historical_year("HS1992", HS) == 1996
    assert first_historical_year("HS2017", HS) == 2022
    with pytest.raises(ValueError):
        first_historical_year("HS2022", HS)


def test_hs_vintage_of_macro_matches_python() -> None:
    conn = duckdb.connect()
    for statement in nomenclature_macros_sql(HS):
        conn.execute(statement)
    for year in range(1988, 2031):
        for classification in (f"CN{year}", "HS2017", "HS1992"):
            sql = conn.execute("SELECT hs_vintage_of(?, ?)", [classification, year]).fetchone()[0]
            assert sql == hs_vintage_of(classification, year, HS)
