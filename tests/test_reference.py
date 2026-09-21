"""Tests des référentiels (``kedro_pipeline/steps/reference.py``, PS-28.4, C-22)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from kedro_pipeline.steps.reference import (
    REFERENCE_COLUMNS,
    concordance_table,
    normalize_countries,
    normalize_products,
    publish_hs_reference,
    publish_reference,
    reference_schema,
    vintages_table,
)

from conftest import file_connector_factory

HS = {"HS2017": 2017, "HS2022": 2022}


def test_normalize_products_eurostat_codes() -> None:
    codelist = pd.DataFrame(
        {"code": ["85", "8541", "854110", "85411000", "TOTAL"],
         "name": ["Chapter 85", "Heading", "Subheading", "CN8 code", "Total"]}
    )
    out = normalize_products(codelist, year=2024, nomenclatures=HS).set_index("code")
    assert list(out.columns) == ["classification", "label", "level", "parent_code"]
    assert out.loc["854110", "classification"] == "HS2022"
    assert out.loc["85411000", "classification"] == "CN2024"
    assert out.loc["85411000", "parent_code"] == "854110"
    assert out.loc["8541", "parent_code"] == "85"
    assert pd.isna(out.loc["85", "parent_code"])
    assert pd.isna(out.loc["TOTAL", "level"])
    assert out.loc["TOTAL", "classification"] == "HS2022"


def test_normalize_products_comtrade_strips_code_prefix() -> None:
    codelist = pd.DataFrame(
        {"id": ["854110"], "text": ["854110 - Diodes, other"], "parent": ["8541"]}
    )
    row = normalize_products(codelist, year=2024, nomenclatures=HS).iloc[0]
    assert row["label"] == "Diodes, other"
    assert row["parent_code"] == "8541"
    assert row["level"] == 6


def test_normalize_countries_comtrade_and_eurostat() -> None:
    comtrade = pd.DataFrame(
        {"reporterCode": [251, 97], "reporterDesc": ["France", "EU"],
         "reporterCodeIsoAlpha3": ["FRA", "EUR"], "isGroup": [False, True]}
    )
    out = normalize_countries(comtrade, source="comtrade").set_index("code")
    assert out.loc["251", "iso3"] == "FRA"
    assert out.loc["251", "m49"] == "251"
    assert bool(out.loc["97", "is_aggregate"]) is True

    eurostat = pd.DataFrame({"code": ["FR", "EU27_2020", "QW"], "name": ["France", "EU", "Other"]})
    out = normalize_countries(
        eurostat, source="eurostat", aggregate_codes=["QW"], table="partners"
    ).set_index("code")
    assert out["is_aggregate"].to_dict() == {"FR": False, "EU27_2020": True, "QW": True}
    assert out["m49"].isna().all()
    assert set(out.reset_index().columns) == set(REFERENCE_COLUMNS["partners"])


def test_concordance_relationships_and_checksum() -> None:
    pair = pd.DataFrame(
        {"source_classification": "HS2022",
         "source_code": ["010121", "010129", "020110"],
         "target_classification": "HS2017",
         "target_code": ["010121", "010121", "020110"],
         "relationship": pd.NA}
    )
    out = concordance_table({("HS2022", "HS2017"): pair})
    assert out["relationship"].tolist() == ["n:1", "n:1", "1:1"]
    assert out["checksum"].nunique() == 1
    assert concordance_table({}).empty


def test_vintages_table() -> None:
    out = vintages_table({"HS1992": 1988, "HS1996": 1996, "HS2022": 2022})
    assert out["in_force_until"].tolist()[:2] == [1995, 2021]
    assert pd.isna(out["in_force_until"].iloc[-1])


def test_publish_reference_is_idempotent(tmp_path: Path) -> None:
    pytest.importorskip("dt_ducklake_manager")
    from kedro_pipeline.io.ducklake import DuckLakeLocation

    location = DuckLakeLocation("eurostat", "eurostat", "DS_045409", "b", "p")
    connector = file_connector_factory(tmp_path)(location, None, None)
    codelists = {
        "reporter": pd.DataFrame({"code": ["FR", "DE"], "name": ["France", "Germany"]}),
        "product": pd.DataFrame({"code": ["854110"], "name": ["Diodes"]}),
        "flow": pd.DataFrame({"code": ["1"], "name": ["Import"]}),  # dimension non publiée
    }
    params = {
        "SCHEMA_PREFIX": "reference",
        "DIMENSIONS": {"reporter": "reporters", "partner": "partners", "product": "products"},
        "NOMENCLATURES": HS,
        "YEAR": 2026,
    }
    first = publish_reference(codelists, connector, source="eurostat", params=params)
    assert first["failures"] == {}
    assert sorted(first["tables"]) == ["products", "reporters"]

    # Deuxième publication, libellé corrigé : upsert par clé, aucun doublon
    codelists["reporter"] = pd.DataFrame({"code": ["FR", "DE"], "name": ["France", "Deutschland"]})
    publish_reference(codelists, connector, source="eurostat", params=params)
    conn = connector.connect()
    try:
        rows = conn.execute(
            f'SELECT code, label FROM eurostat."{reference_schema("reference", "reporters")}".fact_table '
            "ORDER BY code"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("DE", "Deutschland"), ("FR", "France")]

    result = publish_hs_reference(
        {}, connector, params={"SCHEMA_PREFIX": "reference", "NOMENCLATURES": HS}
    )
    # Concordances vides : non écrites ; millésimes toujours publiés
    assert result["tables"] == ["hs_vintages"]
