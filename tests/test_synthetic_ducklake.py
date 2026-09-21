"""Écriture des données fictives dans un catalogue DuckLake temporaire (comme le pipeline)."""

from __future__ import annotations

import pandas as pd
import pytest

from kedro_pipeline.synthetic.comext import ComextConfig, ComextTemplate, build_comext
from kedro_pipeline.synthetic.comtrade import ReportingConfig, SyntheticComtradeClient
from kedro_pipeline.synthetic.io import read_distinct_codes, read_table_sample
from statflows.core.download import _primary_keys
from statflows.storage.ducklake.tables import write_dataframe

pytestmark = pytest.mark.slow


def test_comtrade_rows_are_written_then_upserted(
    ducklake_conn, synthetic_world, synthetic_reference, synthetic_section
) -> None:
    """Les lignes du client fictif se créent puis s'upsertent sans doublon (clés = dimensions)."""
    from statflows import ComtradeQueryRequest

    conn, alias = ducklake_conn
    client = SyntheticComtradeClient(
        synthetic_world, synthetic_reference, ReportingConfig.from_mapping(synthetic_section["REPORTING"]),
    )
    query = ComtradeQueryRequest(
        dataflow="C_A_HS", periods="2020", products=["280519", "810510"],
        flows=["M", "X"], type_code="C", classification="HS", frequency="annual",
    )
    df = client.execute_query(query)
    keys = _primary_keys(client.resolve_query_structure(query), list(df.columns))
    assert {"reporterCode", "partnerCode", "cmdCode", "flowCode", "period"} <= set(keys)

    assert write_dataframe(conn, df, keys, catalog_alias=alias, schema="C_A_HS") is True
    assert write_dataframe(conn, df, keys, catalog_alias=alias, schema="C_A_HS") is False
    count = conn.execute('SELECT count(*) FROM "C_A_HS".fact_table').fetchone()[0]
    assert count == len(df)

    # Un second run aligné sur les types de la table existante écrit sans erreur
    sample = read_table_sample(conn, alias, "C_A_HS")
    aligned_client = SyntheticComtradeClient(
        synthetic_world, synthetic_reference, ReportingConfig.from_mapping(synthetic_section["REPORTING"]),
        force=True, table_dtypes=dict(sample.dtypes),
    )
    again = aligned_client.fetch_updates(query, None)
    write_dataframe(conn, again, keys, catalog_alias=alias, schema="C_A_HS")
    assert conn.execute('SELECT count(*) FROM "C_A_HS".fact_table').fetchone()[0] == len(df)


def test_comext_rows_follow_the_existing_table(
    ducklake_conn, synthetic_world, synthetic_section
) -> None:
    """Le schéma appris sur la table existante est respecté ; réécrire ne duplique pas."""
    conn, alias = ducklake_conn
    iso2 = {c.iso3: c.iso2 for c in synthetic_world.config.countries}
    dims = {"freq": "A", "partner": "*", "flow": ["1", "2"],
            "indicators": ["QUANTITY_IN_100KG", "VALUE_IN_EUROS"]}
    config = ComextConfig.from_mapping({})

    # Table « réelle » : quelques lignes, TIME_PERIOD entier, agrégat EXT_EU27_2020
    real = pd.DataFrame(
        {"freq": ["A", "A"], "reporter": ["FR", "FR"], "partner": ["EXT_EU27_2020", "CN"],
         "product": ["12345678", "12345678"], "flow": [1, 1], "indicators": ["VALUE_IN_EUROS"] * 2,
         "TIME_PERIOD": [2020, 2020], "OBS_VALUE": [10.0, 5.0]}
    )
    keys = ["freq", "reporter", "partner", "product", "flow", "indicators", "TIME_PERIOD"]
    write_dataframe(conn, real, keys, catalog_alias=alias, schema="comext")

    template = ComextTemplate.from_sample(
        read_table_sample(conn, alias, "comext"),
        partner_codes=read_distinct_codes(conn, alias, "comext", "partner", ["WORLD", "EXT_EU", "INT_EU"]),
    )
    assert template.aggregate_code("EXT_EU") == "EXT_EU27_2020"

    data = pd.concat(
        [build_comext(synthetic_world, iso2, config, template, reporter=r, product="81051000", dimensions=dims)
         for r in ("FR", "DE")],
        ignore_index=True,
    )
    assert data["TIME_PERIOD"].dtype == real["TIME_PERIOD"].dtype and data["flow"].dtype == real["flow"].dtype
    write_dataframe(conn, data, keys, catalog_alias=alias, schema="comext")
    write_dataframe(conn, data, keys, catalog_alias=alias, schema="comext")
    n = conn.execute('SELECT count(*) FROM comext.fact_table').fetchone()[0]
    assert n == len(data) + len(real)


def test_comext_rows_feed_the_partner_vulnerabilities(
    ducklake_conn, synthetic_world
) -> None:
    """Les lignes Comext fictives alimentent les métriques partenaires (HHI, CDI2, CDI3)."""
    from macroforecast.trade.vulnerabilities.runner import run_vulnerabilities

    conn, alias = ducklake_conn
    iso2 = {c.iso3: c.iso2 for c in synthetic_world.config.countries}
    dims = {"freq": "A", "partner": "*", "flow": ["1", "2"],
            "indicators": ["QUANTITY_IN_100KG", "VALUE_IN_EUROS"]}
    frames = [
        build_comext(synthetic_world, iso2, ComextConfig.from_mapping({}), ComextTemplate(),
                     reporter=reporter, product=product, dimensions=dims)
        for reporter in ("FR", "DE", "IT", "EU27_2020")
        for product in ("28051910", "81051000", "85411000")
    ]
    data = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    keys = ["freq", "reporter", "partner", "product", "flow", "indicators", "TIME_PERIOD"]
    write_dataframe(conn, data, keys, catalog_alias=alias, schema="DS_045409")

    report = run_vulnerabilities(
        conn, source_catalog_alias=alias, source_schema="DS_045409", result_schema="indicators",
    )
    result = conn.execute('SELECT * FROM "indicators".fact_table').df()
    assert {"HHI", "CDI2", "CDI3"} <= set(result.columns)
    assert result["HHI"].between(0, 1.0 + 1e-9).all()
    # Part extra-UE bornée dans [0, 1] : les agrégats fictifs sont cohérents avec WORLD
    assert result["CDI2"].dropna().between(0, 1.0 + 1e-9).all()
    assert len(result) > 20
