"""Tests des lignes tariffline fictives et du client Comtrade sans réseau."""

from __future__ import annotations

import itertools
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from kedro_pipeline.synthetic.comtrade import (
    NES_CODE,
    TARIFFLINE_COLUMNS,
    WORLD_CODE,
    ReportingConfig,
    SyntheticComtradeClient,
    build_tariffline,
)

PRODUCTS = ["280519", "810510", "854110"]


def _tariffline(world, reference, section, year=2020, products=PRODUCTS, **kwargs):
    reporting = ReportingConfig.from_mapping(section["REPORTING"])
    return build_tariffline(world, reference, reporting, year, products, **kwargs)


def test_columns_and_types(synthetic_world, synthetic_reference, synthetic_section) -> None:
    """Colonnes et types du fait tariffline ; année et nomenclature renseignées."""
    df = _tariffline(synthetic_world, synthetic_reference, synthetic_section)
    assert list(df.columns) == list(TARIFFLINE_COLUMNS)
    assert (df["period"] == 2020).all() and (df["classificationCode"] == "H5").all()
    assert set(df["flowCode"]) == {"M", "X"}
    assert df["reporterCode"].dtype == "int64" and df["partnerCode"].dtype == "int64"
    # Import valorisé CIF (sauf déclarants FOB), export FOB
    exports = df[(df["flowCode"] == "X") & ~df["isAggregate"]]
    assert (exports["cifvalue"] == 0).all() and (exports["fobvalue"] > 0).all()
    fob_reporters = df[(df["reporterISO"] == "CAN") & (df["flowCode"] == "M") & ~df["isAggregate"]]
    assert (fob_reporters["cifvalue"] == 0).all()


def test_mirror_flows_disagree_like_real_ones(synthetic_world, synthetic_reference, synthetic_section) -> None:
    """Les imports CIF dépassent en moyenne les exports FOB miroirs (majoration de fret)."""
    df = _tariffline(synthetic_world, synthetic_reference, synthetic_section, products=["280519"])
    df = df[~df["isAggregate"] & (df["partnerCode"] != NES_CODE)]
    exports = df[df["flowCode"] == "X"].set_index(["reporterISO", "partnerISO"])["primaryValue"]
    imports = df[df["flowCode"] == "M"].set_index(["partnerISO", "reporterISO"])["primaryValue"]
    both = pd.concat([exports.rename("x"), imports.rename("m")], axis=1, join="inner")
    assert len(both) > 5
    assert (both["m"] / both["x"]).median() > 1.0


def test_world_rows_are_sum_of_declared_lines(synthetic_world, synthetic_reference, synthetic_section) -> None:
    """La ligne « Monde » d'un déclarant est la somme de ses lignes (nes incluses)."""
    df = _tariffline(synthetic_world, synthetic_reference, synthetic_section, products=["280519"])
    world = df[df["partnerCode"] == WORLD_CODE].set_index(["reporterISO", "flowCode"])["primaryValue"]
    rest = df[df["partnerCode"] != WORLD_CODE].groupby(["reporterISO", "flowCode"])["primaryValue"].sum()
    assert np.allclose(world.sort_index(), rest.sort_index())


def test_nes_and_world_rows_can_be_disabled(synthetic_world, synthetic_reference, synthetic_section) -> None:
    """NES_SHARE nul et WORLD_ROWS faux suppriment les lignes d'agrégat."""
    section = {**synthetic_section, "REPORTING": {"NES_SHARE": 0.0, "WORLD_ROWS": False}}
    df = _tariffline(synthetic_world, synthetic_reference, section)
    assert not df["partnerCode"].isin([NES_CODE, WORLD_CODE]).any()


def test_query_filters(synthetic_world, synthetic_reference, synthetic_section) -> None:
    """Filtres de la requête : déclarants, partenaires, flux, produits."""
    everything = _tariffline(synthetic_world, synthetic_reference, synthetic_section, products=["280519"])
    line = everything[(everything["flowCode"] == "M") & ~everything["isAggregate"]].iloc[0]
    df = _tariffline(
        synthetic_world, synthetic_reference, synthetic_section,
        reporters=[int(line["reporterCode"])], partners=[int(line["partnerCode"])],
        flows=["M"], products=["280519"],
    )
    assert not df.empty
    assert set(df["reporterCode"]) == {line["reporterCode"]} and set(df["partnerCode"]) == {line["partnerCode"]}
    assert set(df["flowCode"]) == {"M"} and set(df["cmdCode"]) == {"280519"}


def test_generation_is_independent_of_batching(synthetic_world, synthetic_reference, synthetic_section) -> None:
    """Un produit donne les mêmes lignes seul ou dans un lot (graine par produit-année)."""
    alone = _tariffline(synthetic_world, synthetic_reference, synthetic_section, products=["810510"])
    batch = _tariffline(synthetic_world, synthetic_reference, synthetic_section)
    from_batch = batch[batch["cmdCode"] == "810510"].reset_index(drop=True)
    pd.testing.assert_frame_equal(alone, from_batch)


def test_year_outside_range_is_empty(synthetic_world, synthetic_reference, synthetic_section) -> None:
    """Une année hors bornes ne renvoie rien (colonnes conservées)."""
    df = _tariffline(synthetic_world, synthetic_reference, synthetic_section, year=1999)
    assert df.empty and list(df.columns) == list(TARIFFLINE_COLUMNS)


def test_client_answers_requests_without_network(
    synthetic_world, synthetic_reference, synthetic_section
) -> None:
    """``execute_query`` du client fictif : aucun appel HTTP, doublons agrégés sur les clés."""
    from statflows import ComtradeQueryRequest

    client = SyntheticComtradeClient(
        synthetic_world, synthetic_reference, ReportingConfig.from_mapping(synthetic_section["REPORTING"]),
    )
    query = ComtradeQueryRequest(
        dataflow="C_A_HS", periods="2020", products=["280519", "810510"],
        flows=["M", "X"], type_code="C", classification="HS", frequency="annual",
    )
    df = client.execute_query(query)
    assert set(df["cmdCode"]) == {"280519", "810510"} and (df["period"] == 2020).all()
    assert client.api_calls == 0
    # Requête déjà téléchargée : pas de régénération (sauf force)
    import datetime as dt

    assert client.fetch_updates(query, dt.datetime.now(dt.timezone.utc)).empty
    forced = SyntheticComtradeClient(
        synthetic_world, synthetic_reference, ReportingConfig.from_mapping(synthetic_section["REPORTING"]),
        force=True,
    )
    assert not forced.fetch_updates(query, dt.datetime.now(dt.timezone.utc)).empty


def test_baci_runs_on_synthetic_declarations(
    synthetic_world, synthetic_reference, synthetic_section
) -> None:
    """Le redressement BACI complet s'exécute sur les déclarations fictives (6 étapes)."""
    from macroforecast.trade.processing import BaciConfig, required_columns, run_baci

    section = {**synthetic_section, "REPORTING": {**synthetic_section["REPORTING"], "REPORT_PROBABILITY": 0.95}}
    frames = [
        _tariffline(synthetic_world, synthetic_reference, section, year=year,
                    products=["280519", "284690", "810510", "810520", "854110", "854231", "300490", "281000"])
        for year in (2019, 2020, 2021)
    ]
    declarations = pd.concat(frames, ignore_index=True)
    config = replace(
        BaciConfig(), min_mirror_flows=5, fas_countries=("CAN",), apply_nes=True,
    )
    isos = list(synthetic_world.iso3)
    rng = np.random.default_rng(0)
    dist = pd.DataFrame(list(itertools.permutations(isos, 2)), columns=["iso_o", "iso_d"])
    dist["distw"] = rng.uniform(500, 15000, len(dist))
    dist["contig"] = 0
    geo = pd.DataFrame({"iso3": isos, "landlocked": 0})

    result, report = run_baci(
        df_comtrade=declarations[required_columns(config)], df_dist=dist, df_geo=geo, config=config
    )
    assert report.flows == len(result) > 100
    assert {"exporter", "importer", "product", "year", "reconciled_value"} <= set(result.columns)
    assert (result["reconciled_value"] >= 0).all()
