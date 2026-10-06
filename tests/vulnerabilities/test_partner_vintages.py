"""Lignes partenaires par millésime de nomenclature : conversion et identité.

Fixture à deux millésimes (HS2017, en vigueur 2017-2021 ; HS2022, depuis 2022) et
une table de passage HS2022 -> HS2017 couvrant les trois cas :

- code **stable** : ``854110`` -> ``854110`` ;
- code **scindé** en HS2022 (``854140`` devenu ``854141`` et ``854142``) : la
  conversion descendante est ``n:1``, les valeurs se somment exactement ;
- codes **fusionnés** en HS2022 (``030111`` et ``030119`` devenus ``030110``) :
  la conversion descendante est ``1:n`` ; la table UNSD de conversion désigne un
  seul code ancien (``030111``), qui reçoit la valeur entière, sans ventilation.
"""

from __future__ import annotations

from datetime import datetime, timezone

import narwhals as nw
import numpy as np
import pandas as pd
import pytest

from macroforecast.trade.processing import (
    HsHarmonizer,
    conversion_preimage,
    harmonize_partner_flows,
)
from macroforecast.trade.vulnerabilities import VulnerabilityConfig, default_metrics
from macroforecast.trade.vulnerabilities.runner import compute_vulnerabilities
from scripts.compute_trade_vulnerabilities import (
    annotate_nomenclature,
    historical_conversions,
    historical_requested,
    historical_units,
    nomenclature_config,
    prepare_vintage_flows,
)

HS = {"HS2017": 2017, "HS2022": 2022}
CONFIG = VulnerabilityConfig()
PARTNERS = ("CN", "US", "JP")
# Table de passage HS2022 -> HS2017 (feuille « Conversion » : une fonction)
TABLE = pd.DataFrame(
    {
        "source_classification": "HS2022",
        "source_code": ["854110", "854141", "854142", "030110"],
        "target_classification": "HS2017",
        "target_code": ["854110", "854140", "854140", "030111"],
    }
)
CONCORDANCES = {("HS2022", "HS2017"): TABLE}
# Codes déclarés par période (millésime en vigueur)
CODES = {
    "2019": (854110, 854140, 30111, 30119),
    "2023": (854110, 854141, 854142, 30110),
}


def _flows() -> pd.DataFrame:
    """Flux Comext fictifs, codes stockés en entiers, agrégats WORLD et EXT_EU compris."""
    rng = np.random.default_rng(3)
    rows = []
    for period, codes in CODES.items():
        for product in codes:
            for flow in (1, 2):
                values = rng.uniform(1.0, 100.0, len(PARTNERS))
                cell = [*zip(PARTNERS, values), ("WORLD", values.sum() * 1.1),
                        ("EXT_EU", values.sum() * 0.7)]
                rows += [
                    {"freq": "A", "reporter": "FR", "product": product, "flow": flow,
                     "indicators": "VALUE_IN_EUROS", "TIME_PERIOD": period,
                     "partner": partner, "OBS_VALUE": float(value)}
                    for partner, value in cell
                ]
    return pd.DataFrame(rows)


def _scores(df_prepared: pd.DataFrame) -> pd.DataFrame:
    """Métriques des deux sens sur des flux préparés (classification en clé)."""
    config = nomenclature_config(CONFIG)
    flows = ("import", "export")
    result, _ = compute_vulnerabilities(
        nw.from_native(df_prepared, eager_only=True), default_metrics(config, flows), config,
        flows=flows,
    )
    return result.to_native().sort_values(list(config.key_columns)).reset_index(drop=True)


def _hs6_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    return df[(df["TIME_PERIOD"] == period)].reset_index(drop=True)


def test_rows_in_force_are_the_identity_conversion() -> None:
    """Pour une période, la ligne en vigueur est la ligne du millésime en vigueur."""
    flows_2019 = _hs6_period(_flows(), "2019")
    in_force = prepare_vintage_flows(
        flows_2019, target_vintage=None, nomenclatures=HS, concordances={}, config=CONFIG
    )
    same_vintage = prepare_vintage_flows(
        flows_2019, target_vintage="HS2017", nomenclatures=HS, concordances=CONCORDANCES,
        config=CONFIG,
    )
    assert set(in_force["classification"]) == {"HS2017"}
    pd.testing.assert_frame_equal(_scores(in_force), _scores(same_vintage))


def test_split_code_sums_exactly_and_stable_code_is_unchanged() -> None:
    flows_2023 = _hs6_period(_flows(), "2023")
    converted = prepare_vintage_flows(
        flows_2023, target_vintage="HS2017", nomenclatures=HS, concordances=CONCORDANCES,
        config=CONFIG,
    )
    assert set(converted["classification"]) == {"HS2017"}
    # Codes stockés en entiers : le type de la colonne est conservé
    assert converted["product"].dtype == flows_2023["product"].dtype

    keys = ["flow", "partner"]
    # n:1 : 854141 + 854142 -> 854140, somme exacte par partenaire
    expected = (
        flows_2023[flows_2023["product"].isin([854141, 854142])].groupby(keys)["OBS_VALUE"].sum()
    )
    actual = converted[converted["product"] == 854140].set_index(keys)["OBS_VALUE"]
    pd.testing.assert_series_equal(actual.sort_index(), expected.sort_index(), check_names=False)

    # Code stable : métriques identiques entre le millésime en vigueur et l'historique
    in_force = prepare_vintage_flows(
        flows_2023, target_vintage=None, nomenclatures=HS, concordances={}, config=CONFIG
    )
    metrics = ["HHI", "CDI2", "CDI3"]
    stable_in_force = _scores(in_force).query("product == 854110")[metrics].reset_index(drop=True)
    stable_historical = _scores(converted).query("product == 854110")[metrics].reset_index(drop=True)
    pd.testing.assert_frame_equal(stable_in_force, stable_historical)


def test_merged_codes_go_whole_to_the_designated_target() -> None:
    """1:n : 030110 (HS2022) -> 030111 en totalité ; 030119 n'est pas alimenté."""
    flows_2023 = _hs6_period(_flows(), "2023")
    converted = prepare_vintage_flows(
        flows_2023, target_vintage="HS2017", nomenclatures=HS, concordances=CONCORDANCES,
        config=CONFIG,
    )
    assert 30119 not in set(converted["product"])
    keys = ["flow", "partner"]
    expected = flows_2023[flows_2023["product"] == 30110].set_index(keys)["OBS_VALUE"]
    actual = converted[converted["product"] == 30111].set_index(keys)["OBS_VALUE"]
    pd.testing.assert_series_equal(actual.sort_index(), expected.sort_index())


def test_individual_partners_never_exceed_world_after_conversion() -> None:
    converted = prepare_vintage_flows(
        _hs6_period(_flows(), "2023"), target_vintage="HS2017", nomenclatures=HS,
        concordances=CONCORDANCES, config=CONFIG,
    )
    cell = ["product", "flow", "TIME_PERIOD"]
    individual = converted[converted["partner"].isin(PARTNERS)].groupby(cell)["OBS_VALUE"].sum()
    world = converted[converted["partner"] == "WORLD"].set_index(cell)["OBS_VALUE"]
    assert (individual <= world.loc[individual.index] + 1e-9).all()


def test_mixed_periods_convert_each_from_its_own_vintage() -> None:
    """Toutes périodes confondues : 2019 (déjà HS2017) intact, 2023 converti."""
    converted = prepare_vintage_flows(
        _flows(), target_vintage="HS2017", nomenclatures=HS, concordances=CONCORDANCES,
        config=CONFIG,
    )
    assert set(converted.loc[converted["TIME_PERIOD"] == "2019", "product"]) == set(CODES["2019"])
    assert set(converted.loc[converted["TIME_PERIOD"] == "2023", "product"]) == {854110, 854140, 30111}


def test_in_force_classification_marks_cn_codes() -> None:
    flows = pd.DataFrame(
        {"freq": "A", "reporter": "FR", "product": [854110, 85411000], "flow": 1,
         "indicators": "VALUE_IN_EUROS", "TIME_PERIOD": "2023", "partner": "CN", "OBS_VALUE": 1.0}
    )
    prepared = prepare_vintage_flows(
        flows, target_vintage=None, nomenclatures=HS, concordances={}, config=CONFIG
    )
    assert prepared["classification"].tolist() == ["HS2022", "CN2023"]


def test_annotation_columns() -> None:
    frame = nw.from_native(pd.DataFrame({"TIME_PERIOD": ["2019", "2023"]}), eager_only=True)
    in_force = annotate_nomenclature(frame, target_vintage=None, nomenclatures=HS, is_provisional=True)
    historical = annotate_nomenclature(frame, target_vintage="HS2017", nomenclatures=HS,
                                       is_provisional=False)
    assert in_force.to_native()[["hs_vintage", "in_force", "is_provisional"]].values.tolist() == [
        ["HS2017", True, True], ["HS2022", True, True]
    ]
    assert historical.to_native()[["hs_vintage", "in_force"]].values.tolist() == [
        ["HS2017", False], ["HS2017", False]
    ]


def test_harmonize_partner_flows_matches_hs_harmonizer_on_baci_like_data() -> None:
    """Même conversion que l'harmoniseur de BACI sur des déclarations Comtrade."""
    df_comtrade = pd.DataFrame(
        {
            "classificationCode": "H6",
            "reporterCode": [251, 251, 251, 276],
            "partnerCode": [156, 156, 842, 156],
            "flowCode": "M",
            "period": "2023",
            "cmdCode": ["854141", "854142", "854141", "854110"],
            "primaryValue": [1.0, 2.0, 4.0, 8.0],
        }
    )
    baci = HsHarmonizer(
        CONCORDANCES, target_vintage="H5", value_cols=("primaryValue",), weight_cols=()
    ).fit_transform(df_comtrade)
    partners = harmonize_partner_flows(
        df_comtrade.drop(columns="classificationCode"),
        source_vintage="HS2022", target_vintage="HS2017", concordances=CONCORDANCES,
        key_columns=["reporterCode", "partnerCode", "flowCode", "period"],
        measure_columns=["primaryValue"], product_col="cmdCode", period_col="period",
    )
    order = ["reporterCode", "partnerCode", "cmdCode"]
    pd.testing.assert_frame_equal(
        baci.drop(columns="classificationCode")[partners.columns].sort_values(order).reset_index(drop=True),
        partners.sort_values(order).reset_index(drop=True),
    )


def test_conversion_preimage_and_historical_units() -> None:
    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2026, 2, 1, tzinfo=timezone.utc)
    conversions = historical_conversions(CONCORDANCES, ["HS2017"], HS)["HS2017"]
    assert sorted(conversion_preimage(conversions["HS2022"])["854140"]) == ["854141", "854142"]

    last_download = {
        ("FR", "854141"): t1, ("FR", "854142"): t2,   # 854140 : sources complètes
        ("FR", "854110"): t1,                          # code stable
        ("DE", "854141"): t1,                          # 854140 : 854142 jamais téléchargé
        ("FR", "85411000"): t2,                        # NC8 : jamais historique
    }
    units = historical_units(last_download, conversions, "HS2017")
    keys = {unit.key: when for unit, when in units.watermarks.items()}
    # Watermark = téléchargement le plus récent parmi les sources
    assert keys == {"HS2017|FR|854110": t1, "HS2017|FR|854140": t2}
    assert {unit.key: sorted(missing) for unit, missing in units.waiting.items()} == {
        "HS2017|DE|854140": ["854142"]
    }


def test_historical_fingerprint_follows_the_correspondence_table() -> None:
    requested = {"HHI/import": "abc"}
    before = historical_requested(requested, CONCORDANCES, "HS2017", "drop")
    corrected = {("HS2022", "HS2017"): TABLE.assign(target_code=["854110", "854140", "854140", "030119"])}
    after = historical_requested(requested, corrected, "HS2017", "drop")
    assert before["HHI/import"] == after["HHI/import"] == "abc"
    assert before["concordance"] != after["concordance"]
    assert historical_requested(requested, CONCORDANCES, "HS2017", "keep")["concordance"] != before["concordance"]
