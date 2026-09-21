"""Tests des lignes Comext fictives (complément d'un téléchargement partiel)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from kedro_pipeline.synthetic.comext import (
    ComextConfig,
    ComextTemplate,
    _nc8_share,
    build_comext,
)
from kedro_pipeline.synthetic.io import align_to_dtypes

DIMENSIONS = {
    "freq": "A", "partner": "*", "flow": ["1", "2"],
    "indicators": ["QUANTITY_IN_100KG", "VALUE_IN_EUROS"],
}


def _iso2(world) -> dict:
    return {c.iso3: c.iso2 for c in world.config.countries}


def _build(world, template=None, reporter="FR", product="28051910", **kwargs):
    return build_comext(
        world, _iso2(world), ComextConfig.from_mapping({}), template or ComextTemplate(),
        reporter=reporter, product=product, dimensions=DIMENSIONS, **kwargs,
    )


def test_columns_without_template(synthetic_world) -> None:
    """Sans table existante : dimensions de la requête, TIME_PERIOD en texte, valeurs positives."""
    df = pd.concat([_build(synthetic_world, reporter=r, product=p) for r in ("FR", "DE") for p in ("28051910", "30049010")])
    assert {"freq", "reporter", "partner", "product", "flow", "indicators", "TIME_PERIOD", "OBS_VALUE"} <= set(df.columns)
    assert set(df["indicators"]) == {"QUANTITY_IN_100KG", "VALUE_IN_EUROS"}
    assert set(df["flow"]) == {"1", "2"}
    assert (df["OBS_VALUE"] > 0).all() and set(df["TIME_PERIOD"]) == {"2019", "2020", "2021"}


def test_world_is_sum_of_partners_and_extra_eu(synthetic_world) -> None:
    """WORLD = somme des partenaires individuels ; EXT_EU = partenaires hors UE ; INT_EU le reste."""
    df = _build(synthetic_world, product="810510")
    cell = df[(df["flow"] == "1") & (df["indicators"] == "VALUE_IN_EUROS") & (df["TIME_PERIOD"] == "2020")]
    partners = cell.set_index("partner")["OBS_VALUE"]
    individual = partners[~partners.index.isin(["WORLD", "EXT_EU", "INT_EU"])]
    assert np.isclose(partners["WORLD"], individual.sum())
    eu = {"DE", "IT"}
    assert np.isclose(partners["EXT_EU"], individual[~individual.index.isin(eu)].sum())
    assert np.isclose(partners["EXT_EU"] + partners.get("INT_EU", 0.0), partners["WORLD"])


def test_aggregate_codes_follow_the_table(synthetic_world) -> None:
    """Les codes d'agrégat sont ceux DÉJÀ présents dans la table (EXT_EU27_2020…)."""
    template = ComextTemplate(partner_codes=("EXT_EU27_2020", "INT_EU27_2020", "WORLD", "CN"))
    df = _build(synthetic_world, template, product="810510")
    assert {"EXT_EU27_2020", "INT_EU27_2020", "WORLD"} <= set(df["partner"])
    assert "EXT_EU" not in set(df["partner"])


def test_union_reporter_only_has_extra_eu_partners(synthetic_world) -> None:
    """Reporter « Union » : somme des États membres face aux seuls partenaires hors UE."""
    df = _build(synthetic_world, reporter="EU27_2020", product="810510")
    assert not df["partner"].isin(["DE", "IT", "FR", "INT_EU"]).any()
    cell = df[(df["flow"] == "2") & (df["indicators"] == "VALUE_IN_EUROS") & (df["TIME_PERIOD"] == "2020")]
    partners = cell.set_index("partner")["OBS_VALUE"]
    assert np.isclose(partners["WORLD"], partners["EXT_EU"])


def test_unknown_reporter_is_empty(synthetic_world) -> None:
    """Reporter hors de l'univers : aucune ligne (la requête est simplement vide)."""
    assert _build(synthetic_world, reporter="ZZ").empty


def test_start_period_bounds_years(synthetic_world) -> None:
    """``start_period`` borne les années générées."""
    df = _build(synthetic_world, start_period="2021")
    assert set(df["TIME_PERIOD"]) == {"2021"}


def test_nc8_share() -> None:
    """Un code SH6 porte tous les flux ; un code NC8 une part déterministe dans [min, 1]."""
    assert _nc8_share(None, "810510", 0.2) == 1.0


def test_template_dtypes_and_extra_columns(synthetic_world) -> None:
    """Le modèle impose colonnes, types et constantes de la table existante."""
    sample = pd.DataFrame(
        {
            "DATAFLOW": ["ESTAT:DS-045409(1.0)"], "freq": ["A"], "reporter": ["FR"], "partner": ["CN"],
            "product": ["28051910"], "flow": [1], "indicators": ["VALUE_IN_EUROS"],
            "TIME_PERIOD": [2020], "OBS_VALUE": [1.0], "OBS_FLAG": [None],
        }
    )
    template = ComextTemplate.from_sample(sample, partner_codes=["WORLD"])
    df = _build(synthetic_world, template)
    assert list(df.columns) == list(sample.columns)
    assert df["flow"].dtype == sample["flow"].dtype and df["TIME_PERIOD"].dtype == sample["TIME_PERIOD"].dtype
    assert (df["DATAFLOW"] == "ESTAT:DS-045409(1.0)").all()


def test_group_by_product_batches() -> None:
    """Regroupement par lot de produits distincts, dans l'ordre planifié."""
    from scripts.complete_synthetic_comext import group_by_product

    class Query:
        def __init__(self, product: str, reporter: str) -> None:
            self.dimensions = {"product": product, "reporter": reporter}

    queries = [Query(p, r) for p in ("a", "b", "c") for r in ("FR", "DE")]
    batches = group_by_product(queries, 2)
    assert [len(b) for b in batches] == [4, 2]
    assert [q.dimensions["product"] for q in batches[0]] == ["a", "a", "b", "b"]


def test_align_to_dtypes() -> None:
    """Alignement : colonnes ajoutées/retirées, types convertis, no-op sans modèle."""
    frame = pd.DataFrame({"a": ["1"], "extra": [1]})
    aligned = align_to_dtypes(frame, {"a": "int64", "b": "float64"})
    assert list(aligned.columns) == ["a", "b"] and aligned["a"].dtype == "int64"
    assert align_to_dtypes(frame, None) is frame
