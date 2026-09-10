"""Tests de l'orchestration (``macroforecast.trade.aggregation.runner``).

I-01 est corrigé : ``run_aggregation`` oriente ``X`` une seule fois en tête et
utilise cette matrice pour le front, le comptage et le rapport de cohérence.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from macroforecast.trade.aggregation.base import AggregationConfig
from macroforecast.trade.aggregation.pareto import pareto_front
from macroforecast.trade.aggregation.runner import default_pipeline, run_aggregation


@pytest.fixture
def _polarised_frame():
    """Table à deux métriques dont une de polarité ``-1`` (cas I-01)."""
    df = pd.DataFrame(
        {
            "id": ["a", "b", "c", "d"],
            "m1": [0.9, 0.2, 0.5, 0.6],
            "m2": [10.0, 2.0, 5.0, 4.0],
        }
    )
    config = AggregationConfig(
        id_columns=("id",),
        metric_columns=("m1", "m2"),
        polarities={"m2": -1},
    )
    return df, config


def test_run_aggregation_scores_every_method(_polarised_frame) -> None:
    """Une colonne de score par méthode, un rapport renseigné."""
    df, config = _polarised_frame
    methods = {
        "sum": default_pipeline(config, weighting="equal"),
        "geo": default_pipeline(config, weighting="equal", aggregation="geometric_mean"),
    }
    df_scores, report = run_aggregation(df, config, methods)
    assert sorted(df_scores.columns) == ["geo", "sum"]
    assert report.n_products == 4
    assert report.methods == ["sum", "geo"]


def test_run_aggregation_front_size_uses_oriented_matrix(_polarised_frame) -> None:
    """I-01 : ``pareto_front_size`` doit correspondre au front de la matrice
    orientée en polarité positive."""
    df, config = _polarised_frame
    methods = {"sum": default_pipeline(config, weighting="equal")}
    _, report = run_aggregation(df, config, methods)

    oriented = df[["m1", "m2"]].to_numpy() * np.array([1, -1])
    assert report.pareto_front_size == int(pareto_front(oriented).sum())
