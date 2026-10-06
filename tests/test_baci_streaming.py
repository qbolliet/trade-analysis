"""Accumulateurs de statistiques suffisantes : oracles et invariance par partition.

Chaque accumulateur de ``macroforecast.trade.processing.streaming`` est confronté à
l'estimateur monobloc qu'il remplace (pandas, ``statsmodels``, ``linearmodels``) et
alimenté par des partitions arbitraires des observations, dont sept tranches de
tailles inégales.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import OLSInfluence

from macroforecast.trade.processing.streaming import (
    AbsorbedWLSAccumulator,
    CookFilter,
    WelfordGroupStats,
    WeightedLeastSquaresAccumulator,
)

RTOL = 1e-8


def _partitions(n: int, rng: np.random.Generator, parts: int = 7) -> list[np.ndarray]:
    """Découpage aléatoire en tranches contiguës de tailles inégales."""
    cuts = np.sort(rng.choice(np.arange(1, n), size=parts - 1, replace=False))
    return np.split(rng.permutation(n), cuts)


# ──────────────────────────────────────────────────────────────────────
# Moments par groupe
# ──────────────────────────────────────────────────────────────────────


def test_welford_matches_pandas_on_any_partition() -> None:
    rng = np.random.default_rng(0)
    df = pd.DataFrame(
        {
            "product": rng.choice(["a", "b", "c", "d"], 5000),
            "unit": rng.choice([5, 12], 5000),
            # Moyenne grande devant la dispersion : piège des sommes brutes Σx²
            "ratio": 1e6 + rng.normal(0.0, 1.0, 5000),
        }
    )
    df.loc[::97, "ratio"] = np.nan
    acc = WelfordGroupStats()
    for part in _partitions(len(df), rng):
        acc.partial_fit(df.iloc[part], by=["product", "unit"], value="ratio")
    stats = acc.finalize().stats_
    expected = df.groupby(["product", "unit"])["ratio"].agg(["mean", "std", "count"])
    np.testing.assert_allclose(stats["mean"], expected["mean"], rtol=1e-14)
    np.testing.assert_allclose(stats["std"], expected["std"], rtol=1e-9)
    assert stats["count"].tolist() == expected["count"].tolist()


def test_welford_state_round_trip_and_empty_state() -> None:
    df = pd.DataFrame({"k": ["a", "a", "b"], "x": [1.0, 2.0, 4.0]})
    acc = WelfordGroupStats().partial_fit(df, by=["k"], value="x")
    clone = WelfordGroupStats.from_frame(acc.to_frame(), by=["k"]).finalize()
    pd.testing.assert_frame_equal(clone.stats_, acc.finalize().stats_)
    empty = WelfordGroupStats.from_frame(pd.DataFrame(), by=["k"]).finalize()
    assert empty.stats_.empty


# ──────────────────────────────────────────────────────────────────────
# Moindres carrés pondérés et distance de Cook
# ──────────────────────────────────────────────────────────────────────


def _gravity_like(rng: np.random.Generator, n: int = 4000):
    """Design de type gravité : ln dist et (ln dist)² quasi colinéaires, indicatrices."""
    ld = np.log(rng.uniform(300.0, 18000.0, n))
    years = rng.integers(0, 6, n)
    X = np.column_stack(
        [np.ones(n), ld, ld ** 2, rng.integers(0, 2, n), np.log(rng.lognormal(1.0, 1.5, n))]
        + [(years == k).astype(float) for k in range(1, 6)]
        # Indicatrice d'une année sans observation : colonne nulle (solution de norme minimale)
        + [np.zeros(n)]
    )
    w = rng.uniform(0.01, 1.0, n)
    y = X[:, :-1] @ rng.normal(0.0, 0.1, X.shape[1] - 1) + rng.normal(0.0, 0.3, n)
    y[rng.choice(n, 15, replace=False)] += 4.0
    return X, y, w


def test_wls_accumulator_matches_statsmodels_with_a_null_column() -> None:
    rng = np.random.default_rng(1)
    X, y, w = _gravity_like(rng)
    columns = [f"c{i}" for i in range(X.shape[1])]
    acc = WeightedLeastSquaresAccumulator(columns, block_rows=500)
    for part in _partitions(len(y), rng):
        acc.partial_fit(X[part], y[part], w[part])
    sol = acc.solve()
    res = sm.WLS(y, X, weights=w).fit()
    np.testing.assert_allclose(sol.params, res.params, rtol=RTOL, atol=1e-12)
    np.testing.assert_allclose(sol.bse[:-1], res.bse[:-1], rtol=RTOL)
    assert sol.params[-1] == pytest.approx(0.0, abs=1e-12)
    assert sol.nobs == res.nobs and sol.rank == res.model.rank and sol.df_resid == res.df_resid
    assert sol.rss == pytest.approx(res.ssr, rel=1e-10)
    assert sol.r_squared == pytest.approx(res.rsquared, rel=1e-10)
    # Produits croisés exposés : forme non factorisée des mêmes statistiques
    np.testing.assert_allclose(acc.A, (X * w[:, None]).T @ X, rtol=1e-9, atol=1e-6)
    np.testing.assert_allclose(acc.b, X.T @ (w * y), rtol=1e-9, atol=1e-6)


def test_wls_accumulator_column_selection_is_exact() -> None:
    rng = np.random.default_rng(2)
    X, y, w = _gravity_like(rng)
    columns = [f"c{i}" for i in range(X.shape[1])]
    acc = WeightedLeastSquaresAccumulator(columns).partial_fit(X, y, w)
    keep = [0, 1, 2, 4, 6, 7]
    sol = acc.solve([columns[i] for i in keep])
    res = sm.WLS(y, X[:, keep], weights=w).fit()
    np.testing.assert_allclose(sol.params, res.params, rtol=RTOL)


def test_partition_invariance_of_the_wls_accumulator() -> None:
    rng = np.random.default_rng(3)
    X, y, w = _gravity_like(rng, 20000)
    columns = [f"c{i}" for i in range(X.shape[1])]
    whole = WeightedLeastSquaresAccumulator(columns).partial_fit(X, y, w).solve()
    chunked = WeightedLeastSquaresAccumulator(columns, block_rows=777)
    for part in _partitions(len(y), rng):
        chunked.partial_fit(X[part], y[part], w[part])
    np.testing.assert_allclose(chunked.solve().params, whole.params, rtol=1e-10, atol=1e-13)


def test_cook_filter_matches_olsinfluence() -> None:
    rng = np.random.default_rng(4)
    X, y, w = _gravity_like(rng)
    X = X[:, :-1]
    sol = WeightedLeastSquaresAccumulator([f"c{i}" for i in range(X.shape[1])]).partial_fit(X, y, w).solve()
    sqrt_w = np.sqrt(w)
    expected = OLSInfluence(sm.OLS(y * sqrt_w, X * sqrt_w[:, None]).fit()).cooks_distance[0]
    cook = CookFilter(sol, cook_factor=4.0)
    np.testing.assert_allclose(cook.distance(X, y, w), expected, rtol=1e-7)
    keep = cook.keep_mask(X, y, w)
    assert np.array_equal(keep, expected < 4.0 / len(y))
    # Par tranche : même masque, observation par observation
    parts = _partitions(len(y), rng)
    chunked = np.empty(len(y), dtype=bool)
    for part in parts:
        chunked[part] = cook.keep_mask(X[part], y[part], w[part])
    assert np.array_equal(chunked, keep)


# ──────────────────────────────────────────────────────────────────────
# ANOVA pondérée à effets de produit absorbés
# ──────────────────────────────────────────────────────────────────────


def _anova_frame(rng: np.random.Generator, n: int = 6000) -> pd.DataFrame:
    exporters = [f"E{i:02d}" for i in range(15)]
    importers = [f"I{i:02d}" for i in range(12)]
    df = pd.DataFrame(
        {
            "exporter": rng.choice(exporters, n, p=np.r_[0.3, np.full(14, 0.05)]),
            "importer": rng.choice(importers, n),
            "year": rng.choice(["2019", "2020", "2021"], n),
            "product": rng.integers(0, 80, n),
            "w": np.log(rng.lognormal(5.0, 2.0, n) + 1.0),
        }
    )
    df["rd"] = np.abs(rng.normal(0.3, 0.5, n)) + 0.02 * df["exporter"].str[1:].astype(int)
    return df[df["w"] > 0].reset_index(drop=True)


def _oracle(df: pd.DataFrame):
    from linearmodels.iv import AbsorbingLS

    exog = pd.get_dummies(df[["exporter", "importer", "year"]], drop_first=True).astype("float64")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return AbsorbingLS(
            df["rd"], exog, absorb=df[["product"]].astype("category"), weights=df["w"]
        ).fit()


def test_absorbed_accumulator_matches_absorbing_ls_on_any_partition() -> None:
    rng = np.random.default_rng(5)
    df = _anova_frame(rng)
    res = _oracle(df)
    factors = {name: sorted(df[name].unique()) for name in ("exporter", "importer", "year")}
    acc = AbsorbedWLSAccumulator(factors)
    parts = _partitions(len(df), rng)
    for part in parts:
        chunk = df.iloc[part]
        acc.partial_fit(chunk[list(factors)], chunk["rd"], chunk["w"], chunk["product"])
    acc.solve(list(res.params.index))
    for part in parts:
        chunk = df.iloc[part]
        acc.robust_meat(chunk[list(factors)], chunk["rd"], chunk["w"], chunk["product"])
    np.testing.assert_allclose(acc.params_.to_numpy(), res.params.to_numpy(), rtol=RTOL, atol=1e-12)
    cov = acc.covariance()
    np.testing.assert_allclose(cov.to_numpy(), res.cov.to_numpy(), rtol=1e-7, atol=1e-14)


def test_absorbed_accumulator_rejects_a_fully_absorbed_column() -> None:
    df = pd.DataFrame(
        {
            "e": ["a", "b", "a", "b", "c", "c"],
            "y": [1.0, 2.0, 1.5, 2.5, 0.5, 0.7],
            "w": [1.0] * 6,
            # La modalité « c » n'apparaît que dans le produit 3, qu'elle remplit seule
            "k": [1, 1, 2, 2, 3, 3],
        }
    )
    acc = AbsorbedWLSAccumulator({"e": ["a", "b", "c"]}).partial_fit(df[["e"]], df["y"], df["w"], df["k"])
    with pytest.raises(ValueError, match="fully absorbed"):
        acc.solve(["e_b", "e_c"])


def test_absorbed_accumulator_rejects_unknown_levels() -> None:
    acc = AbsorbedWLSAccumulator({"e": ["a"]})
    with pytest.raises(ValueError, match="outside the fixed design"):
        acc.partial_fit(pd.DataFrame({"e": ["z"]}), [1.0], [1.0], [1])
