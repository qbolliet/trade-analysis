"""Redressement BACI par passes : équivalence exacte avec le traitement monobloc.

Sur un monde fictif multi-années (``baci_synthetic``), ``run_baci_passes`` alimenté
par tranches annuelles, puis par tranches année × 2 blocs de chapitres, doit
reproduire ``run_baci`` à ``1e-8`` relatif : taux de conversion, médianes,
coefficients de gravité, observations retirées par Cook, ``σ̂`` par pays, flux
réconciliés et rapport (``to_metrics()``). Les estimateurs refondus sont en outre
confrontés aux implémentations d'origine (``statsmodels``, ``linearmodels``),
gardées ici comme oracles.
"""

from __future__ import annotations

import logging
import math
import warnings
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import pytest
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import OLSInfluence

from baci_synthetic import cepii_tables, chunks_by_year, comtrade_declarations
from macroforecast.tracking import CapturingTracker
from macroforecast.trade.processing import (
    BaciConfig,
    CifGravityModel,
    Fobizer,
    InMemoryPassIO,
    ReportingQualityModel,
    TonnageConverter,
    build_gravity_data,
    build_mirror_flows,
    infer_import_valuation_regime,
    run_baci,
    run_baci_passes,
    world_median_unit_values,
    world_median_unit_values_sql,
)
from macroforecast.trade.processing.baci import _ls_mean_to_sigma

RTOL = 1e-8
KEYS = ["exporter", "importer", "product", "year"]


@pytest.fixture(autouse=True)
def _quiet() -> None:
    logging.disable(logging.INFO)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture(scope="module")
def world() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = comtrade_declarations(seed=0)
    df_dist, df_geo = cepii_tables(seed=0)
    return df, df_dist, df_geo


@pytest.fixture(scope="module")
def monobloc(world):
    df, df_dist, df_geo = world
    tracker = CapturingTracker()
    result, report = run_baci(df, df_dist, df_geo, config=BaciConfig(), tracker=tracker)
    return result, report, tracker


def _assert_metrics_equal(left: Dict[str, float], right: Dict[str, float]) -> None:
    assert set(left) == set(right)
    for name in left:
        assert left[name] == pytest.approx(right[name], rel=RTOL, abs=1e-12), name


def _assert_tables_equal(left: pd.DataFrame, right: pd.DataFrame, keys) -> None:
    left = left.sort_values(keys).reset_index(drop=True)
    right = right.sort_values(keys).reset_index(drop=True)
    assert list(left.columns) == list(right.columns)
    pd.testing.assert_frame_equal(left, right, check_exact=False, rtol=RTOL, atol=1e-12)


# ──────────────────────────────────────────────────────────────────────
# Équivalence de bout en bout
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("blocks", [1, 2], ids=["years", "years-x-2-chapter-blocks"])
def test_passes_reproduce_the_monobloc_run(world, monobloc, blocks) -> None:
    df, df_dist, df_geo = world
    result, report, tracker = monobloc
    io = InMemoryPassIO(chunks_by_year(df, blocks=blocks))
    passes_tracker = CapturingTracker()
    report_p, rows_by_year = run_baci_passes(
        io, df_dist, df_geo, config=BaciConfig(), tracker=passes_tracker,
        memory_probe=lambda: 1.0,
    )

    # Flux réconciliés : mêmes clés, mêmes valeurs et quantités
    _assert_tables_equal(io.result(), result, KEYS)
    # Rapport complet (rapport de run et contrôles), hors métriques de temps
    _assert_metrics_equal(report_p.to_metrics(), report.to_metrics())
    # Artefacts d'étape : taux de conversion, régimes, σ̂, coefficients de gravité
    for name in ("tonnage/conversion_rates.csv", "fobisation/valuation_regimes.csv"):
        _assert_tables_equal(passes_tracker.tables[name], tracker.tables[name], list(tracker.tables[name].columns[:2]))
    _assert_tables_equal(
        passes_tracker.tables["quality/sigma_by_country.csv"],
        tracker.tables["quality/sigma_by_country.csv"],
        ["country"],
    )
    coefficients_p = passes_tracker.dicts["gravity/coefficients.json"]
    coefficients = tracker.dicts["gravity/coefficients.json"]
    _assert_metrics_equal(coefficients_p["coefficients"], coefficients["coefficients"])
    _assert_metrics_equal(coefficients_p["std_errors"], coefficients["std_errors"])
    assert coefficients_p["n_obs"] == coefficients["n_obs"]

    # Lignes par année et métriques de passes émises
    expected_rows = result.groupby("year").size()
    assert rows_by_year.set_index("year")["rows"].to_dict() == expected_rows.to_dict()
    assert {f"passes/{name}/seconds" for name in ("P0", "S1", "P1", "P2", "P3", "S2", "P4", "P5")} <= set(
        passes_tracker.metrics
    )
    assert "output/rows" in passes_tracker.metrics and "memory/peak_mb" in passes_tracker.metrics


def test_resumed_preparation_state_gives_the_same_result(world, monobloc) -> None:
    df, df_dist, df_geo = world
    result, report, _ = monobloc
    first = InMemoryPassIO(chunks_by_year(df))
    run_baci_passes(first, df_dist, df_geo, config=BaciConfig())
    # Reprise : état P0 et fichiers de travail réutilisés, aucune déclaration relue
    resumed = InMemoryPassIO([])
    resumed.store = {kind: dict(chunks) for kind, chunks in first.store.items() if kind != "freight"}
    resumed.p0_state = first.p0_state
    report_r, _ = run_baci_passes(resumed, df_dist, df_geo, config=BaciConfig())
    _assert_tables_equal(resumed.result(), result, KEYS)
    _assert_metrics_equal(report_r.to_metrics(), report.to_metrics())


def test_passes_raise_before_writing_when_the_temporal_filter_empties_the_scope(world) -> None:
    df, df_dist, df_geo = world
    io = InMemoryPassIO(chunks_by_year(df))
    with pytest.raises(ValueError, match="emptied df_comtrade"):
        run_baci_passes(io, df_dist, df_geo, config=BaciConfig(), period_start=2030)
    assert io.written == {}


def test_a_writer_that_does_not_consume_every_block_is_an_error(world) -> None:
    df, df_dist, df_geo = world

    class LazyWriter(InMemoryPassIO):
        def write_year(self, year, blocks):
            next(iter(blocks))

    io = LazyWriter(chunks_by_year(df, blocks=2))
    with pytest.raises(RuntimeError, match="consumed 1 of 2 blocks"):
        run_baci_passes(io, df_dist, df_geo, config=BaciConfig())


# ──────────────────────────────────────────────────────────────────────
# Estimateurs refondus contre les implémentations d'origine (oracles)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def mirror(world) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Flux miroirs convertis en tonnes, variables de gravité et régimes inférés."""
    df, df_dist, df_geo = world
    df_gravity = build_gravity_data(df_dist, df_geo)
    valid = sorted(set(df_gravity["iso_o"]) | set(df_gravity["iso_d"]))
    df_mirror, _, _ = build_mirror_flows(df, valid)
    df_mirror = TonnageConverter().fit(df_mirror).transform(df_mirror)
    return df_mirror, df_gravity, infer_import_valuation_regime(df)


def _statsmodels_gravity(model: CifGravityModel, df_mirror, df_gravity):
    """Implémentation d'origine : WLS statsmodels, Cook sur le modèle blanchi, second WLS."""
    sample = df_mirror[
        (df_mirror["v_x"] > 0) & (df_mirror["v_m"] > 0) & (df_mirror["q_x_t"] > 0) & (df_mirror["q_m_t"] > 0)
    ]
    y = np.log((sample["v_m"] / sample["q_m_t"]) / (sample["v_x"] / sample["q_x_t"]))
    w = np.minimum(sample["q_x_t"], sample["q_m_t"]) / np.maximum(sample["q_x_t"], sample["q_m_t"])
    model.uv_world_ = world_median_unit_values(df_mirror)
    design = model._regressors(sample, df_gravity)
    design = pd.concat(
        [design, pd.get_dummies(sample["year"], prefix="year", drop_first=True).astype("float64")], axis=1
    )
    design["_y"], design["_w"] = y.to_numpy(), w.to_numpy()
    design = design.replace([np.inf, -np.inf], np.nan).dropna()
    y_fit, w_fit = design.pop("_y"), design.pop("_w")
    X = sm.add_constant(design, has_constant="add")
    sqrt_w = np.sqrt(w_fit.to_numpy())
    cook = OLSInfluence(sm.OLS(y_fit.to_numpy() * sqrt_w, X.to_numpy() * sqrt_w[:, None]).fit()).cooks_distance[0]
    keep = pd.Series(cook < 4.0 / len(cook), index=X.index)
    final = sm.WLS(y_fit[keep], X[keep], weights=w_fit[keep]).fit()
    return final, keep


def test_gravity_matches_statsmodels_and_is_partition_invariant(mirror) -> None:
    df_mirror, df_gravity, _ = mirror
    whole = CifGravityModel().fit(df_mirror, df_gravity)
    oracle, oracle_keep = _statsmodels_gravity(CifGravityModel(), df_mirror, df_gravity)
    # Coefficients, écarts-types, R², effectif final (écart à l'implémentation d'origine)
    np.testing.assert_allclose(whole.result_.params[oracle.params.index], oracle.params, rtol=RTOL)
    np.testing.assert_allclose(whole.result_.bse[oracle.bse.index], oracle.bse, rtol=RTOL)
    assert whole.result_.rsquared == pytest.approx(oracle.rsquared, rel=RTOL)
    assert whole.result_.nobs == oracle.nobs
    # Ensemble identique d'observations retirées par Cook
    keep = whole.cook_keep_mask(df_mirror, df_gravity)
    assert keep.index.equals(oracle_keep.index) and np.array_equal(keep.to_numpy(), oracle_keep.to_numpy())

    # Par tranches annuelles : mêmes coefficients et même ensemble retiré
    chunked = CifGravityModel()
    uv_world = world_median_unit_values(df_mirror)
    years = sorted(df_mirror["year"].unique())
    chunks = [df_mirror[df_mirror["year"] == year] for year in years]
    for phase in ("fit", "cook"):
        for chunk in chunks:
            chunked.partial_fit(chunk, df_gravity, uv_world=uv_world, years=years, phase=phase)
        chunked.finalize()
    np.testing.assert_allclose(chunked.result_.params, whole.result_.params, rtol=RTOL)
    keep_chunked = pd.concat([chunked.cook_keep_mask(chunk, df_gravity) for chunk in chunks]).sort_index()
    assert np.array_equal(keep_chunked.to_numpy(), keep.sort_index().to_numpy())


def _absorbing_ls_sigma(df_mirror: pd.DataFrame, target: str):
    """Implémentation d'origine : AbsorbingLS, contrastes somme-nulle, σ̂ non plancher."""
    from linearmodels.iv import AbsorbingLS

    sample = ReportingQualityModel._sample(df_mirror, target)
    exog = pd.get_dummies(sample[["exporter", "importer", "year"]].astype(str), drop_first=True).astype("float64")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = AbsorbingLS(sample["_rd"], exog, absorb=sample[["product"]].astype("category"), weights=sample["_w"]).fit()
    out = {}
    for entity in ("exporter", "importer"):
        levels = sorted(sample[entity].astype(str).unique())
        coefs = pd.Series({e: float(res.params.get(f"{entity}_{e}", 0.0)) for e in levels})
        cols = [c for c in (f"{entity}_{e}" for e in levels) if c in res.params.index]
        v_mat = res.cov.loc[cols, cols].to_numpy()
        se = {}
        for e in levels:
            contrast = np.full(len(cols), -1.0 / len(levels))
            if f"{entity}_{e}" in cols:
                contrast[cols.index(f"{entity}_{e}")] += 1.0
            se[e] = math.sqrt(max(contrast @ v_mat @ contrast, 0.0))
        out[entity] = _ls_mean_to_sigma(coefs - coefs.mean(), pd.Series(se))
    return out


@pytest.mark.parametrize("target", ["value", "quantity"])
def test_reporting_quality_matches_absorbing_ls(mirror, target) -> None:
    df_mirror, df_gravity, df_regime = mirror
    gravity = CifGravityModel().fit(df_mirror, df_gravity)
    df_fob = Fobizer().transform(df_mirror, gravity.cif_rate_, df_regime)
    oracle = _absorbing_ls_sigma(df_fob, target)
    model = ReportingQualityModel(min_sigma_ratio=0.0)
    result = model.fit(df_fob, target)
    pd.testing.assert_series_equal(
        result.sigma_export.sort_index(), oracle["exporter"].sort_index(),
        check_names=False, rtol=RTOL, atol=1e-10,
    )
    pd.testing.assert_series_equal(
        result.sigma_import.sort_index(), oracle["importer"].sort_index(),
        check_names=False, rtol=RTOL, atol=1e-10,
    )
    # Par tranches annuelles
    chunked = ReportingQualityModel(min_sigma_ratio=0.0)
    chunks = [df_fob[df_fob["year"] == year] for year in sorted(df_fob["year"].unique())]
    universes = dict(
        exporters=df_fob["exporter"].unique(), importers=df_fob["importer"].unique(), years=df_fob["year"].unique()
    )
    for chunk in chunks:
        chunked.partial_fit(chunk, target=target, phase="fit", **universes)
    chunked.finalize(target)
    for chunk in chunks:
        chunked.partial_fit(chunk, target=target, phase="cov")
    chunked.finalize(target)
    pd.testing.assert_series_equal(
        chunked.results_[target].sigma_export, result.sigma_export, rtol=RTOL, atol=1e-10
    )


def test_tonnage_rates_are_partition_invariant(world) -> None:
    df, df_dist, df_geo = world
    df_gravity = build_gravity_data(df_dist, df_geo)
    valid = sorted(set(df_gravity["iso_o"]))
    df_mirror, _, _ = build_mirror_flows(df, valid)
    whole = TonnageConverter().fit(df_mirror)
    chunked = TonnageConverter()
    for year in sorted(df_mirror["year"].unique()):
        chunked.partial_fit(df_mirror[df_mirror["year"] == year])
    chunked.finalize()
    assert whole.conversion_rates_.keys() == chunked.conversion_rates_.keys()
    for pair, rate in whole.conversion_rates_.items():
        assert chunked.conversion_rates_[pair] == pytest.approx(rate, rel=1e-12)
    assert (whole.report_.n_validated_rates, whole.report_.n_candidate_pairs) == (
        chunked.report_.n_validated_rates,
        chunked.report_.n_candidate_pairs,
    )
    assert whole.conversion_rates_, "le jeu fictif doit valider des taux d'unités hors poids"


# ──────────────────────────────────────────────────────────────────────
# Médiane des valeurs unitaires en SQL (S1)
# ──────────────────────────────────────────────────────────────────────


def test_unit_value_median_sql_matches_pandas(mirror) -> None:
    duckdb = pytest.importorskip("duckdb")
    df_mirror, _, _ = mirror
    raw = df_mirror.drop(columns=["q_x_t", "q_m_t"]).copy()
    # Cas limites : poids nul, quantité nulle, unité absente, NaN explicites
    raw.loc[raw.index[:20], "nw_x"] = 0.0
    raw.loc[raw.index[20:40], "q_m"] = np.nan
    raw.loc[raw.index[40:60], "unit_x"] = np.nan
    converter = TonnageConverter().fit(raw)
    expected = world_median_unit_values(converter.transform(raw))
    rates = pd.DataFrame(
        [(p, u, r) for (p, u), r in converter.conversion_rates_.items()], columns=["product", "unit", "rate"]
    )
    conn = duckdb.connect()
    conn.register("mirror_rel", raw)
    conn.register("rates_rel", rates)
    sql = world_median_unit_values_sql(
        "mirror_rel", "rates_rel", tonne_conversion_factors=converter.tonne_conversion_factors, prefer_netwgt=True
    )
    got = conn.execute(sql).df().set_index("product")["uv"]
    pd.testing.assert_series_equal(got.sort_index(), expected.sort_index(), check_names=False, rtol=1e-12)
    # Découpage par plages de produits : exact (médiane par produit)
    ranged = world_median_unit_values_sql(
        "mirror_rel", "rates_rel", tonne_conversion_factors=converter.tonne_conversion_factors,
        prefer_netwgt=True, product_range=True,
    )
    parts = [conn.execute(ranged, [lo, hi, lo, hi]).df() for lo, hi in (("280000", "289999"), ("840000", "859999"))]
    got_ranged = pd.concat(parts).set_index("product")["uv"]
    pd.testing.assert_series_equal(got_ranged.sort_index(), expected.sort_index(), check_names=False, rtol=1e-12)
