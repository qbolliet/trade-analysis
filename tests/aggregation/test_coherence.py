"""Tests du runner de cohérence (``macroforecast.trade.aggregation.coherence``).

``run_coherence`` est une fonction pure : table de cellules + table longue de
scores (S-2.4) → table longue de diagnostics (S-2.6, familles ``metrics`` et
``methods``) et rapport (S-2.7). Les tests partent de la sortie de
``run_synthesis`` sur la table jouet ``df_synthesis_toy`` et vérifient le
contrat de sortie (colonnes, sentinelles, clé primaire), la présence de chaque
statistique de S-2.5.a et S-2.5.b, les bornes mathématiques, l'absence de
valeur non finie et le leave-one-metric-out. Aucun test ne dépend de ``jax``.
"""

from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd
import pytest

from macroforecast.trade.aggregation.coherence import (
    COHERENCE_FAMILIES,
    CoherenceConfig,
    CoherenceRunReport,
    pareto_front_share_expected,
    run_coherence,
)
from macroforecast.trade.aggregation.methods import MethodSpec
from macroforecast.trade.aggregation.synthesis import (
    GROUP_SENTINEL,
    ITEM_SENTINEL,
    SynthesisConfig,
    run_synthesis,
)

# Clé primaire de la table longue de diagnostics (S-2.6)
PRIMARY_KEY = [
    "freq",
    "flow",
    "indicators",
    "TIME_PERIOD",
    "level",
    "reporter",
    "product",
    "family",
    "statistic",
    "item_a",
    "item_b",
]

# Colonnes de la table, dans l'ordre exact de S-2.6
DIAGNOSTIC_SCHEMA = [*PRIMARY_KEY, "value", "n"]

# Configuration réduite : huit méthodes, tirages courts, bootstrap au global
_METHODS = (
    MethodSpec(name="pareto", kind="pareto", params={"epsilon": 0.1}),
    MethodSpec(name="rank_mean", kind="rank_mean"),
    MethodSpec(name="critic_sum", kind="weighted", params={"weighting": "critic"}),
    MethodSpec(name="auto_sum", kind="weighted", params={"weighting": "auto"}),
    MethodSpec(name="bod", kind="bod"),
    MethodSpec(name="smaa", kind="smaa"),
    MethodSpec(name="cone_quantile", kind="cone_quantile"),
    MethodSpec(name="mpi", kind="mpi"),
)

# Statistiques attendues de la famille `metrics` (S-2.5.a)
METRICS_STATISTICS = (
    "spearman",
    "kendall_tau_b",
    "kendall_w",
    "kmo",
    "bartlett_p",
    "axis1_share",
    "mean_abs_rho",
    "pareto_front_share",
    "pareto_front_share_expected",
    "n_rows",
    "n_complete",
)

# Statistiques attendues de la famille `methods` (S-2.5.b), hors ellipticité
METHODS_STATISTICS = (
    "kendall_tau_b",
    "weighted_tau",
    "rbo",
    "topk_overlap_5",
    "topk_overlap_10",
    "kendall_w",
    "violation_strict",
    "violation_tie",
    "front_median_rank",
    "front_max_rank",
    "disputed_share",
    "rank_interval_width_median",
    "smaa_confidence_top5_share",
    "score_metric_tau",
    "cluster_id",
    "lomo_tau",
    "lomo_topk_overlap",
)

# Statistiques bornées dans [-1, 1] (corrélations de rang)
CORRELATION_STATISTICS = (
    "spearman",
    "kendall_tau_b",
    "weighted_tau",
    "score_metric_tau",
    "lomo_tau",
)

# Statistiques bornées dans [0, 1] (parts, concordances, recouvrements)
SHARE_STATISTICS = (
    "kendall_w",
    "rbo",
    "topk_overlap_5",
    "topk_overlap_10",
    "pareto_front_share",
    "disputed_share",
    "violation_strict",
    "violation_tie",
    "smaa_confidence_top5_share",
    "lomo_topk_overlap",
    "kmo",
    "bartlett_p",
    "axis1_share",
    "mean_abs_rho",
)


@pytest.fixture
def _synthesis_config() -> SynthesisConfig:
    """Configuration de synthèse réduite, déterministe et rapide."""
    return SynthesisConfig(
        metric_columns=("HHI", "CDI2", "CDI3", "EXPORT_HHI"),
        methods=_METHODS,
        min_group_size=3,
        smaa_n_draws=64,
        smaa_k=5,
        consensus=("borda", "copeland"),
        consensus_top_n=20,
        bootstrap_methods=("critic_sum",),
        bootstrap_levels=("global",),
        bootstrap_n=3,
    )


@pytest.fixture
def _coherence_config() -> CoherenceConfig:
    """Configuration de cohérence : profondeurs courtes, LOMO sur une méthode."""
    return CoherenceConfig(
        topk_depths=(5, 10), lomo=True, lomo_methods=("rank_mean",)
    )


# Mémoïsation de l'exécution de référence : les tests paramétrés sont nombreux
# et la synthèse jouet coûte quelques secondes, à ne payer qu'une fois
_RUN_CACHE: Dict[str, Tuple[pd.DataFrame, CoherenceRunReport]] = {}


@pytest.fixture
def _run(
    df_synthesis_toy: pd.DataFrame,
    _synthesis_config: SynthesisConfig,
    _coherence_config: CoherenceConfig,
) -> Tuple[pd.DataFrame, CoherenceRunReport]:
    """Exécution unique synthèse puis cohérence, partagée par les tests."""
    if "default" not in _RUN_CACHE:
        df_scores, df_fit, _ = run_synthesis(df_synthesis_toy, _synthesis_config)
        _RUN_CACHE["default"] = run_coherence(
            df_synthesis_toy,
            df_scores,
            _synthesis_config,
            _coherence_config,
            df_fit_diagnostics=df_fit,
        )
    df_diagnostics, report = _RUN_CACHE["default"]
    # Copie défensive : un test qui modifierait la table n'affecte pas les autres
    return df_diagnostics.copy(), report


# ──────────────────────────────────────────────────────────────────────
# Contrat de sortie (S-2.6)
# ──────────────────────────────────────────────────────────────────────


def test_schema_and_sentinels(_run) -> None:
    """Colonnes exactement celles de S-2.6, sentinelles jamais nulles."""
    df_diagnostics, _ = _run
    assert list(df_diagnostics.columns) == DIAGNOSTIC_SCHEMA
    assert set(df_diagnostics["family"]) <= set(COHERENCE_FAMILIES)
    # Aucune valeur nulle sur les colonnes de la clé primaire (S-2.6)
    assert not df_diagnostics[PRIMARY_KEY].isna().any().any()

    # La colonne de groupe non identifiante porte la sentinelle 'ALL'
    by_product = df_diagnostics[df_diagnostics["level"] == "by_product"]
    assert (by_product["reporter"] == GROUP_SENTINEL).all()
    assert (by_product["product"] != GROUP_SENTINEL).all()
    by_reporter = df_diagnostics[df_diagnostics["level"] == "by_reporter"]
    assert (by_reporter["product"] == GROUP_SENTINEL).all()
    at_global = df_diagnostics[df_diagnostics["level"] == "global"]
    assert (at_global[["reporter", "product"]] == GROUP_SENTINEL).all().all()


def test_primary_key_is_unique(_run) -> None:
    """La clé primaire de S-2.6 ne porte aucun doublon."""
    df_diagnostics, _ = _run
    assert int(df_diagnostics.duplicated(subset=PRIMARY_KEY).sum()) == 0


def test_no_non_finite_value(_run) -> None:
    """Une statistique indéfinie n'émet pas de ligne : `value` est toujours finie."""
    df_diagnostics, _ = _run
    assert bool(np.isfinite(df_diagnostics["value"].to_numpy()).all())


def test_volumetry_rows_always_emitted(_run) -> None:
    """Chaque groupe porte `n_rows` et `n_complete`, `n_complete <= n_rows`."""
    df_diagnostics, report = _run
    group_key = ["freq", "flow", "indicators", "TIME_PERIOD", "level",
                 "reporter", "product"]
    volumetry = df_diagnostics[df_diagnostics["statistic"].isin(("n_rows", "n_complete"))]
    counts = volumetry.groupby("statistic").size()
    assert counts["n_rows"] == counts["n_complete"] == report.n_groups
    wide = volumetry.pivot_table(
        index=group_key, columns="statistic", values="value"
    )
    assert (wide["n_complete"] <= wide["n_rows"]).all()


# ──────────────────────────────────────────────────────────────────────
# Présence et bornes des statistiques (S-2.5)
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("statistic", METRICS_STATISTICS)
def test_metrics_statistic_present(_run, statistic: str) -> None:
    """Chaque statistique du tableau S-2.5.a est émise au moins une fois."""
    df_diagnostics, _ = _run
    family = df_diagnostics[df_diagnostics["family"] == "metrics"]
    assert (family["statistic"] == statistic).any()


@pytest.mark.parametrize("statistic", METHODS_STATISTICS)
def test_methods_statistic_present(_run, statistic: str) -> None:
    """Chaque statistique du tableau S-2.5.b est émise au moins une fois."""
    df_diagnostics, _ = _run
    family = df_diagnostics[df_diagnostics["family"] == "methods"]
    assert (family["statistic"] == statistic).any()


@pytest.mark.parametrize("statistic", CORRELATION_STATISTICS)
def test_correlation_bounds(_run, statistic: str) -> None:
    """Les corrélations de rang restent dans [-1, 1]."""
    df_diagnostics, _ = _run
    values = df_diagnostics.loc[
        df_diagnostics["statistic"] == statistic, "value"
    ].to_numpy()
    assert values.size
    assert bool(((values >= -1.0) & (values <= 1.0)).all())


@pytest.mark.parametrize("statistic", SHARE_STATISTICS)
def test_share_bounds(_run, statistic: str) -> None:
    """Les parts, concordances et recouvrements restent dans [0, 1]."""
    df_diagnostics, _ = _run
    values = df_diagnostics.loc[
        df_diagnostics["statistic"] == statistic, "value"
    ].to_numpy()
    assert values.size
    assert bool(((values >= 0.0) & (values <= 1.0)).all())


def test_metric_pairs_are_emitted_once(_run) -> None:
    """Les paires de métriques sont ordonnées (`item_a < item_b`) et uniques."""
    df_diagnostics, _ = _run
    pairs = df_diagnostics[
        (df_diagnostics["family"] == "metrics")
        & (df_diagnostics["statistic"] == "spearman")
    ]
    assert (pairs["item_a"] < pairs["item_b"]).all()


def test_method_pairs_are_emitted_once(_run) -> None:
    """Les paires de méthodes sont ordonnées et aucune symétrique n'est émise."""
    df_diagnostics, _ = _run
    pairs = df_diagnostics[
        (df_diagnostics["family"] == "methods")
        & (df_diagnostics["statistic"] == "kendall_tau_b")
    ]
    assert (pairs["item_a"] < pairs["item_b"]).all()
    ordered = set(zip(pairs["item_a"], pairs["item_b"]))
    assert not ordered & {(b, a) for a, b in ordered}


def test_scalar_statistics_carry_no_item(_run) -> None:
    """Les statistiques scalaires portent la sentinelle vide sur `item_a` / `item_b`."""
    df_diagnostics, _ = _run
    for statistic in ("kendall_w", "disputed_share", "mean_abs_rho", "n_rows"):
        selection = df_diagnostics[df_diagnostics["statistic"] == statistic]
        assert (selection["item_a"] == ITEM_SENTINEL).all()
        assert (selection["item_b"] == ITEM_SENTINEL).all()


def test_pareto_front_share_expected_formula() -> None:
    """`(ln n)^(d-1) / ((d-1)! n)` : décroissante en `n`, indéfinie sous `n = 2`."""
    assert pareto_front_share_expected(1_000, 1) == pytest.approx(1e-3)
    assert pareto_front_share_expected(1_000, 3) > pareto_front_share_expected(
        1_000, 2
    )
    assert np.isnan(pareto_front_share_expected(1, 3))


# ──────────────────────────────────────────────────────────────────────
# Leave-one-metric-out, ellipticité et cas dégénérés
# ──────────────────────────────────────────────────────────────────────


def test_lomo_covers_every_metric(_run, _synthesis_config: SynthesisConfig) -> None:
    """`lomo=True` rapporte un `lomo_tau` par méthode listée et par métrique (Q5)."""
    df_diagnostics, _ = _run
    lomo = df_diagnostics[df_diagnostics["statistic"] == "lomo_tau"]
    assert set(lomo["item_a"]) == {"rank_mean"}
    assert set(lomo["item_b"]) == set(_synthesis_config.metric_columns)


def test_lomo_disabled_emits_nothing(
    df_synthesis_toy: pd.DataFrame, _synthesis_config: SynthesisConfig
) -> None:
    """Sans `lomo`, aucune ligne de ré-ajustement n'est produite."""
    df_scores, _, _ = run_synthesis(df_synthesis_toy, _synthesis_config)
    df_diagnostics, _ = run_coherence(
        df_synthesis_toy,
        df_scores,
        _synthesis_config,
        CoherenceConfig(topk_depths=(5, 10)),
    )
    assert not df_diagnostics["statistic"].str.startswith("lomo_").any()


def test_ellipticity_absent_without_kantorovich(_run) -> None:
    """Sans méthode `kantorovich`, l'ellipticité est absente sans lever (D-07)."""
    df_diagnostics, _ = _run
    assert not df_diagnostics["statistic"].str.startswith("ellipticity_").any()


def test_degenerate_group_skips_undefined_statistics(
    _synthesis_config: SynthesisConfig,
) -> None:
    """Un groupe de deux cellules ne garde que les statistiques définies."""
    df_metrics = pd.DataFrame(
        {
            "freq": ["A", "A"],
            "flow": [1, 1],
            "indicators": ["V", "V"],
            "TIME_PERIOD": ["2024", "2024"],
            "reporter": ["FR", "DE"],
            "product": ["a", "a"],
            "HHI": [0.8, 0.2],
            "CDI2": [0.7, 0.3],
            "CDI3": [0.6, 0.1],
            "EXPORT_HHI": [0.5, 0.4],
        }
    )
    config = SynthesisConfig(
        metric_columns=_synthesis_config.metric_columns,
        methods=(MethodSpec(name="mpi", kind="mpi"),),
        levels=("global",),
        consensus=(),
        min_group_size=3,
    )
    df_scores, _, _ = run_synthesis(df_metrics, config)
    df_diagnostics, report = run_coherence(
        df_metrics, df_scores, config, CoherenceConfig()
    )
    assert report.n_groups == 1
    emitted = set(df_diagnostics["statistic"])
    # Volumétrie et front, définis dès deux lignes, contre corrélations et
    # structure factorielle, indéfinies sous trois lignes et sous `n > d`
    assert emitted == {
        "n_rows",
        "n_complete",
        "pareto_front_share",
        "pareto_front_share_expected",
    }
    # Aucune méthode n'a scoré le groupe : la famille `methods` est vide
    assert set(df_diagnostics["family"]) == {"metrics"}


# ──────────────────────────────────────────────────────────────────────
# Rapport (S-2.7) et pureté (D-17)
# ──────────────────────────────────────────────────────────────────────


def test_report_metrics_are_finite(_run, _synthesis_config: SynthesisConfig) -> None:
    """`to_metrics()` est sans NaN ni infini et couvre chaque niveau (S-2.7)."""
    _, report = _run
    metrics = report.to_metrics()
    assert all(np.isfinite(value) for value in metrics.values())
    assert metrics["coherence.n_contexts"] == 2.0
    for level in _synthesis_config.levels:
        assert f"coherence.{level}.kendall_w" in metrics
        assert f"coherence.{level}.disputed_share" in metrics
        assert f"coherence.{level}.mean_abs_rho" in metrics
        assert f"coherence.{level}.violation_strict_mpi" in metrics


def test_runner_is_pure(
    df_synthesis_toy: pd.DataFrame,
    _synthesis_config: SynthesisConfig,
    _coherence_config: CoherenceConfig,
) -> None:
    """Les tables d'entrée ne sont pas modifiées par l'exécution (D-17)."""
    df_scores, df_fit, _ = run_synthesis(df_synthesis_toy, _synthesis_config)
    metrics_before, scores_before = df_synthesis_toy.copy(), df_scores.copy()
    run_coherence(
        df_synthesis_toy,
        df_scores,
        _synthesis_config,
        _coherence_config,
        df_fit_diagnostics=df_fit,
    )
    pd.testing.assert_frame_equal(df_synthesis_toy, metrics_before)
    pd.testing.assert_frame_equal(df_scores, scores_before)


def test_missing_column_raises(
    df_synthesis_toy: pd.DataFrame, _synthesis_config: SynthesisConfig
) -> None:
    """Une colonne absente est signalée immédiatement, avant le premier groupe."""
    df_scores, _, _ = run_synthesis(df_synthesis_toy, _synthesis_config)
    with pytest.raises(KeyError, match="absent from the metric table"):
        run_coherence(
            df_synthesis_toy.drop(columns=["CDI3"]),
            df_scores,
            _synthesis_config,
            CoherenceConfig(),
        )
    with pytest.raises(KeyError, match="absent from the score table"):
        run_coherence(
            df_synthesis_toy,
            df_scores.drop(columns=["score_global"]),
            _synthesis_config,
            CoherenceConfig(),
        )
