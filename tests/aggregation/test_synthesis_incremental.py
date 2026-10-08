"""Synthèse incrémentale par méthode (``run_synthesis(methods=…, df_existing_scores=…)``).

Une méthode calculée seule doit valoir la même méthode calculée avec toutes les
autres (graines de groupe indépendantes du sous-ensemble) ; le consensus, qui
classe toutes les méthodes configurées, doit être identique à celui d'un calcul
complet dès que les scores des méthodes non recalculées lui sont fournis.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from macroforecast.trade.aggregation.methods import MethodSpec
from macroforecast.trade.aggregation.synthesis import (
    CONSENSUS_PREFIX,
    SynthesisConfig,
    run_synthesis,
    selected_methods,
)

# Clé primaire de la table des scores (contexte, cellule, méthode)
PRIMARY_KEY = ["freq", "flow", "indicators", "TIME_PERIOD", "reporter", "product", "method"]

# Méthodes : déterministes, aléatoires (SMAA, quantile de cône) et à bootstrap
_METHODS = (
    MethodSpec(name="pareto", kind="pareto", params={"epsilon": 0.1}),
    MethodSpec(name="rank_mean", kind="rank_mean"),
    MethodSpec(name="critic_sum", kind="weighted", params={"weighting": "critic"}),
    MethodSpec(name="bod", kind="bod"),
    MethodSpec(name="smaa", kind="smaa"),
    MethodSpec(name="cone_quantile", kind="cone_quantile"),
    MethodSpec(name="mpi", kind="mpi"),
)
# Partition des méthodes en deux passes
_FIRST = ("pareto", "critic_sum", "smaa", "mpi")
_SECOND = ("rank_mean", "bod", "cone_quantile")


@pytest.fixture
def _config() -> SynthesisConfig:
    """Configuration réduite : consensus Borda et Copeland, bootstrap au global."""
    return SynthesisConfig(
        metric_columns=("HHI", "CDI2", "CDI3", "WORLD_HHI"),
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


def _sorted(frame: pd.DataFrame) -> pd.DataFrame:
    """Tri canonique d'une table de scores (comparaison indépendante de l'ordre)."""
    return frame.sort_values(PRIMARY_KEY).reset_index(drop=True)


def _rows(frame: pd.DataFrame, methods) -> pd.DataFrame:
    """Lignes des méthodes données, triées."""
    return _sorted(frame[frame["method"].isin(list(methods))])


def _consensus_names(config: SynthesisConfig):
    """Noms des pseudo-méthodes de consensus de la configuration."""
    return [f"{CONSENSUS_PREFIX}{rule}" for rule in config.consensus]


def test_two_passes_equal_complete_run(df_synthesis_toy: pd.DataFrame, _config) -> None:
    """Calcul complet == méthodes A, puis méthodes B avec les scores de A fournis."""
    full, full_fit, _ = run_synthesis(df_synthesis_toy, _config, log_artifacts=False)

    first, first_fit, _ = run_synthesis(
        df_synthesis_toy, _config, methods=_FIRST, log_artifacts=False
    )
    second, second_fit, _ = run_synthesis(
        df_synthesis_toy, _config, methods=_SECOND,
        df_existing_scores=first, log_artifacts=False,
    )

    # Scores des méthodes : identiques au calcul complet, quelle que soit la passe
    pd.testing.assert_frame_equal(_rows(first, _FIRST), _rows(full, _FIRST))
    pd.testing.assert_frame_equal(_rows(second, _SECOND), _rows(full, _SECOND))
    # Consensus de la seconde passe : identique au consensus complet
    consensus = _consensus_names(_config)
    pd.testing.assert_frame_equal(_rows(second, consensus), _rows(full, consensus))
    # Aucune ligne des méthodes non demandées n'est produite
    assert set(first["method"]) == set(_FIRST) | set(consensus)
    assert set(second["method"]) == set(_SECOND) | set(consensus)
    # Diagnostics d'ajustement : ceux des méthodes calculées seulement
    assert set(first_fit["item_a"]) <= set(_FIRST)
    assert set(second_fit["item_a"]) <= set(_SECOND)
    pd.testing.assert_frame_equal(
        second_fit.sort_values(list(second_fit.columns)).reset_index(drop=True),
        full_fit[full_fit["item_a"].isin(_SECOND)]
        .sort_values(list(full_fit.columns)).reset_index(drop=True),
    )


def test_consensus_alone_from_stored_scores(df_synthesis_toy: pd.DataFrame, _config) -> None:
    """``methods=[]`` : seul le consensus est produit, égal au consensus complet."""
    full, _, _ = run_synthesis(df_synthesis_toy, _config, log_artifacts=False)
    stored = full[~full["method"].str.startswith(CONSENSUS_PREFIX)]

    only, fit, report = run_synthesis(
        df_synthesis_toy, _config, methods=[], df_existing_scores=stored, log_artifacts=False
    )

    consensus = _consensus_names(_config)
    assert set(only["method"]) == set(consensus)
    assert fit.empty
    assert report.n_contexts == 2
    pd.testing.assert_frame_equal(_rows(only, consensus), _rows(full, consensus))


def test_stored_scores_typed_differently_are_aligned(df_synthesis_toy: pd.DataFrame, _config) -> None:
    """Les cellules sont appariées sur leur forme texte (codes produits lus en texte)."""
    full, _, _ = run_synthesis(df_synthesis_toy, _config, log_artifacts=False)
    stored = full[full["method"].isin(_FIRST)].copy()
    # Ordre des lignes mélangé et contexte relu en texte : appariement par clé
    stored = stored.sample(frac=1.0, random_state=0)
    stored["flow"] = stored["flow"].astype(str)

    second, _, _ = run_synthesis(
        df_synthesis_toy, _config, methods=_SECOND, df_existing_scores=stored, log_artifacts=False
    )
    consensus = _consensus_names(_config)
    pd.testing.assert_frame_equal(_rows(second, consensus), _rows(full, consensus))


def test_methods_absent_from_configuration_are_ignored(df_synthesis_toy: pd.DataFrame, _config) -> None:
    """Une méthode retirée de la configuration ne pèse plus sur le consensus."""
    full, _, _ = run_synthesis(df_synthesis_toy, _config, log_artifacts=False)
    stored = full[full["method"].isin(_FIRST)].copy()
    # Scores d'une méthode retirée : ignorés
    removed = stored[stored["method"] == "mpi"].copy()
    removed["method"] = "removed_method"
    removed["score_global"] = np.random.default_rng(1).random(len(removed))

    second, _, _ = run_synthesis(
        df_synthesis_toy, _config, methods=_SECOND,
        df_existing_scores=pd.concat([stored, removed]), log_artifacts=False,
    )
    consensus = _consensus_names(_config)
    pd.testing.assert_frame_equal(_rows(second, consensus), _rows(full, consensus))


def test_unknown_method_name_is_rejected(df_synthesis_toy: pd.DataFrame, _config) -> None:
    with pytest.raises(ValueError, match="not configured"):
        run_synthesis(df_synthesis_toy, _config, methods=["critic"], log_artifacts=False)
    with pytest.raises(ValueError):
        selected_methods(_config, ["typo"])


def test_existing_scores_missing_columns_raise(df_synthesis_toy: pd.DataFrame, _config) -> None:
    full, _, _ = run_synthesis(df_synthesis_toy, _config, methods=_FIRST, log_artifacts=False)
    with pytest.raises(KeyError, match="score_global"):
        run_synthesis(
            df_synthesis_toy, _config, methods=_SECOND,
            df_existing_scores=full.drop(columns=["score_global"]), log_artifacts=False,
        )


def test_without_new_parameters_output_is_unchanged(df_synthesis_toy: pd.DataFrame, _config) -> None:
    """``methods=None`` et ``df_existing_scores=None`` : sortie strictement identique."""
    default, default_fit, _ = run_synthesis(df_synthesis_toy, _config, log_artifacts=False)
    explicit, explicit_fit, _ = run_synthesis(
        df_synthesis_toy, _config, methods=None, df_existing_scores=None, log_artifacts=False
    )
    pd.testing.assert_frame_equal(default, explicit)
    pd.testing.assert_frame_equal(default_fit, explicit_fit)
