"""Tests des helpers purs de ``scripts/compute_synthesis_coherence.py``.

Comportement figé, sans base : construction de la configuration méthodologique de
la cohérence par surcharge, sélection des contextes de la table des métriques et
construction de la requête des scores restreinte à ces contextes (S-2.4).
L'écriture DuckLake et ``run_coherence`` ne sont pas exercés ici.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

# Le script importe ``dt_ducklake_manager`` (via le script de synthèse) en tête de module
pytest.importorskip("dt_ducklake_manager")

from scripts.compute_synthesis_coherence import (  # noqa: E402
    _aggregate_reports,
    _sql_literal,
    build_scores_query,
    coherence_config_from_params,
    distinct_contexts,
)
from macroforecast.trade.aggregation import (  # noqa: E402
    CoherenceConfig,
    CoherenceLevelReport,
    CoherenceRunReport,
)

# Chemin de la configuration de référence (exemple S-2.2)
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "synthesis.yaml"


# ──────────────────────────────────────────────────────────────────────
# coherence_config_from_params (coercions)
# ──────────────────────────────────────────────────────────────────────


def test_coherence_config_from_params_defaults() -> None:
    """Sans surcharge (``None`` ou mapping vide) : configuration par défaut."""
    assert coherence_config_from_params(None) == CoherenceConfig()
    assert coherence_config_from_params({}) == CoherenceConfig()


def test_coherence_config_from_params_coercions() -> None:
    """Listes -> tuples pour les champs tuple, scalaires transmis tels quels."""
    config = coherence_config_from_params(
        {
            "topk_depths": [5, 25],
            "lomo_methods": ["critic_sum"],
            "rbo_p": 0.9,
            "lomo": True,
            "metric_pairs": False,
        }
    )
    assert config.topk_depths == (5, 25)
    assert config.lomo_methods == ("critic_sum",)
    assert config.rbo_p == 0.9
    assert config.lomo is True
    assert config.metric_pairs is False


def test_coherence_config_from_params_unknown_key_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Une clé inconnue est ignorée avec un avertissement, sans lever."""
    with caplog.at_level(
        logging.WARNING, logger="scripts.compute_synthesis_coherence"
    ):
        config = coherence_config_from_params({"rbo_p": 0.95, "made_up": 1})
    assert config.rbo_p == 0.95
    assert config == CoherenceConfig(rbo_p=0.95)
    assert "made_up" in caplog.text


def test_coherence_config_from_params_on_reference_config() -> None:
    """La configuration de référence est acceptée intégralement."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        parameters = yaml.safe_load(file)["COHERENCE"]["PARAMETERS"]
    config = coherence_config_from_params(parameters)
    assert config.topk_depths == (10, 50, 100)
    assert config.rbo_p == 0.98
    assert config.dispute_fraction == 0.01
    assert config.lomo is True
    assert config.lomo_methods == ("critic_sum", "auto_sum")
    assert config.metric_pairs is True


# ──────────────────────────────────────────────────────────────────────
# _sql_literal (coercions)
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("VALUE_IN_EUROS", "'VALUE_IN_EUROS'"),
        ("d'Ivoire", "'d''Ivoire'"),
        (1, "1"),
        (np.int64(2024), "2024"),
        (True, "TRUE"),
        (np.bool_(False), "FALSE"),
        (2.5, "2.5"),
    ],
)
def test_sql_literal(value: object, expected: str) -> None:
    """Chaînes échappées, booléens, entiers et réels (scalaires numpy compris)."""
    assert _sql_literal(value) == expected


# ──────────────────────────────────────────────────────────────────────
# distinct_contexts (sélection des contextes)
# ──────────────────────────────────────────────────────────────────────


def test_distinct_contexts_first_seen_order() -> None:
    """Contextes distincts dans l'ordre de première apparition, colonnes ordonnées."""
    frame = pd.DataFrame(
        {
            "freq": ["A", "A", "A", "A"],
            "flow": [1, 1, 1, 2],
            "TIME_PERIOD": ["2023", "2023", "2024", "2024"],
            "reporter": ["FR", "DE", "FR", "FR"],
        }
    )
    assert distinct_contexts(frame, ["freq", "flow", "TIME_PERIOD"]) == [
        ("A", 1, "2023"),
        ("A", 1, "2024"),
        ("A", 2, "2024"),
    ]


def test_distinct_contexts_empty_frame() -> None:
    """Table vide : aucune ligne de contexte."""
    frame = pd.DataFrame({"freq": [], "TIME_PERIOD": []})
    assert distinct_contexts(frame, ["freq", "TIME_PERIOD"]) == []


# ──────────────────────────────────────────────────────────────────────
# build_scores_query (S-2.4)
# ──────────────────────────────────────────────────────────────────────

_EXPECTED_SCORES_QUERY = '''SELECT * FROM "vulnerabilities"."synthesis"."fact_table"
WHERE ("freq", "flow", "indicators", "TIME_PERIOD") IN (
    ('A', 1, 'VALUE_IN_EUROS', '2023'),
    ('A', 1, 'VALUE_IN_EUROS', '2024')
  )'''


def test_build_scores_query_row_value_in_list() -> None:
    """Filtre par liste de n-uplets sur la clé de contexte."""
    query = build_scores_query(
        "vulnerabilities",
        "synthesis",
        ["freq", "flow", "indicators", "TIME_PERIOD"],
        [("A", 1, "VALUE_IN_EUROS", "2023"), ("A", 1, "VALUE_IN_EUROS", "2024")],
    )
    assert query == _EXPECTED_SCORES_QUERY


def test_build_scores_query_without_contexts_guards() -> None:
    """Aucun contexte : garde-fou ``WHERE FALSE``, aucun balayage complet."""
    query = build_scores_query("cat", "synthesis", ["freq"], [])
    assert query == (
        'SELECT * FROM "cat"."synthesis"."fact_table"\nWHERE FALSE'
    )


def test_build_scores_query_from_distinct_contexts_roundtrip() -> None:
    """La requête consomme directement la sortie de ``distinct_contexts``."""
    frame = pd.DataFrame(
        {
            "freq": ["A", "A"],
            "flow": np.array([1, 1], dtype="int64"),
            "indicators": ["VALUE_IN_EUROS", "VALUE_IN_EUROS"],
            "TIME_PERIOD": ["2023", "2024"],
        }
    )
    columns = ["freq", "flow", "indicators", "TIME_PERIOD"]
    query = build_scores_query(
        "vulnerabilities", "synthesis", columns, distinct_contexts(frame, columns)
    )
    assert query == _EXPECTED_SCORES_QUERY


# ──────────────────────────────────────────────────────────────────────
# _aggregate_reports
# ──────────────────────────────────────────────────────────────────────


def test_aggregate_reports_sums_counts_only() -> None:
    """Compteurs sommés ; médianes par niveau laissées à ``NaN`` (écartées par MLflow)."""
    first = CoherenceRunReport(n_contexts=1, n_groups=3)
    first.levels = {
        "global": CoherenceLevelReport(n_groups=1, kendall_w=0.8),
        "by_product": CoherenceLevelReport(n_groups=2, kendall_w=0.4),
    }
    second = CoherenceRunReport(n_contexts=1, n_groups=2)
    second.levels = {"global": CoherenceLevelReport(n_groups=1, kendall_w=0.2)}

    total = _aggregate_reports([first, second])
    assert total.n_contexts == 2
    assert total.n_groups == 5
    assert total.levels["global"].n_groups == 2
    assert total.levels["by_product"].n_groups == 2
    # Les médianes ne sont pas recombinées
    assert np.isnan(total.levels["global"].kendall_w)
    metrics = total.to_metrics()
    assert metrics["coherence.n_groups"] == 5.0
    assert not any(key.endswith("kendall_w") for key in metrics)


def test_aggregate_reports_empty() -> None:
    """Aucun rapport : rapport d'exécution vide."""
    total = _aggregate_reports([])
    assert total.n_contexts == 0 and total.n_groups == 0 and total.levels == {}
