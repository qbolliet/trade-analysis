"""Tests des helpers purs de ``scripts/compute_synthetic_scores.py``.

Comportement figé, sans base : construction de la requête source (S-2.3),
construction de la configuration méthodologique par surcharge, et règle de
fraîcheur (S-2.1 v1). L'écriture DuckLake et ``run_synthesis`` ne sont pas
exercés ici.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path

import pytest
import yaml

# Le script importe ``dt_ducklake_manager`` en tête de module
pytest.importorskip("dt_ducklake_manager")

from scripts.compute_synthetic_scores import (  # noqa: E402
    build_source_query,
    contexts_to_recompute,
    synthesis_config_from_params,
)
from macroforecast.trade.aggregation import SynthesisConfig  # noqa: E402

UTC = timezone.utc

# Chemin de la configuration de référence (exemple S-2.2)
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "synthesis.yaml"


# ──────────────────────────────────────────────────────────────────────
# build_source_query (S-2.3)
# ──────────────────────────────────────────────────────────────────────

# Requête attendue sur l'exemple S-2.2 complet
_EXPECTED_QUERY = '''SELECT p.*, n."EXPORT_HHI", n."CENTRALITY_RISK", n."CLUSTERING_W"
FROM "vulnerabilities"."indicators"."fact_table" AS p
LEFT JOIN "vulnerabilities"."network_indicators"."fact_table" AS n
  ON substr(p."product", 1, 6) = n."product"
  AND CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER) = n."year"
  AND n."classification" = \'HS2022\'
WHERE p."flow" = 1 AND p."indicators" = \'VALUE_IN_EUROS\' AND p."freq" = \'A\'
  AND p."TIME_PERIOD" IN (
    SELECT DISTINCT "TIME_PERIOD"
    FROM "vulnerabilities"."indicators"."fact_table"
    ORDER BY 1 DESC
    LIMIT 5
  )'''


@pytest.fixture
def synthesis_block() -> dict:
    """Bloc ``SYNTHESIS`` de la configuration de référence."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)["SYNTHESIS"]


def test_build_source_query_matches_s2_2(synthesis_block: dict) -> None:
    """La requête construite sur l'exemple S-2.2 est celle attendue."""
    query = build_source_query(
        synthesis_block["SOURCES"], synthesis_block["FILTERS"], "vulnerabilities"
    )
    assert query == _EXPECTED_QUERY


def test_build_source_query_without_last_n_periods(synthesis_block: dict) -> None:
    """``LAST_N_PERIODS`` nul => aucune sous-requête sur les périodes."""
    filters = {**synthesis_block["FILTERS"], "LAST_N_PERIODS": None}
    query = build_source_query(
        synthesis_block["SOURCES"], filters, "vulnerabilities"
    )
    assert "TIME_PERIOD\" IN (" not in query
    assert "SELECT DISTINCT" not in query
    assert query.splitlines()[-1] == (
        'WHERE p."flow" = 1 AND p."indicators" = \'VALUE_IN_EUROS\' AND p."freq" = \'A\''
    )


def test_build_source_query_grid_only() -> None:
    """Grille seule, sans filtre : projection ``p.*`` et aucune clause WHERE."""
    query = build_source_query(
        [{"SCHEMA": "indicators", "ALIAS": "p", "COLUMNS": ["HHI"]}],
        {},
        "cat",
    )
    assert query == (
        'SELECT p.*\n'
        'FROM "cat"."indicators"."fact_table" AS p'
    )


def test_build_source_query_rejects_empty_sources() -> None:
    """Sans source, la construction échoue explicitement."""
    with pytest.raises(ValueError, match="grille"):
        build_source_query([], {}, "cat")


def test_build_source_query_rejects_join_without_condition() -> None:
    """Une source jointe sans condition de jointure échoue explicitement."""
    sources = [
        {"SCHEMA": "indicators", "ALIAS": "p", "COLUMNS": ["HHI"]},
        {"SCHEMA": "network_indicators", "ALIAS": "n", "COLUMNS": ["SPOF"], "JOIN": {}},
    ]
    with pytest.raises(ValueError, match="jointure"):
        build_source_query(sources, {}, "cat")


# ──────────────────────────────────────────────────────────────────────
# synthesis_config_from_params
# ──────────────────────────────────────────────────────────────────────


def test_synthesis_config_from_params_defaults() -> None:
    """Sans surcharge (``None`` ou mapping vide) : configuration par défaut."""
    assert synthesis_config_from_params(None) == SynthesisConfig()
    assert synthesis_config_from_params({}) == SynthesisConfig()


def test_synthesis_config_from_params_coercions() -> None:
    """Listes -> tuples, paires imbriquées, ``methods`` -> ``MethodSpec``."""
    config = synthesis_config_from_params(
        {
            "context_columns": ["freq", "flow"],
            "metric_columns": ["HHI", "CDI2"],
            "levels": ["global"],
            "polarities": [["HHI", -1]],
            "bootstrap_methods": ["critic_sum"],
            "random_state": 7,
            "methods": [
                {"name": "mpi", "kind": "mpi"},
                {"name": "rk", "kind": "rank_mean", "levels": ["global"]},
            ],
        }
    )
    assert config.context_columns == ("freq", "flow")
    assert config.metric_columns == ("HHI", "CDI2")
    assert config.levels == ("global",)
    assert config.polarities == (("HHI", -1),)
    assert config.bootstrap_methods == ("critic_sum",)
    assert config.random_state == 7
    assert isinstance(config.methods, tuple) and len(config.methods) == 2
    assert config.methods[0].name == "mpi" and config.methods[0].kind == "mpi"
    assert config.methods[1].levels == ("global",)


def test_synthesis_config_from_params_unknown_key_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Une clé inconnue est ignorée avec un avertissement, sans lever."""
    with caplog.at_level(logging.WARNING, logger="scripts.compute_synthetic_scores"):
        config = synthesis_config_from_params({"levels": ["global"], "made_up": 1})
    assert config.levels == ("global",)
    assert "made_up" in caplog.text


def test_synthesis_config_from_params_on_reference_config() -> None:
    """La configuration de référence est acceptée intégralement (16 méthodes)."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        parameters = yaml.safe_load(file)["SYNTHESIS"]["PARAMETERS"]
    config = synthesis_config_from_params(parameters)
    assert [spec.name for spec in config.methods] == [
        "pareto",
        "pareto_global",
        "rank_mean",
        "entropy_sum",
        "critic_sum",
        "auto_sum",
        "auto_geo",
        "mpi",
        "topsis_critic",
        "vikor_critic",
        "bod",
        "whitened",
        "kantorovich",
        "smaa",
        "cone_quantile",
    ]
    assert config.metric_columns == (
        "HHI", "CDI2", "CDI3", "EXPORT_HHI", "CENTRALITY_RISK", "CLUSTERING_W"
    )
    assert config.winsorize_quantile is None
    assert config.methods[1].levels == ("global",)
    assert config.methods[12].metrics == ("HHI", "CDI2", "CDI3", "EXPORT_HHI")


# ──────────────────────────────────────────────────────────────────────
# contexts_to_recompute (S-2.1 v1)
# ──────────────────────────────────────────────────────────────────────

_OLD = datetime(2026, 1, 1, tzinfo=UTC)
_NEW = datetime(2026, 6, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("last_upstream", "last_synthesis", "force", "expected"),
    [
        (_NEW, _OLD, False, True),    # amont plus récent
        (_OLD, _NEW, False, False),   # synthèse à jour
        (_OLD, _OLD, False, False),   # instants égaux
        (_NEW, None, False, True),    # jamais synthétisé
        (None, None, False, False),   # rien en amont
        (None, _OLD, False, False),   # rien en amont, synthèse existante
        (_OLD, _NEW, True, True),     # FORCE prime
        (None, None, True, True),     # FORCE prime sans amont
    ],
)
def test_contexts_to_recompute(
    last_upstream, last_synthesis, force, expected
) -> None:
    """Règle de fraîcheur : recalcul global de tous les contextes ou aucun."""
    assert contexts_to_recompute(last_upstream, last_synthesis, force) is expected
