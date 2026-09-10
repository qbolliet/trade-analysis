"""Tests de la couche déclarative (``macroforecast.trade.aggregation.methods``).

Le registre S-1.4 et ``build_method`` sont le seul point de traduction entre la
configuration YAML et les estimateurs : une normalisation inadmissible doit être
refusée ici, avant tout ajustement.
"""

from __future__ import annotations

import pytest

from macroforecast.trade.aggregation.methods import (
    METHOD_REGISTRY,
    MethodSpec,
    build_method,
    method_metrics,
    method_spec_from_mapping,
    registry_entry,
    resolve_normalization,
)
from macroforecast.trade.aggregation.pareto import MetricReducer, ParetoScorer
from macroforecast.trade.aggregation.synthesis import DEFAULT_METHODS, SynthesisConfig


@pytest.fixture
def _config() -> SynthesisConfig:
    """Configuration minimale à quatre métriques."""
    return SynthesisConfig(metric_columns=("HHI", "CDI2", "CDI3", "EXPORT_HHI"))


def test_registry_covers_the_s14_table() -> None:
    """Les douze ``kind`` du tableau S-1.4 sont enregistrés."""
    expected = {
        "pareto",
        "rank_mean",
        "weighted",
        "pca_projection",
        "mpi",
        "topsis",
        "vikor",
        "bod",
        "mahalanobis",
        "whitened_projection",
        "kantorovich",
        "smaa",
        "cone_quantile",
    }
    assert set(METHOD_REGISTRY) == expected
    # Défauts du tableau S-1.4 : la première normalisation est celle par défaut
    assert registry_entry("pareto").default_normalization == "none"
    assert registry_entry("kantorovich").min_group_size == 500
    assert registry_entry("pareto").supports_alert
    assert not registry_entry("mpi").has_fitted_state


def test_build_method_assembles_the_four_steps(_config: SynthesisConfig) -> None:
    """Le pipeline suit l'ordre ``orient -> winsorize -> scale -> estimate``."""
    pipeline = build_method(MethodSpec(name="bod", kind="bod"), _config)
    assert [name for name, _ in pipeline.steps] == [
        "orient",
        "winsorize",
        "scale",
        "estimate",
    ]
    # Winsorisation désactivée par défaut (M-08, D-12)
    assert pipeline.named_steps["winsorize"] == "passthrough"


def test_build_method_rejects_an_inadmissible_normalization(
    _config: SynthesisConfig,
) -> None:
    """Une normalisation hors du tableau S-1.4 lève ``ValueError``."""
    spec = MethodSpec(name="mpi_rank", kind="mpi", normalization="rank")
    with pytest.raises(ValueError, match="not admissible"):
        build_method(spec, _config)


def test_resolve_normalization_prefers_the_admissible_configuration_default(
    _config: SynthesisConfig,
) -> None:
    """D-13 : défaut de configuration s'il est admissible, défaut du ``kind`` sinon."""
    # `minmax` est admissible pour `bod`, pas pour `pareto`
    assert resolve_normalization(MethodSpec(name="b", kind="bod"), _config) == "minmax"
    assert resolve_normalization(MethodSpec(name="p", kind="pareto"), _config) == "none"
    # La normalisation « none » se traduit par une étape neutre
    pipeline = build_method(MethodSpec(name="p", kind="pareto"), _config)
    assert pipeline.named_steps["scale"] == "passthrough"


def test_pareto_reduce_mapping_becomes_a_metric_reducer(
    _config: SynthesisConfig,
) -> None:
    """``reduce`` est un mapping YAML converti en ``MetricReducer``."""
    spec = MethodSpec(
        name="pareto",
        kind="pareto",
        params={"epsilon": 0.1, "reduce": {"threshold": 0.3}, "layers": False},
    )
    estimator = build_method(spec, _config).named_steps["estimate"]
    assert isinstance(estimator, ParetoScorer)
    assert isinstance(estimator.reduce, MetricReducer)
    assert estimator.reduce.threshold == 0.3
    # Alias YAML : `layers` pilote `compute_layers`
    assert estimator.compute_layers is False


def test_method_metrics_rejects_an_unknown_metric(_config: SynthesisConfig) -> None:
    """Un sous-ensemble de métriques doit exister dans la configuration."""
    spec = MethodSpec(name="k", kind="mpi", metrics=("HHI", "UNKNOWN"))
    with pytest.raises(ValueError, match="UNKNOWN"):
        method_metrics(spec, _config)


def test_method_spec_from_mapping_coerces_and_warns(caplog) -> None:
    """Listes converties en tuples, clé inconnue ignorée avec avertissement."""
    with caplog.at_level("WARNING"):
        spec = method_spec_from_mapping(
            {
                "name": "critic_sum",
                "kind": "weighted",
                "metrics": ["HHI", "CDI2"],
                "levels": ["by_reporter", "global"],
                "params": {"weighting": "critic"},
                "unexpected": 1,
            }
        )
    assert spec.metrics == ("HHI", "CDI2")
    assert spec.levels == ("by_reporter", "global")
    assert spec.params["weighting"] == "critic"
    assert "unexpected" in caplog.text


def test_method_spec_from_mapping_requires_name_and_kind() -> None:
    """``name`` et ``kind`` sont obligatoires, le ``kind`` est vérifié."""
    with pytest.raises(KeyError):
        method_spec_from_mapping({"kind": "mpi"})
    with pytest.raises(ValueError, match="Unknown method kind"):
        method_spec_from_mapping({"name": "x", "kind": "unknown_kind"})


def test_default_methods_are_all_buildable() -> None:
    """Chaque méthode par défaut (S-2.2) se construit sans erreur."""
    config = SynthesisConfig(
        metric_columns=(
            "HHI",
            "CDI2",
            "CDI3",
            "EXPORT_HHI",
            "CENTRALITY_RISK",
            "CLUSTERING_W",
        )
    )
    for spec in DEFAULT_METHODS:
        pipeline = build_method(spec, config)
        assert pipeline.steps[-1][0] == "estimate"
