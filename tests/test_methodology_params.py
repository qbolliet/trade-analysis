"""Paramètres méthodologiques des empreintes de fraîcheur (``macroforecast``).

Vérifie que les listes d'exclusion de l'empreinte ne nomment que des champs
existants et purement diagnostiques, et que les seuils d'alerte (qui changent les
colonnes ``{métrique}_ALERT`` écrites) restent dans l'empreinte.
"""

from __future__ import annotations

from dataclasses import dataclass, fields

import pytest

from macroforecast.trade.methodology import methodology_params
from macroforecast.trade.processing import (
    BACI_FINGERPRINT_EXCLUDED,
    BaciConfig,
)
from macroforecast.trade.vulnerabilities import (
    DEFAULT_METRIC_CLASSES,
    DEFAULT_NETWORK_METRIC_CLASSES,
    NETWORK_FINGERPRINT_EXCLUDED,
    VULNERABILITY_FINGERPRINT_EXCLUDED,
    NetworkVulnerabilityConfig,
    VulnerabilityConfig,
)


@pytest.mark.parametrize(
    "config, excluded",
    [
        (VulnerabilityConfig(), VULNERABILITY_FINGERPRINT_EXCLUDED),
        (NetworkVulnerabilityConfig(), NETWORK_FINGERPRINT_EXCLUDED),
        (BaciConfig(), BACI_FINGERPRINT_EXCLUDED),
    ],
)
def test_exclusions_are_fields_and_alert_thresholds_stay(config, excluded) -> None:
    names = {f.name for f in fields(config)}
    assert set(excluded) <= names
    params = methodology_params(config, excluded)
    assert set(params) == names - set(excluded)
    if "metric_alert_thresholds" in names:
        assert "metric_alert_thresholds" in params
        assert "high_score_threshold" in params
    assert not {"artifact_top_n", "drift_relative_change", "psi_n_bins"} & set(params)


def test_baci_fingerprint_keeps_every_field_including_nested_schema() -> None:
    params = methodology_params(BaciConfig(), BACI_FINGERPRINT_EXCLUDED)
    assert params["schema"]["distance_column"] == "distw"
    assert params["tonne_conversion_factors"] == {"21": 1.0, "8": 0.001}


def test_methodology_params_rejects_unknown_exclusion_and_non_dataclass() -> None:
    with pytest.raises(ValueError):
        methodology_params(VulnerabilityConfig(), {"not_a_field"})
    with pytest.raises(TypeError):
        methodology_params({"a": 1})


def test_methodology_params_is_order_stable() -> None:
    @dataclass(frozen=True)
    class Config:
        mapping: dict

    assert methodology_params(Config({"b": 1, "a": 2})) == methodology_params(Config({"a": 2, "b": 1}))


def test_metric_classes_declare_no_version() -> None:
    # Aucune version dans le code : une correction se signale par invalidation
    assert not any(
        hasattr(cls, "version") for cls in (*DEFAULT_METRIC_CLASSES, *DEFAULT_NETWORK_METRIC_CLASSES)
    )
