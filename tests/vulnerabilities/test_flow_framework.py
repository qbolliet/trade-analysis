"""Cadre commun du sens : hyperparamètre ``flow`` des deux familles de métriques."""

from __future__ import annotations

from typing import ClassVar

import narwhals as nw
import pytest

from macroforecast.trade.vulnerabilities import (
    NetworkVulnerabilityConfig,
    NetworkVulnerabilityMetric,
    VulnerabilityConfig,
    VulnerabilityMetric,
    flow_code_map,
)
from macroforecast.trade.vulnerabilities.metrics import (
    ConcentrationDependencyIndex3,
    HerfindahlHirschmanIndex,
    default_metrics,
)
from macroforecast.trade.vulnerabilities.network_metrics import (
    NetworkDiameter,
    WeightedClusteringCoefficient,
    WorldExportConcentration,
    default_network_metrics,
)


# Métrique partenaire fictive au support par défaut (import seul)
class _ImportOnly(VulnerabilityMetric):
    name: ClassVar[str] = "IMPORT_ONLY"

    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        return data


# Métrique réseau fictive au support par défaut (import seul)
class _NetworkImportOnly(NetworkVulnerabilityMetric):
    name: ClassVar[str] = "NETWORK_IMPORT_ONLY"

    def compute(self, data: nw.DataFrame) -> nw.DataFrame:
        return data


@pytest.mark.parametrize("cls", [_ImportOnly, _NetworkImportOnly])
def test_unsupported_flow_raises(cls) -> None:
    assert cls().flow == "import"
    with pytest.raises(ValueError, match="does not support flow 'export'"):
        cls(flow="export")
    with pytest.raises(ValueError, match="Unknown flow"):
        cls(flow="transit")


def test_flow_enters_the_fingerprint_params() -> None:
    import_params = HerfindahlHirschmanIndex(flow="import").fingerprint_params()
    export_params = HerfindahlHirschmanIndex(flow="export").fingerprint_params()
    assert import_params["flow"] == "import" and export_params["flow"] == "export"
    # Seul le sens distingue les deux empreintes
    assert {k: v for k, v in import_params.items() if k != "flow"} == {
        k: v for k, v in export_params.items() if k != "flow"
    }
    assert HerfindahlHirschmanIndex(flow="export").fingerprint_key == "HHI/export"
    assert WorldExportConcentration(flow="export").fingerprint_params()["flow"] == "export"


@pytest.mark.parametrize(
    "cls, config",
    [
        (ConcentrationDependencyIndex3, VulnerabilityConfig()),
        (WorldExportConcentration, NetworkVulnerabilityConfig()),
    ],
)
def test_configuration_is_never_altered_by_the_direction(cls, config) -> None:
    import_metric, export_metric = cls(config), cls(config, flow="export")
    assert export_metric.config == import_metric.config
    assert export_metric.config is config


def test_partner_flow_codes_follow_the_configuration() -> None:
    config = VulnerabilityConfig(import_flow=10, export_flow=20)
    metric = ConcentrationDependencyIndex3(config, flow="export")
    assert (metric.own_flow_code, metric.other_flow_code) == (20, 10)
    assert flow_code_map(config) == {"import": 10, "export": 20}


def test_network_roles() -> None:
    config = NetworkVulnerabilityConfig(exporter_col="o", importer_col="d")
    import_metric = WorldExportConcentration(config)
    export_metric = WorldExportConcentration(config, flow="export")
    assert (import_metric.counterpart_col, import_metric.exposed_col) == ("o", "d")
    assert (export_metric.counterpart_col, export_metric.exposed_col) == ("d", "o")


def test_orientation_invariance_declarations() -> None:
    invariant = {
        type(metric) for metric in default_network_metrics() if metric.orientation_invariant
    }
    assert invariant == {WeightedClusteringCoefficient, NetworkDiameter}


def test_registries_instantiate_every_supported_direction() -> None:
    assert [(m.name, m.flow) for m in default_metrics(flows=("export",))] == [
        ("HHI", "export"), ("CDI2", "export"), ("CDI3", "export"),
    ]
    assert len(default_network_metrics(flows=("import", "export"))) == 12
