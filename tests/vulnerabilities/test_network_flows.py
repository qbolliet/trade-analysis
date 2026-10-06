"""Métriques réseau à l'import et à l'export (graphe BACI transposé), sur un graphe fictif."""

from __future__ import annotations

import narwhals as nw
import pandas as pd
import pytest

import macroforecast.trade.vulnerabilities.network_metrics as network_metrics
from macroforecast.trade.vulnerabilities import (
    NetworkVulnerabilityConfig,
    compute_network_vulnerabilities,
    default_network_metrics,
)
from tests.vulnerabilities.fixtures import GOLDEN_DIR, network_frame

CONFIG = NetworkVulnerabilityConfig()
KEYS = list(CONFIG.key_columns)
ORIENTED = ["CENTRALITY_RISK", "WORLD_HHI", "SPOF", "SPOF_DECILE"]
INVARIANT = ["CLUSTERING_W", "DIAMETER"]


def _run(flows) -> pd.DataFrame:
    data = nw.from_native(network_frame(), eager_only=True)
    result, _ = compute_network_vulnerabilities(
        data, default_network_metrics(CONFIG, flows), CONFIG, flows=flows
    )
    return result.to_pandas()


def _flow(result: pd.DataFrame, code: int) -> pd.DataFrame:
    rows = result[result[CONFIG.flow_col] == code].drop(columns=CONFIG.flow_col)
    return rows.sort_values(KEYS).reset_index(drop=True)


def _golden(name: str) -> pd.DataFrame:
    frame = pd.read_csv(GOLDEN_DIR / f"{name}.csv", dtype={"classification": str, "product": str}, float_precision="round_trip")
    return frame.sort_values(KEYS).reset_index(drop=True)


def test_import_values_are_unchanged() -> None:
    result = _run(("import",))
    assert set(result[CONFIG.flow_col]) == {CONFIG.import_flow}
    expected = _golden("network_import")
    pd.testing.assert_frame_equal(
        _flow(result, CONFIG.import_flow)[expected.columns], expected, check_exact=True, check_dtype=False
    )


def test_export_equals_import_on_the_transposed_graph() -> None:
    result = _run(("import", "export"))
    assert sorted(set(result[CONFIG.flow_col])) == [CONFIG.import_flow, CONFIG.export_flow]
    assert pd.api.types.is_integer_dtype(result[CONFIG.flow_col])

    imports, exports = _flow(result, CONFIG.import_flow), _flow(result, CONFIG.export_flow)
    pd.testing.assert_frame_equal(
        imports[_golden("network_import").columns], _golden("network_import"),
        check_exact=True, check_dtype=False,
    )
    transposed = _golden("network_transposed")
    pd.testing.assert_frame_equal(
        exports[KEYS + ORIENTED], transposed[KEYS + ORIENTED], check_exact=True, check_dtype=False
    )
    # Métriques symétriques : même valeur dans les deux sens
    pd.testing.assert_frame_equal(exports[KEYS + INVARIANT], imports[KEYS + INVARIANT])

    # P1 (exportateur dominant) le plus exposé à l'import, P2 (importateur dominant) à l'export
    for frame, product in ((imports, "P1"), (exports, "P2")):
        top = frame.loc[frame.groupby("year")["SPOF"].idxmax(), "product"]
        assert set(top) == {product}


def test_spof_ranks_are_taken_within_each_flow() -> None:
    result = _run(("import", "export"))
    # Trois produits par (sens, année) : percentiles en tiers, maximum 1 dans chaque sens
    for (_, _), group in result.groupby([CONFIG.flow_col, "year"]):
        assert group["SPOF"].max() == pytest.approx(1.0)
        assert ((group["SPOF"] * 6).round(9) % 1 == 0).all()


def test_invariant_metrics_are_computed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    original = network_metrics.compute_graph_features

    def spy(*args, **kwargs):
        calls.append(kwargs["features"])
        return original(*args, **kwargs)

    monkeypatch.setattr(network_metrics, "compute_graph_features", spy)
    _run(("import", "export"))
    # Un appel pour le clustering, un pour le diamètre, quel que soit le nombre de sens
    assert sorted(calls) == sorted([(network_metrics.CLUSTERING_W,), (network_metrics.DIAMETER,)])


def test_drift_ignores_a_previous_result_without_flow() -> None:
    previous = nw.from_native(_golden("network_import"), eager_only=True)
    data = nw.from_native(network_frame(), eager_only=True)
    _, report = compute_network_vulnerabilities(
        data, default_network_metrics(CONFIG), CONFIG, df_previous=previous
    )
    assert report.flows["import"].drift is None
