"""Métriques partenaires à l'import et à l'export, sur données fictives à deux flux."""

from __future__ import annotations

import narwhals as nw
import numpy as np
import pandas as pd
import pytest

from macroforecast.trade.vulnerabilities import (
    VulnerabilityConfig,
    compute_vulnerabilities,
    default_metrics,
)
from tests.vulnerabilities.fixtures import GOLDEN_DIR, MISSING_EXTRA_EU_CELL, partner_frame

CONFIG = VulnerabilityConfig()
KEYS = list(CONFIG.key_columns)
TEXT_KEYS = {"freq": str, "reporter": str, "product": str, "indicators": str, "TIME_PERIOD": str}


def _run(flows) -> tuple[pd.DataFrame, object]:
    data = nw.from_native(partner_frame(), eager_only=True)
    result, report = compute_vulnerabilities(
        data, default_metrics(CONFIG, flows), CONFIG, flows=flows
    )
    return result.to_pandas().sort_values(KEYS).reset_index(drop=True), report


def _golden() -> pd.DataFrame:
    return pd.read_csv(GOLDEN_DIR / "partners.csv", dtype=TEXT_KEYS, float_precision="round_trip")


def _by_cell(frame: pd.DataFrame, partner: str, flow: int) -> pd.Series:
    """Valeur d'un agrégat partenaire par (reporter, product, period)."""
    rows = frame[(frame["partner"] == partner) & (frame["flow"] == flow)]
    return rows.set_index(["reporter", "product", "TIME_PERIOD"])["OBS_VALUE"]


def test_import_only_writes_no_export_row_and_keeps_previous_values() -> None:
    result, report = _run(("import",))
    assert set(result["flow"]) == {CONFIG.import_flow}
    expected = _golden()
    expected = expected[expected["flow"] == CONFIG.import_flow].sort_values(KEYS).reset_index(drop=True)
    pd.testing.assert_frame_equal(result[expected.columns], expected, check_exact=True, check_dtype=False)
    assert list(report.flows) == ["import"]


def test_both_flows_score_every_metric_on_both_flows() -> None:
    result, report = _run(("import", "export"))
    assert set(result["flow"]) == {CONFIG.import_flow, CONFIG.export_flow}
    golden = _golden().sort_values(KEYS).reset_index(drop=True)

    # Import inchangé ; HHI export égal à l'ancien HHI du flux 2 (déjà calculé pour tous les flux)
    imports = result[result["flow"] == CONFIG.import_flow].reset_index(drop=True)
    expected_imports = golden[golden["flow"] == CONFIG.import_flow].reset_index(drop=True)
    pd.testing.assert_frame_equal(
        imports[expected_imports.columns], expected_imports, check_exact=True, check_dtype=False
    )
    exports = result[result["flow"] == CONFIG.export_flow].set_index(["reporter", "product", "TIME_PERIOD"])
    expected_hhi = golden[golden["flow"] == CONFIG.export_flow].set_index(["reporter", "product", "TIME_PERIOD"])["HHI"]
    pd.testing.assert_series_equal(exports["HHI"].sort_index(), expected_hhi.sort_index(), check_exact=True)

    # Calcul à la main : exports extra-UE / exports totaux, exports extra-UE / imports totaux
    source = partner_frame()
    extra_exports = _by_cell(source, "EXT_EU", CONFIG.export_flow)
    world_exports = _by_cell(source, "WORLD", CONFIG.export_flow)
    world_imports = _by_cell(source, "WORLD", CONFIG.import_flow)
    expected_cdi2 = (extra_exports / world_exports).reindex(exports.index)
    expected_cdi3 = (extra_exports / world_imports).reindex(exports.index)
    np.testing.assert_allclose(exports["CDI2"].to_numpy(float), expected_cdi2.to_numpy(float), rtol=1e-12)
    np.testing.assert_allclose(exports["CDI3"].to_numpy(float), expected_cdi3.to_numpy(float), rtol=1e-12)

    # Cellule export privée d'agrégat extra-UE : CDI2 / CDI3 nuls, alertes fausses
    missing = exports.loc[MISSING_EXTRA_EU_CELL]
    assert pd.isna(missing["CDI2"]) and pd.isna(missing["CDI3"]) and not missing["CDI2_ALERT"]
    # Reporter agrégé : scoré dans les métriques (son exclusion relève de la synthèse)
    assert exports.loc["EU27_2020"]["HHI"].notna().all()

    # Diagnostics par sens : l'agrégat extra-UE manquant n'est vu qu'à l'export
    n_export_cells = report.flows["export"].cells
    assert report.flows["import"].quality.share_cells_missing_extra_eu == 0.0
    assert report.flows["export"].quality.share_cells_missing_extra_eu == pytest.approx(1 / n_export_cells)
    assert report.flows["export"].coverage.share_non_null["CDI2"] == pytest.approx(1 - 1 / n_export_cells)
    assert report.cells == report.flows["import"].cells + n_export_cells


def test_drift_is_measured_within_each_flow() -> None:
    result, _ = _run(("import", "export"))
    data = nw.from_native(partner_frame(), eager_only=True)
    previous = nw.from_native(result, eager_only=True)
    _, report = compute_vulnerabilities(
        data, default_metrics(CONFIG, ("export",)), CONFIG, flows=("export",), df_previous=previous
    )
    drift = report.flows["export"].drift
    # Le résultat précédent contient aussi l'import : aucune cellule « disparue »
    assert drift.n_disappeared_cells == 0 and drift.n_new_cells == 0
    assert drift.spearman["HHI"] == pytest.approx(1.0)
