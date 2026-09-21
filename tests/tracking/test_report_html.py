"""Rapport HTML autonome : avec et sans plotly, tables tronquées."""

from __future__ import annotations

import sys

import pandas as pd
import pytest

from macroforecast.tracking import figures
from macroforecast.tracking.report import Check, RunReport, Section, Units, build_report


def _report(sections) -> RunReport:
    return build_report(
        "process_baci_HS2017",
        metrics={"baci/gravity/r_squared": 0.42},
        checks=[Check("baci/gravity/r_squared", ">=", 0.5, "warning", "R² de la gravité")],
        units=Units(1, 1, 0),
        key_figures=["212 M flux"],
        sections=sections,
        context={"workflow_id": "wf-1"},
    )


def test_page_is_self_contained_and_lists_verdict_checks_and_key_figures() -> None:
    page = _report([]).to_html()
    assert page.startswith("<!doctype html>") and page.rstrip().endswith("</html>")
    assert "⚠️ process_baci_HS2017 — avertissement" in page
    assert "R² de la gravité" in page and "212 M flux" in page and "wf-1" in page
    # Aucune ressource externe : ni script ni feuille de style distants
    assert "src=" not in page and "<link" not in page


def test_tables_are_truncated_to_max_table_rows() -> None:
    table = pd.DataFrame({"pays": [f"P{i}" for i in range(120)], "sigma": range(120)})
    page = _report([Section("Qualité", tables={"σ̂ par pays": table})]).to_html(max_table_rows=10)
    assert "P9<" in page and "P10<" not in page
    assert "110 ligne(s) non affichée(s)" in page


def test_text_is_html_escaped() -> None:
    page = _report([Section("<script>x</script>", notes=["a < b"])]).to_html()
    assert "<script>x</script>" not in page and "&lt;script&gt;" in page and "a &lt; b" in page


def test_unknown_plotly_js_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="plotly_js"):
        _report([]).to_html(plotly_js="file")


class _Figure:
    """Figure factice : enregistre le mode d'inclusion de plotly.js demandé."""

    def __init__(self, name: str) -> None:
        self.name, self.calls = name, []

    def to_html(self, full_html: bool, include_plotlyjs) -> str:
        self.calls.append(include_plotlyjs)
        return f"<div>figure {self.name}</div>"


@pytest.mark.parametrize(("mode", "first"), [("inline", True), ("cdn", "cdn")])
def test_plotly_js_is_embedded_once_by_the_first_figure(mode: str, first) -> None:
    figs = [_Figure("a"), _Figure("b"), _Figure("c")]
    page = _report([Section("Un", figures=figs[:2]), Section("Deux", figures=figs[2:])]).to_html(plotly_js=mode)
    assert [f.calls for f in figs] == [[first], [False], [False]]
    assert all(f"figure {f.name}" in page for f in figs)


def test_with_plotly_the_baci_sections_carry_real_figures() -> None:
    pytest.importorskip("plotly")
    metrics = {
        "baci/tonnage/share_tonnage_missing": 0.02,
        "baci/tonnage/share_converted_from_other_units": 0.3,
        "baci/gravity/r_squared": 0.7,
        "baci/gravity/coefficients/log_dist": -0.8,
        "baci/flows": 1000.0,
    }
    sections = figures.sections_baci(metrics, {})
    assert any(section.figures for section in sections)
    page = _report(sections).to_html(plotly_js="cdn")
    assert "plotly" in page.lower() and "Gravité" in page


def test_without_plotly_sections_keep_their_tables_but_lose_their_figures(monkeypatch) -> None:
    # Import de plotly rendu impossible : `None` dans sys.modules lève ImportError
    monkeypatch.setitem(sys.modules, "plotly", None)
    monkeypatch.setitem(sys.modules, "plotly.graph_objects", None)
    assert figures._plotly() is None
    metrics = {"baci/gravity/r_squared": 0.7, "baci/gravity/coefficients/log_dist": -0.8, "baci/flows": 1000.0}
    sections = figures.sections_baci(metrics, {})
    assert sections and all(not section.figures for section in sections)
    gravity = next(section for section in sections if section.title == "Gravité")
    assert "Coefficients" in gravity.tables
    page = _report(sections).to_html()
    assert "Gravité" in page and "log_dist" in page and "plotly" not in page.lower()
