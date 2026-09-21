"""Description Markdown du run (PS-31.3) : modèle exact, troncature, description réduite."""

from __future__ import annotations

from pathlib import Path

from macroforecast.tracking.report import (
    Check,
    RunReport,
    Units,
    build_report,
    failure_markdown,
    format_duration,
    format_number,
)

GOLDEN = Path(__file__).parent / "golden"


def _ps31_example() -> RunReport:
    """Le run d'exemple de PS-31.3 : 6 contrôles réussis, un avertissement (R² de la gravité)."""
    checks = [
        Check("coverage/years_eligible", ">", 0, "error", "Au moins une année complète"),
        Check("baci/flows", ">", 0, "error", "Flux écrits"),
        Check("baci/tonnage/share_tonnage_missing", "<=", 0.05, "warning", "Flux sans tonnage"),
        Check("baci/gravity/r_squared", ">=", 0.5, "warning", "R² de l'équation de gravité"),
        Check("baci/fobisation/share_clipped_to_zero", "<=", 0.01, "warning", "Valeurs FOB tronquées à zéro"),
    ]
    metrics = {
        "coverage/years_eligible": 24.0,
        "baci/flows": 212_000_000.0,
        "baci/tonnage/share_tonnage_missing": 0.022,
        "baci/gravity/r_squared": 0.42,
        "baci/fobisation/share_clipped_to_zero": 0.002,
    }
    return build_report(
        "process_baci_hs2017",
        metrics=metrics,
        checks=checks,
        units=Units(planned=1, succeeded=1, failed=0, planned_label="1 millésime (24 années)"),
        context={
            "workflow_id": "trade-pipeline-weekly-7k2qd",
            "env": "cloud",
            "image": "sha-6f24c6c",
            "duration_s": 3 * 3600 + 12 * 60,
            "links": {"DAG Argo": "https://argo.example/workflows/trade-pipeline-weekly-7k2qd"},
        },
        key_figures=[
            "24 années écrites",
            "212 M flux",
            "part convertie 97,8 %",
            "taux de fret médian 4,1 %",
            "pic mémoire 21,4 Go",
        ],
    )


def test_description_matches_the_ps31_example_golden_file() -> None:
    report = _ps31_example()
    assert report.health == "warning"
    assert report.to_markdown() == (GOLDEN / "ps31_example.md").read_text(encoding="utf-8").rstrip("\n")


def test_only_failed_checks_are_detailed_the_others_are_counted() -> None:
    text = _ps31_example().to_markdown()
    # 2 contrôles implicites + 4 configurés réussis, 1 avertissement
    assert "**Contrôles** : 6 ✅ · 1 ⚠️ · 0 ❌ · 0 ⏭️" in text
    assert text.count("R² de l'équation de gravité") == 1
    assert "Flux écrits" not in text


def test_failures_are_listed_and_capped() -> None:
    failures = {f"HS{i}": f"erreur {i}" for i in range(5)}
    report = build_report(
        "n", metrics={}, checks=[], units=Units(5, 0, 5), failures=failures, max_failures_listed=2
    )
    text = report.to_markdown()
    assert "❌ n — échec" in text
    assert "- `HS0` : erreur 0" in text and "- `HS1` : erreur 1" in text
    assert "HS2" not in text and "3 autre(s) dans `failures.csv`" in text


def test_truncation_keeps_the_verdict_and_points_to_summary() -> None:
    failures = {f"unité-{i}": "x" * 150 for i in range(20)}
    report = build_report("n", metrics={}, checks=[], units=Units(20, 0, 20), failures=failures)
    full = report.to_markdown()
    short = report.to_markdown(max_chars=600)
    assert len(full) > 600 and len(short) <= 600
    assert short.startswith("### ❌ n — échec")
    assert short.rstrip().endswith("`report/summary.md`.*")
    # Aucune limite : copie intégrale
    assert report.to_markdown(max_chars=None) == full
    # Limite large : rien n'est coupé
    assert report.to_markdown(max_chars=len(full)) == full


def test_truncation_only_drops_whole_lines_from_the_tail() -> None:
    report = _ps31_example()
    full_lines = report.to_markdown().split("\n")
    for limit in range(150, len(report.to_markdown()), 37):
        text = report.to_markdown(max_chars=limit)
        assert len(text) <= limit
        kept = text.split("\n")[:-2]  # sans la ligne vide et la note de troncature
        assert kept == full_lines[: len(kept)]


def test_reduced_description_of_an_aborted_run() -> None:
    text = failure_markdown(
        "compute_synthetic_scores",
        ValueError("boum\nsur deux lignes"),
        step="synthèse",
        context={"links": {"DAG Argo": "https://argo.example/wf"}},
    )
    assert text.splitlines() == [
        "### ❌ compute_synthetic_scores — échec",
        "`ValueError` : boum sur deux lignes",
        "Dernière étape atteinte : `synthèse`",
        "[DAG Argo](https://argo.example/wf)",
    ]


def test_reduced_description_truncates_a_long_message() -> None:
    text = failure_markdown("n", RuntimeError("m" * 5000), max_chars=300)
    assert len(text) <= 300 and text.endswith("…")


def test_number_and_duration_formatting() -> None:
    assert format_number(0.42) == "0,42"
    assert format_number(212_000_000.0) == "212 000 000"
    assert format_number(3.0) == "3"
    assert format_number(None) == "—"
    assert format_duration(11_520) == "3 h 12 min"
    assert format_duration(3 * 3600 + 5 * 60) == "3 h 05 min"
    assert format_duration(720) == "12 min"
    assert format_duration(42) == "42 s"
    assert format_duration(None) == ""


def test_checks_frame_and_failures_frame_columns() -> None:
    report = _ps31_example()
    frame = report.checks_frame()
    assert list(frame.columns) == ["contrôle", "métrique", "valeur", "opérateur", "seuil", "sévérité", "résultat"]
    assert len(frame) == 7 and frame["résultat"].tolist().count("warning") == 1
    assert list(RunReport(node="n").failures_frame().columns) == ["unité", "message"]
