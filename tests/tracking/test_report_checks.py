"""Contrôles déclaratifs du rapport de run (PS-31.2) : opérateurs, jokers, verdict."""

from __future__ import annotations

import pytest

from macroforecast.tracking.report import (
    Check,
    CheckResult,
    checks_for_node,
    count_results,
    evaluate_checks,
    failed_labels,
    health,
)


def _evaluate(metrics, checks, *, planned=1, succeeded=1, failures=None):
    return evaluate_checks(
        metrics, checks, n_planned=planned, n_succeeded=succeeded, failures=failures or {}
    )


@pytest.mark.parametrize(
    ("op", "value", "threshold", "expected"),
    [
        ("<", 1.0, 2.0, "passed"),
        ("<", 2.0, 2.0, "warning"),
        ("<=", 2.0, 2.0, "passed"),
        ("<=", 2.1, 2.0, "warning"),
        (">", 3.0, 2.0, "passed"),
        (">", 2.0, 2.0, "warning"),
        (">=", 2.0, 2.0, "passed"),
        (">=", 1.9, 2.0, "warning"),
        ("==", 2.0, 2.0, "passed"),
        ("==", 2.5, 2.0, "warning"),
        ("!=", 2.5, 2.0, "passed"),
        ("!=", 2.0, 2.0, "warning"),
    ],
)
def test_each_operator(op: str, value: float, threshold: float, expected: str) -> None:
    results = _evaluate({"m": value}, [Check("m", op, threshold, "warning")])
    assert results[-1].status == expected


def test_severity_error_gives_failed() -> None:
    results = _evaluate({"m": 0.0}, [Check("m", ">", 0, "error")])
    assert results[-1].status == "failed"
    assert health(results) == "failed"


def test_absent_or_non_finite_metric_is_skipped_without_effect_on_health() -> None:
    checks = [Check("absent", ">", 0, "error"), Check("nan", ">", 0, "error")]
    results = _evaluate({"nan": float("nan")}, checks)
    assert [r.status for r in results[-2:]] == ["skipped", "skipped"]
    assert [r.value for r in results[-2:]] == [None, None]
    assert health(results) == "ok"


def test_unknown_operator_or_severity_is_rejected() -> None:
    with pytest.raises(ValueError, match="operator"):
        Check("m", "=>", 1)
    with pytest.raises(ValueError, match="severity"):
        Check("m", ">", 1, "critical")


def test_integer_threshold_is_coerced_to_float() -> None:
    assert Check("m", ">", 0).threshold == 0.0


class TestImplicitChecks:
    def test_no_failure_all_processed_gives_two_passed_checks(self) -> None:
        results = _evaluate({}, [], planned=3, succeeded=3)
        assert [r.status for r in results] == ["passed", "passed"]
        assert health(results) == "ok"

    def test_a_failed_unit_is_an_error(self) -> None:
        results = _evaluate({}, [], planned=3, succeeded=2, failures={"HS2017": "boom"})
        assert results[0].check.label == "Aucune unité en échec"
        assert results[0].status == "failed" and results[0].value == 1.0
        assert health(results) == "failed"

    def test_units_left_unprocessed_are_a_warning(self) -> None:
        # Budget de temps épuisé : 3 prévues, 1 réussie, aucune en échec
        results = _evaluate({}, [], planned=3, succeeded=1)
        assert results[1].status == "warning" and results[1].value == 2.0
        assert health(results) == "warning"

    def test_failed_units_are_not_counted_twice_as_unprocessed(self) -> None:
        results = _evaluate({}, [], planned=3, succeeded=2, failures={"a": "x"})
        assert results[1].status == "passed"


def test_health_precedence() -> None:
    ok = CheckResult(Check("m", ">", 0), 1.0, "passed")
    warning = CheckResult(Check("m", ">", 0), 0.0, "warning")
    failed = CheckResult(Check("m", ">", 0, "error"), 0.0, "failed")
    skipped = CheckResult(Check("m", ">", 0), None, "skipped")
    assert health([]) == "ok"
    assert health([ok, skipped]) == "ok"
    assert health([ok, warning, skipped]) == "warning"
    assert health([ok, warning, failed]) == "failed"


def test_counts_and_failed_labels() -> None:
    results = _evaluate(
        {"a": 0.0, "b": 0.0},
        [Check("a", ">", 0, "warning", "Alpha"), Check("b", ">", 0, "error", "Bêta"), Check("c", ">", 0)],
    )
    assert count_results(results) == {"passed": 2, "warning": 1, "failed": 1, "skipped": 1}
    assert failed_labels(results) == "Alpha ; Bêta"
    assert failed_labels(results, max_chars=5) == "Alph…"


class TestChecksForNode:
    CONFIG = {
        "CHECKS": {
            "download_*": [{"metric": "download/processed", "op": ">", "threshold": 0}],
            "process_baci_*": [
                {"metric": "baci/flows", "op": ">", "threshold": 0, "severity": "error", "label": "Flux"}
            ],
            "process_baci_HS2017": [{"metric": "extra", "op": "<", "threshold": 1}],
            "publish_serving": [],
        }
    }

    def test_wildcard_matches_by_fnmatch(self) -> None:
        assert [c.metric for c in checks_for_node("download_comtrade", self.CONFIG)] == ["download/processed"]
        assert [c.metric for c in checks_for_node("download_eurostat", self.CONFIG)] == ["download/processed"]

    def test_every_matching_key_contributes_in_order(self) -> None:
        checks = checks_for_node("process_baci_HS2017", self.CONFIG)
        assert [c.metric for c in checks] == ["baci/flows", "extra"]
        assert checks[0].severity == "error" and checks[0].label == "Flux"

    def test_no_match_or_empty_config(self) -> None:
        assert checks_for_node("compute_synthetic_scores", self.CONFIG) == []
        assert checks_for_node("publish_serving", self.CONFIG) == []
        assert checks_for_node("x", {}) == []

    def test_accepts_the_checks_mapping_directly(self) -> None:
        assert len(checks_for_node("download_x", self.CONFIG["CHECKS"])) == 1

    def test_bad_entry_raises(self) -> None:
        with pytest.raises(ValueError):
            checks_for_node("n", {"CHECKS": {"n": [{"metric": "m", "op": "~", "threshold": 1}]}})
