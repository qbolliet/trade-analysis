"""Tests de la politique d'échec d'un run de téléchargement (``check_download_report``).

Rapports ``statflows`` réels, construits à la main : aucun appel réseau.
"""

from __future__ import annotations

from typing import Optional

import pytest
from statflows.core.reports import DownloadReport, QueryReport

from kedro_pipeline.io import DownloadFailureError, check_download_report


def _report(n_ok: int, n_failed: int, error_type: str = "RuntimeError") -> DownloadReport:
    """Build a report with ``n_ok`` successful and ``n_failed`` failed queries."""
    queries = [QueryReport(identity_key=f"ok-{i}") for i in range(n_ok)]
    queries += [
        QueryReport(
            identity_key=f"ko-{i}",
            error_type=error_type,
            error_message="Comtrade API call failed for period 2023: HTTP 403",
        )
        for i in range(n_failed)
    ]
    return DownloadReport(
        processed=n_ok, errors=n_failed, n_queries_planned=n_ok + n_failed, queries=queries
    )


def test_all_queries_failed_raises() -> None:
    """Le cas de l'incident : toutes les requêtes en échec → l'étape échoue."""
    with pytest.raises(DownloadFailureError, match=r"10/10 queries failed.*RuntimeError x10.*HTTP 403"):
        check_download_report(_report(0, 10), 0.5)


def test_partial_failure_below_threshold_is_tolerated() -> None:
    """Un échec isolé parmi de nombreuses requêtes est retenté au run suivant."""
    check_download_report(_report(9, 1), 0.5)


def test_ratio_equal_to_threshold_is_tolerated() -> None:
    """Le seuil est inclus : le contrôle échoue strictement au-delà."""
    check_download_report(_report(5, 5), 0.5)


def test_zero_threshold_fails_on_any_error() -> None:
    with pytest.raises(DownloadFailureError):
        check_download_report(_report(99, 1), 0.0)


def test_clean_run_passes_with_zero_threshold() -> None:
    check_download_report(_report(10, 0), 0.0)


@pytest.mark.parametrize("threshold", [None])
def test_none_disables_the_check(threshold: Optional[float]) -> None:
    check_download_report(_report(0, 10), threshold)


def test_threshold_one_never_fails() -> None:
    check_download_report(_report(0, 10), 1.0)


def test_run_without_attempted_query_never_fails() -> None:
    """Rien de tenté (ex. arrêt anticipé immédiat) : pas de division par zéro."""
    check_download_report(DownloadReport(n_queries_planned=5, n_queries_remaining=5), 0.0)


@pytest.mark.parametrize("threshold", [-0.1, 1.5])
def test_out_of_range_threshold_is_rejected(threshold: float) -> None:
    with pytest.raises(ValueError, match="max_error_ratio"):
        check_download_report(_report(1, 0), threshold)


def test_message_reports_dominant_error_types() -> None:
    """Le message nomme les types d'erreur (les logs du pod peuvent disparaître)."""
    report = _report(0, 3, error_type="RuntimeError")
    report.queries.append(QueryReport(identity_key="x", error_type="ConnectionError", error_message="boom"))
    report.errors += 1
    with pytest.raises(DownloadFailureError, match=r"RuntimeError x3, ConnectionError x1"):
        check_download_report(report, 0.5)
