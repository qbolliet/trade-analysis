"""Failure policy of a ``statflows`` download run.

``statflows.core.download.download_updates`` isolates errors per query: a failed
query is recorded in ``DownloadReport.errors`` and the run goes on, the query
being retried at the next run. The caller therefore receives a report even when
every query failed. Turning that report into a process exit status is a policy
decision that belongs to the caller, hence this module rather than ``statflows``.
"""
# Importation des modules
# Modules de base
from collections import Counter
from typing import Any, Optional

# Nombre maximal de types d'erreur détaillés dans le message
_MAX_ERROR_TYPES = 3


# Exception levée lorsque la part de requêtes en échec dépasse le seuil toléré
class DownloadFailureError(RuntimeError):
    """Raised when too many queries of a download run failed."""


# Fonction de validation du seuil d'échec
def _validate_ratio(max_error_ratio: float) -> float:
    """Check that the tolerated failure ratio lies in ``[0, 1]``.

    Args:
        max_error_ratio: Candidate threshold.

    Returns:
        The threshold as a float.

    Raises:
        ValueError: If the threshold is outside ``[0, 1]``.
    """
    ratio = float(max_error_ratio)
    if not 0.0 <= ratio <= 1.0:
        raise ValueError(f"max_error_ratio must lie in [0, 1], got {max_error_ratio!r}")
    return ratio


# Fonction de contrôle du rapport d'un run de téléchargement
def check_download_report(report: Any, max_error_ratio: Optional[float]) -> None:
    """Raise if the share of failed queries of a run exceeds the tolerated ratio.

    The ratio is ``report.errors / len(report.queries)``: ``report.queries``
    holds one entry per *attempted* query, whether it succeeded or not, and
    excludes the queries left unprocessed on an early stop (``stopped_early``).
    A run that attempted nothing never fails. Failed queries are not marked as
    downloaded by ``statflows``, so a tolerated partial failure is retried by the
    next run.

    Args:
        report: ``statflows.core.reports.DownloadReport`` of the run.
        max_error_ratio: Highest tolerated share of failed queries, in
            ``[0, 1]``: ``0`` fails on any error, ``1`` never fails, and any
            value below ``1`` fails a run where every query failed. ``None``
            disables the check.

    Raises:
        DownloadFailureError: If the share of failed queries is strictly
            greater than ``max_error_ratio``.
        ValueError: If ``max_error_ratio`` is outside ``[0, 1]``.

    Examples:
        >>> from types import SimpleNamespace
        >>> ok = SimpleNamespace(errors=1, queries=[object()] * 10)
        >>> check_download_report(ok, 0.5)
        >>> ko = SimpleNamespace(errors=10, queries=[SimpleNamespace(error_type="X", error_message="m")] * 10)
        >>> check_download_report(ko, 0.5)
        Traceback (most recent call last):
            ...
        kedro_pipeline.io.download_report.DownloadFailureError: 10/10 queries failed (100%), above the tolerated 50%; errors: X x10 (e.g. m)
    """
    # Contrôle désactivé
    if max_error_ratio is None:
        return
    ratio_limit = _validate_ratio(max_error_ratio)

    # Requêtes effectivement tentées (les requêtes non traitées sont exclues)
    attempted = len(report.queries)
    if attempted == 0:
        return
    error_ratio = report.errors / attempted
    if error_ratio <= ratio_limit:
        return

    # Résumé des causes : types d'erreur les plus fréquents et premier message
    failed = [q for q in report.queries if getattr(q, "error_type", None)]
    causes = Counter(q.error_type for q in failed)
    first_message = failed[0].error_message if failed else ""
    detail = ", ".join(
        f"{error_type} x{count}" for error_type, count in causes.most_common(_MAX_ERROR_TYPES)
    )
    raise DownloadFailureError(
        f"{report.errors}/{attempted} queries failed ({error_ratio:.0%}), "
        f"above the tolerated {ratio_limit:.0%}; errors: {detail}"
        + (f" (e.g. {first_message})" if first_message else "")
    )
