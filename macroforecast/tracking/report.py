# Importation des modules
# Modules de base
from __future__ import annotations
from dataclasses import dataclass, field
import fnmatch
import html
import math
import operator
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence
# Modules de manipulation des données
import pandas as pd

# Ce module est PUR : ni MLflow, ni écriture de fichier, ni Kedro. Il évalue des
# contrôles sur des métriques et met en forme le rapport d'un run (description
# Markdown, page HTML, table des contrôles). La publication vers MLflow vit dans
# ``kedro_pipeline.io.tracking``.

# Opérateurs de comparaison admis dans un contrôle
_OPERATORS: Dict[str, Callable[[float, float], bool]] = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "!=": operator.ne,
}
# Symboles typographiques affichés dans les rapports
_OPERATOR_SYMBOLS: Dict[str, str] = {
    "<": "<",
    "<=": "≤",
    ">": ">",
    ">=": "≥",
    "==": "=",
    "!=": "≠",
}
# Sévérités admises
_SEVERITIES = ("warning", "error")
# Pictogrammes par résultat de contrôle
_STATUS_ICONS: Dict[str, str] = {
    "passed": "✅",
    "warning": "⚠️",
    "failed": "❌",
    "skipped": "⏭️",
}
# Titre du verdict par état de santé
_HEALTH_TITLES: Dict[str, str] = {
    "ok": "✅ {node} — ok",
    "warning": "⚠️ {node} — avertissement",
    "failed": "❌ {node} — échec",
}
# Renvoi affiché en fin de description tronquée
_TRUNCATION_NOTE = "… *description tronquée : voir `report/summary.md`.*"
# Nombre maximal de caractères d'un message d'exception dans la description réduite
_MAX_EXCEPTION_CHARS = 500


# ──────────────────────────────────────────────────────────────────────
# Contrôles déclaratifs
# ──────────────────────────────────────────────────────────────────────

# Contrôle sur une métrique
@dataclass(frozen=True)
class Check:
    """Declarative check on one metric of a run.

    Args:
        metric: Metric name, e.g. ``"baci/gravity/r_squared"``.
        op: Comparison operator, one of ``< <= > >= == !=``. The check
            *passes* when ``value <op> threshold`` holds.
        threshold: Reference value.
        severity: ``"warning"`` or ``"error"``: what a failed check costs to
            the run verdict.
        label: Human-readable name shown in the reports; the metric name
            when empty.

    Raises:
        ValueError: If ``op`` or ``severity`` is not admitted.

    Examples:
        >>> Check("gravity/r_squared", ">=", 0.5, "warning", "R² de la gravité").label
        'R² de la gravité'
        >>> Check("a", "=>", 1)
        Traceback (most recent call last):
            ...
        ValueError: Unknown operator '=>': expected one of ['<', '<=', '>', '>=', '==', '!=']
    """

    metric: str
    op: str
    threshold: float
    severity: str = "warning"
    label: str = ""

    def __post_init__(self) -> None:
        # Vérification des arguments
        if self.op not in _OPERATORS:
            raise ValueError(
                f"Unknown operator {self.op!r}: expected one of {list(_OPERATORS)}"
            )
        if self.severity not in _SEVERITIES:
            raise ValueError(
                f"Unknown severity {self.severity!r}: expected one of {list(_SEVERITIES)}"
            )
        # Seuil converti en flottant (la configuration peut fournir un entier)
        object.__setattr__(self, "threshold", float(self.threshold))
        # Libellé par défaut : le nom de la métrique
        if not self.label:
            object.__setattr__(self, "label", self.metric)


# Résultat de l'évaluation d'un contrôle
@dataclass(frozen=True)
class CheckResult:
    """Outcome of one :class:`Check`.

    Args:
        check: The evaluated check.
        value: Observed value, ``None`` when the metric is absent.
        status: ``"passed"``, ``"warning"`` (failed, severity warning),
            ``"failed"`` (failed, severity error) or ``"skipped"`` (metric
            absent: no effect on the verdict).

    Examples:
        >>> result = CheckResult(Check("m", ">", 0), 3.0, "passed")
        >>> result.check.label, result.status
        ('m', 'passed')
    """

    check: Check
    value: Optional[float]
    status: str


# Sélection des contrôles applicables à un nœud
def checks_for_node(node: str, config: Mapping[str, Any]) -> List[Check]:
    """Return the checks configured for a node.

    The ``CHECKS`` mapping is keyed by node name; a key may hold a ``*``
    wildcard (:mod:`fnmatch`), and every matching key contributes.

    Args:
        node: Node (script) name, e.g. ``"process_baci_HS2017"``.
        config: The ``tracking`` configuration block, or its ``CHECKS``
            mapping directly. Each entry is a mapping with ``metric``, ``op``,
            ``threshold`` and optionally ``severity`` and ``label``.

    Returns:
        Checks in configuration order, empty when no key matches.

    Raises:
        ValueError: If an entry has an unknown operator or severity.

    Examples:
        >>> config = {"CHECKS": {
        ...     "download_*": [{"metric": "download/processed", "op": ">", "threshold": 0}],
        ...     "process_baci_*": [{"metric": "baci/flows", "op": ">", "threshold": 0,
        ...                         "severity": "error", "label": "Flux écrits"}]}}
        >>> [c.metric for c in checks_for_node("download_comtrade", config)]
        ['download/processed']
        >>> checks_for_node("publish_serving", config)
        []
    """
    checks_config = config.get("CHECKS", config) if config else {}
    selected: List[Check] = []
    for pattern, entries in (checks_config or {}).items():
        if not fnmatch.fnmatchcase(node, str(pattern)):
            continue
        for entry in entries or []:
            selected.append(
                Check(
                    metric=str(entry["metric"]),
                    op=str(entry["op"]),
                    threshold=float(entry["threshold"]),
                    severity=str(entry.get("severity", "warning")),
                    label=str(entry.get("label", "")),
                )
            )
    return selected


# Fonction auxiliaire : valeur numérique exploitable
def _finite(value: Any) -> Optional[float]:
    """Return ``value`` as a finite float, ``None`` otherwise.

    Args:
        value: Candidate metric value.

    Returns:
        The float, or ``None`` for ``None``, non-numbers, NaN and infinities.

    Examples:
        >>> _finite(3), _finite(float("nan")), _finite("a"), _finite(None)
        (3.0, None, None, None)
    """
    if isinstance(value, bool):
        return float(value)
    if not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


# Évaluation des contrôles d'un nœud
def evaluate_checks(
    metrics: Mapping[str, float],
    checks: Sequence[Check],
    *,
    n_planned: int,
    n_succeeded: int,
    failures: Mapping[str, str],
) -> List[CheckResult]:
    """Evaluate the configured checks plus the two implicit ones.

    The implicit checks apply to every node, outside any configuration:
    "no unit failed" (severity ``error``, from ``failures``) and "every
    planned unit was processed" (severity ``warning``: units neither
    succeeded nor failed, e.g. an exhausted time budget).

    Args:
        metrics: Metrics of the run, by name.
        checks: Configured checks (:func:`checks_for_node`).
        n_planned: Number of units planned.
        n_succeeded: Number of units that succeeded.
        failures: Failed units, unit -> message.

    Returns:
        Implicit results first, then one result per configured check. A check
        on an absent (or non-finite) metric is ``skipped``.

    Examples:
        >>> checks = [Check("share", "<=", 0.1, "warning", "Part"),
        ...           Check("absent", ">", 0)]
        >>> results = evaluate_checks({"share": 0.3}, checks,
        ...                           n_planned=2, n_succeeded=2, failures={})
        >>> [r.status for r in results]
        ['passed', 'passed', 'warning', 'skipped']
        >>> health(results)
        'warning'
    """
    n_failed = len(failures)
    n_unprocessed = max(int(n_planned) - int(n_succeeded) - n_failed, 0)
    implicit = [
        (Check("units/failed", "==", 0, "error", "Aucune unité en échec"), float(n_failed)),
        (
            Check("units/unprocessed", "==", 0, "warning", "Toutes les unités prévues traitées"),
            float(n_unprocessed),
        ),
    ]
    results: List[CheckResult] = []
    for check, value in implicit:
        results.append(_evaluate_one(check, value))
    for check in checks:
        results.append(_evaluate_one(check, _finite(metrics.get(check.metric))))
    return results


# Fonction auxiliaire : évaluation d'un contrôle
def _evaluate_one(check: Check, value: Optional[float]) -> CheckResult:
    """Evaluate one check on one value.

    Args:
        check: Check to evaluate.
        value: Observed value, ``None`` when absent.

    Returns:
        The result; ``skipped`` when ``value`` is ``None``.

    Examples:
        >>> _evaluate_one(Check("m", ">=", 0.5, "error"), 0.4).status
        'failed'
        >>> _evaluate_one(Check("m", ">=", 0.5), None).status
        'skipped'
    """
    if value is None:
        return CheckResult(check, None, "skipped")
    if _OPERATORS[check.op](value, check.threshold):
        return CheckResult(check, value, "passed")
    return CheckResult(check, value, "failed" if check.severity == "error" else "warning")


# Verdict d'un run
def health(results: Sequence[CheckResult]) -> str:
    """Return the verdict of a run from its check results.

    Args:
        results: Results of :func:`evaluate_checks`.

    Returns:
        ``"failed"`` if a check of severity ``error`` failed, else
        ``"warning"`` if a check of severity ``warning`` failed, else ``"ok"``.
        Skipped checks have no effect.

    Examples:
        >>> ok = CheckResult(Check("m", ">", 0), 1.0, "passed")
        >>> warn = CheckResult(Check("m", ">", 0), -1.0, "warning")
        >>> health([ok]), health([ok, warn])
        ('ok', 'warning')
    """
    statuses = {result.status for result in results}
    if "failed" in statuses:
        return "failed"
    if "warning" in statuses:
        return "warning"
    return "ok"


# Décompte des résultats par statut
def count_results(results: Sequence[CheckResult]) -> Dict[str, int]:
    """Count the check results by status.

    Args:
        results: Results of :func:`evaluate_checks`.

    Returns:
        Mapping ``passed`` / ``warning`` / ``failed`` / ``skipped`` -> count.

    Examples:
        >>> count_results([CheckResult(Check("m", ">", 0), 1.0, "passed")])
        {'passed': 1, 'warning': 0, 'failed': 0, 'skipped': 0}
    """
    return {
        status: sum(1 for result in results if result.status == status)
        for status in ("passed", "warning", "failed", "skipped")
    }


# Libellés des contrôles en défaut (valeur du tag `checks_failed`)
def failed_labels(results: Sequence[CheckResult], max_chars: int = 250) -> str:
    """Join the labels of the failed checks, truncated for a tag value.

    Args:
        results: Results of :func:`evaluate_checks`.
        max_chars: Maximum length of the returned text.

    Returns:
        Labels of the ``warning`` and ``failed`` checks separated by ``" ; "``,
        cut with an ellipsis beyond ``max_chars``; empty when none failed.

    Examples:
        >>> r = CheckResult(Check("m", ">", 0, label="Lignes écrites"), -1.0, "warning")
        >>> failed_labels([r])
        'Lignes écrites'
    """
    text = " ; ".join(
        result.check.label for result in results if result.status in ("warning", "failed")
    )
    return text if len(text) <= max_chars else text[: max_chars - 1] + "…"


# ──────────────────────────────────────────────────────────────────────
# Mise en forme des valeurs
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : nombre à la française
def format_number(value: Optional[float]) -> str:
    """Format a number the French way (decimal comma, space thousands).

    Args:
        value: Number to format; ``None`` gives an em dash.

    Returns:
        Text with three significant digits below 1 000, an integer with
        space-separated thousands above.

    Examples:
        >>> format_number(0.42), format_number(97.8), format_number(212345678.0)
        ('0,42', '97,8', '212 345 678')
        >>> format_number(None)
        '—'
    """
    if value is None:
        return "—"
    if abs(value) >= 1000:
        return f"{value:,.0f}".replace(",", " ")
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.3g}".replace(".", ",")


# Fonction auxiliaire : durée lisible
def format_duration(seconds: Optional[float]) -> str:
    """Format a duration in seconds as ``"3 h 12 min"``.

    Args:
        seconds: Duration in seconds; ``None`` gives an empty text.

    Returns:
        Hours and minutes, minutes alone, or seconds below one minute.

    Examples:
        >>> format_duration(11520), format_duration(720), format_duration(42)
        ('3 h 12 min', '12 min', '42 s')
    """
    if seconds is None:
        return ""
    total = int(round(seconds))
    if total < 60:
        return f"{total} s"
    minutes = total // 60
    if minutes < 60:
        return f"{minutes} min"
    return f"{minutes // 60} h {minutes % 60:02d} min"


# ──────────────────────────────────────────────────────────────────────
# Rapport de run
# ──────────────────────────────────────────────────────────────────────

# Décompte des unités d'un run
@dataclass
class Units:
    """Units of work of a run (queries, vintages, contexts, tables…).

    Args:
        planned: Number of units planned.
        succeeded: Number of units that succeeded.
        failed: Number of units that failed.
        planned_label: Text shown instead of ``planned`` when richer, e.g.
            ``"1 millésime (24 années)"``.

    Examples:
        >>> Units(planned=3, succeeded=2, failed=1).planned_text()
        '3'
    """

    planned: int = 0
    succeeded: int = 0
    failed: int = 0
    planned_label: Optional[str] = None

    def planned_text(self) -> str:
        """Return the text displayed under "Unités prévues".

        Returns:
            ``planned_label`` when given, else the formatted count.
        """
        return self.planned_label or format_number(float(self.planned))


# Section du rapport HTML
@dataclass
class Section:
    """One section of the HTML report: figures, tables and notes.

    Every figure has a tabular equivalent among ``tables``, so that the
    information stays readable when figures cannot be rendered.

    Args:
        title: Section heading.
        figures: Figure objects exposing ``to_html(full_html=False,
            include_plotlyjs=...)`` (Plotly figures). Empty without plotly.
        tables: Caption -> table.
        notes: Short sentences shown under the heading.

    Examples:
        >>> Section("Gravité", notes=["R² = 0,71"]).title
        'Gravité'
    """

    title: str
    figures: List[Any] = field(default_factory=list)
    tables: Dict[str, pd.DataFrame] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


# Rapport complet d'un run
@dataclass
class RunReport:
    """Everything needed to judge the execution of one run.

    Args:
        node: Node (script) name, e.g. ``"process_baci_HS2017"``.
        title: Heading of the report; ``node`` when empty.
        health: Verdict, ``"ok"``, ``"warning"`` or ``"failed"``.
        context: Execution context. Known keys: ``workflow_id``, ``env``,
            ``image``, ``duration_s`` and ``links`` (label -> URL).
        units: Units planned, succeeded and failed.
        checks: Results of :func:`evaluate_checks`.
        key_figures: Ready-to-display key figures (metrics reformatted).
        sections: Sections of the HTML report.
        tables: ``tables/`` artifacts, name -> table.
        failures: Failed units, unit -> message.
        max_failures_listed: Failures listed in the description.

    Examples:
        >>> report = RunReport(node="download_comtrade", health="ok",
        ...                    units=Units(10, 10, 0), key_figures=["10 requêtes"])
        >>> report.to_markdown().splitlines()[0]
        '### ✅ download_comtrade — ok'
    """

    node: str
    title: str = ""
    health: str = "ok"
    context: Dict[str, Any] = field(default_factory=dict)
    units: Units = field(default_factory=Units)
    checks: List[CheckResult] = field(default_factory=list)
    key_figures: List[str] = field(default_factory=list)
    sections: List[Section] = field(default_factory=list)
    tables: Dict[str, pd.DataFrame] = field(default_factory=dict)
    failures: Dict[str, str] = field(default_factory=dict)
    max_failures_listed: int = 20

    # Ligne de contexte d'exécution
    def _context_line(self) -> str:
        """Return the execution line of the description.

        Returns:
            Workflow, environment, image, duration and links joined by ``" · "``.
        """
        ctx = self.context
        parts = [f"Exécution `{ctx['workflow_id']}`" if ctx.get("workflow_id") else "Exécution locale"]
        if ctx.get("env"):
            parts.append(f"env `{ctx['env']}`")
        if ctx.get("image"):
            parts.append(f"image `{ctx['image']}`")
        if ctx.get("duration_s") is not None:
            parts.append(format_duration(ctx["duration_s"]))
        for label, url in (ctx.get("links") or {}).items():
            if url:
                parts.append(f"[{label}]({url})")
        return " · ".join(parts)

    def to_markdown(self, max_chars: Optional[int] = None) -> str:
        """Render the description shown in the *Overview* tab of the run.

        The structure is fixed: verdict, execution line, units, check counts,
        the checks in default, key figures, pointers to the other tabs.
        Passed and skipped checks are only counted (see ``checks.csv``).

        Args:
            max_chars: Maximum length (MLflow tag limit). Beyond it the tail
                is dropped line by line and a note points to
                ``report/summary.md``, the untruncated copy. ``None`` disables
                the truncation.

        Returns:
            Markdown text.

        Examples:
            >>> from macroforecast.tracking.report import Check, CheckResult
            >>> bad = CheckResult(Check("r2", ">=", 0.5, "warning", "R² de la gravité"), 0.42, "warning")
            >>> report = RunReport(node="process_baci_HS2017", health="warning",
            ...                    units=Units(1, 1, 0, "1 millésime"), checks=[bad])
            >>> print(report.to_markdown())
            ### ⚠️ process_baci_HS2017 — avertissement
            Exécution locale
            <BLANKLINE>
            | Unités prévues | Réussies | En échec |
            |---:|---:|---:|
            | 1 millésime | 1 | 0 |
            <BLANKLINE>
            **Contrôles** : 0 ✅ · 1 ⚠️ · 0 ❌ · 0 ⏭️
            <BLANKLINE>
            | | Contrôle | Valeur | Seuil |
            |---|---|---:|---|
            | ⚠️ | R² de la gravité | 0,42 | ≥ 0,5 |
            <BLANKLINE>
            **Détail** : rapport complet `report/report.html` (Artifacts) · métriques par section (Model metrics) · ressources (System metrics)
        """
        counts = count_results(self.checks)
        lines: List[str] = [
            "### " + _HEALTH_TITLES.get(self.health, "{node}").format(node=self.title or self.node),
            self._context_line(),
            "",
            "| Unités prévues | Réussies | En échec |",
            "|---:|---:|---:|",
            f"| {self.units.planned_text()} | {self.units.succeeded} | {self.units.failed} |",
            "",
            "**Contrôles** : "
            + " · ".join(
                f"{counts[status]} {_STATUS_ICONS[status]}"
                for status in ("passed", "warning", "failed", "skipped")
            ),
        ]
        # Contrôles en défaut seulement (les autres sont comptés)
        defaults = [r for r in self.checks if r.status in ("warning", "failed")]
        if defaults:
            lines += ["", "| | Contrôle | Valeur | Seuil |", "|---|---|---:|---|"]
            for result in defaults:
                check = result.check
                lines.append(
                    f"| {_STATUS_ICONS[result.status]} | {check.label} | "
                    f"{format_number(result.value)} | "
                    f"{_OPERATOR_SYMBOLS[check.op]} {format_number(check.threshold)} |"
                )
        # Unités en échec
        if self.failures:
            listed = list(self.failures.items())[: self.max_failures_listed]
            lines += ["", f"**Unités en échec** ({len(self.failures)}) :"]
            lines += [f"- `{unit}` : {_one_line(message, 200)}" for unit, message in listed]
            if len(self.failures) > len(listed):
                lines.append(f"- … {len(self.failures) - len(listed)} autre(s) dans `failures.csv`")
        if self.key_figures:
            lines += ["", "**Chiffres clés** : " + " · ".join(self.key_figures)]
        lines += [
            "",
            "**Détail** : rapport complet `report/report.html` (Artifacts) · "
            "métriques par section (Model metrics) · ressources (System metrics)",
        ]
        text = "\n".join(lines)
        if max_chars is None or len(text) <= max_chars:
            return text
        return _truncate_lines(lines, max_chars)

    def checks_frame(self) -> pd.DataFrame:
        """Return every check result as a table (``report/checks.csv``).

        Returns:
            One row per check, implicit ones included, with the columns
            ``contrôle``, ``métrique``, ``valeur``, ``opérateur``, ``seuil``,
            ``sévérité`` and ``résultat``.

        Examples:
            >>> r = CheckResult(Check("m", ">", 0, "error", "Lignes"), 3.0, "passed")
            >>> RunReport(node="n", checks=[r]).checks_frame().to_dict("records")
            [{'contrôle': 'Lignes', 'métrique': 'm', 'valeur': 3.0, 'opérateur': '>', 'seuil': 0.0, 'sévérité': 'error', 'résultat': 'passed'}]
        """
        return pd.DataFrame(
            [
                {
                    "contrôle": r.check.label,
                    "métrique": r.check.metric,
                    "valeur": r.value,
                    "opérateur": r.check.op,
                    "seuil": r.check.threshold,
                    "sévérité": r.check.severity,
                    "résultat": r.status,
                }
                for r in self.checks
            ],
            columns=["contrôle", "métrique", "valeur", "opérateur", "seuil", "sévérité", "résultat"],
        )

    def failures_frame(self) -> pd.DataFrame:
        """Return the failed units as a table (``failures.csv``).

        Returns:
            One row per failed unit, columns ``unité`` and ``message``.

        Examples:
            >>> RunReport(node="n", failures={"HS2017": "boom"}).failures_frame().to_dict("records")
            [{'unité': 'HS2017', 'message': 'boom'}]
        """
        return pd.DataFrame(
            [{"unité": unit, "message": message} for unit, message in self.failures.items()],
            columns=["unité", "message"],
        )

    def to_html(self, plotly_js: str = "inline", max_table_rows: int = 50) -> str:
        """Render the self-contained HTML report (``report/report.html``).

        Title, verdict, checks, key figures, then one block per section with
        its figures and its tables truncated to ``max_table_rows``.

        Args:
            plotly_js: How the Plotly library reaches the page: ``"inline"``
                embeds it once (about 3.5 MB, works offline), ``"cdn"`` links
                it (light, needs internet in the browser).
            max_table_rows: Rows shown per table.

        Returns:
            A complete HTML document. A figure object is rendered through its
            ``to_html`` method; sections without figures keep their tables.

        Raises:
            ValueError: If ``plotly_js`` is neither ``"inline"`` nor ``"cdn"``.

        Examples:
            >>> page = RunReport(node="n", health="ok").to_html()
            >>> page.startswith("<!doctype html>"), "<h1>" in page
            (True, True)
        """
        if plotly_js not in ("inline", "cdn"):
            raise ValueError(f"plotly_js must be 'inline' or 'cdn', got {plotly_js!r}")
        esc = html.escape
        counts = count_results(self.checks)
        body: List[str] = [
            f"<h1>{esc(_HEALTH_TITLES.get(self.health, '{node}').format(node=self.title or self.node))}</h1>",
            f"<p class='ctx'>{esc(self._context_line())}</p>",
            "<p><b>Unités</b> : "
            f"{esc(self.units.planned_text())} prévues · {self.units.succeeded} réussies · "
            f"{self.units.failed} en échec</p>",
            "<h2>Contrôles</h2>",
            "<p>"
            + " · ".join(f"{counts[s]} {_STATUS_ICONS[s]}" for s in ("passed", "warning", "failed", "skipped"))
            + "</p>",
            _checks_html(self.checks),
        ]
        if self.failures:
            body += ["<h2>Unités en échec</h2>", _table_html(self.failures_frame(), max_table_rows)]
        if self.key_figures:
            body += [
                "<h2>Chiffres clés</h2>",
                "<ul>" + "".join(f"<li>{esc(fig)}</li>" for fig in self.key_figures) + "</ul>",
            ]
        # Une seule copie de plotly.js par page : la première figure la porte
        first_figure = True
        for section in self.sections:
            body.append(f"<h2>{esc(section.title)}</h2>")
            body += [f"<p class='note'>{esc(note)}</p>" for note in section.notes]
            for figure in section.figures:
                include = ("cdn" if plotly_js == "cdn" else True) if first_figure else False
                first_figure = False
                body.append(
                    "<div class='fig'>" + figure.to_html(full_html=False, include_plotlyjs=include) + "</div>"
                )
            for caption, table in section.tables.items():
                body += [f"<h3>{esc(caption)}</h3>", _table_html(table, max_table_rows)]
        return _HTML_PAGE.format(title=esc(self.title or self.node), body="\n".join(body))


# Gabarit de la page HTML autonome
_HTML_PAGE = """<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><title>{title}</title>
<style>
:root {{ color-scheme: light dark; --fg:#1b1f24; --bg:#fff; --mut:#5b6570; --line:#d0d7de; }}
@media (prefers-color-scheme: dark) {{ :root {{ --fg:#e6edf3; --bg:#0d1117; --mut:#9ba7b4; --line:#30363d; }} }}
body {{ font: 14px/1.5 system-ui, sans-serif; color: var(--fg); background: var(--bg); margin: 0; padding: 16px 24px 48px; max-width: 1100px; }}
h1 {{ font-size: 1.5rem; }} h2 {{ margin-top: 2rem; border-bottom: 1px solid var(--line); padding-bottom: .2rem; }}
h3 {{ font-size: 1rem; color: var(--mut); }} .ctx, .note {{ color: var(--mut); }}
.tbl {{ border-collapse: collapse; font-size: 13px; }} .tbl th, .tbl td {{ border: 1px solid var(--line); padding: 2px 8px; text-align: right; white-space: nowrap; }}
.tbl th:first-child, .tbl td:first-child {{ text-align: left; }} .tbl .l {{ text-align: left; }} .wrap {{ overflow-x: auto; }} .fig {{ margin: 1rem 0; }}
code {{ font-size: 12px; }}
</style></head><body>
{body}
</body></html>"""


# Fonction auxiliaire : table HTML des contrôles
def _checks_html(results: Sequence[CheckResult]) -> str:
    """Render the check results as an HTML table (icon, label, metric, value, threshold).

    Args:
        results: Results of :func:`evaluate_checks`.

    Returns:
        HTML fragment; the labels and metrics are escaped.

    Examples:
        >>> r = CheckResult(Check("baci/flows", ">", 0, "error", "Flux écrits"), 12.0, "passed")
        >>> "Flux écrits" in _checks_html([r]) and "baci/flows" in _checks_html([r])
        True
    """
    rows = "".join(
        f"<tr><td>{_STATUS_ICONS[r.status]}</td><td class='l'>{html.escape(r.check.label)}</td>"
        f"<td class='l'><code>{html.escape(r.check.metric)}</code></td>"
        f"<td>{html.escape(format_number(r.value))}</td>"
        f"<td>{_OPERATOR_SYMBOLS[r.check.op]} {html.escape(format_number(r.check.threshold))}</td>"
        f"<td>{r.check.severity}</td></tr>"
        for r in results
    )
    return (
        "<div class='wrap'><table class='tbl checks'><thead><tr><th></th><th class='l'>Contrôle</th>"
        "<th class='l'>Métrique</th><th>Valeur</th><th>Seuil</th><th>Sévérité</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


# Fonction auxiliaire : table HTML tronquée
def _table_html(table: pd.DataFrame, max_rows: int) -> str:
    """Render a table as HTML, truncated to ``max_rows`` rows.

    Args:
        table: Table to render.
        max_rows: Rows shown.

    Returns:
        HTML fragment with a note giving the number of hidden rows, if any.

    Examples:
        >>> "<table" in _table_html(pd.DataFrame({"a": [1, 2, 3]}), 2)
        True
        >>> "1 ligne(s) non affichée(s)" in _table_html(pd.DataFrame({"a": [1, 2, 3]}), 2)
        True
    """
    shown = table.head(max_rows)
    fragment = (
        "<div class='wrap'>"
        + shown.to_html(index=False, border=0, classes="tbl", na_rep="—", float_format=format_number)
        + "</div>"
    )
    hidden = len(table) - len(shown)
    if hidden > 0:
        fragment += f"<p class='note'>{hidden} ligne(s) non affichée(s) : voir le CSV dans <code>tables/</code>.</p>"
    return fragment


# Fonction auxiliaire : texte sur une ligne
def _one_line(text: str, max_chars: int) -> str:
    """Collapse a text on one line and truncate it.

    Args:
        text: Text to shorten.
        max_chars: Maximum length.

    Returns:
        Single-line text ending with an ellipsis when cut.

    Examples:
        >>> _one_line("a\\nb", 10), _one_line("abcdefghij", 5)
        ('a b', 'abcd…')
    """
    flat = " ".join(str(text).split())
    return flat if len(flat) <= max_chars else flat[: max_chars - 1] + "…"


# Fonction auxiliaire : troncature propre par lignes
def _truncate_lines(lines: Sequence[str], max_chars: int) -> str:
    """Drop lines from the tail until the text, plus its note, fits.

    Args:
        lines: Lines of the full description.
        max_chars: Maximum length of the result, note included.

    Returns:
        The kept head followed by a note pointing to ``report/summary.md``.

    Examples:
        >>> out = _truncate_lines(["a" * 10, "b" * 10, "c" * 10], 90)
        >>> out.endswith("summary.md`.*")
        True
    """
    kept = list(lines)
    while kept and len("\n".join(kept + ["", _TRUNCATION_NOTE])) > max_chars:
        kept.pop()
    # Ligne vide finale évitée pour ne pas casser le rendu d'un tableau
    while kept and kept[-1] == "":
        kept.pop()
    return "\n".join(kept + ["", _TRUNCATION_NOTE])[:max_chars]


# Construction d'un rapport à partir des métriques
def build_report(
    node: str,
    *,
    metrics: Mapping[str, float],
    checks: Sequence[Check],
    units: Units,
    failures: Optional[Mapping[str, str]] = None,
    title: str = "",
    context: Optional[Mapping[str, Any]] = None,
    key_figures: Optional[Sequence[str]] = None,
    sections: Optional[Sequence[Section]] = None,
    tables: Optional[Mapping[str, pd.DataFrame]] = None,
    max_failures_listed: int = 20,
) -> RunReport:
    """Evaluate the checks and assemble the :class:`RunReport` of a run.

    Args:
        node: Node (script) name.
        metrics: Metrics of the run, by name.
        checks: Configured checks (:func:`checks_for_node`).
        units: Units planned, succeeded and failed.
        failures: Failed units that make the run fail, unit -> message. A unit
            counted in ``units.failed`` but absent here is a *tolerated* failure
            (e.g. a share of failed download queries below the accepted ratio):
            it is handled, so it does not count as "left unprocessed".
        title: Heading; ``node`` when empty.
        context: Execution context (see :class:`RunReport`).
        key_figures: Ready-to-display key figures.
        sections: Sections of the HTML report.
        tables: ``tables/`` artifacts, name -> table.
        max_failures_listed: Failures listed in the description.

    Returns:
        The report, its verdict computed from the check results.

    Examples:
        >>> report = build_report(
        ...     "publish_serving",
        ...     metrics={"serving/countries/rows": 0.0},
        ...     checks=[Check("serving/countries/rows", ">", 0, "error", "Table countries non vide")],
        ...     units=Units(1, 1, 0))
        >>> report.health
        'failed'
    """
    failures = dict(failures or {})
    # Unités en échec mais tolérées (absentes de `failures`) : traitées, non « restantes »
    tolerated = max(units.failed - len(failures), 0)
    results = evaluate_checks(
        metrics,
        checks,
        n_planned=units.planned,
        n_succeeded=units.succeeded + tolerated,
        failures=failures,
    )
    return RunReport(
        node=node,
        title=title,
        health=health(results),
        context=dict(context or {}),
        units=units,
        checks=results,
        key_figures=list(key_figures or []),
        sections=list(sections or []),
        tables=dict(tables or {}),
        failures=failures,
        max_failures_listed=max_failures_listed,
    )


# Description réduite d'un run interrompu par une exception
def failure_markdown(
    node: str,
    exc: BaseException,
    *,
    step: Optional[str] = None,
    context: Optional[Mapping[str, Any]] = None,
    max_chars: Optional[int] = None,
) -> str:
    """Render the reduced description of a run aborted by an exception.

    Args:
        node: Node (script) name.
        exc: The uncaught exception.
        step: Last step reached, when known.
        context: Execution context; only the links are used.
        max_chars: Maximum length of the text.

    Returns:
        Markdown: failure title, exception type and (truncated) message, last
        step reached and links.

    Examples:
        >>> print(failure_markdown("compute_synthetic_scores", ValueError("boom"), step="synthèse"))
        ### ❌ compute_synthetic_scores — échec
        `ValueError` : boom
        Dernière étape atteinte : `synthèse`
    """
    lines = [
        "### " + _HEALTH_TITLES["failed"].format(node=node),
        f"`{type(exc).__name__}` : {_one_line(str(exc), _MAX_EXCEPTION_CHARS)}",
    ]
    if step:
        lines.append(f"Dernière étape atteinte : `{step}`")
    links = [f"[{label}]({url})" for label, url in ((context or {}).get("links") or {}).items() if url]
    if links:
        lines.append(" · ".join(links))
    text = "\n".join(lines)
    return text if max_chars is None or len(text) <= max_chars else text[: max_chars - 1] + "…"
