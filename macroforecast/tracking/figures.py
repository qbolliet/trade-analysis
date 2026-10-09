# Importation des modules
# Modules de base
from __future__ import annotations
import importlib
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence
# Modules de manipulation des données
import pandas as pd
# Modules du package
from .report import Section, format_number

# Figures Plotly du rapport de run, par étape du pipeline (ARCH PS-31.4).
#
# Chaque étape expose deux fonctions :
#   - ``key_figures_<étape>(metrics) -> list[str]`` : chiffres clés de la description ;
#   - ``sections_<étape>(metrics, artifacts) -> list[Section]`` : sections du rapport HTML.
# Elles ne lisent AUCUNE donnée : seulement les métriques et les tables d'artefacts que
# l'étape a déjà produites. Plotly est importé paresseusement (extra « reports ») : sans
# lui, les sections gardent leurs tables mais n'ont plus de figures.

# Initialisation du logger
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Outils communs
# ──────────────────────────────────────────────────────────────────────

# Import paresseux de plotly
def _plotly() -> Optional[Any]:
    """Import ``plotly.graph_objects`` lazily.

    Returns:
        The module, or ``None`` when plotly is not installed.

    Examples:
        >>> _plotly() is None or hasattr(_plotly(), "Figure")
        True
    """
    try:
        return importlib.import_module("plotly.graph_objects")
    except ImportError:
        return None


# Sous-ensemble de métriques sous un préfixe
def _under(metrics: Mapping[str, float], prefix: str) -> Dict[str, float]:
    """Return the metrics under ``prefix/``, keyed by the remaining name.

    Args:
        metrics: Metrics of the run, by name.
        prefix: Section prefix, without trailing slash.

    Returns:
        Mapping of the suffix to the value, in the original order.

    Examples:
        >>> _under({"gravity/r_squared": 0.7, "output/flows": 3.0}, "gravity")
        {'r_squared': 0.7}
    """
    head = prefix.rstrip("/") + "/"
    return {name[len(head):]: value for name, value in metrics.items() if name.startswith(head)}


# Table métrique / valeur
def _kv_table(values: Mapping[str, float], value_name: str = "valeur") -> pd.DataFrame:
    """Turn a name -> value mapping into a two-column table.

    Args:
        values: Mapping to tabulate.
        value_name: Header of the value column.

    Returns:
        Table with the columns ``métrique`` and ``value_name``.

    Examples:
        >>> _kv_table({"a": 1.0}).to_dict("records")
        [{'métrique': 'a', 'valeur': 1.0}]
    """
    return pd.DataFrame({"métrique": list(values), value_name: list(values.values())})


# Formatage d'un chiffre clé
def _fig(label: str, value: Optional[float], *, unit: str = "", pct: bool = False) -> Optional[str]:
    """Format one key figure, ``None`` when the metric is absent.

    Args:
        label: Text following the value.
        value: Metric value.
        unit: Unit appended to the value.
        pct: Whether ``value`` is a share to display as a percentage.

    Returns:
        E.g. ``"97,8 % converti"``; ``None`` when ``value`` is ``None``.

    Examples:
        >>> _fig("convertie", 0.978, pct=True)
        '97,8 % convertie'
        >>> _fig("flux", 212000000.0)
        '212 000 000 flux'
        >>> _fig("flux", None) is None
        True
    """
    if value is None:
        return None
    if pct:
        return f"{format_number(round(value * 100, 1))} % {label}"
    return f"{format_number(value)}{unit} {label}".strip()


# Liste de chiffres clés sans les absents
def _figs(*items: Optional[str]) -> List[str]:
    """Drop the absent key figures.

    Args:
        *items: Key figures, possibly ``None``.

    Returns:
        The non-empty ones.

    Examples:
        >>> _figs("a", None, "b")
        ['a', 'b']
    """
    return [item for item in items if item]


# Diagramme en barres
def _bar(
    title: str,
    labels: Sequence[Any],
    values: Sequence[float],
    *,
    yaxis: str = "",
    horizontal: bool = False,
) -> Optional[Any]:
    """Build a bar chart, ``None`` without plotly or data.

    Args:
        title: Figure title.
        labels: Category labels.
        values: Bar values.
        yaxis: Value-axis title.
        horizontal: Whether the bars are horizontal.

    Returns:
        A Plotly figure, or ``None``.

    Examples:
        >>> fig = _bar("t", ["a", "b"], [1.0, 2.0])
        >>> fig is None or len(fig.data) == 1
        True
    """
    go = _plotly()
    if go is None or not len(labels):
        return None
    x, y = (list(values), list(labels)) if horizontal else (list(labels), list(values))
    figure = go.Figure(go.Bar(x=x, y=y, orientation="h" if horizontal else "v"))
    figure.update_layout(
        title=title,
        template="plotly_white",
        height=380 if not horizontal else max(320, 22 * len(labels) + 120),
        margin={"l": 60, "r": 20, "t": 50, "b": 50},
        **({"xaxis_title": yaxis} if horizontal else {"yaxis_title": yaxis}),
    )
    return figure


# Histogramme
def _hist(title: str, values: pd.Series, xaxis: str = "", nbins: int = 40) -> Optional[Any]:
    """Build a histogram, ``None`` without plotly or data.

    Args:
        title: Figure title.
        values: Numeric values.
        xaxis: Horizontal-axis title.
        nbins: Number of bins.

    Returns:
        A Plotly figure, or ``None``.

    Examples:
        >>> fig = _hist("t", pd.Series([1.0, 2.0, 2.5]))
        >>> fig is None or len(fig.data) == 1
        True
    """
    go = _plotly()
    values = pd.to_numeric(values, errors="coerce").dropna()
    if go is None or values.empty:
        return None
    figure = go.Figure(go.Histogram(x=values, nbinsx=nbins))
    figure.update_layout(
        title=title, template="plotly_white", height=380, xaxis_title=xaxis,
        margin={"l": 60, "r": 20, "t": 50, "b": 50},
    )
    return figure


# Section à partir de figures optionnelles
def _section(
    title: str,
    figures: Sequence[Optional[Any]] = (),
    tables: Optional[Mapping[str, pd.DataFrame]] = None,
    notes: Sequence[str] = (),
) -> Section:
    """Assemble a section, dropping the absent figures and the empty tables.

    Args:
        title: Section heading.
        figures: Figures, possibly ``None`` (no plotly, no data).
        tables: Caption -> table; empty tables are dropped.
        notes: Short sentences.

    Returns:
        The section.

    Examples:
        >>> s = _section("t", [None], {"a": pd.DataFrame()}, ["n"])
        >>> s.figures, s.tables, s.notes
        ([], {}, ['n'])
    """
    return Section(
        title=title,
        figures=[figure for figure in figures if figure is not None],
        tables={caption: table for caption, table in (tables or {}).items() if len(table)},
        notes=list(notes),
    )


# Sections communes : temps d'exécution
def _timing_section(metrics: Mapping[str, float]) -> Optional[Section]:
    """Build the section of the run duration and memory peak.

    Args:
        metrics: Metrics of the run.

    The BACI pass-by-pass reconstruction adds its duration per pass
    (``passes/<name>/seconds``) and the peak memory of the process
    (``memory/peak_mb``, also logged per year with ``step = year``).

    Returns:
        A section with the ``run/*`` metrics (and the pass durations and memory
        peak when present), ``None`` when none is present.

    Examples:
        >>> _timing_section({}) is None
        True
        >>> _timing_section({"run/duration_seconds": 12.0}).tables["Ressources du run"].shape
        (1, 2)
        >>> section = _timing_section({"run/duration_seconds": 12.0, "passes/P0/seconds": 3.0})
        >>> section.tables["Durée par passe (s)"].shape
        (1, 2)
    """
    run = _under(metrics, "run")
    passes = {
        name.split("/")[0]: value
        for name, value in _under(metrics, "passes").items()
        if name.endswith("/seconds")
    }
    memory = _under(metrics, "memory")
    if not run and not passes and not memory:
        return None
    return _section(
        "Temps et ressources",
        [_bar("Durée par passe (s)", list(passes), list(passes.values())) if passes else None],
        tables={
            "Ressources du run": _kv_table({**run, **{f"memory/{k}": v for k, v in memory.items()}}),
            "Durée par passe (s)": _kv_table(passes, "seconds") if passes else pd.DataFrame(),
        },
        notes=["Courbes CPU, mémoire, disque et réseau : onglet System metrics du run."],
    )


# Sections communes : distribution par métrique
def _distribution_sections(
    metrics: Mapping[str, float], prefix: str, title: str
) -> List[Section]:
    """Build the sections of a family of score distributions.

    The metrics ``<prefix>/<M>/{mean,median,p10,p90,…}`` of every score ``M`` are
    tabulated; the medians are drawn with their ``p10-p90`` range.

    Args:
        metrics: Metrics of the run.
        prefix: Prefix of the family, e.g. ``"vulnerabilities"``.
        title: Heading of the section.

    Returns:
        One section, empty list when no distribution is present.

    Examples:
        >>> m = {"v/HHI/median": 0.4, "v/HHI/p10": 0.1, "v/HHI/p90": 0.9, "v/HHI/n_scored": 10.0}
        >>> sections = _distribution_sections(m, "v", "Distributions")
        >>> sections[0].tables["Distribution par métrique"].shape[0]
        1
    """
    rows: Dict[str, Dict[str, float]] = {}
    for name, value in _under(metrics, prefix).items():
        parts = name.split("/")
        if len(parts) == 2 and parts[0] not in {"drift", "input", "cells", "coverage", "quality", "graph"}:
            rows.setdefault(parts[0], {})[parts[1]] = value
    if not rows:
        return []
    table = pd.DataFrame.from_dict(rows, orient="index").rename_axis("métrique").reset_index()
    figure = None
    go = _plotly()
    if go is not None and "median" in table:
        figure = go.Figure(
            go.Bar(
                x=table["métrique"],
                y=table["median"],
                error_y={
                    "type": "data",
                    "symmetric": False,
                    "array": (table["p90"] - table["median"]).tolist() if "p90" in table else None,
                    "arrayminus": (table["median"] - table["p10"]).tolist() if "p10" in table else None,
                },
            )
        )
        figure.update_layout(
            title="Médiane et intervalle p10-p90 par métrique", template="plotly_white", height=380,
            margin={"l": 60, "r": 20, "t": 50, "b": 50},
        )
    return [_section(title, [figure], {"Distribution par métrique": table})]


# Sections communes : famille de scalaires
def _scalar_section(
    metrics: Mapping[str, float], prefix: str, title: str, *, bar_title: Optional[str] = None,
) -> Optional[Section]:
    """Build a section tabulating the scalars directly under ``prefix``.

    Args:
        metrics: Metrics of the run.
        prefix: Prefix of the family.
        title: Heading of the section.
        bar_title: When given, also draw the values as a bar chart (suited to
            shares, which share one scale).

    Returns:
        A section, ``None`` when no scalar is present.

    Examples:
        >>> _scalar_section({"p/a": 1.0}, "p", "Titre").tables["Métriques"].shape
        (1, 2)
        >>> _scalar_section({}, "p", "Titre") is None
        True
    """
    values = {name: v for name, v in _under(metrics, prefix).items() if "/" not in name}
    if not values:
        return None
    figure = _bar(bar_title, list(values), list(values.values()), horizontal=True) if bar_title else None
    return _section(title, [figure], {"Métriques": _kv_table(values)})


# ──────────────────────────────────────────────────────────────────────
# Téléchargements (download_eurostat, download_comtrade)
# ──────────────────────────────────────────────────────────────────────

# Chiffres clés des téléchargements
def key_figures_downloads(metrics: Mapping[str, float]) -> List[str]:
    """Key figures of a download run.

    Args:
        metrics: Metrics of the run (``download/*``, ``coverage/*``, ``run/*``).

    Returns:
        Queries processed, in error and left, rows written, limiter wait share,
        share of the planned queries ever downloaded and days left at the
        current pace.

    Examples:
        >>> key_figures_downloads({"download/processed": 90.0, "download/errors": 2.0})
        ['90 requêtes traitées', '2 en erreur']
    """
    d = _under(metrics, "download")
    return _figs(
        _fig("requêtes traitées", d.get("processed")),
        _fig("en erreur", d.get("errors")),
        _fig("restantes", d.get("n_queries_remaining")),
        _fig("lignes écrites", d.get("rows_written")),
        _fig("du temps en attente du limiteur", metrics.get("download/wait_share"), pct=True),
        _fig("des requêtes planifiées déjà téléchargées", metrics.get("coverage/share_downloaded"), pct=True),
        _fig("jours restants au rythme actuel", metrics.get("coverage/eta_days")),
        _fig("Mo de pic mémoire", metrics.get("run/peak_memory_mb")),
    )


# Sections des téléchargements
def sections_downloads(
    metrics: Mapping[str, float], artifacts: Mapping[str, pd.DataFrame]
) -> List[Section]:
    """HTML sections of a download run.

    Args:
        metrics: Metrics of the run.
        artifacts: Artifact tables; ``download/queries.csv`` (one row per
            query) feeds the per-query views, ``coverage/by_year.csv`` (share of
            the planned queries downloaded, by year) and
            ``coverage/by_reporter.csv`` (stored coverage, by reporter) the
            coverage audit.

    Returns:
        Requests and errors, per-query progress, limiter waits, coverage,
        timing.

    Examples:
        >>> sections_downloads({"download/processed": 3.0}, {})[0].title
        'Requêtes et erreurs'
    """
    d = _under(metrics, "download")
    counts = {k: d[k] for k in ("processed", "empty", "errors", "n_queries_remaining") if k in d}
    sections = [
        _section(
            "Requêtes et erreurs",
            [_bar("Requêtes par issue", list(counts), list(counts.values()))],
            {"Compteurs": _kv_table(d)},
        )
    ]
    queries = artifacts.get("download/queries.csv")
    if queries is not None and len(queries):
        go = _plotly()
        figure = None
        if go is not None and {"rows_written", "duration_seconds"} <= set(queries.columns):
            figure = go.Figure()
            figure.add_bar(x=list(range(len(queries))), y=queries["rows_written"], name="lignes écrites")
            figure.add_scatter(
                x=list(range(len(queries))), y=queries["duration_seconds"], name="durée (s)", yaxis="y2"
            )
            figure.update_layout(
                title="Requêtes au fil du run (rang de la requête)", template="plotly_white", height=380,
                yaxis2={"overlaying": "y", "side": "right"}, xaxis_title="requête",
                margin={"l": 60, "r": 60, "t": 50, "b": 50},
            )
        errors = queries[queries["error_type"].notna()] if "error_type" in queries else queries.iloc[0:0]
        sections.append(_section("Progression par requête", [figure], {"Requêtes en erreur": errors}))
    waits = {
        "attente du limiteur (s)": d.get("rate_limit_wait_seconds"),
        "temps HTTP (s)": d.get("http_seconds"),
        "durée totale (s)": d.get("duration_seconds"),
    }
    waits = {k: v for k, v in waits.items() if v is not None}
    if waits:
        sections.append(
            _section("Attentes du limiteur", [_bar("Répartition du temps", list(waits), list(waits.values()))],
                     {"Temps": _kv_table(waits)})
        )
    # Audit de couverture : part téléchargée par année, couverture stockée par déclarant
    coverage = _under(metrics, "coverage")
    by_year = artifacts.get("coverage/by_year.csv")
    by_reporter = artifacts.get("coverage/by_reporter.csv")
    if coverage or by_year is not None or by_reporter is not None:
        figure = (
            _bar("Part des requêtes planifiées téléchargées, par année",
                 by_year["year"].astype(str).tolist(), by_year["share_queries_downloaded"].tolist())
            if by_year is not None and {"year", "share_queries_downloaded"} <= set(by_year.columns) else None
        )
        sections.append(
            _section(
                "Couverture",
                [figure],
                {
                    "Audit": _kv_table(coverage),
                    "Par année": by_year if by_year is not None else pd.DataFrame(),
                    "Par déclarant": by_reporter if by_reporter is not None else pd.DataFrame(),
                },
            )
        )
    timing = _timing_section(metrics)
    return sections + ([timing] if timing else [])


# ──────────────────────────────────────────────────────────────────────
# BACI (process_baci_<millésime>)
# ──────────────────────────────────────────────────────────────────────

# Chiffres clés de BACI
def key_figures_baci(metrics: Mapping[str, float]) -> List[str]:
    """Key figures of a BACI vintage run.

    Args:
        metrics: Metrics of the run, one section per step (``conversion/*``,
            ``gravity/*``, ``valuation/*``, ``reconciliation/*``, ``quality/*``,
            ``nes/*``, ``harmonization/*``, ``output/*``), plus ``coverage/*``
            and ``run/*``.

    Returns:
        Years eligible, flows, converted share, median freight rate, gravity
        R², value reallocated from NES, memory peak.

    Examples:
        >>> key_figures_baci({"output/flows": 212000000.0, "conversion/share_tonnage_missing": 0.022})
        ['212 000 000 flux', '97,8 % converti en tonnes']
    """
    missing = metrics.get("conversion/share_tonnage_missing")
    return _figs(
        _fig("années éligibles", metrics.get("coverage/years_eligible")),
        _fig("flux", metrics.get("output/flows")),
        _fig("converti en tonnes", None if missing is None else 1.0 - missing, pct=True),
        _fig("de taux de fret médian", metrics.get("gravity/median_freight_rate"), pct=True),
        _fig("de R² de la gravité", metrics.get("gravity/r_squared")),
        _fig("de valeur NES réallouée", metrics.get("nes/share_nes_value_reallocated"), pct=True),
        _fig("Mo de pic mémoire", metrics.get("run/peak_memory_mb")),
    )


# Sections de BACI : une par étape
def sections_baci(
    metrics: Mapping[str, float], artifacts: Mapping[str, pd.DataFrame]
) -> List[Section]:
    """HTML sections of a BACI vintage run, one per step.

    Conversion, fobisation, gravity, reporter quality, valuation and
    reconciliation, NES, harmonization, output, timing.

    Args:
        metrics: Metrics of the run, one section per step (see
            :func:`key_figures_baci`).
        artifacts: Artifact tables logged by ``run_baci``:
            ``tonnage/conversion_rates.csv``, ``fobisation/valuation_regimes.csv``,
            ``quality/sigma_by_country.csv``; and ``output/rows_by_year.csv``
            (columns ``year``, ``rows``) built by the caller from the written frame.

    Returns:
        Sections in pipeline order; a step without any metric is skipped.

    Examples:
        >>> titles = [s.title for s in sections_baci({"output/flows": 3.0}, {})]
        >>> titles
        ['Sortie']
    """
    sections: List[Section] = []

    # Conversion en tonnes : taux validés par produit et unité
    tonnage = {k: v for k, v in _under(metrics, "conversion").items() if "/" not in k}
    rates = artifacts.get("tonnage/conversion_rates.csv")
    if tonnage or rates is not None:
        shares = {k: v for k, v in tonnage.items() if k.startswith("share_")}
        sections.append(
            _section(
                "Conversion en tonnes",
                [
                    _bar("Origine des tonnages (parts)", list(shares), list(shares.values()), horizontal=True),
                    _hist("Distribution des taux de conversion validés", rates["rate"], "taux (t / unité)")
                    if rates is not None and "rate" in rates else None,
                ],
                {"Métriques": _kv_table(tonnage), "Taux de conversion": rates if rates is not None else pd.DataFrame()},
            )
        )

    # Fobisation : taux de fret, parts FAS rétablies / tronquées
    fob = {
        k: v for k, v in _under(metrics, "valuation").items() if "/" not in k and not k.startswith("regime_")
    }
    regimes = artifacts.get("fobisation/valuation_regimes.csv")
    if fob or regimes is not None:
        shares = {k: v for k, v in fob.items() if k.startswith("share_")}
        freight = {
            k.replace("_freight_rate", ""): v
            for k, v in _under(metrics, "gravity").items()
            if k in ("p10_freight_rate", "median_freight_rate", "mean_freight_rate", "p90_freight_rate")
        }
        sections.append(
            _section(
                "Fobisation",
                [
                    _bar("Parts de flux par traitement", list(shares), list(shares.values()), horizontal=True),
                    _bar("Taux de fret estimés (p10, médian, moyen, p90)", list(freight), list(freight.values())),
                ],
                {"Métriques": _kv_table(fob), "Régimes de valorisation": regimes if regimes is not None else pd.DataFrame()},
            )
        )

    # Gravité : coefficients, R², points de Cook exclus
    gravity = _under(metrics, "gravity")
    if gravity:
        coefficients = {k.split("/", 1)[1]: v for k, v in gravity.items() if k.startswith("coefficients/")}
        scalars = {k: v for k, v in gravity.items() if "/" not in k and "freight" not in k}
        sections.append(
            _section(
                "Gravité",
                [_bar("Coefficients de l'équation de gravité", list(coefficients), list(coefficients.values()), horizontal=True)],
                {"Ajustement": _kv_table(scalars), "Coefficients": _kv_table(coefficients, "coefficient")},
            )
        )

    # Qualité des déclarants : σ̂ par pays, triés
    sigma = artifacts.get("quality/sigma_by_country.csv")
    quality = {
        f"{step}/{k}": v
        for step in ("value", "quantity")
        for k, v in _under(metrics, f"quality/{step}").items()
    }
    if quality or sigma is not None:
        figure, top = None, pd.DataFrame()
        if sigma is not None and "sigma_export_value" in sigma:
            top = sigma.sort_values("sigma_export_value", ascending=False).head(30)
            figure = _bar("σ̂ (valeurs, exports) : 30 déclarants les moins fiables", top["country"].astype(str).tolist(),
                          top["sigma_export_value"].tolist(), horizontal=True)
        sections.append(_section("Qualité des déclarants", [figure],
                                 {"Métriques": _kv_table(quality), "σ̂ par pays (30 plus élevés)": top}))

    # Valorisation et réconciliation des flux miroirs
    mirror = {k: v for k, v in _under(metrics, "reconciliation").items() if "/" not in k}
    regime = {k: v for k, v in _under(metrics, "valuation").items() if k.startswith("regime_")}
    if mirror or regime:
        shares = {k: v for k, v in mirror.items() if k.startswith("share_")}
        sections.append(
            _section(
                "Valorisation et réconciliation",
                [_bar("Flux miroirs par source de déclaration (parts)", list(shares), list(shares.values()))],
                {"Miroirs": _kv_table(mirror), "Régimes par pays-année": _kv_table(regime)},
            )
        )

    # NES : valeur réallouée
    nes = {k: v for k, v in _under(metrics, "nes").items() if "/" not in k}
    if nes:
        values = {k: v for k, v in nes.items() if k.startswith("value_")}
        sections.append(
            _section("NES (« non spécifié ailleurs »)",
                     [_bar("Valeur réallouée, absorbée ou écartée", list(values), list(values.values()))],
                     {"Métriques": _kv_table(nes)})
        )

    # Harmonisation de nomenclature
    hs = _under(metrics, "harmonization")
    if hs:
        sections.append(_scalar_section(metrics, "harmonization", "Harmonisation de nomenclature", bar_title=None))

    # Sortie
    output = {
        k: metrics[k]
        for k in ("output/flows", "output/created", "output/n_input_declarations", "output/total_reconciled_value")
        if k in metrics
    }
    coverage = _under(metrics, "coverage")
    by_year = artifacts.get("output/rows_by_year.csv")
    if output or coverage or by_year is not None:
        figure = (
            _bar("Lignes écrites par année", by_year["year"].astype(str).tolist(), by_year["rows"].tolist())
            if by_year is not None and {"year", "rows"} <= set(by_year.columns) else None
        )
        sections.append(
            _section(
                "Sortie",
                [figure],
                {
                    "Écriture": _kv_table(output),
                    "Complétude des années": _kv_table(coverage),
                    "Lignes par année": by_year if by_year is not None else pd.DataFrame(),
                },
            )
        )

    timing = _timing_section(metrics)
    if timing:
        timing.notes.append("Aucune durée par passe ni par année n'est encore journalisée (K-07).")
        sections.append(timing)
    return [section for section in sections if section is not None]


# ──────────────────────────────────────────────────────────────────────
# Vulnérabilités (partenaires et réseau)
# ──────────────────────────────────────────────────────────────────────

# Préfixes des métriques et des artefacts des deux familles de vulnérabilités
_PARTNER_METRICS = "partners"
_PARTNER_ARTIFACTS = "vulnerabilities"
_NETWORK_METRICS = "network"
_NETWORK_ARTIFACTS = "network_vulnerabilities"


# Sens de flux présents dans les métriques d'une famille
def _flows_in(metrics: Mapping[str, float], family: str) -> List[str]:
    """List the flow directions a vulnerability run reported, in order of appearance.

    The vulnerability metrics are emitted per direction
    (``<family>/<flow>/cells/n_total``), never pooled across directions.

    Args:
        metrics: Metrics of the run.
        family: Metric family prefix (``"partners"``, ``"network"``).

    Returns:
        The directions found.

    Examples:
        >>> _flows_in({"partners/export/cells/n_total": 1.0, "partners/import/cells/n_total": 2.0}, "partners")
        ['export', 'import']
    """
    flows: List[str] = []
    for name in metrics:
        parts = name.split("/")
        if len(parts) == 4 and parts[0] == family and parts[2:] == ["cells", "n_total"]:
            flows.append(parts[1])
    return list(dict.fromkeys(flows))


# Somme ou maximum d'une métrique sur les sens d'une famille
def _over_flows(metrics: Mapping[str, float], family: str, name: str, how: str = "sum") -> Optional[float]:
    """Combine one per-direction metric over the directions of a run.

    Args:
        metrics: Metrics of the run.
        family: Metric family prefix.
        name: Metric name below ``<family>/<flow>/``.
        how: ``"sum"`` (additive counts such as cells) or ``"max"``
            (cardinalities shared by the directions).

    Returns:
        The combined value, ``None`` when no direction reports it.

    Examples:
        >>> m = {"p/import/cells/n_total": 2.0, "p/export/cells/n_total": 3.0}
        >>> _over_flows(m, "p", "cells/n_total"), _over_flows(m, "p", "cells/n_total", "max")
        (5.0, 3.0)
    """
    values = [
        metrics[key]
        for key in (f"{family}/{flow}/{name}" for flow in ("import", "export"))
        if key in metrics
    ]
    if not values:
        return None
    return float(sum(values)) if how == "sum" else float(max(values))


# Chiffres clés des vulnérabilités
def key_figures_partner_vulnerabilities(metrics: Mapping[str, float]) -> List[str]:
    """Key figures of the partner-vulnerability run.

    Args:
        metrics: Metrics of the run (``partners/<flow>/*``).

    Returns:
        Cells (all directions), reporters, products and periods scored.

    Examples:
        >>> key_figures_partner_vulnerabilities({"partners/import/cells/n_total": 1200.0})
        ['1 200 cellules']
    """
    family = _PARTNER_METRICS
    return _figs(
        _fig("cellules", _over_flows(metrics, family, "cells/n_total")),
        _fig("déclarants", _over_flows(metrics, family, "input/n_reporters", "max")),
        _fig("produits", _over_flows(metrics, family, "input/n_products", "max")),
        _fig("périodes", _over_flows(metrics, family, "input/n_periods", "max")),
        _fig("Mo de pic mémoire", metrics.get("run/peak_memory_mb")),
    )


# Sections des vulnérabilités partenaires
def sections_partner_vulnerabilities(
    metrics: Mapping[str, float], artifacts: Mapping[str, pd.DataFrame]
) -> List[Section]:
    """HTML sections of the partner-vulnerability run, one block per direction.

    Args:
        metrics: Metrics of the run (``partners/<flow>/*``).
        artifacts: Artifact tables (``vulnerabilities/<flow>/top_vulnerable_products.csv``…).

    Returns:
        Per direction: distribution of each score, coverage, quality, drift,
        entries, top cells; then the timing section.

    Examples:
        >>> sections_partner_vulnerabilities({"partners/import/cells/n_total": 3.0}, {})[0].title
        'Entrée — import'
    """
    return _flow_sections(metrics, artifacts, _PARTNER_METRICS, _PARTNER_ARTIFACTS)


# Chiffres clés des vulnérabilités de réseau
def key_figures_network_vulnerabilities(metrics: Mapping[str, float]) -> List[str]:
    """Key figures of a network-vulnerability run.

    Args:
        metrics: Metrics of the run (``network/<flow>/*``).

    Returns:
        Cells (all directions), graphs, median graph size and share of
        disconnected graphs (the graph shape does not depend on the direction).

    Examples:
        >>> key_figures_network_vulnerabilities({"network/import/graph/n_graphs": 40.0})
        ['40 graphes']
    """
    family = _NETWORK_METRICS
    return _figs(
        _fig("cellules", _over_flows(metrics, family, "cells/n_total")),
        _fig("graphes", _over_flows(metrics, family, "graph/n_graphs", "max")),
        _fig("nœuds (médiane)", _over_flows(metrics, family, "graph/median_n_nodes", "max")),
        _fig(
            "de graphes déconnectés",
            _over_flows(metrics, family, "graph/share_graphs_disconnected", "max"),
            pct=True,
        ),
        _fig("Mo de pic mémoire", metrics.get("run/peak_memory_mb")),
    )


# Sections des vulnérabilités de réseau
def sections_network_vulnerabilities(
    metrics: Mapping[str, float], artifacts: Mapping[str, pd.DataFrame]
) -> List[Section]:
    """HTML sections of a network-vulnerability run, one block per direction.

    Args:
        metrics: Metrics of the run (``network/<flow>/*``).
        artifacts: Artifact tables (``network_vulnerabilities/<flow>/*.csv``).

    Returns:
        Per direction: distribution of each network metric, coverage, graph
        quality, drift, entries; then the timing section.

    Examples:
        >>> sections_network_vulnerabilities({}, {})
        []
    """
    return _flow_sections(metrics, artifacts, _NETWORK_METRICS, _NETWORK_ARTIFACTS)


# Sections par sens d'une famille de vulnérabilités
def _flow_sections(
    metrics: Mapping[str, float],
    artifacts: Mapping[str, pd.DataFrame],
    family: str,
    artifact_family: str,
) -> List[Section]:
    """Build the sections of each direction, titled after it, then the timing.

    Args:
        metrics: Metrics of the run.
        artifacts: Artifact tables.
        family: Metric family prefix.
        artifact_family: Artifact directory of the family.

    Returns:
        The sections, skipping the empty ones.

    Examples:
        >>> _flow_sections({}, {}, "p", "a")
        []
    """
    sections: List[Section] = []
    for flow in _flows_in(metrics, family):
        for section in _vulnerability_sections(
            metrics, artifacts, f"{family}/{flow}", f"{artifact_family}/{flow}"
        ):
            section.title = f"{section.title} — {flow}"
            sections.append(section)
    timing = _timing_section(metrics)
    return sections + ([timing] if timing else [])


# Sections communes aux deux familles de vulnérabilités
def _vulnerability_sections(
    metrics: Mapping[str, float],
    artifacts: Mapping[str, pd.DataFrame],
    prefix: str,
    artifact_prefix: str,
) -> List[Section]:
    """Build the sections shared by the partner and network vulnerability runs.

    Args:
        metrics: Metrics of the run.
        artifacts: Artifact tables.
        prefix: Metric prefix of the family and direction.
        artifact_prefix: Artifact directory of the family and direction.

    Returns:
        The sections, skipping the empty ones (timing excluded).

    Examples:
        >>> _vulnerability_sections({}, {}, "p", "a")
        []
    """
    sections: List[Section] = []
    inputs = _scalar_section(metrics, f"{prefix}/input", "Entrée")
    cells = {k: v for k, v in metrics.items() if k in (f"{prefix}/cells/n_total", f"{prefix}/created")}
    if inputs is not None or cells:
        base = inputs or _section("Entrée")
        base.tables["Cellules"] = _kv_table(cells)
        sections.append(base)
    sections += _distribution_sections(metrics, prefix, "Distribution des scores")
    coverage = {k: v for k, v in _under(metrics, f"{prefix}/coverage").items()}
    if coverage:
        names = [k.replace("share_non_null/", "") for k in coverage]
        sections.append(_section("Couverture des scores", [_bar("Part de cellules renseignées", names, list(coverage.values()))],
                                 {"Couverture": _kv_table(coverage)}))
    for name, title in (("quality", "Qualité de l'entrée"), ("graph", "Qualité des graphes"), ("drift", "Dérive")):
        section = _scalar_section(metrics, f"{prefix}/{name}", title)
        if section is not None:
            sections.append(section)
    top = {
        path: table for path, table in artifacts.items()
        if path.startswith(artifact_prefix + "/") and path.endswith(".csv")
    }
    if top:
        sections.append(_section("Tables journalisées", tables={path: table.head(50) for path, table in top.items()}))
    return sections


# ──────────────────────────────────────────────────────────────────────
# Synthèse et cohérence
# ──────────────────────────────────────────────────────────────────────

# Regroupement par niveau de comparaison
def _by_level(metrics: Mapping[str, float], prefix: str) -> Dict[str, Dict[str, float]]:
    """Group the ``<prefix>/<level>/<name>`` metrics by comparison level.

    Args:
        metrics: Metrics of the run.
        prefix: Family prefix, e.g. ``"synthesis"``.

    Returns:
        Level (``by_product``, ``by_reporter``, ``global``) -> name -> value.

    Examples:
        >>> _by_level({"s/global/n_groups": 1.0, "s/n_cells": 5.0}, "s")
        {'global': {'n_groups': 1.0}}
    """
    levels: Dict[str, Dict[str, float]] = {}
    for name, value in _under(metrics, prefix).items():
        parts = name.split("/")
        if len(parts) == 2:
            levels.setdefault(parts[0], {})[parts[1]] = value
    return levels


# Chiffres clés de la synthèse
def key_figures_synthesis(metrics: Mapping[str, float]) -> List[str]:
    """Key figures of the synthetic-score run.

    Args:
        metrics: Metrics of the run (``synthesis/*``).

    Returns:
        Contexts and cells computed, groups per level.

    Examples:
        >>> key_figures_synthesis({"synthesis/n_contexts": 2.0, "synthesis/by_product/n_groups": 30.0})
        ['2 contextes', '30 groupes (by_product)']
    """
    levels = _by_level(metrics, "synthesis")
    return _figs(
        _fig("contextes", metrics.get("synthesis/n_contexts")),
        _fig("cellules", metrics.get("synthesis/n_cells")),
        *[_fig(f"groupes ({level})", values.get("n_groups")) for level, values in levels.items()],
        _fig("Mo de pic mémoire", metrics.get("run/peak_memory_mb")),
    )


# Sections de la synthèse
def sections_synthesis(
    metrics: Mapping[str, float], artifacts: Mapping[str, pd.DataFrame]
) -> List[Section]:
    """HTML sections of the synthetic-score run.

    Args:
        metrics: Metrics of the run (``synthesis/*``).
        artifacts: Artifact tables (``synthesis/*.csv``); the last context's only,
            the artifact paths being fixed.

    Returns:
        Contexts by level, duration by method, artifact tables.

    Examples:
        >>> sections_synthesis({"synthesis/global/n_groups": 1.0}, {})[0].title
        'Contextes par niveau'
    """
    levels = _by_level(metrics, "synthesis")
    sections: List[Section] = []
    if levels:
        table = pd.DataFrame.from_dict(levels, orient="index").rename_axis("niveau").reset_index()
        counts = {level: values.get("n_groups") for level, values in levels.items() if "n_groups" in values}
        sections.append(_section("Contextes par niveau", [_bar("Groupes par niveau", list(counts), list(counts.values()))],
                                 {"Niveaux": table}))
        seconds = [
            {"niveau": level, "méthode": name[len("seconds_"):], "secondes": value}
            for level, values in levels.items() for name, value in values.items() if name.startswith("seconds_")
        ]
        if seconds:
            frame = pd.DataFrame(seconds)
            go = _plotly()
            figure = None
            if go is not None:
                figure = go.Figure([go.Bar(name=level, x=part["méthode"], y=part["secondes"]) for level, part in frame.groupby("niveau")])
                figure.update_layout(title="Durée par méthode et par niveau", barmode="group", template="plotly_white", height=380,
                                     yaxis_title="secondes", margin={"l": 60, "r": 20, "t": 50, "b": 50})
            sections.append(_section("Durée par méthode", [figure], {"Durées": frame}))
    tables = {path: table.head(50) for path, table in artifacts.items() if path.startswith("synthesis/")}
    if tables:
        sections.append(_section("Tables journalisées (dernier contexte)", tables=tables))
    timing = _timing_section(metrics)
    return sections + ([timing] if timing else [])


# Chiffres clés de la cohérence
def key_figures_coherence(metrics: Mapping[str, float]) -> List[str]:
    """Key figures of the coherence run.

    Args:
        metrics: Metrics of the run (``coherence/*``).

    Returns:
        Contexts and groups analysed.

    Examples:
        >>> key_figures_coherence({"coherence/n_contexts": 2.0})
        ['2 contextes']
    """
    return _figs(
        _fig("contextes", metrics.get("coherence/n_contexts")),
        _fig("groupes", metrics.get("coherence/n_groups")),
        _fig("Mo de pic mémoire", metrics.get("run/peak_memory_mb")),
    )


# Sections de la cohérence
def sections_coherence(
    metrics: Mapping[str, float], artifacts: Mapping[str, pd.DataFrame]
) -> List[Section]:
    """HTML sections of the coherence run.

    Args:
        metrics: Metrics of the run (``coherence/*``).
        artifacts: Artifact tables (``coherence/tau_matrix_*.csv``, ``coherence/lomo_*.csv``).

    Returns:
        Groups by level, then the logged pair tables. The concordance
        statistics (``kendall_w``, ``disputed_share``…) are not logged at run
        level: the section says so.

    Examples:
        >>> sections_coherence({"coherence/global/n_groups": 1.0}, {})[0].title
        'Groupes par niveau'
    """
    levels = _by_level(metrics, "coherence")
    sections: List[Section] = []
    if levels:
        counts = {level: v.get("n_groups") for level, v in levels.items() if "n_groups" in v}
        sections.append(_section(
            "Groupes par niveau", [_bar("Groupes analysés par niveau", list(counts), list(counts.values()))],
            {"Niveaux": pd.DataFrame.from_dict(levels, orient="index").rename_axis("niveau").reset_index()},
            notes=["Les statistiques de concordance sont calculées par contexte et non agrégées au niveau du run."],
        ))
    tables = {path: table.head(50) for path, table in artifacts.items() if path.startswith("coherence/")}
    if tables:
        sections.append(_section("Paires de métriques et retrait d'une métrique", tables=tables))
    timing = _timing_section(metrics)
    return sections + ([timing] if timing else [])


# ──────────────────────────────────────────────────────────────────────
# Couche de service
# ──────────────────────────────────────────────────────────────────────

# Chiffres clés de la publication
def key_figures_serving(metrics: Mapping[str, float]) -> List[str]:
    """Key figures of the serving publication.

    Args:
        metrics: Metrics of the run (``serving/*``).

    Returns:
        Tables published, total rows, failures.

    Examples:
        >>> key_figures_serving({"serving/cell_scores/rows": 10.0, "serving/countries/rows": 5.0})
        ['2 tables publiées', '15 lignes']
    """
    rows = {k: v for k, v in metrics.items() if k.startswith("serving/") and k.endswith("/rows")}
    return _figs(
        _fig("tables publiées", float(len(rows))) if rows else None,
        _fig("lignes", sum(rows.values())) if rows else None,
        _fig("en échec", metrics.get("serving/n_failures")),
    )


# Sections de la publication
def sections_serving(
    metrics: Mapping[str, float], artifacts: Mapping[str, pd.DataFrame]
) -> List[Section]:
    """HTML sections of the serving publication.

    Args:
        metrics: Metrics of the run (``serving/<table>/{rows,seconds,files}``).
        artifacts: Artifact tables (unused, kept for the common signature).

    Returns:
        Rows and files per table.

    Examples:
        >>> sections_serving({"serving/countries/rows": 5.0}, {})[0].title
        'Lignes et fichiers par table'
    """
    per_table: Dict[str, Dict[str, float]] = {}
    for name, value in _under(metrics, "serving").items():
        parts = name.split("/")
        if len(parts) == 2:
            per_table.setdefault(parts[0], {})[parts[1]] = value
    sections: List[Section] = []
    if per_table:
        table = pd.DataFrame.from_dict(per_table, orient="index").rename_axis("table").reset_index()
        rows = {t: v.get("rows") for t, v in per_table.items() if "rows" in v}
        sections.append(_section("Lignes et fichiers par table", [_bar("Lignes par table", list(rows), list(rows.values()))],
                                 {"Tables": table}))
    timing = _timing_section(metrics)
    return sections + ([timing] if timing else [])
