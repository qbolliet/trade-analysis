"""Serving step: publish the dashboard tables into the ``serving`` catalog (PS-29).

:func:`publish_serving` renders the SQL templates of ``serving.TABLES``
(``config/serving.yaml``) against the source tables of the pipeline, then
publishes every table in one transaction through
:class:`~kedro_pipeline.io.serving.ServingCatalog`. Rendering is pure and
testable: :func:`source_tables` resolves the physical tables from the
configurations of the upstream steps (``demo_*`` schemas of the profile
included), and the generated fragments (synthesis pivot, normalised columns,
partner predicate) come from the parameters. The nomenclature columns
(``classification``, ``hs_vintage``, ``in_force``) are derived by SQL macros
generated from ``runtime.NOMENCLATURES.HS`` while the result tables do not
carry them (PS-29.3).

No environment variable and no YAML path are read here (PS-08 invariant 4).
"""
# Importation des modules
# Modules de base
import logging
import re
from dataclasses import dataclass
from string import Template
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

# Modules internes
from kedro_pipeline.config import nomenclature_macros_sql
from kedro_pipeline.io.ducklake import DuckLakeLocation
from kedro_pipeline.io.serving import ServingCatalog, ServingTableSpec
from kedro_pipeline.steps.reference import REFERENCE_COLUMNS, reference_schema

# Logger
logger = logging.getLogger(__name__)

# Identifiant SQL sûr (méthodes, niveaux, métriques interpolés dans les requêtes)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Niveaux de comparaison de la table synthesis (S-2.4)
_LEVELS = ("by_product", "by_reporter", "global")
# Table de référence de chaque variable de référentiel
_REFERENCE_OF = {
    "ref_eurostat_products": "products",
    "ref_eurostat_reporters": "reporters",
    "ref_eurostat_partners": "partners",
    "ref_comtrade_products": "products",
    "ref_comtrade_reporters": "reporters",
    "hs_concordance": "hs_concordance",
    "hs_vintages": "hs_vintages",
}


# Description d'une table source (catalogue attaché en lecture seule)
@dataclass(frozen=True)
class SourceTable:
    """A source table read by the serving queries.

    Attributes:
        catalog_alias: Alias of the attached source catalog.
        schema: Schema of the table.
        table: Table name (``fact_table`` for every table written by
            ``statflows.write_dataframe``).

    Examples:
        >>> SourceTable("vulnerabilities", "indicators").qualified_name
        'vulnerabilities."indicators"."fact_table"'
    """

    catalog_alias: str
    schema: str
    table: str = "fact_table"

    # Propriété : nom qualifié cité
    @property
    def qualified_name(self) -> str:
        """Quoted qualified name, as used in the rendered queries."""
        return f'{self.catalog_alias}."{self.schema}"."{self.table}"'

    # Propriété : clé d'existence (format de ServingCatalog.existing_tables)
    @property
    def key(self) -> str:
        """Unquoted ``catalog.schema.table`` key."""
        return f"{self.catalog_alias}.{self.schema}.{self.table}"


# Fonction de vérification d'un identifiant SQL
def _identifier(value: str, what: str) -> str:
    """Return ``value`` if it is a safe SQL identifier, else raise ``ValueError``."""
    if not _IDENTIFIER.match(str(value)):
        raise ValueError(f"Invalid {what} '{value}': expected a plain SQL identifier")
    return str(value)


# Fonction de citation d'un littéral SQL
def _literal(value: str) -> str:
    """Escape a string for a single-quoted SQL literal (quotes not included)."""
    return str(value).replace("'", "''")


# Fonction de résolution des tables sources depuis les configurations amont
def source_tables(
    *,
    eurostat: Mapping[str, Any],
    comtrade: Mapping[str, Any],
    vulnerabilities: Mapping[str, Any],
    synthesis: Mapping[str, Any],
) -> Tuple[Dict[str, DuckLakeLocation], Dict[str, SourceTable]]:
    """Resolve the source catalogs and tables from the upstream configurations.

    The profile's schemas (``demo_*``) are read from the same files the
    upstream scripts read, so the serving queries always target what the
    pipeline actually wrote.

    Args:
        eurostat: Parsed ``eurostat.yaml`` (``DATAFLOW``, ``DOWNLOADS``).
        comtrade: Parsed ``comtrade.yaml`` (``DATAFLOW``, ``DOWNLOADS``).
        vulnerabilities: Parsed ``vulnerabilities.yaml``.
        synthesis: Parsed ``synthesis.yaml`` (``SYNTHESIS``, ``COHERENCE``).

    Returns:
        Tuple ``(locations, tables)``: the source catalogs to attach
        ``READ_ONLY`` keyed by alias, and the template variables mapped to
        their :class:`SourceTable`.

    Raises:
        KeyError: If a configuration lacks a required key.
    """
    from statflows.core.download import _schema_name

    # Catalogues sources (Comext, Comtrade, résultats)
    locations: Dict[str, DuckLakeLocation] = {}
    for config in (eurostat, comtrade):
        downloads = config["DOWNLOADS"]
        dataflow = config["DATAFLOW"]
        locations[downloads["CATALOG_ALIAS"]] = DuckLakeLocation(
            dbname=downloads["DBNAME"],
            catalog_alias=downloads["CATALOG_ALIAS"],
            schema=_schema_name(dataflow),
            bucket=downloads[dataflow]["BUCKET"],
            data_path=downloads[dataflow]["PATHS"]["DATA_PATH"],
        )
    catalog = vulnerabilities["VULNERABILITIES"]
    partners = catalog[eurostat["DATAFLOW"]]
    locations[catalog["CATALOG_ALIAS"]] = DuckLakeLocation(
        dbname=catalog["DBNAME"],
        catalog_alias=catalog["CATALOG_ALIAS"],
        schema=_schema_name(partners["RESULT_SCHEMA"]),
        bucket=partners["BUCKET"],
        data_path=partners["PATHS"]["DATA_PATH"],
    )

    eurostat_alias = eurostat["DOWNLOADS"]["CATALOG_ALIAS"]
    comtrade_alias = comtrade["DOWNLOADS"]["CATALOG_ALIAS"]
    results_alias = catalog["CATALOG_ALIAS"]
    eurostat_prefix = eurostat["DOWNLOADS"]["REFERENCE"]["SCHEMA_PREFIX"]
    comtrade_prefix = comtrade["DOWNLOADS"]["REFERENCE"]["SCHEMA_PREFIX"]

    tables = {
        "comext": SourceTable(eurostat_alias, _schema_name(eurostat["DATAFLOW"])),
        "indicators": SourceTable(results_alias, _schema_name(partners["RESULT_SCHEMA"])),
        "network": SourceTable(
            results_alias,
            _schema_name(vulnerabilities["NETWORK_VULNERABILITIES"]["RESULT_SCHEMA"]),
        ),
        "synthesis": SourceTable(
            results_alias, _schema_name(synthesis["SYNTHESIS"]["RESULT_SCHEMA"])
        ),
        "diagnostics": SourceTable(
            results_alias, _schema_name(synthesis["COHERENCE"]["RESULT_SCHEMA"])
        ),
    }
    for name, reference in _REFERENCE_OF.items():
        on_comtrade = name.startswith("ref_comtrade") or name.startswith("hs_")
        tables[name] = SourceTable(
            comtrade_alias if on_comtrade else eurostat_alias,
            reference_schema(comtrade_prefix if on_comtrade else eurostat_prefix, reference),
        )
    return locations, tables


# Fonction de génération du pivot des scores de synthèse
def synthesis_pivot_sql(
    methods: Sequence[str],
    levels: Sequence[str],
    primary_method: str,
    *,
    alias: str = "s",
) -> str:
    """Generate the select list pivoting the long ``synthesis`` table into columns.

    One ``<method>_score_<level>`` and ``<method>_rank_<level>`` column per
    method × level, plus ``primary_score_<level>``, ``primary_rank_<level>``
    and ``primary_n_<level>`` for the primary method. Meant for a query
    grouped by the cell keys (one row per cell).

    Args:
        methods: Values of the ``method`` column to expose.
        levels: Comparison levels to expose (``by_product``, ``by_reporter``,
            ``global``).
        primary_method: Method duplicated in the ``primary_*`` columns.
        alias: Alias of the synthesis table in the query.

    Returns:
        Comma-separated aggregate expressions.

    Raises:
        ValueError: If a method, level or alias is not a plain identifier, or
            a level is unknown.

    Examples:
        >>> print(synthesis_pivot_sql(["auto_sum"], ["by_product"], "auto_sum").splitlines()[0])
        max(CASE WHEN s."method" = 'auto_sum' THEN s."score_by_product" END) AS "auto_sum_score_by_product",
    """
    alias = _identifier(alias, "alias")
    for level in levels:
        if level not in _LEVELS:
            raise ValueError(f"Unknown level '{level}', expected one of {_LEVELS}")

    # Fonction de génération d'une colonne pivotée
    def _column(method: str, kind: str, level: str, name: str) -> str:
        return (
            f"max(CASE WHEN {alias}.\"method\" = '{method}' "
            f'THEN {alias}."{kind}_{level}" END) AS "{name}"'
        )

    columns: List[str] = []
    for method in methods:
        method = _identifier(method, "method")
        for level in levels:
            for kind in ("score", "rank"):
                columns.append(_column(method, kind, level, f"{method}_{kind}_{level}"))
    primary = _identifier(primary_method, "primary method")
    for level in levels:
        for kind in ("score", "rank", "n"):
            columns.append(_column(primary, kind, level, f"primary_{kind}_{level}"))
    return ",\n".join(columns)


# Fonction de génération des colonnes normalisées min-max
def norm_columns_sql(metrics: Sequence[str], partition: Sequence[str]) -> str:
    """Generate the ``<metric>_norm`` min-max columns, per normalisation context.

    ``(x - min) / (max - min)`` over the window ``partition``; ``NULL`` when
    the context is constant (``max = min``) or the value missing.

    Args:
        metrics: Metric columns to normalise.
        partition: Columns defining the normalisation context.

    Returns:
        Comma-separated window expressions.

    Raises:
        ValueError: If a metric or partition column is not a plain identifier.

    Examples:
        >>> print(norm_columns_sql(["HHI"], ["year"]))
        ("HHI" - min("HHI") OVER (PARTITION BY "year")) / NULLIF(max("HHI") OVER (PARTITION BY "year") - min("HHI") OVER (PARTITION BY "year"), 0) AS "HHI_norm"
    """
    over = "OVER (PARTITION BY " + ", ".join(
        f'"{_identifier(column, "partition column")}"' for column in partition
    ) + ")"
    columns = []
    for metric in metrics:
        m = f'"{_identifier(metric, "metric")}"'
        columns.append(
            f"({m} - min({m}) {over}) / NULLIF(max({m}) {over} - min({m}) {over}, 0) "
            f'AS "{metric}_norm"'
        )
    return ",\n".join(columns)


# Fonction de génération du prédicat « partenaire individuel »
def individual_partner_sql(partners: Mapping[str, Any], column: str = "partner") -> str:
    """Generate the predicate selecting individual partner countries.

    Same rule as ``individual_partner_expr`` of the vulnerability metrics:
    explicit aggregate codes excluded and, when enabled, every code
    containing an underscore (``EXT_EU``, ``INT_EU27_2020``…).

    Args:
        partners: ``serving.PARTNERS`` (``AGGREGATE_CODES``,
            ``EXCLUDE_UNDERSCORE``, ``WORLD``, ``EXTRA_EU``).
        column: Partner column.

    Returns:
        A boolean SQL expression.

    Examples:
        >>> individual_partner_sql({"AGGREGATE_CODES": ["WORLD"], "EXCLUDE_UNDERSCORE": True})
        "(partner NOT IN ('WORLD') AND strpos(partner, '_') = 0)"
    """
    column = _identifier(column, "column")
    codes = list(partners.get("AGGREGATE_CODES") or [])
    codes += [partners[key] for key in ("WORLD", "EXTRA_EU") if partners.get(key)]
    codes = list(dict.fromkeys(codes))
    clauses = ["TRUE"]
    if codes:
        clauses = [f"{column} NOT IN (" + ", ".join(f"'{_literal(c)}'" for c in codes) + ")"]
    if partners.get("EXCLUDE_UNDERSCORE", True):
        clauses.append(f"strpos({column}, '_') = 0")
    return "(" + " AND ".join(clauses) + ")"


# Fonction de génération d'une table vide typée (source facultative absente)
def empty_table_sql(columns: Mapping[str, str]) -> str:
    """Generate an empty, typed subquery standing for a missing source table.

    Args:
        columns: Column name -> DuckDB type.

    Returns:
        ``(SELECT CAST(NULL AS <type>) AS "<column>", … WHERE false)``.

    Examples:
        >>> empty_table_sql({"code": "VARCHAR"})
        '(SELECT CAST(NULL AS VARCHAR) AS "code" WHERE false)'
    """
    select = ", ".join(
        f'CAST(NULL AS {kind}) AS "{name}"' for name, kind in columns.items()
    )
    return f"(SELECT {select} WHERE false)"


# Fonction de construction du contexte de rendu des gabarits
def build_context(
    tables: Mapping[str, SourceTable],
    existing: Set[str],
    params: Mapping[str, Any],
) -> Tuple[Dict[str, str], List[str]]:
    """Build the substitution context of the SQL templates.

    Args:
        tables: Template variable -> source table (see :func:`source_tables`).
        existing: Tables present in the session
            (``ServingCatalog.existing_tables``).
        params: ``serving`` parameters.

    Returns:
        Tuple ``(context, missing)``: variable -> SQL text, and the optional
        sources replaced by an empty table.
    """
    optional = params.get("OPTIONAL_SOURCES") or {}
    context: Dict[str, str] = {}
    missing: List[str] = []
    for name, table in tables.items():
        if table.key in existing or name not in optional:
            # Source présente, ou obligatoire (son absence fera échouer la requête)
            context[name] = table.qualified_name
            continue
        spec = optional[name]
        columns = REFERENCE_COLUMNS[_REFERENCE_OF[name]] if spec == "reference" else spec
        context[name] = empty_table_sql(columns)
        missing.append(name)

    partners = params.get("PARTNERS") or {}
    context.update(
        {
            "cell_filter": str(params["CELL_FILTER"]),
            "comext_filter": str(params["COMEXT_FILTER"]),
            "synthesis_pivot": synthesis_pivot_sql(
                params["METHODS"], params["LEVELS"], params["PRIMARY_METHOD"]
            ),
            "norm_columns": norm_columns_sql(
                params["NORM_METRICS"], params["NORM_PARTITION"]
            ),
            "individual_partner": individual_partner_sql(partners),
            "top_partners": str(int(params["TOP_PARTNERS"])),
            "world_code": _literal(partners.get("WORLD", "WORLD")),
            "extra_eu_code": _literal(partners.get("EXTRA_EU", "EXT_EU")),
        }
    )
    return context, missing


# Fonction de rendu d'un gabarit SQL
def render_sql(template: str, context: Mapping[str, str]) -> str:
    """Render a SQL template (``string.Template`` syntax).

    Args:
        template: Query with ``$name`` / ``${name}`` variables.
        context: Variable -> SQL text.

    Returns:
        The rendered query.

    Raises:
        KeyError: If the template uses an unknown variable.

    Examples:
        >>> render_sql("SELECT * FROM $t", {"t": "x.y"})
        'SELECT * FROM x.y'
    """
    return Template(template).substitute(context)


# Fonction de construction des tables à publier
def build_table_specs(
    params: Mapping[str, Any], context: Mapping[str, str]
) -> List[ServingTableSpec]:
    """Render every table of ``serving.TABLES`` into a :class:`ServingTableSpec`.

    Args:
        params: ``serving`` parameters.
        context: Rendering context (see :func:`build_context`).

    Returns:
        Table specifications, in configuration order.

    Raises:
        KeyError: If a template uses an unknown variable.
    """
    specs = []
    for name, table in params["TABLES"].items():
        specs.append(
            ServingTableSpec(
                name=_identifier(name, "table name"),
                sql=render_sql(table["SQL"], context),
                partitioned=bool(table.get("PARTITIONED", False)),
                sort_by=tuple(table.get("SORT_BY") or ()),
            )
        )
    return specs


# Fonction de résolution du mode de publication
def resolve_mode(
    params: Mapping[str, Any], runtime: Mapping[str, Any]
) -> Tuple[str, Optional[List[int]]]:
    """Resolve the publication mode and the years to refresh.

    ``by_year`` needs years: ``serving.YEARS``, else
    ``runtime.FORCE_SCOPE.PERIODS``; without any, the publication falls back
    to ``full``.

    Args:
        params: ``serving`` parameters (``MODE``, ``YEARS``).
        runtime: ``runtime`` parameters.

    Returns:
        Tuple ``(mode, years)``; ``years`` is ``None`` in ``full`` mode.

    Examples:
        >>> resolve_mode({"MODE": "by_year", "YEARS": [2023]}, {})
        ('by_year', [2023])
        >>> resolve_mode({"MODE": "by_year"}, {"FORCE_SCOPE": {"PERIODS": "2021, 2022"}})
        ('by_year', [2021, 2022])
        >>> resolve_mode({"MODE": "by_year"}, {})
        ('full', None)
    """
    mode = params.get("MODE", "full")
    if mode != "by_year":
        return "full", None
    years = params.get("YEARS")
    if not years:
        periods = ((runtime.get("FORCE_SCOPE") or {}).get("PERIODS") or "").split(",")
        years = [period.strip()[:4] for period in periods if period.strip()]
    if not years:
        logger.warning("Mode by_year sans année à republier : repli en mode full.")
        return "full", None
    return "by_year", sorted({int(year) for year in years})


# Fonction d'étape : publication de la couche de service (PS-29)
def publish_serving(
    sources: Mapping[str, SourceTable],
    serving: ServingCatalog,
    *,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    tracker: Any = None,
) -> Dict[str, Any]:
    """Publish the serving tables in one transaction and report the outcome.

    Args:
        sources: Template variable -> source table (see
            :func:`source_tables`).
        serving: Serving catalog handle (its sources attached ``READ_ONLY``).
        params: ``serving`` parameters (``config/serving.yaml``).
        runtime: ``runtime`` parameters (``NOMENCLATURES.HS``,
            ``FORCE_SCOPE``).
        tracker: Run tracker receiving the metrics; inert by default.

    Returns:
        ``StepResult``-like mapping: ``step``, ``mode``, ``tables`` (published
        names), ``rows`` (per table), ``missing_sources``, ``failures``
        (table or ``"publication"`` -> message) and ``metrics``. A failure
        does not raise: the whole publication was rolled back and the caller
        decides (non-zero exit code of the script).

    Examples:
        >>> result = publish_serving(tables, catalog, params=params,
        ...                          runtime=runtime)  # doctest: +SKIP
        >>> result["rows"]["cell_scores"]  # doctest: +SKIP
        312480
    """
    from macroforecast.tracking import NULL_TRACKER
    from kedro_pipeline.io.serving import ServingPublicationError

    tracker = NULL_TRACKER if tracker is None else tracker
    mode, years = resolve_mode(params, runtime)
    result: Dict[str, Any] = {
        "step": "serving",
        "mode": mode,
        "tables": [],
        "rows": {},
        "missing_sources": [],
        "failures": {},
        "metrics": {},
    }

    try:
        with serving.connect() as conn:
            context, missing = build_context(sources, serving.existing_tables(conn), params)
            for name in missing:
                logger.warning(
                    f"Source facultative absente '{name}' ({sources[name].key}) : "
                    "remplacée par une table vide."
                )
            result["missing_sources"] = missing
            specs = build_table_specs(params, context)
            stats = serving.publish(
                specs,
                mode,
                years=years,
                prelude=nomenclature_macros_sql(runtime["NOMENCLATURES"]["HS"]),
                conn=conn,
            )
    except ServingPublicationError as exc:
        logger.error(f"Publication annulée : {exc}")
        result["failures"][exc.table or "publication"] = str(exc)[:1000]
        stats = {}
    except Exception as exc:  # connexion, rendu : rien n'a été écrit
        logger.error(f"Publication impossible : {exc}")
        result["failures"]["publication"] = str(exc)[:1000]
        stats = {}

    # Rapport et métriques (y compris en échec : le run porte son rapport)
    metrics: Dict[str, float] = {}
    for name, table_stats in stats.items():
        result["tables"].append(name)
        result["rows"][name] = table_stats.rows
        metrics.update(table_stats.to_metrics(name))
    # 1 si toutes les tables ont été recréées (mode full demandé ou repli), 0 sinon
    metrics["serving/mode_full"] = float(
        all(table_stats.mode == "full" for table_stats in stats.values())
    )
    metrics["serving/missing_sources"] = float(len(result["missing_sources"]))
    metrics["serving/n_failures"] = float(len(result["failures"]))
    result["metrics"] = metrics
    tracker.log_metrics(metrics)
    return result
