"""Construction de l'archive importable du tableau de bord Superset « Vulnérabilités ».

Le dossier versionné (``superset/vulnerabilites/`` : ``metadata.yaml``,
``databases/``, ``datasets/``, ``charts/``, ``dashboards/``) est le contenu d'un
export Superset décompressé. Ce script le réempaquette en une archive zip à
racine unique, le format attendu par ``POST /api/v1/dashboard/import/`` et par
``superset import-dashboards -p <archive>``, en appliquant au passage les
réglages propres à l'environnement cible :

- ``--assets`` : archive de type « assets », pour ``POST /api/v1/assets/import/``.
  L'import d'un tableau de bord ne remplace que le tableau de bord : les
  datasets et graphiques déjà présents (même uuid) sont conservés tels quels,
  si bien qu'une modification de leur SQL ou de leurs colonnes n'est pas
  appliquée. L'import « assets » remplace tout (connexion, datasets,
  graphiques, tableau de bord) : c'est la forme à utiliser pour mettre à jour
  un Superset où le tableau de bord existe déjà ;

- ``--schema`` : schéma du catalogue ``serving`` visé par les datasets. Les
  exports versionnés visent le schéma de production ``dashboard`` ; la recette
  utilise ``demo_dashboard``. La substitution porte sur la clé ``schema`` des
  datasets et sur le SQL des datasets virtuels (``serving.dashboard`` devient
  ``serving.<schéma>``) ;
- ``--default-year`` : valeur par défaut des filtres natifs portant sur la
  colonne ``year``. Un filtre natif n'a pas de valeur par défaut dynamique, et
  sans année fixée les classements de toutes les années se mélangent : la
  valeur doit être explicite, et correspondre à la dernière année classée de
  l'environnement ;
- ``--top-n`` : nombre d'unités de meilleur rang tracées par les courbes quand
  leur menu est vide, porté par les paramètres de gabarit (``template_params``)
  des datasets virtuels qui le déclarent ;
- ``--menu-defaults`` : valeurs par défaut des menus « Produit à détailler »,
  « Produits des courbes » et « Pays des courbes » (fichier JSON : identifiant
  du filtre → liste de valeurs). Un filtre natif n'a pas de valeur par défaut
  calculée : pour que chaque menu contienne, à l'ouverture, ce que trace le
  graphique qu'il pilote, ses valeurs sont calculées sur les données de
  l'environnement (requêtes de :func:`menu_queries`, exécutées par le Job
  d'import via l'API de données de Superset) puis écrites dans l'archive.

L'archive est validée avant écriture : fichiers YAML lisibles, références
croisées résolues (base de chaque dataset, dataset de chaque graphique et de
chaque filtre natif, graphiques de la mise en page), absence de mot de passe
dans les connexions, et absence dans le SQL des datasets virtuels des
constructions DuckDB ``PIVOT`` / ``UNPIVOT``, que l'analyseur SQL de Superset
rejette (le graphique échoue alors à l'affichage, pas à l'import).

Examples:
    Archive de recette (schéma de démonstration, dernière année classée 2021)::

        $ python tools/superset_bundle.py superset/vulnerabilites \\
            -o /tmp/vulnerabilites.zip --schema demo_dashboard --default-year 2021

    Import par l'API (jeton et jeton CSRF obtenus par ``/api/v1/security/``)::

        $ curl -X POST "$SUPERSET/api/v1/dashboard/import/" \\
            -H "Authorization: Bearer $TOKEN" -H "X-CSRFToken: $CSRF" \\
            -F formData=@/tmp/vulnerabilites.zip -F overwrite=true

    Mise à jour d'un Superset où le tableau de bord existe déjà::

        $ python tools/superset_bundle.py superset/vulnerabilites \\
            -o /tmp/vulnerabilites.zip --assets --default-year 2023
        $ curl -X POST "$SUPERSET/api/v1/assets/import/" \\
            -H "Authorization: Bearer $TOKEN" -H "X-CSRFToken: $CSRF" \\
            -F bundle=@/tmp/vulnerabilites.zip

    Valeurs des menus calculées ailleurs (identifiant du filtre → valeurs)::

        $ python tools/superset_bundle.py superset/vulnerabilites \\
            -o /tmp/vulnerabilites.zip --assets --menu-defaults /tmp/menus.json

    Import dans le conteneur Superset::

        $ superset import-dashboards -p /tmp/vulnerabilites.zip -u admin
"""

from __future__ import annotations

import argparse
import copy
import io
import json
import re
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, NamedTuple, Optional, Sequence

import yaml

# Schéma de production visé par les exports versionnés
SOURCE_SCHEMA = "dashboard"
# Alias du catalogue DuckLake attaché par la connexion Superset
CATALOG_ALIAS = "serving"
# Sous-dossiers d'un export Superset
ASSET_DIRECTORIES = ("databases", "datasets", "charts", "dashboards")
# Colonne des filtres natifs d'année
YEAR_COLUMN = "year"
# Paramètre de gabarit du nombre d'unités tracées par défaut
TOP_N_PARAMETER = "top_n"
# Clés interdites dans une connexion exportée
SECRET_KEYS = ("password", "masked_encrypted_extra", "encrypted_extra")
# Type d'export versionné, et type d'une archive pour l'import « assets »
DASHBOARD_TYPE = "Dashboard"
ASSETS_TYPE = "assets"

# Table interrogée pour calculer les menus préremplis
MENU_TABLE = "cell_scores"
# Menus préremplis : chaque menu reçoit les unités que trace, menu vide, le graphique
# qu'il pilote. Règle des gabarits : unités de meilleur rang consensus à l'année, au flux
# et au pays (ou produit) par défaut de l'onglet, ex æquo départagés par le code.
#   - « Produit à détailler » : produit de rang 1 du pays au sein du niveau de
#     nomenclature (diagramme des partenaires, qui ne lit que les codes en vigueur) ;
#   - « Produits des courbes » : top N produits du pays au sein du niveau ;
#   - « Pays des courbes » : top N pays déclarants du produit.
# Filtres de référence : leur valeur par défaut (colonne de leur cible) restreint la
# requête. ``limit`` None vaut ``top_n`` (paramètre de gabarit des courbes).
MENU_SPECS: Dict[str, Dict[str, Any]] = {
    "NATIVE_FILTER-PAYS-DETAIL": {
        "rank": "primary_rank_by_reporter_level",
        "tiebreak": "product",
        "references": (
            "NATIVE_FILTER-PAYS-YEAR",
            "NATIVE_FILTER-PAYS-FLOW",
            "NATIVE_FILTER-PAYS-FOCUS",
            "NATIVE_FILTER-PAYS-LEVEL",
        ),
        "fixed": {"in_force": [True]},
        "limit": 1,
    },
    "NATIVE_FILTER-PAYS-EXTRA": {
        "rank": "primary_rank_by_reporter_level",
        "tiebreak": "product",
        "references": (
            "NATIVE_FILTER-PAYS-YEAR",
            "NATIVE_FILTER-PAYS-FLOW",
            "NATIVE_FILTER-PAYS-FOCUS",
            "NATIVE_FILTER-PAYS-LEVEL",
            "NATIVE_FILTER-PAYS-INFORCE",
        ),
        "fixed": {},
        "limit": None,
    },
    "NATIVE_FILTER-PRODUIT-EXTRA": {
        "rank": "primary_rank_by_product",
        "tiebreak": "reporter",
        "references": (
            "NATIVE_FILTER-PRODUIT-YEAR",
            "NATIVE_FILTER-PRODUIT-FLOW",
            "NATIVE_FILTER-PRODUIT-FOCUS",
            "NATIVE_FILTER-PRODUIT-INFORCE",
        ),
        "fixed": {},
        "limit": None,
    },
}
# Nombre d'unités des courbes, à défaut de paramètre de gabarit
DEFAULT_TOP_N = 5

# Identifiant de schéma admis (substitué dans du SQL)
_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Constructions DuckDB rejetées par l'analyseur SQL de Superset (sqlglot)
_UNSUPPORTED_SQL = re.compile(r"\b(UNPIVOT|PIVOT)\b", re.IGNORECASE)


# Exception levée lorsqu'un export est incohérent
class BundleError(ValueError):
    """Raised when the export folder is inconsistent or cannot be bundled."""


# Fonction de substitution du schéma dans un texte (clé schema ou SQL)
def substitute_schema(text: Optional[str], schema: str) -> Optional[str]:
    """Replace the production serving schema by ``schema`` in a text.

    Only the qualified form ``serving.dashboard`` is replaced (as a whole
    word), so a column or table merely containing ``dashboard`` is untouched.

    Args:
        text: Value of a ``schema`` key or SQL of a virtual dataset; ``None``
            is returned unchanged.
        schema: Target schema of the ``serving`` catalog.

    Returns:
        The text with ``serving.dashboard`` replaced by ``serving.<schema>``.

    Raises:
        BundleError: If ``schema`` is not a plain identifier.

    Examples:
        >>> substitute_schema("SELECT * FROM serving.dashboard.cell_scores", "demo_dashboard")
        'SELECT * FROM serving.demo_dashboard.cell_scores'
        >>> substitute_schema("serving.dashboards_old", "demo_dashboard")
        'serving.dashboards_old'
    """
    if not _SCHEMA_NAME.match(schema):
        raise BundleError(f"Invalid schema '{schema}': expected a plain identifier")
    if text is None:
        return None
    pattern = re.compile(rf"\b{CATALOG_ALIAS}\.{SOURCE_SCHEMA}\b")
    return pattern.sub(f"{CATALOG_ALIAS}.{schema}", text)


# Fonction de lecture des fichiers YAML d'un export
def load_assets(source: Path) -> Dict[str, Dict[str, Any]]:
    """Read every YAML file of an export folder.

    Args:
        source: Export folder (``metadata.yaml`` and the asset directories).

    Returns:
        Relative POSIX path -> parsed content, ``metadata.yaml`` included.

    Raises:
        BundleError: If ``metadata.yaml`` is missing or a file is not a YAML
            mapping.
    """
    if not (source / "metadata.yaml").is_file():
        raise BundleError(f"{source} has no metadata.yaml: not a Superset export folder")
    assets: Dict[str, Dict[str, Any]] = {}
    for path in sorted(source.rglob("*.yaml")):
        relative = path.relative_to(source).as_posix()
        if relative != "metadata.yaml" and relative.split("/")[0] not in ASSET_DIRECTORIES:
            continue
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(content, dict):
            raise BundleError(f"{relative}: expected a YAML mapping")
        assets[relative] = content
    return assets


# Fonction de parcours des fichiers d'une catégorie
def _of_kind(assets: Mapping[str, Dict[str, Any]], kind: str) -> Iterator[Dict[str, Any]]:
    """Yield the assets stored under ``<kind>/``."""
    for path, content in assets.items():
        if path.startswith(f"{kind}/"):
            yield content


# Fonction de lecture d'un champ JSON exporté en texte ou en objet
def _as_mapping(value: Any) -> Dict[str, Any]:
    """Return ``value`` as a dict (``None``, JSON text or mapping)."""
    if value in (None, ""):
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


# Fonction d'application des réglages d'environnement
def configure_assets(
    assets: Mapping[str, Dict[str, Any]],
    *,
    schema: Optional[str] = None,
    default_year: Optional[int] = None,
    top_n: Optional[int] = None,
    menu_defaults: Optional[Mapping[str, Sequence[Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Apply the target-environment settings to a copy of the assets.

    Args:
        assets: Parsed export (see :func:`load_assets`).
        schema: Target schema of the ``serving`` catalog; unchanged when
            ``None``.
        default_year: Default value of the native filters on ``year``;
            unchanged when ``None``.
        top_n: Number of best-ranked units drawn by the curves when their menu
            is empty, written in the ``template_params`` of the datasets that
            declare it; unchanged when ``None``.
        menu_defaults: Default values of the prefilled menus (filter
            identifier -> values, see :func:`apply_menu_defaults`); unchanged
            when ``None``.

    Returns:
        A configured deep copy of ``assets``.

    Raises:
        BundleError: If ``schema`` is invalid, ``top_n`` is not positive or a
            menu default targets an unknown or single-select filter wrongly.

    Examples:
        >>> assets = {"datasets/serving/t.yaml": {"schema": "serving.dashboard", "sql": None}}
        >>> configure_assets(assets, schema="demo_dashboard")["datasets/serving/t.yaml"]["schema"]
        'serving.demo_dashboard'
    """
    if top_n is not None and int(top_n) < 1:
        raise BundleError(f"top_n must be a positive integer, got {top_n}")
    configured = copy.deepcopy(dict(assets))

    for dataset in _of_kind(configured, "datasets"):
        # Schéma de la table physique et SQL des datasets virtuels
        if schema is not None:
            dataset["schema"] = substitute_schema(dataset.get("schema"), schema)
            dataset["sql"] = substitute_schema(dataset.get("sql"), schema)
        # Nombre d'unités par défaut, seulement là où il est déclaré
        if top_n is not None:
            params = _as_mapping(dataset.get("template_params"))
            if TOP_N_PARAMETER in params:
                params[TOP_N_PARAMETER] = int(top_n)
                # Objet YAML, seule forme admise par la validation de l'import
                dataset["template_params"] = params

    if default_year is not None:
        for dashboard in _of_kind(configured, "dashboards"):
            metadata = dashboard.get("metadata") or {}
            for native_filter in metadata.get("native_filter_configuration") or []:
                columns = {
                    (target.get("column") or {}).get("name")
                    for target in native_filter.get("targets") or []
                }
                if YEAR_COLUMN not in columns:
                    continue
                # État par défaut ET filtre appliqué au premier affichage
                native_filter["defaultDataMask"] = {
                    **(native_filter.get("defaultDataMask") or {}),
                    "filterState": {"value": [int(default_year)]},
                    "extraFormData": {
                        "filters": [{"col": YEAR_COLUMN, "op": "IN", "val": [int(default_year)]}]
                    },
                }
    if menu_defaults is not None:
        apply_menu_defaults(configured, menu_defaults)
    return configured


# Requête de calcul d'un menu prérempli (tuple nommé : le module est aussi chargé par
# son chemin, sans entrée dans sys.modules, ce que les dataclasses n'admettent pas)
class MenuQuery(NamedTuple):
    """Ranking query giving the default values of one prefilled menu.

    Attributes:
        filter_id: Native filter receiving the values.
        column: Column of the menu values (target of the filter).
        rank: Consensus rank column (1 = most vulnerable).
        tiebreak: Code column ordering tied ranks, as the dashboard templates do.
        filters: Column -> accepted values (default values of the reference filters).
        limit: Number of units kept.
        expression: SQL expression of ``column`` when it is a calculated column of
            the Superset dataset (absent from the table); ``None`` for a table column.
    """

    filter_id: str
    column: str
    rank: str
    tiebreak: str
    # Valeur par défaut partagée, jamais modifiée
    filters: Mapping[str, List[Any]] = {}
    limit: int = DEFAULT_TOP_N
    expression: Optional[str] = None


# Fonction de lecture des filtres natifs du tableau de bord
def _native_filters(assets: Mapping[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Return the native filters of the exported dashboards, by identifier."""
    filters: Dict[str, Dict[str, Any]] = {}
    for dashboard in _of_kind(assets, "dashboards"):
        metadata = dashboard.get("metadata") or {}
        for native_filter in metadata.get("native_filter_configuration") or []:
            filters[native_filter.get("id")] = native_filter
    return filters


# Fonction de lecture de la colonne ciblée par un filtre
def _target_column(native_filter: Mapping[str, Any]) -> str:
    """Return the column targeted by a native filter (its first target)."""
    targets = native_filter.get("targets") or [{}]
    column = (targets[0].get("column") or {}).get("name")
    if not column:
        raise BundleError(f"filter {native_filter.get('id')} has no target column")
    return column


# Fonction de lecture du paramètre top_n des courbes
def _top_n(assets: Mapping[str, Dict[str, Any]]) -> int:
    """Return the ``top_n`` template parameter of the virtual datasets (5 by default)."""
    for dataset in _of_kind(assets, "datasets"):
        params = _as_mapping(dataset.get("template_params"))
        if TOP_N_PARAMETER in params:
            return int(params[TOP_N_PARAMETER])
    return DEFAULT_TOP_N


# Fonction de construction des requêtes des menus préremplis
def menu_queries(assets: Mapping[str, Dict[str, Any]]) -> List[MenuQuery]:
    """Build the ranking queries of the prefilled menus from the dashboard defaults.

    Each menu must open with the units that the chart it drives draws when the
    menu is empty: the rank-1 product of the partner bars, the ``top_n`` units of
    the curves. The queries reproduce that rule from the default values of the
    other filters of the same tab, read in the (configured) export, so the
    default year of the environment is taken into account.

    Args:
        assets: Parsed export, after :func:`configure_assets` (schema, default
            year and ``top_n`` of the target environment).

    Returns:
        One query per prefilled menu found in the dashboard.

    Raises:
        BundleError: If a reference filter has no default value.

    Examples:
        >>> dashboard = {"metadata": {"native_filter_configuration": [
        ...     {"id": "NATIVE_FILTER-PRODUIT-EXTRA", "targets": [{"column": {"name": "reporter_display"}}]},
        ...     *[{"id": f"NATIVE_FILTER-PRODUIT-{name}", "targets": [{"column": {"name": column}}],
        ...        "defaultDataMask": {"filterState": {"value": [value]}}}
        ...       for name, column, value in [("YEAR", "year", 2021), ("FLOW", "flow_label", "Import"),
        ...                                   ("FOCUS", "product_display", "850760"), ("INFORCE", "in_force", True)]],
        ... ]}}
        >>> query, = menu_queries({"dashboards/d.yaml": dashboard})
        >>> query.column, query.limit, query.filters["year"]
        ('reporter_display', 5, [2021])
    """
    filters = _native_filters(assets)
    top_n = _top_n(assets)
    # Colonnes calculées du dataset interrogé (cible d'un menu absente de la table)
    calculated = {
        column.get("column_name"): column.get("expression")
        for dataset in _of_kind(assets, "datasets")
        if dataset.get("table_name") == MENU_TABLE
        for column in dataset.get("columns") or []
        if column.get("expression")
    }
    queries: List[MenuQuery] = []
    for filter_id, spec in MENU_SPECS.items():
        if filter_id not in filters:
            continue
        restriction: Dict[str, List[Any]] = {}
        for reference_id in spec["references"]:
            reference = filters.get(reference_id)
            value = ((reference or {}).get("defaultDataMask") or {}).get("filterState", {}).get("value")
            if not value:
                raise BundleError(
                    f"menu {filter_id}: reference filter {reference_id} has no default value"
                )
            restriction[_target_column(reference)] = list(value)
        restriction.update({column: list(values) for column, values in spec["fixed"].items()})
        column = _target_column(filters[filter_id])
        queries.append(
            MenuQuery(
                filter_id=filter_id,
                column=column,
                rank=spec["rank"],
                tiebreak=spec["tiebreak"],
                filters=restriction,
                limit=spec["limit"] or top_n,
                expression=calculated.get(column),
            )
        )
    return queries


# Fonction d'écriture d'un littéral SQL
def _sql_literal(value: Any) -> str:
    """Return ``value`` as a SQL literal (quotes doubled in strings)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


# Fonction de traduction d'une requête de menu en SQL
def menu_query_sql(query: MenuQuery, table: str) -> str:
    """Return the SQL of a menu query, as the curve templates rank their units.

    Used by the tests (against the synthetic world) and for a manual check of
    the defaults of an environment.

    Args:
        query: Menu query (see :func:`menu_queries`).
        table: Qualified table, e.g. ``serving.dashboard.cell_scores``.

    Returns:
        A ``SELECT`` of the menu column, best rank first.

    Examples:
        >>> print(menu_query_sql(MenuQuery("F", "reporter_display", "primary_rank_by_product",
        ...     "reporter", {"flow_label": ["Import"]}, 2), "serving.dashboard.cell_scores"))
        SELECT reporter_display FROM serving.dashboard.cell_scores
        WHERE flow_label IN ('Import') AND primary_rank_by_product IS NOT NULL
        ORDER BY primary_rank_by_product, reporter
        LIMIT 2
    """
    conditions = [
        f"{column} IN ({', '.join(_sql_literal(value) for value in values)})"
        for column, values in query.filters.items()
    ]
    conditions.append(f"{query.rank} IS NOT NULL")
    # Colonne calculée de Superset : son expression, sous le nom de la colonne
    selected = f"{query.expression} AS {query.column}" if query.expression else query.column
    return (
        f"SELECT {selected} FROM {table}\n"
        f"WHERE {' AND '.join(conditions)}\n"
        f"ORDER BY {query.rank}, {query.tiebreak}\n"
        f"LIMIT {int(query.limit)}"
    )


# Fonction de construction de la requête de données Superset d'un menu
def chart_data_payload(query: MenuQuery, datasource_id: int) -> Dict[str, Any]:
    """Return the ``POST /api/v1/chart/data`` body of a menu query.

    The query runs on the physical dataset ``cell_scores`` of the target
    Superset, through its own connection: no catalog credential is needed by the
    caller. One row per unit, ordered by rank then by code, as in
    :func:`menu_query_sql`.

    Args:
        query: Menu query.
        datasource_id: Identifier of the ``cell_scores`` dataset in Superset.

    Returns:
        The JSON body of the request.

    Examples:
        >>> body = chart_data_payload(MenuQuery("F", "product_display", "r", "product"), 1)
        >>> body["queries"][0]["columns"], body["queries"][0]["row_limit"]
        (['product_display', 'product'], 5)
    """
    rank = {"expressionType": "SQL", "sqlExpression": f"MIN({query.rank})", "label": "rank"}
    return {
        "datasource": {"id": int(datasource_id), "type": "table"},
        "force": True,
        "result_type": "full",
        "result_format": "json",
        "queries": [
            {
                "columns": [query.column, query.tiebreak],
                "metrics": [rank],
                "filters": [
                    {"col": column, "op": "IN", "val": list(values)}
                    for column, values in query.filters.items()
                ],
                "extras": {"where": f"{query.rank} IS NOT NULL", "having": ""},
                "orderby": [[rank, True], [query.tiebreak, True]],
                "row_limit": int(query.limit),
            }
        ],
    }


# Fonction de lecture des valeurs d'un menu dans une réponse de données
def menu_values(rows: Sequence[Mapping[str, Any]], query: MenuQuery) -> List[Any]:
    """Return the menu values of query result rows, in order, without duplicates.

    Args:
        rows: Rows of the result (``result[0]["data"]`` of the data API, or
            records of :func:`menu_query_sql`).
        query: Menu query the rows answer.

    Returns:
        At most ``query.limit`` values of ``query.column``.

    Examples:
        >>> menu_values([{"c": "a"}, {"c": "a"}, {"c": "b"}], MenuQuery("F", "c", "r", "t", limit=5))
        ['a', 'b']
    """
    values: List[Any] = []
    for row in rows:
        value = row.get(query.column)
        if value is not None and value not in values:
            values.append(value)
    return values[: int(query.limit)]


# Fonction de lecture des valeurs par défaut actuelles des menus
def current_menu_defaults(assets: Mapping[str, Dict[str, Any]]) -> Dict[str, List[Any]]:
    """Return the default values of the prefilled menus, as exported.

    Args:
        assets: Parsed export.

    Returns:
        Filter identifier -> default values (empty list when none).
    """
    filters = _native_filters(assets)
    return {
        filter_id: list(
            ((filters[filter_id].get("defaultDataMask") or {}).get("filterState") or {}).get("value")
            or []
        )
        for filter_id in MENU_SPECS
        if filter_id in filters
    }


# Fonction d'écriture des valeurs par défaut des menus
def apply_menu_defaults(
    assets: Dict[str, Dict[str, Any]], defaults: Mapping[str, Sequence[Any]]
) -> None:
    """Write default values into native filters, in place.

    Both the displayed state (``filterState``) and the filter applied at the
    first display (``extraFormData``) are written: one without the other makes
    the menu disagree with the charts. An empty list leaves the menu empty, so
    the charts fall back to their own rule (rank 1, ``top_n`` units).

    Args:
        assets: Parsed export, modified in place.
        defaults: Filter identifier -> values.

    Raises:
        BundleError: If a filter is unknown, or a single-select filter receives
            several values.

    Examples:
        >>> assets = {"dashboards/d.yaml": {"metadata": {"native_filter_configuration": [
        ...     {"id": "F", "targets": [{"column": {"name": "c"}}], "controlValues": {"multiSelect": True}}]}}}
        >>> apply_menu_defaults(assets, {"F": ["a", "b"]})
        >>> mask = assets["dashboards/d.yaml"]["metadata"]["native_filter_configuration"][0]["defaultDataMask"]
        >>> mask["filterState"], mask["extraFormData"]["filters"][0]["val"]
        ({'value': ['a', 'b']}, ['a', 'b'])
    """
    filters = _native_filters(assets)
    for filter_id, values in defaults.items():
        native_filter = filters.get(filter_id)
        if native_filter is None:
            raise BundleError(f"menu defaults: unknown native filter {filter_id}")
        values = list(values)
        multiple = (native_filter.get("controlValues") or {}).get("multiSelect", True)
        if not multiple and len(values) > 1:
            raise BundleError(f"menu defaults: {filter_id} is single-select, got {len(values)} values")
        mask = dict(native_filter.get("defaultDataMask") or {})
        if values:
            mask["filterState"] = {"value": values}
            mask["extraFormData"] = {
                "filters": [{"col": _target_column(native_filter), "op": "IN", "val": values}]
            }
        else:
            mask["filterState"] = {"value": None}
            mask["extraFormData"] = {}
        native_filter["defaultDataMask"] = mask


# Fonction de recherche récursive d'une clé
def _find_keys(value: Any, keys: tuple) -> List[str]:
    """Return the keys of ``keys`` present (non-empty) anywhere in ``value``."""
    found: List[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in keys and item not in (None, "", {}):
                found.append(key)
            found += _find_keys(item, keys)
    elif isinstance(value, list):
        for item in value:
            found += _find_keys(item, keys)
    return found


# Fonction de validation des références croisées
def validate_assets(assets: Mapping[str, Dict[str, Any]]) -> None:
    """Check that an export is self-contained and carries no secret.

    Args:
        assets: Parsed (possibly configured) export.

    Raises:
        BundleError: Listing every problem found: metadata type, unresolved
            database, dataset or chart reference, a secret in a database, or
            SQL that the Superset parser rejects.
    """
    problems: List[str] = []
    if assets.get("metadata.yaml", {}).get("type") != DASHBOARD_TYPE:
        problems.append(f"metadata.yaml: type must be '{DASHBOARD_TYPE}'")

    databases = {db.get("uuid") for db in _of_kind(assets, "databases")}
    datasets = {ds.get("uuid") for ds in _of_kind(assets, "datasets")}
    charts = {chart.get("uuid") for chart in _of_kind(assets, "charts")}

    for path, content in assets.items():
        if path.startswith("databases/"):
            for key in _find_keys(content, SECRET_KEYS):
                problems.append(f"{path}: secret key '{key}' must not be exported")
        elif path.startswith("datasets/"):
            if content.get("database_uuid") not in databases:
                problems.append(f"{path}: unknown database {content.get('database_uuid')}")
            unsupported = _UNSUPPORTED_SQL.search(content.get("sql") or "")
            if unsupported:
                problems.append(
                    f"{path}: {unsupported.group(1).upper()} is rejected by the Superset SQL parser"
                )
        elif path.startswith("charts/") and content.get("dataset_uuid") not in datasets:
            problems.append(f"{path}: unknown dataset {content.get('dataset_uuid')}")
        elif path.startswith("dashboards/"):
            for component in (content.get("position") or {}).values():
                if isinstance(component, dict) and component.get("type") == "CHART":
                    uuid = (component.get("meta") or {}).get("uuid")
                    if uuid not in charts:
                        problems.append(f"{path}: layout chart {uuid} not exported")
            metadata = content.get("metadata") or {}
            for native_filter in metadata.get("native_filter_configuration") or []:
                for target in native_filter.get("targets") or []:
                    uuid = target.get("datasetUuid")
                    if uuid is not None and uuid not in datasets:
                        problems.append(
                            f"{path}: filter {native_filter.get('id')} targets unknown dataset {uuid}"
                        )
    if not any(path.startswith("dashboards/") for path in assets):
        problems.append("no dashboard in the export")
    if problems:
        raise BundleError("Invalid Superset export:\n- " + "\n- ".join(problems))


# Fonction de construction de l'archive en mémoire
def build_bundle(
    source: Path,
    *,
    schema: Optional[str] = None,
    default_year: Optional[int] = None,
    top_n: Optional[int] = None,
    menu_defaults: Optional[Mapping[str, Sequence[Any]]] = None,
    root: Optional[str] = None,
    assets_import: bool = False,
) -> bytes:
    """Build the importable zip archive of an export folder.

    Args:
        source: Export folder.
        schema: Target schema of the ``serving`` catalog (see
            :func:`configure_assets`).
        default_year: Default year of the native filters.
        top_n: Default number of units drawn by the curves.
        menu_defaults: Default values of the prefilled menus.
        root: Name of the single top-level folder of the archive; the name of
            ``source`` by default.
        assets_import: Declare the archive as an ``assets`` bundle, for
            ``POST /api/v1/assets/import/``, which overwrites the existing
            datasets and charts too (a dashboard import keeps them).

    Returns:
        The zip archive, as bytes.

    Raises:
        BundleError: If the export is invalid (see :func:`validate_assets`).
    """
    assets = configure_assets(
        load_assets(source),
        schema=schema,
        default_year=default_year,
        top_n=top_n,
        menu_defaults=menu_defaults,
    )
    validate_assets(assets)
    if assets_import:
        assets["metadata.yaml"] = {**assets["metadata.yaml"], "type": ASSETS_TYPE}
    prefix = root or source.resolve().name
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative, content in assets.items():
            archive.writestr(
                f"{prefix}/{relative}",
                yaml.safe_dump(content, sort_keys=False, allow_unicode=True),
            )
    return buffer.getvalue()


# Fonction d'analyse de la ligne de commande
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse the command line.

    Args:
        argv: Arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Build the importable zip archive of a Superset export folder."
    )
    parser.add_argument("source", type=Path, help="Export folder (e.g. superset/vulnerabilites)")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Zip archive to write")
    parser.add_argument(
        "--schema",
        help=f"Schema of the '{CATALOG_ALIAS}' catalog (default: {SOURCE_SCHEMA}, as exported)",
    )
    parser.add_argument("--default-year", type=int, help="Default value of the year filters")
    parser.add_argument("--top-n", type=int, help="Default number of units drawn by the curves")
    parser.add_argument(
        "--menu-defaults",
        type=Path,
        help="JSON file: native filter id -> default values of the prefilled menus",
    )
    parser.add_argument(
        "--assets",
        action="store_true",
        help="Build an 'assets' bundle (POST /api/v1/assets/import/), which also overwrites "
        "existing datasets and charts",
    )
    return parser.parse_args(argv)


# Point d'entrée
def main(argv: Optional[List[str]] = None) -> int:
    """Build the archive; return the process exit code.

    Args:
        argv: Command-line arguments.

    Returns:
        ``0`` on success, ``1`` if the export is invalid.
    """
    args = parse_args(argv)
    try:
        # Valeurs des menus préremplis, lues dans un fichier JSON
        menu_defaults = None
        if args.menu_defaults is not None:
            menu_defaults = json.loads(args.menu_defaults.read_text(encoding="utf-8"))
            if not isinstance(menu_defaults, dict):
                raise BundleError(f"{args.menu_defaults}: expected a JSON object")
        payload = build_bundle(
            args.source,
            schema=args.schema,
            default_year=args.default_year,
            top_n=args.top_n,
            menu_defaults=menu_defaults,
            assets_import=args.assets,
        )
    except BundleError as exc:
        print(exc, file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(payload)
    print(f"{args.output} ({len(payload)} octets)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
