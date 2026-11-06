"""Tests de l'outil d'empaquetage du tableau de bord Superset (``tools/superset_bundle.py``)."""

from __future__ import annotations

import importlib.util
import io
import re
import zipfile
from pathlib import Path

import duckdb
import jinja2
import pandas as pd
import pytest
import yaml

from kedro_pipeline.steps.serving import publish_serving

from conftest import REPO_ROOT

# Chargement du script (dossier tools/ hors paquet)
_SPEC = importlib.util.spec_from_file_location("superset_bundle", REPO_ROOT / "tools" / "superset_bundle.py")
superset_bundle = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(superset_bundle)

DB_UUID = "00000000-0000-0000-0000-00000000000d"
PHYSICAL_UUID = "00000000-0000-0000-0000-000000000001"
VIRTUAL_UUID = "00000000-0000-0000-0000-000000000002"
CHART_UUID = "00000000-0000-0000-0000-0000000000c1"


def _write(root: Path, relative: str, content: dict) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(content, sort_keys=False, allow_unicode=True), encoding="utf-8")


@pytest.fixture
def export(tmp_path: Path) -> Path:
    """Export minimal : une connexion, un dataset physique, un virtuel, un graphique, un tableau."""
    root = tmp_path / "vulnerabilites"
    _write(root, "metadata.yaml", {"version": "1.0.0", "type": "Dashboard"})
    _write(root, "databases/serving.yaml", {
        "database_name": "serving", "sqlalchemy_uri": "duckdb:///:memory:", "uuid": DB_UUID,
    })
    _write(root, "datasets/serving/cell_scores.yaml", {
        "table_name": "cell_scores", "schema": "serving.dashboard", "sql": None,
        "template_params": None, "uuid": PHYSICAL_UUID, "database_uuid": DB_UUID,
    })
    _write(root, "datasets/serving/series.yaml", {
        "table_name": "series", "schema": "serving.dashboard",
        "sql": "SELECT * FROM serving.dashboard.cell_scores JOIN serving.dashboard_archive.x USING (k)"
               " LIMIT {{ top_n }}",
        "template_params": {"top_n": 5}, "uuid": VIRTUAL_UUID, "database_uuid": DB_UUID,
    })
    _write(root, "charts/table.yaml", {"slice_name": "Table", "uuid": CHART_UUID, "dataset_uuid": PHYSICAL_UUID})
    _write(root, "dashboards/main.yaml", {
        "dashboard_title": "Vulnérabilités",
        "position": {"CHART-1": {"type": "CHART", "id": "CHART-1", "meta": {"chartId": 7, "uuid": CHART_UUID}}},
        "metadata": {"native_filter_configuration": [
            {"id": "NATIVE_FILTER-year", "targets": [{"column": {"name": "year"}, "datasetUuid": PHYSICAL_UUID}],
             "defaultDataMask": {"filterState": {"value": [2020]}}},
            {"id": "NATIVE_FILTER-flow", "targets": [{"column": {"name": "flow_label"}, "datasetUuid": PHYSICAL_UUID}],
             "defaultDataMask": {"filterState": {"value": ["Import"]}}},
        ]},
    })
    return root


def _read_bundle(payload: bytes) -> dict:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        return {name: yaml.safe_load(archive.read(name)) for name in archive.namelist()}


def test_bundle_structure_single_root(export: Path) -> None:
    files = _read_bundle(superset_bundle.build_bundle(export))
    assert {name.split("/")[0] for name in files} == {"vulnerabilites"}
    assert files["vulnerabilites/metadata.yaml"]["type"] == "Dashboard"
    assert set(files) == {
        "vulnerabilites/metadata.yaml",
        "vulnerabilites/databases/serving.yaml",
        "vulnerabilites/datasets/serving/cell_scores.yaml",
        "vulnerabilites/datasets/serving/series.yaml",
        "vulnerabilites/charts/table.yaml",
        "vulnerabilites/dashboards/main.yaml",
    }


def test_bundle_without_options_keeps_production_schema(export: Path) -> None:
    files = _read_bundle(superset_bundle.build_bundle(export))
    virtual = files["vulnerabilites/datasets/serving/series.yaml"]
    assert virtual["schema"] == "serving.dashboard"
    assert "serving.dashboard.cell_scores" in virtual["sql"]
    assert virtual["template_params"] == {"top_n": 5}


def test_bundle_substitutes_schema_in_key_and_sql(export: Path) -> None:
    files = _read_bundle(superset_bundle.build_bundle(export, schema="demo_dashboard"))
    physical = files["vulnerabilites/datasets/serving/cell_scores.yaml"]
    virtual = files["vulnerabilites/datasets/serving/series.yaml"]
    assert physical["schema"] == "serving.demo_dashboard"
    assert virtual["schema"] == "serving.demo_dashboard"
    assert "serving.demo_dashboard.cell_scores" in virtual["sql"]
    # Mot entier seulement : un autre schéma préfixé de « dashboard » est intact
    assert "serving.dashboard_archive.x" in virtual["sql"]
    with pytest.raises(superset_bundle.BundleError):
        superset_bundle.build_bundle(export, schema="demo; DROP")


def test_bundle_sets_default_year_and_top_n(export: Path) -> None:
    files = _read_bundle(superset_bundle.build_bundle(export, default_year=2021, top_n=8))
    filters = {
        f["id"]: f for f in files["vulnerabilites/dashboards/main.yaml"]["metadata"]["native_filter_configuration"]
    }
    year = filters["NATIVE_FILTER-year"]["defaultDataMask"]
    assert year["filterState"]["value"] == [2021]
    assert year["extraFormData"]["filters"] == [{"col": "year", "op": "IN", "val": [2021]}]
    # Filtre d'une autre colonne : inchangé
    assert filters["NATIVE_FILTER-flow"]["defaultDataMask"] == {"filterState": {"value": ["Import"]}}
    virtual = files["vulnerabilites/datasets/serving/series.yaml"]
    # Objet YAML (dictionnaire), seule forme admise par l'import de Superset
    assert virtual["template_params"] == {"top_n": 8}
    # Dataset sans paramètre top_n : aucun paramètre ajouté
    assert files["vulnerabilites/datasets/serving/cell_scores.yaml"]["template_params"] is None
    with pytest.raises(superset_bundle.BundleError):
        superset_bundle.build_bundle(export, top_n=0)


@pytest.mark.parametrize(
    "relative, change, message",
    [
        ("charts/table.yaml", {"dataset_uuid": "missing"}, "unknown dataset"),
        ("datasets/serving/series.yaml", {"database_uuid": "missing"}, "unknown database"),
        ("databases/serving.yaml", {"password": "x"}, "secret key"),
        ("metadata.yaml", {"type": "Chart"}, "type must be"),
        (
            "datasets/serving/series.yaml",
            {"sql": "SELECT * FROM (UNPIVOT t ON a, b INTO NAME k VALUE v)"},
            "UNPIVOT is rejected",
        ),
    ],
)
def test_bundle_rejects_inconsistent_export(export: Path, relative: str, change: dict, message: str) -> None:
    content = yaml.safe_load((export / relative).read_text(encoding="utf-8"))
    _write(export, relative, {**content, **change})
    with pytest.raises(superset_bundle.BundleError, match=message):
        superset_bundle.build_bundle(export)


def test_bundle_rejects_layout_chart_without_uuid(export: Path) -> None:
    dashboard = yaml.safe_load((export / "dashboards/main.yaml").read_text(encoding="utf-8"))
    del dashboard["position"]["CHART-1"]["meta"]["uuid"]
    _write(export, "dashboards/main.yaml", dashboard)
    with pytest.raises(superset_bundle.BundleError, match="layout chart"):
        superset_bundle.build_bundle(export)


def test_bundle_assets_import_type(export: Path) -> None:
    """Archive « assets » : seul le type change, l'export source reste un tableau de bord."""
    files = _read_bundle(superset_bundle.build_bundle(export, assets_import=True))
    assert files["vulnerabilites/metadata.yaml"]["type"] == "assets"
    assert yaml.safe_load((export / "metadata.yaml").read_text(encoding="utf-8"))["type"] == "Dashboard"
    assert len(files) == len(_read_bundle(superset_bundle.build_bundle(export)))


def test_cli_writes_archive(export: Path, tmp_path: Path) -> None:
    output = tmp_path / "out" / "bundle.zip"
    code = superset_bundle.main([str(export), "-o", str(output), "--schema", "demo_dashboard"])
    assert code == 0 and zipfile.is_zipfile(output)


def test_committed_export_bundles() -> None:
    """Le dossier versionné s'empaquette tel quel et vise le schéma de production."""
    source = REPO_ROOT / "superset" / "vulnerabilites"
    files = _read_bundle(superset_bundle.build_bundle(source, schema="demo_dashboard", default_year=2021))
    datasets = [content for name, content in files.items() if "/datasets/" in name]
    assert datasets and all(d["schema"] == "serving.demo_dashboard" for d in datasets)
    committed = superset_bundle.load_assets(source)
    assert all(
        content["schema"] == "serving.dashboard"
        for name, content in committed.items() if name.startswith("datasets/")
    )


# ── Exécution du SQL des datasets virtuels versionnés sur la table de service fictive ──

SOURCE = REPO_ROOT / "superset" / "vulnerabilites"
# Libellés du menu « Indicateur » lus par les gabarits des courbes
RANK_LABEL = "Rang (1 = le plus vulnérable)"
HHI_LABEL = "Concentration des partenaires (HHI)"


def _where_in(values) -> str:
    """Équivalent du filtre Jinja ``where_in`` de Superset (littéraux SQL échappés)."""
    def literal(value):
        if isinstance(value, (bool, int, float)):
            return str(value).lower() if isinstance(value, bool) else str(value)
        return "'" + str(value).replace("'", "''") + "'"
    return "(" + ", ".join(literal(value) for value in values) + ")"


def _run_virtual(conn, name: str, filters: dict, top_n: int = 5, **params) -> pd.DataFrame:
    """Rend le gabarit Jinja d'un dataset versionné comme Superset, puis l'exécute."""
    dataset = yaml.safe_load((SOURCE / "datasets" / "serving" / f"{name}.yaml").read_text(encoding="utf-8"))
    environment = jinja2.Environment()
    environment.filters["where_in"] = _where_in
    # Paramètres de gabarit du dataset, surchargés par ceux du test
    template_params = {**(dataset.get("template_params") or {}), "top_n": top_n, **params}
    sql = environment.from_string(dataset["sql"]).render(
        filter_values=lambda column, default=None, remove_filter=False: list(filters.get(column, [])),
        **template_params,
    )
    return conn.execute(sql).df()


@pytest.fixture
def served(serving_world):
    """Connexion DuckDB exposant la table cell_scores fictive sous serving.dashboard."""
    result = publish_serving(
        serving_world.tables, serving_world.catalog,
        params=serving_world.params, runtime=serving_world.runtime,
    )
    assert result["failures"] == {}, result["failures"]
    with serving_world.catalog.connect() as source:
        cells = source.execute(
            f"SELECT * FROM {serving_world.catalog.qualified_name('cell_scores')}"
        ).df()
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS serving")
    conn.execute("CREATE SCHEMA serving.dashboard")
    conn.register("cells_frame", cells)
    conn.execute("CREATE TABLE serving.dashboard.cell_scores AS SELECT * FROM cells_frame")
    # Table flows fictive, lue par les diagrammes des partenaires
    with serving_world.catalog.connect() as source:
        flows = source.execute(f"SELECT * FROM {serving_world.catalog.qualified_name('flows')}").df()
    conn.register("flows_frame", flows)
    conn.execute("CREATE TABLE serving.dashboard.flows AS SELECT * FROM flows_frame")
    yield conn, cells
    conn.close()


def test_committed_country_series_rank_within_level(served) -> None:
    """Courbes de l'onglet Par pays : top N au sein du niveau choisi, grandeur choisie."""
    conn, cells = served
    fr = cells.loc[cells["reporter"] == "FR", "reporter_display"].iloc[0]
    base = {"year": [2019], "flow_label": ["Import"], "reporter_display": [fr], "in_force": [True]}
    expected = cells[(cells["reporter"] == "FR") & (cells["year"] == 2019) & (cells["flow"] == "1")]

    trade = _run_virtual(conn, "trade_series_by_reporter", {
        **base, "product_level_label": ["NC8"], "measure": ["Quantité (100 kg)"],
    })
    nc8 = set(expected.loc[expected["product_level_label"] == "NC8", "product_display"])
    assert set(trade["product_display"]) == nc8 and set(trade["product_level_label"]) == {"NC8"}
    assert set(trade["measure"]) == {"Quantité (100 kg)"}
    merged = trade[trade["year"] == 2019].merge(expected, on="product_display")
    pd.testing.assert_series_equal(merged["value"], merged["trade_quantity_100kg"], check_names=False)

    # Rang tracé : rang au sein du niveau ; top N borné
    metric = _run_virtual(conn, "metric_series_by_reporter", {
        **base, "product_level_label": ["NC8"], "metric": [RANK_LABEL],
    }, top_n=2)
    assert metric["product_display"].nunique() == 2
    merged = metric[metric["year"] == 2019].merge(expected, on="product_display")
    pd.testing.assert_series_equal(
        merged["value"], merged["primary_rank_by_reporter_level"], check_names=False
    )
    assert set(merged["primary_rank_by_reporter_level"]) == {1.0, 2.0}

    # Menu « Produits des courbes » renseigné : exactement ses produits, quel que soit leur
    # niveau, à la place du top N (le menu montre ce qui est tracé)
    sh6 = expected.loc[expected["product_level_label"] == "SH6", "product_display"].iloc[0]
    chosen = _run_virtual(conn, "trade_series_by_reporter", {
        **base, "product_level_label": ["NC8"], "product_display": [sh6],
    }, top_n=1)
    assert set(chosen["product_display"]) == {sh6}


def test_committed_product_series_run(served) -> None:
    """Courbes de l'onglet Par produit : pays de meilleur rang par produit."""
    conn, cells = served
    product = cells.loc[cells["product_level"] == 8, "product_display"].iloc[0]
    filters = {"year": [2019], "flow_label": ["Import"], "product_display": [product], "in_force": [True]}
    trade = _run_virtual(conn, "trade_series_by_product", filters, top_n=2)
    metric = _run_virtual(conn, "metric_series_by_product", {**filters, "metric": [HHI_LABEL]}, top_n=2)
    assert trade["reporter_display"].nunique() == 2 and set(trade["measure"]) == {"Valeur (€)"}
    assert metric["reporter_display"].nunique() == 2 and set(metric["metric"]) == {HHI_LABEL}
    assert trade["value"].notna().all()
    # Menu « Pays des courbes » renseigné : exactement ses pays
    de = cells.loc[cells["reporter"] == "DE", "reporter_display"].iloc[0]
    chosen = _run_virtual(conn, "metric_series_by_product", {**filters, "reporter_display": [de]}, top_n=2)
    assert set(chosen["reporter_display"]) == {de}


def test_committed_menu_defaults_are_drawn_units() -> None:
    """Valeurs par défaut versionnées : produit détaillé = premier des produits des courbes,
    top N pays du produit par défaut de l'onglet Par produit, menus cohérents."""
    dashboard = yaml.safe_load((SOURCE / "dashboards" / "Vulnerabilites.yaml").read_text(encoding="utf-8"))
    filters = {f["id"]: f for f in dashboard["metadata"]["native_filter_configuration"]}
    value = lambda name: filters[name]["defaultDataMask"]["filterState"]["value"]
    products, reporters = value("NATIVE_FILTER-PAYS-EXTRA"), value("NATIVE_FILTER-PRODUIT-EXTRA")
    assets = superset_bundle.load_assets(SOURCE)
    top_n = superset_bundle._top_n(assets)
    assert len(products) == len(reporters) == top_n == 5
    assert len(set(products)) == len(set(reporters)) == 5
    assert value("NATIVE_FILTER-PAYS-DETAIL") == products[:1] == value("NATIVE_FILTER-PRODUIT-FOCUS")
    for name in ("NATIVE_FILTER-PAYS-EXTRA", "NATIVE_FILTER-PRODUIT-EXTRA", "NATIVE_FILTER-PAYS-DETAIL"):
        mask = filters[name]["defaultDataMask"]
        assert mask["extraFormData"]["filters"][0]["val"] == mask["filterState"]["value"]
    # Référence des menus : valeurs par défaut des autres filtres de l'onglet
    queries = {query.filter_id: query for query in superset_bundle.menu_queries(assets)}
    assert queries["NATIVE_FILTER-PRODUIT-EXTRA"].filters["product_display"] == value("NATIVE_FILTER-PRODUIT-FOCUS")
    assert queries["NATIVE_FILTER-PRODUIT-EXTRA"].limit == len(reporters)
    assert queries["NATIVE_FILTER-PAYS-DETAIL"].limit == 1
    for name in ("NATIVE_FILTER-PAYS-DETAIL", "NATIVE_FILTER-PAYS-EXTRA"):
        assert queries[name].filters["reporter_display"] == value("NATIVE_FILTER-PAYS-FOCUS")
        assert queries[name].filters["product_level_label"] == value("NATIVE_FILTER-PAYS-LEVEL")
    assert superset_bundle.current_menu_defaults(assets) == {
        "NATIVE_FILTER-PAYS-DETAIL": products[:1],
        "NATIVE_FILTER-PAYS-EXTRA": products,
        "NATIVE_FILTER-PRODUIT-EXTRA": reporters,
    }


def _synthetic_menu_assets(cells: pd.DataFrame, year: int):
    """Export versionné dont les filtres de référence désignent le monde fictif."""
    fr = cells.loc[cells["reporter"] == "FR", "reporter_display"].iloc[0]
    ranked = cells[(cells["year"] == year) & (cells["flow"] == "1") & cells["primary_rank_by_product"].notna()]
    product = ranked["product_display"].value_counts().index[0]
    assets = superset_bundle.configure_assets(
        superset_bundle.load_assets(SOURCE),
        default_year=year,
        top_n=3,
        menu_defaults={
            "NATIVE_FILTER-PAYS-FOCUS": [fr],
            "NATIVE_FILTER-PAYS-LEVEL": ["NC8"],
            "NATIVE_FILTER-PRODUIT-FOCUS": [product],
        },
    )
    base = {"year": [year], "flow_label": ["Import"], "in_force": [True]}
    return assets, {**base, "reporter_display": [fr], "product_level_label": ["NC8"]}, {**base, "product_display": [product]}


def test_menu_queries_return_the_drawn_units(served) -> None:
    """Menus calculés = unités que tracent, menu vide, les graphiques qu'ils pilotent."""
    conn, cells = served
    assets, country, product = _synthetic_menu_assets(cells, 2019)
    computed = {
        query.filter_id: superset_bundle.menu_values(
            conn.execute(superset_bundle.menu_query_sql(query, "serving.dashboard.cell_scores")).df().to_dict("records"),
            query,
        )
        for query in superset_bundle.menu_queries(assets)
    }
    drawn_products = _run_virtual(conn, "trade_series_by_reporter", country, top_n=3)["product_display"]
    drawn_reporters = _run_virtual(conn, "trade_series_by_product", product, top_n=3)["reporter_display"]
    bars = _run_virtual(conn, "partners_by_reporter", country)
    assert len(computed["NATIVE_FILTER-PAYS-EXTRA"]) == len(computed["NATIVE_FILTER-PRODUIT-EXTRA"]) == 3
    assert set(computed["NATIVE_FILTER-PAYS-EXTRA"]) == set(drawn_products)
    assert set(computed["NATIVE_FILTER-PRODUIT-EXTRA"]) == set(drawn_reporters)
    assert computed["NATIVE_FILTER-PAYS-DETAIL"] == list(bars["product_display"].unique())
    # Produit détaillé = meilleur rang = premier des produits des courbes
    assert computed["NATIVE_FILTER-PAYS-DETAIL"] == computed["NATIVE_FILTER-PAYS-EXTRA"][:1]
    # Requête de données Superset : mêmes filtres, même tri, même limite
    for query in superset_bundle.menu_queries(assets):
        body = superset_bundle.chart_data_payload(query, 7)["queries"][0]
        assert {f["col"]: f["val"] for f in body["filters"]} == query.filters
        assert body["row_limit"] == query.limit and body["orderby"][1] == [query.tiebreak, True]
        assert body["extras"]["where"] == f"{query.rank} IS NOT NULL"


def test_menu_defaults_written_in_both_masks(export: Path, tmp_path: Path) -> None:
    """Valeurs des menus : état affiché et filtre appliqué ; vide ; erreurs ; option CLI."""
    assets = superset_bundle.load_assets(SOURCE)
    configured = superset_bundle.configure_assets(
        assets, menu_defaults={"NATIVE_FILTER-PAYS-EXTRA": ["a", "b"], "NATIVE_FILTER-PAYS-DETAIL": []}
    )
    filters = superset_bundle._native_filters(configured)
    extra = filters["NATIVE_FILTER-PAYS-EXTRA"]["defaultDataMask"]
    assert extra["filterState"] == {"value": ["a", "b"]}
    assert extra["extraFormData"] == {"filters": [{"col": "product_display", "op": "IN", "val": ["a", "b"]}]}
    detail = filters["NATIVE_FILTER-PAYS-DETAIL"]["defaultDataMask"]
    assert detail["filterState"] == {"value": None} and detail["extraFormData"] == {}
    # Export d'origine intact
    assert superset_bundle._native_filters(assets)["NATIVE_FILTER-PAYS-EXTRA"]["defaultDataMask"] != extra
    with pytest.raises(superset_bundle.BundleError, match="single-select"):
        superset_bundle.configure_assets(assets, menu_defaults={"NATIVE_FILTER-PAYS-DETAIL": ["a", "b"]})
    with pytest.raises(superset_bundle.BundleError, match="unknown native filter"):
        superset_bundle.configure_assets(assets, menu_defaults={"NATIVE_FILTER-X": ["a"]})
    # Option --menu-defaults de la ligne de commande
    menus = tmp_path / "menus.json"
    menus.write_text('{"NATIVE_FILTER-PRODUIT-EXTRA": ["NL — Netherlands"]}', encoding="utf-8")
    output = tmp_path / "out.zip"
    assert superset_bundle.main([str(SOURCE), "-o", str(output), "--menu-defaults", str(menus)]) == 0
    dashboards = [n for n in _read_bundle(output.read_bytes()) if "/dashboards/" in n]
    with zipfile.ZipFile(output) as handle:
        content = yaml.safe_load(handle.read(dashboards[0]))
    produit = {f["id"]: f for f in content["metadata"]["native_filter_configuration"]}["NATIVE_FILTER-PRODUIT-EXTRA"]
    assert produit["defaultDataMask"]["filterState"]["value"] == ["NL — Netherlands"]
    menus.write_text("[1]", encoding="utf-8")
    assert superset_bundle.main([str(SOURCE), "-o", str(output), "--menu-defaults", str(menus)]) == 1


@pytest.mark.parametrize(
    "name, focus_column",
    [("metric_series_by_reporter", "reporter_display"), ("metric_series_by_product", "product_display")],
)
@pytest.mark.parametrize(
    "label, column",
    [(RANK_LABEL, None), (HHI_LABEL, "HHI"), ("Concentration mondiale (WORLD_HHI)", "WORLD_HHI")],
)
def test_metric_series_cover_every_year_whatever_the_year_filter(served, name, focus_column, label, column) -> None:
    """Courbe des indicateurs : toutes les années où l'indicateur est défini, filtre « Année »
    renseigné ; aucune valeur vide ni fabriquée."""
    conn, cells = served
    year = 2019
    if focus_column == "reporter_display":
        focus = cells.loc[cells["reporter"] == "FR", "reporter_display"].iloc[0]
        unit_key, rank = "product", "primary_rank_by_reporter_level"
        extra = {"product_level_label": ["NC8"]}
    else:
        ranked = cells[(cells["year"] == year) & (cells["flow"] == "1") & cells["primary_rank_by_product"].notna()]
        focus = ranked["product_display"].value_counts().index[0]
        unit_key, rank = "reporter", "primary_rank_by_product"
        extra = {}
    filters = {"year": [year], "flow_label": ["Import"], focus_column: [focus], "in_force": [True], "metric": [label], **extra}
    series = _run_virtual(conn, name, filters, top_n=3)
    assert series["year"].nunique() > 1 and series["value"].notna().all()
    # Années renvoyées = années où l'indicateur est défini, unité par unité
    display = "product_display" if unit_key == "product" else "reporter_display"
    source = cells[
        (cells[focus_column] == focus) & (cells["flow"] == "1") & cells["in_force"]
        & cells[display].isin(series[display].unique())
    ]
    defined = source[source[column or rank].notna()]
    expected = defined.groupby(display)["year"].apply(lambda years: sorted(set(years))).to_dict()
    returned = series.groupby(display)["year"].apply(lambda years: sorted(set(years))).to_dict()
    assert returned == expected


def test_curve_titles_do_not_claim_most_vulnerable() -> None:
    """Titres exacts des quatre courbes ; aucun texte n'affirme « les plus vulnérables »."""
    titles = {
        chart["uuid"]: chart["slice_name"]
        for chart in (yaml.safe_load(path.read_text(encoding="utf-8")) for path in (SOURCE / "charts").glob("*.yaml"))
    }
    assert titles["0e9afb50-32b1-4f80-a09e-57dd402bfecf"] == "Échanges avec le monde"
    assert titles["08f11d93-001e-4377-9488-8352b4c40b08"] == "Évolution d'un indicateur de vulnérabilité des produits"
    assert titles["27e09fe7-5a5c-4c15-b6f3-34d433eabbd4"] == "Échanges avec le monde des pays"
    assert titles["e2cf75f7-f581-41b3-bce7-bd0d0093e301"] == "Évolution d'un indicateur de vulnérabilité des pays"
    dashboard = yaml.safe_load((SOURCE / "dashboards" / "Vulnerabilites.yaml").read_text(encoding="utf-8"))
    texts = list(titles.values())
    texts += [c["meta"]["code"] for c in dashboard["position"].values() if isinstance(c, dict) and c.get("type") == "MARKDOWN"]
    texts += [f.get("description") or "" for f in dashboard["metadata"]["native_filter_configuration"]]
    texts += [
        yaml.safe_load(path.read_text(encoding="utf-8"))["sql"]
        for path in (SOURCE / "datasets" / "serving").glob("*_series_*.yaml")
    ]
    assert not [text for text in texts if "plus vulnérables" in text]


# ── Diagrammes des partenaires : barres (onglet Par pays) et Sankey (onglet Par produit) ──

def test_committed_partner_bars_on_synthetic_world(served) -> None:
    """Barres : somme égale au total de cell_scores, tri décroissant, rang 1 par défaut."""
    conn, cells = served
    fr = cells.loc[cells["reporter"] == "FR", "reporter_display"].iloc[0]
    base = {"year": [2019], "reporter_display": [fr], "in_force": [True], "product_level_label": ["NC8"]}
    for flow, code in (("Import", "1"), ("Export", "2")):
        bars = _run_virtual(conn, "partners_by_reporter", {**base, "flow_label": [flow]})
        expected = cells[(cells["reporter"] == "FR") & (cells["year"] == 2019) & (cells["flow"] == code)
                         & cells["in_force"] & (cells["product_level_label"] == "NC8")]
        first = expected.sort_values(["primary_rank_by_reporter_level", "product"]).iloc[0]
        # Produit par défaut : rang 1 du niveau choisi
        assert set(bars["product_display"]) == {first["product_display"]}
        # Somme des barres (« Autres pays » compris) = échanges totaux de la cellule
        assert bars["value"].sum() == pytest.approx(first["trade_value_eur"])
        partners = bars[bars["zone"] != "Autres pays"]
        assert len(partners) == 10 and partners["value"].is_monotonic_decreasing
        assert bars["zone"].iloc[-1] == "Autres pays" and set(bars["flow_label"]) == {flow}
        # Une seule colonne de couleur renseignée par ligne
        assert (bars[["value_eu", "value_extra", "value_other"]].notna().sum(axis=1) == 1).all()

    # Produit choisi par le filtre « Produit à détailler »
    other = cells[(cells["reporter"] == "FR") & (cells["year"] == 2019) & (cells["flow"] == "1")
                  & (cells["product_level_label"] == "SH6")]["product_display"].iloc[0]
    chosen = _run_virtual(conn, "partners_by_reporter", {**base, "flow_label": ["Import"], "detail_product": [other]})
    assert set(chosen["product_display"]) == {other}


def test_committed_sankey_on_synthetic_world(served) -> None:
    """Sankey : agrégats déclarants exclus, sens des liens selon le flux."""
    conn, cells = served
    product = cells.loc[cells["product_level"] == 8, "product_display"].iloc[0]
    filters = {"year": [2019], "product_display": [product], "in_force": [True]}
    imports = _run_virtual(conn, "partner_flows_by_product", {**filters, "flow_label": ["Import"]})
    exports = _run_virtual(conn, "partner_flows_by_product", {**filters, "flow_label": ["Export"]})
    labels = cells.set_index("reporter")["reporter_label"].to_dict()
    # Pays de l'UE à droite à l'import, à gauche à l'export ; Union entière exclue
    assert set(imports["target"]) == {labels["FR"].split(" (")[0], labels["DE"].split(" (")[0]}
    assert set(exports["source"]) == set(imports["target"])
    assert not any("European Union" in node for node in set(imports["target"]) | set(imports["source"]))
    assert (imports["value"] > 0).all()


@pytest.fixture
def hand_world():
    """Petit monde écrit à la main : deux États membres, l'Union, trois partenaires hors UE.

    Import 2021 du produit 850760 : France, total 100 (quantité 10), hors UE 70, Chine 50
    (quantité 5), Allemagne 30 (quantité non déclarée), États-Unis 15 (quantité 1,5) ;
    Allemagne, total 200, hors UE 50, France 150, Chine 30, Japon 20 ; Union, hors UE 120.
    Export : France, total 60, entièrement vers les États-Unis. Produit 850780 : rang 2.
    """
    fr, de, eu = "FR — France (incl. Mayotte)", "DE — Germany (incl. GDR)", "EU27_2020 — European Union"
    p1, p2 = "850760 — Lithium-ion accumulators", "850780 — Other accumulators"
    cells = pd.DataFrame([
        # reporter, display, label, product, display, flow, flow_label, rang pays, rang produit, valeur, quantité, CDI2
        ("FR", fr, "France (incl. Mayotte)", "850760", p1, "1", "Import", 1.0, 1.0, 100.0, 10.0, 0.7),
        ("FR", fr, "France (incl. Mayotte)", "850780", p2, "1", "Import", 2.0, 1.0, 40.0, None, 1.0),
        ("FR", fr, "France (incl. Mayotte)", "850760", p1, "2", "Export", 1.0, 1.0, 60.0, 6.0, 1.0),
        ("DE", de, "Germany (incl. GDR)", "850760", p1, "1", "Import", 1.0, 2.0, 200.0, 20.0, 0.25),
        ("EU27_2020", eu, "European Union", "850760", p1, "1", "Import", None, None, 120.0, 12.0, 1.0),
    ], columns=["reporter", "reporter_display", "reporter_label", "product", "product_display", "flow",
                "flow_label", "primary_rank_by_reporter_level", "primary_rank_by_product",
                "trade_value_eur", "trade_quantity_100kg", "CDI2"])
    cells = cells.assign(year=2021, in_force=True, product_level_label="SH6")
    flows = pd.DataFrame([
        # reporter, label, product, flow, partner, label, agrégat, valeur, quantité, part, rang
        ("FR", "France (incl. Mayotte)", "850760", "1", "WORLD", "All countries", True, 100.0, 10.0, 1.0, None),
        ("FR", "France (incl. Mayotte)", "850760", "1", "EXT_EU", "Extra-EU", True, 70.0, 7.0, 0.7, None),
        ("FR", "France (incl. Mayotte)", "850760", "1", "CN", "China", False, 50.0, 5.0, 0.5, 1),
        ("FR", "France (incl. Mayotte)", "850760", "1", "DE", "Germany (incl. GDR)", False, 30.0, None, 0.3, 2),
        ("FR", "France (incl. Mayotte)", "850760", "1", "US", "United States", False, 15.0, 1.5, 0.15, 3),
        ("FR", "France (incl. Mayotte)", "850780", "1", "WORLD", "All countries", True, 40.0, None, 1.0, None),
        ("FR", "France (incl. Mayotte)", "850780", "1", "CN", "China", False, 40.0, None, 1.0, 1),
        ("FR", "France (incl. Mayotte)", "850760", "2", "WORLD", "All countries", True, 60.0, 6.0, 1.0, None),
        ("FR", "France (incl. Mayotte)", "850760", "2", "EXT_EU", "Extra-EU", True, 60.0, 6.0, 1.0, None),
        ("FR", "France (incl. Mayotte)", "850760", "2", "US", "United States", False, 60.0, 6.0, 1.0, 1),
        ("DE", "Germany (incl. GDR)", "850760", "1", "WORLD", "All countries", True, 200.0, 20.0, 1.0, None),
        ("DE", "Germany (incl. GDR)", "850760", "1", "EXT_EU", "Extra-EU", True, 50.0, 5.0, 0.25, None),
        ("DE", "Germany (incl. GDR)", "850760", "1", "FR", "France (incl. Mayotte)", False, 150.0, 15.0, 0.75, 1),
        ("DE", "Germany (incl. GDR)", "850760", "1", "CN", "China", False, 30.0, 3.0, 0.15, 2),
        ("DE", "Germany (incl. GDR)", "850760", "1", "JP", "Japan", False, 20.0, 2.0, 0.1, 3),
        ("EU27_2020", "European Union", "850760", "1", "EXT_EU27_2020", "Extra-EU27", True, 120.0, 12.0, 1.0, None),
        ("EU27_2020", "European Union", "850760", "1", "CN", "China", False, 80.0, 8.0, 0.67, 1),
    ], columns=["reporter", "reporter_label", "product", "flow", "partner", "partner_label", "is_aggregate",
                "value_eur", "quantity_100kg", "share", "rank"]).assign(year=2021)
    flows["rank"] = flows["rank"].astype("Int64")
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS serving")
    conn.execute("CREATE SCHEMA serving.dashboard")
    for name, frame in (("cell_scores", cells), ("flows", flows)):
        conn.register(f"{name}_frame", frame)
        conn.execute(f"CREATE TABLE serving.dashboard.{name} AS SELECT * FROM {name}_frame")
    yield conn, {"fr": fr, "p1": p1, "p2": p2}
    conn.close()


def test_partner_bars_hand_computed(hand_world) -> None:
    """Barres : valeurs, « Autres pays », zones UE / hors UE et quantité calculées à la main."""
    conn, names = hand_world
    base = {"year": [2021], "flow_label": ["Import"], "reporter_display": [names["fr"]], "in_force": [True],
            "product_level_label": ["SH6"]}
    bars = _run_virtual(conn, "partners_by_reporter", base, n_partners=2)
    # Défaut : produit de rang 1 ; Chine 50 (hors UE), Allemagne 30 (UE), Autres 100 − 80 = 20
    assert set(bars["product_display"]) == {names["p1"]}
    assert bars["partner_name"].tolist() == ["China", "Germany", "Autres pays"]
    assert bars["value"].tolist() == [50.0, 30.0, 20.0]
    assert bars["zone"].tolist() == ["Pays hors UE", "Pays de l'UE", "Autres pays"]
    assert bars["bar_label"].tolist() == [" 1. China — 50,0 %", " 2. Germany — 30,0 %", "Autres pays — 20,0 %"]
    # Tri du graphique par libellé décroissant : « Autres pays », puis rang 2, puis rang 1
    assert sorted(bars["bar_label"], reverse=True) == bars["bar_label"].tolist()[::-1]

    # Trois partenaires : Autres 100 − 95 = 5
    bars = _run_virtual(conn, "partners_by_reporter", base, n_partners=3)
    assert bars["value"].tolist() == [50.0, 30.0, 15.0, 5.0]

    # Quantité : Allemagne absente (non déclarée), Autres 10 − 6,5 = 3,5
    quantity = _run_virtual(conn, "partners_by_reporter", {**base, "measure": ["Quantité (100 kg)"]}, n_partners=3)
    assert quantity["partner_name"].tolist() == ["China", "United States", "Autres pays"]
    assert quantity["value"].tolist() == pytest.approx([5.0, 1.5, 3.5])
    assert set(quantity["measure"]) == {"Quantité (100 kg)"}

    # Produit à détailler, puis clic sur un code produit (prioritaire) ; quantité totale
    # inconnue : aucune ligne « Autres pays »
    chosen = _run_virtual(conn, "partners_by_reporter", {**base, "detail_product": [names["p1"]]}, n_partners=3)
    assert set(chosen["product_display"]) == {names["p1"]}
    clicked = _run_virtual(conn, "partners_by_reporter",
                           {**base, "detail_product": [names["p1"]], "product": ["850780"]})
    assert clicked["partner_name"].tolist() == ["China"] and clicked["value"].tolist() == [40.0]
    unknown = _run_virtual(conn, "partners_by_reporter",
                           {**base, "product": ["850780"], "measure": ["Quantité (100 kg)"]})
    assert unknown.empty


def test_sankey_hand_computed(hand_world) -> None:
    """Sankey : nœuds hors UE, « Autres pays hors UE » et nœud intra-UE calculés à la main."""
    conn, names = hand_world
    base = {"year": [2021], "product_display": [names["p1"]], "in_force": [True]}
    links = _run_virtual(conn, "partner_flows_by_product", {**base, "flow_label": ["Import"]}, k_partners=1)
    got = {(row.source, row.target): row.value for row in links.itertuples()}
    # Partenaires hors UE cumulés : Chine 80, Japon 20, États-Unis 15 → K = 1 retient la Chine ;
    # la France, déclarante, n'est pas un partenaire hors UE de l'Allemagne
    intra = "Union européenne (échanges entre États membres)"
    assert got == {
        ("China", "France"): 50.0, ("Autres pays hors UE", "France"): 20.0, (intra, "France"): 30.0,
        ("China", "Germany"): 30.0, ("Autres pays hors UE", "Germany"): 20.0, (intra, "Germany"): 150.0,
    }
    # Somme entrante de chaque pays = son total ; l'Union entière n'apparaît pas
    assert links.groupby("target")["value"].sum().to_dict() == {"France": 100.0, "Germany": 200.0}

    # Export : pays de l'UE à gauche ; liens nuls omis
    exports = _run_virtual(conn, "partner_flows_by_product", {**base, "flow_label": ["Export"]})
    assert exports[["source", "target", "value"]].values.tolist() == [["France", "United States", 60.0]]


# ── Libellés des indicateurs : un seul libellé par indicateur, jamais un sigle seul ──

SIGLES = {"HHI", "CDI2", "CDI3", "WORLD_HHI", "CENTRALITY_RISK", "CLUSTERING_W", "SPOF"}


def _template_keys(name: str) -> list:
    """Clés du dictionnaire Jinja « libellé → colonne » d'un dataset de courbes."""
    sql = yaml.safe_load((SOURCE / "datasets" / "serving" / f"{name}.yaml").read_text(encoding="utf-8"))["sql"]
    line = next(line for line in sql.splitlines() if "{% set columns = {" in line)
    return re.findall(r'"([^"]+)": "', line)


def test_indicator_labels_consistent() -> None:
    """Menu « Indicateur » = clés des gabarits = libellés des colonnes du tableau."""
    options = yaml.safe_load((SOURCE / "datasets" / "serving" / "series_options.yaml").read_text(encoding="utf-8"))
    menu = re.findall(r"\(NULL, '((?:[^']|'')+)'\)", options["sql"])
    assert menu and menu == _template_keys("metric_series_by_reporter") == _template_keys("metric_series_by_product")

    cell_scores = yaml.safe_load((SOURCE / "datasets" / "serving" / "cell_scores.yaml").read_text(encoding="utf-8"))
    verbose = {column["column_name"]: column["verbose_name"] for column in cell_scores["columns"]}
    for sigle in SIGLES:
        # Libellé explicite, sigle en fin de libellé pour les lecteurs experts
        assert verbose[sigle].endswith(f"({sigle})") and verbose[sigle] in menu
    assert verbose["primary_rank_by_reporter_level"] == verbose["primary_rank_by_product"] == menu[0]

    # Aucun sigle seul : en-têtes des tableaux, menu, métriques des graphiques
    for chart in (SOURCE / "charts").glob("*.yaml"):
        params = yaml.safe_load(chart.read_text(encoding="utf-8"))["params"]
        shown = [verbose.get(column, column) for column in params.get("all_columns") or []]
        metrics = params.get("metrics") or [params.get("metric")]
        shown += [metric["label"] for metric in metrics if isinstance(metric, dict)]
        assert not SIGLES & set(shown), chart.name
    assert not SIGLES & set(menu)
