"""Tests de l'outil d'empaquetage du tableau de bord Superset (``tools/superset_bundle.py``)."""

from __future__ import annotations

import importlib.util
import io
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


def _where_in(values) -> str:
    """Équivalent du filtre Jinja ``where_in`` de Superset (littéraux SQL échappés)."""
    def literal(value):
        if isinstance(value, (bool, int, float)):
            return str(value).lower() if isinstance(value, bool) else str(value)
        return "'" + str(value).replace("'", "''") + "'"
    return "(" + ", ".join(literal(value) for value in values) + ")"


def _run_virtual(conn, name: str, filters: dict, top_n: int = 5) -> pd.DataFrame:
    """Rend le gabarit Jinja d'un dataset versionné comme Superset, puis l'exécute."""
    dataset = yaml.safe_load((SOURCE / "datasets" / "serving" / f"{name}.yaml").read_text(encoding="utf-8"))
    environment = jinja2.Environment()
    environment.filters["where_in"] = _where_in
    sql = environment.from_string(dataset["sql"]).render(
        filter_values=lambda column, default=None, remove_filter=False: list(filters.get(column, [])),
        top_n=top_n,
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
        **base, "product_level_label": ["NC8"], "metric": ["Rang consensus"],
    }, top_n=2)
    assert metric["product_display"].nunique() == 2
    merged = metric[metric["year"] == 2019].merge(expected, on="product_display")
    pd.testing.assert_series_equal(
        merged["value"], merged["primary_rank_by_reporter_level"], check_names=False
    )
    assert set(merged["primary_rank_by_reporter_level"]) == {1.0, 2.0}

    # Unité ajoutée d'un autre niveau : tracée en plus du top N
    sh6 = expected.loc[expected["product_level_label"] == "SH6", "product_display"].iloc[0]
    extra = _run_virtual(conn, "trade_series_by_reporter", {
        **base, "product_level_label": ["NC8"], "product_display": [sh6],
    }, top_n=1)
    assert extra["product_display"].nunique() == 2 and sh6 in set(extra["product_display"])


def test_committed_product_series_run(served) -> None:
    """Courbes de l'onglet Par produit : pays de meilleur rang par produit."""
    conn, cells = served
    product = cells.loc[cells["product_level"] == 8, "product_display"].iloc[0]
    filters = {"year": [2019], "flow_label": ["Import"], "product_display": [product], "in_force": [True]}
    trade = _run_virtual(conn, "trade_series_by_product", filters, top_n=2)
    metric = _run_virtual(conn, "metric_series_by_product", {**filters, "metric": ["HHI"]}, top_n=2)
    assert trade["reporter_display"].nunique() == 2 and set(trade["measure"]) == {"Valeur (€)"}
    assert metric["reporter_display"].nunique() == 2 and set(metric["metric"]) == {"HHI"}
    assert trade["value"].notna().all()
