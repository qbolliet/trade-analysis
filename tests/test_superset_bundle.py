"""Tests de l'outil d'empaquetage du tableau de bord Superset (``tools/superset_bundle.py``)."""

from __future__ import annotations

import importlib.util
import io
import zipfile
from pathlib import Path

import pytest
import yaml

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
