"""Fichiers embarqués par le chart Helm ``trade-dashboard`` et import du tableau de bord.

Le chart contient une copie de l'export versionné et de l'outil d'empaquetage
(``tools/sync_chart_files.py``) ; le Job d'import reconstruit l'export depuis les clés
aplaties de son ConfigMap avant de construire l'archive.
"""

from __future__ import annotations

import importlib.util
import io
import zipfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
FILES = ROOT / "helm" / "trade-dashboard" / "files"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sync = _load("sync_chart_files", ROOT / "tools" / "sync_chart_files.py")
importer = _load("import_dashboard", FILES / "import_dashboard.py")
bundle_tool = _load("superset_bundle_chart", FILES / "superset_bundle.py")


def test_chart_copies_match_their_sources() -> None:
    stale = sync.stale_copies(ROOT)
    assert stale == {}, f"Copies du chart périmées {stale} : lancer `python tools/sync_chart_files.py`"


def test_stale_copies_are_detected(tmp_path: Path) -> None:
    (tmp_path / "superset" / "vulnerabilites").mkdir(parents=True)
    (tmp_path / "superset" / "vulnerabilites" / "metadata.yaml").write_text("version: 1.0.0\n")
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "superset_bundle.py").write_text("# outil\n")
    assert set(sync.stale_copies(tmp_path)) == set(sync.CHART_COPIES)


def test_the_flattened_export_rebuilds_into_an_assets_archive(tmp_path: Path) -> None:
    # Clés du ConfigMap : chemin relatif aplati (même règle que le gabarit du chart)
    flat = tmp_path / "flat"
    flat.mkdir()
    for relative, content in sync.source_files(FILES / "dashboard").items():
        (flat / relative.replace("/", importer.PATH_SEPARATOR)).write_bytes(content)
    # Lien de montage d'un ConfigMap, ignoré
    (flat / "..data").mkdir()
    export = tmp_path / "dashboard"
    counts = importer.rebuild_export(flat, export)
    assert counts[""] == 1 and counts["charts"] == 6 and counts["databases"] == 1
    archive = bundle_tool.build_bundle(export, schema="demo_dashboard", default_year=2021, assets_import=True)
    with zipfile.ZipFile(io.BytesIO(archive)) as handle:
        names = handle.namelist()
        metadata = yaml.safe_load(handle.read(next(n for n in names if n.endswith("metadata.yaml"))))
        datasets = [yaml.safe_load(handle.read(n)) for n in names if "/datasets/" in n]
    assert metadata["type"] == "assets"
    # Schéma qualifié par l'alias du catalogue attaché
    assert {dataset["schema"] for dataset in datasets} == {"serving.demo_dashboard"}


def test_the_serving_fragment_is_valid_python() -> None:
    source = (FILES / "superset_serving.py").read_text(encoding="utf-8")
    compile(source, "superset_serving.py", "exec")
    # Discriminant des connexions DuckDB et lecture seule conservés
    assert '"duckdb_engine"' in source and "(READ_ONLY)" in source


@pytest.mark.parametrize("module", [importer, sync])
def test_tools_have_docstrings(module) -> None:
    assert module.__doc__ and module.main.__doc__
