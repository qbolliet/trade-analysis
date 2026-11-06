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
    # Autant de graphiques que dans l'export versionné
    n_charts = len(list((FILES / "dashboard" / "charts").glob("*.yaml")))
    assert counts[""] == 1 and counts["charts"] == n_charts >= 8 and counts["databases"] == 1
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


class _Response:
    """Réponse HTTP simulée (statut et corps JSON)."""

    def __init__(self, status_code: int = 200, payload=None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class _FakeSuperset:
    """Superset simulé : santé, connexion, import « assets », datasets et données."""

    def __init__(self, menus: dict) -> None:
        self.menus = menus
        self.imports: list = []
        self.data_requests: list = []

    def session(self):
        server = self

        class Session:
            def __init__(self) -> None:
                self.headers: dict = {}

            def get(self, url, **kwargs):
                if url.endswith("/health"):
                    return _Response()
                if url.endswith("/csrf_token/"):
                    return _Response(payload={"result": "csrf"})
                if url.endswith("/datasets"):
                    return _Response(payload={"result": [{"id": 3, "table_name": "series"}, {"id": 1, "table_name": "cell_scores"}]})
                raise AssertionError(url)

            def post(self, url, json=None, files=None, **kwargs):
                if url.endswith("/security/login"):
                    return _Response(payload={"access_token": "jeton"})
                if url.endswith("/assets/import/"):
                    server.imports.append(files["bundle"][1])
                    return _Response()
                if url.endswith("/chart/data"):
                    server.data_requests.append(json)
                    query = json["queries"][0]
                    column = query["columns"][0]
                    key = "detail" if query["row_limit"] == 1 else column
                    rows = [{column: value} for value in server.menus[key]]
                    return _Response(payload={"result": [{"data": rows}]})
                raise AssertionError(url)

        return Session


def _menus_of(archive: bytes) -> dict:
    """Valeurs par défaut des menus préremplis d'une archive."""
    with zipfile.ZipFile(io.BytesIO(archive)) as handle:
        name = next(n for n in handle.namelist() if "/dashboards/" in n)
        dashboard = yaml.safe_load(handle.read(name))
    return bundle_tool.current_menu_defaults({"dashboards/d.yaml": dashboard})


@pytest.mark.parametrize("recomputed", [False, True])
def test_the_job_recomputes_the_menus_on_the_data(tmp_path: Path, monkeypatch, capsys, recomputed: bool) -> None:
    # Export aplati du chart, comme le montage du ConfigMap
    flat = tmp_path / "flat"
    flat.mkdir()
    for relative, content in sync.source_files(FILES / "dashboard").items():
        (flat / relative.replace("/", importer.PATH_SEPARATOR)).write_bytes(content)
    exported = bundle_tool.current_menu_defaults(bundle_tool.load_assets(FILES / "dashboard"))
    products = ["P1", "P2", "P3", "P4", "P5"] if recomputed else exported["NATIVE_FILTER-PAYS-EXTRA"]
    server = _FakeSuperset({
        "detail": products[:1],
        "product_display": products,
        "reporter_display": exported["NATIVE_FILTER-PRODUIT-EXTRA"],
    })
    monkeypatch.setattr(importer.requests, "Session", server.session())
    for name, value in {
        "SUPERSET_URL": "http://superset:8088/", "SUPERSET_ADMIN_USERNAME": "admin",
        "SUPERSET_ADMIN_PASSWORD": "mot-de-passe-secret", "DASHBOARD_SOURCE": str(flat),
        "BUNDLE_TOOL": str(FILES / "superset_bundle.py"), "DASHBOARD_SCHEMA": "demo_dashboard",
        "DASHBOARD_DEFAULT_YEAR": "2021", "WAIT_SECONDS": "1",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("DASHBOARD_MENU_DEFAULTS", raising=False)
    assert importer.main() == 0
    # Une requête de données par menu, sur le dataset cell_scores
    assert len(server.data_requests) == 3
    assert {request["datasource"]["id"] for request in server.data_requests} == {1}
    # Second import seulement si les valeurs calculées diffèrent de l'export
    assert len(server.imports) == (2 if recomputed else 1)
    menus = _menus_of(server.imports[-1])
    assert menus["NATIVE_FILTER-PAYS-EXTRA"] == products
    assert menus["NATIVE_FILTER-PAYS-DETAIL"] == products[:1]
    # Journal sans secret ni valeur de menu
    out = capsys.readouterr().out
    assert "mot-de-passe-secret" not in out and "jeton" not in out and products[0] not in out
    assert "Menus : NATIVE_FILTER-PAYS-DETAIL 1" in out


def test_the_job_keeps_the_export_menus_on_request(tmp_path: Path, monkeypatch) -> None:
    flat = tmp_path / "flat"
    flat.mkdir()
    for relative, content in sync.source_files(FILES / "dashboard").items():
        (flat / relative.replace("/", importer.PATH_SEPARATOR)).write_bytes(content)
    server = _FakeSuperset({})
    monkeypatch.setattr(importer.requests, "Session", server.session())
    for name, value in {
        "SUPERSET_URL": "http://superset:8088", "SUPERSET_ADMIN_USERNAME": "admin",
        "SUPERSET_ADMIN_PASSWORD": "x", "DASHBOARD_SOURCE": str(flat),
        "BUNDLE_TOOL": str(FILES / "superset_bundle.py"), "DASHBOARD_MENU_DEFAULTS": "export",
    }.items():
        monkeypatch.setenv(name, value)
    assert importer.main() == 0
    assert len(server.imports) == 1 and server.data_requests == []
    monkeypatch.setenv("DASHBOARD_MENU_DEFAULTS", "autre")
    with pytest.raises(RuntimeError, match="DASHBOARD_MENU_DEFAULTS"):
        importer.main()
