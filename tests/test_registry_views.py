"""Tests de ``DownloadRegistryView`` (PS-12.3) sur des registres fictifs.

Les registres sont écrits au format de ``statflows`` 0.1.1 : fichier unique
``{"DOWNLOADS": {...}}`` ou fragments ``<chemin sans extension>/<fragment>.json``.
La vue doit donner les mêmes résultats dans les deux formats.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from kedro_pipeline.io.registry_views import (
    DownloadRegistryView,
    period_year,
    products_key,
    split_codes,
)
from scripts.compute_trade_vulnerabilities import load_last_download_dates
from statflows.storage.json import Loader

STAMP_OLD = "2026-09-10T08:00:00+00:00"
STAMP_NEW = "2026-09-16T01:47:02+00:00"


def _dt(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp)


def _eurostat(key: str, reporter: Any, product: Any, last_download: Optional[str]) -> Dict[str, Any]:
    """Entrée Eurostat : paramètres = ``query.to_dict()`` (dimensions imbriquées)."""
    return {
        key: {
            "agency": "ESTAT",
            "dataflow": "DS-045409",
            "params": {"dataflow": "DS-045409", "dimensions": {"reporter": reporter, "product": product}},
            "last_download": last_download,
        }
    }


def _comtrade(key: str, periods: Any, products: Any, last_download: str, dataflow: str = "C_A_HS") -> Dict[str, Any]:
    """Entrée Comtrade : paramètres à plat (pas de ``dimensions``)."""
    return {
        key: {
            "agency": "COMTRADE",
            "dataflow": dataflow,
            "params": {"periods": periods, "products": products},
            "last_download": last_download,
        }
    }


def _write_single(path: Path, entries: Dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"DOWNLOADS": entries}), encoding="utf-8")
    return path


def _write_shards(path: Path, shards: Dict[str, Dict[str, Any]]) -> Path:
    """Fragments sous ``<path sans extension>/`` ; le fichier unique n'est pas créé."""
    directory = path.with_suffix("")
    directory.mkdir(parents=True, exist_ok=True)
    for name, entries in shards.items():
        (directory / f"{name}.json").write_text(json.dumps({"DOWNLOADS": entries}), encoding="utf-8")
    return path


# ──────────────────────────────────────────────────────────────────────
# Fonctions pures
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ()),
        ("2710", ("2710",)),
        ("2710+8541", ("2710", "8541")),
        ("2710, 8541 ,8542", ("2710", "8541", "8542")),
        (["2710", "8541+8542"], ("2710", "8541", "8542")),
        (["2710", "2710"], ("2710",)),
        ("", ()),
    ],
)
def test_split_codes(value: Any, expected: tuple) -> None:
    assert split_codes(value) == expected


def test_products_key_is_order_insensitive() -> None:
    assert products_key(["b", "a"]) == products_key("a,b") == ("a", "b")
    assert products_key(None) is None and products_key([]) is None


@pytest.mark.parametrize(("period", "year"), [("2023", 2023), (202305, 2023), ("2023-05", 2023)])
def test_period_year(period: Any, year: int) -> None:
    assert period_year(period) == year


# ──────────────────────────────────────────────────────────────────────
# Eurostat : couples reporter x produit
# ──────────────────────────────────────────────────────────────────────


def _eurostat_entries() -> Dict[str, Any]:
    entries: Dict[str, Any] = {}
    entries.update(_eurostat("k1", "FR", "854140", STAMP_NEW))
    entries.update(_eurostat("k2", "DE", ["854140", "854150"], STAMP_OLD))
    entries.update(_eurostat("k3", ["FR", "IT"], "2710+2711", STAMP_NEW))
    entries.update(_eurostat("k4", "ES", "854140", None))  # jamais abouti : ignoré
    entries.update(_eurostat("k5", "FR", None, STAMP_NEW))  # sans produit : ignoré
    return entries


EXPECTED_PAIRS = {
    ("FR", "854140"): _dt(STAMP_NEW),
    ("DE", "854140"): _dt(STAMP_OLD),
    ("DE", "854150"): _dt(STAMP_OLD),
    ("FR", "2710"): _dt(STAMP_NEW),
    ("FR", "2711"): _dt(STAMP_NEW),
    ("IT", "2710"): _dt(STAMP_NEW),
    ("IT", "2711"): _dt(STAMP_NEW),
}


def test_pairs_last_download_single_file(tmp_path: Path) -> None:
    path = _write_single(tmp_path / "comext.json", _eurostat_entries())
    assert DownloadRegistryView(path).pairs_last_download() == EXPECTED_PAIRS


def test_pairs_last_download_fragments(tmp_path: Path) -> None:
    entries = _eurostat_entries()
    path = _write_shards(
        tmp_path / "comext.json",
        {
            "FR": {k: entries[k] for k in ("k1", "k3", "k5")},
            "DE": {"k2": entries["k2"]},
            "ES": {"k4": entries["k4"]},
        },
    )
    assert not path.exists()
    assert DownloadRegistryView(path).pairs_last_download() == EXPECTED_PAIRS


def test_pairs_last_download_keeps_most_recent_when_layouts_overlap(tmp_path: Path) -> None:
    """Migration en cours : l'entrée la plus récente l'emporte, quel que soit le format."""
    path = _write_single(tmp_path / "comext.json", _eurostat("k1", "FR", "854140", STAMP_OLD))
    _write_shards(path, {"FR": _eurostat("k1", "FR", "854140", STAMP_NEW)})
    assert DownloadRegistryView(path).pairs_last_download() == {("FR", "854140"): _dt(STAMP_NEW)}


def test_pairs_last_download_accepts_flat_params(tmp_path: Path) -> None:
    """Paramètres sans clé ``dimensions`` (ancien format) lus à plat."""
    entry = {
        "k": {
            "agency": "ESTAT",
            "dataflow": "DS-045409",
            "params": {"reporter": "FR", "product": "854140"},
            "last_download": STAMP_NEW,
        }
    }
    path = _write_single(tmp_path / "comext.json", entry)
    assert DownloadRegistryView(path).pairs_last_download() == {("FR", "854140"): _dt(STAMP_NEW)}


def test_missing_registry_is_empty(tmp_path: Path) -> None:
    view = DownloadRegistryView(tmp_path / "absent.json")
    assert view.pairs_last_download() == {}
    assert view.batches_by_year() == {}


def test_load_last_download_dates_wrapper(tmp_path: Path) -> None:
    """Signature et type de retour historiques de ``load_last_download_dates``."""
    path = _write_single(tmp_path / "comext.json", _eurostat_entries())
    assert load_last_download_dates(path, Loader(), None) == EXPECTED_PAIRS
    assert load_last_download_dates(tmp_path / "absent.json", Loader(), None) == {}


# ──────────────────────────────────────────────────────────────────────
# Comtrade : lots par année
# ──────────────────────────────────────────────────────────────────────


def _comtrade_entries() -> Dict[str, Any]:
    entries: Dict[str, Any] = {}
    entries.update(_comtrade("c1", "2024", ["854150", "854140"], STAMP_NEW))
    entries.update(_comtrade("c2", "2024", ["010121", "010129"], STAMP_OLD))
    entries.update(_comtrade("c3", ["2023", "2022"], "010121,010129", STAMP_OLD))
    entries.update(_comtrade("c4", "2023", None, STAMP_NEW))
    entries.update(_comtrade("c5", "2024", ["854140"], STAMP_NEW, dataflow="C_M_HS"))
    return entries


def test_batches_by_year_single_file(tmp_path: Path) -> None:
    path = _write_single(tmp_path / "comtrade.json", _comtrade_entries())
    batches = DownloadRegistryView(path, dataflow="C_A_HS").batches_by_year()
    assert batches == {
        2024: {("854140", "854150"): _dt(STAMP_NEW), ("010121", "010129"): _dt(STAMP_OLD)},
        2023: {("010121", "010129"): _dt(STAMP_OLD), None: _dt(STAMP_NEW)},
        2022: {("010121", "010129"): _dt(STAMP_OLD)},
    }


def test_batches_by_year_fragments_match_single_file(tmp_path: Path) -> None:
    entries = _comtrade_entries()
    single = _write_single(tmp_path / "single" / "comtrade.json", entries)
    sharded = _write_shards(
        tmp_path / "sharded" / "comtrade.json",
        {
            "2024": {k: entries[k] for k in ("c1", "c2", "c5")},
            "2023_2022": {"c3": entries["c3"]},
            "2023": {"c4": entries["c4"]},
        },
    )
    expected = DownloadRegistryView(single, dataflow="C_A_HS").batches_by_year()
    assert DownloadRegistryView(sharded, dataflow="C_A_HS").batches_by_year() == expected


def test_batches_by_year_without_dataflow_filter_keeps_every_dataflow(tmp_path: Path) -> None:
    path = _write_single(tmp_path / "comtrade.json", _comtrade_entries())
    batches = DownloadRegistryView(path).batches_by_year()
    assert batches[2024][("854140",)] == _dt(STAMP_NEW)


def test_entries_without_last_download_are_skipped(tmp_path: Path) -> None:
    entries = _comtrade("c1", "2024", "854140", STAMP_NEW)
    entries["c1"]["last_download"] = None
    path = _write_single(tmp_path / "comtrade.json", entries)
    assert DownloadRegistryView(path).batches_by_year() == {}
