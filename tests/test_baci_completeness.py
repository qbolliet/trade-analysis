"""Tests de la porte de complétude et du périmètre BACI (``scripts/process_baci_hs.py``, PS-14.1).

Le registre de téléchargement fictif est écrit au format exact de
``statflows.core.download.SDMXDownloader`` (racine ``DOWNLOADS``, entrées
``params = query.to_dict()``) dans un fichier local, et relu par
``DownloadRegistryView`` (fichier unique, puis fragments). La lecture SQL filtrée
est exercée sur une base DuckDB en mémoire.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd
import pytest

from statflows import ComtradeQueryRequest
from statflows.core.download import _json_safe

from scripts.download_comtrade import build_split_queries
from scripts.process_baci_hs import (
    _read_comtrade_fact_table,
    _share_min,
    completeness_by_year,
    eligible_years,
    is_provisional_scope,
    resolve_target_start_years,
)
from kedro_pipeline.io.registry_views import DownloadRegistryView


_NULL = {"include": None, "include_regex": None, "exclude": None, "exclude_regex": None}


def _planned() -> List[ComtradeQueryRequest]:
    """Liste planifiée fictive : 3 années x 2 lots de 2 produits."""
    return build_split_queries(
        "C_A_HS",
        {
            "reporters": pd.DataFrame({"code": ["251"]}),
            "products": pd.DataFrame({"code": ["010121", "010129", "854140", "854150"]}),
        },
        {"frequency": "annual", "flows": ["M", "X"]},
        {"reporters": dict(_NULL), "products": {**_NULL, "include_regex": r"^\d{6}$"}},
        periods=["2022", "2023", "2024"],
        products_step=2,
    )


def _entry(query: ComtradeQueryRequest, last_download: Optional[str], dataflow: str = "C_A_HS") -> Dict:
    """Entrée de registre au format de ``SDMXDownloader._process_query``."""
    return {
        "agency": query.agency,
        "dataflow": dataflow,
        "params": _json_safe(query.to_dict()),
        "last_download": last_download,
    }


@pytest.fixture
def registry_path(tmp_path: Path) -> Path:
    """Registre fictif : 2024 complète, 2023 à moitié, 2022 absente (+ bruit)."""
    planned = _planned()
    by_year = {y: [q for q in planned if q.periods == y] for y in ("2022", "2023", "2024")}
    stamp = "2026-09-16T01:47:02+00:00"
    downloads = {}
    # 2024 : tous les lots
    for q in by_year["2024"]:
        downloads[q.identity_key()] = _entry(q, stamp)
    # 2023 : un lot sur deux, l'autre enregistré sans date (jamais abouti)
    downloads[by_year["2023"][0].identity_key()] = _entry(by_year["2023"][0], stamp)
    downloads[by_year["2023"][1].identity_key()] = _entry(by_year["2023"][1], None)
    # 2022 : seulement une entrée d'un autre dataflow (ignorée)
    downloads["other"] = _entry(by_year["2022"][0], stamp, dataflow="C_M_HS")
    path = tmp_path / "last_downloads" / "comtrade.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"DOWNLOADS": downloads}), encoding="utf-8")
    return path


def _batches(path: Path) -> Dict:
    """Lots téléchargés par année, vus par la vue du registre (dataflow C_A_HS)."""
    return DownloadRegistryView(path, bucket=None, dataflow="C_A_HS").batches_by_year()


def test_batches_missing_registry(tmp_path: Path) -> None:
    """Registre absent (premier passage) → aucun lot, aucune année complète."""
    batches = _batches(tmp_path / "absent.json")
    assert batches == {}
    assert completeness_by_year(_planned(), batches) == {2024: 0.0, 2023: 0.0, 2022: 0.0}


def test_completeness_by_year_on_fictive_registry(registry_path: Path) -> None:
    """Part des lots planifiés téléchargés au moins une fois, par année."""
    shares = completeness_by_year(_planned(), _batches(registry_path))
    assert shares == {2024: 1.0, 2023: 0.5, 2022: 0.0}


def test_completeness_by_year_on_sharded_registry(registry_path: Path, tmp_path: Path) -> None:
    """Même résultat quand le registre est fragmenté par année (format de statflows 0.1.1)."""
    entries = json.loads(registry_path.read_text(encoding="utf-8"))["DOWNLOADS"]
    shard_dir = tmp_path / "sharded" / "comtrade"
    shard_dir.mkdir(parents=True)
    by_shard: Dict[str, Dict] = {}
    for key, entry in entries.items():
        periods = (entry.get("params") or {}).get("periods")
        by_shard.setdefault(str(periods), {})[key] = entry
    for shard, shard_entries in by_shard.items():
        (shard_dir / f"{shard}.json").write_text(json.dumps({"DOWNLOADS": shard_entries}), encoding="utf-8")
    # Chemin du registre unique inexistant : seuls les fragments existent
    shares = completeness_by_year(_planned(), _batches(tmp_path / "sharded" / "comtrade.json"))
    assert shares == {2024: 1.0, 2023: 0.5, 2022: 0.0}


@pytest.mark.parametrize(
    ("min_share", "period_end", "expected"),
    [(1.0, None, [2024]), (0.5, None, [2023, 2024]), (0.5, 2023, [2023]), (0.0, None, [2022, 2023, 2024])],
)
def test_eligible_years(registry_path: Path, min_share: float, period_end: Optional[int], expected: List[int]) -> None:
    """Seuil de complétude et borne haute du périmètre."""
    shares = completeness_by_year(_planned(), _batches(registry_path))
    assert eligible_years(shares, min_share, period_end=period_end) == expected


def test_share_min() -> None:
    """Part minimale sur les années planifiées d'un millésime."""
    shares = {2022: 0.0, 2023: 0.5, 2024: 1.0}
    assert _share_min(shares, 2023, None) == 0.5
    assert _share_min(shares, 2024, None) == 1.0
    assert _share_min(shares, 2030, None) == 0.0


def test_resolve_target_start_years() -> None:
    """START_YEAR nul → max(entrée en vigueur, première année Comtrade), PD-08."""
    runtime = {
        "ANALYSIS_START_YEAR": {"comtrade": 1994, "eurostat": 1988},
        "NOMENCLATURES": {"HS": {"HS1992": 1988, "HS1996": 1996, "HS2017": 2017}},
    }
    targets = {
        "HS2017": {"START_YEAR": 2017},
        "HS1996": {"START_YEAR": None},
        "HS1992": {"START_YEAR": None},
    }
    assert resolve_target_start_years(targets, runtime) == {
        "HS2017": 2017, "HS1996": 1996, "HS1992": 1994,
    }


def test_is_provisional_scope() -> None:
    """Sous-ensemble strict du périmètre HS6 → provisoire ; périmètre complet → non."""
    available = ["01", "0101", "010121", "010129", "854140", "999999"]
    regex = r"^\d{6}$"
    # Profil demo : une partie des HS6 seulement
    assert is_provisional_scope(available, ["854140"], regex, exclude=["999999"])
    # Production : tous les HS6 hors exclusions
    assert not is_provisional_scope(
        available, ["010121", "010129", "854140"], regex, exclude=["999999"]
    )


def test_read_comtrade_fact_table_filters_years_in_sql() -> None:
    """Seules les années éligibles et dans les bornes sont lues."""
    conn = duckdb.connect()
    conn.execute('CREATE SCHEMA "C_A_HS"')
    conn.execute(
        'CREATE TABLE "C_A_HS".fact_table AS SELECT * FROM (VALUES '
        "('2015', 'H4', 1.0), ('2017', 'H5', 2.0), ('2018', 'H5', 3.0), "
        "('2019', 'H5', 4.0), ('2020', 'H5', 5.0)) t(period, classificationCode, primaryValue)"
    )
    df = _read_comtrade_fact_table(
        conn, "C_A_HS", ["period", "primaryValue"], "period",
        years=[2015, 2017, 2018, 2020], period_start=2017, period_end=2018,
    )
    assert sorted(df["period"]) == ["2017", "2018"]
    # Sans bornes : toutes les années éligibles
    df_all = _read_comtrade_fact_table(
        conn, "C_A_HS", ["period"], "period", years=[2015, 2019],
    )
    assert sorted(df_all["period"]) == ["2015", "2019"]
    # Aucune année éligible : table vide, pas d'erreur
    assert _read_comtrade_fact_table(conn, "C_A_HS", ["period"], "period", years=[]).empty
