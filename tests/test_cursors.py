"""Curseurs DuckLake et marques « données modifiées » (socle de la parcimonie aval).

Comportement figé :

- un curseur absent, d'empreinte modifiée, invalidé ou dont les snapshots ont
  expiré se replie sur un recalcul complet ;
- un curseur à jour ne voit que les clés des lignes insérées, mises à jour ou
  supprimées depuis lui, jusqu'au snapshot épinglé ;
- la maintenance connaît le plus ancien curseur de chaque catalogue ;
- une unité marquée ``pending`` est planifiée (raison ``pending``) jusqu'à son
  enregistrement après succès ;
- la règle ``upstream_mark_is_newer`` compare les snapshots, et les dates pour
  les entrées enregistrées avant la bascule (jamais de recalcul parasite à la
  bascule, puisque ``last_modified <= last_download``) ;
- la vue du registre de téléchargement expose les marques par couple et par lot.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from kedro_pipeline.io.cursors import (
    FALLBACK_ABSENT,
    FALLBACK_FINGERPRINT,
    FALLBACK_UNAVAILABLE,
    StepCursors,
    current_snapshot,
    cursor_fingerprint,
    decision_mode,
    oldest_cursors,
)
from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    RegistryEntry,
    Unit,
    UpstreamMark,
    mark_pending,
    parse_instant,
    units_to_compute,
    upstream_mark_is_newer,
)
from kedro_pipeline.io.registry_views import DownloadRegistryView

KEYS = ["geo", "TIME_PERIOD"]
BASE = {("FR", "2020"): 1.0, ("FR", "2021"): 2.0, ("DE", "2022"): 3.0}


def _write(conn, alias: str, values) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    frame = pd.DataFrame(
        [{"geo": geo, "TIME_PERIOD": period, "OBS_VALUE": value} for (geo, period), value in values.items()]
    )
    write_dataframe(conn, frame, KEYS, catalog_alias=alias, schema="s1")


def _cursors(root: Path, step: str = "demo") -> StepCursors:
    registry = FreshnessRegistry(
        f"{root.as_posix()}/cursors/{{step}}.json", None, "cursors",
        shard_of=lambda unit: unit.get("step"),
    )
    return StepCursors(registry, step)


def _read(cursors: StepCursors, conn, alias: str, pinned=None, fingerprint="f"):
    return cursors.read(
        conn, alias, "s1", fingerprint=fingerprint, key_columns=["geo"],
        until_snapshot=pinned if pinned is not None else current_snapshot(conn, alias),
    )


def test_cursor_lifecycle(ducklake_conn, tmp_path: Path) -> None:
    pytest.importorskip("dt_ducklake_manager")
    conn, alias = ducklake_conn
    _write(conn, alias, BASE)
    cursors = _cursors(tmp_path)

    # Premier passage : repli « absent », puis curseur initialisé au snapshot épinglé
    first = _read(cursors, conn, alias)
    assert first.fallback == FALLBACK_ABSENT and first.full and first.modified
    cursors.advance(first, fingerprint="f")
    cursors.save()
    assert _cursors(tmp_path).snapshot(alias, "s1") == first.target

    # Upsert identique : snapshot créé, aucune ligne modifiée
    _write(conn, alias, BASE)
    unchanged = _read(_cursors(tmp_path), conn, alias)
    assert not unchanged.full and not unchanged.modified and unchanged.changed_keys == frozenset()

    # Révision et suppression : seules les clés touchées, jusqu'au snapshot épinglé
    _write(conn, alias, {**BASE, ("FR", "2021"): 2.5})
    pinned = current_snapshot(conn, alias)
    conn.execute(f"DELETE FROM {alias}.s1.fact_table WHERE geo = 'DE'")
    changed = _read(_cursors(tmp_path), conn, alias, pinned=pinned)
    assert changed.changed_keys == frozenset({("FR",)}) and changed.target == pinned
    after = _read(_cursors(tmp_path), conn, alias)
    assert after.changed_keys == frozenset({("FR",), ("DE",)})

    # Empreinte modifiée : repli complet
    assert _read(_cursors(tmp_path), conn, alias, fingerprint="g").fallback == FALLBACK_FINGERPRINT

    # Invalidation (--reset-cursor) : repli complet, snapshot conservé pour la maintenance
    reset = _cursors(tmp_path)
    assert reset.invalidate() == [reset.unit(alias, "s1")]
    reset.save()
    assert _read(_cursors(tmp_path), conn, alias).fallback == FALLBACK_FINGERPRINT
    assert oldest_cursors(_cursors(tmp_path).registry) == {alias: first.target}


def test_expired_snapshots_fall_back_to_a_full_recomputation(ducklake_conn, tmp_path: Path) -> None:
    pytest.importorskip("dt_ducklake_manager")
    conn, alias = ducklake_conn
    _write(conn, alias, BASE)
    cursors = _cursors(tmp_path)
    cursors.advance(_read(cursors, conn, alias), fingerprint="f")
    _write(conn, alias, {**BASE, ("FR", "2020"): 9.0})
    _write(conn, alias, {**BASE, ("FR", "2020"): 8.0})
    conn.execute(f"CALL ducklake_expire_snapshots('{alias}', older_than => now() + INTERVAL 1 DAY)")

    assert _read(cursors, conn, alias).fallback == FALLBACK_UNAVAILABLE


def test_cursor_fingerprint_and_decision_mode() -> None:
    assert cursor_fingerprint("s", ["a"], on=["x"]) != cursor_fingerprint("s", ["a"], on=["y"])
    assert decision_mode({"DECISION_MODE": "SNAPSHOT"}) == "snapshot"
    with pytest.raises(ValueError, match="DECISION_MODE"):
        decision_mode({"DECISION_MODE": "fast"})


def test_pending_units_are_planned_until_recorded(tmp_path: Path) -> None:
    registry = FreshnessRegistry(f"{tmp_path.as_posix()}/{{v}}.json", None, "demo", lambda u: u.key)
    done, fresh = Unit.of(v="a"), Unit.of(v="b")
    t0 = parse_instant("2026-01-01")
    for unit in (done, fresh):
        registry.upsert(RegistryEntry(unit, t0, upstream_watermark=t0, fingerprints={"m": "1"}))
    assert mark_pending(registry, [done, Unit.of(v="new")], 42) == 2
    registry.save()

    reread = FreshnessRegistry(f"{tmp_path.as_posix()}/{{v}}.json", None, "demo", lambda u: u.key)
    plans = units_to_compute([done, fresh, Unit.of(v="new")], reread, {}, {"m": "1"}, ForceSpec(), step="demo")
    assert {unit.key: plan.reason for unit, plan in plans.items()} == {"a": "pending", "new": "first"}
    assert reread.get(done).pending_since == 42
    # Enregistrement après succès : une nouvelle entrée efface le marqueur
    reread.upsert(RegistryEntry(done, t0, fingerprints={"m": "1"}, reason="pending"))
    assert units_to_compute([done], reread, {}, {"m": "1"}, ForceSpec(), step="demo") == {}


def test_mark_rule_compares_snapshots_and_never_recomputes_at_the_switch() -> None:
    t_download, t_modified = parse_instant("2026-10-14T03:00"), parse_instant("2026-10-02T03:00")
    unit = Unit.of(v="a")
    # Entrée enregistrée avant la bascule : watermark = last_download
    before = RegistryEntry(unit, t_download, upstream_watermark=t_download)
    assert not upstream_mark_is_newer(unit, before, UpstreamMark(t_modified, 18, None))
    assert upstream_mark_is_newer(unit, before, UpstreamMark(parse_instant("2026-10-15"), 19, None))
    # Entrée enregistrée avec un snapshot : comparaison des snapshots, puis des dates héritées
    after = RegistryEntry(unit, t_download, upstream_watermark=t_modified, extra={"upstream_snapshot": 18})
    assert not upstream_mark_is_newer(unit, after, UpstreamMark(t_modified, 18, None))
    assert upstream_mark_is_newer(unit, after, UpstreamMark(t_modified, 19, None))
    assert upstream_mark_is_newer(unit, after, UpstreamMark(t_modified, 18, parse_instant("2026-10-03")))
    # Aucune donnée jamais écrite : jamais périmé
    assert not upstream_mark_is_newer(unit, after, UpstreamMark())


def test_registry_view_exposes_data_modification_marks(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    entries = {
        "q1": {"agency": "ESTAT", "dataflow": "DS", "last_download": "2026-10-14T03:00:00+00:00",
               "last_modified": "2026-10-02T03:00:00+00:00", "last_modified_snapshot": 12,
               "params": {"dimensions": {"reporter": "FR", "product": "01+02"}}},
        "q2": {"agency": "ESTAT", "dataflow": "DS", "last_download": "2026-10-14T03:00:00+00:00",
               "params": {"dimensions": {"reporter": "FR", "product": "02"}}},
        "q3": {"agency": "ESTAT", "dataflow": "DS", "last_download": "2026-10-14T03:00:00+00:00",
               "last_modified": None, "last_modified_snapshot": None,
               "params": {"dimensions": {"reporter": "DE", "product": "01"}}},
        "c1": {"agency": "COMTRADE", "dataflow": "C", "last_download": "2026-10-14T03:00:00+00:00",
               "last_modified": "2026-10-01T03:00:00+00:00", "last_modified_snapshot": 7,
               "params": {"periods": [2022, 2023], "products": ["01"]}},
    }
    path.write_text(json.dumps({"DOWNLOADS": entries}), encoding="utf-8")
    view = DownloadRegistryView(path)

    marks = view.pairs_last_modified()
    assert marks[("FR", "01")] == UpstreamMark(parse_instant("2026-10-02T03:00"), 12, None)
    # Entrée héritée (sans last_modified) : date de téléchargement, snapshot inconnu
    legacy = parse_instant("2026-10-14T03:00")
    assert marks[("FR", "02")] == UpstreamMark(legacy, 12, legacy)
    # Requête toujours vide : marque vide, couple conservé (mêmes unités que last_download)
    assert marks[("DE", "01")].is_empty
    assert set(marks) == set(view.pairs_last_download())
    assert view.batches_marks_by_year()[2023][("01",)].snapshot == 7
