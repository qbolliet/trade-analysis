"""Décision des unités partenaires selon ``STATE.DECISION_MODE`` (sans base de données).

Comportement figé :

- ``legacy`` : un couple re-téléchargé (``last_download``) est recalculé, même si
  ses données n'ont pas changé ;
- ``snapshot`` : seul un couple dont les données ont été modifiées
  (``last_modified_snapshot``) est recalculé ; la bascule depuis des entrées
  enregistrées en ``legacy`` ne provoque aucun recalcul parasite ;
- ``shadow`` : la décision ``legacy`` est exécutée, l'écart avec ``snapshot`` est
  mesuré, et les entrées enregistrent le snapshot amont.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

from kedro_pipeline.io.freshness import UpstreamMark, parse_instant
from kedro_pipeline.steps.partners import (
    partner_registry,
    plan_partner_passes,
    record_computed_units,
)

NOMENCLATURES = {"HS2017": 2017, "HS2022": 2022}
T_FIRST = parse_instant("2026-10-01T03:00")
T_DOWNLOAD = parse_instant("2026-10-14T03:00")
T_REVISED = parse_instant("2026-10-15T03:00")


class _Downloads:
    """Vue factice du registre : dates de téléchargement et marques de modification."""

    def __init__(self, downloads: Dict[Tuple[str, str], object], marks: Dict[Tuple[str, str], UpstreamMark]):
        self.downloads, self.marks = downloads, marks

    def pairs_last_download(self):
        return dict(self.downloads)

    def pairs_last_modified(self):
        return dict(self.marks)


def _block(root: Path, mode: str) -> dict:
    return {
        "BUCKET": None, "PATHS": {},
        "STATE": {"PATH_TEMPLATE": f"{root.as_posix()}/{{classification}}/{{reporter}}.json",
                  "DECISION_MODE": mode},
    }


def _plan(root: Path, mode: str, downloads: _Downloads):
    block = _block(root, mode)
    state = partner_registry(block, "HS2022")
    plan = plan_partner_passes(
        state, downloads,
        params={"FLOWS": ["import"], "VULNERABILITIES": {"DS": block}},
        runtime={"NOMENCLATURES": {"HS": NOMENCLATURES}}, dataflow="DS",
    )
    return state, plan.passes[0]


def _record(state, partner_pass) -> None:
    record_computed_units(
        state, partner_pass.plans, partner_pass.units, partner_pass.requested, T_DOWNLOAD,
        partner_pass.marks,
    )
    state.save()


def test_switch_from_legacy_to_snapshot_recomputes_only_modified_data(tmp_path: Path) -> None:
    pairs = [("FR", "01"), ("FR", "02")]
    # Premier calcul en legacy, données écrites au premier téléchargement (snapshot 10)
    first = _Downloads({pair: T_FIRST for pair in pairs},
                       {pair: UpstreamMark(T_FIRST, 10, None) for pair in pairs})
    state, legacy = _plan(tmp_path, "legacy", first)
    assert {plan.reason for plan in legacy.plans.values()} == {"first"}
    _record(state, legacy)

    # Re-téléchargement sans changement, sauf FR/02 révisé (snapshot 11)
    later = _Downloads(
        {pair: T_DOWNLOAD for pair in pairs},
        {("FR", "01"): UpstreamMark(T_FIRST, 10, None), ("FR", "02"): UpstreamMark(T_DOWNLOAD, 11, None)},
    )
    _, legacy_again = _plan(tmp_path, "legacy", later)
    assert len(legacy_again.plans) == 2  # faux positif de last_download
    _, snapshot = _plan(tmp_path, "snapshot", later)
    assert [unit.get("product") for unit in snapshot.plans] == ["02"]
    assert {plan.reason for plan in snapshot.plans.values()} == {"new_data"}


def test_snapshot_mode_records_and_compares_snapshots(tmp_path: Path) -> None:
    pair = ("DE", "03")
    downloads = _Downloads({pair: T_DOWNLOAD}, {pair: UpstreamMark(T_DOWNLOAD, 20, None)})
    state, first = _plan(tmp_path, "snapshot", downloads)
    _record(state, first)
    entry = partner_registry(_block(tmp_path, "snapshot"), "HS2022").get(next(iter(first.plans)))
    assert entry.extra["upstream_snapshot"] == 20 and entry.upstream_watermark == T_DOWNLOAD

    # Même snapshot, date de téléchargement plus récente : rien
    unchanged = _Downloads({pair: T_REVISED}, {pair: UpstreamMark(T_DOWNLOAD, 20, None)})
    assert _plan(tmp_path, "snapshot", unchanged)[1].plans == {}
    # Snapshot plus récent : recalcul
    revised = _Downloads({pair: T_REVISED}, {pair: UpstreamMark(T_DOWNLOAD, 21, None)})
    assert len(_plan(tmp_path, "snapshot", revised)[1].plans) == 1


def test_shadow_mode_executes_legacy_and_measures_the_gap(tmp_path: Path) -> None:
    pairs = [("IT", "04"), ("IT", "05")]
    first = _Downloads({pair: T_FIRST for pair in pairs},
                       {pair: UpstreamMark(T_FIRST, 30, None) for pair in pairs})
    state, initial = _plan(tmp_path, "shadow", first)
    _record(state, initial)
    # Snapshot enregistré même en shadow : la bascule compare des snapshots d'emblée
    entry = partner_registry(_block(tmp_path, "shadow"), "HS2022").get(next(iter(initial.plans)))
    assert entry.extra["upstream_snapshot"] == 30 and entry.upstream_watermark == T_FIRST

    later = _Downloads({pair: T_DOWNLOAD for pair in pairs},
                       {pair: UpstreamMark(T_FIRST, 30, None) for pair in pairs})
    _, shadow = _plan(tmp_path, "shadow", later)
    assert len(shadow.plans) == 2  # décision legacy exécutée
    assert shadow.shadow == {
        "freshness/shadow_only_legacy": 2.0,
        "freshness/shadow_only_new": 0.0,
        "freshness/shadow_both": 0.0,
    }
