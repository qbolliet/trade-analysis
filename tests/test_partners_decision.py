"""Décision des unités partenaires sur les données réellement modifiées (sans base de données).

Comportement figé :

- une requête rejouée sans nouvelle donnée (même ``last_modified_snapshot``, date
  de téléchargement plus récente) ne rend aucune unité périmée ;
- seul un couple dont les données ont été modifiées (snapshot qui avance) est
  recalculé, les autres couples du même reporter restant à jour ;
- l'entrée calculée enregistre le snapshot amont pris en compte ;
- sans snapshot connu (écriture sans rapport), la date de modification décide.
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
T_REVISED = parse_instant("2026-10-14T03:00")
T_COMPUTED = parse_instant("2026-10-15T03:00")


class _Downloads:
    """Vue factice du registre de téléchargement : marques de modification des couples."""

    def __init__(self, marks: Dict[Tuple[str, str], UpstreamMark]):
        self.marks = marks

    def pairs_last_modified(self):
        return dict(self.marks)


def _block(root: Path) -> dict:
    return {
        "BUCKET": None,
        "STATE": {"PATH_TEMPLATE": f"{root.as_posix()}/{{classification}}/{{reporter}}.json"},
    }


def _plan(root: Path, downloads: _Downloads):
    block = _block(root)
    state = partner_registry(block)
    plan = plan_partner_passes(
        state, downloads,
        params={"FLOWS": ["import"], "VULNERABILITIES": {"DS": block}},
        runtime={"NOMENCLATURES": {"HS": NOMENCLATURES}}, dataflow="DS",
    )
    return state, plan.passes[0]


def _record(state, partner_pass) -> None:
    record_computed_units(
        state, partner_pass.plans, partner_pass.units, partner_pass.requested, T_COMPUTED,
    )
    state.save()


def test_replayed_query_without_new_data_marks_no_unit_stale(tmp_path: Path) -> None:
    pairs = [("FR", "01"), ("FR", "02")]
    # Premier calcul : données écrites au premier téléchargement (snapshot 10)
    first = _Downloads({pair: UpstreamMark(T_FIRST, 10) for pair in pairs})
    state, initial = _plan(tmp_path, first)
    assert {plan.reason for plan in initial.plans.values()} == {"first"}
    _record(state, initial)

    # Requêtes rejouées : aucune donnée modifiée, les marques sont inchangées
    assert _plan(tmp_path, first)[1].plans == {}


def test_only_the_pair_with_modified_data_is_recomputed(tmp_path: Path) -> None:
    pairs = [("FR", "01"), ("FR", "02")]
    first = _Downloads({pair: UpstreamMark(T_FIRST, 10) for pair in pairs})
    state, initial = _plan(tmp_path, first)
    _record(state, initial)

    # FR/02 révisé (snapshot 11) ; FR/01 inchangé
    revised = _Downloads({("FR", "01"): UpstreamMark(T_FIRST, 10), ("FR", "02"): UpstreamMark(T_REVISED, 11)})
    plans = _plan(tmp_path, revised)[1].plans
    assert [unit.get("product") for unit in plans] == ["02"]
    assert {plan.reason for plan in plans.values()} == {"new_data"}


def test_computed_entry_records_the_upstream_snapshot(tmp_path: Path) -> None:
    pair = ("DE", "03")
    state, first = _plan(tmp_path, _Downloads({pair: UpstreamMark(T_FIRST, 20)}))
    _record(state, first)
    entry = partner_registry(_block(tmp_path)).get(next(iter(first.plans)))
    assert entry.extra["upstream_snapshot"] == 20 and entry.upstream_watermark == T_FIRST

    # Même snapshot : rien
    assert _plan(tmp_path, _Downloads({pair: UpstreamMark(T_FIRST, 20)}))[1].plans == {}
    # Snapshot plus récent : recalcul
    assert len(_plan(tmp_path, _Downloads({pair: UpstreamMark(T_REVISED, 21)}))[1].plans) == 1


def test_date_decides_when_no_snapshot_is_known(tmp_path: Path) -> None:
    pair = ("IT", "04")
    # Écriture sans rapport : date de modification connue, snapshot inconnu
    state, first = _plan(tmp_path, _Downloads({pair: UpstreamMark(T_FIRST, None)}))
    _record(state, first)
    entry = partner_registry(_block(tmp_path)).get(next(iter(first.plans)))
    assert "upstream_snapshot" not in entry.extra

    assert _plan(tmp_path, _Downloads({pair: UpstreamMark(T_FIRST, None)}))[1].plans == {}
    assert len(_plan(tmp_path, _Downloads({pair: UpstreamMark(T_REVISED, None)}))[1].plans) == 1
