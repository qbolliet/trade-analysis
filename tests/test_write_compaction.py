"""Aucune écriture du pipeline ne compacte à la volée : la maintenance quotidienne s'en charge.

Une écriture avec ``compact_after_update=True`` réécrit des fichiers et crée un snapshot de plus à
chaque lot (mesure sur 200 petits upserts : trois fois plus de snapshots, sans gain de temps). La valeur
est posée explicitement à chaque écriture plutôt que laissée au défaut de la bibliothèque.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ("kedro_pipeline", "tradestat", "scripts", "tools")


def _calls(name: str):
    for source in SOURCES:
        for path in sorted((ROOT / source).rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                function = node.func
                called = function.attr if isinstance(function, ast.Attribute) else getattr(function, "id", None)
                if called == name:
                    yield path.relative_to(ROOT).as_posix(), node


def test_every_ducklake_write_sets_the_compaction_option() -> None:
    calls = list(_calls("write_dataframe"))
    assert calls, "aucun appel à write_dataframe trouvé : le test ne vérifie plus rien"
    missing = [
        f"{path}:{node.lineno}"
        for path, node in calls
        # `update_options=` explicite, ou options transmises en bloc (`**options`)
        if not any(keyword.arg in ("update_options", None) for keyword in node.keywords)
    ]
    assert not missing, f"écriture sans compact_after_update explicite : {missing}"


def test_no_write_compacts_after_update() -> None:
    offenders = []
    for path in sorted(p for source in SOURCES for p in (ROOT / source).rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword) and node.arg == "compact_after_update":
                if isinstance(node.value, ast.Constant) and node.value.value is True:
                    offenders.append(f"{path.relative_to(ROOT).as_posix()}:{node.value.lineno}")
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    if (isinstance(key, ast.Constant) and key.value == "compact_after_update"
                            and isinstance(value, ast.Constant) and value.value is True):
                        offenders.append(f"{path.relative_to(ROOT).as_posix()}:{key.lineno}")
    assert not offenders, f"compact_after_update=True : {offenders}"
