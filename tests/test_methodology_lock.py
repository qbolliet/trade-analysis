"""Verrou méthodologique : contrôle de CI et effet sur les empreintes.

Comportement figé :

- le verrou versionné ``kedro_pipeline/methodology.lock`` est à jour : toute
  modification du code d'un nom méthodologique doit être tranchée
  (``lock --bump`` ou ``lock --accept``) avant de passer la CI ;
- l'empreinte du code ignore commentaires, docstrings et mise en forme ;
- ``--bump`` incrémente la révision, ``--accept`` ne met à jour que l'empreinte ;
- la révision n'entre dans les empreintes de fraîcheur que si elle est positive.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from kedro_pipeline import methodology
from kedro_pipeline.methodology import (
    METHODOLOGY_SOURCES,
    code_hash,
    load_lock,
    revised,
    source_hash,
    stale_names,
    stale_message,
    update_lock,
)


def test_lock_is_up_to_date() -> None:
    stale = stale_names(load_lock())
    assert stale == {}, stale_message(stale)


def test_every_declared_source_exists() -> None:
    for specs in METHODOLOGY_SOURCES.values():
        for spec in specs:
            assert len(source_hash(spec)) == 16


def _module(root: Path, body: str) -> None:
    (root / "m.py").write_text(body, encoding="utf-8")


def test_hash_ignores_comments_docstrings_and_layout(tmp_path: Path) -> None:
    _module(tmp_path, 'def f(x):\n    return x + 1\n\nclass C:\n    pass\n')
    reference = source_hash("m.py::f", tmp_path)
    _module(
        tmp_path,
        '"""Module."""\n# commentaire\n\n\ndef f(x):  # commentaire\n    """Doc."""\n'
        '    return (x\n            + 1)\n\nclass C:\n    pass\n',
    )
    assert source_hash("m.py::f", tmp_path) == reference
    _module(tmp_path, 'def f(x):\n    return x + 2\n\nclass C:\n    pass\n')
    assert source_hash("m.py::f", tmp_path) != reference


def test_bump_and_accept(tmp_path: Path) -> None:
    sources = {"F": ("m.py::f",), "C": ("m.py::C",)}
    _module(tmp_path, 'def f(x):\n    return x\n\nclass C:\n    pass\n')
    lock = update_lock({}, root=tmp_path, sources=sources)
    assert stale_names(lock, tmp_path, sources) == {}
    assert {name: entry["revision"] for name, entry in lock.items()} == {"F": 0, "C": 0}

    _module(tmp_path, 'def f(x):\n    return 2 * x\n\nclass C:\n    a = 1\n')
    assert stale_names(lock, tmp_path, sources) == {"F": "changed", "C": "changed"}
    lock = update_lock(lock, bump=["F"], accept=["C"], root=tmp_path, sources=sources)

    assert stale_names(lock, tmp_path, sources) == {}
    assert (lock["F"]["revision"], lock["C"]["revision"]) == (1, 0)
    with pytest.raises(ValueError, match="both"):
        update_lock(lock, bump=["F"], accept=["F"], root=tmp_path, sources=sources)
    with pytest.raises(ValueError, match="Unknown"):
        update_lock(lock, bump=["X"], root=tmp_path, sources=sources)


def test_revision_enters_the_fingerprints_only_when_positive(monkeypatch) -> None:
    from tradestat.trade.vulnerabilities import VulnerabilityConfig

    from kedro_pipeline.steps.partners import partner_requested

    before = partner_requested(VulnerabilityConfig())
    assert revised("HHI", {"a": 1}, lock={"HHI": {"revision": 0}}) == {"a": 1}

    # Révision de HHI incrémentée : seule son empreinte change
    lock = {name: dict(entry) for name, entry in load_lock().items()}
    lock["HHI"]["revision"] = 1
    monkeypatch.setattr(methodology, "load_lock", lambda path=None: lock)
    after = partner_requested(VulnerabilityConfig())

    assert after["HHI/import"] != before["HHI/import"]
    assert after["CDI2/import"] == before["CDI2/import"]


def test_code_hash_depends_on_every_source(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("x = 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("y = 1\n", encoding="utf-8")
    sources = {"N": ("a.py", "b.py")}
    reference = code_hash("N", tmp_path, sources)
    (tmp_path / "b.py").write_text("y = 2\n", encoding="utf-8")
    assert code_hash("N", tmp_path, sources) != reference
