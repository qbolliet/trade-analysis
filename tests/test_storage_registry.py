"""Tests de caractérisation — ``macroforecast.storage.registry``.

Comportement figé : ``read_json`` / ``write_json`` / ``read_registry`` /
``merge_registry``, en local et sur S3 simulé (moto).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from macroforecast.storage import Loader, Saver
from macroforecast.storage.registry import (
    merge_registry,
    read_json,
    read_registry,
    write_json,
)


# ──────────────────────────────────────────────────────────────────────
# Local
# ──────────────────────────────────────────────────────────────────────


def test_read_json_missing_file_returns_none(tmp_path: Path) -> None:
    # Fichier absent → None (pas d'exception), un premier run n'est pas un cas spécial
    assert read_json(tmp_path / "absent.json", Loader(), None) is None


def test_write_then_read_json_roundtrip_local(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    payload = {"a": 1, "b": [1, 2, 3]}

    write_json(path, payload, Saver(), None)

    assert read_json(path, Loader(), None) == payload


def test_write_json_forces_indent_and_non_ascii(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    write_json(path, {"pays": "Suède", "note": "éàü"}, Saver(), None)

    content = path.read_text(encoding="utf-8")
    # indent=2 est toujours forcé
    assert '\n  "pays"' in content
    # ensure_ascii=False est toujours forcé : les accents ne sont pas échappés
    assert "Suède" in content
    assert "\\u" not in content


def test_write_json_creates_missing_parent_dir(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deeper" / "reg.json"

    write_json(path, {"ok": True}, Saver(), None)

    assert path.exists()
    assert read_json(path, Loader(), None) == {"ok": True}


def test_write_json_atomic_no_leftover_tempfile(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    write_json(path, {"v": 1}, Saver(), None)
    # Ré-écriture au-dessus d'un fichier existant
    write_json(path, {"v": 2}, Saver(), None)

    assert read_json(path, Loader(), None) == {"v": 2}
    # Aucun fichier temporaire (tmp*.json) résiduel dans le dossier de destination
    assert [p.name for p in tmp_path.iterdir()] == ["reg.json"]


def test_read_registry_missing_file_returns_empty_dict(tmp_path: Path) -> None:
    assert read_registry(tmp_path / "absent.json", Loader(), None, root="DOWNLOADS") == {}


def test_read_registry_missing_root_returns_empty_dict(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    write_json(path, {"AUTRE": {"x": 1}}, Saver(), None)

    assert read_registry(path, Loader(), None, root="DOWNLOADS") == {}


def test_read_registry_returns_entries_under_root(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    write_json(path, {"DOWNLOADS": {"k1": {"last": "2024"}}}, Saver(), None)

    assert read_registry(path, Loader(), None, root="DOWNLOADS") == {"k1": {"last": "2024"}}


def test_read_registry_root_is_keyword_only(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        read_registry(tmp_path / "reg.json", Loader(), None, "DOWNLOADS")  # type: ignore[misc]


def test_merge_registry_persisted_shape_and_merge_semantics(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    write_json(
        path,
        {"DOWNLOADS": {"k1": {"v": 1}, "k2": {"v": 2}}},
        Saver(),
        None,
    )

    # Seule k2 est fournie : k1 doit être préservée, k2 écrasée
    merge_registry(path, {"k2": {"v": 99}}, Loader(), Saver(), None, root="DOWNLOADS")

    raw = read_json(path, Loader(), None)
    # Forme persistée : {root: {...}}
    assert set(raw) == {"DOWNLOADS"}
    assert raw["DOWNLOADS"] == {"k1": {"v": 1}, "k2": {"v": 99}}


def test_merge_registry_on_absent_file_creates_it(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    merge_registry(path, {"k1": {"v": 1}}, Loader(), Saver(), None, root="DOWNLOADS")

    assert read_json(path, Loader(), None) == {"DOWNLOADS": {"k1": {"v": 1}}}


# ──────────────────────────────────────────────────────────────────────
# S3 simulé (moto)
# ──────────────────────────────────────────────────────────────────────


def test_read_json_missing_key_returns_none_s3(s3_bucket: str) -> None:
    # Objet inexistant → ClientError avalée → None
    assert read_json(Path("reg/state.json"), Loader(), s3_bucket) is None


def test_write_then_read_json_roundtrip_s3(s3_bucket: str, s3_client) -> None:
    key = Path("reg/state.json")
    payload = {"pays": "Suède", "n": 3}

    write_json(key, payload, Saver(), s3_bucket)

    # PUT effectif : l'objet existe et contient indent=2 + accents non échappés
    body = s3_client.get_object(Bucket=s3_bucket, Key="reg/state.json")["Body"].read()
    text = body.decode("utf-8")
    assert '\n  "pays"' in text
    assert "Suède" in text

    # GET via read_json : aller-retour complet
    assert read_json(key, Loader(), s3_bucket) == payload


def test_read_registry_and_merge_registry_s3(s3_bucket: str) -> None:
    key = Path("reg/state.json")

    assert read_registry(key, Loader(), s3_bucket, root="DOWNLOADS") == {}

    merge_registry(key, {"k1": {"v": 1}}, Loader(), Saver(), s3_bucket, root="DOWNLOADS")
    merge_registry(key, {"k2": {"v": 2}}, Loader(), Saver(), s3_bucket, root="DOWNLOADS")

    assert read_registry(key, Loader(), s3_bucket, root="DOWNLOADS") == {
        "k1": {"v": 1},
        "k2": {"v": 2},
    }
