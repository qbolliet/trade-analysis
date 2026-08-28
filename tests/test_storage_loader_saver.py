"""Tests de caractérisation — ``macroforecast.storage`` (``Loader`` / ``Saver`` JSON).

Comportement figé : extension non ``.json`` → ``ValueError``, allers-retours local
et S3 (moto), lecture tolérante (``missing_ok``), écriture atomique locale
(``atomic``), conversion des ``Path`` en clés S3, et convention « racine nommée +
fusion » que les scripts du pipeline appliquent au-dessus de ces deux briques.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from macroforecast.storage import Loader, Saver

_BAD_EXT_MSG = "Unsupported extension '.txt': only '.json' files are supported."


# ──────────────────────────────────────────────────────────────────────
# Extension non supportée
# ──────────────────────────────────────────────────────────────────────


def test_loader_rejects_non_json_extension_local(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"only '\.json' files are supported\."):
        Loader().load(str(tmp_path / "data.txt"))


def test_saver_rejects_non_json_extension_local(tmp_path: Path) -> None:
    with pytest.raises(ValueError) as excinfo:
        Saver().save(str(tmp_path / "data.txt"), {"a": 1})
    assert str(excinfo.value) == _BAD_EXT_MSG


def test_loader_rejects_non_json_extension_s3(s3_bucket: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        Loader().load("data.txt", bucket=s3_bucket)
    assert str(excinfo.value) == _BAD_EXT_MSG


def test_saver_rejects_non_json_extension_s3(s3_bucket: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        Saver().save("data.txt", {"a": 1}, bucket=s3_bucket)
    assert str(excinfo.value) == _BAD_EXT_MSG


def test_loader_extension_error_raised_even_with_missing_ok_s3(s3_bucket: str) -> None:
    # ``missing_ok`` n'avale que l'absence d'objet, pas la validation de format
    with pytest.raises(ValueError) as excinfo:
        Loader().load("data.txt", bucket=s3_bucket, missing_ok=True)
    assert str(excinfo.value) == _BAD_EXT_MSG


# ──────────────────────────────────────────────────────────────────────
# Allers-retours
# ──────────────────────────────────────────────────────────────────────


def test_roundtrip_local(tmp_path: Path) -> None:
    path = str(tmp_path / "data.json")
    obj = {"k": "v", "nums": [1, 2, 3], "nested": {"x": True}}

    Saver().save(path, obj)

    assert Loader().load(path) == obj


def test_roundtrip_s3(s3_bucket: str) -> None:
    obj = {"k": "v", "n": 42}

    Saver().save("dir/data.json", obj, bucket=s3_bucket)

    assert Loader().load("dir/data.json", bucket=s3_bucket) == obj


def test_roundtrip_local_with_path_object(tmp_path: Path) -> None:
    # ``Path`` accepté aussi bien à l'écriture qu'à la lecture
    path = tmp_path / "data.json"
    obj = {"a": 1, "b": [1, 2, 3]}

    Saver().save(path, obj)

    assert Loader().load(path) == obj


def test_path_converted_to_posix_key_s3(s3_bucket: str, s3_client) -> None:
    # Un ``Path`` (séparateurs Windows) devient une clé S3 POSIX
    key = Path("reg") / "state.json"

    Saver().save(key, {"v": 1}, bucket=s3_bucket)

    # L'objet est bien à la clé 'reg/state.json'
    body = s3_client.get_object(Bucket=s3_bucket, Key="reg/state.json")["Body"].read()
    assert body.decode("utf-8") == '{"v": 1}'
    # Et il est relisible via le même ``Path``
    assert Loader().load(key, bucket=s3_bucket) == {"v": 1}


# ──────────────────────────────────────────────────────────────────────
# Lecture tolérante (``missing_ok``)
# ──────────────────────────────────────────────────────────────────────


def test_load_missing_file_returns_none_local(tmp_path: Path) -> None:
    # Fichier absent → None (pas d'exception), un premier run n'est pas un cas spécial
    assert Loader().load(tmp_path / "absent.json", missing_ok=True) is None


def test_load_missing_file_raises_without_missing_ok(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        Loader().load(tmp_path / "absent.json")


def test_load_missing_key_returns_none_s3(s3_bucket: str) -> None:
    # Objet inexistant → ClientError avalée → None
    assert Loader().load(Path("reg/state.json"), bucket=s3_bucket, missing_ok=True) is None


def test_load_missing_key_raises_without_missing_ok_s3(s3_bucket: str) -> None:
    with pytest.raises(ClientError):
        Loader().load("reg/state.json", bucket=s3_bucket)


# ──────────────────────────────────────────────────────────────────────
# Écriture des registres (format, dossier parent, atomicité)
# ──────────────────────────────────────────────────────────────────────


def test_save_honours_indent_and_non_ascii_local(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    Saver().save(path, {"pays": "Suède", "note": "éàü"}, indent=2, ensure_ascii=False)

    content = path.read_text(encoding="utf-8")
    # indent=2 transmis jusqu'à json.dump malgré le passage par le temporaire
    assert '\n  "pays"' in content
    # ensure_ascii=False : les accents ne sont pas échappés
    assert "Suède" in content
    assert "\\u" not in content


def test_save_honours_indent_and_non_ascii_s3(s3_bucket: str, s3_client) -> None:
    key = Path("reg/state.json")
    payload = {"pays": "Suède", "n": 3}

    Saver().save(key, payload, bucket=s3_bucket, indent=2, ensure_ascii=False)

    # PUT effectif : l'objet existe et contient indent=2 + accents non échappés
    body = s3_client.get_object(Bucket=s3_bucket, Key="reg/state.json")["Body"].read()
    text = body.decode("utf-8")
    assert '\n  "pays"' in text
    assert "Suède" in text

    # Aller-retour complet
    assert Loader().load(key, bucket=s3_bucket) == payload


def test_save_creates_missing_parent_dir(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deeper" / "reg.json"

    Saver().save(path, {"ok": True})

    assert path.exists()
    assert Loader().load(path) == {"ok": True}


def test_save_atomic_no_leftover_tempfile(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    Saver().save(path, {"v": 1})
    # Ré-écriture au-dessus d'un fichier existant
    Saver().save(path, {"v": 2})

    assert Loader().load(path) == {"v": 2}
    # Aucun fichier temporaire (tmp*.json) résiduel dans le dossier de destination
    assert [p.name for p in tmp_path.iterdir()] == ["reg.json"]


def test_save_non_atomic_same_content(tmp_path: Path) -> None:
    atomic_path = tmp_path / "atomic.json"
    direct_path = tmp_path / "direct.json"
    payload = {"pays": "Suède", "n": 3}

    Saver().save(atomic_path, payload, indent=2, ensure_ascii=False)
    Saver().save(direct_path, payload, atomic=False, indent=2, ensure_ascii=False)

    assert direct_path.read_text(encoding="utf-8") == atomic_path.read_text(
        encoding="utf-8"
    )


# ──────────────────────────────────────────────────────────────────────
# Convention « racine nommée + fusion » appliquée par les scripts
# ──────────────────────────────────────────────────────────────────────

# Motif inliné dans les scripts du pipeline :
#   lecture = (loader.load(path, bucket=bucket, missing_ok=True) or {}).get(root, {})
#   fusion  = lecture, registry.update(entries), saver.save(path, {root: registry}, ...)


def _read_registry(path, bucket=None, *, root: str) -> dict:
    """Reproduit la lecture inlinée dans les scripts (racine absente → ``{}``)."""
    return (Loader().load(path, bucket=bucket, missing_ok=True) or {}).get(root, {})


def _merge_registry(path, entries, bucket=None, *, root: str) -> None:
    """Reproduit la fusion inlinée dans les scripts (seules les clés fournies bougent)."""
    registry = _read_registry(path, bucket, root=root)
    registry.update(entries)
    Saver().save(path, {root: registry}, bucket=bucket, indent=2, ensure_ascii=False)


def test_registry_read_missing_file_returns_empty_dict(tmp_path: Path) -> None:
    assert _read_registry(tmp_path / "absent.json", root="DOWNLOADS") == {}


def test_registry_read_missing_root_returns_empty_dict(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    Saver().save(path, {"AUTRE": {"x": 1}}, indent=2, ensure_ascii=False)

    assert _read_registry(path, root="DOWNLOADS") == {}


def test_registry_read_returns_entries_under_root(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    Saver().save(path, {"DOWNLOADS": {"k1": {"last": "2024"}}}, indent=2)

    assert _read_registry(path, root="DOWNLOADS") == {"k1": {"last": "2024"}}


def test_registry_merge_persisted_shape_and_merge_semantics(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    Saver().save(path, {"DOWNLOADS": {"k1": {"v": 1}, "k2": {"v": 2}}}, indent=2)

    # Seule k2 est fournie : k1 doit être préservée, k2 écrasée
    _merge_registry(path, {"k2": {"v": 99}}, root="DOWNLOADS")

    raw = Loader().load(path)
    # Forme persistée : {root: {...}}
    assert set(raw) == {"DOWNLOADS"}
    assert raw["DOWNLOADS"] == {"k1": {"v": 1}, "k2": {"v": 99}}


def test_registry_merge_on_absent_file_creates_it(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    _merge_registry(path, {"k1": {"v": 1}}, root="DOWNLOADS")

    assert Loader().load(path) == {"DOWNLOADS": {"k1": {"v": 1}}}


def test_registry_read_and_merge_s3(s3_bucket: str) -> None:
    key = Path("reg/state.json")

    assert _read_registry(key, s3_bucket, root="DOWNLOADS") == {}

    _merge_registry(key, {"k1": {"v": 1}}, s3_bucket, root="DOWNLOADS")
    _merge_registry(key, {"k2": {"v": 2}}, s3_bucket, root="DOWNLOADS")

    assert _read_registry(key, s3_bucket, root="DOWNLOADS") == {
        "k1": {"v": 1},
        "k2": {"v": 2},
    }
