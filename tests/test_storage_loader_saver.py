"""Tests de caractérisation — ``macroforecast.storage`` (``Loader`` / ``Saver`` JSON).

Comportement figé : extension non ``.json`` → ``ValueError``, aller-retour local,
aller-retour S3 via moto.
"""

from __future__ import annotations

from pathlib import Path

import pytest

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
