"""Tests de caractérisation — ``macroforecast.storage2`` (``Loader`` / ``Saver`` tabulaire).

Comportement figé : extension non supportée → ``ValueError``, lecture d'un ``.xls``
et d'un ``.parquet`` (local et S3 via moto), écriture d'un ``.parquet``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from macroforecast.storage2 import Loader, Saver

_LOADER_BAD_EXT = "Unsupported extension '.csv': only ('xls', 'parquet') files are supported."
_SAVER_BAD_EXT = "Unsupported extension '.csv': only '.parquet' files are supported."


# ──────────────────────────────────────────────────────────────────────
# Extension non supportée
# ──────────────────────────────────────────────────────────────────────


def test_loader_rejects_unsupported_extension_local(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    with pytest.raises(ValueError) as excinfo:
        Loader().load(str(tmp_path / "data.csv"))
    assert str(excinfo.value) == _LOADER_BAD_EXT


def test_saver_rejects_unsupported_extension_local(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    with pytest.raises(ValueError) as excinfo:
        Saver().save(str(tmp_path / "data.csv"), sample_df)
    assert str(excinfo.value) == _SAVER_BAD_EXT


def test_loader_rejects_unsupported_extension_s3(s3_bucket: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        Loader().load("data.csv", bucket=s3_bucket)
    assert str(excinfo.value) == _LOADER_BAD_EXT


def test_saver_rejects_unsupported_extension_s3(s3_bucket: str, sample_df: pd.DataFrame) -> None:
    with pytest.raises(ValueError) as excinfo:
        Saver().save("data.csv", sample_df, bucket=s3_bucket)
    assert str(excinfo.value) == _SAVER_BAD_EXT


# ──────────────────────────────────────────────────────────────────────
# Lecture .xls
# ──────────────────────────────────────────────────────────────────────


def _assert_sample_xls(df: pd.DataFrame) -> None:
    assert list(df.columns) == ["geo", "value"]
    assert df["geo"].tolist() == ["FR", "DE"]
    assert df["value"].tolist() == [1.0, 2.0]


def test_read_xls_local(sample_xls_path: Path) -> None:
    _assert_sample_xls(Loader().load(str(sample_xls_path)))


def test_read_xls_s3(s3_bucket: str, s3_client, sample_xls_path: Path) -> None:
    s3_client.put_object(
        Bucket=s3_bucket, Key="dir/sample.xls", Body=sample_xls_path.read_bytes()
    )

    _assert_sample_xls(Loader().load("dir/sample.xls", bucket=s3_bucket))


# ──────────────────────────────────────────────────────────────────────
# Lecture .parquet
# ──────────────────────────────────────────────────────────────────────


def test_read_parquet_local(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "data.parquet"
    sample_df.to_parquet(path)

    assert_frame_equal(Loader().load(str(path)), sample_df)


def test_read_parquet_s3(s3_bucket: str, s3_client, tmp_path: Path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "data.parquet"
    sample_df.to_parquet(path)
    s3_client.put_object(Bucket=s3_bucket, Key="dir/data.parquet", Body=path.read_bytes())

    assert_frame_equal(Loader().load("dir/data.parquet", bucket=s3_bucket), sample_df)


# ──────────────────────────────────────────────────────────────────────
# Écriture .parquet
# ──────────────────────────────────────────────────────────────────────


def test_write_parquet_local(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "out.parquet"

    Saver().save(str(path), sample_df)

    assert_frame_equal(pd.read_parquet(path), sample_df)


def test_write_parquet_s3(s3_bucket: str, s3_client, sample_df: pd.DataFrame) -> None:
    from io import BytesIO

    Saver().save("dir/out.parquet", sample_df, bucket=s3_bucket)

    body = s3_client.get_object(Bucket=s3_bucket, Key="dir/out.parquet")["Body"].read()
    assert_frame_equal(pd.read_parquet(BytesIO(body)), sample_df)
