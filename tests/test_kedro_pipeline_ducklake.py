"""Tests de la fabrique de connecteur DuckLake (``kedro_pipeline.io.ducklake``, PS-06).

Aucune connexion réelle : ``DuckLakeConnector.from_postgres`` est remplacé par
un enregistreur, ce qui vérifie les arguments transmis (notamment
``s3_session_token=None`` sans variable d'environnement, C-03).
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict

import pytest

from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
)


# Environnement minimal complet (sans jeton de session ni rôle d'administration)
_PG_ENV = {
    "PGHOST": "pg.local",
    "PGPORT": "5432",
    "PGUSER": "trade",
    "PGPASSWORD": "secret",
    "PGDATABASE": "defaultdb",
}
_S3_ENV = {
    "AWS_S3_ENDPOINT": "minio.local",
    "AWS_ACCESS_KEY_ID": "key",
    "AWS_SECRET_ACCESS_KEY": "s3secret",
}

_LOCATION = DuckLakeLocation(
    dbname="comtrade",
    catalog_alias="comtrade",
    schema="C_A_HS",
    bucket="bucket",
    data_path="trade/datasets/comtrade",
)


def test_location_is_frozen_and_builds_data_url() -> None:
    """La localisation est immuable et expose l'URL S3 des données."""
    assert _LOCATION.data_url == "s3://bucket/trade/datasets/comtrade"
    assert _LOCATION.table == "fact_table"
    with pytest.raises(dataclasses.FrozenInstanceError):
        _LOCATION.schema = "other"  # type: ignore[misc]


def test_pg_credentials_default_admin_user() -> None:
    """Sans PGADMINUSER, le rôle d'administration vaut ``postgres``."""
    pg = pg_credentials_from_env(_PG_ENV)
    assert pg == {
        "host": "pg.local",
        "port": "5432",
        "user": "trade",
        "password": "secret",
        "admin_dbname": "defaultdb",
        "admin_user": "postgres",
        "admin_password": "secret",
    }


def test_pg_credentials_admin_user_from_env() -> None:
    """PGADMINUSER surcharge le rôle d'administration."""
    pg = pg_credentials_from_env({**_PG_ENV, "PGADMINUSER": "admin"})
    assert pg["admin_user"] == "admin"


def test_pg_credentials_missing_variable_is_explicit() -> None:
    """Une variable obligatoire absente lève une erreur qui la nomme."""
    env = {k: v for k, v in _PG_ENV.items() if k != "PGHOST"}
    with pytest.raises(KeyError, match="PGHOST is not set"):
        pg_credentials_from_env(env)


@pytest.mark.parametrize("token_env", [{}, {"AWS_SESSION_TOKEN": ""}])
def test_s3_session_token_is_optional(token_env: Dict[str, str]) -> None:
    """Jeton de session absent ou vide → ``None`` (clés permanentes, C-03)."""
    s3 = s3_credentials_from_env({**_S3_ENV, **token_env})
    assert s3["session_token"] is None
    assert s3["endpoint"] == "minio.local"


def test_s3_session_token_forwarded_when_set() -> None:
    """Un jeton de session renseigné est conservé."""
    s3 = s3_credentials_from_env({**_S3_ENV, "AWS_SESSION_TOKEN": "tok"})
    assert s3["session_token"] == "tok"


def test_s3_credentials_missing_variable_is_explicit() -> None:
    """Une clé S3 obligatoire absente lève une erreur qui la nomme."""
    env = {k: v for k, v in _S3_ENV.items() if k != "AWS_ACCESS_KEY_ID"}
    with pytest.raises(KeyError, match="AWS_ACCESS_KEY_ID is not set"):
        s3_credentials_from_env(env)


def test_credentials_read_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sans mapping explicite, l'environnement du processus est lu."""
    for key, value in {**_PG_ENV, **_S3_ENV}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.delenv("PGADMINUSER", raising=False)
    assert pg_credentials_from_env()["host"] == "pg.local"
    assert s3_credentials_from_env()["session_token"] is None


def test_build_connector_forwards_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    """``build_connector`` transmet exactement la localisation et les identifiants."""
    ducklake_manager = pytest.importorskip("dt_ducklake_manager")
    calls: list[Dict[str, Any]] = []
    sentinel = object()

    def fake_from_postgres(data_path: str, **kwargs: Any) -> object:
        calls.append({"data_path": data_path, **kwargs})
        return sentinel

    monkeypatch.setattr(
        ducklake_manager.DuckLakeConnector, "from_postgres", staticmethod(fake_from_postgres)
    )

    connector = build_connector(
        _LOCATION, pg_credentials_from_env(_PG_ENV), s3_credentials_from_env(_S3_ENV)
    )

    assert connector is sentinel
    assert calls == [
        {
            "data_path": "s3://bucket/trade/datasets/comtrade",
            "dbname": "comtrade",
            "host": "pg.local",
            "port": "5432",
            "user": "trade",
            "password": "secret",
            "create_db_if_missing": True,
            "admin_dbname": "defaultdb",
            "admin_user": "postgres",
            "admin_password": "secret",
            "catalog_alias": "comtrade",
            "schema": "C_A_HS",
            "s3_endpoint": "minio.local",
            "s3_access_key_id": "key",
            "s3_secret_access_key": "s3secret",
            "s3_session_token": None,
        }
    ]
