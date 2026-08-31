"""Fixtures partagées des tests de caractérisation.

Ces tests figent le comportement ACTUEL des modules touchés par la migration
décrite dans ``MIGRATION_PLAN.md`` : ils constituent le contrat de non-régression
et doivent passer à l'identique avant et après le refactor. Aucun code de
production n'est modifié.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

import pandas as pd
import pytest


# ──────────────────────────────────────────────────────────────────────
# S3 simulé (moto)
# ──────────────────────────────────────────────────────────────────────

# Nom du bucket de test
BUCKET = "test-bucket"


@pytest.fixture
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Renseigne les variables d'environnement AWS attendues par ``S3Connection``.

    ``statflows.storage.S3Connection._connect`` lit ``os.environ[...]`` (et lève ``KeyError`` en
    l'absence) dès qu'un argument S3 vaut ``None`` — ce qui est le cas via les
    ``Loader``/``Saver`` par défaut. On pose donc des valeurs factices.
    """
    monkeypatch.setenv("AWS_S3_ENDPOINT", "s3.amazonaws.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_bucket(aws_env: None):
    """Active le mock S3 de moto et crée un bucket vide.

    Yields:
        Le nom du bucket créé (``"test-bucket"``).
    """
    from moto import mock_aws

    with mock_aws():
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield BUCKET


@pytest.fixture
def s3_client(s3_bucket: str):
    """Client boto3 brut sur le bucket moto (pour préparer / vérifier des objets)."""
    import boto3

    return boto3.client("s3", region_name="us-east-1")


# ──────────────────────────────────────────────────────────────────────
# Jeux de données
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Petit DataFrame déterministe pour les allers-retours parquet."""
    return pd.DataFrame(
        {
            "geo": ["FR", "DE", "IT"],
            "value": [1.5, 2.5, 3.5],
        }
    )


@pytest.fixture
def sample_xls_path(tmp_path: Path) -> Path:
    """Génère un classeur BIFF ``.xls`` lisible par le moteur ``xlrd``.

    ``xlrd`` 2.x ne lit que le format binaire ``.xls`` et aucun writer ``.xls``
    n'est disponible côté pandas ; le fichier est donc construit avec ``xlwt``.

    Returns:
        Chemin vers le ``.xls`` (2 colonnes ``geo``/``value``, 2 lignes).
    """
    xlwt = pytest.importorskip("xlwt")

    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet("Sheet1")
    for col, header in enumerate(("geo", "value")):
        sheet.write(0, col, header)
    for row, (geo, value) in enumerate([("FR", 1.0), ("DE", 2.0)], start=1):
        sheet.write(row, 0, geo)
        sheet.write(row, 1, value)

    path = tmp_path / "sample.xls"
    workbook.save(str(path))
    return path


@pytest.fixture
def sample_xlsx_path(tmp_path: Path) -> Path:
    """Génère un classeur ``.xlsx`` (Office Open XML) lisible par le moteur ``openpyxl``.

    Returns:
        Chemin vers le ``.xlsx`` (2 colonnes ``geo``/``value``, 2 lignes).
    """
    pytest.importorskip("openpyxl")

    frame = pd.DataFrame({"geo": ["FR", "DE"], "value": [1.0, 2.0]})
    path = tmp_path / "sample.xlsx"
    frame.to_excel(path, index=False, engine="openpyxl")
    return path


# ──────────────────────────────────────────────────────────────────────
# Connexion DuckLake sur catalogue fichier temporaire
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def ducklake_conn(tmp_path: Path):
    """Connexion DuckDB avec un catalogue DuckLake ``.ducklake`` en fichier temp.

    Reproduit le montage du pipeline (``duckdb.connect(":memory:")`` +
    ``INSTALL/LOAD ducklake`` + ``ATTACH 'ducklake:...'``) sans passer par le
    connecteur externe. ``pytest.skip`` si l'extension ``ducklake`` ou
    ``dt_ducklake_manager`` n'est pas disponible.

    Yields:
        Tuple ``(conn, catalog_alias)`` ; la connexion est positionnée sur
        ``db.main`` et le schéma ``s1`` est déjà créé.
    """
    pytest.importorskip("dt_ducklake_manager")
    import duckdb

    catalog_path = tmp_path / "catalog.ducklake"
    data_path = tmp_path / "data"
    data_path.mkdir()

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
    except duckdb.Error as exc:  # extension indisponible hors-ligne
        conn.close()
        pytest.skip(f"extension duckdb 'ducklake' indisponible : {exc}")

    catalog_alias = "db"
    conn.execute(
        f"ATTACH 'ducklake:{catalog_path.as_posix()}' AS {catalog_alias} "
        f"(DATA_PATH '{data_path.as_posix()}')"
    )
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog_alias}.s1")
    conn.execute(f"USE {catalog_alias}.main")

    try:
        yield conn, catalog_alias
    finally:
        with contextlib.suppress(Exception):
            conn.close()
