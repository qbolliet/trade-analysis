"""Fixtures partagées des tests de caractérisation.

Ces tests figent le comportement ACTUEL des modules touchés par la migration
décrite dans ``MIGRATION_PLAN.md`` : ils constituent le contrat de non-régression
et doivent passer à l'identique avant et après le refactor. Aucun code de
production n'est modifié.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import NamedTuple, Sequence

import numpy as np
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


# ──────────────────────────────────────────────────────────────────────
# Sources factices de la synthèse (schémas « indicators » / « network_indicators »)
# ──────────────────────────────────────────────────────────────────────

# Grille de la fixture : 2 périodes x 4 pays x 30 produits CN8 (S-2.2 / S-2.3)
SYNTHESIS_PERIODS: tuple[str, ...] = ("2022", "2023")
SYNTHESIS_REPORTERS: tuple[str, ...] = ("FR", "DE", "IT", "ES")
SYNTHESIS_N_PRODUCTS = 30


class SynthesisSources(NamedTuple):
    """Connexion et métadonnées de la grille factice de la synthèse.

    Attributes:
        conn: Connexion DuckLake positionnée sur le catalogue temporaire, les
            schémas ``indicators`` et ``network_indicators`` déjà écrits.
        catalog_alias: Alias du catalogue attaché.
        periods: Valeurs ``TIME_PERIOD`` de la grille.
        reporters: Codes pays de la grille.
        products: Codes produits CN8 de la grille (8 chiffres).
    """
    conn: object
    catalog_alias: str
    periods: Sequence[str]
    reporters: Sequence[str]
    products: Sequence[str]


def _synthesis_products(n: int = SYNTHESIS_N_PRODUCTS) -> list[str]:
    """Build ``n`` deterministic CN8 codes (HS6 base + ``'00'`` suffix).

    Args:
        n: Number of distinct products.

    Returns:
        Eight-digit product codes whose first six digits are unique.
    """
    return [f"{100000 + i:06d}00" for i in range(n)]


@pytest.fixture
def synthesis_source_tables(ducklake_conn) -> SynthesisSources:
    """Write the fake ``indicators`` and ``network_indicators`` schemas (S-2.2/S-2.3).

    ``indicators`` (famille partenaires, grille de la synthèse) : 2 périodes x 4
    pays x 30 produits CN8, flux 1, indicateur ``VALUE_IN_EUROS``, fréquence
    ``A``, colonnes ``HHI``, ``CDI2``, ``CDI3`` et ``_ALERT`` (colonne du schéma
    réel non consommée par la méthodologie, incluse pour fidélité). Primary key
    ``(freq, flow, indicators, TIME_PERIOD, reporter, product)``.

    ``network_indicators`` (famille réseau) : les mêmes 30 produits ramenés à
    leur code HS6 (six premiers chiffres du CN8), 2 années,
    ``classification='HS2022'``, colonnes ``EXPORT_HHI``, ``CENTRALITY_RISK``,
    ``CLUSTERING_W``. Primary key ``(product, year, classification)``, jointe à
    la grille par ``substr(product, 1, 6) = product`` et ``year`` (S-2.2).

    Args:
        ducklake_conn: Connexion DuckLake sur catalogue temporaire.

    Returns:
        :class:`SynthesisSources` décrivant la grille écrite.
    """
    from statflows.storage.ducklake.tables import write_dataframe

    conn, catalog_alias = ducklake_conn
    rng = np.random.default_rng(0)
    products = _synthesis_products()

    df_indicators = pd.DataFrame(
        [
            {
                "freq": "A",
                "flow": 1,
                "indicators": "VALUE_IN_EUROS",
                "TIME_PERIOD": period,
                "reporter": reporter,
                "product": product,
                "HHI": float(rng.uniform(0.0, 1.0)),
                "CDI2": float(rng.uniform(0.0, 1.0)),
                "CDI3": float(rng.uniform(0.0, 1.0)),
                "_ALERT": bool(rng.integers(0, 2)),
            }
            for period in SYNTHESIS_PERIODS
            for reporter in SYNTHESIS_REPORTERS
            for product in products
        ]
    )
    write_dataframe(
        conn,
        df_indicators,
        ["freq", "flow", "indicators", "TIME_PERIOD", "reporter", "product"],
        catalog_alias=catalog_alias,
        schema="indicators",
    )

    df_network = pd.DataFrame(
        [
            {
                "product": product[:6],
                "year": int(period),
                "classification": "HS2022",
                "EXPORT_HHI": float(rng.uniform(0.0, 1.0)),
                "CENTRALITY_RISK": float(rng.uniform(0.0, 1.0)),
                "CLUSTERING_W": float(rng.uniform(0.0, 1.0)),
            }
            for period in SYNTHESIS_PERIODS
            for product in products
        ]
    )
    write_dataframe(
        conn,
        df_network,
        ["product", "year", "classification"],
        catalog_alias=catalog_alias,
        schema="network_indicators",
    )

    return SynthesisSources(
        conn=conn,
        catalog_alias=catalog_alias,
        periods=SYNTHESIS_PERIODS,
        reporters=SYNTHESIS_REPORTERS,
        products=products,
    )


# ──────────────────────────────────────────────────────────────────────
# Monde fictif (données synthétiques) : petit univers, aucun réseau
# ──────────────────────────────────────────────────────────────────────

# Pays du petit univers : (ISO3, ISO2, code M49 fictif, taille, région)
SYNTHETIC_COUNTRIES = (
    ("FRA", "FR", 251, 3.0, "EU"),
    ("DEU", "DE", 276, 4.0, "EU"),
    ("ITA", "IT", 380, 2.0, "EU"),
    ("CHN", "CN", 156, 18.0, "ASIA"),
    ("USA", "US", 842, 27.0, "NORTH_AMERICA"),
    ("JPN", "JP", 392, 4.0, "ASIA"),
    ("KOR", "KR", 410, 2.0, "ASIA"),
    ("COD", "CD", 180, 0.1, "AFRICA"),
    ("CAN", "CA", 124, 2.0, "NORTH_AMERICA"),
    ("BRA", "BR", 76, 2.0, "SOUTH_AMERICA"),
)


@pytest.fixture
def synthetic_section() -> dict:
    """Section ``synthetic`` d'un petit monde fictif (10 pays, 2019-2021)."""
    return {
        "SEED": 7,
        "YEARS": {"START": 2019, "END": 2021},
        "COUNTRIES": [
            {"iso3": iso3, "iso2": iso2, "size": size, "region": region}
            for iso3, iso2, _, size, region in SYNTHETIC_COUNTRIES
        ],
        "MODEL": {
            "DENSITY": 0.8,
            "MIN_VALUE": 100,
            "CONCENTRATION": {"DEFAULT": 1.0, "BY_PREFIX": {"8105": 0.5}},
            "SUPPLIER_BIAS": {"8105": {"COD": 5.0}},
        },
        "REPORTING": {"FOB_IMPORT_REPORTERS": ["CAN"], "NES_SHARE": 0.02},
        "COMEXT": {"PRODUCTS_PER_WRITE": 2},
    }


@pytest.fixture
def synthetic_world(synthetic_section: dict):
    """Monde fictif construit depuis ``synthetic_section``."""
    from kedro_pipeline.synthetic.world import SyntheticWorld, WorldConfig

    return SyntheticWorld(WorldConfig.from_mapping(synthetic_section))


@pytest.fixture
def synthetic_reference():
    """Référence pays (codes M49 fictifs) du petit univers, sans appel réseau."""
    from kedro_pipeline.synthetic.comtrade import CountryReference

    codelist = pd.DataFrame(
        {
            "reporterCode": [m49 for _, _, m49, _, _ in SYNTHETIC_COUNTRIES],
            "reporterDesc": [iso3.title() for iso3, *_ in SYNTHETIC_COUNTRIES],
            "reporterCodeIsoAlpha2": [iso2 for _, iso2, *_ in SYNTHETIC_COUNTRIES],
            "reporterCodeIsoAlpha3": [iso3 for iso3, *_ in SYNTHETIC_COUNTRIES],
        }
    )
    return CountryReference.from_codelist(codelist, [c[0] for c in SYNTHETIC_COUNTRIES])
