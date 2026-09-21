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


# ──────────────────────────────────────────────────────────────────────
# Couche de service (PS-29) : quatre catalogues DuckLake fichiers
# ──────────────────────────────────────────────────────────────────────

# Racine du dépôt (fichiers de configuration réels)
REPO_ROOT = Path(__file__).resolve().parents[1]
# Années de la grille fictive : une par millésime (HS2017 puis HS2022)
SERVING_YEARS: tuple[int, ...] = (2019, 2023)
SERVING_REPORTERS: tuple[str, ...] = ("FR", "DE", "EU27_2020")
# Codes CN8 / SH6 stockés en entier (le zéro initial de 01012100 est perdu, comme en réel)
SERVING_PRODUCTS: tuple[int, ...] = (85411000, 85412100, 1012100, 854110)
# Partenaires individuels (plus que TOP_PARTNERS) et agrégats Comext
SERVING_PARTNERS: tuple[str, ...] = tuple(
    ["US", "CN", "JP", "KR", "TW", "IN", "BR", "CH", "NO", "GB", "CA", "MX", "TR", "VN", "TH"]
)
SERVING_AGGREGATES: tuple[str, ...] = ("WORLD", "EXT_EU", "INT_EU27_2020")
# Valeurs réseau distinctes par millésime : la jointure doit prendre celui en vigueur
SERVING_NETWORK_HHI = {"HS2017": 0.17, "HS2022": 0.22}
SERVING_METHODS: tuple[str, ...] = ("consensus_borda", "auto_sum", "critic_sum", "pareto")


def _load_yaml(path: Path) -> dict:
    """Charge un fichier YAML du dépôt."""
    import yaml

    with open(path, encoding="utf-8") as file:
        return yaml.safe_load(file)


def serving_configs(profile: str = "base") -> dict:
    """Configurations réelles lues par ``serving-script`` (profil ``base`` ou ``demo``)."""
    config = REPO_ROOT / "config"
    if profile == "base":
        paths = {
            "eurostat": config / "datasets" / "eurostat.yaml",
            "comtrade": config / "datasets" / "comtrade.yaml",
            "vulnerabilities": config / "vulnerabilities.yaml",
            "synthesis": config / "synthesis.yaml",
            "runtime": config / "runtime.yaml",
            "serving": config / "serving.yaml",
        }
    else:
        demo = config / "profiles" / profile
        paths = {
            name: demo / f"{name}.yaml"
            for name in ("eurostat", "comtrade", "vulnerabilities", "synthesis", "runtime", "serving")
        }
    loaded = {name: _load_yaml(path) for name, path in paths.items()}
    loaded["runtime"] = loaded["runtime"]["runtime"]
    loaded["serving"] = loaded["serving"]["serving"]
    return loaded


def file_connector_factory(root: Path):
    """Fabrique de connecteurs DuckLake FICHIERS, même signature que ``build_connector``.

    Un catalogue par ``dbname`` (``<root>/<dbname>.ducklake``), inlining désactivé pour
    que chaque écriture produise des fichiers Parquet (partitionnement observable).
    """
    from dt_ducklake_manager import DuckLakeConnector

    def factory(location, pg, s3, *, create_db_if_missing=True, read_only=False):
        return DuckLakeConnector(
            catalog_path=str(root / f"{location.dbname}.ducklake"),
            data_path=str(root / f"{location.dbname}_data"),
            catalog_alias=location.catalog_alias,
            schema=location.schema,
            read_only=read_only,
            data_inlining_row_limit=0,
        )

    return factory


class ServingWorld(NamedTuple):
    """Catalogues fictifs prêts pour ``publish_serving``.

    Attributes:
        catalog: ``ServingCatalog`` sur les catalogues fichiers.
        tables: Variables de gabarit -> ``SourceTable``.
        params: Section ``serving`` de la configuration réelle.
        runtime: Section ``runtime`` de la configuration réelle.
        factory: Fabrique de connecteurs fichiers.
        root: Répertoire des catalogues.
        locations: Catalogues sources, par alias.
    """
    catalog: object
    tables: dict
    params: dict
    runtime: dict
    factory: object
    root: Path
    locations: dict


def _serving_frames(rng: "np.random.Generator") -> dict:
    """Tables sources fictives (Comext, partenaires, réseau, synthèse, cohérence)."""
    from scipy.stats import rankdata

    comext_rows, indicator_rows = [], []
    for year in SERVING_YEARS:
        for reporter in SERVING_REPORTERS:
            for product in SERVING_PRODUCTS:
                for flow in (1, 2):
                    values = rng.uniform(1.0, 100.0, len(SERVING_PARTNERS))
                    total = float(values.sum())
                    for partner, value in zip(SERVING_PARTNERS, values):
                        comext_rows.append((reporter, partner, product, flow, str(year), float(value)))
                    comext_rows.append((reporter, "WORLD", product, flow, str(year), total))
                    comext_rows.append((reporter, "EXT_EU", product, flow, str(year), 0.6 * total))
                    comext_rows.append((reporter, "INT_EU27_2020", product, flow, str(year), 0.4 * total))
                    for indicator in ("VALUE_IN_EUROS", "QUANTITY_IN_100KG"):
                        hhi, cdi2, cdi3 = rng.uniform(0, 1, 3)
                        indicator_rows.append(
                            ("A", reporter, product, flow, indicator, str(year),
                             hhi, cdi2, cdi3, hhi > 0.5, cdi2 > 0.5, cdi3 > 1.0)
                        )
    df_comext = pd.DataFrame(
        comext_rows,
        columns=["reporter", "partner", "product", "flow", "TIME_PERIOD", "OBS_VALUE"],
    )
    df_comext = pd.concat(
        [
            df_comext.assign(freq="A", indicators="VALUE_IN_EUROS"),
            df_comext.assign(freq="A", indicators="QUANTITY_IN_100KG", OBS_VALUE=df_comext["OBS_VALUE"] * 3),
        ],
        ignore_index=True,
    )
    df_indicators = pd.DataFrame(
        indicator_rows,
        columns=["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD",
                 "HHI", "CDI2", "CDI3", "HHI_ALERT", "CDI2_ALERT", "CDI3_ALERT"],
    )

    # Réseau : deux millésimes pour chaque année, valeurs distinctes par millésime
    network_rows = []
    for year in SERVING_YEARS:
        for classification, hhi in SERVING_NETWORK_HHI.items():
            for hs6 in (854110, 854121, 10121):
                network_rows.append(
                    (hs6, year, classification, hhi, 1.5, 0.3, 0.8, hhi > 0.5, False, False, False)
                )
    df_network = pd.DataFrame(
        network_rows,
        columns=["product", "year", "classification", "EXPORT_HHI", "CENTRALITY_RISK",
                 "CLUSTERING_W", "SPOF", "EXPORT_HHI_ALERT", "CENTRALITY_RISK_ALERT",
                 "CLUSTERING_W_ALERT", "SPOF_ALERT"],
    )

    # Synthèse : format long par méthode, rang 1 = score le plus élevé (S-2.4)
    grid = df_indicators[df_indicators["indicators"] == "VALUE_IN_EUROS"][
        ["freq", "flow", "indicators", "TIME_PERIOD", "reporter", "product"]
    ]
    context = ["freq", "flow", "indicators", "TIME_PERIOD"]
    frames = []
    for method in SERVING_METHODS:
        df = grid.copy()
        df["product"] = df["product"].astype(str)
        df["method"] = method
        for level in ("by_product", "by_reporter", "global"):
            df[f"score_{level}"] = rng.uniform(0, 1, len(df))
        for level, group in (("by_product", ["product"]), ("by_reporter", ["reporter"]), ("global", [])):
            keys = context + group
            df[f"rank_{level}"] = df.groupby(keys)[f"score_{level}"].transform(
                lambda s: pd.Series(rankdata(-s.to_numpy(), method="average"), index=s.index)
            )
            df[f"n_{level}"] = df.groupby(keys)[f"score_{level}"].transform("size").astype("int32")
        frames.append(df)
    df_synthesis = pd.concat(frames, ignore_index=True)

    # Cohérence : familles metrics, methods et fit (cette dernière non exposée)
    diag_rows = []
    for year in SERVING_YEARS:
        base = ("A", 1, "VALUE_IN_EUROS", str(year))
        diag_rows += [
            (*base, "global", "ALL", "ALL", "metrics", "spearman", "HHI", "CDI2", 0.4, 24),
            (*base, "by_product", "ALL", "85411000", "metrics", "kendall_w", "", "", 0.7, 3),
            (*base, "by_reporter", "FR", "ALL", "methods", "kendall_tau_b", "auto_sum", "critic_sum", 0.9, 8),
            (*base, "global", "ALL", "ALL", "methods", "lomo_tau", "auto_sum", "HHI", 0.8, 24),
            (*base, "global", "ALL", "ALL", "fit", "weight", "auto_sum", "HHI", 0.2, 24),
        ]
    df_diagnostics = pd.DataFrame(
        diag_rows,
        columns=["freq", "flow", "indicators", "TIME_PERIOD", "level", "reporter", "product",
                 "family", "statistic", "item_a", "item_b", "value", "n"],
    )
    return {
        "comext": df_comext,
        "indicators": df_indicators,
        "network": df_network,
        "synthesis": df_synthesis,
        "diagnostics": df_diagnostics,
    }


# Clés primaires des tables sources fictives
_SERVING_KEYS = {
    "comext": ["freq", "reporter", "partner", "product", "flow", "indicators", "TIME_PERIOD"],
    "indicators": ["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD"],
    "network": ["product", "year", "classification"],
    "synthesis": ["freq", "flow", "indicators", "TIME_PERIOD", "reporter", "product", "method"],
    "diagnostics": ["freq", "flow", "indicators", "TIME_PERIOD", "level", "reporter", "product",
                    "family", "statistic", "item_a", "item_b"],
}


def build_serving_world(
    root: Path, *, with_reference: bool = True, omit: Sequence[str] = ()
) -> ServingWorld:
    """Écrit les catalogues sources fictifs et renvoie la poignée de service.

    Args:
        root: Répertoire des catalogues fichiers.
        with_reference: Publie les référentiels (libellés, concordances).
        omit: Tables sources à ne pas écrire (sources facultatives absentes).
    """
    pytest.importorskip("dt_ducklake_manager")
    from statflows.storage.ducklake.tables import write_dataframe

    from kedro_pipeline.io.ducklake import DuckLakeLocation
    from kedro_pipeline.io.serving import ServingCatalog
    from kedro_pipeline.steps.reference import publish_hs_reference, publish_reference
    from kedro_pipeline.steps.serving import source_tables

    configs = serving_configs("base")
    params, runtime = configs["serving"], configs["runtime"]
    locations, tables = source_tables(
        eurostat=configs["eurostat"],
        comtrade=configs["comtrade"],
        vulnerabilities=configs["vulnerabilities"],
        synthesis=configs["synthesis"],
    )
    factory = file_connector_factory(root)
    frames = _serving_frames(np.random.default_rng(0))

    for alias, location in locations.items():
        connector = factory(location, None, None)
        conn = connector.connect()
        try:
            for name, df in frames.items():
                table = tables[name]
                if table.catalog_alias != alias or name in omit:
                    continue
                write_dataframe(conn, df, _SERVING_KEYS[name], catalog_alias=alias, schema=table.schema)
            if with_reference and alias == "eurostat":
                reference = configs["eurostat"]["DOWNLOADS"]["REFERENCE"]
                publish_reference(
                    {
                        "reporter": pd.DataFrame({"code": list(SERVING_REPORTERS),
                                                  "name": ["France", "Germany", "European Union"]}),
                        "partner": pd.DataFrame({"code": list(SERVING_PARTNERS) + list(SERVING_AGGREGATES),
                                                 "name": [f"Country {c}" for c in SERVING_PARTNERS]
                                                 + ["World", "Extra-EU", "Intra-EU"]}),
                        "product": pd.DataFrame({"code": ["85411000", "85412100", "01012100", "854110"],
                                                 "name": ["Diodes", "Transistors", "Horses", "Diodes (HS6)"]}),
                    },
                    connector,
                    source="eurostat",
                    params={**reference, "NOMENCLATURES": runtime["NOMENCLATURES"]["HS"], "YEAR": 2026},
                    conn=conn,
                )
            if with_reference and alias == "comtrade":
                reference = configs["comtrade"]["DOWNLOADS"]["REFERENCE"]
                publish_reference(
                    {
                        "reporter": pd.DataFrame({"reporterCode": [251, 276], "reporterDesc": ["France", "Germany"],
                                                  "reporterCodeIsoAlpha3": ["FRA", "DEU"], "isGroup": [False, False]}),
                        "cmd:HS": pd.DataFrame({"id": ["85", "8541", "854110"],
                                                "text": ["85 - Electrical", "8541 - Diodes", "854110 - Diodes"],
                                                "parent": ["TOTAL", "85", "8541"]}),
                    },
                    connector,
                    source="comtrade",
                    params={**reference, "NOMENCLATURES": runtime["NOMENCLATURES"]["HS"], "YEAR": 2026},
                    conn=conn,
                )
                publish_hs_reference(
                    {("HS2022", "HS2017"): pd.DataFrame({
                        "source_classification": "HS2022", "source_code": ["854110", "854121", "854129"],
                        "target_classification": "HS2017", "target_code": ["854110", "854121", "854121"],
                        "relationship": pd.NA})},
                    connector,
                    params={"SCHEMA_PREFIX": reference["SCHEMA_PREFIX"],
                            "NOMENCLATURES": runtime["NOMENCLATURES"]["HS"]},
                    conn=conn,
                )
        finally:
            conn.close()

    serving_location = DuckLakeLocation(
        dbname=params["DBNAME"],
        catalog_alias=params["CATALOG_ALIAS"],
        schema=params["SCHEMA"],
        bucket=params["BUCKET"],
        data_path=params["DATA_PATH"],
    )
    catalog = ServingCatalog(serving_location, None, None, locations, connector_factory=factory)
    return ServingWorld(catalog, tables, params, runtime, factory, root, locations)


@pytest.fixture
def serving_world(tmp_path: Path) -> ServingWorld:
    """Catalogues sources fictifs complets (référentiels compris)."""
    return build_serving_world(tmp_path)
