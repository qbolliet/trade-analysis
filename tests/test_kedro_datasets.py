"""Tests des datasets Kedro et de la poignée paresseuse ``DuckLakeTable``.

La poignée est éprouvée sur un vrai catalogue DuckLake fichier (fabrique de
connecteurs de ``conftest``) : connexion à la demande, lecture à paramètres liés,
existence, ajout de colonnes, upsert sans connexion ouverte. Les datasets sont éprouvés
sans aucune connexion : instanciation, chargement d'une poignée, refus d'un DataFrame ou
d'une autre table, registre de fraîcheur local.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from kedro.io import DatasetError

from conftest import file_connector_factory
from kedro_pipeline.io.datasets import (
    DuckLakeTableDataset,
    FreshnessRegistryDataset,
    ServingCatalogDataset,
    s3_storage_options,
    template_fields,
)
from kedro_pipeline.io.ducklake import DuckLakeLocation, DuckLakeTable, MissingCredentialsError
from kedro_pipeline.io.freshness import FreshnessRegistry, RegistryEntry, Unit, utc_now
from kedro_pipeline.io.serving import ServingCatalog

LOCATION = {
    "dbname": "vulnerabilities", "catalog_alias": "vulnerabilities", "schema": "indicators",
    "bucket": "b", "data_path": "trade/datasets/vulnerabilities/",
}
CREDENTIALS = {
    "postgres": {"host": "", "port": "5432", "user": "", "password": ""},
    "s3": {"endpoint": "minio.example", "access_key_id": "", "secret_access_key": ""},
}


class _ExplodingFactory:
    """Fabrique qui échoue si on l'appelle : prouve qu'aucune connexion n'est tentée."""

    def __call__(self, *args, **kwargs):
        raise AssertionError("connection attempted")


# ── Poignée paresseuse sur un catalogue fichier ──────────────────────────────


@pytest.fixture
def lazy_table(tmp_path: Path) -> DuckLakeTable:
    pytest.importorskip("dt_ducklake_manager")
    location = DuckLakeLocation(**{**LOCATION, "schema": "s1"})
    return DuckLakeTable.lazy(location, {}, {}, connector_factory=file_connector_factory(tmp_path))


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"reporter": ["FR", "DE", "IT"], "product": ["01", "02", "03"],
                         "HHI": [0.1, 0.2, 0.3]})


def test_lazy_handle_connects_on_demand(lazy_table: DuckLakeTable) -> None:
    """Sans connexion ouverte, chaque opération ouvre (puis ferme) la sienne."""
    assert lazy_table.conn is None
    assert lazy_table.exists() is False
    assert lazy_table.upsert(_frame(), ["reporter", "product"]) is True
    assert lazy_table.conn is None
    assert lazy_table.exists() is True
    result = lazy_table.query(
        f"SELECT reporter, HHI FROM {lazy_table.qualified_name} WHERE HHI > ? ORDER BY reporter",
        [0.15],
    )
    assert result["reporter"].tolist() == ["DE", "IT"]


def test_connect_binds_one_connection_for_the_block(lazy_table: DuckLakeTable) -> None:
    """Dans ``connect()``, toutes les opérations partagent la même connexion."""
    lazy_table.upsert(_frame(), ["reporter", "product"])
    with lazy_table.connect() as conn:
        assert lazy_table.conn is conn
        assert lazy_table.exists()
        assert len(lazy_table.query(f"SELECT * FROM {lazy_table.qualified_name}")) == 3
    assert lazy_table.conn is None


def test_add_missing_columns_adds_a_new_metric_without_rows(lazy_table: DuckLakeTable) -> None:
    lazy_table.upsert(_frame(), ["reporter", "product"])
    added = lazy_table.add_missing_columns(_frame().assign(CDI2=0.5, HHI=0.0))
    assert added == ["CDI2"]
    result = lazy_table.query(f"SELECT * FROM {lazy_table.qualified_name} ORDER BY reporter")
    assert "CDI2" in result.columns and result["CDI2"].isna().all()
    # Valeurs existantes intactes, deuxième appel sans effet
    assert result["HHI"].tolist() == [0.2, 0.1, 0.3]
    assert lazy_table.add_missing_columns(_frame().assign(CDI2=0.5)) == []


def test_add_missing_columns_on_a_missing_table_is_a_no_op(lazy_table: DuckLakeTable) -> None:
    assert lazy_table.add_missing_columns(_frame()) == []
    assert lazy_table.exists() is False


def test_borrowed_connection_is_never_closed(ducklake_conn) -> None:
    """Mode historique : la connexion de l'appelant est utilisée telle quelle."""
    conn, alias = ducklake_conn
    table = DuckLakeTable(conn, alias, "s1")
    with table.connect() as session:
        assert session is conn
    assert table.conn is conn
    assert table.qualified_name == f'"{alias}"."s1"."fact_table"'


def test_missing_credentials_fail_before_any_connection() -> None:
    table = DuckLakeTableDataset(location=LOCATION, credentials=CREDENTIALS).load()
    with pytest.raises(MissingCredentialsError, match=r"^PGHOST is not set"):
        table.exists()
    complete = {**CREDENTIALS, "postgres": {"host": "h", "port": "1", "user": "u", "password": "p"}}
    table = DuckLakeTableDataset(location=LOCATION, credentials=complete).load()
    with pytest.raises(MissingCredentialsError, match=r"^AWS_ACCESS_KEY_ID is not set"):
        with table.connect():
            pass


# ── DuckLakeTableDataset ─────────────────────────────────────────────────────


def test_table_dataset_loads_a_lazy_handle_without_connecting(monkeypatch) -> None:
    import kedro_pipeline.io.ducklake as ducklake

    monkeypatch.setattr(ducklake, "build_connector", _ExplodingFactory())
    dataset = DuckLakeTableDataset(location=LOCATION, credentials=CREDENTIALS)
    handle = dataset.load()
    assert isinstance(handle, DuckLakeTable) and handle.conn is None
    assert handle.location == DuckLakeLocation(**LOCATION)
    assert handle.qualified_name == '"vulnerabilities"."indicators"."fact_table"'


def test_table_dataset_saves_its_own_handle_only() -> None:
    dataset = DuckLakeTableDataset(location=LOCATION, credentials=CREDENTIALS)
    dataset.save(dataset.load())
    other = DuckLakeTableDataset(location={**LOCATION, "schema": "network_indicators"}).load()
    with pytest.raises(DatasetError, match="returned for the dataset"):
        dataset.save(other)


def test_table_dataset_refuses_a_dataframe() -> None:
    dataset = DuckLakeTableDataset(location=LOCATION)
    with pytest.raises(DatasetError, match="refuses a DataFrame"):
        dataset.save(_frame())


def test_table_dataset_describe_never_shows_credentials() -> None:
    secret = {**CREDENTIALS, "postgres": {"host": "h", "password": "s3cr3t"}}
    description = str(DuckLakeTableDataset(location=LOCATION, credentials=secret))
    assert "indicators" in description and "s3cr3t" not in description


def test_table_dataset_rejects_an_invalid_location() -> None:
    with pytest.raises(DatasetError, match="Invalid DuckLake location"):
        DuckLakeTableDataset(location={"dbname": "x"})


# ── FreshnessRegistryDataset ─────────────────────────────────────────────────


def test_registry_dataset_round_trip(tmp_path: Path) -> None:
    template = (tmp_path / "state" / "{classification}" / "{reporter}.json").as_posix()
    dataset = FreshnessRegistryDataset(path_template=template, step="partners")
    registry = dataset.load()
    assert isinstance(registry, FreshnessRegistry)
    unit = Unit.of(classification="HS2017", reporter="FR", product="01")
    assert registry.shard_of(unit) == "HS2017/FR"
    registry.upsert(RegistryEntry(unit, utc_now(), fingerprints={"HHI": "abc"}))
    dataset.save(registry)
    assert (tmp_path / "state" / "HS2017" / "FR.json").exists()
    # Rechargement : nouvelle instance, entrée relue depuis le fragment
    assert dataset.load().get(unit).fingerprints == {"HHI": "abc"}


def test_registry_dataset_refuses_another_registry(tmp_path: Path) -> None:
    dataset = FreshnessRegistryDataset(path_template="a/{vintage}.json", step="baci")
    other = FreshnessRegistryDataset(path_template="b/{vintage}.json", step="baci").load()
    with pytest.raises(DatasetError, match="returned for the dataset"):
        dataset.save(other)
    with pytest.raises(DatasetError, match="expects a FreshnessRegistry"):
        dataset.save({})


def test_s3_storage_options_and_template_fields() -> None:
    assert s3_storage_options({"endpoint": "https://s3.example", "access_key_id": "k",
                               "secret_access_key": "s", "session_token": "t"}) == {
        "aws_access_key_id": "k", "aws_secret_access_key": "s", "aws_session_token": "t",
        "endpoint_url": "https://s3.example",
    }
    assert s3_storage_options(None) is None
    assert template_fields("trade/state/synthesis/{TIME_PERIOD}.json") == ["TIME_PERIOD"]
    assert template_fields("single.json") == []


# ── ServingCatalogDataset ────────────────────────────────────────────────────


SERVING = {"dbname": "serving", "catalog_alias": "serving", "schema": "dashboard",
           "bucket": "b", "data_path": "trade/datasets/serving/"}


def test_serving_dataset_loads_a_handle_with_its_sources() -> None:
    dataset = ServingCatalogDataset(
        location=SERVING, sources={"vulnerabilities": LOCATION}, credentials=CREDENTIALS
    )
    catalog = dataset.load()
    assert isinstance(catalog, ServingCatalog)
    assert catalog.location.schema == "dashboard"
    assert catalog.sources == {"vulnerabilities": DuckLakeLocation(**LOCATION)}
    with pytest.raises(MissingCredentialsError, match=r"^PGHOST is not set"):
        with catalog.connect():
            pass


def test_serving_dataset_checks_identity() -> None:
    dataset = ServingCatalogDataset(location=SERVING)
    dataset.save(dataset.load())
    demo = ServingCatalogDataset(location={**SERVING, "schema": "demo_dashboard"}).load()
    with pytest.raises(DatasetError, match="returned for the dataset"):
        dataset.save(demo)
    with pytest.raises(DatasetError, match="expects a ServingCatalog"):
        dataset.save(_frame())
