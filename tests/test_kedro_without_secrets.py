"""Le projet s'instancie sans aucun secret ; la première opération réelle échoue clairement.

Condition de la documentation construite en intégration continue (kedro-viz) et des
contrôles de configuration : aucune connexion à l'instanciation du catalogue, des
identifiants vides tant que les variables d'environnement manquent, et un message
explicite (« PGHOST is not set ») à la première opération sur une table.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Variables lues par config/base/credentials.yml
SECRET_VARIABLES = (
    "PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGADMINUSER",
    "AWS_S3_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
    "COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY", "MLFLOW_TRACKING_URI",
)


@pytest.fixture
def empty_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Environnement d'exécution sans aucun secret."""
    for name in SECRET_VARIABLES:
        monkeypatch.delenv(name, raising=False)


def test_session_catalog_and_handle_without_secrets(empty_environment) -> None:
    from kedro.framework.session import KedroSession
    from kedro.framework.startup import bootstrap_project

    from kedro_pipeline.io.ducklake import DuckLakeTable, MissingCredentialsError

    bootstrap_project(ROOT)
    with KedroSession.create(project_path=ROOT, env="base") as session:
        catalog = session.load_context().catalog
        handle = catalog.load("comtrade.tariffline")
        # Poignée paresseuse : aucune connexion ouverte
        assert isinstance(handle, DuckLakeTable)
        assert handle.conn is None
        assert handle.qualified_name == '"comtrade"."C_A_HS"."fact_table"'
        # Première opération réelle : erreur explicite, avant toute tentative de connexion
        with pytest.raises(MissingCredentialsError, match=r"^PGHOST is not set"):
            handle.exists()
        # Idem pour le catalogue de restitution et une factory résolue par son nom
        with pytest.raises(MissingCredentialsError, match=r"^PGHOST is not set"):
            with catalog.load("serving.tables").connect():
                pass
        assert catalog.load("baci.hs2017").schema == "baci_hs2017"


def test_s3_is_checked_once_postgres_is_set(empty_environment, monkeypatch) -> None:
    """PostgreSQL renseigné, S3 absent : l'erreur nomme la première variable S3 manquante.

    Le point de terminaison S3 a une valeur par défaut (minio.lab.sspcloud.fr) : c'est la
    clé d'accès qui manque.
    """
    from kedro.config import OmegaConfigLoader
    from kedro.io import DataCatalog

    from kedro_pipeline.io.ducklake import MissingCredentialsError
    from kedro_pipeline.settings import CONFIG_LOADER_ARGS

    for name, value in {"PGHOST": "h", "PGUSER": "u", "PGPASSWORD": "p"}.items():
        monkeypatch.setenv(name, value)
    loader = OmegaConfigLoader(conf_source=str(ROOT / "config"), env="base", **CONFIG_LOADER_ARGS)
    catalog = DataCatalog.from_config(loader["catalog"], loader["credentials"])
    with pytest.raises(MissingCredentialsError, match=r"^AWS_ACCESS_KEY_ID is not set"):
        catalog.load("eurostat.comext").exists()
