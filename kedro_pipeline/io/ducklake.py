"""DuckLake connector factory (PS-06).

Single replacement of the connector constructions formerly duplicated in every
script (C-04). In phase 0, :func:`pg_credentials_from_env` and
:func:`s3_credentials_from_env` are the only places where the execution
environment is read for credentials; ``AWS_SESSION_TOKEN`` is optional, since
the permanent Minio keys of the ``trade-s3-credentials`` secret carry no
session token (C-03).

``dt_ducklake_manager`` is imported lazily inside :func:`build_connector`, so
this module stays importable without it (only the DuckLake write path requires
the dependency).
"""
# Importation des modules
# Modules de base
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional


# Variables PostgreSQL obligatoires (catalogue DuckLake)
_PG_REQUIRED = ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE")
# Variables S3 obligatoires (stockage des fichiers Parquet)
_S3_REQUIRED = ("AWS_S3_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
# Rôle d'administration par défaut (création de la base du catalogue)
_DEFAULT_ADMIN_USER = "postgres"


# Classe décrivant l'emplacement d'un schéma DuckLake
@dataclass(frozen=True)
class DuckLakeLocation:
    """Where a DuckLake schema lives (catalog identity + data path).

    Attributes:
        dbname: PostgreSQL database holding the catalog metadata (e.g.
            ``"comtrade"``).
        catalog_alias: ``ATTACH`` alias of the catalog (e.g. ``"comtrade"``).
        schema: Schema the connector is positioned on (already sanitised,
            e.g. ``"C_A_HS"``).
        bucket: S3 bucket holding the Parquet data files.
        data_path: S3 prefix of the Parquet data files, under ``bucket``.
        table: Fact table name.

    Examples:
        >>> location = DuckLakeLocation(
        ...     dbname="comtrade", catalog_alias="comtrade", schema="C_A_HS",
        ...     bucket="my-bucket", data_path="trade/datasets/comtrade",
        ... )
        >>> location.data_url
        's3://my-bucket/trade/datasets/comtrade'
    """

    dbname: str
    catalog_alias: str
    schema: str
    bucket: str
    data_path: str
    table: str = "fact_table"

    # Propriété de l'URL complète des données
    @property
    def data_url(self) -> str:
        """Full S3 URL of the data files (``s3://{bucket}/{data_path}``)."""
        return f"s3://{self.bucket}/{self.data_path}"


# Fonction de lecture d'une variable d'environnement obligatoire
def _require(environ: Mapping[str, str], name: str) -> str:
    """Return a mandatory environment variable, with an explicit error if unset.

    Args:
        environ: Environment mapping.
        name: Variable name.

    Returns:
        The variable value.

    Raises:
        KeyError: If the variable is unset or empty.
    """
    value = environ.get(name)
    if not value:
        raise KeyError(f"{name} is not set: required to build the DuckLake connector")
    return value


# Fonction de lecture des identifiants PostgreSQL
def pg_credentials_from_env(
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Read the PostgreSQL credentials of the DuckLake catalogs from the environment.

    Args:
        environ: Environment mapping; ``os.environ`` when ``None``.

    Returns:
        Mapping with keys ``host``, ``port``, ``user``, ``password``,
        ``admin_dbname``, ``admin_user`` (``PGADMINUSER``, ``"postgres"`` by
        default) and ``admin_password``.

    Raises:
        KeyError: If one of ``PGHOST``, ``PGPORT``, ``PGUSER``, ``PGPASSWORD``,
            ``PGDATABASE`` is unset.

    Examples:
        >>> env = {"PGHOST": "h", "PGPORT": "5432", "PGUSER": "u",
        ...        "PGPASSWORD": "p", "PGDATABASE": "d"}
        >>> pg_credentials_from_env(env)["admin_user"]
        'postgres'
    """
    # Environnement par défaut : celui du processus
    env = os.environ if environ is None else environ
    # Vérification des variables obligatoires
    values = {name: _require(env, name) for name in _PG_REQUIRED}
    return {
        "host": values["PGHOST"],
        "port": values["PGPORT"],
        "user": values["PGUSER"],
        "password": values["PGPASSWORD"],
        "admin_dbname": values["PGDATABASE"],
        "admin_user": env.get("PGADMINUSER") or _DEFAULT_ADMIN_USER,
        "admin_password": values["PGPASSWORD"],
    }


# Fonction de lecture des identifiants S3
def s3_credentials_from_env(
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Read the S3 credentials from the environment.

    ``AWS_SESSION_TOKEN`` is optional: permanent keys carry no session token
    (C-03), so an unset or empty value yields ``None``.

    Args:
        environ: Environment mapping; ``os.environ`` when ``None``.

    Returns:
        Mapping with keys ``endpoint``, ``access_key_id``,
        ``secret_access_key`` and ``session_token`` (possibly ``None``).

    Raises:
        KeyError: If one of ``AWS_S3_ENDPOINT``, ``AWS_ACCESS_KEY_ID``,
            ``AWS_SECRET_ACCESS_KEY`` is unset.

    Examples:
        >>> env = {"AWS_S3_ENDPOINT": "e", "AWS_ACCESS_KEY_ID": "k",
        ...        "AWS_SECRET_ACCESS_KEY": "s"}
        >>> s3_credentials_from_env(env)["session_token"] is None
        True
    """
    # Environnement par défaut : celui du processus
    env = os.environ if environ is None else environ
    # Vérification des variables obligatoires
    values = {name: _require(env, name) for name in _S3_REQUIRED}
    return {
        "endpoint": values["AWS_S3_ENDPOINT"],
        "access_key_id": values["AWS_ACCESS_KEY_ID"],
        "secret_access_key": values["AWS_SECRET_ACCESS_KEY"],
        # Jeton de session optionnel (clés permanentes Minio)
        "session_token": env.get("AWS_SESSION_TOKEN") or None,
    }


# Fonction de construction du connecteur DuckLake
def build_connector(
    location: DuckLakeLocation,
    pg: Mapping[str, Any],
    s3: Mapping[str, Any],
    *,
    create_db_if_missing: bool = True,
    read_only: bool = False,
) -> Any:
    """Build (never connect) the DuckLake connector of a schema.

    The PostgreSQL credential secret is named after the catalog alias
    (``ducklake_pg_<alias>``), so several catalogs can be attached to the same
    DuckDB session without overwriting each other's secret (PS-29.1).

    Args:
        location: Catalog identity and data path of the schema.
        pg: PostgreSQL credentials, as returned by
            :func:`pg_credentials_from_env`.
        s3: S3 credentials, as returned by :func:`s3_credentials_from_env`.
        create_db_if_missing: Whether the catalog database is created when
            absent.
        read_only: Whether the catalog is attached ``READ_ONLY`` (source
            catalogs of the serving layer).

    Returns:
        An unconnected ``dt_ducklake_manager.DuckLakeConnector``.

    Raises:
        ImportError: If ``dt-ducklake-manager`` is not installed.

    Examples:
        >>> connector = build_connector(
        ...     location, pg_credentials_from_env(), s3_credentials_from_env()
        ... )  # doctest: +SKIP
    """
    # Import paresseux : le reste du paquet s'importe sans dt-ducklake-manager
    try:
        from dt_ducklake_manager import DuckLakeConnector
    except ImportError as exc:
        raise ImportError(
            "dt-ducklake-manager is required to build a DuckLake connector"
        ) from exc

    return DuckLakeConnector.from_postgres(
        data_path=location.data_url,
        dbname=location.dbname,
        host=pg["host"],
        port=pg["port"],
        user=pg["user"],
        password=pg["password"],
        create_db_if_missing=create_db_if_missing,
        # Secret propre au catalogue : plusieurs ATTACH sur une même session
        secret_name=f"ducklake_pg_{location.catalog_alias}",
        read_only=read_only,
        admin_dbname=pg["admin_dbname"],
        admin_user=pg["admin_user"],
        admin_password=pg["admin_password"],
        catalog_alias=location.catalog_alias,
        schema=location.schema,
        s3_endpoint=s3["endpoint"],
        s3_access_key_id=s3["access_key_id"],
        s3_secret_access_key=s3["secret_access_key"],
        s3_session_token=s3["session_token"],
    )
