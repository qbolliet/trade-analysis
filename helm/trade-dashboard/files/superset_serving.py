"""Superset configuration fragment: read-only access to a DuckLake catalog from DuckDB.

Executed at the end of ``superset_config.py`` (``configOverrides``), in its
namespace. Every new DuckDB connection opened by ``duckdb-engine`` loads the
``ducklake`` and ``httpfs`` extensions, creates an in-memory S3 secret and
attaches the DuckLake catalog read-only, so that the ``serving`` database of
Superset (URI ``duckdb:///:memory:``, no password) reads the tables published
by the pipeline. PostgreSQL and Redis connections are left untouched.

The credentials are read from the pod environment, filled from Kubernetes
secrets; they are never written to a file nor logged. No persistent DuckDB
secret is used: the web pod and the Celery worker (asynchronous SQL Lab) share
no volume.

Environment:
    PGHOST, PGPORT, PGUSER, PGPASSWORD: PostgreSQL holding the catalog metadata.
    S3_ACCESS_KEY, S3_SECRET_KEY: S3 keys of the Parquet files.

Settings (``serving.json`` next to this file, rendered by the Helm chart; the
defaults below apply when it is absent):
    database: Metadata database of the catalog (``serving``).
    alias: Name the catalog is attached as (``serving``).
    s3Endpoint, s3Region: S3 endpoint and region.
    templateProcessing: ``true`` enables Jinja templating of SQL (virtual
        datasets whose SQL reads the native filters).
"""
import json as _json
import os as _os

import sqlalchemy as _sa
from sqlalchemy import event as _sa_event

# Réglages non secrets du catalogue (ConfigMap du chart), valeurs par défaut sinon
_SERVING_SETTINGS = {
    "database": "serving",
    "alias": "serving",
    "s3Endpoint": "minio.lab.sspcloud.fr",
    "s3Region": "us-east-1",
    "templateProcessing": True,
}
try:
    with open("/app/trade/serving.json", encoding="utf-8") as _handle:
        _SERVING_SETTINGS.update(_json.load(_handle))
except FileNotFoundError:
    pass


# Littéral SQL : apostrophes doublées (valeurs lues dans l'environnement)
def _sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


# Attache du catalogue à chaque nouvelle connexion DuckDB
def _attach_serving_ducklake(dbapi_connection, connection_record):
    # Discriminant : seules les connexions de duckdb-engine sont concernées (pas les
    # connexions PostgreSQL des métadonnées de Superset, ni Redis)
    if type(dbapi_connection).__module__ != "duckdb_engine":
        return
    env = _os.environ
    alias = _SERVING_SETTINGS["alias"]
    try:
        dbapi_connection.execute("INSTALL ducklake; LOAD ducklake;")
        dbapi_connection.execute("INSTALL httpfs; LOAD httpfs;")
        dbapi_connection.execute(
            "CREATE OR REPLACE SECRET s3_serving (TYPE s3, KEY_ID {}, SECRET {}, ENDPOINT {}, REGION {})".format(
                _sql_literal(env["S3_ACCESS_KEY"]),
                _sql_literal(env["S3_SECRET_KEY"]),
                _sql_literal(_SERVING_SETTINGS["s3Endpoint"]),
                _sql_literal(_SERVING_SETTINGS["s3Region"]),
            )
        )
        # Lecture seule ; DATA_PATH lu dans les métadonnées du catalogue
        connection_string = "ducklake:postgres:dbname={} host={} port={} user={} password={}".format(
            _SERVING_SETTINGS["database"],
            env["PGHOST"],
            env.get("PGPORT", "5432"),
            env["PGUSER"],
            env["PGPASSWORD"],
        )
        dbapi_connection.execute(
            "ATTACH {} AS {} (READ_ONLY)".format(_sql_literal(connection_string), alias)
        )
        dbapi_connection.execute("USE {}".format(alias))
    except Exception:
        # Catalogue déjà attaché sur cette connexion (pool), ou connexion DuckDB sans
        # rapport avec le catalogue : ne bloque jamais la connexion elle-même
        pass


_sa_event.listen(_sa.engine.Engine, "connect", _attach_serving_ducklake)

# Gabarits Jinja des datasets virtuels (filtres natifs lus dans le SQL)
if _SERVING_SETTINGS["templateProcessing"]:
    FEATURE_FLAGS = {**globals().get("FEATURE_FLAGS", {}), "ENABLE_TEMPLATE_PROCESSING": True}
