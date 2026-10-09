"""DuckLake connector factory and table write handle.

Single replacement of the connector constructions formerly duplicated in every
script. :func:`pg_credentials_from_env` and :func:`s3_credentials_from_env` are
the only places where the execution environment is read for credentials;
``AWS_SESSION_TOKEN`` is optional, since the permanent Minio keys of the
``trade-s3-credentials`` secret carry no session token.

It also holds :class:`DuckLakeTable`, the single write handle of the
computation steps: creation of a schema on first write, upsert by primary key
afterwards, addition of the columns of a new metric through the native API of
``dt-ducklake-manager`` (never a hand-written ``ALTER TABLE``), transactional
replacement of a slice of rows, run identifier and commit message on every
snapshot. The handle either borrows a connection opened by its caller, or —
built by :meth:`DuckLakeTable.lazy`, as the Kedro datasets do — holds only the
location and credentials of its table and opens a connection on demand. The
credentials are then checked before any connection, with an explicit message
(``"PGHOST is not set: …"``), so that a catalog can be instantiated without
secrets.

``dt_ducklake_manager`` is imported lazily inside :func:`build_connector` and
:class:`DuckLakeTable`, so this module stays importable without it (only the
DuckLake write path requires the dependency).
"""
# Importation des modules
# Modules de base
import functools
import itertools
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence

# Initialisation du logger
logger = logging.getLogger(__name__)


# Variables PostgreSQL obligatoires (catalogue DuckLake)
_PG_REQUIRED = ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE")
# Variables S3 obligatoires (stockage des fichiers Parquet)
_S3_REQUIRED = ("AWS_S3_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
# Rôle d'administration par défaut (création de la base du catalogue)
_DEFAULT_ADMIN_USER = "postgres"
# Identifiants obligatoires à la connexion : clé du mapping -> variable d'origine
# (nommée dans le message d'erreur, puisque credentials.yml la lit)
_PG_CREDENTIALS = {
    "host": "PGHOST",
    "port": "PGPORT",
    "user": "PGUSER",
    "password": "PGPASSWORD",
}
_S3_CREDENTIALS = {
    "endpoint": "AWS_S3_ENDPOINT",
    "access_key_id": "AWS_ACCESS_KEY_ID",
    "secret_access_key": "AWS_SECRET_ACCESS_KEY",
}


# Erreur d'identifiants manquants
class MissingCredentialsError(ValueError):
    """Raised when a DuckLake connection is attempted with incomplete credentials."""


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


# Lecteur de connexion ouvrant sa propre connexion à chaque usage
class ConnectionReader:
    """Picklable reader opening, then closing, its own DuckLake connection.

    Worker processes cannot share a DuckDB connection: each task enters the
    reader to get a connection of its own. The reader holds a *factory* of
    unconnected connectors (never a connection), so it can be sent to a process.

    Attributes:
        connector_factory: Picklable callable returning an unconnected connector
            (an object whose ``connect()`` returns the connection).
        setup_statements: SQL statements run on each new connection (session
            macros).

    Examples:
        >>> class _Conn:
        ...     def execute(self, sql): pass
        ...     def close(self): pass
        >>> class _Connector:
        ...     def connect(self): return _Conn()
        >>> with ConnectionReader(_Connector) as conn:
        ...     conn.execute("SELECT 1")
    """

    def __init__(
        self, connector_factory: Callable[[], Any], setup_statements: Sequence[str] = ()
    ) -> None:
        self.connector_factory = connector_factory
        self.setup_statements = tuple(setup_statements)
        self._connection: Any = None

    def __call__(self) -> "ConnectionReader":
        """Return a fresh, unopened reader sharing the same factory."""
        return ConnectionReader(self.connector_factory, self.setup_statements)

    def __enter__(self) -> Any:
        self._connection = self.connector_factory().connect()
        try:
            for statement in self.setup_statements:
                self._connection.execute(statement)
        except Exception:
            self._connection.close()
            raise
        return self._connection

    def __exit__(self, *exc: Any) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __getstate__(self) -> Dict[str, Any]:
        return {"connector_factory": self.connector_factory,
                "setup_statements": self.setup_statements, "_connection": None}


# Lecteur d'une connexion déjà ouverte (exécution dans le processus appelant)
class BorrowedReader:
    """Reader lending an already open connection, which it never closes.

    Only usable in the calling process (a connection cannot be pickled).

    Attributes:
        connection: Open connection of the caller.

    Examples:
        >>> with BorrowedReader("conn") as conn:
        ...     conn
        'conn'
    """

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def __call__(self) -> "BorrowedReader":
        """Return the reader itself."""
        return self

    def __enter__(self) -> Any:
        return self.connection

    def __exit__(self, *exc: Any) -> None:
        return None

    def __getstate__(self) -> Dict[str, Any]:
        raise TypeError(
            "A borrowed connection cannot be sent to another process: "
            "give a ConnectionReader (connector factory) when n_jobs > 1."
        )


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


# Fonction de vérification des identifiants avant connexion
def require_credentials(
    pg: Optional[Mapping[str, Any]],
    s3: Optional[Mapping[str, Any]],
    *,
    what: str = "the DuckLake catalog",
) -> None:
    """Check that the credentials needed by a DuckLake connection are set.

    The Kedro credentials resolve unset environment variables to empty values,
    so that a catalog can be built without secrets: the error is raised here,
    at the first real operation, naming the missing variable.

    Args:
        pg: PostgreSQL credentials (``host``, ``port``, ``user``, ``password``…).
        s3: S3 credentials (``endpoint``, ``access_key_id``, ``secret_access_key``…).
        what: Description of the target, used in the message.

    Raises:
        MissingCredentialsError: If a mandatory value is missing or empty; the
            message starts with the variable name (``"PGHOST is not set: …"``).

    Examples:
        >>> require_credentials(  # doctest: +ELLIPSIS
        ...     {"host": "", "port": "5432", "user": "u", "password": "p"}, {}
        ... )
        Traceback (most recent call last):
        ...
        kedro_pipeline.io.ducklake.MissingCredentialsError: PGHOST is not set: ...
    """
    for mapping, names, kind in ((pg, _PG_CREDENTIALS, "PostgreSQL"), (s3, _S3_CREDENTIALS, "S3")):
        for key, variable in names.items():
            if not (mapping or {}).get(key):
                raise MissingCredentialsError(
                    f"{variable} is not set: {kind} credential '{key}' is required to "
                    f"connect to {what} (environment variable read by credentials.yml)"
                )


# Fabrique de connecteur précédée de la vérification des identifiants
class CheckedConnectorFactory:
    """Connector factory that checks the credentials before building the connector.

    Picklable (it holds only the wrapped factory), so it can travel with a
    handle sent to a worker process.

    Args:
        factory: Wrapped factory, :func:`build_connector` by default.

    Examples:
        >>> CheckedConnectorFactory()(location, {}, {})  # doctest: +SKIP
        Traceback (most recent call last):
        ...
        MissingCredentialsError: PGHOST is not set: ...
    """

    def __init__(self, factory: Optional[Callable[..., Any]] = None) -> None:
        self.factory = factory

    def __call__(
        self,
        location: DuckLakeLocation,
        pg: Optional[Mapping[str, Any]],
        s3: Optional[Mapping[str, Any]],
        **kwargs: Any,
    ) -> Any:
        """Check the credentials, then build the connector (never connected)."""
        require_credentials(pg, s3, what=f"the DuckLake catalog '{location.catalog_alias}'")
        return (self.factory or build_connector)(location, pg, s3, **kwargs)


# Classe de connecteur à métadonnées SQLite locales (construite une seule fois)
@functools.lru_cache(maxsize=None)
def _sqlite_connector_class(base: type) -> type:
    """Return a ``DuckLakeConnector`` subclass suited to a local SQLite catalog.

    Two adjustments to the library connector:

    * the parent directories are created from the real file path: the library
      derives them from the ``sqlite:<path>`` attach target, which is not a
      valid directory name on Windows;
    * the SQLite metadata are attached with a write-ahead journal and a lock
      timeout, so that a reader connection (a worker, a read-only source
      catalog) waits for a writer of the same process instead of failing.

    Args:
        base: The ``DuckLakeConnector`` class (imported lazily by the caller).

    Returns:
        The subclass.
    """

    class SqliteFileConnector(base):  # type: ignore[misc, valid-type]
        """``DuckLakeConnector`` on a local SQLite metadata file."""

        # Chemin réel du fichier de métadonnées (sans le préfixe du backend)
        @property
        def metadata_file(self) -> str:
            return self.catalog_path.split(":", 1)[1]

        def _ensure_paths_exist(self) -> None:
            os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
            os.makedirs(self.data_path, exist_ok=True)

        def _build_attach_sql(self) -> str:
            sql = super()._build_attach_sql()
            # Journal WAL (écriture seulement) et attente des verrous SQLite
            extra = ["META_BUSY_TIMEOUT 60000"]
            if not self.read_only:
                extra.insert(0, "META_JOURNAL_MODE 'WAL'")
            return f"{sql[:-1]}, {', '.join(extra)})"

    return SqliteFileConnector


# Fabrique de connecteurs vers des catalogues DuckLake locaux (fichiers)
class FileConnectorFactory:
    """Connector factory targeting local file catalogs instead of PostgreSQL + S3.

    One catalog per database name: the metadata live in
    ``<root>/<dbname>.sqlite`` (SQLite backend, which several connections of
    one process — or several worker processes — can attach at the same time,
    unlike a ``.ducklake`` DuckDB file locked by its first user) and the
    Parquet files under ``<root>/<dbname>_data/``. The bucket and the data path
    of the location are ignored: every table of a catalog shares its data
    directory, as on S3. Same call signature as :func:`build_connector`, and
    picklable (it holds a path only), so a handle built with it can travel to a
    worker process.

    Args:
        root: Directory holding the catalogs (created on first connection).

    Examples:
        >>> factory = FileConnectorFactory("/tmp/catalogs")
        >>> location = DuckLakeLocation(dbname="comtrade", catalog_alias="comtrade",
        ...     schema="C_A_HS", bucket="b", data_path="trade/datasets/comtrade")
        >>> factory.paths(location)  # doctest: +SKIP
        ('/tmp/catalogs/comtrade.sqlite', '/tmp/catalogs/comtrade_data/')
    """

    def __init__(self, root: Any) -> None:
        self.root = str(root)

    # Chemins du catalogue et des données d'un emplacement
    def paths(self, location: DuckLakeLocation) -> tuple:
        """Return ``(metadata file, data directory)`` of the catalog of ``location``."""
        from pathlib import Path

        root = Path(self.root)
        return (root / f"{location.dbname}.sqlite").as_posix(), (root / f"{location.dbname}_data").as_posix() + "/"

    def __call__(
        self,
        location: DuckLakeLocation,
        pg: Optional[Mapping[str, Any]] = None,
        s3: Optional[Mapping[str, Any]] = None,
        *,
        create_db_if_missing: bool = True,
        read_only: bool = False,
    ) -> Any:
        """Build (never connect) the file connector of ``location``.

        Args:
            location: Catalog identity and schema.
            pg: Ignored (no PostgreSQL server).
            s3: Ignored (no object storage).
            create_db_if_missing: Ignored: a file catalog is created on attach.
            read_only: Whether the catalog is attached ``READ_ONLY``.

        Returns:
            An unconnected ``dt_ducklake_manager.DuckLakeConnector``.

        Raises:
            ImportError: If ``dt-ducklake-manager`` is not installed.
        """
        try:
            from dt_ducklake_manager import DuckLakeConnector
        except ImportError as exc:
            raise ImportError(
                "dt-ducklake-manager is required to build a DuckLake connector"
            ) from exc

        catalog_path, data_path = self.paths(location)
        return _sqlite_connector_class(DuckLakeConnector)(
            catalog_path=f"sqlite:{catalog_path}",
            data_path=data_path,
            catalog_type="sqlite",
            catalog_alias=location.catalog_alias,
            schema=location.schema,
            read_only=read_only,
        )

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, FileConnectorFactory) and other.root == self.root

    def __hash__(self) -> int:
        return hash((FileConnectorFactory, self.root))


# Fonction de choix de la fabrique de connecteurs d'après les identifiants
def connector_factory_for(credentials: Optional[Mapping[str, Any]]) -> Callable[..., Any]:
    """Choose the connector factory of a ``ducklake`` credentials entry.

    A ``file_root`` key (local and test environments) selects local file
    catalogs under that directory; otherwise the catalogs are PostgreSQL +
    S3, with the credentials checked before the first connection.

    Args:
        credentials: ``ducklake`` entry of ``credentials.yml``, or ``None``.

    Returns:
        A connector factory with the signature of :func:`build_connector`.

    Examples:
        >>> isinstance(connector_factory_for({"file_root": "/tmp/x"}), FileConnectorFactory)
        True
        >>> isinstance(connector_factory_for(None), CheckedConnectorFactory)
        True
    """
    root = (credentials or {}).get("file_root")
    return FileConnectorFactory(root) if root else CheckedConnectorFactory()


# Fonction de lecture de l'identifiant d'exécution Argo
def workflow_run_id(environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """Return the Argo workflow id recorded on the DuckLake snapshots.

    Args:
        environ: Environment mapping; ``os.environ`` when ``None``.

    Returns:
        ``WORKFLOW_ID`` when defined and not empty, else ``None`` (local run:
        the write stays valid, without run identifier).

    Examples:
        >>> workflow_run_id({"WORKFLOW_ID": "wf-1"}), workflow_run_id({})
        ('wf-1', None)
    """
    # Environnement par défaut : celui du processus
    env = os.environ if environ is None else environ
    return env.get("WORKFLOW_ID") or None


# Poignée d'écriture d'une table de faits DuckLake
class DuckLakeTable:
    """Write handle of one DuckLake fact table (creation, upsert, column addition).

    Single entry point of the computation steps into ``dt-ducklake-manager``:
    the first write creates the schema (``DuckLakeTablesBuilder``, through
    ``statflows.write_dataframe``), the next ones upsert by primary key
    (``DatabaseUpdater.update_database``). The schema evolves through the
    library only, never through hand-written ``ALTER TABLE`` statements:

    * a column of the frame absent from the table (a new metric) is added
      before the upsert when ``allow_new_columns`` is true, with its metadata
      row; the rows not rewritten carry ``NULL`` until their recomputation;
    * a column of the table absent from the frame keeps its stored values on
      the rows the upsert updates (the library only sets the columns of the
      frame) and is ``NULL`` on the rows it inserts;
    * columns are never dropped nor retyped;
    * :meth:`add_columns` spreads a new column over existing rows by primary
      key without rewriting the other columns, for one-off migrations.

    Every write carries a run identifier (the Argo workflow) and a commit
    message, recorded on the DuckLake snapshot, and leaves the compaction to
    the planned maintenance pass by default.

    ``dt_ducklake_manager`` and ``statflows`` are imported lazily, so that this
    module stays importable without them.

    Two modes:

    * **borrowed connection** (``DuckLakeTable(conn, alias, schema)``): the
      caller owns the open connection, the handle never closes it;
    * **lazy handle** (:meth:`lazy`, the Kedro datasets): the handle holds the
      location and the credentials only, never a connection; :meth:`connect`
      opens one (credentials checked first) and every operation called without
      an open connection opens a short-lived one.

    Args:
        conn: Open DuckLake connection, owned by the caller (``None`` for a lazy
            handle).
        catalog_alias: Alias under which the catalog is attached.
        schema: Schema holding the fact table.
        categorical_threshold: Maximum cardinality for a text column to become
            a dimension table at creation; ``None`` disables dimension tables.
        label: Optional prefix identifying the table in the logs.
        location: Catalog identity and data path (lazy handle).
        pg: PostgreSQL credentials (lazy handle).
        s3: S3 credentials (lazy handle).
        connector_factory: Builds an unconnected connector from
            ``(location, pg, s3)``; by default :func:`build_connector` preceded by
            :func:`require_credentials` (a file-catalog factory in tests).

    Examples:
        >>> table = DuckLakeTable(conn, "vulnerabilities", "indicators")  # doctest: +SKIP
        >>> table.upsert(df, ["reporter", "product"], run_id="wf-1",
        ...              commit_message="compute_x FR")  # doctest: +SKIP
        True
        >>> lazy = DuckLakeTable.lazy(DuckLakeLocation(
        ...     dbname="comtrade", catalog_alias="comtrade", schema="C_A_HS",
        ...     bucket="b", data_path="trade/datasets/comtrade"), pg={}, s3={})
        >>> lazy.qualified_name, lazy.conn is None
        ('"comtrade"."C_A_HS"."fact_table"', True)
    """

    def __init__(
        self,
        conn: Any,
        catalog_alias: str,
        schema: str,
        *,
        categorical_threshold: Optional[int] = None,
        label: Optional[str] = None,
        location: Optional[DuckLakeLocation] = None,
        pg: Optional[Mapping[str, Any]] = None,
        s3: Optional[Mapping[str, Any]] = None,
        connector_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.conn = conn
        self.catalog_alias = catalog_alias
        self.schema = schema
        self.categorical_threshold = categorical_threshold
        self.label = label
        self.location = location
        self.pg = pg
        self.s3 = s3
        self.connector_factory = connector_factory

    # Constructeur d'une poignée paresseuse (aucune connexion)
    @classmethod
    def lazy(
        cls,
        location: DuckLakeLocation,
        pg: Optional[Mapping[str, Any]],
        s3: Optional[Mapping[str, Any]],
        *,
        connector_factory: Optional[Callable[..., Any]] = None,
        categorical_threshold: Optional[int] = None,
        label: Optional[str] = None,
    ) -> "DuckLakeTable":
        """Build a handle that holds a location and credentials, never a connection.

        Args:
            location: Catalog identity, schema and data path of the table.
            pg: PostgreSQL credentials (``credentials.yml`` entry ``ducklake_postgres``).
            s3: S3 credentials (``credentials.yml`` entry ``s3``).
            connector_factory: Factory of unconnected connectors; checked
                :func:`build_connector` by default.
            categorical_threshold: See the class arguments.
            label: See the class arguments.

        Returns:
            The lazy handle.
        """
        return cls(
            None,
            location.catalog_alias,
            location.schema,
            categorical_threshold=categorical_threshold,
            label=label,
            location=location,
            pg=pg,
            s3=s3,
            connector_factory=connector_factory,
        )

    # Propriété : nom qualifié de la table de faits
    @property
    def qualified_name(self) -> str:
        """Quoted qualified name of the fact table (``"alias"."schema"."table"``)."""
        table = self.location.table if self.location is not None else "fact_table"
        return f'"{self.catalog_alias}"."{self.schema}"."{table}"'

    # Gestionnaire de contexte : connexion ouverte à la demande
    @contextmanager
    def connect(self) -> Iterator[Any]:
        """Yield an open connection attached to the catalog of the table.

        A borrowed connection is yielded as is (and left open). A lazy handle
        checks its credentials, opens a connection positioned on its schema,
        binds it for the duration of the block, then closes it.

        Yields:
            The open DuckDB connection.

        Raises:
            MissingCredentialsError: If a mandatory credential is empty.
            RuntimeError: If the handle has neither a connection nor a location.
        """
        # Connexion empruntée : l'appelant la possède et la ferme
        if self.conn is not None:
            yield self.conn
            return
        if self.location is None:
            raise RuntimeError(
                f"DuckLakeTable '{self.schema}' has neither an open connection nor a location"
            )
        factory = self.connector_factory or CheckedConnectorFactory()
        conn = factory(self.location, self.pg, self.s3).connect()
        self.conn = conn
        try:
            yield conn
        finally:
            self.conn = None
            conn.close()

    # Connecteur non connecté de la poignée (bibliothèques attendant un connecteur)
    def connector(self) -> Any:
        """Return the unconnected connector of a lazy handle.

        For the libraries that take a connector rather than a connection
        (``statflows.download_updates``, the reference publication): its
        ``connect()`` opens a connection positioned on the schema of the table.

        Returns:
            The connector built by the handle's factory (credentials checked
            first by the default factory).

        Raises:
            RuntimeError: If the handle borrows a connection (no location).
        """
        if self.location is None:
            raise RuntimeError(
                f"DuckLakeTable '{self.schema}' borrows a connection: it has no connector"
            )
        factory = self.connector_factory or CheckedConnectorFactory()
        return factory(self.location, self.pg, self.s3)

    # Lecteur de connexion transmissible aux processus de calcul
    def reader(self, setup_statements: Sequence[str] = ()) -> Any:
        """Return a reader giving each worker a connection of its own.

        Args:
            setup_statements: SQL statements run on each new connection
                (session macros).

        Returns:
            A picklable :class:`ConnectionReader` for a lazy handle; a
            :class:`BorrowedReader` lending the bound connection otherwise
            (usable in the calling process only, i.e. with one job).

        Raises:
            RuntimeError: If the handle has neither a connection nor a location.
        """
        if self.location is not None:
            return ConnectionReader(
                partial(
                    self.connector_factory or CheckedConnectorFactory(),
                    self.location, self.pg, self.s3,
                ),
                setup_statements,
            )
        if self.conn is None:
            raise RuntimeError(
                f"DuckLakeTable '{self.schema}' has neither an open connection nor a location"
            )
        return BorrowedReader(self.conn)

    # Connexion d'une opération : fournie, liée à la poignée, sinon ouverte pour elle
    @contextmanager
    def _session(self, conn: Any = None) -> Iterator[Any]:
        """Yield the connection of one operation (opened on demand)."""
        if conn is not None:
            yield conn
            return
        with self.connect() as opened:
            yield opened

    # Existence de la table de faits
    def exists(self, conn: Any = None) -> bool:
        """Tell whether the fact table of the schema exists.

        Args:
            conn: Connection to use instead of the handle's one.

        Returns:
            ``True`` when the fact table exists.

        Raises:
            MissingCredentialsError: If a lazy handle lacks a credential.
        """
        from statflows.storage.ducklake.tables import fact_table_exists

        with self._session(conn) as session:
            return fact_table_exists(session, self.catalog_alias, self.schema)

    # Lecture SQL avec paramètres liés
    def query(
        self, sql: str, params: Optional[Sequence[Any]] = None, *, conn: Any = None
    ) -> Any:
        """Run a read query and return its result as a pandas DataFrame.

        Filters are pushed down in SQL (bound parameters ``?``): the table is
        never read in full.

        Args:
            sql: Query, referencing the table by :attr:`qualified_name`.
            params: Values bound to the ``?`` placeholders.
            conn: Connection to use instead of the handle's one.

        Returns:
            The result frame.

        Raises:
            MissingCredentialsError: If a lazy handle lacks a credential.

        Examples:
            >>> table.query(  # doctest: +SKIP
            ...     f'SELECT * FROM {table.qualified_name} WHERE "refYear" = ?', [2020]
            ... )
        """
        with self._session(conn) as session:
            if params is None:
                return session.execute(sql).df()
            return session.execute(sql, list(params)).df()

    # Ajout des colonnes absentes de la table (nouvelle métrique), sans écrire de ligne
    def add_missing_columns(self, df: Any, *, conn: Any = None) -> List[str]:
        """Add to the fact table the columns of ``df`` it lacks, without writing rows.

        The columns are added by the library (``ALTER TABLE … ADD COLUMN`` with
        its metadata row, type inferred from ``df``), exactly as an upsert with
        ``allow_new_columns`` would; existing rows carry ``NULL``. A missing
        table is left alone: its first write creates every column.

        Args:
            df: Frame (pandas, polars or narwhals) carrying the new columns.
            conn: Connection to use instead of the handle's one.

        Returns:
            The added columns, in frame order (empty when none).

        Raises:
            ImportError: If ``dt-ducklake-manager`` is not installed.
            MissingCredentialsError: If a lazy handle lacks a credential.
        """
        import narwhals as nw

        with self._session(conn) as session:
            if not self.exists(session):
                return []
            updater = self._updater(session)
            frame = nw.from_native(df, eager_only=True)
            existing = set(updater._get_fact_table_columns())
            missing = [column for column in frame.columns if column not in existing]
            if missing:
                # Routine d'ajout de colonnes de la bibliothèque (celle de l'upsert)
                updater._add_new_columns_from_update(frame, missing, None)
                updater._invalidate_metadata_cache()
            return missing

    # Gestionnaire de mise à jour de la bibliothèque
    def _updater(self, conn: Any) -> Any:
        """Build the ``DatabaseUpdater`` of the schema."""
        from dt_ducklake_manager import DatabaseUpdater

        return DatabaseUpdater(
            connection=conn,
            categorical_threshold=self.categorical_threshold,
            catalog_alias=self.catalog_alias,
            schema=self.schema,
        )

    # Création de la table par le premier lot
    def _create(
        self,
        conn: Any,
        df: Any,
        primary_keys: Sequence[str],
        build_options: Optional[Mapping[str, Any]],
        run_id: Optional[str],
        commit_message: Optional[str],
    ) -> None:
        """Create the schema from a first frame (metadata, dimensions, fact table)."""
        from statflows.storage.ducklake.tables import write_dataframe

        write_dataframe(
            conn,
            df,
            list(primary_keys),
            catalog_alias=self.catalog_alias,
            schema=self.schema,
            categorical_threshold=self.categorical_threshold,
            label=self.label,
            build_options=build_options,
            run_id=run_id,
            commit_message=commit_message,
        )

    # Insertion ou mise à jour d'un jeu de données
    def upsert(
        self,
        df: Any,
        primary_keys: Sequence[str],
        *,
        allow_new_columns: bool = True,
        compact_after_update: bool = False,
        run_id: Optional[str] = None,
        commit_message: Optional[str] = None,
        conn: Any = None,
        build_options: Optional[Mapping[str, Any]] = None,
        delete_where: Optional[str] = None,
    ) -> bool:
        """Create the schema on first write, else upsert by primary key.

        Args:
            df: Frame to write (any ``IntoDataFrame``: pandas, polars,
                narwhals). It may carry a subset of the table columns: the
                columns it lacks keep their stored values on the updated rows.
            primary_keys: Primary-key columns (used at creation; the upsert
                uses the keys recorded in the table metadata).
            allow_new_columns: Whether columns of ``df`` absent from the table
                are added before the upsert (``ALTER TABLE … ADD COLUMN`` and
                metadata row, by the library). When false, an unknown column
                raises ``ValueError``.
            compact_after_update: Whether to compact right after the commit;
                false by default, the compaction being left to the planned
                maintenance pass.
            run_id: Run identifier recorded on the snapshot.
            commit_message: Commit message recorded on the snapshot.
            conn: Connection to use instead of the handle's one (a lazy handle
                opens one for the call when neither is available).
            build_options: Extra arguments of the creation
                (``{"partition_by": [...]}``), ignored on an existing table.
            delete_where: SQL condition on the fact table: the matching rows
                are deleted in the same transaction as the upsert, so that the
                frame **replaces** that slice of the table (rows no longer
                produced disappear). ``None`` upserts only.

        Returns:
            ``True`` when the schema was created by this call, ``False`` when
            it was updated.

        Raises:
            ImportError: If ``dt-ducklake-manager`` is not installed.
            ValueError: If ``df`` carries an unknown column while
                ``allow_new_columns`` is false, or if the library reports a
                failed update.
        """
        with self._session(conn) as conn:
            # Remplacement d'une tranche : suppression et upsert dans une transaction
            if delete_where is not None:
                return self.upsert_many(
                    [df], primary_keys, delete_where=delete_where,
                    allow_new_columns=allow_new_columns, run_id=run_id,
                    commit_message=commit_message, conn=conn, build_options=build_options,
                )
            # Première écriture : création du schéma
            if not self.exists(conn):
                self._create(conn, df, primary_keys, build_options, run_id, commit_message)
                return True
            # Table existante : upsert par la bibliothèque (évolution de schéma native)
            success = self._updater(conn).update_database(
                df,
                use_transaction=True,
                compact_after_update=compact_after_update,
                allow_new_columns=allow_new_columns,
                run_id=run_id,
                commit_message=commit_message,
            )
            if not success:
                raise ValueError(
                    f"{self.label + ': ' if self.label else ''}DatabaseUpdater reported "
                    f"failure for schema '{self.schema}'"
                )
            return False

    # Remplacement transactionnel d'une tranche de la table par plusieurs lots
    def upsert_many(
        self,
        frames: Iterable[Any],
        primary_keys: Sequence[str],
        *,
        delete_where: Optional[str] = None,
        allow_new_columns: bool = True,
        run_id: Optional[str] = None,
        commit_message: Optional[str] = None,
        commit_info: Optional[Mapping[str, Any]] = None,
        conn: Any = None,
        build_options: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        """Delete a slice of the table, then upsert several frames, atomically.

        On an existing table: ``BEGIN``; delete the rows matching
        ``delete_where`` (if any); upsert every non-empty frame; record the
        commit message; ``COMMIT``. Any failure rolls the whole operation back,
        so the table is never left with a slice half replaced. This is how a
        long table whose set of rows may change between two computations (a
        diagnostic emitted by one fit and not by the next) is kept exact.

        On a missing table, the first non-empty frame creates it (its own
        transaction) and the next ones are upserted in a second transaction.

        Args:
            frames: Frames to write, all with the same primary key.
            primary_keys: Primary-key columns (used at creation).
            delete_where: SQL condition selecting the rows to replace; ``None``
                deletes nothing.
            allow_new_columns: Whether new columns may be added (see
                :meth:`upsert`).
            run_id: Run identifier recorded on the snapshot.
            commit_message: Commit message recorded on the snapshot.
            commit_info: Extra JSON fields recorded on the snapshot.
            conn: Connection to use instead of the handle's one.
            build_options: Extra arguments of the creation.

        Returns:
            ``True`` when the schema was created by this call.

        Raises:
            ImportError: If ``dt-ducklake-manager`` is not installed.
            RuntimeError: If the deletion or an upsert fails (after rollback).
            ValueError: If a frame carries an unknown column while
                ``allow_new_columns`` is false (after rollback).
        """
        from dt_ducklake_manager.operations.deleter import DatabaseDeleter

        with self._session(conn) as conn:
            # Lots non vides, parcourus paresseusement : un appelant qui produit ses
            # lots par tranches (une année BACI) garde une mémoire bornée à un lot
            non_empty = (frame for frame in frames if len(frame) > 0)
            created = False
            # Table absente : création par le premier lot non vide, rien à supprimer
            if not self.exists(conn):
                first = next(non_empty, None)
                if first is None:
                    return False
                self._create(conn, first, primary_keys, build_options, run_id, commit_message)
                created = True
                delete_where = None
            # Lot suivant lu d'avance : aucune transaction vide n'est ouverte
            following = next(non_empty, None)
            if following is None and delete_where is None:
                return created
        pending = itertools.chain([following] if following is not None else [], non_empty)

        updater = self._updater(conn)
        conn.begin()
        try:
            # Suppression de la tranche remplacée, sans nettoyage de colonnes : une
            # colonne vidée par la suppression est réécrite par l'upsert qui suit
            if delete_where is not None:
                deleter = DatabaseDeleter(
                    connection=conn, catalog_alias=self.catalog_alias, schema=self.schema
                )
                report = deleter.delete_rows(
                    delete_where, use_transaction=False, perform_cleanup=False,
                    compact_after_update=False,
                )
                if any("failed" in str(warning).lower() for warning in report.warnings):
                    raise RuntimeError(
                        f"Deletion of '{delete_where}' in '{self.schema}' failed: {report.warnings}"
                    )
            for frame in pending:
                ok = updater.update_database(
                    frame, use_transaction=False, compact_after_update=False,
                    allow_new_columns=allow_new_columns,
                )
                if not ok:
                    raise RuntimeError(f"Upsert into '{self.schema}' failed")
            self._commit_message(conn, run_id, commit_message, commit_info)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        return created

    # Message de commit d'une transaction ouverte par la poignée
    def _commit_message(
        self,
        conn: Any,
        run_id: Optional[str],
        commit_message: Optional[str],
        commit_info: Optional[Mapping[str, Any]],
    ) -> None:
        """Record the run id and message on the snapshot (never fails the write)."""
        import json

        if run_id is None and commit_message is None and not commit_info:
            return
        try:
            conn.execute(
                f"CALL ducklake_set_commit_message('{self.catalog_alias}', ?, ?, extra_info := ?)",
                [run_id, commit_message, json.dumps(dict(commit_info or {}))],
            )
        except Exception as exc:  # pragma: no cover - traçabilité seulement
            # Connexion sans catalogue DuckLake réel : écriture conservée
            logger.debug("Message de commit non enregistré : %s", exc)

    # Diffusion de colonnes nouvelles sur les lignes existantes (migrations)
    def add_columns(
        self,
        df: Any,
        overwrite: bool = False,
        *,
        compact_after_update: bool = False,
        run_id: Optional[str] = None,
        commit_message: Optional[str] = None,
        conn: Any = None,
    ) -> Any:
        """Spread value columns onto existing rows by primary key (one-off migrations).

        ``df`` carries every primary-key column and the columns to add; the
        library adds the columns, updates the matching rows in a single
        ``UPDATE … FROM`` and inserts the unknown keys, other columns left
        ``NULL``. The pipeline itself recomputes whole rows (:meth:`upsert`):
        this method is reserved to migrations.

        Args:
            df: Primary keys plus the value columns to add, unique on the keys.
            overwrite: Whether a column already in the table may be rewritten.
            compact_after_update: Whether to compact right after the commit.
            run_id: Run identifier recorded on the snapshot.
            commit_message: Commit message recorded on the snapshot.
            conn: Connection to use instead of the handle's one.

        Returns:
            The ``OperationReport`` of the library.

        Raises:
            ImportError: If ``dt-ducklake-manager`` is not installed.
            ValueError: If a key is missing or null, if ``df`` is not unique on
                the keys, or if a column exists while ``overwrite`` is false.
        """
        with self._session(conn) as session:
            return self._updater(session).add_columns(
                df,
                overwrite=overwrite,
                compact_after_update=compact_after_update,
                run_id=run_id,
                commit_message=commit_message,
            )

    # Écrivain lié aux options d'une étape
    def writer(
        self,
        *,
        build_options: Optional[Mapping[str, Any]] = None,
        run_id: Optional[str] = None,
        commit_message: Optional[str] = None,
    ) -> Callable[[Any, Sequence[str]], bool]:
        """Return a ``(frame, primary_keys) -> created`` writer bound to options.

        Used by the methodology runners, which know neither DuckLake nor this
        module: they receive the writer and call it with their result.

        Args:
            build_options: Extra arguments of the creation.
            run_id: Run identifier recorded on the snapshots.
            commit_message: Commit message recorded on the snapshots.

        Returns:
            The writer.
        """
        def write(df: Any, primary_keys: Sequence[str]) -> bool:
            return self.upsert(
                df, primary_keys, build_options=build_options, run_id=run_id,
                commit_message=commit_message,
            )

        return write


# Fonction de lecture de l'alias effectif du catalogue d'une poignée
def attached_catalog_alias(table: Any) -> str:
    """Return the alias under which the catalog of a table handle is attached.

    The alias is the one of the connector actually built (the configured one
    for the production factory), so that the qualified names of the writes
    match the attached catalog; a handle borrowing a connection keeps its own.

    Args:
        table: :class:`DuckLakeTable`.

    Returns:
        The catalog alias.
    """
    if getattr(table, "location", None) is None:
        return table.catalog_alias
    return table.connector().catalog_alias


# Fonction de construction des options d'écriture des étapes de calcul
def compute_write_options(
    commit_message: str,
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Build the ``write_dataframe`` options shared by the computation steps (PD-11).

    The computation steps (partner and network vulnerabilities, BACI, synthesis,
    coherence) add the columns of a new metric on the fly and leave the
    compaction to a planned maintenance pass. Every write carries the Argo
    workflow id and an explicit message: they appear on the DuckLake snapshot,
    which ties a write back to its execution.

    Args:
        commit_message: Message recorded on the snapshot, ``"<script> <unit>"``.
        environ: Environment mapping; ``os.environ`` when ``None``.

    Returns:
        Keyword arguments for ``statflows.write_dataframe``: ``update_options``
        (``allow_new_columns=True``, ``compact_after_update=False``), ``run_id``
        (``WORKFLOW_ID`` when defined, else ``None``) and ``commit_message``.

    Examples:
        >>> options = compute_write_options("compute_x C_A_HS", {"WORKFLOW_ID": "wf-1"})
        >>> options["run_id"], options["update_options"]["allow_new_columns"]
        ('wf-1', True)
        >>> compute_write_options("compute_x", {})["run_id"] is None
        True
    """
    # Environnement par défaut : celui du processus
    env = os.environ if environ is None else environ
    return {
        "update_options": {"allow_new_columns": True, "compact_after_update": False},
        # Hors Argo, la variable est absente : l'écriture reste valide, sans run_id
        "run_id": env.get("WORKFLOW_ID") or None,
        "commit_message": commit_message,
    }


# Fonction de construction des options de tamponnage du téléchargement
def download_buffering_options(
    buffering: Optional[Mapping[str, Any]],
    shard_key: Optional[Callable[[Any], str]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Translate ``DOWNLOADS.<DATAFLOW>.BUFFERING`` into ``download_updates`` arguments (PS-27).

    Without a ``BUFFERING`` block, the result reproduces the unbuffered historical
    behaviour of ``statflows`` (registry rewritten and DuckLake written after every
    query), except for the compaction, which stays disabled unless the configuration
    asks for it.

    Args:
        buffering: ``BUFFERING`` mapping of the dataflow configuration (``None`` or
            empty for the historical behaviour).
        shard_key: Callable ``query -> fragment name`` used when ``SHARD_REGISTRY``
            is true (reporter for Eurostat, year for Comtrade).
        environ: Environment mapping; ``os.environ`` when ``None``.

    Returns:
        Keyword arguments for ``statflows.download_updates``:
        ``registry_flush_every``, ``registry_flush_seconds``,
        ``registry_shard_key``, ``write_batch_rows``, ``write_batch_queries``,
        ``update_options`` (``compact_after_update``), ``ducklake_options`` and
        ``run_id`` (``WORKFLOW_ID`` when defined).

    Raises:
        ValueError: If ``SHARD_REGISTRY`` is true and no ``shard_key`` is given.

    Examples:
        >>> options = download_buffering_options(
        ...     {"REGISTRY_FLUSH_EVERY": 500, "SHARD_REGISTRY": True},
        ...     shard_key=lambda query: "FR",
        ...     environ={},
        ... )
        >>> options["registry_flush_every"], options["registry_shard_key"] is not None
        (500, True)
        >>> download_buffering_options(None, environ={})["registry_flush_every"]
        1
    """
    # Environnement par défaut : celui du processus
    env = os.environ if environ is None else environ
    config = buffering or {}
    # Fragmentation du registre : la clé de fragment est propre à la source
    if config.get("SHARD_REGISTRY") and shard_key is None:
        raise ValueError("SHARD_REGISTRY est activé mais aucune clé de fragment n'est fournie")
    return {
        "registry_flush_every": int(config.get("REGISTRY_FLUSH_EVERY") or 1),
        "registry_flush_seconds": config.get("REGISTRY_FLUSH_SECONDS"),
        "registry_shard_key": shard_key if config.get("SHARD_REGISTRY") else None,
        "write_batch_rows": config.get("WRITE_BATCH_ROWS"),
        "write_batch_queries": config.get("WRITE_BATCH_QUERIES"),
        # Les téléchargements n'ajoutent jamais de colonne (allow_new_columns absent)
        "update_options": {"compact_after_update": bool(config.get("COMPACT_AFTER_UPDATE", False))},
        "ducklake_options": config.get("DUCKLAKE_OPTIONS"),
        "run_id": env.get("WORKFLOW_ID") or None,
    }
