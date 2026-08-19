# Importation des modules
# Modules de base
import os
from enum import StrEnum
from pathlib import Path
import logging

# DuckDB
import duckdb

# PostgreSQL
import psycopg2

# Emplacement du fichier
FILE_PATH = Path(os.path.abspath(__file__))


# Fonction d'initialisation du logger
def _init_logger(filename: str | os.PathLike[str]) -> logging.Logger:
    """
    Initializes the logger for logging to a file.

    Parameters:
        filename (os.PathLike): Path to the log file.

    Returns:
        logging.Logger: Initialized logger object.

    Note:
        This function configures logging to output messages to both console and a file.
    """
    # Configuration de logging
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    # Vérification de l'existance du dossier pour le fichier de log
    log_directory = os.path.dirname(filename)

    if (not os.path.exists(log_directory)) & (log_directory != ""):
        os.makedirs(log_directory)

    # Configuration du fichier de logs
    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.INFO)

    # Set a formatter for the file handler
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Initialisation du logger
    logger = logging.getLogger()
    logger.addHandler(file_handler)

    return logger


# Énumération des backends de catalogue supportés par DuckLake
class CatalogType(StrEnum):
    """Supported DuckLake catalog backends.

    DuckLake stores its catalog metadata in a backing database whose type is
    selected by the prefix of the ``ATTACH`` target string:

    - ``DUCKDB``: a local ``.ducklake`` file (``ducklake:<path>``). Default,
      single-process; the file is locked at the process level, so it cannot be
      read by one program while another writes it.
    - ``SQLITE``: a local SQLite file (``ducklake:sqlite:<path>``).
    - ``POSTGRES``: a PostgreSQL server (``ducklake:postgres:<conn>``). A true
      multi-client backend: a read-only consumer (e.g. a GraphQL API) can query
      the catalog while another process updates it, without lock conflicts.

    A ``StrEnum`` so the members compare equal to their plain string values
    (e.g. ``CatalogType.POSTGRES == "postgres"``).
    """

    DUCKDB = "duckdb"
    SQLITE = "sqlite"
    POSTGRES = "postgres"


# Fonction auxiliaire d'échappement d'une valeur destinée à un littéral de chaîne SQL
def _quote_literal(value: str) -> str:
    """Escape single quotes for safe inclusion inside a SQL string literal.

    The value is meant to be wrapped by single quotes by the caller; this helper
    only doubles any embedded single quote, following the SQL standard.

    Args:
        value (str): Raw value to escape (e.g. a password or hostname).

    Returns:
        str: The escaped value, without surrounding quotes.

    Examples:
        >>> _quote_literal("pa'ss")
        "pa''ss"
        >>> f"PASSWORD '{_quote_literal(\"pa'ss\")}'"
        "PASSWORD 'pa''ss'"
    """
    # Doublage des apostrophes : seul caractère à neutraliser dans un littéral SQL
    return value.replace("'", "''")


# Classe de connexion à un catalogue DuckLake
class DuckLakeConnector:
    """
    Creates and configures a DuckDB connection attached to a DuckLake catalog.

    DuckLake stores all table metadata in a catalog backend while row data lives in
    immutable Parquet files under ``data_path``. The catalog backend can be a local
    file (DuckDB ``.ducklake`` or SQLite) or a PostgreSQL server, selected via
    ``catalog_type``. This class handles installing the required extensions,
    attaching the catalog, and routing the session to the correct schema.

    The PostgreSQL backend is the recommended choice for **concurrent read/write**
    deployments: a read-only consumer (e.g. a GraphQL API) can query the catalog
    while another process updates it, which the file-based DuckDB catalog cannot
    support (it is locked at the process level). Use :meth:`from_postgres` to build
    such a connector ergonomically; PostgreSQL credentials are supplied through a
    DuckDB secret rather than embedded in the connection string.

    A single catalog can host **several schemas** (one per result set, e.g.
    ``predictions`` and ``shapley``), each carrying its own ``fact_table``,
    ``metadata`` and ``dim_*`` tables. The ``schema`` argument selects which one this
    connection activates; ``connect()`` creates it if needed (writable connections
    only). Builders and managers also accept a ``schema`` argument, so a single shared
    connection can drive several schemas of the same catalog.

    After calling ``connect()``, the returned ``duckdb.DuckDBPyConnection`` can be
    passed directly to ``DuckLakeTablesBuilder``, ``DatabaseUpdater``, or any other
    class that accepts a ``connection`` parameter.

    Attributes:
        catalog_path (str): Locator of the catalog. A file path for the DuckDB
            backend, or a backend-prefixed connection string for the others
            (e.g. ``'postgres:dbname=ducklake'``, ``'sqlite:catalog.sqlite'``).
        data_path (str): Directory where Parquet data files are stored.
        catalog_type (CatalogType): Catalog backend (DuckDB, SQLite or PostgreSQL).
        meta_secret (Optional[str]): Name of the DuckDB secret holding the catalog
            backend credentials, referenced via ``META_SECRET`` (PostgreSQL only).
        read_only (bool): Whether the connection is read-only.
        catalog_alias (str): Alias used in the ``ATTACH`` statement (default ``'db'``).
        schema (str): DuckLake schema to activate with ``USE`` (default ``'main'``).
        logger (logging.Logger): Logger instance.

    Examples:
        >>> # Read-write connection (local DuckDB catalog file)
        >>> conn = DuckLakeConnector('catalog.ducklake', 'data/').connect()
        >>> # Read-only connection (GraphQL API, dashboard)
        >>> conn = DuckLakeConnector('catalog.ducklake', 'data/',
        read_only=True).connect()
        >>> # Time-travel to a specific snapshot (ML run audit)
        >>> conn = DuckLakeConnector('catalog.ducklake', 'data/',
        snapshot_version=3).connect()
        >>> # PostgreSQL catalog for concurrent read/write (production)
        >>> conn = DuckLakeConnector.from_postgres(
        ...     'data/', dbname='ducklake', host='localhost',
        ...     user='app', password='***',
        ... ).connect()
        >>> # Several schemas in a single catalog (one per result set)
        >>> conn = DuckLakeConnector('catalog.ducklake', 'data/',
        ...     schema='predictions').connect()
    """

    # Initialisation
    def __init__(
        self,
        catalog_path: str,
        data_path: str,
        read_only: bool = False,
        snapshot_version: int | None = None,
        snapshot_time: str | None = None,
        catalog_alias: str = "db",
        schema: str = "main",
        catalog_type: CatalogType | str = CatalogType.DUCKDB,
        meta_secret: str | None = None,
        s3_secret: str | None = None,
        s3_region: str | None = None,
        s3_endpoint: str | None = None,
        s3_access_key_id: str | None = None,
        s3_secret_access_key: str | None = None,
        s3_session_token: str | None = None,
        log_filename: str | os.PathLike[str] | None = os.path.join(
            FILE_PATH.parents[2], "logs/ducklake_connector.log"
        ),
    ) -> None:
        """
        Initialize the DuckLakeConnector.

        For the PostgreSQL backend, prefer the :meth:`from_postgres` class method,
        which assembles the connection string and the credential secret for you.

        Args:
            catalog_path (str): Locator of the catalog. For the DuckDB backend, a
                file path, normally **local** (e.g. ``'data/catalog.ducklake'``).
                An S3 path (e.g. ``'s3://bucket/prefix/catalog.ducklake'``) is
                accepted and will attach, but is only usable in
                ``read_only=True`` mode: DuckDB/httpfs cannot open a
                ``.ducklake``/``.duckdb`` catalog file for writing on S3 (S3
                objects cannot be modified in place), so any write attempt
                fails with a "database does not exist" error regardless of
                whether the file is actually present. For a writable
                catalog that needs to live on shared/remote storage, use the
                ``POSTGRES`` backend instead (see :meth:`from_postgres`).
                For other backends, a backend-prefixed connection string (e.g.
                ``'postgres:dbname=ducklake'`` or ``'sqlite:catalog.sqlite'``).
            data_path (str): Directory where Parquet data files are stored
                (e.g. ``'data/files/'`` or ``'s3://bucket/prefix/'``). Unlike
                ``catalog_path``, an S3 ``data_path`` works normally for both
                reads and writes. When either ``data_path`` or ``catalog_path``
                starts with ``s3://``, the ``httpfs`` extension is loaded and
                an S3 credential secret is created automatically (see
                ``s3_secret``, ``s3_region``, ``s3_endpoint``).
            read_only (bool): Open the catalog in read-only mode. Useful for
                dashboard/API layers that must not write. Defaults to False.
            snapshot_version (Optional[int]): Open a historical snapshot by
                version number. Implies ``READ_ONLY``. Defaults to None.
                **Important**: with the file-based DuckDB backend, DuckLake locks
                the catalog file at the process level, so a snapshot connection
                cannot coexist with another connection to the same catalog. When an
                active connection already exists, use :meth:`at_clause` instead to
                build an ``AT (VERSION => n)`` clause.
            snapshot_time (Optional[str]): Open a historical snapshot by
                timestamp (ISO-8601 string, e.g. ``'2025-01-01 00:00:00'``).
                Implies ``READ_ONLY``. Defaults to None.
                Same restriction as ``snapshot_version`` above; prefer
                :meth:`at_clause` when a connection is already open.
            catalog_alias (str): Alias for the attached DuckLake catalog in SQL
                (e.g. ``USE {alias}.{schema}``). Defaults to ``'db'``.
            schema (str): DuckLake schema to activate. Defaults to ``'main'``.
            catalog_type (CatalogType | str): Catalog backend to use. Defaults to
                ``CatalogType.DUCKDB`` (local file). Drives which DuckDB extensions
                are loaded by :meth:`connect`.
            meta_secret (Optional[str]): Name of a DuckDB secret carrying the
                catalog backend credentials, referenced via ``META_SECRET`` in the
                ``ATTACH`` statement (PostgreSQL only). Defaults to None.
            s3_secret (Optional[str]): Name of an S3 DuckDB secret to use for
                ``data_path``. When ``None`` and ``data_path`` starts with
                ``s3://``, a session secret named ``'s3_credential_chain'`` is
                created automatically. Its provider depends on whether explicit
                credentials are supplied (see ``s3_access_key_id`` below): ``PROVIDER
                credential_chain`` when they are not, or explicit ``KEY_ID`` /
                ``SECRET`` / ``SESSION_TOKEN`` fields when they are. Pass an
                explicit secret name here to reuse an already-existing secret
                instead, bypassing both auto-creation paths. Defaults to None.
            s3_region (Optional[str]): Region hint for the auto-created secret
                (e.g. ``'us-east-1'``). Optional in ``credential_chain`` mode,
                where the provider can usually resolve it from the environment.
                Defaults to None.
            s3_endpoint (Optional[str]): Custom S3 endpoint for the
                auto-created secret (e.g. an Onyxia/MinIO endpoint such as
                ``'minio.lab.sspcloud.fr'``). Required whenever ``data_path``
                lives on a non-AWS S3-compatible store, in both credential
                modes. Defaults to None.
            s3_access_key_id (Optional[str]): Explicit AWS access key ID. When
                provided together with ``s3_secret_access_key``, the auto-created
                secret uses these credentials directly (``KEY_ID`` / ``SECRET``
                / ``SESSION_TOKEN`` fields) instead of ``PROVIDER
                credential_chain``. Useful when the credential chain's
                resolution proves unreliable for the runtime (e.g. some
                Onyxia/httpfs combinations do not reliably pick up
                ``AWS_SESSION_TOKEN`` from the environment), or simply to make
                credential sourcing explicit and match a ``boto3`` client
                configured the same way. Defaults to None.
            s3_secret_access_key (Optional[str]): Explicit AWS secret access key,
                paired with ``s3_access_key_id``. Defaults to None.
            s3_session_token (Optional[str]): Explicit AWS session token, for
                temporary credentials (e.g. Onyxia's injected
                ``AWS_SESSION_TOKEN``). Only meaningful together with
                ``s3_access_key_id``/``s3_secret_access_key``; omitted from the secret when
                ``None``, which is correct for long-lived IAM credentials but
                will fail against temporary credentials that require it.
                Defaults to None.
            log_filename (Optional[os.PathLike]): Path to the log file.

        Examples:
            >>> connector = DuckLakeConnector('catalog.ducklake', 'data/')
            >>> conn = connector.connect()
            >>> # Data on S3, catalog kept local, credentials from the environment
            >>> connector = DuckLakeConnector(
            ...     'catalog.ducklake', 's3://bucket/prefix/',
            ... )
            >>> conn = connector.connect()
        """
        # Stockage des paramètres de connexion
        self.catalog_path = str(catalog_path)
        self.data_path = str(data_path)
        # Normalisation du backend de catalogue (accepte une chaîne ou un CatalogType).
        # CatalogType(...) valide la valeur et lève ValueError si elle est inconnue.
        self.catalog_type = CatalogType(catalog_type)
        self.meta_secret = meta_secret
        # snapshot_version et snapshot_time impliquent un accès en lecture seule :
        # DuckLake ouvre automatiquement le catalogue en READ_ONLY dans ce cas.
        # L'attribut self.read_only reflète cet état effectif pour cohérence.
        self.read_only = (
            read_only or snapshot_version is not None or snapshot_time is not None
        )
        self.snapshot_version = snapshot_version
        self.snapshot_time = snapshot_time
        self.catalog_alias = catalog_alias
        self.schema = schema

        # Détection d'un usage de S3 : sur data_path (fichiers Parquet) et/ou
        # sur catalog_path (catalogue DuckLake lui-même, backend DUCKDB
        # uniquement). Déclenche le chargement de httpfs et, sauf secret déjà
        # fourni, la création automatique d'un secret credential_chain.
        # Note : un catalogue DUCKDB sur S3 repose sur DuckLake pour gérer les
        # accès concurrents au niveau applicatif plutôt que sur un vrai
        # verrou de fichier système ; à réserver à un usage single-writer
        # (scripts de génération/test), et à éviter dès qu'un second
        # processus (ex. une API) doit accéder au même catalogue en
        # parallèle — préférer alors le backend POSTGRES (from_postgres).
        self._catalog_on_s3 = self.catalog_path.startswith("s3://")
        self._uses_s3 = self.data_path.startswith("s3://") or self._catalog_on_s3
        self.s3_region = s3_region
        self.s3_endpoint = s3_endpoint
        if s3_secret is not None:
            # Secret S3 externe déjà créé : aucune création, simple référence par nom
            self.s3_secret = s3_secret
            self._s3_secret_sql: str | None = None
        elif self._uses_s3 and s3_access_key_id is not None and s3_secret_access_key is not None:
            # Identifiants explicites fournis : secret déterministe (KEY_ID/SECRET/
            # SESSION_TOKEN), sans passer par la résolution credential_chain.
            # Reproduit le comportement d'un client boto3 configuré à la main,
            # utile si credential_chain se révèle peu fiable pour le runtime cible.
            self.s3_secret = "s3_explicit_credentials"
            self._s3_secret_sql = self._build_s3_explicit_credentials_sql(
                self.s3_secret,
                key_id=s3_access_key_id,
                secret_key=s3_secret_access_key,
                session_token=s3_session_token,
                region=s3_region,
                endpoint=s3_endpoint,
            )
        elif self._uses_s3:
            # Les credentials S3 seront chargés à partir de leurs variables d'environnement comme c'est le cas 
            self.s3_secret = "s3_credential_chain"
            self._s3_secret_sql = self._build_s3_credential_chain_sql(
                self.s3_secret, region=s3_region, endpoint=s3_endpoint
            )
        else:
            self.s3_secret = None
            self._s3_secret_sql = None

        # SQL de création du secret de session, renseigné par from_postgres lorsque
        # des identifiants bruts sont fournis ; exécuté avant ATTACH par connect/attach.
        # Conservé à part pour ne jamais être journalisé (il contient le mot de passe).
        self._secret_sql: str | None = None

        # Initialisation du logger
        if log_filename is None:
            log_filename = os.path.join(
                FILE_PATH.parents[2], "logs/ducklake_connector.log"
            )
        self.logger = _init_logger(filename=log_filename)

    # ---------------------------------------------------------------------------
    # Méthodes publiques de connexion
    # ---------------------------------------------------------------------------

    # Création d'une nouvelle connexion DuckDB attachée au catalogue DuckLake
    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Create and return a DuckDB connection attached to the DuckLake catalog.

        Steps performed:
        1. Open an in-memory DuckDB connection.
        2. Install and load the required extensions (``ducklake`` plus the catalog
           backend extension when applicable, e.g. ``postgres``).
        3. Create the credential secret when one is configured (PostgreSQL).
        4. Build and execute the ``ATTACH`` statement with appropriate options.
        5. Activate the target schema with ``USE``.

        Returns:
            duckdb.DuckDBPyConnection: Configured DuckDB connection ready for use.

        Raises:
            duckdb.Error: If an extension cannot be loaded or the catalog cannot
                be attached.

        Examples:
            >>> conn = DuckLakeConnector('catalog.ducklake', 'data/').connect()
            >>> conn = DuckLakeConnector(
            ...     'catalog.ducklake', 'data/',
            ...     read_only=True,
            ... ).connect()
        """
        # Ouverture d'une connexion DuckDB en mémoire
        # DuckLake utilise toujours :memory: comme connexion de base car le catalogue
        # est géré séparément (fichier .ducklake ou serveur Postgres).
        conn = duckdb.connect(":memory:")

        # Chargement des extensions requises puis création éventuelle du secret
        self._load_extensions(conn)
        self._apply_secret(conn)

        # Création des répertoires parents manquants dans le cas d'un stockage en local (catalogue et données)
        self._ensure_paths_exist()

        # Construction de la chaîne d'options ATTACH
        attach_sql = self._build_attach_sql()
        conn.execute(attach_sql)
        self.logger.info(
            f"DuckLake catalog attached : '{self.catalog_path}' "
            f"(type={self.catalog_type.value}, alias={self.catalog_alias}, "
            f"read_only={self.read_only})"
        )

        # Création éventuelle puis activation du schéma cible
        self._activate_schema(conn)

        return conn

    # ---------------------------------------------------------------------------
    # Méthodes de voyage dans le temps (time-travel) sur une connexion existante
    # ---------------------------------------------------------------------------

    # Génération d'une clause SQL AT pour requête time-travel sur connexion existante
    def at_clause(self) -> str:
        """
        Return the ``AT (...)`` SQL clause for time-travel queries on an existing
        connection.

        DuckLake prevents the same catalog file from being attached more than once
        per process (even under different aliases), so ``snapshot_version`` /
        ``snapshot_time`` cannot be used by opening a second connection.  Instead,
        append the returned clause to the ``FROM`` part of any query:

            ``SELECT * FROM table_name {connector.at_clause()}``

        Returns ``""`` when neither ``snapshot_version`` nor ``snapshot_time`` was
        supplied, so the method is safe to call unconditionally.

        Returns:
            str: SQL ``AT (VERSION => n)`` or ``AT (TIMESTAMP => 'ts')`` clause,
                or an empty string for the current snapshot.

        Raises:
            ValueError: If both ``snapshot_version`` and ``snapshot_time`` are set.

        Examples:
            >>> c = DuckLakeConnector('cat.ducklake', 'data/', snapshot_version=3)
            >>> c.at_clause()
            'AT (VERSION => 3)'
            >>> c2 = DuckLakeConnector('cat.ducklake', 'data/',
            snapshot_time='2025-01-01')
            >>> c2.at_clause()
            "AT (TIMESTAMP => '2025-01-01')"
            >>> DuckLakeConnector('cat.ducklake', 'data/').at_clause()
            ''
        """
        # Génération de la clause AT selon les paramètres de time-travel configurés
        if self.snapshot_version is not None:
            return f"AT (VERSION => {self.snapshot_version})"
        if self.snapshot_time is not None:
            return f"AT (TIMESTAMP => '{self.snapshot_time}')"
        return ""

    # Attachement d'un catalogue DuckLake à une connexion DuckDB existante
    def attach(self, conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
        """
        Attach the DuckLake catalog to an already-open DuckDB connection.

        Useful when sharing a connection across multiple catalogs. Required
        extensions are (idempotently) loaded and the credential secret is created
        when configured, so the call also works for the PostgreSQL backend. The
        ``USE`` statement is then executed to activate the configured schema.

        Args:
            conn (duckdb.DuckDBPyConnection): Existing DuckDB connection.

        Returns:
            duckdb.DuckDBPyConnection: The same connection, now with the catalog
            attached and the schema activated.

        Examples:
            >>> import duckdb
            >>> conn = duckdb.connect(':memory:')
            >>> connector = DuckLakeConnector('catalog.ducklake', 'data/')
            >>> connector.attach(conn)
        """
        # Préparation de la connexion existante : chargement des extensions
        # (idempotent) puis création éventuelle du secret d'identifiants
        self._load_extensions(conn)
        self._apply_secret(conn)

        # Création des répertoires parents manquants en local (catalogue et données)
        self._ensure_paths_exist()

        # Attachement du catalogue sur la connexion existante
        attach_sql = self._build_attach_sql()
        conn.execute(attach_sql)
        # Création éventuelle puis activation du schéma cible
        self._activate_schema(conn)
        self.logger.info(
            f"DuckLake catalog attached to the existing connection:"
            f"'{self.catalog_path}'"
        )
        return conn

    # ---------------------------------------------------------------------------
    # Constructeur de classe pour le backend PostgreSQL
    # ---------------------------------------------------------------------------

    # Création d'un connecteur attaché à un catalogue PostgreSQL
    @classmethod
    def from_postgres(
        cls,
        data_path: str,
        *,
        dbname: str,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        create_db_if_missing: bool = True,
        admin_dbname: str = "postgres",
        admin_user: str | None = None,
        admin_password: str | None = None,
        meta_secret: str | None = None,
        secret_name: str = "ducklake_pg_secret",
        read_only: bool = False,
        snapshot_version: int | None = None,
        snapshot_time: str | None = None,
        catalog_alias: str = "db",
        schema: str = "main",
        s3_secret: str | None = None,
        s3_region: str | None = None,
        s3_endpoint: str | None = None,
        s3_access_key_id: str | None = None,
        s3_secret_access_key: str | None = None,
        s3_session_token: str | None = None,
        log_filename: str | os.PathLike[str] | None = None,
    ) -> "DuckLakeConnector":
        """
        Build a connector backed by a PostgreSQL catalog.

        This is the recommended entry point for concurrent read/write deployments,
        where a read-only consumer (e.g. a GraphQL API) queries the catalog while
        another process updates it. The catalog metadata lives in PostgreSQL while
        row data stays in Parquet files under ``data_path`` (local or object store).

        Credentials are passed to DuckDB through a secret (``CREATE SECRET ...
        TYPE postgres``) referenced by ``META_SECRET`` in the ``ATTACH`` statement,
        instead of being embedded in the connection string. Two modes are supported:

        - **Inline credentials** (``host``/``user``/``password``/...): a *session*
          (non-persistent) secret is created on the connection at ``connect()`` time
          from the provided fields. Any field left as ``None`` is omitted, letting
          DuckDB's PostgreSQL driver fall back to the standard libpq environment
          variables (``PGHOST``, ``PGUSER``, ``PGPASSWORD``, ...).
        - **Externally managed secret** (``meta_secret`` set): an existing secret is
          referenced by name and no credentials flow through Python. Recommended for
          production, where the secret is created out of band.

        Args:
            data_path (str): Directory where Parquet data files are stored.
            dbname (str): Name of the PostgreSQL catalog database. Included in the
                ``ATTACH`` connection string (it is not a secret).
            host (Optional[str]): PostgreSQL host. Falls back to ``PGHOST`` if None.
            port (Optional[int]): PostgreSQL port. Falls back to ``PGPORT`` if None.
            user (Optional[str]): PostgreSQL user. Falls back to ``PGUSER`` if None.
            password (Optional[str]): PostgreSQL password. Falls back to
                ``PGPASSWORD`` if None.
            create_db_if_missing (bool): Create the ``dbname`` database on the
                PostgreSQL server if it does not exist yet. PostgreSQL has no
                ``CREATE DATABASE IF NOT EXISTS`` clause, so this connects to
                ``admin_dbname`` first, checks ``pg_database``, and issues
                ``CREATE DATABASE`` only if needed. Requires the connecting
                role to hold ``CREATEDB`` (or superuser) on the server.
                Ignored when ``meta_secret`` is set, since credentials are
                then managed out of band. Defaults to False.
            admin_dbname (str): Administrative database used to check for and
                create ``dbname`` when ``create_if_missing`` is True (every
                PostgreSQL server has one, since ``CREATE DATABASE`` cannot
                target the database it runs from). Defaults to ``'postgres'``.
            admin_user (Optional[str]): PostgreSQL role used for the
                check-then-create connection in ``_ensure_database_exists``,
                when it must differ from ``user`` (e.g. the day-to-day
                application role has no ``CREATEDB`` privilege by design, and
                a separate privileged role is used only for provisioning).
                Falls back to ``user`` when None. Ignored when
                ``create_db_if_missing`` is False or ``meta_secret`` is set.
            admin_password (Optional[str]): Password paired with
                ``admin_user``. Falls back to ``password`` when None.
            meta_secret (Optional[str]): Name of an existing DuckDB secret to
                reference. When provided, no secret is created and the
                ``host``/``port``/``user``/``password`` arguments are ignored.
            secret_name (str): Name of the session secret to create from inline
                credentials. Defaults to ``'ducklake_pg_secret'``.
            read_only (bool): Open the catalog in read-only mode (e.g. for the API).
            snapshot_version (Optional[int]): Time-travel by snapshot version.
            snapshot_time (Optional[str]): Time-travel by ISO-8601 timestamp.
            catalog_alias (str): Alias for the attached catalog. Defaults to ``'db'``.
            schema (str): DuckLake schema to activate. Defaults to ``'main'``.
            s3_secret (Optional[str]): Name of an existing S3 secret to reuse for
                ``data_path``, bypassing auto-creation entirely. Defaults to None.
            s3_region (Optional[str]): Region hint for the auto-created S3 secret.
                Defaults to None.
            s3_endpoint (Optional[str]): Custom S3 endpoint for the auto-created
                secret (e.g. an Onyxia/MinIO endpoint). Required whenever
                ``data_path`` lives on a non-AWS S3-compatible store, otherwise
                requests resolve against the default AWS endpoint and fail
                (typically as a 403, since the bucket does not exist there).
                Defaults to None.
            s3_access_key_id (Optional[str]): Explicit AWS access key ID. When given
                together with ``s3_secret_access_key``, the auto-created S3 secret uses
                these credentials directly instead of ``PROVIDER
                credential_chain`` — mirrors a ``boto3`` client configured the
                same way. Defaults to None.
            s3_secret_access_key (Optional[str]): Explicit AWS secret access key,
                paired with ``s3_access_key_id``. Defaults to None.
            s3_session_token (Optional[str]): Explicit AWS session token for
                temporary credentials (e.g. Onyxia's injected
                ``AWS_SESSION_TOKEN``). Only meaningful with
                ``s3_access_key_id``/``s3_secret_access_key``. Defaults to None.
            log_filename (Optional[os.PathLike]): Path to the log file.

        Returns:
            DuckLakeConnector: A connector configured for the PostgreSQL backend.

        Examples:
            >>> # Read-write update job (inline credentials)
            >>> rw = DuckLakeConnector.from_postgres(
            ...     'data/', dbname='ducklake', host='localhost',
            ...     user='app', password='***',
            ... )
            >>> # Read-only API, reusing a secret created out of band
            >>> ro = DuckLakeConnector.from_postgres(
            ...     'data/', dbname='ducklake',
            ...     meta_secret='pg_catalog_secret', read_only=True,
            ... )
            >>> # Onyxia/MinIO, explicit S3 credentials (mirrors a boto3 client)
            >>> onyxia = DuckLakeConnector.from_postgres(
            ...     's3://bucket/prefix/', dbname='ducklake', host='localhost',
            ...     user='app', password='***',
            ...     s3_endpoint=os.environ['AWS_S3_ENDPOINT'],
            ...     s3_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            ...     s3_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            ...     s3_session_token=os.environ['AWS_SESSION_TOKEN'],
            ... )
            >>> # Nouveau catalogue, base cible créée automatiquement si absente
            >>> new_cat = DuckLakeConnector.from_postgres(
            ...     'data/', dbname='vulnerabilities', host='localhost',
            ...     user='app', password='***', create_if_missing=True,
            ... )
            >>> # Rôle applicatif sans CREATEDB : identifiants admin dédiés
            >>> # à la seule étape de création de la base
            >>> new_cat = DuckLakeConnector.from_postgres(
            ...     'data/', dbname='vulnerabilities', host='localhost',
            ...     user='app', password='***',
            ...     admin_user='postgres', admin_password=os.environ['PG_ADMIN_PASSWORD'],
            ... )
        """
        # Création préalable de la base cible sur le serveur si demandée
        # (avant toute logique de secret/ATTACH, puisque indépendante de DuckDB).
        # Connexion de création avec les identifiants admin (rôle privilégié),
        # mais propriété de la base attribuée au rôle applicatif (`user`), pour
        # qu'il puisse ensuite créer des objets dans le schéma public.
        if create_db_if_missing and meta_secret is None:
            cls._ensure_database_exists(
                dbname=dbname,
                admin_dbname=admin_dbname,
                host=host,
                port=port,
                user=admin_user if admin_user is not None else user,
                password=admin_password if admin_password is not None else password,
                owner=user,
            )

        # Cible ATTACH du backend Postgres : le nom de la base figure dans la chaîne
        # de connexion (non sensible) ; les identifiants passent par le secret.
        catalog_path = f"postgres:dbname={dbname}"

        # Détermination du secret à référencer et du SQL de création éventuel
        secret_sql: str | None = None
        if meta_secret is not None:
            # Mode « secret externe » : référence d'un secret existant, sans création.
            effective_secret = meta_secret
        else:
            # Mode « identifiants en ligne » : création d'un secret de session.
            effective_secret = secret_name
            secret_sql = cls._build_postgres_secret_sql(
                secret_name=secret_name,
                dbname=dbname,
                host=host,
                port=port,
                user=user,
                password=password,
            )

        # Instanciation du connecteur en mode Postgres
        connector = cls(
            catalog_path=catalog_path,
            data_path=data_path,
            read_only=read_only,
            snapshot_version=snapshot_version,
            snapshot_time=snapshot_time,
            catalog_alias=catalog_alias,
            schema=schema,
            catalog_type=CatalogType.POSTGRES,
            meta_secret=effective_secret,
            s3_secret=s3_secret,
            s3_region=s3_region,
            s3_endpoint=s3_endpoint,
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            s3_session_token=s3_session_token,
            log_filename=log_filename,
        )
        # Mémorisation du SQL de création du secret (exécuté avant ATTACH)
        connector._secret_sql = secret_sql
        return connector

    # ---------------------------------------------------------------------------
    # Méthodes privées de préparation de connexion et de construction SQL
    # ---------------------------------------------------------------------------

    # Chargement des extensions DuckDB requises selon le backend de catalogue
    def _load_extensions(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Install and load the DuckDB extensions required by the catalog backend
        and by ``data_path``.

        ``ducklake`` is always loaded; the PostgreSQL or SQLite extension is added
        for the corresponding catalog backend, and ``httpfs`` is added whenever
        ``data_path`` points to S3. ``INSTALL``/``LOAD`` are idempotent, so the
        method is safe to call on an already-prepared connection.

        Args:
            conn (duckdb.DuckDBPyConnection): Connection to configure.
        """
        # Extension DuckLake : toujours nécessaire
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        self.logger.info("DuckLake extension loaded")

        # Extension spécifique au backend de catalogue
        if self.catalog_type == CatalogType.POSTGRES:
            conn.execute("INSTALL postgres; LOAD postgres;")
            self.logger.info("PostgreSQL extension loaded")
        elif self.catalog_type == CatalogType.SQLITE:
            conn.execute("INSTALL sqlite; LOAD sqlite;")
            self.logger.info("SQLite extension loaded")

        # Extension httpfs : nécessaire dès lors que data_path pointe vers S3
        if self._uses_s3:
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            self.logger.info("httpfs extension loaded")

    # Création du secret d'identifiants du catalogue lorsqu'il est configuré
    def _apply_secret(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Create the catalog and S3 credential secrets on the connection, when
        configured.

        Only secret *names* are logged: the SQL statements may carry a password
        or session token and must never be written to the logs.

        Args:
            conn (duckdb.DuckDBPyConnection): Connection on which to create the secret.
        """
        # Création du secret de catalogue uniquement si un SQL a été préparé
        # (identifiants Postgres en ligne)
        if self._secret_sql is not None:
            conn.execute(self._secret_sql)
            self.logger.info(
                f"Catalog credential secret created : '{self.meta_secret}'"
            )

        # Création du secret S3 auto-généré (credential_chain), le cas échéant.
        # Absent lorsqu'un s3_secret existant a été fourni explicitement : dans
        # ce cas, le secret est supposé déjà créé ailleurs.
        if self._s3_secret_sql is not None:
            conn.execute(self._s3_secret_sql)
            self.logger.info(f"S3 credential secret created : '{self.s3_secret}'")

    # Création préalable de la base de données cible si elle n'existe pas encore
    @classmethod
    def _ensure_database_exists(
        cls,
        dbname: str,
        admin_dbname: str,
        host: str | None,
        port: int | None,
        user: str | None,
        password: str | None,
        owner: str | None = None,
    ) -> None:
        """
        Create ``dbname`` on the PostgreSQL server if it does not already exist.

        PostgreSQL has no ``CREATE DATABASE IF NOT EXISTS`` clause, so existence
        is checked via ``pg_database`` before issuing ``CREATE DATABASE``. The
        check-then-create connection targets ``admin_dbname`` (a database is
        never created from within itself) and runs in ``autocommit`` mode, since
        ``CREATE DATABASE`` cannot execute inside a transaction block. Any field
        left as ``None`` is omitted from the connection parameters, letting
        psycopg2 fall back to the standard libpq environment variables
        (``PGHOST``, ``PGUSER``, ``PGPASSWORD``, ...), consistent with the
        fallback behaviour of the rest of :meth:`from_postgres`.

        Args:
            dbname (str): Name of the database to check for and create.
            admin_dbname (str): Administrative database used for the
                check-then-create connection.
            host (Optional[str]): PostgreSQL host. Falls back to ``PGHOST``.
            port (Optional[int]): PostgreSQL port. Falls back to ``PGPORT``.
            user (Optional[str]): PostgreSQL role used for the check-then-create
                connection itself (e.g. a privileged admin role). Falls back to
                ``PGUSER``.
            password (Optional[str]): PostgreSQL password. Falls back to
                ``PGPASSWORD``.
            owner (Optional[str]): Role to set as ``OWNER`` of the newly
                created database, when it differs from ``user`` (typically the
                day-to-day application role that will actually ATTACH to it
                afterwards). Required to make the database usable: on
                PostgreSQL 15+, only the database owner has ``CREATE`` on the
                ``public`` schema by default (via the ``pg_database_owner``
                pseudo-role), so a database created without the right owner
                leaves the application role unable to create any table in it.
                Ignored if the database already exists. Defaults to ``user``.

        Raises:
            psycopg2.errors.InsufficientPrivilege: If the connecting role lacks
                the ``CREATEDB`` privilege, or lacks ``CREATEROLE``/membership
                needed to set ``owner``.
            psycopg2.OperationalError: If ``admin_dbname`` cannot be reached.

        Examples:
            >>> # Rôle admin créateur, base possédée par le rôle applicatif
            >>> DuckLakeConnector._ensure_database_exists(
            ...     'vulnerabilities', 'postgres', 'localhost', None,
            ...     user='postgres_admin', password='***', owner='app',
            ... )
        """
        # Assemblage des paramètres de connexion : champs à None omis, pour
        # laisser psycopg2/libpq se replier sur les variables d'environnement
        conn_params = {
            "dbname": admin_dbname,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
        }
        conn_params = {k: v for k, v in conn_params.items() if v is not None}

        # Connexion à la base d'administration, autocommit obligatoire :
        # CREATE DATABASE ne peut pas s'exécuter dans une transaction
        admin_conn = psycopg2.connect(**conn_params)
        admin_conn.autocommit = True

        try:
            with admin_conn.cursor() as cur:
                # Vérification de l'existence préalable
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,)
                )
                if cur.fetchone() is None:
                    # Création de la base cible. Ni le nom, ni le propriétaire ne
                    # peuvent être passés en paramètre lié (CREATE DATABASE ne
                    # l'accepte pas) : ils sont donc interpolés, entre guillemets
                    # doubles, après échappement.
                    quoted_dbname = dbname.replace('"', '""')
                    # Propriétaire de la base : le rôle applicatif par défaut, pour
                    # qu'il hérite du droit CREATE sur le schéma public (PG15+,
                    # via le pseudo-rôle pg_database_owner)
                    effective_owner = owner if owner is not None else user
                    if effective_owner is not None:
                        quoted_owner = effective_owner.replace('"', '""')
                        cur.execute(
                            f'CREATE DATABASE "{quoted_dbname}" OWNER "{quoted_owner}";'
                        )
                    else:
                        cur.execute(f'CREATE DATABASE "{quoted_dbname}";')
        finally:
            admin_conn.close()

    # Construction du SQL de création d'un secret PostgreSQL
    @staticmethod
    def _build_postgres_secret_sql(
        secret_name: str,
        dbname: str,
        host: str | None,
        port: int | None,
        user: str | None,
        password: str | None,
    ) -> str:
        """
        Build a ``CREATE OR REPLACE SECRET ... (TYPE postgres, ...)`` statement.

        Fields left as ``None`` are omitted so that DuckDB's PostgreSQL driver can
        fall back to the libpq environment variables. The created secret is a
        session (non-persistent) secret. String values are escaped for SQL.

        Args:
            secret_name (str): Identifier of the secret to create.
            dbname (str): PostgreSQL database name (always included).
            host (Optional[str]): PostgreSQL host.
            port (Optional[int]): PostgreSQL port.
            user (Optional[str]): PostgreSQL user.
            password (Optional[str]): PostgreSQL password.

        Returns:
            str: The ``CREATE OR REPLACE SECRET`` SQL statement.
        """
        # Paramètres du secret : TYPE et DATABASE toujours présents
        params: list[str] = [
            "TYPE postgres",
            f"DATABASE '{_quote_literal(dbname)}'",
        ]
        # Champs optionnels : omis si non fournis (repli sur l'environnement libpq)
        if host is not None:
            params.append(f"HOST '{_quote_literal(host)}'")
        if port is not None:
            params.append(f"PORT {int(port)}")
        if user is not None:
            params.append(f"USER '{_quote_literal(user)}'")
        if password is not None:
            params.append(f"PASSWORD '{_quote_literal(password)}'")

        params_str = ", ".join(params)
        return f"CREATE OR REPLACE SECRET {secret_name} ({params_str})"

    # Construction du SQL de création d'un secret S3 basé sur credential_chain
    @staticmethod
    def _build_s3_credential_chain_sql(
        secret_name: str,
        region: str | None = None,
        endpoint: str | None = None,
    ) -> str:
        """
        Build a ``CREATE OR REPLACE SECRET ... (TYPE s3, PROVIDER
        credential_chain, ...)`` statement.

        ``PROVIDER credential_chain`` delegates credential resolution to the
        AWS SDK's default chain (environment variables, shared config/profile
        files, EC2/ECS/EKS instance role, etc.), so no key is ever embedded in
        the SQL or logged. This matches environments such as Onyxia, which
        inject ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` /
        ``AWS_SESSION_TOKEN`` (and often ``AWS_S3_ENDPOINT``) as environment
        variables.

        Args:
            secret_name (str): Identifier of the secret to create.
            region (Optional[str]): Region hint (e.g. ``'us-east-1'``). Omitted
                if ``None``, letting the credential chain resolve it.
            endpoint (Optional[str]): Custom S3 endpoint (e.g. a MinIO/Onyxia
                endpoint). Omitted if ``None``.

        Returns:
            str: The ``CREATE OR REPLACE SECRET`` SQL statement.
        """
        # Paramètres du secret : TYPE et PROVIDER toujours présents
        params: list[str] = ["TYPE s3", "PROVIDER credential_chain"]

        # Champs optionnels : omis si non fournis (repli sur la chaîne AWS standard)
        if region is not None:
            params.append(f"REGION '{_quote_literal(region)}'")
        if endpoint is not None:
            params.append(f"ENDPOINT '{_quote_literal(endpoint)}'")

        params_str = ", ".join(params)
        return f"CREATE OR REPLACE SECRET {secret_name} ({params_str})"

    # Construction du SQL de création d'un secret S3 à identifiants explicites
    @staticmethod
    def _build_s3_explicit_credentials_sql(
        secret_name: str,
        key_id: str,
        secret_key: str,
        session_token: str | None = None,
        region: str | None = None,
        endpoint: str | None = None,
    ) -> str:
        """
        Build a ``CREATE OR REPLACE SECRET ... (TYPE s3, KEY_ID ..., SECRET
        ...)`` statement from explicit credentials.

        Unlike :meth:`_build_s3_credential_chain_sql`, this bypasses the AWS
        SDK's credential resolution entirely: the key, secret, and optional
        session token are embedded directly in the secret, mirroring a
        ``boto3`` client constructed with the same fields. Useful when
        ``PROVIDER credential_chain`` proves unreliable for the target
        runtime, or when explicit sourcing is simply preferred.

        Args:
            secret_name (str): Identifier of the secret to create.
            key_id (str): AWS access key ID.
            secret_key (str): AWS secret access key.
            session_token (Optional[str]): AWS session token, required for
                temporary credentials (e.g. Onyxia's injected
                ``AWS_SESSION_TOKEN``). Omitted if ``None``.
            region (Optional[str]): Region hint. Omitted if ``None``.
            endpoint (Optional[str]): Custom S3 endpoint (e.g. a MinIO/Onyxia
                endpoint). Omitted if ``None``.

        Returns:
            str: The ``CREATE OR REPLACE SECRET`` SQL statement.
        """
        # Paramètres du secret : TYPE, KEY_ID et SECRET toujours présents
        params: list[str] = [
            "TYPE s3",
            f"KEY_ID '{_quote_literal(key_id)}'",
            f"SECRET '{_quote_literal(secret_key)}'",
        ]

        # Champs optionnels : jeton de session (identifiants temporaires),
        # région et endpoint custom (ex. MinIO/Onyxia)
        if session_token is not None:
            params.append(f"SESSION_TOKEN '{_quote_literal(session_token)}'")
        if region is not None:
            params.append(f"REGION '{_quote_literal(region)}'")
        if endpoint is not None:
            params.append(f"ENDPOINT '{_quote_literal(endpoint)}'")

        params_str = ", ".join(params)
        return f"CREATE OR REPLACE SECRET {secret_name} ({params_str})"

    # Création éventuelle puis activation du schéma cible
    def _activate_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Create the target schema if needed, then activate it with ``USE``.

        A single DuckLake catalog can host several schemas (one per result set).
        The ``main`` schema exists natively, but a named schema (e.g.
        ``'predictions'``) must be created on first use. ``CREATE SCHEMA IF NOT
        EXISTS`` is idempotent and only issued on writable connections; read-only
        connections (dashboards, APIs) skip creation and simply activate the schema.

        Args:
            conn (duckdb.DuckDBPyConnection): Connection with the catalog attached.
        """
        # Création du schéma cible si la connexion est en écriture.
        # Indispensable pour permettre de construire un nouveau schéma dans un
        # catalogue existant ; sans cela, le USE échouerait sur un schéma absent.
        if not self.read_only:
            conn.execute(
                f"CREATE SCHEMA IF NOT EXISTS {self.catalog_alias}.{self.schema}"
            )

        # Activation du schéma cible pour que les requêtes non qualifiées fonctionnent
        conn.execute(f"USE {self.catalog_alias}.{self.schema}")
        self.logger.info(f"Activated scheme : {self.catalog_alias}.{self.schema}")

    # Création des répertoires parents du catalogue et des données si absents
    def _ensure_paths_exist(self) -> None:
        """
        Create the parent directories of ``catalog_path`` and ``data_path`` if
        needed.

        DuckLake creates the ``.ducklake`` catalog file itself on first
        ``ATTACH``, but it does not create missing intermediate directories,
        so the parent directory must already exist. The same applies to
        ``data_path`` when it is a local directory, where Parquet files will
        be written. This is a no-op for the PostgreSQL catalog backend (since
        ``catalog_path`` is then a connection string rather than a filesystem
        path) and for an S3 ``catalog_path``/``data_path`` (DuckLake/S3
        creates key prefixes implicitly on write; there is no local directory
        to create).
        """
        # Répertoire parent du fichier de catalogue (backends fichier local uniquement)
        if self.catalog_type != CatalogType.POSTGRES and not self._catalog_on_s3:
            catalog_dir = Path(self.catalog_path).parent
            catalog_dir.mkdir(parents=True, exist_ok=True)

        # Répertoire de stockage des fichiers Parquet (chemins locaux uniquement)
        if not self._uses_s3:
            Path(self.data_path).mkdir(parents=True, exist_ok=True)

    # Construction de la clause SQL ATTACH avec les options appropriées
    def _build_attach_sql(self) -> str:
        """
        Build the ``ATTACH`` SQL statement from the connector configuration.

        The option string is built incrementally:
        - ``DATA_PATH`` is always included.
        - ``META_SECRET`` is appended when a credential secret is configured
          (PostgreSQL backend).
        - ``READ_ONLY`` is appended for read-only or time-travel connections.
        - ``SNAPSHOT_VERSION`` or ``SNAPSHOT_TIME`` is appended for time travel.

        Returns:
            str: The complete ``ATTACH`` SQL statement.
        """
        # Liste des options ATTACH à construire
        options = [f"DATA_PATH '{self.data_path}'"]

        # Référence au secret d'identifiants du catalogue (backend PostgreSQL)
        if self.meta_secret is not None:
            options.append(f"META_SECRET '{self.meta_secret}'")

        # Ajout de l'option SNAPSHOT_VERSION si une version est spécifiée
        # (implique READ_ONLY automatiquement selon la spec DuckLake)
        if self.snapshot_version is not None:
            options.append(f"SNAPSHOT_VERSION {self.snapshot_version}")
        # Ajout de l'option SNAPSHOT_TIME si un timestamp est spécifié
        elif self.snapshot_time is not None:
            options.append(f"SNAPSHOT_TIME '{self.snapshot_time}'")
        # Ajout de READ_ONLY si demandé explicitement (hors time travel)
        elif self.read_only:
            options.append("READ_ONLY")

        # Assemblage de la requête ATTACH finale
        options_str = ", ".join(options)
        return (
            f"ATTACH 'ducklake:{self.catalog_path}' AS {self.catalog_alias}"
            f" ({options_str})"
        )