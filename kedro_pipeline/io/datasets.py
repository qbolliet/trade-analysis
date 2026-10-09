"""Kedro datasets of the trade pipeline: lazy handles, never materialised tables.

The catalog never loads a whole DuckLake table in memory: the steps upsert
incremental batches (one context, one vintage), which a "one ``save`` = one
complete DataFrame" dataset cannot express, and the Comtrade / BACI volumes
forbid a full read. A dataset therefore hands out a **handle**:

* :class:`DuckLakeTableDataset` — a lazy
  :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` (location and credentials,
  no connection); ``save`` takes the handle back, after checking it designates
  the same table, and refuses a DataFrame, so that every write goes through the
  explicit upsert of the node;
* :class:`FreshnessRegistryDataset` — a
  :class:`~kedro_pipeline.io.freshness.FreshnessRegistry` split into JSON
  fragments; ``save`` writes the modified fragments only;
* :class:`ServingCatalogDataset` — a
  :class:`~kedro_pipeline.io.serving.ServingCatalog` (serving catalog written,
  source catalogs attached read-only).

Nodes receive handles and return them: Kedro infers the DAG from these
inputs / outputs, kedro-viz draws the lineage, and argo-kedro derives the task
dependencies (a handle reloads identically in another pod). No dataset opens a
connection when it is instantiated, so the catalog can be built without any
secret (documentation build, configuration checks).
"""
# Importation des modules
# Modules de base
import string
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

# Kedro
from kedro.io import AbstractDataset, DatasetError

# Modules internes
from kedro_pipeline.io.clients import ClientFactory
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    DuckLakeTable,
    connector_factory_for,
)
from kedro_pipeline.io.freshness import FreshnessRegistry, Unit
from kedro_pipeline.io.serving import ServingCatalog


# Fonction de séparation des identifiants composés d'un catalogue DuckLake
def _ducklake_credentials(
    credentials: Optional[Mapping[str, Any]],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Split the ``ducklake`` credentials entry into its PostgreSQL and S3 parts.

    Args:
        credentials: ``{"postgres": {...}, "s3": {...}}`` (``credentials.yml``
            entry ``ducklake``), or ``None``.

    Returns:
        Tuple ``(pg, s3)``, empty mappings when absent (the error is then raised
        at the first connection, never at instantiation).
    """
    credentials = credentials or {}
    return dict(credentials.get("postgres") or {}), dict(credentials.get("s3") or {})


# Fonction de construction d'un emplacement depuis la configuration du catalogue
def _location(location: Mapping[str, Any]) -> DuckLakeLocation:
    """Build a :class:`DuckLakeLocation` from a catalog ``location`` mapping.

    Raises:
        DatasetError: If a mandatory field is missing or unknown.
    """
    try:
        return DuckLakeLocation(**dict(location))
    except TypeError as exc:
        raise DatasetError(
            f"Invalid DuckLake location {dict(location)}: expected dbname, catalog_alias, "
            f"schema, bucket, data_path (and optionally table) ({exc})"
        ) from exc


# Poignée de table DuckLake
class DuckLakeTableDataset(AbstractDataset[DuckLakeTable, DuckLakeTable]):
    """Dataset whose data is a lazy handle on one DuckLake fact table.

    Args:
        location: Catalog identity and data path of the table (``dbname``,
            ``catalog_alias``, ``schema`` — sanitised as written by the
            pipeline —, ``bucket``, ``data_path``, optional ``table``).
        credentials: ``ducklake`` entry of ``credentials.yml``
            (``{"postgres": {...}, "s3": {...}}``, or ``{"file_root": ...}`` for
            local file catalogs).
        metadata: Free metadata, ignored by Kedro (kedro-viz displays it).

    Examples:
        >>> dataset = DuckLakeTableDataset(location={
        ...     "dbname": "comtrade", "catalog_alias": "comtrade", "schema": "C_A_HS",
        ...     "bucket": "b", "data_path": "trade/datasets/comtrade"})
        >>> dataset.load().qualified_name
        '"comtrade"."C_A_HS"."fact_table"'
    """

    def __init__(
        self,
        *,
        location: Mapping[str, Any],
        credentials: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        # Aucune connexion ici : le catalogue s'instancie sans secrets
        self._location = _location(location)
        self._pg, self._s3 = _ducklake_credentials(credentials)
        self._factory = connector_factory_for(credentials)
        self.metadata = metadata

    # Propriété : emplacement de la table
    @property
    def location(self) -> DuckLakeLocation:
        """Location of the table designated by the dataset."""
        return self._location

    # Chargement : poignée paresseuse
    def load(self) -> DuckLakeTable:
        """Return a lazy handle on the table (no connection opened).

        Returns:
            The handle; its first real operation checks the credentials.
        """
        return DuckLakeTable.lazy(
            self._location, self._pg, self._s3, connector_factory=self._factory
        )

    # Sauvegarde : vérification d'identité de la poignée
    def save(self, data: DuckLakeTable) -> None:
        """Accept back the handle the node wrote through.

        Nothing is written: the node already upserted its batches through the
        handle, which it returns only to materialise the lineage.

        Args:
            data: The handle returned by the node.

        Raises:
            DatasetError: If ``data`` is a DataFrame (or any non-handle), or a
                handle on another table.
        """
        if not isinstance(data, DuckLakeTable):
            raise DatasetError(
                f"DuckLakeTableDataset '{self._location.schema}' refuses a "
                f"{type(data).__name__}: write through the handle (upsert in the node) "
                "and return the handle itself"
            )
        if data.location != self._location:
            raise DatasetError(
                f"Handle on {data.location} returned for the dataset of {self._location}"
            )

    # Description du dataset (sans identifiants)
    def _describe(self) -> Dict[str, Any]:
        """Describe the dataset (location only, never the credentials)."""
        return {
            "dbname": self._location.dbname,
            "catalog_alias": self._location.catalog_alias,
            "schema": self._location.schema,
            "table": self._location.table,
            "data_url": self._location.data_url,
        }


# Fonction de construction des options S3 du chargeur JSON
def s3_storage_options(s3: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Translate the ``s3`` credentials into ``statflows`` JSON loader options.

    Args:
        s3: ``s3`` entry of ``credentials.yml``.

    Returns:
        ``aws_access_key_id``, ``aws_secret_access_key``, ``aws_session_token``
        (when set) and ``endpoint_url`` (``https://`` added to a bare host);
        ``None`` without access key, the loader then relying on the
        environment.

    Examples:
        >>> s3_storage_options({"endpoint": "minio.example", "access_key_id": "k",
        ...                     "secret_access_key": "s", "session_token": None})
        {'aws_access_key_id': 'k', 'aws_secret_access_key': 's', 'endpoint_url': 'https://minio.example'}
        >>> s3_storage_options({"access_key_id": ""}) is None
        True
    """
    s3 = s3 or {}
    if not s3.get("access_key_id"):
        return None
    options: Dict[str, Any] = {
        "aws_access_key_id": s3["access_key_id"],
        "aws_secret_access_key": s3.get("secret_access_key"),
    }
    if s3.get("session_token"):
        options["aws_session_token"] = s3["session_token"]
    endpoint = s3.get("endpoint")
    if endpoint:
        options["endpoint_url"] = endpoint if "://" in str(endpoint) else f"https://{endpoint}"
    return options


# Fonction d'extraction des champs d'un gabarit de chemin
def template_fields(path_template: str) -> List[str]:
    """Return the field names of a fragment path template, in order.

    Examples:
        >>> template_fields("trade/state/{classification}/{reporter}.json")
        ['classification', 'reporter']
    """
    return [name for _, name, _, _ in string.Formatter().parse(path_template) if name]


# Libellé de fragment : valeurs des champs du gabarit
class _ShardLabel:
    """Picklable ``unit -> fragment label`` function (values of the template fields)."""

    def __init__(self, fields: List[str], separator: str) -> None:
        self.fields = list(fields)
        self.separator = separator

    def __call__(self, unit: Unit) -> str:
        if not self.fields:
            return unit.key
        return self.separator.join(str(unit.get(name)) for name in self.fields)


# Registre de fraîcheur
class FreshnessRegistryDataset(AbstractDataset[FreshnessRegistry, FreshnessRegistry]):
    """Dataset whose data is the freshness registry of one step.

    The registry is split into JSON fragments (``path_template`` formatted with
    the dimensions of each unit) and loaded lazily; two pods working on
    disjoint fragments never lose each other's entries.

    Args:
        path_template: Fragment path template, relative to ``bucket`` (or a
            local path without bucket), e.g.
            ``"trade/state/vulnerabilities/network/{vintage}.json"``.
        step: Step name recorded in each fragment (``"baci"``, ``"partners"``…).
        bucket: S3 bucket; ``None`` for local storage.
        credentials: ``s3`` entry of ``credentials.yml``; empty values rely on
            the environment of the S3 client.
        shard_separator: Separator of the field values in the fragment label.
        metadata: Free metadata, ignored by Kedro.

    Examples:
        >>> dataset = FreshnessRegistryDataset(
        ...     path_template="state/{vintage}.json", step="network")
        >>> registry = dataset.load()
        >>> registry.path_of(Unit.of(vintage="HS2017")), registry.shard_of(Unit.of(vintage="HS2017"))
        ('state/HS2017.json', 'HS2017')
    """

    def __init__(
        self,
        *,
        path_template: str,
        step: str,
        bucket: Optional[str] = None,
        credentials: Optional[Mapping[str, Any]] = None,
        shard_separator: str = "/",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._path_template = str(path_template)
        self._step = step
        self._bucket = bucket
        self._storage_options = s3_storage_options(credentials)
        self._shard_separator = shard_separator
        self.metadata = metadata

    # Chargement : registre paresseux (aucun fragment lu)
    def load(self) -> FreshnessRegistry:
        """Return a fresh registry; fragments are read on first access.

        Returns:
            The registry.
        """
        return FreshnessRegistry(
            self._path_template,
            self._bucket,
            self._step,
            shard_of=_ShardLabel(template_fields(self._path_template), self._shard_separator),
            storage_options=self._storage_options,
        )

    # Sauvegarde : fragments modifiés seulement
    def save(self, data: FreshnessRegistry) -> None:
        """Write the fragments of the registry modified since its last save.

        Args:
            data: The registry returned by the node.

        Raises:
            DatasetError: If ``data`` is not a registry of this dataset.
        """
        if not isinstance(data, FreshnessRegistry):
            raise DatasetError(
                f"FreshnessRegistryDataset '{self._step}' expects a FreshnessRegistry, "
                f"got {type(data).__name__}"
            )
        if (data.path_template, data.bucket) != (self._path_template, self._bucket):
            raise DatasetError(
                f"Registry '{data.path_template}' returned for the dataset of "
                f"'{self._path_template}'"
            )
        data.save()

    # Description du dataset (sans identifiants)
    def _describe(self) -> Dict[str, Any]:
        """Describe the dataset (template and bucket, never the credentials)."""
        return {"path_template": self._path_template, "bucket": self._bucket, "step": self._step}


# Catalogue de restitution
class ServingCatalogDataset(AbstractDataset[ServingCatalog, ServingCatalog]):
    """Dataset whose data is the handle on the DuckLake ``serving`` catalog.

    The handle attaches the serving catalog (written) and the source catalogs
    (read-only) only when a session is opened; the whole publication is then
    written in a single transaction.

    Args:
        location: Serving catalog (``dbname``, ``catalog_alias``, ``schema``
            — ``dashboard``, ``demo_dashboard`` in demo —, ``bucket``,
            ``data_path``).
        sources: Source catalogs attached read-only, by alias
            (``{alias: location mapping}``).
        credentials: ``ducklake`` entry of ``credentials.yml``.
        metadata: Free metadata, ignored by Kedro.

    Examples:
        >>> loc = {"dbname": "serving", "catalog_alias": "serving", "schema": "dashboard",
        ...        "bucket": "b", "data_path": "trade/datasets/serving/"}
        >>> ServingCatalogDataset(location=loc, sources={}).load().schema
        'serving."dashboard"'
    """

    def __init__(
        self,
        *,
        location: Mapping[str, Any],
        sources: Optional[Mapping[str, Mapping[str, Any]]] = None,
        credentials: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._location = _location(location)
        self._sources = {alias: _location(source) for alias, source in (sources or {}).items()}
        self._pg, self._s3 = _ducklake_credentials(credentials)
        self._factory = connector_factory_for(credentials)
        self.metadata = metadata

    # Chargement : poignée sans connexion
    def load(self) -> ServingCatalog:
        """Return the handle on the serving catalog (no connection opened)."""
        return ServingCatalog(
            self._location,
            self._pg,
            self._s3,
            dict(self._sources),
            connector_factory=self._factory,
        )

    # Sauvegarde : vérification d'identité
    def save(self, data: ServingCatalog) -> None:
        """Accept back the handle the node published through.

        Args:
            data: The handle returned by the node.

        Raises:
            DatasetError: If ``data`` is not a serving handle, or targets another
                catalog or schema.
        """
        if not isinstance(data, ServingCatalog):
            raise DatasetError(
                f"ServingCatalogDataset expects a ServingCatalog, got {type(data).__name__}"
            )
        if data.location != self._location:
            raise DatasetError(
                f"Serving handle on {data.location} returned for the dataset of {self._location}"
            )

    # Description du dataset (sans identifiants)
    def _describe(self) -> Dict[str, Any]:
        """Describe the dataset (serving location and source aliases)."""
        return {
            "dbname": self._location.dbname,
            "schema": self._location.schema,
            "data_url": self._location.data_url,
            "sources": sorted(self._sources),
        }


# Fabrique des poignées écrites par les nœuds
class DuckLakeCatalogs:
    """Factory of the handles a node writes through (its output tables, the serving catalog).

    A Kedro node cannot receive a dataset it also outputs: the handle of a table
    it writes is therefore built by the node itself, from the location its
    parameters designate, with the credentials and connector factory held
    here (the very ones of the table datasets). The output dataset then checks
    that the handle returned designates its own table.

    Args:
        pg: PostgreSQL credentials.
        s3: S3 credentials.
        connector_factory: Factory of unconnected connectors.

    Examples:
        >>> catalogs = DuckLakeCatalogsDataset().load()
        >>> catalogs.table(DuckLakeLocation(dbname="d", catalog_alias="d", schema="s",
        ...     bucket="b", data_path="p")).qualified_name
        '"d"."s"."fact_table"'
    """

    def __init__(
        self,
        pg: Mapping[str, Any],
        s3: Mapping[str, Any],
        connector_factory: Any,
    ) -> None:
        self.pg = dict(pg)
        self.s3 = dict(s3)
        self.connector_factory = connector_factory

    # Poignée paresseuse d'une table
    def table(self, location: DuckLakeLocation) -> DuckLakeTable:
        """Return a lazy handle on the table at ``location`` (no connection opened).

        Args:
            location: Catalog identity, schema and data path of the table.

        Returns:
            The handle.
        """
        return DuckLakeTable.lazy(location, self.pg, self.s3, connector_factory=self.connector_factory)

    # Poignée du catalogue de restitution
    def serving(
        self, location: DuckLakeLocation, sources: Mapping[str, DuckLakeLocation]
    ) -> ServingCatalog:
        """Return the handle on the serving catalog and its read-only sources.

        Args:
            location: Serving catalog and schema.
            sources: Source catalogs attached read-only, by alias.

        Returns:
            The handle (no connection opened).
        """
        return ServingCatalog(
            location, self.pg, self.s3, dict(sources), connector_factory=self.connector_factory
        )


# Dataset de la fabrique des poignées écrites
class DuckLakeCatalogsDataset(AbstractDataset[DuckLakeCatalogs, None]):
    """Read-only dataset handing out the :class:`DuckLakeCatalogs` factory.

    Args:
        credentials: ``ducklake`` entry of ``credentials.yml``.
        metadata: Free metadata, ignored by Kedro.

    Examples:
        >>> DuckLakeCatalogsDataset(credentials={"file_root": "/tmp/x"}).load().connector_factory.root
        '/tmp/x'
    """

    def __init__(
        self,
        *,
        credentials: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._pg, self._s3 = _ducklake_credentials(credentials)
        self._factory = connector_factory_for(credentials)
        self.metadata = metadata

    def load(self) -> DuckLakeCatalogs:
        """Return the factory (no connection opened)."""
        return DuckLakeCatalogs(self._pg, self._s3, self._factory)

    def save(self, data: Any) -> None:
        """Refuse any write: the factory is an input only.

        Raises:
            DatasetError: Always.
        """
        raise DatasetError("DuckLakeCatalogsDataset is read-only")

    def _describe(self) -> Dict[str, Any]:
        """Describe the dataset (connector kind only, never the credentials)."""
        return {"connector": type(self._factory).__name__}


# Dataset d'une fabrique de client d'API
class ClientFactoryDataset(AbstractDataset[ClientFactory, None]):
    """Read-only dataset handing out the factory of one API client.

    Args:
        factory: Dotted path of the client class (``"statflows.EurostatClient"``,
            a network-free class in the ``test`` environment).
        options: Public keyword arguments of the client.
        credentials: Secret keyword arguments (``comtrade_api`` entry:
            ``subscription_key``); empty values are dropped, the client then
            applying its own default.
        metadata: Free metadata, ignored by Kedro.

    Examples:
        >>> dataset = ClientFactoryDataset(factory="collections.OrderedDict",
        ...     options={"a": 1}, credentials={"key": ""})
        >>> dataset.load()()
        OrderedDict({'a': 1})
    """

    def __init__(
        self,
        *,
        factory: str,
        options: Optional[Mapping[str, Any]] = None,
        credentials: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._path = str(factory)
        # Valeurs secrètes vides (variable d'environnement absente) écartées
        secrets = {key: value for key, value in (credentials or {}).items() if value not in ("", None)}
        self._options = {**dict(options or {}), **secrets}
        self._public = sorted((options or {}).keys())
        self.metadata = metadata

    def load(self) -> ClientFactory:
        """Return the client factory (no client built here)."""
        return ClientFactory(self._path, self._options)

    def save(self, data: Any) -> None:
        """Refuse any write: the factory is an input only.

        Raises:
            DatasetError: Always.
        """
        raise DatasetError("ClientFactoryDataset is read-only")

    def _describe(self) -> Dict[str, Any]:
        """Describe the dataset (class path and public option names, never secrets)."""
        return {"factory": self._path, "options": self._public}


# Poignée du cache des tables de correspondance SH
@dataclass(frozen=True)
class ConcordanceCache:
    """Location of the shared cache of HS correspondence tables (Parquet).

    Written by the BACI preparation (and completed by the partner metrics when
    a historical vintage needs a missing pair); exchanged between nodes to
    order them, the steps reading the very same location in their parameters.

    Attributes:
        path: Root directory of the cache (``baci.CLASSIFICATIONS.CONCORDANCE_PATH``).
        bucket: S3 bucket, ``None`` for a local cache (``baci.BUCKET``).
    """

    path: str
    bucket: Optional[str] = None


# Dataset du cache des tables de correspondance
class ConcordanceCacheDataset(AbstractDataset[ConcordanceCache, ConcordanceCache]):
    """Dataset whose data is the location of the correspondence-table cache.

    ``load`` never reads the cache (it may not exist yet: the partner metrics
    of the daily run do not wait for BACI); ``save`` only checks the identity
    of the location returned by the node.

    Args:
        cache_path: Root directory of the cache, relative to ``bucket`` (not
            named ``path``: Kedro would turn a relative ``path`` argument into a
            local absolute path).
        bucket: S3 bucket, ``None`` for a local cache.
        metadata: Free metadata, ignored by Kedro.

    Examples:
        >>> ConcordanceCacheDataset(cache_path="trade/unsd", bucket="b").load()
        ConcordanceCache(path='trade/unsd', bucket='b')
    """

    def __init__(
        self,
        *,
        cache_path: str,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._cache = ConcordanceCache(str(cache_path), bucket)
        self.metadata = metadata

    def load(self) -> ConcordanceCache:
        """Return the cache location (nothing read)."""
        return self._cache

    def save(self, data: ConcordanceCache) -> None:
        """Accept back the cache location the node prepared.

        Args:
            data: Location returned by the node.

        Raises:
            DatasetError: If ``data`` designates another cache.
        """
        if data != self._cache:
            raise DatasetError(f"Concordance cache {data} returned for the dataset of {self._cache}")

    def _describe(self) -> Dict[str, Any]:
        """Describe the dataset."""
        return {"path": self._cache.path, "bucket": self._cache.bucket}
