"""HS correspondence tables: Parquet cache shared by BACI and the partner step.

The UNSD correspondence tables convert the codes of a recent HS vintage into an
older one. Two steps need them: the BACI reconstruction (every vintage target
harmonises the Comtrade declarations of later years) and the partner
vulnerabilities (the Comext flows of later years are converted into every older
vintage requested, so that a product can be read in a fixed nomenclature over a
long series). Both read the same cache, so both families speak the same codes.

The tables are invariant once UNSD publishes them: the absence of a cached
Parquet file is the only trigger for a download, unless a refresh is forced.
One normalised table is cached per pair (``{path}/{source}-{target}.parquet``),
next to a JSON registry recording, for every pair actually downloaded, the
download date, source URL, row count and content checksum.

The download client is created only when a pair is missing from the cache, so
a run whose tables are all cached never reaches the network.
"""
# Importation des modules
# Modules de base
from datetime import datetime, timezone
import hashlib
import logging
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# Modules de manipulation de données
import pandas as pd

# Stockage JSON (registre du cache)
from statflows.storage.json import Loader as JsonLoader, Saver as JsonSaver

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom du fichier de registre des téléchargements de tables de correspondance
CONCORDANCE_REGISTRY_FILE = "unsd_correspondance_tables.json"


# Fonction de calcul d'une somme de contrôle du contenu d'une table
def table_checksum(df_table: pd.DataFrame) -> str:
    """Compute a content hash of a table, to detect drift in a cached artefact.

    Args:
        df_table: Table to hash.

    Returns:
        Hex-encoded SHA-256 digest of the table's content (index excluded).

    Examples:
        >>> len(table_checksum(pd.DataFrame({"a": [1, 2]})))
        64
    """
    hashed = pd.util.hash_pandas_object(df_table, index=False)
    return hashlib.sha256(hashed.to_numpy().tobytes()).hexdigest()


# Fonction de calcul d'une somme de contrôle d'un ensemble de tables
def concordances_checksum(concordances: Mapping[Tuple[str, str], pd.DataFrame]) -> str:
    """Compute one checksum over a set of correspondence tables.

    A corrected table published by UNSD changes this value, which is what makes
    the rows converted with it stale.

    Args:
        concordances: Mapping ``(source, target) -> table``.

    Returns:
        Hex-encoded SHA-256 digest of the sorted pair checksums.

    Examples:
        >>> table = pd.DataFrame({"source_code": ["010121"], "target_code": ["010121"]})
        >>> concordances_checksum({("HS2022", "HS2017"): table}) == concordances_checksum(
        ...     {("HS2022", "HS2017"): table.copy()})
        True
    """
    payload = "|".join(
        f"{source}-{target}:{table_checksum(df_table)}"
        for (source, target), df_table in sorted(concordances.items())
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# Fonction de construction du chemin de cache d'une paire de millésimes
def cached_table_path(concordance_path: str, source: str, target: str) -> str:
    """Build the Parquet cache path of a source/target vintage pair.

    Args:
        concordance_path: Root directory of the concordance cache.
        source: Source classification (e.g. ``"HS2022"``).
        target: Target classification (e.g. ``"HS2017"``).

    Returns:
        The Parquet cache path.

    Examples:
        >>> cached_table_path("cache/", "HS2022", "HS2017")
        'cache/HS2022-HS2017.parquet'
    """
    return f"{concordance_path.rstrip('/')}/{source}-{target}.parquet"


# Fonction de construction du chemin du registre des téléchargements
def concordance_registry_path(concordance_path: str) -> str:
    """Build the JSON registry path of the concordance cache.

    Args:
        concordance_path: Root directory of the concordance cache.

    Returns:
        The registry path.

    Examples:
        >>> concordance_registry_path("cache")
        'cache/unsd_correspondance_tables.json'
    """
    return f"{concordance_path.rstrip('/')}/{CONCORDANCE_REGISTRY_FILE}"


# Fonction de lecture non bloquante d'une table mise en cache
def load_cached_table(loader: Any, path: str, bucket: Optional[str]) -> Optional[pd.DataFrame]:
    """Read a cached Parquet table, or ``None`` when it is absent.

    Args:
        loader: Table loader (local or S3, dispatched on ``bucket``).
        path: Cache path (local path or S3 key).
        bucket: S3 bucket name, or ``None`` for a local cache.

    Returns:
        The cached table, or ``None`` when no cache file exists yet.
    """
    # Erreur S3 « objet absent » importée paresseusement : botocore n'est requis
    # que pour un cache S3
    missing: Tuple[type, ...] = (FileNotFoundError,)
    try:
        from botocore.exceptions import ClientError

        missing = (FileNotFoundError, ClientError)
    except ImportError:  # pragma: no cover - botocore installé avec statflows
        pass
    try:
        return loader.load(path, bucket=bucket)
    except missing:
        return None


# Fonction de listage des paires descendantes entre millésimes
def downward_pairs(
    targets: Iterable[str], nomenclatures: Mapping[str, int]
) -> List[Tuple[str, str]]:
    """List the ``(recent, older)`` pairs converting into each target vintage.

    Every vintage more recent than a target is a possible source: the flows
    of each later year are declared in the vintage in force that year.

    Args:
        targets: Target vintages.
        nomenclatures: Mapping vintage label -> entry-into-force year.

    Returns:
        Sorted distinct pairs ``(source, target)`` with
        ``entry(source) > entry(target)``.

    Examples:
        >>> downward_pairs(["HS2012"], {"HS2012": 2012, "HS2017": 2017, "HS2022": 2022})
        [('HS2017', 'HS2012'), ('HS2022', 'HS2012')]
    """
    entries = {str(label): int(year) for label, year in nomenclatures.items()}
    return sorted(
        {
            (source, target)
            for target in targets
            for source, entry in entries.items()
            if entry > entries[target]
        }
    )


# Fonction de chargement des tables de correspondance nécessaires, avec cache
def prepare_concordances(
    pairs: Sequence[Tuple[str, str]],
    *,
    client_factory: Callable[[], Any],
    loader: Any,
    saver: Any,
    concordance_path: str,
    bucket: Optional[str],
    force_refresh: bool = False,
) -> Dict[Tuple[str, str], pd.DataFrame]:
    """Load cached HS correspondence tables, downloading only the missing ones.

    Args:
        pairs: Distinct ``(source, target)`` vintage pairs to resolve (UNSD
            identifiers, e.g. ``("HS2022", "HS2017")``).
        client_factory: Builds the UNSD client (``statflows.UNSDClient``);
            called only when a pair must be downloaded, and closed afterwards.
        loader: Table loader (local or S3) reading the cached Parquet tables.
        saver: Table saver (local or S3) writing the cached Parquet tables.
        concordance_path: Root directory of the concordance cache.
        bucket: S3 bucket name, or ``None`` for a local cache.
        force_refresh: When ``True``, re-download every pair even if cached.

    Returns:
        Mapping ``(source, target) -> normalised conversion table``, one entry
        per requested pair.

    Raises:
        Exception: Whatever the UNSD client raises for a pair it cannot
            download (the caller decides whether that is fatal).
    """
    concordances: Dict[Tuple[str, str], pd.DataFrame] = {}
    to_download: List[Tuple[str, str]] = []
    # Cache existant : aucune vérification de fraîcheur distante
    for source, target in pairs:
        table_path = cached_table_path(concordance_path, source, target)
        df_table = None if force_refresh else load_cached_table(loader, table_path, bucket)
        if df_table is None:
            to_download.append((source, target))
        else:
            concordances[(source, target)] = df_table
    if not to_download:
        return concordances

    # Téléchargement des paires manquantes, client créé à la demande
    registry_path = concordance_registry_path(concordance_path)
    json_saver = JsonSaver()
    registry = JsonLoader().load(registry_path, bucket=bucket, missing_ok=True) or {}
    client = client_factory()
    try:
        # Catalogue des tables déclarées (URL source de chaque paire)
        df_catalogue = client.list_available_tables().set_index(
            ["source_classification", "target_classification"]
        )
        for source, target in to_download:
            key = f"{source}-{target}"
            # Logging
            logger.info("Téléchargement de la table de correspondance %s", key)
            df_table = client.get_correspondence(source, target, kind="conversion")
            saver.save(
                cached_table_path(concordance_path, source, target),
                df_table,
                bucket=bucket,
                index=False,
            )
            # Mise à jour du registre uniquement pour les paires téléchargées
            registry[key] = {
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "source_url": str(df_catalogue.loc[(source, target), "url"]),
                "n_rows": int(len(df_table)),
                "checksum": table_checksum(df_table),
            }
            json_saver.save(registry_path, registry, bucket=bucket, indent=2, ensure_ascii=False)
            concordances[(source, target)] = df_table
    finally:
        client.close()
    # Ordre des paires demandées
    return {pair: concordances[pair] for pair in pairs}
