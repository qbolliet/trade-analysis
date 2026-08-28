"""Shared JSON registry helpers (local filesystem or S3).

The pipeline keeps small JSON registries alongside its data — last-download
dates per SDMX query, last-computation dates per reporter x product pair. Both
the download orchestrator (:mod:`macroforecast.datasets.core.download`) and the
vulnerability script used to carry their own copy of the read/write pair; this
module holds the single implementation.

Two levels are offered: :func:`read_json` / :func:`write_json` handle the bare
storage round-trip, while :func:`read_registry` / :func:`merge_registry` add the
root-key-and-merge convention every registry of the pipeline follows — read the
entries under a named root, update only the keys a run touched, leave the rest
alone.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
# Module de gestion des erreurs S3
from botocore.exceptions import ClientError
# Modules du package
from .loader import Loader
from .saver import Saver


# Fonction de lecture d'un registre JSON (local ou S3)
def read_json(
    path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Read a JSON registry from local storage or S3.

    Routing is driven by ``bucket``: when set, ``path`` is used as the S3 object
    key (POSIX form); otherwise it is a local filesystem path. A missing
    file/object means "no registry yet" and returns ``None`` rather than
    raising, so a first run never has to be special-cased.

    Args:
        path: Registry path (local path or S3 key).
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        The deserialised JSON mapping, or ``None`` when the registry does not
        exist yet.
    """
    # Cas S3 : l'absence d'objet se détecte via l'exception du client
    if bucket is not None:
        try:
            return loader.load(path.as_posix(), bucket=bucket)
        except ClientError:
            # Objet inexistant (NoSuchKey/404) → registre vide
            return None
    # Cas local : court-circuit si le fichier n'existe pas encore
    if not path.exists():
        return None
    return loader.load(str(path))


# Fonction d'écriture d'un registre JSON (local ou S3), atomique en local
def write_json(
    path: Path,
    obj: Any,
    saver: Saver,
    bucket: Optional[str],
) -> None:
    """Write a JSON registry to local storage or S3, atomically when local.

    On S3 the object PUT is atomic, so the payload is written directly. On the
    local filesystem the write goes through a temporary file in the destination
    directory, renamed over the target, so a crash never leaves a half-written
    registry.

    Args:
        path: Registry path (local path or S3 key).
        obj: JSON-serialisable object to persist.
        saver: ``Saver`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
    """
    # Cas S3 : écriture directe (PUT d'objet atomique)
    if bucket is not None:
        saver.save(path.as_posix(), obj, bucket=bucket, indent=2, ensure_ascii=False)
        return

    # Cas local : écriture atomique via fichier temporaire + remplacement
    # Création du dossier parent si nécessaire
    path.parent.mkdir(parents=True, exist_ok=True)
    # L'extension .json est nécessaire pour que Saver reconnaisse le format
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".json")
    # Fermeture immédiate du descripteur : Saver ouvre le fichier lui-même
    os.close(fd)
    try:
        saver.save(tmp_name, obj, indent=2, ensure_ascii=False)
        os.replace(tmp_name, str(path))
    except Exception:
        # Nettoyage du fichier temporaire en cas d'échec
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


# Fonction de lecture des entrées d'un registre (racine nommée)
def read_registry(
    path: Path,
    loader: Loader,
    bucket: Optional[str],
    *,
    root: str,
) -> Dict[str, Dict[str, Any]]:
    """Read the entries of a registry, keyed under a named root.

    Thin wrapper over :func:`read_json` adding the root-key convention the
    pipeline's registries share (``"DOWNLOADS"``, ``"BACI"``,
    ``"NETWORK_VULNERABILITIES"``…): an absent file and an absent root both mean
    "no entry yet" and return an empty mapping, so a first run is never a special
    case.

    Args:
        path: Registry path (local path or S3 key).
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
        root: Root key holding the entries.

    Returns:
        Mapping of entry key to entry, empty when the registry does not exist
        yet.
    """
    # Absence de fichier ou de racine : registre vide
    return (read_json(path, loader, bucket) or {}).get(root, {})


# Fonction de fusion d'entrées dans un registre, puis persistance
def merge_registry(
    path: Path,
    entries: Mapping[str, Dict[str, Any]],
    loader: Loader,
    saver: Saver,
    bucket: Optional[str],
    *,
    root: str,
) -> None:
    """Merge entries into a registry and persist it.

    Merges rather than overwrites: only the supplied keys move, every other
    entry is preserved untouched. That is what lets a partial run — one HS
    vintage, one dataflow — update its own entry without erasing the others.

    Args:
        path: Registry path (local path or S3 key).
        entries: Entries to merge, keyed by their registry key.
        loader: ``Loader`` instance, to read the existing registry first.
        saver: ``Saver`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
        root: Root key holding the entries.
    """
    # Fusion avec le registre existant : seules les entrées fournies bougent
    registry = read_registry(path, loader, bucket, root=root)
    registry.update(entries)
    # Écriture du registre mis à jour
    write_json(path, {root: registry}, saver, bucket)
