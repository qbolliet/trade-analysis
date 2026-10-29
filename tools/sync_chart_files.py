"""Copie, dans le chart Helm ``trade-dashboard``, des sources du tableau de bord.

Le chart embarque l'export versionné du tableau de bord (``superset/vulnerabilites/``) et
l'outil d'empaquetage (``tools/superset_bundle.py``) dans ``helm/trade-dashboard/files/`` :
un chart ne lit que ses propres fichiers. Ce script remplace ces copies par les sources ;
``tests/test_chart_files.py`` échoue tant qu'elles diffèrent.

Examples:
    Mise à jour après modification du tableau de bord ou de l'outil::

        $ python tools/sync_chart_files.py
        helm/trade-dashboard/files/dashboard ← superset/vulnerabilites (15 fichiers)
        helm/trade-dashboard/files/superset_bundle.py ← tools/superset_bundle.py
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Dict, Tuple

# Racine du dépôt
ROOT = Path(__file__).resolve().parents[1]
# Copies du chart : destination → source (relatives à la racine du dépôt)
CHART_COPIES: Dict[str, str] = {
    "helm/trade-dashboard/files/dashboard": "superset/vulnerabilites",
    "helm/trade-dashboard/files/superset_bundle.py": "tools/superset_bundle.py",
}


# Fichiers d'une source (dossier ou fichier seul), par chemin relatif
def source_files(path: Path) -> Dict[str, bytes]:
    """Return the files of a folder (or a single file) with their content.

    Args:
        path: Folder or file.

    Returns:
        Relative POSIX path (``""`` for a single file) → content.

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as folder:
        ...     _ = (Path(folder) / "a.yaml").write_text("x: 1")
        ...     sorted(source_files(Path(folder)))
        ['a.yaml']
    """
    if path.is_file():
        return {"": path.read_bytes()}
    return {
        file.relative_to(path).as_posix(): file.read_bytes()
        for file in sorted(path.rglob("*"))
        if file.is_file() and "__pycache__" not in file.parts
    }


# Écarts entre les copies du chart et leurs sources
def stale_copies(root: Path = ROOT) -> Dict[str, Tuple[str, ...]]:
    """List the chart copies that differ from their source.

    Args:
        root: Repository root.

    Returns:
        Destination → differing relative paths, for the stale copies only.
    """
    stale = {}
    for destination, source in CHART_COPIES.items():
        expected = source_files(root / source)
        current = source_files(root / destination) if (root / destination).exists() else {}
        differing = tuple(sorted(name for name in set(expected) | set(current) if expected.get(name) != current.get(name)))
        if differing:
            stale[destination] = differing
    return stale


# Copie des sources dans le chart
def main() -> int:
    """Replace every chart copy by its source.

    Returns:
        0.
    """
    for destination, source in CHART_COPIES.items():
        target, origin = ROOT / destination, ROOT / source
        if target.is_dir():
            shutil.rmtree(target)
        if origin.is_dir():
            shutil.copytree(origin, target, ignore=shutil.ignore_patterns("__pycache__"))
            print(f"{destination} ← {source} ({len(source_files(origin))} fichiers)")
        else:
            shutil.copyfile(origin, target)
            print(f"{destination} ← {source}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
