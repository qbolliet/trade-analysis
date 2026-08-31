"""Capture et comparaison de l'API publique des packages de données.

Ce script parcourt récursivement le package ``macroforecast.storage`` (chargeurs
tabulaires conservés localement ; le reste est fourni par ``statflows``). Pour chaque module
exposant un ``__all__``, il sérialise dans un JSON, pour chaque symbole réexporté :
le nom qualifié, le type (``class`` / ``function`` / ``constant`` / ``property``),
la signature ``inspect.signature`` pour les callables, ainsi que les méthodes
publiques des classes avec leur signature.

Le snapshot obtenu sert de garde-fou : un mode ``--compare`` affiche le diff avec
un snapshot antérieur et sort en code 1 dès que l'API publique a bougé.

Examples:
    Génération d'un snapshot::

        $ python tools/api_snapshot.py tools/api_snapshot_before.json

    Comparaison avec un snapshot de référence (code retour 1 si divergence)::

        $ python tools/api_snapshot.py tools/api_snapshot_after.json \\
            --compare tools/api_snapshot_before.json
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import pkgutil
import sys
from typing import Any, Dict, List, Tuple

# Packages dont l'API publique est suivie
TRACKED_PACKAGES: Tuple[str, ...] = (
    "macroforecast.storage",
)

# Type alias pour la structure de snapshot : {module: {symbole: description}}
Snapshot = Dict[str, Dict[str, Any]]


def _safe_signature(obj: Any) -> str | None:
    """Return the ``inspect.signature`` string of an object, or None.

    Args:
        obj: Callable to introspect.

    Returns:
        The stringified signature, or None when it cannot be resolved.
    """
    try:
        return str(inspect.signature(obj))
    except (TypeError, ValueError):
        return None


def _describe_class_methods(cls: type) -> Dict[str, Dict[str, Any]]:
    """Describe the public methods and properties of a class.

    Args:
        cls: Class to introspect.

    Returns:
        Mapping ``{member_name: {"kind": ..., "signature": ...}}`` sorted by name,
        restricted to public members (no leading underscore) that are routines or
        properties.
    """
    members: Dict[str, Dict[str, Any]] = {}
    for name, member in inspect.getmembers(cls):
        if name.startswith("_"):
            continue
        if isinstance(member, property):
            members[name] = {"kind": "property", "signature": None}
        elif inspect.isfunction(member) or inspect.ismethod(member) or inspect.isbuiltin(member):
            members[name] = {"kind": "method", "signature": _safe_signature(member)}
        elif isinstance(member, (staticmethod, classmethod)):
            members[name] = {
                "kind": "method",
                "signature": _safe_signature(member.__func__),
            }
    return dict(sorted(members.items()))


def _describe_symbol(qualified_name: str, obj: Any) -> Dict[str, Any]:
    """Describe a single public symbol exported by a module.

    Args:
        qualified_name: Fully qualified name (``module.symbol``).
        obj: The object bound to that symbol.

    Returns:
        Description dict with keys ``qualified_name``, ``kind`` and, depending on
        the kind, ``signature`` (callables), ``methods`` (classes) or ``value``
        (constants).
    """
    description: Dict[str, Any] = {"qualified_name": qualified_name}

    if inspect.isclass(obj):
        description["kind"] = "class"
        description["signature"] = _safe_signature(obj)
        description["methods"] = _describe_class_methods(obj)
    elif inspect.isfunction(obj) or inspect.isbuiltin(obj) or inspect.ismethod(obj):
        description["kind"] = "function"
        description["signature"] = _safe_signature(obj)
    elif callable(obj) and not isinstance(obj, type):
        # Instances callables (ex. objets partiels, singletons configurés)
        description["kind"] = "function"
        description["signature"] = _safe_signature(obj)
    else:
        description["kind"] = "constant"
        description["value"] = _safe_repr(obj)

    return description


def _safe_repr(obj: Any, max_len: int = 200) -> str:
    """Return a truncated ``repr`` of an object, never raising.

    Args:
        obj: Object to represent.
        max_len: Maximum length of the returned string.

    Returns:
        A ``repr`` string, truncated with an ellipsis when longer than ``max_len``.
    """
    try:
        text = repr(obj)
    except Exception:  # noqa: BLE001 - un repr défaillant ne doit pas casser le snapshot
        text = f"<unrepresentable {type(obj).__name__}>"
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return text


def _iter_modules(package_name: str) -> List[str]:
    """Return the names of ``package_name`` and all its importable submodules.

    Args:
        package_name: Dotted path of the root package.

    Returns:
        Sorted list of module names.

    Raises:
        ImportError: If the root package itself cannot be imported.
    """
    root = importlib.import_module(package_name)
    names = {package_name}
    for module_info in pkgutil.walk_packages(root.__path__, prefix=f"{package_name}."):
        names.add(module_info.name)
    return sorted(names)


def build_snapshot(packages: Tuple[str, ...] = TRACKED_PACKAGES) -> Snapshot:
    """Build the public-API snapshot for the given packages.

    Args:
        packages: Dotted paths of the root packages to introspect.

    Returns:
        Mapping ``{module_name: {symbol_name: description}}`` sorted by module and
        by symbol. Only modules exposing a non-empty ``__all__`` are included.
    """
    snapshot: Snapshot = {}

    for package_name in packages:
        for module_name in _iter_modules(package_name):
            try:
                module = importlib.import_module(module_name)
            except Exception as exc:  # noqa: BLE001 - un module cassé est signalé, pas fatal
                print(f"[warn] import impossible : {module_name} ({exc})", file=sys.stderr)
                continue

            exported = getattr(module, "__all__", None)
            if not exported:
                continue

            module_entry: Dict[str, Any] = {}
            for symbol_name in sorted(exported):
                if not hasattr(module, symbol_name):
                    print(
                        f"[warn] {module_name}.__all__ référence un symbole absent : {symbol_name}",
                        file=sys.stderr,
                    )
                    continue
                obj = getattr(module, symbol_name)
                module_entry[symbol_name] = _describe_symbol(
                    f"{module_name}.{symbol_name}", obj
                )

            if module_entry:
                snapshot[module_name] = module_entry

    return dict(sorted(snapshot.items()))


def _diff_symbol(module: str, name: str, before: Dict[str, Any], after: Dict[str, Any]) -> List[str]:
    """Return the human-readable diff lines for a single symbol.

    Args:
        module: Name of the module owning the symbol.
        name: Symbol name.
        before: Previous description.
        after: Current description.

    Returns:
        List of diff lines (empty when the symbol is unchanged).
    """
    lines: List[str] = []
    prefix = f"  {module}.{name}"

    if before.get("kind") != after.get("kind"):
        lines.append(f"{prefix}: type {before.get('kind')} -> {after.get('kind')}")

    if before.get("signature") != after.get("signature"):
        lines.append(
            f"{prefix}: signature {before.get('signature')} -> {after.get('signature')}"
        )

    if before.get("value") != after.get("value"):
        lines.append(f"{prefix}: valeur {before.get('value')} -> {after.get('value')}")

    before_methods = before.get("methods", {}) or {}
    after_methods = after.get("methods", {}) or {}
    for method_name in sorted(set(before_methods) - set(after_methods)):
        lines.append(f"{prefix}.{method_name}: méthode supprimée")
    for method_name in sorted(set(after_methods) - set(before_methods)):
        lines.append(f"{prefix}.{method_name}: méthode ajoutée")
    for method_name in sorted(set(before_methods) & set(after_methods)):
        b_method = before_methods[method_name]
        a_method = after_methods[method_name]
        if b_method != a_method:
            lines.append(
                f"{prefix}.{method_name}: {b_method.get('signature')} -> {a_method.get('signature')}"
            )

    return lines


def diff_snapshots(before: Snapshot, after: Snapshot) -> List[str]:
    """Compute the diff between two snapshots.

    Args:
        before: Reference snapshot.
        after: Current snapshot.

    Returns:
        List of diff lines. An empty list means the public API is unchanged.
    """
    lines: List[str] = []

    for module in sorted(set(before) - set(after)):
        lines.append(f"- module supprimé : {module}")
    for module in sorted(set(after) - set(before)):
        lines.append(f"+ module ajouté : {module}")

    for module in sorted(set(before) & set(after)):
        before_symbols = before[module]
        after_symbols = after[module]
        for name in sorted(set(before_symbols) - set(after_symbols)):
            lines.append(f"- symbole supprimé : {module}.{name}")
        for name in sorted(set(after_symbols) - set(before_symbols)):
            lines.append(f"+ symbole ajouté : {module}.{name}")
        for name in sorted(set(before_symbols) & set(after_symbols)):
            lines.extend(
                _diff_symbol(module, name, before_symbols[name], after_symbols[name])
            )

    return lines


def _parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Optional argument list (defaults to ``sys.argv``).

    Returns:
        Parsed namespace with ``output`` and ``compare`` attributes.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("output", help="Chemin du fichier JSON de sortie du snapshot.")
    parser.add_argument(
        "--compare",
        metavar="FICHIER",
        help=(
            "Snapshot antérieur à comparer au snapshot courant. Affiche le diff et "
            "sort en code 1 si l'API publique a changé."
        ),
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    """Entry point.

    Args:
        argv: Optional argument list (defaults to ``sys.argv``).

    Returns:
        Process exit code: 0 if the API is unchanged (or no comparison was
        requested), 1 if ``--compare`` detected a change.
    """
    args = _parse_args(argv)

    snapshot = build_snapshot()
    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
    print(f"Snapshot écrit : {args.output}")

    if not args.compare:
        return 0

    with open(args.compare, "r", encoding="utf-8") as handle:
        reference = json.load(handle)

    changes = diff_snapshots(reference, snapshot)
    if not changes:
        print(f"API publique inchangée par rapport à {args.compare}.")
        return 0

    print(f"\nChangements d'API par rapport à {args.compare} :")
    for line in changes:
        print(line)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
