"""Methodological lock: code changes that alter results recompute them automatically.

The freshness fingerprints (:func:`kedro_pipeline.io.freshness.fingerprint`)
digest the **parameters** of each metric, method or step, not their **code**.
A formula fix must nevertheless recompute the results it altered. Rather than
counting on someone to invalidate the registries in production, the decision
is taken once, when the change is committed, and enforced by the CI:

- :data:`METHODOLOGY_SOURCES` declares, for every methodological name, the
  code its values depend on (whole modules, or ``module::Class`` objects);
- the versioned file ``kedro_pipeline/methodology.lock`` records, per name, a
  digest of that code — of its **normalised syntax tree**, so that comments,
  docstrings and formatting never count — and a ``revision`` number;
- ``tests/test_methodology_lock.py`` fails when a digest no longer matches,
  until the developer runs either

  - ``python -m kedro_pipeline.methodology lock --bump HHI``: the change alters
    the values; the revision is incremented, or
  - ``python -m kedro_pipeline.methodology lock --accept HHI``: the change does
    not alter them (refactoring, performance); only the digest is updated;

- the revision enters the fingerprint parameters of the name
  (:func:`revised`), only when positive so that introducing the lock
  recomputes nothing. After a ``--bump``, the next scheduled run finds a
  different fingerprint and recomputes the name everywhere (reason
  ``fingerprint``, cascaded downstream); nothing is to be done in production.

``scripts/invalidate_freshness.py`` remains the tool for a recomputation
restricted to a scope (some reporters, vintages, periods).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import argparse
import ast
from functools import lru_cache
import hashlib
from pathlib import Path
import sys
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

# Modules de lecture du verrou
import yaml

# Racine du dépôt (chemins des sources relatifs à elle) et emplacement du verrou
ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = Path(__file__).with_name("methodology.lock")

# Clé de la révision ajoutée aux paramètres d'empreinte
REVISION_KEY = "methodology_revision"

# Modules partagés par les familles de métriques
_VULNERABILITY_SHARED = (
    "macroforecast/trade/vulnerabilities/base.py",
    "macroforecast/trade/vulnerabilities/runner.py",
    "macroforecast/trade/methodology.py",
)
_NETWORK_SHARED = (*_VULNERABILITY_SHARED, "macroforecast/trade/vulnerabilities/graph.py")
_PARTNER_METRICS = {
    "HHI": "HerfindahlHirschmanIndex",
    "CDI2": "ConcentrationDependencyIndex2",
    "CDI3": "ConcentrationDependencyIndex3",
}
_NETWORK_METRICS = {
    "CENTRALITY_RISK": "WeightedOutdegreeCentralityRisk",
    "CLUSTERING_W": "WeightedClusteringCoefficient",
    "DIAMETER": "NetworkDiameter",
    "WORLD_HHI": "WorldExportConcentration",
    "SPOF": "SinglePointOfFailureRisk",
    "SPOF_DECILE": "SinglePointOfFailureDecile",
}
_AGGREGATION = "macroforecast/trade/aggregation"

# Code dont dépendent les valeurs de chaque nom méthodologique (empreintes du même nom)
METHODOLOGY_SOURCES: Mapping[str, Tuple[str, ...]] = {
    **{
        name: (f"macroforecast/trade/vulnerabilities/metrics.py::{cls}", *_VULNERABILITY_SHARED)
        for name, cls in _PARTNER_METRICS.items()
    },
    **{
        name: (f"macroforecast/trade/vulnerabilities/network_metrics.py::{cls}", *_NETWORK_SHARED)
        for name, cls in _NETWORK_METRICS.items()
    },
    # Conversion des lignes historiques vers un millésime SH antérieur (partenaires)
    "concordance": ("macroforecast/trade/processing/classification.py",),
    # Redressement BACI : toutes les étapes sont couplées (une empreinte par millésime)
    "baci": (
        "macroforecast/trade/processing/baci.py",
        "macroforecast/trade/processing/streaming.py",
        "macroforecast/trade/processing/aggregation.py",
        "macroforecast/trade/processing/classification.py",
    ),
    # Synthèse multicritère : toutes les méthodes et le consensus
    "synthesis": tuple(
        f"{_AGGREGATION}/{module}.py"
        for module in (
            "base", "estimators", "functions", "methods", "optimal_transport", "pareto",
            "preprocessing", "runner", "synthesis", "weights",
        )
    ),
    # Diagnostics de cohérence (empreinte globale)
    "coherence": (f"{_AGGREGATION}/coherence.py", f"{_AGGREGATION}/diagnostics.py"),
}


# ──────────────────────────────────────────────────────────────────────
# Empreinte du code
# ──────────────────────────────────────────────────────────────────────

# Fonction de suppression des docstrings d'un arbre syntaxique
def _strip_docstrings(tree: ast.AST) -> ast.AST:
    """Remove the docstrings of a syntax tree, in place.

    Args:
        tree: Parsed tree.

    Returns:
        The same tree, without the leading string expressions of its modules,
        classes and functions (an emptied body gets a ``pass``).
    """
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = node.body
        if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant) \
                and isinstance(body[0].value.value, str):
            node.body = body[1:] or [ast.Pass()]
    return tree


# Fonction de recherche d'une définition dans un module
def _find_definition(tree: ast.Module, qualname: str) -> ast.AST:
    """Return the class or function ``qualname`` (``Outer.Inner`` accepted).

    Raises:
        KeyError: If the definition is absent.
    """
    scope: Sequence[ast.stmt] = tree.body
    found: Optional[ast.AST] = None
    for part in qualname.split("."):
        found = next(
            (
                node for node in scope
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == part
            ),
            None,
        )
        if found is None:
            raise KeyError(qualname)
        scope = getattr(found, "body", [])
    assert found is not None
    return found


# Fonction d'empreinte d'une source déclarée
def source_hash(spec: str, root: Path = ROOT) -> str:
    """Digest of the normalised syntax tree of a module or one of its definitions.

    Comments, docstrings, blank lines and formatting do not change the digest;
    any change of code does.

    Args:
        spec: ``"path/module.py"`` or ``"path/module.py::Qualname"``.
        root: Root the paths are relative to.

    Returns:
        A 16-hex digest.

    Raises:
        FileNotFoundError: If the module is absent.
        KeyError: If the definition is absent from the module.

    Examples:
        >>> len(source_hash("kedro_pipeline/methodology.py::source_hash"))
        16
    """
    path, _, qualname = spec.partition("::")
    tree = ast.parse((root / path).read_text(encoding="utf-8"))
    node = _find_definition(tree, qualname) if qualname else tree
    dumped = ast.dump(_strip_docstrings(node), include_attributes=False)
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()[:16]


# Fonction d'empreinte du code d'un nom méthodologique
def code_hash(name: str, root: Path = ROOT, sources: Optional[Mapping[str, Sequence[str]]] = None) -> str:
    """Digest of all the code a methodological name depends on.

    Args:
        name: Methodological name (a key of :data:`METHODOLOGY_SOURCES`).
        root: Root of the source paths.
        sources: Declaration to use (:data:`METHODOLOGY_SOURCES` by default).

    Returns:
        A 16-hex digest of the digests of its sources, in declaration order.
    """
    declared = (sources or METHODOLOGY_SOURCES)[name]
    joined = "|".join(f"{spec}={source_hash(spec, root)}" for spec in declared)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


# ──────────────────────────────────────────────────────────────────────
# Verrou
# ──────────────────────────────────────────────────────────────────────

# Fonction de lecture du verrou (mise en cache : lu une fois par processus)
@lru_cache(maxsize=None)
def load_lock(path: Path = LOCK_PATH) -> Dict[str, Dict[str, Any]]:
    """Read the methodological lock.

    Args:
        path: Lock file.

    Returns:
        Mapping ``name -> {"revision", "code_hash", "sources"}``; empty when
        the file is absent.
    """
    if not Path(path).is_file():
        return {}
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return {str(name): dict(entry or {}) for name, entry in data.items()}


# Fonction de lecture de la révision d'un nom
def revision(name: str, lock: Optional[Mapping[str, Mapping[str, Any]]] = None) -> int:
    """Return the methodological revision of a name (0 when unknown).

    Args:
        name: Methodological name (metric, ``baci``, ``synthesis``…).
        lock: Lock to read; the packaged one by default.

    Returns:
        The revision.
    """
    entry = (load_lock() if lock is None else lock).get(name) or {}
    return int(entry.get("revision", 0) or 0)


# Fonction d'ajout de la révision aux paramètres d'une empreinte
def revised(
    name: str, params: Mapping[str, Any], lock: Optional[Mapping[str, Mapping[str, Any]]] = None
) -> Dict[str, Any]:
    """Return fingerprint parameters extended by the revision of a name.

    The revision is added only when positive: introducing the lock leaves
    every fingerprint unchanged, and only a ``--bump`` makes a name stale.

    Args:
        name: Methodological name.
        params: Fingerprint parameters.
        lock: Lock to read; the packaged one by default.

    Returns:
        A copy of ``params``, with ``methodology_revision`` when positive.

    Examples:
        >>> revised("HHI", {"a": 1}, lock={"HHI": {"revision": 2}})
        {'a': 1, 'methodology_revision': 2}
        >>> revised("HHI", {"a": 1}, lock={})
        {'a': 1}
    """
    extended = dict(params)
    value = revision(name, lock)
    if value > 0:
        extended[REVISION_KEY] = value
    return extended


# Fonction de détection des noms dont le code a changé
def stale_names(
    lock: Mapping[str, Mapping[str, Any]],
    root: Path = ROOT,
    sources: Optional[Mapping[str, Sequence[str]]] = None,
) -> Dict[str, str]:
    """Return the names whose code digest differs from the lock.

    Args:
        lock: Lock read.
        root: Root of the source paths.
        sources: Declaration to use (:data:`METHODOLOGY_SOURCES` by default).

    Returns:
        Mapping ``name -> reason`` (``changed``, ``new`` or ``removed``).
    """
    declared = sources or METHODOLOGY_SOURCES
    stale: Dict[str, str] = {}
    for name in declared:
        if name not in lock:
            stale[name] = "new"
        elif (lock[name] or {}).get("code_hash") != code_hash(name, root, declared):
            stale[name] = "changed"
    for name in lock:
        if name not in declared:
            stale[name] = "removed"
    return stale


# Fonction de mise à jour du verrou
def update_lock(
    lock: Mapping[str, Mapping[str, Any]],
    *,
    bump: Sequence[str] = (),
    accept: Sequence[str] = (),
    root: Path = ROOT,
    sources: Optional[Mapping[str, Sequence[str]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Record the decision taken on changed names.

    Args:
        lock: Current lock.
        bump: Names whose values the change alters (revision incremented).
        accept: Names whose values it does not alter (digest only).
        root: Root of the source paths.
        sources: Declaration to use (:data:`METHODOLOGY_SOURCES` by default).

    Returns:
        The new lock. New names are added with revision 0, removed names
        dropped.

    Raises:
        ValueError: If a name is unknown, or both bumped and accepted.
    """
    declared = sources or METHODOLOGY_SOURCES
    unknown = sorted((set(bump) | set(accept)) - set(declared))
    if unknown:
        raise ValueError(f"Unknown methodological names: {unknown}")
    both = sorted(set(bump) & set(accept))
    if both:
        raise ValueError(f"Names both bumped and accepted: {both}")
    updated: Dict[str, Dict[str, Any]] = {}
    for name, specs in declared.items():
        entry = dict(lock.get(name) or {})
        current = code_hash(name, root, declared)
        if name not in lock:
            entry = {"revision": 0, "code_hash": current}
        elif name in bump:
            entry["revision"] = int(entry.get("revision", 0) or 0) + 1
            entry["code_hash"] = current
        elif name in accept:
            entry["code_hash"] = current
        entry["sources"] = list(specs)
        updated[name] = {key: entry[key] for key in ("revision", "code_hash", "sources")}
    return updated


# En-tête du fichier de verrou
_HEADER = """\
# Verrou méthodologique : NE PAS ÉDITER À LA MAIN.
# Généré par `python -m kedro_pipeline.methodology lock`. Pour chaque nom méthodologique :
#   - code_hash : empreinte de l'arbre syntaxique de son code (sans commentaires ni
#     docstrings), contrôlée par tests/test_methodology_lock.py ;
#   - revision : incrémentée par `lock --bump <nom>` quand une modification du code change
#     les valeurs ; elle entre dans les empreintes de fraîcheur, ce qui recalcule le nom
#     partout à la passe planifiée suivante. `lock --accept <nom>` (modification sans effet
#     sur les valeurs) ne met à jour que code_hash.
"""


# Fonction d'écriture du verrou
def write_lock(lock: Mapping[str, Mapping[str, Any]], path: Path = LOCK_PATH) -> None:
    """Write the lock (sorted, with its explanatory header).

    Args:
        lock: Lock to write.
        path: Lock file.
    """
    body = yaml.safe_dump(
        {name: dict(lock[name]) for name in sorted(lock)},
        sort_keys=False, allow_unicode=True, default_flow_style=False,
    )
    Path(path).write_text(_HEADER + body, encoding="utf-8", newline="\n")
    load_lock.cache_clear()


# ──────────────────────────────────────────────────────────────────────
# Ligne de commande
# ──────────────────────────────────────────────────────────────────────

# Message de la CI quand le verrou n'est pas à jour
def stale_message(stale: Mapping[str, str]) -> str:
    """Explain what to do when the lock is out of date.

    Args:
        stale: Result of :func:`stale_names`.

    Returns:
        The message.
    """
    changed = sorted(name for name, reason in stale.items() if reason == "changed")
    lines = [f"Verrou méthodologique périmé : {dict(sorted(stale.items()))}."]
    if changed:
        names = " ".join(changed)
        lines += [
            "Le code de ces noms a changé. Décider, pour chacun :",
            f"  - les valeurs changent  : python -m kedro_pipeline.methodology lock --bump {names}",
            f"  - aucune valeur ne change : python -m kedro_pipeline.methodology lock --accept {names}",
        ]
    if any(reason in {"new", "removed"} for reason in stale.values()):
        lines.append("Noms ajoutés ou retirés : python -m kedro_pipeline.methodology lock")
    return "\n".join(lines)


# Point d'entrée de la ligne de commande
def main(argv: Optional[Sequence[str]] = None) -> int:
    """Check or update the methodological lock.

    ``check`` exits 1 when the lock is out of date; ``lock`` records the
    decision (``--bump`` / ``--accept``) and refuses to leave a changed name
    undecided.

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` by default).

    Returns:
        The exit code.
    """
    parser = argparse.ArgumentParser(prog="python -m kedro_pipeline.methodology")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="Vérifier que le verrou est à jour")
    lock_parser = commands.add_parser("lock", help="Enregistrer la décision sur les noms modifiés")
    lock_parser.add_argument("--bump", nargs="*", default=[], help="Noms dont les valeurs changent")
    lock_parser.add_argument("--accept", nargs="*", default=[], help="Noms dont les valeurs ne changent pas")
    args = parser.parse_args(argv)

    lock = load_lock()
    stale = stale_names(lock)
    if args.command == "check":
        if stale:
            print(stale_message(stale), file=sys.stderr)
            return 1
        print("Verrou méthodologique à jour.")
        return 0

    # Toute modification doit être tranchée explicitement
    undecided = sorted(
        name for name, reason in stale.items()
        if reason == "changed" and name not in set(args.bump) | set(args.accept)
    )
    if undecided:
        print(stale_message({name: "changed" for name in undecided}), file=sys.stderr)
        return 1
    updated = update_lock(lock, bump=args.bump, accept=args.accept)
    write_lock(updated)
    for name in args.bump:
        print(f"{name} : révision {updated[name]['revision']} (recalcul à la prochaine passe)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
