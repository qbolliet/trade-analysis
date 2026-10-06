"""Generic freshness registries: fragments, methodological fingerprints, forcing.

Every downstream step of the pipeline (BACI reconstruction, partner and network
vulnerabilities, synthesis, coherence) decides what to (re)compute from a
**freshness registry** indexed by *unit of work* (a vintage, a classification x
reporter x product triple, a synthesis context…). Each registry entry records:

- ``last_computed``: UTC instant captured *before* the computation started, so
  that an upstream update landing during the run is never missed;
- ``upstream_watermark``: the upstream instant the computation took into
  account;
- ``fingerprints``: one digest per metric or method, built from its name and
  the parameters that shape its values;
- ``reason``: why the unit was last computed (``first``, ``forced``,
  ``new_data`` or ``fingerprint``), which the downstream steps read to cascade
  a methodological change;
- free counters (``n_rows``…) and step-specific fields (``fit_id``,
  ``years_scope``, ``years_written`` for BACI).

A unit is recomputed when at least one of the following holds, the first
matching reason winning (``first`` > ``forced`` > ``new_data`` >
``fingerprint``):

1. it was never computed (``first``);
2. it falls within the scope of a one-off forcing (``forced``), given by the
   run-time parameters or the ``FORCE_*`` environment variables;
3. its upstream is more recent than its ``upstream_watermark`` (``new_data``);
4. the fingerprint of a requested metric or method differs from the recorded
   one, or is missing (``fingerprint``): the metric was added, or its
   parameters changed, or its recorded fingerprint was **invalidated**.

Invalidation is the way to recompute after fixing a formula: the code carries
no version number, so a fix is not visible in the fingerprints.
:meth:`FreshnessRegistry.invalidate` (command line:
``scripts/invalidate_freshness.py``) deletes the recorded fingerprint of the
fixed metrics (or of the step) on the chosen units; the next scheduled run
finds it missing and recomputes them, with the reason ``fingerprint``, which
the downstream steps treat as a complete change of their input. Unlike a
forcing, an invalidation is persisted: a failed run leaves the units invalid
and the next run retries them.

Registries are split into **fragments**, one JSON file per partition of the
units (reporter, vintage…), so that a run never rewrites a huge file and two
pods working on disjoint partitions never overwrite each other's entries.
Fragments are read lazily, and only the fragments actually modified are
written back.

Version-1 registries (one global JSON file per step, without fingerprints) are
read through a :class:`LegacySource`. A version-1 entry is read with
``fingerprints={}``: a missing fingerprint means "never computed with this
methodology", so every metric of the unit is recomputed — unless the run sets
``adopt_legacy_fingerprints``, which copies the current fingerprints onto the
version-1 entries without recomputation and rewrites them as fragments (the
deployment migration).

Forcing values are comma- **or** semicolon-separated strings. ``kedro run
--params`` splits its argument on commas *before* separating keys from values,
so a value holding a comma (``PERIODS=2020,2021``) fails there; use the
semicolon on that command line (``PERIODS=2020;2021``). Both separators are
accepted everywhere else (YAML, Argo parameters, environment variables).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Set,
    Tuple,
)

# Stockage JSON local ou S3 (écriture atomique) et normalisation des noms de fragments
from statflows.core.registry import sanitize_shard
from statflows.storage.json import Loader, Saver

# Initialisation du logger
logger = logging.getLogger(__name__)

# Version du format des fragments écrits par ce module
SCHEMA_VERSION = 2

# Raisons de calcul d'une unité, de la plus prioritaire à la moins prioritaire
Reason = Literal["first", "forced", "new_data", "fingerprint"]
REASONS: Tuple[str, ...] = ("first", "forced", "new_data", "fingerprint")

# Valeur de FORCE_STEPS forçant toutes les étapes
FORCE_ALL = "all"

# Séparateurs acceptés dans les valeurs de forçage (« ; » requis sous kedro run --params)
_FORCE_SEPARATORS = re.compile(r"[,;]")

# Variables d'environnement de forçage, par champ de ForceSpec
FORCE_ENV_VARIABLES: Mapping[str, str] = {
    "steps": "FORCE_STEPS",
    "metrics": "FORCE_METRICS",
    "methods": "FORCE_METHODS",
    "reporters": "FORCE_REPORTERS",
    "products": "FORCE_PRODUCTS",
    "periods": "FORCE_PERIODS",
    "vintages": "FORCE_VINTAGES",
}

# Variable d'environnement d'adoption des empreintes courantes par les entrées v1
ADOPT_ENV_VARIABLE = "ADOPT_LEGACY_FINGERPRINTS"

# Dimensions d'unité confrontées à chaque filtre de périmètre de forçage
_SCOPE_DIMENSIONS: Mapping[str, Tuple[str, ...]] = {
    "reporters": ("reporter",),
    "products": ("product",),
    "periods": ("period", "year", "TIME_PERIOD"),
    "vintages": ("vintage", "classification"),
}

# Clés réservées d'une entrée de fragment (le reste est rangé dans ``extra``)
_ENTRY_KEYS = frozenset(
    {"unit", "last_computed", "upstream_watermark", "fingerprints", "reason", "legacy"}
)


# ──────────────────────────────────────────────────────────────────────
# Instants et empreintes
# ──────────────────────────────────────────────────────────────────────

# Fonction de lecture d'un instant ISO-8601 en datetime UTC
def parse_instant(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 instant (or a datetime) into a UTC-aware datetime.

    Args:
        value: ISO-8601 string (``Z`` suffix accepted), ``datetime`` or ``None``.

    Returns:
        UTC-aware datetime, or ``None`` when the value is empty or unparsable.
        Naive values are interpreted as UTC.

    Examples:
        >>> parse_instant("2026-09-16T03:12:44Z").isoformat()
        '2026-09-16T03:12:44+00:00'
        >>> parse_instant(None) is None, parse_instant("not a date") is None
        (True, True)
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            logger.warning("Instant de registre illisible ignoré : %r", value)
            return None
    # Normalisation en UTC (un instant naïf est réputé UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# Fonction de sérialisation d'un instant
def format_instant(value: Optional[datetime]) -> Optional[str]:
    """Serialise an instant as ISO-8601 UTC text (``None`` kept).

    Args:
        value: Instant to serialise.

    Returns:
        ISO-8601 text, or ``None``.

    Examples:
        >>> format_instant(datetime(2026, 1, 2, tzinfo=timezone.utc))
        '2026-01-02T00:00:00+00:00'
    """
    parsed = parse_instant(value)
    return parsed.isoformat() if parsed is not None else None


# Fonction de capture de l'instant courant
def utc_now() -> datetime:
    """Return the current UTC instant (seconds precision).

    Returns:
        UTC-aware datetime.
    """
    return datetime.now(timezone.utc).replace(microsecond=0)


# Fonction de calcul de l'empreinte méthodologique
def fingerprint(name: str, params: Mapping[str, Any]) -> str:
    """Stable 16-hex digest of a metric, method or step methodology.

    The payload is serialised as canonical JSON (sorted keys, no blanks,
    ``str`` fallback for non-JSON values), hence stable across runs, platforms
    and key orders. The code of a metric is deliberately not digested (a
    comment change would recompute everything): a formula fix is propagated by
    invalidating the recorded fingerprints (:meth:`FreshnessRegistry.invalidate`).

    Args:
        name: Name of the metric, method or step.
        params: Parameters influencing its written values.

    Returns:
        The first 16 hexadecimal characters of the SHA-256 digest.

    Examples:
        >>> fingerprint("HHI", {"world_code": "WORLD"})
        '516870fb7092aca9'
        >>> fingerprint("HHI", {"b": 1, "a": 2}) == fingerprint("HHI", {"a": 2, "b": 1})
        True
        >>> fingerprint("HHI", {"world_code": "EXT"}) != fingerprint("HHI", {"world_code": "WORLD"})
        True
    """
    payload = json.dumps(
        {"name": name, "params": params},
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# Séparateur entre un nom de métrique et son qualificatif (le sens du flux)
QUALIFIER_SEPARATOR = "/"


# Fonction de construction d'une clé d'empreinte qualifiée
def qualified_name(name: str, qualifier: str) -> str:
    """Qualify a metric name, e.g. by a flow direction.

    Two instances of a metric computed in two directions share a name: their
    fingerprints are kept under qualified keys, so that adding a direction only
    makes that direction stale.

    Args:
        name: Metric name.
        qualifier: Qualifier (a flow direction).

    Returns:
        ``"<name>/<qualifier>"``.

    Examples:
        >>> qualified_name("HHI", "export")
        'HHI/export'
    """
    return f"{name}{QUALIFIER_SEPARATOR}{qualifier}"


# Fonction de décomposition d'une clé d'empreinte qualifiée
def split_qualified(key: str) -> Tuple[str, Optional[str]]:
    """Split a fingerprint key into its name and qualifier.

    Args:
        key: A plain (``"HHI"``) or qualified (``"HHI/export"``) key.

    Returns:
        ``(name, qualifier)``, the qualifier being ``None`` for a plain key.

    Examples:
        >>> split_qualified("HHI/export"), split_qualified("synthesis")
        (('HHI', 'export'), ('synthesis', None))
    """
    name, separator, qualifier = key.partition(QUALIFIER_SEPARATOR)
    return (name, qualifier) if separator else (key, None)


# Fonction de correspondance entre une clé d'empreinte et des noms demandés
def name_matches(key: str, names: Collection[str]) -> bool:
    """Tell whether a fingerprint key is designated by a set of names.

    A plain name designates every qualified key of that name (``HHI`` matches
    ``HHI/import`` and ``HHI/export``), a qualified name only itself — so that
    forcing or invalidating a metric covers every direction unless one is
    named.

    Args:
        key: Fingerprint key.
        names: Requested names, plain or qualified.

    Returns:
        ``True`` when ``key`` or its unqualified name is in ``names``.

    Examples:
        >>> name_matches("HHI/export", {"HHI"}), name_matches("HHI/export", {"HHI/import"})
        (True, False)
    """
    return key in names or split_qualified(key)[0] in names


# ──────────────────────────────────────────────────────────────────────
# Unités, plans et entrées
# ──────────────────────────────────────────────────────────────────────

# Classe d'unité de fraîcheur
@dataclass(frozen=True, order=True)
class Unit:
    """Hashable unit of freshness: an ordered tuple of named dimensions.

    Args:
        dims: ``(name, value)`` pairs, in declaration order, values as text.

    Examples:
        >>> unit = Unit.of(classification="HS2022", reporter="FR", product="280530")
        >>> unit.key, unit.get("reporter")
        ('HS2022|FR|280530', 'FR')
        >>> unit == Unit.from_mapping(unit.as_dict())
        True
        >>> Unit.of(scope="global").key
        'global'
    """

    dims: Tuple[Tuple[str, str], ...]

    # Construction à partir d'arguments nommés
    @classmethod
    def of(cls, **dims: Any) -> "Unit":
        """Build a unit from keyword dimensions (values converted to text).

        Args:
            **dims: Dimensions of the unit, in order.

        Returns:
            The unit.

        Raises:
            ValueError: If no dimension is given.
        """
        return cls.from_mapping(dims)

    # Construction à partir d'un mapping (relecture JSON)
    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "Unit":
        """Build a unit from a mapping (order of the mapping kept).

        Args:
            mapping: Dimension name -> value.

        Returns:
            The unit.

        Raises:
            ValueError: If the mapping is empty.
        """
        # Vérification des arguments
        if not mapping:
            raise ValueError("A unit needs at least one dimension")
        return cls(tuple((str(name), str(value)) for name, value in mapping.items()))

    # Valeur d'une dimension
    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Return the value of a dimension, or ``default`` when absent.

        Args:
            name: Dimension name.
            default: Value returned when the unit has no such dimension.

        Returns:
            The dimension value.
        """
        for dim, value in self.dims:
            if dim == name:
                return value
        return default

    # Dimensions sous forme de dictionnaire
    def as_dict(self) -> Dict[str, str]:
        """Return the dimensions as an ordered dictionary."""
        return dict(self.dims)

    # Clé textuelle de l'unité dans un fragment
    @property
    def key(self) -> str:
        """Text key of the unit within a fragment (values joined by ``|``)."""
        return "|".join(value for _, value in self.dims)

    # Représentation textuelle
    def __str__(self) -> str:
        return self.key


# Classe de plan de calcul d'une unité
@dataclass(frozen=True)
class UnitPlan:
    """Why a unit is (re)computed, and which metrics or methods are concerned.

    Args:
        reason: One of ``first``, ``forced``, ``new_data``, ``fingerprint``.
        names: Metrics or methods to recompute (a subset of the requested
            ones). Wide-table steps (partners, network) still recompute every
            metric of a planned unit; ``names`` matters to long tables.
    """

    reason: str
    names: FrozenSet[str]


# Classe d'entrée de registre
@dataclass
class RegistryEntry:
    """State of one unit in a freshness registry.

    Args:
        unit: The unit.
        last_computed: Instant captured before the last computation started
            (``None`` for a pass started but never completed).
        upstream_watermark: Upstream instant taken into account.
        fingerprints: Metric or method name -> fingerprint at computation time.
        reason: Reason of the last computation.
        extra: Step-specific fields (counters, ``fit_id``, ``years_scope``,
            ``years_written``, ``upstream_reasons``…), stored at the top level
            of the JSON entry.
        legacy: Whether the entry comes from a version-1 registry and has not
            been migrated yet.

    Examples:
        >>> entry = RegistryEntry(Unit.of(vintage="HS2017"), parse_instant("2026-01-01"),
        ...                       fingerprints={"baci": "abc"}, reason="first",
        ...                       extra={"n_rows": 3})
        >>> RegistryEntry.from_json(entry.to_json()) == entry
        True
    """

    unit: Unit
    last_computed: Optional[datetime] = None
    upstream_watermark: Optional[datetime] = None
    fingerprints: Dict[str, str] = field(default_factory=dict)
    reason: str = "first"
    extra: Dict[str, Any] = field(default_factory=dict)
    legacy: bool = False

    # Sérialisation JSON (format d'un fragment v2)
    def to_json(self) -> Dict[str, Any]:
        """Serialise the entry into its fragment representation.

        Returns:
            JSON-ready mapping; ``extra`` fields are stored at the top level,
            and ``legacy`` only when true.
        """
        payload: Dict[str, Any] = {
            "unit": self.unit.as_dict(),
            "last_computed": format_instant(self.last_computed),
            "upstream_watermark": format_instant(self.upstream_watermark),
            "fingerprints": dict(sorted(self.fingerprints.items())),
            "reason": self.reason,
        }
        for key, value in self.extra.items():
            if key not in _ENTRY_KEYS:
                payload[key] = value
        if self.legacy:
            payload["legacy"] = True
        return payload

    # Désérialisation JSON
    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "RegistryEntry":
        """Read an entry from its fragment representation.

        Args:
            payload: Mapping produced by :meth:`to_json`.

        Returns:
            The entry.

        Raises:
            KeyError: If the payload has no ``unit``.
        """
        return cls(
            unit=Unit.from_mapping(payload["unit"]),
            last_computed=parse_instant(payload.get("last_computed")),
            upstream_watermark=parse_instant(payload.get("upstream_watermark")),
            fingerprints={str(k): str(v) for k, v in (payload.get("fingerprints") or {}).items()},
            reason=str(payload.get("reason") or "first"),
            extra={k: v for k, v in payload.items() if k not in _ENTRY_KEYS},
            legacy=bool(payload.get("legacy", False)),
        )


# Fonction de construction d'une entrée héritée d'un registre v1
def legacy_entry(unit: Unit, last_computed: Any, **extra: Any) -> RegistryEntry:
    """Build the entry of a unit read from a version-1 registry.

    Documented migration choice: a version-1 entry carries no fingerprint, so
    it is read with ``fingerprints={}`` — "never computed with the current
    methodology" — and every metric of the unit will be recomputed, unless
    the run adopts the current fingerprints (``adopt_legacy_fingerprints``).
    Its ``upstream_watermark`` is its ``last_computed``: this reproduces the
    version-1 rule exactly (stale when the upstream is more recent than the
    last computation). The reason is ``first``: version 1 knew no other one.

    Args:
        unit: The unit.
        last_computed: Version-1 computation instant (text or datetime).
        **extra: Fields kept from the version-1 entry (counters…).

    Returns:
        The legacy entry, or an entry with ``last_computed=None`` when the
        instant is unreadable (the unit is then planned as ``first``).

    Examples:
        >>> entry = legacy_entry(Unit.of(vintage="HS2017"), "2026-01-01T00:00:00+00:00")
        >>> entry.legacy, entry.fingerprints, entry.upstream_watermark == entry.last_computed
        (True, {}, True)
    """
    instant = parse_instant(last_computed)
    return RegistryEntry(
        unit=unit,
        last_computed=instant,
        upstream_watermark=instant,
        fingerprints={},
        reason="first",
        extra=dict(extra),
        legacy=True,
    )


# Classe de source de registre v1
@dataclass(frozen=True)
class LegacySource:
    """Version-1 registry read during the migration.

    Args:
        path: Path (or S3 key) of the version-1 JSON file. It may be the very
            path of a fragment (single-fragment registries such as the
            synthesis one): a file without ``schema_version`` is then read as
            version 1.
        bucket: S3 bucket, or ``None`` for a local file.
        parse: Function turning the version-1 document into legacy entries
            (see :func:`legacy_entry`).
    """

    path: Any
    bucket: Optional[str]
    parse: Callable[[Mapping[str, Any]], Iterable[RegistryEntry]]


# ──────────────────────────────────────────────────────────────────────
# Registre fragmenté
# ──────────────────────────────────────────────────────────────────────

# Fonction de conversion d'un chemin en texte POSIX
def _posix(path: Any) -> str:
    """Return a path as POSIX text (registry paths double as S3 keys)."""
    return str(path).replace("\\", "/")


# Classe de registre de fraîcheur fragmenté
class FreshnessRegistry:
    """Freshness registry of one step, split into JSON fragments.

    The fragment of a unit lives at ``path_template`` formatted with the
    unit's dimensions (each value sanitised into a safe file name), e.g.
    ``"trade/state/vulnerabilities/partners/{classification}/{reporter}.json"``.
    A template without any field is a single-fragment registry.

    Fragments are loaded lazily, on the first access to one of their units,
    and :meth:`save` only writes the fragments modified since. Two instances
    working on disjoint fragments (two pods) never lose each other's entries.

    Args:
        path_template: Fragment path template (local path or S3 key).
        bucket: S3 bucket, or ``None`` for local storage.
        step: Step name recorded in each fragment (``"partners"``…).
        shard_of: Function giving the fragment label of a unit, recorded in the
            ``fragment`` field of the file (for readers; the path comes from
            the template).
        legacy: Version-1 registry to fall back on for units absent from the
            fragments.
        loader: JSON loader (a fresh one by default).
        saver: JSON saver (a fresh one by default).
        storage_options: S3 connection arguments forwarded to the loader and
            the saver (``endpoint_url``…); ``None`` relies on the environment.

    Examples:
        >>> registry = FreshnessRegistry(
        ...     "state/network/{vintage}.json", None, "network",
        ...     shard_of=lambda unit: unit.get("vintage"),
        ... )
        >>> registry.path_of(Unit.of(vintage="HS2017"))
        'state/network/HS2017.json'
    """

    # Initialisation
    def __init__(
        self,
        path_template: Any,
        bucket: Optional[str],
        step: str,
        shard_of: Callable[[Unit], str],
        *,
        legacy: Optional[LegacySource] = None,
        loader: Optional[Loader] = None,
        saver: Optional[Saver] = None,
        storage_options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.path_template = _posix(path_template)
        self.bucket = bucket
        self.step = step
        self.shard_of = shard_of
        self.legacy = legacy
        self.loader = loader or Loader()
        self.saver = saver or Saver()
        self.storage_options = dict(storage_options or {})
        # Fragments chargés : chemin -> {clé d'unité -> entrée}
        self._fragments: Dict[str, Dict[str, RegistryEntry]] = {}
        # Libellé de fragment par chemin (champ « fragment » du fichier)
        self._fragment_names: Dict[str, str] = {}
        # Fragments modifiés depuis le dernier save()
        self._dirty: Set[str] = set()
        # Entrées héritées (registre v1), lues une seule fois à la demande
        self._legacy_entries: Optional[Dict[Unit, RegistryEntry]] = None
        # Chemins effectivement lus (traçabilité du chargement paresseux)
        self.loaded_paths: List[str] = []

    # Chemin du fragment d'une unité
    def path_of(self, unit: Unit) -> str:
        """Return the fragment path of a unit.

        Args:
            unit: The unit.

        Returns:
            The template formatted with the unit's sanitised dimensions.

        Raises:
            KeyError: If the template names a dimension the unit lacks.
        """
        values = {name: sanitize_shard(value) for name, value in unit.dims}
        try:
            return self.path_template.format_map(values)
        except KeyError as exc:
            raise KeyError(
                f"Unit {unit.as_dict()} lacks dimension {exc} required by the "
                f"fragment template '{self.path_template}'"
            ) from exc

    # Lecture brute d'un document JSON
    def _read(self, path: str, bucket: Optional[str]) -> Optional[Any]:
        """Read a JSON document (``None`` when absent)."""
        return self.loader.load(path, bucket=bucket, missing_ok=True, **self.storage_options)

    # Chargement paresseux d'un fragment
    def _fragment(self, path: str) -> Dict[str, RegistryEntry]:
        """Return the entries of a fragment, loading it on first access."""
        if path not in self._fragments:
            data = self._read(path, self.bucket)
            self.loaded_paths.append(path)
            entries: Dict[str, RegistryEntry] = {}
            # Fragment v2 ; tout autre contenu (registre v1 au même chemin) est
            # laissé à la source héritée
            if isinstance(data, Mapping) and data.get("schema_version") == SCHEMA_VERSION:
                for key, payload in (data.get("entries") or {}).items():
                    try:
                        entries[str(key)] = RegistryEntry.from_json(payload)
                    except (KeyError, TypeError, ValueError):
                        logger.warning("Entrée de registre illisible ignorée : %s[%s]", path, key)
            self._fragments[path] = entries
        return self._fragments[path]

    # Chargement paresseux des entrées héritées
    def _legacy(self) -> Dict[Unit, RegistryEntry]:
        """Return the legacy entries, reading the version-1 registry once."""
        if self._legacy_entries is None:
            self._legacy_entries = {}
            if self.legacy is not None:
                data = self._read(_posix(self.legacy.path), self.legacy.bucket)
                # Document v1 seulement : un fragment v2 au même chemin n'est pas hérité
                if isinstance(data, Mapping) and "schema_version" not in data:
                    for entry in self.legacy.parse(data):
                        self._legacy_entries[entry.unit] = replace(entry, legacy=True)
                    logger.info(
                        "%d entrée(s) héritée(s) lue(s) dans le registre v1 '%s'",
                        len(self._legacy_entries), self.legacy.path,
                    )
        return self._legacy_entries

    # Lecture de l'entrée d'une unité
    def get(self, unit: Unit) -> Optional[RegistryEntry]:
        """Return the entry of a unit (fragment first, then version 1).

        Args:
            unit: The unit.

        Returns:
            The entry, or ``None`` when the unit was never recorded.
        """
        entry = self._fragment(self.path_of(unit)).get(unit.key)
        if entry is not None:
            return entry
        return self._legacy().get(unit)

    # Insertion ou remplacement de l'entrée d'une unité
    def upsert(self, entry: RegistryEntry) -> None:
        """Insert or replace the entry of a unit (in memory until :meth:`save`).

        Args:
            entry: The new entry; its ``legacy`` flag is cleared.
        """
        path = self.path_of(entry.unit)
        self._fragment(path)[entry.unit.key] = replace(entry, legacy=False)
        self._fragment_names[path] = self.shard_of(entry.unit)
        self._dirty.add(path)

    # Adoption des empreintes courantes par une entrée v1
    def adopt_legacy(self, unit: Unit, requested: Mapping[str, str]) -> bool:
        """Copy the current fingerprints onto a version-1 entry, without recomputation.

        Used once, at deployment, so that switching to fingerprinted
        registries does not recompute every unit. The adopted entry is written
        into its fragment at the next :meth:`save`.

        Args:
            unit: The unit.
            requested: Current fingerprints, by metric or method name.

        Returns:
            ``True`` when a legacy entry was adopted.
        """
        entry = self.get(unit)
        if entry is None or not entry.legacy:
            return False
        self.upsert(replace(entry, fingerprints=dict(requested)))
        return True

    # Écriture des fragments modifiés
    def save(self) -> List[str]:
        """Write the fragments modified since the last call.

        A fragment located at the very path of the version-1 registry also
        keeps the legacy entries it holds (flagged ``legacy``), so that
        overwriting the file never drops a non-migrated unit.

        Returns:
            Paths written, sorted.
        """
        written: List[str] = []
        for path in sorted(self._dirty):
            entries = dict(self._fragments[path])
            # Entrées héritées non migrées d'un registre v1 au même chemin
            if self.legacy is not None and _posix(self.legacy.path) == path:
                for unit, entry in self._legacy().items():
                    if self.path_of(unit) == path and unit.key not in entries:
                        entries[unit.key] = entry
            document = {
                "schema_version": SCHEMA_VERSION,
                "step": self.step,
                "fragment": self._fragment_names.get(path, ""),
                "entries": {key: entries[key].to_json() for key in sorted(entries)},
            }
            self.saver.save(
                path, document, bucket=self.bucket, indent=2, ensure_ascii=False,
                **self.storage_options,
            )
            written.append(path)
        # Logging
        if written:
            logger.info("Registre '%s' : %d fragment(s) écrit(s)", self.step, len(written))
        self._dirty.clear()
        return written

    # Invalidation des empreintes enregistrées (recalcul à la passe suivante)
    def invalidate(
        self,
        names: Optional[Collection[str]] = None,
        scope: Optional["ForceSpec"] = None,
    ) -> List[Unit]:
        """Delete recorded fingerprints so that the next run recomputes the units.

        This is the procedure after fixing the implementation of a metric, a
        method or a step: the fingerprints do not digest the code, so the fix
        must be signalled. Deleting the fingerprint of the fixed names (rather
        than the whole entry) keeps the history of the unit
        (``last_computed``, ``upstream_watermark``) and makes the next run
        recompute it with the reason ``fingerprint``, which the downstream
        steps treat as a complete change of their input. The invalidation is
        persisted by :meth:`save`: a failed run leaves the units invalid and
        the next run retries them.

        Must not run while the step itself is running: a pod that already
        loaded a fragment would write it back with the old fingerprints.

        Args:
            names: Metric or method names whose fingerprint is deleted; a plain
                name also deletes its qualified keys (``HHI`` deletes
                ``HHI/import`` and ``HHI/export``, see :func:`name_matches`).
                ``None`` deletes every fingerprint of the selected units.
            scope: Unit filter (``reporters``, ``products`` by prefix,
                ``periods``, ``vintages``; a filter on a dimension the unit
                does not have is ignored). ``None`` selects every unit.

        Returns:
            Units actually invalidated (those that held at least one of the
            fingerprints), sorted. Nothing is written until :meth:`save`, so a
            dry run simply skips it.

        Examples:
            >>> registry = FreshnessRegistry("{vintage}.json", None, "demo", lambda u: u.key,
            ...                              loader=_EmptyLoader())
            >>> registry.upsert(RegistryEntry(Unit.of(vintage="HS2017"), utc_now(),
            ...                               fingerprints={"m": "x", "n": "y"}))
            >>> [unit.key for unit in registry.invalidate({"m"})]
            ['HS2017']
            >>> registry.get(Unit.of(vintage="HS2017")).fingerprints
            {'n': 'y'}
        """
        selected = [
            entry for entry in list(self._iter_known_entries())
            if scope is None or scope.in_scope(entry.unit)
        ]
        invalidated: List[Unit] = []
        for entry in selected:
            if names is None:
                kept: Dict[str, str] = {}
            else:
                kept = {
                    k: v for k, v in entry.fingerprints.items() if not name_matches(k, set(names))
                }
            if kept == entry.fingerprints:
                continue
            self.upsert(replace(entry, fingerprints=kept))
            invalidated.append(entry.unit)
        # Logging
        logger.info(
            "Registre '%s' : %d unité(s) invalidée(s) (empreintes %s)",
            self.step, len(invalidated), sorted(names) if names is not None else "toutes",
        )
        return sorted(invalidated)

    # Entrées connues : fragments listés, fragments déjà chargés et entrées héritées
    def _iter_known_entries(self) -> Iterator[RegistryEntry]:
        """Iterate over every entry, including fragments only held in memory."""
        seen: Set[Unit] = set()
        for entry in self.iter_entries():
            seen.add(entry.unit)
            yield entry
        for entries in list(self._fragments.values()):
            for entry in list(entries.values()):
                if entry.unit not in seen:
                    seen.add(entry.unit)
                    yield entry

    # Liste des fichiers JSON des fragments sous un préfixe
    def _list_json_recursive(self, directory: str) -> List[str]:
        """List the candidate fragment files under a directory or S3 prefix.

        Locally, only the levels of the template are walked (one ``*`` per
        field); on S3, the prefix is listed without delimiter (every level),
        the caller filtering the keys on the template. Hidden files
        (atomic-write temporaries ``.tmp-*``) are skipped.

        Args:
            directory: Local directory or S3 prefix.

        Returns:
            Sorted POSIX paths (local) or object keys (S3).
        """
        if self.bucket is None:
            root = Path(directory or ".")
            if not root.is_dir():
                return []
            # Motif glob du modèle sous le répertoire statique (un « * » par champ) :
            # seuls les niveaux du modèle sont parcourus, jamais toute l'arborescence
            relative = self.path_template[len(directory):].lstrip("/") if directory else self.path_template
            glob = "".join(
                "*" if index % 2 else part for index, part in enumerate(_interleave(relative))
            )
            return sorted(
                f"{directory}/{_posix(child.relative_to(root))}" if directory else _posix(child)
                for child in root.glob(glob)
                if child.is_file() and not child.name.startswith(".")
            )
        # Connexion S3 paresseuse du chargeur (identifiants de l'environnement)
        if not hasattr(self.loader, "s3"):
            self.loader.connect(**self.storage_options)
        prefix = directory.rstrip("/") + "/" if directory else ""
        if self.loader.s3_package == "boto3":
            keys: List[str] = []
            paginator = self.loader.s3.get_paginator("list_objects_v2")
            # Pas de délimiteur : les sous-préfixes (un niveau par dimension) sont parcourus
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
        else:
            keys = [path.split("/", 1)[1] for path in self.loader.s3.find(f"{self.bucket}/{prefix}")]
        return sorted(
            key for key in keys
            if key.endswith(".json") and not key.rsplit("/", 1)[-1].startswith(".")
        )

    # Itération sur toutes les entrées (lecture aval)
    def iter_entries(self) -> Iterator[RegistryEntry]:
        """Iterate over every entry of the registry: all fragments, then legacy ones.

        Lists the files matching the template under its static prefix (the
        part before the first field), so a downstream step can read the whole
        state of an upstream step without knowing its units. Legacy entries
        are yielded only for units absent from the fragments.

        Yields:
            Registry entries.
        """
        # Motif des chemins de fragments : un segment par champ du modèle
        pattern = re.compile(
            "".join(
                "[^/]+" if index % 2 else re.escape(part)
                for index, part in enumerate(_interleave(self.path_template))
            )
        )
        static = self.path_template.split("{", 1)[0]
        directory = static.rsplit("/", 1)[0] if "/" in static else ""
        if "{" in self.path_template:
            paths = [p for p in self._list_json_recursive(directory) if pattern.fullmatch(p)]
        else:
            paths = [self.path_template]

        seen: Set[Unit] = set()
        for path in paths:
            for entry in self._fragment(path).values():
                seen.add(entry.unit)
                yield entry
        for unit, entry in self._legacy().items():
            if unit not in seen:
                yield entry


# Fonction de découpage d'un modèle de chemin en parties fixes et champs
def _interleave(template: str) -> List[str]:
    """Split a path template into alternating literal parts and fields.

    Args:
        template: Path template (``"a/{x}/{y}.json"``).

    Returns:
        ``[literal, field, literal, field, …, literal]`` (even indices are
        literal text, odd indices field names).

    Examples:
        >>> _interleave("a/{x}/{y}.json")
        ['a/', 'x', '/', 'y', '.json']
    """
    return re.split(r"\{([^{}]*)\}", template)


# ──────────────────────────────────────────────────────────────────────
# Forçage ponctuel
# ──────────────────────────────────────────────────────────────────────

# Fonction de lecture d'une valeur de forçage en liste de chaînes
def parse_force_list(value: Any) -> Tuple[str, ...]:
    """Parse a forcing value into a tuple of non-empty strings.

    Accepts a string separated by commas or semicolons (the semicolon is the
    one that survives ``kedro run --params``, which splits its argument on
    commas), a YAML list, a number (``OmegaConf`` turns ``2020`` into an
    integer) or ``None``.

    Args:
        value: Raw value from the run-time parameters or the environment.

    Returns:
        Distinct values, in order of appearance.

    Examples:
        >>> parse_force_list("FR, DE"), parse_force_list("2020;2021")
        (('FR', 'DE'), ('2020', '2021'))
        >>> parse_force_list(["28", 8541]), parse_force_list(2020), parse_force_list("")
        (('28', '8541'), ('2020',), ())
    """
    if value is None:
        return ()
    if isinstance(value, bool):
        return (str(value).lower(),)
    if isinstance(value, (int, float)):
        return (str(int(value)) if float(value).is_integer() else str(value),)
    if isinstance(value, (list, tuple, set, frozenset)):
        items: List[str] = []
        for item in value:
            items.extend(parse_force_list(item))
        return tuple(dict.fromkeys(items))
    parts = (part.strip() for part in _FORCE_SEPARATORS.split(str(value)))
    return tuple(dict.fromkeys(part for part in parts if part))


# Classe de spécification d'un forçage
@dataclass(frozen=True)
class ForceSpec:
    """One-off forcing of a recomputation, from the run-time parameters.

    A step is forced when it is listed in ``steps`` (or ``steps`` holds
    ``all``), **or** when ``metrics`` / ``methods`` name one of the metrics or
    methods it computes — so ``FORCE_METRICS=HHI`` alone recomputes HHI
    everywhere. The fingerprints recorded after a forced pass are the current
    ones: the next run therefore recomputes nothing. Forcing is ephemeral (a
    failed forced run must be submitted again); to recompute after a formula
    fix, prefer :meth:`FreshnessRegistry.invalidate`, which is persisted.

    The scope filters restrict the forced units of a step; a filter on a
    dimension the unit does not have is ignored (``periods`` on a BACI
    vintage, which is always re-estimated as a whole since its parameters are
    estimated on all its years).

    Args:
        steps: Forced steps (``partners``, ``network``, ``baci``,
            ``synthesis``, ``coherence``…) or ``all``.
        metrics: Forced metric names.
        methods: Forced synthesis method names.
        reporters: Reporter codes (dimension ``reporter``).
        products: Product code prefixes (dimension ``product``).
        periods: Periods (dimensions ``period``, ``year``, ``TIME_PERIOD``).
        vintages: Vintages (dimensions ``vintage``, ``classification``).

    Examples:
        >>> spec = ForceSpec(steps=frozenset({"partners"}), reporters=("FR",))
        >>> spec.covers("partners", Unit.of(classification="HS2022", reporter="FR", product="28"))
        True
        >>> spec.covers("partners", Unit.of(classification="HS2022", reporter="DE", product="28"))
        False
        >>> ForceSpec(metrics=("HHI",)).forces_step("partners", {"HHI": "x", "CDI2": "y"})
        True
    """

    steps: FrozenSet[str] = frozenset()
    metrics: Tuple[str, ...] = ()
    methods: Tuple[str, ...] = ()
    reporters: Tuple[str, ...] = ()
    products: Tuple[str, ...] = ()
    periods: Tuple[str, ...] = ()
    vintages: Tuple[str, ...] = ()

    # Construction depuis les paramètres d'exécution et l'environnement
    @classmethod
    def from_runtime(
        cls,
        runtime_params: Optional[Mapping[str, Any]],
        environ: Optional[Mapping[str, str]] = None,
    ) -> "ForceSpec":
        """Build the forcing from the ``runtime`` parameters and the environment.

        Reads ``FORCE_STEPS``, ``FORCE_METRICS``, ``FORCE_METHODS`` and
        ``FORCE_SCOPE.{REPORTERS, PRODUCTS, PERIODS, VINTAGES}``; a non-empty
        environment variable (``FORCE_STEPS``, ``FORCE_METRICS``,
        ``FORCE_METHODS``, ``FORCE_REPORTERS``, ``FORCE_PRODUCTS``,
        ``FORCE_PERIODS``, ``FORCE_VINTAGES``) overrides the parameter.

        Args:
            runtime_params: The ``runtime`` mapping, or the whole runtime file
                (``{"runtime": {...}}``); ``None`` for no parameter.
            environ: Environment (``os.environ`` by default).

        Returns:
            The forcing specification (empty when nothing is set).

        Examples:
            >>> spec = ForceSpec.from_runtime(
            ...     {"runtime": {"FORCE_STEPS": "baci", "FORCE_SCOPE": {"VINTAGES": "HS2017"}}},
            ...     environ={"FORCE_PERIODS": "2020;2021"},
            ... )
            >>> sorted(spec.steps), spec.vintages, spec.periods
            (['baci'], ('HS2017',), ('2020', '2021'))
        """
        params: Mapping[str, Any] = runtime_params or {}
        if isinstance(params.get("runtime"), Mapping):
            params = params["runtime"]
        scope = params.get("FORCE_SCOPE") or {}
        environ = os.environ if environ is None else environ
        raw = {
            "steps": params.get("FORCE_STEPS"),
            "metrics": params.get("FORCE_METRICS"),
            "methods": params.get("FORCE_METHODS"),
            "reporters": scope.get("REPORTERS"),
            "products": scope.get("PRODUCTS"),
            "periods": scope.get("PERIODS"),
            "vintages": scope.get("VINTAGES"),
        }
        # Surcharge par l'environnement (valeurs non vides seulement)
        for name, variable in FORCE_ENV_VARIABLES.items():
            value = environ.get(variable)
            if value is not None and value.strip():
                raw[name] = value
        values = {name: parse_force_list(value) for name, value in raw.items()}
        return cls(
            steps=frozenset(step.lower() for step in values["steps"]),
            metrics=values["metrics"],
            methods=values["methods"],
            reporters=values["reporters"],
            products=values["products"],
            periods=values["periods"],
            vintages=values["vintages"],
        )

    # Absence de tout forçage
    @property
    def is_empty(self) -> bool:
        """Whether no step and no metric or method is forced."""
        return not (self.steps or self.metrics or self.methods)

    # Noms forcés (métriques et méthodes)
    @property
    def names(self) -> FrozenSet[str]:
        """Forced metric and method names."""
        return frozenset(self.metrics) | frozenset(self.methods)

    # Forçage d'une étape
    def forces_step(self, step: str, requested: Collection[str] = ()) -> bool:
        """Tell whether a step is forced.

        Args:
            step: Step name.
            requested: Metrics or methods the step computes.

        Returns:
            ``True`` when the step is listed (or ``all``), or when a forced
            metric or method is one of ``requested``.
        """
        if FORCE_ALL in self.steps or step.lower() in self.steps:
            return True
        return any(name_matches(key, self.names) for key in requested)

    # Appartenance d'une unité au périmètre de forçage
    def in_scope(self, unit: Unit) -> bool:
        """Tell whether a unit passes the scope filters.

        Args:
            unit: The unit.

        Returns:
            ``True`` when every filter on a dimension of the unit matches
            (product codes match by prefix).
        """
        for name, dimensions in _SCOPE_DIMENSIONS.items():
            accepted: Tuple[str, ...] = getattr(self, name)
            if not accepted:
                continue
            for dimension in dimensions:
                value = unit.get(dimension)
                if value is None:
                    continue
                if name == "products":
                    if not any(value.startswith(prefix) for prefix in accepted):
                        return False
                elif value not in accepted:
                    return False
        return True

    # Couverture d'une unité d'une étape par le forçage
    def covers(self, step: str, unit: Unit, requested: Collection[str] = ()) -> bool:
        """Tell whether a unit of a step is forced.

        Args:
            step: Step name.
            unit: The unit.
            requested: Metrics or methods the step computes.

        Returns:
            ``True`` when the step is forced and the unit is in scope.
        """
        return self.forces_step(step, requested) and self.in_scope(unit)

    # Noms à recalculer pour une unité forcée
    def names_for(self, requested: Collection[str]) -> FrozenSet[str]:
        """Return the metrics or methods to recompute on a forced unit.

        Args:
            requested: Metrics or methods the step computes.

        Returns:
            The forced names among ``requested`` (a plain name selecting every
            qualified key of that name, see :func:`name_matches`), or all of
            ``requested`` when none is named.

        Examples:
            >>> sorted(ForceSpec(metrics=("HHI",)).names_for({"HHI/import", "HHI/export", "CDI2/import"}))
            ['HHI/export', 'HHI/import']
        """
        chosen = {key for key in requested if name_matches(key, self.names)}
        return frozenset(chosen) if chosen else frozenset(requested)

    # Description du forçage (tag MLflow « forced »)
    def describe(self) -> str:
        """Describe the forcing for the ``forced`` run tag.

        Returns:
            ``field=values`` items joined by ``;`` (non-empty fields only).

        Examples:
            >>> ForceSpec(steps=frozenset({"partners"}), metrics=("HHI",)).describe()
            'steps=partners;metrics=HHI'
        """
        items = [("steps", sorted(self.steps))] + [
            (name, list(getattr(self, name)))
            for name in ("metrics", "methods", "reporters", "products", "periods", "vintages")
        ]
        return ";".join(f"{name}={','.join(values)}" for name, values in items if values)


# Fonction de lecture du drapeau d'adoption des empreintes par les entrées v1
def adopt_legacy_flag(
    state_config: Optional[Mapping[str, Any]],
    environ: Optional[Mapping[str, str]] = None,
) -> bool:
    """Read the ``ADOPT_LEGACY_FINGERPRINTS`` flag of a step.

    Args:
        state_config: The ``STATE`` block of the step (key
            ``ADOPT_LEGACY_FINGERPRINTS``), or ``None``.
        environ: Environment (``os.environ`` by default); a non-empty
            ``ADOPT_LEGACY_FINGERPRINTS`` variable overrides the YAML value.

    Returns:
        The flag.

    Examples:
        >>> adopt_legacy_flag({"ADOPT_LEGACY_FINGERPRINTS": False}, {"ADOPT_LEGACY_FINGERPRINTS": "true"})
        True
        >>> adopt_legacy_flag(None, {})
        False
    """
    environ = os.environ if environ is None else environ
    value = environ.get(ADOPT_ENV_VARIABLE)
    if value is not None and value.strip():
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool((state_config or {}).get("ADOPT_LEGACY_FINGERPRINTS", False))


# ──────────────────────────────────────────────────────────────────────
# Décision
# ──────────────────────────────────────────────────────────────────────

# Signature du prédicat « amont nouveau » (surchargeable par étape)
NewDataPredicate = Callable[[Unit, RegistryEntry, Optional[datetime]], bool]


# Prédicat par défaut : amont plus récent que le watermark enregistré
def upstream_is_newer(unit: Unit, entry: RegistryEntry, watermark: Optional[datetime]) -> bool:
    """Default ``new_data`` rule: the upstream is more recent than the recorded watermark.

    Args:
        unit: The unit (unused by the default rule).
        entry: Its registry entry.
        watermark: Current upstream instant of the unit (``None``: unknown).

    Returns:
        ``True`` when ``watermark`` is strictly more recent than the entry's
        ``upstream_watermark`` (or the entry has none).
    """
    if watermark is None:
        return False
    return entry.upstream_watermark is None or watermark > entry.upstream_watermark


# Fonction de décision des unités à (re)calculer
def units_to_compute(
    planned: Iterable[Unit],
    registry: FreshnessRegistry,
    upstream: Mapping[Unit, Optional[datetime]],
    requested: Mapping[str, str],
    force: ForceSpec,
    *,
    step: str,
    is_new_data: Optional[NewDataPredicate] = None,
    adopt_legacy_fingerprints: bool = False,
) -> Dict[Unit, UnitPlan]:
    """Return, per stale unit, the reason and the metrics or methods to (re)compute.

    A unit gets a single plan, the first matching reason winning:

    - ``first``: absent from the registry, or a pass started and never
      completed (``last_computed`` is ``None``) → every requested name;
    - ``forced``: within the forcing scope → the forced names among
      ``requested``, or all of them;
    - ``new_data``: ``is_new_data(unit, entry, upstream[unit])`` → every name;
    - ``fingerprint``: the names whose fingerprint differs from, or is missing
      in, the recorded ones → those names only.

    Args:
        planned: Candidate units (the units that exist upstream).
        registry: Freshness registry of the step.
        upstream: Current upstream instant per unit (missing → ``None``).
        requested: Current fingerprint per metric or method name.
        force: One-off forcing.
        step: Step name (matched against the forcing).
        is_new_data: Staleness rule; :func:`upstream_is_newer` by default.
        adopt_legacy_fingerprints: When true, a version-1 entry adopts the
            current fingerprints before the decision (deployment migration:
            no recomputation caused by the missing fingerprints).

    Returns:
        Mapping ``unit -> UnitPlan`` for the units to compute (empty when all
        are fresh).

    Examples:
        >>> registry = FreshnessRegistry("{vintage}.json", None, "demo", lambda u: u.key,
        ...                              loader=_EmptyLoader())
        >>> unit = Unit.of(vintage="HS2017")
        >>> units_to_compute([unit], registry, {}, {"m": "1"}, ForceSpec(), step="demo")[unit].reason
        'first'
    """
    rule = is_new_data or upstream_is_newer
    all_names = frozenset(requested)
    plans: Dict[Unit, UnitPlan] = {}
    for unit in planned:
        entry = registry.get(unit)
        # Migration : adoption des empreintes courantes par une entrée v1
        if adopt_legacy_fingerprints and entry is not None and entry.legacy:
            registry.adopt_legacy(unit, requested)
            entry = registry.get(unit)
        if entry is None or entry.last_computed is None:
            plans[unit] = UnitPlan("first", all_names)
        elif force.covers(step, unit, requested):
            plans[unit] = UnitPlan("forced", force.names_for(requested))
        elif rule(unit, entry, upstream.get(unit)):
            plans[unit] = UnitPlan("new_data", all_names)
        else:
            changed = frozenset(
                name for name, value in requested.items() if entry.fingerprints.get(name) != value
            )
            if changed:
                plans[unit] = UnitPlan("fingerprint", changed)
    return plans


# Fonction de détermination des qualificatifs (sens) à recalculer
def qualifiers_to_compute(
    plans: Mapping[Unit, UnitPlan], order: Sequence[str]
) -> Tuple[str, ...]:
    """Return the qualifiers (flow directions) named by a set of plans.

    A wide-table step keyed by qualified fingerprints (``HHI/export``)
    recomputes, for its planned units, only the directions whose fingerprint
    is stale: adding a direction to the configuration then computes that
    direction alone. A plain name in a plan designates every qualifier.

    Args:
        plans: Plans of the units to compute.
        order: Every configured qualifier, in output order.

    Returns:
        The qualifiers of ``order`` named by at least one plan, in that order.

    Examples:
        >>> unit = Unit.of(vintage="HS2017")
        >>> qualifiers_to_compute({unit: UnitPlan("fingerprint", frozenset({"SPOF/export"}))},
        ...                       ("import", "export"))
        ('export',)
        >>> qualifiers_to_compute({unit: UnitPlan("first", frozenset({"synthesis"}))}, ("import",))
        ('import',)
    """
    named = set()
    for plan in plans.values():
        for key in plan.names:
            qualifier = split_qualified(key)[1]
            if qualifier is None:
                return tuple(order)
            named.add(qualifier)
    return tuple(item for item in order if item in named)


# Chargeur JSON inerte (doctests et tests : registre vide sans I/O)
class _EmptyLoader(Loader):
    """JSON loader returning nothing (every fragment is absent)."""

    def load(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        return None


# Fonction de construction des métriques de fraîcheur d'une exécution
def plan_metrics(plans: Mapping[Unit, UnitPlan], n_candidates: Optional[int] = None) -> Dict[str, float]:
    """Run metrics describing a freshness decision.

    Args:
        plans: Result of :func:`units_to_compute`.
        n_candidates: Number of candidate units examined, when known.

    Returns:
        ``freshness/units_planned`` and one ``freshness/units_<reason>`` per
        reason (zero included), plus ``freshness/units_candidates`` when
        ``n_candidates`` is given.

    Examples:
        >>> plan_metrics({Unit.of(v="a"): UnitPlan("forced", frozenset())})["freshness/units_forced"]
        1.0
    """
    counts = Counter(plan.reason for plan in plans.values())
    metrics = {"freshness/units_planned": float(len(plans))}
    metrics.update({f"freshness/units_{reason}": float(counts.get(reason, 0)) for reason in REASONS})
    if n_candidates is not None:
        metrics["freshness/units_candidates"] = float(n_candidates)
    return metrics


# ──────────────────────────────────────────────────────────────────────
# Lecture aval (cascade)
# ──────────────────────────────────────────────────────────────────────

# Classe de résumé de l'état d'un registre amont
@dataclass(frozen=True)
class UpstreamSummary:
    """What changed upstream since a given instant.

    Args:
        watermark: Most recent ``last_computed`` of the upstream entries.
        n_units: Number of upstream entries read.
        reasons_since: Reason counts of the units computed strictly after the
            reference instant (all units when there is none).

    Examples:
        >>> UpstreamSummary(None, 0, {"forced": 2}).full_change
        True
    """

    watermark: Optional[datetime]
    n_units: int
    reasons_since: Mapping[str, int]

    # Changement méthodologique ou forcé en amont
    @property
    def full_change(self) -> bool:
        """Whether an upstream unit was recomputed for a ``fingerprint`` or ``forced`` reason.

        A downstream step treats such a change as a complete change of its
        input (every downstream unit is stale), as opposed to ``new_data``.
        """
        return bool(self.reasons_since.get("fingerprint") or self.reasons_since.get("forced"))

    # Représentation JSON (champ « upstream_reasons » des entrées aval)
    def to_json(self) -> Dict[str, Any]:
        """Serialise the summary for a downstream registry entry."""
        return {
            "watermark": format_instant(self.watermark),
            "n_units": self.n_units,
            "reasons_since": dict(sorted(self.reasons_since.items())),
            "full_change": self.full_change,
        }


# Fonction de résumé d'un ou plusieurs registres amont
def summarize_upstream(
    entries: Iterable[RegistryEntry],
    since: Optional[datetime] = None,
) -> UpstreamSummary:
    """Summarise upstream entries for a downstream freshness decision.

    Args:
        entries: Upstream entries (e.g. :meth:`FreshnessRegistry.iter_entries`
            of several registries chained).
        since: Reference instant (the downstream ``upstream_watermark``);
            ``None`` counts every computed entry.

    Returns:
        The upstream summary.

    Examples:
        >>> t1, t2 = parse_instant("2026-01-01"), parse_instant("2026-02-01")
        >>> summary = summarize_upstream(
        ...     [RegistryEntry(Unit.of(v="a"), t1, reason="new_data"),
        ...      RegistryEntry(Unit.of(v="b"), t2, reason="fingerprint")], since=t1)
        >>> summary.watermark == t2, dict(summary.reasons_since), summary.full_change
        (True, {'fingerprint': 1}, True)
    """
    watermark: Optional[datetime] = None
    n_units = 0
    reasons: Counter = Counter()
    for entry in entries:
        n_units += 1
        if entry.last_computed is None:
            continue
        if watermark is None or entry.last_computed > watermark:
            watermark = entry.last_computed
        if since is None or entry.last_computed > since:
            reasons[entry.reason] += 1
    return UpstreamSummary(watermark, n_units, dict(reasons))
