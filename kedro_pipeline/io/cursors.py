"""DuckLake cursors: what changed in an upstream table since a step last read it.

A **cursor** is the watermark of one step on one upstream DuckLake table: the
snapshot up to which the step has taken the changes of ``(catalog, schema)``
into account. It belongs to the consumer, so a skipped or failed run loses
nothing: the next read covers every snapshot since
(``statflows.storage.ducklake.tables.changes_since``).

Cursors are stored in a :class:`~kedro_pipeline.io.freshness.FreshnessRegistry`
(atomic JSON fragments, one per step: ``trade/state/cursors/{step}.json``), one
unit ``(step, catalog, schema)`` per cursor, with the fields ``snapshot`` and
``fingerprint`` (digest of what the step reads: schema, key columns, filters).
The maintenance reads them to never expire a snapshot a cursor still needs.

Protocol (two phases, so that the cursor can move forward without losing a
change even when the computation fails or a budget defers units):

1. pin the reads: ``pinned = current_snapshot(conn, alias)``;
2. :meth:`StepCursors.read` the changes up to ``pinned``, map them to the
   units of the step;
3. flag the touched units ``pending``
   (:func:`kedro_pipeline.io.freshness.mark_pending`) and save the unit
   registry;
4. :meth:`StepCursors.advance` then :meth:`StepCursors.save`;
5. compute the pending units, reading ``AT (VERSION => pinned)``; a successful
   unit is recorded without the flag.

Fallbacks (``CursorRead.fallback``), all meaning "every unit is touched":

- ``absent``: no cursor yet (the step decides: adoption or first run);
- ``fingerprint``: the fingerprint changed, or the cursor was invalidated
  (``invalidate-freshness-script --reset-cursor``);
- ``unavailable``: the snapshots following the cursor were expired, or the
  cursor is ahead of the catalog (``ValueError`` of ``changes_since``).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, replace
from datetime import datetime
import logging
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Tuple

# Modules du package
from kedro_pipeline.io.freshness import (
    FreshnessRegistry,
    RegistryEntry,
    Unit,
    fingerprint,
    utc_now,
)

# Initialisation du logger
logger = logging.getLogger(__name__)

# Table lue par défaut (convention dt_ducklake_manager)
FACT_TABLE = "fact_table"
# Raisons de repli sur un recalcul complet
FALLBACK_ABSENT = "absent"
FALLBACK_FINGERPRINT = "fingerprint"
FALLBACK_UNAVAILABLE = "unavailable"
# Empreinte enregistrée par une invalidation (jamais égale à une empreinte calculée)
INVALIDATED = "invalidated"


# Fonction de lecture du snapshot courant d'un catalogue
def current_snapshot(conn: Any, catalog_alias: str) -> Optional[int]:
    """Return the latest snapshot of an attached catalog (statflows wrapper).

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias of the attached catalog.

    Returns:
        The snapshot id, ``None`` when the catalog has none.
    """
    from statflows.storage.ducklake.tables import current_snapshot as _current

    return _current(conn, catalog_alias)


# Fonction de la clause de lecture à un snapshot (time travel DuckLake)
def time_travel(snapshot: Optional[int]) -> str:
    """Return the clause reading a DuckLake table at a snapshot.

    Placed right after the table name, or after its alias
    (``FROM t AS p AT (VERSION => 12)``).

    Args:
        snapshot: Snapshot id, or ``None`` for the current state.

    Returns:
        `` AT (VERSION => <snapshot>)``, or an empty string.

    Examples:
        >>> time_travel(12), time_travel(None)
        (' AT (VERSION => 12)', '')
    """
    return f" AT (VERSION => {int(snapshot)})" if snapshot is not None else ""


# Fonction de lecture tolérante du snapshot courant (catalogue sans snapshot, base non DuckLake)
def try_current_snapshot(conn: Any, catalog_alias: str) -> Optional[int]:
    """Return the current snapshot of a catalog, ``None`` when it cannot be read.

    Args:
        conn: Open connection.
        catalog_alias: Alias of the attached catalog.

    Returns:
        The snapshot id, or ``None`` (plain DuckDB database, test fakes…).
    """
    try:
        return current_snapshot(conn, catalog_alias)
    except Exception as exc:  # lecture non épinglée plutôt qu'un échec
        logger.debug(f"Snapshot courant de {catalog_alias} illisible : {exc}")
        return None


# Fonction de calcul de l'empreinte d'un curseur
def cursor_fingerprint(schema: str, key_columns: Sequence[str], **selection: Any) -> str:
    """Digest of what a step reads through a cursor.

    It covers only what the reading of the changes depends on — the schema,
    the key columns and the selection (filters) — not the methodology of the
    step, whose fingerprints are kept per unit.

    Args:
        schema: Schema read.
        key_columns: Key columns whose changed values are read.
        **selection: Selection parameters (filters, sources…).

    Returns:
        A 16-hex digest.

    Examples:
        >>> cursor_fingerprint("indicators", ["TIME_PERIOD"]) == cursor_fingerprint(
        ...     "indicators", ["TIME_PERIOD"])
        True
        >>> cursor_fingerprint("indicators", ["TIME_PERIOD"], where="a") != cursor_fingerprint(
        ...     "indicators", ["TIME_PERIOD"], where="b")
        True
    """
    return fingerprint(
        "cursor", {"schema": schema, "key_columns": list(key_columns), "selection": selection}
    )


# Classe de lecture d'un curseur
@dataclass(frozen=True)
class CursorRead:
    """Changes of an upstream table since the cursor of a step.

    Attributes:
        catalog: Catalog alias.
        schema: Schema read.
        since: Snapshot of the cursor (``None`` when absent or reset).
        target: Snapshot the changes were read up to (the pinned snapshot):
            the value :meth:`StepCursors.advance` records.
        fallback: ``None`` when the changes are known, else the reason why
            every unit must be considered touched (``absent``,
            ``fingerprint``, ``unavailable``).
        changed_keys: Distinct key tuples of the changed rows (empty when
            nothing changed); ``None`` on fallback.
        rows_changed: Rows inserted, updated or deleted since the cursor.
    """

    catalog: str
    schema: str
    since: Optional[int]
    target: Optional[int]
    fallback: Optional[str] = None
    changed_keys: Optional[FrozenSet[Tuple[Any, ...]]] = None
    rows_changed: int = 0

    # Repli sur un recalcul complet
    @property
    def full(self) -> bool:
        """Whether every unit must be considered touched."""
        return self.fallback is not None

    # Changement observé
    @property
    def modified(self) -> bool:
        """Whether something changed (always true on fallback)."""
        return self.full or bool(self.changed_keys) or self.rows_changed > 0

    # Métriques de la lecture
    def metrics(self, prefix: str) -> Dict[str, float]:
        """Run metrics of the read (``<prefix>/<schema>/...``).

        Args:
            prefix: Metric prefix (``"cursor"``).

        Returns:
            ``rows_changed``, ``keys_changed``, ``fallback`` (0/1).
        """
        base = f"{prefix}/{self.schema}"
        return {
            f"{base}/rows_changed": float(self.rows_changed),
            f"{base}/keys_changed": float(len(self.changed_keys or ())),
            f"{base}/fallback": float(self.full),
        }


# Classe des curseurs d'une étape
class StepCursors:
    """Cursors of one step on its upstream DuckLake tables.

    Args:
        registry: Registry of the cursors (template with a ``{step}`` field,
            e.g. ``trade/state/cursors/{step}.json``, shared by every step).
        step: Step name.

    Examples:
        >>> from kedro_pipeline.io.freshness import _EmptyLoader
        >>> registry = FreshnessRegistry("cursors/{step}.json", None, "cursors",
        ...                              shard_of=lambda u: u.get("step"), loader=_EmptyLoader())
        >>> cursors = StepCursors(registry, "synthesis")
        >>> cursors.snapshot("vulnerabilities", "indicators") is None
        True
    """

    def __init__(self, registry: FreshnessRegistry, step: str) -> None:
        self.registry = registry
        self.step = step

    # Unité d'un curseur
    def unit(self, catalog: str, schema: str) -> Unit:
        """Return the unit of the cursor of ``(catalog, schema)``."""
        return Unit.of(step=self.step, catalog=catalog, schema=schema)

    # Entrée d'un curseur
    def entry(self, catalog: str, schema: str) -> Optional[RegistryEntry]:
        """Return the stored cursor, ``None`` when absent."""
        return self.registry.get(self.unit(catalog, schema))

    # Snapshot d'un curseur
    def snapshot(self, catalog: str, schema: str) -> Optional[int]:
        """Return the snapshot of a cursor, ``None`` when absent."""
        entry = self.entry(catalog, schema)
        value = entry.extra.get("snapshot") if entry is not None else None
        return int(value) if value is not None else None

    # Lecture des changements depuis le curseur
    def read(
        self,
        conn: Any,
        catalog: str,
        schema: str,
        *,
        fingerprint: str,
        key_columns: Sequence[str],
        until_snapshot: Optional[int],
        table: str = FACT_TABLE,
    ) -> CursorRead:
        """Read the changes of a table since the cursor, up to a pinned snapshot.

        Args:
            conn: Open connection, ``catalog`` attached.
            catalog: Catalog alias.
            schema: Schema read.
            fingerprint: Current fingerprint of the read
                (:func:`cursor_fingerprint`).
            key_columns: Columns whose changed values are returned.
            until_snapshot: Pinned snapshot (upper bound of the changes).
            table: Table read.

        Returns:
            The read; ``fallback`` is set when the changes cannot be known.
        """
        from statflows.storage.ducklake.tables import changes_since

        entry = self.entry(catalog, schema)
        since = self.snapshot(catalog, schema)
        if entry is None or since is None:
            return CursorRead(catalog, schema, None, until_snapshot, FALLBACK_ABSENT)
        if entry.extra.get("fingerprint") != fingerprint:
            # Logging
            logger.info(
                f"Curseur {self.step} sur {catalog}.{schema} : empreinte modifiée "
                f"ou invalidée, recalcul complet."
            )
            return CursorRead(catalog, schema, since, until_snapshot, FALLBACK_FINGERPRINT)
        try:
            changes = changes_since(
                conn, catalog, schema, since,
                table=table, key_columns=list(key_columns), until_snapshot=until_snapshot,
            )
        except ValueError as exc:
            # Logging
            logger.warning(
                f"Curseur {self.step} sur {catalog}.{schema} (snapshot {since}) "
                f"inutilisable, recalcul complet : {exc}"
            )
            return CursorRead(catalog, schema, since, until_snapshot, FALLBACK_UNAVAILABLE)
        return CursorRead(
            catalog, schema, since, changes.current_snapshot,
            changed_keys=frozenset(changes.changed_keys or ()),
            rows_changed=changes.rows_inserted + changes.rows_updated + changes.rows_deleted,
        )

    # Avancement d'un curseur (en mémoire jusqu'à save())
    def advance(
        self, read: CursorRead, *, fingerprint: str, now: Optional[datetime] = None
    ) -> None:
        """Move a cursor to the snapshot its changes were read up to.

        Must only be called once the touched units are flagged ``pending``
        and the unit registry is saved (second phase of the protocol).

        Args:
            read: The read whose ``target`` is recorded.
            fingerprint: Fingerprint recorded with the cursor.
            now: Recording instant (current UTC instant by default).
        """
        if read.target is None:
            return
        self.registry.upsert(
            RegistryEntry(
                unit=self.unit(read.catalog, read.schema),
                last_computed=now or utc_now(),
                fingerprints={},
                reason=read.fallback or "changes",
                extra={"snapshot": int(read.target), "fingerprint": fingerprint},
            )
        )

    # Invalidation des curseurs de l'étape (recalcul complet au run suivant)
    def invalidate(self, schemas: Optional[Iterable[str]] = None) -> List[Unit]:
        """Invalidate the cursors of the step (fallback ``fingerprint`` at the next run).

        The snapshot is kept (the maintenance keeps honouring it), only the
        fingerprint is replaced by a value no read ever computes.

        Args:
            schemas: Schemas whose cursor is invalidated; ``None`` for all.

        Returns:
            The cursors invalidated (in memory until :meth:`save`).
        """
        wanted = set(schemas) if schemas is not None else None
        invalidated: List[Unit] = []
        for entry in list(self.registry.iter_entries()):
            if entry.unit.get("step") != self.step:
                continue
            if wanted is not None and entry.unit.get("schema") not in wanted:
                continue
            self.registry.upsert(replace(entry, extra={**entry.extra, "fingerprint": INVALIDATED}))
            invalidated.append(entry.unit)
        return sorted(invalidated)

    # Écriture des curseurs
    def save(self) -> List[str]:
        """Write the cursors modified since the last call.

        Returns:
            Paths written.
        """
        return self.registry.save()


# Fonction de lecture du plus ancien curseur de chaque catalogue
def oldest_cursors(registry: FreshnessRegistry) -> Dict[str, int]:
    """Return, per catalog, the smallest snapshot a cursor still needs.

    Args:
        registry: Registry of the cursors of every step.

    Returns:
        Mapping ``catalog alias -> smallest cursor snapshot``.
    """
    oldest: Dict[str, int] = {}
    for entry in registry.iter_entries():
        catalog, value = entry.unit.get("catalog"), entry.extra.get("snapshot")
        if catalog is None or value is None:
            continue
        oldest[catalog] = min(int(value), oldest.get(catalog, int(value)))
    return oldest


# Modes de décision d'une étape (bascule progressive vers les curseurs)
DECISION_MODES = ("legacy", "shadow", "snapshot")


# Fonction de lecture du mode de décision d'une étape
def decision_mode(state_config: Optional[Mapping[str, Any]], default: str = "legacy") -> str:
    """Read the ``STATE.DECISION_MODE`` of a step.

    - ``legacy``: the decision of the registries only (behaviour before the
      cursors);
    - ``shadow``: the legacy decision is executed, the new one is computed and
      logged (``freshness/shadow_only_legacy``, ``freshness/shadow_only_new``);
    - ``snapshot``: the new decision is executed.

    Args:
        state_config: The ``STATE`` block of the step.
        default: Mode when the block sets none.

    Returns:
        The mode.

    Raises:
        ValueError: If the mode is unknown.

    Examples:
        >>> decision_mode({"DECISION_MODE": "shadow"}), decision_mode(None)
        ('shadow', 'legacy')
    """
    mode = str((state_config or {}).get("DECISION_MODE") or default).lower()
    if mode not in DECISION_MODES:
        raise ValueError(f"STATE.DECISION_MODE must be one of {DECISION_MODES}, got {mode!r}")
    return mode


# Fonction de comparaison des décisions en mode observation
def shadow_metrics(legacy: Iterable[Unit], new: Iterable[Unit]) -> Dict[str, float]:
    """Compare the legacy and new decisions (``shadow`` mode).

    A unit planned by the new rule only is a change the legacy rule missed
    (false negative); a unit planned by the legacy rule only is a useless
    recomputation (false positive).

    Args:
        legacy: Units planned by the legacy rule.
        new: Units planned by the new rule.

    Returns:
        ``freshness/shadow_only_legacy``, ``freshness/shadow_only_new``,
        ``freshness/shadow_both``.

    Examples:
        >>> shadow_metrics([Unit.of(v="a"), Unit.of(v="b")], [Unit.of(v="b"), Unit.of(v="c")])
        {'freshness/shadow_only_legacy': 1.0, 'freshness/shadow_only_new': 1.0, 'freshness/shadow_both': 1.0}
    """
    legacy_set, new_set = set(legacy), set(new)
    return {
        "freshness/shadow_only_legacy": float(len(legacy_set - new_set)),
        "freshness/shadow_only_new": float(len(new_set - legacy_set)),
        "freshness/shadow_both": float(len(legacy_set & new_set)),
    }
