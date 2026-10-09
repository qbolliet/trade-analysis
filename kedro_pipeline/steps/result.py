"""Outcome of a pipeline step and the run contract shared by scripts and Kedro nodes.

Every step function of :mod:`kedro_pipeline.steps` returns a :class:`StepResult`:
the units planned and succeeded, the failed units (each failure isolated: one
unit never stops the others), the metrics and artifacts logged while the step
ran, and the exception to raise once everything was persisted and reported. It
is the single input of the run report, built the same way by the scripts and by
the Kedro nodes.

Some steps open one tracked run per unit of work (one per pass of the partner
metrics, one per HS vintage of BACI and of the network metrics): they receive a
:class:`UnitRuns` factory instead of a single tracker. The scripts give a factory
opening one MLflow run per unit; :func:`shared_runs` keeps every unit in the
caller's run.

No environment variable and no YAML path are read here.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from typing import Any, Callable, ContextManager, Dict, Iterator, Mapping, Optional, Protocol

# Modules de manipulation de données
import pandas as pd

# Modules du package
from macroforecast.tracking import CapturingTracker, NULL_TRACKER, RunTracker

# Longueur maximale d'un message d'échec conservé par unité
MAX_FAILURE_CHARS = 1000


# Classe de résultat d'une étape
@dataclass
class StepResult:
    """Outcome of one pipeline step, consumed by the run report and the callers.

    Args:
        step: Step name; selects the layout of the run report (``"download"``,
            ``"baci"``, ``"partners"``, ``"network"``, ``"synthesis"``,
            ``"coherence"``, ``"serving"``, ``"reference"``, ``"coverage"``).
        n_units_planned: Units of work planned by this execution.
        n_units_succeeded: Units computed and written.
        failures: Failed unit -> message (truncated). A non-empty mapping makes
            the step fail once :meth:`raise_if_failed` is called.
        metrics: Metrics logged by the step, slash-separated names.
        artifacts: Artifact path -> table logged by the step (sources of the
            report sections).
        tags: Tags set by the step.
        n_units_failed: Units in error when it differs from ``len(failures)``
            (a download tolerates failed queries below a configured share).
        units_label: Text shown instead of the planned count in the report.
        report_tables: Tables attached to the report under ``tables/``.
        reportable: Whether the run carries a report; false when nothing was
            stale, so that an idle run does not trip the "units computed" checks.
        outputs: Step-specific values (publication mode, rows per table, plans,
            computation reports…), also readable by key.
        children: Result of every tracked unit run, keyed by unit label.
        failure_exception: Exception raised by :meth:`raise_if_failed`;
            a generic ``RuntimeError`` listing the failed units when ``None``.

    Examples:
        >>> result = StepResult("serving", 2, 2, outputs={"mode": "full"})
        >>> result["mode"], result["failures"]
        ('full', {})
        >>> result.raise_if_failed()
    """

    step: str
    n_units_planned: int = 0
    n_units_succeeded: int = 0
    failures: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    artifacts: Dict[str, pd.DataFrame] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    n_units_failed: Optional[int] = None
    units_label: Optional[str] = None
    report_tables: Dict[str, pd.DataFrame] = field(default_factory=dict)
    reportable: bool = True
    outputs: Dict[str, Any] = field(default_factory=dict)
    children: Dict[str, "StepResult"] = field(default_factory=dict)
    failure_exception: Optional[BaseException] = field(default=None, repr=False)

    # Propriété : nombre d'unités en échec
    @property
    def n_failed(self) -> int:
        """Units in error (``n_units_failed`` when set, else ``len(failures)``)."""
        return len(self.failures) if self.n_units_failed is None else int(self.n_units_failed)

    # Accès par clé : champs puis valeurs propres à l'étape
    def __getitem__(self, key: str) -> Any:
        """Read a field, else a step-specific output, by name.

        Kept for the callers that read a step outcome as a mapping
        (``result["failures"]``, ``result["rows"]``).

        Args:
            key: Field name or key of :attr:`outputs`.

        Returns:
            The value.

        Raises:
            KeyError: If ``key`` is neither a field nor an output.
        """
        if key in _FIELD_NAMES:
            return getattr(self, key)
        return self.outputs[key]

    # Levée de l'échec, une fois tout persisté et publié
    def raise_if_failed(self) -> None:
        """Raise when at least one unit failed.

        Called by the caller after the registries were saved and the run report
        published, so that a failed run carries its report too.

        Raises:
            BaseException: :attr:`failure_exception` when set, else a
                ``RuntimeError`` naming the failed units.

        Examples:
            >>> StepResult("x", 2, 1, failures={"a": "boom"}).raise_if_failed()
            Traceback (most recent call last):
            ...
            RuntimeError: x: 1 unit(s) failed out of 2: ['a']
        """
        if not self.failures:
            return
        if self.failure_exception is not None:
            raise self.failure_exception
        raise RuntimeError(
            f"{self.step}: {len(self.failures)} unit(s) failed out of "
            f"{self.n_units_planned}: {sorted(self.failures)}"
        )


# Noms des champs lisibles par clé
_FIELD_NAMES = frozenset(f.name for f in fields(StepResult))


# Fonction de mise en forme du message d'échec d'une unité
def failure_message(exc: BaseException) -> str:
    """Return the message kept for a failed unit: ``"<Type>: <message>"``, truncated.

    Args:
        exc: Exception of the unit.

    Returns:
        The truncated message.

    Examples:
        >>> failure_message(ValueError("gravité impossible"))
        'ValueError: gravité impossible'
    """
    return f"{type(exc).__name__}: {exc}"[:MAX_FAILURE_CHARS]


# Fonction d'enveloppe d'enregistrement d'un tracker
def capturing(tracker: Optional[RunTracker]) -> CapturingTracker:
    """Wrap a tracker so that the step can return what it logged.

    The wrapper forwards every call unchanged and is never entered by the step:
    the caller owns the lifecycle of its run.

    Args:
        tracker: Tracker of the open run; the null tracker when ``None``.

    Returns:
        A capturing tracker forwarding to ``tracker``.
    """
    return CapturingTracker(NULL_TRACKER if tracker is None else tracker)


# Protocole de suivi de l'avancement (dernière étape atteinte)
class StepProgress(Protocol):
    """Object whose ``step`` attribute names the last stage reached by a step.

    The run report of a failure names that stage. ``scripts._run_report.RunScope``
    satisfies this protocol.
    """

    step: Optional[str]


# Fonction de mise à jour de l'avancement
def mark(progress: Optional[StepProgress], stage: str) -> None:
    """Record the stage reached on ``progress`` when one is given.

    Args:
        progress: Progress holder, or ``None``.
        stage: Stage name (``"redressement BACI"``…).

    Examples:
        >>> mark(None, "x")
    """
    if progress is not None:
        progress.step = stage


# Run d'une unité de travail
@dataclass
class UnitRun:
    """Tracked run of one unit of work.

    Args:
        tracker: Tracker of the open run.
        progress: Progress holder named by the failure report, or ``None``.
        publish: Publishes the report of the unit from its result, inside the
            run (no-op when the caller reports later).
    """

    tracker: RunTracker
    progress: Optional[StepProgress] = None
    publish: Callable[[StepResult], None] = lambda result: None


# Protocole de fabrique des runs d'unités
class UnitRuns(Protocol):
    """Factory of the tracked run of one unit (pass, vintage).

    Called with the unit label and the tags of the run, it returns a context
    manager yielding a :class:`UnitRun`. An exception raised inside the block
    crosses it, so that the run ends failed with its reduced report; the step
    catches it afterwards and goes on with the next unit.
    """

    def __call__(self, label: str, tags: Mapping[str, str]) -> ContextManager[UnitRun]:
        ...


# Fabrique par défaut : toutes les unités dans le run de l'appelant
def shared_runs(tracker: Optional[RunTracker] = None) -> UnitRuns:
    """Return a factory keeping every unit in the caller's run, without reports.

    Args:
        tracker: Tracker of the caller's open run; the null tracker by default.

    Returns:
        The factory.

    Examples:
        >>> runs = shared_runs()
        >>> with runs("HS2017", {"vintage": "HS2017"}) as run:
        ...     run.publish(StepResult("x"))
    """
    target = NULL_TRACKER if tracker is None else tracker

    @contextmanager
    def runs(label: str, tags: Mapping[str, str]) -> Iterator[UnitRun]:
        # Run de l'appelant, ni ouvert ni fermé ici
        yield UnitRun(tracker=target)

    return runs
