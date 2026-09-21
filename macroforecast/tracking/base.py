# Importation des modules
# Modules de base
from __future__ import annotations
from dataclasses import fields, is_dataclass
from importlib import metadata
import logging
import math
import re
from typing import Any, Dict, Mapping, Optional, Protocol
# Modules de manipulation des données
import pandas as pd

# Initialisation du logger
logger = logging.getLogger(__name__)

# Caractères admis par MLflow dans un nom de métrique, de paramètre ou de tag
_FORBIDDEN_KEY_CHARS = re.compile(r"[^0-9a-zA-Z_\-./ :]")
# Longueur maximale d'une valeur de paramètre acceptée par MLflow
_MAX_PARAM_LENGTH = 500


# ──────────────────────────────────────────────────────────────────────
# Protocole de suivi d'exécution
# ──────────────────────────────────────────────────────────────────────

# Surface minimale de suivi attendue par les pipelines
class RunTracker(Protocol):
    """Minimal experiment-tracking surface used by the pipelines.

    Deliberately kept to the operations ``kedro-mlflow`` itself relies on, so
    that a Kedro-backed implementation can later be substituted without
    touching a single caller. Implementations must never let a tracking
    failure interrupt a computation: an unreachable server, a missing
    experiment or a non-finite metric warn and carry on.
    """

    def log_params(self, params: Mapping[str, Any]) -> None:
        """Record the parameters of the run."""
        ...

    def log_metrics(self, metrics: Mapping[str, float], step: Optional[int] = None) -> None:
        """Record numeric metrics, optionally at a given step."""
        ...

    def log_dict(self, obj: Mapping[str, Any], artifact_file: str) -> None:
        """Record a mapping as a JSON/YAML artifact."""
        ...

    def log_table(self, df_table: pd.DataFrame, artifact_file: str) -> None:
        """Record a table as a CSV artifact."""
        ...

    def log_text(self, text: str, artifact_file: str) -> None:
        """Record a text (Markdown, HTML…) as an artifact."""
        ...

    def set_tags(self, tags: Mapping[str, str]) -> None:
        """Attach tags to the run."""
        ...

    def __enter__(self) -> "RunTracker":
        """Open the run."""
        ...

    def __exit__(self, *exc: Any) -> None:
        """Close the run."""
        ...


# ──────────────────────────────────────────────────────────────────────
# Objet nul — comportement par défaut sans suivi
# ──────────────────────────────────────────────────────────────────────

# Implémentation inerte du protocole (patron « objet nul »)
class NullTracker:
    """No-op :class:`RunTracker`: every call is discarded.

    Being the default of every caller, it removes the need for
    ``if tracker is not None`` guards throughout the pipelines. The context
    manager never swallows an exception: a business error raised inside a
    ``with`` block propagates untouched.

    Examples:
        >>> with NullTracker() as tracker:
        ...     tracker.log_metrics({"share": 0.5})
        >>> NULL_TRACKER.log_params({"apply_nes": True})
    """
    
    def log_params(self, params: Mapping[str, Any]) -> None:
        """Discard the parameters.

        Args:
            params: Ignored.
        """

    def log_metrics(self, metrics: Mapping[str, float], step: Optional[int] = None) -> None:
        """Discard the metrics.

        Args:
            metrics: Ignored.
            step: Ignored.
        """

    def log_dict(self, obj: Mapping[str, Any], artifact_file: str) -> None:
        """Discard the mapping.

        Args:
            obj: Ignored.
            artifact_file: Ignored.
        """

    def log_table(self, df_table: pd.DataFrame, artifact_file: str) -> None:
        """Discard the table.

        Args:
            df_table: Ignored.
            artifact_file: Ignored.
        """

    def log_text(self, text: str, artifact_file: str) -> None:
        """Discard the text.

        Args:
            text: Ignored.
            artifact_file: Ignored.
        """

    def set_tags(self, tags: Mapping[str, str]) -> None:
        """Discard the tags.

        Args:
            tags: Ignored.
        """

    def __enter__(self) -> "NullTracker":
        """Return ``self``, no run being opened.

        Returns:
            The tracker itself.
        """
        return self

    def __exit__(self, *exc: Any) -> None:
        """Close nothing and never swallow an exception.

        Args:
            *exc: Exception triple, ignored.
        """


# Instance partagée : défaut de tous les appelants
NULL_TRACKER: RunTracker = NullTracker()


# ──────────────────────────────────────────────────────────────────────
# Décorateur d'enregistrement — source des rapports de run
# ──────────────────────────────────────────────────────────────────────

# Tracker transmettant tout à un tracker interne tout en le mémorisant
class CapturingTracker:
    """:class:`RunTracker` decorator remembering everything it forwards.

    The pipeline steps log their metrics and artifact tables *while* they run.
    Wrapping the tracker handed to a step lets the caller rebuild the run
    report (checks, figures, tables) from what was already produced, without
    reading any data back. The wrapped tracker receives every call unchanged.

    Args:
        inner: Tracker the calls are forwarded to; the null tracker by default.

    Attributes:
        metrics: Last value of every metric logged so far (all steps merged).
        params: Parameters logged so far.
        tags: Tags logged so far.
        tables: Artifact path -> table, for every :meth:`log_table` call.
        dicts: Artifact path -> mapping, for every :meth:`log_dict` call.
        texts: Artifact path -> text, for every :meth:`log_text` call.

    Examples:
        >>> tracker = CapturingTracker()
        >>> with tracker:
        ...     tracker.log_metrics({"gravity/r_squared": 0.7})
        ...     tracker.set_tags({"vintage": "HS2017"})
        >>> tracker.metrics, tracker.tags
        ({'gravity/r_squared': 0.7}, {'vintage': 'HS2017'})
    """

    # Initialisation
    def __init__(self, inner: Optional["RunTracker"] = None) -> None:
        # Stockage tel quel (convention sklearn) puis état d'enregistrement
        self.inner = inner
        self.metrics: Dict[str, float] = {}
        self.params: Dict[str, Any] = {}
        self.tags: Dict[str, str] = {}
        self.tables: Dict[str, pd.DataFrame] = {}
        self.dicts: Dict[str, Dict[str, Any]] = {}
        self.texts: Dict[str, str] = {}

    # Tracker effectif : l'objet nul en l'absence de tracker interne
    @property
    def _target(self) -> "RunTracker":
        return self.inner if self.inner is not None else NULL_TRACKER

    def log_params(self, params: Mapping[str, Any]) -> None:
        """Forward and remember the parameters.

        Args:
            params: Mapping of parameter names to values.
        """
        self.params.update(params)
        self._target.log_params(params)

    def log_metrics(self, metrics: Mapping[str, float], step: Optional[int] = None) -> None:
        """Forward and remember the metrics.

        Args:
            metrics: Mapping of metric names to values.
            step: Optional step index.
        """
        self.metrics.update(metrics)
        self._target.log_metrics(metrics, step=step)

    def log_dict(self, obj: Mapping[str, Any], artifact_file: str) -> None:
        """Forward and remember a mapping artifact.

        Args:
            obj: Mapping to serialise.
            artifact_file: Artifact path.
        """
        self.dicts[artifact_file] = dict(obj)
        self._target.log_dict(obj, artifact_file)

    def log_table(self, df_table: pd.DataFrame, artifact_file: str) -> None:
        """Forward and remember a table artifact.

        Args:
            df_table: Table to serialise.
            artifact_file: Artifact path.
        """
        self.tables[artifact_file] = df_table
        self._target.log_table(df_table, artifact_file)

    def log_text(self, text: str, artifact_file: str) -> None:
        """Forward and remember a text artifact.

        Args:
            text: Text to record.
            artifact_file: Artifact path.
        """
        self.texts[artifact_file] = text
        self._target.log_text(text, artifact_file)

    def set_tags(self, tags: Mapping[str, str]) -> None:
        """Forward and remember the tags.

        Args:
            tags: Mapping of tag names to values.
        """
        self.tags.update(tags)
        self._target.set_tags(tags)

    def __enter__(self) -> "CapturingTracker":
        """Open the inner run.

        Returns:
            The capturing tracker itself.
        """
        self._target.__enter__()
        return self

    def __exit__(self, *exc: Any) -> None:
        """Close the inner run, propagating the exception triple.

        Args:
            *exc: Exception triple of the ``with`` block.
        """
        self._target.__exit__(*exc)


# ──────────────────────────────────────────────────────────────────────
# Mise en forme des rapports structurés
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : assainissement d'une clé
def _clean_key(key: str) -> str:
    """Replace the characters MLflow rejects in a key.

    Args:
        key: Raw key, possibly holding brackets or accents.

    Returns:
        Key restricted to alphanumerics and ``_-./ :``.

    Examples:
        >>> _clean_key("gravity.year[2020]")
        'gravity.year_2020_'
    """
    return _FORBIDDEN_KEY_CHARS.sub("_", key)


# Fonction auxiliaire : parcours récursif d'un rapport
def _walk(payload: Any, prefix: str, sep: str = ".") -> Dict[str, Any]:
    """Flatten a dataclass, mapping or scalar into ``sep``-joined keys.

    Args:
        payload: Dataclass instance, mapping or scalar to flatten.
        prefix: Key prefix already accumulated (may be empty).
        sep: Separator joining the levels of a key.

    Returns:
        Mapping of dotted keys to leaf values.
    """
    # Rapport structuré : parcours de ses champs
    if is_dataclass(payload) and not isinstance(payload, type):
        out: Dict[str, Any] = {}
        for f in fields(payload):
            key = f"{prefix}{sep}{f.name}" if prefix else f.name
            out.update(_walk(getattr(payload, f.name), key, sep))
        return out
    # Dictionnaire : parcours de ses entrées
    if isinstance(payload, Mapping):
        out = {}
        for name, value in payload.items():
            key = f"{prefix}{sep}{name}" if prefix else str(name)
            out.update(_walk(value, key, sep))
        return out
    # Feuille
    return {prefix: payload}


# Fonction d'aplatissement d'un rapport en métriques
def flatten_metrics(payload: Any, prefix: str = "", sep: str = ".") -> Dict[str, float]:
    """Flatten every finite numeric field of a report into ``sep``-joined metric keys.

    Walks dataclasses and mappings recursively. Booleans are cast to ``0``/``1``;
    strings, ``None``, sequences and pandas objects are dropped, as are ``NaN``
    and infinities — MLflow rejects them.

    Args:
        payload: Report (dataclass instance) or mapping to flatten.
        prefix: Prefix prepended to every key, e.g. ``"baci"``.
        sep: Separator joining the levels of a key. ``"/"`` makes the MLflow
            interface group the charts by section.

    Returns:
        Mapping of metric names to finite floats.

    Examples:
        >>> from dataclasses import dataclass
        >>> @dataclass
        ... class Report:
        ...     n: int = 3
        ...     share: float = float("nan")
        >>> flatten_metrics(Report(), prefix="step")
        {'step.n': 3.0}
        >>> flatten_metrics(Report(), prefix="step", sep="/")
        {'step/n': 3.0}
    """
    metrics: Dict[str, float] = {}
    for key, value in _walk(payload, prefix, sep).items():
        # Exclusion des types non numériques (les booléens sont des entiers)
        if isinstance(value, bool):
            value = int(value)
        elif not isinstance(value, (int, float)):
            continue
        # Exclusion des valeurs non finies, rejetées par MLflow
        numeric = float(value)
        if not math.isfinite(numeric):
            continue
        metrics[_clean_key(key)] = numeric
    return metrics


# Fonction de changement de séparateur des clés de métriques
def rekey_metrics(metrics: Mapping[str, float], sep: str = "/") -> Dict[str, float]:
    """Replace the dots of dotted metric keys by another separator.

    The report classes emit dotted keys (``baci.gravity.r_squared``); the
    MLflow interface only groups the charts by section when levels are
    separated by ``/`` (ARCH C-19, PD-13). Rekeying at the call site leaves the
    ``to_metrics`` methods, and their tests, untouched.

    Args:
        metrics: Mapping of dotted metric names to values.
        sep: Separator replacing the dots.

    Returns:
        New mapping with the rekeyed names, values unchanged.

    Examples:
        >>> rekey_metrics({"baci.gravity.r_squared": 0.7, "coverage/share_min": 1.0})
        {'baci/gravity/r_squared': 0.7, 'coverage/share_min': 1.0}
    """
    return {str(name).replace(".", sep): value for name, value in metrics.items()}


# Fonction d'aplatissement d'une configuration en paramètres
def flatten_params(payload: Any, prefix: str = "") -> Dict[str, str]:
    """Flatten a configuration into dotted, string-valued parameter keys.

    Unlike :func:`flatten_metrics`, every leaf is kept and rendered as text —
    MLflow stores parameters as strings — and truncated to the accepted length.

    Args:
        payload: Configuration (dataclass instance) or mapping to flatten.
        prefix: Prefix prepended to every key, e.g. ``"config"``.

    Returns:
        Mapping of dotted parameter names to their string representation.

    Examples:
        >>> flatten_params({"fas_countries": ("CAN",), "cook_factor": 4.0})
        {'fas_countries': "('CAN',)", 'cook_factor': '4.0'}
    """
    params: Dict[str, str] = {}
    for key, value in _walk(payload, prefix).items():
        params[_clean_key(key)] = str(value)[:_MAX_PARAM_LENGTH]
    return params


# Fonction d'assemblage des paramètres décrivant une exécution
def run_params(
    config: Any,
    context: Optional[Mapping[str, Any]] = None,
    *,
    prefix: str = "config",
    package: str = "macroforecast",
) -> Dict[str, str]:
    """Assemble the parameters describing a pipeline run.

    Every run of the pipeline logs the same three things: its flattened
    methodological configuration, the contextual facts of that particular run
    (schemas, perimeter, effective options) and the version of the package that
    produced it. This helper holds that shape once, so each step only supplies
    its own context.

    Args:
        config: Run configuration (dataclass instance or mapping) to flatten
            under ``prefix``.
        context: Contextual facts of the run, flattened without prefix. ``None``
            logs the configuration and the version alone.
        prefix: Prefix given to the configuration keys.
        package: Distribution whose version is recorded as
            ``macroforecast_version``. Absent from a non-installed source tree,
            in which case ``"unknown"`` is recorded.

    Returns:
        Flat mapping of dotted parameter names to their string representation.

    Examples:
        >>> params = run_params(
        ...     {"cook_factor": 4.0}, {"n_rows": 7}, package="not-installed")
        >>> params["config.cook_factor"], params["n_rows"]
        ('4.0', '7')
        >>> params["macroforecast_version"]
        'unknown'
    """
    # Version du paquet : absente d'une arborescence non installée
    try:
        version = metadata.version(package)
    except Exception:  # pragma: no cover - dépend de l'installation
        version = "unknown"

    # Configuration méthodologique aplatie
    params = flatten_params(config, prefix=prefix)
    # Contexte de l'exécution, complété de la version du paquet
    params.update(
        flatten_params({**dict(context or {}), "macroforecast_version": version})
    )
    return params
