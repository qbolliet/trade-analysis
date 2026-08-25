# Importation des modules
# Modules de base
from __future__ import annotations
from dataclasses import fields, is_dataclass
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
def _walk(payload: Any, prefix: str) -> Dict[str, Any]:
    """Flatten a dataclass, mapping or scalar into dotted keys.

    Args:
        payload: Dataclass instance, mapping or scalar to flatten.
        prefix: Key prefix already accumulated (may be empty).

    Returns:
        Mapping of dotted keys to leaf values.
    """
    # Rapport structuré : parcours de ses champs
    if is_dataclass(payload) and not isinstance(payload, type):
        out: Dict[str, Any] = {}
        for f in fields(payload):
            key = f"{prefix}.{f.name}" if prefix else f.name
            out.update(_walk(getattr(payload, f.name), key))
        return out
    # Dictionnaire : parcours de ses entrées
    if isinstance(payload, Mapping):
        out = {}
        for name, value in payload.items():
            key = f"{prefix}.{name}" if prefix else str(name)
            out.update(_walk(value, key))
        return out
    # Feuille
    return {prefix: payload}


# Fonction d'aplatissement d'un rapport en métriques
def flatten_metrics(payload: Any, prefix: str = "") -> Dict[str, float]:
    """Flatten every finite numeric field of a report into dotted metric keys.

    Walks dataclasses and mappings recursively. Booleans are cast to ``0``/``1``;
    strings, ``None``, sequences and pandas objects are dropped, as are ``NaN``
    and infinities — MLflow rejects them.

    Args:
        payload: Report (dataclass instance) or mapping to flatten.
        prefix: Prefix prepended to every key, e.g. ``"baci"``.

    Returns:
        Mapping of dotted metric names to finite floats.

    Examples:
        >>> from dataclasses import dataclass
        >>> @dataclass
        ... class Report:
        ...     n: int = 3
        ...     share: float = float("nan")
        >>> flatten_metrics(Report(), prefix="step")
        {'step.n': 3.0}
    """
    metrics: Dict[str, float] = {}
    for key, value in _walk(payload, prefix).items():
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
