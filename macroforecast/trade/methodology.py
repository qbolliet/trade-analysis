"""Methodological parameters of a configuration dataclass, as a plain mapping.

The incremental pipeline decides whether a result is stale by comparing, for
each metric or step, a digest of its *methodology*: its name plus the
parameters that influence the values written. This module turns a frozen
configuration dataclass into that parameter mapping, leaving out the fields
declared as irrelevant to the written values (logging, diagnostics and
artifact options).

Pure and dependency-free (standard library only): every family of the package
(partner and network vulnerabilities, BACI reconstruction) declares its own
exclusion list next to its configuration dataclass and calls
:func:`methodology_params`.
"""
# Importation des modules
# Modules de base
from dataclasses import fields, is_dataclass
from typing import Any, Collection, Dict, Mapping


# Fonction de normalisation d'une valeur de configuration en valeur JSON stable
def _normalise(value: Any) -> Any:
    """Turn a configuration value into a JSON-friendly, order-stable value.

    Nested dataclasses become dictionaries of their fields, tuples, lists and
    sets become lists (sets sorted), and mapping keys are converted to text and
    sorted so that ``{8: 1e-3}`` and ``{"8": 1e-3}`` digest alike.

    Args:
        value: Any configuration value.

    Returns:
        A value built only from dictionaries, lists and scalars.

    Examples:
        >>> _normalise({21: 1.0, 8: 0.001})
        {'21': 1.0, '8': 0.001}
        >>> _normalise((("HHI", 0.5), ("CDI2", 0.5)))
        [['HHI', 0.5], ['CDI2', 0.5]]
    """
    if is_dataclass(value) and not isinstance(value, type):
        return {f.name: _normalise(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _normalise(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (set, frozenset)):
        return sorted((_normalise(item) for item in value), key=repr)
    if isinstance(value, (list, tuple)):
        return [_normalise(item) for item in value]
    return value


# Fonction d'extraction des paramètres méthodologiques d'une configuration
def methodology_params(config: Any, excluded: Collection[str] = ()) -> Dict[str, Any]:
    """Return the fields of a configuration dataclass that shape the results.

    Args:
        config: Configuration dataclass instance (``VulnerabilityConfig``,
            ``NetworkVulnerabilityConfig``, ``BaciConfig``…).
        excluded: Names of the top-level fields left out, because they only
            drive logging, diagnostics or artifacts and never change a written
            value.

    Returns:
        Mapping ``field name -> normalised value``, sorted by field name, ready
        to be digested.

    Raises:
        TypeError: If ``config`` is not a dataclass instance.
        ValueError: If ``excluded`` names a field the dataclass does not have
            (a typo would otherwise silently put the field into the digest).

    Examples:
        >>> from dataclasses import dataclass
        >>> @dataclass(frozen=True)
        ... class Config:
        ...     threshold: float = 0.5
        ...     top_n: int = 50
        >>> methodology_params(Config(), excluded={"top_n"})
        {'threshold': 0.5}
    """
    # Vérification des arguments
    if not is_dataclass(config) or isinstance(config, type):
        raise TypeError(f"config must be a dataclass instance, got {type(config).__name__}")
    names = {f.name for f in fields(config)}
    unknown = set(excluded) - names
    if unknown:
        raise ValueError(f"Unknown excluded field(s) for {type(config).__name__}: {sorted(unknown)}")

    return {
        name: _normalise(getattr(config, name))
        for name in sorted(names)
        if name not in excluded
    }
