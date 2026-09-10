"""Declarative method layer: from a YAML entry to a fit-ready pipeline.

The configuration files list methods as mappings (``kind``, ``params``,
``normalization``, ...); the runner of
:mod:`~macroforecast.trade.aggregation.synthesis` needs ``sklearn`` estimators.
This module is the single translation point:

* :class:`MethodSpec` -- one configured method, frozen (S-1.2);
* :data:`METHOD_REGISTRY` -- table S-1.4 of the architecture note, mapping a
  ``kind`` to its estimator factory, its admissible normalisation schemes, its
  default group size, and whether it defines an alert or a fitted state;
* :func:`build_method` -- assembly of the ``orient -> winsorize -> scale ->
  estimate`` pipeline, with validation of the requested normalisation;
* :func:`method_spec_from_mapping` -- YAML mapping to :class:`MethodSpec`.

No method is hardcoded anywhere else: adding one means adding a registry entry.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field, fields
import logging
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, TYPE_CHECKING
# Modules de manipulation de données
import numpy as np
from sklearn.base import BaseEstimator
from sklearn.pipeline import Pipeline
# Modules du package
from .estimators import (
    BenefitOfDoubtScorer,
    ConeQuantileScorer,
    PcaProjectionScorer,
    SmaaScorer,
    WeightedAggregator,
)
from .pareto import MetricReducer, ParetoScorer
from .preprocessing import PolarityOrienter, Winsorizer, make_normalizer

# Typage seulement : `synthesis` importe ce module, l'import serait circulaire
if TYPE_CHECKING:  # pragma: no cover
    from .synthesis import SynthesisConfig

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom conventionnel de l'absence de normalisation (étape `passthrough` du pipeline)
NO_NORMALIZATION = "none"

# Étapes du pipeline d'une méthode, dans l'ordre imposé par S-1.4
PIPELINE_STEPS: Tuple[str, ...] = ("orient", "winsorize", "scale", "estimate")


# ──────────────────────────────────────────────────────────────────────
# Spécification d'une méthode configurée (S-1.2)
# ──────────────────────────────────────────────────────────────────────

# Description gelée d'une méthode telle qu'elle est écrite dans la configuration
@dataclass(frozen=True)
class MethodSpec:
    """One configured method, as a frozen record (S-1.2).

    Attributes:
        name: Value written in the ``method`` column of the score table; also
            the key used by ``bootstrap_methods`` and by the artifacts.
        kind: Key of :data:`METHOD_REGISTRY` (table S-1.4).
        metrics: Subset of the metric columns the method is fitted on;
            ``None`` means every metric of the configuration.
        normalization: Overrides the default normalisation of the ``kind``
            (D-13); ``None`` resolves to the configuration default when that
            one is admissible, and to the default of the ``kind`` otherwise.
        params: Extra keyword arguments forwarded to the estimator factory.
        min_group_size: Overrides the default minimum group size of the
            ``kind``.
        levels: Levels the method is applied at; ``None`` means every level of
            the configuration.

    Examples:
        >>> spec = MethodSpec(name="critic_sum", kind="weighted",
        ...                   params={"weighting": "critic"})
        >>> spec.name, spec.kind
        ('critic_sum', 'weighted')
    """
    name: str
    kind: str
    metrics: Optional[Tuple[str, ...]] = None
    normalization: Optional[str] = None
    params: Mapping[str, Any] = field(default_factory=dict)
    min_group_size: Optional[int] = None
    levels: Optional[Tuple[str, ...]] = None


# Entrée du registre : tout ce que le runner doit savoir d'un `kind`
@dataclass(frozen=True)
class MethodRegistryEntry:
    """Everything the runner needs to know about one ``kind`` (table S-1.4).

    Attributes:
        factory: Callable turning the ``params`` of a :class:`MethodSpec` into
            a fresh, unfitted estimator exposing ``fit`` and ``predict``.
        normalizations: Admissible normalisation schemes, the first being the
            default of the ``kind`` (in bold in table S-1.4).
        min_group_size: Default minimum number of scored rows below which the
            method is skipped.
        supports_alert: Whether the estimator exposes ``alert(X) -> bool[n]``.
        has_fitted_state: Whether the method estimates something on the group
            (weights, covariance, transport map) -- the condition for the
            bootstrap of D-08 to have any meaning.

    Examples:
        >>> METHOD_REGISTRY["mpi"].default_normalization
        'minmax'
    """
    factory: Callable[..., BaseEstimator]
    normalizations: Tuple[str, ...]
    min_group_size: int
    supports_alert: bool = False
    has_fitted_state: bool = False

    # Normalisation par défaut : première entrée admissible
    @property
    def default_normalization(self) -> str:
        """Default normalisation scheme of the ``kind``.

        Returns:
            The first admissible scheme of :attr:`normalizations`.
        """
        return self.normalizations[0]


# ──────────────────────────────────────────────────────────────────────
# Fabriques d'estimateurs (écart entre la forme YAML et les signatures)
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction du réducteur de métriques d'un `ParetoScorer`
def _make_reducer(value: Any) -> Optional[MetricReducer]:
    """Turn the YAML ``reduce`` entry into a :class:`MetricReducer`.

    Args:
        value: ``None`` / ``False`` (no reduction), ``True`` (default
            reducer), a mapping of constructor arguments, or an
            already-built :class:`MetricReducer`.

    Returns:
        The reducer, or ``None`` when the reduction is disabled.

    Raises:
        TypeError: If ``value`` is of no admissible type.

    Examples:
        >>> _make_reducer({"threshold": 0.3}).threshold
        0.3
        >>> _make_reducer(None) is None
        True
    """
    if value is None or value is False:
        return None
    if value is True:
        return MetricReducer()
    if isinstance(value, MetricReducer):
        return value
    if isinstance(value, Mapping):
        return MetricReducer(**value)
    raise TypeError(
        "`reduce` must be None, a boolean, a mapping or a MetricReducer, got "
        f"{type(value).__name__}."
    )


# Fabrique du scoreur de dominance de Pareto
def _build_pareto(**params: Any) -> ParetoScorer:
    """Build a :class:`~macroforecast.trade.aggregation.pareto.ParetoScorer`.

    Args:
        **params: Constructor arguments, with the YAML aliases ``layers``
            (for ``compute_layers``) and ``reduce`` (mapping converted to a
            :class:`~macroforecast.trade.aggregation.pareto.MetricReducer`).

    Returns:
        An unfitted scorer.

    Examples:
        >>> _build_pareto(epsilon=0.1, layers=False).compute_layers
        False
    """
    params = dict(params)
    if "layers" in params:
        params["compute_layers"] = params.pop("layers")
    params["reduce"] = _make_reducer(params.get("reduce"))
    return ParetoScorer(**params)


# Fabrique de la moyenne des rangs normalisés (A-02)
def _build_rank_mean(**params: Any) -> WeightedAggregator:
    """Build the rank-mean aggregator (equal weights on rank-scaled data).

    Args:
        **params: Optional ``weighting`` (default ``"equal"``),
            ``weighting_params`` and ``aggregation_params``.

    Returns:
        An unfitted
        :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`.

    Examples:
        >>> _build_rank_mean().weighting
        'equal'
    """
    params = dict(params)
    return WeightedAggregator(
        weighting=params.pop("weighting", "equal"),
        aggregation="weighted_sum",
        weighting_params=params.pop("weighting_params", None),
        aggregation_params=params.pop("aggregation_params", None),
        **params,
    )


# Fabrique générique de l'agrégateur pondéré
def _build_weighted(**params: Any) -> WeightedAggregator:
    """Build a weighting + aggregation estimator from its YAML parameters.

    Args:
        **params: ``weighting``, ``aggregation``, ``weighting_params`` and
            ``aggregation_params``.

    Returns:
        An unfitted
        :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`.

    Examples:
        >>> _build_weighted(weighting="critic").aggregation
        'weighted_sum'
    """
    return WeightedAggregator(**params)


# Fabrique d'une agrégation à fonction imposée (TOPSIS, VIKOR, MPI, Mahalanobis...)
def _fixed_aggregation_factory(
    aggregation: str, *, weight_free: bool = False
) -> Callable[..., WeightedAggregator]:
    """Build the factory of a ``kind`` whose aggregation function is imposed.

    The YAML of S-2.2 writes the arguments of the aggregation function flat
    (``{weighting: "critic", v: 0.5}``); the factory routes the weighting keys
    to the weighting scheme and every other key to the aggregation function,
    so that a configuration file never has to mirror the internal signature of
    :class:`~macroforecast.trade.aggregation.estimators.WeightedAggregator`.

    Args:
        aggregation: Key of
            :data:`~macroforecast.trade.aggregation.estimators.AGGREGATION_REGISTRY`.
        weight_free: Whether the aggregation ignores the weights (MPI,
            Mahalanobis, whitened projection): the weighting is then forced to
            ``"none"``.

    Returns:
        A factory taking the ``params`` of a :class:`MethodSpec`.

    Examples:
        >>> _fixed_aggregation_factory("vikor")(weighting="critic", v=0.5).aggregation
        'vikor'
    """
    def factory(**params: Any) -> WeightedAggregator:
        """Build the estimator of the imposed aggregation.

        Args:
            **params: Weighting keys and aggregation-function keys, mixed.

        Returns:
            An unfitted ``WeightedAggregator``.
        """
        params = dict(params)
        weighting = "none" if weight_free else params.pop("weighting", "equal")
        weighting_params = params.pop("weighting_params", None)
        # Les clés restantes paramètrent la fonction d'agrégation elle-même
        aggregation_params = dict(params.pop("aggregation_params", None) or {})
        aggregation_params.update(params)
        return WeightedAggregator(
            weighting=weighting,
            aggregation=aggregation,
            weighting_params=weighting_params,
            aggregation_params=aggregation_params or None,
        )

    return factory


# Fabrique du score de Kantorovitch orienté (import paresseux de jax / ott-jax)
def _build_kantorovich(**params: Any) -> BaseEstimator:
    """Build the oriented Kantorovitch scorer, importing its module lazily.

    The module itself imports without ``jax``; the dependency is only required
    when ``fit`` solves the transport problem, so the runner catches
    ``ImportError`` around both construction and fitting.

    Args:
        **params: Constructor arguments, with the YAML alias ``alert`` for
            ``alert_score``.

    Returns:
        An unfitted
        :class:`~macroforecast.trade.aggregation.optimal_transport.OrientedKantorovichScorer`.

    Examples:
        >>> _build_kantorovich(alert="radial").alert_score
        'radial'
    """
    from .optimal_transport import OrientedKantorovichScorer

    params = dict(params)
    if "alert" in params:
        params["alert_score"] = params.pop("alert")
    return OrientedKantorovichScorer(**params)


# ──────────────────────────────────────────────────────────────────────
# Registre des méthodes (S-1.4)
# ──────────────────────────────────────────────────────────────────────

# Table S-1.4 : estimateur, normalisations admissibles, taille minimale, alerte, état
METHOD_REGISTRY: Mapping[str, MethodRegistryEntry] = MappingProxyType(
    {
        "pareto": MethodRegistryEntry(
            factory=_build_pareto,
            normalizations=(NO_NORMALIZATION,),
            min_group_size=2,
            supports_alert=True,
            has_fitted_state=False,
        ),
        "rank_mean": MethodRegistryEntry(
            factory=_build_rank_mean,
            normalizations=("rank", NO_NORMALIZATION),
            min_group_size=2,
        ),
        "weighted": MethodRegistryEntry(
            factory=_build_weighted,
            normalizations=("minmax", "rank"),
            min_group_size=30,
            has_fitted_state=True,
        ),
        "pca_projection": MethodRegistryEntry(
            factory=PcaProjectionScorer,
            normalizations=("standard", "quantile_gaussian"),
            min_group_size=30,
            has_fitted_state=True,
        ),
        "mpi": MethodRegistryEntry(
            factory=_fixed_aggregation_factory("mpi", weight_free=True),
            normalizations=("minmax", "standard", "robust"),
            min_group_size=3,
        ),
        "topsis": MethodRegistryEntry(
            factory=_fixed_aggregation_factory("topsis"),
            normalizations=("minmax", "rank"),
            min_group_size=30,
            has_fitted_state=True,
        ),
        "vikor": MethodRegistryEntry(
            factory=_fixed_aggregation_factory("vikor"),
            normalizations=("minmax", "rank"),
            min_group_size=30,
            has_fitted_state=True,
        ),
        "bod": MethodRegistryEntry(
            factory=BenefitOfDoubtScorer,
            normalizations=("minmax", "rank"),
            min_group_size=3,
            has_fitted_state=True,
        ),
        "mahalanobis": MethodRegistryEntry(
            factory=_fixed_aggregation_factory("mahalanobis", weight_free=True),
            normalizations=("standard", "quantile_gaussian"),
            min_group_size=30,
            has_fitted_state=True,
        ),
        "whitened_projection": MethodRegistryEntry(
            factory=_fixed_aggregation_factory(
                "whitened_projection", weight_free=True
            ),
            normalizations=("standard", "quantile_gaussian"),
            min_group_size=30,
            has_fitted_state=True,
        ),
        "kantorovich": MethodRegistryEntry(
            factory=_build_kantorovich,
            normalizations=("quantile_gaussian", "standard"),
            min_group_size=500,
            supports_alert=True,
            has_fitted_state=True,
        ),
        "smaa": MethodRegistryEntry(
            factory=SmaaScorer,
            normalizations=("minmax", "rank"),
            min_group_size=3,
            has_fitted_state=True,
        ),
        "cone_quantile": MethodRegistryEntry(
            factory=ConeQuantileScorer,
            normalizations=(NO_NORMALIZATION,),
            min_group_size=3,
            has_fitted_state=True,
        ),
    }
)

# Nom de l'argument de graine par `kind` : point d'injection de l'aléa du groupe
SEED_PARAMETERS: Mapping[str, str] = MappingProxyType(
    {
        "smaa": "random_state",
        "cone_quantile": "random_state",
        "kantorovich": "seed",
    }
)

# Nom de l'argument fixant le nombre de tirages de poids (partage SMAA / cône)
DRAW_PARAMETERS: Mapping[str, str] = MappingProxyType(
    {"smaa": "n_draws", "cone_quantile": "n_draws"}
)


# ──────────────────────────────────────────────────────────────────────
# Construction du pipeline d'une méthode
# ──────────────────────────────────────────────────────────────────────

# Fonction de récupération de l'entrée de registre d'une méthode
def registry_entry(kind: str) -> MethodRegistryEntry:
    """Return the registry entry of ``kind``.

    Args:
        kind: Key of :data:`METHOD_REGISTRY`.

    Returns:
        The :class:`MethodRegistryEntry`.

    Raises:
        ValueError: If ``kind`` is unknown.

    Examples:
        >>> registry_entry("mpi").min_group_size
        3
    """
    if kind not in METHOD_REGISTRY:
        raise ValueError(
            f"Unknown method kind {kind!r}. Available: {sorted(METHOD_REGISTRY)}."
        )
    return METHOD_REGISTRY[kind]


# Fonction de résolution des métriques d'une méthode
def method_metrics(spec: MethodSpec, config: "SynthesisConfig") -> Tuple[str, ...]:
    """Resolve the metric columns a method is fitted on.

    Args:
        spec: Configured method.
        config: Synthesis configuration carrying ``metric_columns``.

    Returns:
        ``spec.metrics`` when set, every metric of the configuration
        otherwise.

    Raises:
        ValueError: If ``spec.metrics`` names a column absent from
            ``config.metric_columns``.

    Examples:
        >>> from macroforecast.trade.aggregation.synthesis import SynthesisConfig
        >>> config = SynthesisConfig(metric_columns=("HHI", "CDI2"))
        >>> method_metrics(MethodSpec(name="mpi", kind="mpi"), config)
        ('HHI', 'CDI2')
    """
    if spec.metrics is None:
        return tuple(config.metric_columns)
    unknown = [name for name in spec.metrics if name not in config.metric_columns]
    if unknown:
        raise ValueError(
            f"Method {spec.name!r} asks for the metric(s) {unknown}, absent from "
            f"the configured metrics {list(config.metric_columns)}."
        )
    return tuple(spec.metrics)


# Fonction de résolution de la normalisation d'une méthode (D-13)
def resolve_normalization(spec: MethodSpec, config: "SynthesisConfig") -> str:
    """Resolve the normalisation scheme of a method (D-13).

    Resolution order: the explicit override of the method; failing that the
    configuration-wide default when the ``kind`` admits it; failing that the
    default of the ``kind`` (table S-1.4). A method is thus never silently
    fitted on an input its own construction forbids.

    Args:
        spec: Configured method.
        config: Synthesis configuration carrying the default ``normalization``.

    Returns:
        Name of an admissible normalisation scheme, possibly
        :data:`NO_NORMALIZATION`.

    Raises:
        ValueError: If ``spec.normalization`` is not admissible for the
            ``kind``.

    Examples:
        >>> from macroforecast.trade.aggregation.synthesis import SynthesisConfig
        >>> config = SynthesisConfig(normalization="minmax")
        >>> resolve_normalization(MethodSpec(name="p", kind="pareto"), config)
        'none'
        >>> resolve_normalization(MethodSpec(name="b", kind="bod"), config)
        'minmax'
    """
    entry = registry_entry(spec.kind)
    if spec.normalization is not None:
        if spec.normalization not in entry.normalizations:
            raise ValueError(
                f"The normalisation {spec.normalization!r} is not admissible for "
                f"the method kind {spec.kind!r} (method {spec.name!r}). Admissible "
                f"schemes (table S-1.4): {list(entry.normalizations)}."
            )
        return spec.normalization
    # Défaut de configuration retenu seulement s'il est admissible pour la méthode
    default = getattr(config, "normalization", None)
    if default in entry.normalizations:
        return default
    return entry.default_normalization


# Fonction de construction du vecteur de polarités d'une méthode
def _polarity_vector(
    metrics: Tuple[str, ...], config: "SynthesisConfig"
) -> np.ndarray:
    """Build the ``+/-1`` polarity vector aligned on ``metrics``.

    Args:
        metrics: Metric columns of the method, in matrix order.
        config: Synthesis configuration carrying ``polarities`` as pairs.

    Returns:
        Integer array of shape ``(d,)``.
    """
    polarities: Dict[str, int] = dict(getattr(config, "polarities", ()) or ())
    return np.array([polarities.get(name, 1) for name in metrics])


# Fonction de construction du pipeline complet d'une méthode
def build_method(spec: MethodSpec, config: "SynthesisConfig") -> Pipeline:
    """Assemble the ``orient -> winsorize -> scale -> estimate`` pipeline.

    Args:
        spec: Configured method.
        config: Synthesis configuration (metrics, polarities, winsorisation
            quantile and default normalisation).

    Returns:
        An unfitted ``sklearn.pipeline.Pipeline`` whose last step exposes
        ``fit`` / ``predict``, plus ``alert`` when
        ``METHOD_REGISTRY[spec.kind].supports_alert``.

    Raises:
        ValueError: If the ``kind`` is unknown, if a requested metric is
            absent from the configuration, or if the requested normalisation
            is not admissible for the ``kind``.

    Examples:
        >>> from macroforecast.trade.aggregation.synthesis import SynthesisConfig
        >>> config = SynthesisConfig(metric_columns=("HHI", "CDI2"))
        >>> pipeline = build_method(MethodSpec(name="mpi", kind="mpi"), config)
        >>> [name for name, _ in pipeline.steps]
        ['orient', 'winsorize', 'scale', 'estimate']
    """
    entry = registry_entry(spec.kind)
    metrics = method_metrics(spec, config)
    normalization = resolve_normalization(spec, config)

    # Winsorisation désactivée par défaut (M-08, D-12) : étape neutre
    quantile = getattr(config, "winsorize_quantile", None)
    winsorize: Any = (
        "passthrough" if quantile is None else Winsorizer(quantile=quantile)
    )
    # Normalisation « none » : méthodes invariantes par transformation monotone
    scale: Any = (
        "passthrough"
        if normalization == NO_NORMALIZATION
        else make_normalizer(normalization)
    )

    return Pipeline(
        [
            ("orient", PolarityOrienter(_polarity_vector(metrics, config))),
            ("winsorize", winsorize),
            ("scale", scale),
            ("estimate", entry.factory(**dict(spec.params))),
        ]
    )


# ──────────────────────────────────────────────────────────────────────
# Conversion YAML → MethodSpec
# ──────────────────────────────────────────────────────────────────────

# Fonction de conversion d'un mapping de configuration en spécification de méthode
def method_spec_from_mapping(mapping: Mapping[str, Any]) -> MethodSpec:
    """Convert one YAML method entry into a :class:`MethodSpec`.

    Same contract as the ``*_config_from_params`` helpers of the pipeline
    scripts: lists are coerced to the tuple types of the frozen dataclass and
    an unknown key is dropped with a warning rather than raising, so that a
    configuration written for a later version still runs.

    Args:
        mapping: Mapping read from the YAML ``methods`` list; ``name`` and
            ``kind`` are required.

    Returns:
        The corresponding :class:`MethodSpec`, its ``params`` frozen behind a
        ``MappingProxyType``.

    Raises:
        KeyError: If ``name`` or ``kind`` is absent.
        ValueError: If ``kind`` is unknown.

    Examples:
        >>> spec = method_spec_from_mapping(
        ...     {"name": "rank_mean", "kind": "rank_mean", "levels": ["global"]}
        ... )
        >>> spec.levels
        ('global',)
    """
    for required in ("name", "kind"):
        if required not in mapping:
            raise KeyError(
                f"A method entry requires the key {required!r}; got "
                f"{sorted(mapping)}."
            )
    # Vérification immédiate du `kind` : une faute de frappe ne doit pas attendre
    # l'ajustement du premier groupe pour être signalée
    registry_entry(mapping["kind"])

    valid = {item.name for item in fields(MethodSpec)}
    overrides: Dict[str, Any] = {}
    for key, value in mapping.items():
        if key not in valid:
            # Logging
            logger.warning(f"Clé de méthode inconnue ignorée : {key}")
            continue
        # Coercition listes → tuples pour les champs tuple du dataclass gelé
        if key in ("metrics", "levels") and isinstance(value, (list, tuple)):
            value = tuple(value)
        if key == "params" and isinstance(value, Mapping):
            value = MappingProxyType(dict(value))
        overrides[key] = value

    return MethodSpec(**overrides)
