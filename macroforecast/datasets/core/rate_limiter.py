"""Rate limiter for API requests.

This module provides a rate limiter that enforces request rate limits
for API clients, supporting various time units.
"""
# Importation des modules
from typing import Any, Dict, List, Literal, Union
from collections import deque
import time
import threading
import logging
# Module du package
from .reports import RateLimitStats

# Initialisation du logger
logger = logging.getLogger(__name__)


# Type pour les unités de temps
TimeUnit = Literal["seconds", "minutes", "hours", "days", "weeks"]


# Classe de gestion du rate limiting
class RateLimiter:
    """Rate limiter for API requests.

    This class implements a sliding window rate limiter that tracks
    request timestamps and enforces a maximum number of requests
    per time period.

    Args:
        max_requests: Maximum number of requests allowed.
        time_unit: Unit of time period ('seconds', 'minutes', 'hours', 'days', 'weeks').
        time_count: Number of time units (e.g., 1 hour, 2 days).

    Example:
        >>> limiter = RateLimiter(max_requests=60, time_unit="hours", time_count=1)
        >>> limiter.acquire()  # Attends si nécessaire avant de continuer
        >>> # Exécuter la requête API ici
    """

    # Mapping des unités vers secondes
    _UNIT_TO_SECONDS = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400,
        "weeks": 604800,
    }

    # Initialisation
    def __init__(
        self,
        max_requests: int,
        time_unit: TimeUnit,
        time_count: int = 1,
    ):
        """Initialize rate limiter.

        Args:
            max_requests: Maximum number of requests allowed.
            time_unit: Unit of time period.
            time_count: Number of time units.

        Raises:
            ValueError: If parameters are invalid.
        """
        # Validation des paramètres
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if time_count <= 0:
            raise ValueError("time_count must be positive")
        if time_unit not in self._UNIT_TO_SECONDS:
            raise ValueError(
                f"time_unit must be one of {list(self._UNIT_TO_SECONDS.keys())}"
            )

        # Initialisation des attributs
        self.max_requests = max_requests
        self.time_unit = time_unit
        self.time_count = time_count

        # Calcul de la fenêtre temporelle en secondes
        self.window_seconds = self._UNIT_TO_SECONDS[time_unit] * time_count

        # File des timestamps des requêtes
        self._request_times: deque[float] = deque()

        # Compteurs d'exécution : l'attente imposée est le coût caché d'un
        # téléchargement long
        self._n_acquisitions: int = 0
        self._total_wait_seconds: float = 0.0
        self._max_wait_seconds: float = 0.0

        # Lock pour thread-safety
        self._lock = threading.Lock()

        # Logging
        logger.info(
            f"RateLimiter initialized: {max_requests} requests per "
            f"{time_count} {time_unit} ({self.window_seconds}s)"
        )

    # Méthode d'acquisition de données en respectant le quota de requêtes
    def acquire(self) -> None:
        """Wait if necessary, then record the request.

        This method blocks until a request can be made without
        exceeding the rate limit.

        Example:
            >>> limiter = RateLimiter(max_requests=10, time_unit="seconds", time_count=1)
            >>> limiter.acquire()
            >>> # La requête peut maintenant être effectuée
        """
        with self._lock:
            # Nettoyage des anciennes requêtes
            self._clean_old_requests()

            # Calcul du temps d'attente nécessaire
            wait_seconds = self._wait_time()

            # Attente si nécessaire
            if wait_seconds > 0:
                # Logging
                logger.debug(f"Rate limit reached, waiting {wait_seconds:.2f}s")
                # Attente
                time.sleep(wait_seconds)
                # Re-nettoyage après l'attente
                self._clean_old_requests()

            # Enregistrement de la nouvelle requête
            self._request_times.append(time.time())
            # Comptabilisation de l'acquisition et de l'attente subie
            self._n_acquisitions += 1
            if wait_seconds > 0:
                self._total_wait_seconds += wait_seconds
                self._max_wait_seconds = max(self._max_wait_seconds, wait_seconds)
            # Logging
            logger.debug(
                f"Request recorded ({len(self._request_times)}/{self.max_requests})"
            )

    # Méthode auxiliaire de suppression des requêtes trop anciennes
    def _clean_old_requests(self) -> None:
        """Remove requests older than the time window.

        Deletes all query records, allowing
        you to restart with an empty counter.
        """
        # Extraction du temps présent
        current_time = time.time()
        cutoff_time = current_time - self.window_seconds

        # Suppression des timestamps trop anciens
        while self._request_times and self._request_times[0] < cutoff_time:
            self._request_times.popleft()

    # Méthode de calcul du temps d'attente avant la prochaine requête
    def _wait_time(self) -> float:
        """Calculate how long to wait before next request.

        Returns:
            Number of seconds to wait (0 if no wait needed).
        """
        # Cas où la limite n'est pas atteinte
        if len(self._request_times) < self.max_requests:
            return 0.0

        # Calcul du temps d'attente jusqu'à ce que la requête la plus ancienne sorte de la fenêtre
        oldest_request = self._request_times[0]
        current_time = time.time()
        time_since_oldest = current_time - oldest_request
        wait_time = self.window_seconds - time_since_oldest

        # Retourne le temps d'attente (minimum 0)
        return max(0.0, wait_time)

    # Méthode de création d'une instance du RateLimiter à partir d'un dictionnaire de paramètres
    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "RateLimiter":
        """Create RateLimiter from configuration dictionary.

        Args:
            config: Dictionary with keys 'requests', 'unit', 'count'.

        Returns:
            Configured RateLimiter instance.

        Raises:
            ValueError: If required keys are missing.

        Example:
            >>> config = {"requests": 60, "unit": "hours", "count": 1}
            >>> limiter = RateLimiter.from_dict(config)
        """
        # Validation de la présence des clés
        required_keys = {"requests", "unit"}
        missing_keys = required_keys - set(config.keys())
        if missing_keys:
            raise ValueError(f"Missing required keys: {missing_keys}")

        # Extraction des paramètres
        max_requests = config["requests"]
        time_unit = config["unit"]
        time_count = config.get("count", 1)

        # Création de l'instance
        return RateLimiter(
            max_requests=max_requests,
            time_unit=time_unit,
            time_count=time_count,
        )

    # Méthode de réinitialisation du rate limiter
    def reset(self) -> None:
        """Reset the rate limiter state.

        Deletes all query records, allowing
        you to restart with an empty counter.

        Example:
            >>> limiter = RateLimiter(max_requests=10, time_unit="seconds", time_count=1)
            >>> limiter.acquire()
            >>> limiter.reset()  # Réinitialise le compteur
        """
        with self._lock:
            # Réinitialisation
            self._request_times.clear()
            # Logging
            logger.debug("RateLimiter reset")
            # Les compteurs d'exécution suivent la réinitialisation de l'état
            self._n_acquisitions = 0
            self._total_wait_seconds = 0.0
            self._max_wait_seconds = 0.0

    # Méthode d'extraction des compteurs d'exécution
    def get_stats(self) -> RateLimitStats:
        """Return the execution counters of the limiter.

        Exposes as data what was previously only logged: how many acquisitions
        were granted and how much wall-clock time they cost.

        Returns:
            A :class:`~macroforecast.datasets.core.reports.RateLimitStats`
            snapshot.

        Example:
            >>> limiter = RateLimiter(max_requests=10, time_unit="seconds")
            >>> limiter.acquire()
            >>> limiter.get_stats().n_acquisitions
            1
        """
        # Instantané détaché : l'appelant peut le conserver dans son rapport
        return RateLimitStats(
            n_acquisitions=self._n_acquisitions,
            total_wait_seconds=self._total_wait_seconds,
            max_wait_seconds=self._max_wait_seconds,
            remaining_requests=self.get_remaining_requests(),
        )

    # Méthode d'extraction du nombre de requêtes restantes pouvant être effectuées dans la fenêtre temporelle
    def get_remaining_requests(self) -> int:
        """Get number of remaining requests in current window.

        Returns:
            Number of requests that can be made immediately.

        Example:
            >>> limiter = RateLimiter(max_requests=10, time_unit="seconds", time_count=1)
            >>> remaining = limiter.get_remaining_requests()
        """
        with self._lock:
            # Nettoyage des anciennes requêtes
            self._clean_old_requests()
            return max(0, self.max_requests - len(self._request_times))

    # Représentation sous forme de chaîne de caractères
    def __repr__(self) -> str:
        """String representation of the RateLimiter."""
        return (
            f"RateLimiter(max_requests={self.max_requests}, "
            f"time_unit='{self.time_unit}', time_count={self.time_count})"
        )


# Classe de composition de plusieurs limites de débit simultanées
class CompositeRateLimiter:
    """Enforce several rate limits simultaneously.

    Wraps a list of :class:`RateLimiter` instances and blocks until *every*
    limit allows the next request. Useful for APIs imposing more than one
    constraint at once, e.g. UN Comtrade (1 request per second **and** 500
    requests per day).

    Exposes the same ``acquire`` / ``reset`` / ``get_remaining_requests``
    surface as :class:`RateLimiter` so the two are interchangeable wherever a
    limiter is used (e.g. ``AbstractSDMXClient`` or ``ComtradeClient``).

    Args:
        limiters: Rate limiters to enforce together. Must be non-empty.

    Raises:
        ValueError: If ``limiters`` is empty.

    Example:
        >>> per_second = RateLimiter(max_requests=1, time_unit="seconds")
        >>> per_day = RateLimiter(max_requests=500, time_unit="days")
        >>> limiter = CompositeRateLimiter([per_second, per_day])
        >>> limiter.acquire()  # Attends que les deux limites soient respectées
    """

    # Initialisation
    def __init__(self, limiters: List[RateLimiter]):
        """Initialize the composite rate limiter.

        Args:
            limiters: Non-empty list of :class:`RateLimiter` to enforce.

        Raises:
            ValueError: If ``limiters`` is empty.
        """
        # Validation : au moins une limite
        if not limiters:
            raise ValueError("CompositeRateLimiter requires at least one limiter")

        # Initialisation des attributs
        self.limiters = list(limiters)

        # Logging
        logger.info(
            f"CompositeRateLimiter initialized with {len(self.limiters)} limiters"
        )

    # Méthode d'acquisition respectant l'ensemble des limites
    def acquire(self) -> None:
        """Wait until every wrapped limiter allows the request, then record it.

        Each limiter is acquired in turn; the most constraining one drives the
        wait. Acquiring sequentially is sufficient because each
        :meth:`RateLimiter.acquire` only records the request once its own
        window allows it.
        """
        # Acquisition séquentielle : la limite la plus contraignante bloque
        for limiter in self.limiters:
            limiter.acquire()

    # Méthode de réinitialisation de l'ensemble des limites
    def reset(self) -> None:
        """Reset the state of every wrapped limiter."""
        # Réinitialisation de chaque limite
        for limiter in self.limiters:
            limiter.reset()

    # Méthode d'agrégation des compteurs d'exécution
    def get_stats(self) -> RateLimitStats:
        """Aggregate the execution counters of every wrapped limiter.

        Returns:
            A :class:`~macroforecast.datasets.core.reports.RateLimitStats` whose
            acquisitions are the maximum over the limiters — they are acquired in
            turn, once per request — and whose waits are summed, sequential
            acquisition making the total wait additive.
        """
        # Compteurs individuels
        stats = [limiter.get_stats() for limiter in self.limiters]
        return RateLimitStats(
            n_acquisitions=max((stat.n_acquisitions for stat in stats), default=0),
            total_wait_seconds=sum(stat.total_wait_seconds for stat in stats),
            max_wait_seconds=max(
                (stat.max_wait_seconds for stat in stats), default=0.0
            ),
            remaining_requests=self.get_remaining_requests(),
        )

    # Méthode d'extraction du nombre de requêtes restantes (le plus contraignant)
    def get_remaining_requests(self) -> int:
        """Return the remaining requests allowed by the most constraining limit.

        Returns:
            Minimum of the remaining requests across all wrapped limiters.
        """
        # Minimum des disponibilités sur l'ensemble des limites
        return min(limiter.get_remaining_requests() for limiter in self.limiters)

    # Représentation sous forme de chaîne de caractères
    def __repr__(self) -> str:
        """String representation of the CompositeRateLimiter."""
        return f"CompositeRateLimiter(limiters={self.limiters!r})"


# Fabrique de limiteur de débit à partir d'une configuration (dict ou liste)
def build_rate_limiter(
    config: Union[Dict[str, Any], List[Dict[str, Any]]],
) -> Union[RateLimiter, CompositeRateLimiter]:
    """Build a rate limiter from a configuration entry.

    Accepts either a single limit specification (a dict, returning a
    :class:`RateLimiter`) or several specifications (a list, returning a
    :class:`CompositeRateLimiter`). This keeps the ``RATE_LIMIT`` section of the
    provider configuration files backward compatible: Eurostat/OECD keep a
    single dict, while Comtrade declares a list of limits.

    Args:
        config: A single ``{requests, unit, count}`` dict, or a non-empty list
            of such dicts.

    Returns:
        A :class:`RateLimiter` for a single limit, or a
        :class:`CompositeRateLimiter` for several.

    Raises:
        ValueError: If ``config`` is an empty list or an unsupported type.

    Example:
        >>> build_rate_limiter({"requests": 30, "unit": "minutes"})
        RateLimiter(max_requests=30, time_unit='minutes', time_count=1)
        >>> build_rate_limiter([
        ...     {"requests": 1, "unit": "seconds"},
        ...     {"requests": 500, "unit": "days"},
        ... ])  # doctest: +ELLIPSIS
        CompositeRateLimiter(...)
    """
    # Cas d'une liste de spécifications → limiteur composite
    if isinstance(config, list):
        # Validation : liste non vide
        if not config:
            raise ValueError("RATE_LIMIT list must contain at least one entry")
        # Un seul élément : un RateLimiter simple suffit
        limiters = [RateLimiter.from_dict(entry) for entry in config]
        if len(limiters) == 1:
            return limiters[0]
        return CompositeRateLimiter(limiters)

    # Cas d'un dictionnaire unique → limiteur simple
    if isinstance(config, dict):
        return RateLimiter.from_dict(config)

    # Type non supporté
    raise ValueError(
        f"Unsupported RATE_LIMIT configuration type: {type(config).__name__}"
    )
