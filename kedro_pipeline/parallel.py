"""Parallélisme intra-pod : résolution du nombre de processus et carte parallèle.

Les étapes par contexte (synthèse, cohérence) et par millésime (réseau) calculent
dans des processus ``loky`` ; le processus parent reste le seul écrivain des tables
et des registres. Les fonctions de ce module n'ont aucune connaissance de la
méthodologie : elles fixent seulement l'environnement d'exécution des workers
(un seul fil BLAS par processus, pas de préallocation mémoire JAX) pour que le
résultat ne dépende ni du nombre de processus ni de l'ordre d'arrivée.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
import os
import pickle
import traceback
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Tuple, TypeVar

# Variable d'environnement portant le nombre de CPU alloués au pod
NUM_CPU_VARIABLE = "NUM_CPU"
# Environnement des workers : un fil BLAS/OpenMP par processus (évite la
# sur-souscription et rend les sommes flottantes indépendantes du nombre de
# processus), pas de préallocation de la mémoire par JAX/XLA
DEFAULT_WORKER_ENV: Mapping[str, str] = {
    "OMP_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "XLA_PYTHON_CLIENT_PREALLOCATE": "false",
}

# Initialisation du logger
logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


# Fonction de résolution du nombre de processus
def resolve_n_jobs(configured: Optional[int] = None) -> int:
    """Resolve the number of worker processes of a step.

    The order is: the configured value (when positive), then the ``NUM_CPU``
    environment variable (CPU allocated to the pod), then ``os.cpu_count()``.
    In the script phase this is the only place reading ``NUM_CPU``.

    Args:
        configured: Value of the ``N_JOBS`` setting; ``None`` or a non-positive
            value defers to the environment.

    Returns:
        The number of processes, at least 1.

    Examples:
        >>> resolve_n_jobs(3)
        3
        >>> resolve_n_jobs(None) >= 1
        True
    """
    if configured is not None and int(configured) > 0:
        return int(configured)
    raw = os.environ.get(NUM_CPU_VARIABLE, "").strip()
    try:
        from_env = int(float(raw)) if raw else 0
    except ValueError:
        # Valeur illisible : repli sur le nombre de CPU de la machine
        logger.warning(f"{NUM_CPU_VARIABLE}={raw!r} illisible, ignorée.")
        from_env = 0
    if from_env > 0:
        return from_env
    return max(1, os.cpu_count() or 1)


# Appel enveloppé exécuté dans le worker (picklable : fonction de module + données)
class _WorkerCall:
    """Picklable wrapper applying the worker environment around a call.

    Attributes:
        func: Function applied to one item.
        env: Environment variables set before the call (empty in-process).
    """

    def __init__(self, func: Callable[[Any], Any], env: Mapping[str, str]) -> None:
        self.func = func
        self.env = dict(env)

    def __call__(self, index: int, item: Any) -> Tuple[int, Any]:
        """Apply ``func`` to ``item`` under the worker environment.

        Args:
            index: Position of the item in the input.
            item: Item to process.

        Returns:
            ``(index, func(item))``.
        """
        # Variables lues à l'initialisation tardive de JAX/XLA
        os.environ.update(self.env)
        from threadpoolctl import threadpool_limits

        # Un seul fil BLAS même si numpy est déjà chargé dans le processus
        with threadpool_limits(limits=1):
            return index, self.func(item)


# Carte parallèle à ordre d'arrivée libre
def parallel_map(
    func: Callable[[T], R],
    items: Iterable[T],
    n_jobs: int,
    *,
    backend: str = "loky",
    initializer_env: Mapping[str, str] = DEFAULT_WORKER_ENV,
) -> Iterator[Tuple[int, R]]:
    """Apply ``func`` to each item in worker processes, results in arrival order.

    ``n_jobs=1`` runs in the current process through the same wrapper (BLAS
    limited to one thread), so that the numerical result does not depend on
    the number of processes. With ``n_jobs>1`` the pool is ``joblib`` with the
    ``generator_unordered`` return mode: a result is available as soon as its
    task ends. ``func`` must catch the errors it wants to isolate (an uncaught
    exception interrupts the iteration).

    Args:
        func: Picklable function of one item (module-level function or
            ``functools.partial`` of one).
        items: Items to process.
        n_jobs: Number of processes (at least 1).
        backend: ``joblib`` backend; CPU-bound code must use ``"loky"``.
        initializer_env: Environment variables set in each worker.

    Yields:
        ``(index, result)`` pairs, ``index`` being the position of the item in
        ``items``.

    Raises:
        ValueError: If ``backend`` is ``"threading"`` (forbidden for CPU-bound
            work).

    Examples:
        >>> sorted(parallel_map(abs, [-1, -2], n_jobs=1))
        [(0, 1), (1, 2)]
    """
    if backend == "threading":
        raise ValueError("The threading backend is not allowed for CPU-bound work.")
    n_jobs = max(1, int(n_jobs))
    # En processus : pas de modification durable de l'environnement du parent
    if n_jobs == 1:
        call = _WorkerCall(func, {})
        for index, item in enumerate(items):
            yield call(index, item)
        return

    from joblib import Parallel, delayed, parallel_config

    call = _WorkerCall(func, initializer_env)
    with parallel_config(backend=backend, inner_max_num_threads=1):
        yield from Parallel(n_jobs=n_jobs, return_as="generator_unordered")(
            delayed(call)(index, item) for index, item in enumerate(items)
        )


# Exception sérialisable d'un échec rencontré dans un worker
class ContextFailure(RuntimeError):
    """Failure raised in a worker whose original exception cannot be pickled.

    Attributes:
        original_type: Name of the original exception class.
        worker_traceback: Formatted traceback captured in the worker.
    """

    def __init__(self, message: str, original_type: str = "", worker_traceback: str = "") -> None:
        super().__init__(message)
        self.original_type = original_type
        self.worker_traceback = worker_traceback

    def __reduce__(self):
        return (type(self), (str(self), self.original_type, self.worker_traceback))


# Fonction de transformation d'une exception en exception transmissible
def serialisable_exception(exc: BaseException) -> BaseException:
    """Return an exception that survives the trip back from a worker.

    Args:
        exc: Exception caught in the worker.

    Returns:
        ``exc`` itself when it survives a pickle round trip, else a
        :class:`ContextFailure` carrying its type, message and traceback.

    Examples:
        >>> isinstance(serialisable_exception(ValueError("x")), ValueError)
        True
    """
    try:
        pickle.loads(pickle.dumps(exc))
        return exc
    except Exception:
        return ContextFailure(
            f"{type(exc).__name__}: {exc}",
            original_type=type(exc).__name__,
            worker_traceback="".join(traceback.format_exception(exc)),
        )
