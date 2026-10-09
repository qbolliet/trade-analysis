"""Factories of the API clients (Eurostat, UN Comtrade, UNSD), chosen by configuration.

The download, BACI and partner steps take a *client factory* (a callable without
argument returning an open client, closed by the step). The class behind it
is named in the catalog (``clients.<provider>`` datasets), so that an
environment swaps the real ``statflows`` clients for network-free ones (the
``test`` environment) without touching any node; the subscription key of the
paid API comes from ``credentials.yml``, never from the parameters.
"""
# Importation des modules
# Modules de base
import importlib
from typing import Any, Callable, Dict, Mapping, Optional


# Fonction d'import d'un objet désigné par son chemin pointé
def import_object(path: str) -> Any:
    """Import the object designated by a dotted path.

    Args:
        path: ``"package.module.Name"`` (or ``"package.module:Name"``).

    Returns:
        The imported object.

    Raises:
        ImportError: If the module cannot be imported.
        AttributeError: If the module has no such attribute.

    Examples:
        >>> import_object("collections.OrderedDict").__name__
        'OrderedDict'
    """
    module_name, _, attribute = path.replace(":", ".").rpartition(".")
    if not module_name:
        raise ImportError(f"'{path}' is not a dotted path to an object")
    return getattr(importlib.import_module(module_name), attribute)


# Fabrique de client sans argument, transmissible à un autre processus
class ClientFactory:
    """Callable building one API client from a dotted class path and its options.

    Picklable (it holds the path and the options only, never a client), so it
    can be handed to a step that builds — and closes — its client itself.

    Args:
        path: Dotted path of the client class or factory function (e.g.
            ``"statflows.ComtradeClient"``).
        options: Keyword arguments of the call (public options and
            credentials, e.g. ``{"subscription_key": "..."}``).

    Examples:
        >>> factory = ClientFactory("collections.OrderedDict", {"a": 1})
        >>> factory()
        OrderedDict({'a': 1})
        >>> factory.path
        'collections.OrderedDict'
    """

    def __init__(self, path: str, options: Optional[Mapping[str, Any]] = None) -> None:
        self.path = str(path)
        self.options: Dict[str, Any] = dict(options or {})

    def __call__(self) -> Any:
        """Build a new client.

        Returns:
            The client returned by the designated class or function.
        """
        builder: Callable[..., Any] = import_object(self.path)
        return builder(**self.options)

    def __repr__(self) -> str:
        # Options jamais affichées : elles peuvent porter une clé d'abonnement
        return f"ClientFactory({self.path!r})"
