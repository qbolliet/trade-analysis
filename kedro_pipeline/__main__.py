"""Entry point making the project runnable as ``python -m kedro_pipeline``.

Equivalent to ``kedro run`` from the repository root.
"""
# Importation des modules
# Modules de base
import sys
from pathlib import Path
from typing import Any

# Kedro
from kedro.framework.cli.utils import find_run_command
from kedro.framework.project import configure_project


# Fonction principale
def main(*args: Any, **kwargs: Any) -> Any:
    """Configure the project and run its ``run`` command.

    Args:
        *args: Positional arguments forwarded to the ``run`` command.
        **kwargs: Keyword arguments forwarded to the ``run`` command.

    Returns:
        The result of the ``run`` command (in interactive mode only).

    Examples:
        >>> main(["--pipeline", "__default__"])  # doctest: +SKIP
    """
    package_name = Path(__file__).parent.name
    configure_project(package_name)

    # Mode interactif : pas de sortie du processus en fin de commande
    interactive = hasattr(sys, "ps1")
    kwargs["standalone_mode"] = not interactive

    run = find_run_command(package_name)
    return run(*args, **kwargs)


if __name__ == "__main__":
    main()
