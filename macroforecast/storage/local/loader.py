# Importation des modules
import json
from pathlib import Path
from typing import Any


# Fonction de chargement des données depuis un fichier JSON en local
def load_local(filepath: str, **kwargs) -> Any:
    """Load a JSON file from local storage.

    Args:
        filepath (str): Path to the local JSON file.
        **kwargs: Additional arguments forwarded to ``json.load``.

    Returns:
        Any: The deserialised JSON object (dict, list, etc.).

    Raises:
        ValueError: If the file extension is not ``.json``.
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file is not valid JSON.

    Examples:
        >>> data = load_local('config.json')
        >>> data = load_local('records.json')
    """
    # Vérification de l'extension
    extension = Path(filepath).suffix.lower()[1:]
    if extension != "json":
        raise ValueError(
            f"Unsupported extension '.{extension}': only '.json' files are supported."
        )
    # Lecture du fichier JSON
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f, **kwargs)
