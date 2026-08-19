# Importation des modules
import json
from pathlib import Path
from typing import Optional


# Fonction de sauvegarde de données en local au format JSON
def save_local(filepath: str, obj: Optional[object] = None, **kwargs) -> None:
    """Save an object to a local JSON file.

    Args:
        filepath (str): Path where the JSON file will be written. Parent
            directories are created automatically.
        obj: Any JSON-serialisable object to save.
        **kwargs: Additional arguments forwarded to ``json.dump``
            (e.g. ``indent``, ``ensure_ascii``).

    Raises:
        ValueError: If the file extension is not ``.json``.
        TypeError: If ``obj`` is not JSON-serialisable.
        IOError: If the file cannot be written.

    Examples:
        >>> save_local('output.json', {'key': 'value'}, indent=2)
        >>> save_local('records.json', [1, 2, 3])
    """
    # Vérification de l'extension
    path = Path(filepath)
    extension = path.suffix.lower()[1:]
    if extension != "json":
        raise ValueError(
            f"Unsupported extension '.{extension}': only '.json' files are supported."
        )
    # Création des répertoires parents si nécessaires
    path.parent.mkdir(parents=True, exist_ok=True)
    # Écriture du fichier JSON
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, **kwargs)
