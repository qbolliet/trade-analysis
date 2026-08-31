# Importation des modules
from pathlib import Path
from typing import Optional

# Module de manipulation de données
import pandas as pd


# Fonction de sauvegarde de données en local au format parquet
def save_local(filepath: str, obj: Optional[pd.DataFrame] = None, **kwargs) -> None:
    """Save a DataFrame to a local Parquet file.

    Args:
        filepath (str): Path where the Parquet file will be written. Parent
            directories are created automatically.
        obj: DataFrame to save.
        **kwargs: Additional arguments forwarded to ``pandas.DataFrame.to_parquet``.

    Raises:
        ValueError: If the file extension is not ``.parquet``.
        IOError: If the file cannot be written.

    Examples:
        >>> save_local('output.parquet', pd.DataFrame({'a': [1, 2]}))
    """
    # Vérification de l'extension
    path = Path(filepath)
    extension = path.suffix.lower()[1:]
    if extension != "parquet":
        raise ValueError(
            f"Unsupported extension '.{extension}': only '.parquet' files are supported."
        )
    # Création des répertoires parents si nécessaires
    path.parent.mkdir(parents=True, exist_ok=True)
    # Écriture du fichier parquet
    obj.to_parquet(path, **kwargs)
