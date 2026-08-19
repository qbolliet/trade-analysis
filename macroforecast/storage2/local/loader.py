# Importation des modules
from pathlib import Path
import pandas as pd


# Fonction de chargement des données depuis un fichier xls en local
def load_local(filepath: str, **kwargs) -> pd.DataFrame:
    """Load an xls file from local storage.

    Args:
        filepath (str): Path to the local xls file.
        **kwargs: Additional arguments forwarded to ``pd.read_excel``.

    Returns:
        pd.DataFrame: The DataFrame containing the data of the xls file.

    Raises:
        ValueError: If the file extension is not ``.xls``.
        FileNotFoundError: If the file does not exist.

    Examples:
        >>> data = load_local('dist_cepii.xls')
        >>> data = load_local('geo_cepii.xls')
    """
    # Vérification de l'extension
    extension = Path(filepath).suffix.lower()[1:]
    if extension != "xls":
        raise ValueError(
            f"Unsupported extension '.{extension}': only '.xls' files are supported."
        )
    # Lecture du fichier xls (mode binaire, requis par le moteur xlrd)
    with open(filepath, "rb") as f:
        return pd.read_excel(f, engine="xlrd", **kwargs)
