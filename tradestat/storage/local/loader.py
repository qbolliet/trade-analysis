# Importation des modules
from pathlib import Path
import pandas as pd

# Extensions supportées par le chargeur local
_SUPPORTED_EXTENSIONS = ("xls", "xlsx", "parquet")

# Moteur pandas associé à chaque format Excel
_EXCEL_ENGINES = {"xls": "xlrd", "xlsx": "openpyxl"}


# Fonction de chargement des données depuis un fichier xls, xlsx ou parquet en local
def load_local(filepath: str, **kwargs) -> pd.DataFrame:
    """Load an xls, xlsx or parquet file from local storage.

    Args:
        filepath (str): Path to the local file (``.xls``, ``.xlsx`` or
            ``.parquet``).
        **kwargs: Additional arguments forwarded to ``pd.read_excel`` (``.xls``
            via the ``xlrd`` engine, ``.xlsx`` via the ``openpyxl`` engine) or
            ``pd.read_parquet`` (``.parquet``).

    Returns:
        pd.DataFrame: The DataFrame containing the data of the file.

    Raises:
        ValueError: If the file extension is not ``.xls``, ``.xlsx`` or
            ``.parquet``.
        FileNotFoundError: If the file does not exist.

    Examples:
        >>> data = load_local('dist_cepii.xls')
        >>> data = load_local('geo_cepii.xlsx')
        >>> data = load_local('HS2022-HS2017.parquet')
    """
    # Vérification de l'extension
    extension = Path(filepath).suffix.lower()[1:]
    # Lecture des fichiers Excel (mode binaire, requis par les moteurs xlrd/openpyxl)
    if extension in _EXCEL_ENGINES:
        with open(filepath, "rb") as f:
            return pd.read_excel(f, engine=_EXCEL_ENGINES[extension], **kwargs)
    # Lecture du fichier parquet
    if extension == "parquet":
        return pd.read_parquet(filepath, **kwargs)
    raise ValueError(
        f"Unsupported extension '.{extension}': only {_SUPPORTED_EXTENSIONS} "
        f"files are supported."
    )
