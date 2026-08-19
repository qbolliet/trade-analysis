# Importation des modules
from typing import Optional
import pandas as pd

# Importation du module de connection
from ._connection import _S3Connection


# Classe de chargement de données depuis S3
class S3Loader(_S3Connection):
    """Load xls data from Amazon S3 buckets.

    Args:
        s3_package (str, optional): Package to use for S3 connections
            (``'s3fs'`` or ``'boto3'``). Defaults to ``"boto3"``.

    Attributes:
        s3: The S3 connection object (initialised lazily).
        s3_package (str): The package being used for S3 connectivity.

    Examples:
        Load an xls file from S3 using boto3:
        >>> loader = S3Loader()
        >>> loader.connect(
        ...     aws_access_key_id='YOUR_KEY',
        ...     aws_secret_access_key='YOUR_SECRET'
        ... )
        >>> data = loader.load(bucket='my-bucket', key='path/to/file.xls')
    """

    # Initialisation
    def __init__(self, s3_package: Optional[str] = "boto3") -> None:
        """Initialize the S3Loader with specified S3 package.

        Args:
            s3_package (str, optional): Package to use for S3 connections.
                Must be either ``'s3fs'`` or ``'boto3'``. Defaults to ``"boto3"``.
        """
        # Initialisation du parent
        super().__init__(s3_package=s3_package)

    # Méthode de connexion au S3
    def connect(self, **kwargs) -> None:
        """Establish a connection to the S3 bucket.

        Args:
            **kwargs: Additional keyword arguments for establishing the connection.
        """
        # Etablissement d'une connection
        return self._connect(**kwargs)

    # Méthode de chargement des données
    def load(self, bucket: str, key: str, **kwargs) -> pd.DataFrame:
        """Load a xls object from an S3 object.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The S3 object key. Must end in ``.xls``.
            **kwargs: Additional arguments forwarded to ``pd.read_excel``.

        Returns:
            pd.DataFrame: The DataFrame containing the data of the xls file.

        Raises:
            ValueError: If the key does not end in ``.xls``.

        Examples:
            >>> data = loader.load(bucket='my-bucket', key='data/my-data.xls')
        """
        # Extraction de l'extension
        extension = key.rsplit(".", 1)[-1].lower()
        # Vérification que l'extension est supportée
        if extension != "xls":
            raise ValueError(
                f"Unsupported extension '.{extension}': only '.xls' files are supported."
            )
        # Etablissement d'une connexion si nécessaire
        if not hasattr(self, "s3"):
            self.connect()
        # Chargement du fichier xls
        if self.s3_package == "boto3":
            s3_file = self.s3.get_object(Bucket=bucket, Key=key)["Body"]
            return pd.read_excel(s3_file.read(), engine="xlrd", **kwargs)
        elif self.s3_package == "s3fs":
            with self.s3.open(f"{bucket}/{key}", "r") as s3_file:
                return pd.read_excel(s3_file, engine="xlrd", **kwargs)
