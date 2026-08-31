# Importation des modules
from typing import Optional
import pandas as pd

# Module de chargement de fichiers en local
from .local.loader import load_local
# Module de chargement de fichiers depuis S3
from .s3.loader import S3Loader


# Classe générale de chargement des données
class Loader(S3Loader):
    """A unified class for loading tabular data from S3 or local storage.

    Loads from Amazon S3 when ``bucket`` is supplied, from the local filesystem
    otherwise. Supported extensions: ``.xls``, ``.xlsx`` and ``.parquet``.

    Args:
        s3_package (str, optional): The package to use for S3 connections
            (``'s3fs'`` or ``'boto3'``). Defaults to ``"boto3"``.

    Attributes:
        s3: The S3 connection object, initialised lazily.

    Examples:
        Load an xlsx file from S3:
        >>> loader = Loader(s3_package='boto3')
        >>> data = loader.load(
        ...     filepath='data/dist_cepii.xlsx',
        ...     bucket='my-bucket',
        ...     aws_access_key_id='YOUR_KEY',
        ...     aws_secret_access_key='YOUR_SECRET'
        ... )

        Load a parquet file from local storage:
        >>> loader = Loader()
        >>> data = loader.load(filepath='data/HS2022-HS2017.parquet')
    """

    # Initialisation
    def __init__(self, s3_package: Optional[str] = "boto3") -> None:
        """Initialize the Loader with specified S3 package.

        Args:
            s3_package (str, optional): Package to use for S3 connections.
                Must be either 's3fs' or 'boto3'. Defaults to "boto3".
        """
        super().__init__(s3_package=s3_package)

    # Méthode de chargement des données
    def load(self, filepath: str, bucket: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """Load an xls, xlsx or parquet file from S3 or local storage.

        Args:
            filepath (str): Path to the file. For S3 this is the object key;
                for local storage this is the filesystem path.
            bucket (str, optional): S3 bucket name. If ``None``, loads from local
                storage.
            **kwargs: Additional arguments passed to the underlying loader.
                For S3: ``aws_access_key_id``, ``aws_secret_access_key``,
                ``aws_session_token``, ``endpoint_url``, ``verify``.
                For both: forwarded to ``pd.read_excel`` (``.xls``/``.xlsx``) or
                ``pd.read_parquet`` (``.parquet``).

        Returns:
            pd.DataFrame: The DataFrame containing the data of the file.

        Raises:
            ValueError: If the file extension is not ``.xls``, ``.xlsx`` or
                ``.parquet``.
            FileNotFoundError: If the local file doesn't exist.
            botocore.exceptions.ClientError: If there are S3 access issues.

        Examples:
            Load an xlsx file from S3:
            >>> data = loader.load(
            ...     filepath='data/dist_cepii.xlsx',
            ...     bucket='my-bucket',
            ...     aws_access_key_id='KEY',
            ...     aws_secret_access_key='SECRET'
            ... )

            Load a local xlsx file:
            >>> data = loader.load(filepath='data/dist_cepii.xlsx')
        """
        # Cas du chargement depuis S3
        if bucket is not None:
            # Extraction de kwargs spécifiques à S3
            s3_kwargs = {
                k: kwargs.pop(k)
                for k in [
                    "aws_access_key_id",
                    "aws_secret_access_key",
                    "aws_session_token",
                    "endpoint_url",
                    "verify",
                ]
                if k in kwargs
            }
            # Connection à S3 si nécessaire
            if not hasattr(self, "s3"):
                self.connect(**s3_kwargs)
            # Utilise la méthode de chargement depuis S3 du parent
            return super().load(bucket=bucket, key=filepath, **kwargs)
        # Cas du chargement en local
        else:
            return load_local(filepath=filepath, **kwargs)
