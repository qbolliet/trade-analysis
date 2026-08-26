# Importation des modules
from typing import Optional

import pandas as pd

# Module de sauvegarde de fichiers en local
from .local.saver import save_local
# Module de sauvegarde de fichiers sur S3
from .s3.saver import S3Saver


# Classe générale de sauvegarde des données
class Saver(S3Saver):
    """A unified class for saving parquet data to S3 or local storage.

    Saves to Amazon S3 when ``bucket`` is supplied, to the local filesystem
    otherwise. Only ``.parquet`` files are supported.

    Args:
        s3_package (str, optional): The package to use for S3 connections
            (``'s3fs'`` or ``'boto3'``). Defaults to ``"boto3"``.

    Attributes:
        s3: The S3 connection object, initialised lazily.

    Examples:
        Save a DataFrame to S3:
        >>> saver = Saver(s3_package='boto3')
        >>> saver.save(
        ...     filepath='data/table.parquet',
        ...     bucket='my-bucket',
        ...     obj=df,
        ...     aws_access_key_id='YOUR_KEY',
        ...     aws_secret_access_key='YOUR_SECRET'
        ... )

        Save a DataFrame locally:
        >>> saver = Saver()
        >>> saver.save(filepath='data/table.parquet', obj=df)
    """

    # Initialisation
    def __init__(self, s3_package: Optional[str] = "boto3") -> None:
        """Initialize the Saver with specified S3 package.

        Args:
            s3_package (str, optional): Package to use for S3 connections.
                Must be either 's3fs' or 'boto3'. Defaults to "boto3".
        """
        super().__init__(s3_package=s3_package)

    # Méthode de sauvegarde des données
    def save(
        self,
        filepath: str,
        obj: Optional[pd.DataFrame] = None,
        bucket: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Save a DataFrame to S3 or local storage in Parquet format.

        Args:
            filepath (str): Path for saving the file. For S3, this is the object
                key within the bucket; for local storage, the filesystem path.
            obj: DataFrame to save.
            bucket (str, optional): S3 bucket name. If ``None``, saves to local
                storage.
            **kwargs: Additional arguments passed to the underlying saver.
                For S3: ``aws_access_key_id``, ``aws_secret_access_key``,
                ``aws_session_token``, ``endpoint_url``, ``verify``.
                For both: forwarded to ``pandas.DataFrame.to_parquet``.

        Raises:
            ValueError: If the file extension is not ``.parquet``.
            IOError: If there are issues writing to local storage.
            botocore.exceptions.ClientError: If there are S3 access issues.

        Examples:
            Save a DataFrame to S3:
            >>> saver.save(
            ...     filepath='data/table.parquet',
            ...     bucket='my-bucket',
            ...     obj=df,
            ... )

            Save a DataFrame locally:
            >>> saver.save(filepath='data/table.parquet', obj=df)
        """
        # Cas de la sauvegarde sur S3
        if bucket is not None:
            # Extraction de certains kwargs spécifiques à S3
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
            # Connection au S3 si nécessaire
            if not hasattr(self, "s3"):
                self.connect(**s3_kwargs)
            # Utilisation de la méthode de sauvegarde sur S3 du parent
            super().save(bucket=bucket, key=filepath, obj=obj, **kwargs)
        # Cas de la sauvegarde en local
        else:
            save_local(filepath=filepath, obj=obj, **kwargs)
