# Importation des modules
# Modules de base
from typing import Optional
# Modules de package
from .local.saver import save_local
from .s3.saver import S3Saver


# Classe de sauvegarde de données en local ou sur S3
class Saver(S3Saver):
    """A unified class for saving JSON data to S3 or local storage.

    Saves to Amazon S3 when ``bucket`` is supplied, to the local filesystem
    otherwise. Only ``.json`` files are supported.

    Args:
        s3_package (str, optional): The package to use for S3 connections
            (``'s3fs'`` or ``'boto3'``). Defaults to ``"boto3"``.

    Attributes:
        s3: The S3 connection object, initialised lazily.

    Examples:
        Save a dict to S3:
        >>> saver = Saver(s3_package='boto3')
        >>> saver.save(
        ...     filepath='data/registry.json',
        ...     bucket='my-bucket',
        ...     obj={'key': 'value'},
        ...     aws_access_key_id='YOUR_KEY',
        ...     aws_secret_access_key='YOUR_SECRET'
        ... )

        Save a dict locally:
        >>> saver = Saver()
        >>> saver.save(filepath='data/registry.json', obj={'key': 'value'}, indent=2)
    """

    # Initialisation
    def __init__(self, s3_package: Optional[str] = "boto3"):
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
        obj: Optional[object] = None,
        bucket: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Save a JSON-serialisable object to S3 or local storage.

        Args:
            filepath (str): Path for saving the file. For S3, this is the object
                key within the bucket; for local storage, the filesystem path.
            obj: Any JSON-serialisable object to save.
            bucket (str, optional): S3 bucket name. If ``None``, saves to local
                storage.
            **kwargs: Additional arguments passed to the underlying saver.
                For S3: ``aws_access_key_id``, ``aws_secret_access_key``,
                ``aws_session_token``, ``endpoint_url``, ``verify``.
                For both: forwarded to ``json.dump`` / ``json.dumps``
                (e.g. ``indent``, ``ensure_ascii``).

        Raises:
            ValueError: If the file extension is not ``.json``.
            TypeError: If ``obj`` is not JSON-serialisable.
            IOError: If there are issues writing to local storage.
            botocore.exceptions.ClientError: If there are S3 access issues.

        Examples:
            Save a dict to S3:
            >>> saver.save(
            ...     filepath='data/registry.json',
            ...     bucket='my-bucket',
            ...     obj={'key': 'value'},
            ...     indent=2,
            ... )

            Save a dict locally:
            >>> saver.save(
            ...     filepath='data/registry.json',
            ...     obj={'key': 'value'},
            ...     indent=2,
            ... )
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
            # Utilise la méthode de sauvegarde sur S3 du parent
            super().save(bucket=bucket, key=filepath, obj=obj, **kwargs)
        # Cas de la sauvegarde en local
        else:
            save_local(filepath=filepath, obj=obj, **kwargs)
