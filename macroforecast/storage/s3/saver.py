# Importation des modules
from json import dumps
from typing import Optional

# Importation du module de connection
from ._connection import _S3Connection


# Classe de sauvegarde de données JSON sur un Bucket S3
class S3Saver(_S3Connection):
    """Save JSON data to Amazon S3 buckets.

    Args:
        s3_package (str, optional): Package to use for S3 connections
            (``'s3fs'`` or ``'boto3'``). Defaults to ``"boto3"``.

    Attributes:
        s3: The S3 connection object (initialised lazily).
        s3_package (str): The package being used for S3 connectivity.

    Examples:
        Save a dict to S3 using boto3:
        >>> saver = S3Saver()
        >>> saver.connect(
        ...     aws_access_key_id='YOUR_KEY',
        ...     aws_secret_access_key='YOUR_SECRET'
        ... )
        >>> saver.save(bucket='my-bucket', key='path/to/file.json', obj={'a': 1})
    """

    # Initialisation
    def __init__(self, s3_package: Optional[str] = "boto3") -> None:
        """Initialize the S3Saver with specified S3 package.

        Args:
            s3_package (str, optional): Package to use for S3 connections.
                Must be either ``'s3fs'`` or ``'boto3'``. Defaults to ``"boto3"``.
        """
        # Initialisation du parent
        super().__init__(s3_package=s3_package)

    # Méthode de connexion
    def connect(self, **kwargs) -> None:
        """Establish a connection to the S3 bucket.

        Args:
            **kwargs: Additional keyword arguments for establishing the connection.
        """
        # Etablissement d'une connection
        return self._connect(**kwargs)

    # Méthode de sauvegarde
    def save(
        self, bucket: str, key: str, obj: Optional[object] = None, **kwargs
    ) -> None:
        """Save a JSON-serialisable object to an S3 object.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The S3 object key. Must end in ``.json``.
            obj: Any JSON-serialisable object to save.
            **kwargs: Additional arguments forwarded to ``json.dumps``
                (e.g. ``indent``, ``ensure_ascii``).

        Raises:
            ValueError: If the key does not end in ``.json``.
            TypeError: If ``obj`` is not JSON-serialisable.

        Examples:
            >>> saver.save(
            ...     bucket='my-bucket',
            ...     key='data/registry.json',
            ...     obj={'key': 'value'},
            ...     indent=2,
            ... )
        """
        # Extraction de l'extension
        extension = key.rsplit(".", 1)[-1].lower()
        # Vérification que l'extion est supportée
        if extension != "json":
            raise ValueError(
                f"Unsupported extension '.{extension}': only '.json' files are supported."
            )
        # Etablissement d'une connexion si nécessaire
        if not hasattr(self, "s3"):
            self.connect()
        # Sérialisation et upload
        body = dumps(obj, **kwargs)
        if self.s3_package == "boto3":
            self.s3.put_object(Bucket=bucket, Key=key, Body=body)
        elif self.s3_package == "s3fs":
            with self.s3.open(f"{bucket}/{key}", "w") as s3_file:
                s3_file.write(body)
