# Importation des modules
# Modules de base
import os
import tempfile
from pathlib import Path
from typing import Optional, Union
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
        filepath: Union[str, Path],
        obj: Optional[object] = None,
        bucket: Optional[str] = None,
        atomic: bool = True,
        **kwargs,
    ) -> None:
        """Save a JSON-serialisable object to S3 or local storage.

        Args:
            filepath (str or Path): Path for saving the file. For S3, this is the
                object key within the bucket — a ``Path`` is converted to its
                POSIX form, so a registry path built on Windows still addresses
                the right key; for local storage, the filesystem path.
            obj: Any JSON-serialisable object to save.
            bucket (str, optional): S3 bucket name. If ``None``, saves to local
                storage.
            atomic (bool, optional): If ``True``, the local write goes through a
                temporary file in the destination directory, renamed over the
                target, so a crash never leaves a half-written file. Ignored on
                S3, where the object PUT is already atomic. Defaults to ``True``.
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

            Save a dict locally without the temporary-file dance:
            >>> saver.save(
            ...     filepath='data/registry.json',
            ...     obj={'key': 'value'},
            ...     atomic=False,
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
            super().save(
                bucket=bucket, key=Path(filepath).as_posix(), obj=obj, **kwargs
            )
        # Cas de la sauvegarde en local, écriture directe
        elif not atomic:
            save_local(filepath=str(filepath), obj=obj, **kwargs)
        # Cas de la sauvegarde en local, écriture atomique
        else:
            path = Path(filepath)
            # Création du dossier parent si nécessaire
            path.parent.mkdir(parents=True, exist_ok=True)
            # Extension du temporaire reprise de la destination : la validation de
            # format reste ainsi celle du fichier réellement demandé
            fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=path.suffix)
            # Fermeture immédiate du descripteur : save_local ouvre le fichier lui-même
            os.close(fd)
            try:
                # Écriture dans le temporaire puis remplacement de la destination
                save_local(filepath=tmp_name, obj=obj, **kwargs)
                os.replace(tmp_name, str(path))
            except Exception:
                # Nettoyage du fichier temporaire en cas d'échec
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)
                raise
