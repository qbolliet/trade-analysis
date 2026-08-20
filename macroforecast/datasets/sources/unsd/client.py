"""UNSD classification correspondence client.

High-level client for the correspondence tables published by the United
Nations Statistics Division, converting the workbooks to pandas DataFrames on a
canonical schema. Like UN Comtrade — and unlike Eurostat and OECD — UNSD does
not follow the SDMX conventions: there is neither a DSD nor a structure
endpoint, only static files on a web server, so the catalogue of published
tables is declared in ``parameters/unsd.json``.

It inherits from :class:`~macroforecast.datasets.core.client.APIClient` for the
shared HTTP plumbing (retry session, ``close``) and mirrors the other clients'
shape. Caching is deliberately left out: the tables are invariant once
published, and the pipeline caches the normalised tables to Parquet on the
script side.

The tables are available at:
https://unstats.un.org/unsd/classifications/Econ/
"""
# Importation des modules
# Modules de base
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, TYPE_CHECKING
from urllib.parse import quote

# Module de manipulation de données
import pandas as pd

# Modules du package
from ...core.client import APIClient
from . import parsing
from .formats import CORRESPONDENCE_KINDS, table_key

if TYPE_CHECKING:
    from .queries import UNSDCorrespondenceRequest

# Initialisation du logger
logger = logging.getLogger(__name__)


# Chargement des paramètres
with open(Path(__file__).parents[4] / "parameters" / "unsd.json", "r", encoding="utf-8") as f:
    PARAMETERS: Dict[str, Any] = json.load(f)


# Classe de récupération des tables de correspondance de nomenclatures de l'UNSD
class UNSDClient(APIClient):
    """Client for the UNSD classification correspondence tables.

    Downloads and normalises the correspondence workbooks, exposing
    an API homogeneous with the other provider clients
    (:class:`ComtradeClient`, :class:`EurostatClient`).

    Args:
        base_url: Base URL of the UNSD correspondence-table directory.
        timeout: Request timeout in seconds. Generous by default: the workbooks
            weigh up to a megabyte.
        max_retries: Maximum number of HTTP retry attempts (inherited).
        backoff_factor: Backoff factor between retries (inherited).

    Examples:
        >>> client = UNSDClient()
        >>> # Catalogue des tables déclarées
        >>> df_tables = client.list_available_tables()
        >>> len(df_tables)
        21
        >>> # Table de conversion descendante
        >>> df_table = client.get_correspondence(
        ...     "HS2022", "HS2017",
        ... )  # doctest: +SKIP
    """

    # Nom du fichier de configuration (parité avec PROVIDER_CONFIG_NAME des clients SDMX)
    PROVIDER_CONFIG_NAME = "unsd"

    # URL de base par défaut du répertoire de tables de l'UNSD
    DEFAULT_BASE_URL = "https://unstats.un.org/unsd/classifications/Econ/tables"

    # Initialisation
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 120,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        # Initialisation de la couche HTTP partagée (session, retry, close)
        super().__init__(
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

    # ──────────────────────────────────────────────────────────────────
    # Résolution du catalogue
    # ──────────────────────────────────────────────────────────────────

    # Méthode de résolution du nom de fichier d'une paire de millésimes
    def _table_filename(self, source: str, target: str) -> str:
        """Resolve the published file name of a source/target vintage pair.

        The UNSD file naming is irregular across vintages — six distinct naming
        families coexist — so the names are declared one by one in the
        ``TABLES`` section of ``parameters/unsd.json`` rather than generated
        from a pattern.

        Args:
            source: Source classification (e.g. ``"HS2022"``).
            target: Target classification (e.g. ``"HS2017"``).

        Returns:
            The published file name, spaces included.

        Raises:
            KeyError: If the pair is not published. Only downward pairs are;
                an upward pair fails here rather than 404-ing later.

        Examples:
            >>> UNSDClient()._table_filename("HS2022", "HS2017")
            'HS2022toHS2017ConversionAndCorrelationTables.xlsx'
        """
        # Recherche de la paire dans le catalogue déclaré
        tables: Dict[str, str] = PARAMETERS["TABLES"]
        key = table_key(source, target)
        # Message d'erreur si la table de correspondance n'est pas trouvée dans le registre
        if key not in tables:
            raise KeyError(
                f"No correspondence table published for '{key}'. Only downward "
                f"pairs exist. Available pairs: {sorted(tables)}."
            )
        return tables[key]

    # Méthode de construction de l'URL d'une table
    def _table_url(self, source: str, target: str) -> str:
        """Build the download URL of a source/target vintage pair.

        Args:
            source: Source classification (e.g. ``"HS2022"``).
            target: Target classification (e.g. ``"HS2017"``).

        Returns:
            The absolute, percent-encoded download URL.

        Raises:
            KeyError: If the pair is not published.

        Examples:
            >>> UNSDClient()._table_url("HS2012", "HS2007")
            'https://unstats.un.org/unsd/classifications/Econ/tables/HS%202012%20to%20HS%202007%20Correlation%20and%20conversion%20tables.xls'
        """
        # Encodage du nom de fichier puis concaténation à l'URL de base
        return f"{self.base_url}/{quote(self._table_filename(source, target))}"

    # Méthode de listing du catalogue des tables déclarées
    def list_available_tables(self) -> pd.DataFrame:
        """Return the catalogue of declared correspondence tables.

        Returns:
            DataFrame with one row per published pair and the columns
            ``source_classification``, ``target_classification``, ``filename``,
            ``extension`` and ``url``, sorted by descending vintage.

        Examples:
            >>> df_tables = UNSDClient().list_available_tables()
            >>> df_tables.columns.tolist()
            ['source_classification', 'target_classification', 'filename', 'extension', 'url']
            >>> len(df_tables)
            21
        """
        # Construction d'une ligne par paire déclarée
        records: List[Dict[str, str]] = []
        # Parcours des tables référencées dans le catalogue
        for key, filename in PARAMETERS["TABLES"].items():
            source, target = key.split("-")
            records.append(
                {
                    "source_classification": source,
                    "target_classification": target,
                    "filename": filename,
                    "extension": f".{filename.rsplit('.', 1)[-1]}",
                    "url": f"{self.base_url}/{quote(filename)}",
                }
            )

        # Tri par millésimes décroissants (les paires les plus récentes d'abord)
        df_tables = pd.DataFrame(records, columns=[
            "source_classification",
            "target_classification",
            "filename",
            "extension",
            "url",
        ])
        return df_tables.sort_values(
            ["source_classification", "target_classification"], ascending=False
        ).reset_index(drop=True)

    # ──────────────────────────────────────────────────────────────────
    # Récupération des tables de correspondance
    # ──────────────────────────────────────────────────────────────────

    # Méthode de téléchargement d'un classeur brut
    def download_raw(self, source: str, target: str) -> Tuple[bytes, str]:
        """Return the untouched workbook and its extension.

        Args:
            source: Source classification (e.g. ``"HS2022"``).
            target: Target classification (e.g. ``"HS2017"``).

        Returns:
            Tuple ``(workbook bytes, extension)``, the extension including its
            leading dot (``".xls"`` or ``".xlsx"``).

        Raises:
            KeyError: If the pair is not published.
            requests.exceptions.RequestException: On download failure.

        Examples:
            >>> content, extension = UNSDClient().download_raw(
            ...     "HS2022", "HS2017",
            ... )  # doctest: +SKIP
        """
        # Résolution du fichier publié pour la paire demandée
        filename = self._table_filename(source, target)
        extension = f".{filename.rsplit('.', 1)[-1]}"

        # Téléchargement (l'URL est construite par APIClient.get à partir du
        # nom de fichier encodé, relatif à l'URL de base)
        # Logging
        logger.info(f"Downloading UNSD correspondence table '{filename}'")
        # Exécution de la requête
        response = self.get(quote(filename))

        return response.content, extension

    # Méthode de récupération d'une table de correspondance normalisée
    def get_correspondence(
        self, source: str, target: str, *, kind: str = "conversion"
    ) -> pd.DataFrame:
        """Fetch and normalise one correspondence table.

        Args:
            source: Source classification (e.g. ``"HS2022"``).
            target: Target classification (e.g. ``"HS2017"``).
            kind: Sheet to read. ``"conversion"`` (default) returns the
                functional correspondence — one row per source code — and is
                what data conversion must use. ``"correlation"`` returns the
                complete annotated correspondence, whose relationship
                distribution measures the information the conversion loses.

        Returns:
            DataFrame on the canonical schema ``source_classification``,
            ``source_code``, ``target_classification``, ``target_code``,
            ``relationship``. The last column is ``pd.NA`` throughout for
            ``kind="conversion"``: the Conversion sheet carries no annotation.

        Raises:
            KeyError: If the pair is not published.
            ValueError: If ``kind`` is unknown, if the workbook cannot be
                parsed, or if the parsed Conversion table is not a function
                (duplicated source code or code of wrong length).

        Examples:
            >>> df_table = UNSDClient().get_correspondence(
            ...     "HS2022", "HS2017",
            ... )  # doctest: +SKIP
        """
        # Vérification des arguments
        if kind not in CORRESPONDENCE_KINDS:
            raise ValueError(
                f"Unsupported correspondence kind '{kind}'. "
                f"Expected one of {CORRESPONDENCE_KINDS}."
            )

        # Téléchargement du classeur et normalisation de la feuille demandée
        filename = self._table_filename(source, target)
        content, extension = self.download_raw(source=source, target=target)
        df_table = parsing.parse_correspondence(
            content=content,
            extension=extension,
            source=source,
            target=target,
            kind=kind,
            filename=filename,
        )

        # Validation de la table de conversion
        if kind == "conversion":
            parsing.validate_conversion(df_table=df_table, filename=filename)

        # Logging
        logger.info(
            f"Parsed {len(df_table)} rows from the {kind} sheet of "
            f"'{filename}'"
        )

        return df_table

    # Méthode d'exécution d'un objet requête
    def execute_query(self, query: "UNSDCorrespondenceRequest") -> pd.DataFrame:
        """Execute a :class:`UNSDCorrespondenceRequest` and return its DataFrame.

        Parity with the SDMX clients' ``execute_query``.

        Args:
            query: UNSD correspondence request.

        Returns:
            DataFrame with the normalised correspondence table.

        Raises:
            KeyError: If the requested pair is not published.
            ValueError: If the workbook cannot be parsed or fails validation.
        """
        # Délégation à get_correspondence avec les champs de sélection
        return self.get_correspondence(
            source=query.source,
            target=query.target,
            kind=query.kind,
        )

    # ──────────────────────────────────────────────────────────────────
    # Fermeture des ressources
    # ──────────────────────────────────────────────────────────────────

    # Méthode de fermeture de la connexion HTTP
    def close(self) -> None:
        """Close the HTTP session and release resources."""
        # Fermeture de la session héritée d'APIClient
        super().close()
        # Logging
        logger.info("UNSD client closed")
