"""UN Comtrade data client.

High-level client for querying UN Comtrade international-trade data and
converting responses to pandas DataFrames. Unlike Eurostat and OECD, UN
Comtrade does not follow the SDMX conventions, so this client wraps the
official ``comtradeapicall`` library rather than building SDMX endpoints.

It inherits from :class:`~macroforecast.datasets.core.client.APIClient` for the
shared HTTP plumbing (retry session, ``close``) and mirrors the SDMX clients'
shape: configuration (rate limiter, structures) is loaded from
``parameters/comtrade.json``, and the automated bulk download lives in a
dedicated script (``scripts/download_comtrade.py``) rather than in the client.

The methodology is available at:
https://comtradeapi.un.org/files/v1/app/wiki/MethodologyGuideforComtradePlus.pdf
"""
# Importation des modules
# Modules de base
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

# Module de l'API UN Comtrade
import comtradeapicall
import numpy as np
import pandas as pd

# Modules du package
from ...core.client import APIClient
from ...core.rate_limiter import CompositeRateLimiter, RateLimiter, build_rate_limiter
from ...core.structures import DataflowStructure, DataflowStructureRegistry
from . import parsing
from .formats import (
    AGENCY_ID,
    DIMENSIONS,
    SUBDIVISION_ORDER,
    VALID_FREQUENCIES,
    api_arg_for,
    category_for,
)

if TYPE_CHECKING:
    from .queries import ComtradeQueryRequest

# Initialisation du logger
logger = logging.getLogger(__name__)


# Chargement des paramètres 
with open(Path(__file__).parents[4] / "parameters" / "comtrade.json", "r", encoding="utf-8") as f:
    PARAMETERS: Dict[str, Any] = json.load(f)


# Classe de récupération des données de commerce international du UN Comtrade
class ComtradeClient(APIClient):
    """High-level client for UN Comtrade international-trade data.

    Fetches, subdivides (to respect the per-call record limit) and aggregates
    tariffline trade data from UN Comtrade, exposing an API homogeneous with the
    SDMX clients (:class:`EurostatClient`, :class:`OECDClient`).

    Args:
        base_url: UN Comtrade API base URL (used for the inherited HTTP
            session; ``comtradeapicall`` builds its own request URLs).
        timeout: Request timeout in seconds.
        subscription_key: Comtrade subscription key. Falls back to the
            ``COMTRADE_SUBSCRIPTION_KEY`` environment variable when ``None``.
        proxy: Proxy to use through the comtradeapicall API.
        structure_registry: Optional registry for dataflow structures. When
            ``None`` a new registry is created and populated from the
            ``STRUCTURES`` section of ``parameters/comtrade.json``.
        rate_limiter: Optional rate limiter. When ``None`` and
            ``auto_load_rate_limit`` is ``True``, it is built from the
            ``RATE_LIMIT`` section of ``parameters/comtrade.json`` (a list of
            limits → :class:`CompositeRateLimiter` enforcing 1 req/s and
            500 req/day).
        auto_load_rate_limit: Whether to load the rate limiter automatically.
        max_retries: Maximum number of HTTP retry attempts (inherited).
        backoff_factor: Backoff factor between retries (inherited).

    Examples:
        >>> client = ComtradeClient()
        >>> # Métadonnées d'une catégorie de référence
        >>> reporters = client.get_metadata(category="reporter")  # doctest: +SKIP
        >>> # Données tariffline
        >>> df, meta = client.get_data(
        ...     reporters="FRA", products=["010121"],
        ...     periods="2023", frequency="annual",
        ... )  # doctest: +SKIP
    """

    # Nom du fichier de configuration (parité avec PROVIDER_CONFIG_NAME des clients SDMX)
    PROVIDER_CONFIG_NAME = "comtrade"

    # URL de base par défaut de l'API UN Comtrade
    DEFAULT_BASE_URL = "https://comtradeapi.un.org"

    # Initialisation
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 120,
        subscription_key: Optional[str] = None,
        proxy: Optional[str] = None,
        structure_registry: Optional[DataflowStructureRegistry] = None,
        rate_limiter: Optional[Union[RateLimiter, CompositeRateLimiter]] = None,
        auto_load_rate_limit: bool = True,
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

        # Clé de souscription (argument ou variable d'environnement)
        self.subscription_key = (
            subscription_key
            if subscription_key is not None
            else os.getenv("COMTRADE_SUBSCRIPTION_KEY")
        )

        # Proxy optionnel (hôte:port) propagé à comtradeapicall
        self.proxy = proxy

        # Rate limiter (argument ou chargement automatique depuis la configuration)
        if auto_load_rate_limit and rate_limiter is None:
            rate_limiter = self._load_rate_limiter()
        self.rate_limiter: Optional[Union[RateLimiter, CompositeRateLimiter]] = (
            rate_limiter
        )

        # Registre des structures (argument ou construction depuis les paramètres)
        if structure_registry is not None:
            self.structure_registry = structure_registry
        else:
            self.structure_registry = DataflowStructureRegistry()
            # Chargement des structures déclarées (pas d'endpoint de structure côté API)
            self.structure_registry.load_from_dict(PARAMETERS)

        # Compteur d'appels API (à des fins de logging uniquement ; le quota est
        # garanti par le rate limiter)
        self.api_calls = 0

        # Cache des dates de dernière publication par signature de disponibilité
        # (pour fetch_updates) : évite un appel de disponibilité par lot de produits
        # d'une même période
        self._availability_cache: Dict[Tuple, Optional[datetime]] = {}

    # ──────────────────────────────────────────────────────────────────
    # Chargement de la configuration
    # ──────────────────────────────────────────────────────────────────

    # Méthode de chargement du rate limiter depuis la configuration
    def _load_rate_limiter(
        self,
    ) -> Optional[Union[RateLimiter, CompositeRateLimiter]]:
        """Build the rate limiter from the ``RATE_LIMIT`` configuration section.

        The section is a list of ``{requests, unit, count}`` limits, so a
        :class:`CompositeRateLimiter` enforcing every limit simultaneously is
        returned (cf. :func:`build_rate_limiter`).

        Returns:
            A rate limiter, or ``None`` when no configuration is found.
        """
        # Extraction de la section RATE_LIMIT
        config = PARAMETERS.get("RATE_LIMIT")
        if config is None:
            logger.debug("No RATE_LIMIT configuration found")
            return None
        try:
            logger.info(
                f"Loading rate limiter from "
                f"parameters/{self.PROVIDER_CONFIG_NAME}.json"
            )
            return build_rate_limiter(config)
        except Exception as e:
            # Échec non bloquant de chargement
            logger.warning(f"Could not load rate limiter: {e}")
            return None

    # Propriété d'URL de proxy formatée pour comtradeapicall
    @property
    def _proxy_url(self) -> Optional[str]:
        """Proxy URL passed to ``comtradeapicall`` (``None`` when unset)."""
        # Aucun proxy → None ; sinon préfixe http://
        if self.proxy is None:
            return None
        return f"http://{self.proxy}"

    # Méthode auxiliaire d'application du rate limiter avant un appel API
    def _acquire(self) -> None:
        """Block until the rate limiter allows the next API call (if any)."""
        # Application du rate limiter si configuré
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()

    # ──────────────────────────────────────────────────────────────────
    # Méthodes auxiliaires de preprocessing
    # ──────────────────────────────────────────────────────────────────

    # Méthode auxiliaire de preprocessing des codes
    def _preprocess_codes(
        self, codes: Union[List[int], List[str], int, str, None]
    ) -> Optional[str]:
        """Preprocess country or product codes into the Comtrade CSV format.

        Args:
            codes: Single code or list of codes. Lists are joined with commas,
                integers are stringified, strings and ``None`` are returned
                unchanged.

        Returns:
            Comma-separated string of codes, or ``None``.

        Examples:
            >>> ComtradeClient._preprocess_codes(None, [1, 2, 3])
            '1,2,3'
            >>> ComtradeClient._preprocess_codes(None, "FRA")
            'FRA'
        """
        # Liste de codes → concaténation par des virgules
        if isinstance(codes, list):
            codes = ",".join([str(e) for e in codes])
        # Entier → conversion en chaîne
        elif isinstance(codes, int):
            codes = str(codes)
        # Chaîne ou None → renvoyé tel quel
        return codes

    # Méthode auxiliaire de validation d'une subdivision
    def _validate_subdivision(self, subdivision: Union[str, None]) -> bool:
        """Validate whether a parameter can be split into smaller requests.

        Args:
            subdivision: Comma-separated string with more than one value, or
                ``None``.

        Returns:
            ``True`` if ``None`` or containing multiple values, ``False``
            otherwise.

        Examples:
            >>> ComtradeClient._validate_subdivision(None, "USA,CAN,MEX")
            True
            >>> ComtradeClient._validate_subdivision(None, "USA")
            False
        """
        # None ou liste de plus d'un élément → divisible
        if subdivision is None:
            return True
        elif isinstance(subdivision, str):
            # Identification des différents items de la subdivision
            list_items = subdivision.split(",")
            return len(list_items) > 1
        else:
            return False

    # Méthode auxiliaire d'extraction des codes valides d'une catégorie
    def _extract_codes(self, category: str) -> list:
        """Extract valid codes for a reference category.

        Args:
            category: One of ``"flow"``, ``"reporter"``, ``"partner"`` or
                ``"cmd:HS"``.

        Returns:
            List of valid codes for the category.

        Raises:
            ValueError: If ``category`` is not supported.
        """
        # Extraction des métadonnées puis des codes (logique pure déléguée à parsing)
        df = self.get_metadata(category=category)
        return parsing.extract_codes(df, category)

    # ──────────────────────────────────────────────────────────────────
    # Récupération des données tariffline
    # ──────────────────────────────────────────────────────────────────

    # Méthode auxiliaire indiquant si une dimension peut être subdivisée
    def _can_subdivide(self, name: str, value: Union[str, None]) -> bool:
        """Tell whether a flow dimension can be split into smaller requests.

        A dimension is divisible when it carries several explicit values, or
        when it is unset (``None``) but its valid codes can be enumerated (its
        :data:`DIMENSIONS` ``category`` is not ``None``). Dimensions that are
        unset and cannot be enumerated (e.g. ``periods``, ``customs``, ``mot``)
        are not candidates.

        Args:
            name: Friendly dimension name (a key of :data:`DIMENSIONS`).
            value: Current value of the dimension (comma-separated or ``None``).

        Returns:
            ``True`` if the dimension can be subdivided, ``False`` otherwise.
        """
        # Valeur non divisible (un seul item)
        if not self._validate_subdivision(subdivision=value):
            return False
        # Valeur absente non énumérable (pas de catégorie de métadonnées)
        if value is None and category_for(name) is None:
            return False
        return True

    # Méthode auxiliaire de subdivision récursive d'une requête
    def _divide_request(
        self,
        subdivision: str,
        selection: Dict[str, Union[str, None]],
        fixed: Dict[str, object],
    ) -> Tuple[pd.DataFrame, dict]:
        """Divide a request that exceeds the per-call record limit.

        Splits the requested values of ``subdivision`` into two halves and
        re-issues two recursive :meth:`get_data` calls, concatenating the
        results. Generic over every flow-defining dimension declared in
        :data:`DIMENSIONS`.

        Args:
            subdivision: Friendly dimension to split (a key of
                :data:`DIMENSIONS`).
            selection: Current values of the flow dimensions (friendly name →
                comma-separated value or ``None``).
            fixed: Non-dimensional parameters shared by both halves
                (``type_code``, ``classification``, ``frequency``, …).

        Returns:
            Tuple ``(DataFrame, request_metadata)`` combining both halves.

        Raises:
            ValueError: If ``subdivision`` is invalid or cannot be divided
                further.
        """
        # Vérification de la validité de la subdivision
        if subdivision not in DIMENSIONS:
            raise ValueError(
                f"Invalid subdivision : {subdivision}. "
                f"Should be one of {list(DIMENSIONS)}"
            )

        # Extraction des items sur lesquels effectuer la subdivision
        items = selection.get(subdivision)
        if not self._validate_subdivision(subdivision=items):
            raise ValueError(
                f"Unable to further truncate the request with selection : {selection}"
            )

        # Si None, requête des valeurs valides via la catégorie de métadonnées
        if items is None:
            category = category_for(subdivision)
            if category is None:
                raise ValueError(
                    f"Unable to request the valid values for '{subdivision}'"
                )
            list_items = self._extract_codes(category=category)
        else:
            list_items = items.split(",")

        # Construction des deux sous-listes
        mid = len(list_items) // 2
        list_items1, list_items2 = list_items[:mid], list_items[mid:]

        # Requêtes récursives sur chaque sous-liste (la dimension scindée est
        # surchargée, les autres paramètres sont propagés tels quels)
        df1, request_metadata1 = self.get_data(
            **{**fixed, **selection, subdivision: list_items1}
        )
        df2, request_metadata2 = self.get_data(
            **{**fixed, **selection, subdivision: list_items2}
        )

        # Concaténation des jeux de données
        df = pd.concat([df1, df2], axis=0, ignore_index=True)
        # Concaténation des métadonnées : seules les dimensions scindées sont
        # accolées, les paramètres fixes restent identiques
        request_metadata = {
            k: (
                f"{request_metadata1[k]},{request_metadata2[k]}"
                if k in DIMENSIONS
                else request_metadata1[k]
            )
            for k in request_metadata1.keys()
        }
        return df, request_metadata
    
    # Méthode auxiliaire de construction des périodes à partir du début et de la fin
    def _build_periods_from_boundaries(self, period_start: str, frequency: str, period_end: Optional[str]=None) -> List[str]:
        # Initialisation des periods
        periods = []

        # Construction des périodes
        if (period_start is not None) & (period_end is not None):
            periods = pd.date_range(
                    start=period_start,
                    end=period_end,
                    freq="YS" if frequency == "annual" else "MS",
                ).strftime("%Y" if frequency == "annual" else "%Y%m").tolist()
        elif period_start is not None:
            periods = pd.date_range(
                    start=period_start,
                    end=datetime.today(),
                    freq="YS" if frequency == "annual" else "MS",
                ).strftime("%Y" if frequency == "annual" else "%Y%m").tolist()

        return periods


    # Méthode principale de récupération des données tariffline
    def get_data(
        self,
        flows: Optional[Union[List[str], str, None]] = ["M", "X"],
        products: Optional[Union[List[int], List[str], int, str, None]] = None,
        reporters: Optional[Union[List[int], List[str], int, str, None]] = None,
        partners: Optional[Union[List[int], List[str], int, str, None]] = None,
        partners2: Optional[Union[List[int], List[str], int, str, None]] = None,
        customs: Optional[Union[List[str], str, None]] = None,
        mot: Optional[Union[List[str], str, None]] = None,
        periods: Optional[Union[List[str], str, None]] = None,
        period_start: Optional[Union[str, None]] = None,
        period_end: Optional[Union[str, None]] = None,
        type_code: str = "C",
        classification: str = "HS",
        frequency: str = "annual",
        max_records: Optional[int] = None,
        format_output: str = "JSON",
        count_only: Optional[bool] = None,
        include_desc: bool = True,
    ) -> Tuple[pd.DataFrame, dict]:
        """Fetch tariffline data from UN Comtrade.

        Issues a single ``comtradeapicall._getTarifflineData`` request and, when
        the response hits the per-call record limit (``LIMIT``), recursively
        subdivides it over any flow-defining dimension declared in
        :data:`DIMENSIONS` until each chunk fits. All ``_getTarifflineData``
        arguments are exposed (with sensible defaults) so callers are not boxed
        into a narrow set of requests.

        Args:
            flows: Trade flow codes (e.g. ``["M", "X"]``).
            products: Product codes.
            reporters: Reporter country codes.
            partners: Partner country codes.
            partners2: Secondary partner codes.
            customs: Customs procedure codes.
            mot: Mode-of-transport codes.
            periods: Explicit periods (``YYYY`` or ``YYYYMM``).
            period_start: Start period (used when ``periods`` is omitted).
            period_end: End period (used when ``periods`` is omitted).
            type_code: Trade type (``"C"`` commodities or ``"S"`` services).
            classification: Classification code (``"HS"``, ``"SITC"``, …).
            frequency: Data frequency (``'monthly'`` or ``'annual'``).
            max_records: Maximum number of records returned per call.
            format_output: Response format (``"JSON"`` or ``"CSV"``).
            count_only: When ``True``, return only the record count.
            include_desc: Whether to include the variables' descriptions.

        Returns:
            Tuple ``(DataFrame, request_metadata)`` where ``request_metadata``
            records the resolved request parameters.

        Raises:
            ValueError: If ``frequency`` is invalid.
            RateLimitExceeded: Propagated from the rate limiter when the daily
                quota is exhausted (a partially subdivided request is therefore
                never recorded).

        Examples:
            >>> df, meta = client.get_data(
            ...     reporters="FRA", periods="2023", frequency="annual",
            ... )  # doctest: +SKIP
        """
        # Vérification de la cohérence des paramètres
        if frequency not in VALID_FREQUENCIES:
            raise ValueError(
                f"Invalid value for frequency : {frequency}. "
                f"Should be in {VALID_FREQUENCIES}"
            )

        # Mémorisation du nombre d'appels initial (pour le logging)
        initial_api_calls = self.api_calls

        # Preprocessing des périodes
        if isinstance(periods, list):
            periods = ",".join(periods)
        elif ((period_start is not None) & (period_end is not None)) | (period_start is not None):
            periods = ",".join(self._build_periods_from_boundaries(period_start=period_start, frequency=frequency, period_end=period_end))

        # Preprocessing des flux et des codes (pays, produits, douane, transport)
        selection: Dict[str, Union[str, None]] = {
            "flows": self._preprocess_codes(codes=flows),
            "products": self._preprocess_codes(codes=products),
            "reporters": self._preprocess_codes(codes=reporters),
            "partners": self._preprocess_codes(codes=partners),
            "partners2": self._preprocess_codes(codes=partners2),
            "customs": self._preprocess_codes(codes=customs),
            "mot": self._preprocess_codes(codes=mot),
            "periods": periods,
        }

        # Paramètres non dimensionnels propagés aux sous-requêtes (périodes déjà
        # résolues : period_start/period_end neutralisés pour éviter la ré-expansion)
        fixed: Dict[str, object] = {
            "type_code": type_code,
            "classification": classification,
            "frequency": frequency,
            "period_start": None,
            "period_end": None,
            "max_records": max_records,
            "format_output": format_output,
            "count_only": count_only,
            "include_desc": include_desc,
        }

        # Application du rate limiter avant l'appel API
        self._acquire()

        # Traduction des noms conviviaux vers les arguments de _getTarifflineData
        api_kwargs = {api_arg_for(name): value for name, value in selection.items()}

        # Requête des données tariffline via la lib officielle
        df = comtradeapicall._getTarifflineData(
            self.subscription_key,
            typeCode=type_code,
            freqCode="A" if frequency == "annual" else "M",
            clCode=classification,
            maxRecords=max_records,
            format_output=format_output,
            countOnly=count_only,
            includeDesc=include_desc,
            proxy_url=self._proxy_url,
            **api_kwargs,
        )
        # Incrément du compteur d'appels API
        self.api_calls += 1

        # Si la limite du nombre d'observations est atteinte, subdivision de la requête
        if len(df) >= PARAMETERS["LIMIT"]:
            # Première dimension divisible dans l'ordre de préférence
            subdivision = next(
                (
                    name
                    for name in SUBDIVISION_ORDER
                    if self._can_subdivide(name, selection.get(name))
                ),
                None,
            )
            if subdivision is not None:
                df, request_metadata = self._divide_request(
                    subdivision, selection, fixed
                )
            else:
                # Subdivision impossible : la requête est conservée telle quelle
                logger.warning(
                    "Unable to further truncate the request with selection : %s",
                    selection,
                )
                request_metadata = self._build_request_metadata(selection, fixed)
        else:
            # Pas de subdivision : construction des métadonnées de la requête
            request_metadata = self._build_request_metadata(selection, fixed)

        # Logging du nombre d'appels nécessaires pour finaliser la requête
        logger.info(
            "%d api calls needed to fetch tarifline data with selection : %s",
            self.api_calls - initial_api_calls,
            selection,
        )

        return df, request_metadata

    # Méthode auxiliaire de construction des métadonnées d'une requête
    @staticmethod
    def _build_request_metadata(
        selection: Dict[str, Union[str, None]], fixed: Dict[str, object]
    ) -> dict:
        """Build the metadata dictionary describing a resolved request.

        Args:
            selection: Resolved flow dimensions (friendly name → value).
            fixed: Resolved non-dimensional parameters.

        Returns:
            Dictionary of stringified request parameters.
        """
        # Sérialisation des dimensions puis des paramètres fixes
        metadata = {name: str(value) for name, value in selection.items()}
        metadata.update({name: str(value) for name, value in fixed.items()})
        return metadata

    # ──────────────────────────────────────────────────────────────────
    # Métadonnées et périodes
    # ──────────────────────────────────────────────────────────────────

    # Méthode de chargement des métadonnées d'une catégorie de référence
    def get_metadata(self, category: Optional[Union[str, None]] = None) -> pd.DataFrame:
        """Fetch metadata for a reference category from UN Comtrade.

        Args:
            category: Reference category. When ``None``, returns the registry
                of available categories.

        Returns:
            DataFrame with the metadata for the requested category.

        Raises:
            ValueError: If ``category`` is invalid or the data cannot be
                retrieved.

        Examples:
            >>> categories = client.get_metadata()  # doctest: +SKIP
            >>> reporters = client.get_metadata(category="reporter")  # doctest: +SKIP
        """
        # Application du rate limiter avant l'appel API
        self._acquire()

        # Chargement du registre des références
        metadata_index = comtradeapicall.listReference(
            category=category,
            proxy_url=self._proxy_url,
        )

        # Jeu de données vide → erreur avec les modalités valides
        if metadata_index.empty:
            # Requête de l'ensemble des possibilités
            metadata_options = comtradeapicall.listReference(
                category=None,
                proxy_url=self._proxy_url,
            )
            raise ValueError(
                f"Invalid 'category' : {category}. To get further information, "
                f"run with category=None. 'category' should be in "
                f"{metadata_options['category'].tolist()}."
            )

        # Catégorie non spécifiée → registre des méta-données
        if category is None:
            return metadata_index

        # Requête du fichier de la catégorie (session HTTP héritée d'APIClient)
        response = self.session.get(metadata_index["fileuri"].iloc[0])

        # Disjonction de cas suivant le statut de la requête
        if response.status_code == 200:
            # Extraction et normalisation des données
            data = response.json()
            return pd.json_normalize(data["results"])
        else:
            raise ValueError(
                f"Failed to retrieve data for category : {category}. "
                f"Status code: {response.status_code}"
            )

    # Méthode auxiliaire de validation du format d'une période
    def _validate_date(self, period: Union[str, int]) -> str:
        """Validate and format a date period string.

        Args:
            period: Period to validate (``YYYY``, ``YYYY-MM`` or
                ``YYYY-MM-DD``).

        Returns:
            Validated date string (``YYYY-MM-DD``).

        Raises:
            ValueError: If the period format is invalid.

        Examples:
            >>> ComtradeClient._validate_date(None, "2023")
            '2023-01-01'
            >>> ComtradeClient._validate_date(None, "2023-06")
            '2023-06-01'
        """
        # Conversion en chaîne et remplacement des caractères non numériques par '-'
        period = str(period)
        period = re.sub(r"\D", "-", period.strip())

        # La période doit avoir au minimum quatre chiffres (année)
        if len(period) < 4:
            raise ValueError("Period must be at least 4 digits long")

        # Complétion de la chaîne jusqu'à une longueur de 10 (YYYY-MM-DD)
        if len(period) == 4:
            period += "-01-01"
        elif len(period) == 7:
            period += "-01"
        elif len(period) != 10:
            raise ValueError(
                "Period must be in the format YYYY or YYYY-MM or YYYY-MM-DD"
            )
        return period

    # Méthode de construction des périodes valides
    def get_valid_periods(
        self,
        periods: Optional[Union[List[str], str, None]] = None,
        period_start: Optional[Union[str, None]] = None,
        period_end: Optional[Union[str, None]] = None,
        frequency: Optional[str] = "monthly",
    ) -> List[str]:
        """Generate the list of valid periods for trade-data requests.

        Args:
            periods: Explicit periods to intersect with the valid range.
            period_start: Start period (``YYYY``, ``YYYY-MM`` or
                ``YYYY-MM-DD``).
            period_end: End period.
            frequency: Data frequency (``'monthly'`` or ``'annual'``).

        Returns:
            List of valid periods in the API format (``YYYY`` or ``YYYYMM``).

        Raises:
            ValueError: If ``frequency`` is invalid.
        """
        # Vérification de la cohérence des paramètres
        if frequency not in VALID_FREQUENCIES:
            raise ValueError(
                f"Invalid value for frequency : {frequency}. "
                f"Should be in {VALID_FREQUENCIES}"
            )

        # Résolution des bornes de la plage (1962 → aujourd'hui par défaut)
        start = (
            self._validate_date(period=1962)
            if period_start is None
            else self._validate_date(period=period_start)
        )
        end = (
            datetime.today()
            if period_end is None
            else self._validate_date(period=period_end)
        )

        # Construction du champ des périodes valides
        valid_periods = (
            pd.date_range(
                start=start,
                end=end,
                freq="YS" if frequency == "annual" else "MS",
            )
            .strftime("%Y" if frequency == "annual" else "%Y%m")
            .tolist()
        )

        # La période en cours n'est jamais complète : retrait du dernier élément
        valid_periods = valid_periods[:-1]

        # Intersection avec les périodes en argument si renseignées
        if periods is not None:
            valid_periods = np.intersect1d(valid_periods, periods).tolist()

        return valid_periods

    # ──────────────────────────────────────────────────────────────────
    # Seam de téléchargement incrémental 
    # ──────────────────────────────────────────────────────────────────

    # Méthode de récupération de la disponibilité finale des données
    def get_final_data_availability(
        self,
        reporters: Optional[Union[List[int], List[str], int, str, None]] = None,
        periods: Optional[Union[List[str], str, None]] = None,
        frequency: str = "annual",
        type_code: str = "C",
        classification: str = "HS",
    ) -> pd.DataFrame:
        """Fetch the final-data availability for reporters and periods.

        Wraps ``comtradeapicall.getFinalDataAvailability``. Used by the download
        script to compare each release's ``lastReleased`` date with the last
        recorded download and decide whether a (reporter, period) couple must be
        refreshed.

        Args:
            reporters: Reporter codes (``None`` for all reporters).
            periods: Periods to check (``YYYY`` or ``YYYYMM``).
            frequency: Data frequency (``'annual'`` or ``'monthly'``).
            type_code: Trade type (``'C'`` or ``'S'``).
            classification: Classification code (``'HS'``, ...).

        Returns:
            DataFrame returned by ``getFinalDataAvailability``.
        """
        # Preprocessing des codes
        reporters = self._preprocess_codes(codes=reporters)
        if isinstance(periods, list):
            periods = ",".join([str(p) for p in periods])

        # Application du rate limiter avant l'appel API
        self._acquire()

        # Requête de la disponibilité finale des données
        return comtradeapicall.getFinalDataAvailability(
            subscription_key=self.subscription_key,
            typeCode=type_code,
            freqCode="A" if frequency == "annual" else "M",
            clCode=classification,
            reporterCode=reporters,
            period=periods,
        )
    
    # Méthode de résolution de la date de dernière publication d'une période
    def _period_last_released(
        self, query: "ComtradeQueryRequest"
    ) -> Optional[datetime]:
        """Return the most recent ``lastReleased`` date for a query's period.

        Wraps :meth:`get_final_data_availability` and memoises the result per
        availability signature (period, frequency, type, classification,
        reporters). As the signature is independent of the product batch, the
        many (period, product-batch) queries sharing a period trigger a single
        availability call.

        Args:
            query: Comtrade query request whose period is inspected.

        Returns:
            The most recent publication instant across the queried reporters,
            or ``None`` when the availability could not be resolved.
        """
        # Signature de disponibilité (indépendante du lot de produits)
        cache_key = (
            str(query.periods),
            query.frequency,
            query.type_code,
            query.classification,
            None if query.reporters is None else ",".join(map(str, query.reporters)),
        )

        # Court-circuit sur le cache
        if cache_key in self._availability_cache:
            return self._availability_cache[cache_key]

        try:
            # Disponibilité finale (tous les reporters de la requête) pour la période
            availability = self.get_final_data_availability(
                reporters=query.reporters,
                periods=query.periods,
                frequency=query.frequency,
                type_code=query.type_code,
                classification=query.classification,
            )
            # Dates de publication par reporter
            released = parsing.parse_availability_last_released(availability)
            dates = [value for value in released.values() if value is not None]
            # Date de publication la plus récente sur l'ensemble des reporters
            last_released = (
                pd.to_datetime(max(dates), utc=True).to_pydatetime() if dates else None
            )
        except Exception as e:
            # Échec non bloquant : la période sera rafraîchie par précaution
            logger.warning(
                "Could not fetch availability for %s: %s", query.periods, e
            )
            last_released = None

        # Mise en cache et renvoi
        self._availability_cache[cache_key] = last_released
        return last_released

    # Méthode de récupération incrémentale d'une requête
    def fetch_updates(
        self,
        query: "ComtradeQueryRequest",
        since: Optional[datetime],
        n_observations: int = 10,
    ) -> pd.DataFrame:
        """Fetch the data for a query, incrementally when possible.

        Provider seam consumed by the download orchestrator
        (:class:`~macroforecast.datasets.core.download.SDMXDownloader`). It
        encodes Comtrade's incremental strategy:

        - First download (``since`` is ``None``) → retrieve the full slice.
        - Subsequent download (``since`` set) → compare the period's
          ``lastReleased`` date with ``since`` and re-download only when the
          period has been republished since; otherwise return an empty
          DataFrame.

        Args:
            query: Comtrade query request.
            since: Instant of the previous successful download for this query,
                or ``None`` if it was never downloaded.
            n_observations: Ignored for Comtrade (a republished period is
                re-pulled in full); kept for interface conformity with the
                SDMX clients.

        Returns:
            DataFrame with the retrieved data, empty when nothing was published
            since ``since``.
        """
        # Premier téléchargement : récupération complète
        if since is None:
            return self.execute_query(query)

        # Téléchargements suivants : saut si la période n'a pas été republiée
        last_released = self._period_last_released(query)
        if last_released is not None and since >= last_released:
            # Construction des périodes de la requête
            periods = None
            if query.periods is not None:
                periods = query.periods
            elif ((query.period_start is not None) & (query.period_end is not None)) | (query.period_start is not None):
                period_list = self._build_periods_from_boundaries(period_start=query.period_start, frequency=query.frequency, period_end=query.period_end)
                periods = f"{period_list[0]}-{period_list[-1]}"

            # Logging
            logger.info(
                "%s / %s / %s / %s : no republication since %s",
                query.dataflow,
                periods,
                query.reporters,
                query.products,
                since.isoformat(),
            )
            return pd.DataFrame()

        # Récupération complète de la tranche (période, lot de produits)
        return self.execute_query(query)

    # Méthode d'exécution d'un objet requête
    def execute_query(self, query: "ComtradeQueryRequest") -> pd.DataFrame:
        """Execute a :class:`ComtradeQueryRequest` and return its DataFrame.

        Maps the query selection fields onto :meth:`get_data` and discards the
        request metadata, returning only the DataFrame for symmetry with the
        SDMX clients' ``execute_query``.

        Args:
            query: Comtrade query request.

        Returns:
            DataFrame with the retrieved data.
        """
        # Délégation à get_data avec les champs de sélection de la requête
        df, _ = self.get_data(
            flows=query.flows,
            products=query.products,
            reporters=query.reporters,
            partners=query.partners,
            partners2=query.partners2,
            customs=query.customs,
            mot=query.mot,
            periods=query.periods,
            period_start=query.period_start,
            period_end=query.period_end,
            type_code=query.type_code,
            classification=query.classification,
            frequency=query.frequency,
            max_records=query.max_records,
            format_output=query.format.value.upper(),
            count_only=query.count_only,
            include_desc=query.include_desc,
        )

        # Dans la documentation de comtrade, il est spécifié que les duplicats n'en sont pas vraiment et peuvent être agrégés (cf https://uncomtrade.org/docs/what-is-tariffline-data/)
        # Agrégation selon les clés primaires du flux
        # Extraction des noms de colonnes correspondant aux clés primaires
        dimension_keys = [d.name for d in self.resolve_query_structure(query=query).dimensions]
        # Agrégation
        df = df.groupby(dimension_keys, sort=False, as_index=False, dropna=False).sum()

        return df

    # Méthode de résolution de la structure d'une requête
    def resolve_query_structure(
        self, query: "ComtradeQueryRequest"
    ) -> DataflowStructure:
        """Resolve the dataflow structure backing a query (for primary keys).

        Args:
            query: Comtrade query request.

        Returns:
            The :class:`DataflowStructure` of the query's dataflow.
        """
        # Délégation à get_structure avec les identifiants de la requête
        return self.get_structure(dataflow=query.dataflow, agency=query.agency)

    # Méthode d'extraction de la structure d'un dataflow
    def get_structure(
        self, dataflow: str, agency: str = AGENCY_ID
    ) -> DataflowStructure:
        """Return the structure of a Comtrade dataflow.

        UN Comtrade has no structure endpoint, so the structure is read from the
        registry (populated from ``parameters/comtrade.json``) or derived from
        the declared identifier columns and cached.

        Args:
            dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).
            agency: Maintaining agency (default: ``"COMTRADE"``).

        Returns:
            The resolved :class:`DataflowStructure`.
        """
        # Recherche dans le registre avant toute construction
        cached = self.structure_registry.get(agency, dataflow)
        if cached is not None:
            return cached

        # Construction depuis les paramètres déclarés et mise en cache
        structure = parsing.build_structure_from_parameters(agency, dataflow)
        self.structure_registry.register(structure)
        return structure

    # ──────────────────────────────────────────────────────────────────
    # Fermeture des ressources
    # ──────────────────────────────────────────────────────────────────

    # Méthode de fermeture de la connexion HTTP
    def close(self) -> None:
        """Close the HTTP session and release resources."""
        # Fermeture de la session héritée d'APIClient
        super().close()
        logger.info("Comtrade client closed")
