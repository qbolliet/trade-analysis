"""Read-only views of the ``statflows`` download registry (PS-12.3).

The downstream steps (partner vulnerabilities, BACI completeness gate) need the
last-download dates of the units they work on: (reporter x product) pairs for
Eurostat, (year x product batch) for Comtrade. :class:`DownloadRegistryView`
**explodes** each registry entry into those units.

The registry is always read through ``statflows.iter_registry_entries``, which
hides its physical layout (single file or per-shard fragments, PS-27): this
module is the only place of the repository that knows the shape of a registry
entry, and it never reads the registry root key by hand.

Two readings of each unit are offered:

- ``last_download`` (:meth:`DownloadRegistryView.pairs_last_download`,
  :meth:`DownloadRegistryView.batches_by_year`): moves at every successful
  download, even when nothing changed. It tells whether a unit was downloaded
  at least once (coverage, BACI completeness gate);
- ``last_modified`` / ``last_modified_snapshot``
  (:meth:`DownloadRegistryView.pairs_last_modified`,
  :meth:`DownloadRegistryView.batches_marks_by_year`), as an
  :class:`~kedro_pipeline.io.freshness.UpstreamMark`: moves only when the data
  actually changed. It decides the downstream recomputations.
"""
# Importation des modules
# Modules de base
import logging
import re
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Tuple, TypeVar

# Lecture du registre indépendante de son format physique
from statflows import RegistryEntry, iter_registry_entries

# Marque amont « données modifiées » (last_modified, last_modified_snapshot)
from kedro_pipeline.io.freshness import UpstreamMark

# Initialisation du logger
logger = logging.getLogger(__name__)

# Séparateurs de produits au sein d'une même valeur de dimension (lots)
_PRODUCT_SEPARATORS = re.compile(r"[+,]")

# Clé d'un lot de produits Comtrade : codes triés, ou None si non restreint
ProductsKey = Optional[Tuple[str, ...]]

# Valeur associée à une unité : date de dernier téléchargement ou marque amont
V = TypeVar("V")


# Fonction de lecture de la date de dernier téléchargement d'une entrée
def _last_download(entry: RegistryEntry) -> datetime:
    """Return the ``last_download`` of an entry."""
    return entry.last_download


# Fonction de lecture de la marque « données modifiées » d'une entrée
def _modified_mark(entry: RegistryEntry) -> UpstreamMark:
    """Return the :class:`UpstreamMark` of an entry (statflows >= 0.2.2 fields)."""
    return UpstreamMark.of_entry(
        getattr(entry, "last_modified", None), getattr(entry, "last_modified_snapshot", None)
    )


# Fonction de combinaison de deux valeurs d'une même unité
def _latest(left: Any, right: Any) -> Any:
    """Combine two values of a unit: the latest date, or the field-wise maximum mark."""
    if isinstance(left, UpstreamMark):
        return UpstreamMark.combine([left, right])
    return right if right > left else left


# Fonction d'éclatement d'une valeur de dimension en codes
def split_codes(value: Any) -> Tuple[str, ...]:
    """Explode a dimension value into its codes.

    A value is a single code, a string of codes joined by ``+`` or ``,`` (SDMX
    multiple selection), or a list of such values.

    Args:
        value: Dimension value from a registry entry (``None`` included).

    Returns:
        Codes in order of appearance, without blanks or duplicates.

    Examples:
        >>> split_codes("2710")
        ('2710',)
        >>> split_codes("2710+8541,8542")
        ('2710', '8541', '8542')
        >>> split_codes(["2710", "8541+8542"])
        ('2710', '8541', '8542')
        >>> split_codes(None)
        ()
    """
    if value is None:
        return ()
    values = value if isinstance(value, (list, tuple, set, frozenset)) else [value]
    codes: Dict[str, None] = {}
    for item in values:
        for code in _PRODUCT_SEPARATORS.split(str(item)):
            code = code.strip()
            if code:
                codes[code] = None
    return tuple(codes)


# Fonction de normalisation d'une sélection de produits en clé de lot
def products_key(products: Any) -> ProductsKey:
    """Normalise a product selection into a batch key.

    Args:
        products: List, delimited string, scalar or ``None`` (no restriction).

    Returns:
        Sorted tuple of codes, or ``None`` when the selection is empty.

    Examples:
        >>> products_key(["854150", "854140"])
        ('854140', '854150')
        >>> products_key("854140,854150") == products_key(["854150", "854140"])
        True
        >>> products_key(None) is None
        True
    """
    codes = split_codes(products)
    return tuple(sorted(codes)) if codes else None


# Fonction d'extraction de l'année d'une période
def period_year(period: Any) -> int:
    """Return the year of a period (``YYYY``, ``YYYYMM`` or ``YYYY-MM``).

    Args:
        period: Period as given to the Comtrade API.

    Returns:
        The four-digit year.

    Examples:
        >>> period_year("2023"), period_year(202305), period_year("2023-05")
        (2023, 2023, 2023)
    """
    return int(str(period)[:4])


# Classe de vue du registre de téléchargement
class DownloadRegistryView:
    """Units of work derived from the download registry (read only).

    Args:
        last_download_path: Registry path, as passed to ``download_updates``
            (``DOWNLOADS.<DATAFLOW>.PATHS.LAST_DOWNLOAD_PATH``).
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
        storage_options: Keyword arguments forwarded to the S3 connection (only
            used with ``bucket``); ``None`` relies on the AWS environment.
        dataflow: When set, entries of other dataflows are ignored.

    Examples:
        >>> view = DownloadRegistryView("registries/comext.json")
        >>> view.pairs_last_download()  # doctest: +SKIP
        {('FR', '854140'): datetime.datetime(2026, 9, 16, 1, 47, tzinfo=...)}
    """

    # Initialisation
    def __init__(
        self,
        last_download_path: Any,
        bucket: Optional[str] = None,
        *,
        storage_options: Optional[Mapping[str, Any]] = None,
        dataflow: Optional[str] = None,
    ) -> None:
        self.last_download_path = last_download_path
        self.bucket = bucket
        self.storage_options = dict(storage_options) if storage_options else None
        self.dataflow = dataflow

    # Itération sur les entrées du dataflow
    def entries(self) -> Iterator[RegistryEntry]:
        """Iterate over the registry entries (single file and/or fragments).

        Yields:
            Entries with a valid ``last_download``, restricted to ``dataflow``
            when it was given.
        """
        for entry in iter_registry_entries(
            self.last_download_path, self.bucket, self.storage_options
        ):
            if self.dataflow is None or entry.dataflow == self.dataflow:
                yield entry

    # Valeurs par couple reporter x produit (Eurostat), lecture générique
    def _pairs(self, value_of: Callable[[RegistryEntry], V]) -> Dict[Tuple[str, str], V]:
        """Explode the entries into (reporter, product) pairs, combining their values.

        Args:
            value_of: Value of an entry (date or mark).

        Returns:
            Mapping ``(reporter, product) -> combined value``.
        """
        values: Dict[Tuple[str, str], V] = {}
        for entry in self.entries():
            # Dimensions SDMX de la requête (à plat si absentes)
            params = entry.params or {}
            dims = params.get("dimensions") or params
            reporters = split_codes(dims.get("reporter"))
            products = split_codes(dims.get("product"))
            value = value_of(entry)
            # Éclatement en couples reporter x produit
            for reporter in reporters:
                for product in products:
                    pair = (reporter, product)
                    values[pair] = _latest(values[pair], value) if pair in values else value
        return values

    # Dates de dernier téléchargement par couple reporter x produit (Eurostat)
    def pairs_last_download(self) -> Dict[Tuple[str, str], datetime]:
        """Last-download date per (reporter, product) pair — Eurostat layout.

        A query whose reporter or product value holds several codes (list, or
        ``+`` / ``,`` separated) is exploded: every (reporter, product)
        combination it covers receives its date. When several entries cover the
        same pair, the most recent date is kept.

        Returns:
            Mapping ``(reporter, product) -> last_download`` (UTC-aware).
        """
        return self._pairs(_last_download)

    # Marques « données modifiées » par couple reporter x produit (Eurostat)
    def pairs_last_modified(self) -> Dict[Tuple[str, str], UpstreamMark]:
        """Data-modification mark per (reporter, product) pair — Eurostat layout.

        Same pairs as :meth:`pairs_last_download` (a pair downloaded but never
        written gets an empty mark); the marks of the entries covering a pair
        are combined field by field (:meth:`UpstreamMark.combine`).

        Returns:
            Mapping ``(reporter, product) -> UpstreamMark``.
        """
        return self._pairs(_modified_mark)

    # Valeurs par lot de produits et par année (Comtrade), lecture générique
    def _batches(self, value_of: Callable[[RegistryEntry], V]) -> Dict[int, Dict[ProductsKey, V]]:
        """Map the entries to their (year, product batch) units, combining their values.

        Args:
            value_of: Value of an entry (date or mark).

        Returns:
            Mapping ``year -> {products key -> combined value}``.
        """
        batches: Dict[int, Dict[ProductsKey, V]] = {}
        for entry in self.entries():
            params = entry.params or {}
            periods = params.get("periods")
            if periods is None:
                continue
            key = products_key(params.get("products"))
            value = value_of(entry)
            periods_list: Iterable[Any] = (
                periods if isinstance(periods, (list, tuple)) else [periods]
            )
            for period in periods_list:
                year_batches = batches.setdefault(period_year(period), {})
                year_batches[key] = _latest(year_batches[key], value) if key in year_batches else value
        return batches

    # Marques « données modifiées » par lot de produits et par année (Comtrade)
    def batches_marks_by_year(self) -> Dict[int, Dict[ProductsKey, UpstreamMark]]:
        """Data-modification mark per product batch, by year — Comtrade layout.

        Same units as :meth:`batches_by_year`, valued by the combined
        :class:`UpstreamMark` of their entries.

        Returns:
            Mapping ``year -> {products key -> UpstreamMark}``.
        """
        return self._batches(_modified_mark)

    # Lots de produits téléchargés par année (Comtrade)
    def batches_by_year(self) -> Dict[int, Dict[ProductsKey, datetime]]:
        """Last-download date per product batch, by year — Comtrade layout.

        Every period of an entry (``params.periods``: one value or a list) is
        mapped to its year; the batch is identified by its product selection
        (:func:`products_key`). The dates of one year are the values of the
        inner mapping. When several entries cover the same batch, the most
        recent date is kept.

        Returns:
            Mapping ``year -> {products key -> last_download}``.

        Examples:
            >>> view.batches_by_year()  # doctest: +SKIP
            {2024: {('854140', '854150'): datetime.datetime(2026, 9, 16, ...)}}
        """
        return self._batches(_last_download)
