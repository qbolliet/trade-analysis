"""Download step: plan the queries of a source, download them, audit the coverage.

Two sources share one step function, :func:`run_download`:

- **UN Comtrade** (tariffline): queries split by year x product batch, in a
  **year-major** order (every query of a year before those of the next one), so
  that a year is complete — hence usable by the BACI completeness gate — as soon
  as possible;
- **Eurostat Comext**: one query per product x reporter, every year in the same
  query from the first year of the analysis, in a **product-major** order (a
  product complete for every reporter before the next one, the cell of the
  partner metrics).

``statflows.download_updates`` performs the incremental update and sorts the
never-downloaded queries first with a stable sort: the order of the planned list
is therefore the catch-up order. The codelists fetched for the planning are also
published as reference tables (labels of reporters, partners and products), and
the coverage of the source is audited at the end (:mod:`.coverage`); neither can
fail the download.

No environment variable and no YAML path are read here: the API client comes
from a factory given by the caller (its subscription key included).
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
from datetime import datetime, timedelta
import itertools
import logging
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, TypeVar, Union

# Modules de manipulation de données
import pandas as pd

# Modules d'acquisition des données (statflows)
from statflows import (
    ComtradeClient,
    ComtradeQueryRequest,
    DataflowStructure,
    EurostatClient,
    EurostatQueryRequestV30,
)
from statflows.core.download import download_updates
from statflows.core.factory import codelist_frame, filter_codes
from statflows.core.registry import DEFAULT_SHARD
from statflows.core.reports import QueryReport

# Modules du package
from kedro_pipeline.io.download_report import (
    DownloadFailureError,
    check_download_report,
    download_run_metrics,
)
from kedro_pipeline.io.ducklake import download_buffering_options
from kedro_pipeline.io.registry_views import DownloadRegistryView, period_year, split_codes
from kedro_pipeline.steps.coverage import audit_coverage
from kedro_pipeline.steps.reference import publish_reference
from kedro_pipeline.steps.result import StepProgress, StepResult, capturing, mark
from macroforecast.tracking import RunTracker

# Initialisation du logger
logger = logging.getLogger(__name__)

T = TypeVar("T")


# ──────────────────────────────────────────────────────────────────────
# UN Comtrade : planification année-majeure
# ──────────────────────────────────────────────────────────────────────

# Clé des bornes de périodes dans les filtres de découpage (hors codelists)
_PERIODS_KEY = "periods"

# Ordres admis de la boucle sur les années
_PERIODS_ORDERS = ("desc", "asc")

# Fonction de récupération de la liste des codes d'une catégorie de référence
def fetch_comtrade_codelist(
    dimension: str,
    client: Optional[ComtradeClient] = None,
) -> pd.DataFrame:
    """Fetch the codelist of a UN Comtrade reference dimension, with its labels.

    Generic counterpart of the Comext ``fetch_comtrade_codelist`` helper:
    UN Comtrade exposes no Data Structure Definition, so valid codes are read
    directly from the reference metadata of ``dimension`` rather than deduced
    from a dataflow structure. Delegates to ``statflows.codelist_frame``, which
    drops expired entries and keeps the remaining metadata columns (ISO codes,
    ``isGroup``…) so the reference tables can be published without a second
    network call.

    Args:
        dimension: Reference dimension (e.g. ``"reporter"`` or ``"cmd:HS"``).
        client: ComtradeClient instance; a new one is created if ``None``.

    Returns:
        DataFrame with the ``code`` (text) and ``label`` columns, ``parent`` for
        hierarchical categories, then the metadata columns, in the order of the
        metadata.

    Examples:
        >>> reporter_codes = fetch_comtrade_codelist("reporter")  # doctest: +SKIP
        >>> reporter_codes[["code", "label"]].head(1)  # doctest: +SKIP
    """
    # Initialisation du client s'il n'est pas spécifié
    if client is None:
        client = ComtradeClient()

    # Codelist avec libellés (métadonnées en cache : un seul appel par catégorie)
    codelist = codelist_frame(client, dimension, keep_metadata=True)

    # Logging
    logger.info("%d codes for dimension '%s'", len(codelist), dimension)

    return codelist

# Fonction de plafonnement du nombre de requêtes
def cap_queries(queries: Sequence[T], max_queries: Optional[int]) -> List[T]:
    """Keep at most ``max_queries`` queries, in list order (optional cap of a run).

    Args:
        queries: Ordered queries.
        max_queries: Maximum number of queries; ``None`` means no cap.

    Returns:
        The (possibly truncated) list of queries.

    Examples:
        >>> cap_queries([1, 2, 3], None)
        [1, 2, 3]
        >>> cap_queries([1, 2, 3], 2)
        [1, 2]
    """
    return list(itertools.islice(queries, max_queries))

# Fonction de résolution des périodes à télécharger
def resolve_periods(
    client: Any,
    periods_filter: Optional[Mapping[str, Any]],
    default_start: int,
    frequency: str = "annual",
) -> List[str]:
    """Resolve the list of periods to download, bounds included.

    ``ComtradeClient.get_valid_periods`` drops the last period of its range
    (the current, incomplete one when no end is given — but also an explicit
    ``period_end``): it is therefore called without end, and the upper bound
    is applied here, inclusively.

    Args:
        client: Object exposing ``get_valid_periods`` (a ``ComtradeClient``;
            the method is local, no API call).
        periods_filter: ``{"start": int | None, "end": int | None}`` bounds
            (years, included); ``start`` falls back to ``default_start``.
        default_start: First year of the analysis
            (``runtime.ANALYSIS_START_YEAR.comtrade``).
        frequency: ``"annual"`` or ``"monthly"``.

    Returns:
        Periods in API format (``YYYY`` or ``YYYYMM``), increasing.

    Examples:
        >>> resolve_periods(client, {"start": 2015, "end": None}, 1994)  # doctest: +SKIP
        ['2015', ..., '2025']
    """
    # Bornes configurées (start nul → première année de l'analyse)
    periods_filter = periods_filter or {}
    start = periods_filter.get("start")
    start = default_start if start is None else start
    end = periods_filter.get("end")

    # Périodes valides depuis la borne basse (l'année en cours est exclue)
    periods = client.get_valid_periods(period_start=str(start), frequency=frequency)
    # Borne haute incluse, appliquée sur l'année
    if end is not None:
        periods = [p for p in periods if int(str(p)[:4]) <= int(end)]
    return [str(p) for p in periods]

# Fonction de résolution des valeurs d'une dimension scindée
def _comtrade_dimension_values(
    codes: pd.DataFrame, filters: Mapping[str, Any]
) -> Optional[List[str]]:
    """Filter a codelist, or return ``None`` when no filter narrows it.

    Args:
        codes: Codelist DataFrame with a ``code`` column.
        filters: include/exclude filters forwarded to ``filter_codes``.

    Returns:
        ``None`` (every code, left to the API) when all filters are null,
        else the sorted list of selected codes.
    """
    # Aucun filtre : la dimension n'est pas restreinte (valeur None à l'appel)
    if all(value is None for value in filters.values()):
        return None
    return filter_codes(codes["code"], **filters)

# Fonction de construction des requêtes
def build_comtrade_queries(
    dataflow: str,
    dims_codes: Dict[str, pd.DataFrame],
    fixed_dims: Dict[str, Any],
    split_filters: Dict[str, Dict[str, Union[List[str], str, None]]],
    periods: Sequence[str],
    products_step: Optional[int] = 10,
    periods_order: str = "desc",
) -> List[ComtradeQueryRequest]:
    """Build the split Comtrade queries, one per (period, product batch).

    The include/exclude filters of the YAML configuration are applied to the
    ``reporters`` and ``products`` codelists. A dimension whose filters are all
    null is not restricted: it is sent as ``None`` (every code, e.g. all
    reporters) in a single query. Otherwise the filtered reporters are sent as
    a single list, and the filtered products are grouped once, in natural code
    order, into batches of ``products_step``.

    The returned list is **period-major**: the outer loop runs over
    the periods (``periods_order``), the inner loop over the product batches.
    ``download_updates`` sorts never-downloaded queries first with a stable
    sort, so a period is complete before the next one starts.

    Args:
        dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).
        dims_codes: Mapping ``{"reporters": df, "products": df}`` of the
            codelists to filter, each a DataFrame with a ``code`` column.
        fixed_dims: Fixed query fields shared by every query (``flows``,
            ``type_code``, ``classification``, ``frequency``). Must not carry
            ``periods``, ``products`` or ``reporters``.
        split_filters: Per-dimension include/exclude filters, keyed like
            ``dims_codes``; an optional ``"periods"`` entry (bounds, resolved
            by :func:`resolve_periods`) is ignored here.
        periods: Periods to download (``YYYY`` or ``YYYYMM``).
        products_step: Number of products per query; ``None`` or ``0`` sends
            the whole product selection in each query.
        periods_order: ``"desc"`` (recent periods first) or ``"asc"``.

    Returns:
        List of ``ComtradeQueryRequest`` objects, period-major.

    Raises:
        ValueError: If ``split_filters`` and ``dims_codes`` do not share the
            same keys, or ``periods_order`` is invalid.

    Examples:
        >>> queries = build_comtrade_queries(
        ...     "C_A_HS",
        ...     {"reporters": pd.DataFrame({"code": ["251"]}),
        ...      "products": pd.DataFrame({"code": ["010121", "010129", "854140"]})},
        ...     {"frequency": "annual"},
        ...     {"reporters": {"include": None}, "products": {"include": None}},
        ...     periods=["2023", "2024"], products_step=2,
        ... )
        >>> [(q.periods, q.products) for q in queries]
        [('2024', ['010121', '010129']), ('2024', ['854140']), ('2023', ['010121', '010129']), ('2023', ['854140'])]
    """
    # Filtres de codelists (les bornes de périodes sont résolues séparément)
    code_filters = {k: v for k, v in split_filters.items() if k != _PERIODS_KEY}

    # Vérification que les filtres et les codelists partagent les mêmes clés
    if set(code_filters.keys()) != set(dims_codes.keys()):
        raise ValueError(
            f"'split_filters' and 'dims_codes' should have similar keys. "
            f"Found {code_filters.keys()} for 'split_filters' and "
            f"{dims_codes.keys()} for 'dims_codes'"
        )
    # Vérification de l'ordre des périodes
    if periods_order not in _PERIODS_ORDERS:
        raise ValueError(
            f"Invalid periods_order: {periods_order!r}. Should be in {_PERIODS_ORDERS}"
        )

    # Valeurs des dimensions non découpées en lots (une seule valeur par requête)
    other_dims = {
        dim: _comtrade_dimension_values(dims_codes[dim], filters)
        for dim, filters in code_filters.items()
        if dim != "products"
    }

    # Lots de produits formés une fois, identiques pour toutes les années
    products = (
        _comtrade_dimension_values(dims_codes["products"], code_filters["products"])
        if "products" in code_filters
        else None
    )
    if products is not None and products_step:
        batches: List[Optional[List[str]]] = [
            products[i: i + products_step] for i in range(0, len(products), products_step)
        ]
    else:
        # Pas de découpage produit : toute la sélection (ou None) dans chaque requête
        batches = [products]

    # Ordre des années : boucle externe (récentes d'abord par défaut)
    ordered_periods = sorted(
        (str(p) for p in periods), key=int, reverse=(periods_order == "desc")
    )

    # Construction année-majeure : toutes les requêtes d'une année, puis la suivante
    queries = [
        ComtradeQueryRequest(
            dataflow=dataflow,
            periods=period,
            products=batch,
            **other_dims,
            **fixed_dims,
        )
        for period in ordered_periods
        for batch in batches
    ]

    # Logging
    logger.info(
        "Successfully built %d splitted queries (%d period(s) x %d product batch(es)).",
        len(queries), len(ordered_periods), len(batches),
    )

    return queries

# Fonction de planification de la liste complète des requêtes d'un dataflow
def plan_comtrade_queries(
    config: Mapping[str, Any],
    runtime_config: Mapping[str, Any],
    client: Any,
    dims_codes: Optional[Dict[str, pd.DataFrame]] = None,
) -> List[ComtradeQueryRequest]:
    """Plan the full, ordered list of Comtrade queries of the configured dataflow.

    Single source of the planned queries: used by the download step and by
    the BACI completeness gate, which must
    reason on exactly the same batches.

    Args:
        config: The ``comtrade`` parameter block (``DATAFLOW``, ``parameters``,
            ``fixed_dims``, ``split_filters``).
        runtime_config: Parsed ``runtime`` mapping (``ANALYSIS_START_YEAR``).
        client: ``ComtradeClient`` (codelists and valid periods).
        dims_codes: Codelists ``{"reporters": df, "products": df}``; fetched
            from the API when ``None``.

    Returns:
        Ordered list of ``ComtradeQueryRequest`` (uncapped).
    """
    # Dataflow et sections de configuration associées
    dataflow = config["DATAFLOW"]
    parameters = config["parameters"][dataflow]
    fixed_dims = config["fixed_dims"][dataflow]
    split_filters = config["split_filters"][dataflow]

    # Codelists des dimensions scindées
    if dims_codes is None:
        dims_codes = {
            "reporters": fetch_comtrade_codelist("reporter", client=client),
            "products": fetch_comtrade_codelist("cmd:HS", client=client),
        }

    # Périodes : bornes configurées, première année de l'analyse par défaut (1994 pour Comtrade)
    periods = resolve_periods(
        client,
        split_filters.get(_PERIODS_KEY),
        default_start=runtime_config["ANALYSIS_START_YEAR"]["comtrade"],
        frequency=fixed_dims.get("frequency", "annual"),
    )

    return build_comtrade_queries(
        dataflow=dataflow,
        dims_codes=dims_codes,
        fixed_dims=fixed_dims,
        split_filters=split_filters,
        periods=periods,
        products_step=parameters["products_step"],
        periods_order=parameters.get("periods_order", "desc"),
    )

# Fonction de clé de fragment du registre de téléchargement
def comtrade_shard_key(query: ComtradeQueryRequest) -> str:
    """Name the registry fragment of a Comtrade query: its year.

    The year-major order completes a year before the next one starts, so a flush
    only rewrites the fragment of the year being downloaded.

    Args:
        query: Comtrade query, whose ``periods`` is one period (``YYYY`` or
            ``YYYYMM``) or a list of periods.

    Returns:
        The year(s) of the query joined by ``_``, or the default fragment when
        the query carries no explicit period (``period_start`` / ``period_end``).

    Examples:
        >>> comtrade_shard_key(ComtradeQueryRequest(dataflow="C_A_HS", periods="2023"))
        '2023'
    """
    if query.periods is None:
        return DEFAULT_SHARD
    periods = query.periods if isinstance(query.periods, (list, tuple)) else [query.periods]
    return "_".join(sorted({str(period_year(period)) for period in periods})) or DEFAULT_SHARD


# ──────────────────────────────────────────────────────────────────────
# Eurostat Comext : planification produit-majeure
# ──────────────────────────────────────────────────────────────────────

# Dimension découpée en boucle externe (ordre produit-majeur)
_PRODUCT_DIM = "product"

# Fonction récupération des listes de codes associées à une dimension d'un dataflow
def fetch_eurostat_codelist(
    structure: DataflowStructure,
    dimension: str,
    client: Optional[EurostatClient] = None,
) -> pd.DataFrame:
    """Fetch the codelist of one dimension of a Comext dataflow.

    Deduces the codelist identifier of the requested dimension from the
    dataflow's Data Structure Definition (DSD), then downloads and parses it
    through ``statflows.codelist_frame``.

    Args:
        structure: Resolved dataflow structure (carries the dimension →
            codelist mapping).
        dimension: Name of the dimension to fetch the codelist of (e.g.
            ``"reporter"``, ``"product"``).
        client: ``EurostatClient`` instance; a new one is created if ``None``.

    Returns:
        DataFrame with columns ``(code, label, parent)`` for the requested dimension.

    Examples:
        >>> structure = client.get_dataflow_structure(
        ...     dataflow="DS-045409",
        ... )  # doctest: +SKIP
        >>> reporter_codes = fetch_eurostat_codelist(
        ...     structure, "reporter",
        ... )  # doctest: +SKIP
        >>> "FR" in reporter_codes["code"].values  # doctest: +SKIP
        True
    """
    # Initialisation du client s'il n'est pas spécifié
    if client is None:
        client = EurostatClient()

    # Codelist de la dimension (identifiant déduit de la DSD) avec libellés et parents,
    # mise en cache par le client : un seul appel réseau par codelist
    dimension_codes = codelist_frame(client, dimension, structure)
    # Logging
    logger.info("%d codes %s", len(dimension_codes), dimension)

    return dimension_codes

# Fonction de filtrage d'une codelist en conservant l'ordre de la liste d'inclusion
def _ordered_codes(codes: pd.DataFrame, filters: Mapping[str, Any]) -> List[str]:
    """Filter a codelist, keeping the order of the ``include`` list when given.

    ``filter_codes`` returns sorted codes; the configured ``include`` order is
    the query order of the dimension (e.g. reporters), so it is restored here.

    Args:
        codes: Codelist DataFrame with a ``code`` column.
        filters: include/exclude filters forwarded to ``filter_codes``.

    Returns:
        Selected codes, in ``include`` order if set, else in natural order.
    """
    selected = filter_codes(codes["code"], **filters)
    include = filters.get("include")
    # Pas de liste d'inclusion : ordre naturel des codes
    if include is None:
        return selected
    # Ordre de la liste d'inclusion (codes absents déjà signalés par filter_codes)
    kept = set(selected)
    return [code for code in dict.fromkeys(str(c) for c in include) if code in kept]

# Fonction de construction des requêtes
def build_eurostat_queries(
    dataflow: str,
    dims_codes: Dict[str, pd.DataFrame],
    fixed_dims: Dict[str, Union[List[str], str]] = {},
    split_filters: Dict[str, Dict[str, Union[List[str], str]]] = {},
    products_step: int = 1,
    period_windows: Optional[Sequence[Sequence[Optional[int]]]] = None,
    start_period: Optional[str] = None,
) -> List[EurostatQueryRequestV30]:
    """Build the split queries for a Comext dataflow, product-major.

    Applies the include/exclude filters declared in the YAML configuration to
    the split-dimension codelists, then returns one query per product and per
    combination of the other split dimensions (typically reporters), every
    period in a single query. Order: outer loop over the products in
    natural code order, inner loop over the other dimensions in the order of
    their ``include`` list (natural order when unset).

    Args:
        dataflow: Eurostat dataflow identifier (e.g. ``"DS-045409"``).
        dims_codes: Mapping of split-dimension name to its codelist DataFrame
            (column ``code``), as returned by :func:`fetch_eurostat_codelist`.
            Must contain ``"product"``.
        fixed_dims: Dimensions shared by every query (e.g. freq=A, partner=*,
            flow=1, indicators=QUANTITY_IN_100KG), read from the YAML
            ``fixed_dims`` section.
        split_filters: Per split-dimension include/exclude filters (forwarded
            to :func:`~statflows.core.factory.filter_codes`), read from
            the YAML ``split_filters`` section. Must share the same keys as
            ``dims_codes``.
        products_step: Number of products per query; only ``1`` is supported
            (the limits of the Eurostat API for product batches are not measured yet).
        period_windows: Optional ordered period windows; only ``None``
            is supported.
        start_period: First period requested (``startPeriod``, sent as
            ``c[TIME_PERIOD]=ge:<start>`` by the SDMX 3.0 client); ``None``
            requests every available period.

    Returns:
        List of ``EurostatQueryRequestV30`` objects, product-major.

    Raises:
        ValueError: If ``split_filters`` and ``dims_codes`` do not share the
            same keys, or ``"product"`` is not a split dimension.
        NotImplementedError: If ``products_step > 1`` or ``period_windows`` is
            set.

    Examples:
        >>> queries = build_eurostat_queries(
        ...     "DS-045409",
        ...     dims_codes={"reporter": pd.DataFrame({"code": ["DE", "FR"]}),
        ...                 "product": pd.DataFrame({"code": ["01", "02"]})},
        ...     fixed_dims={"freq": "A"},
        ...     split_filters={"reporter": {"include": ["FR", "DE"]}, "product": {}},
        ... )
        >>> [(q.dimensions["product"], q.dimensions["reporter"]) for q in queries]
        [('01', 'FR'), ('01', 'DE'), ('02', 'FR'), ('02', 'DE')]
    """
    # Paramètres non encore supportés (granularité inchangée tant que les limites de l'API
    # Eurostat pour des lots de produits ne sont pas mesurées)
    if products_step is not None and products_step > 1:
        raise NotImplementedError(
            f"products_step={products_step} is not supported yet: product batches "
            "require measuring the Eurostat API limits first (PR-03). Use 1."
        )
    if period_windows is not None:
        raise NotImplementedError(
            "period_windows is not supported yet (PD-07): set it to null."
        )

    # Vérification que 'split_filters' et 'dims_codes' partagent les mêmes clés
    if set(split_filters.keys()) != set(dims_codes.keys()):
        raise ValueError(f"'split_filters' and 'dims_codes' should have similar keys. Found {split_filters.keys()} for 'split_filters' and {dims_codes.keys()} for 'dims_codes'")
    if _PRODUCT_DIM not in split_filters:
        raise ValueError(f"'{_PRODUCT_DIM}' must be a split dimension (product-major order)")

    # Codes des dimensions scindées, dans l'ordre des requêtes
    split_dims_codes = {
        split_dim: (
            filter_codes(dims_codes[split_dim]["code"], **filters)
            if split_dim == _PRODUCT_DIM
            else _ordered_codes(dims_codes[split_dim], filters)
        )
        for split_dim, filters in split_filters.items()
    }
    # Dimensions de la boucle interne (ordre de la configuration)
    inner_dims = [dim for dim in split_filters if dim != _PRODUCT_DIM]

    # Construction produit-majeure (l'ordre d'insertion des dimensions suit la
    # configuration, comme auparavant)
    queries = [
        EurostatQueryRequestV30(
            dataflow=dataflow,
            dimensions={
                **fixed_dims,
                **{
                    dim: (product if dim == _PRODUCT_DIM else inner[inner_dims.index(dim)])
                    for dim in split_filters
                },
            },
            start_period=start_period,
        )
        for product in split_dims_codes[_PRODUCT_DIM]
        for inner in itertools.product(*(split_dims_codes[dim] for dim in inner_dims))
    ]

    # Logging
    logger.info(f"Successfully built {len(queries)} splitted queries.")

    return queries

# Fonction de clé de fragment du registre de téléchargement
def eurostat_shard_key(query: EurostatQueryRequestV30) -> str:
    """Name the registry fragment of a Comext query: its reporter.

    A fragment per reporter keeps each registry flush small: the product-major
    order touches every reporter, but a flush only rewrites the fragments
    modified since the previous one.

    Args:
        query: Eurostat query, whose ``dimensions["reporter"]`` is a code, a
            list of codes or a ``+`` / ``,`` separated string.

    Returns:
        Reporter code(s) joined by ``_`` (``statflows`` sanitises the file name),
        or the default fragment when the query has no reporter.

    Examples:
        >>> eurostat_shard_key(
        ...     EurostatQueryRequestV30(dataflow="DS-045409", dimensions={"reporter": "FR"})
        ... )
        'FR'
    """
    return "_".join(split_codes(query.dimensions.get("reporter"))) or DEFAULT_SHARD



# ──────────────────────────────────────────────────────────────────────
# Fonction d'étape : téléchargement d'une source
# ──────────────────────────────────────────────────────────────────────

# Libellé des sources dans les journaux
_SOURCE_LABELS = {"comtrade": "Comtrade", "eurostat": "Comext"}


# Planification Comtrade : codelists, requêtes, codelists des référentiels
def _plan_comtrade(
    client: Any, params: Mapping[str, Any], runtime: Mapping[str, Any]
) -> Tuple[List[Any], Dict[str, pd.DataFrame], Callable[[Any], str]]:
    """Plan the Comtrade queries; return them, the reference codelists and the shard key."""
    # Codelists des dimensions scindées (libellés compris, réutilisés par les référentiels)
    dims_codes = {
        "reporters": fetch_comtrade_codelist("reporter", client=client),
        "products": fetch_comtrade_codelist("cmd:HS", client=client),
    }
    # Liste ordonnée des requêtes (année-majeure)
    planned = plan_comtrade_queries(params, runtime, client, dims_codes=dims_codes)
    reference_codes = {"reporter": dims_codes["reporters"], "cmd:HS": dims_codes["products"]}
    return planned, reference_codes, comtrade_shard_key


# Planification Comext : structure, codelists, requêtes, codelists des référentiels
def _plan_eurostat(
    client: Any, params: Mapping[str, Any], runtime: Mapping[str, Any]
) -> Tuple[List[Any], Dict[str, pd.DataFrame], Callable[[Any], str]]:
    """Plan the Comext queries; return them, the reference codelists and the shard key."""
    dataflow = params["DATAFLOW"]
    parameters = params["parameters"][dataflow]
    # Structure du dataflow puis codes des dimensions selon lesquelles les requêtes sont scindées
    structure = client.get_dataflow_structure(dataflow=dataflow)
    dims_codes = {
        split_dim: fetch_eurostat_codelist(structure=structure, dimension=split_dim, client=client)
        for split_dim in params["split_filters"][dataflow].keys()
    }
    # Codelists publiées en référentiel : celles du découpage, plus les dimensions
    # manquantes (partner : un appel SDMX de plus), non bloquant
    reference_codes = dict(dims_codes)
    for dimension in params["DOWNLOADS"]["REFERENCE"]["DIMENSIONS"]:
        if dimension not in reference_codes:
            try:
                reference_codes[dimension] = fetch_eurostat_codelist(
                    structure=structure, dimension=dimension, client=client
                )
            except Exception as exc:
                logger.warning(f"Codelist '{dimension}' indisponible pour les référentiels : {exc}")
    # Requêtes produit-majeures, toutes années depuis la première année de l'analyse
    planned = build_eurostat_queries(
        dataflow=dataflow,
        dims_codes=dims_codes,
        fixed_dims=params["fixed_dims"][dataflow],
        split_filters=params["split_filters"][dataflow],
        products_step=parameters.get("products_step", 1),
        period_windows=parameters.get("period_windows"),
        start_period=str(runtime["ANALYSIS_START_YEAR"]["eurostat"]),
    )
    return planned, reference_codes, eurostat_shard_key


# Planificateurs par source
_PLANNERS = {"comtrade": _plan_comtrade, "eurostat": _plan_eurostat}


# Fonction de lecture du budget de temps d'un téléchargement
def max_runtime(downloads: Mapping[str, Any]) -> timedelta:
    """Return the time budget of a download (``MAX_RUNTIME`` block).

    Args:
        downloads: ``DOWNLOADS.<DATAFLOW>`` block.

    Returns:
        The budget.

    Examples:
        >>> max_runtime({"MAX_RUNTIME": {"WEEKS": 0, "DAYS": 0, "HOURS": 10,
        ...                              "MINUTES": 0, "SECONDS": 0}})
        datetime.timedelta(seconds=36000)
    """
    budget = downloads["MAX_RUNTIME"]
    return timedelta(
        weeks=budget["WEEKS"],
        days=budget["DAYS"],
        hours=budget["HOURS"],
        minutes=budget["MINUTES"],
        seconds=budget["SECONDS"],
    )


# Plan d'un téléchargement : requêtes, codelists des référentiels, clé de fragment
@dataclass
class DownloadPlan:
    """Planned queries of one source and what the following phases need.

    Built by :func:`plan_download` with the API client, then completed by
    :func:`download_planned` with the ``statflows`` report: the reference
    tables (:func:`publish_download_reference`) and the coverage audit
    (:func:`audit_download`) need no client, so they can run in other nodes of
    the same pod.

    Attributes:
        source: ``"comtrade"`` or ``"eurostat"``.
        dataflow: Dataflow identifier.
        planned: Planned queries, uncapped (the coverage audit and the BACI
            completeness gate judge against the full list).
        queries: Queries actually downloaded (capped by ``max_queries``).
        reference_codes: Codelists published as reference tables, by dimension.
        shard_key: Registry fragment of a query (picklable module function).
        report: ``statflows`` download report, once downloaded.
        result: Result of the download (:func:`download_result`), set by the
            caller that splits the phases, so that a later phase can raise its
            failure.
    """

    source: str
    dataflow: str
    planned: List[Any]
    queries: List[Any]
    reference_codes: Dict[str, pd.DataFrame]
    shard_key: Callable[[Any], str]
    report: Any = None
    result: Optional[StepResult] = None


# Phase 1 : planification des requêtes (seule phase, avec le téléchargement, qui
# interroge l'API)
def plan_download(
    client: Any,
    *,
    source: str,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> DownloadPlan:
    """Plan the queries of a source and fetch the codelists of its reference tables.

    Args:
        client: Open API client (``ComtradeClient``, ``EurostatClient``).
        source: ``"comtrade"`` or ``"eurostat"``.
        params: Parameter block of the source.
        runtime: ``runtime`` parameters.

    Returns:
        The plan (``report`` still ``None``).

    Raises:
        KeyError: If ``source`` is unknown or a parameter is missing.
    """
    planner = _PLANNERS[source]
    dataflow = params["DATAFLOW"]
    parameters = params["parameters"][dataflow]
    planned, reference_codes, shard_key = planner(client, params, runtime)
    # Plafond optionnel (null = aucun)
    queries = cap_queries(planned, parameters.get("max_queries"))
    return DownloadPlan(source, dataflow, list(planned), queries, reference_codes, shard_key)


# Phase 2 : référentiels publiés depuis les codelists déjà récupérées (non bloquant)
def publish_download_reference(
    plan: DownloadPlan,
    table: Any,
    *,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    year: Optional[int] = None,
) -> StepResult:
    """Publish the reference tables (country and product labels) of a source.

    Never blocking for the download: the failures are recorded in the result.

    Args:
        plan: Plan of the source (its codelists).
        table: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` of the
            downloaded fact table; the reference schemas live in its catalog.
        params: Parameter block of the source (``DOWNLOADS.REFERENCE``).
        runtime: ``runtime`` parameters (``NOMENCLATURES``).
        year: Year the codelists describe; the current year when ``None``.

    Returns:
        The result of the publication (``rows`` and ``failures`` per table).
    """
    reference = publish_reference(
        plan.reference_codes,
        table.connector(),
        source=plan.source,
        params={
            **params["DOWNLOADS"]["REFERENCE"],
            "NOMENCLATURES": runtime["NOMENCLATURES"]["HS"],
            "YEAR": year if year is not None else datetime.now().year,
        },
    )
    logger.info(
        f"Référentiels {_SOURCE_LABELS.get(plan.source, plan.source)} : {reference['rows']} ; "
        f"échecs : {reference['failures']}"
    )
    return reference


# Sections des métriques d'une requête : statistiques HTTP et du limiteur de débit à
# part (leurs propres sections dans l'interface MLflow), le reste sous « download/ »
QUERY_SECTIONS: Dict[str, str] = {
    "http": "http",
    "rate_limit": "rate_limit",
    "fetch": "download/fetch",
}
QUERY_SECTION = "download/query"


# Fonction des métriques d'une requête de téléchargement
def query_metrics(query_report: Any) -> Dict[str, float]:
    """Return the metrics of one downloaded query, ``/``-separated, by section.

    Logged once per query, with the query rank as step, so that every metric
    draws a curve over the run. Correspondence with the query report fields:
    ``http.*`` → ``http/…``, ``rate_limit.*`` → ``rate_limit/…``, ``fetch.*`` →
    ``download/fetch/…``, any other numeric field → ``download/query/…``.

    Args:
        query_report: ``statflows`` ``QueryReport`` of the query.

    Returns:
        Metric name -> finite value.

    Examples:
        >>> from statflows.core.reports import QueryReport
        >>> metrics = query_metrics(QueryReport(rows_written=5))
        >>> metrics["download/query/rows_written"], metrics["http/n_requests"]
        (5.0, 0.0)
    """
    metrics: Dict[str, float] = {}
    for name, value in query_report.to_metrics(prefix="").items():
        head, _, rest = name.partition(".")
        if rest and head in QUERY_SECTIONS:
            key = f"{QUERY_SECTIONS[head]}/{rest}"
        else:
            key = f"{QUERY_SECTION}/{name}"
        metrics[key.replace(".", "/")] = value
    return metrics


# Phase 3 : téléchargement incrémental des requêtes planifiées
def download_planned(
    client: Any,
    plan: DownloadPlan,
    table: Any,
    *,
    params: Mapping[str, Any],
    tracker: Optional[RunTracker] = None,
    progress: Optional[StepProgress] = None,
) -> Any:
    """Download the planned queries incrementally, streaming per-query metrics.

    Args:
        client: Open API client (the one of the planning: its codelists and
            structures are cached).
        plan: Plan of the source; its ``report`` is set here.
        table: Handle of the downloaded fact table; its connector is handed to
            ``statflows``.
        params: Parameter block of the source (``DOWNLOADS``).
        tracker: Tracker of the open run (never entered here).
        progress: Holder of the stage reached, named by a failure report.

    Returns:
        The ``statflows`` download report (also stored in ``plan.report``).
    """
    tracker = capturing(tracker)
    downloads = params["DOWNLOADS"][plan.dataflow]

    # Suivi requête par requête : statflows ignore tout du suivi, le rappel est
    # le seul point de contact
    streamed_queries: List[QueryReport] = []

    def stream_query_metrics(query_report: QueryReport) -> None:
        """Send one query's diagnostics to the tracker."""
        tracker.log_metrics(query_metrics(query_report), step=len(streamed_queries))
        streamed_queries.append(query_report)

    # Téléchargement incrémental (fetch_updates des clients)
    mark(progress, "téléchargement")
    plan.report = download_updates(
        client=client,
        queries=plan.queries,
        connector=table.connector(),
        structures_path=downloads["PATHS"]["STRUCTURES_PATH"],
        last_download_path=downloads["PATHS"]["LAST_DOWNLOAD_PATH"],
        n_observations=downloads["N_LAST_OBSERVATIONS"],
        fresh_registry=False,
        max_runtime=max_runtime(downloads),
        categorical_threshold=None,  # A supprimer avec la nouvelle version de la base de données
        bucket=downloads["BUCKET"],
        storage_options=None,
        on_query_complete=stream_query_metrics,
        # Partition du registre et des écritures
        **download_buffering_options(downloads.get("BUFFERING"), shard_key=plan.shard_key),
    )
    return plan.report


# Phase 4 : audit de couverture (non bloquant)
def audit_download(
    plan: DownloadPlan,
    table: Any,
    *,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    tracker: Optional[RunTracker] = None,
    registry: Optional[DownloadRegistryView] = None,
) -> Optional[StepResult]:
    """Audit the coverage of the downloaded source; a failure is only a warning.

    Args:
        plan: Downloaded plan (``report`` set).
        table: Handle of the downloaded fact table.
        params: Parameter block of the source.
        runtime: ``runtime`` parameters.
        tracker: Tracker of the open run (never entered here).
        registry: View of the download registry; read from
            ``PATHS.LAST_DOWNLOAD_PATH`` when ``None``.

    Returns:
        The result of the audit, ``None`` when it could not run.
    """
    downloads = params["DOWNLOADS"][plan.dataflow]
    try:
        return audit_coverage(
            table,
            registry or DownloadRegistryView(
                downloads["PATHS"]["LAST_DOWNLOAD_PATH"], bucket=downloads["BUCKET"], dataflow=plan.dataflow
            ),
            plan.planned,
            source=plan.source,
            params=params,
            runtime=runtime,
            n_processed=int(plan.report.processed),
            tracker=tracker,
        )
    except Exception as exc:
        logger.warning(f"Audit de couverture {plan.source} impossible : {exc}")
        return None


# Phase 5 : métriques, tags, table par requête et seuil d'échec du run
def download_result(
    plan: DownloadPlan,
    *,
    params: Mapping[str, Any],
    tracker: Optional[RunTracker] = None,
    progress: Optional[StepProgress] = None,
    reference: Optional[StepResult] = None,
    coverage: Optional[StepResult] = None,
) -> StepResult:
    """Build the result of a download from its report, logging the run metrics.

    The failed queries only fail the step when their share exceeds
    ``MAX_ERROR_RATIO``.

    Args:
        plan: Downloaded plan (``report`` set).
        params: Parameter block of the source (``DOWNLOADS``, ``TRACKING``).
        tracker: Tracker of the open run (never entered here).
        progress: Holder of the stage reached, named by a failure report.
        reference: Result of the reference publication, kept in ``outputs``.
        coverage: Result of the coverage audit, kept in ``outputs`` and
            ``children``.

    Returns:
        The step result (see :func:`run_download`).
    """
    tracker = capturing(tracker)
    report = plan.report
    downloads = params["DOWNLOADS"][plan.dataflow]
    log_artifacts = bool((params.get("TRACKING") or {}).get("LOG_ARTIFACTS", True))

    # Métriques, tags et table par requête du run
    mark(progress, "rapport de run")
    metrics = download_run_metrics(report)
    tracker.log_metrics(metrics)
    tags = {
        "dataflow": plan.dataflow,
        "stopped_early": str(report.stopped_early),
        "n_queries_planned": str(report.n_queries_planned),
    }
    tracker.set_tags(tags)
    frame = report.to_frame()
    if log_artifacts and report.queries:
        tracker.log_table(frame, "download/queries.csv")

    # Échec de l'étape : seulement au-delà de la part tolérée de requêtes en erreur
    failures: Dict[str, str] = {}
    failure: Optional[DownloadFailureError] = None
    try:
        check_download_report(report, downloads.get("MAX_ERROR_RATIO"))
    except DownloadFailureError as exc:
        failures, failure = {"téléchargement": str(exc)}, exc
    errors = frame[frame["error_type"].notna()] if "error_type" in frame else frame.iloc[0:0]

    # Logging
    logger.info("Téléchargement terminé : %s", report.to_metrics())
    return StepResult(
        step="download",
        n_units_planned=int(report.n_queries_planned),
        n_units_succeeded=len(report.queries) - int(report.errors),
        failures=failures,
        # Métriques du rapport : celles du run, hors métriques par requête et couverture
        metrics=metrics,
        artifacts={"download/queries.csv": frame},
        tags=tags,
        n_units_failed=int(report.errors),
        units_label=f"{int(report.n_queries_planned)} requêtes",
        report_tables={"errors": errors} if len(errors) else {},
        outputs={"report": report, "reference": reference, "coverage": coverage},
        children={"coverage": coverage} if coverage is not None else {},
        failure_exception=failure,
    )


# Noms des tables du rapport d'un téléchargement : artefact de l'audit -> table du rapport
COVERAGE_REPORT_TABLES = {
    "coverage/by_year.csv": "coverage_by_year",
    "coverage/by_reporter.csv": "coverage_by_reporter",
}


# Fonction du résultat rapporté d'un téléchargement : téléchargement et audit réunis
def download_report_result(result: StepResult, coverage: Optional[StepResult]) -> StepResult:
    """Merge a download result and its coverage audit into the result of one report.

    The download task publishes one report for both: the units, failures and
    verdict of the download, plus the ``coverage/*`` metrics, the coverage
    tables (``coverage/by_year.csv``, ``coverage/by_reporter.csv``) for the
    report sections and the tables ``coverage_by_year`` / ``coverage_by_reporter``
    (with ``errors``) attached to the run.

    Args:
        result: Result of the download (:func:`download_result`).
        coverage: Result of the audit; ``None`` when it could not run.

    Returns:
        A copy of ``result`` completed with the audit, without children.

    Examples:
        >>> audit = StepResult("coverage", metrics={"coverage/eta_days": 2.0},
        ...                    artifacts={"coverage/by_year.csv": pd.DataFrame({"year": [2020]})})
        >>> merged = download_report_result(StepResult("download", 3, 3), audit)
        >>> merged.metrics["coverage/eta_days"], sorted(merged.report_tables)
        (2.0, ['coverage_by_year'])
    """
    from dataclasses import replace

    if coverage is None:
        return replace(result, children={})
    tables = {
        COVERAGE_REPORT_TABLES[path]: table
        for path, table in coverage.artifacts.items()
        if path in COVERAGE_REPORT_TABLES and isinstance(table, pd.DataFrame)
    }
    return replace(
        result,
        metrics={**result.metrics, **coverage.metrics},
        artifacts={**result.artifacts, **coverage.artifacts},
        report_tables={**result.report_tables, **tables},
        children={},
    )


# Fonction d'étape : téléchargement d'une source
def run_download(
    client_factory: Callable[[], Any],
    table: Any,
    *,
    source: str,
    params: Mapping[str, Any],
    runtime: Mapping[str, Any],
    tracker: Optional[RunTracker] = None,
    progress: Optional[StepProgress] = None,
    registry: Optional[DownloadRegistryView] = None,
    year: Optional[int] = None,
) -> StepResult:
    """Plan, download and audit one source, logging the run to the tracker.

    Steps: codelists and planned queries (capped by ``max_queries``),
    reference tables of the codelists (never blocking), incremental download
    with per-query metrics streamed to the tracker, coverage audit (never
    blocking), then the run metrics, tags and per-query table. The failed
    queries only fail the step when their share exceeds ``MAX_ERROR_RATIO``.
    The phases are also exposed one by one (:func:`plan_download`,
    :func:`publish_download_reference`, :func:`download_planned`,
    :func:`audit_download`, :func:`download_result`) for the pipeline nodes,
    which run them as separate nodes of a single pod.

    Args:
        client_factory: Builds the API client (``ComtradeClient`` with its
            subscription key, ``EurostatClient``); closed by the step.
        table: :class:`~kedro_pipeline.io.ducklake.DuckLakeTable` (lazy handle)
            of the downloaded fact table; its connector is handed to
            ``statflows``.
        source: ``"comtrade"`` or ``"eurostat"``.
        params: Parameter block of the source (``DATAFLOW``, ``DOWNLOADS``,
            ``parameters``, ``fixed_dims``, ``split_filters``, ``TRACKING``,
            ``COVERAGE``).
        runtime: ``runtime`` parameters (``ANALYSIS_START_YEAR``,
            ``NOMENCLATURES``).
        tracker: Tracker of the open run (never entered here).
        progress: Holder of the stage reached, named by a failure report.
        registry: View of the download registry for the coverage audit; read
            from ``PATHS.LAST_DOWNLOAD_PATH`` when ``None``.
        year: Year the codelists describe in the reference tables; the current
            year when ``None``.

    Returns:
        The step result: one unit per planned query, run metrics ``download/*``,
        the per-query table (``download/queries.csv``) for the report sections,
        the failed queries under ``report_tables["errors"]``; ``outputs``
        ``report`` (``statflows`` report), ``reference`` and ``coverage``
        (their own results). ``failures`` holds the over-threshold error, raised
        as :class:`~kedro_pipeline.io.download_report.DownloadFailureError`.

    Raises:
        KeyError: If ``source`` is unknown or a parameter is missing.
    """
    tracker = capturing(tracker)

    # Planification des requêtes
    mark(progress, "planification des requêtes")
    client = client_factory()
    try:
        plan = plan_download(client, source=source, params=params, runtime=runtime)
        # Référentiels (libellés des pays et des produits) depuis les codelists déjà
        # récupérées ; non bloquant pour le téléchargement
        reference = publish_download_reference(plan, table, params=params, runtime=runtime, year=year)
        download_planned(client, plan, table, params=params, tracker=tracker, progress=progress)
    finally:
        client.close()

    # Audit de couverture : jamais bloquant, une panne n'est qu'un avertissement
    coverage = audit_download(plan, table, params=params, runtime=runtime, tracker=tracker, registry=registry)
    return download_result(
        plan, params=params, tracker=tracker, progress=progress, reference=reference, coverage=coverage
    )
