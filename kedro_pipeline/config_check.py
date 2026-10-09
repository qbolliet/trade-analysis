"""Consistency checks between the Kedro catalog and the parameters.

The catalog repeats the locations the steps read in the parameters (catalog
database and alias, sanitised schema, bucket, data path, registry template):
the duplication keeps the catalog readable in kedro-viz, and these checks keep
the two in step. :func:`expected_datasets` derives, from the parameters alone,
what every dataset must designate; :func:`catalog_mismatches` compares it to
the datasets of an instantiated catalog. Both are used by
``kedro trade config-check`` and by the tests, on every environment.
"""
# Importation des modules
# Modules de base
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional

# Modules internes
from kedro_pipeline.config import active_targets
from kedro_pipeline.io.ducklake import DuckLakeLocation


# Attendu d'un registre de fraîcheur
@dataclass(frozen=True)
class ExpectedRegistry:
    """What a freshness-registry dataset must designate.

    Attributes:
        path_template: Fragment path template (``STATE.PATH_TEMPLATE``).
        bucket: S3 bucket of the fragments.
        step: Step name recorded in the fragments.
    """

    path_template: str
    bucket: Optional[str]
    step: str


# Attendu du catalogue de restitution
@dataclass(frozen=True)
class ExpectedServing:
    """What the serving-catalog dataset must designate.

    Attributes:
        location: The serving catalog and schema.
        sources: Source catalogs attached read-only, by alias.
    """

    location: DuckLakeLocation
    sources: Mapping[str, DuckLakeLocation] = field(default_factory=dict)


# Fonction de construction de l'emplacement d'un schéma
def _location(dbname: str, alias: str, schema: str, bucket: str, data_path: str) -> DuckLakeLocation:
    """Build the location of a schema whose name is sanitised as the steps do."""
    from statflows.core.download import _schema_name

    return DuckLakeLocation(
        dbname=dbname, catalog_alias=alias, schema=_schema_name(schema),
        bucket=bucket, data_path=data_path,
    )


# Fonction de dérivation des datasets attendus depuis les paramètres
def expected_datasets(parameters: Mapping[str, Any]) -> Dict[str, Any]:
    """Derive from the parameters what every catalog dataset must designate.

    Args:
        parameters: Merged parameters of one environment (``load_parameters``).

    Returns:
        Mapping ``dataset name -> expectation``: a :class:`DuckLakeLocation` for
        a table, an :class:`ExpectedRegistry` for a freshness registry, an
        :class:`ExpectedServing` for ``serving.tables``, a
        :class:`~kedro_pipeline.io.datasets.ConcordanceCache` for the
        correspondence-table cache ``baci.concordances``. BACI datasets are named
        ``baci.<vintage in lower case>`` (one per enabled target), reference
        tables ``reference.<source>.<table>``.

    Raises:
        KeyError: If a parameter block lacks a key the steps read.

    Examples:
        >>> from kedro_pipeline.config import load_parameters
        >>> expected_datasets(load_parameters("base"))["baci.hs2017"].schema
        'baci_hs2017'
    """
    from kedro_pipeline.io.datasets import ConcordanceCache
    from kedro_pipeline.steps.reference import reference_schema
    from kedro_pipeline.steps.serving import source_tables

    eurostat, comtrade = parameters["eurostat"], parameters["comtrade"]
    vulnerabilities, synthesis = parameters["vulnerabilities"], parameters["synthesis"]
    baci, serving = parameters["baci"], parameters["serving"]
    expected: Dict[str, Any] = {}

    # Données brutes et référentiels de chaque source
    for source, name in ((eurostat, "eurostat.comext"), (comtrade, "comtrade.tariffline")):
        downloads, dataflow = source["DOWNLOADS"], source["DATAFLOW"]
        catalog = (downloads["DBNAME"], downloads["CATALOG_ALIAS"])
        storage = (downloads[dataflow]["BUCKET"], downloads[dataflow]["PATHS"]["DATA_PATH"])
        expected[name] = _location(*catalog, dataflow, *storage)
        reference = downloads["REFERENCE"]
        tables = list(reference["DIMENSIONS"].values())
        # Tables de passage et millésimes SH : publiées par BACI dans le catalogue Comtrade
        if source is comtrade:
            tables += ["hs_concordance", "hs_vintages"]
        for table in tables:
            schema = reference_schema(reference["SCHEMA_PREFIX"], table)
            expected[f"reference.{downloads['CATALOG_ALIAS']}.{table}"] = _location(
                *catalog, schema, *storage
            )

    # BACI : un schéma par millésime cible actif, dans le catalogue Comtrade
    downloads, dataflow = comtrade["DOWNLOADS"], comtrade["DATAFLOW"]
    for label, target in active_targets(baci["CLASSIFICATIONS"]["TARGETS"]).items():
        expected[f"baci.{label.lower()}"] = _location(
            downloads["DBNAME"], downloads["CATALOG_ALIAS"], target["RESULT_SCHEMA"],
            downloads[dataflow]["BUCKET"], downloads[dataflow]["PATHS"]["DATA_PATH"],
        )

    # Résultats : catalogue des vulnérabilités
    catalog = vulnerabilities["VULNERABILITIES"]
    partners = catalog[eurostat["DATAFLOW"]]
    network = vulnerabilities["NETWORK_VULNERABILITIES"]
    scores, coherence = synthesis["SYNTHESIS"], synthesis["COHERENCE"]
    results = {
        "vulnerabilities.partners": (partners, partners["BUCKET"]),
        "vulnerabilities.network": (network, network["BUCKET"]),
        "synthesis.scores": (scores, scores["BUCKET"]),
        # La cohérence écrit dans le bucket de la synthèse
        "synthesis.diagnostics": (coherence, scores["BUCKET"]),
    }
    for name, (block, bucket) in results.items():
        expected[name] = _location(
            catalog["DBNAME"], catalog["CATALOG_ALIAS"], block["RESULT_SCHEMA"],
            bucket, block["PATHS"]["DATA_PATH"],
        )

    # Registres de fraîcheur
    registries = {
        "baci": (baci, baci["BUCKET"]),
        "partners": (partners, partners["BUCKET"]),
        "network": (network, network["BUCKET"]),
        "synthesis": (scores, scores["BUCKET"]),
        "coherence": (coherence, scores["BUCKET"]),
    }
    for step, (block, bucket) in registries.items():
        expected[f"state.{step}"] = ExpectedRegistry(
            str(block["STATE"]["PATH_TEMPLATE"]), bucket, step
        )

    # Cache des tables de correspondance SH, partagé par BACI et les métriques partenaires
    expected["baci.concordances"] = ConcordanceCache(
        str(baci["CLASSIFICATIONS"]["CONCORDANCE_PATH"]), baci.get("BUCKET")
    )

    # Restitution : catalogue `serving` et catalogues sources lus par ses requêtes
    locations, _ = source_tables(
        eurostat=eurostat, comtrade=comtrade, vulnerabilities=vulnerabilities, synthesis=synthesis
    )
    expected["serving.tables"] = ExpectedServing(
        DuckLakeLocation(
            dbname=serving["DBNAME"], catalog_alias=serving["CATALOG_ALIAS"],
            schema=serving["SCHEMA"], bucket=serving["BUCKET"], data_path=serving["DATA_PATH"],
        ),
        dict(locations),
    )
    return expected


# Fonction de lecture de ce que désigne un dataset instancié
def _designated(dataset: Any) -> Any:
    """Return what a project dataset designates, in the form of the expectations."""
    from kedro_pipeline.io.datasets import (
        ConcordanceCacheDataset,
        DuckLakeTableDataset,
        FreshnessRegistryDataset,
        ServingCatalogDataset,
    )

    if isinstance(dataset, DuckLakeTableDataset):
        return dataset.location
    if isinstance(dataset, FreshnessRegistryDataset):
        registry = dataset.load()
        return ExpectedRegistry(registry.path_template, registry.bucket, registry.step)
    if isinstance(dataset, ConcordanceCacheDataset):
        return dataset.load()
    if isinstance(dataset, ServingCatalogDataset):
        catalog = dataset.load()
        return ExpectedServing(catalog.location, dict(catalog.sources))
    return type(dataset).__name__


# Fonction de comparaison du catalogue et des paramètres
def catalog_mismatches(catalog: Any, parameters: Mapping[str, Any]) -> List[str]:
    """List the differences between the catalog and the parameters.

    Every expected dataset is fetched from the catalog (factories resolved by
    name, e.g. ``baci.hs2017``) and compared to what the parameters designate.
    No connection is opened.

    Args:
        catalog: Instantiated Kedro ``DataCatalog`` (``context.catalog``).
        parameters: Merged parameters of the same environment.

    Returns:
        One readable message per difference (empty when consistent).

    Examples:
        >>> catalog_mismatches(context.catalog, context.params)  # doctest: +SKIP
        []
    """
    messages: List[str] = []
    for name, expected in sorted(expected_datasets(parameters).items()):
        try:
            dataset = catalog.get(name)
        except Exception as exc:  # dataset inconnu ou invalide
            messages.append(f"{name}: not resolvable from the catalog ({exc})")
            continue
        if dataset is None:
            messages.append(f"{name}: missing from the catalog")
            continue
        designated = _designated(dataset)
        if designated != expected:
            messages.append(f"{name}: catalog designates {designated}, parameters expect {expected}")
    return messages
