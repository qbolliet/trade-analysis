"""Clients d'API factices de l'environnement Kedro ``test`` (aucun appel réseau).

Le catalogue de ``config/test/`` désigne ces classes à la place des clients
``statflows`` (datasets ``clients.*``) : le pipeline complet tourne alors sur un
petit monde simulé (10 pays, 4 produits SH6, 2018-2022), sans réseau ni secret.

* :class:`FakeComtradeClient` — ``ComtradeClient`` dont seuls les appels réseau
  sont remplacés : les déclarations tariffline viennent du monde simulé (client
  fictif du paquet), les métadonnées de référence (codelists) sont construites
  ici. Il écrit aussi, au besoin, les fichiers de gravité CEPII du monde simulé
  sous la racine de test (lus par la préparation BACI).
* :class:`FakeEurostatClient` — client Comext minimal : structure du dataflow,
  codelists et observations construites depuis le même monde ; une requête déjà
  téléchargée ne renvoie rien (comme l'API sans nouvelle publication).
* :class:`FakeUNSDClient` — tables de correspondance SH identité sur les produits
  du monde.

Ce module est autonome (il n'importe pas ``conftest``) : la session Kedro
l'importe par son chemin pointé ``tests.pipeline.fakes``.
"""

from __future__ import annotations

import functools
import itertools
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from kedro_pipeline.synthetic.comext import ComextConfig, ComextTemplate, build_comext
from kedro_pipeline.synthetic.comtrade import (
    CountryReference,
    ReportingConfig,
    SyntheticComtradeClient,
)
from kedro_pipeline.synthetic.world import SyntheticWorld, WorldConfig

# Pays du monde simulé : (ISO3, ISO2, code M49, taille, région)
COUNTRIES = (
    ("FRA", "FR", 251, 3.0, "EU"),
    ("DEU", "DE", 276, 4.0, "EU"),
    ("ITA", "IT", 380, 2.0, "EU"),
    ("CHN", "CN", 156, 18.0, "ASIA"),
    ("USA", "US", 842, 27.0, "NORTH_AMERICA"),
    ("JPN", "JP", 392, 4.0, "ASIA"),
    ("KOR", "KR", 410, 2.0, "ASIA"),
    ("COD", "CD", 180, 0.1, "AFRICA"),
    ("CAN", "CA", 124, 2.0, "NORTH_AMERICA"),
    ("BRA", "BR", 76, 2.0, "SOUTH_AMERICA"),
)
# Produits SH6 (millésime HS2017) et leurs libellés
PRODUCTS = {
    "280519": "Alkali or alkaline-earth metals",
    "300490": "Medicaments, n.e.s.",
    "810510": "Cobalt mattes and unwrought cobalt",
    "854110": "Diodes",
}
# Section du monde simulé (mêmes clés que la configuration des données fictives)
SECTION: Dict[str, Any] = {
    "SEED": 20261009,
    "YEARS": {"START": 2018, "END": 2022},
    "COUNTRIES": [
        {"iso3": iso3, "iso2": iso2, "size": size, "region": region}
        for iso3, iso2, _, size, region in COUNTRIES
    ],
    "MODEL": {
        "DENSITY": 0.8,
        "MIN_VALUE": 100,
        "CONCENTRATION": {"DEFAULT": 1.0, "BY_PREFIX": {"8105": 0.5}},
        "SUPPLIER_BIAS": {"8105": {"COD": 5.0}},
    },
    "REPORTING": {"FOB_IMPORT_REPORTERS": ["CAN"], "NES_SHARE": 0.02},
}
# Codes Comtrade des agrégats de partenaires
_WORLD, _NES = (0, "World"), (899, "Areas, nes")
# Dataflow Comext et codelists de ses dimensions
COMEXT_DATAFLOW = "DS-045409"
# Types des colonnes d'une table Comext réelle : flux entier (1 = importation,
# 2 = exportation), période en texte, valeur flottante
COMEXT_DTYPES = {
    "freq": "object", "reporter": "object", "partner": "object", "product": "object",
    "flow": "int64", "indicators": "object", "TIME_PERIOD": "object", "OBS_VALUE": "float64",
}
_COMEXT_DIMENSIONS = (
    ("freq", None),
    ("reporter", "CXT_REPORTER"),
    ("partner", "CXT_PARTNER"),
    ("product", "CXT_PRODUCT"),
    ("flow", "CXT_FLOW"),
    ("indicators", "CXT_INDICATORS"),
)


# Monde simulé partagé (construit une fois par processus)
@functools.lru_cache(maxsize=None)
def simulated_world() -> SyntheticWorld:
    """Return the simulated world of the ``test`` environment."""
    return SyntheticWorld(WorldConfig.from_mapping(SECTION))


# Référence des pays (codes M49 et ISO2) du monde simulé
@functools.lru_cache(maxsize=None)
def country_reference() -> CountryReference:
    """Return the country reference of the simulated world, without network."""
    return CountryReference.from_codelist(_reporter_metadata(), [c[0] for c in COUNTRIES])


# Métadonnées Comtrade de la catégorie « reporter »
def _reporter_metadata() -> pd.DataFrame:
    return pd.DataFrame({
        "reporterCode": [m49 for _, _, m49, _, _ in COUNTRIES],
        "reporterDesc": [iso3.title() for iso3, *_ in COUNTRIES],
        "reporterCodeIsoAlpha2": [iso2 for _, iso2, *_ in COUNTRIES],
        "reporterCodeIsoAlpha3": [iso3 for iso3, *_ in COUNTRIES],
        "isGroup": False,
        "entryEffectiveDate": "1900-01-01",
        "entryExpiredDate": None,
    })


# Métadonnées Comtrade de toutes les catégories utilisées par le téléchargement
@functools.lru_cache(maxsize=None)
def _comtrade_metadata() -> Dict[Optional[str], pd.DataFrame]:
    partners = [(m49, iso3.title(), iso3) for iso3, _, m49, _, _ in COUNTRIES]
    partners += [(_WORLD[0], _WORLD[1], "W00"), (_NES[0], _NES[1], "X1")]
    chapters = sorted({code[:2] for code in PRODUCTS})
    headings = sorted({code[:4] for code in PRODUCTS})
    return {
        None: pd.DataFrame({
            "category": ["reporter", "partner", "cmd:HS", "flow"],
            "fileuri": ["reporter.json", "partner.json", "HS.json", "flow.json"],
        }),
        "reporter": _reporter_metadata(),
        "partner": pd.DataFrame({
            "PartnerCode": [code for code, _, _ in partners],
            "PartnerDesc": [label for _, label, _ in partners],
            "PartnerCodeIsoAlpha3": [iso3 for _, _, iso3 in partners],
            "isGroup": False,
            "entryExpiredDate": None,
        }),
        "cmd:HS": pd.DataFrame({
            "id": chapters + headings + list(PRODUCTS),
            "text": [f"Chapter {c}" for c in chapters] + [f"Heading {h}" for h in headings]
            + list(PRODUCTS.values()),
            "parent": ["TOTAL"] * len(chapters) + [h[:2] for h in headings] + [p[:4] for p in PRODUCTS],
        }),
        "flow": pd.DataFrame({"id": ["M", "X"], "text": ["Import", "Export"]}),
    }


# Écriture des fichiers de gravité CEPII du monde simulé
def ensure_cepii_files(root: Any) -> Dict[str, Path]:
    """Write the CEPII distance and geography files of the simulated world, if absent.

    Args:
        root: Root directory of the ``test`` environment.

    Returns:
        Paths of the ``dist`` and ``geo`` Parquet files (``<root>/cepii/``).
    """
    folder = Path(root) / "cepii"
    paths = {"dist": folder / "dist_cepii.parquet", "geo": folder / "geo_cepii.parquet"}
    if all(path.exists() for path in paths.values()):
        return paths
    folder.mkdir(parents=True, exist_ok=True)
    isos = [iso3 for iso3, *_ in COUNTRIES]
    dist = pd.DataFrame(list(itertools.permutations(isos, 2)), columns=["iso_o", "iso_d"])
    dist["distw"] = np.random.default_rng(SECTION["SEED"]).uniform(500, 15000, len(dist))
    dist["contig"] = 0
    dist.to_parquet(paths["dist"], index=False)
    pd.DataFrame({"iso3": isos, "landlocked": 0}).to_parquet(paths["geo"], index=False)
    return paths


# Client Comtrade factice
class FakeComtradeClient(SyntheticComtradeClient):
    """``ComtradeClient`` answering from the simulated world, without any network call.

    Args:
        root: Root directory of the ``test`` environment, where the CEPII
            files of the world are written when missing (``None``: none).
        **kwargs: Forwarded to the synthetic client (``subscription_key`` is
            ignored).
    """

    def __init__(self, root: Optional[str] = None, **kwargs: Any) -> None:
        kwargs.pop("subscription_key", None)
        super().__init__(
            simulated_world(),
            country_reference(),
            ReportingConfig.from_mapping(SECTION["REPORTING"]),
            product_labels=PRODUCTS,
            product_universe=list(PRODUCTS),
            **kwargs,
        )
        if root:
            ensure_cepii_files(root)

    # Métadonnées de référence construites localement (au lieu des fichiers de l'API)
    def get_metadata(self, category: Optional[str] = None, refresh: bool = False) -> pd.DataFrame:
        metadata = _comtrade_metadata()
        if category not in metadata:
            raise ValueError(f"Invalid 'category' : {category}.")
        return metadata[category].copy()


# Client Comext factice
class FakeEurostatClient:
    """Minimal Comext client answering from the simulated world, without any network call.

    Implements what the download task uses: the dataflow structure, the
    codelists of its dimensions, and the incremental fetch of one query
    (nothing new once a query was downloaded).

    Args:
        root: Root directory of the ``test`` environment (unused).
    """

    PROVIDER_CONFIG_NAME = "eurostat"

    def __init__(self, root: Optional[str] = None, **kwargs: Any) -> None:
        from statflows.core.structures import DataflowStructureRegistry

        self.structure_registry = DataflowStructureRegistry()
        self._iso2_by_iso3 = {iso3: iso2 for iso3, iso2, *_ in COUNTRIES}

    # Structure (DSD) du dataflow Comext
    def get_dataflow_structure(self, dataflow: str, version: str = "+") -> Any:
        from statflows.core.structures import DataflowStructure, DimensionInfo

        return DataflowStructure(
            agency="ESTAT",
            dataflow=dataflow,
            num_dimensions=len(_COMEXT_DIMENSIONS),
            dimensions=[
                DimensionInfo(name, position, codelist=codelist)
                for position, (name, codelist) in enumerate(_COMEXT_DIMENSIONS)
            ],
        )

    # Structure d'une requête (clés primaires de la table)
    def resolve_query_structure(self, query: Any) -> Any:
        return self.get_dataflow_structure(query.dataflow)

    # Codelists des dimensions, avec libellés et parents
    def get_codelist(self, codelist_id: str, agency: str = "ESTAT", refresh: bool = False) -> pd.DataFrame:
        if codelist_id == "CXT_REPORTER":
            codes = [iso2 for _, iso2, _, _, region in COUNTRIES if region == "EU"] + ["EU27_2020"]
            return pd.DataFrame({"code": codes, "label": codes, "parent": None})
        if codelist_id == "CXT_PARTNER":
            codes = [iso2 for _, iso2, *_ in COUNTRIES] + ["WORLD", "EXT_EU27_2020", "INT_EU27_2020"]
            return pd.DataFrame({"code": codes, "label": codes, "parent": None})
        if codelist_id == "CXT_PRODUCT":
            return pd.DataFrame({
                "code": list(PRODUCTS), "label": list(PRODUCTS.values()),
                "parent": [code[:4] for code in PRODUCTS],
            })
        raise KeyError(f"Codelist {codelist_id} is not simulated")

    # Requête complète : observations du monde simulé
    def execute_query(self, query: Any) -> pd.DataFrame:
        dimensions = dict(query.dimensions)
        return build_comext(
            simulated_world(), self._iso2_by_iso3, ComextConfig(), ComextTemplate(dtypes=COMEXT_DTYPES),
            reporter=str(dimensions["reporter"]), product=str(dimensions["product"]),
            dimensions=dimensions, start_period=query.start_period,
        )

    # Téléchargement incrémental : rien de nouveau après le premier téléchargement
    def fetch_updates(self, query: Any, since: Optional[datetime], n_observations: int = 10) -> pd.DataFrame:
        if since is not None:
            return pd.DataFrame()
        return self.execute_query(query)

    def close(self) -> None:
        return None


# Client UNSD factice
class FakeUNSDClient:
    """UNSD client returning identity correspondence tables on the simulated products.

    Args:
        root: Root directory of the ``test`` environment (unused).
    """

    def __init__(self, root: Optional[str] = None, **kwargs: Any) -> None:
        self._vintages = ("HS1992", "HS1996", "HS2002", "HS2007", "HS2012", "HS2017", "HS2022")

    # Catalogue des tables publiées
    def list_available_tables(self) -> pd.DataFrame:
        pairs = [(s, t) for s, t in itertools.permutations(self._vintages, 2)]
        return pd.DataFrame({
            "source_classification": [s for s, _ in pairs],
            "target_classification": [t for _, t in pairs],
            "filename": [f"{s}-{t}.xlsx" for s, t in pairs],
            "extension": ".xlsx",
            "url": [f"https://unsd.invalid/{s}-{t}.xlsx" for s, t in pairs],
        })

    # Table de correspondance (identité : les produits simulés existent dans tous les millésimes)
    def get_correspondence(self, source: str, target: str, *, kind: str = "conversion") -> pd.DataFrame:
        return pd.DataFrame({
            "source_classification": source,
            "source_code": list(PRODUCTS),
            "target_classification": target,
            "target_code": list(PRODUCTS),
            "relationship": pd.NA,
        })

    def close(self) -> None:
        return None
