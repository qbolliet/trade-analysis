"""Données fictives multi-années pour les tests d'équivalence du redressement BACI.

Génère des déclarations COMTRADE brutes (deux côtés miroirs) sur plusieurs années,
pays et produits, couvrant les cas que la méthodologie traite à part : unités de
quantité hétérogènes (kg, tonnes, unités, m²), poids net parfois absent, pays FAS
(``CAN``), importateur déclarant FOB, flux « Areas NES » (899), agrégats Monde (0) et
« Other Asia, nes » (490), paire interne BEL–LUX, sous-déclarations ventilées sur
plusieurs lignes (régime douanier) et quelques observations aberrantes (Cook).
"""

from __future__ import annotations

import itertools
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# Pays individuels (ISO-3), dont le pays FAS, l'importateur FOB et la paire BEL–LUX
COUNTRIES: Tuple[str, ...] = (
    "AUS", "AUT", "BEL", "BRA", "CAN", "CHE", "CHN", "DEU", "ESP", "FRA", "GBR", "IDN",
    "IND", "ITA", "JPN", "KOR", "LUX", "MEX", "NLD", "NOR", "POL", "SWE", "USA", "ZAF",
)
# Importateur déclarant ses importations FOB (colonne fobvalue renseignée)
FOB_IMPORTER = "AUS"
# Pays enclavés
LANDLOCKED = ("AUT", "CHE", "LUX")
# Années du jeu
YEARS: Tuple[int, ...] = (2018, 2019, 2020, 2021, 2022)


# Fonction de construction des codes produits (trois chapitres SH2)
def products() -> List[str]:
    """Return 36 HS6 codes spread over chapters 28, 84 and 85."""
    return [f"{chapter}{index:04d}" for chapter in ("28", "84", "85") for index in range(1, 13)]


# Fonction de génération des déclarations COMTRADE fictives
def comtrade_declarations(
    seed: int = 0, density: float = 0.3, years: Tuple[int, ...] = YEARS
) -> pd.DataFrame:
    """Generate raw COMTRADE declarations over several years.

    Args:
        seed: Seed of the random generator.
        density: Probability that a given ``(exporter, importer, product, year)``
            flow exists.
        years: Years generated.

    Returns:
        Raw declarations with the columns of ``required_columns()``.
    """
    rng = np.random.default_rng(seed)
    codes = {iso: 100 + i for i, iso in enumerate(COUNTRIES)}
    prods = products()
    # Unité de déclaration propre au produit : kg (8), tonnes (21), unités (5), m² (12)
    product_unit = {p: [8, 8, 21, 5, 12][i % 5] for i, p in enumerate(prods)}
    # Poids unitaire (tonnes par unité source) des produits déclarés hors poids
    unit_weight = {p: float(rng.uniform(0.001, 0.05)) for p in prods}
    # Valeur unitaire de référence du produit (milliers USD par tonne)
    base_uv = {p: float(np.exp(rng.normal(1.0, 1.0))) for p in prods}
    # Qualité de déclaration propre à chaque pays (dispersion log-normale)
    noise = {iso: float(rng.uniform(0.02, 0.4)) for iso in COUNTRIES}
    # Distance bilatérale (sert aussi à générer le fret)
    distance = distances(seed)

    rows: List[Dict] = []
    pairs = list(itertools.permutations(COUNTRIES, 2))
    for year in years:
        # Pays ne déclarant pas cette année-là (couverture incomplète)
        silent = set(rng.choice(COUNTRIES, size=2, replace=False))
        for product in prods:
            for exporter, importer in pairs:
                if rng.random() > density:
                    continue
                tonnes = float(np.exp(rng.normal(3.0, 1.5)))
                value = tonnes * base_uv[product] * float(np.exp(rng.normal(0.0, 0.3)))
                dist = distance[(exporter, importer)]
                freight = 0.01 + 0.012 * np.log(dist / 400.0) + 0.02 * (importer in LANDLOCKED)
                # Déclaration de l'exportateur (FOB)
                if exporter not in silent:
                    v_x = value * float(np.exp(rng.normal(0.0, noise[exporter])))
                    rows.extend(
                        _side_rows(
                            rng, "X", exporter, importer, codes[importer], product, year, v_x,
                            tonnes * float(np.exp(rng.normal(0.0, noise[exporter]))),
                            product_unit[product], unit_weight[product], fob=True,
                        )
                    )
                # Déclaration de l'importateur (CAF, sauf importateur FOB et pays FAS)
                if importer not in silent:
                    cif = importer not in (FOB_IMPORTER, "CAN")
                    v_m = value * float(np.exp(rng.normal(0.0, noise[importer])))
                    v_m *= (1.0 + freight) if cif else 1.0
                    # Quelques valeurs aberrantes (influentes au sens de Cook)
                    if rng.random() < 0.004:
                        v_m *= float(np.exp(rng.choice([-3.0, 3.0])))
                    rows.extend(
                        _side_rows(
                            rng, "M", importer, exporter, codes[exporter], product, year, v_m,
                            tonnes * float(np.exp(rng.normal(0.0, noise[importer]))),
                            product_unit[product], unit_weight[product], fob=not cif,
                        )
                    )
            # Exportations vers « Areas NES » (899), agrégats Monde (0) et 490
            for exporter in rng.choice(COUNTRIES, size=4, replace=False):
                rows.append(_aggregate_row(rng, exporter, "_X", 899, product, year))
            rows.append(_aggregate_row(rng, COUNTRIES[0], "W00", 0, product, year))
            rows.append(_aggregate_row(rng, COUNTRIES[1], "S19", 490, product, year))
    df = pd.DataFrame(rows)
    df["classificationCode"] = "H5"
    return df


# Fonction de génération des lignes d'un côté de déclaration
def _side_rows(
    rng: np.random.Generator,
    flow: str,
    reporter: str,
    partner: str,
    partner_code: int,
    product: str,
    year: int,
    value: float,
    tonnes: float,
    unit: int,
    unit_weight: float,
    *,
    fob: bool,
) -> List[Dict]:
    """Return the declaration rows of one side, sometimes split over two rows."""
    # Quantité dans l'unité du produit ; certains déclarants passent au kg
    declared_unit = unit if rng.random() < 0.8 else 8
    if declared_unit == 8:
        qty = tonnes * 1000.0
    elif declared_unit == 21:
        qty = tonnes
    else:
        qty = tonnes / unit_weight
    # Poids net (kg) parfois absent
    net_weight = tonnes * 1000.0 if rng.random() < 0.6 else np.nan
    parts = [1.0] if rng.random() < 0.85 else [0.4, 0.6]
    rows = []
    for share in parts:
        rows.append(
            {
                "flowCode": flow,
                "reporterISO": reporter,
                "partnerISO": partner,
                "partnerCode": partner_code,
                "cmdCode": product,
                "period": str(year),
                "primaryValue": value * share,
                "qty": qty * share,
                "qtyUnitCode": declared_unit,
                "netWgt": net_weight * share,
                "cifvalue": 0.0 if (fob or flow == "X") else value * share,
                "fobvalue": value * share if (fob or flow == "X") else 0.0,
            }
        )
    return rows


# Fonction de génération d'une déclaration vers un agrégat de partenaires
def _aggregate_row(
    rng: np.random.Generator, reporter: str, partner_iso: str, partner_code: int, product: str, year: int
) -> Dict:
    """Return one export declaration towards an aggregate partner."""
    value = float(np.exp(rng.normal(4.0, 1.0)))
    return {
        "flowCode": "X",
        "reporterISO": reporter,
        "partnerISO": partner_iso,
        "partnerCode": partner_code,
        "cmdCode": product,
        "period": str(year),
        "primaryValue": value,
        "qty": value * 10.0,
        "qtyUnitCode": 8,
        "netWgt": value * 10.0,
        "cifvalue": 0.0,
        "fobvalue": value,
    }


# Fonction de génération des distances bilatérales
def distances(seed: int = 0) -> Dict[Tuple[str, str], float]:
    """Return a symmetric bilateral distance (km) per ordered pair."""
    rng = np.random.default_rng(seed + 1)
    out: Dict[Tuple[str, str], float] = {}
    for a, b in itertools.combinations(COUNTRIES, 2):
        out[(a, b)] = out[(b, a)] = float(rng.uniform(300.0, 18000.0))
    return out


# Fonction de génération des tables CEPII
def cepii_tables(seed: int = 0) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return the ``dist_cepii`` and ``geo_cepii`` tables of the fictive world."""
    rng = np.random.default_rng(seed + 2)
    distance = distances(seed)
    df_dist = pd.DataFrame(
        [(a, b, d) for (a, b), d in distance.items()], columns=["iso_o", "iso_d", "distw"]
    )
    neighbours = {frozenset(pair) for pair in itertools.combinations(COUNTRIES, 2) if rng.random() < 0.08}
    df_dist["contig"] = [int(frozenset((a, b)) in neighbours) for a, b in zip(df_dist.iso_o, df_dist.iso_d)]
    df_geo = pd.DataFrame({"iso3": list(COUNTRIES), "landlocked": [int(c in LANDLOCKED) for c in COUNTRIES]})
    return df_dist, df_geo


# Fonction de découpage des déclarations en tranches
def chunks_by_year(df: pd.DataFrame, *, blocks: int = 1) -> List[Tuple[object, pd.DataFrame]]:
    """Split declarations by year, then optionally into blocks of HS2 chapters.

    Args:
        df: Raw declarations.
        blocks: Number of chapter blocks per year (``1``: whole years).

    Returns:
        List of ``(ChunkKey, frame)``, year-major.
    """
    from macroforecast.trade.processing import ChunkKey

    chapters = sorted(df["cmdCode"].str[:2].unique())
    groups = np.array_split(np.array(chapters), blocks)
    out = []
    for year in sorted(df["period"].astype(int).unique()):
        df_year = df[df["period"].astype(int) == year]
        for block, chapter_group in enumerate(groups):
            part = df_year[df_year["cmdCode"].str[:2].isin(list(chapter_group))]
            out.append((ChunkKey(int(year), block), part.reset_index(drop=True)))
    return out
