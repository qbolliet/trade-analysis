"""Données fictives déterministes des tests de sens (import / export).

Deux jeux, tirés avec ``numpy.random.default_rng(0)`` :

- une table Comext partenaires à deux flux (``1`` import, ``2`` export), agrégats
  ``WORLD`` / ``EXT_EU`` / ``INT_EU27_2020`` compris, avec une cellule export
  privée de son agrégat extra-UE (contrôle des diagnostics) ;
- une table BACI réconciliée à trois produits sur deux années : ``P1`` a un
  exportateur dominant, ``P2`` un importateur dominant, ``P3`` est équilibré.

Les fichiers de ``golden/`` ont été calculés sur ces données par le code
d'avant l'introduction de l'hyperparamètre ``flow`` : ils sont l'oracle des
valeurs à l'import, qui doivent rester strictement identiques.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

# Répertoire des résultats de référence
GOLDEN_DIR = Path(__file__).resolve().parent / "golden"

# Grille partenaires fictive
PERIODS = ("2021", "2022")
REPORTERS = ("FR", "DE", "EU27_2020")
PRODUCTS = ("28053010", "85414000")
PARTNERS = ("CN", "US", "JP", "IT", "QA")
# Cellule export privée d'agrégat extra-UE (reporter, produit, période)
MISSING_EXTRA_EU_CELL = ("DE", "85414000", "2022")

# Graphe fictif
COUNTRIES = ("A", "B", "C", "D", "E", "F")
NETWORK_PRODUCTS = ("P1", "P2", "P3")
YEARS = (2020, 2021)
CLASSIFICATION = "HS2017"


# Fonction de construction de la table de faits Comext fictive
def partner_frame() -> pd.DataFrame:
    """Build a two-flow Comext-like partner table.

    Returns:
        Partner-level rows keyed by ``freq x reporter x product x flow x
        indicators x TIME_PERIOD x partner`` with an ``OBS_VALUE`` column.

    Examples:
        >>> sorted(partner_frame()["flow"].unique().tolist())
        [1, 2]
    """
    rng = np.random.default_rng(0)
    rows: List[dict] = []
    for period in PERIODS:
        for reporter in REPORTERS:
            for product in PRODUCTS:
                for flow in (1, 2):
                    values = rng.uniform(1.0, 100.0, len(PARTNERS))
                    # Concentration marquée sur le premier partenaire d'une cellule sur deux
                    if rng.uniform() < 0.5:
                        values[0] *= 10
                    total = float(values.sum())
                    extra_share = float(rng.uniform(0.3, 0.95))
                    cell = [*zip(PARTNERS, values), ("WORLD", total)]
                    if (reporter, product, period) != MISSING_EXTRA_EU_CELL or flow == 1:
                        cell.append(("EXT_EU", total * extra_share))
                    cell.append(("INT_EU27_2020", total * (1 - extra_share)))
                    rows.extend(
                        {
                            "freq": "A", "reporter": reporter, "product": product,
                            "flow": flow, "indicators": "VALUE_IN_EUROS",
                            "TIME_PERIOD": period, "partner": partner,
                            "OBS_VALUE": float(value),
                        }
                        for partner, value in cell
                    )
    return pd.DataFrame(rows)


# Fonction de construction de la table BACI fictive
def network_frame() -> pd.DataFrame:
    """Build a BACI-like reconciled flow table with oriented concentrations.

    ``P1``: exporter ``A`` supplies most of every importer; ``P2``: importer
    ``A`` absorbs most of every exporter's sales; ``P3``: balanced flows.

    Returns:
        Rows ``classification x product x year x exporter x importer`` with a
        ``reconciled_value`` column.

    Examples:
        >>> network_frame()["product"].nunique()
        3
    """
    rng = np.random.default_rng(0)
    rows: List[dict] = []
    for year in YEARS:
        for product in NETWORK_PRODUCTS:
            for exporter in COUNTRIES:
                for importer in COUNTRIES:
                    if exporter == importer or rng.uniform() < 0.2:
                        continue
                    value = float(rng.uniform(1.0, 10.0))
                    if product == "P1" and exporter == "A":
                        value *= 50
                    if product == "P2" and importer == "A":
                        value *= 50
                    rows.append(
                        {
                            "classification": CLASSIFICATION, "product": product,
                            "year": year, "exporter": exporter, "importer": importer,
                            "reconciled_value": value,
                        }
                    )
    return pd.DataFrame(rows)


# Fonction de transposition du graphe (échange exportateur / importateur)
def transposed(frame: pd.DataFrame) -> pd.DataFrame:
    """Swap the exporter and importer columns of a BACI-like table.

    Args:
        frame: Table returned by :func:`network_frame`.

    Returns:
        The same edges, reversed.

    Examples:
        >>> df = network_frame()
        >>> (transposed(df)["exporter"] == df["importer"]).all()
        True
    """
    return frame.rename(columns={"exporter": "importer", "importer": "exporter"})
