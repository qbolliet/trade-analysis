"""Fixtures synthétiques déterministes de la suite ``aggregation``.

Toutes les matrices sont tirées avec ``numpy.random.default_rng(0)`` : la
suite est reproductible d'une exécution à l'autre. Aucune fixture ne dépend
de S3, de DuckLake ou de ``jax``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


# ──────────────────────────────────────────────────────────────────────
# Matrices de métriques synthétiques
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def X_uniform() -> np.ndarray:
    """Matrice ``(500, 3)`` de tirages uniformes indépendants sur ``[0, 1)``.

    Métriques quasi indépendantes, toutes positives : le cas de référence
    pour la dominance de Pareto, les pondérations et les fonctions
    d'agrégation exigeant ``X >= 0``.
    """
    rng = np.random.default_rng(0)
    return rng.random((500, 3))


@pytest.fixture
def X_correlated() -> np.ndarray:
    """Matrice ``(300, 4)`` : un facteur commun latent plus un bruit propre.

    Structure factorielle forte (une seule dimension dominante) : sert aux
    diagnostics KMO / Bartlett et à la pondération CRITIC (redondance).
    """
    rng = np.random.default_rng(0)
    common = rng.normal(size=300)
    columns = [common + rng.normal(scale=0.4, size=300) for _ in range(4)]
    return np.column_stack(columns)


@pytest.fixture
def X_minmax(X_uniform: np.ndarray) -> np.ndarray:
    """Normalisation min–max, colonne par colonne, de :func:`X_uniform`.

    Valeurs dans ``[0, 1]`` avec au moins un ``0`` et un ``1`` exacts par
    colonne — le cas dégénéré du *benefit of the doubt* (M-03) et l'entrée
    admissible des pondérations entropie / CRITIC.
    """
    minimum = X_uniform.min(axis=0)
    maximum = X_uniform.max(axis=0)
    return (X_uniform - minimum) / (maximum - minimum)


@pytest.fixture
def X_with_constant_column() -> np.ndarray:
    """Matrice ``(200, 3)`` dont la colonne d'indice 1 est constante.

    Cas dégénéré : écart-type nul, corrélation indéfinie
    (``np.corrcoef`` renvoie ``NaN``). Cible des correctifs I-06 (CRITIC) et
    M-12 (entropie).
    """
    rng = np.random.default_rng(0)
    X = rng.random((200, 3))
    X[:, 1] = 0.5  # 0.5 exactement représentable : écart-type strictement nul
    return X


@pytest.fixture
def df_metrics_toy() -> pd.DataFrame:
    """Table large jouet : colonne ``id`` + 3 métriques, 40 lignes.

    Trois lignes portent une valeur manquante (une par métrique) : support
    des tests de propagation des ``NaN`` (I-16 / D-15).
    """
    rng = np.random.default_rng(0)
    n = 40
    df = pd.DataFrame(
        {
            "id": [f"cell_{i:02d}" for i in range(n)],
            "HHI": rng.random(n),
            "CDI2": rng.random(n),
            "CDI3": rng.random(n),
        }
    )
    # Valeurs manquantes : une par métrique, sur trois lignes distinctes
    df.loc[5, "CDI2"] = np.nan
    df.loc[17, "HHI"] = np.nan
    df.loc[31, "CDI3"] = np.nan
    return df


@pytest.fixture
def df_synthesis_toy() -> pd.DataFrame:
    """Table de cellules jouet : 2 contextes x 3 pays x 20 produits, 4 métriques.

    Reproduit la forme de la table d'entrée de la synthèse (S-2.3) : les
    quatre clés de contexte, ``reporter``, ``product``, puis quatre métriques
    de polarité positive. La métrique ``EXPORT_HHI`` porte des valeurs
    manquantes (jointure réseau incomplète), support des tests de la règle
    « lignes complètes par méthode » (D-15).
    """
    rng = np.random.default_rng(0)
    reporters = ("FR", "DE", "IT")
    products = tuple(f"P{index:02d}" for index in range(20))
    keys = [
        {
            "freq": "A",
            "flow": 1,
            "indicators": "VALUE_IN_EUROS",
            "TIME_PERIOD": period,
            "reporter": reporter,
            "product": product,
        }
        for period in ("2023", "2024")
        for reporter in reporters
        for product in products
    ]
    df_cells = pd.DataFrame(keys)
    n = len(df_cells)
    for metric in ("HHI", "CDI2", "CDI3", "EXPORT_HHI"):
        df_cells[metric] = rng.random(n)
    # Cellules privées de la métrique de réseau : lignes incomplètes (D-15)
    df_cells.loc[[3, 7, 42, 61, 95, 110], "EXPORT_HHI"] = np.nan
    return df_cells
