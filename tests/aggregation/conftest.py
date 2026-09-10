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
