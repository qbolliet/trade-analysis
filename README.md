# trade-analysis

Pipeline de calcul de scores de vulnérabilité du commerce international, du
preprocessing des flux bilatéraux jusqu'aux métriques de dépendance et de réseau.

## Structure

- **`macroforecast/`** — le package. Aucune valeur méthodologique n'y est
  hardcodée : les paramètres sont fournis à l'exécution via les fichiers de
  `config/`.
  - `trade/processing/` — redressement des flux BACI (conversion tonnage, modèle
    gravitaire CIF, fobisation, réconciliation miroir, réallocation « Areas NES »,
    harmonisation des nomenclatures HS).
  - `trade/vulnerabilities/` — métriques de vulnérabilité partenaires (HHI, indices
    de concentration/dépendance) et de réseau (centralité, clustering, diamètre,
    single point of failure), avec leurs diagnostics structurés.
  - `trade/aggregation/` — synthèse multicritère des métriques de vulnérabilité
    (partenaires + réseau) en scores agrégés, méthodologie pure sans I/O. Registre
    déclaratif de méthodes (`methods.py`, configuration → pipeline sklearn) couvrant
    six familles : ordres partiels (Pareto), pondérations endogènes (entropie,
    CRITIC, ACP, *benefit of the doubt*), fonctions d'agrégation (somme pondérée,
    moyenne géométrique, MPI, TOPSIS, VIKOR, Mahalanobis, projection blanchie),
    rangs multivariés par transport optimal (score de Kantorovitch orienté, extra
    `optimal-transport`), exploration des poids (SMAA, quantile de cône) et
    comparaison/contrôle de cohérence (accord de classements, stabilité,
    consensus). Orchestrée à **trois niveaux** de comparaison (`by_product`,
    `by_reporter`, `global`) par deux scripts :
    - `compute_synthetic_scores.py` (`vulnerabilities-synthesis-script`) — calcule
      les scores synthétiques par méthode et niveau, écrits dans le schéma
      DuckLake `synthesis` ;
    - `compute_synthesis_coherence.py` (`vulnerabilities-coherence-script`) —
      contrôle la cohérence entre métriques et entre méthodes de synthèse, écrit
      dans le schéma `synthesis_diagnostics`.

    Convention de rang partagée par tout le module : **rang 1 = unité la plus
    vulnérable**.
  - `storage/` — rôle résiduel : chargeurs / sauveurs tabulaires (`Loader`,
    `Saver`) pour `.xls` / `.xlsx` / `.parquet`, en local ou sur S3.
  - `tracking/` — abstraction de suivi d'expériences (implémentation MLflow
    optionnelle, `mlflow` importé paresseusement).
- **`scripts/`** — étapes du pipeline, paramétrées par `config/`. Ordre
  d'exécution : téléchargement (`comtrade-script` / `eurostat-script`) → redressement
  BACI (`baci-script` / `baci-hs-script`) → vulnérabilités partenaires et de réseau
  (`vulnerabilities-eurostat-script` / `vulnerabilities-network-script`) → synthèse
  (`vulnerabilities-synthesis-script`) → cohérence (`vulnerabilities-coherence-script`).
- **`config/`** — configuration YAML des étapes, dont `config/synthesis.yaml`
  (blocs `SYNTHESIS` et `COHERENCE`).
- **`tests/`** — tests de caractérisation qui figent le comportement courant.
  Certains tests de bout en bout (catalogue DuckLake temporaire) sont marqués
  `slow` (cf. `pyproject.toml`) ; `pytest` seul les exécute, `pytest -m "not slow"`
  les exclut pour une boucle de développement plus rapide.

L'API publique du package expose les points d'entrée des pipelines :

```python
from macroforecast import (
    run_baci,
    run_vulnerabilities,
    run_network_vulnerabilities,
    run_synthesis,
    run_coherence,
)
```

## Dépendance `statflows`

L'acquisition des données sources (clients Comtrade / Eurostat / UNSD, conventions
SDMX, rate limiting, orchestration des mises à jour) et le stockage générique
(JSON, tables DuckLake, connexion S3) sont fournis par la dépendance externe
**`statflows`** (tirée avec les extras `s3` et `ducklake`). Frontière :
`statflows` gère l'acquisition et la persistance des données brutes,
`macroforecast` ne porte que la méthodologie.

L'écriture DuckLake (`statflows.storage.ducklake.tables.write_dataframe`) est le
seul point qui requiert `dt-ducklake-manager` : son absence lève une `ImportError`
explicite, le reste du package s'importe et s'exécute sans lui.

## Installation

```bash
pip install -e .
# suivi d'expériences MLflow
pip install -e ".[tracking]"
# score de Kantorovitch orienté (macroforecast.trade.aggregation.optimal_transport) :
# dépendance lourde jax/ott-jax, import paresseux, ImportError explicite en son absence
pip install -e ".[optimal-transport]"
```

## Tests

```bash
pytest
# exclut les tests de bout en bout marqués `slow` (catalogue DuckLake temporaire)
pytest -m "not slow"
```

## Feuille de route

Migration des étapes de `scripts/` vers Kedro (kedro-viz pour la documentation,
argo-kedro pour l'ordonnancement, kedro-mlflow pour le stockage des métriques).
