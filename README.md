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
  - `storage/` — rôle résiduel : chargeurs / sauveurs tabulaires (`Loader`,
    `Saver`) pour `.xls` / `.xlsx` / `.parquet`, en local ou sur S3.
  - `tracking/` — abstraction de suivi d'expériences (implémentation MLflow
    optionnelle, `mlflow` importé paresseusement).
- **`scripts/`** — étapes du pipeline, paramétrées par `config/`.
- **`config/`** — configuration YAML des étapes.
- **`tests/`** — tests de caractérisation qui figent le comportement courant.

L'API publique du package expose les points d'entrée des pipelines :

```python
from macroforecast import run_baci, run_vulnerabilities, run_network_vulnerabilities
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
```

## Tests

```bash
pytest
```

## Feuille de route

Migration des étapes de `scripts/` vers Kedro (kedro-viz pour la documentation,
argo-kedro pour l'ordonnancement, kedro-mlflow pour le stockage des métriques).
