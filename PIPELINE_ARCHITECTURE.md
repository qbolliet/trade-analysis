# Architecture du pipeline de production — Kedro · Argo · MLflow · DuckLake · Superset

> **Statut** : référence de conception, rédigée le 2026-09-15 à partir de la lecture du
> code de la branche `qb-vulnerabilities` (commit `6f24c6c`), des dépendances installées
> (`statflows 0.1.0`, `dt-ducklake-manager 0.3.1`, `duckdb 1.5.3`) et du code source des
> plugins `argo-kedro 0.1.41`, `kedro-mlflow 2.0.3`, `kedro-viz 12.4.0` (`kedro 1.6.0`).
> **Révisions 1 et 2 du 2026-09-18** (voir le journal ci-dessous). Aucune vérification n'a pu
> être faite sur le cluster (cf. C-20) : tout ce qui touche à Onyxia est une
> **hypothèse**, recensée en §8 (questions ouvertes) ; les opérations qui **doivent** être
> exécutées depuis Onyxia sont listées en §12.
>
> **Compagnon** : `PIPELINE_PROMPTS.md` (liste de prompts d'implémentation, chacun
> exécutable dans une session Claude Code séparée). Ce document fait foi en cas de
> divergence.

### Journal des révisions

| Date | Changement | Sections touchées |
|---|---|---|
| 2026-09-15 | Version initiale | — |
| 2026-09-18 | Dates de début par source : **1988** (Comext) et **1994** (Comtrade) au lieu de 1992 ; millésimes BACI recalés | C-02, PD-08, PS-04, PQ-05 |
| 2026-09-18 | BACI : **estimation exacte par passes et statistiques suffisantes** (mémoire bornée à une année, aucune approximation) ; unité de fraîcheur = millésime ; rafraîchissement hebdomadaire | C-06, C-24, PD-05, PD-10, PD-22, PS-14, PQ-10 |
| 2026-09-18 | `dt-ducklake-manager 0.3.1` : ajout de colonnes natif (`allow_new_columns`, `add_columns`) | C-13, PD-11, PS-16 |
| 2026-09-18 | Métriques d'export `CDI2` / `CDI3` retenues ; `FLOWS: [import, export]` par défaut | C-12, PD-09, PS-15, PQ-06 |
| 2026-09-18 | **Millésimes de nomenclature** : une seule table par famille, clé `classification`, drapeau `in_force` ; métriques partenaires calculées aussi par millésime HS | C-11, C-25, PD-20, PS-28 |
| 2026-09-18 | Téléchargement : **ordre année-majeur**, plus de liste de produits prioritaires, même comportement en `demo` et en production | PD-06, PD-07, PS-12 |
| 2026-09-18 | Paquet Kedro renommé **`kedro_pipeline/`** (ex-`kedro_pipeline/`) ; **suppression des scripts** en fin de migration | PD-01, PD-02, PS-01 |
| 2026-09-18 | **Couche de restitution** : schéma PostgreSQL `serving` alimenté par le pipeline, tableaux de bord **Superset** (vulnérabilités + supervision par étape) — *remplacé par la révision 2 ci-dessous* | C-22, C-23, PD-13, PD-21, PS-29 à PS-31, §5.8 |
| 2026-09-18 | **Deux cadences** (quotidienne / hebdomadaire) sur un même `WorkflowTemplate` ; cohérence sans approximation | PD-12, PD-14, PD-23, PS-21 |
| 2026-09-18 | Ressources Onyxia connues (100 pods, 0,1–30 CPU, 1–200 Gi) | PS-20, PQ-01 |
| 2026-09-18 | Nouvelle section §12 : opérations à exécuter depuis Onyxia | §12 |
| 2026-09-18 (rév. 2) | **Restitution sans PostgreSQL** : Superset lit **directement DuckLake** par `duckdb-engine` (déjà installé dans le chart Superset d'Onyxia, accès S3 déjà assuré) ; les tables de service sont matérialisées dans un **catalogue DuckLake `serving`** (transaction unique, partitionnement par année) ; suppression de la base `trade_serving`, du secret `trade-serving-credentials` et du basculement `psycopg2` | C-23, §2, PD-05, PD-16, PD-18, PD-21, PS-05, PS-07, PS-24, PS-29, PS-30, §6 à §12 |
| 2026-09-18 (rév. 2) | **Supervision exclusivement dans MLflow** : plus de tableau de bord Superset de supervision ni de table `pipeline_metrics` ; chaque run porte un **rapport de contrôle** (description Markdown dans *Overview*, contrôles déclaratifs, tag `health`), des métriques système et un **rapport HTML** dans *Artifacts* (retour des figures BACI par étape) | PD-13, PS-19, PS-31, §5.1, §5.8, PR-19, PR-20, PQ-19 |
| 2026-09-18 (phase 0) | **Jalon démonstration, scripts** : fabrique de connecteur `kedro_pipeline/io/ducklake.py` (sans Kedro) ; `config/runtime.yaml` ; `DATAFLOW` et `max_queries` en configuration ; ordre Comtrade année-majeur (`reporters=None` si non filtrés), Eurostat produit-majeur avec `start_period` ; porte de complétude BACI, lecture SQL bornée, colonne `is_provisional` ; profil `config/profiles/demo/` (copies complètes, sorties préfixées `demo_` / `trade/demo/`) ; suppression de `process_baci.py` et `test_baci.py`. PQ-15 résolue, PQ-17 à vérifier au premier téléchargement. Constats : `get_valid_periods` exclut la borne de fin qu'on lui passe (appel sans fin, borne appliquée par le script) ; une table `baci_hs20xx` déjà créée sans `is_provisional` refusera l'upsert tant que `statflows.write_dataframe` ne transmet pas `allow_new_columns` (C-13) → supprimer les schémas BACI de test avant le premier run de production ; le `Dockerfile` (C-14) doit copier `kedro_pipeline/` et `config/` | C-01 à C-05, C-17, C-18, C-26, PD-06, PD-07, PD-08, PS-04, PS-06, PS-12, PS-14.1, PQ-15, PQ-17 |
| 2026-09-21 (phase 0, K-03b) | **Couche de service et référentiels** : `kedro_pipeline/config.py` (millésimes, macros SQL), `kedro_pipeline/steps/reference.py` (référentiels, un schéma par table `reference_<table>`), `kedro_pipeline/io/serving.py` (`ServingCatalog`, transaction unique vérifiée sur DuckDB 1.5.3), `kedro_pipeline/steps/serving.py` + `config/serving.yaml` (8 tables), `serving-script` ; `product` des tables Comext/partenaires stocké en `BIGINT` → macro `product_code` | C-22, C-23, PS-27, PS-28.4, PS-29.1, PS-29.2 |
| 2026-09-21 (phase 0) | **Données fictives de démonstration** (Comtrade indisponible, Comext trop lent) : monde simulé isolé dans les catalogues `demo_*` (garde d'écriture testée), entrées de registre marquées `synthetic`, code non migré dans Kedro ; **retrait en deux prompts** : K-17b 🔌 (données sur le cluster) puis K-18 (code et fichiers) | PD-24, PS-04.4, PR-22, PQ-20, §12, K-17b, K-18 |
| 2026-09-21 (phase 0, K-03d) | **Rapport de run MLflow des scripts** : `macroforecast/tracking/{report,figures}.py` (contrôles, description, HTML Plotly), `kedro_pipeline/io/tracking.py` (`publish_run_report`), `scripts/_run_report.py`, `config/tracking.yaml` ; métriques des scripts en `/` (rekeyage `rekey_metrics`), une expérience par bloc ; contrôles alignés sur les métriques réellement émises ; constats de rendu vérifiés sur MLflow 3.15 local | C-19, PD-13, PS-31, PR-19, PQ-19 |

## Sommaire

0. [Conventions de lecture](#0-conventions-de-lecture)
1. [État des lieux : constats sur le code actuel](#1-état-des-lieux--constats-sur-le-code-actuel)
2. [Vue d'ensemble de la cible](#2-vue-densemble-de-la-cible)
3. [Décisions d'architecture](#3-décisions-darchitecture)
4. [Spécifications détaillées](#4-spécifications-détaillées)
5. [Runbooks d'exploitation](#5-runbooks-dexploitation)
6. [Stratégie de tests](#6-stratégie-de-tests)
7. [Risques](#7-risques)
8. [Questions ouvertes et hypothèses retenues](#8-questions-ouvertes-et-hypothèses-retenues)
9. [Actions manuelles à la charge de l'utilisateur](#9-actions-manuelles-à-la-charge-de-lutilisateur)
10. [Phasage et planning jusqu'à la présentation](#10-phasage-et-planning-jusquà-la-présentation)
11. [Références](#11-références)
12. [Opérations à exécuter depuis Onyxia](#12-opérations-à-exécuter-depuis-onyxia)

---

## 0. Conventions de lecture

| Préfixe | Signification |
|---|---|
| `C-xx` | Constat sur le code existant (§1) |
| `PD-xx` | Décision d'architecture (§3) : décision, justification, alternatives écartées, conséquences |
| `PS-xx` | Spécification détaillée (§4) : contrat d'implémentation, avec exemples |
| `PR-xx` | Risque (§7) |
| `PQ-xx` | Question ouverte (§8), toujours accompagnée de l'**hypothèse retenue par défaut** |
| `K-xx` | Prompt d'implémentation (`PIPELINE_PROMPTS.md`) |

Les préfixes `D-xx`, `S-x`, `M-xx` sans `P` renvoient à `AGREGATION_ARCHITECTURE.md`
(synthèse multicritère) et ne sont pas redéfinis ici.

Vocabulaire :

- **étape** : l'une des neuf unités fonctionnelles du pipeline (téléchargement Eurostat,
  téléchargement Comtrade, BACI, vulnérabilités partenaires, vulnérabilités réseau,
  synthèse, cohérence, publication de service, maintenance) ;
- **nœud** : un nœud Kedro ; **tâche** : une tâche du DAG Argo (= un pod) ;
- **unité de fraîcheur** : la maille à laquelle une étape décide de (re)calculer
  (couple reporter × produit, millésime HS, contexte de synthèse…) ;
- **empreinte méthodologique** (*fingerprint*) : hachage de la version de code déclarée
  et des paramètres méthodologiques d'une métrique, d'une méthode ou d'une étape (PS-10) ;
- **millésime** (*vintage*) : une édition du Système harmonisé (`HS1992` … `HS2022`) ;
  **nomenclature en vigueur** : le millésime dans lequel les codes d'une année donnée
  sont déclarés (PD-20) ;
- **tranche** : sous-ensemble d'une année (ou une année entière) traité en mémoire par
  BACI (PS-14) ; **passe** : une lecture complète des tranches d'un millésime ;
- **couche de service** (*serving*) : tables dénormalisées et libellées, dérivées des
  tables de résultats et matérialisées dans le catalogue DuckLake `serving`, lues par
  Superset (PD-21) ;
- **rapport de run** : ce que chaque run MLflow expose pour juger, sans rien ouvrir
  d'autre, de la bonne exécution d'une tâche — description Markdown, contrôles,
  métriques, métriques système, rapport HTML (PS-31).

Conventions de code inchangées (cf. `CLAUDE.md`) : commentaires en français à formulation
nominale, docstrings Google Style en anglais, type hints systématiques, API sklearn pour
les transformations, **aucune valeur méthodologique en dur** dans `macroforecast/`.

---

## 1. État des lieux : constats sur le code actuel

Les constats ci-dessous motivent les décisions. Chacun indique où il se trouve et sa
gravité pour la mise en production (🔴 bloquant, 🟠 à traiter avant le régime nominal,
🟡 dette).

| ID | Gravité | Constat | Localisation |
|---|---|---|---|
| C-01 | ✅ | Restrictions de test codées en dur : `queries = queries[:10]` (Comtrade) et `queries[:5]` (Eurostat). En l'état, la production ne téléchargerait que 15 requêtes. *(Traité en phase 0, 2026-09-18 : plafonds remplacés par `parameters.<dataflow>.max_queries` (null partout, y compris `demo`) et `cap_queries`.)* | `scripts/download_comtrade.py:226`, `scripts/download_eurostat_comext.py:216` |
| C-02 | ✅ | Profondeur historique insuffisante : `period_start: "2010"` pour Comtrade ; les millésimes BACI configurés commencent en 2012 (`HS2012`). Les objectifs de couverture — **1988** pour Comext (et les indicateurs partenaires), **1994** pour Comtrade (et BACI, conformément à la couverture déclarative jugée satisfaisante par le CEPII à partir de 1994) — ne sont couverts ni au téléchargement ni au redressement. *(Révision : 1992 → 1988/1994.)* *(Traité en phase 0, 2026-09-18 : `config/runtime.yaml` (`ANALYSIS_START_YEAR` 1988/1994, `NOMENCLATURES.HS`) ; Comtrade dès 1994 (`split_filters.C_A_HS.periods.start: null` → runtime), Eurostat `start_period` 1988 ; cibles BACI HS1992 (START_YEAR null → 1994), HS1996, HS2002, HS2007 ajoutées.)* | `config/datasets/comtrade.yaml:41`, `config/baci.yaml:108-111` |
| C-03 | ✅ | `os.environ["AWS_SESSION_TOKEN"]` est lu sans défaut dans les neuf constructions de connecteur. Avec les **clés Minio permanentes** du secret `trade-s3-credentials`, il n'y a pas de jeton de session : `KeyError` au démarrage de chaque pod. *(Traité en phase 0, 2026-09-18 : `AWS_SESSION_TOKEN` optionnel dans `kedro_pipeline.io.ducklake.s3_credentials_from_env` (None si absent ou vide).)* | tous les `scripts/*.py` |
| C-04 | ✅ | La construction `DuckLakeConnector.from_postgres(...)` est dupliquée neuf fois, avec des divergences : `admin_user="postgres"` codé en dur (absent de `process_baci*.py`), `admin_dbname=os.environ["PGDATABASE"]`. *(Traité en phase 0, 2026-09-18 : fabrique unique `kedro_pipeline.io.ducklake.build_connector` ; `admin_user` = `PGADMINUSER` (défaut `postgres`) partout, `process_baci_hs.py` compris.)* | `scripts/*.py` |
| C-05 | ✅ | Identifiants de dataflow en dur (`DATAFLOW = "C_A_HS"`, `"DS-045409"`) dans les `main()`, contraire à la règle « aucune valeur en dur qui présage de l'exécution ». *(Traité en phase 0, 2026-09-18 : clé `DATAFLOW` dans `config/datasets/{comtrade,eurostat}.yaml`, lue par tous les scripts.)* | `scripts/*.py` |
| C-06 | 🔴 | `process_baci*.py` lit **toute** la table de faits Comtrade dans un DataFrame pandas (`SELECT cols FROM comtrade.fact_table`), puis `run_baci` enchaîne les six étapes sur ce bloc unique. À partir de 1994 et en HS6 bilatéral (≈ 10 à 13 millions de flux miroirs par année récente), un millésime long comme `HS1992` (1994 → aujourd'hui) représente plusieurs centaines de millions de lignes : dépassement mémoire certain. Or **quatre estimations sont mises en commun sur toutes les années du millésime** (taux de conversion en tonnes, médiane mondiale des valeurs unitaires, équation de gravité avec indicatrices d'année, ANOVA de qualité des déclarants), ce qui interdit un simple traitement année par année. La solution retenue est l'expression de ces estimateurs par **statistiques suffisantes accumulées tranche par tranche** (PD-22, PS-14) : exacte et à mémoire bornée. | `scripts/process_baci_hs.py:484`, `scripts/process_baci.py:288`, `macroforecast/trade/processing/baci.py` |
| C-07 | 🟠 | Les registres de fraîcheur sont des fichiers JSON uniques, **relus puis réécrits en entier** à chaque mise à jour, sans verrou. Deux pods parallèles qui écrivent le même registre (ex. deux millésimes BACI) perdent des entrées (*last writer wins*). | `process_baci_hs.py:608-622`, `compute_*` |
| C-08 | 🔴 | Dans `statflows.core.download.SDMXDownloader._process_query`, le registre des téléchargements est **réécrit intégralement sur S3 après chaque requête**, et chaque requête non vide déclenche un upsert DuckLake avec `compact_after_update=True`. Pour 10⁴ à 10⁵ requêtes, le coût devient quadratique (PUT de fichiers de plusieurs Mo, compaction répétée, un snapshot par requête). | `statflows/core/download.py:593-602`, `statflows/storage/ducklake/tables.py:171-175` |
| C-09 | 🟠 | Le client Eurostat décide de l'incrémental à partir de la date de mise à jour **du dataflow entier** (`get_data_last_update`) : après chaque publication Comext, toutes les requêtes déjà téléchargées redeviennent éligibles (10 dernières observations chacune). Le client Comtrade décide, lui, par période (`lastReleased`). | `statflows/sources/eurostat/client.py:733-786`, `statflows/sources/comtrade/client.py:871-925` |
| C-10 | 🟠 | Les règles de fraîcheur ignorent les **changements de méthodologie** : corriger une formule ou ajouter une métrique ne déclenche aucun recalcul. Seuls `SYNTHESIS.FORCE` et `COHERENCE.FORCE` existent ; les étapes partenaires, réseau et BACI n'ont pas de forçage. | `scripts/compute_*.py` |
| C-11 | 🟠 | La synthèse filtre en dur le flux import (`p."flow" = 1`) et les 5 dernières périodes. La jointure réseau ne retient que `HS2022`, y compris pour les années antérieures à 2022, dont les codes Comext sont pourtant déclarés dans un millésime plus ancien : la jointure `substr(product,1,6)` est alors **fausse pour tout code redéfini entre les millésimes** (PD-20). | `config/synthesis.yaml:55-64` |
| C-12 | 🟡 | Côté partenaires, `HHI` est calculé pour les deux flux ; `CDI2` et `CDI3` ne sont définis que pour l'import (valeur nulle ailleurs). Aucune métrique d'export dédiée n'existe. Les métriques de réseau portent sur le graphe mondial d'un produit : elles ne dépendent pas du sens du flux. *(Révision : définitions d'export retenues, PD-09.)* | `macroforecast/trade/vulnerabilities/metrics.py` |
| C-13 | ✅ | ~~L'upsert de `dt_ducklake_manager` ne sait pas ajouter une colonne~~ **Résolu par `dt-ducklake-manager 0.3.1`** : `DatabaseUpdater.update_database(..., allow_new_columns=True)` ajoute les colonnes absentes (`ALTER TABLE … ADD COLUMN … DEFAULT NULL` + ligne de métadonnées) avant l'upsert, et `add_columns(df)` diffuse une nouvelle colonne sur les lignes existantes par clé primaire en une seule mise à jour. Reste à faire : `statflows.write_dataframe` ne transmet ni `allow_new_columns` ni `compact_after_update` (PS-27, PD-11). | `dt_ducklake_manager/operations/updater.py:176-372`, `statflows/storage/ducklake/tables.py:165-175` |
| C-14 | 🔴 | Le `Dockerfile` part de `python:3.12-slim` alors que `requires-python = ">=3.13"`, copie un dossier `parameters/` inexistant, n'installe aucun extra (`tracking`, `optimal-transport`) et utilise `uv:latest` (non reproductible). | `docker/Dockerfile` |
| C-15 | 🔴 | `kubernetes/workflow.yaml` déclare `kind: Workflow` avec des champs de `CronWorkflow` (`schedule`, `concurrencyPolicy`…), invalides pour ce type ; il référence un script `trade-script` inexistant et un secret `comtrade-credentials` qui ne correspond pas aux secrets créés (`comtrade-api-credentials`, `trade-s3-credentials`). À traiter comme un exemple, pas comme une base. | `kubernetes/workflow.yaml` |
| C-16 | 🟡 | `config/base/catalog.yaml` et `config/base/parameters.yaml` existent mais sont vides : amorce d'une arborescence Kedro. | `config/base/` |
| C-17 | ✅ | Incohérence des chemins CEPII : `process_baci.py` préfixe `s3://{bucket}/` **et** passe `bucket=`, alors que `process_baci_hs.py` passe le chemin relatif et `bucket=`. *(Traité en phase 0, 2026-09-18 : `process_baci.py` supprimé (doublon mono-millésime de `process_baci_hs.py`), ainsi que les clés mortes `baci.PATHS.RESULT_*`.)* | `process_baci.py:257-264` vs `process_baci_hs.py:453-454` |
| C-18 | ✅ | `scripts/test_baci.py` (écriture `df_reconciled.xlsx` en local) est un script de mise au point, hors pipeline. *(Traité en phase 0, 2026-09-18 : `scripts/test_baci.py` et l'entry point `test-baci-script` supprimés.)* | `scripts/test_baci.py` |
| C-19 | ✅ | Le suivi MLflow crée une expérience par script, avec des noms de métriques séparés par des points (`gravity.r2`) : l'interface MLflow ne regroupe pas automatiquement les graphiques par étape. *(Traité en phase 0, 2026-09-21 : les scripts journalisent leurs métriques avec `/` (`rekey_metrics`, `flatten_metrics(sep="/")`, préfixes des rapports conservés : `baci/gravity/r_squared`, `download/errors`, `synthesis/by_product/n_groups`) et une expérience par bloc (`trade-01-downloads`…). MLflow 3.15 regroupe les graphiques par préfixe complet (sections `baci`, `baci/gravity`, `baci/tonnage`…) : constat PQ-19.)* | `macroforecast/tracking/` |
| C-20 | 🔴 | **Accès au cluster impossible depuis le poste local** : le jeton de rafraîchissement OIDC de `sspcloud_access_script.txt` est lié à une preuve DPoP (`oauth2: "invalid_grant" "DPoP proof is missing"`), que le fournisseur `oidc` de `kubectl` ne sait pas produire. Voir PQ-09. | `sspcloud_access_script.txt` |
| C-21 | 🟡 | Aucune documentation mkdocs, aucun workflow GitHub Actions, aucun `.dockerignore` dans le dépôt. `sspcloud_access_script.txt` et `Trade deployment.md` sont bien ignorés par git, mais **seraient copiés dans une image** construite avec `COPY . .`. | racine |
| C-22 | 🟠 | **Aucune table de référence** (libellés de produits par millésime, libellés de pays, tables de passage HS exposées) n'est produite : un tableau de bord ne peut afficher que des codes. Les codelists sont pourtant téléchargées à chaque exécution (`fetch_dimension_codelists`) et les concordances UNSD sont en cache Parquet. *(Traité en phase 0, K-03b, 2026-09-21 : `publish_reference` / `publish_hs_reference` appelées par `download_*.py` et `process_baci_hs.py`, PS-28.4.)* | `scripts/download_*.py`, `scripts/process_baci_hs.py:_ensure_concordances` |
| C-23 | 🔴 | **Aucune couche de restitution** : les résultats ne sont lisibles que par une session DuckDB attachée au catalogue DuckLake (extensions, identifiants S3 et PostgreSQL), sous forme de tables normalisées sans libellés. *(Révision 2 : l'accès technique est réglé — le chart Superset d'Onyxia embarque `duckdb-engine` et l'accès S3 est assuré, PQ-13 ; reste à produire des tables prêtes à l'affichage, PD-21.)* *(Traité en phase 0, K-03b, 2026-09-21 : catalogue `serving`, `serving-script`, PS-29 ; première publication à faire sur Onyxia.)* | — |
| C-24 | 🔴 | La fraîcheur BACI envisagée initialement (unité = millésime × **année**) est **méthodologiquement fausse** : les paramètres estimés sur l'ensemble des années du millésime (C-06) changent dès qu'une année est ajoutée ou révisée, donc toutes les années du millésime doivent être réécrites. C'est aussi la pratique du CEPII, qui republie chaque année la totalité de chaque millésime. | ce document, PS-14 v0 |
| C-25 | 🟠 | Les tables de résultats partenaires (`indicators`, `synthesis`, `synthesis_diagnostics`) n'ont **pas de dimension de nomenclature** : un code produit y désigne des définitions différentes selon l'année (SH6 révisé tous les ~5 ans, NC8 chaque année), ce qui rend toute lecture temporelle d'un produit ambiguë. | `config/vulnerabilities.yaml`, `config/synthesis.yaml` |
| C-26 | ✅ | L'ordre de construction des requêtes Comtrade est **produit-majeur** (`for lot in produits: for période …`, `build_split_queries`), et la période n'est pas une dimension de découpage (`period_start` fixé dans `fixed_dims`) : une requête rapporte toutes les années d'un lot. BACI ayant besoin d'**années complètes** (PD-06), c'est l'ordre inverse qui est utile. *(Traité en phase 0, 2026-09-18 : liste année-majeure (`build_split_queries` + `periods_order`), période = dimension de découpage (`periods=<année>`), `reporters=None` quand le filtre est vide ; bug de la branche sans découpage produit corrigé.)* | `scripts/download_comtrade.py:163-190` |

Points solides sur lesquels on s'appuie :

- séparation nette méthodologie pure (`macroforecast/trade/*`, sans I/O) / scripts (I/O) ;
- runners isolant l'échec d'une unité (millésime, contexte) sans interrompre les autres ;
- registres écrits **après** succès de l'écriture, date capturée **avant** le calcul ;
- rapports structurés (`to_metrics()`) et protocole `RunTracker` pensé pour kedro-mlflow ;
- `download_updates` qui traite d'abord les requêtes jamais téléchargées, puis les plus
  anciennes, et s'arrête proprement à l'expiration de `max_runtime` ;
- tests de caractérisation (`tests/`), qui servent de contrat de non-régression.

---

## 2. Vue d'ensemble de la cible

### 2.1 Couches

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ Restitution : Superset (Onyxia, duckdb-engine) ← catalogue DuckLake `serving` │
│               tableau de bord « Vulnérabilités »                              │
│ Supervision : MLflow (Onyxia) — rapport de contrôle par run (PS-31)           │
├──────────────────────────────────────────────────────────────────────────────┤
│ Infrastructure : GHCR (image) · Argo Workflows (WorkflowTemplate + 2 CronWorkflow)│
│                  MLflow (Onyxia) · PostgreSQL (catalogues DuckLake) · Minio S3 │
├──────────────────────────────────────────────────────────────────────────────┤
│ kedro_pipeline/  (NOUVEAU — projet Kedro : orchestration + I/O)               │
│   settings.py · pipeline_registry.py · pipelines/* (nœuds fins)               │
│   steps/*   (logique d'étape ; seule implémentation)                          │
│   io/*      (fabrique de connecteurs, datasets Kedro, registres, serving)     │
│   deploy/*  (rendu des manifestes Argo à partir du DAG argo-kedro)            │
├──────────────────────────────────────────────────────────────────────────────┤
│ scripts/   (TRANSITOIRES — enveloppes CLI de la phase 0, supprimées en K-18)   │
├──────────────────────────────────────────────────────────────────────────────┤
│ macroforecast/  (INCHANGÉ dans son rôle — méthodologie pure, sans I/O ;       │
│                  BACI réécrit en estimateurs « par passes », PS-14)           │
├──────────────────────────────────────────────────────────────────────────────┤
│ statflows (dépendance externe — acquisition, registres de téléchargement,     │
│            écriture DuckLake)  · dt-ducklake-manager (connecteur, maintenance) │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 DAG cible (pipeline `__default__`)

```mermaid
flowchart LR
  subgraph T1["Téléchargement (MLflow : trade-01-downloads) — quotidien"]
    DE["download_eurostat<br/>(+ audit de couverture + référentiels)"]
    DC["download_comtrade<br/>(+ audit de couverture + référentiels)"]
  end
  subgraph T2["BACI (MLflow : trade-02-baci) — hebdomadaire"]
    BS["prepare_baci<br/>porte de complétude + concordances HS"]
    B92["process_baci_hs1992"]
    B96["process_baci_hs1996"]
    Bxx["… un nœud par millésime"]
    B22["process_baci_hs2022"]
  end
  subgraph T3["Vulnérabilités et synthèse (MLflow : trade-03-vulnerabilities)"]
    P["compute_partner_vulnerabilities<br/>(en vigueur + par millésime HS) — quotidien"]
    N["compute_network_vulnerabilities — hebdo"]
    S["compute_synthetic_scores — hebdo"]
    C["compute_synthesis_coherence — hebdo"]
  end
  PUB["publish_serving<br/>(catalogue DuckLake serving → Superset) — les deux"]
  M["maintain_ducklake<br/>(onExit Argo — MLflow : trade-00-maintenance)"]

  DC --> BS --> B92 & B96 & Bxx & B22 --> N
  BS -. concordances .-> P
  DE --> P
  P --> S
  N --> S
  S --> C
  P --> PUB
  C --> PUB
  PUB -.-> M
```

Chaque flèche est une dépendance de **données** portée par un dataset Kedro (PS-07). En
local, `kedro run` exécute tout le DAG dans un seul processus. Sur Argo, chaque nœud (ou
`FusedPipeline`) devient une tâche, donc un pod (PD-05). Le DAG est unique ; deux
**points d'entrée** Argo en exécutent des sous-ensembles à des cadences différentes
(PD-23) :

| Point d'entrée | Nœuds | Cadence | Justification |
|---|---|---|---|
| `daily` | téléchargements, partenaires, `publish_serving` | quotidienne (01:00) | acquisition incrémentale, métriques partenaires bon marché, tableau de bord rafraîchi |
| `weekly` | `prepare_baci`, `process_baci_*`, réseau, synthèse, cohérence, `publish_serving` | hebdomadaire (`WEEKLY_DAY`), fenêtre de 6 jours | BACI réestime tout un millésime (PD-22) ; cohérence **sans approximation** (PD-12) : la justesse prime sur la fraîcheur |

### 2.3 Flux de contrôle

1. 01:00 Europe/Paris : le `CronWorkflow` `trade-pipeline-daily` instancie le
   `WorkflowTemplate` `trade-pipeline` (point d'entrée `daily`). Le samedi à 02:00, le
   `CronWorkflow` `trade-pipeline-weekly` l'instancie avec le point d'entrée `weekly`
   (PS-21.2). Les deux peuvent se recouvrir : ils n'écrivent jamais la même table, et
   `publish_serving` comme la maintenance sont protégés par un **mutex Argo**.
2. Les deux téléchargements tournent en parallèle, chacun dans un **budget de temps**
   (`MAX_RUNTIME`, 10 h par défaut) : ils s'arrêtent proprement une fois le budget épuisé.
3. Les étapes aval s'exécutent **même si un téléchargement a échoué** : elles ne lisent
   que des données commitées et décident seules, par leurs registres de fraîcheur, de ce
   qui est à recalculer (PD-06, PD-10).
4. En fin de workflow, qu'il ait réussi ou non, le gestionnaire `onExit` lance la
   maintenance DuckLake (PD-16).
5. Tous les pods journalisent dans MLflow sous un même `workflow_id` (PD-13) ; chaque
   run se termine par son **rapport de contrôle** (PS-31), qui suffit à juger de la
   bonne exécution de la tâche. La maintenance (`onExit`) clôt en `FAILED` les runs du
   workflow restés `RUNNING` (pod tué sans pouvoir fermer son run, PR-20).

---

## 3. Décisions d'architecture

### PD-01 — Projet Kedro `kedro_pipeline` à la racine, configuration dans `config/`

**Décision.**
- Nouveau package Python **`kedro_pipeline/`** à la racine du dépôt, à côté de
  `macroforecast/` et (transitoirement) `scripts/`. Déclaration dans `pyproject.toml` :
  `[tool.kedro] package_name = "kedro_pipeline"`, `project_name = "trade-analysis"`,
  `kedro_init_version = "1.6.0"`, `source_dir = "."`.
- *Nom.* `kedro_pipeline` (et non `kedro_pipeline`, ni `pipeline`) : le nom dit ce que
  contient le dossier — le projet Kedro et rien d'autre — et le distingue sans ambiguïté
  de `macroforecast/` (la méthodologie) et de `kedro.pipeline` (module de la
  bibliothèque, sans conflit d'import puisque le paquet racine diffère). Un simple
  `pipeline/` serait trop générique ; `kedro_pipeline` laissait croire que la
  méthodologie « trade » y vivait. Les noms des ressources Kubernetes (`trade-pipeline`,
  `trade-pipeline-daily`) désignent, eux, l'application, et sont conservés.
- Source de configuration Kedro : **`config/`** (`CONF_SOURCE = "config"` dans
  `settings.py`), avec les environnements :
  - `base` : paramètres de production, versionnés ;
  - `local` : surcharges de poste (non versionné, sauf `.gitkeep`) ;
  - `cloud` : surcharges d'exécution dans Argo (chemins S3, `n_jobs`, budgets) ;
  - `demo` : périmètre réduit pour la présentation (PD-19, PS-04.4).
- `macroforecast/` reste de la méthodologie pure : **aucune dépendance à Kedro** n'y est
  introduite.

**Justification.** Kedro impose un package projet qui contient `settings.py` et
`pipeline_registry.py`. Le placer dans `macroforecast/` mêlerait orchestration et
méthodologie, alors que `CLAUDE.md` exige de les séparer. `source_dir = "."` évite de
déplacer `macroforecast/` et `scripts/` sous `src/`, ce qui casserait les entrées
`[project.scripts]` et les imports des tests. Enfin, le dossier `config/` existe déjà et
contient l'amorce `config/base/` (C-16).

**Alternatives écartées.** Un layout `src/` standard (déplacements massifs, imports
cassés) ; un dépôt Kedro séparé (deux images, deux versions de configuration).

**Conséquences.** `hatchling` doit empaqueter `macroforecast`, `scripts` et
`kedro_pipeline` (PS-02). Les scripts voient leurs chemins de configuration par défaut
changer (PD-03).

### PD-02 — Implémentation unique dans `kedro_pipeline/steps/` ; les scripts sont transitoires et supprimés en fin de migration

**Décision.** La logique d'orchestration de chaque script (lecture des registres,
sélection des unités, boucle d'isolation des échecs, écriture, mise à jour du registre)
est déplacée dans des **fonctions d'étape** de `kedro_pipeline/steps/<étape>.py`. Ces
fonctions reçoivent des **objets résolus** (paramètres typés, poignées de tables,
registres, tracker) et ne lisent **ni fichier YAML ni variable d'environnement**.

Cycle de vie des scripts :

| Phase | Rôle de `scripts/` |
|---|---|
| 0 (démonstration) | Seul moteur d'exécution : le workflow de transition (K-03) les appelle. Les nouveaux modules (`kedro_pipeline/io`, `kedro_pipeline/steps/serving.py`) sont écrits **sans dépendance à Kedro** pour être appelés par les scripts. |
| 3 (Kedro) | Enveloppes CLI minces (chargement des paramètres, poignées, appel de l'étape) qui **ré-exportent** les helpers importés par `tests/test_scripts_*.py`, afin que la suite de caractérisation reste verte pendant le refactoring (K-11). |
| 4 (production, K-18) | **Supprimés**, ainsi que les entrées `[project.scripts]`. Les tests de caractérisation sont déplacés vers `tests/pipeline/` et importent directement les fonctions d'étape (mêmes assertions). |

**Justification.** Une fois les nœuds Kedro en place, un script n'est plus qu'un second
chemin d'appel de la même fonction d'étape : deux chargeurs de configuration, deux jeux
de variables d'environnement, deux points d'entrée à tester, pour un besoin — déboguer
une étape seule — que Kedro couvre déjà (`kedro run --pipeline baci --nodes
process_baci_hs2017 --env local`, ou `--from-nodes`/`--to-nodes`). Conserver les
scripts, c'est garantir qu'ils finissent **redondants puis inopérants** (paramètre ajouté
au nœud et oublié dans le script). Les seuls utilitaires hors pipeline (mesures
ponctuelles, migrations de registres) vivent dans `tools/` et ne sont pas des points
d'entrée du paquet.

**Conséquences.** Le refactoring (K-11) doit laisser `uv run pytest` vert à l'identique ;
K-18 supprime `scripts/` et migre les tests. Toute exécution ponctuelle en production
passe par `argo submit` (PS-11) ou `kedro run` depuis un service Onyxia (§12).

### PD-03 — Configuration : fichiers `parameters_*.yml` à clé racine, secrets par `credentials.yml`

**Décision.**
1. Les fichiers méthodologiques migrent vers `config/base/`, **un fichier par domaine,
   chacun encapsulé sous une clé racine unique** (le chargeur Kedro fusionne tous les
   `parameters*` dans un seul dictionnaire et refuse les clés dupliquées ; or `MLFLOW`,
   `DOWNLOADS`, `fixed_dims`… existent déjà dans plusieurs fichiers) :

   | Ancien fichier | Nouveau fichier | Clé racine |
   |---|---|---|
   | `config/datasets/comtrade.yaml` | `config/base/parameters_comtrade.yml` | `comtrade` |
   | `config/datasets/eurostat.yaml` | `config/base/parameters_eurostat.yml` | `eurostat` |
   | `config/datasets/oecd.yaml` | `config/base/parameters_oecd.yml` | `oecd` |
   | `config/baci.yaml` | `config/base/parameters_baci.yml` | `baci` |
   | `config/vulnerabilities.yaml` | `config/base/parameters_vulnerabilities.yml` | `vulnerabilities` |
   | `config/synthesis.yaml` | `config/base/parameters_synthesis.yml` | `synthesis` |
   | *(nouveau)* | `config/base/parameters_runtime.yml` | `runtime` (dont les millésimes de nomenclature, PS-04.1) |
   | *(nouveau)* | `config/base/parameters_maintenance.yml` | `maintenance` |
   | *(nouveau)* | `config/base/parameters_serving.yml` | `serving` (PS-29) |
   | *(nouveau)* | `config/base/parameters_tracking.yml` | `tracking` (contrôles et rapport de run, PS-31) |

   Les ancres YAML restent valides car chaque fichier est analysé isolément.
2. Les identifiants de dataflow sortent du code (C-05) : clé `DATAFLOW` dans chaque bloc
   (`comtrade.DATAFLOW: "C_A_HS"`, `eurostat.DATAFLOW: "DS-045409"`).
3. Les blocs `MLFLOW` par script disparaissent au profit de `config/base/mlflow.yml`
   (kedro-mlflow, PD-13). Les options de journalisation (`LOG_ARTIFACTS`, `DRIFT`)
   restent dans les blocs d'étape sous `TRACKING`.
4. Les secrets et variables d'environnement passent exclusivement par
   `config/base/credentials.yml`, avec le résolveur `oc.env` (PS-05). Aucun
   `os.environ[...]` ne subsiste dans `steps/` ni dans les nœuds.
5. Tant qu'ils existent (PD-02), les scripts lisent les nouveaux fichiers via
   `kedro_pipeline.config.load_parameters(env="base")`, qui reproduit la fusion Kedro
   (`OmegaConfigLoader`). Leurs variables d'environnement historiques
   (`BACI_CONFIG_PATH`…) sont supprimées, **`KEDRO_ENV`** les remplace.

**Justification.** On conserve la paramétrisation externe voulue, tout en la rendant
native pour Kedro (`params:baci.CLASSIFICATIONS`) et visible dans kedro-viz.

### PD-04 — Datasets Kedro : poignées paresseuses de tables DuckLake et registres JSON

**Décision.** Le catalogue ne matérialise **jamais** une table DuckLake complète en
mémoire. Deux types de datasets personnalisés vivent dans `kedro_pipeline/io/datasets.py` :

- **`DuckLakeTableDataset`** : `load()` renvoie une **poignée** `DuckLakeTable` (catalogue,
  schéma, table, fabrique de connecteur ; aucune connexion ouverte), qui expose
  `connect()`, `query(sql) -> pd.DataFrame`, `upsert(df, primary_keys)`, `exists()`,
  `add_missing_columns(df)` (PD-11). `save(obj)` accepte une poignée (cas nominal : le
  nœud a écrit via la poignée et la renvoie pour matérialiser la lignée) et vérifie
  qu'elle désigne bien la même table ; il **refuse un DataFrame**, afin que toute écriture
  passe par l'upsert explicite du nœud.
- **`FreshnessRegistryDataset`** : `load()` renvoie un `FreshnessRegistry` (PS-10) adossé
  à des fichiers JSON **fragmentés** sur S3 ; `save(registry)` persiste les seuls
  fragments modifiés.

Les nœuds reçoivent donc des poignées en entrée et **renvoient** des poignées en sortie.
Kedro en déduit les dépendances du DAG, kedro-viz affiche la lignée et argo-kedro calcule
les dépendances entre tâches (il rapproche les sorties d'un nœud des entrées d'un autre).

**Justification.**
1. Les étapes font des **upserts incrémentaux** par lots (un contexte, un millésime),
   incompatibles avec la sémantique « un `save` = un DataFrame complet » de Kedro.
2. Les volumes Comtrade/BACI interdisent le chargement complet (C-06).
3. argo-kedro **n'accepte pas de `MemoryDataset` entre tâches** : toute entrée/sortie doit
   être déclarée au catalogue, et une poignée se recharge à l'identique dans un autre pod.

**Alternatives écartées.** `kedro_datasets.ibis.TableDataset` (pas d'upsert DuckLake) ;
un DataFrame en sortie de nœud (explosion mémoire, pas d'incrémental) ; des nœuds sans
sorties (plus de lignée, plus de dépendances Argo).

**Conséquences.** Les datasets ne se connectent **jamais** dans `__init__` : `kedro viz
build` en CI (sans secrets) doit pouvoir instancier le catalogue.

### PD-05 — Parallélisme : où couper en pods, où paralléliser dans le pod

C'est la décision centrale demandée. Règle générale : **une frontière de pod se justifie
si les trois conditions suivantes sont réunies** :

1. **pas d'écrivain partagé** : les deux branches n'écrivent pas la même table DuckLake ni
   le même fragment de registre (DuckLake gère la concurrence de façon optimiste, et deux
   commits sur la même table entrent en conflit) ;
2. **domaine d'échec ou profil de ressources distinct** : API et quota différents, besoin
   mémoire différent, risque d'OOM à isoler ;
3. **durée ≫ coût de démarrage d'un pod** (≈ 30 à 90 s : tirage d'image, import de
   `macroforecast`, connexion au catalogue), soit plus de 10 minutes environ.

Dans le cas contraire, on parallélise **à l'intérieur du pod** (processus `joblib`/`loky`,
un seul écrivain qui collecte les résultats et écrit par lots).

| Étape | Tâches Argo (pods) | Parallélisme intra-pod | Justification |
|---|---|---|---|
| Téléchargement Eurostat | **1 pod** (`FusedPipeline` téléchargement + audit de couverture) | Aucun : séquentiel, sous rate limiter | API à débit limité par IP ; un seul écrivain du registre `statflows` et de la table `DS_045409` ; paralléliser n'accélère pas un débit plafonné. |
| Téléchargement Comtrade | **1 pod** (idem), **en parallèle** d'Eurostat | Aucun | Autre API, autre quota (clé premium) ; les deux sources sont indépendantes. |
| Préparation BACI | **1 pod léger** (porte de complétude + cache des tables HS) | — | Écrivain unique du cache de concordances (partagé avec les partenaires par millésime, PD-20) ; évite une course entre millésimes. |
| Redressement BACI | **1 pod par millésime HS** (fan-out) | Passes séquentielles sur les tranches annuelles (PS-14) ; BLAS multi-thread ; `n_jobs` limité par `NUM_CPU` | Chaque millésime écrit son propre schéma (`baci_hs2017`…), donc sans écrivain partagé ; mémoire bornée à **une tranche** grâce aux statistiques suffisantes (PD-22) ; un OOM ne doit emporter qu'un millésime ; le fan-out divise le temps écoulé. |
| Vulnérabilités réseau | **1 pod** | `joblib` sur les groupes (millésime × année × produit), écrivain unique | Tous les millésimes écrivent la même table `network_indicators`. |
| Vulnérabilités partenaires | **1 pod**, en parallèle du bloc BACI/réseau | Boucle sur les millésimes (en vigueur puis historiques, PD-20) et découpage par reporter ; calcul vectorisé narwhals ; backend `polars` multi-thread en option | Table unique `indicators`, clé étendue par `classification`. |
| Synthèse | **1 pod** | `joblib` sur les contextes (`n_jobs = NUM_CPU`), écriture par lots de K contextes | Table unique `synthesis` ; grand nombre de petites unités CPU (contextes). |
| Cohérence | **1 pod** | Idem | Table unique `synthesis_diagnostics`. |
| Publication de service | **1 pod** (mutex `trade-serving`) | Séquentiel par table de service, dans une transaction | Écrivain unique du catalogue DuckLake `serving` (PD-21). |
| Maintenance | **1 pod** (`onExit`, mutex `trade-maintenance`) | Séquentiel par table | Les opérations de maintenance DuckLake doivent être sérialisées sur un catalogue. |

Conséquences : 2 + 1 + *n*<sub>millésimes</sub> + 4 + 1 + 1 tâches, soit **16 pods**
avec les sept millésimes HS1992, HS1996, HS2002, HS2007, HS2012, HS2017, HS2022 (le point
d'entrée `daily` n'en instancie que 4 : deux téléchargements, partenaires, publication).
Le chemin critique du point d'entrée `weekly` est `prepare_baci → max(process_baci_*) →
network → synthesis → coherence → publish_serving`. Ressources Onyxia constatées par
l'utilisateur : au plus **100 pods** simultanés, de **0,1 à 30 CPU** et de **1 à 200 Gi**
par pod, le délai de démarrage croissant avec la demande (PS-20, PQ-01).

**Alternatives écartées.**
- *Un pod par étape pour BACI (millésimes séquentiels)* : temps écoulé multiplié par six,
  et le premier OOM emporte tout.
- *Réseau fan-out par millésime* : conflits d'écriture sur `network_indicators` (on
  pourrait ajouter des tentatives, mais le calcul réseau est court comparé à BACI).
- *Téléchargement fragmenté en N pods* : même IP et même quota, registre partagé ;
  aucun gain.

### PD-06 — Rattrapage historique : calcul progressif par unité, ordre de téléchargement année-majeur, porte de complétude BACI

**Décision.** On n'attend **pas** la fin du téléchargement pour calculer. On combine :

1. **Calcul progressif** : chaque jour, chaque étape aval calcule ce qui est devenu
   disponible depuis la veille, grâce aux registres de fraîcheur (PD-10). Concrètement :
   - *partenaires (Eurostat)* : un couple reporter × produit est calculable dès que sa
     requête a été téléchargée une fois. Le couple est complet, puisque la requête
     rapporte tous les partenaires, les deux flux et tous les indicateurs. Le calcul
     progresse donc **produit par produit** ;
   - *synthèse / cohérence* : les contextes s'enrichissent au fil des semaines. Une
     cellule absente un jour J apparaît au jour J+k et fait recalculer le contexte
     (PS-17). Les niveaux `by_reporter` et `global` bougent tant que la couverture
     progresse : c'est attendu, et c'est tracé dans MLflow (`coverage/*`).
2. **Ordre de téléchargement année-majeur, sans liste de produits prioritaires.**
   `download_updates` traite en premier les requêtes jamais téléchargées, **dans l'ordre
   de la liste fournie** (tri stable, `SDMXDownloader._prioritize`). La liste est donc
   construite pour que l'on complète **un lot de produits pour une année, puis tous les
   lots de cette année, puis l'année suivante** : boucle externe sur les années (ordre
   `periods_order`, `desc` par défaut : années récentes d'abord), boucle interne sur les
   lots de produits dans l'**ordre naturel des codes** (PS-12). Il n'y a **pas** de liste
   de produits prioritaires : le périmètre produit d'un environnement est fixé par les
   filtres `include_regex`/`include` (`demo` : une centaine de codes), et **le
   comportement de téléchargement est identique en `demo` et en production** — seul le
   périmètre change. Pourquoi l'année plutôt que le produit comme unité de complétion :
   - BACI n'est calculable que sur des **années complètes** (point 3) et réestime **tout
     le millésime** à chaque passe (PD-22) : disposer tôt d'une année entière débloque
     un premier BACI, alors qu'un produit complet sur toutes les années ne débloque rien ;
   - la décision incrémentale du client Comtrade (`lastReleased`) est prise **par
     période** : une requête = une année rend cette décision exacte (PD-07) ;
   - la table BACI est partitionnée par année (PD-16), et une année arrive d'un bloc ;
   - les indicateurs partenaires (Eurostat) sont calculés par couple reporter × produit
     sur **toutes les années** rapportées par la requête : côté Eurostat, la période
     n'est pas une dimension de découpage (PD-07), l'ordre année-majeur ne s'y applique
     donc qu'à travers `PERIOD_WINDOWS` s'il est activé.
3. **Porte de complétude BACI** : BACI estime la gravité et la qualité des déclarants
   sur l'ensemble des flux du millésime : une année à moitié téléchargée biaiserait
   toutes les autres. Le nœud `prepare_baci` ne retient donc que les **années
   complètes**, c'est-à-dire celles dont toutes les requêtes (lots de produits) ont été
   téléchargées au moins une fois, selon le registre `statflows` (PS-14). Le seuil est
   paramétrable (`baci.COMPLETENESS.MIN_SHARE`, 1.0 en production). En environnement
   `demo`, le périmètre produit est restreint (une centaine de codes), donc les années
   sont rapidement complètes **au sens de ce périmètre**, et le résultat est **étiqueté
   provisoire** dans MLflow et dans une colonne `is_provisional` (un BACI sur un
   sous-ensemble de produits n'est pas le BACI complet : la qualité des déclarants est
   estimée sur tous les produits).

**Justification.** À l'échelle complète (1994 → aujourd'hui pour Comtrade, 1988 → pour
Comext, HS6, tous reporters), le rattrapage prendra **plusieurs jours à semaines**
(PR-01). Attendre la fin pour calculer repousserait tout résultat après la présentation.
Le calcul progressif est déjà dans l'ADN des scripts (registres par unité) ; il suffit
de l'étendre. L'ordre année-majeur est celui qui rend le plus vite une année utile à
BACI, et il supprime toute différence de comportement entre la démonstration et la
production.

### PD-07 — Granularité des requêtes de téléchargement

**Décision.**
- **Comtrade** : une requête = **(année × lot de `PRODUCTS_STEP` produits HS6)**, tous
  reporters (`reporters=None`), flux `M` et `X` ; liste **année-majeure** (PD-06,
  PS-12.1).
  - La période devient une dimension de découpage (C-26) : la décision `lastReleased` de
    `fetch_updates` est prise par période, ce qui la rend exacte et permet d'ordonner par
    année.
  - On ne télécharge que les **sous-positions à 6 chiffres** (`include_regex: '^\d{6}$'`) :
    BACI et le réseau travaillent en HS6, et les agrégats HS2/HS4 se déduisent par somme,
    sans raison de les télécharger (3 à 4 fois moins de requêtes).
- **Eurostat** : une requête = **(reporter × lot de `PRODUCTS_STEP` codes produits)**,
  les codes d'un lot appartenant au même chapitre HS2, **toutes les années** en une
  requête. La réponse contient `product` par ligne : le couple reporter × produit reste
  reconstituable. Liste **produit-majeure** (lot de produits, puis reporters).
  - Pourquoi la période n'est pas une dimension de découpage ici : la réponse d'un
    reporter × produit sur 37 ans reste petite (quelques centaines de partenaires × 2
    flux × 2 indicateurs), alors que l'API est bornée par le **nombre de requêtes**
    (limiteur de débit par IP) ; découper par année multiplierait les requêtes par ~37
    sans accélérer quoi que ce soit, et la décision incrémentale Eurostat est prise par
    dataflow, pas par période (C-09).
  - Paramètre optionnel **`PERIOD_WINDOWS`** (`null` par défaut) : liste ordonnée de
    fenêtres `[[2015, null], [1988, 2014]]` transmises par `startPeriod`/`endPeriod`.
    Quand il est renseigné, la fenêtre devient la boucle externe (ordre année-majeur
    par fenêtres), au prix d'autant de requêtes que de fenêtres. À réserver au cas où
    l'on voudrait des années récentes très tôt sur tout le périmètre ; **hors `demo`
    et hors `base` par défaut**. Sa prise en charge par `EurostatQueryRequestV30`
    est à vérifier (PQ-15).
  - `PRODUCTS_STEP` est un paramètre ; sa valeur de production sera fixée par une mesure
    (longueur d'URL et taille de réponse SDMX acceptées, PR-03).
  - Le lecteur du registre de téléchargement côté partenaires éclate les lots en couples
    (PS-12.3).
- Les plafonds de test (C-01) sont remplacés par un paramètre `MAX_QUERIES` (`null` en
  production **et** en `demo` : le périmètre `demo` est borné par ses filtres de codes,
  pas par un plafond de requêtes).
- **Référentiels** : les codelists récupérées pour construire les requêtes (produits par
  millésime, reporters, partenaires, avec leurs libellés) sont **persistées** dans le
  schéma `reference` du catalogue de la source (C-22, PS-28.4) à chaque exécution du
  téléchargement (upsert idempotent).

**Justification.** Le nombre de requêtes gouverne à la fois la durée du rattrapage, le
coût des registres (C-08) et la volumétrie de petits fichiers DuckLake. Réduire la
cardinalité d'un facteur 20 à 50 côté Eurostat rend la réactualisation post-publication
(C-09) tenable en une journée.

### PD-08 — Couverture historique par source (1988 Comext, 1994 Comtrade) et audit de couverture

**Décision.**
- **Deux dates de début**, une par source, après vérification de la disponibilité
  réelle des données par l'utilisateur :
  - `runtime.ANALYSIS_START_YEAR.eurostat: 1988` — Comext DS-045409 commence en 1988
    (entrée en vigueur du Système harmonisé) ; les indicateurs partenaires couvrent
    donc 1988 → aujourd'hui ;
  - `runtime.ANALYSIS_START_YEAR.comtrade: 1994` — couverture déclarative HS jugée
    satisfaisante à partir de 1994 (Gaulier & Zignago 2010, §2.1, figure 1 : le
    nombre de déclarants en HS passe de ~30 en 1989 à ~110 en 1994) ; BACI et le
    réseau couvrent 1994 → aujourd'hui.
  Ces valeurs sont référencées par interpolation OmegaConf dans
  `comtrade.split_filters.C_A_HS.periods.start`, `eurostat.fixed_dims.DS-045409`
  (`startPeriod`, si le client l'accepte, PQ-15) et `baci.PARAMETERS.period_start`.
- **Millésimes de nomenclature** (référentiel partagé `runtime.NOMENCLATURES.HS`,
  PS-04.1) : `HS1992` (en vigueur dès **1988**, dit H0), `HS1996`, `HS2002`, `HS2007`,
  `HS2012`, `HS2017`, `HS2022`, chacun avec son année d'entrée en vigueur. Le
  `START_YEAR` d'une cible BACI vaut `max(entrée en vigueur, ANALYSIS_START_YEAR.comtrade)`,
  soit **1994 pour HS1992** et l'année d'entrée pour les autres. Seul `HS1992` couvre
  toute la période d'analyse ; les millésimes récents offrent une nomenclature plus
  fine sur une période plus courte (PD-20 pour l'usage de ces millésimes dans les
  restitutions). PQ-05 (étiquette `low_coverage` pour 1992-1994) est **sans objet**.
- **Audit de couverture** en fin de nœud de téléchargement (PS-13) : pour chaque source,
  première et dernière période observées par reporter (et par chapitre HS2), nombre de
  requêtes jamais téléchargées, part téléchargée par année. Il est publié dans MLflow
  (`coverage/*` et artefact CSV). Une **alerte** (tag `coverage_alert=true` + log
  WARNING) se déclenche si `min(period) > ANALYSIS_START_YEAR.<source>` pour un
  reporter censé couvrir la période (liste `EXPECTED_FULL_HISTORY_REPORTERS`).

**Justification.** « Télécharger depuis 1988 (resp. 1994) » ne se vérifie que par les
données effectivement écrites. Les pays entrés tardivement dans l'UE ou ayant adopté le
SH tardivement auront légitimement une couverture plus courte : d'où une liste explicite
plutôt qu'une règle universelle.

### PD-09 — Paramétrage import / export

**Décision.**

| Étape | Paramétrable import/export ? | Comportement retenu |
|---|---|---|
| Téléchargement Comtrade | **Non** | Toujours `M` et `X` : la réconciliation miroir BACI a besoin des deux déclarations (import de A = export de B). |
| Téléchargement Eurostat | **Non** | Toujours flux `1` et `2` : `CDI3` (import) utilise les exports ; les métriques d'export ont besoin des deux. |
| BACI | **Non** | Produit une matrice bilatérale unique exportateur → importateur ; les vulnérabilités à l'import d'un pays lisent sa colonne, à l'export sa ligne. |
| Réseau | **Non** | Métriques du graphe mondial d'un produit, indépendantes du sens. Partagées par les deux sens dans la synthèse. |
| Partenaires (Eurostat) | **Oui** : `vulnerabilities.FLOWS: [import, export]` | Chaque métrique déclare `supported_flows` ; seules les combinaisons (métrique × flux) supportées et demandées sont calculées. |
| Synthèse / cohérence | **Oui** : `synthesis.FLOWS: [import, export]` | Le filtre SQL sur le flux est **généré** à partir de `FLOWS` (plus de `p."flow" = 1` en dur). `flow` fait déjà partie de `context_columns` : import et export ne sont jamais comparés entre eux. |

Métriques d'export (**retenues**, PQ-06 tranchée par l'utilisateur le 2026-09-18) :

| Métrique | Import (existant) | Export (retenu) | Lecture à l'export |
|---|---|---|---|
| `HHI` | concentration des fournisseurs | concentration des débouchés (déjà calculée pour le flux 2) | plus les débouchés sont concentrés, plus l'exportateur est exposé à un choc de demande |
| `CDI2` | imports extra-UE / imports totaux | **exports extra-UE / exports totaux** | part des débouchés situés hors du marché intérieur : dépendance aux marchés tiers |
| `CDI3` | imports extra-UE / exports totaux | **exports extra-UE / imports totaux** | exposition extra-UE rapportée à la capacité d'absorption du pays (ses importations) : image miroir exacte de `CDI3` import, obtenue en permutant les rôles de M et X |

Les deux définitions sont les **miroirs formels** des définitions à l'import (échange
`M ↔ X`), ce qui a trois avantages : une seule classe par métrique, paramétrée par le
sens (`flow_role`), donc une seule version d'empreinte ; la **polarité** reste « plus
élevé = plus vulnérable », donc rien ne change dans la synthèse ; les seuils d'alerte
(`0.5` pour `CDI2`, `1.0` — parité — pour `CDI3`) conservent la même lecture. Deux
réserves à documenter dans les docstrings : `CDI3` export n'a pas d'ancrage dans la
littérature (il est proposé par analogie) ; pour un reporter agrégé `EU27_2020` (PD-21),
`CDI2` vaut 1 par construction (ses flux sont extra-UE par définition) et doit être
exclu de la synthèse par le filtre de contexte.

`FLOWS: [import, export]` devient la valeur de `base` (partenaires et synthèse). Les
métriques réseau sont jointes aux deux sens.

**Justification.** Le paramètre n'a de sens que là où la méthodologie distingue le sens
du flux. L'exposer ailleurs créerait des combinaisons fausses (BACI sans exports).

### PD-10 — Registres de fraîcheur v2 : fragmentés, dotés d'empreintes méthodologiques, forçables

**Décision.** Un module générique `kedro_pipeline/io/freshness.py` remplace les six
lecteurs/écrivains de registres dupliqués. Chaque étape aval tient un registre dont les
entrées sont indexées par **unité de fraîcheur** et portent :

- `last_computed` (instant UTC capturé **avant** le calcul) ;
- `upstream_watermark` (instant amont pris en compte) ;
- `fingerprints` : `{nom_métrique_ou_méthode: empreinte}` (PS-10.2) ;
- `reason` : `new_data` | `fingerprint` | `forced` | `first` ;
- des compteurs (`n_rows`, `n_cells`…).

Une unité est recalculée si **au moins une** des conditions suivantes est vraie :
1. elle n'a jamais été calculée ;
2. son amont est plus récent que `upstream_watermark` ;
3. l'empreinte d'une métrique/méthode **demandée** diffère de celle enregistrée : la
   métrique a été ajoutée ou sa formule/version a changé ;
4. elle entre dans le périmètre d'un **forçage** (PS-11).

Les registres sont **fragmentés** (un fichier JSON par fragment : reporter, millésime,
période…) afin de ne jamais réécrire un fichier géant (C-08) et de supprimer toute course
entre pods (C-07). Chaque étape a son préfixe :
`trade/state/<étape>/<fragment>.json`.

**Cas particulier de BACI** (C-24) : l'unité de fraîcheur est le **millésime entier**,
car ses paramètres sont estimés sur toutes ses années (PD-22). L'entrée porte en plus un
`fit_id` (identifiant de la passe d'estimation) et la liste des **années écrites** sous
ce `fit_id`, qui sert de point de reprise : une passe interrompue laisse une table mixte
(années de deux ajustements), détectable par `fit_id` en colonne, et la passe suivante
repart du début du millésime (les années déjà écrites sous le `fit_id` courant sont
réécrites à l'identique, ce qui est idempotent). Une **cadence** (`baci.REFRESH`) évite
de réestimer sept millésimes chaque jour pendant le rattrapage (PS-14.6).

**Justification.** C'est la réponse aux deux besoins exprimés :
- *ajouter une métrique ou une méthode* : nouvelle empreinte, donc recalcul **de cette
  métrique/méthode** sur **toutes** les unités existantes, sans rien d'autre ;
- *corriger une formule* : on incrémente la `version` déclarée de la métrique (PS-10.2),
  ce qui provoque le recalcul automatique partout. On peut aussi, ponctuellement, forcer
  une étape entière via les paramètres d'exécution (PS-11).

**Alternative écartée.** Hacher le *code source* des classes : trop sensible (un
commentaire modifié recalculerait tout), et invisible pour l'utilisateur. On retient une
**version déclarée explicitement** (`version: ClassVar[str]`) plus les paramètres.

### PD-11 — Évolution de schéma pour les nouvelles métriques (via `dt-ducklake-manager 0.3.1`)

**Décision.** La poignée `DuckLakeTable.upsert(df, keys)` s'appuie sur l'API native de
`dt-ducklake-manager 0.3.1` (C-13 résolu) et **n'exécute plus d'`ALTER TABLE` maison** :

- **cas nominal** (ajout d'une métrique, recalcul de lignes entières par l'empreinte,
  PD-10) : `DatabaseUpdater.update_database(df, allow_new_columns=True,
  compact_after_update=False, run_id=<workflow_id>, commit_message=<étape/unité>)`.
  Les colonnes de `df` absentes de la table sont ajoutées (`ALTER TABLE … ADD COLUMN …
  DEFAULT NULL` + ligne de métadonnées) **avant** l'upsert ; les lignes non touchées
  prennent `NULL` jusqu'à leur recalcul ;
- **cas « diffusion d'une colonne »** (une métrique calculée à part sur des lignes déjà
  présentes, sans réécrire les autres colonnes) : `DatabaseUpdater.add_columns(df)`, qui
  n'exige que les clés primaires et les colonnes à ajouter, en une seule `UPDATE … FROM`
  puis un `INSERT` des clés inconnues. Réservé aux migrations ponctuelles (runbook §5.3) :
  le pipeline recalcule des lignes entières.

`run_id` et `commit_message` sont renseignés systématiquement (ils apparaissent sur le
snapshot DuckLake : traçabilité gratuite d'une écriture vers son exécution Argo).
Suppressions et renommages de colonnes ne sont **jamais** automatiques (runbook §5.3).

Pour les tables **longues** (`synthesis`, `synthesis_diagnostics`, clé incluant `method`
ou `statistic`), ajouter une méthode n'ajoute que des lignes : aucune évolution de schéma
n'est nécessaire.

**Justification.** C-13 est résolu en amont ; réimplémenter l'ajout de colonne dans
`kedro_pipeline` dupliquerait la gestion des métadonnées de `dt-ducklake-manager`
(table `metadata`, drapeau catégoriel) et divergerait. `statflows.write_dataframe` doit
seulement **transmettre** ces options (PS-27, point 4).

**À vérifier dans K-08** (test empirique sur catalogue fichier) : le comportement de
`update_database` quand `df` ne contient qu'un **sous-ensemble** des colonnes de la
table (colonnes absentes préservées, mises à `NULL`, ou erreur) ; selon le résultat,
compléter `df` par relecture des colonnes manquantes des lignes ciblées avant l'upsert.

### PD-12 — Synthèse et cohérence incrémentales, par contexte et par méthode

**Décision.** L'unité de fraîcheur de la synthèse devient le **contexte**
`(freq, flow, indicators, TIME_PERIOD)`. Le registre stocke une entrée par contexte, avec
une empreinte par méthode.

- *Nouvelle méthode ou méthode modifiée* : recalcul de **cette seule méthode** sur tous
  les contextes filtrés, plus les pseudo-méthodes de consensus (`borda`, `copeland`) qui
  dépendent de toutes les autres.
- *Amont partenaires `new_data`* : recalcul des contextes des **`RECENT_PERIODS`**
  dernières périodes (par défaut `eurostat.DOWNLOADS.*.N_LAST_OBSERVATIONS`, puisque
  l'incrémental ne rapporte que ces périodes), plus les contextes **jamais calculés**.
- *Amont partenaires `fingerprint` ou `forced`, ou amont réseau modifié* : recalcul de
  tous les contextes filtrés. Un redressement BACI réestime en effet toutes les années de
  son millésime.
- **Cadence plutôt que budget** : la synthèse et la cohérence tournent dans le point
  d'entrée **`weekly`** (PD-23), dans une fenêtre de six jours, **sans approximation**
  (`MAX_CONTEXTS_PER_RUN: null`, toutes les statistiques de cohérence, tous les tirages
  SMAA et bootstrap configurés). La justesse des chiffres présentés prime sur la
  fraîcheur : des données annuelles n'évoluent pas au jour le jour une fois le
  rattrapage terminé. Un budget `MAX_CONTEXTS_PER_RUN` reste disponible **uniquement**
  pour la phase de rattrapage (les contextes périmés les plus récents passent en
  premier, le reste à la passe suivante), et vaut `null` en régime nominal.
- **Périmètre par défaut** : les contextes de la nomenclature **en vigueur**
  (`p."in_force"`, PD-20). `SYNTHESIS.VINTAGES: "in_force" | "all"` étend la synthèse
  aux millésimes historiques, avec `classification` dans `context_columns` (jamais de
  comparaison entre millésimes).
- `FILTERS.LAST_N_PERIODS` disparaît de `base` (null) : on synthétise toutes les périodes
  depuis `ANALYSIS_START_YEAR.eurostat`, mais seulement celles qui sont périmées.
- La cohérence suit la synthèse : un contexte est recalculé si sa synthèse l'a été depuis
  (watermark par contexte), si l'empreinte de la configuration de cohérence a changé, ou
  en cas de forçage. L'empreinte de cohérence est globale : pas de granularité par
  statistique.

**Justification.** Sur toute la période, avec 15 méthodes (SMAA 2 000 tirages, bootstrap,
Kantorovitch), une synthèse complète ne tient pas dans 24 h ; plutôt que d'approximer
(moins de tirages, statistiques de cohérence tronquées), on **espace** les exécutions.
Le registre global actuel (C-10) serait « tout ou rien ».

### PD-13 — Suivi d'exécution : MLflow seul, avec un rapport de contrôle par run

**Décision.**
- **Serveur** : service MLflow Onyxia (lancé par l'utilisateur, §9), métadonnées en
  PostgreSQL et artefacts sur S3. MLflow est l'**unique** outil de supervision
  (révision 2) : paramètres, métriques, contrôles, artefacts, comparaison de runs.
- **kedro-mlflow 2.0.3** (MLflow ≥ 3) gère le cycle de vie des runs : un run par
  exécution de `kedro run`, soit **un run par tâche Argo**. Il fournit la configuration du
  serveur (`config/base/mlflow.yml`) et les datasets de journalisation.
- **Pas de tableau de bord de supervision dans Superset** (décision utilisateur,
  révision 2) : ni table `serving.pipeline_metrics`, ni tracker composite, ni tables
  d'artefacts publiées dans `serving`. Le besoin est de disposer, **dans chaque run
  MLflow**, de tous les éléments qui permettent de s'assurer de sa bonne exécution.
  L'interface d'un run MLflow 3 a des onglets fixes (on ne peut pas en ajouter sans
  forker l'interface) ; le pipeline donne un rôle à chacun (PS-31.1) :

  | Onglet du run | Rôle | Alimenté par |
  |---|---|---|
  | **Overview** | Verdict en un coup d'œil : **description Markdown** générée (statut, contrôles ✅/⚠️/❌, unités prévues/réussies/en échec, chiffres clés, liens), puis paramètres et tags | tag `mlflow.note.content`, tags `health`, `workflow_id`… (PS-31.3) |
  | **Model metrics** | Toutes les métriques, regroupées en sections par préfixe `/` ; séries par `step` (année pour BACI) | `log_metrics` |
  | **System metrics** | CPU, mémoire, disque, réseau du pod pendant le run | `MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING=true` injecté dans les pods (PS-21) |
  | **Artifacts** | `report/report.html` (rapport autonome avec figures Plotly, une section par étape pour BACI), `report/checks.csv`, tables CSV (couverture, `σ̂` par pays, coefficients de gravité…) | `log_text`, `log_table` (PS-31.4) |

- **Contrôles déclaratifs** : les seuils qui qualifient un run (part de requêtes en
  erreur, R² de la gravité, part convertie en tonnes, unités en échec…) sont des
  **paramètres** de `config/base/parameters_tracking.yml` (PS-26, PS-31.2), évalués en
  fin de nœud sur les métriques du `StepResult`. Résultat : métriques `checks/*`, tag
  `health ∈ {ok, warning, failed}`, section « Contrôles » de la description.
- **Pages HTML BACI rétablies** (elles avaient été abandonnées en révision 1 au profit
  de Superset) sous la forme d'**un** rapport HTML autonome par run, une section par
  étape (conversion, fobisation, gravité, qualité, valorisation, réconciliation, NES,
  harmonisation, sortie) : une seule copie de plotly.js par run au lieu d'une par page.
- **Expériences** : l'expérience est choisie par la variable `MLFLOW_EXPERIMENT_NAME`,
  que le rendu Argo injecte **par tâche** d'après le tag Kedro `experiment:<nom>` du nœud
  (PS-21). Correspondance :

  | Expérience | Nœuds | Runs |
  |---|---|---|
  | `trade-01-downloads` | `download_eurostat`, `download_comtrade` | 1 run par source par jour |
  | `trade-02-baci` | `prepare_baci`, `process_baci_<millésime>` | 1 run par millésime par passe hebdomadaire |
  | `trade-03-vulnerabilities` | partenaires, réseau, synthèse, cohérence | 1 run par nœud par exécution |
  | `trade-04-serving` | `publish_serving` | 1 run par exécution |
  | `trade-00-maintenance` | `maintain_ducklake` | 1 run par exécution |

- **Regroupement** : tous les runs d'une même exécution portent le tag
  `workflow_id=<WORKFLOW_ID>` (injecté par argo-kedro), `run_name =
  <nœud>-<WORKFLOW_ID>`, `git_sha`, `image_tag`, `kedro_env`, `node`, `health`. La vue
  d'une exécution complète est la **liste des runs filtrée** par `tags.workflow_id`,
  avec les colonnes `health`, `checks/n_failed`, durée et statut (PS-31.5) ; le DAG
  lui-même se lit dans l'interface Argo (lien dans chaque description).
- **Noms de métriques hiérarchisés par `/`**, que l'interface MLflow (vue *Chart*)
  regroupe automatiquement en sections :
  `conversion/…`, `gravity/…`, `quality/…`, `valuation/…`, `reconciliation/…`,
  `nes/…`, `harmonization/…`, `output/…`, `timing/…` pour BACI ;
  `download/…`, `http/…`, `rate_limit/…`, `coverage/…` pour les téléchargements ;
  `partners/…`, `network/…`, `synthesis/<niveau>/…`, `coherence/<niveau>/…`,
  `drift/…`, `freshness/…` pour le bloc 3 ; `ducklake/<catalogue>/<schéma>/…` pour la
  maintenance.
  *Phase 0 (K-03d) : les scripts conservent les préfixes de leurs rapports, séparés par `/` :
  `baci/{tonnage,gravity,fobisation,mirror,quality_value,quality_quantity,nes}/…`, `hs/…`,
  `coverage/…`, `download/…`, `query/…`, `vulnerabilities/…`, `network_vulnerabilities/…`,
  `synthesis/<niveau>/…`, `coherence/<niveau>/…`, `serving/<table>/…`, `checks/…`, `run/…` ;
  l'étape « conversion » de BACI s'appelle donc `tonnage`. Les noms de PD-13 ci-dessus
  restent la cible de K-13.*

- Le protocole `RunTracker` est conservé et gagne deux méthodes, `log_text(text,
  artifact_file)` et `set_tags(tags)` (implémentées par `NullTracker` et
  `MlflowTracker`). Une implémentation s'ajoute dans `macroforecast/tracking/` :
  **`ActiveRunTracker`**, qui journalise dans le run actif ouvert par kedro-mlflow.
  Toute erreur de journalisation est journalisée en WARNING et **jamais propagée** (un
  serveur MLflow injoignable n'interrompt pas un calcul).
- Le **rapport de run** est construit par du code pur, testable sans MLflow :
  `macroforecast/tracking/report.py` (`Check`, `CheckResult`, `evaluate_checks`,
  `RunReport` avec `to_markdown()` et `to_html()`) et
  `macroforecast/tracking/figures.py` (figures Plotly, import paresseux, extra
  `reports`). Sa publication (`publish_run_report(tracker, report, params)`) vit dans
  `kedro_pipeline/io/tracking.py`. Le hook `on_node_error` publie une description
  réduite (étape, exception tronquée, `health=failed`) ; les runs laissés `RUNNING` par
  un pod tué (OOM, expiration) sont clos par la maintenance `onExit` (PR-20).
- `flatten_metrics(payload, prefix, sep=".")` gagne un paramètre `sep`. Le pipeline
  utilise `sep="/"` ; le défaut `"."` préserve les tests existants.

**Justification.** Chaque tâche Argo produit exactement un run : c'est la bonne maille
pour répondre à « cette exécution s'est-elle bien passée ? ». En plaçant le verdict
(description et contrôles) dans l'onglet *Overview* et le détail dans les trois autres,
on n'a qu'un endroit à consulter, sans double journalisation ni second tableau de bord à
maintenir. Les seuils étant en configuration, un run n'est plus « à interpréter » : il
est `ok`, `warning` ou `failed`, et la colonne `health` de la liste des runs le montre
pour toute une exécution.

**Alternatives écartées.**
- *Tableau de bord Superset « Supervision »* alimenté par `serving.pipeline_metrics`
  (révision 1) : double écriture des métriques, table PostgreSQL écrite par petits lots
  (ce que DuckLake gère mal), file de rejeu, tables d'artefacts à publier, second tableau
  de bord à maintenir, pour une information déjà présente dans MLflow.
- *Plugin ou fork de l'interface MLflow* pour ajouter des onglets : coût de maintenance
  hors de proportion.
- *Run parent par workflow et runs enfants par tâche* : kedro-mlflow ouvre un run par
  `kedro run` (par pod) ; rattacher des runs de pods différents à un parent exigerait de
  créer le parent avant le DAG et d'en propager l'identifiant. Gardé comme évolution si
  la liste filtrée par `workflow_id` se révèle insuffisante.

### PD-14 — Ordonnancement : argo-kedro pour le DAG, rendu maison en `WorkflowTemplate` + `CronWorkflow`

**Décision.**
- `argo-kedro==0.1.41` (version épinglée : paquet 0.x, API mouvante) sert à :
  1. **calculer le DAG** Argo à partir des pipelines Kedro (`get_argo_dag`), en
     respectant `FusedPipeline` et le `machine_type` par nœud ;
  2. fournir la configuration `config/base/argo.yml` (types de machines, image) ;
  3. exposer `kedro argo submit --dry_run` pour inspecter le rendu brut.
- argo-kedro **ne sait pas** produire de `CronWorkflow` (seul `kind: Workflow` est rendu
  et soumis). Il ne transmet ni `--env` ni `--params` à `kedro run`, ni
  `serviceAccountName`, TTL, `onExit`, politiques de reprise ou dépendances tolérantes à
  l'échec. On ajoute donc un **rendu maison** `kedro_pipeline/deploy/render.py`
  (commande `kedro trade render-argo`), qui :
  - appelle `get_argo_dag` (import de bibliothèque, pas de sous-processus) ;
  - produit, par un gabarit Jinja versionné, **trois manifestes** :
    `kubernetes/generated/workflowtemplate.yaml` (`WorkflowTemplate trade-pipeline`, avec
    deux templates DAG `daily` et `weekly`, PD-23), `kubernetes/generated/cronworkflow-daily.yaml`
    (`CronWorkflow trade-pipeline-daily`) et `kubernetes/generated/cronworkflow-weekly.yaml`
    (`CronWorkflow trade-pipeline-weekly`), les deux CronWorkflow référençant le template
    avec leur point d'entrée ;
  - injecte les paramètres de workflow (`kedro-env`, `image-tag`, `force-steps`,
    `force-metrics`, `force-methods`, `max-runtime-hours`) transmis à
    `kedro run --env … --params …` ;
  - injecte les secrets et l'environnement (PS-05), `MLFLOW_EXPERIMENT_NAME` par tâche,
    les variables des métriques système MLflow (depuis `tracking.SYSTEM_METRICS`, PS-31.2),
    les ressources par `machine_type`, `retryStrategy`, `activeDeadlineSeconds`, et les
    **mutex** (`synchronization.mutex`) des tâches taguées `mutex:<nom>` ;
  - convertit le nœud tagué `onexit` (maintenance) en gestionnaire `onExit` ;
  - rend les dépendances **tolérantes à l'échec amont**
    (`depends: "(a.Succeeded || a.Failed)"`) pour les étapes idempotentes pilotées par
    registre (§2.3). Le workflow reste marqué `Failed` si une tâche a échoué ;
  - affecte chaque nœud à un ou plusieurs points d'entrée d'après son tag Kedro
    `cadence:daily` / `cadence:weekly` (un nœud peut porter les deux, comme
    `publish_serving`).
- Les manifestes générés sont **versionnés**. Un job de CI vérifie qu'ils correspondent au
  code (`render-argo --check`). Le déploiement se fait par `kubectl apply -f
  kubernetes/generated/`.
- Pour une exécution ponctuelle (rattrapage, forçage) :
  `argo submit --from workflowtemplate/trade-pipeline --entrypoint weekly -p force-steps=synthesis`.
- Le pod appelle `kedro run --pipeline __default__ --nodes <nœud>` (commande générée par
  argo-kedro) complétée de `--env` et `--params`. **À vérifier dans K-15** : argo-kedro
  enregistre une commande globale `run` qui remplace `kedro run` par sa version
  `FusedRunner` ; il faut confirmer qu'elle coexiste avec les hooks kedro-mlflow.

**Justification.** Un seul workflow, voulu par l'utilisateur ; kedro reste la source de
vérité du DAG ; la planification quotidienne et les exécutions ponctuelles partagent le
même template. Le rendu maison est petit, testé et remplaçable si argo-kedro ajoute ces
fonctions.

### PD-15 — Image et intégration continue : GHCR public, GitHub Actions

**Décision.**
- Image **`ghcr.io/qbolliet/trade-analysis`**, visibilité **publique** (pas
  d'`imagePullSecret`, gratuit). Étiquettes : SHA court du commit (immuable, utilisée par
  Argo), `main`, `latest`, et tag git `vX.Y.Z` le cas échéant.
- `Dockerfile` à deux étages (PS-22) : `python:3.13-slim`, `uv` épinglé, `uv sync
  --frozen --no-dev --extra tracking --extra reports --extra optimal-transport`, extensions DuckDB
  (`ducklake`, `postgres`, `httpfs`) **préinstallées** dans l'image, utilisateur non root.
- GitHub Actions (PS-23) : `ci.yml` (tests + `render-argo --check`), `image.yml`
  (construction avec cache `type=gha` et publication sur GHCR), `docs.yml` (site statique
  sur GitHub Pages).
- **Coût nul** si le dépôt est public (minutes Actions illimitées, Pages gratuit, stockage
  et bande passante GHCR gratuits pour les paquets publics). Si le dépôt reste privé :
  2 000 minutes/mois gratuites, et **Pages indisponible** sur un compte gratuit (PQ-08).

### PD-16 — Maintenance DuckLake quotidienne, sans intervention humaine

**Décision.**
1. **Écriture** : `compact_after_update` devient configurable et vaut `False` pendant les
   téléchargements (C-08). Les écritures du téléchargement sont **tamponnées** (flush
   toutes les N requêtes ou T secondes, PS-27). L'inlining des petites écritures dans le
   catalogue (`DATA_INLINING_ROW_LIMIT`) est activé si la version DuckLake embarquée le
   supporte (à vérifier sur DuckDB 1.5.3).
2. **Nœud `maintain_ducklake`** (`onExit`, chaque jour), par catalogue (`eurostat`,
   `comtrade`, `vulnerabilities`, `serving`) et pour **chaque table écrite dans les 24 h** (d'après
   les snapshots) :
   | Opération | Fréquence | Paramètre |
   |---|---|---|
   | `ducklake_flush_inlined_data` | quotidienne | si l'inlining est actif |
   | `ducklake_merge_adjacent_files` | quotidienne | — |
   | `ducklake_rewrite_data_files` | quotidienne **si** part de lignes supprimées > seuil, sinon hebdomadaire | `DELETE_RATIO_THRESHOLD: 0.1`, `WEEKLY_DAY: 6` |
   | `ducklake_expire_snapshots` | quotidienne | `SNAPSHOT_RETENTION_DAYS: 7` |
   | `ducklake_cleanup_old_files` (+ fichiers orphelins) | quotidienne | `CLEANUP_OLDER_THAN_DAYS: 7` |
   | `VACUUM (ANALYZE)` du PostgreSQL du catalogue | hebdomadaire | `WEEKLY_DAY` |
   | Sauvegarde logique `pg_dump` du catalogue vers S3 | hebdomadaire, rétention 4 | `BACKUP.*` |
3. **Partitionnement**, posé une fois par le nœud à la création de la table (idempotent) :
   `comtrade/C_A_HS` par année (`refYear` ou `period`), `baci_hs*` par `year`,
   `eurostat/DS_045409` par `reporter`, `indicators` par `classification`,
   `network_indicators` par `classification`, `synthesis` par `TIME_PERIOD`. Les tables
   de service volumineuses (`cell_scores`, `flows`) sont partitionnées par `year` par
   `publish_serving` lui-même, à leur (re)création (PS-29.1).
4. **Observabilité** : avant/après chaque opération, le nœud relève le nombre de fichiers
   de données, leur taille moyenne, le nombre de fichiers de suppression et le nombre de
   snapshots (tables `__ducklake_metadata_*`), et les publie dans
   `trade-00-maintenance`. **Alerte** si le nombre de fichiers d'une table dépasse
   `MAX_FILES_PER_TABLE` après maintenance.
5. Les opérations sont non fatales une à une (sémantique déjà en place dans
   `DuckLakeMaintenance`) ; le nœud échoue seulement si **toutes** échouent sur un
   catalogue.
6. **Clôture des runs orphelins** : le nœud passe en `FAILED` les runs MLflow du même
   `workflow_id` restés `RUNNING` (pod tué avant d'avoir pu fermer son run), avec une
   description « tâche interrompue — voir Argo » (PR-20, PS-31.5).

**Justification.** Un pipeline quotidien qui fait des upserts crée chaque jour des
petits fichiers, des tombstones et des snapshots. Sans compaction, expiration et
nettoyage réguliers, les lectures se dégradent et le stockage croît sans limite. Placer
la maintenance en `onExit` du même workflow respecte la préférence « un seul workflow »
et garantit qu'elle ne tourne jamais pendant une écriture du pipeline.

### PD-17 — Documentation : site statique mkdocs-material + kedro-viz

**Décision.** Site généré par `mkdocs build` (thème Material), avec le rendu statique
de `kedro viz build` copié dans `site/pipeline/` et lié depuis la navigation (« Pipeline
interactif »). Publié sur GitHub Pages par `docs.yml`. Contenu minimal : accueil (README),
architecture (ce document), runbooks (§5), configuration (référence des paramètres
générée depuis `config/base/`), méthodologies (liens vers les PDF LaTeX compilés, s'ils
sont publiés), pipeline (kedro-viz).

`kedro viz build` s'exécute en CI **sans secrets** : les datasets ne se connectent pas à
l'instanciation (PD-04), et `credentials.yml` résout vers des valeurs vides grâce aux
défauts `oc.env`.

### PD-18 — Contrat des secrets et de l'environnement d'exécution

**Décision.** Quatre secrets Kubernetes, exposés comme variables d'environnement dans
toutes les tâches (injectés par le rendu) :

| Secret | Clés | Statut |
|---|---|---|
| `trade-s3-credentials` | `S3_ACCESS_KEY`, `S3_SECRET_KEY` → exposées en `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | **existe** |
| `comtrade-api-credentials` | `COMTRADE_FREE_SUBSCRIPTION_KEY`, `COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY` | **existe** |
| `trade-postgres-credentials` | `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGADMINUSER` | **à créer** (§9) |
| `trade-mlflow-credentials` | `MLFLOW_TRACKING_URI`, et si l'authentification est active `MLFLOW_TRACKING_USERNAME`, `MLFLOW_TRACKING_PASSWORD` | **à créer** (§9) |

Le secret `trade-serving-credentials` de la révision 1 est **supprimé** (révision 2) :
le catalogue DuckLake `serving` est écrit avec `trade-postgres-credentials`, comme les
autres catalogues. Superset lit ce catalogue avec ses **propres** identifiants (rôle
PostgreSQL en lecture seule `superset_reader` sur la base de métadonnées `serving`,
accès S3 déjà assuré par le service), configurés dans Superset et jamais dans le
pipeline (PS-30.1, §9).

Variables non secrètes (valeurs dans le template) : `AWS_S3_ENDPOINT=minio.lab.sspcloud.fr`,
`AWS_DEFAULT_REGION=us-east-1`, `MLFLOW_S3_ENDPOINT_URL=https://minio.lab.sspcloud.fr`,
`KEDRO_ENV`, `PYTHONUNBUFFERED=1`, `WORKFLOW_ID` et `NUM_CPU` (injectées par argo-kedro),
`MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING` et `MLFLOW_SYSTEM_METRICS_SAMPLING_INTERVAL`
(depuis `tracking.SYSTEM_METRICS`, PS-31.2).
`AWS_SESSION_TOKEN` devient **optionnelle** (C-03).

### PD-19 — Phasage : un « jalon démonstration » indépendant de la migration Kedro

**Décision.** La migration complète (Kedro, argo-kedro, kedro-mlflow, rendu, docs)
représente une douzaine de prompts, et **le rattrapage des données prend plusieurs
jours** : le téléchargement doit démarrer **avant** la fin de la migration. D'où deux
voies :

- **Phase 0 (J0–J3)** : corriger les bloquants des scripts (C-01, C-02, C-03, C-14),
  publier l'image, déployer un `WorkflowTemplate`/`CronWorkflow` **écrit à la main** qui
  enchaîne les scripts existants selon le DAG de §2.2, sur le **périmètre `demo`**
  (une centaine de codes produits, années récentes d'abord), puis **publier la couche de
  service et construire le tableau de bord Superset** (K-03b, K-03c), et doter les runs
  des scripts de leur **rapport de contrôle MLflow** (K-03d). Les
  téléchargements tournent dès J1. Le workflow de transition est remplacé par le
  workflow généré en phase 4.
- **Phases 1 à 4** : robustesse `statflows`, méthodologie paramétrable, Kedro, puis
  déploiement généré (§10).

Le **jalon « tableau de bord »** (🎯 dans `PIPELINE_PROMPTS.md`) se situe **après K-03b
et avant K-04** : dès que les vulnérabilités, la synthèse et la cohérence existent pour
la centaine de produits `demo`, la couche de service est publiée et le tableau de bord
construit. Tout ce qui suit (phases 1 à 4) enrichit les tables sans changer leur contrat
de lecture, à l'exception des colonnes `classification` / `in_force` (PD-20), que la
couche de service dérive elle-même tant qu'elles n'existent pas (PS-29.3).

**Justification.** La présentation ne peut montrer « des résultats de chaque étape » que
si des données existent. Découpler garantit ce jalon, même si la migration Kedro prend
du retard.

### PD-20 — Millésimes de nomenclature : une table par famille, clé `classification`, drapeau `in_force`

**Problème.** Les codes déclarables changent dans le temps (SH6 tous les ~5 ans, NC8
chaque année) et la plupart des codes gardent la même définition entre deux révisions.
Les restitutions portent sur une année donnée et doivent parler la nomenclature de
cette année ; mais un graphique d'**évolution temporelle** d'un produit exige une
nomenclature **fixe**. L'option envisagée — une seconde table avec une clé « année du
système de nomenclature », chaque année étant recalculée dans chaque système — multiplie
la base par le nombre de systèmes (×20 avec les NC8 annuelles) et duplique des valeurs
identiques pour les codes stables.

**Décision.**
1. **Un référentiel unique des millésimes**, `runtime.NOMENCLATURES.HS` (PS-04.1) :
   `{HS1992: 1988, HS1996: 1996, HS2002: 2002, HS2007: 2007, HS2012: 2012, HS2017: 2017,
   HS2022: 2022}` (année d'entrée en vigueur). La fonction
   `vintage_in_force(year)` renvoie le millésime SH en vigueur une année donnée.
2. **Une seule table par famille de résultats**, dont la clé primaire gagne
   `classification`, et **pas** de seconde table « par système » :
   - `indicators` (partenaires) : clé `(classification, reporter, product, flow, freq,
     indicators, TIME_PERIOD)` ; colonnes ajoutées `hs_vintage` (millésime SH de
     rattachement du code), `in_force BOOLEAN`, `is_provisional BOOLEAN` ;
   - `network_indicators` : déjà clé par `classification` ; colonne `in_force` ajoutée ;
   - `synthesis`, `synthesis_diagnostics` : `classification` entre dans
     `context_columns` (deux millésimes ne sont jamais comparés).
3. **Quelles lignes existent** dans `indicators`, pour un reporter et une période `t` :
   - les lignes **« en vigueur »** (`in_force = true`) : tous les codes tels que
     déclarés dans Comext pour `t`, à tous les niveaux (SH2, SH4, SH6, NC8), avec
     `classification = vintage_in_force(t)` pour les codes SH et
     `classification = "CN<t>"` pour les codes NC8 (`hs_vintage = vintage_in_force(t)`
     dans les deux cas) ;
   - les lignes **« historiques »** (`in_force = false`) : pour chaque millésime SH `V`
     antérieur au millésime en vigueur et tel que `t ≥ entrée(V)`, les métriques
     recalculées sur les flux Comext de `t` **convertis vers `V`** (codes SH6
     seulement) par le même `HsHarmonizer` et les mêmes tables de passage UNSD que BACI.
     La conversion est **rétrograde** (codes récents → millésime plus ancien), c'est-à-
     dire presque toujours une agrégation (n → 1), exacte pour des valeurs ; les
     rares cas 1 → n suivent la règle déjà implémentée dans `HsHarmonizer` (même
     règle que BACI, donc cohérence entre familles).
   Ainsi, pour la période `t`, la ligne `in_force` **est** la ligne du millésime
   `vintage_in_force(t)` : il n'y a pas de duplication entre « table principale » et
   « table temporelle », la première n'est qu'un **filtre** sur la seconde.
4. **Lecture** :
   - vue d'une année (tableau de bord, page « pays » ou « produit ») : `in_force = true` ;
   - évolution temporelle d'un produit : choisir un millésime de référence `V`
     (par défaut, celui en vigueur l'année sélectionnée) et lire
     `classification = V` sur `t ≥ entrée(V)` ; un millésime plus ancien donne une
     série plus longue au prix d'une nomenclature plus grossière (c'est exactement la
     convention BACI du CEPII, où HS92 offre la plus longue série). La table de
     passage `reference.hs_concordance` (PS-28.4) permet de retrouver le code du
     produit dans `V`.
5. **Volumétrie** : les lignes historiques n'existent qu'au niveau SH6 et qu'à partir de
   l'entrée en vigueur de chaque millésime ; sur 1988-2025, cela représente
   Σ<sub>V</sub>(années ≥ entrée(V)) ≈ 38 + 30 + 24 + 19 + 14 + 9 + 4 = **138
   années-millésimes contre 38** pour les seules lignes en vigueur, soit environ **×3,6
   au niveau SH6** (et rien de plus aux niveaux SH2/SH4/NC8), loin du ×20 redouté. Les
   valeurs identiques entre millésimes (codes stables) sont compressées efficacement
   par le stockage colonnaire Parquet, et `classification` est clé de partition
   (PD-16). La synthèse ne tourne par défaut que sur `in_force` (PD-12).
6. **Calcul** : dans le nœud partenaires, l'unité de fraîcheur devient
   `(classification, reporter, product)` ; pour une ligne historique, la source d'un
   produit cible est l'ensemble des codes de la nomenclature en vigueur qui s'y
   projettent (préimage de la table de passage), et son watermark est le maximum de
   leurs `last_download`. Une unité historique n'est calculée que lorsque **tous** ses
   codes sources ont été téléchargés au moins une fois (sinon `freshness/units_waiting_sources`).
   Paramètre `vulnerabilities.VINTAGES: "all"` (par défaut) ou liste explicite ; les
   millésimes NC8 (`CN<t>`) ne sont pas convertis tant qu'aucune table de passage NC8
   cohérente n'existe (extension prévue : même mécanisme, nouveau bloc
   `runtime.NOMENCLATURES.CN`).
7. **Jointure réseau dans la synthèse** : `n."classification" = p."hs_vintage" AND
   n."product" = substr(p."product", 1, 6)` remplace le `HS2022` en dur (C-11) : chaque
   ligne partenaires est jointe au BACI **de son propre millésime**, y compris pour les
   lignes historiques.

**Alternatives écartées.** Une table par système de nomenclature (×20, duplication) ;
une conversion **prograde** vers le millésime le plus récent pour allonger ses séries
(les scissions 1 → n obligeraient à ventiler des valeurs anciennes sans clé de
répartition : approximation que BACI lui-même refuse) ; ne rien convertir et tracer les
séries « à code constant » (fausses dès qu'un code est redéfini).

**Conséquences.** `prepare_baci` (tables de passage) devient un amont **partagé** des
partenaires historiques et de BACI ; les tables de passage et les libellés sont
exposés dans un schéma `reference` (PS-28.4) ; la clé primaire d'`indicators` change
(migration : recréation de la table, runbook §5.3).

### PD-21 — Couche de restitution : catalogue DuckLake `serving` alimenté par le pipeline, lu directement par Superset

**Décision** (révision 2, qui remplace la base PostgreSQL `trade_serving` de la
révision 1).
- **Superset** (service Onyxia) est l'outil de restitution, pour le seul tableau de bord
  « Vulnérabilités » (PS-30) ; la supervision est dans MLflow (PD-13).
- Superset lit **DuckLake directement**, par le pilote **`duckdb-engine`**, déjà
  installé dans le chart Superset d'Onyxia ; l'accès du service au bucket S3 est déjà
  assuré (PQ-13 résolue). Une connexion Superset = une session DuckDB en mémoire qui
  attache **en lecture seule** le seul catalogue `serving` (PS-30.1).
- Le nœud **`publish_serving`** (fonction d'étape `kedro_pipeline/steps/serving.py`,
  PS-29), en fin des deux points d'entrée, **dérive** des tables de résultats les tables
  de service par des requêtes SQL DuckDB déclarées en configuration
  (`config/base/parameters_serving.yml`), et les **matérialise dans un catalogue DuckLake
  dédié `serving`** (base de métadonnées PostgreSQL `serving` sur la même instance que
  les autres catalogues, fichiers sous `trade/datasets/serving/`, schéma
  `serving.SCHEMA` : `dashboard`, `demo_dashboard` en `demo`) :
  - toutes les tables d'une publication sont écrites dans **une seule transaction**
    DuckDB : Superset voit l'état précédent jusqu'au `COMMIT`, puis le nouvel état en
    entier (un snapshot DuckLake). Plus de tables `__new`, de `RENAME` ni de `psycopg2` ;
  - les tables volumineuses (`cell_scores`, `flows`) sont **partitionnées par `year`**
    et insérées triées (`ORDER BY year, reporter, product`) pour que les filtres du
    tableau de bord élaguent les fichiers lus (statistiques min/max) ;
  - mode `full` (phase 0) : recréation de chaque table ; mode `by_year` (production) :
    `DELETE … WHERE year IN (…)` + `INSERT` des seules années dont l'amont a changé, avec
    retour automatique en `full` si les colonnes produites par la requête diffèrent de
    celles de la table (nouvelle colonne de restitution).
- **Pourquoi un catalogue dédié** plutôt qu'un schéma de `vulnerabilities` : Superset
  n'attache qu'un catalogue, avec un rôle PostgreSQL qui ne lit que la base de
  métadonnées `serving` (moindre privilège) ; les écritures de restitution ne créent pas
  de snapshots dans les catalogues de calcul ; la maintenance le traite comme les autres
  (PD-16).
- Les tables de service sont **larges et prêtes à l'affichage** (une ligne par cellule,
  libellés joints, scores et rangs des méthodes de synthèse retenues en colonnes), pour
  qu'un utilisateur néophyte de Superset n'ait à écrire ni jointure ni requête (PS-29.2).
- L'**Union européenne dans son ensemble** est un reporter Comext à part entière
  (`EU27_2020`, dont les flux sont extra-UE par construction) ajouté à la liste
  `reporter.include` du téléchargement, et non une agrégation des États membres faite
  dans le pipeline : l'HHI des fournisseurs extra-UE de l'Union est la notion de
  dépendance pertinente à ce niveau, et l'agrégation des membres compterait le commerce
  intra-UE comme des « fournisseurs ». `CDI2` y vaut 1 par construction (PD-09) et est
  exclu de la synthèse pour ce reporter. Vérifier que le code existe dans la codelist
  `reporter` de DS-045409 (PQ-17).

**Justification.** Le pipeline produit des tables analytiques normalisées ; un tableau
de bord a besoin de tables dénormalisées et libellées, qu'un utilisateur néophyte de
Superset n'ait pas à joindre lui-même. Le nœud de publication reste donc nécessaire ;
seule sa **cible** change. Le pilote DuckDB étant disponible dans Superset, une copie
PostgreSQL n'apporte plus rien et coûte une base, un secret, un rôle, un basculement
maison et une recopie de dizaines de millions de lignes (ancien PR-15). DuckLake apporte
en outre l'isolation par snapshot, qui rend la publication atomique sans mécanisme
supplémentaire. Les tables de service restent jetables et reconstruites ; Superset
reste hors du chemin critique du calcul.

**Alternatives écartées.**
- *Base PostgreSQL `trade_serving`* (révision 1) : voir ci-dessus.
- *Superset branché sur les catalogues de calcul, avec des vues* : chaque graphique
  referait les jointures (libellés, réseau, scores des méthodes) sur des tables non
  partitionnées pour cet usage, et Superset devrait attacher trois catalogues avec des
  droits de lecture sur toutes les données brutes.

**Conséquences.** La version de DuckDB (et donc de l'extension `ducklake`) de l'image
Superset doit être **compatible avec celle du pipeline** (1.5.3) : un catalogue écrit
par une version plus récente peut être illisible par une plus ancienne (PR-14). La
latence d'un graphique dépend de la lecture de Parquet sur S3 : partitionnement, tri et
**cache des graphiques** Superset y répondent (PS-30.1, PR-15).

### PD-22 — BACI : estimation exacte par passes et statistiques suffisantes ; réestimation du millésime entier

**Décision.** Le redressement BACI d'un millésime est réécrit en une **suite de passes
sur des tranches annuelles**, chaque estimateur mis en commun sur plusieurs années
(conversion en tonnes, médiane mondiale des valeurs unitaires, équation de gravité et
distance de Cook, ANOVA de qualité des déclarants) étant exprimé par des **statistiques
suffisantes additives** accumulées tranche par tranche (PS-14). Le résultat est
**identique** au traitement monobloc (à la tolérance numérique près, vérifiée par test),
la mémoire est bornée à une tranche, et **aucune approximation méthodologique** n'est
introduite : PQ-10 (fenêtres glissantes) est tranchée par la négative.

Corollaire (C-24) : une passe **réestime et réécrit tout le millésime**. Le millésime est
donc l'unité de fraîcheur (PD-10), et la passe tourne à cadence hebdomadaire (PD-23),
déclenchée seulement par l'entrée d'une **nouvelle année complète** dans le périmètre,
par une révision amont au-delà d'un intervalle minimal, par un changement d'empreinte ou
par un forçage (PS-14.6).

**Justification.** Charger « l'ensemble des flux d'une nomenclature » — ce qu'exigerait
une estimation monobloc fidèle — représente plusieurs centaines de millions de lignes
pour `HS1992` ; charger une seule année suffirait pour les étapes par flux mais
trahirait les estimations groupées. Les moindres carrés pondérés (avec ou sans effets
absorbés) ne dépendent des données que par des produits croisés `X'WX`, `X'Wy` et des
sommes par groupe, tous additifs ; la distance de Cook et les écarts-types robustes ne
demandent qu'une seconde lecture des mêmes tranches. C'est la seule manière de
respecter à la fois la méthodologie originale et la contrainte mémoire.

### PD-23 — Deux cadences sur un même template : `daily` et `weekly`

**Décision.** Le `WorkflowTemplate` `trade-pipeline` expose deux templates DAG (§2.2) :

| Point d'entrée | Contenu | Planification | `activeDeadlineSeconds` | `concurrencyPolicy` |
|---|---|---|---|---|
| `daily` | téléchargements ∥ partenaires → `publish_serving` ; `onExit` maintenance | `0 1 * * *` Europe/Paris | 82 800 (23 h) | `Forbid` |
| `weekly` | `prepare_baci` → `process_baci_*` → réseau → synthèse → cohérence → `publish_serving` ; `onExit` maintenance | `0 2 * * 6` (samedi 02:00 ; `WEEKLY_DAY` paramétrable) | 518 400 (6 jours) | `Forbid` |

Les deux exécutions peuvent se recouvrir : elles n'écrivent jamais la même table DuckLake
(la lecture concurrente est isolée par snapshot), et les deux tâches partagées —
`publish_serving` et la maintenance — sont sérialisées par des **mutex Argo**
(`trade-serving`, `trade-maintenance`). En phase de rattrapage, le `weekly` peut être
soumis à la main plus souvent (`argo submit --entrypoint weekly`).

**Justification.** L'utilisateur accepte explicitement des exécutions plus espacées
pour obtenir des chiffres de cohérence **sans approximation**, et BACI réestime un
millésime entier à chaque passe (PD-22) : les deux étapes n'ont rien à faire dans une
fenêtre de 24 h. Séparer les cadences évite qu'un calcul long bloque l'acquisition
quotidienne (`Forbid` sur un workflow unique aurait sauté les téléchargements pendant
toute la durée de la cohérence). Un seul template, un seul DAG Kedro : la préférence
pour « un seul workflow » est respectée dans sa substance.

### PD-24 — Données fictives de démonstration : isolées, marquées, retirables

**Contexte.** Pendant la phase 0, le fournisseur Comtrade a cessé de répondre (API de
données) et le téléchargement Comext était trop lent pour la présentation. Pour éprouver
les étapes aval (BACI, vulnérabilités, synthèse, cohérence, service) et construire le
tableau de bord, un **monde simulé** remplace Comtrade et complète Comext (2026-09-21).
Ce n'est pas une brique de la cible : c'est un **échafaudage transitoire**, à retirer
entièrement une fois les données réelles complètes disponibles.

**Décision.**

1. **Périmètre.** Modèle gravitaire (`kedro_pipeline/synthetic/`, paramètres dans
   `config/profiles/demo/synthetic.yaml`) ; deux scripts : `synthetic-comtrade-script`
   (remplace `comtrade-script` : mêmes requêtes planifiées, même écriture DuckLake, même
   registre — seules les lignes tariffline sont simulées) et
   `complete-synthetic-comext-script` (simule uniquement les requêtes Comext **absentes du
   registre**, schéma appris sur la table existante). Activés par les paramètres du
   workflow de transition `comtrade-mode` et `eurostat-mode` (défaut : `download`, donc
   aucun effet sans demande explicite).
2. **Isolation.** Les données fictives sont écrites **uniquement dans les catalogues
   `demo_*`** (jamais dans un catalogue de production). Garde exécutée avant toute
   connexion : `synthetic.SAFETY.REQUIRED_CATALOG_PREFIX` (`demo_`) est comparé au
   `DOWNLOADS.DBNAME` du fichier de configuration sélectionné ; un profil de production
   (`comtrade`, `eurostat`…) lève `RuntimeError`. Testée (`tests/test_synthetic_isolation.py`).
3. **Marquage.** Chaque entrée de registre écrite est marquée `"synthetic": true`
   (`statflows` ne lit que `last_download` et `params`, l'ajout est inoffensif). Un registre
   de **production** ne doit **jamais** en contenir : c'est le contrôle de non-contamination
   du retrait (point 5). Les tables `demo_*` mélangent réel et fictif (choix explicite du
   2026-09-21) ; leur suppression complète est donc le seul retour arrière propre.
4. **Retirabilité.** Aucun code de production ne dépend du code fictif (testé). Il n'est
   **pas** porté dans Kedro : K-10 à K-12 ne le migrent pas ; il vit dans `scripts/` (dont il
   importe la planification des requêtes) jusqu'à K-18. Si une étape de migration le
   casse, on le **supprime** (K-18) au lieu de le maintenir.
5. **Retrait en deux prompts, dans cet ordre.** **K-17b 🔌** (cluster, données) puis
   **K-18** (dépôt, code). Précondition commune : la production tourne sur les **données
   réelles complètes**.
   - *Données réelles complètes* (précondition de K-17b, à mesurer, PQ-20) : pour chaque
     source de production, part des requêtes planifiées présentes au registre (Comext :
     toutes ; Comtrade : `COMPLETENESS.MIN_SHARE` atteint sur toutes les années), au moins
     une exécution `daily` et une `weekly` de production réussies, tableau de bord recette
     validé sur les schémas de production (`dashboard`, PS-30.5).
   - *K-17b* : inventaire en lecture seule des objets `demo` (bases PostgreSQL
     `demo_*`, schémas `demo_*` des catalogues partagés, préfixes S3 `trade/demo/`,
     registres JSON du profil, expériences MLflow `demo-*`, objets Superset du demo,
     workflows Argo de transition) ; contrôle de non-contamination (aucun registre de
     production marqué `synthetic`, tailles et instantanés des catalogues de production
     relevés avant/après) ; suppression **après confirmation explicite, objet par objet** ;
     vérification qu'aucun objet `demo` ne subsiste et que la production est inchangée.
   - *K-18* : suppression du code (`kedro_pipeline/synthetic/`, les deux scripts et leurs
     entrées `[project.scripts]`, `synthetic.yaml`, `tests/test_synthetic_*.py`, fixtures
     synthétiques de `tests/conftest.py`, paramètres et tâches `*-mode` du workflow de
     transition, sections de README) et de toute référence (`grep`, critères de K-18).

**Justification.** Des chiffres simulés qui atteindraient un catalogue ou un tableau de
bord de production seraient pris pour des mesures : d'où l'isolation par préfixe vérifiée
par du code (pas par convention), le marquage des registres (seule trace durable de
l'origine d'une série) et un retrait en deux temps où la partie irréversible (données)
est isolée, inventoriée et confirmée avant la partie mécanique (fichiers, réversible par
`git`). Ne pas porter le code dans Kedro évite de financer la maintenance d'un
échafaudage voué à disparaître.

**Alternatives écartées.** *Profil `synthetic` à catalogues séparés* : isolation plus forte
mais K-03b/K-03c et les schémas `demo_*` auraient été à dupliquer à J-2 de la présentation
(choix de l'utilisateur : `demo_*`). *Colonne `is_synthetic` dans les tables de faits* :
change le schéma de tables que les étapes aval lisent telles quelles, et ne protège pas les
agrégats calculés dessus.

---

## 4. Spécifications détaillées

### PS-01 — Arborescence cible

```
trade-analysis/
├── .github/workflows/{ci.yml, image.yml, docs.yml}
├── .dockerignore
├── config/
│   ├── base/
│   │   ├── catalog.yml                     # PS-07
│   │   ├── credentials.yml                 # PS-05 (oc.env uniquement, versionné)
│   │   ├── mlflow.yml                      # PS-19
│   │   ├── argo.yml                        # PS-20
│   │   ├── parameters_runtime.yml          # PS-04.1, PS-11
│   │   ├── parameters_comtrade.yml
│   │   ├── parameters_eurostat.yml
│   │   ├── parameters_oecd.yml
│   │   ├── parameters_baci.yml
│   │   ├── parameters_vulnerabilities.yml
│   │   ├── parameters_synthesis.yml
│   │   ├── parameters_serving.yml          # PS-29 (tables de service, requêtes SQL)
│   │   ├── parameters_tracking.yml         # PS-31 (contrôles, rapport de run, métriques système)
│   │   └── parameters_maintenance.yml
│   ├── cloud/  {parameters_runtime.yml, …}  # surcharges Argo
│   ├── demo/   {parameters_*.yml}           # périmètre présentation (supprimé en K-18)
│   ├── test/   {parameters_*.yml, catalog.yml}  # environnement des tests e2e
│   └── local/  .gitkeep
├── docker/Dockerfile
├── docs/ {index.md, architecture.md → lien, runbooks.md, configuration.md, dashboards.md, supervision.md}
├── mkdocs.yml
├── kubernetes/
│   ├── generated/{workflowtemplate.yaml, cronworkflow-daily.yaml, cronworkflow-weekly.yaml}  # rendus (PS-21)
│   ├── transition/{workflowtemplate.yaml, cronworkflow.yaml}  # phase 0 (PD-19), supprimé en K-18
│   └── examples/ (ancien workflow.yaml, configmap.yaml déplacés ; supprimé en K-18)
├── superset/
│   ├── README.md                            # guide pas à pas (connexion DuckDB, PS-30)
│   └── vulnerabilites/                      # export Superset versionné (datasets, charts, dashboard)
├── macroforecast/ (inchangé dans son rôle ; baci.py réorganisé en estimateurs par passes, PS-14 ;
│                   tracking/report.py et tracking/figures.py : rapport de run pur, PS-31)
├── scripts/ (phase 0 → enveloppes minces → supprimés en K-18, PD-02)
├── tools/ (utilitaires hors pipeline : migrations de registres, mesures ponctuelles)
├── kedro_pipeline/
│   ├── __init__.py
│   ├── __main__.py
│   ├── settings.py
│   ├── pipeline_registry.py
│   ├── config.py               # load_parameters(env), accès typé, vintage_in_force
│   ├── hooks.py                # hooks projet (tags MLflow, NUM_CPU → n_jobs)
│   ├── cli.py                  # commandes projet `kedro trade …`
│   ├── parallel.py             # resolve_n_jobs, parallel_map (PS-18)
│   ├── io/
│   │   ├── ducklake.py         # fabrique de connecteur, DuckLakeTable (PS-06)
│   │   ├── datasets.py         # DuckLakeTableDataset, FreshnessRegistryDataset, ServingCatalogDataset (PS-07)
│   │   ├── freshness.py        # FreshnessRegistry, empreintes, décision (PS-10)
│   │   ├── registry_views.py   # DownloadRegistryView (PS-12.3)
│   │   ├── serving.py          # ServingCatalog : écriture transactionnelle du catalogue DuckLake `serving` (PS-29)
│   │   └── tracking.py         # build_tracker, publish_run_report, close_stale_runs, conventions de nommage (PS-19, PS-31)
│   ├── steps/
│   │   ├── downloads.py  baci.py  partners.py  network.py  coverage.py  reference.py
│   │   ├── synthesis.py  coherence.py  serving.py  maintenance.py
│   ├── pipelines/
│   │   ├── downloads/{__init__.py, pipeline.py, nodes.py}
│   │   ├── baci/… vulnerabilities/… synthesis/… serving/… maintenance/…
│   └── deploy/{render.py, templates/workflowtemplate.yaml.j2, templates/cronworkflow.yaml.j2}
├── tests/ (existants + tests/pipeline/, tests/deploy/, tests/serving/, tests/tracking/)
├── PIPELINE_ARCHITECTURE.md
└── PIPELINE_PROMPTS.md
```

### PS-02 — `pyproject.toml`

```toml
[project]
requires-python = ">=3.13"
dependencies = [
    # … existant …
    "kedro>=1.6,<2",
    "kedro-mlflow==2.0.3",
    "argo-kedro==0.1.41",
    "joblib>=1.4",
    "jinja2>=3.1",
]

[project.optional-dependencies]
tracking = ["mlflow>=3,<4", "psutil>=5.9"]   # aligné sur kedro-mlflow 2.x ; psutil : métriques système
reports = ["plotly>=5.24"]               # figures du rapport HTML de run (PS-31), incluses dans l'image
optimal-transport = ["jax>=0.4.30", "ott-jax>=0.4.6"]
viz = ["kedro-viz==12.4.0"]              # hors image de production
docs = ["mkdocs-material>=9.5", "kedro-viz==12.4.0"]

[dependency-groups]
dev = [
    # … existant …
    "duckdb-engine",                     # tests : lecture du catalogue `serving` comme Superset (SQLAlchemy)
]

[project.scripts]
# phase 0 → phase 3 : comtrade-script, eurostat-script, baci-hs-script,
# vulnerabilities-eurostat-script, vulnerabilities-network-script,
# vulnerabilities-synthesis-script, vulnerabilities-coherence-script,
# serving-script (K-03b) ; supprimés : test-baci-script (C-18), baci-script (doublon
# de baci-hs-script), puis TOUS en K-18 (PD-02)

[tool.kedro]
package_name = "kedro_pipeline"
project_name = "trade-analysis"
kedro_init_version = "1.6.0"
source_dir = "."

[tool.hatch.build.targets.wheel]
packages = ["macroforecast", "scripts", "kedro_pipeline"]   # "scripts" retiré en K-18
```

Remarques : l'ancien extra `dashboards` devient `reports` (révision 2 : rapport HTML de
run, PD-13) ; plotly n'est importé que par `macroforecast/tracking/figures.py`, de façon
paresseuse (sans l'extra, le rapport est produit sans figures). `psutil` est requis par
les métriques système de MLflow. Plus aucun usage direct de `psycopg2` pour la
restitution (PD-21). `kedro-viz` n'est pas dans l'image (inutile à l'exécution).

### PS-03 — `kedro_pipeline/settings.py`

```python
"""Kedro project settings for the trade vulnerability pipeline."""
# Importation des modules
from kedro.config import OmegaConfigLoader

from kedro_pipeline.hooks import TradeRunHooks

# Source de configuration : dossier existant `config/` (et non `conf/`)
CONF_SOURCE = "config"

# Chargeur de configuration et environnements
CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "merge_strategy": {"parameters": "soft"},
    "config_patterns": {
        "argo": ["argo*", "argo*/**"],
        "mlflow": ["mlflow*", "mlflow*/**"],
    },
}

# Hooks projet : les hooks kedro-mlflow et argo-kedro sont auto-enregistrés par entry points
HOOKS = (TradeRunHooks(),)
```

Remarques :
- `soft` permet à `config/demo/parameters_eurostat.yml` de ne surcharger que quelques clés
  du bloc `eurostat` ;
- les entry points de `kedro-mlflow` et `argo-kedro` enregistrent leurs hooks
  automatiquement : ne pas les dupliquer dans `HOOKS`.

### PS-04 — Paramètres

#### PS-04.1 `config/base/parameters_runtime.yml`

```yaml
runtime:
  # Première année de l'analyse, PAR SOURCE (PD-08), référencée par interpolation
  # dans les autres fichiers : Comext existe depuis 1988 ; la couverture déclarative
  # Comtrade en SH est jugée satisfaisante à partir de 1994 (Gaulier & Zignago 2010)
  ANALYSIS_START_YEAR:
    eurostat: 1988
    comtrade: 1994
  # Millésimes du Système harmonisé et année d'entrée en vigueur (PD-20). Source
  # unique pour les cibles BACI, les métriques partenaires historiques et la fonction
  # vintage_in_force(year). HS1992 (H0) est en vigueur depuis 1988.
  NOMENCLATURES:
    HS:
      HS1992: 1988
      HS1996: 1996
      HS2002: 2002
      HS2007: 2007
      HS2012: 2012
      HS2017: 2017
      HS2022: 2022
  # Jour de la cadence hebdomadaire (0 = lundi … 6 = dimanche), partagé par BACI,
  # la synthèse, la cohérence et la maintenance hebdomadaire (PD-23)
  WEEKLY_DAY: 5
  # Parallélisme intra-pod : null → NUM_CPU (injecté par Argo) ou os.cpu_count()
  N_JOBS: null
  # Forçage ponctuel (PS-11) : chaînes séparées par des virgules, vides par défaut
  FORCE_STEPS: ""        # ex. "partners,synthesis" ou "all"
  FORCE_METRICS: ""      # ex. "HHI,CDI2"
  FORCE_METHODS: ""      # ex. "critic_sum"
  FORCE_SCOPE:
    REPORTERS: ""        # ex. "FR,DE"
    PRODUCTS: ""         # préfixes acceptés : "28,8541"
    PERIODS: ""          # ex. "2020,2021"
    VINTAGES: ""         # ex. "HS2017"
```

#### PS-04.2 Extrait `config/base/parameters_comtrade.yml`

```yaml
comtrade:
  DATAFLOW: "C_A_HS"
  code_groups:
    majors_m49: &majors_m49 ["251", "276", "380", "724", "528"]
  parameters:
    C_A_HS: &params_c_a_hs
      products_step: 10
      # Plafond de requêtes (null en production ET en demo ; remplace queries[:10], C-01)
      max_queries: null
      # Ordre de la liste de requêtes (PS-12.1) : boucle externe sur les années
      # ("desc" = récentes d'abord), boucle interne sur les lots de produits dans
      # l'ordre naturel des codes. Pas de liste de produits prioritaires (PD-06).
      periods_order: "desc"
  fixed_dims:
    C_A_HS: &fixed_dims_c_a_hs
      frequency: annual
      flows: ["M", "X"]
      type_code: "C"
      classification: "HS"
  split_filters:
    C_A_HS:
      periods:
        start: ${runtime.ANALYSIS_START_YEAR.comtrade}
        end: null               # null → dernière année publiée (client)
      reporters: {include: null, include_regex: null, exclude: null, exclude_regex: null}
      products:
        include: null
        include_regex: '^\d{6}$'   # HS6 uniquement (PD-07)
        exclude: ["999999"]
        exclude_regex: null
  DOWNLOADS:
    DBNAME: "comtrade"
    CATALOG_ALIAS: "comtrade"
    C_A_HS:
      BUCKET: "qbollietdgddi"
      PATHS:
        DATA_PATH: "trade/datasets/comtrade"
        STRUCTURES_PATH: "trade/datasets/structures/comtrade.json"
        LAST_DOWNLOAD_PATH: "trade/datasets/last_downloads/comtrade.json"
      N_LAST_OBSERVATIONS: 10
      MAX_RUNTIME: {WEEKS: 0, DAYS: 0, HOURS: 10, MINUTES: 0, SECONDS: 0}
      COVERAGE:
        EXPECTED_FULL_HISTORY_REPORTERS: []   # PS-13
  TRACKING:
    LOG_ARTIFACTS: true
```

> L'interpolation `${runtime.ANALYSIS_START_YEAR.comtrade}` entre fichiers de paramètres
> est supportée par `OmegaConfigLoader`, qui résout après fusion. **À vérifier dans
> K-10** (sinon : `globals.yml` + résolveur `${globals:…}`).

Extrait `parameters_eurostat.yml` :

```yaml
eurostat:
  DATAFLOW: "DS-045409"
  parameters:
    DS-045409:
      products_step: 1          # > 1 après mesure (PR-03)
      max_queries: null
      # Fenêtres de périodes optionnelles (PD-07) : null → toutes les années en une
      # requête ; sinon liste ordonnée [[start, end|null], …] transmise par
      # startPeriod/endPeriod, la fenêtre devenant la boucle externe
      period_windows: null
  fixed_dims:
    DS-045409:
      freq: "A"
      partner: "*"
      flow: ["1", "2"]
      indicators: [QUANTITY_IN_100KG, VALUE_IN_EUROS]
      startPeriod: ${runtime.ANALYSIS_START_YEAR.eurostat}   # si accepté (PQ-15)
  split_filters:
    DS-045409:
      reporter:
        include: [*eu27, "EU27_2020"]   # États membres + Union (PD-21, PQ-17)
```

Extrait `parameters_baci.yml` :

```yaml
baci:
  COMPLETENESS: {MIN_SHARE: 1.0}
  # Cadence de réestimation d'un millésime (PD-22, PS-14.6)
  REFRESH:
    MIN_INTERVAL_DAYS: 7          # révisions amont : au plus une passe par semaine
    ON_NEW_COMPLETE_YEAR: true    # nouvelle année complète : passe immédiate
  # Mémoire : lignes maximales par tranche ; au-delà, l'année est découpée par blocs
  # de chapitres SH2 (PS-14.5)
  MAX_ROWS_PER_CHUNK: 20000000
  WORK_PATH: "trade/work/baci"    # Parquet de travail des flux miroirs (S3)
  CLASSIFICATIONS:
    TARGETS:                       # START_YEAR = max(entrée en vigueur, ANALYSIS_START_YEAR.comtrade)
      HS2022: {START_YEAR: 2022, RESULT_SCHEMA: "baci_hs2022"}
      HS2017: {START_YEAR: 2017, RESULT_SCHEMA: "baci_hs2017"}
      HS2012: {START_YEAR: 2012, RESULT_SCHEMA: "baci_hs2012"}
      HS2007: {START_YEAR: 2007, RESULT_SCHEMA: "baci_hs2007"}
      HS2002: {START_YEAR: 2002, RESULT_SCHEMA: "baci_hs2002"}
      HS1996: {START_YEAR: 1996, RESULT_SCHEMA: "baci_hs1996"}
      HS1992: {START_YEAR: ${runtime.ANALYSIS_START_YEAR.comtrade}, RESULT_SCHEMA: "baci_hs1992"}
```

#### PS-04.3 Extraits `parameters_vulnerabilities.yml` et `parameters_synthesis.yml`

```yaml
vulnerabilities:
  FLOWS: ["import", "export"]    # PD-09 (PQ-06 tranchée)
  FLOW_CODES: {import: 1, export: 2}
  # Millésimes SH pour lesquels les métriques historiques sont calculées (PD-20) :
  # "all" → tous ceux de runtime.NOMENCLATURES.HS ; liste → sous-ensemble ; [] → en
  # vigueur seulement
  VINTAGES: "all"
  DATAFLOW: "DS-045409"
  VULNERABILITIES: { … inchangé … }
  STATE:
    PATH_TEMPLATE: "trade/state/vulnerabilities/partners/{classification}/{reporter}.json"   # PS-10
    BUCKET: "qbollietdgddi"
  PARAMETERS: { … inchangé … }
  NETWORK_VULNERABILITIES:
    STATE:
      PATH_TEMPLATE: "trade/state/vulnerabilities/network/{vintage}.json"
    N_JOBS: ${runtime.N_JOBS}
    # … reste inchangé …

synthesis:
  SYNTHESIS:
    FLOWS: ${vulnerabilities.FLOWS}
    # "in_force" (défaut) : contextes de la nomenclature en vigueur ; "all" : aussi
    # les millésimes historiques (classification dans context_columns)
    VINTAGES: "in_force"
    RECENT_PERIODS: 10
    MAX_CONTEXTS_PER_RUN: null     # jamais de budget en régime nominal (PD-12)
    WRITE_BATCH_CONTEXTS: 8
    N_JOBS: ${runtime.N_JOBS}
    STATE:
      PATH_TEMPLATE: "trade/state/synthesis/scores/{period}.json"
    SOURCES:
      - {SCHEMA: "indicators", ALIAS: "p", COLUMNS: ["HHI", "CDI2", "CDI3"]}
      - SCHEMA: "network_indicators"
        ALIAS: "n"
        COLUMNS: ["EXPORT_HHI", "CENTRALITY_RISK", "CLUSTERING_W"]
        JOIN:
          "ON":
            - 'substr(p."product", 1, 6) = n."product"'
            - 'CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER) = n."year"'
            - 'n."classification" = p."hs_vintage"'      # PD-20 (remplace HS2022 en dur)
    FILTERS:
      # Les prédicats sur le flux et sur in_force sont GÉNÉRÉS depuis FLOWS et VINTAGES
      WHERE: 'p."indicators" = ''VALUE_IN_EUROS'' AND p."freq" = ''A'' AND p."reporter" <> ''EU27_2020'''
      LAST_N_PERIODS: null
    PARAMETERS:
      context_columns: ["classification", "freq", "flow", "indicators", "TIME_PERIOD"]
      # … reste inchangé …
  COHERENCE:
    STATE:
      PATH_TEMPLATE: "trade/state/synthesis/coherence/{period}.json"
    # … inchangé …
```

#### PS-04.4 Environnement `demo` (présentation)

```yaml
# config/demo/parameters_comtrade.yml
comtrade:
  split_filters:
    C_A_HS:
      periods: {start: 2015, end: null}
      products:
        include_regex: '^(2805|2846|8105|8112|2844|3004|8541|8542|8507)\d{2}$'

# config/demo/parameters_eurostat.yml
eurostat:
  split_filters:
    DS-045409:
      product:
        include_regex: '^(2805|2846|8105|8112|2844|3004|8541|8542|8507)\d{2,4}$'

# config/demo/parameters_baci.yml
baci:
  COMPLETENESS: {MIN_SHARE: 1.0}
  REFRESH: {MIN_INTERVAL_DAYS: 0, ON_NEW_COMPLETE_YEAR: true}   # démonstration : passe à chaque exécution
  CLASSIFICATIONS:
    TARGETS:
      HS2017: {START_YEAR: 2017, RESULT_SCHEMA: "demo_baci_hs2017"}

# config/demo/parameters_vulnerabilities.yml
vulnerabilities:
  VINTAGES: ["HS2017"]           # une seule série historique pour la démonstration

# config/demo/parameters_synthesis.yml
synthesis:
  SYNTHESIS:
    PARAMETERS:
      min_group_size: 10
```

> La liste de produits ci-dessus est un **exemple** (terres rares, cobalt, gallium/germanium,
> uranium, médicaments, semi-conducteurs, batteries : ~100 codes SH6), à remplacer par
> la sélection de l'utilisateur (PQ-07). Le `demo` **ne diffère de `base` que par le
> périmètre** (codes, années, millésimes) : ni plafond de requêtes, ni ordre de
> téléchargement différent (PD-06). Attention : un BACI sur un sous-ensemble de produits
> n'est **pas** le BACI complet (la qualité des déclarants est estimée sur tous les
> produits). Les résultats `demo` sont écrits dans des schémas préfixés `demo_` et
> étiquetés `is_provisional`.

> **Données fictives (2026-09-21, PD-24).** En phase 0, le profil `demo` peut être alimenté par
> un monde simulé (`config/profiles/demo/synthetic.yaml`, paramètres `comtrade-mode` /
> `eurostat-mode` du workflow de transition). Ces données ne sont écrites que dans les
> catalogues `demo_*` et sont **entièrement retirées** avant la production (K-17b, K-18) ;
> elles ne figurent pas dans l'environnement Kedro `config/demo/`.

### PS-05 — `credentials.yml` et variables d'environnement

```yaml
# config/base/credentials.yml — versionné : ne contient QUE des références oc.env
ducklake_postgres:
  host: ${oc.env:PGHOST,""}
  port: ${oc.env:PGPORT,"5432"}
  user: ${oc.env:PGUSER,""}
  password: ${oc.env:PGPASSWORD,""}
  admin_dbname: ${oc.env:PGDATABASE,"postgres"}
  admin_user: ${oc.env:PGADMINUSER,"postgres"}
  admin_password: ${oc.env:PGPASSWORD,""}

s3:
  endpoint: ${oc.env:AWS_S3_ENDPOINT,"minio.lab.sspcloud.fr"}
  access_key_id: ${oc.env:AWS_ACCESS_KEY_ID,""}
  secret_access_key: ${oc.env:AWS_SECRET_ACCESS_KEY,""}
  session_token: ${oc.env:AWS_SESSION_TOKEN,null}   # optionnel (C-03)

comtrade_api:
  subscription_key: ${oc.env:COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY,""}

# Pas de bloc de service dédié (révision 2) : le catalogue DuckLake `serving` est écrit
# avec `ducklake_postgres`, comme les autres catalogues (PD-21).
```

`OmegaConfigLoader` n'autorise `oc.env` que dans `credentials*` : c'est voulu, les
secrets ne transitent donc jamais par les paramètres (ni par les paramètres journalisés
dans MLflow). Les nœuds obtiennent les identifiants **via les datasets** (argument
`credentials:` du catalogue), jamais en paramètre.

Une valeur vide pour `host` ne provoque d'erreur qu'**à la connexion**, avec un message
explicite (`"PGHOST is not set: …"`) : `kedro viz build` passe sans secrets.

### PS-06 — Fabrique de connecteur et poignée de table (`kedro_pipeline/io/ducklake.py`)

```python
@dataclass(frozen=True)
class DuckLakeLocation:
    """Where a DuckLake schema lives (catalog identity + data path)."""
    dbname: str            # base PostgreSQL du catalogue (ex. "comtrade")
    catalog_alias: str     # alias ATTACH (ex. "comtrade")
    schema: str            # schéma assaini par _schema_name (ex. "C_A_HS")
    bucket: str
    data_path: str         # préfixe S3 des fichiers Parquet
    table: str = "fact_table"

def build_connector(location: DuckLakeLocation, pg: Mapping[str, Any],
                    s3: Mapping[str, Any]) -> DuckLakeConnector:
    """Build (never connect) the DuckLake connector — single replacement of the 9 copies (C-04)."""

class DuckLakeTable:
    """Lazy handle on a DuckLake fact table; opens connections on demand."""
    def __init__(self, location: DuckLakeLocation, pg: Mapping, s3: Mapping) -> None: ...
    @contextmanager
    def connect(self) -> Iterator[duckdb.DuckDBPyConnection]: ...
    def exists(self) -> bool: ...
    def query(self, sql: str, params: Sequence[Any] | None = None) -> pd.DataFrame: ...
    def add_missing_columns(self, df: pd.DataFrame) -> list[str]: ...   # PD-11
    def upsert(self, df: pd.DataFrame, primary_keys: Sequence[str], *,
               conn: duckdb.DuckDBPyConnection | None = None) -> bool: ...
    @property
    def qualified_name(self) -> str: ...   # '"alias"."schema"."fact_table"'
```

Exemple d'usage dans une étape :

```python
with baci_hs2017.connect() as conn:
    df_slice = comtrade_raw.query(
        f"SELECT {cols} FROM {comtrade_raw.qualified_name} WHERE CAST(\"refYear\" AS INT) = ?",
        params=[year],
    )  # lecture poussée en SQL, jamais la table entière (C-06)
    baci_hs2017.upsert(df_reconciled, primary_keys, conn=conn)
```

### PS-07 — Catalogue Kedro

```yaml
# config/base/catalog.yml
_pg: &pg {type: kedro_pipeline.io.datasets.DuckLakeTableDataset, credentials: ducklake_postgres}

eurostat.comext:
  <<: *pg
  location: {dbname: eurostat, catalog_alias: eurostat, schema: DS_045409,
             bucket: qbollietdgddi, data_path: trade/datasets/comext}

comtrade.tariffline:
  <<: *pg
  location: {dbname: comtrade, catalog_alias: comtrade, schema: C_A_HS,
             bucket: qbollietdgddi, data_path: trade/datasets/comtrade}

"baci.{vintage}":                      # dataset factory Kedro : baci.hs2017, baci.hs2022…
  <<: *pg
  location: {dbname: comtrade, catalog_alias: comtrade, schema: "baci_{vintage}",
             bucket: qbollietdgddi, data_path: trade/datasets/comtrade}

baci.scope:                            # sortie de prepare_baci : années éligibles par millésime
  type: kedro_datasets.json.JSONDataset   # ou dataset maison S3 via statflows
  filepath: s3://qbollietdgddi/trade/state/baci/scope.json

vulnerabilities.partners:
  <<: *pg
  location: {dbname: vulnerabilities, catalog_alias: vulnerabilities, schema: indicators,
             bucket: qbollietdgddi, data_path: trade/datasets/vulnerabilities/}

vulnerabilities.network:  { <<: *pg, location: { …, schema: network_indicators, … } }
synthesis.scores:         { <<: *pg, location: { …, schema: synthesis, … } }
synthesis.diagnostics:    { <<: *pg, location: { …, schema: synthesis_diagnostics, … } }

"reference.{source}":                  # référentiels (libellés, concordances), PS-28.4
  <<: *pg
  location: {dbname: "{source}", catalog_alias: "{source}", schema: reference, …}

"state.{step}":
  type: kedro_pipeline.io.datasets.FreshnessRegistryDataset
  credentials: s3
  path_template: "${…}"                # renseigné via paramètres (PS-10.1)

serving.tables:                        # poignée du catalogue DuckLake `serving` (PD-21, PS-29)
  type: kedro_pipeline.io.datasets.ServingCatalogDataset
  credentials: ducklake_postgres
  location: {dbname: serving, catalog_alias: serving, schema: dashboard,   # demo_dashboard dans config/demo/catalog.yml
             bucket: qbollietdgddi, data_path: trade/datasets/serving/}
  sources: [eurostat, comtrade, vulnerabilities]   # catalogues attachés en lecture pour les requêtes

"mlflow.metrics.{node}":
  type: kedro_mlflow.io.metrics.MlflowMetricsHistoryDataset
"mlflow.artifacts.{node}":
  type: kedro_mlflow.io.artifacts.MlflowArtifactDataset
  dataset: {type: kedro_datasets.pandas.CSVDataset, filepath: "data/08_reporting/{node}.csv"}
```

> Les noms de schémas du catalogue doivent rester **identiques** aux `RESULT_SCHEMA` des
> paramètres. K-10 ajoute un test qui vérifie cette cohérence et fait échouer la CI en cas
> d'écart (le doublon est le prix de la lisibilité dans kedro-viz). La syntaxe exacte des
> dataset factories et de `kedro_mlflow` est à vérifier sur les versions épinglées.

### PS-08 — API des fonctions d'étape (`kedro_pipeline/steps/`)

Toutes les étapes renvoient un **`StepResult`** :

```python
@dataclass
class StepResult:
    """Outcome of one pipeline step, consumed by nodes (metrics/artifacts) and scripts (logs)."""
    step: str
    n_units_planned: int
    n_units_succeeded: int
    failures: dict[str, str]                 # unité → message (tronqué)
    metrics: dict[str, float]                # déjà préfixées « <famille>/… »
    artifacts: dict[str, pd.DataFrame]       # chemin d'artefact → table
    tags: dict[str, str]
    def raise_if_failed(self) -> None: ...   # RuntimeError si failures (après tentative de tout)
```

Signatures cibles :

```python
def run_download(client_factory: Callable[[], Any], table: DuckLakeTable, *, source: str,
                 params: Mapping[str, Any], tracker: RunTracker) -> StepResult
def audit_coverage(table: DuckLakeTable, registry_path: str, *, source: str,
                   params: Mapping[str, Any]) -> StepResult
def prepare_baci(comtrade: DuckLakeTable, *, params: Mapping[str, Any],
                 runtime: Mapping[str, Any]) -> BaciScope
def run_baci_vintage(vintage: str, scope: BaciScope, comtrade: DuckLakeTable,
                     result: DuckLakeTable, state: FreshnessRegistry, *,
                     params: Mapping[str, Any], runtime: Mapping[str, Any],
                     tracker: RunTracker) -> StepResult
def run_partner_vulnerabilities(source: DuckLakeTable, result: DuckLakeTable,
                                state: FreshnessRegistry, download_registry: DownloadRegistryView, *,
                                params: Mapping, runtime: Mapping, tracker: RunTracker) -> StepResult
def run_network_vulnerabilities(baci: Mapping[str, DuckLakeTable], result: DuckLakeTable,
                                state: FreshnessRegistry, baci_state: FreshnessRegistry, *,
                                params: Mapping, runtime: Mapping, tracker: RunTracker) -> StepResult
def run_synthesis(catalog: DuckLakeTable, scores: DuckLakeTable, diagnostics: DuckLakeTable,
                  state: FreshnessRegistry, upstream: Sequence[FreshnessRegistry], *,
                  params: Mapping, runtime: Mapping, tracker: RunTracker) -> StepResult
def run_coherence(catalog: DuckLakeTable, scores: DuckLakeTable, diagnostics: DuckLakeTable,
                  state: FreshnessRegistry, synthesis_state: FreshnessRegistry, *,
                  params: Mapping, runtime: Mapping, tracker: RunTracker) -> StepResult
def publish_reference(codelists: Mapping[str, pd.DataFrame], reference: DuckLakeTable, *,
                      source: str, params: Mapping) -> StepResult              # PS-28.4
def publish_serving(sources: Mapping[str, DuckLakeTable], serving: ServingCatalog, *,
                    params: Mapping, runtime: Mapping, tracker: RunTracker) -> StepResult   # PS-29
def run_maintenance(tables: Sequence[DuckLakeTable], *, params: Mapping,
                    tracker: RunTracker) -> StepResult
```

`run_baci_vintage` orchestre les passes de PS-14 (préparation des tranches, passes
d'accumulation, résolutions, passe d'écriture) et n'appelle plus `run_baci` en bloc ;
`run_partner_vulnerabilities` boucle sur les millésimes demandés (en vigueur, puis
historiques avec conversion, PD-20).

Invariants communs (hérités des scripts, à conserver) :
1. l'instant de référence est capturé **avant** le calcul ;
2. le registre n'avance **qu'après** une écriture réussie, et seulement pour les unités
   réussies ;
3. l'échec d'une unité n'interrompt pas les autres ; `raise_if_failed()` est appelé par le
   nœud **après** la persistance du registre, la journalisation et la publication du
   rapport de run (PS-31) : un run en échec porte donc lui aussi son rapport ;
4. aucune lecture de variable d'environnement, aucun chemin YAML.

### PS-09 — Pipelines et nœuds

| Pipeline | Nœud Kedro (nom) | Entrées | Sorties | Tags | `machine_type` |
|---|---|---|---|---|---|
| `downloads` | `download_eurostat` *(FusedPipeline avec `audit_coverage_eurostat` et `publish_reference_eurostat`)* | `params:eurostat`, `params:runtime` | `eurostat.comext`, `reference.eurostat`, `mlflow.metrics.download_eurostat`, `mlflow.artifacts.coverage_eurostat` | `experiment:trade-01-downloads`, `cadence:daily` | `io-small` |
| `downloads` | `download_comtrade` *(Fused avec `audit_coverage_comtrade` et `publish_reference_comtrade`)* | `params:comtrade`, `params:runtime` | `comtrade.tariffline`, `reference.comtrade`, metrics, artifacts | idem | `io-small` |
| `baci` | `prepare_baci` | `comtrade.tariffline`, `params:baci`, `params:comtrade`, `params:runtime` | `baci.scope`, `baci.concordances`, `reference.comtrade` (table `hs_concordance`) | `experiment:trade-02-baci`, `cadence:weekly` | `compute-medium` |
| `baci` | `process_baci_<vintage>` (un par `CLASSIFICATIONS.TARGETS`) | `baci.scope`, `baci.concordances`, `comtrade.tariffline`, `state.baci_<vintage>`, `params:baci`, `params:runtime` | `baci.<vintage>`, `state.baci_<vintage>`, metrics, artifacts | idem | `baci-large` |
| `vulnerabilities` | `compute_partner_vulnerabilities` | `eurostat.comext`, `baci.concordances`, `state.partners`, `params:eurostat`, `params:vulnerabilities`, `params:runtime` | `vulnerabilities.partners`, `state.partners`, metrics, artifacts | `experiment:trade-03-vulnerabilities`, `cadence:daily` | `compute-medium` |
| `vulnerabilities` | `compute_network_vulnerabilities` | `baci.<vintage>` (tous), `state.baci_*`, `state.network`, `params:vulnerabilities`, `params:runtime` | `vulnerabilities.network`, `state.network`, metrics, artifacts | `experiment:trade-03-vulnerabilities`, `cadence:weekly` | `compute-medium` |
| `synthesis` | `compute_synthetic_scores` | `vulnerabilities.partners`, `vulnerabilities.network`, `state.partners`, `state.network`, `state.synthesis`, `params:synthesis`, `params:runtime` | `synthesis.scores`, `state.synthesis`, metrics, artifacts | idem, `cadence:weekly` | `synthesis-cpu` |
| `synthesis` | `compute_synthesis_coherence` | `synthesis.scores`, `vulnerabilities.partners`, `vulnerabilities.network`, `state.synthesis`, `state.coherence`, `params:synthesis`, `params:runtime` | `synthesis.diagnostics`, `state.coherence`, metrics, artifacts | idem, `cadence:weekly` | `synthesis-cpu` |
| `serving` | `publish_serving` | `vulnerabilities.partners`, `vulnerabilities.network`, `synthesis.scores`, `synthesis.diagnostics`, `reference.*`, `eurostat.comext`, `params:serving`, `params:runtime` | `serving.tables` (catalogue DuckLake), metrics | `experiment:trade-04-serving`, `cadence:daily`, `cadence:weekly`, `mutex:trade-serving` | `compute-medium` |
| `maintenance` | `maintain_ducklake` | `params:maintenance`, `params:tracking` *(aucune entrée de données : exécuté en `onExit` ; clôt aussi les runs orphelins, PD-16.6)* | `mlflow.metrics.maintain_ducklake` | `experiment:trade-00-maintenance`, `onexit`, `mutex:trade-maintenance` | `io-small` |

Dans Kedro, `publish_serving` dépend de **toutes** les tables de résultats ; dans le
point d'entrée `daily`, seules les tâches `cadence:daily` sont instanciées et le rendu
Argo ne conserve que les dépendances **internes** au point d'entrée (les tables
hebdomadaires sont lues telles qu'elles sont). Les tags `cadence:*` et `mutex:*` sont
lus par le rendu (PD-14).

- `__default__` = somme de tous les pipelines (via `sum(pipelines)`, **pas** un
  `FusedPipeline` englobant : limitation argo-kedro) ;
- les nœuds `process_baci_<vintage>` sont générés dans `create_pipeline()` à partir de
  `baci.CLASSIFICATIONS.TARGETS`, lu par `kedro_pipeline.config.load_parameters(env)`
  avec l'environnement `KEDRO_ENV` ;
- le nœud `compute_synthesis_coherence` prend `synthesis.scores` en entrée : c'est ce qui
  fonde la dépendance synthèse → cohérence dans Argo.

### PS-10 — Registres de fraîcheur v2

#### PS-10.1 Format d'un fragment

Fichier `s3://qbollietdgddi/trade/state/vulnerabilities/partners/FR.json` :

```json
{
  "schema_version": 2,
  "step": "partners",
  "fragment": "FR",
  "entries": {
    "FR|280530": {
      "unit": {"reporter": "FR", "product": "280530"},
      "last_computed": "2026-09-16T03:12:44+00:00",
      "upstream_watermark": "2026-09-16T01:47:02+00:00",
      "fingerprints": {"HHI": "a3f9c1e04b7d2e11", "CDI2": "9be0417c55aa1f02", "CDI3": "0c77d1f3e9a24b58"},
      "reason": "new_data",
      "n_rows": 68
    }
  }
}
```

Maille et fragment par étape :

| Étape | Unité | Fragment | Watermark amont |
|---|---|---|---|
| BACI | **millésime** (entrée enrichie de `fit_id` et `years_written`, PD-10) | millésime | max `last_download` des requêtes Comtrade des années du périmètre |
| Partenaires | classification × reporter × produit | classification / reporter | en vigueur : `last_download` de la requête couvrant le couple ; historique : max des `last_download` des codes sources (PD-20.6) |
| Réseau | millésime | millésime | `last_computed` du millésime BACI |
| Synthèse | contexte `(classification, freq, flow, indicators, TIME_PERIOD)` | période | max des watermarks partenaires et réseau pertinents |
| Cohérence | contexte | période | `last_computed` synthèse du contexte |
| Publication de service | table de service (× année en mode `by_year`) | table | max `last_computed` des étapes sources de la table |

#### PS-10.2 Empreinte méthodologique

```python
def fingerprint(name: str, version: str, params: Mapping[str, Any]) -> str:
    """Stable 16-hex digest of a metric/method/step methodology.

    Examples:
        >>> fingerprint("HHI", "1", {"world_code": "WORLD"})
        '…'  # stable across runs and platforms
    """
    payload = json.dumps({"name": name, "version": version, "params": params},
                         sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
```

- **`version`** : attribut de classe `version: ClassVar[str] = "1"` ajouté à
  `VulnerabilityMetric`, `NetworkVulnerabilityMetric` et à chaque étape BACI. Pour les
  méthodes de synthèse, champ `version` (défaut `"1"`) dans l'entrée YAML de la méthode.
  **Une correction de formule = incrément de `version`** (runbook §5.3).
- **`params`** : paramètres qui influent sur le résultat de la métrique ou de la méthode
  (champs du dataclass de configuration **hors** options de journalisation : `artifact_*`,
  `drift_*`, `psi_n_bins`…, dont la liste d'exclusion est déclarée à côté du dataclass).
  Pour une méthode de synthèse : `kind`, `params`, `metrics`, `levels`, plus les champs de
  `SynthesisConfig` qui la concernent (`normalization`, `winsorize_quantile`,
  `polarities`, `min_group_size`, `metric_columns`).
- Pour BACI, l'empreinte est **unique par millésime** (toutes les étapes sont couplées).

#### PS-10.3 Décision

```python
def units_to_compute(planned: Iterable[Unit], registry: FreshnessRegistry,
                     upstream: Mapping[Unit, datetime], requested: Mapping[str, str],
                     force: ForceSpec) -> dict[Unit, UnitPlan]:
    """Return, per stale unit, the reason and the subset of metrics/methods to (re)compute.

    UnitPlan(reason, names) where names ⊆ requested:
      - 'first'       : unit absent from the registry → all names
      - 'forced'      : unit in force scope → force.names or all names
      - 'new_data'    : upstream[unit] > entry.upstream_watermark → all names
      - 'fingerprint' : names whose fingerprint differs or is missing → those names only
    Precedence: first > forced > new_data > fingerprint (a unit gets a single plan).
    """
```

- **Partenaires et réseau** (tables larges) : même si seule une métrique a changé, on
  recalcule **toutes** les métriques de l'unité (le coût est faible, et l'upsert porte
  sur des lignes entières). Le registre enregistre les nouvelles empreintes de toutes.
- **Synthèse** (table longue par méthode) : on ne recalcule **que** les méthodes de
  `names`, plus le consensus dès que `names` n'est pas vide.

### PS-11 — Forçage ponctuel (paramètres d'exécution)

| Besoin | Commande locale | Exécution Argo ponctuelle |
|---|---|---|
| Recalculer toute la synthèse | `kedro run --pipeline synthesis --params runtime.FORCE_STEPS=synthesis` | `argo submit --from workflowtemplate/trade-pipeline -p force-steps=synthesis` |
| Recalculer HHI partout | `--params runtime.FORCE_STEPS=partners,runtime.FORCE_METRICS=HHI` | `-p force-steps=partners -p force-metrics=HHI` |
| Recalculer une méthode | `--params runtime.FORCE_STEPS=synthesis,runtime.FORCE_METHODS=critic_sum` | `-p force-steps=synthesis -p force-methods=critic_sum` |
| Forcer BACI HS2017 sur 2020-2021 | `--params runtime.FORCE_STEPS=baci,runtime.FORCE_SCOPE.VINTAGES=HS2017,runtime.FORCE_SCOPE.PERIODS=2020,2021` | idem en paramètres |
| Tout recalculer | `--params runtime.FORCE_STEPS=all` | `-p force-steps=all` |

Règles :
- le forçage d'une étape **cascade** vers l'aval : les unités forcées sont inscrites avec
  `reason=forced`, et l'aval traite une raison `forced` amont comme un changement complet
  (PD-12) ;
- les valeurs sont des chaînes séparées par des virgules : pas de liste YAML dans
  `--params`, pour éviter les problèmes d'échappement dans Argo ;
- le `CronWorkflow` quotidien n'expose **jamais** de forçage (valeurs vides) ;
- toute exécution forcée pose le tag MLflow `forced=<steps>`.

> La syntaxe `--params` avec des virgules à l'intérieur d'une valeur doit être validée
> sur Kedro 1.6 (séparateur de paires). À défaut, utiliser `;` comme séparateur interne
> (`FORCE_SCOPE.PERIODS=2020;2021`). K-05 tranche et documente.

### PS-12 — Construction et ordonnancement des requêtes

#### PS-12.1 Comtrade

```python
def build_comtrade_queries(dims_codes, fixed_dims, split_filters, products_step,
                           periods, periods_order) -> list[ComtradeQueryRequest]:
    """One query per (period, product batch); period-major order, natural code order."""
```

Ordre de la liste retournée, exploité par le tri stable de `download_updates` (PD-06) :
1. **boucle externe** sur les années, selon `periods_order` (`desc` par défaut : années
   récentes d'abord) ;
2. **boucle interne** sur les lots de `products_step` codes, dans l'**ordre naturel des
   codes** (lots formés une fois, identiques pour toutes les années).

Il n'y a **pas** de liste de produits prioritaires : le périmètre est fixé par les
filtres `include`/`include_regex`/`exclude` (identiques en `demo` et en production dans
leur mécanisme, différents dans leur valeur).

Exemple : `products_step=2`, codes `[010121, 010129, 854140, 854150]`, années 2023-2024,
`periods_order="desc"` →
`[(2024,[010121,010129]), (2024,[854140,854150]), (2023,[010121,010129]), (2023,[854140,854150])]`.
Une année est ainsi **complète** (tous ses lots téléchargés) avant que l'année
suivante ne commence, ce que la porte de complétude BACI (PS-14.1) exploite.

#### PS-12.2 Eurostat

Une requête = (reporter × lot de `products_step` codes **d'un même chapitre HS2**),
toutes les années. Ordre : **boucle externe** sur les lots de produits (ordre naturel des
codes), **boucle interne** sur les reporters (ordre de la liste `reporter.include`) — un
produit est ainsi complet pour tous les reporters avant le suivant, ce qui est la maille
utile aux indicateurs partenaires (couple reporter × produit) et à leur comparaison
entre pays. Si `period_windows` est renseigné (PD-07), la fenêtre devient la boucle
externe. Tant que PR-03 n'est pas mesuré, `products_step: 1` en `base` (comportement
actuel).

#### PS-12.3 Lecture du registre de téléchargement par les étapes aval

`DownloadRegistryView` (dans `kedro_pipeline/io/freshness.py`) **éclate** chaque entrée
du registre `statflows` en unités :
- Eurostat : `dims.reporter` × chaque code de `dims.product` (chaîne ou liste, séparateurs
  `+` ou `,`) → `{(reporter, product): last_download}` ;
- Comtrade : chaque période de `params.periods` → `{year: [last_download de chaque lot]}`
  pour la porte de complétude (PS-14).

La vue passe par l'API de lecture de `statflows` (PS-27) et **ne suppose pas** le format
physique du registre (fichier unique ou fragments).

### PS-13 — Audit de couverture

Sorties publiées dans `trade-01-downloads` :

| Métrique | Définition |
|---|---|
| `coverage/queries_total` | nombre de requêtes planifiées |
| `coverage/queries_never_downloaded` | requêtes sans entrée de registre |
| `coverage/share_downloaded` | 1 − never / total |
| `coverage/min_period` | plus petite période présente dans la table |
| `coverage/reporters_below_start` | nombre de reporters de `EXPECTED_FULL_HISTORY_REPORTERS` dont `min(period) > ANALYSIS_START_YEAR.<source>` |
| `coverage/eta_days` | estimation naïve : `never / (requêtes traitées aujourd'hui)` |

Artefacts : `coverage/by_reporter.csv` (reporter, min_period, max_period, n_products,
n_rows) et `coverage/by_year.csv` (year, share_queries_downloaded). La lecture passe par
**une seule requête SQL agrégée** (`GROUP BY reporter`), jamais par un chargement de
table.

### PS-14 — BACI : porte de complétude, passes sur tranches annuelles et statistiques suffisantes

#### PS-14.1 Porte de complétude et périmètre

1. `prepare_baci` calcule, pour chaque année `y ≥ ANALYSIS_START_YEAR.comtrade`,
   `share(y) = (lots Comtrade de y téléchargés au moins une fois) / (lots planifiés de y)`,
   par `DownloadRegistryView`. Années éligibles : `share(y) ≥ COMPLETENESS.MIN_SHARE`.
2. Pour chaque millésime `V`, `scope[V] = {y éligible : y ≥ START_YEAR[V]}`.
   Sortie `baci.scope` :
   ```json
   {"HS2017": {"years": [2017, 2018, 2019, 2020, 2021, 2022, 2023],
               "provisional": false, "upstream_watermark": "2026-09-16T01:47:02+00:00"}}
   ```
3. `prepare_baci` télécharge aussi les tables de passage UNSD manquantes
   (`prepare_concordances`, écrivain unique du cache) et les publie dans
   `reference.hs_concordance` (PS-28.4).

#### PS-14.2 Analyse : quelles estimations mettent en commun plusieurs années

Lecture de `macroforecast/trade/processing/baci.py` (état au 2026-09-18), de la note
LaTeX et de Gaulier & Zignago (2010) :

| Étape (`run_baci`) | Objet estimé | Portée dans le code | Portée dans la méthodologie | Décomposable par tranche ? |
|---|---|---|---|---|
| Régime de valorisation (`infer_import_valuation_regime`) | part CIF par importateur (× année) | `country_year` : annuel ; `country` : toutes années | idem (choix de configuration) | oui : sommes de `cifvalue` et `fobvalue` par (importateur[, année]) |
| Conversion en tonnes (`TonnageConverter.fit`) | moyenne et écart-type des ratios par (produit, unité), filtres `n ≥ 10`, `σ < 2,5` | **toutes années** du millésime | « pour chaque produit », sans dimension temporelle | oui : `n`, `Σr`, `Σr²` par (produit, unité) |
| Médiane mondiale `UV^k` (`world_median_unit_values`) | médiane des valeurs unitaires (deux côtés empilés) par produit | **toutes années** | `UV^k` sans indice temporel (éq. 1 du papier) | non additive, mais calculable **en SQL** hors mémoire pandas (`median() GROUP BY product` sur les Parquet de travail) |
| Gravité (`CifGravityModel.fit`) | WLS `ln(UVm/UVx)` sur distance, contiguïté, enclavement, `ln UV^k`, **indicatrices d'année** ; retrait des observations influentes (Cook) puis second ajustement | **toutes années** (pooled) | « OLS on pooled data over the period » avec `time dummies` (papier §2.3.2) | oui : `X'WX`, `X'Wy`, `y'Wy`, `N` ; Cook en seconde passe |
| Fobisation (`Fobizer.transform`) | par flux (règles FAS, plancher) | par flux | par flux | oui (ligne à ligne) |
| Qualité (`ReportingQualityModel.fit`, `AbsorbingLS`) | ANOVA pondérée `RD ~ exportateur + importateur + année`, produit absorbé (within), erreurs-types **robustes** (défaut `cov_type="robust"` de `linearmodels`) | **toutes années** | éq. 5 du papier, `λ_t` effets d'année : pooled | oui : produits croisés globaux + sommes par produit ; « viande » robuste en seconde passe |
| Réconciliation (`MirrorReconciler`) | par flux, à `σ̂` donnés | par flux | par flux | oui |
| NES (`AreaNesReallocator`) | par (exportateur, produit, année) | par groupe | par groupe (appendice du papier) | oui, par tranche annuelle |
| Harmonisation HS (`HsHarmonizer`) | correspondance code → code, agrégation des mesures | ligne à ligne puis agrégation par clé (année incluse) | — | oui, par tranche annuelle |

Conclusion : **quatre** estimations sont groupées sur toutes les années (tonnage,
médiane `UV^k`, gravité, qualité). Aucune n'exige les lignes en mémoire : toutes se
réduisent à des **statistiques suffisantes additives** sur des partitions arbitraires
des observations (la médiane, non additive, est déléguée à DuckDB). Un traitement par
tranches est donc **exact** ; PQ-10 est tranchée : pas de fenêtres glissantes.

#### PS-14.3 Passes

Pour un millésime `V` et son périmètre `scope[V]`, `run_baci_vintage` enchaîne :

| Passe | Lecture | Travail par tranche | Accumulé | Résolution après la passe |
|---|---|---|---|---|
| **P0 préparation** | Comtrade, `WHERE year = y` (projection `required_columns` + classification) | harmonisation vers `V` ; `build_mirror_flows` ; écriture du Parquet de travail `WORK_PATH/<V>/mirror/year=<y>.parquet` (colonnes : clés, `v_x`, `v_m`, `q_x`, `q_m`, unités, `netwgt`, `cif/fob` par côté) ; flux NES écrits à part | régime : `Σ cif`, `Σ fob`, `n` par (importateur[, année]) ; tonnage : `n`, `Σr`, `Σr²` par (produit, unité source) | `df_regime` ; taux de conversion validés (`n ≥ min_mirror_flows`, `σ < max_conversion_std`, même estimateur d'écart-type que le code actuel) |
| **S1 médianes** | Parquet de travail, **en SQL** | — | — | `UV^k` = `median(uv)` par produit sur l'empilement des deux côtés, quantités converties par jointure avec la table des taux (petite) ; DuckDB calcule la médiane exacte hors mémoire pandas |
| **P1 gravité, 1er ajustement** | Parquet `year=y`, colonnes de l'échantillon | conversion en tonnes ; échantillon = flux miroirs complets à quantités > 0 ; design `x` (7 régresseurs + indicatrices d'année, colonnes fixées d'avance par `scope[V]`) ; `y = ln(UVm/UVx)` ; `w = min(Q)/max(Q)` ; lignes non finies retirées | `A = Σ w x xᵀ` (p×p), `b = Σ w x y`, `c = Σ w y²`, `N` | `β₁ = A⁻¹ b` ; `RSS₁ = c − βᵀ b` ; `s² = RSS₁/(N − p)` ; `A⁻¹` conservé |
| **P2 gravité, Cook** | idem | par observation : résidu blanchi `ẽ = √w (y − xβ₁)`, levier `h = w xᵀ A⁻¹ x`, `D = ẽ² h / (p s² (1−h)²)` ; conservation si `D < cook_factor / N` | `A`, `b`, `c`, `N` sur les observations **conservées** ; `n_cook_dropped` | `β₂` = ajustement final ; `r²`, coefficients → `GravityReport` |
| **P3 fobisation + qualité, 1er ajustement** | Parquet `year=y` | conversion en tonnes ; `τ̂ = exp(xβ₂) − 1` ; `Fobizer.transform` (règles FAS, plancher) ; pour `target ∈ {value, quantity}` : `RD = |ln(V_i/V_j)|`, poids `w = ln(v_x + v_m_fob)`, design `z` = indicatrices exportateur, importateur, année (référence retirée) ; groupe absorbé = produit | par cible : `G = Σ w z zᵀ`, `g = Σ w z RD`, `q = Σ w RD²`, `N` ; par produit `k` : `W_k = Σ w`, `s_k = Σ w z` (vecteur), `t_k = Σ w RD` | `G̃ = G − Σ_k s_k s_kᵀ / W_k`, `g̃ = g − Σ_k s_k t_k / W_k` ; `β_q = G̃⁻¹ g̃` ; moyennes de groupe `m_k = s_k / W_k`, `μ_k = t_k / W_k` conservées |
| **P4 qualité, covariance robuste** | idem | recalcul des mêmes `z`, `RD`, `w` ; démoyennage `z̃ = z − m_k`, `R̃D = RD − μ_k` ; résidu `e = R̃D − z̃ β_q` | « viande » `M = Σ (w e)² z̃ z̃ᵀ` (ou la forme exacte de `linearmodels` pour `cov_type="robust"` avec poids : à reproduire à l'identique, test à l'appui) | `Cov = G̃⁻¹ M G̃⁻¹` ; effets recentrés somme-nulle et erreurs-types de contraste (`_absorbed_anova_effects`) ; `σ̂` (éq. 12-13), plancher (`_sigma_floor`) |
| **P5 réconciliation + écriture** | Parquet `year=y` (+ NES de l'année) | conversion, fobisation (recalculées, déterministes), `MirrorReconciler.transform` avec `σ̂`, `AreaNesReallocator.transform`, nettoyage | compteurs des rapports (`MirrorReport`, `NesReport`, `FobisationReport`, sommes de `BaciReport`) | par année : transaction DuckLake `DELETE WHERE year = y` puis insertion (colonne `fit_id`) ; entrée de registre `years_written` mise à jour ; métriques MLflow avec `step = y` |

Six passes sur les Parquet de travail au lieu d'une sur un DataFrame géant ; chaque
passe est bornée en mémoire par **une tranche** et lit avec projection de colonnes
(DuckDB, `read_parquet`). Les passes P1/P2 et P3/P4 sont les deux lectures qu'exigent
respectivement la distance de Cook et la covariance robuste : c'est le prix exact de la
fidélité à l'implémentation actuelle.

#### PS-14.4 Formes des estimateurs (rappel, pour les tests d'équivalence)

- **WLS** : `β = (X'WX)⁻¹ X'Wy` ; tous les termes sont des sommes sur les observations,
  donc invariants par partition des observations en tranches.
- **Distance de Cook** sur le modèle blanchi (`OLSInfluence(sm.OLS(√w y, √w X))`,
  comme dans `CifGravityModel.fit`) : `D_i = ẽ_i² h_ii / (p · MSE · (1 − h_ii)²)` avec
  `MSE = RSS/(N − p)` ; elle ne dépend de l'échantillon complet que par `A⁻¹` et `MSE`,
  connus après P1.
- **Within à un facteur pondéré** : pour un groupe absorbé `k`, démoyenner par la
  moyenne pondérée du groupe, puis WLS sur les variables démoyennées ; les produits
  croisés démoyennés s'écrivent `Σ w z zᵀ − Σ_k s_k s_kᵀ / W_k` : additifs par tranche
  tant que `s_k` et `W_k` sont accumulés sur **toutes** les tranches (un produit s'étend
  sur toutes les années).
- **Dimensions** : gravité `p ≈ 7 + |scope[V]| − 1` (≤ 40) ; qualité `p ≈ 2 × ~230
  pays + |scope[V]|` (≤ 500) → `G` dense de 500², `s_k` : 5 000 × 500 flottants
  (20 Mo). Négligeable.
- **Tolérance** attendue entre monobloc et passes : `1e-8` relatif sur les
  coefficients, ensemble identique d'observations retirées par Cook, `σ̂` à `1e-8`.

#### PS-14.5 Tranches, mémoire et garde-fous

- Tranche par défaut = **une année**. Ordre de grandeur : 10 à 13 millions de flux
  miroirs pour une année récente, ~15 colonnes numériques → 1,5 à 2,5 Go en pandas.
  `machine_type: baci-large` (PS-20) le couvre avec marge.
- Si une année dépasse `MAX_ROWS_PER_CHUNK`, elle est **découpée par blocs de chapitres
  SH2** (`product BETWEEN … AND …`) : toutes les accumulations sont invariantes par
  partition, et les groupes de la réallocation NES (exportateur × produit × année)
  restent entiers. Aucun échec « OOM » silencieux : dépassement → découpage, puis
  échec explicite si un bloc d'un seul chapitre dépasse encore.
- Les Parquet de travail sont écrits sous `WORK_PATH/<V>/<fit_id>/…` et supprimés à la
  fin d'une passe réussie (paramètre `KEEP_WORK_FILES: false`) ; en cas d'échec, ils
  sont réutilisés par la reprise si `fit_id` est identique (même périmètre, même
  watermark), sinon régénérés.

#### PS-14.6 Fraîcheur et cadence du millésime

Une passe sur `V` est lancée (dans le point d'entrée `weekly`, ou à la main) si l'une
des conditions est vraie :
1. `V` n'a jamais été calculé, ou son entrée porte un `fit_id` dont `years_written ≠
   scope[V]` (passe interrompue) ;
2. `scope[V]` contient une **nouvelle année complète** (`REFRESH.ON_NEW_COMPLETE_YEAR`) ;
3. le watermark amont a bougé (révisions Comtrade) **et** `last_computed` remonte à plus
   de `REFRESH.MIN_INTERVAL_DAYS` ;
4. l'empreinte méthodologique du millésime a changé, ou `V` est dans la portée d'un
   forçage.

`fit_id = hash(V, scope[V], upstream_watermark, fingerprint)`. Le réseau (PS-10) lit
`last_computed` du millésime : toute passe BACI le rend périmé, ce qui est voulu (les
métriques réseau dépendent de toutes les années).

#### PS-14.7 Organisation du code (`macroforecast/`, sans I/O)

- `macroforecast/trade/processing/streaming.py` : accumulateurs réutilisables,
  convention sklearn `partial_fit(chunk) → self`, `finalize() → self` :
  `WelfordGroupStats` (tonnage, régimes), `WeightedLeastSquaresAccumulator` (`A`, `b`,
  `c`, `N` ; `solve()`), `CookFilter` (P2, à partir d'un ajustement P1),
  `AbsorbedWLSAccumulator` (P3/P4 : globaux + par groupe, `solve()`, `robust_cov(...)`).
- `baci.py` : `TonnageConverter`, `CifGravityModel`, `ReportingQualityModel` gagnent
  chacun un chemin `partial_fit`/`finalize` en plus de `fit` (qui reste et devient un
  simple `partial_fit(df) ; finalize()` : **une seule implémentation**, la version
  monobloc n'est que le cas d'une tranche unique). `run_baci` (monobloc) est conservé
  tel quel pour les tests et les petits jeux ; `run_baci_passes(chunks: Iterable[…],
  …)` orchestre les passes sur un itérateur de tranches fourni par l'appelant (aucune
  lecture de fichier dans `macroforecast/`).
- `kedro_pipeline/steps/baci.py` : fournit l'itérateur de tranches (requêtes DuckDB sur
  Comtrade puis sur les Parquet de travail), la médiane SQL (S1), l'écriture par année
  et le registre.

### PS-15 — Paramètre `FLOWS`

- `VulnerabilityMetric.supported_flows: ClassVar[frozenset[str]]` : `HHI`, `CDI2` et
  `CDI3` → `{"import", "export"}` (PD-09). Chaque classe est paramétrée par le sens :
  pour le flux `f`, on note `own(f)` le flux lui-même et `other(f)` l'autre sens ;
  `CDI2(f) = extra_UE(own) / monde(own)` et `CDI3(f) = extra_UE(own) / monde(other)`.
  À l'import, cela redonne exactement les définitions actuelles ; à l'export, les
  définitions retenues. `version` reste `"1"` pour l'import (valeurs inchangées) ;
- le runner filtre la grille sur `FLOW_CODES[f]` pour `f ∈ FLOWS` et met à `null` toute
  métrique non supportée pour un flux ;
- la synthèse génère `p."flow" IN (1, 2)` à partir de `FLOWS` et l'ajoute par conjonction
  à `FILTERS.WHERE` ; `build_source_query` reçoit un argument `flow_codes` optionnel (le
  test existant passe `None` et reste inchangé) ;
- MLflow : les métriques partenaires sont suffixées par flux
  (`partners/import/HHI/mean`, `partners/export/HHI/mean`).

### PS-16 — Évolution de schéma (délégation à `dt-ducklake-manager 0.3.1`)

```python
class DuckLakeTable:
    def upsert(self, df: pd.DataFrame, primary_keys: Sequence[str], *,
               allow_new_columns: bool = True, compact_after_update: bool = False,
               run_id: str | None = None, commit_message: str | None = None,
               conn: duckdb.DuckDBPyConnection | None = None) -> bool:
        """Create the schema on first write, else upsert through DatabaseUpdater.

        ``allow_new_columns`` lets columns of ``df`` absent from the fact table be
        added (``ALTER TABLE … ADD COLUMN`` + metadata row) before the upsert; columns
        are never dropped nor retyped. ``run_id``/``commit_message`` are recorded on
        the DuckLake snapshot.
        """
    def add_columns(self, df: pd.DataFrame, *, overwrite: bool = False) -> OperationReport:
        """Broadcast new value columns onto existing rows by primary key (migrations)."""
```

La poignée passe par `statflows.write_dataframe`, qui doit **transmettre** ces options
(PS-27, point 4) ; en attendant la nouvelle version de `statflows`, la poignée appelle
`DatabaseUpdater` directement pour la branche « table existante ».

Test obligatoire (catalogue DuckLake local) : table créée avec `HHI` → upsert d'un
DataFrame `HHI, NEW_METRIC` → la colonne existe, les anciennes lignes valent `NULL`, les
nouvelles portent la valeur ; puis upsert d'un DataFrame **sans** `NEW_METRIC` → les
valeurs existantes de `NEW_METRIC` sont **préservées** (sinon compléter le DataFrame avant
upsert, cf. PD-11) ; enfin `add_columns` d'une colonne `MIGRATED` sur la moitié des
clés → les autres lignes valent `NULL`, aucune ligne dupliquée.

### PS-17 — Synthèse incrémentale (algorithme)

```
entrées : registre synthèse S (fragments par période), registres amont P (partenaires) et N (réseau),
          méthodes demandées M (avec empreintes), FLOWS, VINTAGES, RECENT_PERIODS, budget B (null en nominal)
1. contextes_planifiés ← SELECT DISTINCT classification, freq, flow, indicators, TIME_PERIOD FROM grille
                          WHERE FILTERS.WHERE AND flow IN FLOWS
                            AND (VINTAGES = 'all' OR in_force)
2. changement_amont_complet ← ∃ entrée de P ou N plus récente que S.watermark_global
                               avec reason ∈ {fingerprint, forced}  OU  N a changé
3. pour chaque contexte c :
     si c ∉ S                                  → plan(c) = (first, M)
     sinon si c ∈ portée de forçage            → plan(c) = (forced, M_forcées ou M)
     sinon si changement_amont_complet         → plan(c) = (new_data, M)
     sinon si P a changé et période(c) ∈ RECENT_PERIODS dernières → plan(c) = (new_data, M)
     sinon m ← {méthodes dont l'empreinte diffère}; si m ≠ ∅ → plan(c) = (fingerprint, m ∪ consensus)
4. trier les contextes planifiés par période décroissante ; tronquer à B
5. joblib.Parallel(n_jobs) sur les contextes (lecture SQL du contexte dans le worker,
   run_synthesis restreint aux méthodes du plan) ; résultats collectés dans le processus parent
6. écriture par lots de WRITE_BATCH_CONTEXTS contextes (upsert scores + diagnostics fit),
   puis avancement des entrées de registre du lot
```

Le worker **n'écrit jamais** et ouvre sa propre connexion de lecture (les connexions
DuckDB ne se partagent pas entre processus). `run_synthesis` doit accepter un
sous-ensemble de méthodes (paramètre `methods: Sequence[str] | None`), ce qui ne change
pas les résultats des méthodes calculées : un test d'équivalence le vérifie.

### PS-18 — Contrat du parallélisme intra-pod

- `n_jobs = runtime.N_JOBS or int(os.environ.get("NUM_CPU", 0)) or os.cpu_count()`,
  résolu **dans le hook** `TradeRunHooks.before_node_run`, puis passé en paramètre (les
  étapes ne lisent pas l'environnement) ;
- backend `loky` (processus) pour le code Python/pandas ; `threading` interdit pour le
  CPU ;
- dans chaque worker : `OMP_NUM_THREADS=MKL_NUM_THREADS=OPENBLAS_NUM_THREADS=1`
  (sur-souscription) ; JAX : `XLA_PYTHON_CLIENT_PREALLOCATE=false` ;
- **un seul écrivain** par table (le processus parent) ;
- déterminisme : graine par unité dérivée de `random_state` et de la clé d'unité (pas de
  l'ordre d'exécution) ; un test vérifie l'égalité `n_jobs=1` / `n_jobs=2`.

### PS-19 — Journalisation MLflow

`config/base/mlflow.yml` (extrait) :

```yaml
server:
  mlflow_tracking_uri: ${oc.env:MLFLOW_TRACKING_URI, null}
tracking:
  disable_tracking:
    pipelines: []
  experiment:
    name: ${oc.env:MLFLOW_EXPERIMENT_NAME, trade-local}
    restore_if_deleted: true
  run:
    id: null
    name: ${oc.env:WORKFLOW_ID, local}
    nested: true
  params:
    dict_params: {flatten: true, recursive: true, sep: "."}
    long_params_strategy: truncate
```

> `oc.env` est interdit hors `credentials*` par défaut : `mlflow.yml` est lu par
> kedro-mlflow avec ses propres résolveurs. **À vérifier dans K-13** ; en cas de refus,
> déclarer `custom_resolvers={"oc.env": oc.env}` dans `CONFIG_LOADER_ARGS`.

Conventions :
- `TradeRunHooks.before_pipeline_run` pose les tags `workflow_id`, `git_sha` (variable
  `GIT_SHA` gravée dans l'image), `image_tag`, `kedro_env`, `node`, `forced` ;
- le nom du run est **`<nœud>-<WORKFLOW_ID>`** (surcharge du nom kedro-mlflow par le hook
  via `mlflow.set_tag("mlflow.runName", …)`) ;
- les nœuds renvoient `StepResult.metrics` vers `mlflow.metrics.<nœud>`
  (`MlflowMetricsHistoryDataset`, format `{nom: [{"value": v, "step": 0}]}`) et
  `StepResult.artifacts` vers des `MlflowArtifactDataset` ;
- les runners de `macroforecast` continuent de journaliser **pendant** le calcul via le
  `RunTracker` (métriques par étape BACI, artefacts) ; le tracker reçu par les étapes est
  l'`ActiveRunTracker` renvoyé par `kedro_pipeline/io/tracking.py::build_tracker(...)`
  (`NullTracker` si aucun run n'est actif) ; toute exception de journalisation est
  journalisée en WARNING, jamais propagée ;
- **pas de paramètre sensible** : `credentials` n'est jamais passé en `params:` ;
- en fin de nœud, **avant** `raise_if_failed()`, le nœud appelle
  `publish_run_report(tracker, report, params["tracking"])` : description Markdown,
  contrôles, `report/report.html`, `report/checks.csv`, tags `health` et
  `checks_failed` (PS-31) ; le hook `on_node_error` publie une description réduite si le
  nœud lève avant d'y arriver ;
- les métriques système sont activées par variable d'environnement dans les pods
  (PS-21.1) : aucune ligne de code dans les nœuds.

### PS-20 — `config/base/argo.yml`

```yaml
namespace: user-qbollietdgddi
deployment:
  image: ghcr.io/qbolliet/trade-analysis
  tag: ${oc.env:IMAGE_TAG, latest}      # surchargé au rendu par le SHA court
  target_platform: linux/amd64
  context: ./
  dockerfile: docker/Dockerfile
runner:
  use_memory_datasets: true              # uniquement à l'intérieur des FusedPipeline
# Limites Onyxia constatées (PQ-01) : ≤ 100 pods simultanés, 0,1 à 30 CPU et 1 à
# 200 Gi par pod, délai de démarrage croissant avec la demande → rester modeste
machine_types:
  io-small:       {mem: 2,  cpu: 1, num_gpu: 0, emph_storage: 5}
  compute-medium: {mem: 16, cpu: 4, num_gpu: 0, emph_storage: 20}
  baci-large:     {mem: 32, cpu: 4, num_gpu: 0, emph_storage: 80}   # une tranche annuelle ≈ 2,5 Go (PS-14.5)
  synthesis-cpu:  {mem: 32, cpu: 8, num_gpu: 0, emph_storage: 20}
default_machine_type: io-small
parallelism: 8                           # spec.parallelism du template (7 BACI + 1)
```

`mem` est en GiB, `emph_storage` en GiB. Le rendu maison fixe `requests = limits` pour la
mémoire (OOM prévisible) et `requests = cpu`, `limits = cpu` pour le CPU. Le
partitionnement par tranches (PS-14) rend inutiles les pods à très forte mémoire : la
taille `baci-large` est choisie pour démarrer vite ; elle se relève par configuration si
`memory/peak_mb` (MLflow) s'en approche. Le quota **total** du namespace (somme des
pods) reste à relever (PQ-16) : sept pods `baci-large` simultanés demandent 224 Gi.

### PS-21 — Manifestes générés

#### PS-21.1 `WorkflowTemplate` (extrait attendu)

```yaml
apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: trade-pipeline
  namespace: user-qbollietdgddi
  labels: {app: trade-analysis, generated-by: kedro_pipeline.deploy.render}
  annotations: {trade-analysis/git-sha: "6f24c6c"}
spec:
  entrypoint: daily                     # surchargé par chaque CronWorkflow (PD-23)
  serviceAccountName: workflow          # PQ-02
  onExit: maintain-ducklake
  activeDeadlineSeconds: 518400         # 6 jours (borne du weekly ; le daily est borné par ses budgets MAX_RUNTIME)
  ttlStrategy: {secondsAfterSuccess: 259200, secondsAfterFailure: 604800}
  podGC: {strategy: OnPodSuccess}
  parallelism: 8
  arguments:
    parameters:
      - {name: image-tag, value: "6f24c6c"}
      - {name: kedro-env, value: cloud}
      - {name: force-steps, value: ""}
      - {name: force-metrics, value: ""}
      - {name: force-methods, value: ""}
  templates:
    - name: kedro
      inputs:
        parameters: [{name: kedro-node}, {name: experiment}, {name: cpu}, {name: mem}]
      podSpecPatch: |
        containers:
          - name: main
            resources:
              requests: {cpu: "{{inputs.parameters.cpu}}", memory: "{{inputs.parameters.mem}}Gi"}
              limits:   {cpu: "{{inputs.parameters.cpu}}", memory: "{{inputs.parameters.mem}}Gi"}
      retryStrategy: {limit: 1, retryPolicy: OnError}
      container:
        image: "ghcr.io/qbolliet/trade-analysis:{{workflow.parameters.image-tag}}"
        command: [kedro]
        args:
          - run
          - --pipeline=__default__
          - --env={{workflow.parameters.kedro-env}}
          - --nodes={{inputs.parameters.kedro-node}}
          - >-
            --params=runtime.FORCE_STEPS={{workflow.parameters.force-steps}},runtime.FORCE_METRICS={{workflow.parameters.force-metrics}},runtime.FORCE_METHODS={{workflow.parameters.force-methods}}
        env:
          - {name: KEDRO_ENV, value: "{{workflow.parameters.kedro-env}}"}
          - {name: MLFLOW_EXPERIMENT_NAME, value: "{{inputs.parameters.experiment}}"}
          - {name: WORKFLOW_ID, valueFrom: {fieldRef: {fieldPath: "metadata.labels['workflows.argoproj.io/workflow']"}}}
          - {name: NUM_CPU, value: "{{inputs.parameters.cpu}}"}
          - {name: PYTHONUNBUFFERED, value: "1"}
          - {name: AWS_S3_ENDPOINT, value: minio.lab.sspcloud.fr}
          - {name: AWS_DEFAULT_REGION, value: us-east-1}
          - {name: MLFLOW_S3_ENDPOINT_URL, value: "https://minio.lab.sspcloud.fr"}
          - {name: MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING, value: "true"}      # tracking.SYSTEM_METRICS.ENABLED
          - {name: MLFLOW_SYSTEM_METRICS_SAMPLING_INTERVAL, value: "30"}     # tracking.SYSTEM_METRICS.SAMPLING_SECONDS
          - {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: trade-s3-credentials, key: S3_ACCESS_KEY}}}
          - {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: trade-s3-credentials, key: S3_SECRET_KEY}}}
          - {name: COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY, valueFrom: {secretKeyRef: {name: comtrade-api-credentials, key: COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY}}}
        envFrom:
          - secretRef: {name: trade-postgres-credentials}
          - secretRef: {name: trade-mlflow-credentials}
    - name: kedro-serving                 # même conteneur, sérialisé (PD-23)
      synchronization: {mutex: {name: trade-serving}}
      # … identique à `kedro` …
    - name: daily
      dag:
        tasks:
          - {name: download-eurostat, template: kedro, arguments: {parameters: [{name: kedro-node, value: download_eurostat}, {name: experiment, value: trade-01-downloads}, {name: cpu, value: "1"}, {name: mem, value: "2"}]}}
          - {name: download-comtrade, template: kedro, arguments: {parameters: [ … ]}}
          - name: compute-partner-vulnerabilities
            depends: "(download-eurostat.Succeeded || download-eurostat.Failed)"
            template: kedro
            arguments: {parameters: [ … ]}
          - name: publish-serving
            depends: "(compute-partner-vulnerabilities.Succeeded || compute-partner-vulnerabilities.Failed)"
            template: kedro-serving
            arguments: {parameters: [ … ]}
    - name: weekly
      dag:
        tasks:
          - {name: prepare-baci, template: kedro, arguments: {parameters: [ … ]}}
          # … un process-baci-<millésime> par cible, dépendant de prepare-baci.Succeeded …
          - name: compute-network-vulnerabilities
            depends: "(process-baci-hs1992.Succeeded || process-baci-hs1992.Failed) && … "
            template: kedro
            arguments: {parameters: [ … ]}
          - name: compute-synthetic-scores
            depends: "(compute-network-vulnerabilities.Succeeded || compute-network-vulnerabilities.Failed)"
            template: kedro
            arguments: {parameters: [ … ]}
          - name: compute-synthesis-coherence
            depends: "(compute-synthetic-scores.Succeeded || compute-synthetic-scores.Failed)"
            template: kedro
            arguments: {parameters: [ … ]}
          - name: publish-serving
            depends: "(compute-synthesis-coherence.Succeeded || compute-synthesis-coherence.Failed)"
            template: kedro-serving
            arguments: {parameters: [ … ]}
    - name: maintain-ducklake
      synchronization: {mutex: {name: trade-maintenance}}
      steps: [[{name: run, template: kedro, arguments: {parameters: [{name: kedro-node, value: maintain_ducklake}, {name: experiment, value: trade-00-maintenance}, {name: cpu, value: "1"}, {name: mem, value: "2"}]}}]]
```

Exceptions de tolérance : `process-baci-*` dépendent de `prepare-baci.Succeeded`
**strict**, car sans périmètre valide rien n'est calculable. Les téléchargements ont
`retryStrategy: {limit: 0}` : une reprise dépasserait le budget horaire. Dans `daily`,
`compute-partner-vulnerabilities` lit `baci.concordances` **tel qu'il existe** (produit
par le `weekly`) : la dépendance Kedro est hors du point d'entrée, donc absente du DAG
Argo `daily`.

#### PS-21.2 `CronWorkflow` (deux)

```yaml
apiVersion: argoproj.io/v1alpha1
kind: CronWorkflow
metadata: {name: trade-pipeline-daily, namespace: user-qbollietdgddi}
spec:
  schedules: ["0 1 * * *"]
  timezone: Europe/Paris
  concurrencyPolicy: Forbid
  startingDeadlineSeconds: 3600
  successfulJobsHistoryLimit: 7
  failedJobsHistoryLimit: 7
  workflowSpec:
    workflowTemplateRef: {name: trade-pipeline}
    entrypoint: daily
    activeDeadlineSeconds: 82800
---
apiVersion: argoproj.io/v1alpha1
kind: CronWorkflow
metadata: {name: trade-pipeline-weekly, namespace: user-qbollietdgddi}
spec:
  schedules: ["0 2 * * 6"]             # samedi 02:00 ; dérivé de runtime.WEEKLY_DAY au rendu
  timezone: Europe/Paris
  concurrencyPolicy: Forbid
  startingDeadlineSeconds: 21600
  successfulJobsHistoryLimit: 4
  failedJobsHistoryLimit: 4
  workflowSpec:
    workflowTemplateRef: {name: trade-pipeline}
    entrypoint: weekly
    activeDeadlineSeconds: 518400
```

> `schedules` (liste) requiert Argo Workflows ≥ 3.6 ; sinon `schedule: "0 1 * * *"`. K-15
> lit la version du contrôleur (PQ-02) et choisit. La surcharge de `entrypoint` et
> d'`activeDeadlineSeconds` dans `workflowSpec` avec `workflowTemplateRef` est
> **à vérifier** sur la version déployée (repli : deux `WorkflowTemplate` rendus depuis
> le même gabarit).

#### PS-21.3 Commandes

```bash
uv run kedro trade render-argo --env cloud --image-tag "$(git rev-parse --short HEAD)"
uv run kedro trade render-argo --check      # CI : échoue si kubernetes/generated/ diffère
kubectl apply -f kubernetes/generated/
argo submit --from workflowtemplate/trade-pipeline --entrypoint daily  -p kedro-env=demo --watch
argo submit --from workflowtemplate/trade-pipeline --entrypoint weekly -p kedro-env=demo --watch
```

### PS-22 — `docker/Dockerfile`

```dockerfile
# syntax=docker/dockerfile:1.7
FROM ghcr.io/astral-sh/uv:0.8.17 AS uv

FROM python:3.13-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project \
      --extra tracking --extra reports --extra optimal-transport
COPY macroforecast/ macroforecast/
COPY scripts/ scripts/
COPY kedro_pipeline/ kedro_pipeline/
COPY config/base/ config/base/
COPY config/cloud/ config/cloud/
COPY config/demo/ config/demo/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra tracking --extra reports --extra optimal-transport
# Extensions DuckDB préinstallées (pas de téléchargement au démarrage des pods)
ENV HOME=/app
RUN /app/.venv/bin/python -c "import duckdb; c=duckdb.connect(); [c.install_extension(e) for e in ('ducklake','postgres','httpfs')]"

FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client ca-certificates \
    && rm -rf /var/lib/apt/lists/* && useradd --uid 1000 --create-home --home-dir /app app
COPY --from=builder --chown=app:app /app /app
ARG GIT_SHA=unknown
ENV PATH="/app/.venv/bin:$PATH" HOME=/app PYTHONUNBUFFERED=1 GIT_SHA=${GIT_SHA} KEDRO_ENV=cloud
WORKDIR /app
USER 1000
ENTRYPOINT []
CMD ["kedro", "info"]
```

`.dockerignore` : `.git`, `.venv`, `**/__pycache__`, `notebooks/`, `tests/`, `*.tex`,
`*.pdf`, `*.bbl`, `data/`, `mlruns/`, `site/`, `sspcloud_access_script.txt`,
`Trade deployment.md`, `config/local/`, `.claude/`.

Remarques :
- `postgresql-client` est requis par la sauvegarde `pg_dump` (PD-16) ;
- la commande par défaut n'exécute rien de destructif ; les scripts restent appelables
  (`docker run … baci-hs-script`).

### PS-23 — GitHub Actions

| Workflow | Déclencheur | Tâches |
|---|---|---|
| `ci.yml` | push, pull_request | `uv sync --frozen --all-extras --group dev` (sans `optimal-transport` si trop lourd), `uv run pytest -m "not slow"`, `uv run kedro trade render-argo --check`, `uv run kedro registry list` |
| `image.yml` | push sur `main`, tags `v*`, `workflow_dispatch` | `docker/setup-buildx-action`, `docker/login-action` (`ghcr.io`, `${{ github.actor }}`, `${{ secrets.GITHUB_TOKEN }}`), `docker/metadata-action` (tags `sha-<court>`, `main`, `latest`, semver), `docker/build-push-action` (`cache-from/to: type=gha,mode=max`, `build-args: GIT_SHA=${{ github.sha }}`) ; permissions `packages: write`, `contents: read` |
| `docs.yml` | push sur `main` (chemins `docs/**`, `kedro_pipeline/**`, `config/base/**`, `mkdocs.yml`) | `uv sync --extra docs`, `kedro viz build`, `mkdocs build`, copie de `build/` dans `site/pipeline/`, `actions/upload-pages-artifact` + `actions/deploy-pages` |

L'étiquette immuable utilisée par Argo est le **SHA court** (`sha-6f24c6c` → paramètre
`image-tag`). `latest` sert au confort local uniquement.

### PS-24 — Nœud de maintenance

```yaml
# config/base/parameters_maintenance.yml
maintenance:
  CATALOGS:
    - {dbname: eurostat,        catalog_alias: eurostat,        bucket: qbollietdgddi, data_path: trade/datasets/comext}
    - {dbname: comtrade,        catalog_alias: comtrade,        bucket: qbollietdgddi, data_path: trade/datasets/comtrade}
    - {dbname: vulnerabilities, catalog_alias: vulnerabilities, bucket: qbollietdgddi, data_path: trade/datasets/vulnerabilities/}
    - {dbname: serving,         catalog_alias: serving,         bucket: qbollietdgddi, data_path: trade/datasets/serving/}   # PD-21
  ONLY_TABLES_WRITTEN_WITHIN_HOURS: 24
  DELETE_RATIO_THRESHOLD: 0.1
  WEEKLY_DAY: 6                       # 0 = lundi … 6 = dimanche (Europe/Paris)
  SNAPSHOT_RETENTION_DAYS: 7
  CLEANUP_OLDER_THAN_DAYS: 7
  MAX_FILES_PER_TABLE: 2000
  DATA_INLINING_ROW_LIMIT: 1000       # null → désactivé
  PARTITIONS:
    comtrade.C_A_HS: ["refYear"]
    comtrade.baci_hs*: ["year"]
    eurostat.DS_045409: ["reporter"]
    vulnerabilities.indicators: ["classification"]
    vulnerabilities.network_indicators: ["classification"]
    vulnerabilities.synthesis: ["TIME_PERIOD"]
    # serving.* : partitionné par publish_serving à la création (PD-21), non repris ici
  STALE_RUNS:                         # clôture des runs MLflow orphelins (PD-16.6)
    ENABLED: true
    MIN_AGE_MINUTES: 10               # marge pour ne pas clore un run en cours de fermeture
  BACKUP:
    ENABLED: true
    WEEKLY_DAY: 6
    PATH_TEMPLATE: "trade/backups/postgres/{dbname}/{date}.sql.gz"
    RETENTION: 4
```

> **Note K-03b (2026-09-21) pour K-14.** Aucune liste de catalogues maintenus n'existe
> encore dans la configuration de phase 0 (pas de `parameters_maintenance.yml`) : K-14
> doit créer `maintenance.CATALOGS` avec l'entrée `serving` ci-dessus (base `serving`,
> alias `serving`, `trade/datasets/serving/`), qui porte **les deux schémas**
> `dashboard` et `demo_dashboard`. Chaque publication `full` recrée ses tables : les
> fichiers remplacés deviennent orphelins d'un snapshot à l'autre, d'où l'intérêt
> d'`expire_snapshots` + `cleanup_old_files` quotidiens sur ce catalogue ;
> `merge_adjacent_files` y a peu d'effet attendu en mode `full` (une écriture triée par
> partition et par publication), davantage en `by_year`.

Ordre par table : `flush_inlined` → `merge_adjacent_files` → `rewrite_data_files`
(conditionnel) ; puis par catalogue : `expire_snapshots` → `cleanup_old_files` ; puis
`VACUUM ANALYZE` et la sauvegarde si c'est le jour hebdomadaire ; enfin la clôture des
runs orphelins du workflow (`STALE_RUNS`). Le catalogue `serving` est entièrement
réécrit à chaque publication en mode `full` : `expire_snapshots` et `cleanup_old_files`
y sont ce qui borne le stockage. Les requêtes
d'inspection lisent les tables de métadonnées DuckLake (`ducklake_data_file`,
`ducklake_delete_file`, `ducklake_snapshot`) : leurs noms exacts sont **à vérifier sur
la version DuckLake embarquée** (K-14).

### PS-25 — Documentation

`mkdocs.yml` : thème `material`, navigation `Accueil` (README), `Architecture`
(`PIPELINE_ARCHITECTURE.md` inclus par `pymdownx.snippets` ou copié au build),
`Exploitation` (runbooks §5), `Configuration` (page générée par
`kedro_pipeline.cli docs-config` : tableau clé/valeur/commentaire depuis
`config/base/parameters_*.yml`), `Pipeline interactif` (lien `pipeline/index.html`).
La navigation référence la page kedro-viz via un lien relatif (pas d'iframe).

### PS-26 — Rappel : aucune valeur méthodologique en dur

Tout nouveau seuil, toute nouvelle liste ou tout nouveau budget introduit par ces
spécifications (complétude, budgets, rétentions, tailles de lots, `n_jobs`…) est un
**paramètre** de `config/base/`, avec un commentaire en français. Les défauts des
dataclasses restent possibles, mais la valeur de production vit dans le YAML.

### PS-27 — Évolutions à apporter à `statflows` (dépôt `qbolliet/statflows`)

Ces changements relèvent de la frontière « acquisition et persistance » : ils sont
réalisés **dans le dépôt `statflows`** (K-04), puis la dépendance est mise à jour
(`uv lock --upgrade-package statflows`).

1. **Registre de téléchargement tamponné** : `SDMXDownloader(registry_flush_every=500,
   registry_flush_seconds=300)`. Persistance à la fin du run, dans le `finally`, et sur
   `SIGTERM` (arrêt de pod) via un gestionnaire de signal restauré en sortie.
   Rétrocompatibilité : `registry_flush_every=1` reproduit le comportement actuel.
2. **Registre fragmenté** (optionnel, activé par `registry_shard_key: Callable[[query], str]`) :
   un fichier par fragment (reporter pour Eurostat, année pour Comtrade), et une **API de
   lecture** `iter_registry_entries(path, bucket) -> Iterator[RegistryEntry]` qui masque
   le format physique (fichier unique ou fragments).
3. **Écritures DuckLake tamponnées** : accumulation des DataFrames non vides jusqu'à
   `write_batch_rows` lignes (ou `write_batch_queries` requêtes), puis une seule écriture.
   Le registre n'avance **qu'après** l'écriture du lot qui contient la requête (invariant
   d'ordre préservé).
4. **`write_dataframe(..., compact_after_update: bool = True, allow_new_columns: bool =
   False, run_id: str | None = None, commit_message: str | None = None)`** transmis à
   `DatabaseUpdater.update_database` (API 0.3.1, C-13) ; le téléchargeur passe
   `compact_after_update=False` ; les étapes de calcul passent `allow_new_columns=True`
   (PD-11). `statflows` doit exiger `dt-ducklake-manager >= 0.3.1` dans son extra
   `ducklake`.
5. **`ducklake_options`** transmis à l'`ATTACH` (ex. `DATA_INLINING_ROW_LIMIT`) si
   `DuckLakeConnector` le permet ; sinon, ticket sur `dt-ducklake-manager`.
6. **Codelists persistées** : `SDMXDownloader` (ou un utilitaire de `statflows.core`)
   expose les codelists résolues (`code`, `label`, dimension, dataflow) sous forme de
   DataFrame, afin que le nœud de téléchargement les publie dans `reference` (PS-28.4)
   sans second appel réseau. *Constat K-03b :* côté Eurostat, `parse_codelist_response`
   renvoie déjà `(code, name)` (libellé anglais) ; côté Comtrade, `ComtradeClient.
   _extract_codes` ne rendait que les codes alors que `get_metadata(category)` porte les
   libellés — `scripts/download_comtrade.py::fetch_dimension_codelists` conserve
   désormais ces métadonnées (même appel). **Manque côté Comtrade** : (a) `cmd:HS` n'est
   pas millésimée (codes et libellés de l'édition la plus récente) — exposer les
   catégories par édition (`cmd:H0` … `cmd:H6`) pour des libellés par millésime ;
   (b) la catégorie `partner` n'est pas récupérée par le téléchargement (seuls
   `reporter` et `cmd:HS` le sont) ; (c) `extract_codes` écarte les entrées expirées,
   donc les pays disparus n'ont pas de libellé pour les années anciennes.
7. Tests : un faux client produisant 2 000 requêtes → nombre de PUT du registre ≤ 5,
   nombre de snapshots DuckLake ≤ 5, contenu final identique au mode non tamponné.

### PS-28 — Nomenclatures : référentiel des millésimes, conversion des flux Comext et tables de référence

#### PS-28.1 Fonctions pures (`kedro_pipeline/config.py`, sans I/O)

```python
def vintage_in_force(year: int, nomenclatures: Mapping[str, int]) -> str:
    """Return the HS vintage in force for ``year``.

    Examples:
        >>> hs = {"HS1992": 1988, "HS1996": 1996, "HS2002": 2002, "HS2022": 2022}
        >>> vintage_in_force(1990, hs), vintage_in_force(2001, hs), vintage_in_force(2024, hs)
        ('HS1992', 'HS1996', 'HS2022')
    """

def historical_vintages(year: int, nomenclatures: Mapping[str, int]) -> list[str]:
    """Vintages V with entry(V) <= year, excluding the one in force, oldest first."""

def classification_of(product: str, year: int, nomenclatures: Mapping[str, int]) -> str:
    """'HSxxxx' for 2/4/6-digit codes, 'CN<year>' for 8-digit codes (PD-20.3)."""
```

#### PS-28.2 Conversion des flux Comext vers un millésime historique

Pour un reporter `r`, un millésime cible `V` et une période `t ≥ entrée(V)` :

1. lecture SQL des lignes `(r, t)` de `DS_045409` aux codes **SH6** (`length(product) = 6`),
   tous partenaires, flux et indicateurs ;
2. `HsHarmonizer(source=vintage_in_force(t), target=V, concordances=…)` sur les colonnes
   de mesure (`OBS_VALUE`) avec clés `(reporter, partner, flow, indicators, TIME_PERIOD)`
   — même classe et mêmes tables que BACI ; les partenaires agrégés (`WORLD`, `EXT_EU`)
   sont des lignes comme les autres et s'agrègent de la même manière ;
3. calcul des métriques (`run_vulnerabilities`) sur le résultat, exactement comme pour
   les lignes en vigueur ;
4. colonnes ajoutées : `classification = V`, `hs_vintage = V`, `in_force = false`.

Quand `vintage_in_force(t) = V`, l'étape 2 est l'identité : les lignes « en vigueur »
sont produites par le même chemin (une seule implémentation).

#### PS-28.3 Unité de fraîcheur et gate des sources

Unité `(V, r, p_V)`. Sources : `S(p_V) = {codes c de vintage_in_force(t) : c ↦ p_V}` par
la table de passage (préimage). Watermark = `max(last_download(r, c), c ∈ S)`. Si un
code de `S` n'a jamais été téléchargé, l'unité est reportée
(`freshness/units_waiting_sources`). Empreinte : celle des métriques **plus** la somme de
contrôle de la table de passage utilisée (une table UNSD corrigée déclenche le recalcul).

#### PS-28.4 Tables de référence (schéma `reference` de chaque catalogue source)

| Table | Clé | Colonnes | Alimentée par |
|---|---|---|---|
| `products` | `(classification, code)` | `label`, `level` (2/4/6/8), `parent_code` | téléchargements (codelists `cmd:HS` par millésime pour Comtrade ; codelist `product` Comext, rattachée à `CN<année>`/`HS<millésime>` par `classification_of`) |
| `reporters` / `partners` | `(source, code)` | `label`, `iso3`, `m49`, `is_aggregate` | téléchargements (codelists `reporter`, `partner`) |
| `hs_concordance` | `(source_classification, source_code, target_classification, target_code)` | `relationship` (1:1, n:1, 1:n, n:n), `checksum` | `prepare_baci` (cache UNSD) |
| `hs_vintages` | `classification` | `entry_year`, `in_force_until` | `runtime.NOMENCLATURES` |

Ces tables sont recopiées telles quelles dans le catalogue `serving` (PS-29) pour les
libellés du tableau de bord.

**Mise en œuvre (K-03b, 2026-09-21).** `statflows.write_dataframe` écrit toujours une
table `<schéma>.fact_table` : chaque référentiel occupe donc **son propre schéma**,
`<SCHEMA_PREFIX>_<table>` (`reference_products`, `reference_reporters`,
`reference_partners` dans `eurostat` ; `reference_reporters`, `reference_products`,
`reference_hs_concordance`, `reference_hs_vintages` dans `comtrade`), le préfixe étant
`DOWNLOADS.REFERENCE.SCHEMA_PREFIX` des configurations de téléchargement (les tables SH
réutilisent celui de Comtrade, même catalogue). Écriture par upsert idempotent
(`kedro_pipeline/steps/reference.py` : `publish_reference`, `publish_hs_reference`),
**non bloquante** pour l'étape appelante. Les noms « propres » n'existent que dans
`serving`. Précisions :
- `products` : les codelists ne sont **pas millésimées** ; un code est rattaché à la
  classification en vigueur l'année du téléchargement (`classification_of` :
  `HS<millésime>` pour 2/4/6 chiffres, `CN<année>` pour 8) ; `level` = nombre de
  chiffres (NULL pour `TOTAL`…), `parent_code` fourni par Comtrade (`parent`) ou déduit
  (8 → 6 → 4 → 2 chiffres) ;
- `reporters`/`partners` : Comtrade fournit `m49` (`reporterCode`), `iso3`
  (`reporterCodeIsoAlpha3`) et `isGroup` ; Comext ne fournit que le libellé (`iso3`,
  `m49` NULL) et `is_aggregate` suit la règle de `VulnerabilityConfig` (code listé dans
  `AGGREGATE_CODES` ou non ISO2 : `EXT_EU`, `EU27_2020`…) ;
- `hs_concordance` : `relationship` calculée par cardinalités dans chaque paire
  (`1:1`, `n:1`, `1:n`, `n:n` ; les tables UNSD de conversion sont des fonctions, donc
  `1:1` ou `n:1`), `checksum` = SHA-256 du contenu de la paire (même règle que le
  registre du cache) ;
- Comext : la codelist `partner` est récupérée **en plus** des dimensions de découpage
  (un appel SDMX supplémentaire, `parse_codelist_response` → `code`, `name`).

### PS-29 — Couche de service : catalogue DuckLake `serving`

#### PS-29.1 Contrat

- Catalogue DuckLake `serving` : base de métadonnées PostgreSQL `serving` (même
  instance que les autres catalogues, identifiants `trade-postgres-credentials`),
  fichiers sous `s3://qbollietdgddi/trade/datasets/serving/`, schéma `serving.SCHEMA`
  (`dashboard` ; `demo_dashboard` en `demo`). Lecture par Superset avec un rôle
  PostgreSQL **lecture seule** `superset_reader` sur la base `serving` (§9).
- Poignée `kedro_pipeline/io/serving.py::ServingCatalog(location, pg, s3, sources)` :
  aucune connexion à l'instanciation ; `connect()` ouvre une session DuckDB qui attache
  le catalogue `serving` en écriture et les catalogues sources (`eurostat`, `comtrade`,
  `vulnerabilities`) en **lecture seule** (`READ_ONLY`), afin que les requêtes de
  `serving.TABLES` les référencent par leur alias.
- Écriture par `publish_serving` (PS-08), **une transaction pour toute la
  publication** :
  ```sql
  BEGIN;
  -- pour chaque table de serving.TABLES, mode full :
  CREATE OR REPLACE TABLE serving.dashboard.cell_scores AS (<SQL>) LIMIT 0;  -- schéma seul
  ALTER TABLE serving.dashboard.cell_scores SET PARTITIONED BY (year);       -- si PARTITIONED
  INSERT INTO serving.dashboard.cell_scores <SQL> ORDER BY year, reporter, product;
  -- mode by_year (tables PARTITIONED, colonnes inchangées) :
  DELETE FROM serving.dashboard.cell_scores WHERE year IN (<années périmées>);
  INSERT INTO serving.dashboard.cell_scores <SQL filtré sur ces années> ORDER BY …;
  COMMIT;
  ```
  Superset lit l'ancien snapshot jusqu'au `COMMIT`. En cas d'échec d'une table :
  `ROLLBACK` complet (le tableau de bord reste sur l'état précédent, cohérent) et
  échec du nœud après publication du rapport de run.
- **Vérifications (K-03b, 2026-09-21, DuckDB 1.5.3, catalogue DuckLake fichier local)** —
  toutes concluantes, **aucun repli** (la transaction unique est conservée) :
  1. dans **une** transaction, `CREATE OR REPLACE TABLE … AS SELECT … LIMIT 0`,
     `ALTER TABLE … SET PARTITIONED BY (year)` puis `INSERT … BY NAME … ORDER BY …` sur
     deux tables, puis `COMMIT` : accepté. Un lecteur concurrent (connexion distincte de
     la même instance, `conn.cursor()`) voit l'**ancien état des deux tables** jusqu'au
     `COMMIT`, puis le nouveau ; un `ROLLBACK` laisse l'ancien état intact ;
  2. `DELETE … WHERE year IN (…)` + `INSERT` filtré sur la table partitionnée (mode
     `by_year`) : seules les années demandées changent ;
  3. élagage : `EXPLAIN ANALYZE SELECT … WHERE year = 2020` affiche `Total Files Read: 1`
     (fichiers rangés sous `…/<table>/year=<année>/`) ; le nombre de fichiers d'une table
     se lit par `ducklake_list_files('<alias>', '<table>', schema => '<schéma>')` ;
  4. `ATTACH 'ducklake:…' AS serving (READ_ONLY)` depuis une nouvelle connexion, puis la
     même lecture par SQLAlchemy + `duckdb-engine 0.17` (écouteur `connect` qui exécute
     `LOAD ducklake; ATTACH … (READ_ONLY); USE serving`, mécanisme (b) de PS-30.1) :
     lecture filtrée correcte, `INSERT`/`DELETE` refusés (« attached in read-only
     mode »). `duckdb-engine` présente le schéma sous le nom **`serving.dashboard`**
     (liste `get_schema_names()`), et `dashboard.<table>` après `USE serving`.
  Limites du test local : DuckDB refuse un second `ATTACH` du même fichier `.ducklake`
  dans un processus, d'où le lecteur par `cursor()` ; la concurrence inter-processus
  (Superset) repose sur le catalogue PostgreSQL, non testable ici. DuckLake refuse aussi
  de rattacher un catalogue avec un `DATA_PATH` différent de celui enregistré (« does not
  match existing data path ») : le profil `demo` partage donc le `DATA_PATH` du catalogue
  `serving` et n'est isolé que par son schéma `demo_dashboard`.
- Implémentation : `ServingCatalog(location, pg, s3, sources, connector_factory=build_connector)`
  (`connect()` : `serving` en écriture, base créée si absente, sources attachées
  `READ_ONLY` sans changer le schéma courant ; `publish(tables, mode, years=…, prelude=…,
  before_commit=…)` → `dict[str, TableStats]` : lignes, secondes, fichiers, mode
  effectif) ; échec → `ROLLBACK` et `ServingPublicationError(table=…)`. `build_connector`
  nomme le secret PostgreSQL `ducklake_pg_<alias>` (sinon plusieurs `ATTACH` sur une même
  session s'écraseraient le secret) et accepte `read_only=`.
- Sources facultatives (`serving.OPTIONAL_SOURCES`) : un référentiel ou une table de
  résultats absente est remplacée par une table vide typée (libellés / scores `NULL`,
  métrique `serving/missing_sources`) ; une source obligatoire absente (`comext`,
  `indicators`) fait échouer la publication, qui est annulée.
- Années périmées (mode `by_year`) : années des lignes de l'amont dont
  `last_computed` est postérieur à la dernière publication (registre de fraîcheur de
  `serving`, PS-10) ; retour automatique en `full` si l'ensemble des colonnes produites
  par la requête diffère de celui de la table (nouvelle colonne de restitution).
- Phase 0 (K-03b) : les années du mode `by_year` viennent de `serving.YEARS`, sinon de
  `runtime.FORCE_SCOPE.PERIODS`, sinon repli en `full` ; `MODE: full` dans les deux
  profils. Les tables non partitionnées sont toujours recréées.
- Métriques (vers MLflow) : `serving/<table>/rows`, `serving/<table>/seconds`,
  `serving/<table>/files`, `serving/mode_full` (1 si toutes les tables ont été
  recréées), `serving/missing_sources`, `serving/n_failures` ; contrôle par défaut « `rows > 0` pour
  `cell_scores` et `countries` » (PS-31.2).
- Idempotence : republier des tables déjà à jour est sans effet visible ; la fraîcheur
  (PS-10) évite le travail inutile en régime nominal.

#### PS-29.2 Tables de service (`config/serving.yaml`, racine `serving`)

Phase 0 : `config/serving.yaml` (lu par `SERVING_CONFIG_PATH`) et sa copie complète
`config/profiles/demo/serving.yaml` (`SCHEMA: demo_dashboard`, expérience
`demo-serving`) ; migrera vers `config/base/parameters_serving.yml` (K-10). Paramètres :

```yaml
serving:
  DBNAME: serving              # base de métadonnées PostgreSQL (créée si absente)
  CATALOG_ALIAS: serving
  BUCKET: qbollietdgddi
  DATA_PATH: trade/datasets/serving/   # identique en demo (un catalogue = un DATA_PATH)
  SCHEMA: dashboard            # demo : demo_dashboard
  MODE: full                   # full | by_year
  YEARS: null                  # by_year : années ; null → runtime.FORCE_SCOPE.PERIODS
  METHODS: [consensus_borda, auto_sum, critic_sum]   # valeurs RÉELLES de synthesis.method
  LEVELS: [by_reporter, by_product]
  PRIMARY_METHOD: consensus_borda
  TOP_PARTNERS: 12
  CELL_FILTER: indicators = 'VALUE_IN_EUROS' AND freq = 'A'   # sur p (indicators)
  COMEXT_FILTER: idem sur c (DS_045409)
  NORM_METRICS: [HHI, CDI2, CDI3, EXPORT_HHI, CENTRALITY_RISK, CLUSTERING_W, SPOF]
  NORM_PARTITION: [flow, year]
  PARTNERS: {WORLD: WORLD, EXTRA_EU: EXT_EU, AGGREGATE_CODES: [WORLD, QW], EXCLUDE_UNDERSCORE: true}
  OPTIONAL_SOURCES: {...}      # référentiels, network, synthesis, diagnostics
  MLFLOW: {TRACKING_URI: null, EXPERIMENT: serving}
  TABLES: {<table>: {PARTITIONED, SORT_BY, SQL}}     # gabarits string.Template
```

Les gabarits SQL référencent les tables sources par variables (`$indicators`, `$network`,
`$synthesis`, `$diagnostics`, `$comext`, `$ref_eurostat_products|reporters|partners`,
`$ref_comtrade_products|reporters`, `$hs_concordance`, `$hs_vintages`), résolues par
`kedro_pipeline.steps.serving.source_tables` depuis les fichiers de configuration des
étapes amont (schémas `demo_*` du profil compris) ; les fragments `$synthesis_pivot`,
`$norm_columns`, `$individual_partner`, `$top_partners`… sont générés depuis les
paramètres ; les macros de session `vintage_in_force(y)`, `classification_of(p, y)` et
`product_code(p)` sont générées depuis `runtime.NOMENCLATURES.HS`
(`kedro_pipeline.config.nomenclature_macros_sql`).

**Schémas réels des sources** (vérifiés contre le code, K-03b) :
- `synthesis.fact_table` (S-2.4) : **long par méthode**, large par niveau — clé
  `(freq, flow, indicators, TIME_PERIOD, reporter, product, method)` ; colonnes
  `score_<niveau>`, `rank_<niveau>`, `n_<niveau>`, `alert_<niveau>`,
  `rank_low_<niveau>`, `rank_high_<niveau>` pour `by_product`, `by_reporter`, `global`.
  Les règles de consensus sont des pseudo-méthodes **`consensus_borda`**,
  `consensus_copeland` (et non `borda`). `product` y est du texte issu d'un `BIGINT`.
- `synthesis_diagnostics.fact_table` (S-2.6) : clé `(freq, flow, indicators, TIME_PERIOD,
  level, reporter, product, family, statistic, item_a, item_b)`, colonnes `value`, `n` ;
  `reporter`/`product` valent `'ALL'` hors groupe, `item_a`/`item_b` valent `''` quand
  sans objet ; `family` ∈ `metrics`, `methods`, `fit` (cette dernière non exposée).
- `indicators` : `product` stocké en **`BIGINT`** (zéro initial perdu pour les chapitres
  01-09) → macro `product_code` (code de longueur impaire préfixé d'un zéro : exact pour
  des codes SH/NC de 2, 4, 6 ou 8 chiffres).

**Tables publiées** (référence du tableau de bord, K-03c) :

| Table | Grain | Colonnes |
|---|---|---|
| `cell_scores` (partitionnée `year`, tri `year, reporter, product`) | (classification, reporter, product, flow, year) | `classification`, `hs_vintage`, `in_force`, `year`, `time_period`, `freq`, `flow`, `indicators`, `reporter`, `reporter_label`, `product`, `product_label`, `product_level`, `HHI`, `CDI2`, `CDI3`, `EXPORT_HHI`, `CENTRALITY_RISK`, `CLUSTERING_W`, `SPOF`, `<métrique>_ALERT` (×7), `<méthode>_score_<niveau>` et `<méthode>_rank_<niveau>` (METHODS × LEVELS), `primary_score_<niveau>`, `primary_rank_<niveau>`, `primary_n_<niveau>`, `<métrique>_norm` (min-max par `flow, year`, NORM_METRICS) |
| `coherence_metrics` | contexte × niveau × groupe × statistique × paire | `classification`, `hs_vintage`, `year`, `time_period`, `freq`, `flow`, `indicators`, `level`, `reporter` (NULL si ALL), `product` (NULL si ALL), `statistic`, `metric_a`, `metric_b`, `value`, `n` |
| `coherence_methods` | idem | mêmes colonnes avec `method_a`, `method_b` (pour `lomo_*` et `score_metric_tau`, `method_b` est une métrique) |
| `flows` (partitionnée `year`, tri `year, product, reporter`) | (reporter, product, flow, year, partner) | `classification`, `hs_vintage`, `year`, `reporter`, `reporter_label`, `product`, `product_label`, `product_level`, `flow`, `partner`, `partner_label`, `is_aggregate`, `value`, `share` (/ WORLD), `rank` (1 = premier partenaire ; NULL pour WORLD et EXT_EU) — `TOP_PARTNERS` partenaires individuels + WORLD + EXT_EU |
| `products` | (source, classification, code) | `source`, `classification`, `code`, `label`, `level`, `parent_code` |
| `countries` | (source, code) | `source`, `code`, `label`, `iso3`, `m49`, `is_aggregate`, `is_reporter`, `is_partner` |
| `hs_concordance` | clé de PS-28.4 | `source_classification`, `source_code`, `target_classification`, `target_code`, `relationship`, `checksum` |
| `hs_vintages` | `classification` | `classification`, `entry_year`, `in_force_until` |

`flow` vaut `'1'` (import) / `'2'` (export), en texte. Les libellés de produits sont
joints **par code** (dernier libellé connu) : les codelists des fournisseurs ne sont pas
millésimées (PS-28.4).

#### PS-29.3 Phase 0 (avant PD-20)

Tant que `indicators` n'a pas de colonnes `classification`/`hs_vintage`/`in_force`, la
requête de `cell_scores` les **dérive** : `hs_vintage = vintage_in_force(year)`,
`classification = classification_of(product, year)` (`CN<année>` pour un code à 8
chiffres), macros SQL générées depuis `runtime.NOMENCLATURES` et testées égales aux
fonctions Python de PS-28.1 sur 1988-2030, `in_force = true`. La jointure réseau utilise
ce millésime dérivé (ce qui corrige déjà C-11 côté tableau de bord). Le contrat de
lecture du tableau de bord est donc **stable** de la phase 0 à la production.

#### PS-29.4 Volumétrie

| Table | Démonstration (~100 produits) | Production (SH6 + NC8, en vigueur) | Production avec historiques |
|---|---|---|---|
| `cell_scores` | ~0,3 M lignes | ~30 M | ~45 M (SH6 historiques seulement) |
| `flows` | ~3 M | ~80 M (12 partenaires/cellule) | idem |
| `coherence_*` | faible | quelques M | idem |

Ces volumes ne sont plus recopiés ailleurs : ils restent en Parquet dans le catalogue
`serving`. Le filtre `year` du tableau de bord (toujours renseigné, PS-30.2) élague les
partitions ; le tri d'insertion rend efficaces les filtres `reporter` et `product` au
sein d'une partition. **Objectif de latence** à mesurer en K-03c (démo) puis K-17
(production) : moins de 5 s pour un graphique à froid, instantané une fois en cache
Superset. Au-delà, leviers dans l'ordre : cache des graphiques, `TOP_PARTNERS`,
`serving.YEARS_BACK`, ressources DuckDB de la connexion Superset (`threads`,
`memory_limit`), tables d'agrégats dédiées (PR-15).

### PS-30 — Tableau de bord Superset « Vulnérabilités »

#### PS-30.1 Connexion et jeux de données

0. **Prérequis vérifiés en tête de K-03c** (consignés dans PQ-13) : version de
   `duckdb` et de l'extension `ducklake` dans l'image Superset (`SELECT version()` ;
   `SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded`)
   **compatible** avec celle du pipeline (1.5.3), sinon épingler `duckdb==1.5.3` dans
   la configuration du chart (PR-14) ; extensions `ducklake`, `postgres`, `httpfs`
   installables ou préinstallées dans le pod Superset ; mécanisme d'accès S3 déjà en
   place dans le service (variables du service, secret DuckDB persistant ou paramètres
   du moteur : le relever, ne pas le dupliquer).
1. Superset → *Settings → Database Connections → + Database → DuckDB* (ou *Other* avec
   l'URI SQLAlchemy). La connexion ouvre une session DuckDB **en mémoire** et attache le
   **seul** catalogue `serving`, **en lecture seule**. Deux mécanismes possibles, le
   premier qui fonctionne sur la version déployée est retenu et documenté :
   - **(a) secrets DuckDB persistants + URI** : créer une fois, dans le pod Superset,
     un secret `postgres` (rôle `superset_reader`) et un secret `ducklake`
     (`METADATA_PATH 'postgres:dbname=serving host=…'`,
     `DATA_PATH 's3://qbollietdgddi/trade/datasets/serving/'`) stockés dans
     `~/.duckdb/stored_secrets` (volume persistant), puis attacher par l'URI ou par les
     paramètres du moteur ;
   - **(b) écouteur SQLAlchemy `connect`** déclaré dans `superset_config.py` (surcharge
     de configuration du chart), qui exécute `LOAD ducklake; LOAD httpfs; ATTACH
     'ducklake:postgres:dbname=serving host=…' AS serving (READ_ONLY); USE serving;` à
     chaque nouvelle connexion DuckDB, identifiants lus dans les variables
     d'environnement du service.
   *Engine parameters* (onglet *Advanced → Other*) : `{"connect_args": {"config":
   {"threads": 4, "memory_limit": "4GB"}}}` (à ajuster aux ressources du service).
   *Allow DML* **non** ; *Expose in SQL Lab* oui ; *Advanced → Performance → Chart
   cache timeout* = `86400` (données rafraîchies une fois par jour : le cache absorbe
   la latence S3). À vérifier : qu'un `ATTACH … (READ_ONLY)` n'exige aucun droit
   d'écriture sur la base de métadonnées `serving`.
2. *Datasets → + Dataset* : schéma `dashboard` du catalogue `serving` (selon la façon
   dont `duckdb-engine` présente les catalogues attachés : `serving.dashboard` ou, après
   `USE serving`, `dashboard` ; à défaut, un dataset virtuel `SELECT * FROM
   serving.dashboard.<table>`), tables `cell_scores`, `flows`,
   `coherence_metrics`, `coherence_methods`, `products`, `countries`. Dans chaque
   dataset, marquer `year` comme **temporel** (type entier → *Is temporal* avec
   expression `make_date(year,1,1)` dans une **colonne calculée** `period`), et définir
   des **métriques** : `MAX(HHI)`, `MAX(CDI2)`… (les tables sont déjà à la maille
   cellule ; `MAX` = valeur elle-même).
3. Superset ≥ 3 : les *cross-filters* de tableau de bord et les *native filters* sont
   actifs par défaut ; la *drill to detail* aussi.

#### PS-30.2 Structure : un tableau de bord, deux onglets, un parcours

**Filtres natifs** (barre de gauche, appliqués aux deux onglets) : `reporter` (défaut
`FR`), `year` (défaut : dernière année disponible, via *Default value → Last value*
… ou valeur fixe), `flow` (défaut `import`), `classification` (défaut : *Filter value is
required = non*, prérempli par le millésime en vigueur ; l'onglet Produit s'en sert pour
la profondeur des séries), `in_force` (masqué, défaut `true`).

**Onglet 1 — « Pays »** (le reporter sélectionné, puis l'Union) :

| # | Graphique (type Superset) | Dataset | Contenu |
|---|---|---|---|
| 1 | *Big Number* × 4 | `cell_scores` | nombre de produits en alerte HHI, en alerte CDI2, part des importations en alerte, score synthétique médian |
| 2 | **Table** « Produits classés » | `cell_scores` | colonnes `product`, `product_label`, `borda_rank_by_reporter` (tri croissant, rang 1 = plus vulnérable), `borda_score_by_reporter`, `HHI`, `CDI2`, `CDI3`, `EXPORT_HHI`, `CENTRALITY_RISK`, `CLUSTERING_W`, alertes en *conditional formatting* ; **Emit dashboard cross filters** activé sur `product` → clic = filtre global |
| 3 | *Bar chart* horizontal | `cell_scores` | top 20 produits par score synthétique |
| 4 | *Heatmap* | `cell_scores` (top 30) | produits × métriques, valeurs normalisées 0-1 (colonnes `*_norm` à ajouter à `cell_scores`) |
| 5 | **Table** « Cohérence des métriques » | `coherence_metrics` filtré `reporter = <filtre>` | `metric_a`, `metric_b`, `statistic` (Spearman, Kendall…), `value` ; ou *Heatmap* métriques × métriques |
| 6 | Même bloc 2 + 5 pour **l'Union** | mêmes datasets, filtre **fixe** `reporter = 'EU27_2020'` (*Filter → Custom SQL* dans le graphique, et *Ignore dashboard filter* sur `reporter` via *scoping* du filtre natif) | |
| 7 | **Table** « Cohérence pays × produit » | `coherence_metrics` sans filtre reporter | `reporter`, `product`, statistiques par paire de métriques (colonnes pivotées : *Pivot Table v2*) |

**Onglet 2 — « Produit »** (activé par le cross-filter `product` de l'onglet 1 ou par un
filtre natif `product` avec recherche sur le libellé) :

| # | Graphique | Dataset | Contenu |
|---|---|---|---|
| 8 | *Big Number* + *Markdown* | `products` | libellé, millésime, niveau |
| 9 | **Table** « Pays classés » | `cell_scores` | `reporter_label`, `borda_rank_by_product`, `borda_score_by_product`, métriques ; cross-filter sur `reporter` |
| 10 | *World Map* (choroplèthe ISO-3) | `cell_scores` | score synthétique par pays (`countries.iso3`) |
| 11 | **Table** « Flux » | `flows` | `reporter_label`, `partner_label`, `flow`, `value`, `share`, `rank` ; *Bar chart* empilé des parts des 12 premiers fournisseurs (explique visuellement l'HHI) |
| 12 | *Line chart* « Évolution d'une métrique » | `cell_scores` avec `in_force = false OR in_force = true`, `classification = <filtre>` | X = `period`, Y = métrique choisie (*Dashboard native filter* de type *Value* sur une colonne `metric` d'une **vue longue** `cell_scores_long`, ou un graphique par métrique), une série par `reporter` (multi-sélection) |
| 13 | *Line chart* « Évolution des flux » | `flows` agrégé (`WORLD`, `EXT_EU`) | valeurs import/export par pays sélectionné |

Pour 12 et 13, un **second filtre natif** `reporters_compare` (multi-valeurs, *scope*
limité à l'onglet 2) évite de perturber l'onglet 1.

#### PS-30.3 Autres idées de restitution (à retenir ou non)

- *Scatter* `HHI` × `CDI2` (taille = valeur importée, couleur = alerte) : positionne
  chaque produit dans un plan « concentration / dépendance extra-UE », lisible en
  présentation ;
- *Sankey* reporter → partenaires pour un produit (flux de l'onglet 2) ;
- *Bullet / Gauge* de l'écart entre le rang du reporter et la médiane UE pour le produit ;
- graphique « **concordance des méthodes** » : *Heatmap* méthodes × méthodes (corrélation
  de rangs) depuis `coherence_methods`, et *Table* des produits sur lesquels les méthodes
  divergent le plus (rang max − rang min) — le message « la synthèse est robuste (ou
  pas) » en un coup d'œil ;
- *Time-series* du **rang** d'un produit (avec `classification` fixé) plutôt que de la
  métrique brute : plus stable et plus parlant ;
- **encadré méthodologique** (*Markdown*) par onglet : définitions des métriques et
  convention « rang 1 = plus vulnérable ».

#### PS-30.4 Parcours utilisateur retenu

1. Arrivée sur « Pays » (France, dernière année, import) : chiffres clés, tableau des
   produits classés, cohérence ; comparaison immédiate avec l'Union en dessous.
2. Clic sur un produit → cross-filter → onglet « Produit » : classement des pays, carte,
   fournisseurs, puis choix des pays et de la métrique pour les évolutions.
3. Retour à « Pays » : le cross-filter se retire d'un clic dans la barre de filtres.

#### PS-30.5 Versionnement

Une fois construit, le tableau de bord est **exporté** (*Dashboards → Export*, ZIP de
YAML : base, datasets, charts, dashboard) et commité sous `superset/vulnerabilites/`.
Réimport : *Dashboards → Import* (ou `superset import-dashboards -p …` dans le conteneur
Superset). Les identifiants de connexion ne sont pas dans l'export (mot de passe
demandé à l'import). Passage de `demo` à la production : les datasets pointent sur le
schéma `demo_dashboard` ; remplacer `demo_dashboard` par `dashboard` dans les YAML de
l'export (`datasets/*.yaml`, clé `schema` ou SQL des datasets virtuels) puis réimporter.

**Constat K-03c (2026-09-22).** Tableau de bord construit **intégralement par l'API**
(voie « assets as code ») : connexion, 8 datasets, 18 graphiques, 2 onglets, 7 filtres
natifs — exporté sous `superset/vulnerabilites/`, guide détaillé (mécanisme, pièges) dans
`superset/README.md`. Écarts avec la spécification initiale :
- **Mécanisme de connexion (PS-30.1 point 1)** : **(b) seul est retenu**, (a) écarté —
  le pod Superset n'a pas de volume persistant pour `~/.duckdb/stored_secrets`, et il y a
  **deux pods** (web + `worker` Celery, qui exécute SQL Lab en asynchrone) qui devraient
  partager le même secret, impossible sans volume commun. L'écouteur `connect` (dans
  `superset_config.py`) discrimine les connexions DuckDB via
  `type(dbapi_connection).__module__ == "duckdb_engine"` (les connexions PostgreSQL/Redis
  n'ont pas cet attribut).
- **Câblage hors Helm** : la version de chart exacte déjà déployée (`superset-0.1.12`)
  n'est plus dans l'index du dépôt (`insee-datascience`, seules des versions 1.x y
  restent) — un `helm upgrade --reuse-values` aurait risqué de changer bien plus que
  prévu. Les deux Secrets Kubernetes rendus par le chart (`superset-899573-env` pour les
  identifiants, `superset-899573-config` pour `superset_config.py`) ont été patchés
  **directement**, puis les pods redémarrés. **Conséquence : hors du cycle de vie Helm,
  perdu si le service est un jour redéployé/relancé depuis l'UI Onyxia** — à rejouer
  (procédure dans `superset/README.md`) ou, mieux, à porter dans une vraie surcharge de
  chart si une version compatible redevient trouvable.
- **Table de service `serving` jamais publiée sur l'instance courante** : la base
  PostgreSQL du namespace a été recréée après la dernière exécution de `publish_serving`
  (host `postgresql-cnpg-699488-rw` → `postgresql-cnpg-82431-rw`) ; `serving-script`
  relancé en K-03c (profil `demo`) pour repeupler le catalogue.
- **Référentiels jamais publiés sur le profil `demo`** : `products`/`countries` étaient
  vides (`publish_reference` jamais exécuté sur ce profil), laissant tous les libellés
  `NULL`. Repeuplés en K-03c par un appel ciblé (codelists Eurostat + Comtrade, sans
  retélécharger les flux) : 100 % des lignes de `cell_scores` ont désormais un libellé.
  Limite résiduelle : `countries.iso3` n'existe que pour les codes Comtrade (M49), pas
  pour les codes Eurostat alpha-2 (dont `FR`) — le graphique *World Map* (PS-30.2 #10)
  n'a **pas** été construit (pas de crosswalk ISO2→ISO3 disponible).
- **Pièges de construction par l'API** (détaillés dans `superset/README.md`) : filtre
  natif `year` sans valeur par défaut **explicite** → toutes les années se mélangent (un
  produit rang 1 par année, ressemble à un rang constant) ; `heatmap_v2` exige **deux**
  dimensions catégorielles, donc un dataset virtuel dédié en **format long**
  (`cell_scores_metrics_long`, `UNION ALL` des colonnes `*_norm`) puisque `cell_scores`
  est large ; un graphique `viz_type: "markdown"` n'est **pas** interrogeable (le Markdown
  est un composant de mise en page natif, pas un `Chart`) ; format D3 `PERCENT_1_POINT`
  invalide (`.1%` correct) ; tables de cohérence à filtrer `metric_a`/`metric_b IS NOT
  NULL` (sinon dominées par les statistiques globales `kmo`/`bartlett_p`/`axis1_share`…).
- **Simplifications** : Big Number « part des importations en alerte » → « part des
  **produits** en alerte » (pondération par valeur importée non implémentée) ; table
  « Cohérence pays × produit » en table plate plutôt qu'en *Pivot Table v2* ; graphiques
  PS-30.3 ajoutés (nuage HHI × CDI2, concordance des méthodes via `kendall_tau_b`, table
  de divergence des méthodes exposées, encadré méthodologique).

### PS-31 — Rapport de run MLflow (supervision)

Révision 2 : la supervision est **exclusivement** dans MLflow (PD-13). Objectif : pour
n'importe quel run, pouvoir répondre à « la tâche s'est-elle bien passée, et sinon
pourquoi ? » sans ouvrir de fichier, écrire de requête ni consulter un autre outil que
l'interface Argo pour le DAG.

#### PS-31.1 Rôle des onglets d'un run

| Onglet MLflow 3 | Ce qu'on y lit | Produit par |
|---|---|---|
| **Overview** | **Description** (Markdown rendu en tête de page) : verdict (`ok` / `warning` / `failed`), contrôles, unités, chiffres clés, liens ; puis paramètres (configuration aplatie par kedro-mlflow) et tags (`workflow_id`, `node`, `health`, `git_sha`, `image_tag`, `kedro_env`, `forced`, `checks_failed`) | `publish_run_report` : tag `mlflow.note.content` + `set_tags` (PS-31.3) |
| **Model metrics** | Toutes les métriques, en sections par préfixe (`gravity/`, `download/`, `checks/`…) ; courbes par `step` (année pour BACI, index de requête pour les téléchargements) | `log_metrics` des étapes et des runners |
| **System metrics** | CPU, mémoire, disque, réseau du pod au cours du temps (remplace la seule valeur `memory/peak_mb` pour dimensionner les `machine_types`, PS-20) | variables `MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING` / `MLFLOW_SYSTEM_METRICS_SAMPLING_INTERVAL` (PS-21.1), `psutil` |
| **Artifacts** | `report/report.html` (rapport autonome : mêmes informations que la description, plus figures et tables, une section par étape pour BACI) ; `report/summary.md` (copie intégrale de la description, jamais tronquée) ; `report/checks.csv` ; `tables/*.csv` (couverture, `σ̂` par pays, coefficients…) ; `failures.csv` (unités en échec et message) | `log_text`, `log_table` (PS-31.4) |

#### PS-31.2 Paramètres et contrôles déclaratifs (`config/tracking.yaml` en phase 0, `config/base/parameters_tracking.yml` sous Kedro)

Phase 0 : le fichier `config/tracking.yaml` (racine `tracking`, copie dans
`config/profiles/demo/`, variable `TRACKING_CONFIG_PATH`) est lu par `scripts/_run_report.py` ;
sous Kedro (K-13) le même contenu devient le paramètre `tracking`. Les métriques visées sont
celles **réellement émises** par les scripts (relevées le 2026-09-21) ; un test
(`tests/tracking/test_checks_target_emitted_metrics.py`) échoue si un contrôle vise une
métrique que son script n'émet pas.

```yaml
tracking:
  SYSTEM_METRICS: {ENABLED: true, SAMPLING_SECONDS: 30}   # injecté dans les pods par le rendu (PS-21.1)
  REPORT:
    HTML: true                        # report/report.html
    PLOTLY_JS: inline                 # inline (autonome, ~3,5 Mo) | cdn (léger, exige internet côté navigateur)
    MAX_DESCRIPTION_CHARS: 7500       # sous la limite de longueur des tags du serveur (PQ-19)
    MAX_TABLE_ROWS: 50                # lignes affichées par table dans le rapport HTML
    MAX_FAILURES_LISTED: 20
  LINKS:
    ARGO_WORKFLOW: null               # "https://<argo-ui>/workflows/user-qbollietdgddi/{workflow_id}" ; null : pas de lien
  CHECKS:                             # clé = nom de nœud, joker `*` accepté ; valeurs initiales à calibrer (K-17)
    "download_*":
      - {metric: download/error_share,  op: "<=", threshold: 0.05, severity: warning, label: "Part de requêtes en erreur"}
      - {metric: download/processed,    op: ">",  threshold: 0,    severity: warning, label: "Au moins une requête traitée"}
      - {metric: download/wait_share,   op: "<=", threshold: 0.5,  severity: warning, label: "Temps passé en attente du limiteur"}
    "process_baci_*":                 # nœud par millésime : process_baci_<millésime> ; inclut la porte de complétude
      - {metric: coverage/years_eligible,                  op: ">",  threshold: 0,    severity: error,   label: "Au moins une année complète"}
      - {metric: baci/flows,                               op: ">",  threshold: 0,    severity: error,   label: "Flux écrits"}
      - {metric: baci/tonnage/share_tonnage_missing,       op: "<=", threshold: 0.05, severity: warning, label: "Flux sans tonnage (1 − part convertie en tonnes)"}
      - {metric: baci/gravity/r_squared,                   op: ">=", threshold: 0.5,  severity: warning, label: "R² de l'équation de gravité"}
      - {metric: baci/fobisation/share_clipped_to_zero,    op: "<=", threshold: 0.01, severity: warning, label: "Valeurs FOB tronquées à zéro"}
    compute_partner_vulnerabilities:
      - {metric: vulnerabilities/cells/n_total,            op: ">",  threshold: 0,    severity: warning, label: "Cellules de vulnérabilité calculées"}
    "compute_network_vulnerabilities*":   # un run par millésime : compute_network_vulnerabilities_<millésime>
      - {metric: network_vulnerabilities/cells/n_total,    op: ">",  threshold: 0,    severity: warning, label: "Cellules de vulnérabilité de réseau calculées"}
    compute_synthetic_scores:
      - {metric: synthesis/n_contexts,                     op: ">",  threshold: 0,    severity: warning, label: "Contextes calculés"}
    compute_synthesis_coherence:
      - {metric: coherence/n_contexts,                     op: ">",  threshold: 0,    severity: warning, label: "Contextes de cohérence calculés"}
    publish_serving:
      - {metric: serving/cell_scores/rows,                 op: ">",  threshold: 0,    severity: error,   label: "Table cell_scores non vide"}
      - {metric: serving/countries/rows,                   op: ">",  threshold: 0,    severity: error,   label: "Table countries non vide"}
```

Écarts entre les noms indicatifs de la première rédaction et les noms réels (K-03d) :

| Nom indicatif | Nom réel / traitement |
|---|---|
| `download/queries_done`, `download/error_share`, `rate_limit/wait_share` | `download/processed` ; `download/error_share` et `download/wait_share` sont **dérivées** par `download_run_metrics` (elles ne figurent pas dans `DownloadReport`) |
| `prepare_baci` : `coverage/years_eligible` | déplacé sur `process_baci_*` : c'est `process_baci_hs.py` qui évalue la porte de complétude (`coverage/years_eligible`, `coverage/share_min`) ; `prepare_baci` n'existe pas avant K-11 |
| `output/rows` | `baci/flows` |
| `conversion/share_converted >= 0,95` | `baci/tonnage/share_tonnage_missing <= 0,05` (aucun `share_converted` global n'est émis) |
| `gravity/r_squared`, `fobisation/share_clipped_to_zero` | préfixe `baci/` conservé |
| `freshness/units_waiting_sources` | absent avant K-05 : remplacé par `vulnerabilities/cells/n_total > 0` |
| `synthesis/contexts_computed >= 0` (tautologique) | `synthesis/n_contexts > 0` |
| `serving/cell_scores/rows`, `serving/countries/rows` | inchangés (déjà en `/`) |
| `maintain_ducklake` : `ducklake/max_files_per_table` | non repris : aucun script avant K-14 |
| — | ajouts : `network_vulnerabilities/cells/n_total`, `coherence/n_contexts` (`> 0`, warning) |

Les statistiques de concordance de la cohérence (`kendall_w`, `disputed_share`, `mean_abs_rho`,
`violation_strict_*`) **ne sont pas émises au niveau du run** (agrégat à `NaN`, écartées par
`flatten_metrics`) : aucun contrôle ne peut les viser tant que K-13 ne les journalise pas par
contexte.

Sémantique :
- deux contrôles **implicites**, hors configuration, sur tout nœud : « aucune unité en
  échec » (`error`, d'après `StepResult.failures`) et « toutes les unités prévues
  traitées » (`warning`, `n_units_succeeded < n_units_planned` hors échecs, par exemple
  budget de temps épuisé) ;
- `op ∈ {<, <=, >, >=, ==, !=}` ; métrique absente → `skipped` (listé, sans effet sur le
  verdict) ; `severity ∈ {warning, error}` ;
- verdict `health` : `failed` si un contrôle `error` échoue, sinon `warning` si un
  contrôle `warning` échoue, sinon `ok` ;
- journalisé : métriques `checks/n_passed`, `checks/n_warnings`, `checks/n_failed`,
  `checks/n_skipped` ; tags `health` et `checks_failed` (libellés, tronqués) ; artefact
  `report/checks.csv` (contrôle, métrique, valeur, opérateur, seuil, sévérité,
  résultat).

#### PS-31.3 Description du run (onglet *Overview*)

Générée par `RunReport.to_markdown()` et posée dans le tag `mlflow.note.content`,
tronquée à `MAX_DESCRIPTION_CHARS` avec renvoi vers `report/summary.md`. Structure
fixe, dans cet ordre (exemple) :

```markdown
### ⚠️ process_baci_hs2017 — avertissement
Exécution `trade-pipeline-weekly-7k2qd` · env `cloud` · image `sha-6f24c6c` · 3 h 12 min · [DAG Argo](…)

| Unités prévues | Réussies | En échec |
|---:|---:|---:|
| 1 millésime (24 années) | 1 | 0 |

**Contrôles** : 6 ✅ · 1 ⚠️ · 0 ❌ · 0 ⏭️

| | Contrôle | Valeur | Seuil |
|---|---|---:|---|
| ⚠️ | R² de l'équation de gravité | 0,42 | ≥ 0,5 |

**Chiffres clés** : 24 années écrites · 212 M flux · part convertie 97,8 % · taux de
fret médian 4,1 % · pic mémoire 21,4 Go

**Détail** : rapport complet `report/report.html` (Artifacts) · métriques par section
(Model metrics) · ressources (System metrics)
```

Les contrôles réussis ne sont pas détaillés dans la description (seulement comptés) ;
ils figurent dans `report/checks.csv` et le rapport HTML. Les chiffres clés sont
déclarés par étape (PS-31.4) ; ce sont des métriques déjà journalisées, remises en
forme.

Description réduite en cas d'exception non rattrapée (hook `on_node_error`) : titre
`❌ <nœud> — échec`, type et message de l'exception (tronqués), dernière étape atteinte
si connue, lien Argo ; tag `health=failed`.

#### PS-31.4 Contenu par étape (chiffres clés, rapport HTML, tables)

| Nœud | Chiffres clés (description) | Sections et figures du rapport HTML | Tables (`tables/`) |
|---|---|---|---|
| `download_eurostat`, `download_comtrade` | requêtes traitées / en erreur / jamais téléchargées restantes ; budget consommé ; `coverage/eta_days` | couverture par année (barres : part téléchargée) ; requêtes et erreurs au fil du run (`step` = index de requête) ; attentes du limiteur ; audit de couverture par reporter (alertes `EXPECTED_FULL_HISTORY_REPORTERS`) | `coverage_by_year.csv`, `coverage_by_reporter.csv`, `errors.csv` (requête, code HTTP, message) |
| `prepare_baci` | années éligibles par millésime ; part minimale de complétude | complétude par année et millésime (heatmap) | `scope.csv` |
| `process_baci_<millésime>` | années écrites, lignes, part convertie, taux de fret médian, `σ̂` plancher, valeur NES réallouée, durée par passe, pic mémoire | **une section par étape BACI** : conversion (distribution des taux par unité), fobisation (distribution des taux de fret, parts FAS rétablies / tronquées), gravité (coefficients, R², points de Cook exclus), qualité des déclarants (`σ̂` par pays, triés), valorisation et réconciliation (part des flux miroirs retenus par source), NES (valeur réallouée par zone), harmonisation, sortie (lignes par année), temps par passe et par année | `conversion_rates.csv`, `gravity_coefficients.csv`, `sigma_by_country.csv`, `rows_by_year.csv` |
| `compute_partner_vulnerabilities` | unités calculées par raison (`first`, `new_data`, `fingerprint`, `forced`), en attente, en échec ; par flux : nombre de cellules en alerte | distribution de chaque métrique par flux (histogrammes) ; alertes par métrique ; dérive (`drift/*`) ; unités par millésime | `units_by_reason.csv`, `alerts_by_metric.csv` |
| `compute_network_vulnerabilities` | groupes calculés par millésime | distributions des métriques réseau par millésime | `groups_by_vintage.csv` |
| `compute_synthetic_scores` | contextes calculés / à jour / en échec ; méthodes recalculées ; durée | durée par méthode ; contextes par niveau ; `n_jobs` | `contexts.csv` |
| `compute_synthesis_coherence` | contextes calculés ; concordance médiane entre méthodes (statistique `PRIMARY` de PQ-18) | distribution des concordances par paire de méthodes ; paires de métriques les moins cohérentes | `coherence_summary.csv` |
| `publish_serving` | tables publiées, lignes par table, mode (`full` / `by_year`), durée | lignes et fichiers par table | `tables.csv` |
| `maintain_ducklake` | tables maintenues, fichiers avant → après, snapshots expirés, sauvegarde, runs orphelins clos | fichiers par table avant / après | `maintenance.csv` |

Les figures sont construites par `macroforecast/tracking/figures.py` à partir des
métriques et des DataFrames d'artefacts que les étapes produisent déjà ; aucune
relecture de données. Chaque figure a un équivalent tabulaire (CSV) : si le rendu HTML
n'est pas disponible dans l'interface (PQ-19), l'information reste accessible.

#### PS-31.5 Vue d'une exécution complète

- **Liste des runs** d'une expérience, filtre `tags.workflow_id = '<id>'` ; colonnes à
  afficher : `tags.node`, `tags.health`, `metrics.checks/n_failed`,
  `metrics.checks/n_warnings`, statut, durée. L'URL de l'interface conserve filtre et
  colonnes : le runbook §5.1 en donne le modèle. Pour voir toutes les expériences d'une
  exécution d'un coup : sélection de plusieurs expériences dans la liste, puis vue
  comparée (à vérifier sur la version déployée, PQ-19).
- **DAG et statut des pods** : interface Argo (lien dans chaque description).
- **Runs orphelins** : un pod tué (OOM, `activeDeadlineSeconds`) ne peut pas fermer son
  run, qui resterait `RUNNING`. La maintenance `onExit` les passe en `FAILED` avec une
  description « tâche interrompue — voir Argo » (PD-16.6, PR-20).

#### PS-31.6 Points à vérifier (K-03d, K-13)

Constats du 2026-09-21 sur un MLflow **3.15.1 local** (`mlflow server`, magasin fichier, run
fictif de `process_baci_hs.main()` piloté dans un Chromium sans tête ; test
`tests/tracking/test_process_baci_hs_e2e.py`). « À revérifier » = à refaire sur le MLflow d'Onyxia.

1. **HTML Plotly dans *Artifacts*** — **s'affiche et s'exécute** : le fichier est servi dans
   une iframe `blob:` où les scripts tournent (`window.Plotly` défini, 7 figures et leurs SVG
   dessinés, aucune erreur JS) avec `PLOTLY_JS: inline` (fichier de 4,7 Mo, aperçu immédiat).
   Repli PR-19 non nécessaire ; `cdn` non testé (exige un accès internet du navigateur).
   *À revérifier* : la version et les en-têtes de sécurité (CSP) du serveur d'Onyxia.
2. **Longueur d'un tag** — la description de 7 500 caractères est acceptée par le magasin
   (test `test_description_of_maximal_length_is_accepted_by_the_store`) ; la limite de MLflow 3
   est de 8 000 caractères par valeur de tag, `MAX_DESCRIPTION_CHARS` reste à 7 500.
   *À revérifier* : le serveur PostgreSQL d'Onyxia.
3. **Métriques système** — `MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING=true` suffit, sur un run
   ouvert par `mlflow.start_run` : huit métriques `system/*` (CPU, mémoire, disque, réseau)
   et l'onglet *System metrics* peuplé. Un échantillon n'est écrit qu'après un intervalle
   d'échantillonnage : un run plus court n'en a aucun. *À revérifier* sur les runs ouverts par
   kedro-mlflow (K-13) et `psutil` dans l'image.
4. **Description dans *Overview*** — elle est affichée **sous** « About this run », les tags
   et les jeux de données (pas « en tête de page »), **repliée** sur environ quatre lignes
   avec un lien *Show more* : le titre (verdict) et la ligne d'exécution sont visibles, le
   tableau des unités est tronqué. Les **tags** `health` et `checks_failed` (libellés des
   contrôles en défaut) sont, eux, visibles sans clic : c'est là qu'on lit d'un coup d'œil ce
   qui ne va pas. Le Markdown (titres, tableaux, gras, code) est rendu. Ne pas allonger le
   préambule de la description : le verdict et la ligne d'exécution doivent rester les deux
   premières lignes.
5. **Regroupement des métriques** — MLflow 3.15 groupe les graphiques de l'onglet *Model
   metrics* par **préfixe complet** (tout ce qui précède le dernier `/`) : sections `baci`,
   `baci/gravity`, `baci/tonnage`, `checks`, `coverage`, `hs`, `run`… La vue multi-expériences
   et la conservation des colonnes dans l'URL restent à vérifier sur Onyxia.
6. **Magasin fichier** — MLflow 3.15 met le magasin `file:` en mode maintenance et le refuse
   sans `MLFLOW_ALLOW_FILE_STORE=true` (variable posée par les tests et à poser pour un
   `mlflow server --backend-store-uri file:…` local). Sans objet sur Onyxia (PostgreSQL).
7. **Statut d'un run en échec** — avant K-03d, `MlflowTracker.__exit__` clôturait tout run en
   `FINISHED`, même traversé par une exception ; il le clôt désormais en `FAILED`. Les runs
   laissés `RUNNING` par un pod tué relèvent toujours de la maintenance `onExit` (PR-20).

Écarts laissés à K-13 : aucun run n'est ouvert quand un script sort tôt (« rien à
recalculer ») ; BACI n'émet aucune durée par passe ni par année (section « Temps et
ressources » réduite à la durée et au pic mémoire du run) ; les sections de couverture par
année et par déclarant des téléchargements exigeraient une lecture du registre, non faite.

---

## 5. Runbooks d'exploitation

### 5.1 Exécution quotidienne (rien à faire)

Le `CronWorkflow` se lance à 01:00. Contrôle recommandé, sans obligation, en deux
minutes :
1. interface Argo : statut du dernier `trade-pipeline-daily-*`, noter son nom
   (`workflow_id`) ;
2. MLflow, dans chaque expérience concernée (`trade-01-downloads`,
   `trade-03-vulnerabilities`, `trade-04-serving`, `trade-00-maintenance`) : liste des
   runs filtrée par `tags.workflow_id = '<workflow_id>'`, colonnes `tags.health` et
   `metrics.checks/n_failed` (PS-31.5). Tout `ok` : rien à faire ;
3. pour un run `warning` ou `failed` : onglet *Overview* (description : contrôle en
   défaut, unités en échec), puis *Artifacts → report/report.html* pour le détail et
   *System metrics* pour un problème de ressources.

Modèle d'URL de la liste filtrée (à compléter en K-17 avec l'URL réelle et les
colonnes retenues) : `<mlflow>/#/experiments/<id>?searchFilter=tags.workflow_id%3D'<workflow_id>'`.

### 5.2 Ajouter une métrique de vulnérabilité ou une méthode de synthèse

1. Implémenter la classe (métrique) ou déclarer l'entrée `methods` (méthode), avec
   `version = "1"`.
2. L'ajouter à la configuration : `metric_columns`, `SOURCES.COLUMNS` pour la synthèse,
   entrée `methods`.
3. Pousser sur `main` : l'image est publiée, puis mettre à jour le template
   (`render-argo`, `kubectl apply`).
4. Au prochain run quotidien, l'empreinte absente déclenche le calcul de la nouvelle
   métrique ou méthode **sur toutes les unités**. L'évolution de schéma ajoute la colonne
   (métrique). Pour la synthèse, seule la nouvelle méthode et le consensus sont calculés.
5. Pour ne pas attendre : `argo submit --from workflowtemplate/trade-pipeline -p
   kedro-env=cloud` (sans forçage : l'empreinte suffit).

### 5.3 Corriger une formule (métrique, méthode, cohérence, étape BACI)

1. Corriger le code **et incrémenter `version`** (`"1"` → `"2"`) de la classe ou de la
   méthode concernée.
2. Publier l'image et appliquer le template.
3. Le recalcul est automatique au prochain run, sur toutes les unités, avec cascade vers
   l'aval (`reason=fingerprint`).
4. Pour une correction **qui ne change pas le code**, par exemple une donnée amont
   corrigée à la main : forcer
   `argo submit … -p force-steps=partners,synthesis,coherence`.
5. Renommer ou supprimer une colonne de métrique : **opération manuelle**, jamais
   automatique. Faire `ALTER TABLE … RENAME COLUMN` dans une session DuckDB, puis mettre
   à jour la configuration, puis forcer l'étape.
6. Diffuser une colonne calculée à part sur des lignes existantes (migration
   ponctuelle) : `DuckLakeTable.add_columns(df)` (PD-11), depuis un service Onyxia.
7. Changer la **clé primaire** d'une table (ex. ajout de `classification` à
   `indicators`, PD-20) : script de migration dans `tools/` qui recrée la table à partir
   de l'ancienne (testé sur catalogue fichier), exécuté depuis Onyxia (§12), puis
   forçage de l'étape pour les colonnes nouvelles.

### 5.4 Rattrapage (backfill) et suivi de l'avancement

- Suivre `coverage/eta_days` (description des runs de téléchargement) et
  `tables/coverage_by_year.csv` / la figure de couverture du rapport HTML dans MLflow.
- Accélérer temporairement : `argo submit --from workflowtemplate/trade-pipeline -p
  kedro-env=cloud` avec une surcharge de `MAX_RUNTIME` via `config/cloud`, en respectant
  `concurrencyPolicy` : **ne pas** lancer pendant un run quotidien.
- Changer le périmètre ou l'ordre : éditer les filtres de codes (`include*`) et
  `periods_order` dans `config/base` (ou `demo`), puis republier ; il n'y a pas de liste
  de produits prioritaires (PD-06).

### 5.5 Relancer une exécution échouée

`argo retry <workflow>` relance les seules tâches échouées ; les registres garantissent
l'idempotence. Pour repartir de zéro : `argo submit --from workflowtemplate/trade-pipeline`.

### 5.6 Changer d'image

`render-argo --image-tag <sha>` puis `kubectl apply -f kubernetes/generated/`. Retour
arrière : réappliquer le manifeste du commit précédent (`git checkout <sha> --
kubernetes/generated/`).

### 5.7 Faire tourner les secrets

`kubectl create secret generic <nom> --from-literal=… --dry-run=client -o yaml | kubectl
apply -f -` : les pods suivants lisent les nouvelles valeurs, aucun redéploiement
nécessaire.

### 5.8 Restitution (Superset) et supervision (MLflow)

- **Supervision** : dans MLflow uniquement (PS-31). Un run par tâche, nommé
  `<nœud>-<workflow_id>` ; verdict dans la description (*Overview*), détail dans
  *Model metrics*, *System metrics* et *Artifacts* (`report/report.html`). Vue d'une
  exécution : §5.1. **Ajuster un seuil de contrôle** : modifier
  `config/base/parameters_tracking.yml` (`CHECKS`), pousser, republier l'image ; aucune
  donnée n'est recalculée (les contrôles ne font pas partie des empreintes, PS-10.2).
- **Vulnérabilités** : tableau de bord Superset « Vulnérabilités » (PS-30), branché par
  `duckdb-engine` sur le catalogue DuckLake `serving` en lecture seule, rafraîchi par
  `publish_serving` à la fin de chaque exécution `daily` (métriques partenaires) et
  `weekly` (synthèse, cohérence, réseau). Si un graphique affiche des données
  anciennes : vider le cache du graphique (*… → Force refresh*) ; le cache expire de
  toute façon après 24 h (PS-30.1).
- Après modification du tableau de bord dans l'interface : *Export* → remplacer le
  dossier `superset/vulnerabilites/` → commit. Après changement de tables de service
  (`parameters_serving.yml`) : republier (`argo submit … --entrypoint daily`, le
  changement de colonnes force le mode `full`) puis, dans Superset, *Datasets → Sync
  columns from source*.
- Si Superset ne lit plus le catalogue après une mise à jour de DuckDB côté pipeline :
  aligner la version de `duckdb` du service Superset (PR-14).

### 5.9 Relancer BACI à la main (rattrapage ou après correction)

`argo submit --from workflowtemplate/trade-pipeline --entrypoint weekly -p
force-steps=baci -p force-vintages=HS2017` : réestime le millésime complet (PD-22), puis
le réseau, la synthèse et la cohérence en cascade. Durée indicative à consigner après la
première passe complète (K-17).

---

## 6. Stratégie de tests

| Niveau | Contenu | Emplacement | Marqueur |
|---|---|---|---|
| Caractérisation existante | inchangée, **doit rester verte** à chaque prompt | `tests/` | — |
| Unitaires nouveaux | empreintes, décision de fraîcheur, ordre des requêtes, rendu Argo (golden files), évolution de schéma, `FLOWS` | `tests/pipeline/`, `tests/deploy/` | — |
| Intégration locale | catalogue DuckLake **fichier** (`DuckLakeConnector` sur fichier `.ducklake` + dossier temporaire) et S3 simulé `moto` : chaque étape sur données fictives, en séquence, deux fois (idempotence : 2ᵉ passage = 0 unité recalculée) | `tests/pipeline/test_e2e_*.py` | `slow` |
| Kedro | `kedro run --env test` sur jeu fictif (environnement `config/test/`, datasets pointant sur des catalogues fichiers) | `tests/pipeline/test_kedro_run.py` | `slow` |
| BACI par passes | égalité monobloc / passes (PS-14.4) sur données fictives multi-années ; découpage par chapitres ; reprise après interruption (`fit_id`) | `tests/processing/test_baci_streaming.py` | — / `slow` |
| Nomenclatures | `vintage_in_force`, conversion Comext → millésime (n:1 exact, 1:n selon la règle `HsHarmonizer`), égalité ligne en vigueur / ligne du millésime en vigueur | `tests/pipeline/test_vintages.py` | — |
| Couche de service | Catalogue DuckLake **fichier** `serving` + catalogues sources fichiers : colonnes et maille des tables, transaction unique (un snapshot par publication ; `ROLLBACK` complet si une table échoue), partitionnement par `year`, mode `by_year` et retour en `full` sur changement de colonnes ; **lecture concurrente** pendant une publication (ancien état visible jusqu'au `COMMIT`) ; lecture **par SQLAlchemy `duckdb-engine`** avec `ATTACH … (READ_ONLY)`, comme Superset | `tests/serving/` | — / `slow` |
| Rapport de run | `evaluate_checks` (opérateurs, jokers de nœuds, `skipped`, verdict), `to_markdown` (golden file, troncature), `to_html` avec et sans plotly ; publication dans un MLflow `file:` temporaire (description, tags, artefacts, métriques `checks/*`) ; chaque contrôle configuré vise une métrique émise par son étape ; clôture des runs orphelins | `tests/tracking/` | — |
| Rendu | `render-argo --check` en CI ; validation de schéma `argo lint` si le binaire est disponible ; deux points d'entrée, mutex, `onExit` | CI | — |
| Recette cluster | exécution `demo` réelle (K-17), check-list §9 | manuel | — |

Les données fictives sont générées dans `tests/pipeline/fixtures.py` : 3 reporters,
5 produits HS6 de 2 chapitres, 4 années, 2 flux, partenaires `WORLD` / `EXT_EU` /
individuels, et un Comtrade miroir cohérent pour BACI. Aucun test ne requiert le réseau
ni le cluster.

---

## 7. Risques

| ID | Risque | Probabilité / impact | Mitigation |
|---|---|---|---|
| PR-01 | Rattrapage complet long (Eurostat : dizaines à centaines de milliers de requêtes ; Comtrade : ~ 540 lots × 34 ans ≈ 18 000 requêtes) : plusieurs jours à semaines | Certaine / fort | Calcul progressif (PD-06), priorités (PS-12), lots de produits (PD-07), périmètre `demo` pour la présentation, `coverage/eta_days` |
| PR-02 | Coût quadratique des registres et écritures par requête (C-08) | Certaine à grande échelle / fort | PS-27 **avant** le rattrapage complet ; en phase 0, périmètre `demo` limité |
| PR-03 | Limites de l'API Eurostat (longueur d'URL, taille de réponse, bascule asynchrone SDMX 3.0) avec des lots de produits | Moyenne / moyen | Mesure dans K-01 : lots de 1, 10, 50 codes sur un reporter ; valeur retenue documentée |
| PR-04 | Quotas de la clé Comtrade premium (appels/jour, enregistrements par appel) | Moyenne / fort | Rate limiter `statflows`, métriques `rate_limit/*`, budget `MAX_RUNTIME` ; découper plus finement si `max_records` est atteint |
| PR-05 | BACI : une tranche annuelle récente dépasse la mémoire du pod | Faible / moyen | Découpage par chapitres SH2 (PS-14.5), `MAX_ROWS_PER_CHUNK`, `memory/peak_mb` suivi ; `baci-large` relevable jusqu'à 200 Gi |
| PR-05b | BACI : écart numérique entre l'implémentation par passes et l'ancienne (covariance robuste de `linearmodels`, distance de Cook) | Moyenne / moyen | Tests d'équivalence à `1e-8` (PS-14.4) ; reproduction de la formule exacte de `linearmodels` lue dans son code ; en cas d'écart irréductible documenté, incrément de `version` BACI |
| PR-05c | BACI : six passes sur les Parquet de travail → durée d'une passe hebdomadaire de plusieurs heures par millésime | Élevée / faible | Cadence hebdomadaire (PD-23), fan-out par millésime, projection de colonnes, `KEEP_WORK_FILES` pour la reprise ; durée mesurée en K-17 |
| PR-06 | Quota **total** du namespace insuffisant pour 7 pods BACI simultanés (7 × 32 Gi) | Moyenne / moyen | `parallelism` du template ; machine types de PS-20 ; PQ-16 |
| PR-07 | Service PostgreSQL Onyxia non persistant ou supprimé → perte des catalogues DuckLake | Faible / critique | Sauvegarde hebdomadaire `pg_dump` vers S3 (PD-16) ; données Parquet conservées sur S3 ; procédure de restauration à documenter (K-17) |
| PR-08 | argo-kedro 0.1.x : API instable, commande `run` globale remplacée | Moyenne / moyen | Version épinglée, rendu maison n'utilisant que `get_argo_dag`, tests golden ; repli possible sur un calcul de DAG maison (`pipeline.grouped_nodes`) |
| PR-09 | Dépendances aval tolérantes à l'échec : calculs sur des données partielles pendant un incident prolongé | Faible / moyen | Registres et watermarks, alertes MLflow, workflow marqué `Failed` |
| PR-10 | Accès au cluster : jetons liés DPoP, impossible depuis le poste | Certaine / moyen | Déployer depuis un terminal de service Onyxia (VSCode) ou avec un jeton court copié de l'interface (PQ-09) |
| PR-11 | Métriques d'export non validées méthodologiquement | Moyenne / moyen | `FLOWS: [import]` par défaut (PD-09) |
| PR-12 | Coût mémoire de JAX/Kantorovitch en parallèle `loky` | Moyenne / moyen | `n_jobs` spécifique à la méthode (`kantorovich` exécuté en séquentiel dans le processus parent), préallocation XLA désactivée |
| PR-13 | Image lourde (JAX + MLflow 3) : démarrage de pod lent | Certaine / faible | Cache de nœud, `imagePullPolicy: IfNotPresent` avec étiquettes immuables (SHA) |
| PR-14 | Version de DuckDB / de l'extension `ducklake` de l'image Superset **incompatible** avec celle du pipeline (catalogue illisible, ou lisible mais écrit dans un format plus récent) | Moyenne / fort pour la démonstration | Vérification en tête de K-03c (PS-30.1 point 0) ; épingler `duckdb==1.5.3` dans la configuration du chart ; toute montée de version DuckDB du pipeline s'accompagne de celle de Superset (§5.8). *(Vérifié en K-03c, 2026-09-22, conditions réelles : DuckDB 1.5.5 (Superset) lit sans erreur un catalogue `serving` écrit par DuckDB 1.5.3 (pipeline, cette même exécution) — `SHOW ALL TABLES`, `DESCRIBE`, lecture filtrée correctes ; écriture refusée. **Compatible en l'état, aucun épinglage nécessaire.** À revérifier à chaque montée de version de l'un des deux côtés.)* |
| PR-15 | Latence des graphiques Superset (lecture de Parquet sur S3 à chaque requête non mise en cache) sur les volumes de production (`cell_scores` ~30 M, `flows` ~80 M lignes) | Moyenne / moyen | Partitionnement par `year` et tri d'insertion (PS-29.1), cache des graphiques 24 h, `TOP_PARTNERS`, `serving.YEARS_BACK`, `threads`/`memory_limit` de la connexion ; en dernier recours, tables d'agrégats dédiées dans `serving`. *(Mesuré en K-03c, 2026-09-22, volumes `demo` — `cell_scores` ~0,29 M lignes, `flows` ~2,8 M : requête filtrée `year`+`reporter` — **0,24 s à froid, 0,04 s à chaud** (SQL Lab, `threads=4`, `memory_limit=4GB`). Largement sous l'objectif de 5 s ; à remesurer aux volumes de production (K-17).)* |
| PR-16 | Tables `indicators` multipliées par ~3,6 au niveau SH6 (PD-20) : durée du calcul partenaires et volume | Certaine / faible | `VINTAGES` configurable, calcul incrémental par unité, partition par `classification` |
| PR-17 | Recouvrement `daily`/`weekly` : `publish_serving` lu pendant une publication | Faible / faible | Publication en une transaction DuckLake (Superset lit le snapshot précédent jusqu'au `COMMIT`) ; mutex Argo (écrivain unique) |
| PR-18 | Code reporter `EU27_2020` absent ou différent dans DS-045409 | Moyenne / moyen | PQ-17 : vérification de la codelist au premier téléchargement ; repli : agrégation des membres avec partenaires extra-UE seulement (documentée comme approximation) |
| PR-19 | L'onglet *Artifacts* de MLflow n'exécute pas le JavaScript d'un HTML Plotly (iframe restreinte) : figures invisibles | Moyenne / faible | Chaque figure a son équivalent CSV (PS-31.4) ; repli PNG statique (`matplotlib`, `log_figure`) ; vérifié en K-03d (PQ-19) : le HTML Plotly **s'exécute** dans l'aperçu de MLflow 3.15, repli non utilisé ; à revérifier sur Onyxia |
| PR-20 | Pod tué (OOM, dépassement de délai) : le run MLflow reste `RUNNING` sans rapport, et un échec passe inaperçu dans MLflow | Moyenne / moyen | Clôture des runs orphelins par la maintenance `onExit` (PD-16.6) ; tag `health` absent = run à regarder ; statut du workflow dans Argo |
| PR-21 | Seuils de contrôle mal calibrés : fausses alertes (bruit) ou alertes manquées | Certaine au début / faible | Valeurs initiales `warning` sauf évidences ; recalibrage en K-17 sur les premières exécutions réelles ; seuils en configuration (§5.8) |
| PR-22 | Données fictives prises pour des mesures : écrites dans un catalogue de production, restées dans un tableau de bord, ou données `demo_*` mélangeant réel et fictif conservées après le retour au réel | Faible avec la garde / **critique** (décisions sur des chiffres simulés) | Garde `REQUIRED_CATALOG_PREFIX` avant toute connexion (PD-24, testée), marquage `synthetic` des registres, mention « données simulées » sur tout support de démonstration, retrait K-17b (contrôle de non-contamination) puis K-18 |

---

## 8. Questions ouvertes et hypothèses retenues

| ID | Question | Hypothèse retenue par défaut (le travail avance avec) |
|---|---|---|
| PQ-01 | ~~Quelles sont les ressources maximales du namespace ?~~ **Résolu (2026-09-18)** : ≤ 100 pods actifs, 100 m à 30 000 m CPU et 1 à 200 Gi par pod, délai de démarrage croissant avec la demande. Reste le quota **total** (PQ-16). | Machine types de PS-20 ; `parallelism: 8` |
| PQ-02 | ~~Le service **Argo Workflows** est-il lancé dans le namespace, dans quelle version, avec quel **service account** ?~~ **Résolu (2026-09-20)** : Argo Workflows **v3.6.10** (contrôleur et serveur) dans `user-qbollietdgddi` ; service account **`workflow`** (`argo-workflow-sa` existe aussi) ; `schedules:` (liste) supporté ; pas de CLI `argo` dans le pod VSCode (validation `kubectl apply --dry-run=server`, soumission par `kubectl create` d'un `Workflow`). | — |
| PQ-03 | Service **MLflow** : URL interne au cluster, authentification, bucket d'artefacts ? | Service lancé par l'utilisateur ; URL interne `http://<service>.user-qbollietdgddi.svc.cluster.local:5000`, sans authentification, artefacts `s3://qbollietdgddi/mlflow` |
| PQ-04 | Service **PostgreSQL** des catalogues DuckLake : existe-t-il déjà (les scripts tournaient sur Onyxia avec `PGHOST`…) ? Volume persistant ? Nom d'hôte interne ? | Service Onyxia PostgreSQL existant et persistant ; ses identifiants seront placés dans `trade-postgres-credentials` |
| PQ-05 | ~~Millésimes BACI : faut-il HS1992 dès 1992 ?~~ **Résolu (2026-09-18)** : l'analyse Comtrade commence en **1994** ; `HS1992` a `START_YEAR: 1994`, plus d'étiquette `low_coverage`. | — |
| PQ-06 | ~~Définition des vulnérabilités à l'export~~ **Résolu (2026-09-18)** : `CDI2` export = exports extra-UE / exports totaux ; `CDI3` export = exports extra-UE / imports totaux (miroirs formels, PD-09) ; `FLOWS: [import, export]` en `base`. | — |
| PQ-07 | **Périmètre de la présentation** : quels produits (une dizaine à une centaine de codes SH6), quelles années, quels pays ? | Liste d'exemple de PS-04.4, années ≥ 2015, EU27 + `EU27_2020` |
| PQ-08 | Le dépôt `qbolliet/trade-analysis` est-il (ou peut-il devenir) **public** ? (Pages gratuit, minutes Actions illimitées) | Public. Sinon : site de documentation publié comme artefact de CI ou via un service Onyxia statique |
| PQ-09 | Comment obtenir un accès `kubectl` utilisable par Claude Code ? | Exécuter les prompts de déploiement depuis un **terminal VSCode Onyxia** (service account du pod) ou coller un jeton court frais |
| PQ-10 | ~~Accepte-t-on un traitement BACI par fenêtres glissantes ?~~ **Résolu (2026-09-18)** : **non** ; fidélité à la méthodologie originale par statistiques suffisantes (PD-22, PS-14), aucune approximation. | — |
| PQ-11 | Faut-il calculer des **métriques partenaires pour les pays non-UE**, à partir de BACI (Eurostat ne couvre que les reporters UE) ? | Hors périmètre de cette architecture ; prévu comme extension (nouvelle source de grille `baci_partners`) |
| PQ-12 | Faut-il conserver les **flux mensuels** (`C_M_HS`) ? | Non : seul l'annuel est ordonnancé |
| PQ-13 | ~~**Superset** est-il disponible dans le catalogue Onyxia, avec `duckdb-engine` ?~~ **Résolu (2026-09-18, rév. 2)** : `duckdb-engine` est installé dans le chart Superset d'Onyxia et l'accès du service au bucket S3 est assuré. Restent à relever en K-03c : version de Superset, version de `duckdb` dans l'image (PR-14), mécanisme d'`ATTACH` retenu (PS-30.1). *(Relevé en K-03c, 2026-09-22 : Superset **4.1.1** ; `duckdb` **1.5.5**, `duckdb-engine` **0.17.0** dans le pod, installés par le `bootstrapScript` du chart à chaque démarrage — non figés dans l'image ; extensions `ducklake`/`httpfs`/`postgres` installables à la volée (accès réseau sortant du pod). Précision sur « l'accès S3 est assuré » : c'est la **joignabilité réseau** à `minio.lab.sspcloud.fr` qui est assurée (confirmé : un essai sans identifiants obtient un `403 Access Denied`, pas une erreur réseau) — **aucune identifiant S3 n'était configuré** dans le pod Superset avant K-03c ; ce n'est pas une régression, c'est justement l'objet de PS-30.1 point 1. Câblé en K-03c en réutilisant `trade-postgres-credentials`/`trade-s3-credentials` (aucun identifiant dédié à Superset créé).)* | Lecture directe du catalogue `serving` (PD-21) |
| PQ-14 | La base de métadonnées du catalogue `serving` peut-elle vivre sur la **même instance PostgreSQL** que les autres catalogues ? | Oui (métadonnées seulement, les données sont sur S3) ; rôle `superset_reader` en lecture seule sur cette base |
| PQ-15 | ~~Le client Eurostat de `statflows` accepte-t-il `startPeriod`/`endPeriod` ?~~ **Résolu (2026-09-18, phase 0)** : oui. `EurostatQueryRequestV30` porte les champs `start_period`/`end_period`, transmis en SDMX 3.0 par `c[TIME_PERIOD]=ge:<start>+le:<end>` (`statflows/sources/eurostat/endpoints.py`) et conservés par `fetch_updates` (qui n'ajoute que `lastNObservations`). Ces champs **n'entrent pas** dans `identity_key` (seules les `dimensions` y figurent) : les entrées de registre existantes restent valides. Le script Eurostat envoie `start_period = runtime.ANALYSIS_START_YEAR.eurostat` ; `PERIOD_WINDOWS` reste non implémenté (`NotImplementedError`). | — |
| PQ-16 | ~~**Quota total** du namespace (somme CPU/mémoire des pods actifs) ?~~ **Résolu (2026-09-20)** : `onyxia-quota` ne borne **ni CPU ni mémoire** au total ; il limite `count/pods` à 100, GPU à 1 (`nvidia.com/gpu`) et `requests.storage` à 2 Ti. **Aucun `LimitRange`** dans le namespace. Seules les limites par pod (PQ-01) contraignent : les ressources de départ du workflow de transition (4 CPU / 32 Gi pour BACI, 4 / 16 Gi ailleurs) sont conservées. | — |
| PQ-17 | Le reporter agrégé **`EU27_2020`** existe-t-il dans la codelist `reporter` de DS-045409 avec des flux extra-UE ? **Non vérifiable hors ligne (2026-09-18)** : aucune structure DS-045409 en cache dans le dépôt. `EU27_2020` est ajouté à `reporter.include` ; **à vérifier au premier téléchargement** (un code inclus absent de la codelist est signalé par un avertissement de `filter_codes`, sans échec). | Oui ; sinon repli de PR-18 |
| PQ-18 | Quelle **statistique de cohérence** et quelle **méthode de synthèse** afficher par défaut dans le tableau de bord (« indicateur synthétique le plus pertinent ») ? | `PRIMARY_METHOD: borda` (consensus) et corrélation de Spearman ; changeable en configuration `serving` |
| PQ-19 | Sur le MLflow 3 déployé : le rapport HTML Plotly s'affiche-t-il dans *Artifacts* ? Quelle longueur maximale pour la description (tag) ? Les métriques système s'activent-elles par variable d'environnement sur les runs de kedro-mlflow ? La vue multi-expériences existe-t-elle ? | Oui pour tout ; limite de tag 8 000 caractères ; replis de PS-31.6 sinon. Vérifié en K-03d sur MLflow 3.15 local : **oui, oui (7 500 caractères acceptés ; limite de MLflow 3 : 8 000) et oui** (variable d'environnement suffisante sur un run `mlflow.start_run`) ; vue multi-expériences non vérifiée ; constats détaillés en PS-31.6 ; à refaire sur le MLflow d'Onyxia |
| PQ-20 | À partir de quel seuil les **données réelles sont-elles « complètes »** pour retirer le demo (K-17b) ? | Toutes les requêtes Comext planifiées présentes au registre ; Comtrade : `COMPLETENESS.MIN_SHARE` (1,0) atteint sur toutes les années de `ANALYSIS_START_YEAR.comtrade` à l'année complète la plus récente ; au moins une exécution `daily` et une `weekly` de production réussies (PR-01 : plusieurs jours à semaines). K-17b mesure ces critères et **s'arrête** s'ils ne sont pas remplis |

---

## 9. Actions manuelles à la charge de l'utilisateur

Claude Code ne peut pas les faire à votre place : interface Onyxia, secrets saisis à la
main, réglages GitHub. À faire **avant** K-03 (phase 0), sauf mention contraire.

1. **Accès cluster** (PQ-09) : ouvrir un service VSCode Onyxia avec un rôle Kubernetes
   `admin` sur le namespace, y cloner le dépôt et y lancer Claude Code pour les prompts
   K-03, K-15 et K-17 ; ou fournir un kubeconfig à jeton court valide.
2. **PostgreSQL** (PQ-04) : vérifier que le service existe et est persistant ; relever
   hôte, port, utilisateur, mot de passe, base d'administration, puis :
   ```bash
   kubectl create secret generic trade-postgres-credentials \
     --from-literal=PGHOST='…' --from-literal=PGPORT='5432' \
     --from-literal=PGUSER='…' --from-literal=PGPASSWORD='…' \
     --from-literal=PGDATABASE='defaultdb' --from-literal=PGADMINUSER='…'
   ```
3. **MLflow** (PQ-03) : lancer le service MLflow depuis le catalogue Onyxia (persistance
   activée), relever son URL **interne** au cluster, puis :
   ```bash
   kubectl create secret generic trade-mlflow-credentials \
     --from-literal=MLFLOW_TRACKING_URI='http://…:5000'
   ```
4. **Argo Workflows** (PQ-02) : vérifier que le service est lancé ; relever sa version
   (`argo version`) et le service account utilisable par les workflows.
5. **Secrets existants** : vérifier les noms `trade-s3-credentials` et
   `comtrade-api-credentials` (`kubectl get secrets`).
6. **GitHub** : rendre le dépôt public ou confirmer qu'il reste privé (PQ-08) ; après le
   premier `image.yml`, passer le paquet GHCR `trade-analysis` en **Public** (*Package
   settings → Change visibility*) ; activer Pages (*Settings → Pages → GitHub Actions*).
7. **Périmètre de présentation** (PQ-07) : valider ou remplacer la liste de produits de
   PS-04.4.
8. ~~Méthodologie export (PQ-06) et HS1992 (PQ-05)~~ : tranchées le 2026-09-18.
9. **Quota total** (PQ-16) : relever `kubectl describe resourcequota` (K-03 le fait).
10. **Lecture du catalogue `serving` par Superset** (PQ-14, PD-21) : **après** la
    première publication (K-03b exécuté sur Onyxia, qui crée la base de métadonnées
    `serving` et ses tables), créer le rôle en lecture seule sur l'instance
    PostgreSQL :
    ```sql
    CREATE ROLE superset_reader LOGIN PASSWORD '…';
    GRANT CONNECT ON DATABASE serving TO superset_reader;
    \c serving
    GRANT USAGE ON SCHEMA public TO superset_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO superset_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO superset_reader;
    ```
    (schéma des tables de métadonnées DuckLake à confirmer par `\dt` ; aucun secret
    Kubernetes à créer pour le pipeline.)
11. **Superset** : le service existe dans le catalogue Onyxia avec `duckdb-engine` et
    l'accès S3 (PQ-13). Le lancer (persistance activée) et relever son URL ; la
    connexion DuckDB au catalogue `serving` est créée **dans K-03c** (PS-30.1), avec les
    identifiants de `superset_reader` saisis par vous. À faire **avant K-03c**.

---

## 10. Phasage et planning jusqu'à la présentation

Révision du **vendredi 2026-09-18**. Présentation : **semaine du 2026-09-21**.

| Phase | Prompts | Objectif | Échéance cible |
|---|---|---|---|
| **0 — Démonstration** | K-01, K-02, K-03 🔌 (+ §9 points 1 à 5, 7, 11) | Scripts corrigés, image GHCR, workflow de transition sur le périmètre `demo` : **téléchargements lancés** | J+1 |
| 0 bis | K-03b, K-03d, **K-03c 🔌** (+ §9 point 10) | Catalogue `serving` publié, **tableau de bord Superset** « Vulnérabilités » construit sur les produits `demo` ; **rapport de contrôle MLflow** sur les runs des scripts | J+2 → J+4 |
| 🎯 **Jalon tableau de bord** | — | Vulnérabilités + synthèse + cohérence pour ~100 produits visibles dans Superset | avant la présentation |
| **1 — Robustesse de l'acquisition** | K-04 (dépôt `statflows`), K-04b | Registres et écritures tamponnés, options d'écriture 0.3.1 : prérequis du rattrapage complet | après la présentation |
| **2 — Méthodologie paramétrable** | K-05, K-06, K-06b, K-07, K-08, K-09 | Fraîcheur v2, import/export, millésimes de nomenclature, BACI exact par passes, évolution de schéma et synthèse incrémentale, parallélisme | après la présentation |
| **3 — Kedro** | K-10, K-11, K-12, K-13, K-14 | Projet Kedro `kedro_pipeline`, étapes partagées, pipelines (deux cadences), rapport de run kedro-mlflow, maintenance | après la présentation |
| **4 — Production** | K-15, K-16, K-17 🔌, **K-17b 🔌**, K-18 | Rendu Argo (deux CronWorkflow), documentation, recette, **retrait des données de démonstration et fictives** (K-17b), puis suppression des scripts, du code fictif et des fichiers de démonstration (K-18) | après la présentation |

Ce qui sera montrable à la présentation (phase 0 seule) : le tableau de bord Superset
(page pays avec la France et l'Union, page produit, cohérence), les rapports de
contrôle MLflow des runs de la semaine (description, contrôles, rapport HTML BACI),
métriques partenaires sur les produits `demo`, BACI
`demo` sur au moins une année complète, réseau sur ce BACI, scores synthétiques et
diagnostics de cohérence sur les contextes disponibles, avec la mention explicite du
caractère **provisoire** (périmètre réduit). Les évolutions temporelles (PS-30, blocs
12-13) n'auront qu'une profondeur de quelques années en `demo` (2015+), ce qui est
accepté.

---

## 11. Références

- Code : `scripts/*.py`, `macroforecast/tracking/`, `macroforecast/trade/vulnerabilities/`,
  `macroforecast/trade/processing/baci.py`, `macroforecast/trade/aggregation/`.
- Dépendances : `statflows/core/download.py` (`SDMXDownloader`, `download_updates`),
  `statflows/sources/{comtrade,eurostat}/client.py` (`fetch_updates`),
  `statflows/storage/ducklake/tables.py` (`write_dataframe`),
  `dt_ducklake_manager/maintenance/compaction.py` (`DuckLakeMaintenance`).
- Plugins : argo-kedro <https://github.com/everycure-org/kedro-argo> (`argo_kedro/argo/__init__.py:get_argo_dag`,
  `templates/argo_wf_spec.tmpl`, `config/kedro_argo_config.py`) ; kedro-mlflow
  <https://github.com/Galileo-Galilei/kedro-mlflow> ; kedro-viz (`kedro viz build`).
- Documents du dépôt : `AGREGATION_ARCHITECTURE.md` (décisions D-xx de la synthèse),
  `Methodologie synthese multicritere.tex`, `BACI - Méthodologie détaillée succinte.tex`,
  `ssrn-1994500.pdf` (Gaulier G., Zignago S., *BACI: International Trade Database at
  the Product-level — The 1994-2007 Version*, CEPII WP 2010-23 : §2.3.2 équation de
  gravité sur données empilées avec indicatrices d'année, §2.4 ANOVA de qualité avec
  produit absorbé par transformation within, appendice sur les zones NES),
  `Trade deployment.md` (création des secrets).
- `dt-ducklake-manager 0.3.1` : `DatabaseUpdater.update_database(allow_new_columns=…)`,
  `DatabaseUpdater.add_columns`, `delete_rows`, `run_id`/`commit_message` sur snapshot.
- DuckLake : procédures `ducklake_merge_adjacent_files`, `ducklake_rewrite_data_files`,
  `ducklake_expire_snapshots`, `ducklake_cleanup_old_files`, `ducklake_flush_inlined_data`
  (disponibilité à vérifier sur la version embarquée par DuckDB 1.5.3).
- Superset : documentation *Creating your first dashboard*, *Cross-filtering*,
  *Native filters*, *Importing and exporting datasources and dashboards*
  (<https://superset.apache.org/docs/>), page *Connecting to Databases → DuckDB* ;
  `duckdb-engine` (<https://github.com/Mause/duckdb_engine>, `connect_args`,
  `preload_extensions`) ; DuckLake : `ATTACH 'ducklake:postgres:…' (READ_ONLY)`,
  partitionnement (`ALTER TABLE … SET PARTITIONED BY`), secrets DuckDB persistants
  (<https://ducklake.select/docs/>, <https://duckdb.org/docs/configuration/secrets_manager>).
- MLflow 3 : description de run (tag `mlflow.note.content`), métriques système
  (`MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING`, `psutil`), `log_text` / `log_table` /
  `log_figure`, recherche de runs par tag (<https://mlflow.org/docs/latest/>).

---

## 12. Opérations à exécuter depuis Onyxia

Ces opérations ne peuvent pas être réalisées (ou pas sans risque d'erreur) depuis le
poste local : elles touchent au cluster, aux services Onyxia ou aux données réelles. Les
prompts marqués 🔌 dans `PIPELINE_PROMPTS.md` doivent être lancés depuis un **service
VSCode Onyxia** (rôle Kubernetes `admin` sur le namespace, dépôt cloné, `uv` installé),
les autres depuis le poste local.

| Opération | Quand | Prompt / action | Pourquoi Onyxia |
|---|---|---|---|
| Création des secrets (`trade-postgres-credentials`, `trade-mlflow-credentials`) | avant K-03 | §9, à la main | valeurs secrètes saisies dans le cluster |
| Lancement des services MLflow, PostgreSQL (vérification), Superset | avant K-03 / K-03c | §9, catalogue Onyxia | services du namespace |
| Création du rôle `superset_reader` sur la base de métadonnées `serving` | après la 1ʳᵉ publication (K-03b), avant K-03c | §9 point 10 | accès réseau à PostgreSQL |
| Vérification du rendu du rapport de run (description, HTML, métriques système) sur le MLflow du namespace | K-03d (fin), K-13 | exécution d'un script `demo`, interface MLflow | serveur MLflow réel (PQ-19) |
| Relevé des quotas (`resourcequota`, `limitrange`), version d'Argo, service account | K-03 étape 0 | K-03 🔌 | `kubectl` |
| Déploiement et première exécution du workflow de transition ; diagnostic des pods | K-03 | K-03 🔌 | `kubectl`, logs |
| Première publication de la couche de service et vérification des volumes | K-03b (exécution), K-03c | `argo submit`, session DuckDB | données réelles, catalogue `serving` |
| Connexion DuckDB de Superset, construction du tableau de bord, export versionné | K-03c | K-03c 🔌 | interface et pod Superset du namespace |
| Mesure des limites de l'API Eurostat pour `products_step` (PR-03) | K-01 ou K-04b | local possible (réseau public), **mais** l'adresse IP et le limiteur diffèrent en production → mesurer aussi depuis un pod | débit réel |
| Migration des registres v1 → v2 sur S3 (`adopt_legacy_fingerprints`) | après K-05 | `tools/migrate_registries.py` depuis un pod ou VSCode Onyxia | S3 réel, irréversible : sauvegarder le préfixe avant |
| Recréation de la table `indicators` avec la nouvelle clé (`classification`) | après K-06b | runbook §5.3, depuis un service Onyxia | catalogue et Parquet réels |
| Première passe BACI par millésime sur données réelles ; relevé de `memory/peak_mb` et de la durée | après K-07 | `argo submit --entrypoint weekly` | seul moyen d'avoir les vraies volumétries |
| Déploiement des manifestes générés, bascule, recette, runbooks | K-17 | K-17 🔌 | `kubectl apply`, `argo` |
| **Retrait des données de démonstration et fictives** : bases `demo_*`, schémas `demo_*`, préfixes S3 `trade/demo/`, registres du profil, expériences MLflow `demo-*`, objets Superset du demo, workflow de transition ; contrôle de non-contamination de la production | K-17b (après K-17, données réelles complètes — PQ-20) | K-17b 🔌, confirmation objet par objet | suppressions **irréversibles** sur PostgreSQL, S3, MLflow, Superset |
| Suppression du code et des fichiers de démonstration (dépôt) | K-18 (après K-17b) | K-18 (local) | — |
| Restauration d'une sauvegarde PostgreSQL (test) | K-17 | K-17 🔌 | instance réelle |

Tout ce qui n'est pas dans cette table (code, tests sur données fictives, rendu des
manifestes, image Docker, documentation) se fait en local.
