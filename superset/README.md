# Tableau de bord Superset « Vulnérabilités »

Construit via l'API Superset (voie « Assets as code », K-03c) sur le catalogue DuckLake
`serving`, profil `demo`. Ce document explique le mécanisme retenu, comment le
réimporter/republier, et les pièges rencontrés — utile même si tu repasses par l'API,
et indispensable si tu dois un jour reconstruire un graphique à la main dans l'interface.

## Ce qui a été construit

- Connexion DuckDB `serving` (id Superset : voir `databases/serving.yaml`), lecture
  seule, catalogue `serving` attaché à chaque connexion par un écouteur SQLAlchemy
  `connect` (mécanisme (b) de PS-30.1 — voir plus bas).
- 8 datasets (`cell_scores`, `flows`, `coherence_metrics`, `coherence_methods`,
  `products`, `countries`, plus 2 techniques : `cell_scores_metrics_long` pour la
  heatmap).
- 15 graphiques + 1 tableau de bord à 2 onglets (« Pays », « Produit »), exportés dans
  ce dossier.

## Mécanisme de connexion retenu (PS-30.1)

**Mécanisme (b) uniquement** — l'écouteur `connect` dans `superset_config.py`. Le
mécanisme (a) (secret DuckDB persistant sur disque) a été écarté : le pod Superset n'a
**pas de volume persistant** pour `~/.duckdb/stored_secrets`, et il y a **deux pods**
(web + worker Celery, qui exécute SQL Lab en asynchrone) qui devraient partager le même
secret — impossible sans volume commun.

Concrètement (appliqué hors Helm, directement sur les Secrets Kubernetes rendus par le
chart — voir « Pièges » ci-dessous) :

- Secret `superset-899573-env` : ajout de `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`,
  `PGDATABASE` (copiés de `trade-postgres-credentials`) et `S3_ACCESS_KEY`,
  `S3_SECRET_KEY` (copiés de `trade-s3-credentials`).
- Secret `superset-899573-config`, fichier `superset_config.py` : un écouteur
  `sqlalchemy.event.listen(Engine, "connect", ...)` qui, uniquement pour les connexions
  DuckDB (`type(dbapi_connection).__module__ == "duckdb_engine"`, discriminant qui
  évite de perturber les connexions PostgreSQL/Redis), charge `ducklake`/`httpfs`, crée
  un secret S3 en mémoire et attache `serving` en lecture seule (`ATTACH … (READ_ONLY)`,
  sans `DATA_PATH` explicite : il est lu depuis les métadonnées du catalogue, comme
  documenté en PS-29.1).

## Réimport / republication

```bash
# Dans le conteneur Superset (ou via l'UI Dashboards -> Import) :
superset import-dashboards -p superset/vulnerabilites -u admin
```

L'export ne contient **aucun mot de passe** (`databases/serving.yaml` n'a qu'une URI
`duckdb:///:memory:` et des `engine_params` non sensibles : les identifiants vivent dans
l'environnement du pod, jamais dans l'export). Après import, Superset redemande de
confirmer la connexion existante (ou d'en créer une) si `database_name` ne correspond
pas exactement.

## Passage `demo_dashboard` → `dashboard` (PS-30.5)

Les datasets pointent sur le schéma `serving.demo_dashboard`. Pour bascule en
production, remplacer `demo_dashboard` par `dashboard` dans chaque
`datasets/serving/*.yaml` (clé `schema`, et dans le `sql` du dataset virtuel
`cell_scores_metrics_long`), puis réimporter (`--overwrite`).

## Pièges rencontrés

- **Version de chart Helm introuvable** : le chart exact déjà déployé
  (`superset-0.1.12`) n'est plus dans l'index du dépôt (seules des versions 1.x y
  restent) — un `helm upgrade --reuse-values` aurait pu changer bien plus que prévu. Les
  deux Secrets (`*-env`, `*-config`) ont donc été patchés **directement** (`kubectl
  patch`), hors Helm : si le service est un jour redéployé/relancé depuis l'UI Onyxia,
  **ce patch sera perdu** et doit être rejoué (voir section précédente).
- **`year` non filtré par défaut → tableau « toujours au même rang »** : le calcul
  automatique « première valeur » (`controlValues.defaultToFirstItem`) d'un filtre natif
  ne s'est pas déclenché de façon fiable via l'API — sans année fixée, les 38 années
  (1988-2025) se mélangent et de nombreux produits affichent chacun un rang 1 (un par
  année), ce qui ressemble à un rang constant. **Toujours donner une valeur par défaut
  explicite** (`defaultDataMask.filterState.value`) à un filtre natif créé par l'API,
  plutôt que de compter sur le calcul automatique.
- **Colonnes entières non temporelles** : `year` est un entier, pas une date — la
  colonne calculée `period = make_date(year, 1, 1)` (marquée *Is temporal*) est
  nécessaire pour les graphiques en série temporelle (Line chart).
- **`MAX` vs `AVG` sur une table à la maille cellule** : `cell_scores` a une ligne par
  `(classification, reporter, product, flow, year)` — `MAX(HHI)` y équivaut à la valeur
  elle-même (pas d'agrégation réelle), c'est voulu (PS-30.1 point 2). Une heatmap
  « produits × métriques », en revanche, a besoin d'un format **long** (une ligne par
  paire produit/métrique) : `cell_scores` étant large (une colonne par métrique), un
  dataset virtuel dédié (`cell_scores_metrics_long`, `UNION ALL` des colonnes `*_norm`)
  a été créé spécifiquement.
- **`heatmap_v2` a besoin de DEUX dimensions catégorielles** (`all_columns_x` et
  `all_columns_y`) : lui en donner une seule (ex. juste le produit) produit une
  « Unexpected error » sans détail exploitable côté API.
- **Un graphique `viz_type: "markdown"` créé via `POST /api/v1/chart/` échoue** (« Empty
  query ? ») : le Markdown n'est **pas** un type de graphique interrogeable, c'est un
  composant de mise en page natif du tableau de bord (`position_json`, type
  `"MARKDOWN"|"code"`), pas un `Chart` de la table `slices`. Utiliser un bloc Markdown de
  layout, pas un chart.
- **Table de cohérence sans filtre `metric_a IS NOT NULL AND metric_b IS NOT NULL`** :
  `coherence_metrics`/`coherence_methods` mélangent des statistiques **par paire**
  (`spearman`, `kendall_tau_b`…) et des statistiques **globales** sans deuxième métrique
  (`kmo`, `bartlett_p`, `n_complete`, `axis1_share`…, `metric_b` vaut alors `NULL`) —
  sans ce filtre, une table triée alphabétiquement par `statistic` peut n'afficher que
  ces dernières.
- **Format `PERCENT_1_POINT` invalide** : ce n'est pas un format D3 reconnu par cette
  version — utiliser un format D3 valide (`".1%"`).
- **Portée des filtres natifs par onglet** : le filtre `reporters_compare` est scopé à
  `TAB-PRODUIT` (`scope.rootPath`) pour ne pas polluer l'onglet Pays ; le bloc « Union »
  **exclut** le filtre natif `reporter` (`scope.excluded` sur ce filtre) et porte son
  propre filtre fixe `reporter = 'EU27_2020'` en `adhoc_filters` du graphique.
- **Cache des graphiques et Force refresh** : `cache_timeout` de la connexion à 86400 s
  (24 h) — après une republication de `serving`, un graphique peut afficher des données
  périmées jusqu'à *… → Force refresh* dans l'interface, ou l'expiration du cache.
- **Filtre `year` toujours renseigné** : indispensable pour l'élagage des partitions
  DuckLake (`Total Files Read: 1` contre le nombre total de fichiers de la table sans ce
  filtre) — sans lui, un graphique lit toutes les années à chaque requête.
- **Compatibilité de version DuckDB (PR-14)** : vérifiée en conditions réelles — DuckDB
  1.5.5 (Superset) lit sans problème un catalogue écrit par DuckDB 1.5.3 (pipeline),
  latence à froid 0,24 s. Pas de version à épingler pour l'instant, mais surveiller à
  chaque montée de version de l'un des deux côtés (§5.8).
