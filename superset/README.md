# Tableau de bord Superset « Vulnérabilités » (v2)

Tableau de bord versionné « as code » : ce dossier est le contenu décompressé d'un export
Superset (`metadata.yaml`, `databases/`, `datasets/`, `charts/`, `dashboards/`). Il est
réimporté par une archive construite avec `tools/superset_bundle.py`. C'est aussi cette
archive que le Job d'import du chart Helm (K-17g) utilisera. Ce document décrit le
parcours, le mécanisme de connexion, le mécanisme du « top N » et les pièges rencontrés.

## Parcours

Pour une **année** et un **flux** (import ou export), l'utilisateur interroge soit un
**pays**, soit un **produit**. Il y a un onglet par type de requête, car un tableau ne
change pas de colonnes selon un filtre.

| Onglet | Choix | Tableau (trié par rang, 1 = le plus vulnérable) | Courbes sous le tableau |
|---|---|---|---|
| **Par pays** | un pays déclarant | produits classés : code, libellé, rang consensus, incertitude du rang, écart interquartile, score de consensus, HHI, CDI2, CDI3, WORLD_HHI, CENTRALITY_RISK, CLUSTERING_W, SPOF, nombre d'alertes | échanges avec le monde (valeur en € ou quantité en 100 kg) ; évolution d'une métrique (rang consensus par défaut) |
| **Par produit** | un produit | pays déclarants classés, mêmes colonnes | idem, une courbe par pays |

Points communs aux deux onglets :
- **Courbes** : par défaut, les 5 unités les mieux classées l'année choisie. Le filtre
  « Ajouter des produits / pays » en ajoute d'autres, sans toucher au tableau.
- **Bloc Markdown d'aide** en tête de chaque onglet : lecture du rang, de l'incertitude, et
  sources.
- **Mise en forme conditionnelle** : une métrique est rouge au-delà de son seuil d'alerte
  (seuils de `parameters_vulnerabilities.yml`, règle « valeur > seuil »). « Nb alertes »
  est jaune de 1 à 2 alertes, rouge à partir de 3.

### Lecture du rang et de l'incertitude

- **Rang consensus** : `primary_rank_<niveau>` (méthode `consensus_borda`).
  - L'onglet *Par pays* utilise le niveau `by_reporter` : produits classés au sein d'un
    pays.
  - L'onglet *Par produit* utilise le niveau `by_product` : pays classés au sein d'un
    produit.
  - Les codes SH6 et NC8 sont classés ensemble par la synthèse. Les doublons quasi exacts
    (850760 et 85076000) se suivent donc.
- **Incertitude du rang** : « [q25–q75] (n) ». C'est l'intervalle interquartile des rangs
  que les `n` méthodes de synthèse de `serving.UNCERTAINTY_METHODS` donnent à la même ligne.
  La colonne `rank_spread_<niveau>` en donne la largeur.
  - Elle mesure le **désaccord entre méthodes**. Ce n'est pas une incertitude
    d'échantillonnage.
  - Elle est vide sous deux méthodes.
  - Avec la configuration par défaut, `n` vaut 10 au niveau `by_reporter` et 3 au niveau
    `by_product`, car les méthodes pondérées ne sont pas ajustées sur les groupes d'environ
    22 pays.
- **Score de consensus** : `1 − (rang − 1)/(n − 1)`, dans [0, 1], 1 = le plus vulnérable.
  Le score brut de `consensus_borda` vaut −rang et ne serait pas lisible.

## Contenu de l'export

**Connexion** : `databases/serving.yaml`, la connexion DuckDB `serving`, sans mot de passe.
Voir le mécanisme plus bas.

**Datasets** :

| Dataset | Type | Rôle |
|---|---|---|
| `cell_scores` | physique | tableaux ; colonnes calculées `rank_iqr_<niveau>` (incertitude lisible), `consensus_score_<niveau>`, `n_alerts`, `period` |
| `trade_series_by_reporter` | virtuel, Jinja | échanges, onglet *Par pays* |
| `metric_series_by_reporter` | virtuel, Jinja | métriques, onglet *Par pays* |
| `trade_series_by_product` | virtuel, Jinja | échanges, onglet *Par produit* |
| `metric_series_by_product` | virtuel, Jinja | métriques, onglet *Par produit* |
| `series_options` | virtuel, `VALUES` | valeurs des filtres « Grandeur » et « Métrique » |

**Graphiques et tableau de bord** : 6 graphiques (un tableau et deux courbes par onglet), un
tableau de bord (slug `vulnerabilites`), 14 filtres natifs. Les filtres sont portés par
onglet (`scope.rootPath`), et chacun a une valeur par défaut **explicite** :

| Filtre | Défaut | Notes |
|---|---|---|
| Année | 2021 | dernière année classée en demo ; option `--default-year` |
| Flux | Import | |
| Pays ou Produit | « FR — France (…) » ou « 850760 — Lithium-ion accumulators (excl. spent) » | sélection unique, recherche par code et libellé |
| Nomenclature en vigueur | vrai | |
| Ajouter des produits / pays | vide | multiple ; tableau hors de sa portée |
| Grandeur | Valeur (€) | portée limitée à la courbe des échanges |
| Métrique | Rang consensus | portée limitée à la courbe des métriques |

Les datasets visent le schéma de **production** `serving.dashboard`. La recette
(`demo_dashboard`) passe par l'option `--schema` de l'outil d'archive.

## Mécanisme du top N (datasets virtuels Jinja)

Un filtre natif n'a pas de valeur par défaut dynamique. Les courbes reposent donc sur des
datasets virtuels dont le SQL est un gabarit Jinja :

1. **Lecture des filtres** : `filter_values('<colonne>', remove_filter=True)` lit la valeur
   des filtres année, flux, pays ou produit, en vigueur, ajout d'unités, grandeur et
   métrique. `remove_filter=True` retire le filtre de la requête englobante : le gabarit
   l'applique lui-même, dans le CTE `ctx`.
2. **Sélection des unités** : le CTE `top` retient les `top_n` unités de meilleur rang
   consensus pour l'année choisie. À défaut d'année, il prend la dernière année ayant des
   rangs. Le CTE `units` y ajoute les unités du filtre « Ajouter ».
3. **Séries** : la requête renvoie les séries de ces unités sur **toutes les années**,
   `period = make_date(year, 1, 1)` en abscisse.
4. **Grandeur tracée** : elle est choisie par un dictionnaire Jinja libellé → colonne. Une
   valeur inconnue retombe sur la grandeur par défaut, si bien qu'aucun texte libre n'est
   interpolé dans le SQL. Les libellés de filtres sont cités par `|where_in`, qui double les
   apostrophes, par exemple « Belgium (incl. Luxembourg 'LU' -> 1998) ».
5. **Paramètre** : `top_n` vaut 5 et est porté par les `template_params` des datasets. Il
   est modifiable à l'empaquetage (`--top-n`).

Le mécanisme requiert, dans `superset_config.py` :

```python
FEATURE_FLAGS = {**globals().get("FEATURE_FLAGS", {}), "ENABLE_TEMPLATE_PROCESSING": True}
```

Cette ligne doit être portée par le chart Helm (K-17g). Sur le service actuel, elle a été
ajoutée hors Helm, au Secret `superset-899573-config`, puis web et worker ont été redémarrés.

## Mécanisme de connexion

L'écouteur SQLAlchemy `connect` de `superset_config.py` agit seulement pour les connexions
DuckDB : `type(dbapi_connection).__module__ == "duckdb_engine"`. Ce discriminant évite de
toucher aux connexions PostgreSQL et Redis. L'écouteur :
- charge `ducklake` et `httpfs` ;
- crée un secret S3 en mémoire ;
- attache le catalogue `serving` en lecture seule (`ATTACH … (READ_ONLY)`). `DATA_PATH` est
  lu dans les métadonnées du catalogue.

Les identifiants sont dans l'environnement des pods. Le Secret `superset-899573-env` reçoit
les variables `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` et `PGDATABASE` (copiées de
`trade-postgres-credentials`), et `S3_ACCESS_KEY`, `S3_SECRET_KEY` (copiées de
`trade-s3-credentials`).

Pourquoi pas un secret DuckDB persistant sur disque : le pod web et le worker Celery, qui
exécute SQL Lab en asynchrone, n'ont pas de volume commun. L'export ne contient aucun mot
de passe : `databases/serving.yaml` n'a qu'une URI `duckdb:///:memory:` et des
`engine_params` non sensibles.

## Réimport

```bash
# Archive (racine unique, format de l'API et de la CLI) ; recette sur le schéma demo
python tools/superset_bundle.py superset/vulnerabilites -o /tmp/vulnerabilites.zip \
    --schema demo_dashboard --default-year 2021 [--top-n 5]

# Import par l'API (jeton : POST /api/v1/security/login ; CSRF : GET /api/v1/security/csrf_token/)
curl -X POST "$SUPERSET/api/v1/dashboard/import/" \
    -H "Authorization: Bearer $TOKEN" -H "X-CSRFToken: $CSRF" -H "Referer: $SUPERSET" \
    -F formData=@/tmp/vulnerabilites.zip -F overwrite=true

# ou dans le conteneur Superset
superset import-dashboards -p /tmp/vulnerabilites.zip -u admin
```

Avant d'écrire l'archive, l'outil vérifie :
- que les YAML sont lisibles ;
- que les références croisées sont résolues : base de chaque dataset, dataset de chaque
  graphique et de chaque filtre, UUID de chaque graphique de la mise en page ;
- qu'aucune clé `password` n'est présente.

En production (K-17g), `--schema` est omis (les exports visent `dashboard`), et
`--default-year` reçoit la dernière année classée.

**Modifier le tableau de bord** : le modifier dans l'interface, l'exporter (*Dashboards →
Export*), puis remplacer `charts/`, `datasets/` et `dashboards/` par le contenu du zip :
1. ramener les datasets au schéma de production (`serving.demo_dashboard` devient
   `serving.dashboard` dans la clé `schema` et dans le SQL) ;
2. garder `databases/serving.yaml` ;
3. vérifier par `python tools/superset_bundle.py … -o /tmp/x.zip`.

## Pièges rencontrés

**Construction et export par l'API**
- **Mise en page sans UUID** : un tableau de bord construit par l'API (`position_json` avec
  `chartId` seul) s'exporte sans `meta.uuid` dans ses composants `CHART`. L'export ajoute
  alors une copie orpheline de chaque graphique en bas de la mise en page, et le réimport ne
  relie pas les composants aux bons graphiques. Il faut renseigner `meta.uuid` (lu dans
  l'export des graphiques) avant d'exporter. `tools/superset_bundle.py` refuse une mise en
  page sans UUID. L'export v1 (K-03c) avait ce défaut.
- **`template_params` doit être un objet YAML** dans un dataset importé (`top_n: 5`), pas un
  texte JSON. Sinon l'import échoue en validation : « Not a valid mapping type ». L'API
  répond alors par une simple erreur 500 HTML. Pour obtenir le détail de la validation,
  lancer `ImportDashboardsCommand` dans le pod et lire `normalized_messages()`.
- **Un graphique `viz_type: "markdown"` créé par `POST /api/v1/chart/` échoue** (« Empty
  query ? ») : le Markdown est un composant de mise en page (`position_json`, type
  `MARKDOWN`), pas un graphique.
- **Format `PERCENT_1_POINT` invalide** : utiliser un format D3 (`.1%`).

**Gabarits Jinja des datasets virtuels**
- **Contrôle d'espaces Jinja et commentaires SQL** : `{%- … -%}` supprime les retours à la
  ligne. Placé après un commentaire `-- …`, il fait passer la requête dans le commentaire
  (« syntax error at or near ")" »). Utiliser `{% … %}`.
- **Syntaxe propre à DuckDB refusée** : avant d'exécuter, Superset analyse le SQL avec
  sqlglot, qui ne connaît pas `UNPIVOT … ON … INTO NAME … VALUE …` (« Error parsing near
  'ON' »). D'où le choix de colonne par dictionnaire Jinja.
- **Création d'un dataset virtuel à gabarit** : Superset exécute le SQL rendu sans filtre
  pour en déduire les colonnes. Chaque variable doit donc avoir une valeur par défaut :
  `top_n|default(5)`, flux, pays ou produit, grandeur. Après création, il faut appeler
  `PUT /api/v1/dataset/<id>/refresh`.
- **Filtre sur une colonne absente du dataset** : il est ignoré sans erreur. C'est ainsi
  qu'un même filtre natif s'applique au tableau (`cell_scores`) et aux courbes (datasets
  virtuels) par le nom de colonne. Les portées sont tout de même restreintes explicitement
  (`scope.excluded`).

**Filtres natifs et données**
- **Valeur par défaut explicite obligatoire** : le calcul « première valeur »
  (`defaultToFirstItem`) ne se déclenche pas de façon fiable par l'API. Sans année fixée,
  toutes les années se mélangent (un rang 1 par année). Toujours renseigner
  `defaultDataMask.filterState.value` et `extraFormData`.
- **Défaut de pays ou de produit lié au libellé** : les filtres portent sur
  `reporter_display` et `product_display` (« code — libellé »). Si le référentiel change un
  libellé, la valeur par défaut ne correspond plus à rien (tableau vide) : à revérifier
  après une mise à jour des référentiels.
- **`year` entier** : une colonne `period = make_date(year, 1, 1)` marquée temporelle est
  nécessaire aux courbes.
- **Années sans rang** : en demo, les rangs n'existent que pour 2017–2021 (réseau BACI).
  Pour une autre année, le tableau affiche les métriques sans rang, et les courbes n'ont
  aucune unité par défaut (seulement les unités ajoutées).
- **CDI3 infini** (flux opposé nul) : l'étape partenaires n'y lève pas d'alerte, alors que
  la mise en forme « > 1 » la colore. Cela concerne 44 lignes sur 250 000 en demo.
  « Nb alertes », calculé sur les colonnes `_ALERT`, est exact.

**Exploitation**
- **Cache** : `cache_timeout` de la connexion à 86 400 s (24 h). Après une republication de
  `serving`, un graphique déjà en cache garde l'ancien état jusqu'à *… → Force refresh* ou
  l'expiration. La clé de cache inclut les valeurs des filtres : chaque combinaison est
  mise en cache séparément.
- **Élagage** : les tableaux filtrent toujours `year`, ce qui limite la lecture à une
  partition. Les courbes lisent toutes les années, mais d'un seul pays ou produit : elles
  s'appuient sur les statistiques min/max du tri d'insertion. Latences à froid mesurées en
  demo (K-03c v2) : 0,8 à 1,1 s pour les tableaux, 2,0 à 2,6 s pour les courbes.
- **Patch hors Helm** : la version de chart déployée (`superset-0.1.12`) n'est plus dans
  l'index du dépôt. Les Secrets `superset-899573-env` et `superset-899573-config` (écouteur
  `connect`, `FEATURE_FLAGS`) ont donc été patchés directement. Un redéploiement depuis
  l'interface Onyxia perd ce patch, qui doit alors être rejoué jusqu'à K-17g.
- **Compatibilité DuckDB** : DuckDB 1.5.5 (Superset) lit le catalogue écrit par DuckDB 1.5.3
  (pipeline). À revérifier à chaque montée de version de l'un des deux côtés.
