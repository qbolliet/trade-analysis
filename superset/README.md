# Tableau de bord Superset « Vulnérabilités » (v2.2)

Tableau de bord versionné « as code » : ce dossier est le contenu décompressé d'un export
Superset (`metadata.yaml`, `databases/`, `datasets/`, `charts/`, `dashboards/`). Il est
réimporté par une archive construite avec `tools/superset_bundle.py`. C'est aussi cette
archive que construit le Job d'import du chart Helm `trade-dashboard` (`helm/`), à partir
d'une copie de ce dossier (`python tools/sync_chart_files.py`). Ce document décrit le
parcours, le mécanisme de connexion, le mécanisme du « top N », celui du produit détaillé et
des diagrammes des partenaires, et les pièges rencontrés.

## Parcours

Pour une **année** et un **flux** (import ou export), l'utilisateur interroge soit un
**pays**, soit un **produit**. Il y a un onglet par type de requête, car un tableau ne
change pas de colonnes selon un filtre.

| Onglet | Choix | Tableau (trié par rang, 1 = le plus vulnérable) | Sous le tableau | Courbes |
|---|---|---|---|---|
| **Par pays** | un pays déclarant et un niveau de nomenclature (SH6 par défaut) | produits de ce niveau classés : code, libellé, rang, fourchette du rang et sa largeur, score de vulnérabilité, sept indicateurs, nombre d'alertes | **principaux partenaires** du pays pour le produit détaillé (barres horizontales) | échanges avec le monde (valeur en € ou quantité en 100 kg) ; évolution d'un indicateur (rang par défaut) |
| **Par produit** | un produit | pays déclarants classés, mêmes colonnes | **flux du produit** entre pays de l'UE et partenaires (Sankey) | idem, une courbe par pays |

Points communs aux deux onglets :
- **Courbes** : les unités du filtre « Produits des courbes » / « Pays des courbes » ; s'il
  est vide, les 5 unités les mieux classées l'année choisie (dans l'onglet *Par pays*, au
  sein du niveau de nomenclature choisi). Le filtre ne touche pas au tableau. Les titres ne
  disent pas « les plus vulnérables », car le menu est modifiable : *Par pays*, « Échanges
  avec le monde » et « Évolution d'un indicateur de vulnérabilité des produits » ; *Par
  produit*, « Échanges avec le monde des pays » et « Évolution d'un indicateur de
  vulnérabilité des pays ». La règle par défaut (« au départ, les 5 premiers du tableau »)
  se lit dans le bloc d'aide et dans la description du filtre.
- **Période des courbes** : la courbe des échanges couvre toutes les années ; celle des
  indicateurs, les années où l'indicateur choisi est défini, quelle que soit l'année du
  filtre (en demo : 2017–2021 pour le rang et les indicateurs de réseau, années du réseau
  BACI ; toutes les années pour les indicateurs partenaires). Une année manquante au milieu
  d'une série reste un trou ; un point isolé reste visible grâce aux marqueurs.
- **Bloc Markdown d'aide** en tête de chaque onglet, pour des lecteurs non spécialistes : ce
  que montre la page, comment la lire, comment s'en servir, sources. Les précisions de
  méthode (méthodes de synthèse, quartiles, nombre de méthodes) sont dans
  `docs/dashboards.md`, que le bloc cite sans lien : le site de documentation n'était pas
  publié lors de la construction.
- **Mise en forme conditionnelle** : une métrique est rouge au-delà de son seuil d'alerte
  (seuils de `parameters_vulnerabilities.yml`, règle « valeur > seuil »). « Nb alertes »
  est jaune de 1 à 2 alertes, rouge à partir de 3.
- **Couleur de la fourchette du rang** : selon sa largeur rapportée au nombre de lignes
  classées du groupe (`rank_spread_<niveau>` / `primary_n_<niveau>`), en couleur unie :
  vert `#66BD63` jusqu'à 5 %, vert clair `#D9EF8B` jusqu'à 10 %, orange `#FDAE61` jusqu'à
  20 %, rouge `#EFA1AA` au-delà (bornes incluses). La part est rapportée au groupe, et non
  exprimée en rangs, pour que les seuils gardent leur sens de la demo (au plus 53 produits
  SH6 par pays) à la production (des milliers) et d'un onglet à l'autre (environ 27 pays).
  L'infobulle de la cellule donne cette part. « Largeur de la fourchette » n'est pas
  colorée. Mécanisme : voir les pièges, « Colorer une colonne texte ».
- **Bloc d'aide** : il précise que les indicateurs ne se comparent pas d'une colonne à
  l'autre (échelles et unités différentes) et définit chaque colonne en une phrase, sans
  formule ; dans l'onglet *Par produit*, il signale que les quatre indicateurs de réseau ont
  la même valeur pour tous les pays d'un produit.

### Libellés des indicateurs

Un seul libellé par indicateur dans tout le tableau de bord : `verbose_name` des colonnes de
`cell_scores` (en-têtes des tableaux), valeurs du menu « Indicateur » (`series_options`),
clés du dictionnaire Jinja des courbes (`metric_series_*`), libellés des métriques `MAX(…)`
du dataset. Les noms de colonnes de `serving` sont inchangés. Le sigle reste en fin de
libellé ; la définition est dans la `description` de la colonne. Le test
`test_indicator_labels_consistent` vérifie la correspondance et l'absence de sigle seul.

| Colonne | Libellé | Sens |
|---|---|---|
| `primary_rank_<niveau>` | Rang (1 = le plus vulnérable) | rang consensus |
| `rank_iqr_<niveau>` | Fourchette du rang | « q25 – q75 » des rangs des méthodes, cellule colorée (fragment HTML) |
| `rank_spread_<niveau>` | Largeur de la fourchette | q75 − q25 |
| `consensus_score_<niveau>` | Score de vulnérabilité (0 à 1) | 1 = le plus vulnérable |
| `HHI` | Concentration des partenaires (HHI) | fournisseurs (import), clients (export) |
| `CDI2` | Part des échanges hors UE (CDI2) | |
| `CDI3` | Hors UE / flux de sens inverse (CDI3) | |
| `WORLD_HHI` | Concentration mondiale (WORLD_HHI) | offre (import), demande (export) |
| `CENTRALITY_RISK` | Poids des pays pivots (CENTRALITY_RISK) | |
| `CLUSTERING_W` | Cloisonnement du réseau (CLUSTERING_W) | identique dans les deux sens |
| `SPOF` | Point de défaillance unique (SPOF) | |
| `n_alerts` | Seuils d'alerte dépassés (sur 7) | |

Un libellé modifié doit l'être aux quatre endroits à la fois : la valeur du menu est la
valeur de filtre que lit le gabarit des courbes (valeur inconnue : retour au rang).

### Lecture du rang et de l'incertitude

- **Rang consensus** : `primary_rank_<niveau>` (méthode `consensus_borda`).
  - L'onglet *Par pays* utilise le niveau `by_reporter_level` : produits d'un pays
    classés au sein du niveau de nomenclature choisi (filtre « Niveau de nomenclature »,
    colonne `product_level_label` : SH2, SH4, SH6, NC8).
  - L'onglet *Par produit* utilise le niveau `by_product` : pays classés au sein d'un
    produit.
  - `by_reporter_level` est dérivé dans `serving` : rangs `by_reporter` renumérotés au
    sein du niveau, ordre de chaque méthode conservé. La synthèse classe désormais par
    niveau (`product_level` en colonne de contexte) : `by_reporter_level` y est identique
    à `by_reporter`. Sur une synthèse antérieure, qui classait tous les niveaux d'un pays
    ensemble (850760 et 85076000 s'y suivaient), c'est une restriction du classement
    commun. Le tableau de bord fonctionne ainsi avant comme après le recalcul.
  - Les codes SH2 et SH4 n'ont pas de rang : les métriques réseau n'existent qu'à partir
    du SH6, et une ligne incomplète n'est pas classée.
- **Fourchette du rang** : « q25 – q75 ». C'est l'intervalle interquartile des rangs que les
  `n_methods_<niveau>` méthodes de synthèse de `serving.UNCERTAINTY_METHODS` donnent à la
  même ligne (`n` n'est plus affiché, voir `docs/dashboards.md`). La colonne
  `rank_spread_<niveau>` en donne la largeur.
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
| `cell_scores` | physique | tableaux ; colonnes calculées `rank_iqr_<niveau>` (fourchette lisible), `consensus_score_<niveau>` (niveaux `by_reporter`, `by_reporter_level`, `by_product`), `n_alerts`, `period`, `detail_product` (cible du filtre « Produit à détailler ») |
| `trade_series_by_reporter` | virtuel, Jinja | échanges, onglet *Par pays* |
| `metric_series_by_reporter` | virtuel, Jinja | métriques, onglet *Par pays* |
| `trade_series_by_product` | virtuel, Jinja | échanges, onglet *Par produit* |
| `metric_series_by_product` | virtuel, Jinja | métriques, onglet *Par produit* |
| `partners_by_reporter` | virtuel, Jinja | principaux partenaires du produit détaillé, onglet *Par pays* (`flows` et `cell_scores`) |
| `partner_flows_by_product` | virtuel, Jinja | liens du Sankey, onglet *Par produit* (`flows` et `cell_scores`) |
| `series_options` | virtuel, `VALUES` | valeurs des filtres « Grandeur » et « Indicateur » |

**Graphiques et tableau de bord** : 8 graphiques (par onglet un tableau et deux courbes ;
*Par pays* : un diagramme en barres `echarts_timeseries_bar` horizontal ; *Par produit* : un Sankey `sankey_v2`, le type ECharts
non déprécié de Superset 4.1), un tableau de bord (slug `vulnerabilites`), 16 filtres natifs. Les filtres sont portés par
onglet (`scope.rootPath`), et chacun a une valeur par défaut **explicite** :

| Filtre | Défaut | Notes |
|---|---|---|
| Année | 2021 | dernière année classée en demo ; option `--default-year` |
| Flux | Import | |
| Pays ou Produit | « FR — France (…) » ou « 850760 — Lithium-ion accumulators (excl. spent) » | sélection unique, recherche par code et libellé |
| Niveau de nomenclature | SH6 | onglet *Par pays* seulement ; valeur obligatoire (sans elle, plusieurs rangs 1 cohabiteraient) ; valeurs lues dans les données ; parent des filtres « Produit à détailler » et « Produits des courbes » |
| Nomenclature en vigueur | vrai | |
| Produit à détailler | le produit de rang 1 du pays, de l'année, du flux et du niveau par défaut, soit « 850760 — Lithium-ion accumulators (excl. spent) » en demo (vide : produit de rang 1) | onglet *Par pays* ; sélection unique sur `detail_product` ; parents Pays, Flux, Niveau ; portée : barres ; valeur fixe recalculée par le Job d'import |
| Produits des courbes / Pays des courbes | les 5 premiers produits du pays par défaut (France, SH6, import 2021 en demo) / les 5 premiers pays du produit par défaut (850760) (vide : les 5 premiers) | multiple ; portée limitée aux courbes ; valeurs fixes recalculées par le Job d'import, voir les pièges |
| Grandeur (échanges) | Valeur (€) | courbe des échanges, barres, Sankey |
| Indicateur (courbe des indicateurs) | Rang (1 = le plus vulnérable) | portée limitée à la courbe des indicateurs |

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
   rangs. Dans l'onglet *Par pays*, il se limite au niveau de nomenclature choisi et lit
   les rangs `by_reporter_level` (sans niveau choisi : rangs `by_reporter`, tous niveaux
   confondus). Si le filtre « Produits des courbes » / « Pays des courbes » est renseigné,
   le CTE `units` retient exactement ses unités, quel que soit leur niveau, à la place du
   top N : le menu montre ainsi toujours ce qui est tracé.
3. **Séries** : la requête renvoie les séries de ces unités sur **toutes les années**,
   `period = make_date(year, 1, 1)` en abscisse, quelle que soit l'année du filtre, qui ne
   sert qu'à choisir les unités par défaut. Les datasets `metric_series_*` ne renvoient que
   les lignes où l'indicateur choisi est défini : l'axe couvre sa période, sans valeur
   fabriquée.
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

Cette ligne est portée par le chart Helm `trade-dashboard`
(`helm/trade-dashboard/files/superset_serving.py`, réglage `catalog.templateProcessing`).

## Produit détaillé et diagrammes des partenaires

**Produit détaillé** (`partners_by_reporter`). Le gabarit choisit, par ordre de priorité :
1. un code produit reçu par filtrage croisé (`filter_values('product')`, clic sur la colonne
   code d'un tableau ; voir les pièges) ;
2. le filtre « Produit à détailler » (`filter_values('detail_product')`) ;
3. sinon le produit de rang 1 (`by_reporter_level`) du pays, de l'année, du flux et du
   niveau choisis, parmi les lignes en vigueur (`flows` n'a que les codes en vigueur).

Un titre Superset est statique : le produit affiché se lit dans le filtre « Produit à
détailler » ou, filtre vide, en première ligne du tableau. Un tableau d'une ligne
« produit détaillé » au-dessus des barres a été essayé puis retiré : il occupait une ligne de
la page sans apporter d'information que le tableau ne donne déjà.

Le filtre porte sur une colonne calculée `detail_product` (= `product_display`) et non sur
`product_display`, déjà prise par « Produits des courbes » : deux filtres sur la même colonne
se confondraient dans `filter_values`.

**Barres.** Les `n_partners` premiers partenaires (10, `template_params`, borné de fait par
`TOP_PARTNERS` = 12 de `flows`), puis « Autres pays » = échanges totaux de la cellule
(`trade_value_eur` ou `trade_quantity_100kg` de `cell_scores`) − somme des barres, omise si
nulle. Trois métriques empilées (`value_eu`, `value_extra`, `value_other`, une seule renseignée
par ligne) colorent les barres sans dimension, car le tri de l'axe n'est proposé qu'en
l'absence de dimension. Le tri porte sur le libellé « rang. pays — part » (rang complété
d'une espace sous 10, « Autres pays » après les chiffres) : il est déterministe, quel que
soit le montant de « Autres pays ». **Pays de l'UE** : déclarant individuel de `flows` la
même année (Comext ne publie que les États membres ; les agrégats déclarants contiennent un
`_`), sans attribut ajouté à `serving`.

**Sankey** (`partner_flows_by_product`). Côté UE, les États membres déclarants
(`EU27_2020` exclu). Côté partenaires, les `k_partners` (12) premiers partenaires hors UE
cumulés sur les déclarants, « Autres pays hors UE » (`EXT_EU` − flux vers ces partenaires,
borné à 0) et « Union européenne (échanges entre États membres) » (`WORLD` − `EXT_EU`, la
décomposition de CDI2). Le Sankey ECharts refuse les cycles et confond deux nœuds de même
nom : regrouper l'intra-UE en un nœud évite qu'un pays figure des deux côtés. Les nœuds
sont nommés par le libellé court (texte avant « ( »). Sens : partenaire → pays de l'UE à
l'import, l'inverse à l'export.

Contrôles (demo, 2021, import et export, valeur et quantité) : somme des barres = échanges
totaux de `cell_scores`, tri décroissant, produit par défaut de rang 1, somme des flux de
chaque pays du Sankey = ses échanges totaux. Les tests rendent les deux gabarits sur le
monde fictif et sur un petit jeu calculé à la main.

## Mécanisme de connexion

L'écouteur SQLAlchemy `connect` de `superset_config.py` agit seulement pour les connexions
DuckDB : `type(dbapi_connection).__module__ == "duckdb_engine"`. Ce discriminant évite de
toucher aux connexions PostgreSQL et Redis. L'écouteur :
- charge `ducklake` et `httpfs` ;
- crée un secret S3 en mémoire ;
- attache le catalogue `serving` en lecture seule (`ATTACH … (READ_ONLY)`). `DATA_PATH` est
  lu dans les métadonnées du catalogue.

Les identifiants sont dans l'environnement des pods : le chart `trade-dashboard` y expose
en entier les secrets `global.dashboard.readerSecret` (`PGHOST`, `PGPORT`, `PGUSER`,
`PGPASSWORD` ; défaut `trade-postgres-credentials`, un rôle en lecture seule
`superset_reader` est préférable) et `global.dashboard.s3Secret` (`S3_ACCESS_KEY`,
`S3_SECRET_KEY` ; défaut `trade-s3-credentials`). L'écouteur est le fichier
`helm/trade-dashboard/files/superset_serving.py`, monté dans les pods et exécuté à la fin
de `superset_config.py`.

Pourquoi pas un secret DuckDB persistant sur disque : le pod web et le worker Celery, qui
exécute SQL Lab en asynchrone, n'ont pas de volume commun. L'export ne contient aucun mot
de passe : `databases/serving.yaml` n'a qu'une URI `duckdb:///:memory:` et des
`engine_params` non sensibles.

## Réimport

**Mise à jour d'un Superset où le tableau de bord existe déjà** : archive « assets »
(`--assets`) et `POST /api/v1/assets/import/`, qui remplace la connexion, les datasets,
les graphiques et le tableau de bord. L'import d'un tableau de bord (`dashboard/import`,
`import-dashboards`), même avec `overwrite=true`, ne remplace **que le tableau de bord** :
les datasets et graphiques existants (même UUID) restent tels quels.

```bash
# Archive (racine unique) ; recette sur le schéma demo
python tools/superset_bundle.py superset/vulnerabilites -o /tmp/vulnerabilites.zip \
    --assets --schema demo_dashboard --default-year 2021 [--top-n 5]

# Import par l'API (jeton : POST /api/v1/security/login ; CSRF : GET /api/v1/security/csrf_token/)
curl -X POST "$SUPERSET/api/v1/assets/import/" \
    -H "Authorization: Bearer $TOKEN" -H "X-CSRFToken: $CSRF" -H "Referer: $SUPERSET" \
    -F bundle=@/tmp/vulnerabilites.zip
```

**Premier import** (Superset vierge), sans `--assets` : `POST /api/v1/dashboard/import/`
(`-F formData=@… -F overwrite=true`) ou, dans le conteneur,
`superset import-dashboards -p /tmp/vulnerabilites.zip -u admin`.

Avant d'écrire l'archive, l'outil vérifie :
- que les YAML sont lisibles ;
- que les références croisées sont résolues : base de chaque dataset, dataset de chaque
  graphique et de chaque filtre, UUID de chaque graphique de la mise en page ;
- qu'aucune clé `password` n'est présente ;
- que le SQL des datasets virtuels n'emploie ni `PIVOT` ni `UNPIVOT` (voir les pièges).

Le test `tests/test_superset_bundle.py` rend en outre le gabarit Jinja des datasets
virtuels versionnés (avec un équivalent de `filter_values` et `|where_in`) et l'exécute sur
la table `cell_scores` du monde fictif : une colonne absente ou un niveau mal appliqué y
échoue.

En production (Job d'import du chart, `dashboardImport.schema` vide), `--schema` est omis (les exports visent `dashboard`), et
`--default-year` reçoit la dernière année classée. Le Job recalcule ensuite les menus
préremplis sur les données (voir les pièges, « Menus préremplis ») et réimporte l'archive
s'ils diffèrent de l'export ; `dashboardImport.menuDefaults: export` garde les valeurs de
l'export. Hors du Job, `--menu-defaults menus.json` (identifiant du filtre → valeurs) écrit
des valeurs calculées ailleurs.

**Modifier le tableau de bord** : soit éditer directement les YAML de ce dossier (SQL des
datasets virtuels, colonnes, paramètres des graphiques, filtres natifs), soit modifier le
tableau de bord dans l'interface, l'exporter (*Dashboards → Export*), puis remplacer
`charts/`, `datasets/` et `dashboards/` par le contenu du zip :
1. ramener les datasets au schéma de production (`serving.demo_dashboard` devient
   `serving.dashboard` dans la clé `schema` et dans le SQL) ;
2. garder `databases/serving.yaml` ;
3. vérifier par `python tools/superset_bundle.py … -o /tmp/x.zip` et
   `pytest tests/test_superset_bundle.py`.

Une modification faite **par l'API** sur un dataset ou un graphique n'est dans le dépôt
qu'après un nouvel export : à faire aussitôt, sinon l'export versionné diverge du service.

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

**Colorer une colonne texte (fourchette du rang)**
- **Mise en forme conditionnelle réservée aux colonnes numériques** : en 4.1.1, le tableau
  n'applique `conditional_formatting` qu'aux colonnes `isNumeric`. « Fourchette du rang »
  (« 3 – 4.8 ») est un texte : une règle sur elle est ignorée sans erreur.
- **Dégradé d'opacité** : la couleur d'une règle native varie dans sa tranche (opacité de
  5 % au seuil à 100 % à l'extrême, `getColorFormatters` appelé sans `alpha`) ; une valeur
  juste au-delà d'un seuil paraît presque blanche. C'est le rendu des cellules rouges des
  indicateurs.
- **Solution retenue** : la colonne calculée `rank_iqr_<niveau>` renvoie un fragment HTML,
  un `<span title="… % du classement">` qui contient un `<span>` de fond en
  `position:absolute` (`top/right/bottom/left: 0`) puis le texte en `position:relative`. Le
  `<td>` du tableau est en `position: relative` et reçoit le HTML directement : le fond
  remplit la cellule, en couleur unie. Le rendu HTML (`allow_render_html`, vrai par
  défaut) est explicite dans les deux tableaux ; l'assainissement de Superset (`xss`,
  `css: false`) garde les attributs `style` et `title` de `span`.
- **Effets de bord** : un export CSV ou Excel du tableau contient les balises ; un tri sur
  la colonne porte sur le texte HTML, où la part du classement (infobulle) vient en premier
  (ordre non vérifié à l'écran). Les tests `test_rank_range_colour_*` rendent l'expression
  sur un jeu calculé à la main (bornes 5, 10 et 20 % incluses, ligne à une méthode vide)
  et sur le monde fictif.

**Gabarits Jinja des datasets virtuels**
- **Contrôle d'espaces Jinja et commentaires SQL** : `{%- … -%}` supprime les retours à la
  ligne. Placé après un commentaire `-- …`, il fait passer la requête dans le commentaire
  (« syntax error at or near ")" »). Utiliser `{% … %}`.
- **Syntaxe propre à DuckDB refusée** : avant d'exécuter, Superset analyse le SQL avec
  sqlglot, qui ne connaît pas `UNPIVOT … ON … INTO NAME … VALUE …` (« Error parsing near
  'ON' »). D'où le choix de colonne par dictionnaire Jinja. L'erreur n'apparaît qu'à
  l'affichage du graphique, pas à l'import : `tools/superset_bundle.py` refuse donc
  `PIVOT` et `UNPIVOT`.
- **Création d'un dataset virtuel à gabarit** : Superset exécute le SQL rendu sans filtre
  pour en déduire les colonnes. Chaque variable doit donc avoir une valeur par défaut :
  `top_n|default(5)`, flux, pays ou produit, grandeur. Après création, il faut appeler
  `PUT /api/v1/dataset/<id>/refresh`.
- **Filtre sur une colonne absente du dataset** : il est ignoré sans erreur. C'est ainsi
  qu'un même filtre natif s'applique au tableau (`cell_scores`) et aux courbes (datasets
  virtuels) par le nom de colonne. Les portées sont tout de même restreintes explicitement
  (`scope.excluded`).

**Import**
- **L'import d'un tableau de bord ne met pas à jour ses datasets ni ses graphiques** : en
  4.1, `ImportDashboardsCommand` importe datasets et graphiques avec `overwrite=False`,
  quel que soit le paramètre `overwrite` (qui ne vaut que pour le tableau de bord). Seul
  `POST /api/v1/assets/import/` (archive `--assets`) remplace tout. Constat de K-03c v2 :
  le SQL des courbes, corrigé par l'API sur le service (choix de colonne sans `UNPIVOT`),
  n'avait pas été réexporté. L'export versionné gardait la version `UNPIVOT`, et l'import
  « réussi » de cet export n'avait rien changé aux datasets du service. Un Superset vierge
  aurait reçu des courbes en erreur. Corrigé avec l'ajout du filtre de niveau.

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

**Diagrammes des partenaires**
- **Tri d'un diagramme en barres** : en 4.1, « X-Axis Sort By » n'est proposé que sans
  dimension (`groupby` vide), et seulement sur le libellé de l'axe ou une métrique tracée.
  D'où les trois métriques empilées pour la couleur et le rang en préfixe du libellé.
- **Liens du Sankey** : un pays de part et d'autre du diagramme crée un cycle (« Sankey is a
  DAG ») ; deux nœuds homonymes sont fusionnés. Les nœuds partenaires ne comptent donc
  aucun État membre.
- **Synthétique ≠ réel** : dans le monde fictif des tests, `EXT_EU` vaut 0,6 × `WORLD` alors
  que tous les partenaires sont hors UE ; la somme des flux d'un pays du Sankey n'y égale pas
  son total. L'égalité est testée sur le jeu calculé à la main et contrôlée sur la demo.

- **Filtrage croisé non activé** : le gabarit des barres lit un code produit reçu par clic
  (`filter_values('product')`), mais `cross_filters_enabled` reste à `false`. Activé, il
  ferait émettre un filtre à chaque graphique (clic sur une courbe : filtre sur `unit`) avec
  une portée globale par défaut, qui filtrerait aussi le tableau de l'onglet *Par produit*
  (colonne `product`) ; sa portée par graphique (`chart_configuration`) n'a pas pu être
  essayée dans un navigateur. À activer après un essai à l'écran.

**Menus préremplis (valeurs fixes)**
- **Pas de valeur par défaut calculée** : en 4.1, l'option « première valeur par défaut »
  (`defaultToFirstItem`) ne s'applique qu'au premier affichage (valeur encore indéfinie), ne
  retient qu'une valeur et n'est pas recalculée quand un filtre parent change. Les menus
  « Produit à détailler », « Produits des courbes » et « Pays des courbes » reçoivent donc des
  valeurs **fixes** : celles que tracent les graphiques au premier affichage (rang 1 et
  5 premiers rangs pour les filtres par défaut). Après un changement de pays, de produit,
  d'année ou de flux, elles restent sélectionnées : les graphiques suivent le menu, qu'il
  faut vider pour revenir au rang 1 et aux 5 premiers.
- **Recalcul par le Job d'import** : les valeurs versionnées sont celles de la demo (France,
  2021, import, SH6 ; produit 850760). Après l'import, le Job lance une requête par menu
  (`menu_queries` de `tools/superset_bundle.py`, même règle que les gabarits : meilleur rang,
  ex æquo départagés par le code) par l'API de données de Superset sur `cell_scores`. Il
  n'a besoin d'aucun identifiant du catalogue et réimporte l'archive si les valeurs
  diffèrent. Elles sont justes après chaque `helm install` / `upgrade` ; après une
  republication de `serving` ou un changement d'année par défaut, relancer le Job (`helm
  upgrade`). Un menu sans résultat (pays ou produit par défaut introuvable, année sans
  rang) reste vide, avec un avertissement dans le journal du Job.
- **Contrôle à la main** : `menu_query_sql(query, "serving.dashboard.cell_scores")` donne le
  SQL de chaque menu. Les tests `test_committed_menu_defaults_are_drawn_units` (cohérence
  interne de l'export) et `test_menu_queries_return_the_drawn_units` (menus calculés =
  unités tracées, monde fictif) le vérifient.
- **État des filtres mémorisé dans l'URL** (constat K-17i) : Superset ajoute à l'URL un
  paramètre `native_filters_key`, clé d'un état des filtres stocké sur le serveur. Un lien,
  un favori ou un onglet qui contient ce paramètre restaure cet état **en entier**, à la
  place des valeurs par défaut, y compris après un réimport : menus vidés, niveau ou
  libellé d'une session antérieure. Les symptômes « menu vide alors que le graphique trace
  quelque chose » et « courbe d'un seul point » (produits NC8 dont le rang n'existe qu'une
  année) venaient d'une clé créée pendant la construction K-17h. Pour voir les valeurs par
  défaut, ouvrir l'URL sans paramètre (`/superset/dashboard/vulnerabilites/`, fenêtre
  privée en cas de doute).
- **Libellé périmé dans un état mémorisé** : une valeur de menu qui n'existe plus (ancien
  libellé « Rang consensus ») ne provoque pas d'erreur. Le gabarit retombe sur le rang,
  mais le menu affiche l'ancien libellé.
- **Diagnostic de ce qu'envoie le navigateur** : le journal des événements
  (`GET /api/v1/log/`, action `ChartDataRestApi.data`) contient, pour chaque graphique
  affiché, les filtres réellement envoyés (`queries[0].filters`) et `url_params`. Les
  requêtes d'un état mémorisé y sont repérables, comme celles d'une fenêtre privée.
- **Années manquantes non reliées** : la courbe ECharts de Superset 4.1.1 ne relie les
  valeurs vides (`connectNulls`) que pour les séries de comparaison temporelle ; aucun
  réglage ne l'expose. Une année manquante dans une série reste donc un trou.

**Exploitation**
- **Cache** : `cache_timeout` de la connexion à 86 400 s (24 h). Après une republication de
  `serving`, un graphique déjà en cache garde l'ancien état jusqu'à *… → Force refresh* ou
  l'expiration. La clé de cache inclut les valeurs des filtres : chaque combinaison est
  mise en cache séparément.
- **Élagage** : les tableaux filtrent toujours `year`, ce qui limite la lecture à une
  partition. Les courbes lisent toutes les années, mais d'un seul pays ou produit : elles
  s'appuient sur les statistiques min/max du tri d'insertion. Latences à froid mesurées en
  demo (K-03c v2) : 0,8 à 1,1 s pour les tableaux, 2,0 à 2,6 s pour les courbes ;
  (K-17h) 1,3 à 1,9 s pour les barres et le Sankey, qui ne lisent
  qu'une partition de `flows` et de `cell_scores`. (K-17i) 2,0 à 2,7 s pour les courbes, 0,7 à 0,8 s par requête de
  menu du Job d'import.
- **Configuration portée par Helm** : l'ancien service Onyxia `superset-899573` avait été
  configuré en modifiant directement ses Secrets (`superset-899573-env`,
  `superset-899573-config`), modification perdue à tout redéploiement depuis Onyxia. Le
  Superset du chart `trade-dashboard` porte l'écouteur, `FEATURE_FLAGS` et les variables
  dans sa propre release : un redéploiement les conserve.
- **Compatibilité DuckDB** : DuckDB 1.5.5 (Superset) lisait le catalogue écrit par DuckDB
  1.5.3 (pipeline). Le pipeline est passé à DuckDB 2.0.0, qui exige Python ≥ 3.11 ; l'image
  Superset 4.1.1 (Python 3.10) reste en DuckDB 1.5.6, épinglé par le `bootstrapScript` du
  chart. La lecture d'un catalogue écrit en 2.0 est à vérifier en recette (voir
  `helm/README.md`, « Points de vigilance »).
