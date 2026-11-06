# Tableaux de bord

Les vulnérabilités sont restituées dans **Apache Superset**, qui lit directement le
catalogue DuckLake `serving` alimenté par le pipeline. La supervision des exécutions est
dans MLflow : voir [Supervision](supervision.md).

## Tableau de bord « Vulnérabilités »

Un parcours unique : pour une **année** et un **flux** (import ou export), on interroge
soit un **pays**, soit un **produit**, chacun dans son onglet. Chaque onglet s'ouvre sur un
bloc d'aide (ce que montre la page, comment la lire, comment s'en servir).

- **Par pays** : les produits du pays, classés du plus au moins vulnérable au sein d'un
  **niveau de nomenclature** au choix (SH2, SH4, SH6 ou NC8 selon les données ; SH6 par
  défaut), puis les **principaux partenaires** du pays pour un produit, puis deux courbes.
- **Par produit** : les pays déclarants, classés de la même façon, puis le **diagramme de
  flux** du produit entre les pays de l'UE et leurs partenaires, puis deux courbes.

### Le tableau

Une ligne par produit (onglet *Par pays*) ou par pays (onglet *Par produit*), triée par
rang. Une cellule rouge signale un indicateur au-delà de son seuil d'alerte ; la colonne
« Seuils d'alerte dépassés » est jaune de 1 à 2 alertes, rouge à partir de 3. La
**fourchette du rang** est colorée selon sa largeur rapportée au nombre de lignes classées
(produits du pays au sein du niveau, ou pays du produit) : vert jusqu'à 5 %, vert clair
jusqu'à 10 %, orange jusqu'à 20 %, rouge au-delà ; l'infobulle donne cette part.

Les valeurs des indicateurs ne se comparent pas d'une colonne à l'autre : elles n'ont ni la
même échelle ni la même unité (part, indice de concentration, rapport, position parmi les
produits). Le rang et le score les résument. Dans l'onglet *Par produit*, les quatre
indicateurs de réseau (concentration mondiale, poids des pays pivots, cloisonnement, point
de défaillance unique) décrivent le commerce mondial du produit : ils ont la même valeur
pour tous les pays du tableau. Le bloc d'aide de chaque onglet reprend une définition
courte de chaque colonne.

| Colonne | Sigle | Définition (import ; export) | Seuil d'alerte |
|---|---|---|---|
| Rang (1 = le plus vulnérable) | — | Rang consensus : rang de Borda des méthodes de synthèse multicritère (méthode `consensus_borda`), au sein du niveau de nomenclature (*Par pays*) ou du produit (*Par produit*). | — |
| Fourchette du rang | — | Rangs donnés à la ligne par les méthodes de synthèse : quartiles q25 et q75 de ces rangs, affichés « q25 – q75 ». Mesure du désaccord entre méthodes (voir l'encadré). Couleur : largeur ÷ lignes classées, seuils 5, 10 et 20 %. | — |
| Largeur de la fourchette | — | q75 − q25, en rangs. | — |
| Score de vulnérabilité (0 à 1) | — | `1 − (rang − 1)/(n − 1)`, `n` lignes classées : 1 = la plus vulnérable. | — |
| Concentration des partenaires | HHI | Indice de Herfindahl-Hirschman : somme des carrés des parts des partenaires individuels dans les échanges du pays pour le produit ; ses fournisseurs à l'import, ses clients à l'export. 1 = un seul partenaire. | 0,5 |
| Part des échanges hors UE | CDI2 | Échanges avec les pays hors UE / échanges totaux du pays (agrégats Comext extra-UE et monde du même flux). Vaut 1 pour l'Union entière. | 0,5 |
| Hors UE / flux de sens inverse | CDI3 | Import : importations hors UE / exportations totales du pays ; au-delà de 1, les achats hors UE dépassent ce que le pays exporte, signe d'une faible capacité à les remplacer par sa production. Export : exportations hors UE / importations totales (miroir proposé par analogie, sans ancrage dans la littérature). Infini si le flux inverse est nul. | 1 |
| Concentration mondiale | WORLD_HHI | Indice de Herfindahl-Hirschman des parts des pays dans le commerce mondial du produit (BACI) : exportations mondiales (offre) à l'import, importations mondiales (demande) à l'export. | 0,5 |
| Poids des pays pivots | CENTRALITY_RISK | Écart type de la centralité pondérée des fournisseurs (import) ou des acheteurs (export, graphe transposé) dans le réseau mondial du produit : quelques pays très centraux donnent une forte dispersion. 2,5 correspond à un premier exportateur qui fournit environ deux tiers du monde. | 2,5 |
| Cloisonnement du réseau | CLUSTERING_W | Coefficient de regroupement pondéré de Barrat, moyen sur les pays : tendance des partenaires d'un pays à commercer aussi entre eux. Élevé : un marché organisé en blocs, avec peu de marge de diversification après un choc. Graphe non orienté : même valeur à l'import et à l'export. | 0,5 |
| Point de défaillance unique | SPOF | Moyenne des percentiles du produit en poids des pays pivots et en concentration mondiale, parmi les produits de même nomenclature et de même année ; dans ]0, 1], 1 = le plus exposé. | 0,9 |
| Seuils d'alerte dépassés (sur 7) | — | Nombre des sept indicateurs au-delà de leur seuil. | — |

Pour les sept indicateurs, plus la valeur est élevée, plus la ligne est vulnérable, à
l'import comme à l'export. Le sigle reste en fin de libellé pour les lecteurs qui le
connaissent ; le même libellé est employé dans le menu « Indicateur » des courbes.

!!! note "Lire la fourchette du rang"
    Plusieurs méthodes de synthèse multicritère classent chaque ligne. La fourchette est
    l'intervalle interquartile des rangs qu'elles lui donnent : étroite, les méthodes
    s'accordent et le classement est robuste ; large, il dépend de la façon de pondérer les
    indicateurs. Elle mesure le **désaccord entre méthodes**, pas une incertitude
    d'échantillonnage. Elle repose sur 10 méthodes pour le classement des produits d'un pays
    et sur 3 pour celui des pays d'un produit (les méthodes pondérées ne sont pas estimées
    sur des groupes d'environ 22 pays) ; elle est vide quand moins de deux méthodes ont
    classé la ligne.

!!! note "Classement par niveau de nomenclature"
    La synthèse classe les produits par finesse de nomenclature : le niveau du code
    (`product_level` : SH2, SH4, SH6 ou NC8) fait partie du contexte de comparaison, si
    bien qu'un produit n'est comparé qu'à des produits de même niveau. Les synthèses
    calculées avant cette règle classaient tous les niveaux d'un pays ensemble : pour
    elles, l'onglet *Par pays* restreint le classement commun au niveau choisi (ordre de
    chaque méthode conservé, rangs renumérotés de 1 à n). Les codes SH2 et SH4 n'ont pas de
    rang, faute de métriques réseau à ces niveaux.

### Les principaux partenaires (onglet *Par pays*)

Pour le pays choisi et **un** produit, une barre horizontale par partenaire : ses
fournisseurs à l'import, ses clients à l'export. Par défaut, le produit détaillé est le
produit classé premier dans le niveau de nomenclature choisi (première ligne du tableau) ;
le filtre « Produit à détailler » en choisit un autre.

!!! note "Menus préremplis"
    Au chargement, les menus « Produit à détailler », « Produits des courbes » et « Pays des
    courbes » montrent ce que tracent les graphiques par défaut (pour le pays, l'année, le
    flux et le niveau par défaut : le produit classé premier et les cinq premiers produits ;
    pour le produit par défaut, les cinq premiers pays). Superset ne recalcule pas ces
    valeurs quand on change de pays, de produit, d'année ou de flux : les graphiques suivent
    alors le menu. Videz-le pour revenir au produit classé premier et aux cinq premières
    unités.

    Un lien qui contient `native_filters_key=…` rouvre les filtres tels qu'ils étaient
    lors d'une visite antérieure, pas les valeurs par défaut. Pour retrouver celles-ci,
    ouvrez l'adresse du tableau de bord sans ce paramètre.

- Les **10 premiers partenaires** en valeur sont classés du plus important au moins
  important ; leur libellé donne leur rang et leur part dans les échanges du pays, en
  valeur.
- La barre **« Autres pays »**, toujours en dernier, complète jusqu'aux échanges totaux :
  la somme des barres est égale au total, et sa longueur montre le poids de la « longue
  traîne ». Elle est omise si elle est nulle.
- **Couleur** : pays de l'UE ou hors UE. Un pays est compté dans l'UE s'il est déclarant
  dans Comext l'année choisie (Comext ne publie que les États membres).
- **Grandeur** : valeur en euros ou quantité en 100 kg (filtre « Grandeur »). En quantité,
  un partenaire dont la quantité n'est pas déclarée n'a pas de barre (et non une barre
  nulle) ; les partenaires restent ceux des 10 premiers en valeur.

Seuls les codes de la nomenclature en vigueur ont un détail par partenaire.

### Le diagramme de flux (onglet *Par produit*)

Pour le produit choisi, un diagramme de Sankey relie les pays de l'UE à leurs
partenaires : à l'import, des partenaires (à gauche) vers les pays de l'UE (à droite) ; à
l'export, des pays de l'UE vers leurs clients. L'épaisseur d'un flux est proportionnelle
aux échanges (valeur ou quantité, filtre « Grandeur »).

- Côté UE : les États membres déclarants. L'Union prise comme un tout (`EU27_2020`) est
  exclue, car elle doublerait les flux.
- Côté partenaires :
    - les **12 premiers partenaires hors UE** du produit, cumulés sur l'ensemble des
      États membres ;
    - **« Autres pays hors UE »** : pour chaque État membre, ses échanges hors UE moins
      ceux qui vont aux partenaires détaillés ;
    - **« Union européenne (échanges entre États membres) »** : échanges totaux moins
      échanges hors UE, la décomposition de l'indicateur « Part des échanges hors UE ».
- Les échanges entre États membres forment un seul bloc : un pays n'apparaît ainsi jamais
  des deux côtés (le diagramme refuse les cycles), et la dépendance hors UE ressort. La
  somme des flux qui arrivent dans un pays de l'UE est égale à ses échanges totaux.

En démonstration (2021, en valeur), « Autres pays hors UE » et la barre « Autres pays »
pèsent peu à l'import (médiane de 4 % des échanges d'un produit pour le Sankey) et
davantage à l'export, où les clients sont plus dispersés (médiane de 19 %).

### Les courbes

Sous les diagrammes, deux courbes portent sur les unités du filtre « Produits des courbes »
(onglet *Par pays*) ou « Pays des courbes » (onglet *Par produit*) ; filtre vide, sur les cinq
unités les mieux classées l'année choisie. Le menu étant modifiable, les courbes ne montrent
pas forcément les unités les plus vulnérables : leurs titres disent « Échanges avec le
monde » et « Évolution d'un indicateur de vulnérabilité des produits » (ou « … des pays ») :
- l'évolution des **échanges avec le monde**, en valeur (€) ou en quantité (100 kg) au
  choix ;
- l'évolution d'un **indicateur** au choix (filtre « Indicateur »), le rang par défaut.

Les deux courbes montrent toute la série temporelle des unités choisies, quelle que soit
l'année du filtre « Année », qui ne sert qu'à choisir les unités par défaut. La période
couverte dépend de l'indicateur :
- le **rang** et les indicateurs de **réseau** (concentration mondiale, poids des pays
  pivots, cloisonnement, point de défaillance unique) n'existent que les années couvertes
  par le réseau mondial BACI (2017–2021 en démonstration) ;
- les indicateurs **partenaires** (concentration des partenaires, part hors UE, rapport aux
  flux de sens inverse) et les échanges couvrent toutes les années.

Aucune valeur n'est interpolée : une année manquante dans une série laisse un trou, et un
point isolé reste visible grâce à son marqueur. Une unité sans aucune valeur pour
l'indicateur choisi n'apparaît pas dans la légende (par exemple un code NC8 non classé).

L'année doit toujours avoir une valeur, ce qui est le cas par défaut : elle permet
l'élagage des partitions DuckLake.

## Connexion DuckDB au catalogue `serving`

Superset se connecte par `duckdb-engine`, avec une connexion nommée `serving` en **lecture
seule**. Un écouteur SQLAlchemy `connect`, défini dans `superset_config.py`, attache le
catalogue DuckLake `serving` à chaque connexion DuckDB :
- il charge les extensions `ducklake` et `httpfs` ;
- il crée un secret S3 en mémoire ;
- il exécute `ATTACH … (READ_ONLY)`.

Les identifiants PostgreSQL (métadonnées du catalogue) et S3 (fichiers Parquet) viennent
de l'environnement des pods Superset. L'export du tableau de bord n'en contient aucun.

Les courbes et les diagrammes des partenaires reposent sur des datasets virtuels à gabarit
Jinja : leur SQL retient les N unités les mieux classées l'année choisie, ou le produit
détaillé et ses partenaires. Superset doit donc activer les gabarits :

```python
FEATURE_FLAGS = {**globals().get("FEATURE_FLAGS", {}), "ENABLE_TEMPLATE_PROCESSING": True}
```

!!! note "Configuration portée par le chart Helm"
    L'écouteur et ce drapeau font partie du chart `trade-dashboard`
    (`helm/trade-dashboard/files/superset_serving.py`), monté dans les pods Superset et
    exécuté à la fin de `superset_config.py` : ils survivent à tout redéploiement. Le
    tableau de bord est réimporté par le Job du chart après chaque installation ou mise à
    jour (voir `helm/README.md`).

## Tables de service

Les tables du schéma `dashboard` (`demo_dashboard` en démonstration) sont **dérivées**
des tables de résultats par des requêtes SQL déclarées dans
`config/base/parameters_serving.yml`. Elles sont larges, libellées et prêtes à
l'affichage.

| Table | Grain | Contenu |
|---|---|---|
| `cell_scores` | classification, déclarant, produit, flux, année | métriques et alertes ; scores et rangs de synthèse (dont les rangs par pays au sein du niveau de nomenclature, niveau `by_reporter_level`) ; niveau de nomenclature (`product_level_label`) ; incertitude du rang (`rank_min/max/q25/q75/spread_<niveau>`, `n_methods_<niveau>`) ; échanges totaux avec le monde (`trade_value_eur`, `trade_quantity_100kg`) ; libellés « code — libellé » ; métriques normalisées. Lue par les tableaux et les courbes |
| `flows` | déclarant, produit, flux, année, partenaire | valeur (`value_eur`), quantité (`quantity_100kg`) et part des principaux partenaires, plus les agrégats. Lue par les diagrammes des partenaires |
| `coherence_metrics`, `coherence_methods` | contexte, niveau, statistique, paire | cohérence entre métriques et entre méthodes (SQL Lab) |
| `products`, `countries` | source, code | référentiels (SQL Lab) |
| `hs_concordance`, `hs_vintages` | — | concordances et millésimes du Système harmonisé |

Les paramètres sont décrits dans la page [Configuration](configuration.md) :
- méthodes de synthèse exposées (`METHODS`) ;
- méthodes de l'incertitude (`UNCERTAINTY_METHODS`) ;
- niveaux de comparaison (`LEVELS`, dont le niveau dérivé `by_reporter_level`) ;
- libellés des niveaux de nomenclature (`PRODUCT_LEVEL_LABELS`) ;
- indicateur principal (`PRIMARY_METHOD`) ;
- agrégats « total avec le monde » (`PARTNERS.TOTAL` : `WORLD`, puis `EXT_EU27_2020` pour
  l'Union) ;
- nombre de partenaires conservés (`TOP_PARTNERS`).

Une quantité Comext nulle pour une valeur positive signale une quantité non déclarée. Elle
est exposée vide, et non comme un flux nul.

## Rafraîchissement

- **Republication** : l'étape `publish_serving` republie les tables de service à la fin de
  chaque exécution **`daily`** (métriques partenaires) et **`weekly`** (synthèse,
  cohérence, réseau). Chaque publication recrée les tables en une seule transaction :
  Superset voit l'état précédent jusqu'au `COMMIT`.
- **Cache** : il expire après 24 heures. Un graphique déjà consulté peut donc afficher
  l'état précédent jusqu'à *… → Force refresh*. Chaque combinaison de filtres a son
  propre cache.
- **Réimport** : le tableau de bord est versionné sous `superset/vulnerabilites/` (schéma
  de production `dashboard`). Il est réimporté à partir de l'archive produite par
  `tools/superset_bundle.py` (option `--schema demo_dashboard` pour la démonstration,
  `--default-year` pour l'année affichée par défaut). Pour mettre à jour un Superset où il
  existe déjà, l'archive doit être de type « assets » (option `--assets`, import par
  `POST /api/v1/assets/import/`) : l'import d'un tableau de bord ne remplace ni ses
  datasets ni ses graphiques.
- **Version de DuckDB** : si Superset ne lit plus le catalogue après une mise à jour de
  DuckDB côté pipeline, aligner la version de `duckdb` du service Superset.

Le détail de la construction (parcours, mécanisme du top N, réimport, pièges rencontrés)
est dans
[`superset/README.md`](https://github.com/qbolliet/trade-analysis/blob/main/superset/README.md).
