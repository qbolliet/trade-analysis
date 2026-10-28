# Tableaux de bord

Les vulnérabilités sont restituées dans **Apache Superset**, qui lit directement le
catalogue DuckLake `serving` alimenté par le pipeline. La supervision des exécutions est
dans MLflow : voir [Supervision](supervision.md).

## Tableau de bord « Vulnérabilités »

Un parcours unique : pour une **année** et un **flux** (import ou export), on interroge
soit un **pays**, soit un **produit**, chacun dans son onglet.

- **Par pays** : les produits du pays, classés du plus au moins vulnérable.
- **Par produit** : les pays déclarants, classés de la même façon.

Le tableau donne :
- le code et le libellé ;
- le **rang consensus** (1 = le plus vulnérable) ;
- l'**incertitude du rang**, sous la forme « [q25–q75] (n) » ;
- le score de consensus (0 à 1) ;
- les sept métriques (HHI, CDI2, CDI3, WORLD_HHI, CENTRALITY_RISK, CLUSTERING_W, SPOF),
  avec leur nombre d'alertes. Une métrique au-delà de son seuil d'alerte apparaît en rouge.

Sous le tableau, deux courbes portent sur les cinq unités les mieux classées l'année
choisie, plus celles que l'on ajoute par le filtre « Ajouter des produits / pays » :
- l'évolution des **échanges avec le monde**, en valeur (€) ou en quantité (100 kg) au
  choix ;
- l'évolution d'une **métrique** au choix, le rang consensus par défaut.

!!! note "Lire l'incertitude du rang"
    Plusieurs méthodes de synthèse classent chaque ligne. L'incertitude est l'intervalle
    interquartile des rangs qu'elles lui donnent : un intervalle étroit signifie que les
    méthodes s'accordent. Elle mesure le **désaccord entre méthodes**, pas une incertitude
    d'échantillonnage. `n` est le nombre de méthodes prises en compte : 10 pour le
    classement des produits d'un pays, 3 pour celui des pays d'un produit (les méthodes
    pondérées ne sont pas estimées sur des groupes d'environ 22 pays).

Les codes SH6 et NC8 sont classés ensemble. Un code SH6 et son unique code NC8 se suivent
donc souvent, avec des métriques identiques.

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

Les courbes reposent sur des datasets virtuels à gabarit Jinja : leur SQL retient les N
unités les mieux classées l'année choisie. Superset doit donc activer les gabarits :

```python
FEATURE_FLAGS = {**globals().get("FEATURE_FLAGS", {}), "ENABLE_TEMPLATE_PROCESSING": True}
```

!!! note "Correctif hors Helm"
    Sur le service actuel, l'écouteur et ce drapeau ont été ajoutés directement aux
    Secrets Kubernetes du service. Un redéploiement depuis l'interface Onyxia les perd :
    il faut alors les rejouer (détails dans `superset/README.md`), en attendant leur
    reprise par le chart Helm.

## Tables de service

Les tables du schéma `dashboard` (`demo_dashboard` en démonstration) sont **dérivées**
des tables de résultats par des requêtes SQL déclarées dans
`config/base/parameters_serving.yml`. Elles sont larges, libellées et prêtes à
l'affichage.

| Table | Grain | Contenu |
|---|---|---|
| `cell_scores` | classification, déclarant, produit, flux, année | métriques et alertes ; scores et rangs de synthèse ; incertitude du rang (`rank_min/max/q25/q75/spread_<niveau>`, `n_methods_<niveau>`) ; échanges totaux avec le monde (`trade_value_eur`, `trade_quantity_100kg`) ; libellés « code — libellé » ; métriques normalisées. **Seule table lue par le tableau de bord** |
| `flows` | déclarant, produit, flux, année, partenaire | valeur (`value_eur`), quantité (`quantity_100kg`) et part des principaux partenaires, plus les agrégats |
| `coherence_metrics`, `coherence_methods` | contexte, niveau, statistique, paire | cohérence entre métriques et entre méthodes (SQL Lab) |
| `products`, `countries` | source, code | référentiels (SQL Lab) |
| `hs_concordance`, `hs_vintages` | — | concordances et millésimes du Système harmonisé |

Les paramètres sont décrits dans la page [Configuration](configuration.md) :
- méthodes de synthèse exposées (`METHODS`) ;
- méthodes de l'incertitude (`UNCERTAINTY_METHODS`) ;
- niveaux de comparaison (`LEVELS`) ;
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
  `--default-year` pour l'année affichée par défaut).
- **Version de DuckDB** : si Superset ne lit plus le catalogue après une mise à jour de
  DuckDB côté pipeline, aligner la version de `duckdb` du service Superset.

Le détail de la construction (parcours, mécanisme du top N, réimport, pièges rencontrés)
est dans
[`superset/README.md`](https://github.com/qbolliet/trade-analysis/blob/main/superset/README.md).
