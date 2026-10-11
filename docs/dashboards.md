# Tableaux de bord

Les vulnérabilités sont restituées dans **Apache Superset**, qui lit directement le
catalogue DuckLake `serving`, alimenté par le pipeline. (La supervision des exécutions,
elle, est dans MLflow : voir [Supervision](supervision.md).)

## Tableau de bord « Vulnérabilités »

Un tableau de bord à deux onglets, construit sur les tables de service du schéma
`demo_dashboard` (profil de démonstration) :

- **Pays** : pour un déclarant (pays ou Union européenne), classement des produits par
  score synthétique, métriques de vulnérabilité partenaires et de réseau, cohérence entre
  métriques ;
- **Produit** : pour un produit, classement des pays, flux par partenaire, évolution
  dans le temps.

Huit jeux de données et quinze graphiques le composent. Convention de lecture :
**rang 1 = unité la plus vulnérable**. Le filtre sur l'année doit toujours avoir une
valeur : il permet l'élagage des partitions DuckLake (sans lui, chaque graphique lit
toutes les années).

## Connexion DuckDB au catalogue `serving`

Superset se connecte par `duckdb-engine` (connexion nommée `serving`, **lecture seule**).
Le catalogue DuckLake `serving` est attaché à chaque connexion DuckDB par un écouteur
SQLAlchemy `connect` défini dans `superset_config.py` : il charge les extensions
`ducklake` et `httpfs`, crée un secret S3 en mémoire et exécute `ATTACH … (READ_ONLY)`.
Les identifiants PostgreSQL (métadonnées du catalogue) et S3 (fichiers Parquet) viennent
de l'environnement des pods Superset ; l'export du tableau de bord n'en contient aucun.

!!! note "Mécanisme retenu"
    Un secret DuckDB persistant sur disque a été écarté : les pods Superset (web et
    worker Celery, qui exécute SQL Lab en asynchrone) n'ont pas de volume commun.
    Le correctif appliqué aux Secrets Kubernetes est perdu si le service est redéployé
    depuis l'interface Onyxia : il doit alors être rejoué (détails dans
    `superset/README.md`).

## Tables de service

Les tables du schéma `dashboard` (`demo_dashboard` en démonstration) sont **dérivées**
des tables de résultats par des requêtes SQL déclarées dans
`config/base/parameters_serving.yml` ; elles sont larges, libellées et prêtes à
l'affichage.

| Table | Grain | Contenu |
|---|---|---|
| `cell_scores` | classification, déclarant, produit, flux, année | métriques de vulnérabilité, alertes, scores et rangs de synthèse par méthode et niveau, métriques normalisées pour la heatmap |
| `flows` | déclarant, produit, flux, année, partenaire | valeur et part des principaux partenaires (plus `WORLD` et `EXT_EU`) |
| `coherence_metrics` | contexte, niveau, statistique, paire | cohérence entre métriques |
| `coherence_methods` | contexte, niveau, statistique, paire | cohérence entre méthodes de synthèse |
| `products` | source, classification, code | libellés et niveaux de nomenclature |
| `countries` | source, code | libellés, codes ISO3 et M49, déclarant / partenaire / agrégat |
| `hs_concordance`, `hs_vintages` | — | concordances et millésimes du Système harmonisé |

La liste des méthodes de synthèse exposées, des niveaux de comparaison, de l'indicateur
principal et du nombre de partenaires conservés est configurable (clés `METHODS`,
`LEVELS`, `PRIMARY_METHOD`, `TOP_PARTNERS`) : voir la page
[Configuration](configuration.md).

## Rafraîchissement

- Les tables de service sont republiées par l'étape `publish_serving` à la fin de chaque
  exécution **`daily`** (métriques partenaires) et **`weekly`** (synthèse, cohérence,
  réseau). Chaque publication recrée les tables en une seule transaction : Superset voit
  l'état précédent jusqu'au `COMMIT`.
- Si un graphique affiche des données anciennes : vider son cache (*… → Force refresh*) ;
  il expire de toute façon après 24 heures.
- Après un changement de tables de service : republier (`argo submit … --entrypoint
  daily`) puis, dans Superset, *Datasets → Sync columns from source*.
- Après modification du tableau de bord dans l'interface : *Export*, remplacer le dossier
  `superset/vulnerabilites/`, puis commit.
- Si Superset ne lit plus le catalogue après une mise à jour de DuckDB côté pipeline :
  aligner la version de `duckdb` du service Superset.

Le détail de la construction (réimport, passage du profil de démonstration à la
production, pièges rencontrés) est dans
[`superset/README.md`](https://github.com/qbolliet/trade-analysis/blob/main/superset/README.md).
