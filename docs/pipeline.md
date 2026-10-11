# Pipeline interactif

<!-- Lien HTML brut : le rendu kedro-viz est copié dans site/pipeline/ après mkdocs build -->
<a class="md-button md-button--primary" href="pipeline/index.html">Ouvrir le pipeline interactif</a>

Cette page renvoie vers la visualisation **kedro-viz** du pipeline, générée à chaque
publication du site par `kedro viz build` puis copiée dans le dossier `pipeline/` du
site. Il s'agit d'une application statique (aucun serveur ni secret nécessaire) qui
affiche :

- le graphe orienté des nœuds Kedro et des jeux de données, par pipeline enregistré
  (`downloads`, `baci`, `vulnerabilities`, `synthesis`, `serving`, `maintenance`, ainsi
  que les cadences `daily` et `weekly`) ;
- pour chaque nœud : ses entrées, ses sorties, ses paramètres, ses étiquettes (étape,
  expérience MLflow, cadence) et son code source ;
- pour chaque jeu de données : son type et sa configuration (tables DuckLake, registres
  de fraîcheur, catalogue de service).

La visualisation décrit la **structure** du pipeline : elle n'affiche ni statistiques
d'exécution ni aperçus de données (les jeux de données ne se connectent pas à
l'instanciation). Pour le détail de chaque exécution, voir la page
[Supervision](supervision.md).

!!! note "Chargement"
    La visualisation charge ses ressources par chemins relatifs : elle fonctionne depuis
    le sous-dossier `pipeline/` du site. Son code JavaScript est volumineux (plusieurs
    Mo) : le premier affichage peut prendre quelques secondes.
