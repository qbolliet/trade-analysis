# Supervision

La supervision du pipeline se fait **exclusivement dans MLflow**. Chaque tâche du
workflow produit un run, nommé `<nœud>-<workflow_id>`, qui répond à la question « la
tâche s'est-elle bien passée, et sinon pourquoi ? » sans ouvrir de fichier ni écrire de
requête. Seuls le graphe des tâches et l'état des pods se lisent dans l'interface Argo
(un lien y figure dans la description de chaque run).

## Lire un run MLflow

| Onglet | Ce qu'on y lit |
|---|---|
| **Overview** | La **description** du run (Markdown rendu en tête de page) : verdict, contrôles en défaut, unités prévues / réussies / en échec, chiffres clés, liens. Puis les paramètres (configuration aplatie) et les tags : `workflow_id`, `node`, `health`, `git_sha`, `image_tag`, `kedro_env`, `forced`, `checks_failed`. |
| **Model metrics** | Toutes les métriques, en sections par préfixe (`gravity/`, `download/`, `checks/`…) ; courbes par `step` (année pour BACI, index de requête pour les téléchargements). |
| **System metrics** | CPU, mémoire, disque et réseau du pod au cours du temps : à consulter pour un problème de ressources. |
| **Artifacts** | `report/report.html` (rapport autonome avec figures et tables, une section par étape pour BACI), `report/summary.md` (copie intégrale de la description), `report/checks.csv`, `tables/*.csv` (couverture, écarts-types par pays, coefficients de gravité…), `failures.csv` (unités en échec et message). |

## Contrôles et verdict `health`

À la fin de chaque étape, des **contrôles déclaratifs** sont évalués sur les métriques du
run. Chacun compare une métrique à un seuil (`op` ∈ `<`, `<=`, `>`, `>=`, `==`, `!=` ;
le contrôle est réussi quand `valeur op seuil` est vrai) et porte une sévérité
`warning` ou `error`. Deux contrôles implicites s'ajoutent à tous les nœuds : « aucune
unité en échec » (`error`) et « toutes les unités prévues traitées » (`warning`). Une
métrique absente donne un contrôle ignoré (*skipped*), sans effet sur le verdict.

Le tag `health` résume le run :

| `health` | Signification |
|---|---|
| `ok` | Tous les contrôles sont réussis (ou ignorés) : rien à faire. |
| `warning` | Au moins un contrôle de sévérité `warning` est en défaut. |
| `failed` | Au moins un contrôle `error` est en défaut, ou la tâche a été interrompue. |

Les métriques `checks/n_passed`, `checks/n_warnings` et `checks/n_failed` sont
journalisées pour pouvoir filtrer et trier les runs. Pour un run `warning` ou `failed` :
lire d'abord la description (onglet *Overview*), puis `report/report.html` pour le détail
et *System metrics* en cas de problème de ressources.

!!! warning "Run resté `RUNNING`"
    Un pod tué (mémoire insuffisante, délai dépassé) ne peut pas fermer son run. La
    maintenance le clôt en fin de workflow avec `health=failed` et la description
    « tâche interrompue — voir Argo » : il faut alors lire le pod dans Argo. Sans
    `MLFLOW_TRACKING_URI`, ou serveur injoignable, les tâches s'exécutent sans suivi
    (avertissement dans les journaux du pod).

## Voir une exécution complète par `workflow_id`

Tous les runs d'une même exécution du workflow portent le même tag `workflow_id`, égal
au nom du workflow dans Argo (par exemple `trade-pipeline-daily-…`).

1. Dans l'interface Argo, noter le nom du dernier workflow (`workflow_id`).
2. Dans MLflow, ouvrir chaque expérience concernée — `trade-00-maintenance`,
   `trade-01-downloads`, `trade-02-baci`, `trade-03-vulnerabilities`,
   `trade-04-serving` — et filtrer la liste des runs avec
   `tags.workflow_id = '<workflow_id>'`.
3. Afficher les colonnes `tags.node`, `tags.health` et `metrics.checks/n_failed` (menu
   *Columns*) : tout `ok` signifie que rien n'est à faire.

Modèles d'URL (vérifiés sur MLflow 3.15 ; remplacer `<mlflow>`, `<id>` et
`<workflow_id>`) :

- liste filtrée d'une expérience — le chemin `/runs` est nécessaire :
  `<mlflow>/#/experiments/<id>/runs?searchFilter=tags.workflow_id%20%3D%20'<workflow_id>'` ;
- toute l'exécution, toutes expériences confondues :
  `<mlflow>/#/compare-experiments/s?experiments=%5B%22<id1>%22%2C%22<id2>%22%5D&searchFilter=tags.workflow_id%20%3D%20'<workflow_id>'`
  (ou : liste des expériences, cocher les cinq `trade-*`, *Compare*, puis saisir le
  filtre).

!!! tip "Expérience ouverte sur *Traces*"
    Si une expérience s'ouvre sur l'onglet *Traces* (mode « GenAI »), basculer sur *Model
    training* en haut à gauche. Le pipeline pose le tag `mlflow.experimentKind` pour
    éviter ce cas.

## Ajuster un seuil de contrôle

Les seuils sont des **paramètres**, pas du code : ils vivent dans le bloc `CHECKS` de
`config/base/parameters_tracking.yml` (voir la page [Configuration](configuration.md)).
Pour en modifier un :

1. éditer la valeur (`threshold`) ou la sévérité (`severity`) du contrôle concerné ;
2. pousser la modification sur `main` : l'image est republiée ;
3. rien n'est recalculé — les contrôles ne font pas partie des empreintes de fraîcheur
   des données.

Le nom d'une métrique visée doit être une métrique réellement émise par le nœud : un
test échoue sinon.
