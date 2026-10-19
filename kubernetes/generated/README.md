# Manifestes Argo de production (générés)

Manifestes **générés** depuis les pipelines Kedro par `kedro trade render-argo` : ne pas
les modifier à la main. La CI (`.github/workflows/ci.yml`) échoue s'ils ne correspondent
plus au code (`render-argo --check`).

| Fichier | Ressource |
|---|---|
| `workflowtemplate.yaml` | `WorkflowTemplate` `trade-pipeline` : templates DAG `daily` et `weekly`, gestionnaire de fin, mutex |
| `cronworkflow-daily.yaml` | `CronWorkflow` `trade-pipeline-daily` : tous les jours à 01:00 Europe/Paris, point d'entrée `daily`, 23 h max |
| `cronworkflow-weekly.yaml` | `CronWorkflow` `trade-pipeline-weekly` : chaque `runtime.WEEKLY_DAY` (samedi) à 02:00, point d'entrée `weekly`, 6 jours max |

## Ce que fait le workflow

```
daily   download-eurostat ─► compute-partner-vulnerabilities ─► publish-serving*
        download-comtrade ───────────────────────────────────────┘
weekly  prepare-baci ═► process-baci-hs1992 … hs2022 ─► compute-network-vulnerabilities
          ─► compute-synthetic-scores ─► compute-synthesis-coherence ─► publish-serving*
onExit  maintain-ducklake*                                     (* : mutex, voir plus bas)
```

* **Une tâche = un pod = un nœud Kedro** (les téléchargements sont des tâches fusionnées),
  exécuté par `python -m kedro_pipeline --pipeline=__default__ --env=cloud --nodes=<nœud>
  --params=…`. La commande `kedro run` n'est pas utilisée : c'est celle d'argo-kedro, qui
  ignore `--params`.
* **Dépendances** calculées par argo-kedro à partir des datasets Kedro, **dans chaque
  point d'entrée** : une table produite par l'autre cadence est lue telle qu'elle est
  (les partenaires du `daily` lisent les concordances BACI du dernier `weekly`).
  Elles tolèrent l'échec amont (`(x.Succeeded || x.Failed)`) : chaque étape décide par
  ses registres de fraîcheur de ce qu'elle recalcule. Seule exception : `process-baci-*`
  exige `prepare-baci.Succeeded`. Le workflow reste `Failed` si une tâche a échoué.
* **Mutex** : `publish-serving` (`trade-serving`) et la maintenance (`trade-maintenance`)
  sont sérialisés entre exécutions concurrentes : les deux cadences peuvent se recouvrir.
* **Ressources** (`config/base/argo.yml`, `machine_types`) : téléchargements
  1 CPU / 8 Gi, maintenance 1 CPU / 2 Gi ; BACI 4 / 32 Gi ; synthèse et cohérence 8 / 32 Gi ;
  autres 4 / 16 Gi. `parallelism: 8`.
* **Reprises** : 1 sur erreur d'infrastructure (`OnError`), 0 pour les téléchargements.
* **Suivi** : un run MLflow par tâche, dans l'expérience du tag `experiment.*` du nœud
  (`MLFLOW_EXPERIMENT_NAME`), regroupés par le tag `workflow_id`.
* **Secrets** : `trade-s3-credentials`, `comtrade-api-credentials`,
  `trade-postgres-credentials`, `trade-mlflow-credentials`.
* **Environnement Kedro figé** à celui du rendu (`cloud`) : le nombre de tâches BACI suit
  les millésimes cibles de cet environnement. Il n'y a pas de paramètre `kedro-env`.

### Paramètres du workflow

| Nom | Défaut | Rôle |
|---|---|---|
| `image-tag` | `deployment.tag` de `argo.yml` | étiquette de `ghcr.io/qbolliet/trade-analysis` |
| `force-steps` | *(vide)* | étapes forcées, séparées par **`;`** (ex. `partners;synthesis`, `all`) |
| `force-metrics` | *(vide)* | métriques forcées (ex. `HHI;CDI2`) |
| `force-methods` | *(vide)* | méthodes de synthèse forcées (ex. `critic_sum`) |
| `extra-params` | *(vide)* | paires `clé=valeur` ajoutées à `--params`, séparées par `,` (ex. `runtime.FORCE_SCOPE.VINTAGES=HS2017,runtime.FORCE_SCOPE.PERIODS=2020;2021`) |

**Jamais de `,` dans une valeur** : Kedro découpe `--params` sur les virgules avant de
séparer clés et valeurs ; la tâche échouerait au démarrage. Les CronWorkflow n'exposent
aucun forçage.

## Régénérer

```bash
uv run kedro trade render-argo                              # étiquette d'image de argo.yml
uv run kedro trade render-argo --image-tag sha-1a2b3c4       # autre étiquette par défaut
uv run kedro trade render-argo --check                       # contrôle (CI) : diff et code ≠ 0
```

À relancer après toute modification des pipelines (nœuds, tags, `machine_type`), de
`config/base/argo.yml`, de `runtime.WEEKLY_DAY`, de `tracking.SYSTEM_METRICS` ou des
gabarits `kedro_pipeline/deploy/templates/`, puis committer les fichiers.

Pour changer d'image durablement : publier l'image (`image.yml`), renseigner son étiquette
immuable `sha-<SHA court>` dans `deployment.tag` de `argo.yml` (et
`image_pull_policy: IfNotPresent`), régénérer, appliquer.

**Démonstration** (`demo` : BACI HS2017 seul) : rendre hors du dépôt, mêmes noms de
ressources, qui remplacent donc ceux de production sur le cluster :

```bash
uv run kedro trade render-argo --env demo --output-dir /tmp/argo-demo
```

## Appliquer (depuis un terminal VSCode Onyxia)

Le binaire `argo` n'est pas installé dans le pod VSCode : tout se fait avec `kubectl`.

```bash
# kubectl ne lit, dans un dossier, que les fichiers .yaml / .yml / .json (pas ce README)
kubectl apply --dry-run=server -f kubernetes/generated/
kubectl apply -f kubernetes/generated/
```

## Exécution ponctuelle (rattrapage, forçage)

```bash
# Point d'entrée hebdomadaire, synthèse recalculée
kubectl create -f - <<'YAML'
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {generateName: trade-pipeline-weekly-manual-}
spec:
  workflowTemplateRef: {name: trade-pipeline}
  entrypoint: weekly
  activeDeadlineSeconds: 518400
  arguments:
    parameters:
      - {name: force-steps, value: synthesis}
YAML

# Point d'entrée quotidien, HHI recalculé partout sur les partenaires
kubectl create -f - <<'YAML'
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {generateName: trade-pipeline-daily-manual-}
spec:
  workflowTemplateRef: {name: trade-pipeline}
  entrypoint: daily
  activeDeadlineSeconds: 82800
  arguments:
    parameters:
      - {name: force-steps, value: partners}
      - {name: force-metrics, value: HHI}
YAML

# BACI HS2017 seul réestimé (le DAG weekly entier s'exécute ; les autres millésimes
# n'ont rien à faire si leurs registres sont à jour)
#   - {name: force-steps, value: baci}
#   - {name: extra-params, value: "runtime.FORCE_SCOPE.VINTAGES=HS2017"}
```

Avec la CLI `argo` (poste disposant d'un accès) :
`argo submit --from workflowtemplate/trade-pipeline --entrypoint weekly -p force-steps=synthesis --watch`.

Suivi : `kubectl get wf -l trade-analysis/entry-point=daily` (exécutions planifiées),
`kubectl logs -f -l workflows.argoproj.io/workflow=<nom> -c main --prefix`.

## Remplacer le workflow de transition

Le workflow de transition (`kubernetes/transition/`, scripts, profil `demo`) et le
workflow généré ne doivent pas tourner ensemble sur la même cible.

1. Suspendre le cron de transition :
   `kubectl patch cronworkflow trade-pipeline-transition-daily --type merge -p '{"spec":{"suspend":true}}'`
   et attendre la fin d'une éventuelle exécution en cours (`kubectl get wf`).
2. Publier l'image contenant ce code et épingler son étiquette (voir « Régénérer »).
3. Appliquer les trois manifestes (voir « Appliquer »).
4. Soumettre une exécution ponctuelle de **chaque** point d'entrée (voir plus haut) et
   vérifier dans Argo et MLflow (runs filtrés par `tags.workflow_id`, colonne `health`).
5. Après une exécution réussie de chaque cadence, supprimer l'ancien workflow :
   `kubectl delete cronworkflow trade-pipeline-transition-daily` puis
   `kubectl delete workflowtemplate trade-pipeline-transition`, et le dossier
   `kubernetes/transition/`.

Retour arrière : suspendre `trade-pipeline-daily` et `trade-pipeline-weekly`
(`kubectl patch cronworkflow <nom> --type merge -p '{"spec":{"suspend":true}}'`),
reprendre le cron de transition (`"suspend":false`).
