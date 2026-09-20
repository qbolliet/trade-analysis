# Workflow de transition (Argo, profil `demo`)

Manifestes **écrits à la main** qui enchaînent les scripts existants selon le DAG cible de
`PIPELINE_ARCHITECTURE.md` §2.2 (PD-19, phase 0). Ils seront **remplacés** par le workflow
généré depuis Kedro (K-15, `kubernetes/generated/`) : ne pas les enrichir.

| Fichier | Ressource |
|---|---|
| `workflowtemplate.yaml` | `WorkflowTemplate` `trade-pipeline-transition` |
| `cronworkflow.yaml` | `CronWorkflow` `trade-pipeline-transition-daily` (01:00 Europe/Paris) |

## Ce que fait le workflow

```
download-eurostat ──► partners ─────────────┐
                                            ├─► synthesis ─► coherence ─► serving*
download-comtrade ──► baci ──► network ─────┘                     (partners aussi)
```

* Chaque tâche est un pod du template `script`, qui lance un point d'entrée `[project.scripts]`
  (`eurostat-script`, `comtrade-script`, `baci-hs-script`, `vulnerabilities-*-script`).
* Les dépendances aval sont **tolérantes** : `(amont.Succeeded || amont.Failed)`. Les étapes
  ne lisent que des données commitées et décident seules, par leurs registres, de ce qui est
  à recalculer. Le workflow reste `Failed` si une tâche a échoué.
* `serving*` : `serving-script` (K-03b) n'existe pas encore ; la tâche est ignorée tant que
  `publish-serving` vaut `false`. Elle est sérialisée par le mutex `trade-serving`.
* Ressources : téléchargements 1 CPU / 2 Gi ; `baci` 4 CPU / 32 Gi ; autres 4 CPU / 16 Gi.
  Le namespace n'a pas de quota CPU/mémoire total (ARCH §8, PQ-16).
* Reprises : 1 sur erreur d'infrastructure (`OnError`), 0 pour les téléchargements.
* `activeDeadlineSeconds: 82800` (23 h), pods réussis supprimés (`podGC: OnPodSuccess`).

### Paramètres

| Nom | Défaut | Rôle |
|---|---|---|
| `image-tag` | `sha-543f256` | tag de `ghcr.io/qbolliet/trade-analysis` (`sha-<court>` ou nom de branche) |
| `profile` | `demo` | `config/profiles/<profil>/` ; `base` = chemins de configuration historiques |
| `publish-serving` | `false` | active la tâche `serving` |
| `max-runtime-hours` | `10` | **réservé, sans effet** : le budget des téléchargements est `MAX_RUNTIME` dans le YAML du profil |

Profil : un `sh -c` d'entrée exporte `COMTRADE|EUROSTAT|BACI|VULNERABILITIES|SYNTHESIS|RUNTIME_CONFIG_PATH`
vers `config/profiles/<profil>/…` ; pour `base` il n'exporte rien et les scripts retombent sur
leurs défauts (`config/datasets/*.yaml`, `config/baci.yaml`…).

### Secrets attendus (PD-18)

`trade-s3-credentials`, `comtrade-api-credentials`, `trade-postgres-credentials`,
`trade-mlflow-credentials`. Le template injecte aussi `AWS_SESSION_TOKEN=""` : `statflows`
(`storage/_connection.py`) lit cette variable sans défaut (C-03 côté dépendance).

## Utilisation

Le binaire `argo` n'est pas installé dans le pod VSCode : tout se fait avec `kubectl`
(l'interface web Argo du service convient aussi).

```bash
# Déployer / mettre à jour
kubectl apply --dry-run=server -f kubernetes/transition/
kubectl apply -f kubernetes/transition/

# Soumettre une exécution manuelle
kubectl create -f - <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {generateName: trade-transition-manual-}
spec:
  workflowTemplateRef: {name: trade-pipeline-transition}
  arguments: {parameters: [{name: profile, value: demo}]}
EOF

# Changer d'image : ajouter -p équivalent dans arguments (ex. image-tag = sha-1a2b3c4),
# ou modifier la valeur par défaut dans workflowtemplate.yaml puis réappliquer.

# Suivre
kubectl get wf                                   # phases
kubectl get wf <nom> -o jsonpath='{range .status.nodes.*}{.displayName}={.phase} {end}'
kubectl logs -f -l workflows.argoproj.io/workflow=<nom> -c main --prefix

# Arrêter / relancer
kubectl patch wf <nom> --type merge -p '{"spec":{"shutdown":"Terminate"}}'

# Suspendre / reprendre le CronWorkflow (équivalent de `argo cron suspend|resume`)
kubectl patch cronworkflow trade-pipeline-transition-daily --type merge -p '{"spec":{"suspend":true}}'
kubectl patch cronworkflow trade-pipeline-transition-daily --type merge -p '{"spec":{"suspend":false}}'
```

## Remplacement

En K-15, `kedro trade render-argo` génère `kubernetes/generated/` (`WorkflowTemplate`
`trade-pipeline`, deux `CronWorkflow`). Ordre : suspendre le cron de transition, appliquer les
manifestes générés, vérifier une exécution, puis supprimer `trade-pipeline-transition*` et ce
dossier. Les deux workflows n'écrivent pas au même endroit tant que le profil `demo` isole ses
sorties (préfixes `demo_`), mais ne pas les laisser tourner ensemble sur la même cible.
