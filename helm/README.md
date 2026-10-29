# Charts Helm : déploiement de trade-analysis

Le pipeline et le tableau de bord s'installent par **une commande**, `helm upgrade
--install` du chart parapluie, sur Onyxia (SSP Cloud) comme sur un cluster Kubernetes
quelconque.

| Chart | Rôle | Contenu |
|---|---|---|
| `trade-pipeline` | backend | `WorkflowTemplate` (un DAG par point d'entrée), un `CronWorkflow` par point d'entrée, compte de service facultatif, secrets facultatifs, `helm test` de pré-vol |
| `trade-dashboard` | restitution | configuration de Superset (lecture seule du catalogue DuckLake `serving`, gabarits Jinja), Superset facultatif (chart Superset d'Onyxia), Job d'import du tableau de bord après chaque installation ou mise à jour |
| `trade-analysis` | parapluie | dépendances `file://` vers les deux précédents (`pipeline.enabled`, `dashboard.enabled`) ; Argo Workflows et MLflow amont, facultatifs, hors Onyxia |

Pourquoi deux charts plutôt qu'un :
- **Cycles de vie distincts.** Le pipeline change à chaque image, le tableau de bord
  rarement.
- **Droits distincts.**
- **Pas de redémarrage croisé.** Un `helm upgrade` du pipeline ne redémarre pas Superset.
- **Modèle Onyxia.** Un service correspond à un chart.

Les deux charts sont génériques : rien n'y est propre au projet en dehors des valeurs par
défaut (nom, image, points d'entrée, secrets, export du tableau de bord dans `files/`). Ils
peuvent donc être déplacés tels quels dans un dépôt d'outillage commun à plusieurs projets
Kedro.

Un seul jeu de charts sert toutes les plateformes. La plateforme est choisie par
`global.platform` (`onyxia` ou `generic`) et par l'un des deux préréglages
`trade-analysis/values-onyxia.yaml` ou `trade-analysis/values-generic.yaml`. La surcouche
`values-demo.yaml` sert à la recette.

## Ce qu'Onyxia fournit

Relevé du 2026-10-29 sur le namespace `user-qbollietdgddi`. Les commandes utilisées
(`helm list`, `helm get values` réduit aux noms de clés, `helm get manifest`, `helm show
values`) sont en lecture seule.

| Service | Release / chart (dépôt) | Dernière version de l'index | Ce que le projet en utilise |
|---|---|---|---|
| Argo Workflows | `argo-workflows-470615`, `argo-workflows-1.0.7` (`inseefrlab/helm-charts-automation`, enveloppe d'argo-helm 0.45.x) | 1.0.8 | contrôleur `quay.io/argoproj/workflow-controller:v3.6.10`, lancé avec `--namespaced` ; compte de service `workflow` (créé par la release, avec Role et RoleBinding `workflow`) |
| MLflow | `mlflow-284750`, `mlflow-3.1.6` (`automation`) | 3.1.7 | image `inseefrlab/mlflow:v3.15.1`, Service `mlflow-284750:80` ; Secret de découverte `discoverable-mlflow-284750` |
| PostgreSQL | `postgresql-cnpg-82431`, `postgresql-cnpg-0.5.1` (`inseefrlab/helm-charts-databases`, CloudNativePG) | 0.5.2 | base `serving` des métadonnées du catalogue ; Secret de découverte `discoverable-postgresql-cnpg-82431` |
| Superset | `superset-899573`, `superset-0.1.12` (`inseefrlab/helm-charts-datavisualization`) ; la version est de nouveau présente dans l'index | 0.1.13 = apache/superset 0.13.5 (Superset 4.1.1) + library-chart 1.5.41 | remplacé par le Superset du chart `trade-dashboard` |

**Convention de découverte** (library-chart d'InseeFrLab, lue dans le gabarit
`_discovery.tpl` du chart Superset) : un service publie un **Secret** annoté
`onyxia/discovery: <type>`.

| Type | Clés |
|---|---|
| `postgres` | `postgres-service`, `postgres-port`, `postgres-username`, `postgres-password`, `postgres-database`, `password` |
| `mlflow` | `uri` (URL **externe** en https), `MLFLOW_TRACKING_USERNAME`, `MLFLOW_TRACKING_PASSWORD`, `MLFLOW_S3_ENDPOINT_URL` |

Argo Workflows ne publie pas de Secret de découverte : sa version se lit sur l'image du
Deployment du contrôleur.

**Chart Superset d'Onyxia** (`helm show values superset --repo
https://inseefrlab.github.io/helm-charts-datavisualization`) : c'est une enveloppe. Les
valeurs du chart amont apache/superset se placent sous `superset.*` :
- `configOverrides` (passées par `tpl`) ;
- `bootstrapScript` (installation des pilotes, `duckdb-engine` sans version épinglée par
  défaut) ;
- `init.initscript` ;
- `extraConfigs` (connexions découvertes) ;
- `envFromSecrets`, `extraVolumes` et `extraVolumeMounts` (tous trois passés par `tpl`) ;
- `discovery.*` (connexions importées depuis les Secrets de découverte).

L'enveloppe ajoute l'Ingress du library-chart (`ingress.hostname`, `ingressClassName:
onyxia`). Le chart s'installe en CLI avec nos valeurs.

**Autres constats**
- **NetworkPolicies** sur Argo, MLflow et PostgreSQL : un pod du namespace atteint MLflow
  et PostgreSQL, ce qui couvre les tâches du pipeline et le pré-vol.
- **Secrets du projet** :
  - `trade-postgres-credentials` désigne la même instance et le même utilisateur que le
    Secret de découverte PostgreSQL ;
  - `trade-mlflow-credentials` pointe sur l'URL **interne** du service MLflow, et non sur
    l'URL externe découverte.
- **Rôle PostgreSQL en lecture seule `superset_reader`** : il **n'existe pas encore**. Le
  Superset actuel lit le catalogue avec les identifiants du pipeline.
- **Versions de DuckDB** : l'image Superset 4.1.1 tourne sous Python 3.10, avec DuckDB 1.5.6.
  DuckDB 2.0.0 (version du pipeline) exige Python ≥ 3.11 : voir « Points de vigilance ».

### Stratégie de découverte (`platform: onyxia`)

Le chart ne recopie jamais une valeur de secret : il ne référence que des noms de Secrets
et de clés (`secretKeyRef`, `secretRef`).

1. **Une valeur explicite gagne toujours** : `pipeline.postgres.existingSecret`,
   `pipeline.mlflow.existingSecret`, `pipeline.mlflow.url`, `pipeline.argo.version`.
2. **Sinon, `lookup` dans le namespace** :
   - **version d'Argo** : étiquette de l'image d'un Deployment `workflow-controller`. Elle
     détermine le champ `schedules` (Argo ≥ 3.6) ou `schedule` des `CronWorkflow` ;
   - **PostgreSQL** : Secret `onyxia/discovery: postgres`, projeté sur PGHOST, PGUSER,
     PGPASSWORD, PGDATABASE et PGADMINUSER. PGPORT vient de `pipeline.postgres.port`, car le
     port du Secret de découverte n'est pas fiable ;
   - **MLflow** : Secret `onyxia/discovery: mlflow`, projeté sur MLFLOW_TRACKING_URI
     (`uri`), MLFLOW_TRACKING_USERNAME et MLFLOW_TRACKING_PASSWORD.
3. **Sinon, valeur par défaut** (Argo 3.6), ou **échec explicite** (aucun secret
   PostgreSQL).

Sous `helm template`, sans accès au cluster, `lookup` ne renvoie rien. Les préréglages
nomment donc les secrets `trade-*` existants, et la découverte reste un repli.

La version d'Argo se découvre à **chaque** `helm upgrade`. Si Onyxia met à jour Argo, une
simple mise à jour du chart suffit. Si une clé facultative disparaît d'un Secret de
découverte, la référence correspondante (marquée `optional`) reste vide sans faire échouer
le pod.

### Stratégie Superset

Le chart `trade-dashboard` déclare une dépendance facultative vers le **chart Superset
d'Onyxia** :
- plage de versions `>=0.1.12 <0.2.0` ;
- `Chart.lock` verrouillé sur 0.1.13 (Superset 4.1.1, celui du service actuel) ;
- condition `superset.enabled`, vraie dans `values-onyxia.yaml`.

Le Superset fait alors partie de la release. Sa configuration est portée par le chart, si
bien qu'**aucun correctif n'est perdu à un redéploiement**. Elle comprend :
- **le fragment Python** `files/superset_serving.py`. Il est monté depuis le ConfigMap
  `<release>-superset-serving` et exécuté à la fin de `superset_config.py`. Il reprend à
  l'identique la configuration actuelle :
  - écouteur `connect` restreint aux connexions `duckdb_engine` ;
  - extensions `ducklake` et `httpfs` ;
  - secret S3 en mémoire ;
  - `ATTACH 'ducklake:postgres:dbname=serving …' AS serving (READ_ONLY)` ;
  - `ENABLE_TEMPLATE_PROCESSING`.
- **les secrets** `global.dashboard.readerSecret`, `s3Secret` et `credentialsSecret`,
  exposés dans les pods ;
- **des pilotes épinglés** ;
- **la découverte désactivée** : sinon, la connexion PostgreSQL CNPG serait importée dans
  Superset avec ses identifiants ;
- **une base de métadonnées embarquée** nommée `<release>-superset-db`, distincte de la
  `superset-db` du service Onyxia.

Hors Onyxia, ou pour un Superset existant (`superset.enabled: false`), le chart ne fait que
l'import et le ConfigMap. La configuration est alors à reporter une fois dans ce Superset
(voir « Superset existant »).

## Prérequis

1. **Accès au cluster** : `kubectl` et `helm` (version 3.8 ou plus, pour l'OCI), un rôle
   `admin` sur le namespace. Sur Onyxia, un service VSCode suffit.
2. **Services** :
   - **Onyxia** : lancer Argo Workflows, MLflow (persistance activée) et PostgreSQL depuis
     le catalogue ;
   - **ailleurs** : un contrôleur Argo Workflows (ou `argoWorkflows.enabled`), et un serveur
     MLflow si le suivi est voulu (ou `mlflow.enabled`).
3. **Secrets**, à créer une fois, jamais versionnés. Aucune valeur ne doit être recopiée
   dans un fichier du dépôt.

```bash
# Clés de l'API UN Comtrade (https://comtradedeveloper.un.org, Profile → subscriptions)
kubectl create secret generic comtrade-api-credentials \
  --from-literal=COMTRADE_FREE_SUBSCRIPTION_KEY='…' \
  --from-literal=COMTRADE_PREMIUM_INSTITUTIONNAL_SUBSCRIPTION_KEY='…'

# Stockage S3 (Minio du SSP Cloud : compte de service, https://docs.sspcloud.fr/content/storage.html)
kubectl create secret generic trade-s3-credentials \
  --from-literal=S3_ACCESS_KEY='…' --from-literal=S3_SECRET_KEY='…'

# PostgreSQL des catalogues DuckLake (relevé dans le service PostgreSQL d'Onyxia)
kubectl create secret generic trade-postgres-credentials \
  --from-literal=PGHOST='…' --from-literal=PGPORT='5432' \
  --from-literal=PGUSER='…' --from-literal=PGPASSWORD='…' \
  --from-literal=PGDATABASE='postgres' --from-literal=PGADMINUSER='…'

# Serveur MLflow : URL INTERNE au cluster (http://<service MLflow>), identifiants si l'authentification est active
kubectl create secret generic trade-mlflow-credentials \
  --from-literal=MLFLOW_TRACKING_URI='http://…' \
  --from-literal=MLFLOW_TRACKING_USERNAME='…' --from-literal=MLFLOW_TRACKING_PASSWORD='…'

# Superset installé par le chart : clé de chiffrement, compte administrateur (utilisé par
# l'import du tableau de bord) et mot de passe de sa base de métadonnées, tirés au hasard
kubectl create secret generic trade-superset-credentials \
  --from-literal=SUPERSET_SECRET_KEY="$(openssl rand -base64 48)" \
  --from-literal=ADMIN_USERNAME='admin' \
  --from-literal=ADMIN_PASSWORD="$(openssl rand -base64 24)" \
  --from-literal=DB_PASS="$(openssl rand -hex 24)"

# Vérification (noms de clés seulement)
kubectl get secret trade-superset-credentials -o go-template='{{range $k, $v := .data}}{{$k}} {{end}}{{"\n"}}'
```

Ce dernier secret peut aussi être créé par le chart, avec `dashboard.credentials.create=true`.
Les valeurs sont alors tirées au hasard à la première installation, puis conservées.
D'autres secrets peuvent être créés depuis des valeurs passées à l'installation
(`pipeline.secrets.create`) : la méthode existe, mais les secrets existants restent
recommandés.

**Facultatif, recommandé : lecture du catalogue par un rôle en lecture seule.** Créer le
rôle sur l'instance PostgreSQL (le schéma des tables de métadonnées DuckLake est à
confirmer par `\dt`) :

```sql
CREATE ROLE superset_reader LOGIN PASSWORD '…';
GRANT CONNECT ON DATABASE serving TO superset_reader;
\c serving
GRANT USAGE ON SCHEMA public TO superset_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO superset_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO superset_reader;
```

Créer ensuite un secret `trade-superset-reader` avec les clés `PGHOST`, `PGPORT`, `PGUSER`
et `PGPASSWORD`. L'installer enfin avec `--set global.dashboard.readerSecret=trade-superset-reader`.

## Installation

Depuis le dépôt cloné, les dépendances sont reconstruites (`Chart.lock`) une fois, puis
après chaque changement de chart :

```bash
helm dependency build helm/trade-dashboard && helm dependency build helm/trade-analysis
```

**Onyxia, production (une ligne) :**

```bash
helm upgrade --install trade helm/trade-analysis -f helm/trade-analysis/values-onyxia.yaml \
  --set pipeline.image.tag=sha-1a2b3c4 --set dashboard.dashboardImport.defaultYear=2023 \
  --set dashboard.superset.ingress.hostname=user-qbollietdgddi-trade.user.lab.sspcloud.fr
```

**Onyxia, recette** (environnement `demo`, schéma `demo_dashboard`, planifications
suspendues) : ajouter `-f helm/trade-analysis/values-demo.yaml`.

**Hors Onyxia (une ligne) :**

```bash
helm upgrade --install trade helm/trade-analysis -f helm/trade-analysis/values-generic.yaml \
  --set pipeline.mlflow.url=http://mlflow.mlflow:5000 \
  --set dashboard.dashboardImport.supersetUrl=http://superset.superset:8088
```

**Depuis le registre OCI**, sans cloner le dépôt : les charts sont publiés par
`.github/workflows/helm.yml` à chaque étiquette `charts-v*`.

```bash
helm upgrade --install trade oci://ghcr.io/qbolliet/charts/trade-analysis --version 0.1.0 \
  -f values-onyxia.yaml --set dashboard.superset.ingress.hostname=…
```

**Vérification :**

```bash
helm test trade --logs        # pré-vol : clés des secrets, MLflow /health, PostgreSQL, S3, Superset /health
kubectl logs job/trade-dashboard-import
```

Le pré-vol n'affiche **aucune valeur**, seulement `OK` ou `KO` par contrôle, avec un code
HTTP ou un type d'erreur.

## Reprise des ressources existantes (première installation)

Une ressource créée par `kubectl apply` bloque `helm install` : « exists and cannot be
imported into the current release ». Au 2026-10-29, seul le **`WorkflowTemplate
trade-pipeline`** est concerné : environnement `demo`, appliqué depuis
`kubernetes/generated/`. Les `CronWorkflow trade-pipeline-daily` et `-weekly` n'existent
pas.

Deux options :

1. **Reprise par Helm (recommandée).** Le template ne disparaît à aucun moment, et une
   exécution en cours n'est pas touchée :

   ```bash
   kubectl annotate workflowtemplate trade-pipeline meta.helm.sh/release-name=trade meta.helm.sh/release-namespace=user-qbollietdgddi
   kubectl label workflowtemplate trade-pipeline app.kubernetes.io/managed-by=Helm
   ```

2. **Suppression préalable** : `kubectl delete workflowtemplate trade-pipeline`. Aucune
   exécution ne doit être en cours (`kubectl get wf`) : une exécution déjà lancée garde sa
   copie du template, mais une resoumission échouerait jusqu'à l'installation.

Le **workflow de transition** (`trade-pipeline-transition`, `CronWorkflow
trade-pipeline-transition-daily`) n'est **pas** géré par Helm. Il doit rester suspendu
jusqu'à son retrait (voir `kubernetes/transition/README.md`).

**Superset** : la release Onyxia `superset-899573` peut coexister avec celui du chart
(noms distincts). Une fois le nouveau tableau de bord vérifié, la supprimer depuis
l'interface Onyxia. Son volume de base de métadonnées est alors à supprimer à part. Les
comptes et tableaux de bord créés à la main dans l'ancien Superset ne sont pas repris : le
tableau de bord « Vulnérabilités » l'est, par l'import.

## Valeurs principales

Toutes les valeurs sont documentées dans `trade-pipeline/values.yaml` et
`trade-dashboard/values.yaml`, et validées par `values.schema.json`. Dans le parapluie,
elles se placent sous `pipeline.` et `dashboard.`.

| Valeur (parapluie) | Défaut | Rôle |
|---|---|---|
| `global.platform` | `generic` (`onyxia` dans le préréglage) | découverte des services Onyxia |
| `pipeline.kedroEnv` | `cloud` | environnement Kedro des pods et DAG utilisé (`files/dag-<env>.yaml`) |
| `pipeline.image.repository` / `.tag` | `ghcr.io/qbolliet/trade-analysis` / appVersion | image ; `tag` est aussi la valeur par défaut du paramètre de workflow `image-tag` |
| `pipeline.image.pullPolicy` | auto : `IfNotPresent` pour `sha-…` ou `X.Y.Z`, `Always` sinon | |
| `pipeline.schedules.suspend` | `false` | suspend toutes les planifications |
| `pipeline.schedules.entryPoints.<ep>.cron` | celle du fichier DAG (`0 1 * * *`, `0 2 * * 6`) | planification d'un point d'entrée |
| `pipeline.schedules.entryPoints.<ep>.activeDeadlineSeconds` | 82800 (daily), 518400 (weekly) | durée maximale d'une exécution |
| `pipeline.schedules.timezone` | `Europe/Paris` | fuseau des cron |
| `pipeline.workflow.parallelism` / `runMutex` | `8` / `trade-pipeline-run` | tâches simultanées ; exclusion entre cadences |
| `pipeline.serviceAccount.create` / `.name` | `false` / `workflow` | compte de service des pods |
| `pipeline.argo.version` | découverte, sinon `3.6` | `schedules` ou `schedule` |
| `pipeline.env`, `pipeline.secretEnv` | point de terminaison Minio, clés S3 et Comtrade | variables des pods |
| `pipeline.postgres.existingSecret`, `pipeline.mlflow.existingSecret`, `pipeline.mlflow.url` | `trade-postgres-credentials`, `trade-mlflow-credentials`, vide | secrets exposés en entier ; vide sur Onyxia = découverte |
| `pipeline.preflight.s3Path` | `s3://qbollietdgddi/trade` | préfixe listé par le pré-vol |
| `global.dashboard.readerSecret` / `s3Secret` / `credentialsSecret` | `trade-postgres-credentials` / `trade-s3-credentials` / `trade-superset-credentials` | secrets lus par Superset et par l'import |
| `dashboard.superset.enabled` | `false` (`true` sur Onyxia) | Superset installé par le chart |
| `dashboard.superset.ingress.hostname` | vide, **à fournir** sur Onyxia | adresse web de Superset |
| `dashboard.dashboardImport.schema` | vide = `dashboard` (production) ; `demo_dashboard` en recette | schéma lu par les datasets |
| `dashboard.dashboardImport.defaultYear` | vide = valeur de l'export (2021) | dernière année classée, défaut des filtres |
| `dashboard.dashboardImport.supersetUrl` | service du Superset du chart | URL d'un Superset existant |
| `dashboard.catalog.*` | `serving`, Minio du SSP Cloud | base, alias, point de terminaison S3 du catalogue |

## Opérations courantes

**Mettre à jour l'image.** Publier l'image par `image.yml` (étiquette immuable
`sha-<SHA court>`), puis :

```bash
helm upgrade trade helm/trade-analysis --reuse-values --set pipeline.image.tag=sha-1a2b3c4
```

Seul le `WorkflowTemplate` change : Superset n'est pas redémarré, et le Job d'import est
rejoué, ce qui est sans effet si l'export n'a pas changé. **Retour arrière** : `helm
rollback trade <révision>` (`helm history trade`).

**Changer le DAG** (nœuds, tags, `machine_type`, `config/base/argo.yml`,
`runtime.WEEKLY_DAY`, `tracking.SYSTEM_METRICS`) :
1. `uv run kedro trade render-argo` régénère `helm/trade-pipeline/files/dag-<env>.yaml` ;
2. committer ces fichiers ;
3. lancer `helm upgrade`.

La CI échoue si ces fichiers ne correspondent plus au code (`render-argo --check`). C'est
la **source unique** du DAG : argo-kedro le calcule et Helm n'y ajoute que le déploiement.

**Changer le tableau de bord.**
1. Modifier ou exporter `superset/vulnerabilites/` (voir `superset/README.md`).
2. Lancer `python tools/sync_chart_files.py` : le chart lit sa copie dans
   `trade-dashboard/files/`, et un test échoue tant qu'elle diffère.
3. Lancer `helm upgrade`. L'import « assets » écrase connexion, datasets, graphiques et
   tableau de bord.

**Passer de la recette à la production.** Pipeline et tableau de bord ne diffèrent que par
des valeurs :

```bash
helm upgrade trade helm/trade-analysis -f helm/trade-analysis/values-onyxia.yaml \
  --set pipeline.image.tag=sha-1a2b3c4 --set dashboard.dashboardImport.defaultYear=<dernière année classée> \
  --set dashboard.superset.ingress.hostname=…
```

Ce qui change, faute de `values-demo.yaml` :
- `kedroEnv` revient à `cloud` (sept millésimes BACI, catalogues de production) ;
- le schéma revient à `dashboard` ;
- les planifications sont actives.

**Exécution ponctuelle** (rattrapage, forçage) :

```bash
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
```

Paramètres de workflow :

| Paramètre | Rôle | Exemple |
|---|---|---|
| `image-tag` | étiquette d'image | |
| `force-steps` | étapes forcées | `partners;synthesis`, `all` |
| `force-metrics` | métriques forcées | `HHI;CDI2` |
| `force-methods` | méthodes de synthèse forcées | `critic_sum` |
| `extra-params` | paires `clé=valeur` séparées par `,` | `runtime.FORCE_SCOPE.VINTAGES=HS2017` |

Plusieurs valeurs d'un même paramètre se séparent par **`;`**, jamais par `,` : Kedro
découpe `--params` sur les virgules. Les `CronWorkflow` n'exposent aucun forçage.

Avec la CLI `argo` : `argo submit --from workflowtemplate/trade-pipeline --entrypoint weekly
-p force-steps=synthesis --watch`.

**Suspendre ou reprendre les planifications** :

```bash
helm upgrade trade helm/trade-analysis --reuse-values --set pipeline.schedules.suspend=true
```

Éviter `kubectl patch` : Helm rétablirait la valeur à la mise à jour suivante.

## Superset existant (`dashboard.superset.enabled=false`)

Reporter une fois dans le Superset, par ses propres valeurs Helm ou sa configuration :
1. **montage** du ConfigMap `<release>-superset-serving` sur `/app/trade` (pods web, worker
   et init) ;
2. **à la fin de `superset_config.py`** :
   `exec(open("/app/trade/superset_serving.py", encoding="utf-8").read(), globals())` ;
3. **variables d'environnement** : `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` (lecture du
   catalogue), `S3_ACCESS_KEY` et `S3_SECRET_KEY` ;
4. **pilotes** `duckdb` et `duckdb-engine`, dans une version de DuckDB qui lit le
   catalogue.

Le Job d'import a besoin de `dashboard.dashboardImport.supersetUrl` et d'un secret
`credentialsSecret` portant `ADMIN_USERNAME` et `ADMIN_PASSWORD`.

## Désinstallation

```bash
helm uninstall trade
```

Sont supprimés le `WorkflowTemplate`, les `CronWorkflow`, Superset (pods, Redis, base de
métadonnées), les ConfigMaps et l'Ingress.

Ne sont **pas** supprimés :
- les **données** : catalogues DuckLake (bases PostgreSQL `serving`, `trade_*`, `demo_*`)
  et fichiers S3 ;
- les **expériences MLflow** ;
- les **workflows déjà exécutés** (ils expirent selon leur TTL) ;
- les **secrets existants**, et les secrets créés par le chart (`helm.sh/resource-policy:
  keep`) ;
- le **volume de la base de métadonnées de Superset** (PVC `data-trade-superset-db-0`),
  à supprimer à la main si Superset ne doit pas être réinstallé ;
- les **services Onyxia** (Argo Workflows, MLflow, PostgreSQL).

## Points de vigilance

- **DuckDB dans Superset.**
  - Le pipeline écrit le catalogue avec DuckDB 2.0.0 ; Superset 4.1.1 (Python 3.10) ne
    peut installer que DuckDB ≤ 1.5. Le `bootstrapScript` épingle `duckdb==1.5.6`, version
    du service actuel.
  - À vérifier en recette : la lecture par Superset d'un catalogue écrit en 2.0.
  - En cas d'échec : passer à une image Superset en Python ≥ 3.11
    (`dashboard.superset.superset.image.tag`), et épingler `duckdb==2.0.0` dans
    `dashboard.superset.superset.bootstrapScript`.
- **Nom d'hôte de l'Ingress** : le chart Superset d'Onyxia ne le calcule pas, il est à
  fournir.
- **Versions des charts.** La `version` de chaque chart (semver) est indépendante de celle
  de l'image ; `appVersion` est l'étiquette d'image par défaut. Publication : créer
  l'étiquette git `charts-vX.Y.Z` après avoir relevé les `version` des `Chart.yaml`.
