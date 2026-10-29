{{/*
Fonctions communes du chart trade-pipeline : noms, labels, plateforme, lecture du DAG,
découverte des services Onyxia et environnement des pods.
*/}}

{{/* Nom de base des ressources */}}
{{- define "trade-pipeline.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels standard de toutes les ressources */}}
{{- define "trade-pipeline.labels" -}}
app.kubernetes.io/name: {{ include "trade-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ include "trade-pipeline.imageTag" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- range $key, $value := .Values.workflow.labels }}
{{ $key }}: {{ $value | quote }}
{{- end }}
{{- end -}}

{{/* Plateforme effective : valeur globale du parapluie, sinon valeur du chart */}}
{{- define "trade-pipeline.platform" -}}
{{- $global := (.Values.global | default dict).platform | default "" -}}
{{- $platform := $global | default .Values.platform | default "generic" -}}
{{- if not (has $platform (list "onyxia" "generic")) -}}
{{- fail (printf "platform: valeur '%s' inconnue (onyxia ou generic)" $platform) -}}
{{- end -}}
{{- $platform -}}
{{- end -}}

{{/* Étiquette d'image : valeur explicite, sinon appVersion du chart */}}
{{- define "trade-pipeline.imageTag" -}}
{{- .Values.image.tag | default .Chart.AppVersion -}}
{{- end -}}

{{/*
Politique de téléchargement : valeur explicite, sinon IfNotPresent pour une étiquette
immuable (sha-<SHA> ou version X.Y.Z), Always pour une étiquette mobile (branche, latest),
dont le contenu change sans que le nom change.
*/}}
{{- define "trade-pipeline.pullPolicy" -}}
{{- if .Values.image.pullPolicy -}}
{{- .Values.image.pullPolicy -}}
{{- else if regexMatch "^(sha-[0-9a-f]{7,40}|v?[0-9]+\\.[0-9]+\\.[0-9]+.*)$" (include "trade-pipeline.imageTag" .) -}}
IfNotPresent
{{- else -}}
Always
{{- end -}}
{{- end -}}

{{/*
Données du DAG de l'environnement Kedro (files/dag-<kedroEnv>.yaml), rendues en YAML ;
l'appelant les relit par fromYaml. Échec explicite pour un environnement non rendu ou un
format de fichier inattendu.
*/}}
{{- define "trade-pipeline.dag" -}}
{{- $path := printf "files/dag-%s.yaml" .Values.kedroEnv -}}
{{- $text := .Files.Get $path -}}
{{- if not $text -}}
{{- fail (printf "kedroEnv: aucun fichier %s dans le chart (environnements rendus par `kedro trade render-argo`, liste chart_environments de argo.yml)" $path) -}}
{{- end -}}
{{- $dag := fromYaml $text -}}
{{- if hasKey $dag "Error" -}}
{{- fail (printf "%s : YAML illisible (%s)" $path $dag.Error) -}}
{{- end -}}
{{- if ne (int $dag.formatVersion) 1 -}}
{{- fail (printf "%s : format %v non pris en charge par ce chart (attendu : 1)" $path $dag.formatVersion) -}}
{{- end -}}
{{- toYaml $dag -}}
{{- end -}}

{{/*
Version du contrôleur Argo : valeur explicite ; sinon, sur toute plateforme, étiquette de
l'image d'un Deployment du namespace du contrôleur dont une image contient
`workflow-controller` (lookup, vide sous `helm template`) ; sinon 3.6.
*/}}
{{- define "trade-pipeline.argoVersion" -}}
{{- $version := .Values.argo.version | default "" | toString -}}
{{- if not $version -}}
{{- $namespace := .Values.argo.controllerNamespace | default .Release.Namespace -}}
{{- $deployments := lookup "apps/v1" "Deployment" $namespace "" | default dict -}}
{{- range $deployment := ($deployments.items | default list) -}}
{{- range $container := $deployment.spec.template.spec.containers -}}
{{- if and (not $version) (contains "workflow-controller:" $container.image) -}}
{{- $version = last (splitList ":" $container.image) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $version | default "3.6" | trimPrefix "v" -}}
{{- end -}}

{{/* Champ de planification d'un CronWorkflow : liste à partir d'Argo 3.6 */}}
{{- define "trade-pipeline.scheduleField" -}}
{{- $version := include "trade-pipeline.argoVersion" . -}}
{{- $parts := splitList "." $version -}}
{{- $normalised := printf "%s.%s.0" (index $parts 0) (index $parts 1 | default "0") -}}
{{- if semverCompare ">=3.6.0-0" $normalised -}}schedules{{- else -}}schedule{{- end -}}
{{- end -}}

{{/*
Nom du premier secret du namespace annoté `onyxia/discovery: <type>` (convention de
découverte des services Onyxia), vide hors Onyxia ou sous `helm template`.
Paramètre : dict "root" (contexte) "type" (type de service : postgres, mlflow).
*/}}
{{- define "trade-pipeline.discoveredSecret" -}}
{{- $root := .root -}}
{{- $found := "" -}}
{{- if eq (include "trade-pipeline.platform" $root) "onyxia" -}}
{{- $secrets := lookup "v1" "Secret" $root.Release.Namespace "" | default dict -}}
{{- range $secret := ($secrets.items | default list) -}}
{{- $annotations := $secret.metadata.annotations | default dict -}}
{{- if and (not $found) (eq (index $annotations "onyxia/discovery" | default "" | toString) $.type) -}}
{{- $found = $secret.metadata.name -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $found -}}
{{- end -}}

{{/*
Variables d'environnement issues de secrets (liste `env` d'un conteneur) : clés S3 et
Comtrade, PostgreSQL et MLflow découverts quand aucun secret existant n'est nommé, URL
MLflow explicite. Les références d'un pod de test sont facultatives (`optional`), pour
qu'une clé absente soit signalée par le contrôle au lieu de bloquer le démarrage.
Paramètre : dict "root" (contexte) "optional" (booléen).
*/}}
{{- define "trade-pipeline.secretEnv" -}}
{{- $root := .root -}}
{{- $optional := .optional -}}
{{- range $name, $ref := $root.Values.secretEnv }}
- {name: {{ $name }}, valueFrom: {secretKeyRef: {name: {{ $ref.secret }}, key: {{ $ref.key }}{{ if $optional }}, optional: true{{ end }}}}}
{{- end }}
{{- if not $root.Values.postgres.existingSecret }}
{{- $secret := include "trade-pipeline.discoveredSecret" (dict "root" $root "type" "postgres") }}
{{- if not $secret }}
{{- fail "postgres.existingSecret : aucun secret nommé et aucun secret PostgreSQL découvert (onyxia/discovery: postgres)" }}
{{- end }}
# PostgreSQL découvert ({{ $secret }}) : clés du service projetées sur les variables PG*
{{- range $variable, $key := dict "PGHOST" "postgres-service" "PGUSER" "postgres-username" "PGPASSWORD" "postgres-password" "PGDATABASE" "postgres-database" "PGADMINUSER" "postgres-username" }}
- {name: {{ $variable }}, valueFrom: {secretKeyRef: {name: {{ $secret }}, key: {{ $key }}{{ if $optional }}, optional: true{{ end }}}}}
{{- end }}
- {name: PGPORT, value: {{ $root.Values.postgres.port | quote }}}
{{- end }}
{{- if not $root.Values.mlflow.existingSecret }}
{{- $secret := include "trade-pipeline.discoveredSecret" (dict "root" $root "type" "mlflow") }}
{{- if $secret }}
# MLflow découvert ({{ $secret }})
{{- range $variable, $key := dict "MLFLOW_TRACKING_URI" "uri" "MLFLOW_TRACKING_USERNAME" "MLFLOW_TRACKING_USERNAME" "MLFLOW_TRACKING_PASSWORD" "MLFLOW_TRACKING_PASSWORD" }}
{{- if or (ne $variable "MLFLOW_TRACKING_URI") (not $root.Values.mlflow.url) }}
- {name: {{ $variable }}, valueFrom: {secretKeyRef: {name: {{ $secret }}, key: {{ $key }}, optional: true}}}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- if $root.Values.mlflow.url }}
# URL MLflow explicite, prioritaire sur celle du secret
- {name: MLFLOW_TRACKING_URI, value: {{ $root.Values.mlflow.url | quote }}}
{{- end }}
{{- end -}}

{{/*
Secrets exposés en entier (liste `envFrom` d'un conteneur), vide si aucun.
Paramètre : dict "root" (contexte) "optional" (booléen).
*/}}
{{- define "trade-pipeline.secretEnvFrom" -}}
{{- $root := .root -}}
{{- $optional := .optional -}}
{{- $names := list -}}
{{- with $root.Values.postgres.existingSecret }}{{ $names = append $names . }}{{ end -}}
{{- with $root.Values.mlflow.existingSecret }}{{ $names = append $names . }}{{ end -}}
{{- $names = concat $names ($root.Values.extraSecretEnvFrom | default list) -}}
{{- range $name := $names }}
- secretRef: {name: {{ $name }}{{ if $optional }}, optional: true{{ end }}}
{{- end }}
{{- end -}}

{{/* Variables non secrètes des pods (valeurs `env`), triées par nom */}}
{{- define "trade-pipeline.plainEnv" -}}
{{- range $name, $value := .Values.env }}
- {name: {{ $name }}, value: {{ $value | toString | quote }}}
{{- end }}
{{- end -}}
