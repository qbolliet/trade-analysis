{{/*
Fonctions communes du chart trade-dashboard : noms, labels, plateforme, URL de Superset.
*/}}

{{/* Nom de base des ressources */}}
{{- define "trade-dashboard.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels standard de toutes les ressources */}}
{{- define "trade-dashboard.labels" -}}
app.kubernetes.io/name: {{ include "trade-dashboard.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{/* Plateforme effective : valeur globale du parapluie, sinon valeur du chart */}}
{{- define "trade-dashboard.platform" -}}
{{- $platform := (.Values.global | default dict).platform | default .Values.platform | default "generic" -}}
{{- if not (has $platform (list "onyxia" "generic")) -}}
{{- fail (printf "platform: valeur '%s' inconnue (onyxia ou generic)" $platform) -}}
{{- end -}}
{{- $platform -}}
{{- end -}}

{{/*
URL de Superset : valeur explicite, sinon service du Superset de ce chart (nom complet
du chart amont : <release>-superset), sinon échec explicite.
*/}}
{{- define "trade-dashboard.supersetUrl" -}}
{{- if .Values.dashboardImport.supersetUrl -}}
{{- .Values.dashboardImport.supersetUrl | trimSuffix "/" -}}
{{- else if .Values.superset.enabled -}}
{{- printf "http://%s-superset:8088" .Release.Name -}}
{{- else -}}
{{- fail "dashboardImport.supersetUrl : URL requise quand superset.enabled est faux (Superset existant)" -}}
{{- end -}}
{{- end -}}

{{/* Nom du ConfigMap du fragment de configuration, monté par les pods Superset */}}
{{- define "trade-dashboard.servingConfigMap" -}}
{{- printf "%s-superset-serving" .Release.Name -}}
{{- end -}}
