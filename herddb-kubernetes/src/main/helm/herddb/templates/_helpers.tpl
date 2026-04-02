{{/*
Expand the name of the chart.
*/}}
{{- define "herddb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "herddb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart label value.
*/}}
{{- define "herddb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "herddb.labels" -}}
helm.sh/chart: {{ include "herddb.chart" . }}
{{ include "herddb.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "herddb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "herddb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name
*/}}
{{- define "herddb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "herddb.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Compute the remote.file.servers list from fileServer replica count.
Produces: <fullname>-file-server-0.<fullname>-file-server.<namespace>.svc.cluster.local:<port>,...
*/}}
{{- define "herddb.remoteFileServers" -}}
{{- $servers := list -}}
{{- $root := . -}}
{{- range $i, $_ := until (int .Values.fileServer.replicaCount) -}}
{{- $host := printf "%s-file-server-%d.%s-file-server.%s.svc.cluster.local:%d"
      (include "herddb.fullname" $root)
      $i
      (include "herddb.fullname" $root)
      $root.Release.Namespace
      (int $root.Values.fileServer.port) -}}
{{- $servers = append $servers $host -}}
{{- end -}}
{{- join "," $servers -}}
{{- end }}

{{/*
JDBC URL for the first server pod (used by the tools pod).
*/}}
{{- define "herddb.jdbcUrl" -}}
{{- printf "jdbc:herddb:server://%s-server-0.%s-server.%s.svc.cluster.local:%d/herd"
    (include "herddb.fullname" .)
    (include "herddb.fullname" .)
    .Release.Namespace
    (int .Values.server.port) -}}
{{- end }}
