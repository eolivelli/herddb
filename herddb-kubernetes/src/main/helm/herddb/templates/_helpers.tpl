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
ZooKeeper connection address (first ZK pod via headless service).
*/}}
{{- define "herddb.zkAddress" -}}
{{- printf "%s-zookeeper-0.%s-zookeeper.%s.svc.cluster.local:%d"
    (include "herddb.fullname" .)
    (include "herddb.fullname" .)
    .Release.Namespace
    (int .Values.zookeeper.clientPort) -}}
{{- end }}

{{/*
JDBC URL for the tools pod.
Uses a direct server connection to the first server pod (FQDN).
The client auto-discovers cluster topology via the sysnodes / systablespaces
system tables (ServerBasedClientSideMetadataProvider), so no ZooKeeper
address is needed in the JDBC URL even when server.mode=cluster.
*/}}
{{- define "herddb.jdbcUrl" -}}
{{- printf "jdbc:herddb:server:%s-server-0.%s-server.%s.svc.cluster.local:%d"
    (include "herddb.fullname" .)
    (include "herddb.fullname" .)
    .Release.Namespace
    (int .Values.server.port) -}}
{{- end }}

{{/*
gRPC address of the first file-server pod, used by fileserver-admin CLI.
The admin service shares the same gRPC port as the data-plane service.
*/}}
{{- define "herddb.fileServerAdminAddress" -}}
{{- printf "%s-file-server-0.%s-file-server.%s.svc.cluster.local:%d"
    (include "herddb.fullname" .)
    (include "herddb.fullname" .)
    .Release.Namespace
    (int .Values.fileServer.port) -}}
{{- end }}
