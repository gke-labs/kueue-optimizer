#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")" && pwd)"
DEPLOY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd "${DEPLOY_DIR}/.." && pwd)"

KWOK_REPO="${KWOK_REPO:-${ROOT_DIR}/kwok}"
KUEUE_REPO="${KUEUE_REPO:-${ROOT_DIR}/kueue}"
JOBSET_REPO="${JOBSET_REPO:-${ROOT_DIR}/jobset}"

BASE_DIR="${DEPLOY_DIR}/base"
CONFIG_DIR="${BASE_DIR}/config"
MANIFESTS_DIR="${BASE_DIR}/manifests"

NAMESPACE="${NAMESPACE:-${HOST_NAMESPACE:-kueue-optimizer-chamber-01}}"
APISERVER_SVC="${APISERVER_SVC:-kwok-kube-apiserver}"
CONTROLLER_SVC="${CONTROLLER_SVC:-kwok-controller}"
KUEUE_SVC="${KUEUE_SVC:-kwok-kueue}"
JOBSET_SVC="${JOBSET_SVC:-kwok-jobset}"
NODE_COUNT="${NODE_COUNT:-10}"
NODE_CPU="${NODE_CPU:-4}"
NODE_MEMORY_GB="${NODE_MEMORY_GB:-16}"
PROMETHEUS_SCRAPE_INTERVAL="${PROMETHEUS_SCRAPE_INTERVAL:-10s}"
if [[ "${PROMETHEUS_SCRAPE_INTERVAL}" =~ ^[0-9]+$ ]]; then
  PROMETHEUS_SCRAPE_INTERVAL="${PROMETHEUS_SCRAPE_INTERVAL}s"
fi

mkdir -p "${CONFIG_DIR}" "${MANIFESTS_DIR}"

echo "==> [1/5] Configuring target namespace '${NAMESPACE}' across Kustomize manifests..."
cat > "${BASE_DIR}/namespace.yaml" <<EOF
# Copyright 2026 Google LLC
# SPDX-License-Identifier: Apache-2.0
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    app.kubernetes.io/part-of: kueue-optimizer
    kueue-optimizer.gke.io/chamber: "true"
EOF

sed -i "s/^namespace: .*/namespace: ${NAMESPACE}/" \
  "${BASE_DIR}/kustomization.yaml" \
  "${DEPLOY_DIR}/overlays/with-sample-queues/kustomization.yaml" \
  "${DEPLOY_DIR}/kustomization.yaml"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# Private/public keys and certificates are never stored in the repository; they are
# either fetched from Secret/kwok-pki in the cluster or generated ad-hoc at deploy time.
CA_B64="__KWOK_CA_BUNDLE__"

cat > "${CONFIG_DIR}/prometheus.yaml" <<EOF
# Copyright 2026 Google LLC
# SPDX-License-Identifier: Apache-2.0
global:
  scrape_interval: ${PROMETHEUS_SCRAPE_INTERVAL}
  evaluation_interval: ${PROMETHEUS_SCRAPE_INTERVAL}

scrape_configs:
- job_name: kueue-metrics
  scrape_interval: ${PROMETHEUS_SCRAPE_INTERVAL}
  metrics_path: /metrics
  scheme: https
  authorization:
    type: Bearer
    credentials: kwok-metrics-reader-token
  tls_config:
    ca_file: /etc/kubernetes/pki/ca.crt
    server_name: ${KUEUE_SVC}
  static_configs:
  - targets:
    - ${KUEUE_SVC}:8443
    labels:
      component: kueue-controller-manager
EOF

echo "==> [2/5] Building KWOK CRDs, custom pod/node stages, config, and simulated nodes from ${KWOK_REPO}..."
kubectl --kubeconfig=/dev/null kustomize "${KWOK_REPO}/kustomize/crd" > "${MANIFESTS_DIR}/kwok-crds.yaml"
{
  cat <<'NODE_INIT_EOF'
apiVersion: kwok.x-k8s.io/v1alpha1
kind: Stage
metadata:
  name: node-initialize
spec:
  next:
    statusTemplate: |
      {{ $now := Now }}
      {{ $lastTransitionTime := or .metadata.creationTimestamp $now }}
      conditions:
      {{ range NodeConditions }}
      - lastHeartbeatTime: {{ $now | Quote }}
        lastTransitionTime: {{ $lastTransitionTime | Quote }}
        message: {{ .message | Quote }}
        reason: {{ .reason | Quote }}
        status: {{ .status | Quote }}
        type: {{ .type  | Quote}}
      {{ end }}

      {{ $hasInternalIP := false }}
      {{ $hasHostname := false }}
      {{ range .status.addresses }}
        {{ if eq .type "InternalIP" }}
          {{ $hasInternalIP = true }}
        {{ end }}
        {{ if eq .type "Hostname" }}
          {{ $hasHostname = true }}
        {{ end }}
      {{ end }}
      {{ if or (not $hasInternalIP) (not $hasHostname) }}
      addresses:
      {{ end }}
      {{ if not $hasInternalIP }}
      {{ with NodeIP }}
      - address: {{ . | Quote }}
        type: InternalIP
      {{ end }}
      {{ end }}
      {{ if not $hasHostname }}
      {{ with NodeName }}
      - address: {{ . | Quote }}
        type: Hostname
      {{ end }}
      {{ end }}

      {{ with NodePort }}
      daemonEndpoints:
        kubeletEndpoint:
          Port: {{ . }}
      {{ end }}

      {{ $nodeCpu := "" }}
      {{ $nodeMem := "" }}
      {{ $nodePods := "" }}
      {{ $accelRes := "" }}
      {{ $accelCnt := "" }}
      {{ with .metadata.annotations }}
        {{ $nodeCpu = index . "kueue-optimizer.gke.io/node-cpu" }}
        {{ $nodeMem = index . "kueue-optimizer.gke.io/node-memory" }}
        {{ $nodePods = index . "kueue-optimizer.gke.io/node-pods" }}
        {{ $accelRes = index . "kueue-optimizer.gke.io/node-accel-resource" }}
        {{ $accelCnt = index . "kueue-optimizer.gke.io/node-accel-count" }}
      {{ end }}
      allocatable:
      {{ if $nodeCpu }}
        cpu: {{ $nodeCpu | Quote }}
        memory: {{ or $nodeMem "1Ti" | Quote }}
        pods: {{ or $nodePods "110" | Quote }}
        {{ if and $accelRes $accelCnt }}
        {{ $accelRes }}: {{ $accelCnt | Quote }}
        {{ end }}
      {{ else }}
      {{ with .status.allocatable }}
      {{ YAML . 1 }}
      {{ else }}
        cpu: 1k
        memory: 1Ti
        pods: 1M
      {{ end }}
      {{ end }}
      capacity:
      {{ if $nodeCpu }}
        cpu: {{ $nodeCpu | Quote }}
        memory: {{ or $nodeMem "1Ti" | Quote }}
        pods: {{ or $nodePods "110" | Quote }}
        {{ if and $accelRes $accelCnt }}
        {{ $accelRes }}: {{ $accelCnt | Quote }}
        {{ end }}
      {{ else }}
      {{ with .status.capacity }}
      {{ YAML . 1 }}
      {{ else }}
        cpu: 1k
        memory: 1Ti
        pods: 1M
      {{ end }}
      {{ end }}

      {{ $nodeInfo := .status.nodeInfo }}
      {{ $kwokVersion := printf "kwok-%s" Version }}
      nodeInfo:
        architecture: {{ or $nodeInfo.architecture "amd64" }}
        bootID: {{ or $nodeInfo.bootID `""` }}
        containerRuntimeVersion: {{ or $nodeInfo.containerRuntimeVersion $kwokVersion }}
        kernelVersion: {{ or $nodeInfo.kernelVersion $kwokVersion }}
        kubeProxyVersion: {{ or $nodeInfo.kubeProxyVersion $kwokVersion }}
        kubeletVersion: {{ or $nodeInfo.kubeletVersion $kwokVersion }}
        machineID: {{ or $nodeInfo.machineID `""` }}
        operatingSystem: {{ or $nodeInfo.operatingSystem "linux" }}
        osImage: {{ or $nodeInfo.osImage `""` }}
        systemUUID: {{ or $nodeInfo.systemUUID `""` }}
      phase: Running
  resourceRef:
    apiGroup: v1
    kind: Node
  selector:
    matchExpressions:
    - key: .status.conditions.[] | select( .type == "Ready" ) | .status
      operator: NotIn
      values:
      - "True"
NODE_INIT_EOF
  echo "---"
  kubectl --kubeconfig=/dev/null kustomize "${KWOK_REPO}/kustomize/stage/node/heartbeat-with-lease"
  echo "---"
  cat <<'STAGE_EOF'
apiVersion: kwok.x-k8s.io/v1alpha1
kind: Stage
metadata:
  name: pod-ready
spec:
  resourceRef:
    apiGroup: v1
    kind: Pod
  selector:
    matchExpressions:
    - key: .metadata.deletionTimestamp
      operator: DoesNotExist
    - key: .spec.nodeName
      operator: Exists
    - key: .status.phase
      operator: In
      values:
      - ""
      - Pending
    - key: .status.conditions.[] | select( .type == "Ready" ) | .status
      operator: NotIn
      values:
      - "True"
  next:
    statusTemplate: |
      {{ $now := Now }}

      conditions:
      - lastTransitionTime: {{ $now | Quote }}
        status: "True"
        type: Initialized
      - lastTransitionTime: {{ $now | Quote }}
        status: "True"
        type: Ready
      - lastTransitionTime: {{ $now | Quote }}
        status: "True"
        type: ContainersReady
      - lastTransitionTime: {{ $now | Quote }}
        status: "True"
        type: PodScheduled
      {{ range .spec.readinessGates }}
      - lastTransitionTime: {{ $now | Quote }}
        status: "True"
        type: {{ .conditionType | Quote }}
      {{ end }}

      containerStatuses:
      {{ range .spec.containers }}
      - image: {{ .image | Quote }}
        name: {{ .name | Quote }}
        ready: true
        restartCount: 0
        started: true
        state:
          running:
            startedAt: {{ $now | Quote }}
      {{ end }}

      initContainerStatuses:
      {{ range .spec.initContainers }}
      - image: {{ .image | Quote }}
        name: {{ .name | Quote }}
        ready: true
        restartCount: 0
        {{ if eq .restartPolicy "Always" }}
        started: true
        state:
          running:
            startedAt: {{ $now | Quote }}
        {{ else }}
        state:
          terminated:
            exitCode: 0
            finishedAt: {{ $now | Quote }}
            reason: Completed
            startedAt: {{ $now | Quote }}
        {{ end }}
      {{ end }}
      {{ $hostIPs := NodeIPsWith .spec.nodeName }}
      {{ with $hostIPs }}
      hostIP: {{ index . 0 | Quote }}
      hostIPs:
      {{ range . }}
      - ip: {{ . | Quote }}
      {{ end }}
      {{ end }}
      {{ $podIPs := PodIPsWith .spec.nodeName ( or .spec.hostNetwork false ) ( or .metadata.uid "" ) ( or .metadata.name "" ) ( or .metadata.namespace "" ) }}
      {{ with $podIPs }}
      podIP: {{ index . 0 | Quote }}
      podIPs:
      {{ range . }}
      - ip: {{ . | Quote }}
      {{ end }}
      {{ end }}
      phase: Running
      startTime: {{ $now | Quote }}
---
apiVersion: kwok.x-k8s.io/v1alpha1
kind: Stage
metadata:
  name: pod-complete
spec:
  resourceRef:
    apiGroup: v1
    kind: Pod
  selector:
    matchExpressions:
    - key: .metadata.deletionTimestamp
      operator: DoesNotExist
    - key: .status.phase
      operator: In
      values:
      - Running
  delay:
    durationMilliseconds: 10000
    durationFrom:
      jq:
        expression: '(.metadata.annotations["kueue-optimizer.gke.io/pod-execution-time-seconds"] // "10") | tostring | if . == "" then "10s" elif test("[a-zA-Z]$") then . else . + "s" end'
  next:
    statusTemplate: |
      {{ $now := Now }}
      {{ $root := . }}
      conditions:
      - lastTransitionTime: {{ $now | Quote }}
        status: "True"
        type: Initialized
      - lastTransitionTime: {{ $now | Quote }}
        reason: PodCompleted
        status: "False"
        type: Ready
      - lastTransitionTime: {{ $now | Quote }}
        reason: PodCompleted
        status: "False"
        type: ContainersReady
      - lastTransitionTime: {{ $now | Quote }}
        status: "True"
        type: PodScheduled
      containerStatuses:
      {{ range $index, $item := .spec.containers }}
      {{ $origin := index $root.status.containerStatuses $index }}
      - image: {{ $item.image | Quote }}
        name: {{ $item.name | Quote }}
        ready: false
        restartCount: 0
        started: false
        state:
          terminated:
            exitCode: 0
            finishedAt: {{ $now | Quote }}
            reason: Completed
            startedAt: {{ or $origin.state.running.startedAt $now | Quote }}
      {{ end }}
      {{ if .spec.initContainers }}
      initContainerStatuses:
      {{ range .spec.initContainers }}
      - image: {{ .image | Quote }}
        name: {{ .name | Quote }}
        ready: true
        restartCount: 0
        state:
          terminated:
            exitCode: 0
            finishedAt: {{ $now | Quote }}
            reason: Completed
            startedAt: {{ $now | Quote }}
      {{ end }}
      {{ end }}
      phase: Succeeded
---
apiVersion: kwok.x-k8s.io/v1alpha1
kind: Stage
metadata:
  name: pod-delete
spec:
  resourceRef:
    apiGroup: v1
    kind: Pod
  selector:
    matchExpressions:
    - key: .metadata.deletionTimestamp
      operator: Exists
  next:
    delete: true
STAGE_EOF
} > "${MANIFESTS_DIR}/kwok-stages.yaml"
cp "${KWOK_REPO}/kustomize/kwok/kwok.yaml" "${CONFIG_DIR}/kwok.yaml"

# Generate initial simulated KWOK Nodes
{
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    if [ "${i}" -gt 0 ]; then
      echo "---"
    fi
    cat <<EOF
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
    node.alpha.kubernetes.io/ttl: "0"
  labels:
    beta.kubernetes.io/arch: amd64
    beta.kubernetes.io/os: linux
    kubernetes.io/arch: amd64
    kubernetes.io/hostname: kwok-node-${i}
    kubernetes.io/os: linux
    kubernetes.io/role: agent
    node-role.kubernetes.io/agent: ""
    type: kwok
  name: kwok-node-${i}
spec:
  podCIDR: 10.244.${i}.0/24
  podCIDRs:
  - 10.244.${i}.0/24
status:
  allocatable:
    cpu: "${NODE_CPU}"
    memory: ${NODE_MEMORY_GB}Gi
    pods: "110"
  capacity:
    cpu: "${NODE_CPU}"
    memory: ${NODE_MEMORY_GB}Gi
    pods: "110"
  nodeInfo:
    architecture: amd64
    kubeProxyVersion: fake
    kubeletVersion: fake
    operatingSystem: linux
EOF
  done
} > "${MANIFESTS_DIR}/kwok-nodes.yaml"

echo "==> [3/5] Building and transforming Kueue manifests from ${KUEUE_REPO}..."
RAW_KUEUE_MANIFEST="${TMP_DIR}/kueue-raw.yaml"
kubectl --kubeconfig=/dev/null kustomize "${KUEUE_REPO}/config/default" > "${RAW_KUEUE_MANIFEST}"

python3 - "${RAW_KUEUE_MANIFEST}" "${CA_B64}" "${KUEUE_SVC}" "${CONFIG_DIR}/controller_manager_config.yaml" "${MANIFESTS_DIR}/kueue-crds.yaml" "${MANIFESTS_DIR}/kueue-workloads-crd.json" "${MANIFESTS_DIR}/kueue-bootstrap.yaml" <<'PYEOF'
import json
import sys
import yaml

raw_manifest_path = sys.argv[1]
ca_bundle = sys.argv[2]
external_name = sys.argv[3]
out_config_path = sys.argv[4]
out_crds_path = sys.argv[5]
out_workloads_crd_path = sys.argv[6]
out_bootstrap_path = sys.argv[7]

def strip_descriptions(obj):
    if isinstance(obj, dict):
        obj.pop("description", None)
        for v in obj.values():
            strip_descriptions(v)
    elif isinstance(obj, list):
        for item in obj:
            strip_descriptions(item)

with open(raw_manifest_path, "r", encoding="utf-8") as f:
    docs = list(yaml.safe_load_all(f))

other_crds = []
workloads_crd = None
bootstrap_docs = []
manager_config_str = None

for doc in docs:
    if not doc:
        continue
    kind = doc.get("kind")
    api_version = doc.get("apiVersion")
    meta = doc.get("metadata", {})
    name = meta.get("name", "")

    if kind == "ConfigMap" and name == "kueue-manager-config":
        raw_cfg = doc.get("data", {}).get("controller_manager_config.yaml", "")
        cfg_obj = yaml.safe_load(raw_cfg)
        cfg_obj["namespace"] = "kueue-system"
        cfg_obj.setdefault("metrics", {})["enableClusterQueueResources"] = True
        cfg_obj.setdefault("leaderElection", {})["leaderElect"] = False
        cfg_obj.setdefault("internalCertManagement", {})["enable"] = False
        cfg_obj.setdefault("fairSharing", {})["preemptionStrategies"] = [
            "LessThanOrEqualToFinalShare",
            "LessThanInitialShare",
        ]
        cfg_obj.setdefault("controller", {}).setdefault("groupKindConcurrency", {})["JobSet.jobset.x-k8s.io"] = 5
        manager_config_str = yaml.safe_dump(cfg_obj, sort_keys=False)
        continue

    if kind == "CustomResourceDefinition" and api_version == "apiextensions.k8s.io/v1":
        meta.get("annotations", {}).pop("cert-manager.io/inject-ca-from", None)
        conv = doc.get("spec", {}).get("conversion", {})
        if conv.get("strategy") == "Webhook":
            wh_cfg = conv.setdefault("webhook", {}).setdefault("clientConfig", {})
            wh_cfg["caBundle"] = ca_bundle
            wh_cfg.setdefault("service", {})["port"] = 9443
        # Strip OpenAPI schema descriptions so ConfigMaps stay well under the 256KiB client-side apply annotation limit
        strip_descriptions(doc.get("spec", {}).get("versions", []))
        if name == "workloads.kueue.x-k8s.io":
            workloads_crd = doc
        else:
            other_crds.append(doc)
        continue

    if kind == "Namespace" and api_version == "v1":
        bootstrap_docs.append(doc)
        continue

    if kind == "Service" and api_version == "v1":
        spec = doc.get("spec", {})
        spec.pop("ports", None)
        spec.pop("selector", None)
        spec["type"] = "ExternalName"
        spec["externalName"] = external_name
        bootstrap_docs.append(doc)
        continue

    if kind == "APIService" and api_version == "apiregistration.k8s.io/v1":
        meta.get("annotations", {}).pop("cert-manager.io/inject-ca-from", None)
        spec = doc.get("spec", {})
        group = spec.get("group", "")
        port = 8082 if group == "visibility.kueue.x-k8s.io" else 9443
        spec.setdefault("service", {})["port"] = port
        bootstrap_docs.append(doc)
        continue

    if kind in ("MutatingWebhookConfiguration", "ValidatingWebhookConfiguration") and api_version == "admissionregistration.k8s.io/v1":
        meta.get("annotations", {}).pop("cert-manager.io/inject-ca-from", None)
        for wh in doc.get("webhooks", []):
            cc = wh.setdefault("clientConfig", {})
            cc["caBundle"] = ca_bundle
            cc.setdefault("service", {})["port"] = 9443
            if kind == "MutatingWebhookConfiguration" and wh.get("name") == "mpod.kb.io":
                obj_sel = wh.setdefault("objectSelector", {})
                obj_exprs = obj_sel.setdefault("matchExpressions", [])
                if not any(
                    ex.get("key") == "kueue-optimizer.gke.io/initial-pod" and ex.get("operator") == "NotIn"
                    for ex in obj_exprs
                ):
                    obj_exprs.append(
                        {
                            "key": "kueue-optimizer.gke.io/initial-pod",
                            "operator": "NotIn",
                            "values": ["true"],
                        }
                    )
        bootstrap_docs.append(doc)
        continue

if not manager_config_str:
    raise RuntimeError("kueue-manager-config ConfigMap not found in kustomize output")
if not workloads_crd:
    raise RuntimeError("workloads.kueue.x-k8s.io CRD not found in kustomize output")

with open(out_config_path, "w", encoding="utf-8") as f:
    f.write(manager_config_str)

with open(out_crds_path, "w", encoding="utf-8") as f:
    yaml.safe_dump_all(other_crds, f, sort_keys=False)

with open(out_workloads_crd_path, "w", encoding="utf-8") as f:
    json.dump(workloads_crd, f, separators=(",", ":"))

with open(out_bootstrap_path, "w", encoding="utf-8") as f:
    yaml.safe_dump_all(bootstrap_docs, f, sort_keys=False)
PYEOF

echo "==> [4/5] Building and transforming JobSet manifests from ${JOBSET_REPO}..."
RAW_JOBSET_MANIFEST="${TMP_DIR}/jobset-raw.yaml"
kubectl --kubeconfig=/dev/null kustomize "${JOBSET_REPO}/config/default" > "${RAW_JOBSET_MANIFEST}"

python3 - "${RAW_JOBSET_MANIFEST}" "${CA_B64}" "${JOBSET_SVC}" "${CONFIG_DIR}/jobset_controller_manager_config.yaml" "${MANIFESTS_DIR}/jobset-crds.json" "${MANIFESTS_DIR}/jobset-bootstrap.yaml" <<'PYEOF'
import json
import sys
import yaml

raw_manifest_path = sys.argv[1]
ca_bundle = sys.argv[2]
external_name = sys.argv[3]
out_config_path = sys.argv[4]
out_crds_path = sys.argv[5]
out_bootstrap_path = sys.argv[6]

def strip_descriptions(obj):
    if isinstance(obj, dict):
        obj.pop("description", None)
        for v in obj.values():
            strip_descriptions(v)
    elif isinstance(obj, list):
        for item in obj:
            strip_descriptions(item)

with open(raw_manifest_path, "r", encoding="utf-8") as f:
    docs = list(yaml.safe_load_all(f))

jobsets_crd = None
bootstrap_docs = []
manager_config_str = None

for doc in docs:
    if not doc:
        continue
    kind = doc.get("kind")
    api_version = doc.get("apiVersion")
    meta = doc.get("metadata", {})
    name = meta.get("name", "")

    if kind == "ConfigMap" and name == "jobset-manager-config":
        raw_cfg = doc.get("data", {}).get("controller_manager_config.yaml", "")
        cfg_obj = yaml.safe_load(raw_cfg) or {}
        cfg_obj.setdefault("health", {})["healthProbeBindAddress"] = ":8081"
        cfg_obj.setdefault("metrics", {})["bindAddress"] = ":8443"
        cfg_obj.setdefault("webhook", {})["port"] = 9443
        cfg_obj.setdefault("leaderElection", {})["leaderElect"] = False
        cfg_obj["leaderElection"].setdefault("resourceName", "6d4f6a47.jobset.x-k8s.io")
        cfg_obj.setdefault("clientConnection", {"qps": 500, "burst": 500})
        cfg_obj.setdefault("internalCertManagement", {})["enable"] = False
        manager_config_str = yaml.safe_dump(cfg_obj, sort_keys=False)
        continue

    if kind == "CustomResourceDefinition" and api_version == "apiextensions.k8s.io/v1":
        meta.get("annotations", {}).pop("cert-manager.io/inject-ca-from", None)
        conv = doc.get("spec", {}).get("conversion", {})
        if conv.get("strategy") == "Webhook":
            wh_cfg = conv.setdefault("webhook", {}).setdefault("clientConfig", {})
            wh_cfg["caBundle"] = ca_bundle
            wh_cfg.setdefault("service", {})["port"] = 9443
        # Strip OpenAPI schema descriptions so ConfigMap stays well under the 256KiB annotation limit
        strip_descriptions(doc.get("spec", {}).get("versions", []))
        jobsets_crd = doc
        continue

    if kind == "Namespace" and api_version == "v1":
        bootstrap_docs.append(doc)
        continue

    if kind == "Service" and api_version == "v1":
        spec = doc.get("spec", {})
        spec.pop("ports", None)
        spec.pop("selector", None)
        spec["type"] = "ExternalName"
        spec["externalName"] = external_name
        bootstrap_docs.append(doc)
        continue

    if kind in ("MutatingWebhookConfiguration", "ValidatingWebhookConfiguration") and api_version == "admissionregistration.k8s.io/v1":
        meta.get("annotations", {}).pop("cert-manager.io/inject-ca-from", None)
        for wh in doc.get("webhooks", []):
            cc = wh.setdefault("clientConfig", {})
            cc["caBundle"] = ca_bundle
            cc.setdefault("service", {})["port"] = 9443
            if kind == "MutatingWebhookConfiguration" and wh.get("name") == "mpod.kb.io":
                obj_sel = wh.setdefault("objectSelector", {})
                obj_exprs = obj_sel.setdefault("matchExpressions", [])
                if not any(
                    ex.get("key") == "kueue-optimizer.gke.io/initial-pod" and ex.get("operator") == "NotIn"
                    for ex in obj_exprs
                ):
                    obj_exprs.append(
                        {
                            "key": "kueue-optimizer.gke.io/initial-pod",
                            "operator": "NotIn",
                            "values": ["true"],
                        }
                    )
        bootstrap_docs.append(doc)
        continue

if not manager_config_str:
    raise RuntimeError("jobset-manager-config ConfigMap not found in kustomize output")
if not jobsets_crd:
    raise RuntimeError("jobsets.jobset.x-k8s.io CRD not found in kustomize output")

with open(out_config_path, "w", encoding="utf-8") as f:
    f.write(manager_config_str)

with open(out_crds_path, "w", encoding="utf-8") as f:
    json.dump(jobsets_crd, f, separators=(",", ":"))

with open(out_bootstrap_path, "w", encoding="utf-8") as f:
    yaml.safe_dump_all(bootstrap_docs, f, sort_keys=False)
PYEOF

echo "==> [5/5] Done! All artifacts generated for namespace '${NAMESPACE}'."
