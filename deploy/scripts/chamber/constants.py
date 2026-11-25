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

"""Shared constants, repository paths, and Kubernetes label validation for chamber scripts."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any


PACKAGE_DIR = Path(__file__).resolve().parent
SCRIPT_DIR = PACKAGE_DIR.parent
SCRIPT_PATH = SCRIPT_DIR / "chamber_common.py"
DEPLOY_DIR = SCRIPT_DIR.parent
ROOT_DIR = DEPLOY_DIR.parent

DEFAULT_CHAMBER_NAMESPACE = "kueue-optimizer-chamber-01"
DEFAULT_INFRA_PODS_NAMESPACE = "infra-pods"
DEFAULT_FLAVOR_NAME = "default-flavor"
DEFAULT_TOPOLOGY_NAME = "default-topology"
DEFAULT_PAUSE_IMAGE = "registry.k8s.io/pause:3.10"
DEFAULT_KUEUE_IMAGE = "us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main"
DEFAULT_APISERVER_PORT = 16443
DEFAULT_PROMETHEUS_BASE_PORT = 19090
DEFAULT_REGISTRY_BASE_PORT = 30500
NODEPORT_MIN = 30000
NODEPORT_MAX = 32767
DEFAULT_REGISTRY_MEMORY_LIMIT = "4Gi"
DEFAULT_SCRAPE_INTERVAL = "10s"
DEFAULT_FALLBACK_EXEC_SEC = 10.0
DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES: tuple[str, ...] = (
    "LessThanOrEqualToFinalShare",
    "LessThanInitialShare",
)

CHAMBER_NAMESPACE_LABEL_KEY = "kueue-optimizer.gke.io/chamber"
CHAMBER_NAMESPACE_LABEL_VALUE = "true"
CHAMBER_PART_OF_LABEL_VALUE = "kueue-optimizer"

DNS_LABEL_RE = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
DNS_SUBDOMAIN_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")


def validate_dns_label(value: Any, field_desc: str = "Kubernetes name") -> str:
    """Validate that value is a strict RFC 1123 DNS label (1..63 lowercase [a-z0-9-], alphanumeric ends)."""
    s = str(value if value is not None else "").strip()
    if not s or len(s) > 63 or not DNS_LABEL_RE.fullmatch(s):
        raise ValueError(
            f"Invalid {field_desc} {value!r}: must be a valid RFC 1123 DNS label "
            "(1..63 lowercase alphanumeric characters or '-', starting and ending with [a-z0-9])."
        )
    return s


def validate_dns_subdomain(value: Any, field_desc: str = "Kubernetes name") -> str:
    """Validate that value is a Kubernetes RFC 1123 DNS subdomain (1..253 chars, matching k8s IsDNS1123Subdomain)."""
    s = str(value if value is not None else "").strip()
    if not s or len(s) > 253 or not DNS_SUBDOMAIN_RE.fullmatch(s):
        raise ValueError(
            f"Invalid {field_desc} {value!r}: must be a valid RFC 1123 DNS subdomain "
            "(1..253 lowercase alphanumeric characters, '-' or '.', with each dot-separated segment "
            "starting and ending with [a-z0-9])."
        )
    return s


BINARY_SI_FACTORS: dict[str, float] = {
    "Ki": 1024.0,
    "Mi": 1024.0**2,
    "Gi": 1024.0**3,
    "Ti": 1024.0**4,
    "Pi": 1024.0**5,
    "Ei": 1024.0**6,
}

DECIMAL_SI_FACTORS: dict[str, float] = {
    "n": 1e-9,
    "u": 1e-6,
    "m": 1e-3,
    "k": 1e3,
    "K": 1e3,
    "M": 1e6,
    "G": 1e9,
    "T": 1e12,
    "P": 1e15,
    "E": 1e18,
}

CA_BUNDLE_PLACEHOLDER = "__KWOK_CA_BUNDLE__"
METRICS_TOKEN_CSV = 'kwok-metrics-reader-token,kwok-metrics-reader,1001,"system:masters"\n'
PKI_SECRET_NAME = "kwok-pki"
PKI_REQUIRED_KEYS: tuple[str, ...] = (
    "ca.crt",
    "ca.key",
    "admin.crt",
    "admin.key",
    "kubeconfig",
    "external-kubeconfig",
)
CA_BUNDLE_MANIFEST_FILES: tuple[str, ...] = (
    "jobset-bootstrap.yaml",
    "jobset-crds.json",
    "kueue-bootstrap.yaml",
    "kueue-crds.yaml",
    "kueue-workloads-crd.json",
)

CHAMBER_DEPLOYMENTS: tuple[str, ...] = (
    "deployment/kwok-registry",
    "deployment/kwok-etcd",
    "deployment/kwok-kube-apiserver",
    "deployment/kwok-kube-controller-manager",
    "deployment/kwok-kube-scheduler",
    "deployment/kwok-controller",
    "deployment/jobset-controller-manager",
    "deployment/kueue-controller-manager",
    "deployment/prometheus",
)

CHAMBER_CORE_HEALTH_COMPONENTS: dict[str, str] = {
    "etcd": "kwok-etcd",
    "kube-apiserver": "kwok-kube-apiserver",
    "kube-controller-manager": "kwok-kube-controller-manager",
    "kube-scheduler": "kwok-kube-scheduler",
    "kwok-controller": "kwok-controller",
    "jobset-controller-manager": "jobset-controller-manager",
    "kueue-controller-manager": "kueue-controller-manager",
    "prometheus": "prometheus",
}

REQUIRED_KWOK_CRDS: tuple[str, ...] = (
    "stages.kwok.x-k8s.io",
    "jobsets.jobset.x-k8s.io",
    "resourceflavors.kueue.x-k8s.io",
    "clusterqueues.kueue.x-k8s.io",
    "localqueues.kueue.x-k8s.io",
    "workloads.kueue.x-k8s.io",
    "cohorts.kueue.x-k8s.io",
    "topologies.kueue.x-k8s.io",
    "workloadpriorityclasses.kueue.x-k8s.io",
)

RESERVED_POD_LABELS: frozenset[str] = frozenset(
    {
        "job-name",
        "batch.kubernetes.io/job-name",
        "controller-uid",
        "batch.kubernetes.io/controller-uid",
        "kueue.x-k8s.io/queue-name",
        "kueue.x-k8s.io/priority-class",
    }
)
