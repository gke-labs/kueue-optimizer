#!/usr/bin/env python3
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

"""Shared configuration, Kubernetes manifest builders, and ChamberManager for kueue-optimizer runners.

Used by:
  - deploy/scripts/testrunner.py
  - deploy/scripts/chamber_registry.py
  - deploy/scripts/kueue_metrics_report.py
  - deploy/scripts/generate_scenario_readme.py
  - deploy/scripts/demo.py
"""

from __future__ import annotations

import argparse
import atexit
import base64
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
import http.client
import json
import math
import os
from pathlib import Path
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from typing import Any


SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
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


def validate_dns_label(value: Any, field_desc: str = "Kubernetes name") -> str:
    """Validate that value is a strict RFC 1123 DNS label (1..63 lowercase [a-z0-9-], alphanumeric ends)."""
    s = str(value if value is not None else "").strip()
    if not s or len(s) > 63 or not DNS_LABEL_RE.fullmatch(s):
        raise ValueError(
            f"Invalid {field_desc} {value!r}: must be a valid RFC 1123 DNS label "
            "(1..63 lowercase alphanumeric characters or '-', starting and ending with [a-z0-9])."
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


def env_str(name: str, default: str) -> str:
    val = os.environ.get(name)
    return val if val is not None and val != "" else default


def env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None or val.strip() == "":
        return default
    return int(val.strip())


def env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None or val.strip() == "":
        return default
    return float(val.strip())


def env_bool(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None or val.strip() == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


def parse_duration_seconds(raw: str | float | int | None) -> float | None:
    """Parse a duration string (e.g. '300', '45s', '10m', '1.5h') into seconds."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip().lower()
    if not s:
        return None
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*(ms|s|sec|secs|m|min|mins|h|hr|hrs|d)?", s)
    if not m:
        raise ValueError(
            f"Invalid duration '{raw}'. Expected seconds (e.g. 120) or unit suffix (e.g. 30s, 5m, 1h)."
        )
    val = float(m.group(1))
    unit = m.group(2) or "s"
    if unit == "ms":
        return val / 1000.0
    if unit in ("s", "sec", "secs"):
        return val
    if unit in ("m", "min", "mins"):
        return val * 60.0
    if unit in ("h", "hr", "hrs"):
        return val * 3600.0
    if unit == "d":
        return val * 86400.0
    return val


def normalize_scrape_interval(raw: str) -> str:
    s = str(raw).strip()
    if re.fullmatch(r"\d+", s):
        return f"{s}s"
    return s


def normalize_queueing_strategy(raw: str) -> str:
    lower = raw.strip().lower()
    if lower in ("strictfifo", "strict-fifo", "strict_fifo", "strict", "fifo"):
        return "StrictFIFO"
    if lower in (
        "besteffortfifo",
        "best-effort-fifo",
        "best_effort_fifo",
        "best-effort",
        "best_effort",
        "besteffort",
    ):
        return "BestEffortFIFO"
    raise ValueError(
        f"Invalid queueing mode '{raw}'. Supported values: StrictFIFO (strict-fifo) or BestEffortFIFO (best-effort)."
    )


def format_duration_hms(seconds: float) -> str:
    if seconds < 0:
        seconds = 0.0
    total_sec = int(round(seconds))
    hrs, rem = divmod(total_sec, 3600)
    mins, secs = divmod(rem, 60)
    if hrs > 0:
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"


def format_duration_human(sec: float) -> str:
    if sec < 0:
        sec = 0.0
    if sec == 0:
        return "0s"
    if sec < 60:
        return f"{sec:.1f}s"
    if sec < 3600:
        mins = sec / 60.0
        return f"{sec:.1f}s ({mins:.1f}m)"
    hrs = sec / 3600.0
    return f"{sec:,.1f}s ({hrs:.2f}h / {format_duration_hms(sec)})"


def format_duration_short(sec: float) -> str:
    if sec < 0:
        sec = 0.0
    if sec < 60:
        return f"{sec:.1f}s" if sec < 10 else f"{sec:.0f}s"
    if sec < 3600:
        return f"{sec:.0f}s ({sec / 60.0:.1f}m)"
    return f"{sec:.0f}s ({sec / 3600.0:.2f}h)"


def parse_rfc3339_epoch(ts_str: str | None, *, strict: bool = False) -> float | None:
    """Parse an RFC3339 / RFC3339Nano UTC timestamp string into Unix epoch seconds."""
    if not ts_str:
        return None
    s = str(ts_str).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)([+-]\d{2}:\d{2})$", s)
    if m:
        frac = (m.group(2) + "000000")[:6]
        s = f"{m.group(1)}.{frac}{m.group(3)}"
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        if strict:
            raise
        return None


def format_epoch_iso(epoch: float | None, *, microsecond_precision: bool = False) -> str:
    if epoch is None:
        return "n/a"
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    if microsecond_precision:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if dt.microsecond:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_positive_duration_annotation(anns: dict[str, Any] | None, *keys: str) -> float | None:
    """Parse the first present positive duration annotation (numeric seconds or unit-suffixed string)."""
    if not isinstance(anns, dict):
        return None
    for key in keys:
        raw = anns.get(key)
        if raw is None:
            continue
        try:
            val = parse_duration_seconds(raw)
        except (ValueError, TypeError):
            continue
        if val is not None and not math.isnan(val) and not math.isinf(val) and val > 0.0:
            return val
    return None


def format_pod_execution_seconds(seconds: float, min_seconds: float = 0.1) -> str:
    """Format pod execution time in plain numeric seconds for kueue-optimizer.gke.io/pod-execution-time-seconds."""
    clamped = max(min_seconds, float(seconds))
    if float(clamped).is_integer():
        return str(int(clamped))
    formatted = f"{clamped:.3f}".rstrip("0").rstrip(".")
    return formatted or "0.1"


# Backward-compatible alias used across runner scripts
format_kwok_pod_duration = format_pod_execution_seconds


def parse_k8s_quantity(raw: Any, resource_name: str = "") -> float:
    """Parse a Kubernetes resource.Quantity into standard base units:
    - memory / ephemeral-storage -> GiB (1024^3 bytes)
    - cpu -> cores (1.0 = 1 vCPU)
    - all other resources (pods, GPUs, TPUs, hugepages) -> scalar base units
    """
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        val = float(raw)
        if resource_name in ("memory", "ephemeral-storage"):
            return val / (1024.0**3)
        return val
    s = str(raw).strip().strip("\"'").strip()
    if not s:
        return 0.0

    factor = 1.0
    num_str = s
    for suf, mult in BINARY_SI_FACTORS.items():
        if s.endswith(suf):
            num_str = s[: -len(suf)]
            factor = mult
            break
    else:
        for suf, mult in DECIMAL_SI_FACTORS.items():
            if s.endswith(suf):
                num_str = s[: -len(suf)]
                factor = mult
                break

    try:
        base_val = float(num_str) * factor
    except ValueError:
        return 0.0

    if resource_name in ("memory", "ephemeral-storage"):
        return base_val / (1024.0**3)
    return base_val


def format_resource_quantity(val: float, resource_name: str = "", *, show_tib_paren: bool = False) -> str:
    """Format a parsed resource value (cores, GiB, or count) into a readable string."""
    if resource_name in ("memory", "ephemeral-storage"):
        if abs(val) >= 1024.0 * 10.0:
            if show_tib_paren and abs(val / 1024.0 - round(val / 1024.0, 1)) < 0.05:
                return f"{val:,.0f} GiB ({val / 1024.0:,.1f} TiB)"
            if not show_tib_paren:
                return f"{val / 1024.0:,.1f} TiB"
        if abs(val - round(val)) < 1e-2:
            return f"{int(round(val)):,} GiB"
        return f"{val:,.2f} GiB"
    if resource_name == "cpu":
        if abs(val - round(val)) < 1e-2:
            return f"{int(round(val)):,} cores"
        return f"{val:,.2f} cores"
    if abs(val - round(val)) < 1e-2:
        return f"{int(round(val)):,}"
    return f"{val:,.2f}"


def format_compact_si(val: float) -> str:
    abs_v = abs(val)
    if abs_v >= 1e9:
        return f"{val / 1e9:.2f}B"
    if abs_v >= 1e6:
        return f"{val / 1e6:.2f}M"
    if abs_v >= 1e4:
        return f"{val / 1e3:.2f}k"
    return f"{val:.1f}"


def format_num(val: float, unit: str = "", *, is_bytes: bool = False) -> str:
    if math.isnan(val):
        return "n/a"
    if math.isinf(val):
        return "+Inf" if val > 0 else "-Inf"
    abs_v = abs(val)
    if is_bytes and unit == "":
        if abs_v == 0:
            return "0.00B"
        if abs_v >= 1024.0**4:
            return f"{val / (1024.0**4):.2f}Ti"
        if abs_v >= 1024.0**3:
            return f"{val / (1024.0**3):.2f}Gi"
        if abs_v >= 1024.0**2:
            return f"{val / (1024.0**2):.2f}Mi"
        if abs_v >= 1024.0:
            return f"{val / 1024.0:.2f}Ki"
        return f"{val:.0f}B"
    if abs_v == 0:
        return f"0.00{unit}"
    if abs_v >= 1e9 and unit == "":
        return f"{val / 1e9:.2f}B"
    if abs_v >= 1e6 and unit == "":
        return f"{val / 1e6:.2f}M"
    if abs_v < 0.01:
        return f"{val:.4f}{unit}"
    if abs_v >= 1000 and val == int(val):
        return f"{int(val)}{unit}"
    return f"{val:.2f}{unit}"


def format_pct(val: float) -> str:
    """Format a percentage value (0..100+) with two decimal places."""
    if math.isnan(val):
        return "n/a"
    if math.isinf(val):
        return "+Inf" if val > 0 else "-Inf"
    return f"{val:.2f}%"


def compute_percentile(sorted_vals: list[float], q: float, *, empty_default: float = math.nan) -> float:
    """Compute the q-th percentile (0.0..1.0) on a sorted list via linear interpolation."""
    n = len(sorted_vals)
    if n == 0:
        return empty_default
    if n == 1:
        return float(sorted_vals[0])
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    weight = pos - lo
    return float(sorted_vals[lo]) * (1.0 - weight) + float(sorted_vals[hi]) * weight


def summarize_samples(values: list[float]) -> tuple[float, float, float, float] | None:
    """Return (p50, p90, p99, avg) for a list of numeric samples."""
    finite = [float(v) for v in values if not math.isnan(v) and not math.isinf(v)]
    if not finite:
        return None
    s = sorted(finite)
    return (
        compute_percentile(s, 0.50),
        compute_percentile(s, 0.90),
        compute_percentile(s, 0.99),
        sum(s) / len(s),
    )


def calc_summary_stats(vals: list[float]) -> dict[str, float]:
    """Return count, min, p50, p90, p99, avg, max, and sum for a list of values."""
    finite = [float(v) for v in vals if not math.isnan(v) and not math.isinf(v)]
    if not finite:
        return {
            "count": 0.0,
            "min": 0.0,
            "p50": 0.0,
            "p90": 0.0,
            "p99": 0.0,
            "avg": 0.0,
            "max": 0.0,
            "sum": 0.0,
        }
    s = sorted(finite)
    total = sum(s)
    return {
        "count": float(len(s)),
        "min": s[0],
        "p50": compute_percentile(s, 0.50, empty_default=0.0),
        "p90": compute_percentile(s, 0.90, empty_default=0.0),
        "p99": compute_percentile(s, 0.99, empty_default=0.0),
        "avg": total / len(s),
        "max": s[-1],
        "sum": total,
    }


def sanitize_k8s_name(name: str, max_len: int = 52) -> str:
    s = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
    if not s:
        s = "job"
    if len(s) > max_len:
        s = s[:max_len].rstrip("-")
    return s


def compute_prometheus_port(apiserver_port: int, explicit_prom_port: int | None = None) -> int:
    if explicit_prom_port is not None:
        return explicit_prom_port
    env_port = os.environ.get("LOCAL_PROMETHEUS_PORT", "").strip()
    if env_port:
        return int(env_port)
    return DEFAULT_PROMETHEUS_BASE_PORT + apiserver_port - DEFAULT_APISERVER_PORT


def compute_registry_port(apiserver_port: int, explicit_registry_port: int | None = None) -> int:
    """Compute a deterministic Kubernetes NodePort (30000..32767) for the per-chamber registry."""
    if explicit_registry_port is not None:
        port = int(explicit_registry_port)
        if not (NODEPORT_MIN <= port <= NODEPORT_MAX):
            raise ValueError(
                f"Registry NodePort {port} out of valid Kubernetes NodePort range ({NODEPORT_MIN}..{NODEPORT_MAX})."
            )
        return port
    env_port = os.environ.get("LOCAL_REGISTRY_PORT", "").strip()
    if env_port:
        port = int(env_port)
        if not (NODEPORT_MIN <= port <= NODEPORT_MAX):
            raise ValueError(
                f"LOCAL_REGISTRY_PORT={port} out of valid Kubernetes NodePort range ({NODEPORT_MIN}..{NODEPORT_MAX})."
            )
        return port
    candidate = DEFAULT_REGISTRY_BASE_PORT + int(apiserver_port) - DEFAULT_APISERVER_PORT
    if NODEPORT_MIN <= candidate <= NODEPORT_MAX:
        return candidate
    span = NODEPORT_MAX - NODEPORT_MIN + 1
    return NODEPORT_MIN + ((candidate - NODEPORT_MIN) % span)


def parse_image_ref(image_ref: str) -> tuple[str, str, str]:
    """Parse an OCI/Docker image reference into (registry_host, repository, tag_or_digest).

    Examples:
      'localhost:30500/kueue:dev' -> ('localhost:30500', 'kueue', 'dev')
      'my-kueue:v1'               -> ('', 'my-kueue', 'v1')
      'kueue-dev'                 -> ('', 'kueue-dev', 'latest')
      'us-central1-docker.pkg.dev/proj/repo/kueue:main'
                                  -> ('us-central1-docker.pkg.dev', 'proj/repo/kueue', 'main')
    """
    ref = image_ref.strip()
    if not ref:
        raise ValueError("Image reference cannot be empty.")

    digest_part = ""
    if "@" in ref:
        ref, digest_part = ref.split("@", 1)

    # Separate optional tag after the last '/' (so registry host:port colons are preserved)
    last_slash = ref.rfind("/")
    after_slash = ref[last_slash + 1 :]
    if ":" in after_slash:
        colon_idx = last_slash + 1 + after_slash.rfind(":")
        name_part = ref[:colon_idx]
        tag_part = ref[colon_idx + 1 :]
    else:
        name_part = ref
        tag_part = ""

    if "/" in name_part:
        first_seg, rest = name_part.split("/", 1)
        if "." in first_seg or ":" in first_seg or first_seg == "localhost":
            registry_host = first_seg
            repo_path = rest
        else:
            registry_host = ""
            repo_path = name_part
    else:
        registry_host = ""
        repo_path = name_part

    qualifier = digest_part if digest_part else (tag_part or "latest")
    return registry_host, repo_path, qualifier


def docker_image_exists_locally(image_ref: str) -> bool:
    """Return True if image_ref exists in the local workstation Docker daemon."""
    try:
        proc = subprocess.run(
            ["docker", "image", "inspect", image_ref],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=15.0,
        )
        return proc.returncode == 0
    except Exception:
        return False


def resolve_kubeconfig_path(chamber_namespace: str, explicit_path: str | Path | None = None) -> Path:
    if explicit_path:
        return Path(explicit_path)
    env_kc = os.environ.get("KUBECONFIG_OUT", "").strip()
    if env_kc:
        return Path(env_kc)
    return ROOT_DIR / f"kwok-{chamber_namespace}.kubeconfig"


YAML_SPECIAL_STRINGS: frozenset[str] = frozenset(
    {"true", "false", "yes", "no", "on", "off", "null", "~", ""}
)
YAML_SAFE_UNQUOTED_RE = re.compile(r"^[A-Za-z0-9_./-]+$")
K8S_TOP_KEY_ORDER: tuple[str, ...] = ("apiVersion", "kind", "metadata", "value", "spec", "status")
K8S_META_KEY_ORDER: tuple[str, ...] = ("name", "namespace", "uid", "creationTimestamp", "labels", "annotations")


def _yaml_scalar(val: Any) -> str:
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, dict) and not val:
        return "{}"
    if isinstance(val, list) and not val:
        return "[]"
    s = str(val)
    if s == "":
        return '""'
    if s.lower() in YAML_SPECIAL_STRINGS:
        return json.dumps(s)
    try:
        float(s)
        return json.dumps(s)
    except ValueError:
        pass
    if YAML_SAFE_UNQUOTED_RE.match(s) and not s.startswith("-"):
        return s
    return json.dumps(s)


def _ordered_dict_items(d: dict[str, Any], level: int = 0, parent_key: str = "") -> list[tuple[str, Any]]:
    if level == 0:
        priority = {k: i for i, k in enumerate(K8S_TOP_KEY_ORDER)}
        return sorted(d.items(), key=lambda kv: (priority.get(kv[0], 99), kv[0]))
    if parent_key == "metadata":
        priority = {k: i for i, k in enumerate(K8S_META_KEY_ORDER)}
        return sorted(d.items(), key=lambda kv: (priority.get(kv[0], 99), kv[0]))
    return list(d.items())


def to_clean_yaml(obj: Any, indent: int = 0, parent_key: str = "") -> str:
    """Serialize a Python dict/list/scalar into clean, zero-dependency Kubernetes YAML."""
    pad = "  " * indent
    if isinstance(obj, dict):
        if not obj:
            return f"{pad}{{}}" if indent > 0 else "{}"
        lines: list[str] = []
        for k, v in _ordered_dict_items(obj, level=indent, parent_key=parent_key):
            key_str = k if YAML_SAFE_UNQUOTED_RE.match(str(k)) else json.dumps(str(k))
            if isinstance(v, dict):
                if not v:
                    lines.append(f"{pad}{key_str}: {{}}")
                else:
                    lines.append(f"{pad}{key_str}:")
                    lines.append(to_clean_yaml(v, indent + 1, parent_key=str(k)))
            elif isinstance(v, list):
                if not v:
                    lines.append(f"{pad}{key_str}: []")
                else:
                    lines.append(f"{pad}{key_str}:")
                    lines.append(to_clean_yaml(v, indent, parent_key=str(k)))
            else:
                lines.append(f"{pad}{key_str}: {_yaml_scalar(v)}")
        return "\n".join(lines)

    if isinstance(obj, list):
        if not obj:
            return f"{pad}[]" if indent > 0 else "[]"
        lines = []
        for item in obj:
            if isinstance(item, dict):
                if not item:
                    lines.append(f"{pad}- {{}}")
                    continue
                items_kv = _ordered_dict_items(item, level=indent + 1)
                first_k, first_v = items_kv[0]
                fk_str = first_k if YAML_SAFE_UNQUOTED_RE.match(str(first_k)) else json.dumps(str(first_k))
                if isinstance(first_v, dict):
                    if not first_v:
                        lines.append(f"{pad}- {fk_str}: {{}}")
                    else:
                        lines.append(f"{pad}- {fk_str}:")
                        lines.append(to_clean_yaml(first_v, indent + 2, parent_key=str(first_k)))
                elif isinstance(first_v, list):
                    if not first_v:
                        lines.append(f"{pad}- {fk_str}: []")
                    else:
                        lines.append(f"{pad}- {fk_str}:")
                        lines.append(to_clean_yaml(first_v, indent + 1, parent_key=str(first_k)))
                else:
                    lines.append(f"{pad}- {fk_str}: {_yaml_scalar(first_v)}")
                for rk, rv in items_kv[1:]:
                    rk_str = rk if YAML_SAFE_UNQUOTED_RE.match(str(rk)) else json.dumps(str(rk))
                    if isinstance(rv, dict):
                        if not rv:
                            lines.append(f"{pad}  {rk_str}: {{}}")
                        else:
                            lines.append(f"{pad}  {rk_str}:")
                            lines.append(to_clean_yaml(rv, indent + 2, parent_key=str(rk)))
                    elif isinstance(rv, list):
                        if not rv:
                            lines.append(f"{pad}  {rk_str}: []")
                        else:
                            lines.append(f"{pad}  {rk_str}:")
                            lines.append(to_clean_yaml(rv, indent + 1, parent_key=str(rk)))
                    else:
                        lines.append(f"{pad}  {rk_str}: {_yaml_scalar(rv)}")
            elif isinstance(item, list):
                if not item:
                    lines.append(f"{pad}- []")
                else:
                    lines.append(f"{pad}-")
                    lines.append(to_clean_yaml(item, indent + 1))
            else:
                lines.append(f"{pad}- {_yaml_scalar(item)}")
        return "\n".join(lines)

    return f"{pad}{_yaml_scalar(obj)}"


def docs_to_yaml_stream(docs: list[dict[str, Any]]) -> str:
    """Serialize a list of Kubernetes manifest dicts into a multi-document YAML/JSON stream."""
    return "\n---\n".join(json.dumps(doc) for doc in docs) + "\n"


def docs_to_pretty_yaml(docs: list[dict[str, Any]]) -> str:
    """Format documents as readable YAML for --dry-run, README rendering, and error reporting."""
    if not docs:
        return ""
    return "\n---\n".join(to_clean_yaml(doc) for doc in docs) + "\n"


def load_jsonl_events(path: Path, *, missing_ok: bool = False) -> list[dict[str, Any]]:
    """Read a JSON Lines (NDJSON) file one line at a time, also supporting legacy JSON arrays."""
    if not path.exists():
        if missing_ok:
            return []
        raise FileNotFoundError(f"Scenario file not found: {path}")
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        checked_first = False
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            if not checked_first:
                checked_first = True
                if stripped.startswith("["):
                    f.seek(0)
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
                    if missing_ok:
                        return []
                    raise ValueError(f"Unexpected JSON root in {path}")
            events.append(json.loads(stripped))
    return events


@dataclass
class WorkloadExecutionInfo:
    """Indexed execution timing metadata for a single workload from execution-time.json."""

    workload_name: str
    namespace: str
    owner_kind: str
    owner_name: str
    local_queue_name: str = ""
    cluster_queue_name: str = ""
    full_duration_sec: float | None = None
    partial_durations_sec: list[float] = field(default_factory=list)
    admitted_epochs: list[float] = field(default_factory=list)
    preempted_epochs: list[float] = field(default_factory=list)

    def resolved_duration_sec(self, default_sec: float = DEFAULT_FALLBACK_EXEC_SEC) -> float:
        """Return the workload's execution duration in seconds (preferring FullExecution)."""
        if self.full_duration_sec is not None and self.full_duration_sec > 0:
            return self.full_duration_sec
        if self.partial_durations_sec:
            return max(self.partial_durations_sec)
        return default_sec


@dataclass
class ExecutionTimeIndex:
    """Pre-computed lookup index of workload execution durations from execution-time.json."""

    by_owner_kind_name: dict[tuple[str, str, str], WorkloadExecutionInfo] = field(default_factory=dict)
    by_owner_name: dict[tuple[str, str], WorkloadExecutionInfo] = field(default_factory=dict)
    by_workload_name: dict[tuple[str, str], WorkloadExecutionInfo] = field(default_factory=dict)
    total_events: int = 0
    admitted_events: int = 0
    partial_events: int = 0
    full_events: int = 0
    first_event_epoch: float | None = None
    last_event_epoch: float | None = None

    def lookup(
        self,
        namespace: str,
        object_kind: str,
        name: str,
        parent_jobset_name: str | None = None,
    ) -> WorkloadExecutionInfo | None:
        """Look up execution info for a workflow object by (namespace, kind, name) or workload name."""
        info = self.by_owner_kind_name.get((namespace, object_kind, name))
        if info is not None:
            return info
        info = self.by_owner_name.get((namespace, name))
        if info is not None:
            return info
        info = self.by_workload_name.get((namespace, name))
        if info is not None:
            return info
        if parent_jobset_name:
            info = self.by_owner_kind_name.get((namespace, "JobSet", parent_jobset_name))
            if info is not None:
                return info
            info = self.by_owner_name.get((namespace, parent_jobset_name))
            if info is not None:
                return info
        return None

    def resolve_duration_sec(
        self,
        namespace: str,
        object_kind: str,
        name: str,
        fallback_sec: float = DEFAULT_FALLBACK_EXEC_SEC,
        parent_jobset_name: str | None = None,
    ) -> tuple[float, bool]:
        """Return (duration_seconds, found_in_index) for the given workload."""
        info = self.lookup(namespace, object_kind, name, parent_jobset_name=parent_jobset_name)
        if info is not None:
            if info.full_duration_sec is not None and info.full_duration_sec > 0:
                return info.full_duration_sec, True
            if info.partial_durations_sec:
                return max(info.partial_durations_sec), True
        return fallback_sec, False


def index_execution_times(
    execution_time_path: Path,
    *,
    missing_ok: bool = False,
    strict_timestamps: bool = False,
) -> ExecutionTimeIndex:
    """Index execution time of all workloads from execution-time.json before any cluster setup."""
    idx = ExecutionTimeIndex()
    events = load_jsonl_events(execution_time_path, missing_ok=missing_ok)
    idx.total_events = len(events)

    for ev_idx, ev in enumerate(events):
        ev_type = str(ev.get("eventType") or "")
        raw_ev_ts = ev.get("timestamp")
        if strict_timestamps and (raw_ev_ts is None or not str(raw_ev_ts).strip()):
            raise ValueError(f"{execution_time_path} event #{ev_idx}: missing required 'timestamp' field.")
        ev_ts = parse_rfc3339_epoch(raw_ev_ts, strict=strict_timestamps)
        if ev_ts is not None:
            if idx.first_event_epoch is None or ev_ts < idx.first_event_epoch:
                idx.first_event_epoch = ev_ts
            if idx.last_event_epoch is None or ev_ts > idx.last_event_epoch:
                idx.last_event_epoch = ev_ts
        pl = ev.get("payload") or {}
        wl_name = str(pl.get("workloadName") or "")
        ns = str(pl.get("namespace") or "default")
        owner_kind = str(pl.get("ownerKind") or "")
        owner_name = str(pl.get("ownerName") or "")
        lq_name = str(pl.get("localQueueName") or "")
        cq_name = str(pl.get("clusterQueueName") or "")

        key_wl = (ns, wl_name)
        info = idx.by_workload_name.get(key_wl)
        if info is None and owner_name:
            info = idx.by_owner_kind_name.get((ns, owner_kind, owner_name)) or idx.by_owner_name.get(
                (ns, owner_name)
            )
        if info is None:
            info = WorkloadExecutionInfo(
                workload_name=wl_name,
                namespace=ns,
                owner_kind=owner_kind,
                owner_name=owner_name,
                local_queue_name=lq_name,
                cluster_queue_name=cq_name,
            )

        if lq_name and not info.local_queue_name:
            info.local_queue_name = lq_name
        if cq_name and not info.cluster_queue_name:
            info.cluster_queue_name = cq_name

        dur_raw = pl.get("executionDurationSeconds")
        dur_val = float(dur_raw) if dur_raw is not None else None
        if dur_val is None and pl.get("executionDuration"):
            try:
                dur_val = parse_duration_seconds(pl.get("executionDuration"))
            except ValueError:
                dur_val = None

        if ev_type == "Admitted":
            idx.admitted_events += 1
            adm_ts = parse_rfc3339_epoch(pl.get("admittedTime"), strict=strict_timestamps) or ev_ts
            if adm_ts is not None:
                info.admitted_epochs.append(adm_ts)
        elif ev_type == "PartialExecution":
            idx.partial_events += 1
            if dur_val is not None and dur_val > 0:
                info.partial_durations_sec.append(dur_val)
            end_ts = parse_rfc3339_epoch(pl.get("endTime"), strict=strict_timestamps) or ev_ts
            if end_ts is not None:
                info.preempted_epochs.append(end_ts)
        elif ev_type == "FullExecution":
            idx.full_events += 1
            if dur_val is not None and dur_val > 0:
                info.full_duration_sec = dur_val

        if wl_name:
            idx.by_workload_name[(ns, wl_name)] = info
        if owner_name:
            idx.by_owner_name[(ns, owner_name)] = info
            if owner_kind:
                idx.by_owner_kind_name[(ns, owner_kind, owner_name)] = info

    return idx


def extract_pod_spec_resources(
    p_spec: dict[str, Any] | None,
    *,
    include_ephemeral_storage: bool = False,
) -> dict[str, float]:
    """Compute effective per-pod resource requests from a Kubernetes PodSpec.

    Follows exact Kubernetes / Kueue resource semantics:
      - Each container resource defaults to limits[rk] when omitted from requests[rk] ({**limits, **requests}).
      - Regular containers are summed across the pod; each initContainer takes max(current, init_req).
      - Pod overhead (spec.overhead), if present, is added once per pod.
    """
    if not isinstance(p_spec, dict):
        return {}
    per_pod: dict[str, float] = {}
    for c in p_spec.get("containers") or []:
        if not isinstance(c, dict):
            continue
        res_block = c.get("resources") or {}
        eff_reqs = {**(res_block.get("limits") or {}), **(res_block.get("requests") or {})}
        for rk, rv in eff_reqs.items():
            if not include_ephemeral_storage and rk == "ephemeral-storage":
                continue
            per_pod[rk] = per_pod.get(rk, 0.0) + parse_k8s_quantity(rv, rk)

    for ic in p_spec.get("initContainers") or []:
        if not isinstance(ic, dict):
            continue
        res_block = ic.get("resources") or {}
        eff_reqs = {**(res_block.get("limits") or {}), **(res_block.get("requests") or {})}
        for rk, rv in eff_reqs.items():
            if not include_ephemeral_storage and rk == "ephemeral-storage":
                continue
            per_pod[rk] = max(per_pod.get(rk, 0.0), parse_k8s_quantity(rv, rk))

    overhead = p_spec.get("overhead") or {}
    if isinstance(overhead, dict):
        for rk, rv in overhead.items():
            if not include_ephemeral_storage and rk == "ephemeral-storage":
                continue
            per_pod[rk] = per_pod.get(rk, 0.0) + parse_k8s_quantity(rv, rk)

    return per_pod


def extract_workflow_pods_and_requests(
    obj: dict[str, Any],
    kind: str | None = None,
) -> tuple[int, dict[str, float], dict[str, float]]:
    """Return (total_pods, total_requests_across_all_pods, representative_per_pod_requests)
    across JobSet, Job, Deployment, StatefulSet, LeaderWorkerSet, RayJob, RayCluster, Workload, and Pod."""
    resolved_kind = str(kind or obj.get("kind") or "JobSet")
    spec = obj.get("spec") or {}
    total_pods = 0
    total_reqs: dict[str, float] = defaultdict(float)
    sample_pod_reqs: dict[str, float] = {}

    def _accumulate_pod_group(p_spec: dict[str, Any] | None, count: int) -> None:
        nonlocal total_pods, sample_pod_reqs
        if count <= 0:
            return
        p_reqs = extract_pod_spec_resources(p_spec)
        if p_reqs and not sample_pod_reqs:
            sample_pod_reqs = dict(p_reqs)
        total_pods += count
        for rk, rv in p_reqs.items():
            total_reqs[rk] += rv * count

    if resolved_kind == "JobSet":
        for rj in spec.get("replicatedJobs") or []:
            if not isinstance(rj, dict):
                continue
            replicas = max(1, int(rj.get("replicas") or 1))
            j_spec = ((rj.get("template") or {}).get("spec")) or {}
            parallelism = max(1, int(j_spec.get("parallelism") or j_spec.get("completions") or 1))
            pod_spec = ((j_spec.get("template") or {}).get("spec")) or {}
            _accumulate_pod_group(pod_spec, replicas * parallelism)
    elif resolved_kind == "Job":
        parallelism = max(1, int(spec.get("parallelism") or spec.get("completions") or 1))
        _accumulate_pod_group(((spec.get("template") or {}).get("spec")) or {}, parallelism)
    elif resolved_kind in ("Deployment", "StatefulSet", "ReplicaSet"):
        replicas = max(1, int(spec.get("replicas") or 1))
        _accumulate_pod_group(((spec.get("template") or {}).get("spec")) or {}, replicas)
    elif resolved_kind == "LeaderWorkerSet":
        replicas = max(1, int(spec.get("replicas") or 1))
        lwt = spec.get("leaderWorkerTemplate") or {}
        size = max(1, int(lwt.get("size") or 1))
        worker_tpl_spec = ((lwt.get("workerTemplate") or {}).get("spec")) or {}
        leader_tpl = lwt.get("leaderTemplate")
        if isinstance(leader_tpl, dict) and leader_tpl:
            _accumulate_pod_group(leader_tpl.get("spec") or {}, replicas)
            _accumulate_pod_group(worker_tpl_spec, replicas * max(0, size - 1))
        else:
            _accumulate_pod_group(worker_tpl_spec, replicas * size)
    elif resolved_kind == "RayJob":
        sub_tpl = spec.get("submitterPodTemplate")
        if isinstance(sub_tpl, dict) and sub_tpl:
            _accumulate_pod_group(sub_tpl.get("spec") or {}, 1)
        rcs = spec.get("rayClusterSpec") or {}
        if isinstance(rcs, dict):
            head = rcs.get("headGroupSpec")
            if isinstance(head, dict):
                _accumulate_pod_group(((head.get("template") or {}).get("spec")) or {}, 1)
            for wg in rcs.get("workerGroupSpecs") or []:
                if isinstance(wg, dict):
                    _accumulate_pod_group(
                        ((wg.get("template") or {}).get("spec")) or {},
                        max(1, int(wg.get("replicas") or 1)),
                    )
    elif resolved_kind == "RayCluster":
        head = spec.get("headGroupSpec")
        if isinstance(head, dict):
            _accumulate_pod_group(((head.get("template") or {}).get("spec")) or {}, 1)
        for wg in spec.get("workerGroupSpecs") or []:
            if isinstance(wg, dict):
                _accumulate_pod_group(
                    ((wg.get("template") or {}).get("spec")) or {},
                    max(1, int(wg.get("replicas") or 1)),
                )
    elif resolved_kind == "Workload":
        for ps in spec.get("podSets") or []:
            if isinstance(ps, dict):
                ps_count = max(1, int(ps.get("count") if ps.get("count") is not None else 1))
                _accumulate_pod_group(((ps.get("template") or {}).get("spec")) or {}, ps_count)
    elif resolved_kind == "Pod":
        _accumulate_pod_group(spec, 1)
    else:
        replicas = max(1, int(spec.get("replicas") or 1))
        _accumulate_pod_group(((spec.get("template") or {}).get("spec")) or {}, replicas)

    total_pods = max(1, total_pods)
    total_reqs["pods"] = float(total_pods)
    return total_pods, dict(total_reqs), sample_pod_reqs


def collect_known_parent_workflows(
    workflow_events: list[dict[str, Any]],
    override_workload_namespace: str | None = None,
) -> set[tuple[str, str, str]]:
    """Collect (namespace, kind, name) of all parent JobSet and RayJob objects in workflows.json."""
    known_parents: set[tuple[str, str, str]] = set()
    for ev in workflow_events:
        pl = ev.get("payload") or {}
        obj = pl.get("object") if isinstance(pl.get("object"), dict) else {}
        meta = (obj or {}).get("metadata") or {}
        kind = str(pl.get("objectKind") or (obj or {}).get("kind") or "")
        name = str(pl.get("name") or meta.get("name") or "")
        ns = override_workload_namespace or str(pl.get("namespace") or meta.get("namespace") or "default")
        if kind in ("JobSet", "RayJob") and name:
            known_parents.add((ns, kind, name))
    return known_parents


def is_child_workflow_owned_by_tracked_parent(
    obj: dict[str, Any],
    object_kind: str,
    namespace: str,
    known_parents: set[tuple[str, str, str]],
) -> tuple[bool, str | None]:
    """Return (is_child, parent_jobset_name) if this workflow is managed by a parent JobSet or RayJob."""
    meta = obj.get("metadata") or {}
    labels = meta.get("labels") or {}
    annotations = meta.get("annotations") or {}

    js_name = labels.get("jobset.sigs.k8s.io/jobset-name") or annotations.get(
        "jobset.sigs.k8s.io/jobset-name"
    )
    if object_kind == "Job" and js_name:
        if (namespace, "JobSet", str(js_name)) in known_parents:
            return True, str(js_name)

    for ref in meta.get("ownerReferences") or []:
        if not isinstance(ref, dict):
            continue
        ref_kind = str(ref.get("kind") or "")
        ref_name = str(ref.get("name") or "")
        if ref_kind and ref_name and (namespace, ref_kind, ref_name) in known_parents:
            return True, ref_name if ref_kind == "JobSet" else None

    return False, str(js_name) if js_name else None


def collect_not_admitted_workflow_admit_epochs(
    workflow_events: list[dict[str, Any]],
    known_parents: set[tuple[str, str, str]],
    override_workload_namespace: str | None = None,
    *,
    strict_timestamps: bool = False,
) -> dict[str, float]:
    """Return earliest admission epoch per namespace for workflows first observed as NotAdmitted.

    Works identically whether cluster-scraper ran with plain names or --hash-names (HMAC-SHA256),
    allowing initial-pod filtering to exclude pods spawned after NotAdmitted workflows became Admitted.
    """
    first_seen_state: dict[tuple[str, str, str], str] = {}
    earliest_admit_by_ns: dict[str, float] = {}

    for ev in workflow_events:
        ev_type = str(ev.get("eventType") or "")
        if ev_type == "Deleted":
            continue
        ev_ep = parse_rfc3339_epoch(ev.get("timestamp"), strict=strict_timestamps)
        wl_state = str(ev.get("workloadState") or "")
        pl = ev.get("payload") or {}
        raw_obj = pl.get("object")
        if not isinstance(raw_obj, dict):
            continue
        meta = raw_obj.get("metadata") or {}
        kind = str(pl.get("objectKind") or raw_obj.get("kind") or "")
        name = str(pl.get("name") or meta.get("name") or "")
        ns = override_workload_namespace or str(pl.get("namespace") or meta.get("namespace") or "default")
        if not kind or not name:
            continue
        is_child, _ = is_child_workflow_owned_by_tracked_parent(raw_obj, kind, ns, known_parents)
        if is_child:
            continue
        if not wl_state:
            suspend_val = (raw_obj.get("spec") or {}).get("suspend")
            wl_state = "NotAdmitted" if suspend_val is True else "Admitted"
        key = (ns, kind, name)
        if key not in first_seen_state:
            first_seen_state[key] = wl_state
        elif first_seen_state[key] == "NotAdmitted" and wl_state in ("Admitted", "AdmissionInProgress"):
            if ev_ep is not None:
                prev = earliest_admit_by_ns.get(ns)
                if prev is None or ev_ep < prev:
                    earliest_admit_by_ns[ns] = ev_ep

    return earliest_admit_by_ns


def filter_initial_running_pods(
    pod_new_events: dict[tuple[str, str], tuple[float, dict[str, Any]]],
    pod_delete_epochs: dict[tuple[str, str], float],
    *,
    t0_epoch: float,
    effective_start_epoch: float,
    initial_window_sec: float = 1.0,
    not_admitted_workflow_names: list[tuple[str, str]] | set[tuple[str, str]] = (),
    earliest_admit_by_ns: dict[str, float] | None = None,
    strict_timestamps: bool = False,
) -> tuple[list[tuple[str, str, float, dict[str, Any], float | None]], int]:
    """Select pods from infra.json that were genuinely running at the start of the test.

    Handles:
      1. Plain names (excludes exact NotAdmitted Pod names and `<workflow_name>-` prefixes),
      2. Anonymized `cluster-scraper --hash-names` traces where pod names are independent HMAC-SHA256
         hashes (uses nanosecond `InfraEvent.timestamp` vs `earliest_admit_by_ns` and accounts for
         1-second floor truncation on Kubernetes `metav1.Time` `schedulingTime` / `firstNoticeTime`),
      3. Synthetic scenarios where initial `Pod` events sit at `t0` with no `Update` `Admitted` events.

    Returns (qualifying_pods, expired_before_effective_start_count) where each qualifying entry is
    `(namespace, pod_name, ev_epoch, payload, delete_epoch_or_none)`.
    """
    not_admitted_exact = {(ns, name) for ns, name in not_admitted_workflow_names if name}
    prefixes_by_ns: dict[str, list[str]] = defaultdict(list)
    for ns, name in not_admitted_exact:
        # Only use prefix matching for unhashed names; HMAC-SHA256 names ("h-<32-hex>") do not share prefixes
        if not (name.startswith("h-") and len(name) == 34):
            prefixes_by_ns[ns].append(f"{name}-")
    ns_prefix_tuples = {ns: tuple(p_list) for ns, p_list in prefixes_by_ns.items()}
    admit_map = earliest_admit_by_ns or {}

    qualifying: list[tuple[str, str, float, dict[str, Any], float | None]] = []
    expired_count = 0

    for (p_ns, p_name), (ev_epoch, pl) in sorted(pod_new_events.items()):
        if (p_ns, p_name) in not_admitted_exact:
            continue
        ns_prefixes = ns_prefix_tuples.get(p_ns)
        if ns_prefixes and p_name.startswith(ns_prefixes):
            continue

        sched_ep = parse_rfc3339_epoch(pl.get("schedulingTime"), strict=strict_timestamps)
        first_notice_ep = parse_rfc3339_epoch(pl.get("firstNoticeTime"), strict=strict_timestamps)
        ns_first_admit = admit_map.get(p_ns)

        if ns_first_admit is not None:
            # A NotAdmitted workflow in this namespace became Admitted during the scrape at `ns_first_admit`.
            # Pods spawned by that workflow have:
            #   - InfraEvent.timestamp (nanosecond precision) >= ns_first_admit (or at the next periodic scrape),
            #   - schedulingTime / firstNoticeTime (1s-truncated metav1.Time) >= floor(ns_first_admit).
            sec_floor_admit = float(math.floor(ns_first_admit))
            ev_cutoff = min(t0_epoch + initial_window_sec, ns_first_admit)
            is_initial = (
                ev_epoch < ev_cutoff
                or (sched_ep is not None and sched_ep < sec_floor_admit and sched_ep <= effective_start_epoch)
                or (
                    first_notice_ep is not None
                    and first_notice_ep < sec_floor_admit
                    and first_notice_ep <= effective_start_epoch
                )
            )
        else:
            # No NotAdmitted workflow in this namespace transitioned to Admitted in workflows.json
            is_initial = (
                ev_epoch <= t0_epoch + initial_window_sec
                or (sched_ep is not None and sched_ep <= effective_start_epoch)
                or (first_notice_ep is not None and first_notice_ep <= effective_start_epoch)
            )

        if not is_initial:
            continue

        del_ep = pod_delete_epochs.get((p_ns, p_name))
        if del_ep is not None and del_ep <= effective_start_epoch:
            expired_count += 1
            continue

        qualifying.append((p_ns, p_name, ev_epoch, pl, del_ep))

    return qualifying, expired_count


def obj_to_pretty_yaml(obj: Any) -> str:
    """Pretty-print a Kubernetes object (dict, list of dicts, or JSON/YAML string/bytes) as YAML."""
    if obj is None:
        return ""
    if isinstance(obj, bytes):
        obj = obj.decode("utf-8", errors="replace")
    if isinstance(obj, str):
        stripped = obj.strip()
        if not stripped:
            return ""
        try:
            parsed_json = json.loads(stripped)
            if isinstance(parsed_json, dict):
                return docs_to_pretty_yaml([parsed_json])
            if isinstance(parsed_json, list):
                return docs_to_pretty_yaml([d for d in parsed_json if isinstance(d, dict)])
        except Exception:
            pass
        try:
            import yaml  # type: ignore

            loaded = [d for d in yaml.safe_load_all(stripped) if d is not None]
            if loaded:
                return yaml.safe_dump_all(loaded, sort_keys=False)
        except Exception:
            pass
        return stripped + ("\n" if not stripped.endswith("\n") else "")
    if isinstance(obj, dict):
        return docs_to_pretty_yaml([obj])
    if isinstance(obj, list):
        dict_items = [d for d in obj if isinstance(d, dict)]
        if dict_items:
            return docs_to_pretty_yaml(dict_items)
    return str(obj) + "\n"


def _describe_k8s_obj_header(obj: Any) -> str:
    """Return a concise 'Kind namespace/name' label for a Kubernetes manifest dict."""
    if not isinstance(obj, dict):
        return "Object"
    kind = str(obj.get("kind") or "Object")
    meta = obj.get("metadata") or {}
    name = str(meta.get("name") or "")
    ns = str(meta.get("namespace") or "")
    if ns and name:
        return f"{kind} {ns}/{name}"
    if name:
        return f"{kind} {name}"
    return kind


def build_kwok_node_manifest(
    name: str,
    node_idx: int,
    cpu: int,
    mem_gi: int,
    pods: int = 110,
    role: str = "agent",
    extra_labels: dict[str, str] | None = None,
    extra_annotations: dict[str, str] | None = None,
    accel_resource: str | None = None,
    accel_count: int | None = None,
    use_legacy_cidr: bool = False,
) -> dict[str, Any]:
    """Build a simulated KWOK Node manifest with annotations for Stage/node-initialize."""
    if use_legacy_cidr and node_idx < 256:
        cidr = f"10.244.{node_idx}.0/24"
    else:
        octet2 = 100 + (node_idx // 256)
        octet3 = node_idx % 256
        cidr = f"10.{octet2}.{octet3}.0/24"

    annotations: dict[str, str] = {
        "kwok.x-k8s.io/node": "fake",
        "node.alpha.kubernetes.io/ttl": "0",
        "kueue-optimizer.gke.io/node-cpu": str(cpu),
        "kueue-optimizer.gke.io/node-memory": f"{mem_gi}Gi",
        "kueue-optimizer.gke.io/node-pods": str(pods),
    }
    if accel_resource and accel_count:
        annotations["kueue-optimizer.gke.io/node-accel-resource"] = accel_resource
        annotations["kueue-optimizer.gke.io/node-accel-count"] = str(accel_count)
    if extra_annotations:
        annotations.update(extra_annotations)

    labels: dict[str, str] = {
        "beta.kubernetes.io/arch": "amd64",
        "beta.kubernetes.io/os": "linux",
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/hostname": name,
        "kubernetes.io/os": "linux",
        "kubernetes.io/role": "agent",
        "node-role.kubernetes.io/agent": "",
        "type": "kwok",
        "kueue-optimizer.gke.io/node-role": role,
    }
    if extra_labels:
        labels.update(extra_labels)

    resources: dict[str, str] = {
        "cpu": str(cpu),
        "memory": f"{mem_gi}Gi",
        "pods": str(pods),
    }
    if accel_resource and accel_count:
        resources[accel_resource] = str(accel_count)

    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": name,
            "annotations": annotations,
            "labels": labels,
        },
        "spec": {
            "podCIDR": cidr,
            "podCIDRs": [cidr],
        },
        "status": {
            "allocatable": dict(resources),
            "capacity": dict(resources),
            "nodeInfo": {
                "architecture": "amd64",
                "kubeProxyVersion": "fake",
                "kubeletVersion": "fake",
                "operatingSystem": "linux",
            },
        },
    }


def build_namespace_manifest(
    name: str,
    part_of: str = "kueue-optimizer",
    exclude_from_kueue: bool = False,
    extra_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    labels: dict[str, str] = {
        "app.kubernetes.io/part-of": part_of,
    }
    if exclude_from_kueue:
        labels["kueue.x-k8s.io/exclude"] = "true"
        labels["kueue-optimizer.gke.io/kueue-exclude"] = "true"
    if extra_labels:
        labels.update(extra_labels)
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": labels,
        },
    }


def build_topology_manifest(
    name: str = DEFAULT_TOPOLOGY_NAME,
    levels: list[str] | None = None,
) -> dict[str, Any]:
    level_labels = levels if levels is not None else ["cube", "kubernetes.io/hostname"]
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "Topology",
        "metadata": {"name": name},
        "spec": {
            "levels": [{"nodeLabel": lbl} for lbl in level_labels],
        },
    }


def build_resource_flavor_manifest(
    name: str = DEFAULT_FLAVOR_NAME,
    node_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ResourceFlavor",
        "metadata": {"name": name},
        "spec": {
            "nodeLabels": node_labels if node_labels is not None else {"type": "kwok"},
        },
    }


def build_priority_class_manifest(name: str, value: int) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "WorkloadPriorityClass",
        "metadata": {"name": name},
        "value": int(value),
    }


def build_cohort_manifest(name: str) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "Cohort",
        "metadata": {"name": name},
    }


def build_cluster_queue_manifest(
    name: str,
    flavor_name: str,
    resources: list[dict[str, str]],
    queueing_strategy: str = "StrictFIFO",
    cohort_name: str | None = None,
    enable_preemption: bool = True,
    enable_fair_sharing: bool = True,
    fair_sharing_weight: int | str | None = None,
) -> dict[str, Any]:
    covered = [r["name"] for r in resources]
    spec: dict[str, Any] = {
        "namespaceSelector": {},
        "queueingStrategy": queueing_strategy,
        "resourceGroups": [
            {
                "coveredResources": covered,
                "flavors": [
                    {
                        "name": flavor_name,
                        "resources": resources,
                    }
                ],
            }
        ],
    }
    if cohort_name:
        spec["cohortName"] = cohort_name
    if enable_fair_sharing:
        spec["fairSharing"] = {"weight": 1 if fair_sharing_weight is None else fair_sharing_weight}
    elif fair_sharing_weight is not None:
        spec["fairSharing"] = {"weight": fair_sharing_weight}
    if enable_preemption:
        preemption_spec: dict[str, Any] = {
            "withinClusterQueue": "LowerPriority",
            "reclaimWithinCohort": "Any",
        }
        if not enable_fair_sharing:
            preemption_spec["borrowWithinCohort"] = {
                "policy": "LowerPriority",
            }
        spec["preemption"] = preemption_spec
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ClusterQueue",
        "metadata": {"name": name},
        "spec": spec,
    }


def build_local_queue_manifest(name: str, namespace: str, cluster_queue: str) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "LocalQueue",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "clusterQueue": cluster_queue,
        },
    }


def fetch_pki_from_cluster(chamber_namespace: str, target_pki_dir: Path) -> bool:
    """Attempt to read existing Secret/kwok-pki from chamber_namespace in the host cluster."""
    try:
        proc = subprocess.run(
            ["kubectl", "-n", chamber_namespace, "get", "secret", PKI_SECRET_NAME, "-o", "json"],
            capture_output=True,
            text=True,
            check=False,
            timeout=15.0,
        )
    except Exception:
        return False

    if proc.returncode != 0:
        err_lower = (proc.stderr or "").lower()
        if "invalid_rapt" in err_lower or "gke-gcloud-auth-plugin" in err_lower:
            if proc.stderr:
                sys.stderr.write(proc.stderr)
            raise RuntimeError(
                "Host cluster GKE credentials expired (invalid_rapt / gke-gcloud-auth-plugin). "
                "Please re-authenticate in your terminal:\n"
                "  gcloud auth login && gcloud auth application-default login"
            )
        return False

    if not proc.stdout:
        return False

    try:
        secret_obj = json.loads(proc.stdout)
        data = secret_obj.get("data") or {}
        if not all(k in data for k in PKI_REQUIRED_KEYS):
            return False

        target_pki_dir.mkdir(parents=True, exist_ok=True)
        for key in (*PKI_REQUIRED_KEYS, "token.csv"):
            if key in data:
                raw_bytes = base64.b64decode(data[key])
                file_path = target_pki_dir / key
                file_path.write_bytes(raw_bytes)
                file_path.chmod(0o644)
        token_path = target_pki_dir / "token.csv"
        if not token_path.exists():
            token_path.write_text(METRICS_TOKEN_CSV, encoding="utf-8")
            token_path.chmod(0o644)
        return True
    except Exception:
        return False


def generate_adhoc_pki(chamber_namespace: str, target_pki_dir: Path) -> None:
    """Generate ad-hoc CA & admin keypair, certificates, kubeconfigs, and token.csv via openssl."""
    target_pki_dir.mkdir(parents=True, exist_ok=True)
    apiserver_svc = "kwok-kube-apiserver"
    controller_svc = "kwok-controller"
    kueue_svc = "kwok-kueue"
    jobset_svc = "kwok-jobset"

    with tempfile.TemporaryDirectory(prefix="kueue-optimizer-openssl-") as tmp_str:
        tmp_dir = Path(tmp_str)
        ca_cnf = tmp_dir / "ca.cnf"
        ca_cnf.write_text(
            "[req]\n"
            "distinguished_name = req_distinguished_name\n"
            "x509_extensions = v3_ca\n"
            "[req_distinguished_name]\n"
            "[v3_ca]\n"
            "basicConstraints = critical, CA:TRUE, pathlen:0\n"
            "keyUsage = critical, digitalSignature, keyCertSign, cRLSign\n"
            "subjectKeyIdentifier = hash\n",
            encoding="utf-8",
        )

        ca_key = target_pki_dir / "ca.key"
        ca_crt = target_pki_dir / "ca.crt"
        subprocess.run(
            ["openssl", "genrsa", "-out", str(ca_key), "2048"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-new",
                "-nodes",
                "-key",
                str(ca_key),
                "-subj",
                "/CN=kwok-ca",
                "-days",
                "3650",
                "-config",
                str(ca_cnf),
                "-extensions",
                "v3_ca",
                "-out",
                str(ca_crt),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        admin_cnf = tmp_dir / "admin.cnf"
        admin_cnf.write_text(
            f"""[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = kubernetes
DNS.2 = kubernetes.default
DNS.3 = kubernetes.default.svc
DNS.4 = kubernetes.default.svc.cluster.local
DNS.5 = {apiserver_svc}
DNS.6 = {apiserver_svc}.{chamber_namespace}
DNS.7 = {apiserver_svc}.{chamber_namespace}.svc
DNS.8 = {apiserver_svc}.{chamber_namespace}.svc.cluster.local
DNS.9 = {controller_svc}
DNS.10 = {controller_svc}.{chamber_namespace}
DNS.11 = {controller_svc}.{chamber_namespace}.svc
DNS.12 = {controller_svc}.{chamber_namespace}.svc.cluster.local
DNS.13 = {kueue_svc}
DNS.14 = {kueue_svc}.{chamber_namespace}
DNS.15 = {kueue_svc}.{chamber_namespace}.svc
DNS.16 = {kueue_svc}.{chamber_namespace}.svc.cluster.local
DNS.17 = kueue-webhook-service
DNS.18 = kueue-webhook-service.kueue-system
DNS.19 = kueue-webhook-service.kueue-system.svc
DNS.20 = kueue-webhook-service.kueue-system.svc.cluster.local
DNS.21 = kueue-visibility-server
DNS.22 = kueue-visibility-server.kueue-system
DNS.23 = kueue-visibility-server.kueue-system.svc
DNS.24 = kueue-visibility-server.kueue-system.svc.cluster.local
DNS.25 = kueue-controller-manager-metrics-service
DNS.26 = kueue-controller-manager-metrics-service.kueue-system
DNS.27 = kueue-controller-manager-metrics-service.kueue-system.svc
DNS.28 = kueue-controller-manager-metrics-service.kueue-system.svc.cluster.local
DNS.29 = {jobset_svc}
DNS.30 = {jobset_svc}.{chamber_namespace}
DNS.31 = {jobset_svc}.{chamber_namespace}.svc
DNS.32 = {jobset_svc}.{chamber_namespace}.svc.cluster.local
DNS.33 = jobset-webhook-service
DNS.34 = jobset-webhook-service.jobset-system
DNS.35 = jobset-webhook-service.jobset-system.svc
DNS.36 = jobset-webhook-service.jobset-system.svc.cluster.local
DNS.37 = jobset-controller-manager-metrics-service
DNS.38 = jobset-controller-manager-metrics-service.jobset-system
DNS.39 = jobset-controller-manager-metrics-service.jobset-system.svc
DNS.40 = jobset-controller-manager-metrics-service.jobset-system.svc.cluster.local
DNS.41 = localhost
IP.1 = 127.0.0.1
IP.2 = 10.0.0.1
""",
            encoding="utf-8",
        )

        admin_key = target_pki_dir / "admin.key"
        admin_csr = tmp_dir / "admin.csr"
        admin_crt = target_pki_dir / "admin.crt"
        ca_srl = tmp_dir / "ca.srl"

        subprocess.run(
            ["openssl", "genrsa", "-out", str(admin_key), "2048"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "openssl",
                "req",
                "-new",
                "-key",
                str(admin_key),
                "-subj",
                "/CN=kwok-admin/O=system:masters",
                "-out",
                str(admin_csr),
                "-config",
                str(admin_cnf),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                str(admin_csr),
                "-CA",
                str(ca_crt),
                "-CAkey",
                str(ca_key),
                "-CAcreateserial",
                "-CAserial",
                str(ca_srl),
                "-out",
                str(admin_crt),
                "-days",
                "3650",
                "-extensions",
                "v3_req",
                "-extfile",
                str(admin_cnf),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    ca_b64 = base64.b64encode(ca_crt.read_bytes()).decode("ascii")
    cert_b64 = base64.b64encode(admin_crt.read_bytes()).decode("ascii")
    key_b64 = base64.b64encode(admin_key.read_bytes()).decode("ascii")

    def _build_kc(server_url: str) -> str:
        ctx_name = f"kwok-{chamber_namespace}"
        user_name = f"kwok-{chamber_namespace}-admin"
        return (
            "apiVersion: v1\n"
            "kind: Config\n"
            "clusters:\n"
            "- cluster:\n"
            f"    certificate-authority-data: {ca_b64}\n"
            f"    server: {server_url}\n"
            f"  name: {ctx_name}\n"
            "contexts:\n"
            "- context:\n"
            f"    cluster: {ctx_name}\n"
            f"    user: {user_name}\n"
            f"  name: {ctx_name}\n"
            f"current-context: {ctx_name}\n"
            "preferences: {}\n"
            "users:\n"
            f"- name: {user_name}\n"
            "  user:\n"
            f"    client-certificate-data: {cert_b64}\n"
            f"    client-key-data: {key_b64}\n"
        )

    (target_pki_dir / "kubeconfig").write_text(
        _build_kc(f"https://{apiserver_svc}:6443"),
        encoding="utf-8",
    )
    (target_pki_dir / "external-kubeconfig").write_text(
        _build_kc(f"https://{apiserver_svc}.{chamber_namespace}.svc:6443"),
        encoding="utf-8",
    )
    (target_pki_dir / "token.csv").write_text(METRICS_TOKEN_CSV, encoding="utf-8")

    for fname in (*PKI_REQUIRED_KEYS, "token.csv"):
        (target_pki_dir / fname).chmod(0o644)


def ensure_pki_dir(
    chamber_namespace: str,
    target_pki_dir: Path,
    prefer_cluster: bool = True,
) -> tuple[Path, str]:
    """Ensure PKI files exist in target_pki_dir by picking from cluster or generating ad-hoc."""
    if all((target_pki_dir / k).exists() for k in PKI_REQUIRED_KEYS):
        return target_pki_dir, "cached"
    if prefer_cluster and fetch_pki_from_cluster(chamber_namespace, target_pki_dir):
        return target_pki_dir, "cluster"
    generate_adhoc_pki(chamber_namespace, target_pki_dir)
    return target_pki_dir, "adhoc"


def inject_ca_bundle_into_manifests(manifests_dir: Path, ca_crt_bytes: bytes) -> None:
    """Inject base64-encoded CA certificate into webhook/CRD caBundle fields in manifests_dir."""
    ca_b64 = base64.b64encode(ca_crt_bytes).decode("ascii")
    for fname in CA_BUNDLE_MANIFEST_FILES:
        fpath = manifests_dir / fname
        if not fpath.exists():
            continue
        text = fpath.read_text(encoding="utf-8")
        text = text.replace(CA_BUNDLE_PLACEHOLDER, ca_b64)
        text = re.sub(r"(caBundle:\s*)[A-Za-z0-9+/=_]+", rf"\g<1>{ca_b64}", text)
        text = re.sub(r'("caBundle":\s*")[A-Za-z0-9+/=_]+(")', rf"\g<1>{ca_b64}\g<2>", text)
        fpath.write_text(text, encoding="utf-8")


class ChamberManager:
    """Manages connection to the kueue-optimizer chamber in the host cluster and its KWOK apiserver."""

    def __init__(
        self,
        chamber_namespace: str,
        workload_namespace: str = "default",
        apiserver_port: int = DEFAULT_APISERVER_PORT,
        prometheus_port: int | None = None,
        scrape_interval: str = DEFAULT_SCRAPE_INTERVAL,
        kubeconfig_out: Path | str | None = None,
        registry_port: int | None = None,
        registry_memory_limit: str = DEFAULT_REGISTRY_MEMORY_LIMIT,
        kueue_image: str | None = None,
        kueue_build_dir: Path | str | None = None,
        enable_fair_sharing: bool = True,
    ) -> None:
        self.chamber_namespace = validate_dns_label(chamber_namespace, "chamber namespace")
        self.workload_namespace = validate_dns_label(workload_namespace, "workload namespace")
        self.apiserver_port = apiserver_port
        self.prometheus_port = compute_prometheus_port(apiserver_port, prometheus_port)
        self._explicit_registry_port = registry_port
        self.registry_port = compute_registry_port(apiserver_port, registry_port)
        self.registry_memory_limit = (registry_memory_limit or DEFAULT_REGISTRY_MEMORY_LIMIT).strip()
        self.kueue_image = (kueue_image or "").strip() or None
        self.kueue_build_dir = Path(kueue_build_dir).resolve() if kueue_build_dir else None
        self._resolved_kueue_image: str | None = None
        self.enable_fair_sharing = bool(enable_fair_sharing)
        self.scrape_interval = normalize_scrape_interval(scrape_interval)
        self.kubeconfig_path = resolve_kubeconfig_path(self.chamber_namespace, kubeconfig_out)
        self._created_namespace_in_this_run: bool = False

        self._apiserver_pf_proc: subprocess.Popen[bytes] | None = None
        self._prom_pf_proc: subprocess.Popen[bytes] | None = None
        self._registry_pf_proc: subprocess.Popen[bytes] | None = None
        self._pf_lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = None
        self._tmp_deploy_dir: Path | None = None
        self._tmp_pki_dir: Path | None = None
        self._pki_source: str | None = None
        self._ssl_ctx: ssl.SSLContext | None = None
        self._tls_local = threading.local()
        atexit.register(self.close)

    @property
    def created_namespace_in_this_run(self) -> bool:
        """True only when this ChamberManager instance created a previously absent namespace during apply."""
        return self._created_namespace_in_this_run

    def _ensure_namespace_safe_for_apply(self, *, record_creation: bool = True) -> bool:
        """Verify target host namespace before server-side apply.

        Returns True if the namespace already existed (and already carries the chamber ownership label),
        False if the namespace does not yet exist (recording creation when record_creation=True).
        Raises RuntimeError if the namespace already exists without `kueue-optimizer.gke.io/chamber=true`.
        """
        get_proc = self.host_kubectl(
            ["get", "namespace", "-o", "json", "--", self.chamber_namespace],
            check=False,
            timeout=30.0,
        )
        if get_proc.returncode != 0:
            err_msg = (get_proc.stderr or "").strip()
            if "notfound" in err_msg.lower() or "not found" in err_msg.lower():
                if record_creation:
                    self._created_namespace_in_this_run = True
                return False
            raise RuntimeError(
                f"Failed to inspect namespace '{self.chamber_namespace}' before apply "
                f"(exit {get_proc.returncode}): {err_msg}"
            )

        try:
            ns_obj = json.loads(get_proc.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Failed to parse namespace '{self.chamber_namespace}' JSON before apply: {exc}"
            ) from exc

        labels = ((ns_obj.get("metadata") or {}).get("labels")) or {}
        if labels.get(CHAMBER_NAMESPACE_LABEL_KEY) != CHAMBER_NAMESPACE_LABEL_VALUE:
            raise RuntimeError(
                f"Refusing to deploy chamber into existing namespace '{self.chamber_namespace}': "
                f"missing chamber ownership label ('{CHAMBER_NAMESPACE_LABEL_KEY}={CHAMBER_NAMESPACE_LABEL_VALUE}')."
            )
        return True

    def detect_live_fair_sharing_enabled(self) -> bool | None:
        """Read ConfigMap/kueue-manager-config in the chamber namespace to detect whether fairSharing is active."""
        proc = self.host_kubectl(
            ["-n", self.chamber_namespace, "get", "configmap", "kueue-manager-config", "-o", "json"],
            check=False,
            timeout=15.0,
        )
        if proc.returncode != 0 or not proc.stdout:
            return None
        try:
            cm_obj = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return None
        cfg_yaml = str(((cm_obj.get("data") or {}).get("controller_manager_config.yaml")) or "")
        if not cfg_yaml.strip():
            return None
        return bool(re.search(r"^fairSharing\s*:", cfg_yaml, flags=re.MULTILINE))

    def ensure_pki(self, prefer_cluster: bool = True) -> Path:
        """Obtain PKI from Secret/kwok-pki in the cluster if present, or generate ad-hoc."""
        if self._tmp_pki_dir is None or not self._tmp_pki_dir.exists():
            self._tmp_pki_dir = Path(tempfile.mkdtemp(prefix="kueue-optimizer-pki-"))
            self._ssl_ctx = None
        _, source = ensure_pki_dir(
            chamber_namespace=self.chamber_namespace,
            target_pki_dir=self._tmp_pki_dir,
            prefer_cluster=prefer_cluster,
        )
        if self._pki_source is None:
            self._pki_source = source
        return self._tmp_pki_dir

    def _get_ssl_ctx(self) -> ssl.SSLContext:
        if self._ssl_ctx is None:
            pki_dir = self.ensure_pki(prefer_cluster=True)
            ctx = ssl.create_default_context(cafile=str(pki_dir / "ca.crt"))
            if hasattr(ssl, "VERIFY_X509_STRICT"):
                ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
            ctx.load_cert_chain(
                certfile=str(pki_dir / "admin.crt"),
                keyfile=str(pki_dir / "admin.key"),
            )
            self._ssl_ctx = ctx
        return self._ssl_ctx

    def _get_https_conn(self) -> http.client.HTTPSConnection:
        conn: http.client.HTTPSConnection | None = getattr(self._tls_local, "conn", None)
        if conn is None:
            conn = http.client.HTTPSConnection(
                "127.0.0.1",
                self.apiserver_port,
                context=self._get_ssl_ctx(),
                timeout=30.0,
            )
            self._tls_local.conn = conn
        return conn

    def _probe_apiserver_ready(self, timeout: float = 2.0) -> bool:
        """Return True if the local port-forwarded KWOK kube-apiserver responds 200 on /readyz."""
        conn: http.client.HTTPSConnection | None = None
        try:
            conn = http.client.HTTPSConnection(
                "127.0.0.1",
                self.apiserver_port,
                context=self._get_ssl_ctx(),
                timeout=timeout,
            )
            conn.request("GET", "/readyz")
            resp = conn.getresponse()
            resp.read()
            return resp.status == 200
        except Exception:
            return False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _pkill_chamber_port_forward(self, service: str, port_mapping: str | None = None) -> None:
        """Terminate stale kubectl port-forward processes strictly matching this chamber's exact namespace."""
        ns = re.escape(self.chamber_namespace)
        svc = re.escape(service)
        pattern = rf"(^|/)kubectl -n {ns} port-forward svc/{svc}"
        if port_mapping:
            pattern += rf" {re.escape(port_mapping)}$"
        else:
            pattern += r"( |$)"
        try:
            subprocess.run(
                ["pkill", "-f", pattern],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            pass

    def _start_or_restart_apiserver_pf_locked(self, timeout_sec: float = 60.0) -> None:
        """Start (or restart) kubectl port-forward to svc/kwok-kube-apiserver. Caller must hold _pf_lock."""
        if self._probe_apiserver_ready(timeout=1.5):
            return

        if self._apiserver_pf_proc is not None:
            try:
                self._apiserver_pf_proc.terminate()
                self._apiserver_pf_proc.wait(timeout=2)
            except Exception:
                try:
                    self._apiserver_pf_proc.kill()
                except Exception:
                    pass
            self._apiserver_pf_proc = None

        self._pkill_chamber_port_forward("kwok-kube-apiserver", f"{self.apiserver_port}:6443")

        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if self._apiserver_pf_proc is None or self._apiserver_pf_proc.poll() is not None:
                self._apiserver_pf_proc = subprocess.Popen(
                    [
                        "kubectl",
                        "-n",
                        self.chamber_namespace,
                        "port-forward",
                        "svc/kwok-kube-apiserver",
                        f"{self.apiserver_port}:6443",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            for _ in range(10):
                if self._probe_apiserver_ready(timeout=1.5):
                    return
                if self._apiserver_pf_proc.poll() is not None:
                    break
                time.sleep(0.3)
            time.sleep(0.5)

        raise RuntimeError(
            f"Timed out waiting to connect to KWOK kube-apiserver in namespace '{self.chamber_namespace}' "
            f"on localhost:{self.apiserver_port}"
        )

    def api_request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes]:
        """Perform a direct keep-alive HTTPS request against the KWOK kube-apiserver with auto-reconnect."""
        hdrs = {"Accept": "application/json", "Connection": "keep-alive"}
        if headers:
            hdrs.update(headers)
        max_attempts = 6
        for attempt in range(max_attempts):
            conn = self._get_https_conn()
            try:
                conn.request(method, path, body=body, headers=hdrs)
                resp = conn.getresponse()
                data = resp.read()
                if resp.status in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
                    time.sleep(min(2.0, 0.25 * (attempt + 1)))
                    continue
                return resp.status, data
            except Exception as exc:
                try:
                    conn.close()
                except Exception:
                    pass
                self._tls_local.conn = None
                if attempt == max_attempts - 1:
                    if body:
                        pretty_body = obj_to_pretty_yaml(body).rstrip()
                        raise RuntimeError(
                            f"API request {method} {path} failed: {exc}\n"
                            f"\n--- Object YAML that caused the issue ({method} {path}) ---\n"
                            f"{pretty_body}\n---"
                        ) from exc
                    raise
                with self._pf_lock:
                    try:
                        self._start_or_restart_apiserver_pf_locked(timeout_sec=30.0)
                    except Exception:
                        if attempt == max_attempts - 1:
                            if body:
                                pretty_body = obj_to_pretty_yaml(body).rstrip()
                                raise RuntimeError(
                                    f"API request {method} {path} failed: {exc}\n"
                                    f"\n--- Object YAML that caused the issue ({method} {path}) ---\n"
                                    f"{pretty_body}\n---"
                                ) from exc
                            raise
                time.sleep(min(1.0, 0.15 * (attempt + 1)))
        raise RuntimeError(f"Failed API request: {method} {path}")

    @staticmethod
    def _resource_ssa_path(doc: dict[str, Any], field_manager: str = "kueue-optimizer-runner") -> str:
        api_version = str(doc.get("apiVersion", "v1"))
        kind = str(doc.get("kind", ""))
        meta = doc.get("metadata", {}) or {}
        name = str(meta.get("name", ""))
        ns = meta.get("namespace")

        plural_map = {
            "Node": "nodes",
            "Namespace": "namespaces",
            "Pod": "pods",
            "Job": "jobs",
            "Deployment": "deployments",
            "LeaderWorkerSet": "leaderworkersets",
            "RayJob": "rayjobs",
            "RayCluster": "rayclusters",
            "Topology": "topologies",
            "ResourceFlavor": "resourceflavors",
            "WorkloadPriorityClass": "workloadpriorityclasses",
            "PriorityClass": "priorityclasses",
            "RuntimeClass": "runtimeclasses",
            "Cohort": "cohorts",
            "ClusterQueue": "clusterqueues",
            "LocalQueue": "localqueues",
            "Workload": "workloads",
            "JobSet": "jobsets",
            "Stage": "stages",
            "MutatingWebhookConfiguration": "mutatingwebhookconfigurations",
            "ValidatingWebhookConfiguration": "validatingwebhookconfigurations",
        }
        kind_lower = kind.lower()
        default_plural = f"{kind_lower}es" if kind_lower.endswith("s") else f"{kind_lower}s"
        plural = plural_map.get(kind, default_plural)
        prefix = "/api/v1" if api_version == "v1" else f"/apis/{api_version}"
        cluster_scoped = {
            "Node",
            "Namespace",
            "Topology",
            "ResourceFlavor",
            "WorkloadPriorityClass",
            "PriorityClass",
            "RuntimeClass",
            "Cohort",
            "ClusterQueue",
            "Stage",
            "MutatingWebhookConfiguration",
            "ValidatingWebhookConfiguration",
        }
        if ns and kind not in cluster_scoped:
            return f"{prefix}/namespaces/{ns}/{plural}/{name}?fieldManager={field_manager}&force=true"
        return f"{prefix}/{plural}/{name}?fieldManager={field_manager}&force=true"

    def _fetch_existing_object_for_error(self, path: str) -> dict[str, Any] | list[dict[str, Any]] | None:
        """Best-effort GET of the target resource (or collection) when a create/update/delete fails."""
        clean_path = path.split("?", 1)[0]
        if clean_path.endswith("/status"):
            clean_path = clean_path[: -len("/status")]
        try:
            st, raw = self.api_request("GET", clean_path)
            if 200 <= st < 300 and raw:
                parsed = json.loads(raw.decode("utf-8", errors="replace"))
                if isinstance(parsed, dict) and parsed.get("kind") != "Status":
                    if "items" in parsed and isinstance(parsed.get("items"), list):
                        return [it for it in parsed["items"] if isinstance(it, dict)]
                    return parsed
        except Exception:
            pass
        return None

    def _format_k8s_api_error(
        self,
        operation: str,
        path: str,
        status: int,
        data: bytes,
        obj: Any = None,
    ) -> str:
        """Format a detailed error message including the full API response and pretty-printed YAML of the object."""
        raw_err = data.decode("utf-8", errors="replace").strip()
        lines: list[str] = [f"{operation} {path} failed with HTTP {status}: {raw_err}"]

        try:
            err_json = json.loads(raw_err)
            if isinstance(err_json, dict) and err_json.get("kind") == "Status":
                msg = err_json.get("message")
                reason = err_json.get("reason")
                details = err_json.get("details") or {}
                causes = details.get("causes") or []
                if msg or reason:
                    lines.append(f"  Reason : {reason or 'Unknown'}")
                    lines.append(f"  Message: {msg or ''}")
                for c in causes:
                    if isinstance(c, dict):
                        c_field = c.get("field") or ""
                        c_reason = c.get("reason") or ""
                        c_msg = c.get("message") or ""
                        field_prefix = f"[{c_field}] " if c_field else ""
                        lines.append(f"  Cause  : {field_prefix}{c_reason}: {c_msg}")
        except Exception:
            pass

        server_obj = self._fetch_existing_object_for_error(path)

        if obj is not None:
            hdr = _describe_k8s_obj_header(obj)
            yaml_str = obj_to_pretty_yaml(obj).rstrip()
            lines.append(f"\n--- Object YAML that caused the issue ({hdr}) ---")
            lines.append(yaml_str)
            lines.append("---")
            if isinstance(server_obj, dict):
                srv_hdr = _describe_k8s_obj_header(server_obj)
                srv_yaml = obj_to_pretty_yaml(server_obj).rstrip()
                lines.append(f"--- Existing Object YAML on API Server ({srv_hdr}) ---")
                lines.append(srv_yaml)
                lines.append("---")
        elif server_obj is not None:
            hdr = _describe_k8s_obj_header(server_obj)
            yaml_str = obj_to_pretty_yaml(server_obj).rstrip()
            lines.append(f"\n--- Object YAML that caused the issue ({hdr}) ---")
            lines.append(yaml_str)
            lines.append("---")

        return "\n".join(lines)

    def ssa_apply_doc(self, doc: dict[str, Any]) -> None:
        """Server-Side Apply a single Kubernetes manifest directly over keep-alive HTTPS."""
        path = self._resource_ssa_path(doc)
        payload = json.dumps(doc).encode("utf-8")
        status, data = self.api_request(
            "PATCH",
            path,
            body=payload,
            headers={"Content-Type": "application/apply-patch+yaml"},
        )
        if status < 200 or status >= 300:
            raise RuntimeError(
                self._format_k8s_api_error("SSA PATCH", path, status, data, obj=doc)
            )

    def ssa_apply_node_status(self, node_doc: dict[str, Any]) -> None:
        """Ensure Node .status.allocatable and .status.capacity match node_doc."""
        name = node_doc["metadata"]["name"]
        status_doc = {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": name},
            "status": {
                "allocatable": node_doc["status"]["allocatable"],
                "capacity": node_doc["status"]["capacity"],
            },
        }
        path = f"/api/v1/nodes/{name}/status?fieldManager=kueue-optimizer-runner&force=true"
        status, data = self.api_request(
            "PATCH",
            path,
            body=json.dumps(status_doc).encode("utf-8"),
            headers={"Content-Type": "application/apply-patch+yaml"},
        )
        if status < 200 or status >= 300:
            raise RuntimeError(
                self._format_k8s_api_error("SSA PATCH", path, status, data, obj=node_doc)
            )

    def api_delete(
        self,
        path: str,
        obj: Any = None,
        ignore_not_found: bool = True,
    ) -> tuple[int, bytes]:
        """Perform a DELETE against the KWOK kube-apiserver and pretty-print the object YAML on failure."""
        status, data = self.api_request("DELETE", path)
        if ignore_not_found and status == 404:
            return status, data
        if status < 200 or status >= 300:
            raise RuntimeError(
                self._format_k8s_api_error("DELETE", path, status, data, obj=obj)
            )
        return status, data

    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=12)
        return self._executor

    def apply_manifests_parallel(
        self,
        docs: list[dict[str, Any]],
        max_workers: int = 12,
    ) -> None:
        """Apply manifests concurrently over persistent keep-alive TLS sockets."""
        if not docs:
            return
        if len(docs) == 1:
            self.ssa_apply_doc(docs[0])
            return

        pool = self._get_executor()
        futures = [pool.submit(self.ssa_apply_doc, d) for d in docs]
        for fut in as_completed(futures):
            fut.result()

    def close(self) -> None:
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self._executor = None
        for proc in (self._apiserver_pf_proc, self._prom_pf_proc, self._registry_pf_proc):
            if proc is not None and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        self._apiserver_pf_proc = None
        self._prom_pf_proc = None
        self._registry_pf_proc = None
        for tmp_d in (self._tmp_deploy_dir, self._tmp_pki_dir):
            if tmp_d and tmp_d.exists():
                shutil.rmtree(tmp_d, ignore_errors=True)
        self._tmp_deploy_dir = None
        self._tmp_pki_dir = None
        self._ssl_ctx = None

    @staticmethod
    def _run_kubectl(
        cmd: list[str],
        input_text: str | None = None,
        check: bool = True,
        timeout: float = 300.0,
        stream_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run(
            cmd,
            input=input_text,
            capture_output=not stream_output,
            text=True,
            check=False,
            timeout=timeout,
        )
        if proc.returncode != 0:
            is_mutating = any(
                verb in cmd for verb in ("apply", "create", "patch", "replace", "delete")
            )
            if check or is_mutating:
                if not stream_output:
                    if proc.stdout:
                        sys.stderr.write(proc.stdout)
                    if proc.stderr:
                        sys.stderr.write(proc.stderr)
                    sys.stderr.flush()
                    err_lower = (proc.stderr or "").lower()
                    if "invalid_rapt" in err_lower or "gke-gcloud-auth-plugin" in err_lower:
                        print(
                            "\n==> ERROR: Host cluster GKE credentials expired (invalid_rapt / gke-gcloud-auth-plugin).\n"
                            "    Please re-authenticate in your terminal:\n"
                            "      gcloud auth login && gcloud auth application-default login",
                            file=sys.stderr,
                            flush=True,
                        )
                if input_text and is_mutating:
                    pretty_in = obj_to_pretty_yaml(input_text).rstrip()
                    print(
                        f"\n--- Object YAML that caused the issue ({' '.join(cmd)}) ---\n"
                        f"{pretty_in}\n---",
                        file=sys.stderr,
                        flush=True,
                    )
            if check:
                raise subprocess.CalledProcessError(proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr)
        return proc

    def kwok_kubectl(
        self,
        args: list[str],
        input_text: str | None = None,
        check: bool = True,
        timeout: float = 120.0,
        stream_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        cmd = ["kubectl", f"--kubeconfig={self.kubeconfig_path}", *args]
        return self._run_kubectl(
            cmd,
            input_text=input_text,
            check=check,
            timeout=timeout,
            stream_output=stream_output,
        )

    def host_kubectl(
        self,
        args: list[str],
        input_text: str | None = None,
        check: bool = True,
        timeout: float = 300.0,
        stream_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        cmd = ["kubectl", *args]
        return self._run_kubectl(
            cmd,
            input_text=input_text,
            check=check,
            timeout=timeout,
            stream_output=stream_output,
        )

    def _resolve_cluster_registry_port(self, check_cluster: bool = True) -> int:
        """Resolve per-chamber NodePort, reusing existing Service/kwok-registry port or avoiding collisions."""
        if not check_cluster:
            return self.registry_port
        try:
            proc = self.host_kubectl(["get", "svc", "-A", "-o", "json"], check=False, timeout=15.0)
            if proc.returncode != 0 or not proc.stdout:
                return self.registry_port
            data = json.loads(proc.stdout)
        except Exception:
            return self.registry_port

        other_used_nodeports: dict[int, str] = {}
        for item in data.get("items") or []:
            meta = item.get("metadata") or {}
            ns = str(meta.get("namespace") or "")
            name = str(meta.get("name") or "")
            for p in (item.get("spec") or {}).get("ports") or []:
                np = p.get("nodePort")
                if not np:
                    continue
                np_int = int(np)
                if ns == self.chamber_namespace and name == "kwok-registry":
                    self.registry_port = np_int
                    return np_int
                if ns != self.chamber_namespace:
                    other_used_nodeports[np_int] = f"{ns}/{name}"

        if self.registry_port not in other_used_nodeports:
            return self.registry_port

        if self._explicit_registry_port is not None:
            holder = other_used_nodeports[self.registry_port]
            raise RuntimeError(
                f"Requested --registry-port {self.registry_port} is already allocated by Service '{holder}'. "
                f"Choose an unused NodePort in {NODEPORT_MIN}..{NODEPORT_MAX} or tear down that chamber first."
            )

        prev_port = self.registry_port
        holder = other_used_nodeports[prev_port]
        for offset in range(1, NODEPORT_MAX - NODEPORT_MIN + 1):
            cand = NODEPORT_MIN + ((prev_port - NODEPORT_MIN + offset) % (NODEPORT_MAX - NODEPORT_MIN + 1))
            if cand not in other_used_nodeports:
                self.registry_port = cand
                print(
                    f"    Note: NodePort {prev_port} is in use by '{holder}'; "
                    f"selected free per-chamber registry NodePort {cand} for '{self.chamber_namespace}'.",
                    flush=True,
                )
                return cand
        raise RuntimeError(f"No free Kubernetes NodePorts available in {NODEPORT_MIN}..{NODEPORT_MAX}.")

    @staticmethod
    def _match_yaml_section_field(yaml_text: str, section_key: str, field_key: str) -> str | None:
        """Return the scalar value of `field_key` inside an indented YAML mapping `section_key:` (or None)."""
        pattern = (
            rf"^(?P<indent>[ \t]*)(?:-[ \t]+)?{re.escape(section_key)}:[ \t]*(?:#[^\n]*)?\n"
            rf"(?:(?P=indent)[ \t]+(?![- \t]*{re.escape(field_key)}:[ \t]*)[A-Za-z0-9_./-]+:[ \t]*[^\n]*\n)*"
            rf"(?P=indent)[ \t]+{re.escape(field_key)}:[ \t]*(?P<val>['\"]?[0-9A-Za-z._+-]+['\"]?)"
        )
        m = re.search(pattern, yaml_text, flags=re.MULTILINE)
        return m.group("val") if m else None

    @staticmethod
    def _replace_yaml_section_field(yaml_text: str, section_key: str, field_key: str, new_value: str) -> str:
        """Replace the scalar value of `field_key` inside any `section_key:` YAML mapping without depending on its prior value."""
        pattern = (
            rf"(^(?P<indent>[ \t]*)(?:-[ \t]+)?{re.escape(section_key)}:[ \t]*(?:#[^\n]*)?\n"
            rf"(?:(?P=indent)[ \t]+(?![- \t]*{re.escape(field_key)}:[ \t]*)[A-Za-z0-9_./-]+:[ \t]*[^\n]*\n)*"
            rf"(?P=indent)[ \t]+{re.escape(field_key)}:[ \t]*)['\"]?[0-9A-Za-z._+-]+['\"]?"
        )
        return re.sub(pattern, rf"\g<1>{new_value}", yaml_text, flags=re.MULTILINE)

    @classmethod
    def _patch_registry_manifest(cls, base_dst: Path, node_port: int, memory_limit: str) -> None:
        reg_yaml = base_dst / "registry.yaml"
        if not reg_yaml.exists():
            return
        reg_text = reg_yaml.read_text(encoding="utf-8")
        reg_text = re.sub(
            r"^([ \t]*nodePort:[ \t]*)\d+",
            rf"\g<1>{node_port}",
            reg_text,
            flags=re.MULTILINE,
        )
        mem_lim = (memory_limit or "").strip()
        if mem_lim:
            reg_text = cls._replace_yaml_section_field(reg_text, "emptyDir", "sizeLimit", mem_lim)
            reg_text = cls._replace_yaml_section_field(reg_text, "limits", "memory", mem_lim)
            # Ensure requests.memory never exceeds limits.memory when a smaller memory limit is requested
            cur_req = cls._match_yaml_section_field(reg_text, "requests", "memory")
            if cur_req is not None:
                req_gib = parse_k8s_quantity(cur_req, "memory")
                lim_gib = parse_k8s_quantity(mem_lim, "memory")
                if lim_gib > 0 and req_gib > lim_gib:
                    reg_text = cls._replace_yaml_section_field(reg_text, "requests", "memory", mem_lim)
        reg_yaml.write_text(reg_text, encoding="utf-8")

    @staticmethod
    def _patch_kueue_fair_sharing_config(base_dst: Path, enable_fair_sharing: bool) -> None:
        """Enable or disable Preemption-Based Fair Sharing in controller_manager_config.yaml."""
        cfg_yaml = base_dst / "config" / "controller_manager_config.yaml"
        if not cfg_yaml.exists():
            return
        cfg_text = cfg_yaml.read_text(encoding="utf-8")
        # Strip any existing top-level fairSharing block cleanly
        cfg_text = re.sub(
            r"^fairSharing:[ \t]*(?:#[^\n]*)?\n(?:[ \t]+[^\n]*\n|-+[ \t]*[^\n]*\n)*",
            "",
            cfg_text,
            flags=re.MULTILINE,
        )
        if enable_fair_sharing:
            fs_lines = [
                "fairSharing:",
                "  preemptionStrategies:",
                *[f"  - {s}" for s in DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES],
            ]
            fs_block = "\n".join(fs_lines) + "\n"
            if re.search(r"^internalCertManagement:", cfg_text, flags=re.MULTILINE):
                cfg_text = re.sub(
                    r"^internalCertManagement:",
                    f"{fs_block}internalCertManagement:",
                    cfg_text,
                    count=1,
                    flags=re.MULTILINE,
                )
            else:
                cfg_text = cfg_text.rstrip("\n") + "\n" + fs_block
        cfg_yaml.write_text(cfg_text, encoding="utf-8")

    @staticmethod
    def _patch_kustomize_kueue_image(base_dst: Path, image_ref: str) -> None:
        ref = image_ref.strip()
        if not ref:
            return
        kueue_yaml = base_dst / "kueue.yaml"
        if kueue_yaml.exists():
            k_text = kueue_yaml.read_text(encoding="utf-8")
            k_text = re.sub(
                r"(-\s+name:\s+manager\s*\n\s+image:\s*)\S+(\s*\n\s+imagePullPolicy:\s*)\S+",
                rf"\g<1>{ref}\g<2>Always",
                k_text,
                count=1,
            )
            kueue_yaml.write_text(k_text, encoding="utf-8")

        kust_yaml = base_dst / "kustomization.yaml"
        if kust_yaml.exists():
            kust_text = kust_yaml.read_text(encoding="utf-8")
            # Remove the default kueue images override block so the explicit image in kueue.yaml wins verbatim
            kust_text = re.sub(
                r"-\s+name:\s+us-central1-docker\.pkg\.dev/k8s-staging-images/kueue/kueue\s*\n"
                r"\s+newName:\s+\S+\s*\n"
                r"\s+newTag:\s+\S+\s*\n?",
                "",
                kust_text,
            )
            kust_yaml.write_text(kust_text, encoding="utf-8")

    def prepare_tmp_deploy_dir(
        self,
        initial_nodes: list[dict[str, Any]] | None = None,
        include_overlays: bool = False,
        prefer_cluster_pki: bool = True,
        check_cluster_ports: bool = False,
    ) -> Path:
        if check_cluster_ports:
            self._resolve_cluster_registry_port(check_cluster=True)

        if self._tmp_deploy_dir is None or not self._tmp_deploy_dir.exists():
            self._tmp_deploy_dir = Path(tempfile.mkdtemp(prefix="kueue-optimizer-chamber-deploy-"))
        base_dst = self._tmp_deploy_dir / "base"
        if base_dst.exists():
            shutil.rmtree(base_dst)
        shutil.copytree(DEPLOY_DIR / "base", base_dst)

        # Populate PKI (from cluster Secret/kwok-pki if running, or generated ad-hoc) and inject caBundle
        pki_src = self.ensure_pki(prefer_cluster=prefer_cluster_pki)
        pki_dst = base_dst / "pki"
        if pki_dst.exists():
            shutil.rmtree(pki_dst)
        shutil.copytree(pki_src, pki_dst)
        inject_ca_bundle_into_manifests(base_dst / "manifests", (pki_dst / "ca.crt").read_bytes())

        # Keep bootstrap kwok-nodes.yaml minimal when node count is large so ConfigMap/kwok-bootstrap
        # stays well under the 1MiB ConfigMap limit; nodes are synced via sync_kwok_nodes().
        nodes_file = base_dst / "manifests" / "kwok-nodes.yaml"
        if initial_nodes is not None:
            if len(initial_nodes) <= 50 and len(initial_nodes) > 0:
                nodes_file.write_text(docs_to_pretty_yaml(initial_nodes), encoding="utf-8")
            else:
                nodes_file.write_text("# Managed by ChamberManager\n", encoding="utf-8")

        prom_cfg = base_dst / "config" / "prometheus.yaml"
        prom_text = prom_cfg.read_text(encoding="utf-8")
        prom_text = re.sub(
            r"^(\s*(?:scrape|evaluation)_interval:).*",
            rf"\1 {self.scrape_interval}",
            prom_text,
            flags=re.MULTILINE,
        )
        prom_cfg.write_text(prom_text, encoding="utf-8")

        ns_yaml = base_dst / "namespace.yaml"
        ns_text = re.sub(
            r"^(\s*name:\s*).*",
            rf"\g<1>{self.chamber_namespace}",
            ns_yaml.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
        ns_yaml.write_text(ns_text, encoding="utf-8")

        kust_yaml = base_dst / "kustomization.yaml"
        kust_text = re.sub(
            r"^(namespace:\s*).*",
            rf"\g<1>{self.chamber_namespace}",
            kust_yaml.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
        kust_yaml.write_text(kust_text, encoding="utf-8")

        self._patch_registry_manifest(base_dst, self.registry_port, self.registry_memory_limit)
        self._patch_kueue_fair_sharing_config(base_dst, self.enable_fair_sharing)

        active_kueue_img = self._resolved_kueue_image or self.kueue_image
        if active_kueue_img:
            self._patch_kustomize_kueue_image(base_dst, active_kueue_img)

        if include_overlays:
            overlays_dst = self._tmp_deploy_dir / "overlays"
            if overlays_dst.exists():
                shutil.rmtree(overlays_dst)
            shutil.copytree(DEPLOY_DIR / "overlays", overlays_dst)
            ov_kust = overlays_dst / "with-sample-queues" / "kustomization.yaml"
            if ov_kust.exists():
                ov_kust.write_text(
                    re.sub(
                        r"^(namespace:\s*).*",
                        rf"\g<1>{self.chamber_namespace}",
                        ov_kust.read_text(encoding="utf-8"),
                        flags=re.MULTILINE,
                    ),
                    encoding="utf-8",
                )
            root_kust = self._tmp_deploy_dir / "kustomization.yaml"
            shutil.copy2(DEPLOY_DIR / "kustomization.yaml", root_kust)
            root_kust.write_text(
                re.sub(
                    r"^(namespace:\s*).*",
                    rf"\g<1>{self.chamber_namespace}",
                    root_kust.read_text(encoding="utf-8"),
                    flags=re.MULTILINE,
                ),
                encoding="utf-8",
            )
        return base_dst

    def stage_registry_only(self, timeout_sec: float = 180.0) -> str:
        """Deploy Namespace + per-chamber in-memory kwok-registry before full chamber rollout and wait until ready."""
        self._ensure_namespace_safe_for_apply(record_creation=False)
        self._resolve_cluster_registry_port(check_cluster=True)
        stage_dir = Path(tempfile.mkdtemp(prefix="kueue-optimizer-registry-stage-"))
        try:
            shutil.copy2(DEPLOY_DIR / "base" / "namespace.yaml", stage_dir / "namespace.yaml")
            shutil.copy2(DEPLOY_DIR / "base" / "registry.yaml", stage_dir / "registry.yaml")
            ns_yaml = stage_dir / "namespace.yaml"
            ns_yaml.write_text(
                re.sub(
                    r"^(\s*name:\s*).*",
                    rf"\g<1>{self.chamber_namespace}",
                    ns_yaml.read_text(encoding="utf-8"),
                    flags=re.MULTILINE,
                ),
                encoding="utf-8",
            )
            self._patch_registry_manifest(stage_dir, self.registry_port, self.registry_memory_limit)
            (stage_dir / "kustomization.yaml").write_text(
                f"apiVersion: kustomize.config.k8s.io/v1beta1\n"
                f"kind: Kustomization\n"
                f"namespace: {self.chamber_namespace}\n"
                f"resources:\n"
                f"- namespace.yaml\n"
                f"- registry.yaml\n",
                encoding="utf-8",
            )
            print(
                f"==> Staging per-chamber in-memory image registry in '{self.chamber_namespace}' "
                f"(NodePort {self.registry_port}, memory limit {self.registry_memory_limit})...",
                flush=True,
            )
            self._ensure_namespace_safe_for_apply(record_creation=True)
            self.host_kubectl(
                ["apply", "--server-side", "--force-conflicts", "-k", str(stage_dir)],
                stream_output=True,
            )
            self.host_kubectl(
                [
                    "-n",
                    self.chamber_namespace,
                    "rollout",
                    "status",
                    f"--timeout={int(timeout_sec)}s",
                    "deployment/kwok-registry",
                ],
                timeout=timeout_sec + 20.0,
                stream_output=True,
            )
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)
        return self.ensure_registry_connection(timeout_sec=60.0)

    def _probe_registry_ready(self, port: int | None = None, timeout: float = 1.5) -> bool:
        target_port = int(port if port is not None else self.registry_port)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(f"http://127.0.0.1:{target_port}/v2/", timeout=timeout) as resp:
                resp.read()
                return 200 <= resp.status < 300
        except Exception:
            return False

    def ensure_registry_connection(self, timeout_sec: float = 45.0) -> str:
        """Ensure local port-forward 127.0.0.1:<registry_port> -> svc/kwok-registry:5000 is active."""
        self._resolve_cluster_registry_port(check_cluster=True)
        with self._pf_lock:
            if self._probe_registry_ready(self.registry_port, timeout=1.5):
                return f"localhost:{self.registry_port}"

            if self._registry_pf_proc is not None:
                try:
                    self._registry_pf_proc.terminate()
                    self._registry_pf_proc.wait(timeout=2)
                except Exception:
                    try:
                        self._registry_pf_proc.kill()
                    except Exception:
                        pass
                self._registry_pf_proc = None

            self._pkill_chamber_port_forward("kwok-registry", f"{self.registry_port}:5000")

            deadline = time.monotonic() + timeout_sec
            while time.monotonic() < deadline:
                if self._registry_pf_proc is None or self._registry_pf_proc.poll() is not None:
                    self._registry_pf_proc = subprocess.Popen(
                        [
                            "kubectl",
                            "-n",
                            self.chamber_namespace,
                            "port-forward",
                            "svc/kwok-registry",
                            f"{self.registry_port}:5000",
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                for _ in range(10):
                    if self._probe_registry_ready(self.registry_port, timeout=1.5):
                        return f"localhost:{self.registry_port}"
                    if self._registry_pf_proc.poll() is not None:
                        break
                    time.sleep(0.3)
                time.sleep(0.5)

        raise RuntimeError(
            f"Timed out waiting to connect to per-chamber registry svc/kwok-registry in namespace "
            f"'{self.chamber_namespace}' on localhost:{self.registry_port}"
        )

    def fetch_registry_manifest_digest(self, repo: str, tag: str) -> str:
        """Query the per-chamber OCI registry HTTP API for Docker-Content-Digest of <repo>:<tag>."""
        self.ensure_registry_connection(timeout_sec=30.0)
        accept = ", ".join(
            [
                "application/vnd.oci.image.index.v1+json",
                "application/vnd.oci.image.manifest.v1+json",
                "application/vnd.docker.distribution.manifest.list.v2+json",
                "application/vnd.docker.distribution.manifest.v2+json",
            ]
        )
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.registry_port}/v2/{repo}/manifests/{tag}",
            method="HEAD",
            headers={"Accept": accept},
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=10.0) as resp:
            digest = resp.headers.get("Docker-Content-Digest") or ""
            return digest.strip()

    def _ensure_registry_staged_or_connected(self, stage_if_needed: bool = True) -> None:
        """Ensure kwok-registry is deployed (when stage_if_needed=True) and local port-forward is connected."""
        if stage_if_needed:
            proc_check = self.host_kubectl(
                ["-n", self.chamber_namespace, "get", "deployment/kwok-registry"],
                check=False,
                timeout=15.0,
            )
            if proc_check.returncode != 0:
                self.stage_registry_only()
                return
        self.ensure_registry_connection()

    def push_image_to_chamber_registry(
        self,
        source_image: str,
        repo: str = "kueue",
        tag: str | None = None,
        stage_if_needed: bool = True,
    ) -> tuple[str, str]:
        """Tag and push a local Docker image into this chamber's in-memory registry.

        Returns (pullable_tag_ref, manifest_sha256_digest), e.g.:
          ('localhost:30500/kueue:adhoc', 'sha256:...')
        """
        self._ensure_registry_staged_or_connected(stage_if_needed=stage_if_needed)

        _, _, parsed_qual = parse_image_ref(source_image)
        if tag:
            clean_tag = re.sub(r"[^A-Za-z0-9_.-]+", "-", tag.strip()).strip(".-") or "adhoc"
        elif parsed_qual and not parsed_qual.startswith("sha256:"):
            clean_tag = re.sub(r"[^A-Za-z0-9_.-]+", "-", parsed_qual).strip(".-") or "adhoc"
        else:
            clean_tag = "adhoc"

        clean_repo = repo.strip("/ ") or "kueue"
        target_ref = f"localhost:{self.registry_port}/{clean_repo}:{clean_tag}"
        if source_image.strip() != target_ref:
            subprocess.run(["docker", "tag", source_image.strip(), target_ref], check=True, timeout=60.0)

        print(
            f"==> Pushing '{source_image}' -> per-chamber in-memory registry '{target_ref}' "
            f"(namespace '{self.chamber_namespace}')...",
            flush=True,
        )
        subprocess.run(["docker", "push", target_ref], check=True, timeout=300.0)
        digest = ""
        try:
            digest = self.fetch_registry_manifest_digest(clean_repo, clean_tag)
        except Exception:
            digest = ""
        digest_note = f" ({digest})" if digest else ""
        print(f"    Stored in chamber RAM registry: {target_ref}{digest_note}", flush=True)
        return target_ref, digest

    def build_and_push_kueue_image(
        self,
        kueue_src_dir: Path | str,
        tag: str = "adhoc",
        dockerfile: str | None = None,
        build_args: list[str] | None = None,
        stage_if_needed: bool = True,
    ) -> tuple[str, str]:
        """Build an ad-hoc Kueue image from source directory and push it into this chamber's in-memory registry."""
        src_path = Path(kueue_src_dir).resolve()
        if not src_path.is_dir():
            raise RuntimeError(f"Kueue build directory does not exist: {src_path}")

        self._ensure_registry_staged_or_connected(stage_if_needed=stage_if_needed)

        clean_tag = re.sub(r"[^A-Za-z0-9_.-]+", "-", (tag or "adhoc").strip()).strip(".-") or "adhoc"
        local_tag = f"localhost:{self.registry_port}/kueue:{clean_tag}"
        cmd = ["docker", "build", "-t", local_tag]
        if dockerfile:
            cmd.extend(["-f", dockerfile])
        elif (src_path / "Dockerfile").is_file():
            cmd.extend(["-f", str(src_path / "Dockerfile")])
        for ba in build_args or ():
            cmd.extend(["--build-arg", ba])
        cmd.append(str(src_path))

        print(
            f"==> Building ad-hoc Kueue image from '{src_path}' -> '{local_tag}'...",
            flush=True,
        )
        subprocess.run(cmd, check=True, timeout=900.0)
        return self.push_image_to_chamber_registry(
            source_image=local_tag,
            repo="kueue",
            tag=clean_tag,
            stage_if_needed=False,
        )

    def _prepare_adhoc_kueue_image_before_deploy(self) -> None:
        """If kueue_build_dir or a local Docker kueue_image is configured, stage registry & push before deploy."""
        if self.kueue_build_dir is not None:
            target_ref, digest = self.build_and_push_kueue_image(
                kueue_src_dir=self.kueue_build_dir,
                tag="adhoc",
                stage_if_needed=True,
            )
            self._resolved_kueue_image = f"{target_ref}@{digest}" if digest else target_ref
            return

        if not self.kueue_image:
            self._resolved_kueue_image = None
            return

        raw_img = self.kueue_image.strip()
        reg_host, repo_path, qual = parse_image_ref(raw_img)
        # If the image exists in the local workstation Docker daemon, push it into THIS chamber's in-memory registry
        # so every GKE worker node can pull it over loopback NodePort (localhost:<registry_port>/...).
        if docker_image_exists_locally(raw_img):
            leaf_repo = repo_path.split("/")[-1] if repo_path else "kueue"
            target_tag = qual if (qual and not qual.startswith("sha256:")) else "adhoc"
            target_ref, digest = self.push_image_to_chamber_registry(
                source_image=raw_img,
                repo=leaf_repo,
                tag=target_tag,
                stage_if_needed=True,
            )
            self._resolved_kueue_image = f"{target_ref}@{digest}" if digest else target_ref
            return

        # Otherwise treat as an already-pushed chamber registry reference or external remote registry image
        if reg_host.startswith("localhost:"):
            self._resolved_kueue_image = raw_img
        else:
            self._resolved_kueue_image = raw_img

    def update_kueue_image(
        self,
        kueue_image: str,
        tag: str | None = None,
        timeout_sec: float = 180.0,
        push_if_local: bool = True,
    ) -> str:
        """Push (if local) and hot-swap deployment/kueue-controller-manager in a running chamber."""
        self.kueue_image = kueue_image.strip()
        if push_if_local and docker_image_exists_locally(self.kueue_image):
            _, repo_path, qual = parse_image_ref(self.kueue_image)
            leaf_repo = repo_path.split("/")[-1] if repo_path else "kueue"
            target_tag = tag or (qual if (qual and not qual.startswith("sha256:")) else "adhoc")
            target_ref, digest = self.push_image_to_chamber_registry(
                source_image=self.kueue_image,
                repo=leaf_repo,
                tag=target_tag,
                stage_if_needed=True,
            )
            resolved = f"{target_ref}@{digest}" if digest else target_ref
        else:
            resolved = self.kueue_image

        self._resolved_kueue_image = resolved
        patch_json = json.dumps(
            {
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "kueue-optimizer.gke.io/kueue-image-updated-at": f"{time.time():.6f}",
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": "manager",
                                    "image": resolved,
                                    "imagePullPolicy": "Always",
                                }
                            ]
                        },
                    }
                }
            }
        )
        print(
            f"==> Updating deployment/kueue-controller-manager in '{self.chamber_namespace}' -> {resolved}",
            flush=True,
        )
        self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "patch",
                "deployment",
                "kueue-controller-manager",
                "--type=strategic",
                "-p",
                patch_json,
            ],
            stream_output=True,
        )
        self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "rollout",
                "status",
                f"--timeout={int(timeout_sec)}s",
                "deployment/kueue-controller-manager",
            ],
            timeout=timeout_sec + 20.0,
            stream_output=True,
        )
        return resolved

    def list_chamber_registry_images(self) -> list[dict[str, Any]]:
        """Return a list of {repository, tag, digest, image_ref} entries stored in this chamber's in-memory registry."""
        self.ensure_registry_connection(timeout_sec=30.0)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f"http://127.0.0.1:{self.registry_port}/v2/_catalog", timeout=10.0) as resp:
            catalog = json.loads(resp.read().decode("utf-8"))
        repos = catalog.get("repositories") or []
        entries: list[dict[str, Any]] = []
        for repo in repos:
            try:
                with opener.open(f"http://127.0.0.1:{self.registry_port}/v2/{repo}/tags/list", timeout=10.0) as r_tags:
                    tags_data = json.loads(r_tags.read().decode("utf-8"))
                tags = tags_data.get("tags") or []
            except Exception:
                tags = []
            for t in sorted(tags):
                digest = ""
                try:
                    digest = self.fetch_registry_manifest_digest(repo, t)
                except Exception:
                    digest = ""
                entries.append(
                    {
                        "repository": repo,
                        "tag": t,
                        "digest": digest,
                        "image_ref": f"localhost:{self.registry_port}/{repo}:{t}",
                    }
                )
        return entries

    def get_chamber_registry_memory_usage(self) -> dict[str, Any]:
        """Inspect tmpfs (/var/lib/registry) memory utilization inside deployment/kwok-registry."""
        proc = self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "exec",
                "deployment/kwok-registry",
                "--",
                "df",
                "-k",
                "/var/lib/registry",
            ],
            check=False,
            timeout=15.0,
        )
        if proc.returncode != 0 or not proc.stdout:
            return {}
        lines = [ln.strip() for ln in proc.stdout.strip().splitlines() if ln.strip()]
        if len(lines) < 2:
            return {}
        parts = lines[-1].split()
        if len(parts) < 6:
            return {}
        try:
            total_kib = int(parts[1])
            used_kib = int(parts[2])
            avail_kib = int(parts[3])
            return {
                "filesystem": parts[0],
                "total_mib": round(total_kib / 1024.0, 2),
                "used_mib": round(used_kib / 1024.0, 2),
                "available_mib": round(avail_kib / 1024.0, 2),
                "use_percent": parts[4],
                "mountpoint": parts[5],
            }
        except Exception:
            return {}

    def garbage_collect_chamber_registry(self, delete_untagged: bool = True) -> str:
        """Run OCI registry garbage-collect inside deployment/kwok-registry to free tmpfs RAM."""
        args = [
            "-n",
            self.chamber_namespace,
            "exec",
            "deployment/kwok-registry",
            "--",
            "registry",
            "garbage-collect",
        ]
        if delete_untagged:
            args.append("--delete-untagged=true")
        args.append("/etc/docker/registry/config.yml")
        proc = self.host_kubectl(args, check=True, timeout=60.0)
        return (proc.stdout or "") + (proc.stderr or "")

    def deploy_chamber(self, initial_nodes: list[dict[str, Any]] | None = None) -> None:
        self._ensure_namespace_safe_for_apply(record_creation=False)
        self._resolve_cluster_registry_port(check_cluster=True)
        self._prepare_adhoc_kueue_image_before_deploy()
        base_dst = self.prepare_tmp_deploy_dir(initial_nodes=initial_nodes, check_cluster_ports=False)
        pki_note = "cluster Secret/kwok-pki" if self._pki_source == "cluster" else "ad-hoc generated PKI"
        kueue_img_note = self._resolved_kueue_image or DEFAULT_KUEUE_IMAGE
        fs_note = "enabled" if self.enable_fair_sharing else "disabled"
        print(
            f"==> Preparing & deploying chamber into host cluster namespace '{self.chamber_namespace}' "
            f"(PKI: {pki_note}, Registry: localhost:{self.registry_port} [RAM {self.registry_memory_limit}], "
            f"Kueue: {kueue_img_note}, FairSharing: {fs_note}, Prometheus scrape: {self.scrape_interval})...",
            flush=True,
        )
        self._ensure_namespace_safe_for_apply(record_creation=True)
        self.host_kubectl(
            ["apply", "--server-side", "--force-conflicts", "-k", str(base_dst)],
            stream_output=True,
        )

        print(
            f"==> Waiting for chamber control-plane Deployments in '{self.chamber_namespace}' to become Available...",
            flush=True,
        )
        for dep in CHAMBER_DEPLOYMENTS:
            self.host_kubectl(
                [
                    "-n",
                    self.chamber_namespace,
                    "rollout",
                    "status",
                    "--timeout=300s",
                    dep,
                ],
                timeout=320.0,
                stream_output=True,
            )
        self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "wait",
                "--for=condition=Available",
                "--timeout=300s",
                *CHAMBER_DEPLOYMENTS,
            ],
            timeout=320.0,
            stream_output=True,
        )

    def write_local_kubeconfig(self, prefer_cluster: bool = True) -> Path:
        pki_dir = self.ensure_pki(prefer_cluster=prefer_cluster)
        raw_kc = (pki_dir / "kubeconfig").read_text(encoding="utf-8")
        kc_text = raw_kc.replace(
            "https://kwok-kube-apiserver:6443",
            f"https://127.0.0.1:{self.apiserver_port}",
        )
        self.kubeconfig_path.write_text(kc_text, encoding="utf-8")
        return self.kubeconfig_path

    def ensure_kwok_connection(self) -> None:
        self.write_local_kubeconfig(prefer_cluster=True)
        with self._pf_lock:
            self._start_or_restart_apiserver_pf_locked(timeout_sec=60.0)

    def ensure_prometheus_connection(self) -> None:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        url = f"http://127.0.0.1:{self.prometheus_port}/-/ready"
        try:
            with opener.open(url, timeout=1.5):
                return
        except Exception:
            pass

        if self._prom_pf_proc is not None:
            try:
                self._prom_pf_proc.terminate()
                self._prom_pf_proc.wait(timeout=2)
            except Exception:
                try:
                    self._prom_pf_proc.kill()
                except Exception:
                    pass
            self._prom_pf_proc = None

        self._pkill_chamber_port_forward("prometheus", f"{self.prometheus_port}:9090")

        self._prom_pf_proc = subprocess.Popen(
            [
                "kubectl",
                "-n",
                self.chamber_namespace,
                "port-forward",
                "svc/prometheus",
                f"{self.prometheus_port}:9090",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(15):
            try:
                with opener.open(url, timeout=1.5):
                    return
            except Exception:
                time.sleep(1.0)

    def wait_for_crds(
        self,
        required_crds: tuple[str, ...] | set[str] = REQUIRED_KWOK_CRDS,
        timeout_sec: float = 120.0,
    ) -> None:
        req_set = set(required_crds)
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            st, data = self.api_request("GET", "/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
            if st == 200 and data:
                items = json.loads(data.decode("utf-8")).get("items", [])
                established = set()
                for item in items:
                    name = item.get("metadata", {}).get("name", "")
                    conds = {
                        c.get("type"): c.get("status")
                        for c in (item.get("status", {}) or {}).get("conditions", []) or []
                    }
                    if conds.get("Established") == "True":
                        established.add(name)
                if req_set.issubset(established):
                    return
            time.sleep(1.0)
        raise RuntimeError(f"Timed out waiting for CRDs to be Established: {sorted(req_set)}")

    def apply_kwok_stages(self) -> None:
        stages_yaml = (DEPLOY_DIR / "base" / "manifests" / "kwok-stages.yaml").read_text(encoding="utf-8")
        self.kwok_kubectl(
            ["apply", "--server-side", "--force-conflicts", "--validate=false", "-f", "-"],
            input_text=stages_yaml,
        )

    def ensure_kueue_webhook_namespace_exclusions(
        self,
        excluded_namespaces: tuple[str, ...] | list[str] = (DEFAULT_INFRA_PODS_NAMESPACE,),
    ) -> None:
        """Ensure Kueue mutating/validating webhooks explicitly exclude excluded_namespaces and kueue.x-k8s.io/exclude=true."""
        targets = (
            ("MutatingWebhookConfiguration", "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/kueue-mutating-webhook-configuration"),
            ("ValidatingWebhookConfiguration", "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/kueue-validating-webhook-configuration"),
        )
        for kind, get_path in targets:
            st, raw = self.api_request("GET", get_path)
            if st != 200 or not raw:
                continue
            obj = json.loads(raw.decode("utf-8", errors="replace"))
            changed = False
            for wh in obj.get("webhooks") or []:
                ns_sel = wh.get("namespaceSelector")
                if not isinstance(ns_sel, dict):
                    continue
                exprs = ns_sel.setdefault("matchExpressions", [])
                name_expr = None
                excl_label_expr = None
                for ex in exprs:
                    if ex.get("key") == "kubernetes.io/metadata.name" and ex.get("operator") == "NotIn":
                        name_expr = ex
                    elif ex.get("key") == "kueue.x-k8s.io/exclude" and ex.get("operator") == "NotIn":
                        excl_label_expr = ex
                if name_expr is not None:
                    vals = list(name_expr.get("values") or [])
                    for ns_name in excluded_namespaces:
                        if ns_name and ns_name not in vals:
                            vals.append(ns_name)
                            changed = True
                    name_expr["values"] = vals
                if excl_label_expr is None:
                    exprs.append(
                        {
                            "key": "kueue.x-k8s.io/exclude",
                            "operator": "NotIn",
                            "values": ["true"],
                        }
                    )
                    changed = True
            if changed:
                patch_doc = {
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": kind,
                    "metadata": {"name": obj["metadata"]["name"]},
                    "webhooks": obj.get("webhooks") or [],
                }
                self.ssa_apply_doc(patch_doc)

    def clear_workload_namespace(self, delete_sample_cq: bool = True) -> None:
        self.api_delete(f"/apis/jobset.x-k8s.io/v1alpha2/namespaces/{self.workload_namespace}/jobsets")
        self.api_delete(f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{self.workload_namespace}/workloads")
        self.api_delete(f"/api/v1/namespaces/{self.workload_namespace}/pods")
        if delete_sample_cq:
            self.api_delete("/apis/kueue.x-k8s.io/v1beta2/clusterqueues/cluster-queue")

    def sync_kwok_nodes(
        self,
        nodes: list[dict[str, Any]],
        remove_stale: bool = True,
        timeout_sec: float = 180.0,
    ) -> None:
        """Apply simulated KWOK nodes concurrently, remove stale nodes, and wait until Ready."""
        nodes_by_name = {n["metadata"]["name"]: n for n in nodes}
        t_start = time.monotonic()
        self.apply_manifests_parallel(nodes, max_workers=24)
        elapsed = time.monotonic() - t_start
        if len(nodes) > 20:
            print(f"    Applied {len(nodes)} KWOK nodes in {elapsed:.1f}s. Waiting for nodes Ready...", flush=True)

        stale_deleted = not remove_stale
        deadline = time.monotonic() + timeout_sec
        nodes_needing_status_patch: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            st, raw = self.api_request("GET", "/api/v1/nodes")
            if st == 200 and raw:
                items = json.loads(raw.decode("utf-8")).get("items", [])
                if not stale_deleted:
                    stale_items = [
                        it
                        for it in items
                        if (it.get("metadata", {}).get("labels") or {}).get("type") == "kwok"
                        and it["metadata"]["name"] not in nodes_by_name
                    ]
                    for s_it in stale_items:
                        sname = s_it["metadata"]["name"]
                        self.api_delete(f"/api/v1/nodes/{sname}", obj=s_it)
                    stale_deleted = True

                ready_count = 0
                nodes_needing_status_patch.clear()
                for it in items:
                    nname = it.get("metadata", {}).get("name", "")
                    if nname not in nodes_by_name:
                        continue
                    st_obj = it.get("status", {}) or {}
                    conds = {c.get("type"): c.get("status") for c in (st_obj.get("conditions") or [])}
                    if conds.get("Ready") == "True":
                        ready_count += 1
                        alloc = st_obj.get("allocatable") or {}
                        expected_alloc = nodes_by_name[nname]["status"]["allocatable"]
                        if any(str(alloc.get(k)) != str(v) for k, v in expected_alloc.items()):
                            nodes_needing_status_patch.append(nodes_by_name[nname])

                if ready_count >= len(nodes_by_name):
                    break
            time.sleep(0.5)
        else:
            raise RuntimeError(f"Timed out waiting for {len(nodes_by_name)} KWOK nodes to become Ready")

        if nodes_needing_status_patch:
            pool = self._get_executor()
            futs = [pool.submit(self.ssa_apply_node_status, nd) for nd in nodes_needing_status_patch]
            for fut in as_completed(futs):
                fut.result()

    def apply_and_wait_queues(
        self,
        queue_docs: list[dict[str, Any]],
        timeout_sec: float = 60.0,
    ) -> None:
        """Apply prerequisite resources and queues, then wait until all ClusterQueues & LocalQueues are Active."""
        cq_names = {d["metadata"]["name"] for d in queue_docs if d.get("kind") == "ClusterQueue"}
        lq_names = {d["metadata"]["name"] for d in queue_docs if d.get("kind") == "LocalQueue"}

        prereq_docs = [d for d in queue_docs if d.get("kind") not in ("ClusterQueue", "LocalQueue")]
        cq_lq_docs = [d for d in queue_docs if d.get("kind") in ("ClusterQueue", "LocalQueue")]
        self.apply_manifests_parallel(prereq_docs, max_workers=16)
        self.apply_manifests_parallel(cq_lq_docs, max_workers=16)

        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            st_cq, raw_cq = self.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/clusterqueues")
            st_lq, raw_lq = self.api_request(
                "GET",
                f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{self.workload_namespace}/localqueues",
            )
            if st_cq == 200 and st_lq == 200 and raw_cq and raw_lq:
                active_cqs = {
                    it["metadata"]["name"]
                    for it in json.loads(raw_cq.decode("utf-8")).get("items", [])
                    if any(
                        c.get("type") == "Active" and c.get("status") == "True"
                        for c in (it.get("status", {}) or {}).get("conditions") or []
                    )
                }
                active_lqs = {
                    it["metadata"]["name"]
                    for it in json.loads(raw_lq.decode("utf-8")).get("items", [])
                    if any(
                        c.get("type") == "Active" and c.get("status") == "True"
                        for c in (it.get("status", {}) or {}).get("conditions") or []
                    )
                }
                if cq_names.issubset(active_cqs) and lq_names.issubset(active_lqs):
                    return
            time.sleep(0.5)
        raise RuntimeError("Timed out waiting for ClusterQueues and LocalQueues to become Active")

    def wait_for_prometheus_scrape(self, target_epoch: float) -> int:
        """Wait for at least one Prometheus scrape of kueue-metrics at or after target_epoch."""
        self.ensure_prometheus_connection()
        m = re.match(r"^(\d+)", self.scrape_interval)
        max_wait = (int(m.group(1)) if m else 10) + 5
        path = "/api/v1/query?query=timestamp(up%7Bjob%3D%22kueue-metrics%22%7D)"
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def _get_last_scrape_ts() -> float:
            try:
                with opener.open(f"http://127.0.0.1:{self.prometheus_port}{path}", timeout=3.0) as r:
                    data = json.loads(r.read().decode("utf-8"))
            except Exception:
                raw = f"/api/v1/namespaces/{self.chamber_namespace}/services/http:prometheus:9090/proxy{path}"
                proc = self.host_kubectl(["get", "--raw", raw], check=False, timeout=10.0)
                if proc.returncode != 0 or not proc.stdout:
                    return 0.0
                data = json.loads(proc.stdout)
            res = data.get("data", {}).get("result", [])
            if res:
                return float(res[0]["value"][1])
            return 0.0

        deadline = time.time() + max_wait
        last_ts = 0.0
        while time.time() < deadline:
            last_ts = _get_last_scrape_ts()
            if last_ts >= target_epoch:
                break
            time.sleep(1.0)
        return int(max(target_epoch, last_ts, time.time()))

    def capture_cluster_saturation_snapshot(self, eval_epoch: float | None = None) -> dict[str, Any] | None:
        """Capture exact Node capacities, Kueue Workloads, and Pod phases at test end (before scrape wait)."""
        try:
            self.ensure_kwok_connection()
            snapshot: dict[str, Any] = {
                "capturedAtEpoch": float(eval_epoch if eval_epoch is not None else time.time()),
                "nodes": [],
                "workloads": [],
                "pods": [],
            }
            st_n, raw_n = self.api_request("GET", "/api/v1/nodes")
            if st_n == 200 and raw_n:
                snapshot["nodes"] = (json.loads(raw_n.decode("utf-8")) or {}).get("items") or []

            st_w, raw_w = self.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/workloads")
            if st_w != 200 or not raw_w:
                st_w, raw_w = self.api_request("GET", "/apis/kueue.x-k8s.io/v1beta1/workloads")
            if st_w == 200 and raw_w:
                snapshot["workloads"] = (json.loads(raw_w.decode("utf-8")) or {}).get("items") or []

            st_p, raw_p = self.api_request("GET", "/api/v1/pods")
            if st_p == 200 and raw_p:
                snapshot["pods"] = (json.loads(raw_p.decode("utf-8")) or {}).get("items") or []

            return snapshot
        except Exception:
            return None

    def run_kueue_metrics_report(
        self,
        queueing_strategy: str,
        start_epoch: float,
        end_epoch: float,
        jobset_count: int,
        total_pods: int,
        wait_for_scrape: bool = True,
    ) -> None:
        """Capture test-end cluster state, wait for final Prometheus scrape, and run kueue_metrics_report.py."""
        # Snapshot cluster state IMMEDIATELY at end_epoch before waiting for the final Prometheus scrape
        # so workloads actively running at test end are captured without post-test drift.
        snap_path: Path | None = None
        snap_data = self.capture_cluster_saturation_snapshot(eval_epoch=end_epoch)
        if snap_data is not None:
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    prefix="kueue-optimizer-sat-snap-",
                    suffix=".json",
                    delete=False,
                ) as tf:
                    json.dump(snap_data, tf)
                    snap_path = Path(tf.name)
            except Exception:
                snap_path = None

        try:
            self.ensure_prometheus_connection()
            if wait_for_scrape:
                print("==> Waiting for final Prometheus scrape of Kueue metrics...", flush=True)
                query_end_epoch = self.wait_for_prometheus_scrape(end_epoch)
            else:
                query_end_epoch = int(max(start_epoch + 1, end_epoch))

            cmd = [
                sys.executable,
                str(SCRIPT_DIR / "kueue_metrics_report.py"),
                "--namespace",
                self.chamber_namespace,
                "--workload-namespace",
                self.workload_namespace,
                "--queueing-strategy",
                queueing_strategy,
                "--prometheus-url",
                f"http://127.0.0.1:{self.prometheus_port}",
                "--start-epoch",
                f"{start_epoch:.3f}",
                "--end-epoch",
                f"{max(start_epoch, end_epoch):.3f}",
                "--query-end-epoch",
                str(int(query_end_epoch)),
                "--jobset-count",
                str(jobset_count),
                "--total-pods",
                str(total_pods),
            ]
            if snap_path is not None:
                cmd.extend(["--cluster-snapshot-json", str(snap_path)])
            if self.kubeconfig_path.is_file():
                cmd.extend(["--kubeconfig", str(self.kubeconfig_path)])

            subprocess.run(cmd, check=False)
        finally:
            if snap_path is not None:
                try:
                    snap_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def cleanup_chamber(self) -> None:
        print(f"==> Cleaning up chamber in host cluster namespace '{self.chamber_namespace}'...", flush=True)
        self.close()
        for svc in ("kwok-kube-apiserver", "prometheus", "kwok-registry"):
            self._pkill_chamber_port_forward(svc)

        get_proc = self.host_kubectl(
            ["get", "namespace", "-o", "json", "--", self.chamber_namespace],
            check=False,
            timeout=30.0,
        )
        if get_proc.returncode != 0:
            err_msg = (get_proc.stderr or "").strip()
            if "notfound" in err_msg.lower() or "not found" in err_msg.lower():
                if self.kubeconfig_path.exists():
                    self.kubeconfig_path.unlink()
                    print(f"    Removed {self.kubeconfig_path}", flush=True)
                print(
                    f"==> Chamber namespace '{self.chamber_namespace}' does not exist; cleanup complete.",
                    flush=True,
                )
                return
            raise RuntimeError(
                f"Failed to inspect namespace '{self.chamber_namespace}' before cleanup "
                f"(exit {get_proc.returncode}): {err_msg}"
            )

        try:
            ns_obj = json.loads(get_proc.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Failed to parse namespace '{self.chamber_namespace}' JSON during cleanup: {exc}"
            ) from exc

        labels = ((ns_obj.get("metadata") or {}).get("labels")) or {}
        if labels.get(CHAMBER_NAMESPACE_LABEL_KEY) != CHAMBER_NAMESPACE_LABEL_VALUE:
            raise RuntimeError(
                f"Refusing to delete namespace '{self.chamber_namespace}': missing chamber ownership label "
                f"('{CHAMBER_NAMESPACE_LABEL_KEY}={CHAMBER_NAMESPACE_LABEL_VALUE}')."
            )

        del_proc = self.host_kubectl(
            ["delete", "namespace", "--ignore-not-found", "--", self.chamber_namespace],
            check=False,
        )
        if del_proc.returncode != 0:
            err_msg = (del_proc.stderr or "").strip()
            raise RuntimeError(
                f"Failed to delete chamber namespace '{self.chamber_namespace}' "
                f"(exit {del_proc.returncode}): {err_msg}"
            )

        if self.kubeconfig_path.exists():
            self.kubeconfig_path.unlink()
            print(f"    Removed {self.kubeconfig_path}", flush=True)
        print(f"==> Chamber '{self.chamber_namespace}' cleanup complete.", flush=True)


@dataclass
class ClusterStatusSnapshot:
    """Snapshot of JobSets, Kueue Workloads, ClusterQueues, and KWOK Pods."""

    jobsets_total: int = 0
    jobsets_active: int = 0
    jobsets_completed: int = 0
    jobsets_failed: int = 0
    jobsets_suspended: int = 0
    completed_job_ids: set[str] = field(default_factory=set)

    workloads_admitted: int = 0
    workloads_pending: int = 0
    workloads_finished: int = 0
    workloads_evicted: int = 0

    pods_running: int = 0
    pods_pending: int = 0
    pods_succeeded: int = 0
    pods_failed: int = 0

    cq_summary: list[str] = field(default_factory=list)
    cq_admitted_by_name: dict[str, int] = field(default_factory=dict)
    cq_pending_by_name: dict[str, int] = field(default_factory=dict)


def query_cluster_status(
    mgr: ChamberManager,
    accel_resource: str | None = None,
    *,
    workload_namespaces: list[str] | tuple[str, ...] | set[str] | None = None,
    include_kueue_workloads: bool = False,
) -> ClusterStatusSnapshot:
    """Query current JobSets, Kueue Workloads, ClusterQueues, and Pods from the KWOK cluster via HTTPS."""
    snap = ClusterStatusSnapshot()
    ns_filter = set(workload_namespaces) if workload_namespaces else None

    # 1. ClusterQueues status
    try:
        st_code, cq_raw = mgr.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/clusterqueues")
        if st_code == 200 and cq_raw:
            cq_data = json.loads(cq_raw.decode("utf-8"))
            for item in cq_data.get("items", []):
                name = item.get("metadata", {}).get("name", "?")
                st = item.get("status", {}) or {}
                adm = int(st.get("admittedWorkloads") or 0)
                pend = int(st.get("pendingWorkloads") or 0)
                snap.workloads_admitted += adm
                snap.workloads_pending += pend
                snap.cq_admitted_by_name[name] = adm
                snap.cq_pending_by_name[name] = pend

                accel_used = ""
                if accel_resource:
                    for flv in st.get("flavorsReservation", []) or []:
                        for res in flv.get("resources", []) or []:
                            if res.get("name") == accel_resource:
                                accel_used = f",{accel_resource.split('/')[-1]}={res.get('total', '0')}"
                snap.cq_summary.append(f"{name}[adm={adm},pend={pend}{accel_used}]")
    except Exception:
        pass

    # 2. JobSets status
    try:
        if ns_filter and (len(ns_filter) > 1 or mgr.workload_namespace not in ns_filter):
            js_path = "/apis/jobset.x-k8s.io/v1alpha2/jobsets"
        else:
            js_path = f"/apis/jobset.x-k8s.io/v1alpha2/namespaces/{mgr.workload_namespace}/jobsets"
        st_code, js_raw = mgr.api_request("GET", js_path)
        if st_code == 200 and js_raw:
            js_data = json.loads(js_raw.decode("utf-8"))
            items = [
                it
                for it in js_data.get("items", [])
                if not ns_filter or (it.get("metadata") or {}).get("namespace", "default") in ns_filter
            ]
            snap.jobsets_total = len(items)
            for item in items:
                meta = item.get("metadata", {}) or {}
                jid = (meta.get("annotations") or {}).get("kueue-optimizer.gke.io/job-id") or (
                    meta.get("labels") or {}
                ).get("kueue-optimizer.gke.io/job-id")
                spec = item.get("spec", {}) or {}
                st = item.get("status", {}) or {}
                conds = {c.get("type"): c.get("status") for c in (st.get("conditions") or [])}
                if conds.get("Completed") == "True":
                    snap.jobsets_completed += 1
                    snap.workloads_finished += 1
                    if jid:
                        snap.completed_job_ids.add(str(jid))
                elif conds.get("Failed") == "True":
                    snap.jobsets_failed += 1
                    snap.workloads_finished += 1
                    if jid:
                        snap.completed_job_ids.add(str(jid))
                elif spec.get("suspend") is True:
                    snap.jobsets_suspended += 1
                else:
                    snap.jobsets_active += 1
    except Exception:
        pass

    # 3. Optional Kueue Workloads status (counts non-JobSet workflows such as Job, Deployment, LWS, Ray, Pod)
    if include_kueue_workloads:
        try:
            st_wl, raw_wl = mgr.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/workloads")
            if st_wl == 200 and raw_wl:
                items = json.loads(raw_wl.decode("utf-8")).get("items", [])
                adm_cnt = pend_cnt = finished_cnt = evicted_cnt = 0
                scoped_items = [
                    it
                    for it in items
                    if not ns_filter or (it.get("metadata") or {}).get("namespace", "default") in ns_filter
                ]
                js_wl_owners = {
                    (
                        (it.get("metadata") or {}).get("namespace", "default"),
                        str(oref.get("name")),
                    )
                    for it in scoped_items
                    for oref in ((it.get("metadata") or {}).get("ownerReferences") or [])
                    if isinstance(oref, dict) and str(oref.get("kind") or "") == "JobSet" and oref.get("name")
                }
                for it in scoped_items:
                    meta = it.get("metadata") or {}
                    ns = meta.get("namespace", "default")
                    orefs = [o for o in (meta.get("ownerReferences") or []) if isinstance(o, dict)]
                    if js_wl_owners and not any(str(o.get("kind") or "") == "JobSet" for o in orefs):
                        parent_js = (meta.get("labels") or {}).get("jobset.sigs.k8s.io/jobset-name")
                        if parent_js and (ns, str(parent_js)) in js_wl_owners:
                            continue
                    conds = {
                        c.get("type"): c.get("status")
                        for c in ((it.get("status") or {}).get("conditions") or [])
                    }
                    if conds.get("Finished") == "True":
                        finished_cnt += 1
                    elif conds.get("Evicted") == "True" or conds.get("Preempted") == "True":
                        evicted_cnt += 1
                        pend_cnt += 1
                    elif conds.get("Admitted") == "True" or conds.get("QuotaReserved") == "True":
                        adm_cnt += 1
                    else:
                        pend_cnt += 1
                if scoped_items:
                    snap.workloads_admitted = adm_cnt
                    snap.workloads_pending = pend_cnt
                    snap.workloads_finished = max(snap.workloads_finished, finished_cnt)
                    snap.workloads_evicted = evicted_cnt
        except Exception:
            pass

    # 4. Pods phase summary
    try:
        if ns_filter and (len(ns_filter) > 1 or mgr.workload_namespace not in ns_filter):
            pod_path = "/api/v1/pods"
        else:
            pod_path = f"/api/v1/namespaces/{mgr.workload_namespace}/pods"
        st_code, pod_raw = mgr.api_request("GET", pod_path)
        if st_code == 200 and pod_raw:
            pod_data = json.loads(pod_raw.decode("utf-8"))
            for item in pod_data.get("items", []):
                if ns_filter and (item.get("metadata") or {}).get("namespace", "default") not in ns_filter:
                    continue
                p = (item.get("status", {}) or {}).get("phase", "")
                if p == "Running":
                    snap.pods_running += 1
                elif p == "Pending":
                    snap.pods_pending += 1
                elif p == "Succeeded":
                    snap.pods_succeeded += 1
                elif p == "Failed":
                    snap.pods_failed += 1
    except Exception:
        pass

    return snap


def add_common_chamber_args(
    parser: argparse.ArgumentParser,
    default_workload_namespace: str,
    default_queueing_mode: str,
    default_pause_image: str | None = DEFAULT_PAUSE_IMAGE,
) -> None:
    """Register CLI flags shared across chamber runner scripts (testrunner.py and demo.py)."""
    default_ns = env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE)
    default_wl_ns = env_str("WORKLOAD_NAMESPACE", default_workload_namespace)
    default_q_mode = env_str("QUEUEING_MODE", env_str("QUEUEING_STRATEGY", default_queueing_mode))
    default_port = env_int("LOCAL_APISERVER_PORT", DEFAULT_APISERVER_PORT)
    default_scrape = env_str("PROMETHEUS_SCRAPE_INTERVAL", DEFAULT_SCRAPE_INTERVAL)

    parser.add_argument(
        "--namespace",
        default=default_ns,
        help=f"Host cluster namespace for the kueue-optimizer chamber (default: {default_ns})",
    )
    parser.add_argument(
        "--workload-namespace",
        default=default_wl_ns,
        help=f"Namespace inside the KWOK cluster for queues and workloads (default: {default_wl_ns})",
    )
    parser.add_argument(
        "--queueing-mode",
        "--queueing-strategy",
        "--mode",
        "--strategy",
        dest="queueing_mode",
        default=default_q_mode,
        help=f"ClusterQueue queueing strategy: StrictFIFO (strict-fifo) or BestEffortFIFO (best-effort) (default: {default_q_mode})",
    )
    parser.add_argument(
        "--strict-fifo",
        action="store_const",
        const="StrictFIFO",
        dest="queueing_mode",
        help="Shorthand for --queueing-mode StrictFIFO",
    )
    parser.add_argument(
        "--best-effort",
        "--best-effort-fifo",
        action="store_const",
        const="BestEffortFIFO",
        dest="queueing_mode",
        help="Shorthand for --queueing-mode BestEffortFIFO",
    )
    parser.add_argument(
        "--fair-sharing",
        dest="fair_sharing",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Enable Preemption-Based Fair Sharing (fairSharing.preemptionStrategies: "
            "[LessThanOrEqualToFinalShare, LessThanInitialShare]) in Kueue controller configuration "
            "(default: enabled; pass --no-fair-sharing for Classical Preemption; incompatible with --skip-chamber-deploy)"
        ),
    )
    parser.add_argument(
        "--pause-image",
        default=env_str("PAUSE_IMAGE", default_pause_image) if default_pause_image else os.environ.get("PAUSE_IMAGE"),
        help="Container image for simulated KWOK pods",
    )
    parser.add_argument(
        "--scrape-interval",
        default=default_scrape,
        help=f"Prometheus scrape interval for Kueue metrics (default: {default_scrape})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=default_port,
        help=f"Local port for port-forwarding the KWOK kube-apiserver (default: {default_port})",
    )
    parser.add_argument(
        "--prometheus-port",
        type=int,
        default=None,
        help="Local port for port-forwarding chamber Prometheus (default: 19090 + port - 16443)",
    )
    parser.add_argument(
        "--registry-port",
        type=int,
        default=None,
        help=(
            f"Kubernetes NodePort ({NODEPORT_MIN}..{NODEPORT_MAX}) & local port-forward port for the "
            f"per-chamber in-memory image registry (default: {DEFAULT_REGISTRY_BASE_PORT} + port - {DEFAULT_APISERVER_PORT})"
        ),
    )
    parser.add_argument(
        "--registry-memory-limit",
        default=env_str("REGISTRY_MEMORY_LIMIT", DEFAULT_REGISTRY_MEMORY_LIMIT),
        help=(
            "RAM limit (tmpfs sizeLimit & container memory limit, e.g. '4Gi', '8Gi') for the per-chamber "
            f"in-memory image registry (default: {DEFAULT_REGISTRY_MEMORY_LIMIT})"
        ),
    )
    parser.add_argument(
        "--kueue-image",
        default=os.environ.get("KUEUE_IMAGE"),
        help=(
            "Override image for kueue-controller-manager (accepts a local Docker image tag — automatically "
            "pushed into this chamber's in-memory registry — or a localhost:<NodePort>/... or remote registry ref)"
        ),
    )
    parser.add_argument(
        "--kueue-build-dir",
        type=Path,
        default=Path(os.environ["KUEUE_BUILD_DIR"]) if os.environ.get("KUEUE_BUILD_DIR") else None,
        help=(
            "Optional path to a local Kueue source checkout to docker-build and push into this chamber's "
            "in-memory registry before deploying kueue-controller-manager"
        ),
    )
    parser.add_argument(
        "--skip-chamber-deploy",
        "--reuse-chamber",
        action="store_true",
        default=False,
        help="Skip deploying the chamber into the host cluster and connect to an already-running chamber in --namespace",
    )
    parser.add_argument(
        "--cleanup",
        "--clean",
        action="store_true",
        default=env_bool("CLEANUP", False),
        help="Tear down an existing chamber in host namespace --namespace and exit",
    )
    parser.add_argument(
        "--cleanup-workloads",
        "--clean-workloads",
        action="store_true",
        default=env_bool("CLEANUP_WORKLOADS", False),
        help="Delete workloads and Kueue queues in the KWOK cluster and exit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=env_bool("DRY_RUN", False),
        help="Print generated manifests and exit without applying to a cluster",
    )


def resolve_fair_sharing_arg(
    cli_fair_sharing: bool | None,
    skip_chamber_deploy: bool,
    parser: argparse.ArgumentParser,
) -> bool:
    """Resolve effective --fair-sharing boolean and reject overriding it alongside --skip-chamber-deploy."""
    if skip_chamber_deploy and cli_fair_sharing is not None:
        parser.error(
            "--fair-sharing / --no-fair-sharing cannot be combined with --skip-chamber-deploy "
            "because Kueue's controller configuration is only applied when deploying a chamber."
        )
    if cli_fair_sharing is not None:
        return bool(cli_fair_sharing)
    return env_bool("FAIR_SHARING", True)


def _cli_main() -> int:
    """CLI entrypoint for Makefile targets (render, apply, delete, local-kubeconfig)."""
    parser = argparse.ArgumentParser(
        description="Chamber PKI & Kustomize staging utility (generates PKI ad-hoc or picks from cluster)."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    for cmd_name in ("render", "apply"):
        p = sub.add_parser(cmd_name)
        p.add_argument("--namespace", default=env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE))
        p.add_argument("--scrape-interval", default=env_str("PROMETHEUS_SCRAPE_INTERVAL", DEFAULT_SCRAPE_INTERVAL))
        p.add_argument("--with-sample-queues", action="store_true", default=False)
        p.add_argument("--adhoc-only", action="store_true", default=False)
        p.add_argument(
            "--fair-sharing",
            dest="fair_sharing",
            action=argparse.BooleanOptionalAction,
            default=env_bool("FAIR_SHARING", True),
        )

    p_del = sub.add_parser("delete", aliases=["cleanup"])
    p_del.add_argument("--namespace", default=env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE))

    p_kc = sub.add_parser("local-kubeconfig")
    p_kc.add_argument("--namespace", default=env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE))
    p_kc.add_argument("--port", type=int, default=env_int("LOCAL_APISERVER_PORT", DEFAULT_APISERVER_PORT))
    p_kc.add_argument("--out", default=None)

    args = parser.parse_args()

    if args.cmd in ("delete", "cleanup"):
        mgr = ChamberManager(chamber_namespace=args.namespace)
        mgr.cleanup_chamber()
        return 0

    if args.cmd == "local-kubeconfig":
        mgr = ChamberManager(
            chamber_namespace=args.namespace,
            apiserver_port=args.port,
            kubeconfig_out=args.out,
        )
        try:
            kc_path = mgr.write_local_kubeconfig(prefer_cluster=True)
            print(
                f"Wrote {kc_path} (PKI source: {mgr._pki_source}, server: https://127.0.0.1:{args.port})"
            )
        finally:
            mgr.close()
        return 0

    mgr = ChamberManager(
        chamber_namespace=args.namespace,
        scrape_interval=args.scrape_interval,
        enable_fair_sharing=args.fair_sharing,
    )
    try:
        if args.cmd == "apply":
            mgr._ensure_namespace_safe_for_apply(record_creation=False)
        base_dst = mgr.prepare_tmp_deploy_dir(
            initial_nodes=None,
            include_overlays=args.with_sample_queues,
            prefer_cluster_pki=not args.adhoc_only,
        )
        target_k_dir = base_dst.parent if args.with_sample_queues else base_dst
        if args.cmd == "render":
            res = subprocess.run(
                ["kubectl", "--kubeconfig=/dev/null", "kustomize", str(target_k_dir)],
                check=True,
                text=True,
                capture_output=True,
            )
            sys.stdout.write(res.stdout)
        elif args.cmd == "apply":
            mgr._ensure_namespace_safe_for_apply(record_creation=True)
            mgr.host_kubectl(
                ["apply", "--server-side", "--force-conflicts", "-k", str(target_k_dir)],
                stream_output=True,
            )
    finally:
        mgr.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(_cli_main())
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
