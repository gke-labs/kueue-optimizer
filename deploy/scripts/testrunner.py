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

"""kueue-optimizer Chamber Scraper Scenario Test Runner (`testrunner`).

Reads a scraped test case directory conforming to `SCRAPER-SCHEMA.md`
(`infra.json`, `workflows.json`, `execution-time.json` in JSON Lines format)
and replays the scenario inside a kueue-optimizer KWOK + JobSet + Kueue chamber:

1. Indexes the execution time of all workloads from `execution-time.json`
   BEFORE creating any workloads, ClusterQueues, LocalQueues, Nodes, or Pods.
2. Deploys (or reuses) a kueue-optimizer chamber and recreates the scraped cluster
   infrastructure from `infra.json` (`Node`s with exact capacity/allocatable,
   `Namespace`s, `Topology`, `ResourceFlavor`s, `Cohort`s, `ClusterQueue`s,
   and `LocalQueue`s).
3. Places any `Pod`s that were already running at the very beginning (`t0`)
   onto their recorded nodes (`spec.nodeName = payload.placement`) with their
   remaining execution time (`deletionTime - t0`, or a super-long / infinite
   duration if never deleted during the scrape).
4. Replays workflow objects (`JobSet`, `Job`, `LeaderWorkerSet`, `Deployment`,
   `RayJob`, `RayCluster`, standalone Kueue `Pod`) from `workflows.json` based
   on their timestamp offset from the beginning (`t0`):
   - Only creates top-level workflow objects that were not admitted yet
     (`workloadState == "NotAdmitted"`), or were admitted at `t0` and later
     preempted (`workloadState` transitioned to `"NotAdmitted"`).
   - Configures all pods of each created workload with the execution duration
     indexed from `execution-time.json` via the
     `kueue-optimizer.gke.io/pod-execution-time-seconds` annotation.
5. Watches workloads until completion, prints periodic status and optional
   Prometheus Kueue metrics report, and cleans up the chamber when requested.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import heapq
import json
import math
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from typing import Any

from chamber_common import (
    ChamberManager,
    ClusterStatusSnapshot,
    DEFAULT_FALLBACK_EXEC_SEC,
    DEFAULT_INFRA_PODS_NAMESPACE,
    DEFAULT_PAUSE_IMAGE,
    ExecutionTimeIndex,
    RESERVED_POD_LABELS,
    ROOT_DIR,
    WorkloadExecutionInfo,
    add_common_chamber_args,
    build_cohort_manifest,
    build_namespace_manifest,
    build_priority_class_manifest,
    calc_summary_stats,
    collect_known_parent_workflows,
    collect_not_admitted_workflow_admit_epochs,
    docs_to_pretty_yaml,
    env_bool,
    env_str,
    extract_workflow_pods_and_requests,
    filter_initial_running_pods,
    format_duration_hms,
    format_pod_execution_seconds,
    index_execution_times,
    is_child_workflow_owned_by_tracked_parent,
    load_jsonl_events,
    normalize_queueing_strategy,
    parse_duration_seconds,
    parse_k8s_quantity,
    parse_positive_duration_annotation,
    parse_rfc3339_epoch,
    query_cluster_status,
    resolve_fair_sharing_arg,
    sanitize_k8s_name,
    validate_dns_label,
)


DEFAULT_SCENARIO_DIR = ROOT_DIR / "testscenarios" / "demo"
DEFAULT_INFINITE_POD_DURATION_SEC = 864000.0  # 10 days (240h) for pods with no deletionTime

ALLOWED_WORKFLOW_GVKS: frozenset[tuple[str, str]] = frozenset(
    {
        ("batch/v1", "Job"),
        ("jobset.x-k8s.io/v1alpha2", "JobSet"),
        ("leaderworkerset.x-k8s.io/v1", "LeaderWorkerSet"),
        ("apps/v1", "Deployment"),
        ("ray.io/v1", "RayJob"),
        ("ray.io/v1", "RayCluster"),
        ("v1", "Pod"),
    }
)
ALLOWED_WORKFLOW_KINDS: frozenset[str] = frozenset(k for _, k in ALLOWED_WORKFLOW_GVKS)

ALLOWED_INFRA_GVKS: frozenset[tuple[str, str]] = frozenset(
    {
        ("v1", "Node"),
        ("v1", "Namespace"),
        ("v1", "Pod"),
        ("node.k8s.io/v1", "RuntimeClass"),
        ("kueue.x-k8s.io/v1beta2", "ClusterQueue"),
        ("kueue.x-k8s.io/v1beta2", "LocalQueue"),
        ("kueue.x-k8s.io/v1beta2", "ResourceFlavor"),
        ("kueue.x-k8s.io/v1beta2", "Topology"),
        ("kueue.x-k8s.io/v1beta2", "Cohort"),
        ("kueue.x-k8s.io/v1beta2", "WorkloadPriorityClass"),
    }
)


def _require_event_epoch(ev: dict[str, Any], source_file: Path, event_index: int) -> float:
    """Parse a required RFC3339 event timestamp strictly, raising ValueError with file & event index on failure."""
    raw_ts = ev.get("timestamp")
    if raw_ts is None or not str(raw_ts).strip():
        raise ValueError(f"{source_file} event #{event_index}: missing required 'timestamp' field.")
    try:
        ep = parse_rfc3339_epoch(str(raw_ts), strict=True)
    except ValueError as exc:
        raise ValueError(
            f"{source_file} event #{event_index}: invalid RFC3339 timestamp {raw_ts!r}: {exc}"
        ) from exc
    if ep is None:
        raise ValueError(f"{source_file} event #{event_index}: invalid RFC3339 timestamp {raw_ts!r}.")
    return ep


def _parse_optional_event_epoch(
    raw_ts: Any,
    source_file: Path,
    event_index: int,
    field_name: str,
) -> float | None:
    """Parse an optional RFC3339 timestamp strictly, raising ValueError when present and malformed."""
    if raw_ts is None or not str(raw_ts).strip():
        return None
    try:
        return parse_rfc3339_epoch(str(raw_ts), strict=True)
    except ValueError as exc:
        raise ValueError(
            f"{source_file} event #{event_index}: invalid RFC3339 {field_name} {raw_ts!r}: {exc}"
        ) from exc

SYSTEM_NAMESPACES: frozenset[str] = frozenset(
    {
        "default",
        "kube-system",
        "kube-public",
        "kube-node-lease",
        "kueue-system",
        "jobset-system",
    }
)

SERVER_METADATA_FIELDS: tuple[str, ...] = (
    "uid",
    "resourceVersion",
    "creationTimestamp",
    "generation",
    "managedFields",
    "selfLink",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
    "finalizers",
    "ownerReferences",
)


@dataclass
class InitialPodSpec:
    """Representation of a Pod that was already running at the start of the scrape (t0)."""

    name: str
    namespace: str
    placement: str
    resource_request: dict[str, str]
    remaining_duration_sec: float
    is_infinite: bool
    manifest: dict[str, Any]


@dataclass(order=True)
class ScheduledWorkflowItem:
    """Workflow object scheduled for creation at a relative timestamp offset from test start."""

    target_wall_offset_sec: float
    seq: int
    raw_offset_sec: float = field(compare=False)
    raw_offset_from_t0_sec: float = field(compare=False)
    event_timestamp: str = field(compare=False)
    object_kind: str = field(compare=False)
    name: str = field(compare=False)
    namespace: str = field(compare=False)
    local_queue_name: str = field(compare=False)
    workload_state: str = field(compare=False)
    trigger_reason: str = field(compare=False)  # "initial_not_admitted" | "preempted_after_initial_admission"
    execution_duration_sec: float = field(compare=False)
    compressed_duration_sec: float = field(compare=False)
    indexed_from_execution_time: bool = field(compare=False)
    pod_count: int = field(compare=False)
    manifest: dict[str, Any] = field(compare=False)


@dataclass
class ScenarioBundle:
    """Parsed and prepared test scenario ready for execution in a kueue-optimizer chamber."""

    scenario_dir: Path
    infra_path: Path
    workflows_path: Path
    execution_time_path: Path
    exec_index: ExecutionTimeIndex
    t0_epoch: float
    t0_iso: str
    effective_start_epoch: float
    effective_start_iso: str
    initial_jump_raw_sec: float
    initial_jump_wall_sec: float
    expired_initial_pods_count: int
    t_end_epoch: float
    t_end_iso: str
    infra_event_count: int
    workflow_event_count: int
    nodes: list[dict[str, Any]]
    namespaces: list[dict[str, Any]]
    prereq_and_queue_docs: list[dict[str, Any]]
    initial_pods: list[InitialPodSpec]
    scheduled_workflows: list[ScheduledWorkflowItem]
    skipped_already_admitted_workflows: list[tuple[str, str, str]]
    skipped_child_workflows: int
    workload_namespaces: list[str]
    primary_workload_namespace: str
    infra_pods_namespace: str
    cluster_queue_names: list[str]
    queueing_strategy: str


def _clean_metadata(meta: dict[str, Any], keep_annotations: bool = True) -> dict[str, Any]:
    cleaned = json.loads(json.dumps(meta or {}))
    for k in SERVER_METADATA_FIELDS:
        cleaned.pop(k, None)
    if not keep_annotations:
        cleaned.pop("annotations", None)
    return cleaned


def _prepare_node_manifest(node_obj: dict[str, Any], node_idx: int) -> dict[str, Any]:
    """Prepare a scraped v1/Node manifest so KWOK initializes it with its exact scraped capacity."""
    node = json.loads(json.dumps(node_obj))
    meta = _clean_metadata(node.get("metadata") or {})
    name = str(meta.get("name") or f"kwok-node-{node_idx}")
    meta["name"] = name

    labels = dict(meta.get("labels") or {})
    labels["type"] = "kwok"
    labels.setdefault("kubernetes.io/hostname", name)
    labels.setdefault("kubernetes.io/os", "linux")
    labels.setdefault("kubernetes.io/arch", "amd64")
    labels.setdefault("kubernetes.io/role", "agent")
    labels.setdefault("node-role.kubernetes.io/agent", "")
    meta["labels"] = labels

    st = node.get("status") or {}
    alloc = dict(st.get("allocatable") or st.get("capacity") or {"cpu": "4", "memory": "16Gi", "pods": "110"})
    cap = dict(st.get("capacity") or alloc)

    annotations = dict(meta.get("annotations") or {})
    annotations["kwok.x-k8s.io/node"] = "fake"
    annotations["node.alpha.kubernetes.io/ttl"] = "0"
    if "cpu" in alloc:
        annotations["kueue-optimizer.gke.io/node-cpu"] = str(alloc["cpu"])
    if "memory" in alloc:
        annotations["kueue-optimizer.gke.io/node-memory"] = str(alloc["memory"])
    if "pods" in alloc:
        annotations["kueue-optimizer.gke.io/node-pods"] = str(alloc["pods"])
    for r_key, r_val in alloc.items():
        if r_key not in ("cpu", "memory", "pods", "ephemeral-storage"):
            annotations["kueue-optimizer.gke.io/node-accel-resource"] = str(r_key)
            annotations["kueue-optimizer.gke.io/node-accel-count"] = str(r_val)
            break
    meta["annotations"] = annotations

    spec = dict(node.get("spec") or {})
    if not spec.get("podCIDR"):
        octet2 = 100 + (node_idx // 256)
        octet3 = node_idx % 256
        cidr = f"10.{octet2}.{octet3}.0/24"
        spec["podCIDR"] = cidr
        spec["podCIDRs"] = [cidr]

    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": meta,
        "spec": spec,
        "status": {
            "allocatable": alloc,
            "capacity": cap,
            "nodeInfo": st.get("nodeInfo")
            or {
                "architecture": "amd64",
                "kubeProxyVersion": "fake",
                "kubeletVersion": "fake",
                "operatingSystem": "linux",
            },
        },
    }


def _prepare_kueue_or_ns_doc(
    obj: dict[str, Any],
    override_namespace: str | None = None,
    override_queueing_strategy: str | None = None,
    override_cq_fair_sharing_preemption: bool = False,
) -> dict[str, Any]:
    doc = json.loads(json.dumps(obj))
    doc.pop("status", None)
    meta = _clean_metadata(doc.get("metadata") or {})
    kind = str(doc.get("kind") or "")
    if override_namespace and kind == "LocalQueue":
        meta["namespace"] = override_namespace
    doc["metadata"] = meta
    if kind == "Namespace":
        doc.pop("spec", None)
    elif kind == "ClusterQueue":
        cq_spec = doc.setdefault("spec", {})
        if override_queueing_strategy:
            cq_spec["queueingStrategy"] = override_queueing_strategy
        if override_cq_fair_sharing_preemption:
            fs_spec = cq_spec.setdefault("fairSharing", {})
            if fs_spec.get("weight") is None:
                fs_spec["weight"] = 1
            pre_spec = cq_spec.setdefault("preemption", {})
            if pre_spec.get("withinClusterQueue") in (None, "", "Never"):
                pre_spec["withinClusterQueue"] = "LowerPriority"
            if pre_spec.get("reclaimWithinCohort") in (None, "", "Never"):
                pre_spec["reclaimWithinCohort"] = "Any"
            pre_spec.pop("borrowWithinCohort", None)
    return doc


def _extract_existing_pod_exec_seconds(obj: dict[str, Any]) -> float | None:
    """Extract kueue-optimizer.gke.io/pod-execution-time-seconds from an object or its pod templates if present."""
    dur_keys = ("kueue-optimizer.gke.io/pod-execution-time-seconds", "kueue-optimizer.gke.io/real-duration-sec")
    top = parse_positive_duration_annotation((obj.get("metadata") or {}).get("annotations"), *dur_keys)
    if top is not None:
        return top

    spec = obj.get("spec") or {}
    for rj in spec.get("replicatedJobs") or []:
        pod_ann = (((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get(
            "metadata", {}
        ).get("annotations")
        val = parse_positive_duration_annotation(pod_ann, *dur_keys)
        if val is not None:
            return val

    lwt = spec.get("leaderWorkerTemplate") or {}
    if isinstance(lwt, dict):
        for tpl_key in ("leaderTemplate", "workerTemplate"):
            tpl_ann = ((lwt.get(tpl_key) or {}).get("metadata") or {}).get("annotations")
            val = parse_positive_duration_annotation(tpl_ann, *dur_keys)
            if val is not None:
                return val

    for ray_root in (spec, spec.get("rayClusterSpec") or {}):
        if isinstance(ray_root, dict):
            sub_ann = ((ray_root.get("submitterPodTemplate") or {}).get("metadata") or {}).get("annotations")
            val = parse_positive_duration_annotation(sub_ann, *dur_keys)
            if val is not None:
                return val
            head_ann = (((ray_root.get("headGroupSpec") or {}).get("template") or {}).get("metadata") or {}).get("annotations")
            val = parse_positive_duration_annotation(head_ann, *dur_keys)
            if val is not None:
                return val
            for wg in ray_root.get("workerGroupSpecs") or []:
                if isinstance(wg, dict):
                    wg_ann = (((wg.get("template") or {}).get("metadata")) or {}).get("annotations")
                    val = parse_positive_duration_annotation(wg_ann, *dur_keys)
                    if val is not None:
                        return val

    tpl_ann = ((spec.get("template") or {}).get("metadata") or {}).get("annotations")
    return parse_positive_duration_annotation(tpl_ann, *dur_keys)


MAX_K8S_NAME_LEN = 62  # Strictly shorter than 63 characters (< 63)


def _max_workflow_name_len(obj: dict[str, Any], kind: str) -> int:
    """Return the maximum allowed workflow name length so all generated child/workload names are < 63 chars."""
    spec = obj.get("spec") or {}
    if kind == "JobSet":
        max_extra = 13  # len("jobset-") + len("-abcde")
        for idx, rj in enumerate(spec.get("replicatedJobs") or []):
            rj_name = str(rj.get("name") or f"ps-{idx}")
            replicas = max(1, int(rj.get("replicas") or 1))
            j_spec = ((rj.get("template") or {}).get("spec")) or {}
            completions = max(1, int(j_spec.get("completions") or j_spec.get("parallelism") or 1))
            pod_suffix = f"-{rj_name}-{replicas - 1}-{completions - 1}-abcde"
            if len(pod_suffix) > max_extra:
                max_extra = len(pod_suffix)
        return max(10, MAX_K8S_NAME_LEN - max_extra)
    if kind == "Job":
        completions = max(1, int(spec.get("completions") or spec.get("parallelism") or 1))
        max_extra = max(10, len(f"-{completions - 1}-abcde"))
        return max(10, MAX_K8S_NAME_LEN - max_extra)
    if kind in ("LeaderWorkerSet", "RayJob", "RayCluster", "Deployment"):
        return 45
    return MAX_K8S_NAME_LEN


def _inject_pod_execution_time_and_sanitize_workflow(
    raw_obj: dict[str, Any],
    exec_str: str,
    raw_duration_sec: float,
    compressed_duration_sec: float,
    pause_image_override: str | None = None,
    override_namespace: str | None = None,
) -> tuple[dict[str, Any], int]:
    """Prepare a workflow object for submission and inject pod execution duration into all pod templates.

    Returns (prepared_manifest, total_pod_count).
    """
    obj = json.loads(json.dumps(raw_obj))
    obj.pop("status", None)
    kind = str(obj.get("kind") or "")

    meta = _clean_metadata(obj.get("metadata") or {})
    orig_name = str(meta.get("name") or "workflow")
    max_name_len = _max_workflow_name_len(obj, kind)
    if len(orig_name) > max_name_len or not orig_name[0].isalpha():
        base = orig_name[2:] if orig_name.startswith("w-") else orig_name
        meta["name"] = f"w-{sanitize_k8s_name(base, max_len=max_name_len - 2)}"
    if override_namespace and meta.get("namespace"):
        meta["namespace"] = override_namespace
    ann = dict(meta.get("annotations") or {})
    if meta.get("name") != orig_name:
        ann["kueue-optimizer.gke.io/original-name"] = orig_name
    ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = exec_str
    ann["kueue-optimizer.gke.io/real-duration-sec"] = f"{raw_duration_sec:.2f}"
    ann["kueue-optimizer.gke.io/compressed-duration-sec"] = f"{compressed_duration_sec:.3f}"
    meta["annotations"] = ann
    labels = dict(meta.get("labels") or {})
    labels["kueue-optimizer.gke.io/testrunner"] = "true"
    meta["labels"] = labels
    obj["metadata"] = meta

    spec = obj.setdefault("spec", {})

    def _patch_pod_spec(pod_spec: dict[str, Any]) -> None:
        if not isinstance(pod_spec, dict):
            return
        pod_spec.setdefault("restartPolicy", "Never")
        overhead_blk = pod_spec.get("overhead")
        if isinstance(overhead_blk, dict) and overhead_blk and not pod_spec.get("runtimeClassName"):
            pod_spec["runtimeClassName"] = "kueue-optimizer-overhead-rc"
        tolerations = list(pod_spec.get("tolerations") or [])
        if not any(t.get("key") == "kwok.x-k8s.io/node" for t in tolerations if isinstance(t, dict)):
            tolerations.append(
                {
                    "key": "kwok.x-k8s.io/node",
                    "operator": "Equal",
                    "value": "fake",
                    "effect": "NoSchedule",
                }
            )
        pod_spec["tolerations"] = tolerations
        for c_list_key in ("containers", "initContainers"):
            for c in pod_spec.get(c_list_key) or []:
                if isinstance(c, dict):
                    if pause_image_override:
                        c["image"] = pause_image_override
                    elif not c.get("image"):
                        c["image"] = DEFAULT_PAUSE_IMAGE

    def _patch_pod_template(tpl: dict[str, Any]) -> None:
        if not isinstance(tpl, dict):
            return
        t_meta = tpl.setdefault("metadata", {})
        t_ann = dict(t_meta.get("annotations") or {})
        t_ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = exec_str
        t_meta["annotations"] = t_ann
        t_labels = dict(t_meta.get("labels") or {})
        for r_lbl in RESERVED_POD_LABELS:
            t_labels.pop(r_lbl, None)
        if t_labels:
            t_meta["labels"] = t_labels
        elif "labels" in t_meta:
            t_meta.pop("labels", None)
        _patch_pod_spec(tpl.setdefault("spec", {}))

    total_pods = 0

    if kind == "JobSet":
        spec["suspend"] = True
        for rj in spec.get("replicatedJobs") or []:
            replicas = max(1, int(rj.get("replicas") or 1))
            job_tpl_spec = (rj.setdefault("template", {})).setdefault("spec", {})
            parallelism = max(1, int(job_tpl_spec.get("parallelism") or job_tpl_spec.get("completions") or 1))
            total_pods += replicas * parallelism
            _patch_pod_template(job_tpl_spec.setdefault("template", {}))

    elif kind == "Job":
        spec["suspend"] = True
        # Remove controller-generated UID selectors from scraped Jobs
        spec.pop("selector", None)
        spec.pop("manualSelector", None)
        parallelism = max(1, int(spec.get("parallelism") or spec.get("completions") or 1))
        total_pods += parallelism
        _patch_pod_template(spec.setdefault("template", {}))

    elif kind == "Deployment":
        replicas = max(1, int(spec.get("replicas") or 1))
        total_pods += replicas
        tpl = spec.setdefault("template", {})
        t_meta = tpl.setdefault("metadata", {})
        t_ann = dict(t_meta.get("annotations") or {})
        t_ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = exec_str
        t_meta["annotations"] = t_ann
        _patch_pod_spec(tpl.setdefault("spec", {}))

    elif kind == "LeaderWorkerSet":
        replicas = max(1, int(spec.get("replicas") or 1))
        lwt = spec.setdefault("leaderWorkerTemplate", {})
        size = max(1, int(lwt.get("size") or 1))
        total_pods += replicas * size
        if "leaderTemplate" in lwt and isinstance(lwt["leaderTemplate"], dict):
            _patch_pod_template(lwt["leaderTemplate"])
        if "workerTemplate" in lwt and isinstance(lwt["workerTemplate"], dict):
            _patch_pod_template(lwt["workerTemplate"])

    elif kind == "RayJob":
        spec["suspend"] = True
        if "submitterPodTemplate" in spec and isinstance(spec["submitterPodTemplate"], dict):
            _patch_pod_template(spec["submitterPodTemplate"])
            total_pods += 1
        rcs = spec.get("rayClusterSpec") or {}
        if isinstance(rcs, dict):
            if "headGroupSpec" in rcs and isinstance(rcs["headGroupSpec"], dict):
                _patch_pod_template(rcs["headGroupSpec"].setdefault("template", {}))
                total_pods += 1
            for wg in rcs.get("workerGroupSpecs") or []:
                if isinstance(wg, dict):
                    total_pods += max(1, int(wg.get("replicas") or 1))
                    _patch_pod_template(wg.setdefault("template", {}))

    elif kind == "RayCluster":
        spec["suspend"] = True
        if "headGroupSpec" in spec and isinstance(spec["headGroupSpec"], dict):
            _patch_pod_template(spec["headGroupSpec"].setdefault("template", {}))
            total_pods += 1
        for wg in spec.get("workerGroupSpecs") or []:
            if isinstance(wg, dict):
                total_pods += max(1, int(wg.get("replicas") or 1))
                _patch_pod_template(wg.setdefault("template", {}))

    elif kind == "Pod":
        total_pods += 1
        spec.pop("nodeName", None)
        _patch_pod_spec(spec)

    return obj, max(1, total_pods)


def load_scenario_bundle(
    scenario_dir: Path,
    infra_path: Path | None = None,
    workflows_path: Path | None = None,
    execution_time_path: Path | None = None,
    speedup: float = 1.0,
    min_pod_duration_sec: float = 0.1,
    infinite_pod_duration_sec: float = DEFAULT_INFINITE_POD_DURATION_SEC,
    default_exec_sec: float = DEFAULT_FALLBACK_EXEC_SEC,
    initial_window_sec: float = 1.0,
    skip_initial_offset: bool = True,
    pause_image_override: str | None = None,
    override_workload_namespace: str | None = None,
    override_queueing_strategy: str | None = None,
    override_cq_fair_sharing_preemption: bool = False,
    infra_pods_namespace: str = DEFAULT_INFRA_PODS_NAMESPACE,
) -> ScenarioBundle:
    """Index execution-time.json first, then load infra.json and workflows.json into a ScenarioBundle."""
    resolved_dir = scenario_dir.resolve()
    inf_file = (infra_path or (resolved_dir / "infra.json")).resolve()
    wf_file = (workflows_path or (resolved_dir / "workflows.json")).resolve()
    ex_file = (execution_time_path or (resolved_dir / "execution-time.json")).resolve()

    infra_pods_namespace = validate_dns_label(infra_pods_namespace, "infra_pods_namespace")
    if override_workload_namespace:
        override_workload_namespace = validate_dns_label(
            override_workload_namespace, "override_workload_namespace"
        )

    # STEP 1: Index execution times of all workloads BEFORE creating any workloads, CQs, LQs, or Nodes.
    exec_index = index_execution_times(ex_file, strict_timestamps=True)

    # STEP 2: Load infra.json and workflows.json and validate timestamps, GVK allowlists, and DNS labels.
    infra_events = load_jsonl_events(inf_file)
    workflow_events = load_jsonl_events(wf_file)

    infra_event_epochs: list[float] = [
        _require_event_epoch(ev, inf_file, idx_inf) for idx_inf, ev in enumerate(infra_events)
    ]
    workflow_event_epochs: list[float] = [
        _require_event_epoch(ev, wf_file, idx_wf) for idx_wf, ev in enumerate(workflow_events)
    ]

    for idx_wf, ev in enumerate(workflow_events):
        ev_type = str(ev.get("eventType") or "")
        pl = ev.get("payload")
        if not isinstance(pl, dict):
            raise ValueError(f"{wf_file} event #{idx_wf}: missing or non-object 'payload'.")
        kind = str(pl.get("objectKind") or "")
        if kind not in ALLOWED_WORKFLOW_KINDS:
            raise ValueError(
                f"{wf_file} event #{idx_wf}: disallowed workflow objectKind {kind!r}; "
                f"allowed kinds: {sorted(ALLOWED_WORKFLOW_KINDS)}."
            )
        wf_name = validate_dns_label(pl.get("name"), f"{wf_file} event #{idx_wf} payload.name")
        wf_orig_ns = validate_dns_label(
            pl.get("namespace") or "default", f"{wf_file} event #{idx_wf} payload.namespace"
        )
        wf_lq = str(pl.get("localQueueName") or "")
        if wf_lq:
            validate_dns_label(wf_lq, f"{wf_file} event #{idx_wf} payload.localQueueName")
        _parse_optional_event_epoch(pl.get("creationTime"), wf_file, idx_wf, "payload.creationTime")

        raw_obj = pl.get("object")
        if ev_type != "Deleted" or raw_obj is not None:
            if not isinstance(raw_obj, dict):
                raise ValueError(f"{wf_file} event #{idx_wf}: missing or non-object 'payload.object'.")
            obj_api_ver = str(raw_obj.get("apiVersion") or "")
            obj_kind = str(raw_obj.get("kind") or "")
            if obj_kind != kind:
                raise ValueError(
                    f"{wf_file} event #{idx_wf}: payload.objectKind {kind!r} does not match "
                    f"embedded object kind {obj_kind!r}."
                )
            if (obj_api_ver, obj_kind) not in ALLOWED_WORKFLOW_GVKS:
                raise ValueError(
                    f"{wf_file} event #{idx_wf}: disallowed workflow (apiVersion, kind)=({obj_api_ver!r}, {obj_kind!r}); "
                    f"allowed: {sorted(ALLOWED_WORKFLOW_GVKS)}."
                )
            meta = raw_obj.get("metadata")
            if meta is not None and not isinstance(meta, dict):
                raise ValueError(f"{wf_file} event #{idx_wf}: 'payload.object.metadata' must be a JSON object.")
            meta_dict = meta or {}
            if meta_dict.get("name"):
                meta_name = validate_dns_label(
                    meta_dict.get("name"), f"{wf_file} event #{idx_wf} payload.object.metadata.name"
                )
                if meta_name != wf_name:
                    raise ValueError(
                        f"{wf_file} event #{idx_wf}: payload.name {wf_name!r} does not match "
                        f"payload.object.metadata.name {meta_name!r}."
                    )
            if meta_dict.get("namespace"):
                meta_ns = validate_dns_label(
                    meta_dict.get("namespace"), f"{wf_file} event #{idx_wf} payload.object.metadata.namespace"
                )
                if meta_ns != wf_orig_ns:
                    raise ValueError(
                        f"{wf_file} event #{idx_wf}: payload.namespace {wf_orig_ns!r} does not match "
                        f"payload.object.metadata.namespace {meta_ns!r}."
                    )

    all_first_epochs: list[float] = []
    all_last_epochs: list[float] = []
    if infra_event_epochs:
        all_first_epochs.append(min(infra_event_epochs))
        all_last_epochs.append(max(infra_event_epochs))
    if workflow_event_epochs:
        all_first_epochs.append(min(workflow_event_epochs))
        all_last_epochs.append(max(workflow_event_epochs))
    for w_info in exec_index.by_workload_name.values():
        all_last_epochs.extend(w_info.admitted_epochs)
        all_last_epochs.extend(w_info.preempted_epochs)

    t0_epoch = min(all_first_epochs) if all_first_epochs else time.time()
    t_end_epoch = max(all_last_epochs) if all_last_epochs else t0_epoch
    if t_end_epoch < t0_epoch:
        t_end_epoch = t0_epoch
    t0_iso = datetime.fromtimestamp(t0_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    t_end_iso = datetime.fromtimestamp(t_end_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # Collect all known parent workflows (JobSet, RayJob) and earliest NotAdmitted -> Admitted
    # timestamps per namespace (works with both plain names and cluster-scraper --hash-names).
    known_parents = collect_known_parent_workflows(
        workflow_events,
        override_workload_namespace=override_workload_namespace,
    )
    earliest_admit_by_ns = collect_not_admitted_workflow_admit_epochs(
        workflow_events,
        known_parents,
        override_workload_namespace=override_workload_namespace,
        strict_timestamps=True,
    )

    # Track workflow lifecycles in chronological order:
    # We only create workflows that were NotAdmitted initially, OR were Admitted at t0 and later
    # preempted (transitioning to NotAdmitted).
    first_seen_state: dict[tuple[str, str, str], str] = {}
    scheduled_by_key: dict[tuple[str, str, str], ScheduledWorkflowItem] = {}
    latest_obj_by_key: dict[tuple[str, str, str], tuple[dict[str, Any], str]] = {}
    skipped_child_count = 0
    seq = 0

    for idx_wf, ev in enumerate(workflow_events):
        ev_type = str(ev.get("eventType") or "")
        if ev_type == "Deleted":
            continue
        ev_ts_str = str(ev.get("timestamp") or "")
        ev_epoch = workflow_event_epochs[idx_wf]
        wl_state = str(ev.get("workloadState") or "")
        pl = ev.get("payload") or {}
        kind = str(pl.get("objectKind") or "")
        name = str(pl.get("name") or "")
        orig_ns = str(pl.get("namespace") or "default")
        ns = override_workload_namespace or orig_ns
        lq_name = str(pl.get("localQueueName") or "")
        raw_obj = dict(pl.get("object") or {})
        raw_meta = dict(raw_obj.get("metadata") or {})
        raw_meta.setdefault("name", name)
        raw_meta.setdefault("namespace", orig_ns)
        raw_obj["metadata"] = raw_meta

        is_child, parent_js = is_child_workflow_owned_by_tracked_parent(
            raw_obj, kind, ns, known_parents
        )
        if is_child:
            if ev_type == "New":
                skipped_child_count += 1
            continue

        # Infer workloadState if reading an older trace without workloadState field
        if not wl_state:
            suspend_val = (raw_obj.get("spec") or {}).get("suspend")
            wl_state = "NotAdmitted" if suspend_val is True else "Admitted"

        key = (ns, kind, name)
        latest_obj_by_key[key] = (raw_obj, lq_name)

        if key not in first_seen_state:
            first_seen_state[key] = wl_state

        if key in scheduled_by_key:
            continue

        if wl_state == "NotAdmitted":
            trigger_reason = (
                "initial_not_admitted"
                if first_seen_state[key] == "NotAdmitted"
                else "preempted_after_initial_admission"
            )
            fallback_dur = _extract_existing_pod_exec_seconds(raw_obj) or default_exec_sec
            raw_dur, found_in_idx = exec_index.resolve_duration_sec(
                namespace=orig_ns,
                object_kind=kind,
                name=name,
                fallback_sec=fallback_dur,
                parent_jobset_name=parent_js,
            )
            comp_dur = max(min_pod_duration_sec, raw_dur / speedup)
            exec_str = format_pod_execution_seconds(comp_dur, min_seconds=min_pod_duration_sec)
            manifest, pod_cnt = _inject_pod_execution_time_and_sanitize_workflow(
                raw_obj=raw_obj,
                exec_str=exec_str,
                raw_duration_sec=raw_dur,
                compressed_duration_sec=comp_dur,
                pause_image_override=pause_image_override,
                override_namespace=override_workload_namespace,
            )
            raw_off = max(0.0, ev_epoch - t0_epoch)
            wall_off = raw_off / speedup
            scheduled_by_key[key] = ScheduledWorkflowItem(
                target_wall_offset_sec=wall_off,
                seq=seq,
                raw_offset_sec=raw_off,
                raw_offset_from_t0_sec=raw_off,
                event_timestamp=ev_ts_str,
                object_kind=kind,
                name=name,
                namespace=ns,
                local_queue_name=lq_name,
                workload_state=wl_state,
                trigger_reason=trigger_reason,
                execution_duration_sec=raw_dur,
                compressed_duration_sec=comp_dur,
                indexed_from_execution_time=found_in_idx,
                pod_count=pod_cnt,
                manifest=manifest,
            )
            seq += 1

    # Also check if any workload that started Admitted at t0 was recorded as preempted in
    # execution-time.json (PartialExecution with workloadState == "NotAdmitted") even if
    # workflows.json did not have a separate Update event for it.
    for key, initial_st in first_seen_state.items():
        if key in scheduled_by_key or initial_st == "NotAdmitted":
            continue
        ns, kind, name = key
        info = exec_index.lookup(ns, kind, name)
        if info and info.preempted_epochs and key in latest_obj_by_key:
            raw_obj, lq_name = latest_obj_by_key[key]
            preempt_epoch = min(info.preempted_epochs)
            fallback_dur = _extract_existing_pod_exec_seconds(raw_obj) or default_exec_sec
            raw_dur = info.resolved_duration_sec(default_sec=fallback_dur)
            comp_dur = max(min_pod_duration_sec, raw_dur / speedup)
            exec_str = format_pod_execution_seconds(comp_dur, min_seconds=min_pod_duration_sec)
            manifest, pod_cnt = _inject_pod_execution_time_and_sanitize_workflow(
                raw_obj=raw_obj,
                exec_str=exec_str,
                raw_duration_sec=raw_dur,
                compressed_duration_sec=comp_dur,
                pause_image_override=pause_image_override,
                override_namespace=override_workload_namespace,
            )
            raw_off = max(0.0, preempt_epoch - t0_epoch)
            wall_off = raw_off / speedup
            preempt_iso = datetime.fromtimestamp(preempt_epoch, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            scheduled_by_key[key] = ScheduledWorkflowItem(
                target_wall_offset_sec=wall_off,
                seq=seq,
                raw_offset_sec=raw_off,
                raw_offset_from_t0_sec=raw_off,
                event_timestamp=preempt_iso,
                object_kind=kind,
                name=name,
                namespace=ns,
                local_queue_name=lq_name,
                workload_state="NotAdmitted",
                trigger_reason="preempted_after_initial_admission",
                execution_duration_sec=raw_dur,
                compressed_duration_sec=comp_dur,
                indexed_from_execution_time=True,
                pod_count=pod_cnt,
                manifest=manifest,
            )
            seq += 1

    skipped_admitted = [
        key for key in first_seen_state if key not in scheduled_by_key
    ]

    scheduled_workflows = sorted(
        scheduled_by_key.values(),
        key=lambda item: (item.target_wall_offset_sec, item.seq),
    )

    # Immediately jump to the time when the first workload is to be added (unless disabled via
    # --no-skip-initial-offset) so the test does not start by waiting idle for a long period.
    initial_jump_raw_sec = 0.0
    initial_jump_wall_sec = 0.0
    if skip_initial_offset and scheduled_workflows:
        initial_jump_raw_sec = max(0.0, scheduled_workflows[0].raw_offset_from_t0_sec)
        initial_jump_wall_sec = max(0.0, scheduled_workflows[0].target_wall_offset_sec)
        if initial_jump_raw_sec > 0 or initial_jump_wall_sec > 0:
            for item in scheduled_workflows:
                item.raw_offset_sec = max(0.0, item.raw_offset_from_t0_sec - initial_jump_raw_sec)
                item.target_wall_offset_sec = item.raw_offset_sec / speedup

    effective_start_epoch = t0_epoch + initial_jump_raw_sec
    effective_start_iso = datetime.fromtimestamp(effective_start_epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    # Parse infra.json for Nodes, Namespaces, Topology, ResourceFlavors, ClusterQueues, LocalQueues,
    # and Initial Running Pods at t0.
    nodes_by_name: dict[str, dict[str, Any]] = {}
    namespaces_by_name: dict[str, dict[str, Any]] = {}
    runtime_classes_by_name: dict[str, dict[str, Any]] = {}
    topologies_by_name: dict[str, dict[str, Any]] = {}
    flavors_by_name: dict[str, dict[str, Any]] = {}
    cohorts_by_name: dict[str, dict[str, Any]] = {}
    priority_classes_by_name: dict[str, dict[str, Any]] = {}
    cqs_by_name: dict[str, dict[str, Any]] = {}
    lqs_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    pod_new_events: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
    pod_delete_epochs: dict[tuple[str, str], float] = {}

    for idx_inf, ev in enumerate(infra_events):
        ev_type = str(ev.get("eventType") or "")
        ev_epoch = infra_event_epochs[idx_inf]
        pl = ev.get("payload")
        if not isinstance(pl, dict):
            raise ValueError(f"{inf_file} event #{idx_inf}: missing or non-object 'payload'.")
        api_ver = str(pl.get("apiVersion") or "")
        kind = str(pl.get("kind") or "")
        if (api_ver, kind) not in ALLOWED_INFRA_GVKS:
            raise ValueError(
                f"{inf_file} event #{idx_inf}: disallowed infra (apiVersion, kind)=({api_ver!r}, {kind!r}); "
                f"allowed: {sorted(ALLOWED_INFRA_GVKS)}."
            )

        if kind == "Node":
            n_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} Node metadata.name"
            )
            if ev_type != "Deleted" and n_name not in nodes_by_name:
                nodes_by_name[n_name] = _prepare_node_manifest(pl, len(nodes_by_name))

        elif kind == "Namespace":
            ns_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} Namespace metadata.name"
            )
            if ev_type != "Deleted" and ns_name not in SYSTEM_NAMESPACES and ns_name not in namespaces_by_name:
                namespaces_by_name[ns_name] = _prepare_kueue_or_ns_doc(pl)

        elif kind == "RuntimeClass":
            rc_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} RuntimeClass metadata.name"
            )
            if ev_type != "Deleted" and rc_name not in runtime_classes_by_name:
                runtime_classes_by_name[rc_name] = _prepare_kueue_or_ns_doc(pl)

        elif kind == "Topology":
            t_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} Topology metadata.name"
            )
            if ev_type != "Deleted" and t_name not in topologies_by_name:
                topologies_by_name[t_name] = _prepare_kueue_or_ns_doc(pl)

        elif kind == "ResourceFlavor":
            rf_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} ResourceFlavor metadata.name"
            )
            if ev_type != "Deleted" and rf_name not in flavors_by_name:
                flavors_by_name[rf_name] = _prepare_kueue_or_ns_doc(pl)

        elif kind == "Cohort":
            c_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} Cohort metadata.name"
            )
            if ev_type != "Deleted" and c_name not in cohorts_by_name:
                cohorts_by_name[c_name] = _prepare_kueue_or_ns_doc(pl)

        elif kind == "WorkloadPriorityClass":
            pc_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"),
                f"{inf_file} event #{idx_inf} WorkloadPriorityClass metadata.name",
            )
            if ev_type != "Deleted" and pc_name not in priority_classes_by_name:
                priority_classes_by_name[pc_name] = _prepare_kueue_or_ns_doc(pl)

        elif kind == "ClusterQueue":
            cq_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} ClusterQueue metadata.name"
            )
            if ev_type != "Deleted" and cq_name not in cqs_by_name:
                cqs_by_name[cq_name] = _prepare_kueue_or_ns_doc(
                    pl,
                    override_queueing_strategy=override_queueing_strategy,
                    override_cq_fair_sharing_preemption=override_cq_fair_sharing_preemption,
                )

        elif kind == "LocalQueue":
            meta = pl.get("metadata") or {}
            lq_name = validate_dns_label(meta.get("name"), f"{inf_file} event #{idx_inf} LocalQueue metadata.name")
            raw_lq_ns = validate_dns_label(
                meta.get("namespace") or "default", f"{inf_file} event #{idx_inf} LocalQueue metadata.namespace"
            )
            lq_ns = override_workload_namespace or raw_lq_ns
            if ev_type != "Deleted" and (lq_ns, lq_name) not in lqs_by_key:
                lqs_by_key[(lq_ns, lq_name)] = _prepare_kueue_or_ns_doc(
                    pl, override_namespace=override_workload_namespace
                )

        elif kind == "Pod":
            p_meta = pl.get("metadata") or {}
            p_name = validate_dns_label(
                pl.get("name") or p_meta.get("name"), f"{inf_file} event #{idx_inf} Pod name"
            )
            raw_p_ns = validate_dns_label(
                pl.get("namespace") or p_meta.get("namespace") or "default",
                f"{inf_file} event #{idx_inf} Pod namespace",
            )
            if pl.get("placement"):
                validate_dns_label(pl.get("placement"), f"{inf_file} event #{idx_inf} Pod placement")
            _parse_optional_event_epoch(pl.get("schedulingTime"), inf_file, idx_inf, "Pod schedulingTime")
            _parse_optional_event_epoch(pl.get("firstNoticeTime"), inf_file, idx_inf, "Pod firstNoticeTime")
            del_ts = _parse_optional_event_epoch(pl.get("deletionTime"), inf_file, idx_inf, "Pod deletionTime")
            p_ns = override_workload_namespace or raw_p_ns
            p_key = (p_ns, p_name)
            if ev_type == "New" and p_key not in pod_new_events:
                pod_new_events[p_key] = (ev_epoch, pl)
            elif ev_type == "Deleted" and p_key not in pod_delete_epochs:
                pod_delete_epochs[p_key] = del_ts if del_ts is not None else ev_epoch

    # Determine which pods in infra.json are running at the start of the test
    # (handles plain names, cluster-scraper --hash-names anonymized names, and synthetic traces).
    not_admitted_wf_names = [
        (w.namespace, w.name) for w in scheduled_workflows if w.trigger_reason == "initial_not_admitted"
    ]
    qualifying_initial_pods, expired_initial_pods_count = filter_initial_running_pods(
        pod_new_events,
        pod_delete_epochs,
        t0_epoch=t0_epoch,
        effective_start_epoch=effective_start_epoch,
        initial_window_sec=initial_window_sec,
        not_admitted_workflow_names=not_admitted_wf_names,
        earliest_admit_by_ns=earliest_admit_by_ns,
        strict_timestamps=True,
    )

    initial_pods: list[InitialPodSpec] = []
    pause_img = pause_image_override or DEFAULT_PAUSE_IMAGE

    for p_ns, p_name, _ev_epoch, pl, del_ep in qualifying_initial_pods:
        if del_ep is not None and del_ep > effective_start_epoch:
            rem_raw = del_ep - effective_start_epoch
            rem_compressed = max(min_pod_duration_sec, rem_raw / speedup)
            is_inf = False
        else:
            rem_compressed = max(min_pod_duration_sec, infinite_pod_duration_sec / speedup)
            is_inf = True

        exec_str = format_pod_execution_seconds(rem_compressed, min_seconds=min_pod_duration_sec)
        placement = str(pl.get("placement") or "")
        reqs = dict(pl.get("resourceRequest") or {"cpu": "1", "memory": "1Gi"})
        base_p_name = p_name[2:] if p_name.startswith("p-") else p_name
        k8s_pod_name = (
            f"p-{sanitize_k8s_name(base_p_name, max_len=MAX_K8S_NAME_LEN - 2)}"
            if (len(p_name) > MAX_K8S_NAME_LEN or not p_name[0].isalpha())
            else p_name
        )

        pod_manifest: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": k8s_pod_name,
                "namespace": infra_pods_namespace,
                "labels": {
                    "kueue-optimizer.gke.io/initial-pod": "true",
                    "kueue-optimizer.gke.io/testrunner": "true",
                },
                "annotations": {
                    "kueue-optimizer.gke.io/pod-execution-time-seconds": exec_str,
                    "kueue-optimizer.gke.io/original-name": p_name,
                    "kueue-optimizer.gke.io/original-namespace": p_ns,
                },
            },
            "spec": {
                "restartPolicy": "Never",
                "tolerations": [
                    {
                        "key": "kwok.x-k8s.io/node",
                        "operator": "Equal",
                        "value": "fake",
                        "effect": "NoSchedule",
                    }
                ],
                "containers": [
                    {
                        "name": "pause",
                        "image": pause_img,
                        "resources": {
                            "requests": reqs,
                            "limits": dict(reqs),
                        },
                    }
                ],
            },
        }
        if placement:
            pod_manifest["spec"]["nodeName"] = placement

        initial_pods.append(
            InitialPodSpec(
                name=p_name,
                namespace=infra_pods_namespace,
                placement=placement,
                resource_request=reqs,
                remaining_duration_sec=rem_compressed,
                is_infinite=is_inf,
                manifest=pod_manifest,
            )
        )

    # Ensure all namespaces referenced by LocalQueues, InitialPods, or ScheduledWorkflows exist.
    # Always create the dedicated infra_pods_namespace with explicit Kueue exclusion labels so
    # Kueue's mutating/validating webhooks never add schedulingGates to pre-existing infra pods.
    referenced_namespaces: set[str] = set(namespaces_by_name.keys())
    referenced_namespaces.add(infra_pods_namespace)
    for lq_ns, _ in lqs_by_key:
        referenced_namespaces.add(lq_ns)
    for ipod in initial_pods:
        referenced_namespaces.add(ipod.namespace)
    for sw in scheduled_workflows:
        referenced_namespaces.add(sw.namespace)

    namespaces_by_name[infra_pods_namespace] = build_namespace_manifest(
        infra_pods_namespace,
        part_of="kueue-optimizer-testrunner",
        exclude_from_kueue=True,
    )
    for ns_name in sorted(referenced_namespaces):
        if ns_name not in SYSTEM_NAMESPACES and ns_name not in namespaces_by_name:
            namespaces_by_name[ns_name] = build_namespace_manifest(
                ns_name,
                part_of="kueue-optimizer-testrunner",
                exclude_from_kueue=(ns_name == infra_pods_namespace),
            )

    # Synthesize any Cohort or WorkloadPriorityClass objects referenced by ClusterQueues or Workflows
    for cq_doc in cqs_by_name.values():
        c_name = (cq_doc.get("spec") or {}).get("cohortName")
        if c_name:
            c_name_clean = validate_dns_label(c_name, "ClusterQueue spec.cohortName")
            if c_name_clean not in cohorts_by_name:
                cohorts_by_name[c_name_clean] = build_cohort_manifest(c_name_clean)

    for idx_pc, sw in enumerate(scheduled_workflows):
        w_meta = sw.manifest.get("metadata") or {}
        w_labels = w_meta.get("labels") or {}
        w_ann = w_meta.get("annotations") or {}
        pc_name = w_labels.get("kueue.x-k8s.io/priority-class")
        if pc_name:
            pc_name_clean = validate_dns_label(pc_name, "kueue.x-k8s.io/priority-class")
            if pc_name_clean not in priority_classes_by_name:
                prio_val = int(w_ann.get("kueue-optimizer.gke.io/priority") or (1000 - idx_pc))
                priority_classes_by_name[pc_name_clean] = build_priority_class_manifest(
                    pc_name_clean, value=prio_val
                )
        # Synthesize RuntimeClass if any pod template specifies runtimeClassName / pod.spec.overhead
        w_spec = sw.manifest.get("spec") or {}
        candidate_pod_specs: list[dict[str, Any]] = []
        if sw.object_kind == "JobSet":
            for rj in w_spec.get("replicatedJobs") or []:
                candidate_pod_specs.append((((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get("spec") or {})
        elif sw.object_kind in ("Job", "Deployment"):
            candidate_pod_specs.append(((w_spec.get("template") or {}).get("spec")) or {})
        elif sw.object_kind == "Pod":
            candidate_pod_specs.append(w_spec)
        for p_sp in candidate_pod_specs:
            if not isinstance(p_sp, dict):
                continue
            rc_raw = str(p_sp.get("runtimeClassName") or "").strip()
            ov_b = p_sp.get("overhead") if isinstance(p_sp.get("overhead"), dict) else None
            if rc_raw:
                rc_n = validate_dns_label(rc_raw, "PodSpec runtimeClassName")
                if rc_n not in runtime_classes_by_name:
                    rc_doc: dict[str, Any] = {
                        "apiVersion": "node.k8s.io/v1",
                        "kind": "RuntimeClass",
                        "metadata": {"name": rc_n},
                        "handler": "kwok",
                    }
                    if ov_b:
                        rc_doc["overhead"] = {"podFixed": {str(k): str(v) for k, v in ov_b.items()}}
                    runtime_classes_by_name[rc_n] = rc_doc

    prereq_and_queue_docs: list[dict[str, Any]] = []
    prereq_and_queue_docs.extend(namespaces_by_name.values())
    prereq_and_queue_docs.extend(runtime_classes_by_name.values())
    prereq_and_queue_docs.extend(topologies_by_name.values())
    prereq_and_queue_docs.extend(flavors_by_name.values())
    prereq_and_queue_docs.extend(cohorts_by_name.values())
    prereq_and_queue_docs.extend(priority_classes_by_name.values())
    prereq_and_queue_docs.extend(cqs_by_name.values())
    prereq_and_queue_docs.extend(lqs_by_key.values())

    workload_namespaces = sorted(referenced_namespaces) if referenced_namespaces else ["default"]
    primary_ns = (
        override_workload_namespace
        or (scheduled_workflows[0].namespace if scheduled_workflows else workload_namespaces[0])
    )

    detected_strategy = override_queueing_strategy or "StrictFIFO"
    for cq_doc in cqs_by_name.values():
        st_val = (cq_doc.get("spec") or {}).get("queueingStrategy")
        if st_val:
            detected_strategy = str(st_val)
            break

    return ScenarioBundle(
        scenario_dir=resolved_dir,
        infra_path=inf_file,
        workflows_path=wf_file,
        execution_time_path=ex_file,
        exec_index=exec_index,
        t0_epoch=t0_epoch,
        t0_iso=t0_iso,
        effective_start_epoch=effective_start_epoch,
        effective_start_iso=effective_start_iso,
        initial_jump_raw_sec=initial_jump_raw_sec,
        initial_jump_wall_sec=initial_jump_wall_sec,
        expired_initial_pods_count=expired_initial_pods_count,
        t_end_epoch=t_end_epoch,
        t_end_iso=t_end_iso,
        infra_event_count=len(infra_events),
        workflow_event_count=len(workflow_events),
        nodes=list(nodes_by_name.values()),
        namespaces=list(namespaces_by_name.values()),
        prereq_and_queue_docs=prereq_and_queue_docs,
        initial_pods=initial_pods,
        scheduled_workflows=scheduled_workflows,
        skipped_already_admitted_workflows=skipped_admitted,
        skipped_child_workflows=skipped_child_count,
        workload_namespaces=workload_namespaces,
        primary_workload_namespace=primary_ns,
        infra_pods_namespace=infra_pods_namespace,
        cluster_queue_names=sorted(cqs_by_name.keys()),
        queueing_strategy=detected_strategy,
    )


def apply_scenario_queues_and_wait(
    mgr: ChamberManager,
    prereq_and_queue_docs: list[dict[str, Any]],
    timeout_sec: float = 60.0,
) -> None:
    """Apply Namespaces, Topologies, ResourceFlavors, Cohorts, PriorityClasses, ClusterQueues, and LocalQueues."""
    cq_names = {d["metadata"]["name"] for d in prereq_and_queue_docs if d.get("kind") == "ClusterQueue"}
    lq_keys = {
        (d["metadata"].get("namespace", "default"), d["metadata"]["name"])
        for d in prereq_and_queue_docs
        if d.get("kind") == "LocalQueue"
    }

    ns_docs = [d for d in prereq_and_queue_docs if d.get("kind") == "Namespace"]
    other_prereq_docs = [
        d for d in prereq_and_queue_docs if d.get("kind") not in ("Namespace", "ClusterQueue", "LocalQueue")
    ]
    cq_lq_docs = [d for d in prereq_and_queue_docs if d.get("kind") in ("ClusterQueue", "LocalQueue")]
    mgr.apply_manifests_parallel(ns_docs, max_workers=16)
    mgr.apply_manifests_parallel(other_prereq_docs, max_workers=16)
    mgr.apply_manifests_parallel(cq_lq_docs, max_workers=16)

    if not cq_names and not lq_keys:
        return

    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        st_cq, raw_cq = mgr.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/clusterqueues")
        st_lq, raw_lq = mgr.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/localqueues")
        if st_cq == 200 and st_lq == 200 and raw_cq and raw_lq:
            active_cqs = {
                it["metadata"]["name"]
                for it in json.loads(raw_cq.decode("utf-8")).get("items", [])
                if any(
                    c.get("type") == "Active" and c.get("status") == "True"
                    for c in (it.get("status") or {}).get("conditions") or []
                )
            }
            active_lqs = {
                (it["metadata"].get("namespace", "default"), it["metadata"]["name"])
                for it in json.loads(raw_lq.decode("utf-8")).get("items", [])
                if any(
                    c.get("type") == "Active" and c.get("status") == "True"
                    for c in (it.get("status") or {}).get("conditions") or []
                )
            }
            if cq_names.issubset(active_cqs) and lq_keys.issubset(active_lqs):
                return
        time.sleep(0.5)
    raise RuntimeError("Timed out waiting for ClusterQueues and LocalQueues to become Active")


def cleanup_scenario_workloads(mgr: ChamberManager, bundle: ScenarioBundle) -> None:
    """Delete scenario workloads, pods, queues, flavors, topologies, and nodes from the KWOK cluster."""
    print(
        f"==> Cleaning up scenario workloads and queues in KWOK cluster "
        f"(chamber: '{mgr.chamber_namespace}', namespaces: {', '.join(bundle.workload_namespaces)})...",
        flush=True,
    )
    mgr.ensure_kwok_connection()
    for ns in bundle.workload_namespaces:
        ns_valid = validate_dns_label(ns, "workload namespace")
        for resource in ("jobsets", "jobs", "deployments", "pods", "workloads", "localqueues"):
            mgr.kwok_kubectl(
                ["-n", ns_valid, "delete", resource, "--all", "--ignore-not-found"],
                check=False,
            )
    mgr.kwok_kubectl(["delete", "clusterqueues", "--all", "--ignore-not-found"], check=False)
    mgr.kwok_kubectl(["delete", "resourceflavors", "--all", "--ignore-not-found"], check=False)
    mgr.kwok_kubectl(["delete", "topologies", "--all", "--ignore-not-found"], check=False)
    mgr.kwok_kubectl(["delete", "cohorts", "--all", "--ignore-not-found"], check=False)
    mgr.kwok_kubectl(["delete", "workloadpriorityclasses", "--all", "--ignore-not-found"], check=False)
    mgr.kwok_kubectl(["delete", "runtimeclasses", "--all", "--ignore-not-found"], check=False)
    for ns_doc in bundle.namespaces:
        ns_name = validate_dns_label(ns_doc["metadata"]["name"], "scenario namespace")
        if ns_name not in SYSTEM_NAMESPACES:
            mgr.kwok_kubectl(["delete", "namespace", "--ignore-not-found", "--", ns_name], check=False)
    print("==> Scenario workload & queue cleanup complete.", flush=True)


def print_scenario_outline(
    bundle: ScenarioBundle,
    speedup: float,
    enable_fair_sharing: bool | None = True,
    *,
    reused_chamber: bool = False,
) -> None:
    """Print a detailed description of the test case: length, execution-time index, nodes & initial usage, queues, and workflows."""
    idx = bundle.exec_index
    durations = [
        info.resolved_duration_sec()
        for info in idx.by_workload_name.values()
        if (info.full_duration_sec is not None or info.partial_durations_sec)
    ]
    dur_stats = calc_summary_stats(durations)
    min_d = dur_stats["min"]
    max_d = dur_stats["max"]
    avg_d = dur_stats["avg"]
    p50_d = dur_stats["p50"]
    p90_d = dur_stats["p90"]

    cq_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "ClusterQueue"]
    lq_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "LocalQueue"]
    rf_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "ResourceFlavor"]
    topo_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "Topology"]
    cohort_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "Cohort"]

    raw_scenario_span = max(0.0, bundle.t_end_epoch - bundle.t0_epoch)
    comp_scenario_span = raw_scenario_span / speedup

    first_raw_off = bundle.scheduled_workflows[0].raw_offset_sec if bundle.scheduled_workflows else 0.0
    last_raw_off = bundle.scheduled_workflows[-1].raw_offset_sec if bundle.scheduled_workflows else 0.0
    first_wall_off = bundle.scheduled_workflows[0].target_wall_offset_sec if bundle.scheduled_workflows else 0.0
    last_wall_off = bundle.scheduled_workflows[-1].target_wall_offset_sec if bundle.scheduled_workflows else 0.0
    arrival_window_raw = max(0.0, last_raw_off - first_raw_off)
    arrival_window_wall = max(0.0, last_wall_off - first_wall_off)

    est_min_finish_raw = max(
        (w.raw_offset_sec + w.execution_duration_sec for w in bundle.scheduled_workflows),
        default=0.0,
    )
    raw_exec_span = (
        max(0.0, idx.last_event_epoch - bundle.effective_start_epoch)
        if idx.last_event_epoch is not None
        else est_min_finish_raw
    )
    comp_exec_span = raw_exec_span / speedup

    # 1. Compute Node pool groups & total cluster capacity
    ignore_label_keys = {
        "beta.kubernetes.io/arch",
        "beta.kubernetes.io/os",
        "kubernetes.io/arch",
        "kubernetes.io/hostname",
        "kubernetes.io/os",
        "kubernetes.io/role",
        "node-role.kubernetes.io/agent",
        "kueue-optimizer.gke.io/node-role",
        "kueue-optimizer.gke.io/trace-node",
        "cloud.google.com/gke-nodepool",
        "cloud.google.com/gce-topology-block",
        "cloud.google.com/gce-topology-subblock",
        "cloud.google.com/gke-tpu-partition-4x4x4-id",
        "cloud.google.com/gke-tpu-partition-2x4x4-id",
        "cloud.google.com/gke-tpu-partition-2x2x4-id",
        "cloud.google.com/gke-tpu-partition-2x2x2-id",
        "cloud.google.com/gke-tpu-partition-2x2x1-id",
    }
    node_groups: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    total_cluster_cpu = 0.0
    total_cluster_mem_gi = 0.0
    total_cluster_pods = 0
    total_cluster_accels: dict[str, int] = {}

    for nd in bundle.nodes:
        meta = nd.get("metadata") or {}
        labels = meta.get("labels") or {}
        alloc = (nd.get("status") or {}).get("allocatable") or {}
        role = str(labels.get("kueue-optimizer.gke.io/node-role") or labels.get("kubernetes.io/role") or "agent")
        node_type = str(
            labels.get("cloud.google.com/gke-tpu-accelerator")
            or labels.get("cloud.google.com/gke-accelerator")
            or labels.get("cloud.google.com/machine-family")
            or labels.get("type")
            or f"{role}-node"
        )
        cpu_str = str(alloc.get("cpu", "0"))
        mem_str = str(alloc.get("memory", "0"))
        pods_str = str(alloc.get("pods", "110"))
        cpu_val = parse_k8s_quantity(cpu_str, "cpu")
        mem_val = parse_k8s_quantity(mem_str, "memory")
        pods_val = int(round(parse_k8s_quantity(pods_str, "pods")))

        total_cluster_cpu += cpu_val
        total_cluster_mem_gi += mem_val
        total_cluster_pods += pods_val

        accel_parts: list[str] = []
        for rk, rv in sorted(alloc.items()):
            if rk not in ("cpu", "memory", "pods", "ephemeral-storage") and not rk.startswith("hugepages-"):
                accel_parts.append(f"{rk}={rv}")
                total_cluster_accels[rk] = total_cluster_accels.get(rk, 0) + int(round(parse_k8s_quantity(rv, rk)))
        accel_str = ", ".join(accel_parts) if accel_parts else "-"

        sel_parts = [f"{k}={v}" for k, v in sorted(labels.items()) if k not in ignore_label_keys]
        sel_str = ", ".join(sel_parts) if sel_parts else "type=kwok"

        grp_key = (role, node_type, cpu_str, mem_str, accel_str, sel_str)
        grp = node_groups.get(grp_key)
        if grp is None:
            grp = {
                "role": role,
                "node_type": node_type,
                "count": 0,
                "cpu_val": cpu_val,
                "mem_val": mem_val,
                "pods_val": pods_val,
                "cpu_str": cpu_str,
                "mem_str": mem_str,
                "pods_str": pods_str,
                "accel_str": accel_str,
                "selectors": sel_str,
                "node_names": [],
            }
            node_groups[grp_key] = grp
        grp["count"] += 1
        grp["node_names"].append(str(meta.get("name") or ""))

    # 2. Compute Initial Usage at t0 from bundle.initial_pods
    init_cpu_used = 0.0
    init_mem_used_gi = 0.0
    init_accels_used: dict[str, int] = {}
    init_pods_by_node: dict[str, list[InitialPodSpec]] = {}
    finite_init_durs: list[float] = []
    infinite_init_count = 0

    for ipod in bundle.initial_pods:
        c_req = parse_k8s_quantity(ipod.resource_request.get("cpu"), "cpu")
        m_req = parse_k8s_quantity(ipod.resource_request.get("memory"), "memory")
        init_cpu_used += c_req
        init_mem_used_gi += m_req
        for rk, rv in ipod.resource_request.items():
            if rk not in ("cpu", "memory", "pods", "ephemeral-storage") and not rk.startswith("hugepages-"):
                init_accels_used[rk] = init_accels_used.get(rk, 0) + int(round(parse_k8s_quantity(rv, rk)))
        init_pods_by_node.setdefault(ipod.placement or "<unassigned>", []).append(ipod)
        if ipod.is_infinite:
            infinite_init_count += 1
        else:
            finite_init_durs.append(ipod.remaining_duration_sec)

    cpu_util_pct = (100.0 * init_cpu_used / total_cluster_cpu) if total_cluster_cpu > 0 else 0.0
    mem_util_pct = (100.0 * init_mem_used_gi / total_cluster_mem_gi) if total_cluster_mem_gi > 0 else 0.0
    pod_util_pct = (100.0 * len(bundle.initial_pods) / total_cluster_pods) if total_cluster_pods > 0 else 0.0
    active_init_nodes = len([n for n in init_pods_by_node if n != "<unassigned>"])

    # 3. Map LocalQueues -> ClusterQueues and aggregate scheduled workflows per queue
    lq_to_cq: dict[tuple[str, str], str] = {}
    lqs_for_cq: dict[str, list[str]] = {}
    for lq in lq_docs:
        lq_meta = lq.get("metadata") or {}
        lq_ns = str(lq_meta.get("namespace") or "default")
        lq_name = str(lq_meta.get("name") or "")
        cq_target = str((lq.get("spec") or {}).get("clusterQueue") or "")
        lq_to_cq[(lq_ns, lq_name)] = cq_target
        lqs_for_cq.setdefault(cq_target, []).append(f"{lq_ns}/{lq_name}")

    wf_stats_by_cq: dict[str, dict[str, float | int]] = {}
    wf_counts_by_kind: dict[str, int] = {}
    wf_counts_by_reason: dict[str, int] = {}
    total_wf_pods = 0
    total_wf_cpu = 0.0
    total_wf_mem_gi = 0.0

    for sw in bundle.scheduled_workflows:
        wf_counts_by_kind[sw.object_kind] = wf_counts_by_kind.get(sw.object_kind, 0) + 1
        wf_counts_by_reason[sw.trigger_reason] = wf_counts_by_reason.get(sw.trigger_reason, 0) + 1
        _, wf_tot_reqs, _ = extract_workflow_pods_and_requests(sw.manifest, sw.object_kind)
        w_cpu = wf_tot_reqs.get("cpu", 0.0)
        w_mem = wf_tot_reqs.get("memory", 0.0)
        total_wf_pods += sw.pod_count
        total_wf_cpu += w_cpu
        total_wf_mem_gi += w_mem

        cq_name = lq_to_cq.get((sw.namespace, sw.local_queue_name), sw.local_queue_name or "<unmapped>")
        st = wf_stats_by_cq.setdefault(cq_name, {"workflows": 0, "pods": 0, "cpu": 0.0, "mem_gi": 0.0})
        st["workflows"] = int(st["workflows"]) + 1
        st["pods"] = int(st["pods"]) + sw.pod_count
        st["cpu"] = float(st["cpu"]) + w_cpu
        st["mem_gi"] = float(st["mem_gi"]) + w_mem

    print("")
    print("========================================================================================")
    print("  KUEUE-OPTIMIZER TESTRUNNER — SCENARIO & INFRASTRUCTURE DESCRIPTION")
    print("========================================================================================")
    print(f"  Scenario Directory        : {bundle.scenario_dir}")
    print(
        f"  Input Trace Files         : infra.json ({bundle.infra_event_count} events), "
        f"workflows.json ({bundle.workflow_event_count} events), "
        f"execution-time.json ({idx.total_events} events)"
    )
    print(f"  Time Compression (--speedup): {speedup:g}x")
    if enable_fair_sharing is None:
        fs_mode_desc = "inherited from running chamber (--skip-chamber-deploy)"
    else:
        base_fs_desc = (
            "enabled (LessThanOrEqualToFinalShare, LessThanInitialShare)"
            if enable_fair_sharing
            else "disabled (Classical Preemption)"
        )
        fs_mode_desc = f"{base_fs_desc} [detected from live chamber]" if reused_chamber else base_fs_desc
    print(f"  Kueue FairSharing Config  : {fs_mode_desc}")
    print("")
    print("  [1] TEST CASE LENGTH & TIMELINE")
    print(f"      Scenario Start (t0)   : {bundle.t0_iso}")
    if bundle.initial_jump_raw_sec > 0:
        print(
            f"      First Workload Start  : {bundle.effective_start_iso} "
            f"(immediately jumping ahead +{bundle.initial_jump_raw_sec:.2f}s raw / "
            f"+{bundle.initial_jump_wall_sec:.2f}s wall)"
        )
    print(f"      Scenario End (t_end)  : {bundle.t_end_iso}")
    print(
        f"      Total Scraped Span    : {raw_scenario_span:.2f}s ({format_duration_hms(raw_scenario_span)}) "
        f"-> {comp_scenario_span:.2f}s ({format_duration_hms(comp_scenario_span)}) at {speedup:g}x"
    )
    print(
        f"      Workflow Arrival Span : +{first_raw_off:.2f}s .. +{last_raw_off:.2f}s from test start "
        f"(window={arrival_window_raw:.2f}s raw / {arrival_window_wall:.2f}s wall)"
    )
    print(
        f"      Active Execution Span : {raw_exec_span:.2f}s ({format_duration_hms(raw_exec_span)}) "
        f"-> {comp_exec_span:.2f}s ({format_duration_hms(comp_exec_span)}) at {speedup:g}x "
        f"(test start to last workload completion)"
    )
    print("")
    print("  [2] WORKLOAD EXECUTION TIME INDEX (execution-time.json, indexed before cluster setup)")
    print(
        f"      Indexed Workloads     : {len(idx.by_workload_name)} workloads "
        f"(Admitted={idx.admitted_events}, PartialExecution={idx.partial_events}, FullExecution={idx.full_events})"
    )
    print(
        f"      Execution Durations   : min={min_d:.1f}s, p50={p50_d:.1f}s, avg={avg_d:.1f}s, "
        f"p90={p90_d:.1f}s, max={max_d:.1f}s "
        f"(at {speedup:g}x: min={min_d / speedup:.2f}s .. max={max_d / speedup:.2f}s)"
    )
    print("")
    accel_tot_str = (
        ", " + ", ".join(f"{v} {k}" for k, v in sorted(total_cluster_accels.items()))
        if total_cluster_accels
        else ""
    )
    print(
        f"  [3] CLUSTER NODES & INITIAL USAGE AT START ({len(bundle.nodes)} Nodes: "
        f"{total_cluster_cpu:g} CPU, {total_cluster_mem_gi:g}Gi Memory, {total_cluster_pods} Pods{accel_tot_str})"
    )
    if node_groups:
        print(
            f"      {'ROLE':<8}  {'NODE TYPE':<16}  {'NODES':>5}  "
            f"{'PER-NODE CAPACITY':<30}  {'POOL TOTAL CAPACITY':<28}  {'NODE LABELS / SELECTORS'}"
        )
        print(
            f"      {'-' * 8}  {'-' * 16}  {'-' * 5}  "
            f"{'-' * 30}  {'-' * 28}  {'-' * 26}"
        )
        for grp in node_groups.values():
            cnt = grp["count"]
            per_node = f"{grp['cpu_str']} CPU, {grp['mem_str']} Mem, {grp['pods_str']} pods"
            if grp["accel_str"] != "-":
                per_node += f", {grp['accel_str']}"
            tot_pool = f"{cnt * grp['cpu_val']:g} CPU, {cnt * grp['mem_val']:g}Gi Mem, {cnt * grp['pods_val']} pods"
            names_preview = (
                f"{grp['node_names'][0]}..{grp['node_names'][-1]}"
                if cnt > 2
                else ", ".join(grp["node_names"])
            )
            print(
                f"      {grp['role']:<8}  {grp['node_type']:<16}  {cnt:>5}  "
                f"{per_node:<30}  {tot_pool:<28}  {grp['selectors']} ({names_preview})"
            )
    else:
        print("      No nodes recorded in infra.json")

    print("")
    expired_note = (
        f" (+ {bundle.expired_initial_pods_count} pod(s) from t0 already finished before +{bundle.initial_jump_raw_sec:.2f}s jump)"
        if bundle.expired_initial_pods_count > 0
        else ""
    )
    print(
        f"      Initial Usage at Start: {len(bundle.initial_pods)} running pod(s) in Kueue-excluded namespace "
        f"'{bundle.infra_pods_namespace}' (kueue.x-k8s.io/exclude=true) across "
        f"{active_init_nodes}/{len(bundle.nodes)} active node(s) "
        f"({len(bundle.nodes) - active_init_nodes} idle){expired_note}"
    )
    accel_load_parts: list[str] = []
    for rk, tot_acc in sorted(total_cluster_accels.items()):
        used_acc = init_accels_used.get(rk, 0)
        acc_pct = (100.0 * used_acc / tot_acc) if tot_acc > 0 else 0.0
        accel_load_parts.append(f"{rk}={used_acc}/{tot_acc} ({acc_pct:.1f}%)")
    accel_load_str = (", " + ", ".join(accel_load_parts)) if accel_load_parts else ""
    print(
        f"      Initial Resource Load : CPU={init_cpu_used:g}/{total_cluster_cpu:g} cores ({cpu_util_pct:.1f}%), "
        f"Memory={init_mem_used_gi:g}/{total_cluster_mem_gi:g}Gi ({mem_util_pct:.1f}%), "
        f"Pods={len(bundle.initial_pods)}/{total_cluster_pods} ({pod_util_pct:.1f}%){accel_load_str}"
    )
    if bundle.initial_pods:
        if finite_init_durs:
            fin_str = (
                f"{len(finite_init_durs)} finite (remaining {min(finite_init_durs):.1f}s .. "
                f"{max(finite_init_durs):.1f}s)"
            )
        else:
            fin_str = "0 finite"
        inf_str = f"{infinite_init_count} infinite/super-long"
        print(f"      Initial Pod Lifetimes : {fin_str}, {inf_str}")
        sorted_init_nodes = sorted(init_pods_by_node.items())
        for n_name, npods in sorted_init_nodes[:8]:
            n_cpu = sum(parse_k8s_quantity(p.resource_request.get("cpu"), "cpu") for p in npods)
            n_mem = sum(parse_k8s_quantity(p.resource_request.get("memory"), "memory") for p in npods)
            print(f"        - {n_name}: {len(npods)} pod(s) requesting {n_cpu:g} CPU, {n_mem:g}Gi Memory")
        if len(sorted_init_nodes) > 8:
            print(f"        - ... and {len(sorted_init_nodes) - 8} more node(s) with initial pods")

    print("")
    print(
        f"  [4] KUEUE QUEUES — QUOTA, PREEMPTION, FAIR SHARING & RECLAIM SETTINGS "
        f"({len(cq_docs)} ClusterQueue(s), {len(lq_docs)} LocalQueue(s), {len(rf_docs)} ResourceFlavor(s))"
    )
    if rf_docs or topo_docs:
        rf_summaries: list[str] = []
        for rf in rf_docs:
            rf_name = (rf.get("metadata") or {}).get("name", "?")
            rf_labels = (rf.get("spec") or {}).get("nodeLabels") or {}
            rf_topo = (rf.get("spec") or {}).get("topologyName")
            lbl_s = ",".join(f"{k}={v}" for k, v in sorted(rf_labels.items())) or "any"
            if rf_topo:
                lbl_s += f", topology={rf_topo}"
            rf_summaries.append(f"{rf_name}[{lbl_s}]")
        print(f"      ResourceFlavors       : {', '.join(rf_summaries)}")
        if topo_docs:
            topo_summaries = [
                f"{(t.get('metadata') or {}).get('name', '?')}[levels="
                + "->".join(str(lv.get("nodeLabel", "")) for lv in ((t.get("spec") or {}).get("levels") or []))
                + "]"
                for t in topo_docs
            ]
            print(f"      Topologies            : {', '.join(topo_summaries)}")
        if cohort_docs:
            cohort_summaries: list[str] = []
            for ch in cohort_docs:
                ch_name = (ch.get("metadata") or {}).get("name", "?")
                ch_rgs = ((ch.get("spec") or {}).get("resourceGroups") or [])
                ch_q_parts: list[str] = []
                for rg in ch_rgs:
                    for flv in rg.get("flavors") or []:
                        for res in flv.get("resources") or []:
                            ch_q_parts.append(f"{res.get('name')}={res.get('nominalQuota')}")
                if ch_q_parts:
                    cohort_summaries.append(f"{ch_name}[nominalQuota: {', '.join(ch_q_parts)}]")
                else:
                    cohort_summaries.append(ch_name)
            print(f"      Cohorts               : {', '.join(cohort_summaries)}")

    if cq_docs:
        print(
            f"      {'CLUSTER QUEUE':<15}  {'COHORT':<14}  {'STRATEGY':<14}  {'FAIR SHARING':<14}  "
            f"{'PREEMPTION (WITHIN / RECLAIM / BORROW)':<52}  {'LOCAL QUEUES'}"
        )
        print(
            f"      {'-' * 15}  {'-' * 14}  {'-' * 14}  {'-' * 14}  "
            f"{'-' * 52}  {'-' * 20}"
        )
        for cq in cq_docs:
            cq_name = str((cq.get("metadata") or {}).get("name") or "?")
            spec = cq.get("spec") or {}
            cohort = str(spec.get("cohortName") or spec.get("cohort") or "-")
            strategy = str(spec.get("queueingStrategy") or "BestEffortFIFO")
            stop_pol = str(spec.get("stopPolicy") or "None")
            if stop_pol != "None":
                strategy = f"{strategy}(stop={stop_pol})"

            fs_obj = spec.get("fairSharing") or {}
            fs_weight = fs_obj.get("weight")
            fs_str = f"weight={fs_weight}" if fs_weight is not None else "none"

            preempt = spec.get("preemption") or {}
            within_cq = str(preempt.get("withinClusterQueue") or "Never")
            reclaim_cohort = str(preempt.get("reclaimWithinCohort") or "Never")
            borrow_obj = preempt.get("borrowWithinCohort") or {}
            borrow_pol = str(borrow_obj.get("policy") or "Never")
            max_prio = borrow_obj.get("maxPriorityThreshold")
            if max_prio is not None:
                borrow_pol += f"(maxPrio={max_prio})"
            preempt_str = f"within={within_cq}, reclaim={reclaim_cohort}, borrow={borrow_pol}"

            mapped_lqs = ", ".join(lqs_for_cq.get(cq_name, [])) or "-"
            print(
                f"      {cq_name:<15}  {cohort:<14}  {strategy:<14}  {fs_str:<14}  "
                f"{preempt_str:<52}  {mapped_lqs}"
            )

            fung = spec.get("flavorFungibility") or {}
            fung_str = ""
            if fung:
                fung_str = (
                    f" | flavorFungibility(borrow={fung.get('whenCanBorrow', 'MayStopSearch')}, "
                    f"preempt={fung.get('whenCanPreempt', 'TryNextFlavor')})"
                )

            for rg_idx, rg in enumerate(spec.get("resourceGroups") or []):
                covered = ",".join(str(x) for x in (rg.get("coveredResources") or []))
                for flv in rg.get("flavors") or []:
                    flv_name = str(flv.get("name") or "default")
                    q_parts: list[str] = []
                    for res in flv.get("resources") or []:
                        r_name = str(res.get("name") or "")
                        nom = str(res.get("nominalQuota") or "0")
                        b_lim = res.get("borrowingLimit")
                        l_lim = res.get("lendingLimit")
                        extras: list[str] = []
                        if b_lim is not None:
                            extras.append(f"borrowLimit={b_lim}")
                        if l_lim is not None:
                            extras.append(f"lendLimit={l_lim}")
                        extra_s = f" [{', '.join(extras)}]" if extras else ""
                        q_parts.append(f"{r_name}: nominal={nom}{extra_s}")
                    print(
                        f"        -> flavor='{flv_name}' (covered=[{covered}]): "
                        + ", ".join(q_parts)
                        + (fung_str if rg_idx == 0 else "")
                    )

            w_st = wf_stats_by_cq.get(cq_name)
            if w_st:
                print(
                    f"        -> scenario load: {w_st['workflows']} workflow(s), "
                    f"{w_st['pods']} pod(s), total demand={w_st['cpu']:g} CPU, {w_st['mem_gi']:g}Gi Memory"
                )
    else:
        print("      No ClusterQueues recorded in infra.json")

    print("")
    kinds_str = ", ".join(f"{k}={v}" for k, v in sorted(wf_counts_by_kind.items())) or "none"
    reasons_str = ", ".join(f"{k}={v}" for k, v in sorted(wf_counts_by_reason.items())) or "none"
    print(
        f"  [5] WORKFLOWS TO CREATE ({len(bundle.scheduled_workflows)} top-level NotAdmitted workflows, "
        f"{total_wf_pods} total pods, {total_wf_cpu:g} CPU, {total_wf_mem_gi:g}Gi Memory)"
    )
    print(f"      By Resource Kind      : {kinds_str}")
    print(f"      By Trigger Reason     : {reasons_str}")
    print(
        f"      Skipped Workflows     : {len(bundle.skipped_already_admitted_workflows)} already-admitted at t0, "
        f"{bundle.skipped_child_workflows} controller-managed child object(s)"
    )
    print("========================================================================================", flush=True)


def run_scenario_replay(
    mgr: ChamberManager,
    bundle: ScenarioBundle,
    speedup: float,
    max_run_seconds: float | None,
    status_interval_sec: float,
    wait_for_completion: bool,
) -> tuple[float, float, int, int]:
    """Place initial pods and immediately start creating workflow objects from the first workload timestamp."""
    if bundle.initial_jump_raw_sec > 0:
        expired_msg = (
            f"; {bundle.expired_initial_pods_count} initial pod(s) from t0 already finished before jump"
            if bundle.expired_initial_pods_count > 0
            else ""
        )
        print(
            f"==> Immediately jumping to first workload timestamp {bundle.effective_start_iso} "
            f"(+{bundle.initial_jump_raw_sec:.2f}s raw / +{bundle.initial_jump_wall_sec:.2f}s wall from t0{expired_msg})...",
            flush=True,
        )

    if bundle.initial_pods:
        print(
            f"==> Placing {len(bundle.initial_pods)} initial running Pod(s) into Kueue-excluded "
            f"namespace '{bundle.infra_pods_namespace}' onto their nodes...",
            flush=True,
        )
        mgr.apply_manifests_parallel([p.manifest for p in bundle.initial_pods], max_workers=16)
        for ipod in bundle.initial_pods[:10]:
            dur_label = "infinite/super-long" if ipod.is_infinite else f"{ipod.remaining_duration_sec:.2f}s"
            print(
                f"    [Initial Pod] {ipod.namespace}/{ipod.name} -> node={ipod.placement} "
                f"requests={ipod.resource_request} remaining={dur_label}",
                flush=True,
            )
        if len(bundle.initial_pods) > 10:
            print(f"    ... and {len(bundle.initial_pods) - 10} more initial pods.", flush=True)

    event_heap: list[ScheduledWorkflowItem] = list(bundle.scheduled_workflows)
    heapq.heapify(event_heap)

    total_planned = len(bundle.scheduled_workflows)
    submitted_count = 0
    total_pods_submitted = sum(1 for _ in bundle.initial_pods)

    stop_requested = False

    def _sig_handler(signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        print(f"\n==> Received signal {signum}; stopping testrunner gracefully...", flush=True)
        stop_requested = True

    old_sigint = signal.signal(signal.SIGINT, _sig_handler)
    old_sigterm = signal.signal(signal.SIGTERM, _sig_handler)

    start_epoch = time.time()
    t0_mono = time.monotonic()
    next_status_mono = t0_mono

    def _emit_status(now_mono: float, note: str = "") -> ClusterStatusSnapshot:
        wall_elapsed = now_mono - t0_mono
        sim_elapsed = wall_elapsed * speedup
        snap = query_cluster_status(
            mgr,
            workload_namespaces=bundle.workload_namespaces,
            include_kueue_workloads=True,
        )
        if event_heap:
            next_in = max(0.0, event_heap[0].target_wall_offset_sec - wall_elapsed)
            next_str = f"next_in={next_in:.2f}s"
        else:
            next_str = "next_in=none"

        cqs_str = " ".join(snap.cq_summary) if snap.cq_summary else "none"
        ts_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        note_str = f" | {note}" if note else ""
        done_count = max(snap.workloads_finished, snap.jobsets_completed + snap.jobsets_failed)
        print(
            f"[{ts_str}] wall={format_duration_hms(wall_elapsed)} sim={sim_elapsed:.1f}s "
            f"| submitted={submitted_count}/{total_planned} queued={len(event_heap)} ({next_str}) "
            f"| workloads[adm={snap.workloads_admitted},pend={snap.workloads_pending},done={done_count}/{total_planned}] "
            f"| pods[run={snap.pods_running},pend={snap.pods_pending},succ={snap.pods_succeeded}] "
            f"| cqs: {cqs_str}{note_str}",
            flush=True,
        )
        return snap

    try:
        while not stop_requested:
            now_mono = time.monotonic()
            wall_elapsed = now_mono - t0_mono

            if max_run_seconds is not None and max_run_seconds > 0 and wall_elapsed >= max_run_seconds:
                _emit_status(now_mono, note="run duration reached")
                print(
                    f"==> Configured test run duration ({max_run_seconds:.1f}s) reached after submitting "
                    f"{submitted_count}/{total_planned} workflows.",
                    flush=True,
                )
                break

            due_batch: list[ScheduledWorkflowItem] = []
            while event_heap and event_heap[0].target_wall_offset_sec <= (time.monotonic() - t0_mono):
                due_batch.append(heapq.heappop(event_heap))
                if len(due_batch) >= 100:
                    break

            if due_batch:
                mgr.apply_manifests_parallel([item.manifest for item in due_batch], max_workers=12)
                now_off = time.monotonic() - t0_mono
                for item in due_batch:
                    submitted_count += 1
                    total_pods_submitted += item.pod_count

                if len(due_batch) == 1:
                    it = due_batch[0]
                    src_tag = "exec-time.json" if it.indexed_from_execution_time else "fallback"
                    print(
                        f"  + [t={now_off:.2f}s wall / +{it.raw_offset_sec:.2f}s from start (t0+{it.raw_offset_from_t0_sec:.2f}s)] "
                        f"Created {it.object_kind} '{it.namespace}/{it.name}' "
                        f"(state={it.workload_state}, reason={it.trigger_reason}, "
                        f"queue={it.local_queue_name or '-'}, pods={it.pod_count}, "
                        f"pod_exec={it.compressed_duration_sec:.2f}s [raw={it.execution_duration_sec:.1f}s, {src_tag}])",
                        flush=True,
                    )
                else:
                    first_it = due_batch[0]
                    last_it = due_batch[-1]
                    print(
                        f"  + [t={now_off:.2f}s wall / +{first_it.raw_offset_sec:.2f}s..+{last_it.raw_offset_sec:.2f}s from start "
                        f"(t0+{first_it.raw_offset_from_t0_sec:.2f}s..+{last_it.raw_offset_from_t0_sec:.2f}s)] "
                        f"Created batch of {len(due_batch)} workflows "
                        f"('{first_it.name}' .. '{last_it.name}', pods={sum(x.pod_count for x in due_batch)}, "
                        f"pod_exec={first_it.compressed_duration_sec:.2f}s..{last_it.compressed_duration_sec:.2f}s)",
                        flush=True,
                    )
                continue

            now_mono = time.monotonic()
            if now_mono >= next_status_mono:
                snap = _emit_status(now_mono)
                while next_status_mono <= now_mono:
                    next_status_mono += status_interval_sec

                if not event_heap:
                    if not wait_for_completion:
                        print("==> All scenario workflows submitted (--no-wait-for-completion).", flush=True)
                        break
                    done_count = max(snap.workloads_finished, snap.jobsets_completed + snap.jobsets_failed)
                    if submitted_count > 0 and done_count >= submitted_count:
                        print(f"==> All {submitted_count} submitted scenario workflows completed!", flush=True)
                        break
                    if submitted_count == 0 and snap.pods_running == 0:
                        print("==> No pending workflows or running initial pods remaining.", flush=True)
                        break

            now_mono = time.monotonic()
            wall_elapsed = now_mono - t0_mono
            wake_candidates = [next_status_mono - now_mono]
            if event_heap:
                wake_candidates.append(event_heap[0].target_wall_offset_sec - wall_elapsed)
            if max_run_seconds is not None and max_run_seconds > 0:
                wake_candidates.append(max_run_seconds - wall_elapsed)
            sleep_sec = max(0.01, min(wake_candidates))
            time.sleep(min(sleep_sec, 1.0))

    finally:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    end_epoch = time.time()
    return start_epoch, end_epoch, submitted_count, total_pods_submitted


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "kueue-optimizer Scenario Test Runner (`testrunner`): replays a scraped test case "
            "(`infra.json`, `workflows.json`, `execution-time.json` per SCRAPER-SCHEMA.md) in a chamber."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "scenario_pos",
        nargs="?",
        default=None,
        help="Optional positional path to the scraped scenario directory (default: testscenarios/demo)",
    )
    parser.add_argument(
        "--scenario-dir",
        "--test-dir",
        "-d",
        dest="scenario_dir",
        type=Path,
        default=Path(env_str("SCENARIO_DIR", str(DEFAULT_SCENARIO_DIR))),
        help=f"Path to scraped test scenario directory (default: {DEFAULT_SCENARIO_DIR})",
    )
    parser.add_argument(
        "--infra-file",
        type=Path,
        default=None,
        help="Explicit path to infra.json (defaults to <scenario-dir>/infra.json)",
    )
    parser.add_argument(
        "--workflows-file",
        type=Path,
        default=None,
        help="Explicit path to workflows.json (defaults to <scenario-dir>/workflows.json)",
    )
    parser.add_argument(
        "--execution-time-file",
        type=Path,
        default=None,
        help="Explicit path to execution-time.json (defaults to <scenario-dir>/execution-time.json)",
    )

    add_common_chamber_args(
        parser,
        default_workload_namespace="",
        default_queueing_mode="",
        default_pause_image=DEFAULT_PAUSE_IMAGE,
    )

    parser.add_argument(
        "--speedup",
        "--time-compression",
        "-s",
        dest="speedup",
        type=float,
        default=1.0,
        help="Time compression factor N (> 0): divides timestamp offsets and pod execution times by N (default: 1.0)",
    )
    parser.add_argument(
        "--duration",
        "--run-duration",
        "--max-run-seconds",
        dest="duration",
        default=None,
        help="Maximum wall-clock duration to run the test (e.g. '60s', '5m'; default: until all workflows complete)",
    )
    parser.add_argument(
        "--status-interval",
        default="3s",
        help="Interval for printing periodic cluster & workflow status updates (default: 3s)",
    )
    parser.add_argument(
        "--default-exec-seconds",
        default=str(DEFAULT_FALLBACK_EXEC_SEC),
        help=(
            f"Fallback pod execution time in seconds for workloads without an entry in execution-time.json "
            f"(default: {DEFAULT_FALLBACK_EXEC_SEC})"
        ),
    )
    parser.add_argument(
        "--infinite-pod-duration",
        default=str(int(DEFAULT_INFINITE_POD_DURATION_SEC)),
        help=(
            "Execution duration for initial running pods present at t0 that have no deletionTime in infra.json "
            f"(default: {int(DEFAULT_INFINITE_POD_DURATION_SEC)}s = 10d)"
        ),
    )
    parser.add_argument(
        "--min-pod-duration",
        default="0.1s",
        help="Minimum KWOK pod execution duration after time compression (default: 0.1s)",
    )
    parser.add_argument(
        "--initial-window",
        default="1.0s",
        help="Window after t0 within which Pod 'New' events in infra.json are treated as initial pods (default: 1.0s)",
    )
    parser.add_argument(
        "--skip-initial-offset",
        "--zero-first-offset",
        dest="skip_initial_offset",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Immediately jump to the timestamp of the first workload to be added (default: true)",
    )
    parser.add_argument(
        "--wait-for-completion",
        "--watch",
        dest="wait_for_completion",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Wait for all submitted workflows to complete before finishing (default: true)",
    )
    parser.add_argument(
        "--metrics-report",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run the Prometheus Kueue metrics report at the end of the scenario run (default: true)",
    )
    parser.add_argument(
        "--no-cleanup",
        "--keep",
        dest="no_cleanup",
        action="store_true",
        default=env_bool("NO_CLEANUP", False),
        help="Keep the chamber running after the test run completes instead of tearing it down",
    )
    parser.add_argument(
        "--cleanup-after",
        dest="cleanup_after",
        action="store_true",
        default=False,
        help="Tear down the chamber after watching workflows to completion (default behavior)",
    )
    parser.add_argument(
        "--infra-pods-namespace",
        default=env_str("INFRA_PODS_NAMESPACE", DEFAULT_INFRA_PODS_NAMESPACE),
        help=(
            "Dedicated namespace (explicitly excluded from Kueue webhooks/management) for initial running "
            f"Pods from infra.json (default: {DEFAULT_INFRA_PODS_NAMESPACE})"
        ),
    )
    parser.add_argument(
        "--cq-fair-sharing-preemption",
        "--override-cq-fair-sharing-preemption",
        dest="cq_fair_sharing_preemption",
        action="store_true",
        default=False,
        help=(
            "Override scraped infra.json ClusterQueues to enable Preemption-Based Fair Sharing "
            "(sets fairSharing.weight=1 when unset, withinClusterQueue=LowerPriority, and reclaimWithinCohort=Any; "
            "default: False so infra.json ClusterQueue specs are replayed verbatim)"
        ),
    )
    parser.add_argument(
        "--dry-run-limit",
        type=int,
        default=5,
        help="Number of sample Nodes, Initial Pods, and Workflows to print in --dry-run mode (0 = print all; default: 5)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.scenario_pos:
        args.scenario_dir = Path(args.scenario_pos)

    if args.speedup <= 0:
        parser.error("--speedup / --time-compression must be > 0")

    enable_fair_sharing = resolve_fair_sharing_arg(
        args.fair_sharing,
        args.skip_chamber_deploy and not args.cleanup and not args.cleanup_workloads,
        parser,
    )

    if args.cleanup_after:
        args.no_cleanup = False
    elif args.skip_chamber_deploy:
        args.no_cleanup = True
    elif not args.wait_for_completion:
        args.no_cleanup = True

    override_q_strategy = (
        normalize_queueing_strategy(args.queueing_mode) if args.queueing_mode else None
    )
    override_wl_ns = args.workload_namespace.strip() if args.workload_namespace else None

    status_interval_sec = parse_duration_seconds(args.status_interval) or 3.0
    min_pod_duration_sec = parse_duration_seconds(args.min_pod_duration) or 0.1
    default_exec_sec = parse_duration_seconds(args.default_exec_seconds) or DEFAULT_FALLBACK_EXEC_SEC
    infinite_pod_sec = (
        parse_duration_seconds(args.infinite_pod_duration) or DEFAULT_INFINITE_POD_DURATION_SEC
    )
    initial_window_sec = parse_duration_seconds(args.initial_window) or 1.0
    max_run_seconds = parse_duration_seconds(args.duration)

    if args.cleanup:
        if args.dry_run:
            print(f"[dry-run] Would tear down chamber in host namespace '{args.namespace}'.")
            return 0
        mgr = ChamberManager(
            chamber_namespace=args.namespace,
            workload_namespace=override_wl_ns or "default",
            apiserver_port=args.port,
            prometheus_port=args.prometheus_port,
            scrape_interval=args.scrape_interval,
            registry_port=args.registry_port,
            registry_memory_limit=args.registry_memory_limit,
            kueue_image=args.kueue_image,
            kueue_build_dir=args.kueue_build_dir,
            enable_fair_sharing=enable_fair_sharing,
        )
        mgr.cleanup_chamber()
        return 0

    infra_pods_ns = args.infra_pods_namespace.strip() or DEFAULT_INFRA_PODS_NAMESPACE

    bundle = load_scenario_bundle(
        scenario_dir=args.scenario_dir,
        infra_path=args.infra_file,
        workflows_path=args.workflows_file,
        execution_time_path=args.execution_time_file,
        speedup=args.speedup,
        min_pod_duration_sec=min_pod_duration_sec,
        infinite_pod_duration_sec=infinite_pod_sec,
        default_exec_sec=default_exec_sec,
        initial_window_sec=initial_window_sec,
        skip_initial_offset=args.skip_initial_offset,
        pause_image_override=args.pause_image,
        override_workload_namespace=override_wl_ns,
        override_queueing_strategy=override_q_strategy,
        override_cq_fair_sharing_preemption=args.cq_fair_sharing_preemption,
        infra_pods_namespace=infra_pods_ns,
    )

    mgr = ChamberManager(
        chamber_namespace=args.namespace,
        workload_namespace=bundle.primary_workload_namespace,
        apiserver_port=args.port,
        prometheus_port=args.prometheus_port,
        scrape_interval=args.scrape_interval,
        registry_port=args.registry_port,
        registry_memory_limit=args.registry_memory_limit,
        kueue_image=args.kueue_image,
        kueue_build_dir=args.kueue_build_dir,
        enable_fair_sharing=enable_fair_sharing,
    )

    if args.cleanup_workloads:
        if args.dry_run:
            print(
                f"[dry-run] Would delete workloads and queues in chamber '{args.namespace}' "
                f"(namespaces: {', '.join(bundle.workload_namespaces)})."
            )
            return 0
        try:
            cleanup_scenario_workloads(mgr, bundle)
        finally:
            mgr.close()
        return 0

    if args.dry_run:
        mgr.close()
        outline_fs = None if args.skip_chamber_deploy else enable_fair_sharing
        print_scenario_outline(
            bundle,
            speedup=args.speedup,
            enable_fair_sharing=outline_fs,
            reused_chamber=args.skip_chamber_deploy,
        )
        limit = args.dry_run_limit
        sample_nodes = bundle.nodes[:limit] if limit > 0 else bundle.nodes
        sample_pods = bundle.initial_pods[:limit] if limit > 0 else bundle.initial_pods
        sample_wfs = bundle.scheduled_workflows[:limit] if limit > 0 else bundle.scheduled_workflows
        print(f"# === KWOK Nodes (showing {len(sample_nodes)} of {len(bundle.nodes)}) ===")
        print(docs_to_pretty_yaml(sample_nodes), end="")
        print("---\n# === Namespaces, ResourceFlavors, Topologies, ClusterQueues, LocalQueues ===")
        print(docs_to_pretty_yaml(bundle.prereq_and_queue_docs), end="")
        if bundle.initial_pods:
            print(
                f"---\n# === Initial Running Pods at t0 (showing {len(sample_pods)} of {len(bundle.initial_pods)}) ==="
            )
            print(docs_to_pretty_yaml([p.manifest for p in sample_pods]), end="")
        print(
            f"---\n# === Scheduled Workflows (showing {len(sample_wfs)} of {len(bundle.scheduled_workflows)}) ==="
        )
        print(docs_to_pretty_yaml([w.manifest for w in sample_wfs]), end="")
        return 0

    banner_fs = (
        mgr.detect_live_fair_sharing_enabled() if args.skip_chamber_deploy else enable_fair_sharing
    )
    print_scenario_outline(
        bundle,
        speedup=args.speedup,
        enable_fair_sharing=banner_fs,
        reused_chamber=args.skip_chamber_deploy,
    )

    should_cleanup_chamber = not args.no_cleanup
    run_succeeded = False
    try:
        if not args.skip_chamber_deploy:
            mgr.deploy_chamber(initial_nodes=bundle.nodes)
        elif args.kueue_build_dir or args.kueue_image:
            if args.kueue_build_dir:
                built_ref, built_dig = mgr.build_and_push_kueue_image(
                    args.kueue_build_dir,
                    tag="adhoc",
                    stage_if_needed=True,
                )
                mgr.update_kueue_image(
                    f"{built_ref}@{built_dig}" if built_dig else built_ref,
                    push_if_local=False,
                )
            elif args.kueue_image:
                mgr.update_kueue_image(args.kueue_image)

        print(
            f"==> Connecting to chamber KWOK kube-apiserver in '{args.namespace}' on localhost:{args.port}...",
            flush=True,
        )
        mgr.ensure_kwok_connection()
        mgr.wait_for_crds()
        mgr.apply_kwok_stages()
        mgr.ensure_kueue_webhook_namespace_exclusions(excluded_namespaces=(bundle.infra_pods_namespace,))

        for ns in bundle.workload_namespaces:
            mgr.workload_namespace = ns
            mgr.clear_workload_namespace(delete_sample_cq=True)
        mgr.workload_namespace = bundle.primary_workload_namespace

        if bundle.nodes:
            print(f"==> Syncing {len(bundle.nodes)} KWOK Node(s) from infra.json...", flush=True)
            mgr.sync_kwok_nodes(bundle.nodes, remove_stale=True)

        print(
            f"==> Applying Namespaces, ResourceFlavors, ClusterQueues ({', '.join(bundle.cluster_queue_names)}), "
            f"and LocalQueues from infra.json...",
            flush=True,
        )
        apply_scenario_queues_and_wait(mgr, bundle.prereq_and_queue_docs)

        start_epoch, end_epoch, submitted_count, total_pods = run_scenario_replay(
            mgr=mgr,
            bundle=bundle,
            speedup=args.speedup,
            max_run_seconds=max_run_seconds,
            status_interval_sec=status_interval_sec,
            wait_for_completion=args.wait_for_completion,
        )

        final_snap = query_cluster_status(
            mgr,
            workload_namespaces=bundle.workload_namespaces,
            include_kueue_workloads=True,
        )
        observed_pods = (
            final_snap.pods_running
            + final_snap.pods_pending
            + final_snap.pods_succeeded
            + final_snap.pods_failed
        )
        total_pods = max(total_pods, observed_pods)

        print("")
        print("================================================================================")
        print("  KUEUE-OPTIMIZER TESTRUNNER — EXECUTION SUMMARY")
        print("================================================================================")
        print(f"  Scenario Directory      : {bundle.scenario_dir}")
        print(f"  Chamber Registry        : localhost:{mgr.registry_port} (in-memory tmpfs {mgr.registry_memory_limit})")
        if mgr._resolved_kueue_image or mgr.kueue_image:
            print(f"  Kueue Image             : {mgr._resolved_kueue_image or mgr.kueue_image}")
        print(f"  Wall-Clock Duration     : {end_epoch - start_epoch:.1f}s ({format_duration_hms(end_epoch - start_epoch)})")
        print(f"  Initial Pods Placed     : {len(bundle.initial_pods)}")
        print(f"  Workflows Submitted     : {submitted_count}/{len(bundle.scheduled_workflows)} ({total_pods} total pods)")
        print(
            f"  Workloads Status        : admitted={final_snap.workloads_admitted}, "
            f"pending={final_snap.workloads_pending}, finished={final_snap.workloads_finished}"
        )
        print(
            f"  KWOK Pods Status        : running={final_snap.pods_running}, "
            f"pending={final_snap.pods_pending}, succeeded={final_snap.pods_succeeded}, "
            f"failed={final_snap.pods_failed}"
        )
        print(f"  ClusterQueues Status    : {' '.join(final_snap.cq_summary)}")
        print("================================================================================", flush=True)

        if args.metrics_report:
            mgr.run_kueue_metrics_report(
                queueing_strategy=bundle.queueing_strategy,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                jobset_count=submitted_count,
                total_pods=total_pods,
                wait_for_scrape=True,
            )
        run_succeeded = True
    finally:
        if should_cleanup_chamber and run_succeeded:
            print("", flush=True)
            mgr.cleanup_chamber()
        elif should_cleanup_chamber and mgr.created_namespace_in_this_run:
            print("", flush=True)
            try:
                mgr.cleanup_chamber()
            except Exception as cleanup_exc:
                print(
                    f"WARNING: Chamber cleanup after error failed for '{args.namespace}': {cleanup_exc}",
                    file=sys.stderr,
                    flush=True,
                )
        else:
            mgr.close()

    if should_cleanup_chamber:
        return 0

    print(f"\nChamber '{args.namespace}' left running (--no-cleanup).")
    print(f"  export KUBECONFIG={mgr.kubeconfig_path}")
    print(f"  kubectl -n {bundle.primary_workload_namespace} get clusterqueues,localqueues,jobsets,workloads,pods")
    print(f"To clean up workloads only: python3 deploy/scripts/testrunner.py --cleanup-workloads --namespace {args.namespace}")
    print(f"To tear down the chamber  : python3 deploy/scripts/testrunner.py --cleanup --namespace {args.namespace}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        sys.exit(0)
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode or 1)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
