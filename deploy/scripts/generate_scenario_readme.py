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

"""Generate comprehensive README.md summaries for kueue-optimizer test scenario directories.

Analyzes the three SCRAPER-SCHEMA.md files in a test scenario directory
(`infra.json`, `workflows.json`, `execution-time.json`) and generates a detailed
Markdown summary (`README.md`) covering:
  1. Executive overview of the scenario
  2. Cluster Nodes, distinct hardware/configuration node groups, and a full
     formatted YAML manifest of a sample node from each distinct group
  3. Cluster Topology definition and node distribution across every topology level
  4. Initial running Pods at t0 and initial cluster utilization across all resources
  5. Kueue configuration: ResourceFlavors, Topologies, WorkloadPriorityClasses,
     Cohorts, ClusterQueues (quotas, borrowing/lending limits, preemption &
     fair-sharing policies), Namespaces, and LocalQueues
  6. Test timeline (from first unadmitted workload arrival to last arrival, plus
     arrival rate distribution over time)
  7. Workload analysis: priority distribution, exact cluster saturation
     sum(workload_size * duration) / (test_length * cluster_capacity) per
     priority and per LocalQueue (and vs. target ClusterQueue quota), and ASCII
     histograms of workload execution durations (aggregated and per priority).

Usage:
  # Single scenario directory:
  python3 deploy/scripts/generate_scenario_readme.py testscenarios/demo

  # All scenario directories containing infra.json under testscenarios/:
  python3 deploy/scripts/generate_scenario_readme.py --all
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
import math
import os
from pathlib import Path
import re
import sys
from typing import Any

from chamber_common import (
    calc_summary_stats,
    collect_known_parent_workflows,
    collect_not_admitted_workflow_admit_epochs,
    extract_pod_spec_resources,
    extract_workflow_pods_and_requests,
    filter_initial_running_pods,
    format_duration_human,
    format_duration_short,
    format_epoch_iso,
    format_resource_quantity,
    index_execution_times,
    is_child_workflow_owned_by_tracked_parent,
    load_jsonl_events,
    parse_k8s_quantity,
    parse_positive_duration_annotation,
    parse_rfc3339_epoch,
    to_clean_yaml,
)


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parent.parent.parent

# Per-node unique label keys excluded when fingerprinting distinct hardware/pool node groups
UNIQUE_NODE_LABEL_KEYS: frozenset[str] = frozenset(
    {
        "kubernetes.io/hostname",
        "topology.kubernetes.io/zone",
        "failure-domain.beta.kubernetes.io/zone",
    }
)

UNIQUE_NODE_LABEL_SUBSTRINGS: tuple[str, ...] = (
    "partition-",
    "topology-block",
    "rack",
    "island",
    "cube",
    "host-id",
    "node-id",
    "pod-network",
    "switch",
    "subblock",
    "rail",
    "spine",
    "leaf",
    "tor-",
)

COMMON_TOPOLOGY_LABEL_CANDIDATES: tuple[str, ...] = (
    "cloud.google.com/gce-topology-block",
    "cloud.google.com/gce-topology-subblock",
    "cloud.google.com/gce-topology-host",
    "cloud.google.com/gke-tpu-partition-4x4x4-id",
    "cloud.google.com/gke-tpu-partition-2x4x4-id",
    "cloud.google.com/gke-tpu-partition-2x2x4-id",
    "cloud.google.com/gke-tpu-partition-2x2x2-id",
    "cloud.google.com/gke-tpu-partition-2x2x1-id",
    "topology.kubernetes.io/region",
    "topology.kubernetes.io/zone",
    "kubernetes.io/hostname",
)


# ---------------------------------------------------------------------------
# Formatting Aliases (delegating to shared chamber_common helpers)
# ---------------------------------------------------------------------------

fmt_epoch_iso = format_epoch_iso
fmt_duration_human = format_duration_human
fmt_duration_short = format_duration_short


def fmt_quantity(val: float, resource_name: str = "") -> str:
    """Format a parsed resource value back into a clean human-readable string."""
    return format_resource_quantity(val, resource_name, show_tib_paren=True)


def fmt_num_compact(val: float) -> str:
    return format_resource_quantity(val, "")


# ---------------------------------------------------------------------------
# Node Group Fingerprinting
# ---------------------------------------------------------------------------


def _is_unique_node_label(label_key: str) -> bool:
    if label_key in UNIQUE_NODE_LABEL_KEYS:
        return True
    lk = label_key.lower()
    return any(sub in lk for sub in UNIQUE_NODE_LABEL_SUBSTRINGS)


_NUMERIC_INDEX_SUFFIX_RE = re.compile(r"[-_]\d+$")


def _normalize_group_label_value(label_key: str, label_val: str) -> str:
    """Normalize indexed slice/pool suffixes (e.g. tpu-pool-0000 -> tpu-pool-*)
    so identical hardware nodes across N slices/pools group into one distinct hardware tier."""
    lk = label_key.lower()
    if any(
        tok in lk
        for tok in (
            "nodepool",
            "node-pool",
            "superpod",
            "subblock",
            "slice",
            "rack",
            "island",
        )
    ):
        return _NUMERIC_INDEX_SUFFIX_RE.sub("-*", label_val)
    return label_val


def node_group_fingerprint(node_obj: dict[str, Any], non_unique_label_keys: set[str]) -> tuple[Any, ...]:
    """Compute a deterministic hashable signature defining a distinct node group."""
    meta = node_obj.get("metadata") or {}
    labels = meta.get("labels") or {}
    spec = node_obj.get("spec") or {}
    status = node_obj.get("status") or {}
    cap = status.get("capacity") or {}
    alloc = status.get("allocatable") or cap

    group_labels = tuple(
        sorted(
            (k, _normalize_group_label_value(k, str(labels[k])))
            for k in non_unique_label_keys
            if k in labels
        )
    )
    cap_sig = tuple(sorted((k, str(v)) for k, v in cap.items()))
    alloc_sig = tuple(sorted((k, str(v)) for k, v in alloc.items()))
    taints_sig = tuple(
        sorted(
            (str(t.get("key") or ""), str(t.get("value") or ""), str(t.get("effect") or ""))
            for t in (spec.get("taints") or [])
            if isinstance(t, dict)
        )
    )
    return (cap_sig, alloc_sig, group_labels, taints_sig)


def describe_node_group(
    sample_node: dict[str, Any],
    group_idx: int,
    group_nodes: list[dict[str, Any]] | None = None,
) -> str:
    """Produce a concise human-readable title for a node group."""
    labels = (sample_node.get("metadata") or {}).get("labels") or {}
    cap = (sample_node.get("status") or {}).get("capacity") or {}
    machine = (
        labels.get("node.kubernetes.io/instance-type")
        or labels.get("beta.kubernetes.io/instance-type")
        or labels.get("cloud.google.com/gce-machine-type")
        or labels.get("kueue-optimizer.gke.io/machine-type")
    )
    accel = (
        labels.get("cloud.google.com/gke-tpu-accelerator")
        or labels.get("nvidia.com/gpu.product")
        or labels.get("cloud.google.com/gke-accelerator")
        or labels.get("kueue-optimizer.gke.io/accelerator")
    )
    pool = (
        labels.get("cloud.google.com/gke-nodepool")
        or labels.get("kueue.x-k8s.io/resource-flavor")
        or labels.get("kueue-optimizer.gke.io/node-pool")
        or labels.get("node-role.kubernetes.io/role")
    )
    parts: list[str] = []
    if machine:
        parts.append(f"`{machine}`")
    if accel and accel not in str(machine or ""):
        parts.append(f"accel=`{accel}`")
    if pool and pool not in str(machine or ""):
        distinct_pools: set[str] = set()
        if group_nodes:
            for gn in group_nodes:
                gl = (gn.get("metadata") or {}).get("labels") or {}
                gp = (
                    gl.get("cloud.google.com/gke-nodepool")
                    or gl.get("kueue.x-k8s.io/resource-flavor")
                    or gl.get("kueue-optimizer.gke.io/node-pool")
                )
                if gp:
                    distinct_pools.add(str(gp))
        if len(distinct_pools) > 1:
            norm_pool = _NUMERIC_INDEX_SUFFIX_RE.sub("-*", str(pool))
            parts.append(f"pool=`{norm_pool}` ({len(distinct_pools)} pools)")
        else:
            parts.append(f"pool=`{pool}`")
    if not parts:
        accel_res = [
            f"{v}x {k}"
            for k, v in sorted(cap.items())
            if k not in ("cpu", "memory", "pods", "ephemeral-storage")
        ]
        cpu_s = cap.get("cpu", "?")
        mem_s = cap.get("memory", "?")
        if accel_res:
            parts.append(f"{', '.join(accel_res)} ({cpu_s} CPU, {mem_s} RAM)")
        else:
            parts.append(f"CPU Node ({cpu_s} CPU, {mem_s} RAM)")
    return f"Group {group_idx + 1}: " + " / ".join(parts)


# ---------------------------------------------------------------------------
# Workload Data Model
# ---------------------------------------------------------------------------


@dataclass
class ParsedWorkload:
    name: str
    namespace: str
    kind: str
    workload_state: str  # "NotAdmitted" | "Admitted"
    arrival_epoch: float
    local_queue: str
    priority_class: str
    priority_value: int
    total_pods: int
    total_requests: dict[str, float]  # resource_name -> total across all pods in workload
    per_pod_requests: dict[str, float]
    duration_sec: float
    found_duration: bool
    annotations: dict[str, str]
    labels: dict[str, str]


# ---------------------------------------------------------------------------
# ASCII Histogram Builder
# ---------------------------------------------------------------------------

DURATION_BINS: list[tuple[str, float, float]] = [
    ("< 1m   (0-60s)", 0.0, 60.0),
    ("1-5m   (60-300s)", 60.0, 300.0),
    ("5-10m  (300-600s)", 300.0, 600.0),
    ("10-15m (600-900s)", 600.0, 900.0),
    ("15-20m (900-1200s)", 900.0, 1200.0),
    ("20-30m (1200-1800s)", 1200.0, 1800.0),
    ("30-45m (1800-2700s)", 1800.0, 2700.0),
    ("45-60m (2700-3600s)", 2700.0, 3600.0),
    ("1-2h   (3600-7200s)", 3600.0, 7200.0),
    (">= 2h  (7200s+)", 7200.0, math.inf),
]


def render_duration_histogram(
    title: str,
    durations: list[float],
    bins: list[tuple[str, float, float]] = DURATION_BINS,
    max_bar_width: int = 32,
) -> str:
    """Render a self-contained ASCII bar histogram of execution durations with summary quantiles."""
    if not durations:
        return f"=== {title} (count=0) ===\n  (no workloads)"

    st = calc_summary_stats(durations)
    n = int(st["count"])
    bin_counts = [0] * len(bins)
    for d in durations:
        placed = False
        for i, (_, lo, hi) in enumerate(bins):
            if lo <= d < hi or (i == len(bins) - 1 and d >= lo):
                bin_counts[i] += 1
                placed = True
                break
        if not placed:
            bin_counts[0] += 1

    peak = max(bin_counts) if bin_counts else 1
    lines: list[str] = [
        (
            f"=== {title} | count={n:,} | min={st['min']:.0f}s, "
            f"p50={st['p50']:.0f}s ({st['p50']/60.0:.1f}m), "
            f"avg={st['avg']:.1f}s ({st['avg']/60.0:.1f}m), "
            f"p90={st['p90']:.0f}s ({st['p90']/60.0:.1f}m), "
            f"p99={st['p99']:.0f}s ({st['p99']/60.0:.1f}m), "
            f"max={st['max']:.0f}s ==="
        )
    ]

    # Trim trailing empty bins after the highest populated bin (keep at least first 6 bins)
    last_non_zero = max((i for i, c in enumerate(bin_counts) if c > 0), default=0)
    visible_end = max(min(len(bins), 6), last_non_zero + 1)

    for i in range(visible_end):
        label, _, _ = bins[i]
        cnt = bin_counts[i]
        pct = (100.0 * cnt / n) if n > 0 else 0.0
        bar_len = int(round((cnt / peak) * max_bar_width)) if (peak > 0 and cnt > 0) else 0
        if cnt > 0 and bar_len == 0:
            bar_len = 1
        bar = "█" * bar_len
        lines.append(f"  {label:<20} | {cnt:>5,} ({pct:>5.1f}%) | {bar}")

    return "\n".join(lines)


def render_timeline_arrival_histogram(
    arrivals_offset_sec: list[float],
    test_duration_sec: float,
    num_buckets: int = 12,
    max_bar_width: int = 32,
) -> str:
    """Render an ASCII arrival histogram showing how unadmitted workload arrivals distribute over time."""
    if not arrivals_offset_sec:
        return "  (no unadmitted workload arrivals)"
    if test_duration_sec <= 0:
        return f"  All {len(arrivals_offset_sec):,} unadmitted workloads arrive simultaneously at t = 0.0s."

    bucket_width = test_duration_sec / num_buckets
    counts = [0] * num_buckets
    for off in arrivals_offset_sec:
        b = int(off / bucket_width) if bucket_width > 0 else 0
        b = max(0, min(num_buckets - 1, b))
        counts[b] += 1

    n = len(arrivals_offset_sec)
    peak = max(counts) if counts else 1
    lines = [
        f"=== Unadmitted Workload Arrival Distribution Across {num_buckets} Equal Time Windows (total={n:,}, span={fmt_duration_human(test_duration_sec)}) ==="
    ]
    for b in range(num_buckets):
        lo = b * bucket_width
        hi = (b + 1) * bucket_width
        cnt = counts[b]
        pct = 100.0 * cnt / n
        bar_len = int(round((cnt / peak) * max_bar_width)) if (peak > 0 and cnt > 0) else 0
        if cnt > 0 and bar_len == 0:
            bar_len = 1
        bar = "█" * bar_len
        if test_duration_sec >= 3600:
            lbl = f"{lo/3600.0:>5.2f}h - {hi/3600.0:>5.2f}h"
        else:
            lbl = f"{lo:>6.1f}s - {hi:>6.1f}s"
        lines.append(f"  [{lbl}] | {cnt:>5,} ({pct:>5.1f}%) | {bar}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main Scenario Analysis & Markdown Generation
# ---------------------------------------------------------------------------


def generate_readme_for_scenario(scenario_dir: Path) -> str:
    """Analyze `infra.json`, `workflows.json`, and `execution-time.json` in `scenario_dir` and return Markdown."""
    scenario_dir = scenario_dir.resolve()
    try:
        rel_dir = str(scenario_dir.relative_to(REPO_ROOT))
    except ValueError:
        rel_dir = str(scenario_dir)
    scenario_title = (
        scenario_dir.name
        if scenario_dir.name != "default"
        else f"{scenario_dir.parent.name}/{scenario_dir.name}"
    )

    infra_path = scenario_dir / "infra.json"
    workflows_path = scenario_dir / "workflows.json"
    exec_path = scenario_dir / "execution-time.json"

    infra_events = load_jsonl_events(infra_path, missing_ok=True)
    workflow_events = load_jsonl_events(workflows_path, missing_ok=True)
    exec_index = index_execution_times(exec_path, missing_ok=True)

    # -----------------------------------------------------------------------
    # 1. Parse infra.json objects
    # -----------------------------------------------------------------------
    nodes_by_name: dict[str, dict[str, Any]] = {}
    topologies_by_name: dict[str, dict[str, Any]] = {}
    flavors_by_name: dict[str, dict[str, Any]] = {}
    wpc_by_name: dict[str, dict[str, Any]] = {}
    cohorts_by_name: dict[str, dict[str, Any]] = {}
    cqs_by_name: dict[str, dict[str, Any]] = {}
    lqs_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    namespaces_by_name: dict[str, dict[str, Any]] = {}

    pod_new_events: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
    pod_delete_epochs: dict[tuple[str, str], float] = {}

    infra_timestamps: list[float] = []
    for ev in infra_events:
        ep = parse_rfc3339_epoch(ev.get("timestamp"))
        if ep is not None:
            infra_timestamps.append(ep)
        ev_type = str(ev.get("eventType") or "")
        pl = ev.get("payload") or {}
        if not isinstance(pl, dict):
            continue
        kind = str(pl.get("kind") or pl.get("objectKind") or "")
        meta = pl.get("metadata") or {}
        name = str(meta.get("name") or pl.get("name") or "")
        ns = str(meta.get("namespace") or pl.get("namespace") or "default")

        if kind == "Node" and ev_type in ("New", "Update"):
            if name:
                nodes_by_name[name] = pl
        elif kind == "Topology" and ev_type in ("New", "Update"):
            if name:
                topologies_by_name[name] = pl
        elif kind == "ResourceFlavor" and ev_type in ("New", "Update"):
            if name:
                flavors_by_name[name] = pl
        elif kind == "WorkloadPriorityClass" and ev_type in ("New", "Update"):
            if name:
                wpc_by_name[name] = pl
        elif kind == "Cohort" and ev_type in ("New", "Update"):
            if name:
                cohorts_by_name[name] = pl
        elif kind == "ClusterQueue" and ev_type in ("New", "Update"):
            if name:
                cqs_by_name[name] = pl
        elif kind == "LocalQueue" and ev_type in ("New", "Update"):
            if name:
                lqs_by_key[(ns, name)] = pl
        elif kind == "Namespace" and ev_type in ("New", "Update"):
            if name:
                namespaces_by_name[name] = pl
        elif kind == "Pod":
            if ev_type == "New" and name and (ns, name) not in pod_new_events:
                pod_new_events[(ns, name)] = (ep if ep is not None else 0.0, pl)
            elif ev_type == "Deleted" and name and (ns, name) not in pod_delete_epochs:
                del_ep = parse_rfc3339_epoch(pl.get("deletionTime")) or ep
                if del_ep is not None:
                    pod_delete_epochs[(ns, name)] = del_ep

    wf_timestamps = [
        ep
        for ev in workflow_events
        if (ep := parse_rfc3339_epoch(ev.get("timestamp"))) is not None
    ]
    all_t0_candidates = infra_timestamps + wf_timestamps
    t0_epoch = min(all_t0_candidates) if all_t0_candidates else None

    # -----------------------------------------------------------------------
    # 2. Cluster Nodes & Distinct Node Group Classification
    # -----------------------------------------------------------------------
    all_nodes = list(nodes_by_name.values())
    total_node_count = len(all_nodes)

    # Identify label keys whose cardinality is small/shared (pool/machine/accel labels) vs unique per node/rack
    label_val_sets: dict[str, set[str]] = defaultdict(set)
    for n_obj in all_nodes:
        for lk, lv in ((n_obj.get("metadata") or {}).get("labels") or {}).items():
            label_val_sets[lk].add(str(lv))

    non_unique_label_keys: set[str] = set()
    for lk, vset in label_val_sets.items():
        if _is_unique_node_label(lk):
            continue
        # Labels shared across node pools (e.g., machine type, accelerator type, os/arch, role)
        if len(vset) <= max(8, total_node_count // 8):
            non_unique_label_keys.add(lk)

    grouped_nodes_map: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for n_obj in all_nodes:
        fp = node_group_fingerprint(n_obj, non_unique_label_keys)
        grouped_nodes_map[fp].append(n_obj)

    # Sort node groups from largest node count to smallest, then by name of first node
    node_groups = sorted(
        grouped_nodes_map.values(),
        key=lambda grp: (-len(grp), str((grp[0].get("metadata") or {}).get("name") or "")),
    )

    cluster_cap: dict[str, float] = defaultdict(float)
    cluster_alloc: dict[str, float] = defaultdict(float)
    for n_obj in all_nodes:
        st = n_obj.get("status") or {}
        cap = st.get("capacity") or {}
        alloc = st.get("allocatable") or cap
        for rk, rv in cap.items():
            cluster_cap[rk] += parse_k8s_quantity(rv, rk)
        for rk, rv in alloc.items():
            cluster_alloc[rk] += parse_k8s_quantity(rv, rk)

    # Identify accelerator resources present on cluster nodes
    accel_resources = sorted(
        rk for rk in cluster_cap.keys() if rk not in ("cpu", "memory", "pods", "ephemeral-storage")
    )
    primary_accel_resource = accel_resources[0] if accel_resources else None

    # -----------------------------------------------------------------------
    # 3. Priority Classes & Workflows (Deduplicating Child Jobs & Resolving Durations)
    # -----------------------------------------------------------------------
    priority_value_by_name: dict[str, int] = {}
    for wpc_name, wpc_obj in wpc_by_name.items():
        try:
            priority_value_by_name[wpc_name] = int(wpc_obj.get("value") or 0)
        except (ValueError, TypeError):
            priority_value_by_name[wpc_name] = 0

    known_parents = collect_known_parent_workflows(workflow_events)
    earliest_admit_by_ns = collect_not_admitted_workflow_admit_epochs(workflow_events, known_parents)

    admitted_at_start_wfs: list[ParsedWorkload] = []
    unadmitted_wfs: list[ParsedWorkload] = []
    seen_top_workflows: set[tuple[str, str, str]] = set()

    for ev in workflow_events:
        ev_type = str(ev.get("eventType") or "")
        if ev_type not in ("New", ""):
            continue
        wl_state = str(ev.get("workloadState") or "NotAdmitted")
        pl = ev.get("payload") or {}
        obj = pl.get("object") or {}
        if not isinstance(obj, dict) or not obj:
            continue
        kind = str(pl.get("objectKind") or obj.get("kind") or "JobSet")
        meta = obj.get("metadata") or {}
        wf_name = str(pl.get("name") or meta.get("name") or "")
        wf_ns = str(pl.get("namespace") or meta.get("namespace") or "default")
        if not wf_name:
            continue

        is_child, parent_js = is_child_workflow_owned_by_tracked_parent(obj, kind, wf_ns, known_parents)
        if is_child:
            continue

        wf_key = (wf_ns, kind, wf_name)
        if wf_key in seen_top_workflows:
            continue
        seen_top_workflows.add(wf_key)

        labels = dict(meta.get("labels") or {})
        annotations = dict(meta.get("annotations") or {})

        arr_ep = (
            parse_rfc3339_epoch(ev.get("timestamp"))
            or parse_rfc3339_epoch(pl.get("creationTime"))
            or parse_rfc3339_epoch(meta.get("creationTimestamp"))
            or (t0_epoch or 0.0)
        )
        lq_name = str(
            pl.get("localQueueName")
            or labels.get("kueue.x-k8s.io/queue-name")
            or annotations.get("kueue.x-k8s.io/queue-name")
            or "<none>"
        )

        # Resolve priority class name & numeric value
        pc_name = str(
            labels.get("kueue.x-k8s.io/priority-class")
            or annotations.get("kueue.x-k8s.io/priority-class")
            or ""
        ).strip()
        if not pc_name:
            spec = obj.get("spec") or {}
            for rj in spec.get("replicatedJobs") or []:
                p_spec = (((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get("spec") or {}
                if p_spec.get("priorityClassName"):
                    pc_name = str(p_spec["priorityClassName"])
                    break
        if not pc_name:
            pc_name = "<default>"

        if pc_name in priority_value_by_name:
            pc_val = priority_value_by_name[pc_name]
        elif "kueue-optimizer.gke.io/priority" in annotations:
            try:
                pc_val = int(float(annotations["kueue-optimizer.gke.io/priority"]))
            except ValueError:
                pc_val = 0
            if pc_name != "<default>":
                priority_value_by_name.setdefault(pc_name, pc_val)
        else:
            pc_val = 0

        total_pods, total_reqs, per_pod_reqs = extract_workflow_pods_and_requests(obj, kind)

        # Resolve execution duration from execution-time.json, falling back to unit-aware duration annotations
        ann_dur = parse_positive_duration_annotation(
            annotations,
            "kueue-optimizer.gke.io/pod-execution-time-seconds",
            "kueue-optimizer.gke.io/real-duration-sec",
        )
        fallback_dur = ann_dur if ann_dur is not None else 10.0
        dur_sec, found_in_idx = exec_index.resolve_duration_sec(
            namespace=wf_ns,
            object_kind=kind,
            name=wf_name,
            fallback_sec=fallback_dur,
            parent_jobset_name=parent_js,
        )
        found_dur = found_in_idx or (ann_dur is not None)

        parsed_wf = ParsedWorkload(
            name=wf_name,
            namespace=wf_ns,
            kind=kind,
            workload_state=wl_state,
            arrival_epoch=arr_ep,
            local_queue=lq_name,
            priority_class=pc_name,
            priority_value=pc_val,
            total_pods=total_pods,
            total_requests=total_reqs,
            per_pod_requests=per_pod_reqs,
            duration_sec=dur_sec,
            found_duration=found_dur,
            annotations=annotations,
            labels=labels,
        )
        if wl_state == "Admitted":
            admitted_at_start_wfs.append(parsed_wf)
        else:
            unadmitted_wfs.append(parsed_wf)

    unadmitted_wfs.sort(key=lambda w: (w.arrival_epoch, w.name))

    if unadmitted_wfs:
        t_first_unadmitted = unadmitted_wfs[0].arrival_epoch
        t_last_unadmitted = unadmitted_wfs[-1].arrival_epoch
        test_length_sec = max(0.0, t_last_unadmitted - t_first_unadmitted)
    else:
        t_first_unadmitted = t0_epoch
        t_last_unadmitted = t0_epoch
        test_length_sec = 0.0

    # -----------------------------------------------------------------------
    # 4. Initial Running Pods at t0 (Filtered Identically to testrunner.py)
    # -----------------------------------------------------------------------
    effective_start_ep = (
        t_first_unadmitted
        if t_first_unadmitted is not None
        else (t0_epoch if t0_epoch is not None else 0.0)
    )
    qualifying_initial_pods, _expired_init_pods = filter_initial_running_pods(
        pod_new_events,
        pod_delete_epochs,
        t0_epoch=t0_epoch if t0_epoch is not None else effective_start_ep,
        effective_start_epoch=effective_start_ep,
        initial_window_sec=1.0,
        not_admitted_workflow_names=[(w.namespace, w.name) for w in unadmitted_wfs],
        earliest_admit_by_ns=earliest_admit_by_ns,
    )

    init_pod_requests_sum: dict[str, float] = defaultdict(float)
    init_pod_lifetimes: list[float] = []
    init_pod_infinite_count = 0
    init_nodes_used: Counter[str] = Counter()
    init_ns_counts: Counter[str] = Counter()
    init_pod_profiles: Counter[tuple[tuple[str, str], ...]] = Counter()

    for p_ns, _p_name, p_ep, pl, del_ep in qualifying_initial_pods:
        init_ns_counts[p_ns] += 1
        placement = str(pl.get("placement") or ((pl.get("spec") or {}).get("nodeName")) or "")
        if placement:
            init_nodes_used[placement] += 1

        raw_req = pl.get("resourceRequest")
        if isinstance(raw_req, dict) and raw_req:
            prof = tuple(sorted((str(k), str(v)) for k, v in raw_req.items()))
            init_pod_profiles[prof] += 1
            for rk, rv in raw_req.items():
                init_pod_requests_sum[rk] += parse_k8s_quantity(rv, rk)
        else:
            p_reqs = extract_pod_spec_resources(pl.get("spec") if isinstance(pl, dict) else None)
            prof = tuple(sorted((str(k), f"{v:g}") for k, v in p_reqs.items()))
            init_pod_profiles[prof] += 1
            for rk, rv in p_reqs.items():
                init_pod_requests_sum[rk] += rv
        init_pod_requests_sum["pods"] += 1.0

        if del_ep is not None:
            init_pod_lifetimes.append(max(0.0, del_ep - effective_start_ep))
        else:
            init_pod_infinite_count += 1

    total_initial_pods = len(qualifying_initial_pods)

    # Determine dominant resource for headline saturation
    # If cluster has accelerators (TPU/GPU) and workloads request them, primary resource is that accelerator;
    # otherwise CPU (and we always report every resource column explicitly).
    total_wf_accel = (
        sum(w.total_requests.get(primary_accel_resource, 0.0) for w in unadmitted_wfs)
        if primary_accel_resource
        else 0.0
    )
    if primary_accel_resource and total_wf_accel > 0 and cluster_cap.get(primary_accel_resource, 0.0) > 0:
        primary_sat_resource = primary_accel_resource
        primary_sat_label = f"`{primary_accel_resource}`"
    else:
        primary_sat_resource = "cpu"
        primary_sat_label = "`cpu`"

    DEFAULT_SIX_HOUR_SEC = 21600.0

    def compute_offered_resource_seconds(workloads_subset: list[ParsedWorkload], res_key: str) -> float:
        return sum(w.total_requests.get(res_key, 0.0) * w.duration_sec for w in workloads_subset)

    def compute_saturation_over_window_pct(
        workloads_subset: list[ParsedWorkload], res_key: str, window_sec: float
    ) -> float | None:
        if window_sec <= 0.0:
            return None
        cap_val = cluster_cap.get(res_key, 0.0)
        if cap_val <= 0.0:
            return None
        offered = compute_offered_resource_seconds(workloads_subset, res_key)
        return (offered / (window_sec * cap_val)) * 100.0

    def compute_saturation_pct(workloads_subset: list[ParsedWorkload], res_key: str) -> float | None:
        return compute_saturation_over_window_pct(workloads_subset, res_key, test_length_sec)

    def compute_ideal_full_capacity_sec(workloads_subset: list[ParsedWorkload], res_key: str) -> float | None:
        """Ideal 1.0x service time T_100%(r) = sum(size_w(r) * dur_w) / C_cluster(r)."""
        cap_val = cluster_cap.get(res_key, 0.0)
        if cap_val <= 0.0:
            return None
        return compute_offered_resource_seconds(workloads_subset, res_key) / cap_val

    def format_speedup_val(val: float) -> str:
        if val >= 100.0:
            return f"{val:.1f}"
        if val >= 10.0:
            return f"{val:.2f}"
        return f"{val:.3f}"

    def build_target_window_plan(target_sec: float, target_label: str) -> dict[str, Any] | None:
        """Compute exact --speedup and --duration settings so offered work on primary_sat_resource
        saturates 100% of the cluster over target_sec wall-clock seconds."""
        t100 = compute_ideal_full_capacity_sec(unadmitted_wfs, primary_sat_resource)
        if t100 is None or t100 <= 0.0 or target_sec <= 0.0:
            return None
        speedup_100 = t100 / target_sec
        comp_arrival_sec = test_length_sec / speedup_100 if test_length_sec > 0.0 else 0.0
        comp_p50_dur_sec = (
            (calc_summary_stats([w.duration_sec for w in unadmitted_wfs])["p50"] / speedup_100)
            if unadmitted_wfs
            else 0.0
        )
        # If all arrivals fit inside target_sec (i.e. native arrival saturation >= 100%),
        # --duration {target_label} runs for target_sec at 100% offered saturation.
        # If native arrival saturation < 100%, comp_arrival_sec > target_sec:
        #   - Capped at --duration {target_label} stops at exactly target_label wall-clock time;
        #   - Full-arrival completion window is ceil(comp_arrival_sec + comp_p50_dur_sec)s.
        if comp_arrival_sec <= target_sec + 1e-6:
            rec_duration_flag = target_label
            note = (
                f"All arrivals submit within `{comp_arrival_sec:.1f}s` (≤ `{target_sec:.0f}s`); "
                f"`--duration {target_label}` captures `100%` of arrivals and `100.0%` full-cluster work in `{target_label}`."
            )
        else:
            full_drain_sec = int(math.ceil(comp_arrival_sec + comp_p50_dur_sec))
            rec_duration_flag = target_label
            note = (
                f"Offered work equals `100.0%` of `{target_label}` (`{target_sec:.0f}s`), while compressed arrivals span "
                f"`{comp_arrival_sec:.1f}s` (since arrival-window saturation < `100%`); use `--duration {target_label}` "
                f"for a hard `{target_label}` wall-clock window or `--duration {full_drain_sec}s` to admit all late arrivals."
            )
        return {
            "target_sec": target_sec,
            "target_label": target_label,
            "t100_sec": t100,
            "speedup": speedup_100,
            "speedup_str": format_speedup_val(speedup_100),
            "comp_arrival_sec": comp_arrival_sec,
            "rec_duration_flag": rec_duration_flag,
            "note": note,
        }

    # -----------------------------------------------------------------------
    # Build Markdown Document
    # -----------------------------------------------------------------------
    md: list[str] = []

    md.append(f"# Test Scenario Summary: `{scenario_title}`")
    md.append("")
    rel_script = os.path.relpath(SCRIPT_PATH, start=scenario_dir.resolve())
    md.append(
        f"> **Auto-generated scenario analysis** from [`{rel_dir}/infra.json`](./infra.json), "
        f"[`{rel_dir}/workflows.json`](./workflows.json), and [`{rel_dir}/execution-time.json`](./execution-time.json) "
        f"via [`deploy/scripts/generate_scenario_readme.py`]({rel_script})."
    )
    md.append("")

    # =======================================================================
    # Section 1: High-Level Scenario Overview
    # =======================================================================
    md.append("## 1. Scenario Overview")
    md.append("")

    total_wf_dur_stats = calc_summary_stats([w.duration_sec for w in unadmitted_wfs])
    total_wf_pod_stats = calc_summary_stats([float(w.total_pods) for w in unadmitted_wfs])
    headline_sat = compute_saturation_pct(unadmitted_wfs, primary_sat_resource)
    cpu_sat_all = compute_saturation_pct(unadmitted_wfs, "cpu")
    mem_sat_all = compute_saturation_pct(unadmitted_wfs, "memory")

    cap_summary_parts = [
        f"**{total_node_count:,} nodes**",
        f"`{fmt_quantity(cluster_cap.get('cpu', 0.0), 'cpu')}`",
        f"`{fmt_quantity(cluster_cap.get('memory', 0.0), 'memory')}`",
    ]
    for ar in accel_resources:
        cap_summary_parts.append(f"`{fmt_quantity(cluster_cap[ar], ar)}` `{ar}`")

    md.append("| Dimension | Summary |")
    md.append("| :--- | :--- |")
    md.append(f"| **Scenario Directory** | `{rel_dir}` |")
    md.append(
        f"| **Raw Event Counts** | `infra.json`: **{len(infra_events):,}** events · "
        f"`workflows.json`: **{len(workflow_events):,}** events · "
        f"`execution-time.json`: **{exec_index.total_events:,}** events |"
    )
    md.append(f"| **Cluster Capacity** | {' · '.join(cap_summary_parts)} ({len(node_groups)} distinct node group{'s' if len(node_groups) != 1 else ''}) |")
    md.append(
        f"| **Topology Configuration** | **{len(topologies_by_name)}** `Topology` CRD(s) "
        f"({', '.join(f'`{t}`' for t in sorted(topologies_by_name)) if topologies_by_name else 'none'}) |"
    )
    md.append(
        f"| **Initial In-Flight Pods at $t_0$** | **{total_initial_pods:,}** pods across "
        f"**{len(init_nodes_used):,}** / {total_node_count:,} nodes "
        f"(initial `{primary_sat_resource}` utilization: "
        f"**{(100.0 * init_pod_requests_sum.get(primary_sat_resource, 0.0) / cluster_alloc[primary_sat_resource]) if cluster_alloc.get(primary_sat_resource, 0.0) > 0 else 0.0:.2f}%**) |"
    )
    md.append(
        f"| **Kueue Objects** | **{len(flavors_by_name)}** `ResourceFlavor`(s) · "
        f"**{len(cohorts_by_name)}** `Cohort`(s) · "
        f"**{len(cqs_by_name)}** `ClusterQueue`(s) · "
        f"**{len(lqs_by_key)}** `LocalQueue`(s) · "
        f"**{len(wpc_by_name)}** `WorkloadPriorityClass`(es) |"
    )
    md.append(
        f"| **Test Arrival Window ($T_{{\\text{{test}}}}$)** | **{fmt_duration_human(test_length_sec)}** "
        f"(`{fmt_epoch_iso(t_first_unadmitted)}` $\\rightarrow$ `{fmt_epoch_iso(t_last_unadmitted)}`) |"
    )
    md.append(
        f"| **Unadmitted Workloads** | **{len(unadmitted_wfs):,}** `NotAdmitted` arrivals "
        f"({int(total_wf_pod_stats['sum']):,} total pods; gang size min={int(total_wf_pod_stats['min'])}, "
        f"p50={int(total_wf_pod_stats['p50'])}, p90={int(total_wf_pod_stats['p90'])}, max={int(total_wf_pod_stats['max'])}) "
        f"+ **{len(admitted_at_start_wfs):,}** pre-admitted at $t_0$ |"
    )
    md.append(
        f"| **Execution Duration Quantiles** | "
        f"`min={fmt_duration_short(total_wf_dur_stats['min'])}`, "
        f"`P50={fmt_duration_short(total_wf_dur_stats['p50'])}`, "
        f"`AVG={fmt_duration_short(total_wf_dur_stats['avg'])}`, "
        f"`P90={fmt_duration_short(total_wf_dur_stats['p90'])}`, "
        f"`P99={fmt_duration_short(total_wf_dur_stats['p99'])}`, "
        f"`max={fmt_duration_short(total_wf_dur_stats['max'])}` |"
    )
    sat_parts = []
    if headline_sat is not None:
        sat_parts.append(f"{primary_sat_label}: **{headline_sat:.2f}%**")
    if primary_sat_resource != "cpu" and cpu_sat_all is not None:
        sat_parts.append(f"`cpu`: **{cpu_sat_all:.2f}%**")
    if mem_sat_all is not None:
        sat_parts.append(f"`memory`: **{mem_sat_all:.2f}%**")

    headline_sat_6h = compute_saturation_over_window_pct(unadmitted_wfs, primary_sat_resource, DEFAULT_SIX_HOUR_SEC)
    cpu_sat_6h = compute_saturation_over_window_pct(unadmitted_wfs, "cpu", DEFAULT_SIX_HOUR_SEC)
    mem_sat_6h = compute_saturation_over_window_pct(unadmitted_wfs, "memory", DEFAULT_SIX_HOUR_SEC)
    sat_6h_parts = []
    if headline_sat_6h is not None:
        sat_6h_parts.append(f"{primary_sat_label}: **{headline_sat_6h:.2f}%**")
    if primary_sat_resource != "cpu" and cpu_sat_6h is not None:
        sat_6h_parts.append(f"`cpu`: **{cpu_sat_6h:.2f}%**")
    if mem_sat_6h is not None:
        sat_6h_parts.append(f"`memory`: **{mem_sat_6h:.2f}%**")

    plan_5m = build_target_window_plan(300.0, "5m")
    plan_10m = build_target_window_plan(600.0, "10m")

    empty_sat_note = "n/a (instantaneous $t=0$ burst: $T_{\\text{test}} = 0$)"
    sat_window_summary = " · ".join(sat_parts) if sat_parts else empty_sat_note
    md.append(
        f"| **1. Saturation (Arrival Window $T_{{\\text{{test}}}}$)** | "
        f"{sat_window_summary} "
        f"*(over $T_{{\\text{{test}}}} = {fmt_duration_short(test_length_sec)}$)* |"
    )
    md.append(
        f"| **2. Saturation (Reference `6h` / `21,600s` Window)** | "
        f"{' · '.join(sat_6h_parts) if sat_6h_parts else 'n/a'} "
        f"*(over $T_{{6\\text{{h}}}} = 21{{,}}600\\text{{s}}$)* |"
    )
    if plan_5m and plan_10m:
        md.append(
            f"| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | "
            f"**5m (`300s`)**: `--speedup {plan_5m['speedup_str']} --duration {plan_5m['rec_duration_flag']}` · "
            f"**10m (`600s`)**: `--speedup {plan_10m['speedup_str']} --duration {plan_10m['rec_duration_flag']}` "
            f"*(bottleneck: {primary_sat_label}, $T_{{100\\%}} = {fmt_duration_human(plan_5m['t100_sec'])}$)* |"
        )
    else:
        md.append("| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | n/a |")
    md.append("")

    # =======================================================================
    # Section 2: Cluster Nodes & Sample Node Manifests per Distinct Group
    # =======================================================================
    md.append("---")
    md.append("")
    md.append("## 2. Cluster Nodes & Distinct Node Groups")
    md.append("")
    md.append(
        f"The cluster contains **{total_node_count:,}** `v1/Node` objects partitioned into "
        f"**{len(node_groups)}** distinct hardware/label configuration group(s)."
    )
    md.append("")

    # 2.1 Aggregate Cluster Capacity Table
    md.append("### 2.1 Aggregate Physical Cluster Capacity & Allocatable Resources")
    md.append("")
    md.append("| Resource | Total Capacity | Total Allocatable | Per-Node Average (Capacity) |")
    md.append("| :--- | ---: | ---: | ---: |")
    md.append(f"| `nodes` | **{total_node_count:,}** | **{total_node_count:,}** | `1` |")
    ordered_res_keys = ["cpu", "memory", "pods"] + [
        rk for rk in sorted(cluster_cap.keys()) if rk not in ("cpu", "memory", "pods")
    ]
    for rk in ordered_res_keys:
        if rk not in cluster_cap and rk not in cluster_alloc:
            continue
        c_val = cluster_cap.get(rk, 0.0)
        a_val = cluster_alloc.get(rk, 0.0)
        per_node = c_val / total_node_count if total_node_count > 0 else 0.0
        md.append(
            f"| `{rk}` | **{fmt_quantity(c_val, rk)}** | **{fmt_quantity(a_val, rk)}** | {fmt_quantity(per_node, rk)} |"
        )
    md.append("")

    # 2.2 Node Groups Summary Table
    md.append("### 2.2 Summary of Distinct Node Groups")
    md.append("")
    accel_col_hdr = " | ".join(f"`{ar}` / Node" for ar in accel_resources) if accel_resources else "Accelerators"
    accel_col_sep = " | ".join("---:" for _ in accel_resources) if accel_resources else ":---:"
    md.append(
        f"| Group | Node Count | Share | Sample Node Name | `cpu` / Node | `memory` / Node | `pods` / Node | {accel_col_hdr} | Key Identifying Labels |"
    )
    md.append(f"| :--- | ---: | ---: | :--- | ---: | ---: | ---: | {accel_col_sep} | :--- |")

    for g_idx, grp in enumerate(node_groups):
        sample_n = grp[0]
        g_cnt = len(grp)
        g_share = (100.0 * g_cnt / total_node_count) if total_node_count > 0 else 0.0
        n_meta = sample_n.get("metadata") or {}
        n_labels = n_meta.get("labels") or {}
        n_cap = (sample_n.get("status") or {}).get("capacity") or {}
        g_title = describe_node_group(sample_n, g_idx, grp)

        if accel_resources:
            accel_cells = " | ".join(str(n_cap.get(ar, "0")) for ar in accel_resources)
        else:
            accel_cells = "none"

        key_lbl_items = [
            f"`{k}={v}`"
            for k, v in sorted(n_labels.items())
            if k in non_unique_label_keys
            and k
            not in (
                "beta.kubernetes.io/arch",
                "beta.kubernetes.io/os",
                "kubernetes.io/arch",
                "kubernetes.io/os",
                "node-role.kubernetes.io/agent",
            )
            and str(v) != ""
        ]
        key_lbl_str = ", ".join(key_lbl_items[:5]) if key_lbl_items else "*(standard KWOK labels)*"

        md.append(
            f"| **{g_title}** | **{g_cnt:,}** | {g_share:.1f}% | `{n_meta.get('name', '?')}` | "
            f"`{n_cap.get('cpu', '?')}` | `{n_cap.get('memory', '?')}` | `{n_cap.get('pods', '?')}` | "
            f"{accel_cells} | {key_lbl_str} |"
        )
    md.append("")

    # 2.3 Full Sample Node YAML per Distinct Group
    md.append("### 2.3 Sample Node Manifest (YAML) from Each Distinct Group")
    md.append("")
    for g_idx, grp in enumerate(node_groups):
        sample_n = grp[0]
        g_title = describe_node_group(sample_n, g_idx, grp)
        n_name = (sample_n.get("metadata") or {}).get("name", f"node-{g_idx}")
        md.append(f"#### {g_title} — Sample Node `{n_name}` ({len(grp):,} nodes in group)")
        md.append("")
        md.append("```yaml")
        md.append(to_clean_yaml(sample_n))
        md.append("```")
        md.append("")

    # =======================================================================
    # Section 3: Topology & Node Distribution Across Topology Levels
    # =======================================================================
    md.append("---")
    md.append("")
    md.append("## 3. Cluster Topology & Node Distribution")
    md.append("")

    if topologies_by_name:
        md.append(
            f"This scenario defines **{len(topologies_by_name)}** Kueue `Topology` resource(s) for "
            f"Topology-Aware Scheduling (`TAS`):"
        )
        md.append("")
        for t_name, t_obj in sorted(topologies_by_name.items()):
            levels = [
                str(lv.get("nodeLabel") or "")
                for lv in ((t_obj.get("spec") or {}).get("levels") or [])
                if isinstance(lv, dict) and lv.get("nodeLabel")
            ]
            md.append(f"### 3.1 Topology `{t_name}` ({len(levels)} Hierarchy Level{'s' if len(levels) != 1 else ''})")
            md.append("")
            md.append("```yaml")
            md.append(to_clean_yaml(t_obj))
            md.append("```")
            md.append("")
            md.append(f"#### Node Distribution Across `{t_name}` Hierarchy Levels")
            md.append("")
            md.append(
                "| Level | Topology Label (`nodeLabel`) | Distinct Domains | Nodes w/ Label | Min Nodes/Domain | Median (P50) | Mean Nodes/Domain | Max Nodes/Domain | Domain Balance | Sample Domain Counts |"
            )
            md.append("| :---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- | :--- |")

            for lv_idx, lbl_key in enumerate(levels, start=1):
                dom_counts: Counter[str] = Counter()
                for n_obj in all_nodes:
                    val = ((n_obj.get("metadata") or {}).get("labels") or {}).get(lbl_key)
                    if val is not None and str(val) != "":
                        dom_counts[str(val)] += 1
                if not dom_counts:
                    md.append(
                        f"| `L{lv_idx}` | `{lbl_key}` | `0` | `0 / {total_node_count:,}` | — | — | — | — | Unused | *(label absent on nodes)* |"
                    )
                    continue
                cnt_vals = list(dom_counts.values())
                st_c = calc_summary_stats([float(c) for c in cnt_vals])
                covered = int(st_c["sum"])
                balanced = "Uniform" if st_c["min"] == st_c["max"] else f"Asymmetric (`{int(st_c['min'])}..{int(st_c['max'])}`)"
                # Show representative domain counts
                dist_of_sizes = Counter(cnt_vals)
                if len(dist_of_sizes) == 1:
                    size_val = next(iter(dist_of_sizes.keys()))
                    sample_doms = ", ".join(f"`{d}` ({c})" for d, c in sorted(dom_counts.items())[:3])
                    if len(dom_counts) > 3:
                        sample_doms += f", … ({len(dom_counts)}x `{size_val}` nodes)"
                else:
                    breakdown = ", ".join(
                        f"{num_d} domain{'s' if num_d != 1 else ''} × {sz} nodes"
                        for sz, num_d in sorted(dist_of_sizes.items(), reverse=True)
                    )
                    sample_doms = breakdown
                md.append(
                    f"| `L{lv_idx}` | `{lbl_key}` | **{len(dom_counts):,}** | `{covered:,} / {total_node_count:,}` | "
                    f"`{int(st_c['min'])}` | `{int(st_c['p50'])}` | `{st_c['avg']:.1f}` | `{int(st_c['max'])}` | "
                    f"{balanced} | {sample_doms} |"
                )
            md.append("")

            # Show parent -> child branching summary across consecutive levels
            if len(levels) >= 2:
                md.append(f"#### Hierarchical Tree Branching Summary (`{t_name}`)")
                md.append("")
                md.append("```text")
                md.append(f"Cluster Total: {total_node_count:,} nodes")
                for lv_idx, lbl_key in enumerate(levels, start=1):
                    dom_counts = Counter(
                        str(((n.get("metadata") or {}).get("labels") or {}).get(lbl_key))
                        for n in all_nodes
                        if ((n.get("metadata") or {}).get("labels") or {}).get(lbl_key) is not None
                    )
                    indent_str = "  " * lv_idx
                    if not dom_counts:
                        md.append(f"{indent_str}└── L{lv_idx} [{lbl_key}]: 0 domains (not labeled on nodes)")
                        continue
                    size_hist = Counter(dom_counts.values())
                    hist_desc = ", ".join(
                        f"{cnt} domain(s) of {sz} node(s)" for sz, cnt in sorted(size_hist.items(), reverse=True)
                    )
                    md.append(
                        f"{indent_str}└── L{lv_idx} [{lbl_key}]: {len(dom_counts):,} total domains ({hist_desc})"
                    )
                md.append("```")
                md.append("")
    else:
        md.append(
            "No `kueue.x-k8s.io/Topology` CRD is defined in `infra.json` (Topology-Aware Scheduling is not explicitly configured via a `Topology` object)."
        )
        md.append("")

    # Also check any non-trivial placement/topology labels on nodes not already covered above
    covered_topo_keys = {
        str(lv.get("nodeLabel") or "")
        for t_obj in topologies_by_name.values()
        for lv in ((t_obj.get("spec") or {}).get("levels") or [])
        if isinstance(lv, dict)
    }
    extra_topo_keys = [
        lk
        for lk in sorted(label_val_sets.keys())
        if lk not in covered_topo_keys
        and lk != "kubernetes.io/hostname"
        and (
            lk in COMMON_TOPOLOGY_LABEL_CANDIDATES
            or any(sub in lk.lower() for sub in ("topology", "partition", "rack", "island", "cube", "zone", "block", "switch"))
        )
        and 1 < len(label_val_sets[lk]) < total_node_count
    ]
    if extra_topo_keys:
        md.append("### Additional Node Placement / Topology Labels on Nodes")
        md.append("")
        md.append("| Node Label Key | Distinct Values | Min Nodes/Value | Median (P50) | Mean Nodes/Value | Max Nodes/Value | Distribution Summary |")
        md.append("| :--- | ---: | ---: | ---: | ---: | ---: | :--- |")
        for lk in extra_topo_keys:
            dom_counts = Counter(
                str(((n.get("metadata") or {}).get("labels") or {}).get(lk))
                for n in all_nodes
                if ((n.get("metadata") or {}).get("labels") or {}).get(lk) is not None
            )
            st_c = calc_summary_stats([float(c) for c in dom_counts.values()])
            size_hist = Counter(dom_counts.values())
            hist_desc = ", ".join(
                f"{num_d} × {sz} nodes" for sz, num_d in sorted(size_hist.items(), reverse=True)
            )
            md.append(
                f"| `{lk}` | **{len(dom_counts):,}** | `{int(st_c['min'])}` | `{int(st_c['p50'])}` | "
                f"`{st_c['avg']:.1f}` | `{int(st_c['max'])}` | {hist_desc} |"
            )
        md.append("")

    # =======================================================================
    # Section 4: Initial Running Pods & Initial Cluster Utilization at t0
    # =======================================================================
    md.append("---")
    md.append("")
    md.append("## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$")
    md.append("")

    if total_initial_pods == 0:
        md.append(
            f"- **Initial Running Pods in `infra.json`**: **`0`** (the cluster starts completely empty at $t_0 = {fmt_epoch_iso(t0_epoch)}$)."
        )
        if admitted_at_start_wfs:
            md.append(
                f"- Note: `workflows.json` records **{len(admitted_at_start_wfs)}** workflow(s) marked with `workloadState: \"Admitted\"` at $t_0$."
            )
        md.append("- **Initial Cluster Utilization**: **`0.00%`** across all resources (`cpu`, `memory`, `pods`, accelerators).")
        md.append("")
    else:
        life_stats = calc_summary_stats(init_pod_lifetimes)
        md.append(
            f"At scenario start ($t_0 = {fmt_epoch_iso(t0_epoch)}$), **`{total_initial_pods:,}`** running `Pod` objects "
            f"are pre-placed across **`{len(init_nodes_used):,}`** of the **`{total_node_count:,}`** cluster nodes "
            f"(`{100.0 * len(init_nodes_used) / max(1, total_node_count):.1f}%` of nodes occupied; "
            f"`{total_node_count - len(init_nodes_used):,}` nodes completely free)."
        )
        if admitted_at_start_wfs:
            md.append(
                f"Additionally, `workflows.json` records **`{len(admitted_at_start_wfs):,}`** pre-existing `Admitted` "
                f"`JobSet` warmup workflow object(s) at $t_0$."
            )
        md.append("")

        # 4.1 Initial Cluster Utilization Table
        md.append("### 4.1 Initial Cluster Utilization Breakdown")
        md.append("")
        md.append(
            "| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |"
        )
        md.append("| :--- | ---: | ---: | ---: | ---: | ---: |")
        for rk in ordered_res_keys:
            if rk not in cluster_alloc and rk not in init_pod_requests_sum:
                continue
            used = init_pod_requests_sum.get(rk, 0.0)
            alloc_v = cluster_alloc.get(rk, 0.0)
            cap_v = cluster_cap.get(rk, 0.0)
            free_v = max(0.0, alloc_v - used)
            pct_alloc = (100.0 * used / alloc_v) if alloc_v > 0 else 0.0
            pct_cap = (100.0 * used / cap_v) if cap_v > 0 else 0.0
            md.append(
                f"| `{rk}` | **{fmt_quantity(used, rk)}** | {fmt_quantity(alloc_v, rk)} | "
                f"{fmt_quantity(free_v, rk)} | **{pct_alloc:.2f}%** | {pct_cap:.2f}% |"
            )
        md.append("")

        # 4.2 Initial Pod Characteristics (Remaining Lifetime & Resource Profiles)
        md.append("### 4.2 Initial Pod Remaining Lifetimes & Request Profiles")
        md.append("")
        if init_pod_lifetimes:
            md.append(
                f"- **Pods with Finite Remaining Lifetime (`deletionTime - t0`)**: **{len(init_pod_lifetimes):,}** pods — "
                f"`min={fmt_duration_short(life_stats['min'])}`, "
                f"`P50={fmt_duration_short(life_stats['p50'])}`, "
                f"`AVG={fmt_duration_short(life_stats['avg'])}`, "
                f"`P90={fmt_duration_short(life_stats['p90'])}`, "
                f"`P99={fmt_duration_short(life_stats['p99'])}`, "
                f"`max={fmt_duration_short(life_stats['max'])}`"
            )
        if init_pod_infinite_count > 0:
            md.append(
                f"- **Pods with No Deletion Event (Run Entire Scenario / Infinite Duration)**: **{init_pod_infinite_count:,}** pods"
            )
        pods_per_occ_node = calc_summary_stats([float(c) for c in init_nodes_used.values()])
        if init_nodes_used:
            md.append(
                f"- **Pods per Occupied Node (`{len(init_nodes_used):,}` active nodes)**: "
                f"`min={int(pods_per_occ_node['min'])}`, `P50={int(pods_per_occ_node['p50'])}`, "
                f"`AVG={pods_per_occ_node['avg']:.2f}`, `P90={int(pods_per_occ_node['p90'])}`, "
                f"`max={int(pods_per_occ_node['max'])}`"
            )
        md.append("")
        md.append("| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |")
        md.append("| :---: | :--- | ---: | ---: |")
        for rank_idx, (prof, p_cnt) in enumerate(init_pod_profiles.most_common(10), start=1):
            prof_str = ", ".join(f"`{k}: {v}`" for k, v in prof) if prof else "*(unspecified)*"
            md.append(f"| `{rank_idx}` | {prof_str} | **{p_cnt:,}** | {100.0 * p_cnt / total_initial_pods:.1f}% |")
        md.append("")

    # =======================================================================
    # Section 5: Kueue Setup — RF, Topology, Cohort, CQ, Namespaces & LQ
    # =======================================================================
    md.append("---")
    md.append("")
    md.append("## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues")
    md.append("")

    # 5.1 ResourceFlavors & Topology Binding
    md.append("### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding")
    md.append("")
    if flavors_by_name:
        md.append("| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |")
        md.append("| :--- | :--- | :--- | :--- |")
        for rf_name, rf_obj in sorted(flavors_by_name.items()):
            rf_spec = rf_obj.get("spec") or {}
            topo_ref = rf_spec.get("topologyName") or "*(none)*"
            n_lbls = rf_spec.get("nodeLabels") or {}
            n_lbl_str = ", ".join(f"`{k}={v}`" for k, v in sorted(n_lbls.items())) if n_lbls else "*(matches all nodes)*"
            taints = rf_spec.get("nodeTaints") or []
            tols = rf_spec.get("tolerations") or []
            tt_parts = []
            if taints:
                tt_parts.append(f"{len(taints)} taint(s)")
            if tols:
                tt_parts.append(f"{len(tols)} toleration(s)")
            tt_str = ", ".join(tt_parts) if tt_parts else "none"
            md.append(f"| `{rf_name}` | `{topo_ref}` | {n_lbl_str} | {tt_str} |")
        md.append("")
    else:
        md.append("*(No `ResourceFlavor` objects defined in `infra.json`.)*")
        md.append("")

    # 5.2 WorkloadPriorityClasses
    md.append("### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)")
    md.append("")
    if wpc_by_name:
        md.append("| Priority Class Name | Priority `value` | API Version | Description |")
        md.append("| :--- | ---: | :--- | :--- |")
        sorted_wpcs = sorted(
            wpc_by_name.values(),
            key=lambda w: (-int(w.get("value") or 0), str((w.get("metadata") or {}).get("name") or "")),
        )
        for w_obj in sorted_wpcs:
            wm = w_obj.get("metadata") or {}
            w_desc = str(w_obj.get("description") or "—")
            md.append(
                f"| `{wm.get('name', '?')}` | **`{w_obj.get('value', 0)}`** | `{w_obj.get('apiVersion', '')}` | {w_desc} |"
            )
        md.append("")
    else:
        md.append(
            "*(No explicit `WorkloadPriorityClass` objects defined in `infra.json`; all workloads run at default priority unless annotated.)*"
        )
        md.append("")

    # 5.3 Cohorts & ClusterQueues Setup, Quotas, Preemption & Fair Sharing
    md.append("### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing")
    md.append("")

    # Helper to summarize resourceGroups on a Cohort or ClusterQueue
    def extract_rg_totals(obj_spec: dict[str, Any]) -> tuple[dict[str, float], dict[str, str], dict[str, str], list[str]]:
        nom_totals: dict[str, float] = defaultdict(float)
        borrow_limits: dict[str, str] = {}
        lend_limits: dict[str, str] = {}
        flavors_used: list[str] = []
        for rg in obj_spec.get("resourceGroups") or []:
            for flv in rg.get("flavors") or []:
                f_name = str(flv.get("name") or "")
                if f_name and f_name not in flavors_used:
                    flavors_used.append(f_name)
                for r_entry in flv.get("resources") or []:
                    r_name = str(r_entry.get("name") or "")
                    if not r_name:
                        continue
                    if "nominalQuota" in r_entry and r_entry["nominalQuota"] is not None:
                        nom_totals[r_name] += parse_k8s_quantity(r_entry["nominalQuota"], r_name)
                    if r_entry.get("borrowingLimit") is not None:
                        borrow_limits[r_name] = str(r_entry["borrowingLimit"])
                    if r_entry.get("lendingLimit") is not None:
                        lend_limits[r_name] = str(r_entry["lendingLimit"])
        return dict(nom_totals), borrow_limits, lend_limits, flavors_used

    all_quota_resources: list[str] = []
    for cq_obj in cqs_by_name.values():
        nom, _, _, _ = extract_rg_totals(cq_obj.get("spec") or {})
        for rk in nom:
            if rk not in all_quota_resources:
                all_quota_resources.append(rk)
    for ch_obj in cohorts_by_name.values():
        nom, _, _, _ = extract_rg_totals(ch_obj.get("spec") or {})
        for rk in nom:
            if rk not in all_quota_resources:
                all_quota_resources.append(rk)
    # Order quota resources consistently
    quota_res_ordered = [rk for rk in ("cpu", "memory", "pods") if rk in all_quota_resources] + [
        rk for rk in sorted(all_quota_resources) if rk not in ("cpu", "memory", "pods")
    ]

    # Cohort summary
    referenced_cohorts: set[str] = set()
    for cq_obj in cqs_by_name.values():
        c_spec = cq_obj.get("spec") or {}
        c_ref = str(c_spec.get("cohortName") or c_spec.get("cohort") or "").strip()
        if c_ref:
            referenced_cohorts.add(c_ref)

    all_cohort_names = sorted(set(cohorts_by_name.keys()) | referenced_cohorts)
    if all_cohort_names:
        md.append("#### Cohort Configuration")
        md.append("")
        md.append(
            "| Cohort Name | Explicit CRD in `infra.json` | Parent Cohort | Fair-Sharing Weight | Member ClusterQueues | Direct Cohort Remainder Quota |"
        )
        md.append("| :--- | :---: | :--- | :---: | :---: | :--- |")
        for ch_name in all_cohort_names:
            ch_obj = cohorts_by_name.get(ch_name)
            ch_spec = (ch_obj or {}).get("spec") or {}
            has_crd = "Yes" if ch_obj is not None else "Implicit (via CQ `spec.cohort`)"
            parent_c = str(ch_spec.get("parentName") or ch_spec.get("parent") or "*(root)*")
            fs_w = str(((ch_spec.get("fairSharing") or {}).get("weight")) or "1 (default)")
            member_cqs = [
                cq_n
                for cq_n, cq_o in sorted(cqs_by_name.items())
                if str((cq_o.get("spec") or {}).get("cohortName") or (cq_o.get("spec") or {}).get("cohort") or "") == ch_name
            ]
            ch_nom, _, _, _ = extract_rg_totals(ch_spec)
            rem_str = (
                ", ".join(f"`{rk}`: {fmt_quantity(v, rk)}" for rk, v in ch_nom.items())
                if ch_nom
                else "none (`0`)"
            )
            md.append(
                f"| `{ch_name}` | {has_crd} | `{parent_c}` | `{fs_w}` | "
                f"**{len(member_cqs)}** ({', '.join(f'`{c}`' for c in member_cqs) if member_cqs else 'none'}) | {rem_str} |"
            )
        md.append("")

    # ClusterQueue Detailed Table
    md.append("#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration")
    md.append("")
    if cqs_by_name:
        res_hdr = " | ".join(f"Nominal `{rk}`" for rk in quota_res_ordered)
        res_sep = " | ".join("---:" for _ in quota_res_ordered)
        md.append(
            f"| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | {res_hdr} | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |"
        )
        md.append(f"| :--- | :--- | :--- | :---: | :--- | {res_sep} | :--- | :--- |")

        total_nom_across_all: dict[str, float] = defaultdict(float)
        for ch_obj in cohorts_by_name.values():
            ch_nom, _, _, _ = extract_rg_totals(ch_obj.get("spec") or {})
            for rk, rv in ch_nom.items():
                total_nom_across_all[rk] += rv

        for cq_name, cq_obj in sorted(cqs_by_name.items()):
            cq_spec = cq_obj.get("spec") or {}
            c_ref = str(cq_spec.get("cohortName") or cq_spec.get("cohort") or "*(standalone)*")
            q_strat = str(cq_spec.get("queueingStrategy") or "BestEffortFIFO")
            fs_w = str(((cq_spec.get("fairSharing") or {}).get("weight")) or "1")
            nom, b_lim, l_lim, flvs = extract_rg_totals(cq_spec)
            for rk, rv in nom.items():
                total_nom_across_all[rk] += rv

            nom_cells = " | ".join(
                f"**{fmt_quantity(nom.get(rk, 0.0), rk)}**" for rk in quota_res_ordered
            )
            bl_parts = []
            if b_lim:
                bl_parts.append("borrow: " + ", ".join(f"`{k}={v}`" for k, v in sorted(b_lim.items())))
            if l_lim:
                bl_parts.append("lend: " + ", ".join(f"`{k}={v}`" for k, v in sorted(l_lim.items())))
            bl_str = "; ".join(bl_parts) if bl_parts else "unbounded (default)"

            pre = cq_spec.get("preemption") or {}
            w_cq = str(pre.get("withinClusterQueue") or "Never")
            rec = str(pre.get("reclaimWithinCohort") or "Never")
            bwc_obj = pre.get("borrowWithinCohort") or {}
            bwc = str(bwc_obj.get("policy") or "Never") if isinstance(bwc_obj, dict) else str(bwc_obj)
            pre_str = f"`{w_cq}` / `{rec}` / `{bwc}`"

            md.append(
                f"| `{cq_name}` | `{c_ref}` | `{q_strat}` | `{fs_w}` | "
                f"{', '.join(f'`{f}`' for f in flvs) if flvs else '—'} | {nom_cells} | {bl_str} | {pre_str} |"
            )

        # Total row comparing total nominal quota (all CQs + Cohort remainder) against physical cluster capacity
        tot_cells = []
        for rk in quota_res_ordered:
            t_val = total_nom_across_all.get(rk, 0.0)
            c_cap = cluster_cap.get(rk, 0.0)
            pct_s = f" ({100.0 * t_val / c_cap:.1f}% of cluster)" if c_cap > 0 else ""
            tot_cells.append(f"**{fmt_quantity(t_val, rk)}**{pct_s}")
        md.append(
            f"| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | {' | '.join(tot_cells)} | — | — |"
        )
        md.append("")
    else:
        md.append("*(No `ClusterQueue` objects defined in `infra.json`.)*")
        md.append("")

    # 5.4 Namespaces & LocalQueues
    md.append("### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)")
    md.append("")
    all_ns_names = sorted(
        set(namespaces_by_name.keys())
        | {ns for ns, _ in lqs_by_key.keys()}
        | {w.namespace for w in unadmitted_wfs}
    )
    md.append(
        f"- **Namespaces (`{len(all_ns_names)}`)**: "
        + (", ".join(f"`{ns}`" for ns in all_ns_names) if all_ns_names else "*(default)*")
    )
    md.append("")

    if lqs_by_key:
        wf_count_by_lq: Counter[tuple[str, str]] = Counter(
            (w.namespace, w.local_queue) for w in unadmitted_wfs
        )
        md.append(
            "| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |"
        )
        md.append("| :--- | :--- | :--- | :---: | :--- | ---: |")
        for (lq_ns, lq_name), lq_obj in sorted(lqs_by_key.items()):
            target_cq = str(((lq_obj.get("spec") or {}).get("clusterQueue")) or "")
            cq_exists = target_cq in cqs_by_name
            target_cohort = (
                str(
                    ((cqs_by_name[target_cq].get("spec") or {}).get("cohortName"))
                    or ((cqs_by_name[target_cq].get("spec") or {}).get("cohort"))
                    or "*(standalone)*"
                )
                if cq_exists
                else "*(missing CQ)*"
            )
            w_cnt = wf_count_by_lq.get((lq_ns, lq_name), 0)
            md.append(
                f"| `{lq_ns}` | `{lq_name}` | `{target_cq}` | {'Yes' if cq_exists else '**NO**'} | "
                f"`{target_cohort}` | **{w_cnt:,}** |"
            )
        # Also note any (ns, lq) used by workloads that is NOT declared in infra.json!
        undeclared_lqs = sorted(
            (ns, lq, cnt)
            for (ns, lq), cnt in wf_count_by_lq.items()
            if (ns, lq) not in lqs_by_key and cnt > 0
        )
        for u_ns, u_lq, u_cnt in undeclared_lqs:
            md.append(
                f"| `{u_ns}` | `{u_lq}` *(undeclared in `infra.json`)* | *(none)* | **NO** | — | **{u_cnt:,}** |"
            )
        md.append("")
    else:
        md.append("*(No `LocalQueue` objects defined in `infra.json`.)*")
        md.append("")

    # =======================================================================
    # Section 6: Scenario Timeline
    # =======================================================================
    md.append("---")
    md.append("")
    md.append("## 6. Scenario Timeline & Arrival Dynamics")
    md.append("")

    inter_arrivals = [
        unadmitted_wfs[i].arrival_epoch - unadmitted_wfs[i - 1].arrival_epoch
        for i in range(1, len(unadmitted_wfs))
    ]
    ia_stats = calc_summary_stats(inter_arrivals)
    arrival_rate_per_min = (
        (len(unadmitted_wfs) / (test_length_sec / 60.0)) if test_length_sec > 0 else 0.0
    )
    arrival_rate_per_hr = (
        (len(unadmitted_wfs) / (test_length_sec / 3600.0)) if test_length_sec > 0 else 0.0
    )

    md.append("| Timeline Metric | Value |")
    md.append("| :--- | :--- |")
    md.append(f"| **Infrastructure Bootstrap Timestamp ($t_0$)** | `{fmt_epoch_iso(t0_epoch)}` |")
    md.append(
        f"| **First Unadmitted Workload Arrival ($t_{{\\text{{first}}}}$)** | `{fmt_epoch_iso(t_first_unadmitted)}` "
        f"(offset `+{max(0.0, (t_first_unadmitted or 0.0) - (t0_epoch or 0.0)):.2f}s` from $t_0$) |"
    )
    md.append(
        f"| **Last Unadmitted Workload Arrival ($t_{{\\text{{last}}}}$)** | `{fmt_epoch_iso(t_last_unadmitted)}` "
        f"(offset `+{max(0.0, (t_last_unadmitted or 0.0) - (t0_epoch or 0.0)):.2f}s` from $t_0$) |"
    )
    md.append(
        f"| **Active Test Arrival Duration ($T_{{\\text{{test}}}} = t_{{\\text{{last}}}} - t_{{\\text{{first}}}}$)** | "
        f"**{fmt_duration_human(test_length_sec)}** |"
    )
    md.append(
        f"| **Unadmitted Workload Submission Count** | **{len(unadmitted_wfs):,}** workloads "
        f"(plus **{len(admitted_at_start_wfs):,}** pre-admitted at $t_0$) |"
    )
    if test_length_sec > 0:
        md.append(
            f"| **Mean Arrival Rate ($\\lambda$)** | **{arrival_rate_per_min:.2f} workloads/min** "
            f"(**{arrival_rate_per_hr:.1f} workloads/hour**) |"
        )
    if inter_arrivals:
        md.append(
            f"| **Inter-Arrival Time ($\\Delta t_i$) Distribution** | "
            f"`min={ia_stats['min']:.2f}s`, `P50={ia_stats['p50']:.2f}s`, `AVG={ia_stats['avg']:.2f}s`, "
            f"`P90={ia_stats['p90']:.2f}s`, `P99={ia_stats['p99']:.2f}s`, `max={ia_stats['max']:.2f}s` |"
        )
    md.append("")

    if unadmitted_wfs:
        base_t = t_first_unadmitted or 0.0
        rel_arrivals = [max(0.0, w.arrival_epoch - base_t) for w in unadmitted_wfs]
        md.append("```text")
        md.append(render_timeline_arrival_histogram(rel_arrivals, test_length_sec))
        md.append("```")
        md.append("")

    # =======================================================================
    # Section 7: Workload Analysis — Priorities, Saturation, Per-LQ & Histograms
    # =======================================================================
    md.append("---")
    md.append("")
    md.append("## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms")
    md.append("")
    md.append("### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings")
    md.append("")
    md.append(
        "For any resource $r \\in \\{\\text{accelerators}, \\text{cpu}, \\text{memory}\\}$ with physical cluster capacity $C_{\\text{cluster}}(r)$ "
        "and total offered resource-seconds $W(r) = \\sum_{w} \\left(\\text{size}_w(r) \\times \\text{duration}_w\\right)$, "
        "let $T_{100\\%}(r) = W(r) / C_{\\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:"
    )
    md.append("")
    md.append(
        "$$\\text{Sat}_{\\text{window}}(r) = \\frac{W(r)}{T_{\\text{window}} \\times C_{\\text{cluster}}(r)} \\times 100\\% = \\frac{T_{100\\%}(r)}{T_{\\text{window}}} \\times 100\\%, "
        "\\qquad N_{100\\%}(T_{\\text{target}}) = \\frac{T_{100\\%}(r^*)}{T_{\\text{target}}}$$"
    )
    md.append("")
    md.append("#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\\text{test}}$) vs. Default `6h` (`21,600s`) Duration")
    md.append("")
    md.append(
        f"| Resource ($r$) | Physical Cluster Capacity $C_{{\\text{{cluster}}}}(r)$ | Total Offered Work ($1.0\\text{{x}}$) | Ideal `100%`-Util Service Time $T_{{100\\%}}(r)$ | (1) Saturation over Arrival Window ($T_{{\\text{{test}}}} = {fmt_duration_short(test_length_sec)}$) | (2) Saturation over Default `6h` ($21{{,}}600\\text{{s}}$) |"
    )
    md.append("| :--- | ---: | ---: | ---: | ---: | ---: |")

    sat_summary_res_keys: list[str] = []
    if primary_accel_resource and primary_accel_resource in cluster_cap:
        sat_summary_res_keys.append(primary_accel_resource)
    for extra_rk in ("cpu", "memory"):
        if extra_rk in cluster_cap and extra_rk not in sat_summary_res_keys:
            sat_summary_res_keys.append(extra_rk)

    for rk in sat_summary_res_keys:
        c_val = cluster_cap.get(rk, 0.0)
        w_sec = compute_offered_resource_seconds(unadmitted_wfs, rk)
        w_hrs = w_sec / 3600.0
        t100_rk = compute_ideal_full_capacity_sec(unadmitted_wfs, rk)
        s_arr = compute_saturation_over_window_pct(unadmitted_wfs, rk, test_length_sec)
        s_6h = compute_saturation_over_window_pct(unadmitted_wfs, rk, DEFAULT_SIX_HOUR_SEC)
        is_dom = rk == primary_sat_resource
        rk_disp = f"**`{rk}`** *(bottleneck)*" if is_dom else f"`{rk}`"
        unit_hr = f"`{rk}`-h" if rk not in ("cpu", "memory") else ("core-h" if rk == "cpu" else "GiB-h")
        s_arr_str = (
            f"**{s_arr:.2f}%**"
            if (s_arr is not None and is_dom)
            else (f"{s_arr:.2f}%" if s_arr is not None else "n/a ($T_{\\text{test}}=0$)")
        )
        s_6h_str = (
            f"**{s_6h:.2f}%**"
            if (s_6h is not None and is_dom)
            else (f"{s_6h:.2f}%" if s_6h is not None else "n/a")
        )
        t100_str = fmt_duration_human(t100_rk) if t100_rk is not None else "n/a"
        md.append(
            f"| {rk_disp} | {fmt_quantity(c_val, rk)} | `{w_hrs:,.2f}` {unit_hr} | `{t100_str}` | {s_arr_str} | {s_6h_str} |"
        )
    md.append("")

    md.append("#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)")
    md.append("")
    if plan_5m and plan_10m:
        md.append(
            f"| Target Wall-Clock Window ($T_{{\\text{{target}}}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{{100\\%}}$) | Recommended `--duration` | Compressed Arrival Span ($T_{{\\text{{test}}}} / N_{{100\\%}}$) | Execution & Drain Behavior |"
        )
        md.append("| :--- | :---: | :---: | :---: | :---: | :--- |")
        for plan in (plan_5m, plan_10m):
            md.append(
                f"| **`{plan['target_label']}` (`{int(plan['target_sec'])}s`)** | `{primary_sat_resource}` | **`--speedup {plan['speedup_str']}`** | **`--duration {plan['rec_duration_flag']}`** | `{plan['comp_arrival_sec']:.1f}s` | {plan['note']} |"
            )
        md.append("")
        md.append("```bash")
        md.append(
            f"# Run at 100% offered {primary_sat_resource} saturation in a 5-minute (300s) wall-clock window:"
        )
        md.append(
            f"python3 deploy/scripts/testrunner.py --scenario {rel_dir} --speedup {plan_5m['speedup_str']} --duration {plan_5m['rec_duration_flag']}"
        )
        md.append("")
        md.append(
            f"# Run at 100% offered {primary_sat_resource} saturation in a 10-minute (600s) wall-clock window:"
        )
        md.append(
            f"python3 deploy/scripts/testrunner.py --scenario {rel_dir} --speedup {plan_10m['speedup_str']} --duration {plan_10m['rec_duration_flag']}"
        )
        md.append("```")
        md.append("")

    # 7.1 Workload Count & Saturation by Priority
    md.append("### 7.1 Workload Breakdown & Cluster Saturation by Priority Class")
    md.append("")

    wfs_by_prio: dict[str, list[ParsedWorkload]] = defaultdict(list)
    for w in unadmitted_wfs:
        wfs_by_prio[w.priority_class].append(w)

    ordered_priorities = sorted(
        wfs_by_prio.keys(),
        key=lambda p: (-priority_value_by_name.get(p, 0), p),
    )

    if primary_accel_resource and total_wf_accel > 0:
        md.append(
            f"| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `{primary_accel_resource}`-Hours | `{primary_accel_resource}` Saturation | `cpu` Saturation | `memory` Saturation |"
        )
        md.append(
            "| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |"
        )
    else:
        md.append(
            "| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered Core-Hours | `cpu` Saturation | `memory` Saturation |"
        )
        md.append("| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: |")

    def _format_prio_row(label_str: str, prio_val_str: str, subset: list[ParsedWorkload], bold_total: bool = False) -> str:
        w_cnt = len(subset)
        w_share = (100.0 * w_cnt / len(unadmitted_wfs)) if unadmitted_wfs else 0.0
        p_st = calc_summary_stats([float(w.total_pods) for w in subset])
        d_st = calc_summary_stats([w.duration_sec for w in subset])
        gang_str = f"`{int(p_st['min'])} / {int(p_st['p50'])} / {int(p_st['p90'])} / {int(p_st['max'])}`"
        dur_str = (
            f"`{d_st['p50']:.0f}s / {d_st['avg']:.0f}s / {d_st['p90']:.0f}s / {d_st['p99']:.0f}s`"
        )
        c_sat = compute_saturation_pct(subset, "cpu")
        m_sat = compute_saturation_pct(subset, "memory")
        c_sat_s = f"**{c_sat:.2f}%**" if (c_sat is not None and bold_total) else (f"{c_sat:.2f}%" if c_sat is not None else "n/a")
        m_sat_s = f"**{m_sat:.2f}%**" if (m_sat is not None and bold_total) else (f"{m_sat:.2f}%" if m_sat is not None else "n/a")

        if primary_accel_resource and total_wf_accel > 0:
            acc_hrs = sum(w.total_requests.get(primary_accel_resource, 0.0) * w.duration_sec for w in subset) / 3600.0
            a_sat = compute_saturation_pct(subset, primary_accel_resource)
            a_sat_s = f"**{a_sat:.2f}%**" if a_sat is not None else "n/a"
            return (
                f"| {label_str} | {prio_val_str} | {'**' if bold_total else ''}{w_cnt:,}{'**' if bold_total else ''} | "
                f"{w_share:.1f}% | {int(p_st['sum']):,} | {gang_str} | {dur_str} | "
                f"{acc_hrs:,.1f} | {a_sat_s} | {c_sat_s} | {m_sat_s} |"
            )
        else:
            core_hrs = sum(w.total_requests.get("cpu", 0.0) * w.duration_sec for w in subset) / 3600.0
            return (
                f"| {label_str} | {prio_val_str} | {'**' if bold_total else ''}{w_cnt:,}{'**' if bold_total else ''} | "
                f"{w_share:.1f}% | {int(p_st['sum']):,} | {gang_str} | {dur_str} | "
                f"{core_hrs:,.1f} | {c_sat_s} | {m_sat_s} |"
            )

    for p_name in ordered_priorities:
        p_val = priority_value_by_name.get(p_name, 0)
        md.append(_format_prio_row(f"`{p_name}`", f"`{p_val}`", wfs_by_prio[p_name], bold_total=False))
    if unadmitted_wfs:
        md.append(_format_prio_row("**ALL (Total)**", "—", unadmitted_wfs, bold_total=True))
    md.append("")

    # 7.2 Dissection per LocalQueue
    md.append("### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)")
    md.append("")

    wfs_by_lq: dict[tuple[str, str], list[ParsedWorkload]] = defaultdict(list)
    for w in unadmitted_wfs:
        wfs_by_lq[(w.namespace, w.local_queue)].append(w)

    ordered_lq_keys = sorted(
        set(lqs_by_key.keys()) | set(wfs_by_lq.keys()),
        key=lambda k: (-len(wfs_by_lq.get(k, [])), k[0], k[1]),
    )

    if primary_accel_resource and total_wf_accel > 0:
        md.append(
            f"| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `{primary_accel_resource}`-h | Cluster `{primary_accel_resource}` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |"
        )
        md.append(
            "| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |"
        )
    else:
        md.append(
            "| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered Core-Hours | Cluster `cpu` Saturation | Target CQ Nominal `cpu` Saturation | Priority Breakdown (Count) |"
        )
        md.append(
            "| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |"
        )

    for lq_ns, lq_name in ordered_lq_keys:
        subset = wfs_by_lq.get((lq_ns, lq_name), [])
        lq_obj = lqs_by_key.get((lq_ns, lq_name))
        target_cq = str(((lq_obj or {}).get("spec") or {}).get("clusterQueue") or "*(undeclared)*")
        cq_obj = cqs_by_name.get(target_cq)
        cq_nom, _, _, _ = extract_rg_totals((cq_obj or {}).get("spec") or {})

        w_cnt = len(subset)
        w_share = (100.0 * w_cnt / len(unadmitted_wfs)) if unadmitted_wfs else 0.0
        p_st = calc_summary_stats([float(w.total_pods) for w in subset])
        d_st = calc_summary_stats([w.duration_sec for w in subset])
        dur_str = (
            f"`{d_st['p50']:.0f}s / {d_st['avg']:.0f}s / {d_st['p90']:.0f}s / {d_st['p99']:.0f}s`"
            if subset
            else "—"
        )
        prio_c = Counter(w.priority_class for w in subset)
        prio_brk = ", ".join(
            f"`{p}`:{prio_c[p]}" for p in ordered_priorities if prio_c.get(p, 0) > 0
        ) or "none"

        if primary_accel_resource and total_wf_accel > 0:
            offered_sec = sum(w.total_requests.get(primary_accel_resource, 0.0) * w.duration_sec for w in subset)
            cl_sat = compute_saturation_pct(subset, primary_accel_resource)
            cq_q_val = cq_nom.get(primary_accel_resource, 0.0)
            cq_sat = (
                (100.0 * offered_sec / (test_length_sec * cq_q_val))
                if (test_length_sec > 0 and cq_q_val > 0)
                else None
            )
            md.append(
                f"| `{lq_ns}/{lq_name}` | `{target_cq}` | **{w_cnt:,}** | {w_share:.1f}% | "
                f"{int(p_st['sum']):,} | {dur_str} | {offered_sec / 3600.0:,.1f} | "
                f"**{cl_sat:.2f}%** | {f'**{cq_sat:.2f}%**' if cq_sat is not None else 'n/a'} | {prio_brk} |"
                if cl_sat is not None
                else f"| `{lq_ns}/{lq_name}` | `{target_cq}` | **{w_cnt:,}** | {w_share:.1f}% | "
                f"{int(p_st['sum']):,} | {dur_str} | {offered_sec / 3600.0:,.1f} | n/a | n/a | {prio_brk} |"
            )
        else:
            offered_sec = sum(w.total_requests.get("cpu", 0.0) * w.duration_sec for w in subset)
            cl_sat = compute_saturation_pct(subset, "cpu")
            cq_q_val = cq_nom.get("cpu", 0.0)
            cq_sat = (
                (100.0 * offered_sec / (test_length_sec * cq_q_val))
                if (test_length_sec > 0 and cq_q_val > 0)
                else None
            )
            md.append(
                f"| `{lq_ns}/{lq_name}` | `{target_cq}` | **{w_cnt:,}** | {w_share:.1f}% | "
                f"{int(p_st['sum']):,} | {dur_str} | {offered_sec / 3600.0:,.1f} | "
                f"{f'**{cl_sat:.2f}%**' if cl_sat is not None else 'n/a'} | "
                f"{f'**{cq_sat:.2f}%**' if cq_sat is not None else 'n/a'} | {prio_brk} |"
            )
    md.append("")

    # Detailed per-LQ x per-Priority breakdown
    if ordered_lq_keys and unadmitted_wfs:
        md.append("#### Detailed Per-`LocalQueue` × Priority Dissection")
        md.append("")
        for lq_ns, lq_name in ordered_lq_keys:
            subset = wfs_by_lq.get((lq_ns, lq_name), [])
            if not subset:
                continue
            lq_obj = lqs_by_key.get((lq_ns, lq_name))
            target_cq = str(((lq_obj or {}).get("spec") or {}).get("clusterQueue") or "*(undeclared)*")
            md.append(f"<details>")
            md.append(
                f"<summary><b>LocalQueue <code>{lq_ns}/{lq_name}</code> → ClusterQueue <code>{target_cq}</code></b> ({len(subset):,} workloads)</summary>"
            )
            md.append("")
            if primary_accel_resource and total_wf_accel > 0:
                md.append(
                    f"| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `{primary_accel_resource}` Saturation | Cluster `cpu` Saturation |"
                )
                md.append("| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |")
            else:
                md.append(
                    "| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `cpu` Saturation | Cluster `memory` Saturation |"
                )
                md.append("| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |")

            for p_name in ordered_priorities:
                p_sub = [w for w in subset if w.priority_class == p_name]
                if not p_sub:
                    continue
                p_val = priority_value_by_name.get(p_name, 0)
                d_st = calc_summary_stats([w.duration_sec for w in p_sub])
                dur_s = f"`{d_st['p50']:.0f}s / {d_st['avg']:.0f}s / {d_st['p90']:.0f}s / {d_st['p99']:.0f}s`"
                c_sat = compute_saturation_pct(p_sub, "cpu")
                if primary_accel_resource and total_wf_accel > 0:
                    a_sat = compute_saturation_pct(p_sub, primary_accel_resource)
                    md.append(
                        f"| `{p_name}` | `{p_val}` | **{len(p_sub):,}** | {100.0 * len(p_sub) / len(subset):.1f}% | "
                        f"{sum(w.total_pods for w in p_sub):,} | {dur_s} | "
                        f"{f'{a_sat:.2f}%' if a_sat is not None else 'n/a'} | "
                        f"{f'{c_sat:.2f}%' if c_sat is not None else 'n/a'} |"
                    )
                else:
                    m_sat = compute_saturation_pct(p_sub, "memory")
                    md.append(
                        f"| `{p_name}` | `{p_val}` | **{len(p_sub):,}** | {100.0 * len(p_sub) / len(subset):.1f}% | "
                        f"{sum(w.total_pods for w in p_sub):,} | {dur_s} | "
                        f"{f'{c_sat:.2f}%' if c_sat is not None else 'n/a'} | "
                        f"{f'{m_sat:.2f}%' if m_sat is not None else 'n/a'} |"
                    )
            md.append("")
            md.append("</details>")
            md.append("")

    # 7.3 Workload Duration Histograms (Aggregated & Per Priority)
    md.append("### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)")
    md.append("")
    md.append("#### Aggregated Duration Histogram (All Unadmitted Workloads)")
    md.append("")
    md.append("```text")
    md.append(
        render_duration_histogram(
            "ALL PRIORITIES (Aggregated)",
            [w.duration_sec for w in unadmitted_wfs],
        )
    )
    md.append("```")
    md.append("")

    if ordered_priorities:
        md.append("#### Per-Priority Duration Histograms")
        md.append("")
        md.append("```text")
        hist_blocks: list[str] = []
        for p_name in ordered_priorities:
            p_val = priority_value_by_name.get(p_name, 0)
            p_durs = [w.duration_sec for w in wfs_by_prio[p_name]]
            hist_blocks.append(
                render_duration_histogram(
                    f"Priority: {p_name} (value={p_val})",
                    p_durs,
                )
            )
        md.append("\n\n".join(hist_blocks))
        md.append("```")
        md.append("")

    return "\n".join(md).rstrip() + "\n"


def discover_scenario_dirs(root: Path) -> list[Path]:
    """Find all directories under `root` containing `infra.json`."""
    dirs = [p.parent for p in root.rglob("infra.json") if p.is_file()]
    return sorted(set(dirs))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate comprehensive README.md summaries for kueue-optimizer test scenario directories."
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        type=Path,
        help="One or more scenario directories containing infra.json, workflows.json, and execution-time.json.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Discover and generate README.md for all directories under --root containing infra.json.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=REPO_ROOT / "testscenarios",
        help="Root directory searched when --all is specified (default: testscenarios/).",
    )
    parser.add_argument(
        "--filename",
        default="README.md",
        help="Output markdown filename written inside each scenario directory (default: README.md).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Explicit output path (only valid when processing a single scenario directory).",
    )
    args = parser.parse_args(argv)

    targets: list[Path] = []
    if args.all or not args.scenarios:
        targets.extend(discover_scenario_dirs(args.root.resolve()))
    for s in args.scenarios:
        targets.append(s.resolve())

    # Deduplicate while preserving order
    seen: set[Path] = set()
    unique_targets: list[Path] = []
    for t in targets:
        if t not in seen:
            seen.add(t)
            unique_targets.append(t)

    if not unique_targets:
        print("ERROR: No scenario directories found.", file=sys.stderr)
        return 1

    if args.output and len(unique_targets) > 1:
        print("ERROR: --output can only be specified when generating for a single scenario directory.", file=sys.stderr)
        return 1

    for idx, sdir in enumerate(unique_targets, start=1):
        out_path = args.output.resolve() if args.output else (sdir / args.filename)
        content = generate_readme_for_scenario(sdir)
        out_path.write_text(content, encoding="utf-8")
        try:
            rel_out = out_path.relative_to(REPO_ROOT)
        except ValueError:
            rel_out = out_path
        print(f"[{idx:>2}/{len(unique_targets):>2}] Wrote {rel_out} ({len(content):,} bytes)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
