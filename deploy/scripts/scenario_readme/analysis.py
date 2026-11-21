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

"""Scenario parsing, node group classification, and saturation analysis for README generation."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import math
from pathlib import Path
import re
from typing import Any

from chamber.constants import ROOT_DIR
from chamber.parsers import (
    calc_summary_stats,
    parse_k8s_quantity,
    parse_positive_duration_annotation,
    parse_rfc3339_epoch,
)
from chamber.scenario import (
    ExecutionTimeIndex,
    collect_known_parent_workflows,
    collect_not_admitted_workflow_admit_epochs,
    extract_pod_spec_resources,
    extract_workflow_pods_and_requests,
    filter_initial_running_pods,
    index_execution_times,
    is_child_workflow_owned_by_tracked_parent,
)
from chamber.yaml_utils import load_jsonl_events


DEFAULT_SIX_HOUR_SEC = 21600.0

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

_NUMERIC_INDEX_SUFFIX_RE = re.compile(r"[-_]\d+$")


def _is_unique_node_label(label_key: str) -> bool:
    if label_key in UNIQUE_NODE_LABEL_KEYS:
        return True
    lk = label_key.lower()
    return any(sub in lk for sub in UNIQUE_NODE_LABEL_SUBSTRINGS)


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


def extract_rg_totals(obj_spec: dict[str, Any]) -> tuple[dict[str, float], dict[str, str], dict[str, str], list[str]]:
    """Summarize resourceGroups nominalQuota, borrowing/lending limits, and flavors on a Cohort or ClusterQueue."""
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


def format_speedup_val(val: float) -> str:
    if val >= 100.0:
        return f"{val:.1f}"
    if val >= 10.0:
        return f"{val:.2f}"
    return f"{val:.3f}"


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


@dataclass
class ScenarioAnalysis:
    """Structured analysis of a scenario directory used to render README.md sections."""

    scenario_dir: Path
    rel_dir: str
    scenario_title: str
    infra_events_count: int
    workflow_events_count: int
    exec_index: ExecutionTimeIndex

    nodes_by_name: dict[str, dict[str, Any]]
    topologies_by_name: dict[str, dict[str, Any]]
    flavors_by_name: dict[str, dict[str, Any]]
    wpc_by_name: dict[str, dict[str, Any]]
    cohorts_by_name: dict[str, dict[str, Any]]
    cqs_by_name: dict[str, dict[str, Any]]
    lqs_by_key: dict[tuple[str, str], dict[str, Any]]
    namespaces_by_name: dict[str, dict[str, Any]]

    all_nodes: list[dict[str, Any]]
    total_node_count: int
    label_val_sets: dict[str, set[str]]
    non_unique_label_keys: set[str]
    node_groups: list[list[dict[str, Any]]]
    cluster_cap: dict[str, float]
    cluster_alloc: dict[str, float]
    accel_resources: list[str]
    primary_accel_resource: str | None
    ordered_res_keys: list[str]

    priority_value_by_name: dict[str, int]
    admitted_at_start_wfs: list[ParsedWorkload]
    unadmitted_wfs: list[ParsedWorkload]
    t0_epoch: float | None
    t_first_unadmitted: float | None
    t_last_unadmitted: float | None
    test_length_sec: float

    total_initial_pods: int
    init_pod_requests_sum: dict[str, float]
    init_pod_lifetimes: list[float]
    init_pod_infinite_count: int
    init_nodes_used: Counter[str]
    init_ns_counts: Counter[str]
    init_pod_profiles: Counter[tuple[tuple[str, str], ...]]

    total_wf_accel: float
    primary_sat_resource: str
    primary_sat_label: str

    def offered_resource_seconds(self, workloads_subset: list[ParsedWorkload], res_key: str) -> float:
        return sum(w.total_requests.get(res_key, 0.0) * w.duration_sec for w in workloads_subset)

    def saturation_over_window_pct(
        self, workloads_subset: list[ParsedWorkload], res_key: str, window_sec: float
    ) -> float | None:
        if window_sec <= 0.0:
            return None
        cap_val = self.cluster_cap.get(res_key, 0.0)
        if cap_val <= 0.0:
            return None
        offered = self.offered_resource_seconds(workloads_subset, res_key)
        return (offered / (window_sec * cap_val)) * 100.0

    def saturation_pct(self, workloads_subset: list[ParsedWorkload], res_key: str) -> float | None:
        return self.saturation_over_window_pct(workloads_subset, res_key, self.test_length_sec)

    def ideal_full_capacity_sec(self, workloads_subset: list[ParsedWorkload], res_key: str) -> float | None:
        """Ideal 1.0x service time T_100%(r) = sum(size_w(r) * dur_w) / C_cluster(r)."""
        cap_val = self.cluster_cap.get(res_key, 0.0)
        if cap_val <= 0.0:
            return None
        return self.offered_resource_seconds(workloads_subset, res_key) / cap_val

    def build_target_window_plan(self, target_sec: float, target_label: str) -> dict[str, Any] | None:
        """Compute exact --speedup and --duration settings so offered work on primary_sat_resource
        saturates 100% of the cluster over target_sec wall-clock seconds."""
        t100 = self.ideal_full_capacity_sec(self.unadmitted_wfs, self.primary_sat_resource)
        if t100 is None or t100 <= 0.0 or target_sec <= 0.0:
            return None
        speedup_100 = t100 / target_sec
        comp_arrival_sec = self.test_length_sec / speedup_100 if self.test_length_sec > 0.0 else 0.0
        comp_p50_dur_sec = (
            (calc_summary_stats([w.duration_sec for w in self.unadmitted_wfs])["p50"] / speedup_100)
            if self.unadmitted_wfs
            else 0.0
        )
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


def _parse_infra_events(infra_events: list[dict[str, Any]]) -> dict[str, Any]:
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

    return {
        "nodes_by_name": nodes_by_name,
        "topologies_by_name": topologies_by_name,
        "flavors_by_name": flavors_by_name,
        "wpc_by_name": wpc_by_name,
        "cohorts_by_name": cohorts_by_name,
        "cqs_by_name": cqs_by_name,
        "lqs_by_key": lqs_by_key,
        "namespaces_by_name": namespaces_by_name,
        "pod_new_events": pod_new_events,
        "pod_delete_epochs": pod_delete_epochs,
        "infra_timestamps": infra_timestamps,
    }


def _classify_nodes(nodes_by_name: dict[str, dict[str, Any]]) -> dict[str, Any]:
    all_nodes = list(nodes_by_name.values())
    total_node_count = len(all_nodes)

    label_val_sets: dict[str, set[str]] = defaultdict(set)
    for n_obj in all_nodes:
        for lk, lv in ((n_obj.get("metadata") or {}).get("labels") or {}).items():
            label_val_sets[lk].add(str(lv))

    non_unique_label_keys: set[str] = set()
    for lk, vset in label_val_sets.items():
        if _is_unique_node_label(lk):
            continue
        if len(vset) <= max(8, total_node_count // 8):
            non_unique_label_keys.add(lk)

    grouped_nodes_map: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for n_obj in all_nodes:
        fp = node_group_fingerprint(n_obj, non_unique_label_keys)
        grouped_nodes_map[fp].append(n_obj)

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

    accel_resources = sorted(
        rk for rk in cluster_cap.keys() if rk not in ("cpu", "memory", "pods", "ephemeral-storage")
    )
    primary_accel_resource = accel_resources[0] if accel_resources else None
    ordered_res_keys = ["cpu", "memory", "pods"] + [
        rk for rk in sorted(cluster_cap.keys()) if rk not in ("cpu", "memory", "pods")
    ]
    return {
        "all_nodes": all_nodes,
        "total_node_count": total_node_count,
        "label_val_sets": label_val_sets,
        "non_unique_label_keys": non_unique_label_keys,
        "node_groups": node_groups,
        "cluster_cap": dict(cluster_cap),
        "cluster_alloc": dict(cluster_alloc),
        "accel_resources": accel_resources,
        "primary_accel_resource": primary_accel_resource,
        "ordered_res_keys": ordered_res_keys,
    }


def _parse_workflows(
    workflow_events: list[dict[str, Any]],
    wpc_by_name: dict[str, dict[str, Any]],
    exec_index: ExecutionTimeIndex,
    t0_epoch: float | None,
) -> tuple[dict[str, int], list[ParsedWorkload], list[ParsedWorkload], dict[str, float]]:
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
    return priority_value_by_name, admitted_at_start_wfs, unadmitted_wfs, earliest_admit_by_ns


def _summarize_initial_pods(
    pod_new_events: dict[tuple[str, str], tuple[float, dict[str, Any]]],
    pod_delete_epochs: dict[tuple[str, str], float],
    t0_epoch: float | None,
    t_first_unadmitted: float | None,
    unadmitted_wfs: list[ParsedWorkload],
    earliest_admit_by_ns: dict[str, float],
) -> dict[str, Any]:
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

    for p_ns, _p_name, _p_ep, pl, del_ep in qualifying_initial_pods:
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

    return {
        "total_initial_pods": len(qualifying_initial_pods),
        "init_pod_requests_sum": dict(init_pod_requests_sum),
        "init_pod_lifetimes": init_pod_lifetimes,
        "init_pod_infinite_count": init_pod_infinite_count,
        "init_nodes_used": init_nodes_used,
        "init_ns_counts": init_ns_counts,
        "init_pod_profiles": init_pod_profiles,
    }


def analyze_scenario(scenario_dir: Path) -> ScenarioAnalysis:
    """Load and analyze `infra.json`, `workflows.json`, and `execution-time.json` in `scenario_dir`."""
    resolved_dir = scenario_dir.resolve()
    try:
        rel_dir = str(resolved_dir.relative_to(ROOT_DIR))
    except ValueError:
        rel_dir = str(resolved_dir)
    scenario_title = (
        resolved_dir.name
        if resolved_dir.name != "default"
        else f"{resolved_dir.parent.name}/{resolved_dir.name}"
    )

    infra_events = load_jsonl_events(resolved_dir / "infra.json", missing_ok=True)
    workflow_events = load_jsonl_events(resolved_dir / "workflows.json", missing_ok=True)
    exec_index = index_execution_times(resolved_dir / "execution-time.json", missing_ok=True)

    infra = _parse_infra_events(infra_events)
    wf_timestamps = [
        ep
        for ev in workflow_events
        if (ep := parse_rfc3339_epoch(ev.get("timestamp"))) is not None
    ]
    all_t0_candidates = infra["infra_timestamps"] + wf_timestamps
    t0_epoch = min(all_t0_candidates) if all_t0_candidates else None

    nodes_info = _classify_nodes(infra["nodes_by_name"])
    priority_value_by_name, admitted_at_start_wfs, unadmitted_wfs, earliest_admit_by_ns = _parse_workflows(
        workflow_events, infra["wpc_by_name"], exec_index, t0_epoch
    )

    if unadmitted_wfs:
        t_first_unadmitted: float | None = unadmitted_wfs[0].arrival_epoch
        t_last_unadmitted: float | None = unadmitted_wfs[-1].arrival_epoch
        test_length_sec = max(0.0, t_last_unadmitted - t_first_unadmitted)
    else:
        t_first_unadmitted = t0_epoch
        t_last_unadmitted = t0_epoch
        test_length_sec = 0.0

    init_pods = _summarize_initial_pods(
        infra["pod_new_events"],
        infra["pod_delete_epochs"],
        t0_epoch,
        t_first_unadmitted,
        unadmitted_wfs,
        earliest_admit_by_ns,
    )

    primary_accel_resource = nodes_info["primary_accel_resource"]
    cluster_cap = nodes_info["cluster_cap"]
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

    return ScenarioAnalysis(
        scenario_dir=resolved_dir,
        rel_dir=rel_dir,
        scenario_title=scenario_title,
        infra_events_count=len(infra_events),
        workflow_events_count=len(workflow_events),
        exec_index=exec_index,
        nodes_by_name=infra["nodes_by_name"],
        topologies_by_name=infra["topologies_by_name"],
        flavors_by_name=infra["flavors_by_name"],
        wpc_by_name=infra["wpc_by_name"],
        cohorts_by_name=infra["cohorts_by_name"],
        cqs_by_name=infra["cqs_by_name"],
        lqs_by_key=infra["lqs_by_key"],
        namespaces_by_name=infra["namespaces_by_name"],
        all_nodes=nodes_info["all_nodes"],
        total_node_count=nodes_info["total_node_count"],
        label_val_sets=nodes_info["label_val_sets"],
        non_unique_label_keys=nodes_info["non_unique_label_keys"],
        node_groups=nodes_info["node_groups"],
        cluster_cap=cluster_cap,
        cluster_alloc=nodes_info["cluster_alloc"],
        accel_resources=nodes_info["accel_resources"],
        primary_accel_resource=primary_accel_resource,
        ordered_res_keys=nodes_info["ordered_res_keys"],
        priority_value_by_name=priority_value_by_name,
        admitted_at_start_wfs=admitted_at_start_wfs,
        unadmitted_wfs=unadmitted_wfs,
        t0_epoch=t0_epoch,
        t_first_unadmitted=t_first_unadmitted,
        t_last_unadmitted=t_last_unadmitted,
        test_length_sec=test_length_sec,
        total_initial_pods=init_pods["total_initial_pods"],
        init_pod_requests_sum=init_pods["init_pod_requests_sum"],
        init_pod_lifetimes=init_pods["init_pod_lifetimes"],
        init_pod_infinite_count=init_pods["init_pod_infinite_count"],
        init_nodes_used=init_pods["init_nodes_used"],
        init_ns_counts=init_pods["init_ns_counts"],
        init_pod_profiles=init_pods["init_pod_profiles"],
        total_wf_accel=total_wf_accel,
        primary_sat_resource=primary_sat_resource,
        primary_sat_label=primary_sat_label,
    )
