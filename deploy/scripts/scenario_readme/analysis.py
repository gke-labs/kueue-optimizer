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
from dataclasses import dataclass, field
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
    WorkloadExecutionInfo,
    collect_known_parent_workflows,
    collect_not_admitted_workflow_admit_epochs,
    extract_pod_spec_resources,
    extract_workflow_pods_and_requests,
    extract_workflow_podset_shapes,
    filter_initial_running_pods,
    index_execution_times,
    is_child_workflow_owned_by_tracked_parent,
)
from chamber.yaml_utils import load_jsonl_events
from runner.validation import SYSTEM_NAMESPACES


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
    if val >= 0.001 or val <= 0.0:
        return f"{val:.3f}"
    return f"{val:.3g}"


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
    ready_replicas: int | None = None
    desired_replicas: int | None = None
    incarnation_index: int = 0
    podset_shapes: list[tuple[str, int, dict[str, float]]] = field(default_factory=list)
    creation_epoch: float | None = None
    deleted_epoch: float | None = None
    exec_info: WorkloadExecutionInfo | None = None
    has_kueue_signal: bool = True
    pg_total_count: int | None = None


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
    kueue_configuration: dict[str, Any] | None

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
    post_t0_node_events: list[tuple[str, str, float | None]] = field(default_factory=list)
    synthesized_initial_pods: int = 0

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


def _merge_node_update(existing: dict[str, Any], update_pl: dict[str, Any]) -> dict[str, Any]:
    """Merge an in-window Node Update payload into an existing Node manifest without wiping omitted fields."""
    merged: dict[str, Any] = dict(existing)
    for k, v in update_pl.items():
        if k not in ("metadata", "spec", "status") and v is not None:
            merged[k] = v

    old_meta = existing.get("metadata") if isinstance(existing.get("metadata"), dict) else {}
    new_meta = update_pl.get("metadata") if isinstance(update_pl.get("metadata"), dict) else {}
    if old_meta or new_meta:
        merged_meta = dict(old_meta)
        for mk, mv in new_meta.items():
            if mk in ("labels", "annotations"):
                if isinstance(mv, dict) and mv:
                    old_sub = old_meta.get(mk) if isinstance(old_meta.get(mk), dict) else {}
                    merged_meta[mk] = {**old_sub, **mv}
            elif mv is not None:
                merged_meta[mk] = mv
        merged["metadata"] = merged_meta

    old_spec = existing.get("spec") if isinstance(existing.get("spec"), dict) else {}
    new_spec = update_pl.get("spec") if isinstance(update_pl.get("spec"), dict) else {}
    if old_spec or new_spec:
        merged_spec = dict(old_spec)
        for sk, sv in new_spec.items():
            if sv is not None:
                merged_spec[sk] = sv
        merged["spec"] = merged_spec

    old_status = existing.get("status") if isinstance(existing.get("status"), dict) else {}
    new_status = update_pl.get("status") if isinstance(update_pl.get("status"), dict) else {}
    if old_status or new_status:
        merged_status = dict(old_status)
        for stk, stv in new_status.items():
            if stk in ("capacity", "allocatable"):
                if isinstance(stv, dict) and stv:
                    merged_status[stk] = dict(stv)
            elif stv is not None:
                merged_status[stk] = stv
        merged["status"] = merged_status

    return merged


def _parse_infra_events(
    infra_events: list[dict[str, Any]],
    *,
    t0_epoch: float | None = None,
    initial_window_sec: float = 1.0,
) -> dict[str, Any]:
    nodes_by_name: dict[str, dict[str, Any]] = {}
    post_t0_node_events: list[tuple[str, str, float | None]] = []
    topologies_by_name: dict[str, dict[str, Any]] = {}
    flavors_by_name: dict[str, dict[str, Any]] = {}
    wpc_by_name: dict[str, dict[str, Any]] = {}
    cohorts_by_name: dict[str, dict[str, Any]] = {}
    cqs_by_name: dict[str, dict[str, Any]] = {}
    lqs_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    namespaces_by_name: dict[str, dict[str, Any]] = {}
    kueue_configuration: dict[str, Any] | None = None
    pod_new_events: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
    pod_delete_epochs: dict[tuple[str, str], float] = {}
    pod_incarnations_mut: list[list[Any]] = []
    active_pod_inc_by_key: dict[tuple[str, str], list[Any]] = {}
    infra_timestamps: list[float] = []

    for ev in infra_events:
        ep = parse_rfc3339_epoch(ev.get("timestamp"))
        if ep is not None:
            infra_timestamps.append(ep)
        in_initial_window = (
            t0_epoch is None
            or ep is None
            or ep <= (t0_epoch + initial_window_sec + 1e-6)
        )
        ev_type = str(ev.get("eventType") or "")
        pl = ev.get("payload") or {}
        if not isinstance(pl, dict):
            continue
        kind = str(pl.get("kind") or pl.get("objectKind") or "")
        meta = pl.get("metadata") or {}
        name = str(meta.get("name") or pl.get("name") or "")
        ns = str(meta.get("namespace") or pl.get("namespace") or "default")

        if kind == "Node":
            if in_initial_window:
                if ev_type == "Deleted":
                    if name:
                        nodes_by_name.pop(name, None)
                elif ev_type == "Update" and name and name in nodes_by_name:
                    nodes_by_name[name] = _merge_node_update(nodes_by_name[name], pl)
                elif ev_type in ("New", "Update", "") and name:
                    nodes_by_name[name] = pl
            elif name:
                post_t0_node_events.append((ev_type or "Update", name, ep))
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
        elif kind == "Configuration":
            if ev_type == "Deleted":
                kueue_configuration = None
            elif ev_type in ("New", "Update", ""):
                kueue_configuration = pl
        elif kind == "Pod" and name:
            p_key = (ns, name)
            sched_ts = parse_rfc3339_epoch(pl.get("schedulingTime"))
            ev_ep_val = ep if ep is not None else 0.0
            if ev_type == "New":
                if p_key not in pod_new_events:
                    pod_new_events[p_key] = (ev_ep_val, pl)
                cur_pod_inc = active_pod_inc_by_key.get(p_key)
                if cur_pod_inc is not None:
                    prev_sched_ts = cur_pod_inc[5]
                    if prev_sched_ts is not None and sched_ts is not None and sched_ts > prev_sched_ts + 0.5:
                        cur_pod_inc[4] = ev_ep_val
                        cur_pod_inc = None
                if cur_pod_inc is None:
                    new_pod_inc: list[Any] = [ns, name, ev_ep_val, pl, None, sched_ts]
                    pod_incarnations_mut.append(new_pod_inc)
                    active_pod_inc_by_key[p_key] = new_pod_inc
            elif ev_type == "Deleted":
                del_ep = parse_rfc3339_epoch(pl.get("deletionTime")) or ep
                if del_ep is not None:
                    if p_key not in pod_delete_epochs:
                        pod_delete_epochs[p_key] = del_ep
                    cur_pod_inc = active_pod_inc_by_key.pop(p_key, None)
                    if cur_pod_inc is not None:
                        cur_pod_inc[4] = del_ep

    pod_incarnations: list[tuple[str, str, float, dict[str, Any], float | None]] = [
        (inc[0], inc[1], inc[2], inc[3], inc[4]) for inc in pod_incarnations_mut
    ]
    return {
        "nodes_by_name": nodes_by_name,
        "post_t0_node_events": post_t0_node_events,
        "topologies_by_name": topologies_by_name,
        "flavors_by_name": flavors_by_name,
        "wpc_by_name": wpc_by_name,
        "cohorts_by_name": cohorts_by_name,
        "cqs_by_name": cqs_by_name,
        "lqs_by_key": lqs_by_key,
        "namespaces_by_name": namespaces_by_name,
        "kueue_configuration": kueue_configuration,
        "pod_new_events": pod_new_events,
        "pod_delete_epochs": pod_delete_epochs,
        "pod_incarnations": pod_incarnations,
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


def _dict_has_kueue_domain(d: Any) -> bool:
    if not isinstance(d, dict):
        return False
    return any("kueue.x-k8s.io/" in str(k) for k in d)


def _has_kueue_queue_or_domain(raw_obj: dict[str, Any] | None, lq_name: str) -> bool:
    if lq_name and lq_name != "<none>":
        return True
    if not isinstance(raw_obj, dict):
        return False
    meta = raw_obj.get("metadata") or {}
    if _dict_has_kueue_domain(meta.get("labels")) or _dict_has_kueue_domain(meta.get("annotations")):
        return True
    spec = raw_obj.get("spec") or {}
    if not isinstance(spec, dict):
        return False
    tpl_meta = (spec.get("template") or {}).get("metadata") or {}
    if _dict_has_kueue_domain(tpl_meta.get("labels")) or _dict_has_kueue_domain(tpl_meta.get("annotations")):
        return True
    for rj in spec.get("replicatedJobs") or []:
        if isinstance(rj, dict):
            rj_p_meta = (((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get("metadata") or {}
            if _dict_has_kueue_domain(rj_p_meta.get("labels")) or _dict_has_kueue_domain(rj_p_meta.get("annotations")):
                return True
    lwt = spec.get("leaderWorkerTemplate")
    if isinstance(lwt, dict):
        for tk in ("leaderTemplate", "workerTemplate"):
            tm = (lwt.get(tk) or {}).get("metadata") or {}
            if _dict_has_kueue_domain(tm.get("labels")) or _dict_has_kueue_domain(tm.get("annotations")):
                return True
    for r_root in (spec, spec.get("rayClusterSpec") or {}):
        if isinstance(r_root, dict):
            hm = ((r_root.get("headGroupSpec") or {}).get("template") or {}).get("metadata") or {}
            if _dict_has_kueue_domain(hm.get("labels")) or _dict_has_kueue_domain(hm.get("annotations")):
                return True
            for wg in r_root.get("workerGroupSpecs") or []:
                if isinstance(wg, dict):
                    wm = ((wg.get("template") or {}).get("metadata")) or {}
                    if _dict_has_kueue_domain(wm.get("labels")) or _dict_has_kueue_domain(wm.get("annotations")):
                        return True
    return False


def _merge_podset_shapes(
    existing_shapes: list[tuple[str, int, dict[str, float]]],
    new_shapes: list[tuple[str, int, dict[str, float]]],
) -> list[tuple[str, int, dict[str, float]]]:
    by_key: dict[tuple[str, tuple[tuple[str, float], ...]], int] = {}
    out: list[tuple[str, int, dict[str, float]]] = []
    for ps_name, cnt, reqs in list(existing_shapes) + list(new_shapes):
        if cnt <= 0:
            continue
        gk = (ps_name, _canonical_profile_from_reqs(reqs))
        if gk in by_key:
            idx = by_key[gk]
            prev_ps, prev_cnt, prev_reqs = out[idx]
            out[idx] = (prev_ps, prev_cnt + cnt, prev_reqs)
        else:
            by_key[gk] = len(out)
            out.append((ps_name, cnt, dict(reqs)))
    return out


def _parse_workflows(
    workflow_events: list[dict[str, Any]],
    wpc_by_name: dict[str, dict[str, Any]],
    exec_index: ExecutionTimeIndex,
    t0_epoch: float | None,
    *,
    kueue_configuration: dict[str, Any] | None = None,
) -> tuple[dict[str, int], list[ParsedWorkload], list[ParsedWorkload], dict[str, float]]:
    priority_value_by_name: dict[str, int] = {}
    for wpc_name, wpc_obj in wpc_by_name.items():
        try:
            priority_value_by_name[wpc_name] = int(wpc_obj.get("value") or 0)
        except (ValueError, TypeError):
            priority_value_by_name[wpc_name] = 0

    manage_jobs_without_queue_name = bool(
        isinstance(kueue_configuration, dict) and kueue_configuration.get("manageJobsWithoutQueueName") is True
    )
    known_parents = collect_known_parent_workflows(workflow_events)
    earliest_admit_by_ns = collect_not_admitted_workflow_admit_epochs(workflow_events, known_parents)

    all_incarnations: list[ParsedWorkload] = []
    active_top_workflows: dict[tuple[str, str, str], float | None] = {}
    active_parsed_by_key: dict[tuple[str, str, str], ParsedWorkload] = {}
    incarnation_counts: Counter[tuple[str, str, str]] = Counter()
    podgroups_by_key: dict[tuple[str, str], ParsedWorkload] = {}
    active_podgroup_members: dict[tuple[str, str], set[str]] = defaultdict(set)
    pod_to_pg_key: dict[tuple[str, str], tuple[str, str]] = {}

    for ev in workflow_events:
        ev_type = str(ev.get("eventType") or "")
        if ev_type not in ("New", "Deleted", ""):
            continue
        wl_state = str(ev.get("workloadState") or "NotAdmitted")
        pl = ev.get("payload") or {}
        obj = pl.get("object") or {}
        if not isinstance(obj, dict):
            obj = {}
        kind = str(pl.get("objectKind") or obj.get("kind") or "JobSet")
        meta = obj.get("metadata") or {}
        wf_name = str(pl.get("name") or meta.get("name") or "")
        wf_ns = str(pl.get("namespace") or meta.get("namespace") or "default")
        if not wf_name:
            continue

        is_child, parent_js = is_child_workflow_owned_by_tracked_parent(obj, kind, wf_ns, known_parents)
        if is_child:
            continue

        labels = dict(meta.get("labels") or {})
        annotations = dict(meta.get("annotations") or {})
        pg_name = (
            str(
                annotations.get("kueue.x-k8s.io/pod-group-name")
                or labels.get("kueue.x-k8s.io/pod-group-name")
                or ""
            ).strip()
            if kind == "Pod"
            else ""
        )

        wf_key = (wf_ns, kind, wf_name)
        if ev_type == "Deleted":
            del_ep = parse_rfc3339_epoch(ev.get("timestamp"))
            active_top_workflows.pop(wf_key, None)
            pg_key_del: tuple[str, str] | None = (
                (wf_ns, pg_name)
                if pg_name
                else (pod_to_pg_key.pop((wf_ns, wf_name), None) if kind == "Pod" else None)
            )
            if pg_key_del is not None:
                pod_to_pg_key.pop((wf_ns, wf_name), None)
                if pg_key_del in active_podgroup_members:
                    active_podgroup_members[pg_key_del].discard(wf_name)
                    if not active_podgroup_members[pg_key_del]:
                        active_podgroup_members.pop(pg_key_del, None)
                        del_pg = podgroups_by_key.pop(pg_key_del, None)
                        if del_pg is not None and del_ep is not None:
                            del_pg.deleted_epoch = del_ep
            else:
                del_wf = active_parsed_by_key.pop(wf_key, None)
                if del_wf is not None and del_ep is not None:
                    del_wf.deleted_epoch = del_ep
            continue

        if not obj:
            continue

        ev_creation_ep = parse_rfc3339_epoch(pl.get("creationTime")) or parse_rfc3339_epoch(
            meta.get("creationTimestamp")
        )
        arr_ep = (
            parse_rfc3339_epoch(ev.get("timestamp"))
            or ev_creation_ep
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

        podset_shapes = extract_workflow_podset_shapes(obj, kind)
        total_pods, total_reqs, per_pod_reqs = extract_workflow_pods_and_requests(obj, kind)
        desired_replicas: int | None = None
        if kind in ("Deployment", "StatefulSet", "ReplicaSet"):
            raw_rep = (obj.get("spec") or {}).get("replicas")
            try:
                desired_replicas = max(0, int(raw_rep)) if raw_rep is not None else 1
            except (TypeError, ValueError):
                desired_replicas = 1
            if not per_pod_reqs:
                per_pod_reqs = extract_pod_spec_resources(
                    ((obj.get("spec") or {}).get("template") or {}).get("spec")
                )
        ready_replicas: int | None = None
        if kind == "Deployment" and pl.get("readyReplicas") is not None:
            try:
                ready_replicas = max(0, int(pl["readyReplicas"]))
            except (TypeError, ValueError):
                ready_replicas = None

        ann_dur = parse_positive_duration_annotation(
            annotations,
            "kueue-optimizer.gke.io/pod-execution-time-seconds",
            "kueue-optimizer.gke.io/real-duration-sec",
        )
        wf_true_dur = parse_positive_duration_annotation(
            annotations,
            "kueue-optimizer.gke.io/true-execution-time-seconds",
            "kueue-optimizer.gke.io/expected-execution-time-seconds",
        )
        wf_mult: float | None = None
        if annotations.get("kueue-optimizer.gke.io/missing-duration-multiplier") is not None:
            try:
                cand_m = float(str(annotations["kueue-optimizer.gke.io/missing-duration-multiplier"]).strip())
                if cand_m > 1.0:
                    wf_mult = cand_m
            except (TypeError, ValueError):
                pass
        fallback_dur = ann_dur if ann_dur is not None else 10.0

        if pg_name:
            pg_key = (wf_ns, pg_name)
            pg_inc_key = (wf_ns, "PodGroup", pg_name)
            existing_pg = podgroups_by_key.get(pg_key)
            if existing_pg is not None:
                ref_pg_creation = (
                    existing_pg.creation_epoch
                    if existing_pg.creation_epoch is not None
                    else active_top_workflows.get(wf_key)
                )
                if (
                    ref_pg_creation is not None
                    and ev_creation_ep is not None
                    and ev_creation_ep > ref_pg_creation + 0.5
                ):
                    old_members = active_podgroup_members.pop(pg_key, set())
                    for old_m in old_members:
                        active_top_workflows.pop((wf_ns, "Pod", old_m), None)
                        pod_to_pg_key.pop((wf_ns, old_m), None)
                    podgroups_by_key.pop(pg_key, None)
                    existing_pg = None

            if existing_pg is not None:
                if wf_name in active_podgroup_members[pg_key]:
                    continue
                active_podgroup_members[pg_key].add(wf_name)
                pod_to_pg_key[(wf_ns, wf_name)] = pg_key
                active_top_workflows[wf_key] = ev_creation_ep
                if existing_pg.creation_epoch is None and ev_creation_ep is not None:
                    existing_pg.creation_epoch = ev_creation_ep
                existing_pg.total_pods += total_pods
                for rk, rv in total_reqs.items():
                    existing_pg.total_requests[rk] = existing_pg.total_requests.get(rk, 0.0) + rv
                existing_pg.podset_shapes = _merge_podset_shapes(existing_pg.podset_shapes, podset_shapes)
                if arr_ep < existing_pg.arrival_epoch:
                    existing_pg.arrival_epoch = arr_ep
                if existing_pg.local_queue == "<none>" and lq_name != "<none>":
                    existing_pg.local_queue = lq_name
                continue

            inc_idx = incarnation_counts[pg_inc_key]
            incarnation_counts[pg_inc_key] += 1
            active_podgroup_members[pg_key].add(wf_name)
            pod_to_pg_key[(wf_ns, wf_name)] = pg_key
            active_top_workflows[wf_key] = ev_creation_ep

            pg_tot_raw = str(
                annotations.get("kueue.x-k8s.io/pod-group-total-count")
                or labels.get("kueue.x-k8s.io/pod-group-total-count")
                or ""
            ).strip()
            try:
                pg_tot_cnt: int | None = int(pg_tot_raw) if pg_tot_raw else None
            except ValueError:
                pg_tot_cnt = None

            pg_exec_info = exec_index.lookup(
                wf_ns,
                "PodGroup",
                pg_name,
                creation_epoch=ev_creation_ep,
                incarnation_index=inc_idx,
            )
            dur_sec, found_in_idx = exec_index.resolve_duration_sec(
                namespace=wf_ns,
                object_kind="PodGroup",
                name=pg_name,
                fallback_sec=fallback_dur,
                workflow_true_duration_sec=wf_true_dur,
                workflow_missing_multiplier=wf_mult,
                creation_epoch=ev_creation_ep,
                incarnation_index=inc_idx,
            )
            found_dur = found_in_idx or (ann_dur is not None)
            parsed_pg = ParsedWorkload(
                name=pg_name,
                namespace=wf_ns,
                kind="PodGroup",
                workload_state=wl_state,
                arrival_epoch=arr_ep,
                local_queue=lq_name,
                priority_class=pc_name,
                priority_value=pc_val,
                total_pods=total_pods,
                total_requests=dict(total_reqs),
                per_pod_requests=dict(per_pod_reqs),
                duration_sec=dur_sec,
                found_duration=found_dur,
                annotations=annotations,
                labels=labels,
                incarnation_index=inc_idx,
                podset_shapes=list(podset_shapes),
                creation_epoch=ev_creation_ep,
                exec_info=pg_exec_info,
                has_kueue_signal=True,
                pg_total_count=pg_tot_cnt,
            )
            podgroups_by_key[pg_key] = parsed_pg
            all_incarnations.append(parsed_pg)
            continue

        if wf_key in active_top_workflows:
            prev_creation_ep = active_top_workflows[wf_key]
            if (
                prev_creation_ep is not None
                and ev_creation_ep is not None
                and ev_creation_ep > prev_creation_ep + 0.5
            ):
                active_top_workflows.pop(wf_key, None)
                active_parsed_by_key.pop(wf_key, None)
            else:
                continue

        inc_idx = incarnation_counts[wf_key]
        incarnation_counts[wf_key] += 1
        active_top_workflows[wf_key] = ev_creation_ep

        wf_exec_info = exec_index.lookup(
            wf_ns,
            kind,
            wf_name,
            parent_jobset_name=parent_js,
            creation_epoch=ev_creation_ep,
            incarnation_index=inc_idx,
        )
        dur_sec, found_in_idx = exec_index.resolve_duration_sec(
            namespace=wf_ns,
            object_kind=kind,
            name=wf_name,
            fallback_sec=fallback_dur,
            parent_jobset_name=parent_js,
            workflow_true_duration_sec=wf_true_dur,
            workflow_missing_multiplier=wf_mult,
            creation_epoch=ev_creation_ep,
            incarnation_index=inc_idx,
        )
        found_dur = found_in_idx or (ann_dur is not None)
        has_sig = _has_kueue_queue_or_domain(obj, "" if lq_name == "<none>" else lq_name)

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
            ready_replicas=ready_replicas,
            desired_replicas=desired_replicas,
            incarnation_index=inc_idx,
            podset_shapes=list(podset_shapes),
            creation_epoch=ev_creation_ep,
            exec_info=wf_exec_info,
            has_kueue_signal=has_sig,
        )
        active_parsed_by_key[wf_key] = parsed_wf
        all_incarnations.append(parsed_wf)

    admitted_at_start_wfs: list[ParsedWorkload] = []
    unadmitted_wfs: list[ParsedWorkload] = []
    for wf in all_incarnations:
        if t0_epoch is not None and wf.deleted_epoch is not None and wf.deleted_epoch <= t0_epoch + 1e-6:
            continue
        if wf.workload_state == "NotAdmitted":
            unadmitted_wfs.append(wf)
            continue
        created_after_t0 = (
            t0_epoch is not None
            and wf.creation_epoch is not None
            and wf.creation_epoch > t0_epoch + 1e-6
        )
        admitted_by_t0 = t0_epoch is None or (
            not created_after_t0
            and (
                wf.arrival_epoch <= t0_epoch + 1.0 + 1e-6
                or bool(
                    wf.incarnation_index == 0
                    and wf.exec_info is not None
                    and wf.exec_info.admitted_epochs
                    and min(wf.exec_info.admitted_epochs) <= t0_epoch + 1.0 + 1e-6
                )
            )
        )
        if admitted_by_t0:
            admitted_at_start_wfs.append(wf)
        elif wf.kind != "PodGroup" and (
            wf.has_kueue_signal
            or wf.exec_info is not None
            or (manage_jobs_without_queue_name and wf.namespace not in SYSTEM_NAMESPACES)
        ):
            if t0_epoch is not None and wf.creation_epoch is not None and wf.creation_epoch >= t0_epoch:
                wf.arrival_epoch = wf.creation_epoch
            wf.workload_state = "NotAdmitted"
            unadmitted_wfs.append(wf)

    unadmitted_wfs.sort(key=lambda w: (w.arrival_epoch, w.name, w.incarnation_index))
    return priority_value_by_name, admitted_at_start_wfs, unadmitted_wfs, earliest_admit_by_ns


def _canonical_profile_from_reqs(reqs: dict[str, float]) -> tuple[tuple[str, float], ...]:
    items: list[tuple[str, float]] = []
    for rk, rv in sorted(reqs.items()):
        if rk in ("pods", "ephemeral-storage"):
            continue
        val = round(float(rv), 6)
        if val > 0.0:
            items.append((str(rk), val))
    return tuple(items)


def _canonical_accel_profile_from_reqs(reqs: dict[str, float]) -> tuple[tuple[str, float], ...]:
    items: list[tuple[str, float]] = []
    for rk, rv in sorted(reqs.items()):
        if rk in ("cpu", "memory", "pods", "ephemeral-storage") or str(rk).startswith("hugepages-"):
            continue
        val = round(float(rv), 6)
        if val > 0.0:
            items.append((str(rk), val))
    return tuple(items)


def _format_profile_value(rk: str, val: float) -> str:
    if rk == "memory":
        if abs(val - round(val)) < 1e-6:
            return f"{int(round(val))}Gi"
        return f"{int(round(val * 1024))}Mi"
    if abs(val - round(val)) < 1e-6:
        return str(int(round(val)))
    return f"{val:g}"


def _effective_t0_podset_shapes(wf: ParsedWorkload) -> list[tuple[str, int, dict[str, float]]]:
    shapes = list(wf.podset_shapes)
    if not shapes and wf.kind != "Deployment" and wf.total_pods > 0:
        shapes = [("main", wf.total_pods, dict(wf.per_pod_requests))]
    if (
        wf.kind == "PodGroup"
        and len(shapes) == 1
        and wf.pg_total_count is not None
        and wf.pg_total_count > shapes[0][1]
    ):
        shapes = [(shapes[0][0], wf.pg_total_count, shapes[0][2])]
    return shapes


def _max_match_pods(wf: ParsedWorkload) -> int:
    if wf.kind == "Deployment":
        if wf.desired_replicas is not None:
            return wf.desired_replicas
        return wf.total_pods
    shapes = _effective_t0_podset_shapes(wf)
    return sum(cnt for _, cnt, _ in shapes) if shapes else wf.total_pods


def _resolve_t0_lq_cq(
    wf: ParsedWorkload,
    lqs_by_key: dict[tuple[str, str], dict[str, Any]],
    cqs_by_name: dict[str, dict[str, Any]],
    *,
    manage_jobs_without_queue_name: bool = False,
) -> tuple[str, str]:
    """Read-only parity mirror of runner/loader.py's `_resolve_cq_and_lq_for_candidate`."""
    ns = wf.namespace
    lq_name = (
        (wf.local_queue if wf.local_queue and wf.local_queue != "<none>" else "")
        or (wf.exec_info.local_queue_name if wf.exec_info else "")
        or str(wf.labels.get("kueue.x-k8s.io/queue-name") or wf.annotations.get("kueue.x-k8s.io/queue-name") or "")
    )
    cq_name = wf.exec_info.cluster_queue_name if wf.exec_info else ""

    has_signal = bool(lq_name or cq_name or wf.exec_info is not None or wf.has_kueue_signal)
    if not has_signal:
        if ns in SYSTEM_NAMESPACES or not manage_jobs_without_queue_name:
            return "", ""

    if lq_name and not cq_name:
        lq_doc = lqs_by_key.get((ns, lq_name))
        if lq_doc:
            cq_name = str((lq_doc.get("spec") or {}).get("clusterQueue") or "")

    if not lq_name and cq_name:
        for (k_ns, k_lq), lq_doc in sorted(lqs_by_key.items()):
            if k_ns == ns and str((lq_doc.get("spec") or {}).get("clusterQueue") or "") == cq_name:
                lq_name = k_lq
                break

    if not lq_name:
        ns_lqs = [(k_lq, doc) for (k_ns, k_lq), doc in sorted(lqs_by_key.items()) if k_ns == ns]
        if len(ns_lqs) == 1:
            lq_name, lq_doc = ns_lqs[0]
            if not cq_name:
                cq_name = str((lq_doc.get("spec") or {}).get("clusterQueue") or "")

    if not cq_name and len(cqs_by_name) == 1:
        cq_name = next(iter(cqs_by_name.keys()))

    if cq_name and cq_name not in cqs_by_name:
        return "", ""

    if cq_name and cq_name in cqs_by_name and not lq_name:
        lq_name = f"{cq_name}-init-lq"

    if not lq_name or not cq_name or cq_name not in cqs_by_name:
        return "", ""
    return lq_name, cq_name


def _resolve_synthesized_t0_end_epoch(
    wf: ParsedWorkload,
    *,
    t0_epoch: float,
    effective_start_ep: float,
) -> float | None:
    """Resolve uninterrupted completion epoch for synthesized t0 pods, mirroring loader `_resolve_t0_pod_end_epoch`."""
    if wf.kind == "Deployment":
        return None
    exec_info = wf.exec_info
    if exec_info is not None:
        had_interruption = bool(exec_info.preempted_epochs or exec_info.interrupted_epochs)
        if exec_info.full_duration_sec is not None and exec_info.full_duration_sec > 0 and had_interruption:
            adm_ep = min(exec_info.admitted_epochs) if exec_info.admitted_epochs else t0_epoch
            end_from_full = adm_ep + exec_info.full_duration_sec
            if end_from_full <= effective_start_ep:
                end_from_full = effective_start_ep + exec_info.full_duration_sec
            return end_from_full
        if exec_info.full_end_epoch is not None and exec_info.full_end_epoch > effective_start_ep:
            return exec_info.full_end_epoch
        if exec_info.full_duration_sec is not None and exec_info.full_duration_sec > 0:
            adm_ep = min(exec_info.admitted_epochs) if exec_info.admitted_epochs else t0_epoch
            return adm_ep + exec_info.full_duration_sec
        if exec_info.is_interrupted_only:
            adm_ep = min(exec_info.admitted_epochs) if exec_info.admitted_epochs else t0_epoch
            return adm_ep + wf.duration_sec
    if wf.found_duration and wf.duration_sec > 0.0:
        return wf.arrival_epoch + wf.duration_sec
    return None


def _summarize_initial_pods(
    pod_new_events: dict[tuple[str, str], tuple[float, dict[str, Any]]],
    pod_delete_epochs: dict[tuple[str, str], float],
    pod_incarnations: list[tuple[str, str, float, dict[str, Any], float | None]],
    t0_epoch: float | None,
    t_first_unadmitted: float | None,
    admitted_at_start_wfs: list[ParsedWorkload],
    unadmitted_wfs: list[ParsedWorkload],
    earliest_admit_by_ns: dict[str, float],
    *,
    lqs_by_key: dict[tuple[str, str], dict[str, Any]] | None = None,
    cqs_by_name: dict[str, dict[str, Any]] | None = None,
    kueue_configuration: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective_start_ep = (
        t0_epoch
        if t0_epoch is not None
        else (t_first_unadmitted if t_first_unadmitted is not None else 0.0)
    )
    manage_without_queue = bool(
        isinstance(kueue_configuration, dict) and kueue_configuration.get("manageJobsWithoutQueueName") is True
    )
    lqs_map = lqs_by_key or {}
    cqs_map = cqs_by_name or {}

    def _is_active_t0_wf(wf: ParsedWorkload) -> bool:
        if wf.deleted_epoch is not None and wf.deleted_epoch <= effective_start_ep + 1e-6:
            return False
        if wf.exec_info is not None:
            if wf.exec_info.full_end_epoch is not None and wf.exec_info.full_end_epoch <= effective_start_ep + 1e-6:
                return False
            if (
                wf.exec_info.terminal_end_epoch is not None
                and wf.exec_info.terminal_end_epoch <= effective_start_ep + 1e-6
                and not (
                    wf.exec_info.admitted_epochs
                    and max(wf.exec_info.admitted_epochs) > wf.exec_info.terminal_end_epoch + 1e-6
                )
            ):
                return False
        return True

    active_t0_indices = [i for i, wf in enumerate(admitted_at_start_wfs) if _is_active_t0_wf(wf)]

    admitted_at_start_owners: set[tuple[str, str]] = set()
    for w_idx in active_t0_indices:
        w = admitted_at_start_wfs[w_idx]
        admitted_at_start_owners.add((w.namespace, w.name))
        if w.exec_info is not None and w.exec_info.workload_name:
            admitted_at_start_owners.add((w.namespace, w.exec_info.workload_name))
    not_admitted_wf_names = [
        (w.namespace, w.name)
        for w in unadmitted_wfs
        if (w.namespace, w.name) not in admitted_at_start_owners
    ]
    qualifying_initial_pods, _expired_init_pods = filter_initial_running_pods(
        pod_new_events,
        pod_delete_epochs,
        t0_epoch=effective_start_ep,
        effective_start_epoch=effective_start_ep,
        initial_window_sec=1.0,
        not_admitted_workflow_names=not_admitted_wf_names,
        earliest_admit_by_ns=earliest_admit_by_ns,
        pod_incarnations=pod_incarnations or None,
    )

    init_pod_requests_sum: dict[str, float] = defaultdict(float)
    init_pod_lifetimes: list[float] = []
    init_pod_infinite_count = 0
    init_nodes_used: Counter[str] = Counter()
    init_ns_counts: Counter[str] = Counter()
    init_pod_profiles: Counter[tuple[tuple[str, str], ...]] = Counter()
    pod_parsed_reqs: list[dict[str, float]] = []

    for p_ns, _p_name, _p_ep, pl, del_ep in qualifying_initial_pods:
        init_ns_counts[p_ns] += 1
        placement = str(pl.get("placement") or ((pl.get("spec") or {}).get("nodeName")) or "")
        if placement:
            init_nodes_used[placement] += 1

        raw_req = pl.get("resourceRequest")
        parsed_single: dict[str, float] = {}
        if isinstance(raw_req, dict) and raw_req:
            prof = tuple(sorted((str(k), str(v)) for k, v in raw_req.items()))
            init_pod_profiles[prof] += 1
            for rk, rv in raw_req.items():
                q_val = parse_k8s_quantity(rv, rk)
                parsed_single[str(rk)] = q_val
                init_pod_requests_sum[rk] += q_val
        else:
            p_reqs = extract_pod_spec_resources(pl.get("spec") if isinstance(pl, dict) else None)
            prof = tuple(sorted((str(k), f"{v:g}") for k, v in p_reqs.items()))
            init_pod_profiles[prof] += 1
            for rk, rv in p_reqs.items():
                parsed_single[str(rk)] = rv
                init_pod_requests_sum[rk] += rv
        pod_parsed_reqs.append(parsed_single)
        init_pod_requests_sum["pods"] += 1.0

        if del_ep is not None:
            init_pod_lifetimes.append(max(0.0, del_ep - effective_start_ep))
        else:
            init_pod_infinite_count += 1

    # Align with loader Tier-1..4 matching and synthesis for pre-admitted t0 workloads.
    has_enriched_pod_linkage = any(
        bool(pl.get("workloadName") or pl.get("ownerName"))
        for _p_ns, _p_name, _p_ep, pl, _del_ep in qualifying_initial_pods
    )
    matched_pods_by_wf: dict[int, list[tuple[dict[str, Any], dict[str, float]]]] = {
        i: [] for i in range(len(admitted_at_start_wfs))
    }
    claimed_pod_indices: set[int] = set()
    wf_lq_cq: dict[int, tuple[str, str]] = {
        w_idx: _resolve_t0_lq_cq(
            admitted_at_start_wfs[w_idx],
            lqs_map,
            cqs_map,
            manage_jobs_without_queue_name=manage_without_queue,
        )
        for w_idx in active_t0_indices
    }

    wfs_by_wl: dict[tuple[str, str], list[int]] = defaultdict(list)
    wfs_by_owner_kind: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    wfs_by_owner: dict[tuple[str, str], list[int]] = defaultdict(list)
    for w_idx in active_t0_indices:
        wf = admitted_at_start_wfs[w_idx]
        if wf.exec_info is not None and wf.exec_info.workload_name:
            wfs_by_wl[(wf.namespace, wf.exec_info.workload_name)].append(w_idx)
        if wf.kind and wf.name:
            wfs_by_owner_kind[(wf.namespace, wf.kind, wf.name)].append(w_idx)
        if wf.name:
            wfs_by_owner[(wf.namespace, wf.name)].append(w_idx)

    def _pick_target_wf(candidates: list[int]) -> int:
        for cand_i in candidates:
            if len(matched_pods_by_wf[cand_i]) < _max_match_pods(admitted_at_start_wfs[cand_i]):
                return cand_i
        return candidates[0]

    # Pass 1 (Tier 1): Exact enriched scraper linkage (`workloadName` / `ownerName`).
    for p_idx, (p_ns, _p_name, _p_ep, pl, _del_ep) in enumerate(qualifying_initial_pods):
        p_wl = str(pl.get("workloadName") or "")
        p_okind = str(pl.get("ownerKind") or "")
        p_owner = str(pl.get("ownerName") or "")
        if not p_wl and not p_owner:
            continue
        matched_idx: int | None = None
        if p_wl and (p_ns, p_wl) in wfs_by_wl:
            matched_idx = _pick_target_wf(wfs_by_wl[(p_ns, p_wl)])
        elif p_okind and p_owner and (p_ns, p_okind, p_owner) in wfs_by_owner_kind:
            matched_idx = _pick_target_wf(wfs_by_owner_kind[(p_ns, p_okind, p_owner)])
        elif p_owner and (p_ns, p_owner) in wfs_by_owner:
            matched_idx = _pick_target_wf(wfs_by_owner[(p_ns, p_owner)])
        elif p_wl and (p_ns, p_wl) in wfs_by_owner:
            matched_idx = _pick_target_wf(wfs_by_owner[(p_ns, p_wl)])
        if matched_idx is not None:
            matched_pods_by_wf[matched_idx].append((pl, pod_parsed_reqs[p_idx]))
            claimed_pod_indices.add(p_idx)

    # Pass 2 (Tiers 2 & 3): Legacy fallbacks strictly when trace lacks enriched pod-workload linkage.
    if not has_enriched_pod_linkage and active_t0_indices and qualifying_initial_pods:
        # Tier 2: Plain-name prefix matching (`<wf_name>-*` or `== <wf_name>`).
        for w_idx in active_t0_indices:
            if matched_pods_by_wf[w_idx]:
                continue
            wf = admitted_at_start_wfs[w_idx]
            if not wf.name or (wf.name.startswith("h-") and len(wf.name) == 34):
                continue
            max_pods = _max_match_pods(wf)
            if max_pods <= 0:
                continue
            prefix = f"{wf.name}-"
            for p_idx, (p_ns, p_name, _p_ep, pl, _del_ep) in enumerate(qualifying_initial_pods):
                if p_idx in claimed_pod_indices:
                    continue
                if p_ns == wf.namespace and (p_name == wf.name or p_name.startswith(prefix)):
                    matched_pods_by_wf[w_idx].append((pl, pod_parsed_reqs[p_idx]))
                    claimed_pod_indices.add(p_idx)
                    if len(matched_pods_by_wf[w_idx]) >= max_pods:
                        break

        # Tier 3: Per-PodSet resource-profile matching (with accelerator fallback).
        for w_idx in active_t0_indices:
            if matched_pods_by_wf[w_idx]:
                continue
            lq_chk, cq_chk = wf_lq_cq[w_idx]
            if not lq_chk or not cq_chk:
                continue
            wf = admitted_at_start_wfs[w_idx]
            ps_groups = _effective_t0_podset_shapes(wf)
            if not ps_groups:
                continue
            tentative_indices: list[int] = []
            all_groups_matched = True
            for _ps_name, want_cnt, want_reqs in ps_groups:
                want_full = _canonical_profile_from_reqs(want_reqs)
                want_accel = _canonical_accel_profile_from_reqs(want_reqs)
                chosen: list[int] = []
                if want_full:
                    exact_matches = [
                        p_idx
                        for p_idx in range(len(qualifying_initial_pods))
                        if p_idx not in claimed_pod_indices
                        and p_idx not in tentative_indices
                        and qualifying_initial_pods[p_idx][0] == wf.namespace
                        and _canonical_profile_from_reqs(pod_parsed_reqs[p_idx]) == want_full
                    ]
                    chosen = exact_matches[:want_cnt]
                if len(chosen) < want_cnt and want_accel:
                    accel_matches = [
                        p_idx
                        for p_idx in range(len(qualifying_initial_pods))
                        if p_idx not in claimed_pod_indices
                        and p_idx not in tentative_indices
                        and p_idx not in chosen
                        and qualifying_initial_pods[p_idx][0] == wf.namespace
                        and _canonical_accel_profile_from_reqs(pod_parsed_reqs[p_idx]) == want_accel
                    ]
                    chosen.extend(accel_matches[: want_cnt - len(chosen)])
                if not chosen:
                    all_groups_matched = False
                    break
                tentative_indices.extend(chosen)
            if all_groups_matched and tentative_indices:
                for p_idx in tentative_indices:
                    matched_pods_by_wf[w_idx].append(
                        (qualifying_initial_pods[p_idx][3], pod_parsed_reqs[p_idx])
                    )
                    claimed_pod_indices.add(p_idx)

    # Pass 3 (Tier 4): Account for missing pods synthesized by the loader for pre-admitted t0 workloads.
    synthesized_initial_pods = 0

    def _record_synthesized_pods(
        wf_ns: str,
        count: int,
        reqs: dict[str, float],
        end_ep: float | None,
    ) -> None:
        nonlocal synthesized_initial_pods, init_pod_infinite_count
        if count <= 0:
            return
        synthesized_initial_pods += count
        init_ns_counts[wf_ns] += count
        init_pod_requests_sum["pods"] += float(count)
        for rk, rv in reqs.items():
            if rk != "pods":
                init_pod_requests_sum[rk] += rv * count
        synth_prof = tuple(
            sorted(
                (str(rk), _format_profile_value(str(rk), rv))
                for rk, rv in reqs.items()
                if rk != "pods" and rv > 0.0
            )
        )
        init_pod_profiles[synth_prof] += count
        if end_ep is not None:
            rem_life = max(0.0, end_ep - effective_start_ep)
            init_pod_lifetimes.extend([rem_life] * count)
        else:
            init_pod_infinite_count += count

    for w_idx in active_t0_indices:
        wf = admitted_at_start_wfs[w_idx]
        lq_name, cq_name = wf_lq_cq[w_idx]
        if not lq_name or not cq_name:
            continue
        matched_pods = matched_pods_by_wf[w_idx]

        if wf.kind == "Deployment":
            desired = wf.desired_replicas if wf.desired_replicas is not None else wf.total_pods
            matched_cnt = len(matched_pods)
            if wf.ready_replicas is not None:
                k_admitted = min(desired, max(0, wf.ready_replicas))
                if k_admitted == 0 and matched_cnt > 0:
                    k_admitted = min(desired, matched_cnt)
            elif matched_cnt > 0:
                k_admitted = min(desired, matched_cnt)
            else:
                k_admitted = desired
            missing = max(0, k_admitted - matched_cnt)
            if missing > 0:
                _record_synthesized_pods(wf.namespace, missing, wf.per_pod_requests, None)
            continue

        ps_groups = _effective_t0_podset_shapes(wf)
        expected_total_pods = sum(cnt for _, cnt, _ in ps_groups)
        if not ps_groups or len(matched_pods) >= expected_total_pods:
            continue

        synth_end_ep = _resolve_synthesized_t0_end_epoch(
            wf,
            t0_epoch=effective_start_ep,
            effective_start_ep=effective_start_ep,
        )
        if synth_end_ep is not None and synth_end_ep <= effective_start_ep + 1e-6:
            continue

        # Allocate already-matched pods across ps_groups in two passes (exact profile, then hint/accel).
        unmatched_existing = list(matched_pods)
        group_have = [0] * len(ps_groups)
        group_matched_reqs: list[dict[str, float] | None] = [None] * len(ps_groups)
        for g_i, (_ps_name, cnt, reqs) in enumerate(ps_groups):
            prof_grp = _canonical_profile_from_reqs(reqs)
            still_unmatched: list[tuple[dict[str, Any], dict[str, float]]] = []
            for pt_pl, pt_reqs in unmatched_existing:
                if group_have[g_i] < cnt and _canonical_profile_from_reqs(pt_reqs) == prof_grp:
                    group_have[g_i] += 1
                else:
                    still_unmatched.append((pt_pl, pt_reqs))
            unmatched_existing = still_unmatched
        for g_i, (ps_name, cnt, reqs) in enumerate(ps_groups):
            if not unmatched_existing or group_have[g_i] >= cnt:
                continue
            accel_grp = _canonical_accel_profile_from_reqs(reqs)
            unmatched_existing.sort(
                key=lambda item: (
                    0 if str(item[0].get("podSetName") or "") == ps_name else 1,
                    0
                    if accel_grp and _canonical_accel_profile_from_reqs(item[1]) == accel_grp
                    else 1,
                )
            )
            take_cnt = min(cnt - group_have[g_i], len(unmatched_existing))
            if take_cnt > 0:
                if unmatched_existing[0][1]:
                    group_matched_reqs[g_i] = unmatched_existing[0][1]
                group_have[g_i] += take_cnt
                del unmatched_existing[:take_cnt]

        for g_i, (_ps_name, cnt, reqs) in enumerate(ps_groups):
            missing_cnt = max(0, cnt - group_have[g_i])
            if missing_cnt <= 0:
                continue
            eff_reqs = group_matched_reqs[g_i] if group_matched_reqs[g_i] is not None else reqs
            _record_synthesized_pods(wf.namespace, missing_cnt, eff_reqs, synth_end_ep)

    return {
        "total_initial_pods": len(qualifying_initial_pods) + synthesized_initial_pods,
        "synthesized_initial_pods": synthesized_initial_pods,
        "init_pod_requests_sum": dict(init_pod_requests_sum),
        "init_pod_lifetimes": init_pod_lifetimes,
        "init_pod_infinite_count": init_pod_infinite_count,
        "init_nodes_used": init_nodes_used,
        "init_ns_counts": init_ns_counts,
        "init_pod_profiles": init_pod_profiles,
    }


def _select_primary_resources(
    cluster_cap: dict[str, float],
    accel_resources: list[str],
    unadmitted_wfs: list[ParsedWorkload],
    init_pod_requests_sum: dict[str, float],
) -> tuple[str | None, float, str, str]:
    """Select `(primary_accel_resource, total_wf_accel, primary_sat_resource, primary_sat_label)`.

    Selects the true bottleneck resource with the highest demand-to-capacity ratio
    (`offered_resource_seconds / cluster_cap`, i.e., largest `T_100%`) across all resources
    (`accelerators`, `cpu`, `memory`) that have positive capacity and demand, breaking ties
    deterministically in favor of accelerators before `cpu` before `memory`.
    """
    accel_set = set(accel_resources)

    def _tie_key(rk: str) -> tuple[int, str]:
        if rk in accel_set:
            return (0, rk)
        if rk == "cpu":
            return (1, rk)
        return (2, rk)

    def _offered_sec(rk: str) -> float:
        return sum(w.total_requests.get(rk, 0.0) * w.duration_sec for w in unadmitted_wfs)

    def _pick_highest_ratio(candidates: list[str], use_init: bool = False) -> str | None:
        ratios: dict[str, float] = {}
        for rk in candidates:
            cap_val = cluster_cap.get(rk, 0.0)
            if cap_val <= 0.0:
                continue
            demand = init_pod_requests_sum.get(rk, 0.0) if use_init else _offered_sec(rk)
            if demand > 0.0:
                ratios[rk] = demand / cap_val
        if not ratios:
            return None
        max_ratio = max(ratios.values())
        tied = [
            rk
            for rk, ratio in ratios.items()
            if ratio >= max_ratio * (1.0 - 1e-9) - 1e-12
        ]
        tied.sort(key=_tie_key)
        return tied[0]

    primary_accel_resource: str | None = None
    if accel_resources:
        primary_accel_resource = (
            _pick_highest_ratio(accel_resources, use_init=False)
            or _pick_highest_ratio(accel_resources, use_init=True)
            or accel_resources[0]
        )

    total_wf_accel = (
        sum(w.total_requests.get(primary_accel_resource, 0.0) for w in unadmitted_wfs)
        if primary_accel_resource
        else 0.0
    )

    all_candidates = list(accel_resources) + [rk for rk in ("cpu", "memory") if rk in cluster_cap]
    primary_sat_resource = (
        _pick_highest_ratio(all_candidates, use_init=False)
        or _pick_highest_ratio(all_candidates, use_init=True)
        or (
            primary_accel_resource
            if primary_accel_resource and cluster_cap.get(primary_accel_resource, 0.0) > 0.0
            else "cpu"
        )
    )
    return primary_accel_resource, total_wf_accel, primary_sat_resource, f"`{primary_sat_resource}`"


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

    infra_timestamps = [
        ep
        for ev in infra_events
        if (ep := parse_rfc3339_epoch(ev.get("timestamp"))) is not None
    ]
    wf_timestamps = [
        ep
        for ev in workflow_events
        if (ep := parse_rfc3339_epoch(ev.get("timestamp"))) is not None
    ]
    all_t0_candidates = infra_timestamps + wf_timestamps
    t0_epoch = min(all_t0_candidates) if all_t0_candidates else None

    infra = _parse_infra_events(infra_events, t0_epoch=t0_epoch, initial_window_sec=1.0)
    nodes_info = _classify_nodes(infra["nodes_by_name"])
    priority_value_by_name, admitted_at_start_wfs, unadmitted_wfs, earliest_admit_by_ns = _parse_workflows(
        workflow_events,
        infra["wpc_by_name"],
        exec_index,
        t0_epoch,
        kueue_configuration=infra["kueue_configuration"],
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
        infra["pod_incarnations"],
        t0_epoch,
        t_first_unadmitted,
        admitted_at_start_wfs,
        unadmitted_wfs,
        earliest_admit_by_ns,
        lqs_by_key=infra["lqs_by_key"],
        cqs_by_name=infra["cqs_by_name"],
        kueue_configuration=infra["kueue_configuration"],
    )

    cluster_cap = nodes_info["cluster_cap"]
    primary_accel_resource, total_wf_accel, primary_sat_resource, primary_sat_label = _select_primary_resources(
        cluster_cap,
        nodes_info["accel_resources"],
        unadmitted_wfs,
        init_pods["init_pod_requests_sum"],
    )

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
        kueue_configuration=infra["kueue_configuration"],
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
        post_t0_node_events=infra["post_t0_node_events"],
        synthesized_initial_pods=init_pods["synthesized_initial_pods"],
    )
