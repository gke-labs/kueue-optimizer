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

"""Shared scenario parsing primitives: execution-time indexing, resource extraction, and initial-pod filtering."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import math
from pathlib import Path
from typing import Any

from chamber.constants import DEFAULT_FALLBACK_EXEC_SEC
from chamber.parsers import parse_duration_seconds, parse_k8s_quantity, parse_rfc3339_epoch
from chamber.yaml_utils import load_jsonl_events


@dataclass
class WorkloadExecutionInfo:
    """Indexed execution timing metadata for a single workload from execution-time.json."""

    workload_name: str
    namespace: str
    owner_kind: str
    owner_name: str
    local_queue_name: str = ""
    cluster_queue_name: str = ""
    priority_class_name: str = ""
    priority: int | None = None
    pod_set_assignments: list[dict[str, Any]] = field(default_factory=list)
    creation_epoch: float | None = None
    full_duration_sec: float | None = None
    full_end_epoch: float | None = None
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
        pc_name = str(pl.get("priorityClassName") or "")
        if pc_name and not info.priority_class_name:
            info.priority_class_name = pc_name
        if pl.get("priority") is not None and info.priority is None:
            try:
                info.priority = int(pl["priority"])
            except (TypeError, ValueError):
                pass
        if info.creation_epoch is None and pl.get("creationTime"):
            info.creation_epoch = parse_rfc3339_epoch(pl.get("creationTime"), strict=strict_timestamps)

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
            psa_raw = pl.get("podSetAssignments")
            if isinstance(psa_raw, list) and psa_raw:
                info.pod_set_assignments = [dict(x) for x in psa_raw if isinstance(x, dict)]
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
            end_ts = parse_rfc3339_epoch(pl.get("endTime"), strict=strict_timestamps) or ev_ts
            if end_ts is not None:
                info.full_end_epoch = end_ts

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


def build_topology_assignment(
    placements: list[str],
    topology_levels: list[str],
    nodes_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Build a valid Kueue v1beta2 `TopologyAssignment` from pod placements and Topology levels.

    Respects Kueue's `tasCache` hostname-lowest-level shortcut (`levels = ["kubernetes.io/hostname"]`
    whenever `topology_levels[-1] == "kubernetes.io/hostname"`) and CEL slice invariants.
    """
    valid_placements = [p for p in placements if p]
    clean_levels = [str(lv).strip() for lv in topology_levels if str(lv).strip()]
    if not valid_placements or not clean_levels:
        return None

    eff_levels = (
        ["kubernetes.io/hostname"]
        if clean_levels[-1] == "kubernetes.io/hostname"
        else list(clean_levels)
    )

    domain_counts: dict[tuple[str, ...], int] = {}
    for node_name in valid_placements:
        nd = nodes_by_name.get(node_name) or {}
        labels = (nd.get("metadata") or {}).get("labels") or {}
        vals: list[str] = []
        for lk in eff_levels:
            if lk == "kubernetes.io/hostname":
                vals.append(str(labels.get(lk) or node_name))
            else:
                v = str(labels.get(lk) or "")
                if not v:
                    return None
                vals.append(v)
        dom = tuple(vals)
        domain_counts[dom] = domain_counts.get(dom, 0) + 1

    if not domain_counts:
        return None

    domains = list(domain_counts.keys())
    counts = [domain_counts[d] for d in domains]
    dom_cnt = len(domains)

    values_per_level: list[dict[str, Any]] = []
    for li in range(len(eff_levels)):
        level_vals = [d[li] for d in domains]
        if len(set(level_vals)) == 1:
            values_per_level.append({"universal": level_vals[0]})
        else:
            values_per_level.append({"individual": {"roots": level_vals}})

    pod_counts: dict[str, Any] = (
        {"universal": counts[0]} if len(set(counts)) == 1 else {"individual": counts}
    )

    return {
        "levels": eff_levels,
        "slices": [
            {
                "domainCount": dom_cnt,
                "valuesPerLevel": values_per_level,
                "podCounts": pod_counts,
            }
        ],
    }
