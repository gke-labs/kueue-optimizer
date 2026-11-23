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

"""Queue setup, workload cleanup, and timed scenario replay loop."""

from __future__ import annotations

from dataclasses import replace as dc_replace
from datetime import datetime, timezone
import heapq
import json
import signal
import time
from typing import Any
import urllib.parse

from chamber.constants import validate_dns_label
from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot, query_cluster_status
from chamber.parsers import format_duration_hms
from runner.loader import (
    InitialPodSpec,
    InitialWorkloadPodGroup,
    ScenarioBundle,
    ScheduledInfraEvent,
    ScheduledWorkflowDeletion,
    ScheduledWorkflowItem,
)
from runner.validation import SYSTEM_NAMESPACES


def _pod_tolerates_taint(pod_spec: dict[str, Any] | None, taint: dict[str, Any]) -> bool:
    """Return True if pod_spec's tolerations tolerate the given taint indefinitely (or with positive tolerationSeconds)."""
    if not isinstance(pod_spec, dict) or not isinstance(taint, dict):
        return False
    t_key = str(taint.get("key") or "")
    t_val = str(taint.get("value") or "")
    t_eff = str(taint.get("effect") or "")
    for tol in pod_spec.get("tolerations") or []:
        if not isinstance(tol, dict):
            continue
        tol_eff = str(tol.get("effect") or "")
        if tol_eff and tol_eff != t_eff:
            continue
        tol_op = str(tol.get("operator") or "Equal")
        tol_key = str(tol.get("key") or "")
        tol_val = str(tol.get("value") or "")
        key_val_ok = False
        if tol_op == "Exists":
            key_val_ok = (tol_key == "") or (tol_key == t_key)
        elif tol_op in ("", "Equal"):
            key_val_ok = (tol_key == t_key) and (tol_val == t_val)
        if not key_val_ok:
            continue
        tol_sec = tol.get("tolerationSeconds")
        if tol_sec is not None:
            try:
                if float(tol_sec) <= 0:
                    continue
            except (TypeError, ValueError):
                pass
        return True
    return False


def _pod_tolerates_all_no_execute_taints(
    pod_spec: dict[str, Any] | None,
    no_execute_taints: list[dict[str, Any]],
) -> bool:
    return all(_pod_tolerates_taint(pod_spec, t) for t in no_execute_taints)


def _delete_rest_path(api_version: str, kind: str, namespace: str, name: str) -> str:
    plural_map = {
        "Node": ("v1", "nodes", True),
        "Namespace": ("v1", "namespaces", True),
        "Pod": ("v1", "pods", False),
        "Job": ("batch/v1", "jobs", False),
        "Deployment": ("apps/v1", "deployments", False),
        "JobSet": ("jobset.x-k8s.io/v1alpha2", "jobsets", False),
        "Workload": ("kueue.x-k8s.io/v1beta2", "workloads", False),
        "ClusterQueue": ("kueue.x-k8s.io/v1beta2", "clusterqueues", True),
        "Cohort": ("kueue.x-k8s.io/v1beta2", "cohorts", True),
        "LocalQueue": ("kueue.x-k8s.io/v1beta2", "localqueues", False),
        "ResourceFlavor": ("kueue.x-k8s.io/v1beta2", "resourceflavors", True),
        "Topology": ("kueue.x-k8s.io/v1beta2", "topologies", True),
        "WorkloadPriorityClass": ("kueue.x-k8s.io/v1beta2", "workloadpriorityclasses", True),
        "PriorityClass": ("scheduling.k8s.io/v1", "priorityclasses", True),
        "RuntimeClass": ("node.k8s.io/v1", "runtimeclasses", True),
    }
    default_api, plural, cluster_scoped = plural_map.get(kind, (api_version or "v1", f"{kind.lower()}s", False))
    eff_api = api_version or default_api
    prefix = "/api/v1" if eff_api == "v1" else f"/apis/{eff_api}"
    if cluster_scoped or not namespace:
        return f"{prefix}/{plural}/{name}"
    return f"{prefix}/namespaces/{namespace}/{plural}/{name}"


def _fail_pods_bound_to_node(
    mgr: ChamberManager,
    allowed_namespaces: set[str],
    node_name: str,
    reason: str,
    message: str,
    *,
    no_execute_taints: list[dict[str, Any]] | None = None,
) -> int:
    """Transition active non-terminal Pods bound to `node_name` to phase=Failed so Job/JobSet & Kueue TAS react immediately."""
    q_sel = urllib.parse.quote(f"spec.nodeName={node_name}", safe="=")
    st, raw = mgr.api_request("GET", f"/api/v1/pods?fieldSelector={q_sel}")
    if st != 200 or not raw:
        return 0
    items = json.loads(raw.decode("utf-8")).get("items") or []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    failed_count = 0
    for pod in items:
        meta = pod.get("metadata") or {}
        ns = str(meta.get("namespace") or "default")
        p_name = str(meta.get("name") or "")
        if not p_name or (allowed_namespaces and ns not in allowed_namespaces):
            continue
        if meta.get("deletionTimestamp"):
            continue
        p_status = pod.get("status") or {}
        if str(p_status.get("phase") or "") in ("Succeeded", "Failed"):
            continue
        p_spec = pod.get("spec") or {}
        if no_execute_taints and _pod_tolerates_all_no_execute_taints(p_spec, no_execute_taints):
            continue
        containers = p_spec.get("containers") or []
        c_statuses = []
        for c in containers:
            if not isinstance(c, dict):
                continue
            c_statuses.append(
                {
                    "name": str(c.get("name") or "pause"),
                    "image": str(c.get("image") or "registry.k8s.io/pause:3.10"),
                    "ready": False,
                    "started": False,
                    "restartCount": 0,
                    "state": {
                        "terminated": {
                            "exitCode": 137,
                            "reason": reason,
                            "message": message,
                            "startedAt": str(p_status.get("startTime") or now_iso),
                            "finishedAt": now_iso,
                        }
                    },
                }
            )
        status_patch = {
            "status": {
                "phase": "Failed",
                "reason": reason,
                "message": message,
                "conditions": [
                    {"type": "Initialized", "status": "True", "lastTransitionTime": now_iso},
                    {"type": "PodScheduled", "status": "True", "lastTransitionTime": now_iso},
                    {"type": "Ready", "status": "False", "reason": reason, "lastTransitionTime": now_iso},
                    {"type": "ContainersReady", "status": "False", "reason": reason, "lastTransitionTime": now_iso},
                ],
                "containerStatuses": c_statuses,
            }
        }
        p_st, _ = mgr.api_request(
            "PATCH",
            f"/api/v1/namespaces/{ns}/pods/{p_name}/status",
            body=json.dumps(status_patch).encode("utf-8"),
            headers={"Content-Type": "application/merge-patch+json"},
        )
        if p_st in (200, 201):
            failed_count += 1
    return failed_count


def _patch_node_ready_condition(
    mgr: ChamberManager,
    node_name: str,
    ready_status: str,
    reason: str,
    message: str,
    extra_status: dict[str, Any] | None = None,
) -> None:
    """Patch Node /status Ready condition while preserving allocatable/capacity."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status_body: dict[str, Any] = {
        "conditions": [
            {
                "type": "Ready",
                "status": ready_status,
                "reason": reason,
                "message": message,
                "lastHeartbeatTime": now_iso,
                "lastTransitionTime": now_iso,
            }
        ],
        "phase": "Running",
    }
    if extra_status:
        for k in ("allocatable", "capacity", "nodeInfo"):
            if extra_status.get(k):
                status_body[k] = extra_status[k]
    mgr.api_request(
        "PATCH",
        f"/api/v1/nodes/{node_name}/status",
        body=json.dumps({"status": status_body}).encode("utf-8"),
        headers={"Content-Type": "application/merge-patch+json"},
    )


PROTECTED_PREREQ_INFRA_KINDS = frozenset(
    {
        "Namespace",
        "PriorityClass",
        "WorkloadPriorityClass",
        "RuntimeClass",
        "Topology",
    }
)
DEFERRED_QUEUE_INFRA_KINDS = frozenset(
    {
        "LocalQueue",
        "ClusterQueue",
        "ResourceFlavor",
        "Cohort",
    }
)


def _extract_cq_flavors_and_cohort(cq_doc: dict[str, Any]) -> tuple[str, set[str]]:
    spec = (cq_doc.get("spec") or {}) if isinstance(cq_doc, dict) else {}
    cohort = str(spec.get("cohortName") or spec.get("cohort") or "").strip()
    flavors: set[str] = set()
    for rg in spec.get("resourceGroups") or []:
        if isinstance(rg, dict):
            for flv in rg.get("flavors") or []:
                if isinstance(flv, dict) and flv.get("name"):
                    flavors.add(str(flv["name"]))
    return cohort, flavors


def _is_kueue_infra_object_still_in_use(
    mgr: ChamberManager,
    ie: ScheduledInfraEvent,
    *,
    event_heap: list[ScheduledWorkflowItem],
    cancelled_unsubmitted_keys: set[tuple[str, str, str, int]],
    active_initial_wls: list[Any],
    lq_to_cq: dict[tuple[str, str], str],
    cq_to_cohort: dict[str, str],
    cq_to_flavors: dict[str, set[str]],
    submitted_non_cancelled: list[tuple[str, str, str]],
) -> tuple[bool, str]:
    """Return `(True, reason)` if any unsubmitted or incomplete chamber workload still depends on `ie`."""
    kind = ie.object_kind
    target_ns = ie.namespace
    target_name = ie.name

    def _cq_uses_target(cq_n: str) -> bool:
        if not cq_n:
            return False
        if kind == "ClusterQueue":
            return cq_n == target_name
        if kind == "Cohort":
            return cq_to_cohort.get(cq_n, "") == target_name
        if kind == "ResourceFlavor":
            return target_name in cq_to_flavors.get(cq_n, set())
        return False

    def _lq_uses_target(ns_val: str, lq_val: str) -> bool:
        if kind == "LocalQueue":
            return ns_val == target_ns and lq_val == target_name
        cq_n = lq_to_cq.get((ns_val, lq_val), "")
        if cq_n:
            return _cq_uses_target(cq_n)
        # Conservative fallback when a workflow omits queue-name (e.g., manageJobsWithoutQueueName or single CQ)
        if not lq_val and len(cq_to_flavors) == 1:
            return _cq_uses_target(next(iter(cq_to_flavors.keys())))
        return False

    # 1. Unsubmitted future workflows in event_heap
    for w in event_heap:
        c_key = (w.namespace, w.object_kind, w.name, w.incarnation_index)
        if c_key in cancelled_unsubmitted_keys:
            continue
        if _lq_uses_target(w.namespace, w.local_queue_name):
            return True, f"unsubmitted workflow '{w.namespace}/{w.name}' still queued in replay timeline"

    # 2. Active t0 initial workloads
    for iwl in active_initial_wls:
        if kind == "LocalQueue" and iwl.namespace == target_ns and iwl.local_queue_name == target_name:
            return True, f"active t0 Workload '{iwl.namespace}/{iwl.workload_name}'"
        if kind != "LocalQueue" and _cq_uses_target(iwl.cluster_queue_name):
            return True, f"active t0 Workload '{iwl.namespace}/{iwl.workload_name}'"

    # 3. Live Kueue Workloads (and newly submitted workflows awaiting Workload creation) in the chamber
    st_wl, raw_wl = mgr.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/workloads")
    if st_wl != 200 or not raw_wl:
        return True, "transient workload API check"

    try:
        wl_items = json.loads(raw_wl.decode("utf-8")).get("items") or []
    except Exception:
        return True, "workload JSON parse retry"

    seen_owner_names_by_ns: dict[str, set[str]] = {}
    for wl in wl_items:
        w_meta = wl.get("metadata") or {}
        w_ns = str(w_meta.get("namespace") or "default")
        w_name = str(w_meta.get("name") or "")
        for oref in w_meta.get("ownerReferences") or []:
            if isinstance(oref, dict) and oref.get("name"):
                seen_owner_names_by_ns.setdefault(w_ns, set()).add(str(oref["name"]))
        w_spec = wl.get("spec") or {}
        if w_spec.get("active") is False:
            continue
        w_status = wl.get("status") or {}
        conds = w_status.get("conditions") or []
        is_finished = any(
            isinstance(c, dict) and c.get("type") == "Finished" and c.get("status") == "True"
            for c in conds
        )
        if is_finished:
            continue
        w_lq = str(w_spec.get("queueName") or "")
        adm_cq = str(((w_status.get("admission") or {}).get("clusterQueue")) or "")
        if kind == "LocalQueue" and w_ns == target_ns and w_lq == target_name:
            return True, f"incomplete Workload '{w_ns}/{w_name}'"
        if kind != "LocalQueue" and (_cq_uses_target(adm_cq) or _lq_uses_target(w_ns, w_lq)):
            return True, f"incomplete Workload '{w_ns}/{w_name}'"

    # Guard the brief controller-creation window right after submitting a parent JobSet/Job before its Workload exists
    for sub_ns, sub_lq, sub_applied_name in submitted_non_cancelled:
        if sub_applied_name not in seen_owner_names_by_ns.get(sub_ns, set()):
            if _lq_uses_target(sub_ns, sub_lq):
                return True, f"newly submitted workflow '{sub_ns}/{sub_applied_name}' awaiting Workload reconciliation"

    return False, ""


def _execute_scheduled_infra_event(
    mgr: ChamberManager,
    bundle: ScenarioBundle,
    ie: ScheduledInfraEvent,
    wall_elapsed: float,
    *,
    event_heap: list[ScheduledWorkflowItem] | None = None,
    cancelled_unsubmitted_keys: set[tuple[str, str, str, int]] | None = None,
    active_initial_wls: list[Any] | None = None,
    lq_to_cq: dict[tuple[str, str], str] | None = None,
    cq_to_cohort: dict[str, str] | None = None,
    cq_to_flavors: dict[str, set[str]] | None = None,
    submitted_non_cancelled: list[tuple[str, str, str]] | None = None,
) -> tuple[bool, str]:
    """Apply a scheduled post-t0 Node, Queue/Quota, PriorityClass, RuntimeClass, Namespace, or non-Kueue Pod event.

    Returns `(deferred, defer_reason)`: `(True, reason)` when a queue/flavor/cohort deletion is safely
    deferred because chamber workloads depending on it have not finished yet; `(False, "")` otherwise.
    """
    if ie.object_kind != "Node":
        target_label = f"{ie.namespace}/{ie.name}" if ie.namespace else ie.name
        if ie.event_type == "Deleted":
            if ie.object_kind in PROTECTED_PREREQ_INFRA_KINDS:
                # Keep workload namespaces and static scheduling prerequisites intact so delayed arrivals,
                # Kueue Workload history, and end-of-run metrics remain readable.
                return False, ""
            if ie.object_kind in DEFERRED_QUEUE_INFRA_KINDS and lq_to_cq is not None:
                in_use, use_reason = _is_kueue_infra_object_still_in_use(
                    mgr,
                    ie,
                    event_heap=event_heap or [],
                    cancelled_unsubmitted_keys=cancelled_unsubmitted_keys or set(),
                    active_initial_wls=active_initial_wls or [],
                    lq_to_cq=lq_to_cq,
                    cq_to_cohort=cq_to_cohort or {},
                    cq_to_flavors=cq_to_flavors or {},
                    submitted_non_cancelled=submitted_non_cancelled or [],
                )
                if in_use:
                    return True, use_reason
            del_path = _delete_rest_path("", ie.object_kind, ie.namespace, ie.name)
            mgr.api_delete(del_path, ignore_not_found=True)
            if ie.object_kind == "LocalQueue" and lq_to_cq is not None:
                lq_to_cq.pop((ie.namespace, ie.name), None)
            elif ie.object_kind == "ClusterQueue":
                if cq_to_cohort is not None:
                    cq_to_cohort.pop(ie.name, None)
                if cq_to_flavors is not None:
                    cq_to_flavors.pop(ie.name, None)
            print(
                f"  - [t={wall_elapsed:.2f}s wall / +{ie.raw_offset_sec:.2f}s (t0+{ie.raw_offset_from_t0_sec:.2f}s)] "
                f"Deleted {ie.object_kind} '{target_label}'",
                flush=True,
            )
            return False, ""
        if ie.manifest:
            mgr.ssa_apply_doc(ie.manifest)
            if ie.object_kind == "LocalQueue" and lq_to_cq is not None:
                cq_ref = str(((ie.manifest.get("spec") or {}).get("clusterQueue")) or "")
                if cq_ref:
                    lq_to_cq[(ie.namespace, ie.name)] = cq_ref
            elif ie.object_kind == "ClusterQueue":
                c_ref, flvs = _extract_cq_flavors_and_cohort(ie.manifest)
                if cq_to_cohort is not None:
                    cq_to_cohort[ie.name] = c_ref
                if cq_to_flavors is not None:
                    cq_to_flavors[ie.name] = flvs
            print(
                f"  * [t={wall_elapsed:.2f}s wall / +{ie.raw_offset_sec:.2f}s (t0+{ie.raw_offset_from_t0_sec:.2f}s)] "
                f"Applied {ie.event_type} {ie.object_kind} '{target_label}'",
                flush=True,
            )
        return False, ""

    allowed_ns = set(bundle.workload_namespaces) | {bundle.infra_pods_namespace}
    if ie.event_type == "Deleted":
        failed_pods = _fail_pods_bound_to_node(
            mgr,
            allowed_ns,
            ie.name,
            reason="NodeDeleted",
            message=f"Simulated Node '{ie.name}' deleted during scenario replay",
        )
        mgr.api_request("DELETE", f"/api/v1/nodes/{ie.name}")
        print(
            f"  ! [t={wall_elapsed:.2f}s wall / +{ie.raw_offset_sec:.2f}s (t0+{ie.raw_offset_from_t0_sec:.2f}s)] "
            f"Deleted Node '{ie.name}' (failed {failed_pods} bound pod(s))",
            flush=True,
        )
        return False, ""

    if ie.event_type == "New":
        mgr.ssa_apply_doc(ie.manifest)
        st_blk = (ie.manifest.get("status") or {}) if isinstance(ie.manifest, dict) else {}
        if ie.is_not_ready:
            _patch_node_ready_condition(
                mgr,
                ie.name,
                ready_status=ie.ready_status or "False",
                reason="KubeletNotReady",
                message="Node added in NotReady state",
                extra_status=st_blk,
            )
        else:
            _patch_node_ready_condition(
                mgr,
                ie.name,
                ready_status="True",
                reason="KubeletReady",
                message="kubelet is posting ready status",
                extra_status=st_blk,
            )
        print(
            f"  * [t={wall_elapsed:.2f}s wall / +{ie.raw_offset_sec:.2f}s (t0+{ie.raw_offset_from_t0_sec:.2f}s)] "
            f"Added Node '{ie.name}' (Ready={'False' if ie.is_not_ready else 'True'})",
            flush=True,
        )
        return False, ""

    # ie.event_type == "Update"
    spec_in = dict((ie.manifest.get("spec") or {}) if isinstance(ie.manifest, dict) else {})
    taints_list = [dict(t) for t in (spec_in.get("taints") or []) if isinstance(t, dict)]
    if ie.is_not_ready or ie.has_no_execute_taint:
        if ie.is_not_ready and not any(
            t.get("key") == "node.kubernetes.io/not-ready" for t in taints_list
        ):
            taints_list.append({"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"})
        patch_doc = {
            "metadata": {"annotations": {"kueue-optimizer.gke.io/node-failed": "true"}},
            "spec": {
                "unschedulable": bool(spec_in.get("unschedulable", ie.is_not_ready)),
                "taints": taints_list,
            },
        }
        mgr.api_request(
            "PATCH",
            f"/api/v1/nodes/{ie.name}",
            body=json.dumps(patch_doc).encode("utf-8"),
            headers={"Content-Type": "application/merge-patch+json"},
        )
        if ie.is_not_ready:
            _patch_node_ready_condition(
                mgr,
                ie.name,
                ready_status=ie.ready_status or "False",
                reason="KubeletNotReady",
                message="Simulated node failure during scenario replay",
            )
        no_exec_filter = (
            [t for t in taints_list if str(t.get("effect") or "") == "NoExecute"]
            if (not ie.is_not_ready and ie.has_no_execute_taint)
            else None
        )
        failed_pods = _fail_pods_bound_to_node(
            mgr,
            allowed_ns,
            ie.name,
            reason="NodeFailure" if ie.is_not_ready else "TaintManagerEviction",
            message=f"Simulated node {'failure' if ie.is_not_ready else 'NoExecute taint eviction'} on '{ie.name}'",
            no_execute_taints=no_exec_filter,
        )
        print(
            f"  ! [t={wall_elapsed:.2f}s wall / +{ie.raw_offset_sec:.2f}s (t0+{ie.raw_offset_from_t0_sec:.2f}s)] "
            f"Node '{ie.name}' marked unhealthy (Ready={ie.ready_status or ('False' if ie.is_not_ready else 'True')}, "
            f"noExecuteTaint={ie.has_no_execute_taint}, failed {failed_pods} bound pod(s))",
            flush=True,
        )
    else:
        patch_doc = {
            "metadata": {"annotations": {"kueue-optimizer.gke.io/node-failed": None}},
            "spec": {
                "unschedulable": bool(spec_in.get("unschedulable", False)),
                "taints": taints_list,
            },
        }
        mgr.api_request(
            "PATCH",
            f"/api/v1/nodes/{ie.name}",
            body=json.dumps(patch_doc).encode("utf-8"),
            headers={"Content-Type": "application/merge-patch+json"},
        )
        _patch_node_ready_condition(
            mgr,
            ie.name,
            ready_status="True",
            reason="KubeletReady",
            message="kubelet is posting ready status",
        )
        print(
            f"  * [t={wall_elapsed:.2f}s wall / +{ie.raw_offset_sec:.2f}s (t0+{ie.raw_offset_from_t0_sec:.2f}s)] "
            f"Node '{ie.name}' updated/recovered (Ready=True, unschedulable={bool(spec_in.get('unschedulable', False))})",
            flush=True,
        )
    return False, ""


def apply_scenario_queues_and_wait(
    mgr: ChamberManager,
    prereq_and_queue_docs: list[dict[str, Any]],
    timeout_sec: float = 60.0,
    *,
    initial_workloads: list[InitialWorkloadPodGroup] | None = None,
    initial_pods: list[InitialPodSpec] | None = None,
) -> None:
    """Apply prerequisites, optionally bootstrap t0 PodGroup Workloads & Pods, then activate ClusterQueues/LocalQueues."""
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

    # Bootstrap pre-admitted t0 Kueue PodGroup Workloads + member Pods + non-Kueue infra-pods BEFORE
    # ClusterQueues/LocalQueues exist so Cache.AddClusterQueue loads quota, FairSharing, and TAS usage atomically.
    if initial_workloads:
        wl_pod_total = sum(len(w.pods) for w in initial_workloads)
        print(
            f"==> Bootstrapping {len(initial_workloads)} pre-admitted t0 Kueue PodGroup Workload(s) "
            f"({wl_pod_total} pod(s)) before queue activation...",
            flush=True,
        )
        now_dt = datetime.now(timezone.utc)
        stamped_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for idx_w, w in enumerate(initial_workloads):
            age_offset = max(0.0, float(getattr(w, "original_age_sec", 0.0) or 0.0))
            if age_offset == 0.0 and len(initial_workloads) > 1:
                age_offset = float(len(initial_workloads) - idx_w)
            wl_ts = datetime.fromtimestamp(now_dt.timestamp() - age_offset, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            st_copy = json.loads(json.dumps(w.workload_status))
            for cond in st_copy.get("conditions") or []:
                if isinstance(cond, dict):
                    cond["lastTransitionTime"] = wl_ts
            stamped_pairs.append((w.workload_manifest, st_copy))
        mgr.apply_pre_admitted_workloads_parallel(stamped_pairs)
        for iwl in initial_workloads[:8]:
            tas_tag = " [TAS]" if iwl.is_tas else ""
            print(
                f"    [Initial Workload] {iwl.namespace}/{iwl.workload_name} -> cq={iwl.cluster_queue_name} "
                f"lq={iwl.local_queue_name} pods={len(iwl.pods)}{tas_tag}",
                flush=True,
            )
        if len(initial_workloads) > 8:
            print(f"    ... and {len(initial_workloads) - 8} more initial Kueue workloads.", flush=True)

    all_bootstrap_pods: list[InitialPodSpec] = [
        p for w in (initial_workloads or []) for p in w.pods
    ] + list(initial_pods or [])
    if all_bootstrap_pods:
        if initial_pods:
            print(
                f"==> Placing {len(initial_pods)} non-Kueue initial Pod(s) into Kueue-excluded "
                f"namespace '{initial_pods[0].namespace}' onto their nodes...",
                flush=True,
            )
            for ipod in initial_pods[:8]:
                dur_label = "infinite/super-long" if ipod.is_infinite else f"{ipod.remaining_duration_sec:.2f}s"
                print(
                    f"    [Initial Infra Pod] {ipod.namespace}/{ipod.name} -> node={ipod.placement} "
                    f"requests={ipod.resource_request} remaining={dur_label}",
                    flush=True,
                )
            if len(initial_pods) > 8:
                print(f"    ... and {len(initial_pods) - 8} more initial infra pods.", flush=True)

        mgr.apply_manifests_parallel([p.manifest for p in all_bootstrap_pods], max_workers=16)
        mgr.wait_for_pods_running(
            [(p.namespace, str((p.manifest.get("metadata") or {}).get("name") or p.name)) for p in all_bootstrap_pods]
        )

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


def run_scenario_replay(
    mgr: ChamberManager,
    bundle: ScenarioBundle,
    speedup: float,
    max_run_seconds: float | None,
    status_interval_sec: float,
    wait_for_completion: bool,
    *,
    initial_state_already_bootstrapped: bool = False,
) -> tuple[float, float, int, int]:
    """Run timed workflow arrivals and watch workloads until completion."""
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

    if not initial_state_already_bootstrapped:
        if bundle.initial_workloads:
            mgr.apply_pre_admitted_workloads_parallel(
                [(w.workload_manifest, w.workload_status) for w in bundle.initial_workloads]
            )
            mgr.apply_manifests_parallel(
                [p.manifest for w in bundle.initial_workloads for p in w.pods], max_workers=16
            )
        if bundle.initial_pods:
            print(
                f"==> Placing {len(bundle.initial_pods)} initial running Pod(s) into Kueue-excluded "
                f"namespace '{bundle.infra_pods_namespace}' onto their nodes...",
                flush=True,
            )
            mgr.apply_manifests_parallel([p.manifest for p in bundle.initial_pods], max_workers=16)

    event_heap: list[ScheduledWorkflowItem] = list(bundle.scheduled_workflows)
    heapq.heapify(event_heap)
    infra_heap: list[ScheduledInfraEvent] = list(bundle.scheduled_infra_events)
    heapq.heapify(infra_heap)
    delete_heap: list[ScheduledWorkflowDeletion] = list(bundle.scheduled_workflow_deletions)
    heapq.heapify(delete_heap)

    initial_wl_by_key = {(w.namespace, w.workload_name): w for w in bundle.initial_workloads}
    initial_wl_keys = set(initial_wl_by_key.keys())
    finite_initial_wl_keys = {
        (w.namespace, w.workload_name)
        for w in bundle.initial_workloads
        if not all(p.is_infinite for p in w.pods)
    }
    resubmitted_initial_wls: set[tuple[str, str]] = set()
    ever_finished_sched_wl_keys: set[tuple[str, str]] = set()
    ever_finished_init_wl_keys: set[tuple[str, str]] = set()
    cancelled_workflow_count = 0
    cancelled_unsubmitted_keys: set[tuple[str, str, str, int]] = set()
    recreated_names: set[tuple[str, str]] = {
        (d.namespace, d.applied_name)
        for d in bundle.scheduled_workflow_deletions
        if d.delete_reason == "before_recreate" or d.has_later_incarnation
    }
    lq_to_cq: dict[tuple[str, str], str] = {}
    cq_to_cohort: dict[str, str] = {}
    cq_to_flavors: dict[str, set[str]] = {}
    for q_doc in bundle.prereq_and_queue_docs:
        q_kind = str(q_doc.get("kind") or "")
        q_meta = q_doc.get("metadata") or {}
        q_name = str(q_meta.get("name") or "")
        if q_kind == "LocalQueue" and q_name:
            q_ns = str(q_meta.get("namespace") or "default")
            cq_ref = str(((q_doc.get("spec") or {}).get("clusterQueue")) or "")
            if cq_ref:
                lq_to_cq[(q_ns, q_name)] = cq_ref
        elif q_kind == "ClusterQueue" and q_name:
            c_ref, flvs = _extract_cq_flavors_and_cohort(q_doc)
            cq_to_cohort[q_name] = c_ref
            cq_to_flavors[q_name] = flvs
    submitted_non_cancelled: dict[tuple[str, str], tuple[str, str, str]] = {}
    deferred_infra_logged_seqs: set[int] = set()

    total_planned = len(bundle.scheduled_workflows)
    submitted_count = 0
    total_pods_submitted = bundle.initial_workload_pods_count + len(bundle.initial_pods)

    stop_requested = False

    def _sig_handler(signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        print(f"\n==> Received signal {signum}; stopping testrunner gracefully...", flush=True)
        stop_requested = True

    old_sigint = signal.signal(signal.SIGINT, _sig_handler)
    old_sigterm = signal.signal(signal.SIGTERM, _sig_handler)

    start_epoch = time.time()
    t0_mono = time.monotonic()
    fast_poll_floor = 1.5 if (total_planned + len(initial_wl_keys)) > 200 else 0.5
    poll_interval_sec = min(status_interval_sec, fast_poll_floor)
    next_status_mono = t0_mono
    next_poll_mono = t0_mono

    def _check_and_resubmit_evicted_initial_workloads(
        snap: ClusterStatusSnapshot,
        wall_elapsed: float,
    ) -> None:
        nonlocal total_planned, submitted_count, total_pods_submitted
        if not initial_wl_keys:
            return
        detected_evicted = set(snap.evicted_workload_keys & initial_wl_keys)
        if snap.workloads_query_ok:
            # When Kueue preempts a PodGroup (`retriable-in-group: "false"`), its member Pods are deleted
            # and Kubernetes GC may garbage-collect the PodGroup Workload within milliseconds before the next
            # poll; normal completion leaves Succeeded Pods and the Finished Workload intact.
            gc_after_preempt = (
                initial_wl_keys - snap.present_workload_keys - snap.finished_workload_keys
            )
            detected_evicted |= gc_after_preempt
        newly_evicted = sorted(detected_evicted - resubmitted_initial_wls)
        for wl_key in newly_evicted:
            resubmitted_initial_wls.add(wl_key)
            iwl = initial_wl_by_key[wl_key]
            if iwl.resubmit_item is None:
                continue
            it = iwl.resubmit_item
            mgr.ssa_apply_doc(it.manifest)
            resub_applied = str((it.manifest.get("metadata") or {}).get("name") or it.name)
            submitted_non_cancelled[(it.namespace, resub_applied)] = (
                it.namespace,
                it.local_queue_name,
                resub_applied,
            )
            total_planned += 1
            submitted_count += 1
            total_pods_submitted += it.pod_count
            print(
                f"  ~ [t={wall_elapsed:.2f}s wall] Initial t0 PodGroup Workload '{iwl.namespace}/{iwl.workload_name}' "
                f"was preempted/evicted -> dynamically re-submitted {it.object_kind} '{it.namespace}/{it.name}' "
                f"(queue={it.local_queue_name or '-'}, pods={it.pod_count}, pod_exec={it.compressed_duration_sec:.2f}s)",
                flush=True,
            )

    def _scheduled_done_count(snap: ClusterStatusSnapshot) -> int:
        ever_finished_sched_wl_keys.update(snap.finished_workload_keys - initial_wl_keys)
        wl_done = len(ever_finished_sched_wl_keys) + cancelled_workflow_count
        if recreated_names:
            return wl_done
        if initial_wl_keys:
            return max(wl_done, snap.jobsets_completed + snap.jobsets_failed + cancelled_workflow_count)
        return max(wl_done, snap.workloads_finished + cancelled_workflow_count, snap.jobsets_completed + snap.jobsets_failed + cancelled_workflow_count)

    def _finite_initial_wls_done(snap: ClusterStatusSnapshot) -> bool:
        for wl_key in finite_initial_wl_keys:
            if (
                wl_key in snap.finished_workload_keys
                or wl_key in resubmitted_initial_wls
                or wl_key in snap.evicted_workload_keys
                or (snap.workloads_query_ok and wl_key not in snap.present_workload_keys)
            ):
                continue
            return False
        return True

    def _delete_workflow_and_children(del_ev: ScheduledWorkflowDeletion, wait_gone: bool) -> None:
        del_path = _delete_rest_path(
            del_ev.manifest_api_version,
            del_ev.manifest_kind,
            del_ev.namespace,
            del_ev.applied_name,
        )
        del_opts = json.dumps({"apiVersion": "meta/v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground"}).encode("utf-8")
        mgr.api_request(
            "DELETE",
            del_path,
            body=del_opts,
            headers={"Content-Type": "application/json"},
        )
        # Also remove pods & child Jobs in namespace belonging to this workflow so KWOK pods release nodes immediately.
        st_p, raw_p = mgr.api_request("GET", f"/api/v1/namespaces/{del_ev.namespace}/pods")
        if st_p == 200 and raw_p:
            pfx = f"{del_ev.applied_name}-"
            for p_it in json.loads(raw_p.decode("utf-8")).get("items") or []:
                pm = p_it.get("metadata") or {}
                pn = str(pm.get("name") or "")
                plbls = pm.get("labels") or {}
                owned = (
                    pn == del_ev.applied_name
                    or pn.startswith(pfx)
                    or plbls.get("job-name") == del_ev.applied_name
                    or plbls.get("jobset.sigs.k8s.io/jobset-name") == del_ev.applied_name
                )
                if owned and pn:
                    mgr.api_request(
                        "DELETE",
                        f"/api/v1/namespaces/{del_ev.namespace}/pods/{pn}?gracePeriodSeconds=0",
                    )
        if wait_gone:
            wait_deadline = time.monotonic() + 5.0
            while time.monotonic() < wait_deadline:
                st_chk, _ = mgr.api_request("GET", del_path)
                if st_chk == 404:
                    break
                time.sleep(0.1)

    def _execute_workflow_deletion(del_ev: ScheduledWorkflowDeletion, wall_elapsed: float) -> None:
        nonlocal cancelled_workflow_count, total_planned
        if del_ev.initial_workload_name:
            wl_key = (del_ev.namespace, del_ev.initial_workload_name)
            resubmitted_initial_wls.add(wl_key)
            iwl = initial_wl_by_key.get(wl_key)
            if iwl is not None:
                for p_spec in iwl.pods:
                    pm_name = str((p_spec.manifest.get("metadata") or {}).get("name") or p_spec.name)
                    mgr.api_request(
                        "DELETE",
                        f"/api/v1/namespaces/{del_ev.namespace}/pods/{pm_name}?gracePeriodSeconds=0",
                    )
            mgr.api_delete(
                f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{del_ev.namespace}/workloads/{del_ev.initial_workload_name}",
                ignore_not_found=True,
            )
            print(
                f"  - [t={wall_elapsed:.2f}s wall / +{del_ev.raw_offset_sec:.2f}s] "
                f"Deleted t0 initial Workload '{del_ev.namespace}/{del_ev.initial_workload_name}'",
                flush=True,
            )
            return

        # Snapshot finished workload keys BEFORE deleting an object that is about to be re-created with the same name.
        pre_snap = query_cluster_status(
            mgr,
            workload_namespaces=bundle.workload_namespaces,
            include_kueue_workloads=True,
        )
        ever_finished_sched_wl_keys.update(pre_snap.finished_workload_keys - initial_wl_keys)
        ever_finished_init_wl_keys.update(pre_snap.finished_workload_keys & initial_wl_keys)

        unsubmitted_match = any(
            w.namespace == del_ev.namespace
            and w.object_kind == del_ev.object_kind
            and w.name == del_ev.name
            and w.incarnation_index == del_ev.incarnation_index
            for w in event_heap
        )
        if unsubmitted_match and del_ev.delete_reason == "cancelled":
            cancelled_unsubmitted_keys.add(
                (del_ev.namespace, del_ev.object_kind, del_ev.name, del_ev.incarnation_index)
            )
            total_planned = max(0, total_planned - 1)
            print(
                f"  - [t={wall_elapsed:.2f}s wall / +{del_ev.raw_offset_sec:.2f}s] "
                f"Cancelled pending unsubmitted {del_ev.object_kind} '{del_ev.namespace}/{del_ev.name}' "
                f"(incarnation={del_ev.incarnation_index})",
                flush=True,
            )
            return

        # Check whether this submitted incarnation already finished cleanly in the chamber prior to deletion
        # so faster chamber execution does not double-count both its Finished Workload and cancelled_workflow_count.
        already_succeeded_in_chamber = False
        st_wls, raw_wls = mgr.api_request(
            "GET",
            f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{del_ev.namespace}/workloads",
        )
        if st_wls == 200 and raw_wls:
            try:
                for wl_it in json.loads(raw_wls.decode("utf-8")).get("items") or []:
                    w_m = wl_it.get("metadata") or {}
                    owners = [
                        str(o.get("name") or "")
                        for o in (w_m.get("ownerReferences") or [])
                        if isinstance(o, dict)
                    ]
                    if del_ev.applied_name in owners or str(w_m.get("name") or "") == del_ev.applied_name:
                        conds = ((wl_it.get("status") or {}).get("conditions")) or []
                        if any(
                            isinstance(c, dict)
                            and c.get("type") == "Finished"
                            and c.get("status") == "True"
                            and c.get("reason") == "Succeeded"
                            for c in conds
                        ):
                            already_succeeded_in_chamber = True
                            break
            except Exception:
                pass

        submitted_non_cancelled.pop((del_ev.namespace, del_ev.applied_name), None)
        has_future_same_name = any(
            w.namespace == del_ev.namespace
            and str((w.manifest.get("metadata") or {}).get("name") or w.name) == del_ev.applied_name
            for w in event_heap
        )
        _delete_workflow_and_children(del_ev, wait_gone=(del_ev.delete_reason == "before_recreate" or has_future_same_name))
        if del_ev.delete_reason == "cancelled" and not already_succeeded_in_chamber:
            cancelled_workflow_count += 1
        print(
            f"  - [t={wall_elapsed:.2f}s wall / +{del_ev.raw_offset_sec:.2f}s] "
            f"Deleted {del_ev.object_kind} '{del_ev.namespace}/{del_ev.name}' "
            f"(incarnation={del_ev.incarnation_index}, reason={del_ev.delete_reason})",
            flush=True,
        )

    def _print_status_line(snap: ClusterStatusSnapshot, now_mono: float, note: str = "") -> None:
        wall_elapsed = now_mono - t0_mono
        sim_elapsed = wall_elapsed * speedup
        next_candidates: list[float] = []
        if event_heap:
            next_candidates.append(max(0.0, event_heap[0].target_wall_offset_sec - wall_elapsed))
        if delete_heap:
            next_candidates.append(max(0.0, delete_heap[0].target_wall_offset_sec - wall_elapsed))
        if infra_heap:
            next_candidates.append(max(0.0, infra_heap[0].target_wall_offset_sec - wall_elapsed))
        if next_candidates:
            next_str = f"next_in={min(next_candidates):.2f}s"
        else:
            next_str = "next_in=none"

        cqs_str = " ".join(snap.cq_summary) if snap.cq_summary else "none"
        ts_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        note_str = f" | {note}" if note else ""
        done_count = _scheduled_done_count(snap)
        print(
            f"[{ts_str}] wall={format_duration_hms(wall_elapsed)} sim={sim_elapsed:.1f}s "
            f"| submitted={submitted_count}/{total_planned} queued={len(event_heap)} ({next_str}) "
            f"| workloads[adm={snap.workloads_admitted},pend={snap.workloads_pending},done={done_count}/{total_planned}] "
            f"| pods[run={snap.pods_running},pend={snap.pods_pending},succ={snap.pods_succeeded}] "
            f"| cqs: {cqs_str}{note_str}",
            flush=True,
        )

    def _poll_cluster(now_mono: float) -> ClusterStatusSnapshot:
        wall_elapsed = now_mono - t0_mono
        snap = query_cluster_status(
            mgr,
            workload_namespaces=bundle.workload_namespaces,
            include_kueue_workloads=True,
        )
        ever_finished_sched_wl_keys.update(snap.finished_workload_keys - initial_wl_keys)
        ever_finished_init_wl_keys.update(snap.finished_workload_keys & initial_wl_keys)
        _check_and_resubmit_evicted_initial_workloads(snap, wall_elapsed)
        return snap

    def _emit_status(now_mono: float, note: str = "") -> ClusterStatusSnapshot:
        snap = _poll_cluster(now_mono)
        _print_status_line(snap, now_mono, note=note)
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

            due_infra: list[ScheduledInfraEvent] = []
            while infra_heap and infra_heap[0].target_wall_offset_sec <= (time.monotonic() - t0_mono):
                due_infra.append(heapq.heappop(infra_heap))

            if due_infra:
                active_init_wls = [
                    iwl
                    for wl_k, iwl in initial_wl_by_key.items()
                    if wl_k not in resubmitted_initial_wls and wl_k not in ever_finished_init_wl_keys
                ]
                for ie in due_infra:
                    now_el = time.monotonic() - t0_mono
                    deferred, defer_reason = _execute_scheduled_infra_event(
                        mgr,
                        bundle,
                        ie,
                        now_el,
                        event_heap=event_heap,
                        cancelled_unsubmitted_keys=cancelled_unsubmitted_keys,
                        active_initial_wls=active_init_wls,
                        lq_to_cq=lq_to_cq,
                        cq_to_cohort=cq_to_cohort,
                        cq_to_flavors=cq_to_flavors,
                        submitted_non_cancelled=list(submitted_non_cancelled.values()),
                    )
                    if deferred:
                        if ie.seq not in deferred_infra_logged_seqs:
                            deferred_infra_logged_seqs.add(ie.seq)
                            target_lbl = f"{ie.namespace}/{ie.name}" if ie.namespace else ie.name
                            print(
                                f"  ~ [t={now_el:.2f}s wall / +{ie.raw_offset_sec:.2f}s] "
                                f"Deferring Deleted {ie.object_kind} '{target_lbl}' until workloads finish ({defer_reason})",
                                flush=True,
                            )
                        heapq.heappush(
                            infra_heap,
                            dc_replace(ie, target_wall_offset_sec=now_el + max(1.0, poll_interval_sec)),
                        )
                continue

            # Execute due workflow deletions BEFORE same-timestamp creations so delete-and-recreate works cleanly.
            due_deletes: list[ScheduledWorkflowDeletion] = []
            while delete_heap and delete_heap[0].target_wall_offset_sec <= (time.monotonic() - t0_mono):
                due_deletes.append(heapq.heappop(delete_heap))

            if due_deletes:
                for del_ev in due_deletes:
                    _execute_workflow_deletion(del_ev, time.monotonic() - t0_mono)
                continue

            due_batch: list[ScheduledWorkflowItem] = []
            while event_heap and event_heap[0].target_wall_offset_sec <= (time.monotonic() - t0_mono):
                cand_it = heapq.heappop(event_heap)
                c_key = (cand_it.namespace, cand_it.object_kind, cand_it.name, cand_it.incarnation_index)
                if c_key in cancelled_unsubmitted_keys:
                    cancelled_unsubmitted_keys.discard(c_key)
                    continue
                due_batch.append(cand_it)
                if len(due_batch) >= 200:
                    break

            if due_batch:
                # Preserve exact creationTime FIFO order across small batches: group by creation_epoch
                # and space distinct creation_epoch tiers across whole RFC3339 seconds when small (<= 12).
                due_batch.sort(key=lambda x: (round(x.target_wall_offset_sec, 6), x.creation_epoch, x.seq))
                distinct_c_epochs: list[float] = []
                for x in due_batch:
                    c_sec = round(float(x.creation_epoch), 3)
                    if not distinct_c_epochs or abs(c_sec - distinct_c_epochs[-1]) > 1e-3:
                        distinct_c_epochs.append(c_sec)

                can_space_fifo = (
                    1 < len(distinct_c_epochs) <= 12
                    and len(due_batch) <= 24
                    and (max_run_seconds is None or max_run_seconds >= len(distinct_c_epochs) * 2.5)
                )
                if can_space_fifo:
                    groups: list[list[ScheduledWorkflowItem]] = []
                    for x in due_batch:
                        if not groups or abs(float(x.creation_epoch) - float(groups[-1][0].creation_epoch)) > 1e-3:
                            groups.append([x])
                        else:
                            groups[-1].append(x)
                    for g_idx, grp in enumerate(groups):
                        mgr.apply_manifests_parallel(
                            [it_g.manifest for it_g in grp],
                            max_workers=min(24, max(1, len(grp))),
                        )
                        if g_idx + 1 < len(groups):
                            rem_sec = max(0.05, 1.02 - (time.time() % 1.0))
                            time.sleep(rem_sec)
                elif len(distinct_c_epochs) > 1 and len(due_batch) <= 24:
                    mgr.apply_manifests_parallel([item.manifest for item in due_batch], max_workers=1)
                else:
                    mgr.apply_manifests_parallel([item.manifest for item in due_batch], max_workers=24)

                now_off = time.monotonic() - t0_mono
                for item in due_batch:
                    it_applied = str((item.manifest.get("metadata") or {}).get("name") or item.name)
                    submitted_non_cancelled[(item.namespace, it_applied)] = (
                        item.namespace,
                        item.local_queue_name,
                        it_applied,
                    )
                    submitted_count += 1
                    total_pods_submitted += item.pod_count

                if len(due_batch) == 1:
                    it = due_batch[0]
                    src_tag = "exec-time.json" if it.indexed_from_execution_time else "fallback/imputed"
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
            due_status = now_mono >= next_status_mono
            need_fast_poll = (not event_heap and not delete_heap) or bool(initial_wl_keys)
            due_poll = need_fast_poll and now_mono >= next_poll_mono
            if due_status or due_poll:
                snap = _poll_cluster(now_mono)
                status_printed = False
                if due_status:
                    _print_status_line(snap, now_mono)
                    status_printed = True
                    while next_status_mono <= now_mono:
                        next_status_mono += status_interval_sec
                while next_poll_mono <= now_mono:
                    next_poll_mono += poll_interval_sec

                if not event_heap and not delete_heap:
                    if not wait_for_completion:
                        if not status_printed:
                            _print_status_line(snap, now_mono)
                        print("==> All scenario workflows submitted (--no-wait-for-completion).", flush=True)
                        break
                    done_count = _scheduled_done_count(snap)
                    init_done = _finite_initial_wls_done(snap)
                    if submitted_count > 0 and done_count >= submitted_count and init_done:
                        if not status_printed:
                            _print_status_line(snap, now_mono)
                        print(f"==> All {submitted_count} submitted scenario workflows completed!", flush=True)
                        break
                    if (
                        submitted_count == 0
                        and init_done
                        and (
                            finite_initial_wl_keys
                            or (not infra_heap and snap.pods_running == 0 and snap.workloads_admitted == 0)
                        )
                    ):
                        if not status_printed:
                            _print_status_line(snap, now_mono)
                        print("==> No pending workflows or running finite initial workloads remaining.", flush=True)
                        break

            now_mono = time.monotonic()
            wall_elapsed = now_mono - t0_mono
            wake_candidates = [next_status_mono - now_mono]
            if (not event_heap and not delete_heap) or initial_wl_keys:
                wake_candidates.append(next_poll_mono - now_mono)
            if event_heap:
                wake_candidates.append(event_heap[0].target_wall_offset_sec - wall_elapsed)
            if delete_heap:
                wake_candidates.append(delete_heap[0].target_wall_offset_sec - wall_elapsed)
            if infra_heap:
                wake_candidates.append(infra_heap[0].target_wall_offset_sec - wall_elapsed)
            if max_run_seconds is not None and max_run_seconds > 0:
                wake_candidates.append(max_run_seconds - wall_elapsed)
            sleep_sec = max(0.01, min(wake_candidates))
            time.sleep(min(sleep_sec, 1.0))

    finally:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    end_epoch = time.time()
    return start_epoch, end_epoch, submitted_count, total_pods_submitted
