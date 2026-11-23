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
from chamber.parsers import format_duration_hms, multiply_k8s_quantity_exact
from chamber.scenario import build_topology_assignment
from runner.loader import (
    InitialDeploymentBootstrap,
    InitialPodSpec,
    InitialWorkloadPodGroup,
    ScenarioBundle,
    ScheduledInfraEvent,
    ScheduledWorkflowDeletion,
    ScheduledWorkflowItem,
    ScheduledWorkflowUpdate,
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


def _stamp_initial_workloads_for_bootstrap(
    initial_workloads: list[InitialWorkloadPodGroup],
    *,
    now_dt: datetime | None = None,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Stamp back-dated admission seniority and `PodsReady=True` (`reason=Started`) onto t0 Workloads.

    Including `PodsReady=True` prevents Kueue's `waitForPodsReady` controller from immediately evicting
    pre-admitted t0 Workloads whose back-dated `Admitted` timestamp is older than `waitForPodsReady.timeout`.
    """
    ref_now = now_dt if now_dt is not None else datetime.now(timezone.utc)
    stamped_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for idx_w, w in enumerate(initial_workloads):
        age_offset = max(0.0, float(getattr(w, "original_age_sec", 0.0) or 0.0))
        if age_offset == 0.0 and len(initial_workloads) > 1:
            age_offset = float(len(initial_workloads) - idx_w)
        wl_ts = datetime.fromtimestamp(ref_now.timestamp() - age_offset, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        st_copy = json.loads(json.dumps(w.workload_status))
        conds = st_copy.setdefault("conditions", [])
        has_pods_ready = False
        for cond in conds:
            if isinstance(cond, dict):
                cond["lastTransitionTime"] = wl_ts
                if cond.get("type") == "PodsReady":
                    has_pods_ready = True
                    cond["status"] = "True"
                    cond.setdefault("reason", "Started")
        if not has_pods_ready:
            conds.append(
                {
                    "type": "PodsReady",
                    "status": "True",
                    "reason": "Started",
                    "message": "All pods reached readiness and the workload is running",
                    "lastTransitionTime": wl_ts,
                    "observedGeneration": 1,
                }
            )
        stamped_pairs.append((w.workload_manifest, st_copy))
    return stamped_pairs


def _pod_belongs_to_workflow(
    pod_doc: dict[str, Any],
    del_ev: ScheduledWorkflowDeletion,
    *,
    owned_job_names: set[str] | None = None,
    owned_rs_names: set[str] | None = None,
) -> bool:
    """Return True if `pod_doc` belongs strictly to `del_ev` via exact name, owner labels, or ownerReferences."""
    pm = pod_doc.get("metadata") or {}
    pn = str(pm.get("name") or "")
    if not pn:
        return False
    target = del_ev.applied_name
    if del_ev.manifest_kind == "Pod":
        return pn == target

    job_set = owned_job_names or set()
    rs_set = owned_rs_names or set()
    for ref in pm.get("ownerReferences") or []:
        if not isinstance(ref, dict):
            continue
        r_kind = str(ref.get("kind") or "")
        r_name = str(ref.get("name") or "")
        if r_name == target and (not del_ev.manifest_kind or r_kind in (del_ev.manifest_kind, del_ev.object_kind)):
            return True
        if r_kind == "Job" and r_name in job_set:
            return True
        if r_kind == "ReplicaSet" and r_name in rs_set:
            return True

    plbls = pm.get("labels") or {}
    if del_ev.manifest_kind == "Job":
        if plbls.get("batch.kubernetes.io/job-name") == target or plbls.get("job-name") == target:
            return True
    elif del_ev.manifest_kind == "JobSet":
        if plbls.get("jobset.sigs.k8s.io/jobset-name") == target:
            return True
        if plbls.get("batch.kubernetes.io/job-name") in job_set or plbls.get("job-name") in job_set:
            return True

    if del_ev.object_kind == "PodGroup" and plbls.get("kueue.x-k8s.io/pod-group-name") == target:
        return True
    if del_ev.object_kind == "LeaderWorkerSet" and plbls.get("leaderworkerset.sigs.k8s.io/name") == target:
        return True
    if del_ev.object_kind in ("RayCluster", "RayJob") and plbls.get("ray.io/cluster") == target:
        return True

    return False


def _build_single_pod_deployment_workload_status(
    idep: InitialDeploymentBootstrap,
    wl_doc: dict[str, Any],
    placement: str,
    wl_ts: str,
) -> dict[str, Any]:
    """Build pre-admitted Kueue Workload `/status` for one t0 Deployment replica Pod."""
    pod_sets = ((wl_doc.get("spec") or {}).get("podSets")) or []
    ps_doc = pod_sets[0] if pod_sets and isinstance(pod_sets[0], dict) else {}
    ps_name = str(ps_doc.get("name") or "main")

    reqs = dict(idep.per_pod_requests)
    tpl_containers = ((((ps_doc.get("template") or {}).get("spec")) or {}).get("containers")) or []
    if tpl_containers and isinstance(tpl_containers[0], dict):
        c_reqs = ((tpl_containers[0].get("resources") or {}).get("requests")) or {}
        if c_reqs:
            reqs = {str(k): str(v) for k, v in c_reqs.items() if str(k) != "ephemeral-storage"}

    cq_rgs = ((idep.cq_doc.get("spec") or {}).get("resourceGroups")) or []
    covered_to_flavors: dict[str, list[str]] = {}
    all_cq_flavors: list[str] = []
    for rg in cq_rgs:
        if not isinstance(rg, dict):
            continue
        flvs = [str(f.get("name") or "") for f in (rg.get("flavors") or []) if isinstance(f, dict) and f.get("name")]
        for fn in flvs:
            if fn not in all_cq_flavors:
                all_cq_flavors.append(fn)
        for cr in rg.get("coveredResources") or []:
            covered_to_flavors[str(cr)] = flvs

    node_doc = idep.nodes_by_name.get(placement) if placement else None

    def _matches_flv(n_d: dict[str, Any] | None, f_d: dict[str, Any] | None) -> bool:
        if not n_d or not f_d:
            return True
        req_lbls = ((f_d.get("spec") or {}).get("nodeLabels")) or {}
        if not req_lbls:
            return True
        n_lbls = ((n_d.get("metadata") or {}).get("labels")) or {}
        return all(str(n_lbls.get(k)) == str(v) for k, v in req_lbls.items())

    def _pick_flv(candidates: list[str]) -> str:
        for fn in candidates:
            if _matches_flv(node_doc, idep.flavors_by_name.get(fn)):
                return fn
        return candidates[0] if candidates else "default-flavor"

    scraped_flavors: dict[str, str] = {}
    for s_psa in idep.scraped_pod_set_assignments:
        for rk, fn in (s_psa.get("flavors") or {}).items():
            if str(fn) in idep.flavors_by_name:
                scraped_flavors[str(rk)] = str(fn)

    target_resources = [rk for rk in reqs if rk in covered_to_flavors]
    if not target_resources:
        target_resources = [rk for rk in reqs if rk != "ephemeral-storage"]

    flavors_map: dict[str, str] = {}
    resource_usage: dict[str, str] = {}
    assigned_tas_topology = ""
    for rk in sorted(target_resources):
        flv_name = scraped_flavors.get(rk) or _pick_flv(covered_to_flavors.get(rk) or all_cq_flavors)
        flavors_map[rk] = flv_name
        resource_usage[rk] = multiply_k8s_quantity_exact(reqs.get(rk, "0"), 1, rk)
        flv_doc = idep.flavors_by_name.get(flv_name) or {}
        topo_ref = str(((flv_doc.get("spec") or {}).get("topologyName")) or "")
        if topo_ref and not assigned_tas_topology:
            assigned_tas_topology = topo_ref

    psa: dict[str, Any] = {
        "name": ps_name,
        "count": 1,
        "flavors": flavors_map,
        "resourceUsage": resource_usage,
    }
    if assigned_tas_topology and assigned_tas_topology in idep.topologies_by_name and placement:
        topo_doc = idep.topologies_by_name[assigned_tas_topology]
        topo_levels = [
            str(lv.get("nodeLabel") or "")
            for lv in ((topo_doc.get("spec") or {}).get("levels") or [])
            if isinstance(lv, dict) and lv.get("nodeLabel")
        ]
        ta = build_topology_assignment([placement], topo_levels, idep.nodes_by_name)
        if ta is not None:
            psa["topologyAssignment"] = ta

    return {
        "admission": {
            "clusterQueue": idep.cluster_queue_name,
            "podSetAssignments": [psa],
        },
        "conditions": [
            {
                "type": "QuotaReserved",
                "status": "True",
                "reason": "QuotaReserved",
                "message": "Pre-admitted initial Deployment replica at scenario start",
                "lastTransitionTime": wl_ts,
                "observedGeneration": 1,
            },
            {
                "type": "Admitted",
                "status": "True",
                "reason": "Admitted",
                "message": "Pre-admitted initial Deployment replica at scenario start",
                "lastTransitionTime": wl_ts,
                "observedGeneration": 1,
            },
            {
                "type": "PodsReady",
                "status": "True",
                "reason": "Started",
                "message": "All pods reached readiness and the workload is running",
                "lastTransitionTime": wl_ts,
                "observedGeneration": 1,
            },
        ],
    }


def _bootstrap_initial_deployments(
    mgr: ChamberManager,
    initial_deployments: list[InitialDeploymentBootstrap],
) -> None:
    """Bootstrap t0 apps/v1 Deployments (full or partial readiness) before queue activation."""
    from concurrent.futures import ThreadPoolExecutor

    if not initial_deployments:
        return
    total_admitted = sum(d.admitted_replicas for d in initial_deployments)
    total_desired = sum(d.desired_replicas for d in initial_deployments)
    print(
        f"==> Bootstrapping {len(initial_deployments)} t0 Deployment(s) "
        f"({total_admitted}/{total_desired} replica(s) pre-admitted) before queue activation...",
        flush=True,
    )
    mgr.apply_manifests_parallel([d.manifest for d in initial_deployments], max_workers=16)

    for idep in initial_deployments[:8]:
        tas_tag = " [TAS]" if idep.is_tas else ""
        print(
            f"    [Initial Deployment] {idep.namespace}/{idep.applied_name} -> cq={idep.cluster_queue_name} "
            f"lq={idep.local_queue_name} ready={idep.admitted_replicas}/{idep.desired_replicas}{tas_tag}",
            flush=True,
        )
    if len(initial_deployments) > 8:
        print(f"    ... and {len(initial_deployments) - 8} more initial Deployments.", flush=True)

    active_ideps = [
        (idx_d, idep) for idx_d, idep in enumerate(initial_deployments) if idep.admitted_replicas > 0
    ]
    if not active_ideps:
        return

    namespaces = sorted({idep.namespace for _, idep in active_ideps})
    matched_by_idx: dict[int, list[tuple[str, dict[str, Any]]]] = {}
    wait_start = time.monotonic()
    wait_deadline = wait_start + 30.0
    while time.monotonic() < wait_deadline:
        rs_by_dep: dict[tuple[str, str], set[str]] = {}
        pods_by_rs: dict[tuple[str, str], list[str]] = {}
        wl_by_pod: dict[tuple[str, str], dict[str, Any]] = {}
        for ns_q in namespaces:
            st_rs, raw_rs = mgr.api_request("GET", f"/apis/apps/v1/namespaces/{ns_q}/replicasets")
            if st_rs == 200 and raw_rs:
                for rs_it in json.loads(raw_rs.decode("utf-8")).get("items") or []:
                    rsm = rs_it.get("metadata") or {}
                    rsn = str(rsm.get("name") or "")
                    for ref in rsm.get("ownerReferences") or []:
                        if isinstance(ref, dict) and ref.get("kind") == "Deployment" and ref.get("name") and rsn:
                            rs_by_dep.setdefault((ns_q, str(ref["name"])), set()).add(rsn)
            st_p, raw_p = mgr.api_request("GET", f"/api/v1/namespaces/{ns_q}/pods")
            if st_p == 200 and raw_p:
                for p_it in json.loads(raw_p.decode("utf-8")).get("items") or []:
                    pm = p_it.get("metadata") or {}
                    pn = str(pm.get("name") or "")
                    for ref in pm.get("ownerReferences") or []:
                        if isinstance(ref, dict) and ref.get("kind") == "ReplicaSet" and ref.get("name") and pn:
                            pods_by_rs.setdefault((ns_q, str(ref["name"])), []).append(pn)
            st_w, raw_w = mgr.api_request("GET", f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{ns_q}/workloads")
            if st_w == 200 and raw_w:
                for wl_it in json.loads(raw_w.decode("utf-8")).get("items") or []:
                    wm = wl_it.get("metadata") or {}
                    for ref in wm.get("ownerReferences") or []:
                        if isinstance(ref, dict) and ref.get("kind") == "Pod" and ref.get("name"):
                            wl_by_pod[(ns_q, str(ref["name"]))] = wl_it
                            break

        all_ready = True
        elapsed = time.monotonic() - wait_start
        for idx_d, idep in active_ideps:
            if idx_d in matched_by_idx:
                continue
            owned_pods: list[str] = []
            for rsn in rs_by_dep.get((idep.namespace, idep.applied_name), set()):
                owned_pods.extend(pods_by_rs.get((idep.namespace, rsn), []))
            owned_pods.sort()
            cur_pairs = [
                (pn, wl_by_pod[(idep.namespace, pn)])
                for pn in owned_pods
                if (idep.namespace, pn) in wl_by_pod
            ]
            all_desired_created = len(owned_pods) >= idep.desired_replicas and len(cur_pairs) >= idep.desired_replicas
            enough_admitted_after_grace = len(cur_pairs) >= idep.admitted_replicas and elapsed >= 3.0
            if all_desired_created or enough_admitted_after_grace:
                matched_by_idx[idx_d] = cur_pairs[: idep.admitted_replicas]
            else:
                all_ready = False

        if all_ready:
            break
        time.sleep(0.15)

    for idx_d, idep in active_ideps:
        if len(matched_by_idx.get(idx_d, [])) < idep.admitted_replicas:
            raise RuntimeError(
                f"Timed out waiting for Kueue Workloads of t0 Deployment '{idep.namespace}/{idep.applied_name}' "
                f"(got {len(matched_by_idx.get(idx_d, []))}/{idep.admitted_replicas})"
            )

    ref_now = datetime.now(timezone.utc)
    patch_tasks: list[tuple[str, str, dict[str, Any]]] = []
    admitted_pod_keys: list[tuple[str, str]] = []
    for idx_d, idep in active_ideps:
        age_offset = max(0.0, float(idep.original_age_sec or 0.0))
        if age_offset == 0.0 and len(initial_deployments) > 1:
            age_offset = float(len(initial_deployments) - idx_d)
        wl_ts = datetime.fromtimestamp(ref_now.timestamp() - age_offset, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        for idx_p, (pod_name, wl_doc) in enumerate(matched_by_idx[idx_d]):
            wl_name = str((wl_doc.get("metadata") or {}).get("name") or "")
            plc = idep.placements[idx_p] if idx_p < len(idep.placements) else ""
            st_doc = _build_single_pod_deployment_workload_status(idep, wl_doc, plc, wl_ts)
            patch_tasks.append((idep.namespace, wl_name, st_doc))
            admitted_pod_keys.append((idep.namespace, pod_name))

    def _patch_one(task: tuple[str, str, dict[str, Any]]) -> None:
        ns_t, wl_n_t, st_t = task
        mgr.api_request(
            "PATCH",
            f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{ns_t}/workloads/{wl_n_t}/status",
            body=json.dumps({"status": st_t}).encode("utf-8"),
            headers={"Content-Type": "application/merge-patch+json"},
        )

    with ThreadPoolExecutor(max_workers=min(16, max(1, len(patch_tasks)))) as pool:
        list(pool.map(_patch_one, patch_tasks))

    mgr.wait_for_pods_running(admitted_pod_keys)

    # Ensure PodsReady=True (Started) and back-dated Admitted seniority remain intact once pods are Ready.
    with ThreadPoolExecutor(max_workers=min(16, max(1, len(patch_tasks)))) as pool:
        list(pool.map(_patch_one, patch_tasks))


def apply_scenario_queues_and_wait(
    mgr: ChamberManager,
    prereq_and_queue_docs: list[dict[str, Any]],
    timeout_sec: float = 60.0,
    *,
    initial_workloads: list[InitialWorkloadPodGroup] | None = None,
    initial_deployments: list[InitialDeploymentBootstrap] | None = None,
    initial_pods: list[InitialPodSpec] | None = None,
) -> None:
    """Apply prerequisites, optionally bootstrap t0 PodGroup/Deployment Workloads & Pods, then activate ClusterQueues/LocalQueues."""
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

    if initial_deployments:
        _bootstrap_initial_deployments(mgr, initial_deployments)

    # Bootstrap pre-admitted t0 Kueue PodGroup Workloads + member Pods + non-Kueue infra-pods BEFORE
    # ClusterQueues/LocalQueues exist so Cache.AddClusterQueue loads quota, FairSharing, and TAS usage atomically.
    if initial_workloads:
        wl_pod_total = sum(len(w.pods) for w in initial_workloads)
        print(
            f"==> Bootstrapping {len(initial_workloads)} pre-admitted t0 Kueue PodGroup Workload(s) "
            f"({wl_pod_total} pod(s)) before queue activation...",
            flush=True,
        )
        stamped_pairs = _stamp_initial_workloads_for_bootstrap(initial_workloads)
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
        if bundle.initial_deployments:
            _bootstrap_initial_deployments(mgr, bundle.initial_deployments)
        if bundle.initial_workloads:
            mgr.apply_pre_admitted_workloads_parallel(
                _stamp_initial_workloads_for_bootstrap(bundle.initial_workloads)
            )
            init_wl_pods = [p for w in bundle.initial_workloads for p in w.pods]
            mgr.apply_manifests_parallel([p.manifest for p in init_wl_pods], max_workers=16)
            mgr.wait_for_pods_running(
                [(p.namespace, str((p.manifest.get("metadata") or {}).get("name") or p.name)) for p in init_wl_pods]
            )
        if bundle.initial_pods:
            print(
                f"==> Placing {len(bundle.initial_pods)} initial running Pod(s) into Kueue-excluded "
                f"namespace '{bundle.infra_pods_namespace}' onto their nodes...",
                flush=True,
            )
            mgr.apply_manifests_parallel([p.manifest for p in bundle.initial_pods], max_workers=16)
            mgr.wait_for_pods_running(
                [(p.namespace, str((p.manifest.get("metadata") or {}).get("name") or p.name)) for p in bundle.initial_pods]
            )

    mgr._retired_serving_workloads = {}
    event_heap: list[ScheduledWorkflowItem] = list(bundle.scheduled_workflows)
    heapq.heapify(event_heap)
    update_heap: list[ScheduledWorkflowUpdate] = list(bundle.scheduled_workflow_updates)
    heapq.heapify(update_heap)
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
    submitted_non_cancelled: dict[tuple[str, str], tuple[str, str, str]] = {
        (idep.namespace, idep.applied_name): (idep.namespace, idep.local_queue_name, idep.applied_name)
        for idep in bundle.initial_deployments
        if idep.desired_replicas > 0
    }
    deferred_infra_logged_seqs: set[int] = set()

    known_deploy_names_by_ns: dict[str, set[str]] = {}
    active_deployment_replicas: dict[tuple[str, str], int] = {}
    expected_clean_retirements: dict[tuple[str, str], int] = {}
    live_serving_wls: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
    serving_wl_keys: set[tuple[str, str]] = set()
    scale_zero_drain_deadline: float | None = None

    for idep in bundle.initial_deployments:
        known_deploy_names_by_ns.setdefault(idep.namespace, set()).add(idep.applied_name)
        active_deployment_replicas[(idep.namespace, idep.applied_name)] = idep.desired_replicas
    for sw_d in bundle.scheduled_workflows:
        if sw_d.object_kind == "Deployment":
            ap_n = str((sw_d.manifest.get("metadata") or {}).get("name") or sw_d.name)
            known_deploy_names_by_ns.setdefault(sw_d.namespace, set()).add(ap_n)
    for su_d in bundle.scheduled_workflow_updates:
        if su_d.object_kind == "Deployment":
            known_deploy_names_by_ns.setdefault(su_d.namespace, set()).add(su_d.applied_name)

    def _match_deploy_for_serving_wl(ns_val: str, wl_doc: dict[str, Any]) -> str:
        owners = (wl_doc.get("metadata") or {}).get("ownerReferences") or []
        pod_ref = ""
        for o in owners:
            if isinstance(o, dict) and o.get("kind") == "Pod" and o.get("name"):
                pod_ref = str(o["name"])
                break
        if not pod_ref:
            return ""
        best = ""
        for cand_dep in known_deploy_names_by_ns.get(ns_val, set()):
            if pod_ref.startswith(f"{cand_dep}-") and len(cand_dep) > len(best):
                best = cand_dep
        return best

    def _classify_serving_workload_doc(ns_val: str, wl_doc: dict[str, Any]) -> tuple[bool, str]:
        dep_n = _match_deploy_for_serving_wl(ns_val, wl_doc)
        if dep_n:
            return True, dep_n
        wm_ann = ((wl_doc.get("metadata") or {}).get("annotations")) or {}
        if (
            str(wm_ann.get("kueue-optimizer.gke.io/long-running-pod") or "").lower() == "true"
            or str(wm_ann.get("kueue.x-k8s.io/pod-suspending-parent") or "").lower() == "deployment"
        ):
            return True, ""
        for ps in ((wl_doc.get("spec") or {}).get("podSets")) or []:
            if not isinstance(ps, dict):
                continue
            ann = (((ps.get("template") or {}).get("metadata")) or {}).get("annotations") or {}
            if (
                str(ann.get("kueue-optimizer.gke.io/long-running-pod") or "").lower() == "true"
                or str(ann.get("kueue.x-k8s.io/pod-suspending-parent") or "").lower() == "deployment"
            ):
                return True, ""
        return False, ""

    def _non_serving_finished_keys(keys: set[tuple[str, str]]) -> set[tuple[str, str]]:
        if not known_deploy_names_by_ns and not serving_wl_keys:
            return keys
        out: set[tuple[str, str]] = set()
        for ns_k, wn_k in keys:
            if (ns_k, wn_k) in serving_wl_keys:
                continue
            if wn_k.startswith("pod-") and any(
                wn_k.startswith(f"pod-{d}-") for d in known_deploy_names_by_ns.get(ns_k, set())
            ):
                serving_wl_keys.add((ns_k, wn_k))
                continue
            out.add((ns_k, wn_k))
        return out

    def _sync_serving_workloads(mark_deleted_deploy: tuple[str, str] | None = None) -> None:
        if not known_deploy_names_by_ns:
            return
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cur_wls: dict[tuple[str, str], dict[str, Any]] = {}
        owner_gone_ts: dict[tuple[str, str], str] = {}
        query_all_ok = True
        for ns_q in bundle.workload_namespaces:
            if ns_q == bundle.infra_pods_namespace:
                continue
            st_w, raw_w = mgr.api_request("GET", f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{ns_q}/workloads")
            if st_w != 200 or not raw_w:
                query_all_ok = False
                continue
            for wl_it in json.loads(raw_w.decode("utf-8")).get("items") or []:
                if not isinstance(wl_it, dict):
                    continue
                wm = wl_it.get("metadata") or {}
                wn = str(wm.get("name") or "")
                actual_ns = str(wm.get("namespace") or ns_q)
                if not wn:
                    continue
                is_srv, dep_n = _classify_serving_workload_doc(actual_ns, wl_it)
                if not is_srv:
                    continue
                wk = (actual_ns, wn)
                serving_wl_keys.add(wk)
                if wk in mgr._retired_serving_workloads:
                    continue
                conds = ((wl_it.get("status") or {}).get("conditions")) or []
                owner_gone_c = next(
                    (
                        c
                        for c in conds
                        if isinstance(c, dict)
                        and c.get("type") == "Finished"
                        and c.get("status") == "True"
                        and c.get("reason") == "OwnerNotFound"
                    ),
                    None,
                )
                was_ev_or_pre = any(
                    isinstance(c, dict)
                    and c.get("type") in ("Evicted", "Preempted")
                    and c.get("status") == "True"
                    for c in conds
                )
                has_adm_hist = bool(((wl_it.get("status") or {}).get("admission") or {}).get("clusterQueue")) or any(
                    isinstance(c, dict)
                    and c.get("type") == "Admitted"
                    and (c.get("status") == "True" or bool(c.get("lastTransitionTime")))
                    for c in conds
                )
                if owner_gone_c is not None and not was_ev_or_pre:
                    owner_gone_ts[wk] = str(owner_gone_c.get("lastTransitionTime") or now_iso)
                    if wk not in live_serving_wls and has_adm_hist:
                        live_serving_wls[wk] = (dep_n, wl_it)
                    continue

                cur_wls[wk] = wl_it
                if has_adm_hist or was_ev_or_pre:
                    if wk in live_serving_wls:
                        prev_dep, prev_doc = live_serving_wls[wk]
                        merged_doc = json.loads(json.dumps(wl_it))
                        prev_adm = (prev_doc.get("status") or {}).get("admission")
                        if prev_adm and not ((merged_doc.get("status") or {}).get("admission")):
                            merged_doc.setdefault("status", {})["admission"] = prev_adm
                        live_serving_wls[wk] = (dep_n or prev_dep, merged_doc)
                    else:
                        live_serving_wls[wk] = (dep_n, wl_it)

        to_retire: list[tuple[str, str]] = []
        for wk, (dep_n, _doc) in list(live_serving_wls.items()):
            if mark_deleted_deploy is not None and (wk[0], dep_n) == mark_deleted_deploy:
                to_retire.append(wk)
            elif wk in owner_gone_ts:
                to_retire.append(wk)
            elif query_all_ok and wk not in cur_wls:
                to_retire.append(wk)

        for wk in sorted(set(to_retire)):
            dep_n, last_doc = live_serving_wls.pop(wk, ("", {}))
            if not last_doc:
                continue
            retire_iso = owner_gone_ts.get(wk, now_iso)
            doc_cp = json.loads(json.dumps(last_doc))
            st_blk = doc_cp.setdefault("status", {})
            conds = st_blk.setdefault("conditions", [])
            was_evicted_or_preempted = any(
                isinstance(c, dict)
                and c.get("type") in ("Evicted", "Preempted")
                and c.get("status") == "True"
                for c in conds
            )
            dep_key = (wk[0], dep_n)
            if was_evicted_or_preempted:
                mgr._retired_serving_workloads[wk] = doc_cp
            elif (
                mark_deleted_deploy == dep_key
                or expected_clean_retirements.get(dep_key, 0) > 0
            ):
                if mark_deleted_deploy != dep_key and expected_clean_retirements.get(dep_key, 0) > 0:
                    expected_clean_retirements[dep_key] -= 1
                conds = [
                    c for c in conds if not (isinstance(c, dict) and c.get("type") == "Finished")
                ]
                conds.append(
                    {
                        "type": "Finished",
                        "status": "True",
                        "reason": "Succeeded",
                        "message": "Serving Deployment replica cleanly retired",
                        "lastTransitionTime": retire_iso,
                        "observedGeneration": 1,
                    }
                )
                st_blk["conditions"] = conds
                mgr._retired_serving_workloads[wk] = doc_cp
            else:
                for c in conds:
                    if isinstance(c, dict) and c.get("type") == "Admitted":
                        c["status"] = "False"
                        c["reason"] = "Unsuspended"
                        c["lastTransitionTime"] = retire_iso
                conds.append(
                    {
                        "type": "Evicted",
                        "status": "True",
                        "reason": "Preempted",
                        "message": "Serving Deployment replica evicted prior to garbage collection",
                        "lastTransitionTime": retire_iso,
                        "observedGeneration": 1,
                    }
                )
                mgr._retired_serving_workloads[wk] = doc_cp

    total_planned = len(bundle.scheduled_workflows)
    submitted_count = 0
    finite_batch_submitted = 0
    finite_batch_cancelled = 0
    total_pods_submitted = (
        bundle.initial_workload_pods_count
        + sum(max(0, d.desired_replicas - d.admitted_replicas) for d in bundle.initial_deployments)
        + len(bundle.initial_pods)
    )
    default_serving_settle_wall_sec = max(
        5.0,
        (bundle.t_end_epoch - bundle.effective_start_epoch) / max(1e-6, speedup) + 5.0,
    )

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
        nonlocal total_planned, submitted_count, finite_batch_submitted, total_pods_submitted
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
            if it.object_kind != "Deployment":
                finite_batch_submitted += 1
            total_pods_submitted += it.pod_count
            print(
                f"  ~ [t={wall_elapsed:.2f}s wall] Initial t0 PodGroup Workload '{iwl.namespace}/{iwl.workload_name}' "
                f"was preempted/evicted -> dynamically re-submitted {it.object_kind} '{it.namespace}/{it.name}' "
                f"(queue={it.local_queue_name or '-'}, pods={it.pod_count}, pod_exec={it.compressed_duration_sec:.2f}s)",
                flush=True,
            )

    def _scheduled_done_count(snap: ClusterStatusSnapshot) -> int:
        ever_finished_sched_wl_keys.update(_non_serving_finished_keys(snap.finished_workload_keys - initial_wl_keys))
        wl_done = len(ever_finished_sched_wl_keys) + cancelled_workflow_count
        if recreated_names or known_deploy_names_by_ns:
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

        owned_job_names: set[str] = set()
        owned_rs_names: set[str] = set()
        if del_ev.manifest_kind == "JobSet":
            st_j, raw_j = mgr.api_request("GET", f"/apis/batch/v1/namespaces/{del_ev.namespace}/jobs")
            if st_j == 200 and raw_j:
                try:
                    for j_it in json.loads(raw_j.decode("utf-8")).get("items") or []:
                        jm = j_it.get("metadata") or {}
                        jn = str(jm.get("name") or "")
                        if not jn:
                            continue
                        jlbls = jm.get("labels") or {}
                        j_owners = jm.get("ownerReferences") or []
                        if jlbls.get("jobset.sigs.k8s.io/jobset-name") == del_ev.applied_name or any(
                            isinstance(ref, dict)
                            and ref.get("kind") == "JobSet"
                            and ref.get("name") == del_ev.applied_name
                            for ref in j_owners
                        ):
                            owned_job_names.add(jn)
                except Exception:
                    pass
        elif del_ev.manifest_kind == "Deployment":
            st_rs, raw_rs = mgr.api_request("GET", f"/apis/apps/v1/namespaces/{del_ev.namespace}/replicasets")
            if st_rs == 200 and raw_rs:
                try:
                    for rs_it in json.loads(raw_rs.decode("utf-8")).get("items") or []:
                        rsm = rs_it.get("metadata") or {}
                        rsn = str(rsm.get("name") or "")
                        if not rsn:
                            continue
                        if any(
                            isinstance(ref, dict)
                            and ref.get("kind") == "Deployment"
                            and ref.get("name") == del_ev.applied_name
                            for ref in (rsm.get("ownerReferences") or [])
                        ):
                            owned_rs_names.add(rsn)
                except Exception:
                    pass

        mgr.api_request(
            "DELETE",
            del_path,
            body=del_opts,
            headers={"Content-Type": "application/json"},
        )
        for jn in sorted(owned_job_names):
            mgr.api_request(
                "DELETE",
                f"/apis/batch/v1/namespaces/{del_ev.namespace}/jobs/{jn}",
                body=del_opts,
                headers={"Content-Type": "application/json"},
            )
        for rsn in sorted(owned_rs_names):
            mgr.api_request(
                "DELETE",
                f"/apis/apps/v1/namespaces/{del_ev.namespace}/replicasets/{rsn}",
                body=del_opts,
                headers={"Content-Type": "application/json"},
            )

        # Remove pods strictly owned by this workflow so KWOK pods release nodes immediately without touching sibling jobs.
        st_p, raw_p = mgr.api_request("GET", f"/api/v1/namespaces/{del_ev.namespace}/pods")
        if st_p == 200 and raw_p:
            for p_it in json.loads(raw_p.decode("utf-8")).get("items") or []:
                pm = p_it.get("metadata") or {}
                pn = str(pm.get("name") or "")
                if pn and _pod_belongs_to_workflow(
                    p_it,
                    del_ev,
                    owned_job_names=owned_job_names,
                    owned_rs_names=owned_rs_names,
                ):
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
        nonlocal cancelled_workflow_count, finite_batch_cancelled, total_planned
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
        ever_finished_sched_wl_keys.update(_non_serving_finished_keys(pre_snap.finished_workload_keys - initial_wl_keys))
        ever_finished_init_wl_keys.update(pre_snap.finished_workload_keys & initial_wl_keys)

        unsubmitted_match = any(
            w.namespace == del_ev.namespace
            and w.object_kind == del_ev.object_kind
            and w.name == del_ev.name
            and w.incarnation_index == del_ev.incarnation_index
            for w in event_heap
        )
        if unsubmitted_match:
            cancelled_unsubmitted_keys.add(
                (del_ev.namespace, del_ev.object_kind, del_ev.name, del_ev.incarnation_index)
            )
            total_planned = max(0, total_planned - 1)
            print(
                f"  - [t={wall_elapsed:.2f}s wall / +{del_ev.raw_offset_sec:.2f}s] "
                f"Cancelled pending unsubmitted {del_ev.object_kind} '{del_ev.namespace}/{del_ev.name}' "
                f"(incarnation={del_ev.incarnation_index}, reason={del_ev.delete_reason})",
                flush=True,
            )
            return

        # Check whether this submitted incarnation already reached Finished=True in the chamber prior to deletion:
        # - If already Finished=True, record its Workload key in ever_finished_sched_wl_keys and do NOT also
        #   increment cancelled_workflow_count (avoiding double-counting).
        # - If NOT Finished=True (for BOTH `cancelled` and `before_recreate` deletions that cut short a still-running
        #   or still-pending incarnation), increment cancelled_workflow_count so `done` never stalls below total_planned.
        already_finished_in_chamber = False
        st_wls, raw_wls = mgr.api_request(
            "GET",
            f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{del_ev.namespace}/workloads",
        )
        if st_wls == 200 and raw_wls:
            try:
                for wl_it in json.loads(raw_wls.decode("utf-8")).get("items") or []:
                    w_m = wl_it.get("metadata") or {}
                    wl_n = str(w_m.get("name") or "")
                    owners = [
                        str(o.get("name") or "")
                        for o in (w_m.get("ownerReferences") or [])
                        if isinstance(o, dict)
                    ]
                    if del_ev.applied_name in owners or wl_n == del_ev.applied_name:
                        conds = ((wl_it.get("status") or {}).get("conditions")) or []
                        if any(
                            isinstance(c, dict)
                            and c.get("type") == "Finished"
                            and c.get("status") == "True"
                            for c in conds
                        ):
                            already_finished_in_chamber = True
                            if wl_n and (del_ev.namespace, wl_n) not in initial_wl_keys:
                                ever_finished_sched_wl_keys.add((del_ev.namespace, wl_n))
                            break
            except Exception:
                pass

        if del_ev.object_kind == "Deployment" or del_ev.manifest_kind == "Deployment":
            _sync_serving_workloads(mark_deleted_deploy=(del_ev.namespace, del_ev.applied_name))
            active_deployment_replicas.pop((del_ev.namespace, del_ev.applied_name), None)
            expected_clean_retirements.pop((del_ev.namespace, del_ev.applied_name), None)

        submitted_non_cancelled.pop((del_ev.namespace, del_ev.applied_name), None)
        has_future_same_name = any(
            w.namespace == del_ev.namespace
            and str((w.manifest.get("metadata") or {}).get("name") or w.name) == del_ev.applied_name
            for w in event_heap
        )
        _delete_workflow_and_children(del_ev, wait_gone=(del_ev.delete_reason == "before_recreate" or has_future_same_name))
        if not already_finished_in_chamber:
            cancelled_workflow_count += 1
            if del_ev.object_kind != "Deployment" and del_ev.manifest_kind != "Deployment":
                finite_batch_cancelled += 1
        print(
            f"  - [t={wall_elapsed:.2f}s wall / +{del_ev.raw_offset_sec:.2f}s] "
            f"Deleted {del_ev.object_kind} '{del_ev.namespace}/{del_ev.name}' "
            f"(incarnation={del_ev.incarnation_index}, reason={del_ev.delete_reason})",
            flush=True,
        )

    def _execute_workflow_update(upd: ScheduledWorkflowUpdate, wall_elapsed: float) -> None:
        nonlocal total_pods_submitted
        _sync_serving_workloads()
        dep_key = (upd.namespace, upd.applied_name)
        known_deploy_names_by_ns.setdefault(upd.namespace, set()).add(upd.applied_name)
        prev_rep = active_deployment_replicas.get(dep_key, upd.replicas)
        if "Upgrade" in upd.update_types:
            expected_clean_retirements[dep_key] = expected_clean_retirements.get(dep_key, 0) + max(prev_rep, upd.replicas)
        if upd.replicas < prev_rep:
            expected_clean_retirements[dep_key] = expected_clean_retirements.get(dep_key, 0) + (prev_rep - upd.replicas)
        elif upd.replicas > prev_rep:
            total_pods_submitted += upd.replicas - prev_rep
        active_deployment_replicas[dep_key] = upd.replicas
        if upd.replicas > 0:
            submitted_non_cancelled[dep_key] = (upd.namespace, upd.local_queue_name, upd.applied_name)
        else:
            submitted_non_cancelled.pop(dep_key, None)

        mgr.ssa_apply_doc(upd.manifest)
        types_desc = ",".join(upd.update_types) if upd.update_types else "SpecUpdate"
        tpl_desc = f", tpl={upd.template_hash}" if upd.template_hash else ""
        print(
            f"  ~ [t={wall_elapsed:.2f}s wall / +{upd.raw_offset_sec:.2f}s (t0+{upd.raw_offset_from_t0_sec:.2f}s)] "
            f"Updated {upd.object_kind} '{upd.namespace}/{upd.name}' "
            f"(types=[{types_desc}], replicas={prev_rep}->{upd.replicas}, paused={upd.paused}{tpl_desc})",
            flush=True,
        )

    def _print_status_line(snap: ClusterStatusSnapshot, now_mono: float, note: str = "") -> None:
        wall_elapsed = now_mono - t0_mono
        sim_elapsed = wall_elapsed * speedup
        next_candidates: list[float] = []
        if event_heap:
            next_candidates.append(max(0.0, event_heap[0].target_wall_offset_sec - wall_elapsed))
        if update_heap:
            next_candidates.append(max(0.0, update_heap[0].target_wall_offset_sec - wall_elapsed))
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
        _sync_serving_workloads()
        snap = query_cluster_status(
            mgr,
            workload_namespaces=bundle.workload_namespaces,
            include_kueue_workloads=True,
        )
        ever_finished_sched_wl_keys.update(_non_serving_finished_keys(snap.finished_workload_keys - initial_wl_keys))
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
                    if item.object_kind == "Deployment":
                        it_rep = int((item.manifest.get("spec") or {}).get("replicas", 1))
                        known_deploy_names_by_ns.setdefault(item.namespace, set()).add(it_applied)
                        active_deployment_replicas[(item.namespace, it_applied)] = it_rep
                        if it_rep > 0:
                            submitted_non_cancelled[(item.namespace, it_applied)] = (
                                item.namespace,
                                item.local_queue_name,
                                it_applied,
                            )
                    else:
                        finite_batch_submitted += 1
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

            due_updates: list[ScheduledWorkflowUpdate] = []
            while update_heap and update_heap[0].target_wall_offset_sec <= (time.monotonic() - t0_mono):
                due_updates.append(heapq.heappop(update_heap))

            if due_updates:
                for upd_ev in due_updates:
                    _execute_workflow_update(upd_ev, time.monotonic() - t0_mono)
                continue

            now_mono = time.monotonic()
            due_status = now_mono >= next_status_mono
            need_fast_poll = (
                (not event_heap and not update_heap and not delete_heap)
                or bool(initial_wl_keys)
                or bool(known_deploy_names_by_ns)
            )
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

                if not event_heap and not update_heap and not delete_heap:
                    if not wait_for_completion:
                        if not status_printed:
                            _print_status_line(snap, now_mono)
                        print("==> All scenario workflows submitted (--no-wait-for-completion).", flush=True)
                        break
                    done_count = _scheduled_done_count(snap)
                    finite_batch_done = len(ever_finished_sched_wl_keys) + finite_batch_cancelled
                    init_done = _finite_initial_wls_done(snap)
                    active_has_serving = any(r > 0 for r in active_deployment_replicas.values())
                    if active_has_serving:
                        scale_zero_drain_deadline = None
                    elif live_serving_wls and scale_zero_drain_deadline is None:
                        scale_zero_drain_deadline = now_mono + 4.0
                    serving_drained = (
                        not live_serving_wls
                        or scale_zero_drain_deadline is None
                        or now_mono >= scale_zero_drain_deadline
                    )
                    if not active_has_serving and serving_drained:
                        all_batch_done = (
                            (done_count >= submitted_count)
                            if not known_deploy_names_by_ns
                            else (finite_batch_done >= finite_batch_submitted)
                        )
                        if submitted_count > 0 and all_batch_done and init_done:
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
                    elif (
                        max_run_seconds is None
                        and not infra_heap
                        and finite_batch_done >= finite_batch_submitted
                        and init_done
                        and (now_mono - t0_mono) >= default_serving_settle_wall_sec
                    ):
                        if not status_printed:
                            _print_status_line(snap, now_mono)
                        print(
                            f"==> All scheduled events replayed and serving Deployments settled "
                            f"({default_serving_settle_wall_sec:.1f}s window reached).",
                            flush=True,
                        )
                        break

            now_mono = time.monotonic()
            wall_elapsed = now_mono - t0_mono
            wake_candidates = [next_status_mono - now_mono]
            if (not event_heap and not update_heap and not delete_heap) or initial_wl_keys or known_deploy_names_by_ns:
                wake_candidates.append(next_poll_mono - now_mono)
            if event_heap:
                wake_candidates.append(event_heap[0].target_wall_offset_sec - wall_elapsed)
            if update_heap:
                wake_candidates.append(update_heap[0].target_wall_offset_sec - wall_elapsed)
            if delete_heap:
                wake_candidates.append(delete_heap[0].target_wall_offset_sec - wall_elapsed)
            if infra_heap:
                wake_candidates.append(infra_heap[0].target_wall_offset_sec - wall_elapsed)
            if max_run_seconds is not None and max_run_seconds > 0:
                wake_candidates.append(max_run_seconds - wall_elapsed)
            sleep_sec = max(0.01, min(wake_candidates))
            time.sleep(min(sleep_sec, 1.0))

    finally:
        try:
            _sync_serving_workloads()
        except Exception:
            pass
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    end_epoch = time.time()
    return start_epoch, end_epoch, submitted_count, total_pods_submitted
