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

from collections.abc import Iterator
from dataclasses import dataclass, replace as dc_replace
from datetime import datetime, timezone
import heapq
import json
import re
import signal
import time
from typing import Any
import urllib.parse

from chamber.constants import validate_dns_label
from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot, query_cluster_status
from chamber.parsers import (
    format_duration_hms,
    multiply_k8s_quantity_exact,
    parse_k8s_quantity,
    parse_rfc3339_epoch,
)
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


@dataclass(frozen=True)
class ReplayResult:
    """Structured outcome of `run_scenario_replay`, unpackable as `(start_epoch, end_epoch, submitted_count, total_pods)`."""

    start_epoch: float
    end_epoch: float
    submitted_count: int
    total_pods: int
    stop_reason: str = "completed"
    signal_num: int | None = None

    @property
    def interrupted(self) -> bool:
        return self.stop_reason == "interrupted"

    @property
    def exit_code(self) -> int:
        if not self.interrupted:
            return 0
        if self.signal_num is not None and self.signal_num > 0:
            return 128 + self.signal_num
        return 130

    def __iter__(self) -> Iterator[Any]:
        yield self.start_epoch
        yield self.end_epoch
        yield self.submitted_count
        yield self.total_pods

    def __len__(self) -> int:
        return 4

    def __getitem__(self, idx: int | slice) -> Any:
        return (self.start_epoch, self.end_epoch, self.submitted_count, self.total_pods)[idx]


def _checked_api_request(
    mgr: ChamberManager,
    method: str,
    path: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    content_type: str | None = None,
    ignore_not_found: bool = False,
    context: str = "",
) -> tuple[int, bytes]:
    """Execute an API request and raise RuntimeError on unexpected non-2xx responses."""
    kwargs: dict[str, Any] = {}
    if body is not None:
        kwargs["body"] = body
    if headers is not None:
        kwargs["headers"] = headers
    if content_type is not None:
        kwargs["content_type"] = content_type
    st_code, raw_resp = mgr.api_request(method, path, **kwargs)
    if 200 <= st_code < 300 or (ignore_not_found and st_code == 404):
        return st_code, raw_resp
    resp_txt = raw_resp.decode("utf-8", errors="replace").strip()[:500] if raw_resp else ""
    ctx_prefix = f"{context}: " if context else ""
    raise RuntimeError(f"{ctx_prefix}{method} {path} failed with HTTP {st_code}: {resp_txt}")


@dataclass
class _SubmittedBatchRecord:
    """Tracks a submitted finite batch workflow for vanished-before-first-poll reconciliation."""

    api_version: str
    kind: str
    namespace: str
    applied_name: str
    submitted_mono: float
    pre_existing_wl_keys: frozenset[tuple[str, str]] = frozenset()
    last_probe_mono: float = 0.0


def _workload_name_matches_owner(wl_name: str, kind: str, owner_name: str) -> bool:
    """Return True if Kueue Workload name `wl_name` matches owner `(kind, owner_name)`.

    Requires Kueue's exact 5-character lowercase hex hash suffix (`-[0-9a-f]{5}`) after the
    owner prefix (including the 57-character truncation branch for long names) so sibling
    workflows sharing a hyphenated prefix (e.g., `train` vs. `train-eval`) never collide.
    """
    if not wl_name or not owner_name:
        return False
    if wl_name == owner_name:
        return True
    kind_prefixes = {
        "JobSet": ("jobset",),
        "Job": ("job",),
        "Pod": ("pod",),
        "PodGroup": ("pod",),
        "LeaderWorkerSet": ("leaderworkerset", "lws"),
        "RayJob": ("rayjob",),
        "RayCluster": ("raycluster",),
    }.get(kind, (kind.lower(),) if kind else ())
    for pref in kind_prefixes:
        base = f"{pref}-{owner_name}"
        if wl_name == base:
            return True
        if len(base) <= 57:
            if re.fullmatch(rf"{re.escape(base)}-[0-9a-f]{{5}}", wl_name):
                return True
        else:
            trunc = base[:57]
            if re.fullmatch(rf"{re.escape(trunc)}-[0-9a-f]{{5}}", wl_name):
                return True
            trunc_stripped = trunc.rstrip("-")
            if trunc_stripped and trunc_stripped != trunc and re.fullmatch(
                rf"{re.escape(trunc_stripped)}-[0-9a-f]{{5}}", wl_name
            ):
                return True
    return False


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
    st, raw = _checked_api_request(
        mgr,
        "GET",
        f"/api/v1/pods?fieldSelector={q_sel}",
        context=f"List pods bound to Node '{node_name}'",
    )
    if not raw:
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
        p_st, _ = _checked_api_request(
            mgr,
            "PATCH",
            f"/api/v1/namespaces/{ns}/pods/{p_name}/status",
            body=json.dumps(status_patch).encode("utf-8"),
            headers={"Content-Type": "application/merge-patch+json"},
            ignore_not_found=True,
            context=f"Fail bound Pod '{ns}/{p_name}' on Node '{node_name}'",
        )
        if 200 <= p_st < 300:
            failed_count += 1
    return failed_count


def _patch_node_ready_condition(
    mgr: ChamberManager,
    node_name: str,
    ready_status: str,
    reason: str,
    message: str,
    extra_status: dict[str, Any] | None = None,
    *,
    ignore_not_found: bool = False,
) -> bool:
    """Patch Node /status Ready condition while preserving allocatable/capacity.

    Returns False only when `ignore_not_found=True` and the Node returned HTTP 404.
    """
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
    st_code, _ = _checked_api_request(
        mgr,
        "PATCH",
        f"/api/v1/nodes/{node_name}/status",
        body=json.dumps({"status": status_body}).encode("utf-8"),
        headers={"Content-Type": "application/merge-patch+json"},
        ignore_not_found=ignore_not_found,
        context=f"Patch Node '{node_name}' status",
    )
    return st_code != 404


def _log_missing_node_update_skip(ie: ScheduledInfraEvent, wall_elapsed: float) -> None:
    print(
        f"  ~ [t={wall_elapsed:.2f}s wall / +{ie.raw_offset_sec:.2f}s] "
        f"Skipping Update for Node '{ie.name}': the node does not exist in the chamber",
        flush=True,
    )


def _fetch_live_node_or_skip_missing(
    mgr: ChamberManager,
    ie: ScheduledInfraEvent,
    wall_elapsed: float,
) -> tuple[dict[str, Any] | None, bool]:
    """GET the live Node before an Update so RFC 7396 merge-patches can null out removed keys."""
    st_code, raw_resp = _checked_api_request(
        mgr,
        "GET",
        f"/api/v1/nodes/{ie.name}",
        ignore_not_found=True,
        context=f"Get Node '{ie.name}'",
    )
    if st_code == 404:
        _log_missing_node_update_skip(ie, wall_elapsed)
        return None, True
    if not raw_resp:
        return {}, False
    try:
        parsed = json.loads(raw_resp.decode("utf-8"))
        return (parsed if isinstance(parsed, dict) else {}), False
    except Exception:
        return {}, False


def _patch_node_or_skip_missing(
    mgr: ChamberManager,
    ie: ScheduledInfraEvent,
    patch_doc: dict[str, Any],
    wall_elapsed: float,
) -> bool:
    """PATCH a Node for an Update event. Return True when the Node no longer exists and the event is skipped."""
    st_code, _ = _checked_api_request(
        mgr,
        "PATCH",
        f"/api/v1/nodes/{ie.name}",
        body=json.dumps(patch_doc).encode("utf-8"),
        headers={"Content-Type": "application/merge-patch+json"},
        ignore_not_found=True,
        context=f"Patch Node '{ie.name}'",
    )
    if st_code != 404:
        return False
    _log_missing_node_update_skip(ie, wall_elapsed)
    return True


def _patch_node_status_resources(
    mgr: ChamberManager,
    node_name: str,
    extra_status: dict[str, Any] | None,
    *,
    ignore_not_found: bool = False,
) -> bool:
    """Patch Node /status allocatable/capacity/nodeInfo fields without modifying Ready conditions.

    Returns False only when `ignore_not_found=True` and the Node returned HTTP 404.
    """
    if not extra_status:
        return True
    status_body = {
        k: extra_status[k]
        for k in ("allocatable", "capacity", "nodeInfo")
        if extra_status.get(k)
    }
    if not status_body:
        return True
    st_code, _ = _checked_api_request(
        mgr,
        "PATCH",
        f"/api/v1/nodes/{node_name}/status",
        body=json.dumps({"status": status_body}).encode("utf-8"),
        headers={"Content-Type": "application/merge-patch+json"},
        ignore_not_found=ignore_not_found,
        context=f"Patch Node '{node_name}' status resources",
    )
    return st_code != 404


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
        _checked_api_request(
            mgr,
            "DELETE",
            f"/api/v1/nodes/{ie.name}",
            ignore_not_found=True,
            context=f"Delete Node '{ie.name}'",
        )
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
    live_node, node_missing = _fetch_live_node_or_skip_missing(mgr, ie, wall_elapsed)
    if node_missing:
        return False, "not_found"
    live_meta = (live_node.get("metadata") or {}) if isinstance(live_node, dict) else {}
    live_labels = live_meta.get("labels") if isinstance(live_meta.get("labels"), dict) else {}
    live_anns = live_meta.get("annotations") if isinstance(live_meta.get("annotations"), dict) else {}
    live_status = (live_node.get("status") or {}) if isinstance(live_node, dict) else {}

    meta_in = (ie.manifest.get("metadata") or {}) if isinstance(ie.manifest, dict) else {}
    labels_in = meta_in.get("labels") if isinstance(meta_in.get("labels"), dict) else None
    anns_in = meta_in.get("annotations") if isinstance(meta_in.get("annotations"), dict) else None
    spec_in = dict((ie.manifest.get("spec") or {}) if isinstance(ie.manifest, dict) else {})
    st_in = (ie.manifest.get("status") or {}) if isinstance(ie.manifest, dict) else {}
    # Apply capacity only when the raw Update payload carried it. Otherwise `ie.manifest` holds the
    # synthetic default from _prepare_node_manifest(), which must never overwrite a live node.
    has_explicit_cap = bool(getattr(ie, "has_explicit_capacity", False))
    extra_status: dict[str, Any] | None = None
    if has_explicit_cap and isinstance(st_in, dict) and any(
        st_in.get(k) for k in ("allocatable", "capacity", "nodeInfo")
    ):
        extra_status = {}
        for field in ("allocatable", "capacity"):
            in_map = st_in.get(field)
            if isinstance(in_map, dict) and in_map:
                merged_res: dict[str, Any] = dict(in_map)
                live_map = live_status.get(field) if isinstance(live_status.get(field), dict) else {}
                # RFC 7396 merge-patch leaves omitted keys untouched; emit explicit nulls for dropped resources.
                for rk in live_map:
                    if rk not in merged_res:
                        merged_res[rk] = None
                extra_status[field] = merged_res
        if st_in.get("nodeInfo"):
            extra_status["nodeInfo"] = st_in["nodeInfo"]
    taints_list = [dict(t) for t in (spec_in.get("taints") or []) if isinstance(t, dict)]
    patch_meta: dict[str, Any] = {}
    if labels_in is not None:
        merged_labels: dict[str, Any] = dict(labels_in)
        for lk in live_labels:
            if lk not in merged_labels:
                merged_labels[lk] = None
        if merged_labels:
            patch_meta["labels"] = merged_labels
    patch_anns: dict[str, Any] = dict(anns_in) if anns_in else {}
    _node_res_anns = (
        "kueue-optimizer.gke.io/node-cpu",
        "kueue-optimizer.gke.io/node-memory",
        "kueue-optimizer.gke.io/node-pods",
        "kueue-optimizer.gke.io/node-accel-resource",
        "kueue-optimizer.gke.io/node-accel-count",
    )
    if not has_explicit_cap:
        for res_ann in _node_res_anns:
            patch_anns.pop(res_ann, None)
    else:
        for res_ann in _node_res_anns:
            if res_ann in live_anns and res_ann not in patch_anns:
                patch_anns[res_ann] = None
    if ie.is_not_ready or ie.has_no_execute_taint:
        if ie.is_not_ready and not any(
            t.get("key") == "node.kubernetes.io/not-ready" for t in taints_list
        ):
            taints_list.append({"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"})
        patch_anns["kueue-optimizer.gke.io/node-failed"] = "true"
        patch_meta["annotations"] = patch_anns
        patch_doc = {
            "metadata": patch_meta,
            "spec": {
                "unschedulable": bool(spec_in.get("unschedulable", ie.is_not_ready)),
                "taints": taints_list,
            },
        }
        if _patch_node_or_skip_missing(mgr, ie, patch_doc, wall_elapsed):
            return False, "not_found"
        if ie.is_not_ready:
            if not _patch_node_ready_condition(
                mgr,
                ie.name,
                ready_status=ie.ready_status or "False",
                reason="KubeletNotReady",
                message="Simulated node failure during scenario replay",
                extra_status=extra_status,
                ignore_not_found=True,
            ):
                _log_missing_node_update_skip(ie, wall_elapsed)
                return False, "not_found"
        elif extra_status:
            if not _patch_node_status_resources(
                mgr, ie.name, extra_status, ignore_not_found=True
            ):
                _log_missing_node_update_skip(ie, wall_elapsed)
                return False, "not_found"
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
        patch_anns["kueue-optimizer.gke.io/node-failed"] = None
        patch_meta["annotations"] = patch_anns
        patch_doc = {
            "metadata": patch_meta,
            "spec": {
                "unschedulable": bool(spec_in.get("unschedulable", False)),
                "taints": taints_list,
            },
        }
        if _patch_node_or_skip_missing(mgr, ie, patch_doc, wall_elapsed):
            return False, "not_found"
        if not _patch_node_ready_condition(
            mgr,
            ie.name,
            ready_status="True",
            reason="KubeletReady",
            message="kubelet is posting ready status",
            extra_status=extra_status,
            ignore_not_found=True,
        ):
            _log_missing_node_update_skip(ie, wall_elapsed)
            return False, "not_found"
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
    if del_ev.manifest_kind == "Pod" and del_ev.object_kind != "PodGroup":
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
    panns = pm.get("annotations") or {}
    if del_ev.manifest_kind == "Job":
        if plbls.get("batch.kubernetes.io/job-name") == target or plbls.get("job-name") == target:
            return True
    elif del_ev.manifest_kind == "JobSet":
        if plbls.get("jobset.sigs.k8s.io/jobset-name") == target:
            return True
        if plbls.get("batch.kubernetes.io/job-name") in job_set or plbls.get("job-name") in job_set:
            return True

    if del_ev.object_kind == "PodGroup" and (
        panns.get("kueue.x-k8s.io/pod-group-name") == target
        or plbls.get("kueue.x-k8s.io/pod-group-name") == target
    ):
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

    scraped_flavors_by_rk: dict[str, set[str]] = {}
    for s_psa in idep.scraped_pod_set_assignments:
        for rk, fn in (s_psa.get("flavors") or {}).items():
            fn_s = str(fn)
            if fn_s in idep.flavors_by_name:
                scraped_flavors_by_rk.setdefault(str(rk), set()).add(fn_s)

    def _pick_scraped_flv(rk: str) -> str:
        cands = scraped_flavors_by_rk.get(rk)
        if not cands:
            return ""
        if node_doc is not None:
            for fn in sorted(cands):
                if _matches_flv(node_doc, idep.flavors_by_name.get(fn)):
                    return fn
            return ""
        if len(cands) == 1:
            return next(iter(cands))
        return ""

    target_resources = [rk for rk in reqs if rk in covered_to_flavors]
    if not target_resources:
        target_resources = [rk for rk in reqs if rk != "ephemeral-storage"]

    flavors_map: dict[str, str] = {}
    resource_usage: dict[str, str] = {}
    assigned_tas_topology = ""
    for rk in sorted(target_resources):
        flv_name = _pick_scraped_flv(rk) or _pick_flv(covered_to_flavors.get(rk) or all_cq_flavors)
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
    # Kubernetes Deployment controller does not create the first ReplicaSet while `spec.paused` is True.
    # Bootstrap paused t0 Deployments with `paused: false` first, then restore `paused: true` after pre-admission.
    initial_apply_docs: list[dict[str, Any]] = []
    paused_to_restore: list[dict[str, Any]] = []
    for d in initial_deployments:
        if d.admitted_replicas > 0 and bool((d.manifest.get("spec") or {}).get("paused", False)):
            unpaused_doc = json.loads(json.dumps(d.manifest))
            unpaused_doc.setdefault("spec", {})["paused"] = False
            initial_apply_docs.append(unpaused_doc)
            paused_to_restore.append(d.manifest)
        else:
            initial_apply_docs.append(d.manifest)
    mgr.apply_manifests_parallel(initial_apply_docs, max_workers=16)

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
    now_ts = ref_now.strftime("%Y-%m-%dT%H:%M:%SZ")
    initial_patch_tasks: list[tuple[str, str, dict[str, Any]]] = []
    final_patch_tasks: list[tuple[str, str, dict[str, Any]]] = []
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
            # Stamp `now_ts` on the first patch so WaitForPodsReady does not evict aged t0 pods before they become Ready.
            initial_patch_tasks.append(
                (idep.namespace, wl_name, _build_single_pod_deployment_workload_status(idep, wl_doc, plc, now_ts))
            )
            final_patch_tasks.append(
                (idep.namespace, wl_name, _build_single_pod_deployment_workload_status(idep, wl_doc, plc, wl_ts))
            )
            admitted_pod_keys.append((idep.namespace, pod_name))

    def _patch_one(task: tuple[str, str, dict[str, Any]]) -> None:
        ns_t, wl_n_t, st_t = task
        st_code, raw_resp = mgr.api_request(
            "PATCH",
            f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{ns_t}/workloads/{wl_n_t}/status",
            body=json.dumps({"status": st_t}).encode("utf-8"),
            headers={"Content-Type": "application/merge-patch+json"},
        )
        if not (200 <= st_code < 300):
            resp_txt = raw_resp.decode("utf-8", errors="replace")[:300] if raw_resp else ""
            raise RuntimeError(
                f"Failed to patch t0 Deployment Workload status '{ns_t}/{wl_n_t}' (HTTP {st_code}): {resp_txt}"
            )

    with ThreadPoolExecutor(max_workers=min(16, max(1, len(initial_patch_tasks)))) as pool:
        list(pool.map(_patch_one, initial_patch_tasks))

    mgr.wait_for_pods_running(admitted_pod_keys, require_ready=True)

    # Restore PodsReady=True (Started) and apply back-dated Admitted seniority once all pods are Ready.
    with ThreadPoolExecutor(max_workers=min(16, max(1, len(final_patch_tasks)))) as pool:
        list(pool.map(_patch_one, final_patch_tasks))

    if paused_to_restore:
        mgr.apply_manifests_parallel(paused_to_restore, max_workers=16)


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

    # Bootstrap pre-admitted t0 Kueue PodGroup Workloads + member Pods + non-Kueue infra-pods FIRST
    # so pinned `spec.nodeName` pods occupy their recorded nodes before kube-scheduler places unpinned
    # t0 Deployment pods, and both finish BEFORE ClusterQueues/LocalQueues activate.
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

    if initial_deployments:
        _bootstrap_initial_deployments(mgr, initial_deployments)

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
) -> ReplayResult:
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

    mgr._retired_serving_workloads = {}
    mgr._node_capacity_timeline = []

    def _extract_node_alloc(node_doc: dict[str, Any] | None) -> dict[str, float]:
        if not isinstance(node_doc, dict):
            return {}
        st_blk = node_doc.get("status") or {}
        alloc_blk = st_blk.get("allocatable") or st_blk.get("capacity") or {}
        if not isinstance(alloc_blk, dict):
            return {}
        out: dict[str, float] = {}
        for rk, rv in alloc_blk.items():
            if rk == "ephemeral-storage":
                continue
            val = parse_k8s_quantity(rv, rk)
            if val > 0.0:
                out[str(rk)] = val
        return out

    live_node_alloc: dict[str, dict[str, float]] = {}
    for n_doc in bundle.nodes:
        if not isinstance(n_doc, dict):
            continue
        n_name = str((n_doc.get("metadata") or {}).get("name") or "")
        n_alloc = _extract_node_alloc(n_doc)
        if n_name and n_alloc:
            live_node_alloc[n_name] = n_alloc

    def _record_node_capacity_snapshot(ep: float) -> None:
        totals: dict[str, float] = {}
        for alloc_map in live_node_alloc.values():
            for rk, rv in alloc_map.items():
                if rv > 0.0:
                    totals[rk] = totals.get(rk, 0.0) + rv
        mgr._node_capacity_timeline.append({"epoch": float(ep), "capacity": totals})

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
    ever_seen_sched_wl_keys: set[tuple[str, str]] = set()
    ever_finished_sched_wl_keys: set[tuple[str, str]] = set()
    ever_finished_init_wl_keys: set[tuple[str, str]] = set()
    cancelled_sched_wl_keys: set[tuple[str, str]] = set()
    # Workloads counted as finished only because they disappeared, and how many polls in a row each
    # seen-but-unfinished Workload has been absent.
    vanished_assumed_finished: set[tuple[str, str]] = set()
    absent_poll_counts: dict[tuple[str, str], int] = {}
    submitted_batch_records: dict[tuple[str, str, str, int], _SubmittedBatchRecord] = {}
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
    # Wall offset before which no workflow may be created. Set after creating one creation-time tier
    # while later tiers wait, so each tier gets a creationTimestamp in a later whole second.
    fifo_gate_wall_sec = 0.0

    known_deploy_names_by_ns: dict[str, set[str]] = {}
    known_standalone_pod_names_by_ns: dict[str, set[str]] = {}
    active_deployment_replicas: dict[tuple[str, str], int] = {}
    expected_clean_retirements: dict[tuple[str, str], int] = {}
    retire_eligible_wls: dict[tuple[str, str], set[tuple[str, str]]] = {}
    upgrade_active_deps: set[tuple[str, str]] = set()
    deployment_paused_state: dict[tuple[str, str], bool] = {}
    deferred_paused_upgrade: set[tuple[str, str]] = set()
    live_serving_wls: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
    serving_wl_keys: set[tuple[str, str]] = set()
    non_serving_wl_keys: set[tuple[str, str]] = set()
    # (namespace, workload) -> (is_serving, deployment_name), confirmed from the owning Pod's annotations.
    serving_class_by_wl: dict[tuple[str, str], tuple[bool, str]] = {}
    # Pod-owned Workloads whose owning Pod was already gone when first looked up.
    owner_pod_missing_wls: set[tuple[str, str]] = set()
    terminated_incarnations: set[tuple[str, str, str, int]] = set()
    scale_zero_drain_deadline: float | None = None

    for idep in bundle.initial_deployments:
        known_deploy_names_by_ns.setdefault(idep.namespace, set()).add(idep.applied_name)
        active_deployment_replicas[(idep.namespace, idep.applied_name)] = idep.desired_replicas
        deployment_paused_state[(idep.namespace, idep.applied_name)] = bool(idep.paused)
    for sw_d in bundle.scheduled_workflows:
        if sw_d.object_kind == "Deployment":
            ap_n = str((sw_d.manifest.get("metadata") or {}).get("name") or sw_d.name)
            known_deploy_names_by_ns.setdefault(sw_d.namespace, set()).add(ap_n)
        elif sw_d.object_kind == "Pod":
            ap_p = str((sw_d.manifest.get("metadata") or {}).get("name") or sw_d.name)
            known_standalone_pod_names_by_ns.setdefault(sw_d.namespace, set()).update({sw_d.name, ap_p})
    for ip_s in bundle.initial_pods:
        ap_ip = str((ip_s.manifest.get("metadata") or {}).get("name") or ip_s.name)
        known_standalone_pod_names_by_ns.setdefault(ip_s.namespace, set()).update({ip_s.name, ap_ip})
    for iwl_p in bundle.initial_workloads:
        if iwl_p.owner_kind == "Pod":
            known_standalone_pod_names_by_ns.setdefault(iwl_p.namespace, set()).add(iwl_p.owner_name)
            for p_s in iwl_p.pods:
                ap_ps = str((p_s.manifest.get("metadata") or {}).get("name") or p_s.name)
                known_standalone_pod_names_by_ns.setdefault(iwl_p.namespace, set()).update({p_s.name, ap_ps})
    for su_d in bundle.scheduled_workflow_updates:
        if su_d.object_kind == "Deployment":
            known_deploy_names_by_ns.setdefault(su_d.namespace, set()).add(su_d.applied_name)

    def _is_known_standalone_pod(ns_val: str, pod_name: str) -> bool:
        return bool(pod_name) and pod_name in known_standalone_pod_names_by_ns.get(ns_val, set())

    def _is_known_standalone_pod_wl_name(ns_val: str, wl_name: str) -> bool:
        if not wl_name.startswith("pod-"):
            return False
        for sp in known_standalone_pod_names_by_ns.get(ns_val, set()):
            if (
                wl_name == f"pod-{sp}"
                or wl_name.startswith(f"pod-{sp}-")
                or (len(sp) > 53 and wl_name.startswith(f"pod-{sp[:53]}-"))
            ):
                return True
        return False

    def _is_currently_admitted_serving_wl(wl_doc: dict[str, Any]) -> bool:
        st_obj = wl_doc.get("status") or {}
        conds = st_obj.get("conditions") or []
        if any(
            isinstance(c, dict)
            and c.get("type") in ("Evicted", "Preempted")
            and c.get("status") == "True"
            for c in conds
        ):
            return False
        return bool((st_obj.get("admission") or {}).get("clusterQueue")) or any(
            isinstance(c, dict)
            and c.get("type") == "Admitted"
            and c.get("status") == "True"
            for c in conds
        )

    def _owner_pod_name(wl_doc: dict[str, Any]) -> str:
        for o in (wl_doc.get("metadata") or {}).get("ownerReferences") or []:
            if isinstance(o, dict) and o.get("kind") == "Pod" and o.get("name"):
                return str(o["name"])
        return ""

    def _match_deploy_for_serving_wl(ns_val: str, wl_doc: dict[str, Any]) -> str:
        pod_ref = _owner_pod_name(wl_doc)
        if not pod_ref:
            return ""
        best = ""
        for cand_dep in known_deploy_names_by_ns.get(ns_val, set()):
            if pod_ref.startswith(f"{cand_dep}-") and len(cand_dep) > len(best):
                best = cand_dep
        return best

    def _has_serving_marker(ann: dict[str, Any]) -> bool:
        return (
            str(ann.get("kueue-optimizer.gke.io/long-running-pod") or "").lower() == "true"
            or str(ann.get("kueue.x-k8s.io/pod-suspending-parent") or "").lower() == "deployment"
        )

    def _classify_serving_workload_doc(
        ns_val: str,
        wl_doc: dict[str, Any],
        owner_pod_ann: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        """Return `(is_serving, deployment_name)` for a Workload.

        Kueue's pod integration builds `spec.podSets[].template` from `Pod.spec` only and copies no
        `kueue-optimizer.gke.io/*` annotation onto the Workload. So the Deployment name and the finite
        pod marker come from the annotations of the owning Pod (`owner_pod_ann`, `None` when the Pod is
        unknown). A result that the owning Pod confirmed is cached, because the Pod can be gone on a
        later poll while its Workload is still listed.
        """
        wk_c = (ns_val, str((wl_doc.get("metadata") or {}).get("name") or ""))
        cached = serving_class_by_wl.get(wk_c)
        if cached is not None:
            return cached
        wm_ann = ((wl_doc.get("metadata") or {}).get("annotations")) or {}
        has_serving_marker = _has_serving_marker(wm_ann)
        for ps in ((wl_doc.get("spec") or {}).get("podSets")) or []:
            if isinstance(ps, dict):
                ps_ann = (((ps.get("template") or {}).get("metadata")) or {}).get("annotations") or {}
                has_serving_marker = has_serving_marker or _has_serving_marker(ps_ann)

        owner_pod = _owner_pod_name(wl_doc)
        is_known_standalone = _is_known_standalone_pod(ns_val, owner_pod)
        result: tuple[bool, str] | None = None
        if owner_pod_ann is not None:
            dep_ann = str(owner_pod_ann.get("kueue-optimizer.gke.io/deployment-name") or "").strip()
            has_serving_marker = has_serving_marker or _has_serving_marker(owner_pod_ann)
            has_finite_pod_exec = bool(
                str(owner_pod_ann.get("kueue-optimizer.gke.io/pod-execution-time-seconds") or "").strip()
            )
            if dep_ann:
                result = (True, dep_ann)
            elif (has_finite_pod_exec or is_known_standalone) and not has_serving_marker:
                result = (False, "")
        elif is_known_standalone and not has_serving_marker:
            # Fast-exiting standalone Pod disappeared before the first Pod list: never fall back to
            # Deployment prefix matching.
            result = (False, "")
        if result is None:
            dep_n = _match_deploy_for_serving_wl(ns_val, wl_doc)
            if dep_n:
                result = (True, dep_n)
            else:
                result = (has_serving_marker, "")
        if (owner_pod_ann is not None or is_known_standalone) and wk_c[1]:
            serving_class_by_wl[wk_c] = result
        return result

    def _non_serving_finished_keys(keys: set[tuple[str, str]]) -> set[tuple[str, str]]:
        if not known_deploy_names_by_ns and not serving_wl_keys:
            return keys
        out: set[tuple[str, str]] = set()
        for ns_k, wn_k in keys:
            if (ns_k, wn_k) in serving_wl_keys:
                continue
            if (ns_k, wn_k) in non_serving_wl_keys:
                out.add((ns_k, wn_k))
                continue
            if _is_known_standalone_pod_wl_name(ns_k, wn_k):
                non_serving_wl_keys.add((ns_k, wn_k))
                out.add((ns_k, wn_k))
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
        now_dt_sync = datetime.now(timezone.utc)
        now_iso = now_dt_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
        cur_wls: dict[tuple[str, str], dict[str, Any]] = {}
        cur_dep_total_counts: dict[tuple[str, str], int] = {}
        owner_gone_ts: dict[tuple[str, str], str] = {}
        query_all_ok = True
        for ns_q in bundle.workload_namespaces:
            if ns_q == bundle.infra_pods_namespace:
                continue
            st_w, raw_w = mgr.api_request("GET", f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{ns_q}/workloads")
            if st_w != 200 or not raw_w:
                query_all_ok = False
                continue
            wl_items = [w for w in (json.loads(raw_w.decode("utf-8")).get("items") or []) if isinstance(w, dict)]

            # List the Pods of this namespace only when a Pod-owned Workload is not classified yet.
            def _needs_owner_pod(w_doc: dict[str, Any]) -> bool:
                w_meta = w_doc.get("metadata") or {}
                w_key = (str(w_meta.get("namespace") or ns_q), str(w_meta.get("name") or ""))
                return (
                    bool(_owner_pod_name(w_doc))
                    and w_key not in serving_class_by_wl
                    and w_key not in owner_pod_missing_wls
                )

            pod_ann_by_name: dict[str, dict[str, Any]] | None = None
            if any(_needs_owner_pod(w) for w in wl_items):
                st_p, raw_p = mgr.api_request("GET", f"/api/v1/namespaces/{ns_q}/pods")
                if st_p == 200 and raw_p:
                    pod_ann_by_name = {}
                    for p_it in json.loads(raw_p.decode("utf-8")).get("items") or []:
                        p_meta = (p_it.get("metadata") or {}) if isinstance(p_it, dict) else {}
                        if p_meta.get("name"):
                            pod_ann_by_name[str(p_meta["name"])] = dict(p_meta.get("annotations") or {})

            for wl_it in wl_items:
                wm = wl_it.get("metadata") or {}
                wn = str(wm.get("name") or "")
                actual_ns = str(wm.get("namespace") or ns_q)
                if not wn:
                    continue
                wk = (actual_ns, wn)
                owner_pod_ann: dict[str, Any] | None = None
                if pod_ann_by_name is not None and _needs_owner_pod(wl_it):
                    owner_pod_ann = pod_ann_by_name.get(_owner_pod_name(wl_it))
                    if owner_pod_ann is None:
                        # The Pod is already gone and its name is never reused: stop asking for it.
                        owner_pod_missing_wls.add(wk)
                is_srv, dep_n = _classify_serving_workload_doc(actual_ns, wl_it, owner_pod_ann)
                if not is_srv:
                    non_serving_wl_keys.add(wk)
                    continue
                serving_wl_keys.add(wk)
                if wk in mgr._retired_serving_workloads:
                    continue
                st_obj = wl_it.get("status") or {}
                conds = st_obj.get("conditions") or []
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
                is_preempted_true = any(
                    isinstance(c, dict)
                    and c.get("type") == "Preempted"
                    and c.get("status") == "True"
                    for c in conds
                )
                accum_past_sec = float(st_obj.get("accumulatedPastExecutionTimeSeconds") or 0.0)
                has_adm_hist = (
                    bool((st_obj.get("admission") or {}).get("clusterQueue"))
                    or accum_past_sec > 0.0
                    or any(
                        isinstance(c, dict)
                        and c.get("type") == "Admitted"
                        and c.get("status") == "True"
                        for c in conds
                    )
                )
                if owner_gone_c is not None:
                    ret_ts_str = str(owner_gone_c.get("lastTransitionTime") or now_iso)
                    owner_gone_ts[wk] = ret_ts_str
                    if wk in live_serving_wls:
                        if was_ev_or_pre:
                            prev_dep, prev_doc = live_serving_wls[wk]
                            merged_doc = json.loads(json.dumps(wl_it))
                            prev_adm = (prev_doc.get("status") or {}).get("admission")
                            if prev_adm and not ((merged_doc.get("status") or {}).get("admission")):
                                merged_doc.setdefault("status", {})["admission"] = prev_adm
                            live_serving_wls[wk] = (dep_n or prev_dep, merged_doc)
                    elif has_adm_hist or is_preempted_true:
                        seed_doc = json.loads(json.dumps(wl_it))
                        if (
                            not was_ev_or_pre
                            and accum_past_sec > 0.0
                            and not any(
                                isinstance(c, dict)
                                and c.get("type") == "Admitted"
                                and c.get("status") == "True"
                                for c in conds
                            )
                        ):
                            ret_ep = parse_rfc3339_epoch(ret_ts_str) or now_dt_sync.timestamp()
                            est_adm_iso = datetime.fromtimestamp(
                                ret_ep - accum_past_sec, tz=timezone.utc
                            ).strftime("%Y-%m-%dT%H:%M:%SZ")
                            s_conds = [
                                c
                                for c in (seed_doc.setdefault("status", {}).get("conditions") or [])
                                if not (isinstance(c, dict) and c.get("type") == "Admitted")
                            ]
                            s_conds.append(
                                {
                                    "type": "Admitted",
                                    "status": "True",
                                    "reason": "Admitted",
                                    "lastTransitionTime": est_adm_iso,
                                }
                            )
                            seed_doc["status"]["conditions"] = s_conds
                        live_serving_wls[wk] = (dep_n, seed_doc)
                    continue

                cur_wls[wk] = wl_it
                if dep_n:
                    dk_cnt = (actual_ns, dep_n)
                    cur_dep_total_counts[dk_cnt] = cur_dep_total_counts.get(dk_cnt, 0) + 1
                if wk in live_serving_wls:
                    if _is_currently_admitted_serving_wl(wl_it) or was_ev_or_pre:
                        prev_dep, prev_doc = live_serving_wls[wk]
                        merged_doc = json.loads(json.dumps(wl_it))
                        prev_adm = (prev_doc.get("status") or {}).get("admission")
                        if prev_adm and not ((merged_doc.get("status") or {}).get("admission")):
                            merged_doc.setdefault("status", {})["admission"] = prev_adm
                        live_serving_wls[wk] = (dep_n or prev_dep, merged_doc)
                elif has_adm_hist or is_preempted_true:
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
            elig_set = retire_eligible_wls.get(dep_key)
            was_eligible = elig_set is not None and wk in elig_set
            if elig_set is not None:
                elig_set.discard(wk)
            if was_evicted_or_preempted:
                conds = [
                    c for c in conds if not (isinstance(c, dict) and c.get("type") == "Finished")
                ]
                st_blk["conditions"] = conds
                mgr._retired_serving_workloads[wk] = doc_cp
            elif mark_deleted_deploy == dep_key or (
                expected_clean_retirements.get(dep_key, 0) > 0
                and was_eligible
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
                conds = [
                    c for c in conds if not (isinstance(c, dict) and c.get("type") == "Finished")
                ]
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
                st_blk["conditions"] = conds
                mgr._retired_serving_workloads[wk] = doc_cp

        if query_all_ok:
            live_wk_set = set(live_serving_wls.keys())
            for dep_key in list(expected_clean_retirements.keys()):
                rem_elig = retire_eligible_wls.get(dep_key, set()) & live_wk_set
                retire_eligible_wls[dep_key] = rem_elig
                cur_live_adm = sum(
                    1
                    for wk_i, (dn_i, d_i) in live_serving_wls.items()
                    if (wk_i[0], dn_i) == dep_key and _is_currently_admitted_serving_wl(d_i)
                )
                cur_tot = cur_dep_total_counts.get(dep_key, 0)
                desired_rep = active_deployment_replicas.get(dep_key, 0)
                scaledown_converged = (
                    dep_key not in upgrade_active_deps
                    and cur_tot <= desired_rep
                    and cur_live_adm <= desired_rep
                )
                if (
                    expected_clean_retirements.get(dep_key, 0) <= 0
                    or not rem_elig
                    or scaledown_converged
                ):
                    expected_clean_retirements.pop(dep_key, None)
                    retire_eligible_wls.pop(dep_key, None)
                    upgrade_active_deps.discard(dep_key)

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
    received_signal: int | None = None
    stop_reason = "completed"

    def _sig_handler(signum: int, _frame: Any) -> None:
        nonlocal stop_requested, received_signal, stop_reason
        print(f"\n==> Received signal {signum}; stopping testrunner gracefully...", flush=True)
        stop_requested = True
        received_signal = signum
        stop_reason = "interrupted"

    old_sigint = signal.signal(signal.SIGINT, _sig_handler)
    old_sigterm = signal.signal(signal.SIGTERM, _sig_handler)

    start_epoch = time.time()
    _record_node_capacity_snapshot(start_epoch)
    t0_mono = time.monotonic()
    fast_poll_floor = 1.5 if (total_planned + len(initial_wl_keys)) > 200 else 0.5
    poll_interval_sec = min(status_interval_sec, fast_poll_floor)
    next_status_mono = t0_mono
    next_poll_mono = t0_mono

    def _record_sched_wl_progress(snap: ClusterStatusSnapshot, *, count_absence: bool = False) -> None:
        # `count_absence` is set only by _poll_cluster(), so each cluster poll counts once even though
        # this function runs several times per poll.
        present_sched = _non_serving_finished_keys(snap.present_workload_keys - initial_wl_keys)
        finished_sched = _non_serving_finished_keys(snap.finished_workload_keys - initial_wl_keys)
        ever_seen_sched_wl_keys.update(present_sched)
        ever_finished_sched_wl_keys.update(finished_sched)
        vanished_assumed_finished.difference_update(finished_sched)
        # A Workload that was assumed finished because it vanished, but is present and unfinished
        # again (Kueue deleted and re-created it under the same name), is running, not finished.
        reappeared = (vanished_assumed_finished & present_sched) - finished_sched
        if reappeared:
            ever_finished_sched_wl_keys.difference_update(reappeared)
            vanished_assumed_finished.difference_update(reappeared)
        if count_absence and snap.workloads_query_ok:
            # If a previously observed scheduled Workload disappears before any poll catches
            # Finished=True (for example due to ttlSecondsAfterFinished or external deletion),
            # treat it as terminal unless it was explicitly cancelled by replay. Require two
            # consecutive polls without it, so a delete-and-re-create between polls is not
            # mistaken for completion.
            absent_now = (
                ever_seen_sched_wl_keys
                - snap.present_workload_keys
                - ever_finished_sched_wl_keys
                - cancelled_sched_wl_keys
            )
            for wk_gone in list(absent_poll_counts):
                if wk_gone not in absent_now:
                    del absent_poll_counts[wk_gone]
            for wk_gone in absent_now:
                absent_poll_counts[wk_gone] = absent_poll_counts.get(wk_gone, 0) + 1
            vanished_seen = {wk_gone for wk_gone, cnt in absent_poll_counts.items() if cnt >= 2}
            if vanished_seen:
                ever_finished_sched_wl_keys.update(vanished_seen)
                vanished_assumed_finished.update(vanished_seen)
                for wk_gone in vanished_seen:
                    del absent_poll_counts[wk_gone]

    def _snapshot_matching_wl_keys(r_ns: str, m_kind: str, r_name: str) -> frozenset[tuple[str, str]]:
        known_now = ever_seen_sched_wl_keys | ever_finished_sched_wl_keys | cancelled_sched_wl_keys
        return frozenset(
            (w_ns, w_n)
            for (w_ns, w_n) in known_now
            if w_ns == r_ns and _workload_name_matches_owner(w_n, m_kind, r_name)
        )

    def _reconcile_vanished_unseen_workflows(snap: ClusterStatusSnapshot, now_mono: float) -> None:
        """Detect submitted batch workflows whose parent and Workload both vanished before first poll."""
        if not snap.workloads_query_ok or not submitted_batch_records:
            return
        accounted_non_cancelled = len(
            (ever_seen_sched_wl_keys | ever_finished_sched_wl_keys) - cancelled_sched_wl_keys
        )
        if accounted_non_cancelled + finite_batch_cancelled >= finite_batch_submitted:
            return
        grace_sec = max(2.0, poll_interval_sec * 2.0)
        probe_cooldown_sec = max(5.0, poll_interval_sec * 4.0)
        max_probes_per_poll = 16
        known_wls = ever_seen_sched_wl_keys | ever_finished_sched_wl_keys | cancelled_sched_wl_keys
        known_by_ns: dict[str, list[str]] = {}
        for w_ns, w_n in known_wls:
            known_by_ns.setdefault(w_ns, []).append(w_n)

        probe_candidates: list[tuple[float, tuple[str, str, str, int], _SubmittedBatchRecord]] = []
        for rec_key, rec in list(submitted_batch_records.items()):
            if any(
                (rec.namespace, w_n) not in rec.pre_existing_wl_keys
                and _workload_name_matches_owner(w_n, rec.kind, rec.applied_name)
                for w_n in known_by_ns.get(rec.namespace, ())
            ):
                submitted_batch_records.pop(rec_key, None)
                continue
            if now_mono - rec.submitted_mono < grace_sec:
                continue
            if rec.last_probe_mono > 0.0 and (now_mono - rec.last_probe_mono) < probe_cooldown_sec:
                continue
            probe_candidates.append((rec.last_probe_mono, rec_key, rec))

        probe_candidates.sort(key=lambda item: item[0])
        for _last_p, rec_key, rec in probe_candidates[:max_probes_per_poll]:
            rec.last_probe_mono = now_mono
            parent_path = _delete_rest_path(rec.api_version, rec.kind, rec.namespace, rec.applied_name)
            st_parent, _ = mgr.api_request("GET", parent_path)
            if st_parent == 404:
                vanished_key = (rec.namespace, f"__vanished__/{rec.kind}/{rec.applied_name}#{rec_key[3]}")
                ever_finished_sched_wl_keys.add(vanished_key)
                submitted_batch_records.pop(rec_key, None)

    def _check_and_resubmit_evicted_initial_workloads(
        snap: ClusterStatusSnapshot,
        wall_elapsed: float,
    ) -> None:
        nonlocal total_planned, submitted_count, finite_batch_submitted, total_pods_submitted
        if not initial_wl_keys:
            return
        ever_finished_init_wl_keys.update(snap.finished_workload_keys & initial_wl_keys)
        detected_evicted = set(snap.evicted_workload_keys & initial_wl_keys) - ever_finished_init_wl_keys
        if snap.workloads_query_ok:
            # When Kueue preempts a PodGroup (`retriable-in-group: "false"`), its member Pods are deleted
            # and Kubernetes GC may garbage-collect the PodGroup Workload within milliseconds before the next
            # poll; normal completion leaves Succeeded Pods and the Finished Workload intact until GC.
            # Never re-submit an initial Workload that was already observed Finished before disappearing.
            gc_after_preempt = (
                initial_wl_keys
                - snap.present_workload_keys
                - snap.finished_workload_keys
                - ever_finished_init_wl_keys
            )
            detected_evicted |= gc_after_preempt
        newly_evicted = sorted(detected_evicted - resubmitted_initial_wls - ever_finished_init_wl_keys)
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
                resub_api = str(it.manifest.get("apiVersion") or "")
                resub_kind = str(it.manifest.get("kind") or it.object_kind)
                submitted_batch_records[(it.namespace, it.object_kind, it.name, it.incarnation_index)] = (
                    _SubmittedBatchRecord(
                        api_version=resub_api,
                        kind=resub_kind,
                        namespace=it.namespace,
                        applied_name=resub_applied,
                        submitted_mono=time.monotonic(),
                        pre_existing_wl_keys=_snapshot_matching_wl_keys(it.namespace, resub_kind, resub_applied),
                    )
                )
            total_pods_submitted += it.pod_count
            print(
                f"  ~ [t={wall_elapsed:.2f}s wall] Initial t0 PodGroup Workload '{iwl.namespace}/{iwl.workload_name}' "
                f"was preempted/evicted -> dynamically re-submitted {it.object_kind} '{it.namespace}/{it.name}' "
                f"(queue={it.local_queue_name or '-'}, pods={it.pod_count}, pod_exec={it.compressed_duration_sec:.2f}s)",
                flush=True,
            )

    def _scheduled_done_count(snap: ClusterStatusSnapshot) -> int:
        _record_sched_wl_progress(snap)
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
                or wl_key in ever_finished_init_wl_keys
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

        _checked_api_request(
            mgr,
            "DELETE",
            del_path,
            body=del_opts,
            headers={"Content-Type": "application/json"},
            ignore_not_found=True,
            context=f"Delete {del_ev.manifest_kind} '{del_ev.namespace}/{del_ev.applied_name}'",
        )
        for jn in sorted(owned_job_names):
            _checked_api_request(
                mgr,
                "DELETE",
                f"/apis/batch/v1/namespaces/{del_ev.namespace}/jobs/{jn}",
                body=del_opts,
                headers={"Content-Type": "application/json"},
                ignore_not_found=True,
                context=f"Delete owned Job '{del_ev.namespace}/{jn}'",
            )
        for rsn in sorted(owned_rs_names):
            _checked_api_request(
                mgr,
                "DELETE",
                f"/apis/apps/v1/namespaces/{del_ev.namespace}/replicasets/{rsn}",
                body=del_opts,
                headers={"Content-Type": "application/json"},
                ignore_not_found=True,
                context=f"Delete owned ReplicaSet '{del_ev.namespace}/{rsn}'",
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
                    _checked_api_request(
                        mgr,
                        "DELETE",
                        f"/api/v1/namespaces/{del_ev.namespace}/pods/{pn}?gracePeriodSeconds=0",
                        ignore_not_found=True,
                        context=f"Delete owned Pod '{del_ev.namespace}/{pn}'",
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
                    _checked_api_request(
                        mgr,
                        "DELETE",
                        f"/api/v1/namespaces/{del_ev.namespace}/pods/{pm_name}?gracePeriodSeconds=0",
                        ignore_not_found=True,
                        context=f"Delete t0 Pod '{del_ev.namespace}/{pm_name}'",
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
        _record_sched_wl_progress(pre_snap)
        ever_finished_init_wl_keys.update(pre_snap.finished_workload_keys & initial_wl_keys)

        unsubmitted_match = any(
            w.namespace == del_ev.namespace
            and w.object_kind == del_ev.object_kind
            and w.name == del_ev.name
            and w.incarnation_index == del_ev.incarnation_index
            for w in event_heap
        )
        inc_key = (del_ev.namespace, del_ev.object_kind, del_ev.name, del_ev.incarnation_index)
        terminated_incarnations.add(inc_key)
        if unsubmitted_match:
            cancelled_unsubmitted_keys.add(inc_key)
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
        matched_wl_keys: set[tuple[str, str]] = set()
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
                        if wl_n and (del_ev.namespace, wl_n) not in initial_wl_keys:
                            matched_wl_keys.add((del_ev.namespace, wl_n))
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
            dep_del_key = (del_ev.namespace, del_ev.applied_name)
            _sync_serving_workloads(mark_deleted_deploy=dep_del_key)
            active_deployment_replicas.pop(dep_del_key, None)
            deployment_paused_state.pop(dep_del_key, None)
            deferred_paused_upgrade.discard(dep_del_key)
            expected_clean_retirements.pop(dep_del_key, None)
            retire_eligible_wls.pop(dep_del_key, None)
            upgrade_active_deps.discard(dep_del_key)

        has_future_same_name = any(
            w.namespace == del_ev.namespace
            and str((w.manifest.get("metadata") or {}).get("name") or w.name) == del_ev.applied_name
            for w in event_heap
        )
        _delete_workflow_and_children(del_ev, wait_gone=(del_ev.delete_reason == "before_recreate" or has_future_same_name))
        submitted_non_cancelled.pop((del_ev.namespace, del_ev.applied_name), None)
        submitted_batch_records.pop(inc_key, None)
        if not already_finished_in_chamber:
            cancelled_workflow_count += 1
            if del_ev.object_kind != "Deployment" and del_ev.manifest_kind != "Deployment":
                finite_batch_cancelled += 1
            cancelled_sched_wl_keys.update(matched_wl_keys)
            del_kind = del_ev.manifest_kind or del_ev.object_kind
            for wk_seen in list(ever_seen_sched_wl_keys - ever_finished_sched_wl_keys):
                if wk_seen[0] == del_ev.namespace and _workload_name_matches_owner(
                    wk_seen[1], del_kind, del_ev.applied_name
                ):
                    cancelled_sched_wl_keys.add(wk_seen)
        print(
            f"  - [t={wall_elapsed:.2f}s wall / +{del_ev.raw_offset_sec:.2f}s] "
            f"Deleted {del_ev.object_kind} '{del_ev.namespace}/{del_ev.name}' "
            f"(incarnation={del_ev.incarnation_index}, reason={del_ev.delete_reason})",
            flush=True,
        )

    def _execute_workflow_update(upd: ScheduledWorkflowUpdate, wall_elapsed: float) -> None:
        nonlocal total_pods_submitted
        if (upd.namespace, upd.object_kind, upd.name, upd.incarnation_index) in terminated_incarnations:
            return
        _sync_serving_workloads()
        dep_key = (upd.namespace, upd.applied_name)
        known_deploy_names_by_ns.setdefault(upd.namespace, set()).add(upd.applied_name)
        prev_rep = active_deployment_replicas.get(dep_key, upd.replicas)

        pre_admitted_wls = {
            wk
            for wk, (dn, wdoc) in live_serving_wls.items()
            if (wk[0], dn) == dep_key and _is_currently_admitted_serving_wl(wdoc)
        }
        live_admitted = len(pre_admitted_wls)

        if "Upgrade" in upd.update_types and upd.paused:
            deferred_paused_upgrade.add(dep_key)

        effective_upgrade = (not upd.paused) and (
            "Upgrade" in upd.update_types or dep_key in deferred_paused_upgrade
        )
        if effective_upgrade:
            deferred_paused_upgrade.discard(dep_key)

        scale_down_budget = max(0, live_admitted - upd.replicas) if upd.replicas < prev_rep else 0
        added_budget = live_admitted if effective_upgrade else scale_down_budget
        if added_budget > 0:
            expected_clean_retirements[dep_key] = min(
                live_admitted,
                expected_clean_retirements.get(dep_key, 0) + added_budget,
            )
            retire_eligible_wls.setdefault(dep_key, set()).update(pre_admitted_wls)
            if effective_upgrade:
                upgrade_active_deps.add(dep_key)
        elif not effective_upgrade and upd.replicas < prev_rep and expected_clean_retirements.get(dep_key, 0) == 0:
            expected_clean_retirements.pop(dep_key, None)

        if upd.replicas > prev_rep:
            total_pods_submitted += upd.replicas - prev_rep
        active_deployment_replicas[dep_key] = upd.replicas
        deployment_paused_state[dep_key] = bool(upd.paused)
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

    def _overdue_create_ts_by_key(now_el: float) -> dict[tuple[str, str, str, int], float]:
        """Return the target time of every due create that still waits on event_heap, by incarnation key."""
        out: dict[tuple[str, str, str, int], float] = {}
        for w in event_heap:
            if w.target_wall_offset_sec > now_el:
                continue
            w_key = (w.namespace, w.object_kind, w.name, w.incarnation_index)
            if w_key in cancelled_unsubmitted_keys:
                continue
            out[w_key] = min(out.get(w_key, w.target_wall_offset_sec), w.target_wall_offset_sec)
        return out

    def _held_behind_create(
        ev: ScheduledWorkflowUpdate | ScheduledWorkflowDeletion,
        overdue_creates: dict[tuple[str, str, str, int], float],
        *,
        strictly_after_create: bool,
    ) -> bool:
        """Return True when the create of the same incarnation is due earlier but did not run yet.

        A delete is held only when the create is strictly earlier: a delete at or before the create
        time cancels the unsubmitted create instead.
        """
        if not overdue_creates or getattr(ev, "initial_workload_name", ""):
            return False
        create_ts = overdue_creates.get((ev.namespace, ev.object_kind, ev.name, ev.incarnation_index))
        if create_ts is None:
            return False
        if strictly_after_create:
            return create_ts < ev.target_wall_offset_sec - 1e-9
        return create_ts <= ev.target_wall_offset_sec + 1e-9

    def _first_runnable_event(
        heap: list[Any],
        now_el: float,
        overdue_creates: dict[tuple[str, str, str, int], float],
        *,
        strictly_after_create: bool,
    ) -> Any | None:
        """Return the earliest due event of `heap` that is not held behind its own pending create."""
        if not heap or heap[0].target_wall_offset_sec > now_el:
            return None
        if not _held_behind_create(heap[0], overdue_creates, strictly_after_create=strictly_after_create):
            return heap[0]
        for ev in sorted(e for e in heap if e.target_wall_offset_sec <= now_el):
            if not _held_behind_create(ev, overdue_creates, strictly_after_create=strictly_after_create):
                return ev
        return None

    def _print_status_line(snap: ClusterStatusSnapshot, now_mono: float, note: str = "") -> None:
        wall_elapsed = now_mono - t0_mono
        sim_elapsed = wall_elapsed * speedup
        next_candidates: list[float] = []
        if event_heap:
            next_candidates.append(
                max(0.0, max(event_heap[0].target_wall_offset_sec, fifo_gate_wall_sec) - wall_elapsed)
            )
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
        _record_sched_wl_progress(snap, count_absence=True)
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
                if not stop_requested:
                    stop_reason = "duration_reached"
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
                    elif ie.object_kind == "Node" and ie.name:
                        if defer_reason == "not_found":
                            # The node no longer exists in the chamber; drop stale tracking
                            # without recording an un-applied capacity transition.
                            live_node_alloc.pop(ie.name, None)
                        elif ie.event_type == "Deleted":
                            if live_node_alloc.pop(ie.name, None) is not None:
                                _record_node_capacity_snapshot(time.time())
                        elif isinstance(ie.manifest, dict) and (
                            ie.event_type == "New"
                            or (
                                ie.event_type == "Update"
                                and getattr(ie, "has_explicit_capacity", False)
                                and ie.name in live_node_alloc
                            )
                        ):
                            # An Update changes the timeline only when its raw payload carried capacity,
                            # which is also the only case where replay patches it onto the node.
                            new_alloc = _extract_node_alloc(ie.manifest)
                            if new_alloc and new_alloc != live_node_alloc.get(ie.name):
                                live_node_alloc[ie.name] = new_alloc
                                _record_node_capacity_snapshot(time.time())
                continue

            # Drain due deletes, creates, and updates in chronological order so a stale update cannot
            # run after a later delete (or same-timestamp delete) and re-create the object via SSA.
            # Tie-break equal timestamps as delete -> create -> update (with terminated_incarnations
            # skipping any same-timestamp update for an already-deleted incarnation).
            # An update or delete whose own earlier create still waits on event_heap (behind
            # fifo_gate_wall_sec or the batch cap) is held until that create ran, so it cannot
            # overtake the create.
            now_el_ev = time.monotonic() - t0_mono
            overdue_creates = (
                _overdue_create_ts_by_key(now_el_ev) if (update_heap or delete_heap) and event_heap else {}
            )
            due_del = _first_runnable_event(delete_heap, now_el_ev, overdue_creates, strictly_after_create=True)
            due_upd = _first_runnable_event(update_heap, now_el_ev, overdue_creates, strictly_after_create=False)
            next_del_ts = due_del.target_wall_offset_sec if due_del is not None else None
            next_sub_ts = (
                event_heap[0].target_wall_offset_sec
                if (
                    now_el_ev >= fifo_gate_wall_sec
                    and event_heap
                    and event_heap[0].target_wall_offset_sec <= now_el_ev
                )
                else None
            )
            next_upd_ts = due_upd.target_wall_offset_sec if due_upd is not None else None
            due_kinds: list[tuple[float, int, str]] = []
            if next_del_ts is not None:
                due_kinds.append((round(next_del_ts, 6), 0, "delete"))
            if next_sub_ts is not None:
                due_kinds.append((round(next_sub_ts, 6), 1, "create"))
            if next_upd_ts is not None:
                due_kinds.append((round(next_upd_ts, 6), 2, "update"))

            if due_kinds:
                due_kinds.sort()
                win_kind = due_kinds[0][2]
                if win_kind == "delete":
                    delete_heap.remove(due_del)
                    heapq.heapify(delete_heap)
                    _execute_workflow_deletion(due_del, time.monotonic() - t0_mono)
                    continue
                if win_kind == "update":
                    update_heap.remove(due_upd)
                    heapq.heapify(update_heap)
                    _execute_workflow_update(due_upd, time.monotonic() - t0_mono)
                    continue

                # win_kind == "create": batch due creates up to any earlier due delete or update boundary.
                batch_cutoff = now_el_ev
                if delete_heap and delete_heap[0].target_wall_offset_sec <= batch_cutoff:
                    batch_cutoff = min(batch_cutoff, delete_heap[0].target_wall_offset_sec - 1e-6)
                if update_heap and update_heap[0].target_wall_offset_sec < batch_cutoff:
                    batch_cutoff = min(batch_cutoff, update_heap[0].target_wall_offset_sec)
                batch_cutoff = max(batch_cutoff, next_sub_ts)
                due_batch: list[ScheduledWorkflowItem] = []
                popped_cancelled = False
                while (
                    event_heap
                    and event_heap[0].target_wall_offset_sec <= batch_cutoff + 1e-9
                ):
                    cand_it = heapq.heappop(event_heap)
                    c_key = (cand_it.namespace, cand_it.object_kind, cand_it.name, cand_it.incarnation_index)
                    if c_key in cancelled_unsubmitted_keys:
                        cancelled_unsubmitted_keys.discard(c_key)
                        popped_cancelled = True
                        continue
                    due_batch.append(cand_it)
                    if len(due_batch) >= 200:
                        break
                if not due_batch and popped_cancelled:
                    continue
            else:
                due_batch = []

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
                    # Create only the earliest creation_epoch tier now. Later tiers go back onto event_heap
                    # unchanged and wait behind fifo_gate_wall_sec, which is set from the moment this tier
                    # was actually created. Infra/delete events, --duration and SIGINT stay responsive, a
                    # slow status poll cannot put two tiers into the same second, and a workflow that
                    # becomes due meanwhile is sorted behind the waiting tiers instead of passing them.
                    first_grp = groups[0]
                    mgr.apply_manifests_parallel(
                        [it_g.manifest for it_g in first_grp],
                        max_workers=min(24, max(1, len(first_grp))),
                    )
                    for grp in groups[1:]:
                        for it_g in grp:
                            heapq.heappush(event_heap, it_g)
                    fifo_gate_wall_sec = (time.monotonic() - t0_mono) + max(0.05, 1.02 - (time.time() % 1.0))
                    due_batch = first_grp
                elif len(distinct_c_epochs) > 1 and len(due_batch) <= 24:
                    mgr.apply_manifests_parallel([item.manifest for item in due_batch], max_workers=1)
                else:
                    mgr.apply_manifests_parallel([item.manifest for item in due_batch], max_workers=24)

                now_off = time.monotonic() - t0_mono
                for item in due_batch:
                    it_applied = str((item.manifest.get("metadata") or {}).get("name") or item.name)
                    if item.object_kind == "Deployment":
                        it_spec = item.manifest.get("spec") or {}
                        it_rep = int(it_spec.get("replicas", 1))
                        it_paused = bool(it_spec.get("paused", False))
                        dep_k = (item.namespace, it_applied)
                        known_deploy_names_by_ns.setdefault(item.namespace, set()).add(it_applied)
                        active_deployment_replicas[dep_k] = it_rep
                        deployment_paused_state[dep_k] = it_paused
                        deferred_paused_upgrade.discard(dep_k)
                        expected_clean_retirements.pop(dep_k, None)
                        retire_eligible_wls.pop(dep_k, None)
                        upgrade_active_deps.discard(dep_k)
                        if it_rep > 0:
                            submitted_non_cancelled[dep_k] = (
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
                        it_kind = str(item.manifest.get("kind") or item.object_kind)
                        submitted_batch_records[
                            (item.namespace, item.object_kind, item.name, item.incarnation_index)
                        ] = _SubmittedBatchRecord(
                            api_version=str(item.manifest.get("apiVersion") or ""),
                            kind=it_kind,
                            namespace=item.namespace,
                            applied_name=it_applied,
                            submitted_mono=t0_mono + now_off,
                            pre_existing_wl_keys=_snapshot_matching_wl_keys(item.namespace, it_kind, it_applied),
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
                        if not stop_requested:
                            stop_reason = "completed"
                        break
                    _reconcile_vanished_unseen_workflows(snap, now_mono)
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
                            if not stop_requested:
                                stop_reason = "completed"
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
                            if not stop_requested:
                                stop_reason = "completed"
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
                        if not stop_requested:
                            stop_reason = "completed"
                        break

            now_mono = time.monotonic()
            wall_elapsed = now_mono - t0_mono
            wake_candidates = [next_status_mono - now_mono]
            if (not event_heap and not update_heap and not delete_heap) or initial_wl_keys or known_deploy_names_by_ns:
                wake_candidates.append(next_poll_mono - now_mono)
            if event_heap:
                wake_candidates.append(max(event_heap[0].target_wall_offset_sec, fifo_gate_wall_sec) - wall_elapsed)
            # An update or delete held behind its own pending create wakes with that create (above).
            overdue_wake = (
                _overdue_create_ts_by_key(wall_elapsed) if (update_heap or delete_heap) and event_heap else {}
            )
            for wake_heap, wake_strict in ((update_heap, False), (delete_heap, True)):
                free_ts = [
                    e.target_wall_offset_sec
                    for e in wake_heap
                    if not _held_behind_create(e, overdue_wake, strictly_after_create=wake_strict)
                ]
                if free_ts:
                    wake_candidates.append(min(free_ts) - wall_elapsed)
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
    return ReplayResult(
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        submitted_count=submitted_count,
        total_pods=total_pods_submitted,
        stop_reason="interrupted" if stop_requested else stop_reason,
        signal_num=received_signal if stop_requested else None,
    )
