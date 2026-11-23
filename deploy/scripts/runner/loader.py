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

"""Scenario loading, timeline alignment, infra parsing, and object synthesis."""

from __future__ import annotations

from dataclasses import dataclass, field, replace as dc_replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
import time
from typing import Any

from chamber.constants import (
    DEFAULT_FALLBACK_EXEC_SEC,
    DEFAULT_INFRA_PODS_NAMESPACE,
    DEFAULT_PAUSE_IMAGE,
    ROOT_DIR,
    validate_dns_label,
)
from chamber.manifests import (
    KueueScenarioConfiguration,
    build_cohort_manifest,
    build_local_queue_manifest,
    build_namespace_manifest,
    build_priority_class_manifest,
    parse_kueue_configuration_payload,
)
from chamber.parsers import (
    format_pod_execution_seconds,
    multiply_k8s_quantity_exact,
    parse_k8s_quantity,
    sanitize_k8s_name,
)
from chamber.scenario import (
    ExecutionTimeIndex,
    InterruptedDurationOverride,
    WorkloadExecutionInfo,
    build_topology_assignment,
    collect_known_parent_workflows,
    collect_not_admitted_workflow_admit_epochs,
    extract_pod_spec_resources,
    filter_initial_running_pods,
    index_execution_times,
    is_child_workflow_owned_by_tracked_parent,
    parse_interrupted_duration_override,
)
from chamber.yaml_utils import load_jsonl_events
from runner.validation import (
    ALLOWED_INFRA_GVKS,
    MAX_K8S_NAME_LEN,
    SYSTEM_NAMESPACES,
    _extract_existing_pod_exec_seconds,
    _extract_workflow_true_duration_and_multiplier,
    _inject_pod_execution_time_and_sanitize_workflow,
    _parse_optional_event_epoch,
    _prepare_kueue_or_ns_doc,
    _prepare_node_manifest,
    _require_event_epoch,
    extract_node_ready_status,
    is_node_payload_unhealthy,
    validate_workflow_events,
)


DEFAULT_SCENARIO_DIR = ROOT_DIR / "testscenarios" / "demo"
DEFAULT_INFINITE_POD_DURATION_SEC = 864000.0  # 10 days (240h) for pods with no deletionTime


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
    trigger_reason: str = field(compare=False)  # "initial_not_admitted" | "preempted_after_initial_admission" | "post_t0_fast_admitted"
    execution_duration_sec: float = field(compare=False)
    compressed_duration_sec: float = field(compare=False)
    indexed_from_execution_time: bool = field(compare=False)
    pod_count: int = field(compare=False)
    manifest: dict[str, Any] = field(compare=False)
    creation_epoch: float = field(compare=False, default=0.0)
    incarnation_index: int = field(compare=False, default=0)
    has_explicit_pod_exec_annotation: bool = field(compare=False, default=False)


@dataclass(order=True)
class ScheduledWorkflowUpdate:
    """Scheduled spec mutation of an active Deployment workflow recorded as Update in workflows.json."""

    target_wall_offset_sec: float
    seq: int
    raw_offset_sec: float = field(compare=False)
    raw_offset_from_t0_sec: float = field(compare=False)
    event_timestamp: str = field(compare=False)
    object_kind: str = field(compare=False)
    name: str = field(compare=False)
    applied_name: str = field(compare=False)
    namespace: str = field(compare=False)
    local_queue_name: str = field(compare=False)
    incarnation_index: int = field(compare=False, default=0)
    replicas: int = field(compare=False, default=1)
    paused: bool = field(compare=False, default=False)
    template_hash: str = field(compare=False, default="")
    update_types: tuple[str, ...] = field(compare=False, default_factory=tuple)
    manifest: dict[str, Any] = field(compare=False, default_factory=dict)


@dataclass
class InitialDeploymentBootstrap:
    """Representation of an apps/v1 Deployment running (fully or partially admitted) at t0."""

    namespace: str
    name: str
    applied_name: str
    local_queue_name: str
    cluster_queue_name: str
    desired_replicas: int
    admitted_replicas: int
    paused: bool
    original_age_sec: float
    placements: list[str]
    per_pod_requests: dict[str, str]
    priority_class_name: str
    priority: int | None
    is_tas: bool
    manifest: dict[str, Any]
    cq_doc: dict[str, Any]
    flavors_by_name: dict[str, dict[str, Any]] = field(default_factory=dict)
    topologies_by_name: dict[str, dict[str, Any]] = field(default_factory=dict)
    nodes_by_name: dict[str, dict[str, Any]] = field(default_factory=dict)
    scraped_pod_set_assignments: list[dict[str, Any]] = field(default_factory=list)


@dataclass(order=True)
class ScheduledWorkflowDeletion:
    """Scheduled deletion of a workflow (or t0 PodGroup) recorded as Deleted in workflows.json."""

    target_wall_offset_sec: float
    seq: int
    raw_offset_sec: float = field(compare=False)
    raw_offset_from_t0_sec: float = field(compare=False)
    event_timestamp: str = field(compare=False)
    object_kind: str = field(compare=False)
    manifest_kind: str = field(compare=False)
    manifest_api_version: str = field(compare=False)
    name: str = field(compare=False)
    applied_name: str = field(compare=False)
    namespace: str = field(compare=False)
    delete_reason: str = field(compare=False, default="cancelled")  # "cancelled" | "before_recreate"
    initial_workload_name: str = field(compare=False, default="")
    incarnation_index: int = field(compare=False, default=0)
    has_later_incarnation: bool = field(compare=False, default=False)


@dataclass(order=True)
class ScheduledInfraEvent:
    """Post-t0 infrastructure lifecycle event (Node, Queue/Quota, Namespace, PriorityClass, or non-Kueue Pod)."""

    target_wall_offset_sec: float
    seq: int
    raw_offset_sec: float = field(compare=False)
    raw_offset_from_t0_sec: float = field(compare=False)
    event_timestamp: str = field(compare=False)
    event_type: str = field(compare=False)  # "New" | "Update" | "Deleted"
    object_kind: str = field(compare=False)
    name: str = field(compare=False)
    namespace: str = field(compare=False, default="")
    ready_status: str | None = field(compare=False, default=None)
    is_not_ready: bool = field(compare=False, default=False)
    has_no_execute_taint: bool = field(compare=False, default=False)
    manifest: dict[str, Any] = field(compare=False, default_factory=dict)


@dataclass
class InitialWorkloadPodGroup:
    """Representation of an already-admitted Kueue workload running at t0 as a pre-admitted PodGroup."""

    workload_name: str
    namespace: str
    local_queue_name: str
    cluster_queue_name: str
    owner_kind: str
    owner_name: str
    priority_class_name: str
    priority: int | None
    is_tas: bool
    pods: list[InitialPodSpec]
    workload_manifest: dict[str, Any]
    workload_status: dict[str, Any]
    resubmit_item: ScheduledWorkflowItem | None = None
    original_age_sec: float = 0.0


@dataclass
class _AdmittedInitialCandidate:
    namespace: str
    orig_namespace: str
    kind: str
    name: str
    local_queue_name: str
    raw_obj: dict[str, Any]
    exec_info: WorkloadExecutionInfo | None
    preempt_item: ScheduledWorkflowItem | None
    from_workflows_json: bool = True
    member_objs: list[dict[str, Any]] = field(default_factory=list)
    first_seen_epoch: float = 0.0
    creation_epoch: float | None = None
    deleted_epoch: float | None = None
    deleted_ts_str: str = ""
    incarnation_index: int = 0
    ready_replicas: int | None = None


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
    initial_workloads: list[InitialWorkloadPodGroup] = field(default_factory=list)
    initial_deployments: list[InitialDeploymentBootstrap] = field(default_factory=list)
    scheduled_infra_events: list[ScheduledInfraEvent] = field(default_factory=list)
    scheduled_workflow_updates: list[ScheduledWorkflowUpdate] = field(default_factory=list)
    scheduled_workflow_deletions: list[ScheduledWorkflowDeletion] = field(default_factory=list)
    interrupted_duration_multiplier: float | None = None
    interrupted_duration_override: InterruptedDurationOverride | None = None
    kueue_configuration: KueueScenarioConfiguration | None = None
    missing_duration_warnings: list[str] = field(default_factory=list)
    imputed_duration_count: int = 0

    @property
    def initial_workload_pods_count(self) -> int:
        return sum(len(w.pods) for w in self.initial_workloads) + sum(
            d.admitted_replicas for d in self.initial_deployments
        )


@dataclass
class _ParsedInfra:
    nodes_by_name: dict[str, dict[str, Any]]
    namespaces_by_name: dict[str, dict[str, Any]]
    runtime_classes_by_name: dict[str, dict[str, Any]]
    topologies_by_name: dict[str, dict[str, Any]]
    flavors_by_name: dict[str, dict[str, Any]]
    cohorts_by_name: dict[str, dict[str, Any]]
    priority_classes_by_name: dict[str, dict[str, Any]]
    cqs_by_name: dict[str, dict[str, Any]]
    lqs_by_key: dict[tuple[str, str], dict[str, Any]]
    pod_new_events: dict[tuple[str, str], tuple[float, dict[str, Any]]]
    pod_delete_epochs: dict[tuple[str, str], float]
    scheduled_infra_events: list[ScheduledInfraEvent] = field(default_factory=list)
    kueue_configuration: KueueScenarioConfiguration | None = None
    k8s_priority_classes_by_name: dict[str, dict[str, Any]] = field(default_factory=dict)
    post_t0_cq_names: set[str] = field(default_factory=set)
    post_t0_lq_keys: set[tuple[str, str]] = field(default_factory=set)


def _compute_timeline_bounds(
    infra_event_epochs: list[float],
    workflow_event_epochs: list[float],
    exec_index: ExecutionTimeIndex,
) -> tuple[float, str, float, str]:
    """Compute scenario start/end epochs and UTC ISO-8601 timestamps."""
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
        all_last_epochs.extend(w_info.interrupted_epochs)

    t0_epoch = min(all_first_epochs) if all_first_epochs else time.time()
    t_end_epoch = max(all_last_epochs) if all_last_epochs else t0_epoch
    if t_end_epoch < t0_epoch:
        t_end_epoch = t0_epoch
    t0_iso = datetime.fromtimestamp(t0_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    t_end_iso = datetime.fromtimestamp(t_end_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return t0_epoch, t0_iso, t_end_epoch, t_end_iso


@dataclass
class _WorkflowIncarnation:
    inc_id: int
    ns: str
    orig_ns: str
    kind: str
    name: str
    parent_js: str | None
    first_seen_epoch: float
    first_seen_state: str
    creation_epoch: float | None
    raw_obj: dict[str, Any]
    lq_name: str
    scheduled_item: ScheduledWorkflowItem | None = None
    deleted_epoch: float | None = None
    deleted_ts_str: str = ""
    member_objs: dict[str, dict[str, Any]] = field(default_factory=dict)
    ready_replicas: int | None = None
    last_spec_digest: str = ""
    last_prepared_manifest: dict[str, Any] = field(default_factory=dict)
    updates: list[ScheduledWorkflowUpdate] = field(default_factory=list)


def _dict_has_kueue_domain(d: Any) -> bool:
    if not isinstance(d, dict):
        return False
    return any("kueue.x-k8s.io/" in str(k) for k in d)


def _has_kueue_queue_or_domain(raw_obj: dict[str, Any] | None, lq_name: str) -> bool:
    if lq_name:
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


def _default_manifest_gvk_for_kind(kind: str) -> tuple[str, str]:
    if kind in ("JobSet", "LeaderWorkerSet", "RayJob", "RayCluster"):
        return ("jobset.x-k8s.io/v1alpha2", "JobSet")
    if kind == "Job":
        return ("batch/v1", "Job")
    if kind == "Deployment":
        return ("apps/v1", "Deployment")
    return ("v1", "Pod")


def _extract_deployment_template_hash(manifest: dict[str, Any]) -> str:
    spec = manifest.get("spec") or {}
    tpl = spec.get("template") or {}
    t_meta = tpl.get("metadata") or {}
    t_ann = t_meta.get("annotations") or {}
    return str(t_ann.get("kueue-optimizer.gke.io/template-hash") or "")


def _deployment_spec_digest(manifest: dict[str, Any]) -> str:
    meta = manifest.get("metadata") or {}
    labels = {
        str(k): str(v)
        for k, v in (meta.get("labels") or {}).items()
        if str(k) != "kueue-optimizer.gke.io/testrunner"
    }
    anns = {
        str(k): str(v)
        for k, v in (meta.get("annotations") or {}).items()
        if str(k)
        not in (
            "kueue-optimizer.gke.io/pod-execution-time-seconds",
            "kueue-optimizer.gke.io/real-duration-sec",
            "kueue-optimizer.gke.io/compressed-duration-sec",
        )
    }
    return json.dumps(
        {
            "spec": manifest.get("spec") or {},
            "labels": labels,
            "annotations": anns,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _classify_deployment_spec_update(
    prev_manifest: dict[str, Any],
    new_manifest: dict[str, Any],
    explicit_types: Any = None,
) -> tuple[str, ...]:
    prev_spec = prev_manifest.get("spec") or {}
    new_spec = new_manifest.get("spec") or {}
    prev_rep = int(prev_spec["replicas"]) if prev_spec.get("replicas") is not None else 1
    new_rep = int(new_spec["replicas"]) if new_spec.get("replicas") is not None else 1
    prev_paused = bool(prev_spec.get("paused", False))
    new_paused = bool(new_spec.get("paused", False))
    prev_tpl_sig = json.dumps(prev_spec.get("template") or {}, sort_keys=True, separators=(",", ":"))
    new_tpl_sig = json.dumps(new_spec.get("template") or {}, sort_keys=True, separators=(",", ":"))
    prev_strat_sig = json.dumps(prev_spec.get("strategy") or {}, sort_keys=True, separators=(",", ":"))
    new_strat_sig = json.dumps(new_spec.get("strategy") or {}, sort_keys=True, separators=(",", ":"))

    inferred: list[str] = []
    if new_rep > prev_rep:
        inferred.append("ScaleUp")
    elif new_rep < prev_rep:
        inferred.append("ScaleDown")
    if new_tpl_sig != prev_tpl_sig:
        inferred.append("Upgrade")
    if not prev_paused and new_paused:
        inferred.append("Paused")
    elif prev_paused and not new_paused:
        inferred.append("Resumed")
    if new_strat_sig != prev_strat_sig:
        inferred.append("StrategyChange")

    if isinstance(explicit_types, list):
        explicit_clean = [
            str(x)
            for x in explicit_types
            if str(x) and str(x) != "StateTransition"
        ]
        for inf in inferred:
            if inf not in explicit_clean:
                explicit_clean.append(inf)
        if explicit_clean:
            return tuple(explicit_clean)
    return tuple(inferred) if inferred else ("SpecUpdate",)


def _schedule_workflows(
    workflow_events: list[dict[str, Any]],
    workflow_event_epochs: list[float],
    known_parents: set[tuple[str, str, str]],
    exec_index: ExecutionTimeIndex,
    *,
    t0_epoch: float,
    speedup: float,
    min_pod_duration_sec: float,
    default_exec_sec: float,
    pause_image_override: str | None,
    override_workload_namespace: str | None,
    interrupted_duration_multiplier: float | None = None,
    interrupted_duration_override: InterruptedDurationOverride | None = None,
    manage_jobs_without_queue_name: bool = False,
) -> tuple[
    list[ScheduledWorkflowItem],
    list[ScheduledWorkflowUpdate],
    list[ScheduledWorkflowDeletion],
    list[_AdmittedInitialCandidate],
    list[tuple[str, str, str]],
    int,
]:
    """Track workflow lifecycles (including Deployment updates, deletions, and re-creations) chronologically."""
    from chamber.parsers import parse_rfc3339_epoch

    incarnations_by_key: dict[tuple[str, str, str], list[_WorkflowIncarnation]] = {}
    active_inc_by_key: dict[tuple[str, str, str], _WorkflowIncarnation] = {}
    skipped_child_count = 0
    seq = 0
    upd_seq = 0

    for idx_wf, ev in enumerate(workflow_events):
        ev_type = str(ev.get("eventType") or "")
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

        ev_creation_ep = parse_rfc3339_epoch(pl.get("creationTime") or raw_meta.get("creationTimestamp"))

        is_child, parent_js = is_child_workflow_owned_by_tracked_parent(raw_obj, kind, ns, known_parents)
        if is_child:
            if ev_type == "New":
                skipped_child_count += 1
            continue

        pg_name = (
            _get_pod_meta_annotation_or_label(raw_meta, "kueue.x-k8s.io/pod-group-name")
            if kind == "Pod"
            else ""
        )

        if ev_type == "Deleted":
            if pg_name:
                pg_key = (ns, "PodGroup", pg_name)
                cur_pg = active_inc_by_key.get(pg_key)
                if cur_pg is not None:
                    cur_pg.member_objs.pop(name, None)
                    if not cur_pg.member_objs:
                        cur_pg.deleted_epoch = ev_epoch
                        cur_pg.deleted_ts_str = ev_ts_str
                        active_inc_by_key.pop(pg_key, None)
                        continue
            base_key_del = (ns, kind, name)
            cur_del = active_inc_by_key.get(base_key_del)
            if cur_del is not None:
                cur_del.deleted_epoch = ev_epoch
                cur_del.deleted_ts_str = ev_ts_str
                active_inc_by_key.pop(base_key_del, None)
            continue

        # Infer workloadState if reading an older trace without workloadState field
        if not wl_state:
            suspend_val = (raw_obj.get("spec") or {}).get("suspend")
            wl_state = "NotAdmitted" if suspend_val is True else "Admitted"

        # Filter non-Kueue / system-namespace workflows that carry no queue, Kueue metadata, or execution-time record
        if not _has_kueue_queue_or_domain(raw_obj, lq_name) and not pg_name:
            info_chk = exec_index.lookup(
                orig_ns,
                kind,
                name,
                parent_jobset_name=parent_js,
                creation_epoch=ev_creation_ep,
            )
            if info_chk is None and (orig_ns in SYSTEM_NAMESPACES or not manage_jobs_without_queue_name):
                continue

        if pg_name:
            pg_key = (ns, "PodGroup", pg_name)
            cur_pg = active_inc_by_key.get(pg_key)
            if (cur_pg is not None and cur_pg.first_seen_state != "NotAdmitted") or (
                cur_pg is None and wl_state != "NotAdmitted"
            ):
                if cur_pg is None:
                    pg_list = incarnations_by_key.setdefault(pg_key, [])
                    cur_pg = _WorkflowIncarnation(
                        inc_id=len(pg_list),
                        ns=ns,
                        orig_ns=orig_ns,
                        kind="PodGroup",
                        name=pg_name,
                        parent_js=None,
                        first_seen_epoch=ev_epoch,
                        first_seen_state=wl_state,
                        creation_epoch=ev_creation_ep,
                        raw_obj=raw_obj,
                        lq_name=lq_name,
                    )
                    pg_list.append(cur_pg)
                    active_inc_by_key[pg_key] = cur_pg
                cur_pg.raw_obj = raw_obj
                cur_pg.lq_name = lq_name or cur_pg.lq_name
                cur_pg.member_objs[name] = raw_obj
                continue

        key = (ns, kind, name)
        cur_inc = active_inc_by_key.get(key)
        if cur_inc is not None and ev_type == "New":
            if (
                cur_inc.creation_epoch is not None
                and ev_creation_ep is not None
                and ev_creation_ep > cur_inc.creation_epoch + 0.5
            ):
                active_inc_by_key.pop(key, None)
                cur_inc = None

        if cur_inc is None:
            inc_list = incarnations_by_key.setdefault(key, [])
            init_rr: int | None = None
            if kind == "Deployment" and pl.get("readyReplicas") is not None:
                try:
                    init_rr = max(0, int(pl["readyReplicas"]))
                except (TypeError, ValueError):
                    init_rr = None
            cur_inc = _WorkflowIncarnation(
                inc_id=len(inc_list),
                ns=ns,
                orig_ns=orig_ns,
                kind=kind,
                name=name,
                parent_js=parent_js,
                first_seen_epoch=ev_epoch,
                first_seen_state=wl_state,
                creation_epoch=ev_creation_ep,
                raw_obj=raw_obj,
                lq_name=lq_name,
                ready_replicas=init_rr,
            )
            if kind == "Deployment":
                init_dep_doc, _ = _inject_pod_execution_time_and_sanitize_workflow(
                    raw_obj=raw_obj,
                    exec_str="1",
                    raw_duration_sec=1.0,
                    compressed_duration_sec=1.0,
                    pause_image_override=pause_image_override,
                    override_namespace=override_workload_namespace,
                    speedup=speedup,
                    min_pod_duration_sec=min_pod_duration_sec,
                )
                cur_inc.last_prepared_manifest = init_dep_doc
                cur_inc.last_spec_digest = _deployment_spec_digest(init_dep_doc)
            inc_list.append(cur_inc)
            active_inc_by_key[key] = cur_inc
        else:
            if cur_inc.creation_epoch is None and ev_creation_ep is not None:
                cur_inc.creation_epoch = ev_creation_ep
            cur_inc.lq_name = lq_name or cur_inc.lq_name
            if kind == "Deployment":
                upd_doc, _ = _inject_pod_execution_time_and_sanitize_workflow(
                    raw_obj=raw_obj,
                    exec_str="1",
                    raw_duration_sec=1.0,
                    compressed_duration_sec=1.0,
                    pause_image_override=pause_image_override,
                    override_namespace=override_workload_namespace,
                    speedup=speedup,
                    min_pod_duration_sec=min_pod_duration_sec,
                )
                upd_digest = _deployment_spec_digest(upd_doc)
                if ev_epoch <= t0_epoch + 1e-6 and cur_inc.scheduled_item is None:
                    cur_inc.raw_obj = raw_obj
                    cur_inc.last_prepared_manifest = upd_doc
                    cur_inc.last_spec_digest = upd_digest
                    if pl.get("readyReplicas") is not None:
                        try:
                            cur_inc.ready_replicas = max(0, int(pl["readyReplicas"]))
                        except (TypeError, ValueError):
                            pass
                elif upd_digest != cur_inc.last_spec_digest:
                    upd_types = _classify_deployment_spec_update(
                        cur_inc.last_prepared_manifest,
                        upd_doc,
                        pl.get("updateTypes"),
                    )
                    cur_inc.last_prepared_manifest = upd_doc
                    cur_inc.last_spec_digest = upd_digest
                    upd_spec = upd_doc.get("spec") or {}
                    upd_rep = int(upd_spec["replicas"]) if upd_spec.get("replicas") is not None else 1
                    upd_paused = bool(upd_spec.get("paused", False))
                    upd_tpl_hash = str(pl.get("templateHash") or _extract_deployment_template_hash(upd_doc))
                    applied_upd_name = str((upd_doc.get("metadata") or {}).get("name") or name)
                    raw_upd_off = max(0.0, ev_epoch - t0_epoch)
                    wall_upd_off = raw_upd_off / speedup
                    cur_inc.updates.append(
                        ScheduledWorkflowUpdate(
                            target_wall_offset_sec=wall_upd_off,
                            seq=upd_seq,
                            raw_offset_sec=raw_upd_off,
                            raw_offset_from_t0_sec=raw_upd_off,
                            event_timestamp=ev_ts_str,
                            object_kind=kind,
                            name=name,
                            applied_name=applied_upd_name,
                            namespace=ns,
                            local_queue_name=cur_inc.lq_name,
                            incarnation_index=cur_inc.inc_id,
                            replicas=upd_rep,
                            paused=upd_paused,
                            template_hash=upd_tpl_hash,
                            update_types=upd_types,
                            manifest=upd_doc,
                        )
                    )
                    upd_seq += 1
            else:
                cur_inc.raw_obj = raw_obj

        if cur_inc.scheduled_item is not None:
            continue

        if wl_state == "NotAdmitted":
            trigger_reason = (
                "initial_not_admitted"
                if cur_inc.first_seen_state == "NotAdmitted"
                else "preempted_after_initial_admission"
            )
            existing_pod_ann = _extract_existing_pod_exec_seconds(raw_obj)
            has_explicit_pod_ann = existing_pod_ann is not None
            fallback_dur = existing_pod_ann or default_exec_sec
            wf_true_dur, wf_mult = _extract_workflow_true_duration_and_multiplier(raw_obj)
            found_in_idx = False
            raw_dur = fallback_dur
            if pg_name:
                raw_dur, found_in_idx = exec_index.resolve_duration_sec(
                    namespace=orig_ns,
                    object_kind="PodGroup",
                    name=pg_name,
                    fallback_sec=fallback_dur,
                    interrupted_duration_multiplier=interrupted_duration_multiplier,
                    interrupted_duration_override=interrupted_duration_override,
                    workflow_true_duration_sec=wf_true_dur,
                    workflow_missing_multiplier=wf_mult,
                    creation_epoch=cur_inc.creation_epoch,
                    incarnation_index=cur_inc.inc_id,
                )
            if not found_in_idx:
                raw_dur, found_in_idx = exec_index.resolve_duration_sec(
                    namespace=orig_ns,
                    object_kind=kind,
                    name=name,
                    fallback_sec=fallback_dur,
                    parent_jobset_name=parent_js,
                    interrupted_duration_multiplier=interrupted_duration_multiplier,
                    interrupted_duration_override=interrupted_duration_override,
                    workflow_true_duration_sec=wf_true_dur,
                    workflow_missing_multiplier=wf_mult,
                    creation_epoch=cur_inc.creation_epoch,
                    incarnation_index=cur_inc.inc_id,
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
                speedup=speedup,
                scale_by_waves=bool(found_in_idx or not has_explicit_pod_ann),
                min_pod_duration_sec=min_pod_duration_sec,
            )
            raw_off = max(0.0, ev_epoch - t0_epoch)
            wall_off = raw_off / speedup
            eff_creation_ep = cur_inc.creation_epoch if cur_inc.creation_epoch is not None else ev_epoch
            cur_inc.scheduled_item = ScheduledWorkflowItem(
                target_wall_offset_sec=wall_off,
                seq=seq,
                raw_offset_sec=raw_off,
                raw_offset_from_t0_sec=raw_off,
                event_timestamp=ev_ts_str,
                object_kind=kind,
                name=name,
                namespace=ns,
                local_queue_name=cur_inc.lq_name,
                workload_state=wl_state,
                trigger_reason=trigger_reason,
                execution_duration_sec=raw_dur,
                compressed_duration_sec=comp_dur,
                indexed_from_execution_time=found_in_idx,
                pod_count=pod_cnt,
                manifest=manifest,
                creation_epoch=eff_creation_ep,
                incarnation_index=cur_inc.inc_id,
                has_explicit_pod_exec_annotation=has_explicit_pod_ann,
            )
            seq += 1

    all_scheduled_items: list[ScheduledWorkflowItem] = []
    scheduled_updates: list[ScheduledWorkflowUpdate] = []
    scheduled_deletions: list[ScheduledWorkflowDeletion] = []
    admitted_candidates: list[_AdmittedInitialCandidate] = []
    skipped_admitted: list[tuple[str, str, str]] = []
    del_seq = 0

    for key, incs in incarnations_by_key.items():
        for inc in incs:
            # Drop any incarnation that was both created and deleted at or before t0.
            if inc.deleted_epoch is not None and inc.deleted_epoch <= t0_epoch + 1e-6:
                continue

            if inc.updates:
                scheduled_updates.extend(inc.updates)

            info = exec_index.lookup(
                inc.orig_ns,
                inc.kind,
                inc.name,
                parent_jobset_name=inc.parent_js,
                creation_epoch=inc.creation_epoch,
                incarnation_index=inc.inc_id,
            )

            # Check if a workload first seen Admitted was recorded as preempted/interrupted in execution-time.json
            # even if workflows.json lacked a separate Update(NotAdmitted) event.
            # Deployments running at t0 are bootstrapped as real apps/v1 Deployments, so ReplicaSet handles pod replacement.
            if inc.scheduled_item is None and inc.first_seen_state != "NotAdmitted" and inc.kind not in ("PodGroup", "Deployment"):
                cand_int_epochs = (info.preempted_epochs or info.interrupted_epochs) if info else []
                if info and cand_int_epochs:
                    preempt_epoch = min(cand_int_epochs)
                    existing_pod_ann = _extract_existing_pod_exec_seconds(inc.raw_obj)
                    has_explicit_pod_ann = existing_pod_ann is not None
                    fallback_dur = existing_pod_ann or default_exec_sec
                    wf_true_dur, wf_mult = _extract_workflow_true_duration_and_multiplier(inc.raw_obj)
                    raw_dur = info.resolved_duration_sec(
                        default_sec=fallback_dur,
                        interrupted_duration_multiplier=interrupted_duration_multiplier,
                        interrupted_duration_override=interrupted_duration_override,
                        workflow_true_duration_sec=wf_true_dur,
                        workflow_missing_multiplier=wf_mult,
                    )
                    comp_dur = max(min_pod_duration_sec, raw_dur / speedup)
                    exec_str = format_pod_execution_seconds(comp_dur, min_seconds=min_pod_duration_sec)
                    manifest, pod_cnt = _inject_pod_execution_time_and_sanitize_workflow(
                        raw_obj=inc.raw_obj,
                        exec_str=exec_str,
                        raw_duration_sec=raw_dur,
                        compressed_duration_sec=comp_dur,
                        pause_image_override=pause_image_override,
                        override_namespace=override_workload_namespace,
                        speedup=speedup,
                        scale_by_waves=True,
                        min_pod_duration_sec=min_pod_duration_sec,
                    )
                    raw_off = max(0.0, preempt_epoch - t0_epoch)
                    wall_off = raw_off / speedup
                    preempt_iso = datetime.fromtimestamp(preempt_epoch, tz=timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    )
                    eff_creation_ep = inc.creation_epoch if inc.creation_epoch is not None else inc.first_seen_epoch
                    inc.scheduled_item = ScheduledWorkflowItem(
                        target_wall_offset_sec=wall_off,
                        seq=seq,
                        raw_offset_sec=raw_off,
                        raw_offset_from_t0_sec=raw_off,
                        event_timestamp=preempt_iso,
                        object_kind=inc.kind,
                        name=inc.name,
                        namespace=inc.ns,
                        local_queue_name=inc.lq_name,
                        workload_state="NotAdmitted",
                        trigger_reason="preempted_after_initial_admission",
                        execution_duration_sec=raw_dur,
                        compressed_duration_sec=comp_dur,
                        indexed_from_execution_time=True,
                        pod_count=pod_cnt,
                        manifest=manifest,
                        creation_epoch=eff_creation_ep,
                        incarnation_index=inc.inc_id,
                        has_explicit_pod_exec_annotation=has_explicit_pod_ann,
                    )
                    seq += 1

            if inc.scheduled_item is not None:
                all_scheduled_items.append(inc.scheduled_item)
            else:
                skipped_admitted.append(key)

            if inc.first_seen_state != "NotAdmitted":
                admitted_candidates.append(
                    _AdmittedInitialCandidate(
                        namespace=inc.ns,
                        orig_namespace=inc.orig_ns,
                        kind=inc.kind,
                        name=inc.name,
                        local_queue_name=inc.lq_name,
                        raw_obj=inc.raw_obj,
                        exec_info=info,
                        preempt_item=inc.scheduled_item,
                        from_workflows_json=True,
                        member_objs=list(inc.member_objs.values()),
                        first_seen_epoch=inc.first_seen_epoch,
                        creation_epoch=inc.creation_epoch,
                        deleted_epoch=inc.deleted_epoch,
                        deleted_ts_str=inc.deleted_ts_str,
                        incarnation_index=inc.inc_id,
                        ready_replicas=inc.ready_replicas,
                    )
                )

            has_later_incarnation = (inc.inc_id + 1) < len(incs)
            eff_del_epoch = inc.deleted_epoch
            eff_del_ts_str = inc.deleted_ts_str
            if eff_del_epoch is None and has_later_incarnation:
                next_ep = incs[inc.inc_id + 1].first_seen_epoch
                eff_del_epoch = max(t0_epoch + 0.01, next_ep - 0.05)
                eff_del_ts_str = datetime.fromtimestamp(eff_del_epoch, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )

            if eff_del_epoch is not None and eff_del_epoch > t0_epoch + 1e-6:
                completed_cleanly = bool(
                    info is not None
                    and info.full_duration_sec is not None
                    and info.full_duration_sec > 0
                    and not any(r == "Deleted" for r in info.interruption_reasons)
                    and (info.full_end_epoch is None or info.full_end_epoch <= eff_del_epoch + 1.0)
                )
                # Replay workflow deletions when the workflow was cancelled/killed before completing OR
                # when a subsequent incarnation with the same (namespace, kind, name) is created later.
                # Normal TTL garbage-collection after clean completion (with no re-creation) is skipped so
                # chamber timing differences never prematurely kill a job that completed in the source cluster.
                if (not completed_cleanly) or has_later_incarnation:
                    if inc.scheduled_item is not None:
                        m_api = str(inc.scheduled_item.manifest.get("apiVersion") or "")
                        m_kind = str(inc.scheduled_item.manifest.get("kind") or "")
                        applied_name = str(
                            (inc.scheduled_item.manifest.get("metadata") or {}).get("name") or inc.name
                        )
                    else:
                        m_api, m_kind = _default_manifest_gvk_for_kind(inc.kind)
                        tmp_doc, _ = _inject_pod_execution_time_and_sanitize_workflow(
                            raw_obj=inc.raw_obj,
                            exec_str="1",
                            raw_duration_sec=1.0,
                            compressed_duration_sec=1.0,
                            pause_image_override=pause_image_override,
                            override_namespace=override_workload_namespace,
                            speedup=speedup,
                        ) if inc.raw_obj and inc.kind != "PodGroup" else ({}, 1)
                        if tmp_doc:
                            m_api = str(tmp_doc.get("apiVersion") or m_api)
                            m_kind = str(tmp_doc.get("kind") or m_kind)
                            applied_name = str((tmp_doc.get("metadata") or {}).get("name") or inc.name)
                        else:
                            applied_name = inc.name
                    raw_del_off = max(0.0, eff_del_epoch - t0_epoch)
                    wall_del_off = raw_del_off / speedup
                    scheduled_deletions.append(
                        ScheduledWorkflowDeletion(
                            target_wall_offset_sec=wall_del_off,
                            seq=del_seq,
                            raw_offset_sec=raw_del_off,
                            raw_offset_from_t0_sec=raw_del_off,
                            event_timestamp=eff_del_ts_str,
                            object_kind=inc.kind,
                            manifest_kind=m_kind,
                            manifest_api_version=m_api,
                            name=inc.name,
                            applied_name=applied_name,
                            namespace=inc.ns,
                            delete_reason="before_recreate" if (completed_cleanly and has_later_incarnation) else "cancelled",
                            incarnation_index=inc.inc_id,
                            has_later_incarnation=has_later_incarnation,
                        )
                    )
                    del_seq += 1

    scheduled_workflows = sorted(
        all_scheduled_items,
        key=lambda item: (
            round(item.target_wall_offset_sec, 6),
            item.creation_epoch,
            item.raw_offset_from_t0_sec,
            item.seq,
        ),
    )
    scheduled_updates.sort(key=lambda u: (u.target_wall_offset_sec, u.seq))
    scheduled_deletions.sort(key=lambda d: (d.target_wall_offset_sec, d.seq))
    return (
        scheduled_workflows,
        scheduled_updates,
        scheduled_deletions,
        admitted_candidates,
        skipped_admitted,
        skipped_child_count,
    )


def _parse_infra_resources(
    infra_events: list[dict[str, Any]],
    infra_event_epochs: list[float],
    inf_file: Path,
    *,
    t0_epoch: float,
    initial_window_sec: float,
    speedup: float,
    min_pod_duration_sec: float = 0.1,
    infinite_pod_duration_sec: float = DEFAULT_INFINITE_POD_DURATION_SEC,
    pause_image_override: str | None = None,
    infra_pods_namespace: str = DEFAULT_INFRA_PODS_NAMESPACE,
    override_workload_namespace: str | None,
    override_queueing_strategy: str | None,
    override_cq_fair_sharing_preemption: bool,
) -> _ParsedInfra:
    """Validate and parse infra.json into t0 state, post-t0 infra/queue/pod events, and raw Pod events."""
    nodes_by_name: dict[str, dict[str, Any]] = {}
    node_idx_by_name: dict[str, int] = {}
    scheduled_infra_events: list[ScheduledInfraEvent] = []
    infra_seq = 0
    namespaces_by_name: dict[str, dict[str, Any]] = {}
    runtime_classes_by_name: dict[str, dict[str, Any]] = {}
    k8s_priority_classes_by_name: dict[str, dict[str, Any]] = {}
    topologies_by_name: dict[str, dict[str, Any]] = {}
    flavors_by_name: dict[str, dict[str, Any]] = {}
    cohorts_by_name: dict[str, dict[str, Any]] = {}
    priority_classes_by_name: dict[str, dict[str, Any]] = {}
    cqs_by_name: dict[str, dict[str, Any]] = {}
    lqs_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    post_t0_cq_names: set[str] = set()
    post_t0_lq_keys: set[tuple[str, str]] = set()
    pod_new_events: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
    pod_delete_epochs: dict[tuple[str, str], float] = {}
    kueue_configuration: KueueScenarioConfiguration | None = None

    def _enqueue_post_t0_infra(
        ev_epoch_val: float,
        ev_ts_val: str,
        ev_type_val: str,
        obj_kind_val: str,
        obj_name_val: str,
        obj_ns_val: str = "",
        doc_val: dict[str, Any] | None = None,
        ready_st: str | None = None,
        not_ready: bool = False,
        no_exec: bool = False,
    ) -> None:
        nonlocal infra_seq
        raw_off = max(0.0, ev_epoch_val - t0_epoch)
        wall_off = raw_off / max(1e-6, speedup)
        scheduled_infra_events.append(
            ScheduledInfraEvent(
                target_wall_offset_sec=wall_off,
                seq=infra_seq,
                raw_offset_sec=raw_off,
                raw_offset_from_t0_sec=raw_off,
                event_timestamp=ev_ts_val,
                event_type=ev_type_val,
                object_kind=obj_kind_val,
                name=obj_name_val,
                namespace=obj_ns_val,
                ready_status=ready_st,
                is_not_ready=not_ready,
                has_no_execute_taint=no_exec,
                manifest=doc_val or {},
            )
        )
        infra_seq += 1

    for idx_inf, ev in enumerate(infra_events):
        ev_type = str(ev.get("eventType") or "")
        ev_epoch = infra_event_epochs[idx_inf]
        ev_ts_str = str(ev.get("timestamp") or "")
        in_initial_window = ev_epoch <= (t0_epoch + initial_window_sec + 1e-6)
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
            if n_name not in node_idx_by_name:
                node_idx_by_name[n_name] = len(node_idx_by_name)
            n_idx = node_idx_by_name[n_name]
            if in_initial_window:
                if ev_type == "Deleted":
                    nodes_by_name.pop(n_name, None)
                else:
                    nodes_by_name[n_name] = _prepare_node_manifest(pl, n_idx)
            else:
                ready_st = extract_node_ready_status(pl)
                not_ready, no_exec = is_node_payload_unhealthy(pl)
                prep_node = _prepare_node_manifest(pl, n_idx) if ev_type != "Deleted" else {}
                _enqueue_post_t0_infra(
                    ev_epoch,
                    ev_ts_str,
                    ev_type,
                    "Node",
                    n_name,
                    doc_val=prep_node,
                    ready_st=ready_st,
                    not_ready=not_ready,
                    no_exec=no_exec,
                )

        elif kind == "Namespace":
            ns_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} Namespace metadata.name"
            )
            if ns_name not in SYSTEM_NAMESPACES:
                if in_initial_window:
                    if ev_type == "Deleted":
                        namespaces_by_name.pop(ns_name, None)
                    else:
                        namespaces_by_name[ns_name] = _prepare_kueue_or_ns_doc(pl)
                else:
                    doc = _prepare_kueue_or_ns_doc(pl) if ev_type != "Deleted" else {}
                    _enqueue_post_t0_infra(ev_epoch, ev_ts_str, ev_type, "Namespace", ns_name, doc_val=doc)

        elif kind == "RuntimeClass":
            rc_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} RuntimeClass metadata.name"
            )
            if in_initial_window:
                if ev_type == "Deleted":
                    runtime_classes_by_name.pop(rc_name, None)
                else:
                    runtime_classes_by_name[rc_name] = _prepare_kueue_or_ns_doc(pl)
            else:
                doc = _prepare_kueue_or_ns_doc(pl) if ev_type != "Deleted" else {}
                _enqueue_post_t0_infra(ev_epoch, ev_ts_str, ev_type, "RuntimeClass", rc_name, doc_val=doc)

        elif kind == "PriorityClass":
            pc_k8s_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} PriorityClass metadata.name"
            )
            if pc_k8s_name not in ("system-cluster-critical", "system-node-critical"):
                if in_initial_window:
                    if ev_type == "Deleted":
                        k8s_priority_classes_by_name.pop(pc_k8s_name, None)
                    else:
                        k8s_priority_classes_by_name[pc_k8s_name] = _prepare_kueue_or_ns_doc(pl)
                else:
                    doc = _prepare_kueue_or_ns_doc(pl) if ev_type != "Deleted" else {}
                    _enqueue_post_t0_infra(ev_epoch, ev_ts_str, ev_type, "PriorityClass", pc_k8s_name, doc_val=doc)

        elif kind == "Configuration":
            if ev_type == "Deleted":
                kueue_configuration = None
            else:
                kueue_configuration = parse_kueue_configuration_payload(
                    pl,
                    source_desc=f"{inf_file} event #{idx_inf} Configuration",
                )

        elif kind == "Topology":
            t_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} Topology metadata.name"
            )
            if in_initial_window:
                if ev_type == "Deleted":
                    topologies_by_name.pop(t_name, None)
                else:
                    topologies_by_name[t_name] = _prepare_kueue_or_ns_doc(pl)
            else:
                doc = _prepare_kueue_or_ns_doc(pl) if ev_type != "Deleted" else {}
                _enqueue_post_t0_infra(ev_epoch, ev_ts_str, ev_type, "Topology", t_name, doc_val=doc)

        elif kind == "ResourceFlavor":
            rf_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} ResourceFlavor metadata.name"
            )
            if in_initial_window:
                if ev_type == "Deleted":
                    flavors_by_name.pop(rf_name, None)
                else:
                    flavors_by_name[rf_name] = _prepare_kueue_or_ns_doc(pl)
            else:
                doc = _prepare_kueue_or_ns_doc(pl) if ev_type != "Deleted" else {}
                _enqueue_post_t0_infra(ev_epoch, ev_ts_str, ev_type, "ResourceFlavor", rf_name, doc_val=doc)

        elif kind == "Cohort":
            c_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} Cohort metadata.name"
            )
            if in_initial_window:
                if ev_type == "Deleted":
                    cohorts_by_name.pop(c_name, None)
                else:
                    cohorts_by_name[c_name] = _prepare_kueue_or_ns_doc(pl)
            else:
                doc = _prepare_kueue_or_ns_doc(pl) if ev_type != "Deleted" else {}
                _enqueue_post_t0_infra(ev_epoch, ev_ts_str, ev_type, "Cohort", c_name, doc_val=doc)

        elif kind == "WorkloadPriorityClass":
            pc_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"),
                f"{inf_file} event #{idx_inf} WorkloadPriorityClass metadata.name",
            )
            if in_initial_window:
                if ev_type == "Deleted":
                    priority_classes_by_name.pop(pc_name, None)
                else:
                    priority_classes_by_name[pc_name] = _prepare_kueue_or_ns_doc(pl)
            else:
                doc = _prepare_kueue_or_ns_doc(pl) if ev_type != "Deleted" else {}
                _enqueue_post_t0_infra(ev_epoch, ev_ts_str, ev_type, "WorkloadPriorityClass", pc_name, doc_val=doc)

        elif kind == "ClusterQueue":
            cq_name = validate_dns_label(
                (pl.get("metadata") or {}).get("name"), f"{inf_file} event #{idx_inf} ClusterQueue metadata.name"
            )
            if in_initial_window:
                if ev_type == "Deleted":
                    cqs_by_name.pop(cq_name, None)
                else:
                    cqs_by_name[cq_name] = _prepare_kueue_or_ns_doc(
                        pl,
                        override_queueing_strategy=override_queueing_strategy,
                        override_cq_fair_sharing_preemption=override_cq_fair_sharing_preemption,
                    )
            else:
                post_t0_cq_names.add(cq_name)
                doc = (
                    _prepare_kueue_or_ns_doc(
                        pl,
                        override_queueing_strategy=override_queueing_strategy,
                        override_cq_fair_sharing_preemption=override_cq_fair_sharing_preemption,
                    )
                    if ev_type != "Deleted"
                    else {}
                )
                _enqueue_post_t0_infra(ev_epoch, ev_ts_str, ev_type, "ClusterQueue", cq_name, doc_val=doc)

        elif kind == "LocalQueue":
            meta = pl.get("metadata") or {}
            lq_name = validate_dns_label(meta.get("name"), f"{inf_file} event #{idx_inf} LocalQueue metadata.name")
            raw_lq_ns = validate_dns_label(
                meta.get("namespace") or "default", f"{inf_file} event #{idx_inf} LocalQueue metadata.namespace"
            )
            lq_ns = override_workload_namespace or raw_lq_ns
            if in_initial_window:
                if ev_type == "Deleted":
                    lqs_by_key.pop((lq_ns, lq_name), None)
                else:
                    lqs_by_key[(lq_ns, lq_name)] = _prepare_kueue_or_ns_doc(
                        pl, override_namespace=override_workload_namespace
                    )
            else:
                post_t0_lq_keys.add((lq_ns, lq_name))
                doc = (
                    _prepare_kueue_or_ns_doc(pl, override_namespace=override_workload_namespace)
                    if ev_type != "Deleted"
                    else {}
                )
                _enqueue_post_t0_infra(
                    ev_epoch, ev_ts_str, ev_type, "LocalQueue", lq_name, obj_ns_val=lq_ns, doc_val=doc
                )

        elif kind == "Pod":
            p_meta = pl.get("metadata") or {}
            p_name = validate_dns_label(pl.get("name") or p_meta.get("name"), f"{inf_file} event #{idx_inf} Pod name")
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

    scheduled_infra_events.sort(key=lambda item: (item.target_wall_offset_sec, item.seq))
    return _ParsedInfra(
        nodes_by_name=nodes_by_name,
        namespaces_by_name=namespaces_by_name,
        runtime_classes_by_name=runtime_classes_by_name,
        k8s_priority_classes_by_name=k8s_priority_classes_by_name,
        topologies_by_name=topologies_by_name,
        flavors_by_name=flavors_by_name,
        cohorts_by_name=cohorts_by_name,
        priority_classes_by_name=priority_classes_by_name,
        cqs_by_name=cqs_by_name,
        lqs_by_key=lqs_by_key,
        post_t0_cq_names=post_t0_cq_names,
        post_t0_lq_keys=post_t0_lq_keys,
        pod_new_events=pod_new_events,
        pod_delete_epochs=pod_delete_epochs,
        scheduled_infra_events=scheduled_infra_events,
        kueue_configuration=kueue_configuration,
    )


def _canonical_resource_profile(reqs: dict[str, Any]) -> tuple[tuple[str, float], ...]:
    """Normalize a pod resource request dict into a comparable tuple of `(resource_name, rounded_amount)`."""
    items: list[tuple[str, float]] = []
    for rk, rv in sorted((reqs or {}).items()):
        if rk in ("pods", "ephemeral-storage"):
            continue
        val = round(parse_k8s_quantity(rv, rk), 6)
        if val > 0:
            items.append((str(rk), val))
    return tuple(items)


def _canonical_accel_profile(reqs: dict[str, Any]) -> tuple[tuple[str, float], ...]:
    """Normalize accelerator-only resource requests for fallback matching when CPU/memory differ."""
    items: list[tuple[str, float]] = []
    for rk, rv in sorted((reqs or {}).items()):
        if rk in ("cpu", "memory", "pods", "ephemeral-storage") or str(rk).startswith("hugepages-"):
            continue
        val = round(parse_k8s_quantity(rv, rk), 6)
        if val > 0:
            items.append((str(rk), val))
    return tuple(items)


def _extract_workflow_podset_templates(
    obj: dict[str, Any],
    kind: str,
) -> list[tuple[str, int, dict[str, str], bool]]:
    """Extract `(podset_name, count, string_requests, is_tas)` groups from a sanitized workflow object."""
    spec = obj.get("spec") or {}
    groups: list[tuple[str, int, dict[str, str], bool]] = []

    def _pod_tpl_to_entry(ps_name: str, count: int, tpl: dict[str, Any]) -> None:
        if count <= 0:
            return
        t_meta = tpl.get("metadata") or {}
        t_anns = t_meta.get("annotations") or {}
        is_tas = any(
            str(t_anns.get(k) or "").strip()
            for k in (
                "kueue.x-k8s.io/podset-required-topology",
                "kueue.x-k8s.io/podset-preferred-topology",
                "kueue.x-k8s.io/podset-unconstrained-topology",
                "kueue.x-k8s.io/podset-slice-required-topology",
            )
        )
        p_spec = tpl.get("spec") if "spec" in tpl else tpl
        parsed = extract_pod_spec_resources(p_spec)
        raw_reqs: dict[str, str] = {}
        for c in (p_spec or {}).get("containers") or []:
            if not isinstance(c, dict):
                continue
            r_blk = c.get("resources") or {}
            eff = {**(r_blk.get("limits") or {}), **(r_blk.get("requests") or {})}
            for rk, rv in eff.items():
                if rk != "ephemeral-storage" and rk not in raw_reqs:
                    raw_reqs[str(rk)] = str(rv)
        if (
            len((p_spec or {}).get("containers") or []) > 1
            or (p_spec or {}).get("initContainers")
            or (p_spec or {}).get("overhead")
            or (p_spec or {}).get("resources")
            or any(
                isinstance(c, dict)
                and bool(((c.get("resources") or {}).get("limits") or {}))
                and set(((c.get("resources") or {}).get("limits") or {}).keys())
                != set(((c.get("resources") or {}).get("requests") or {}).keys())
                for c in ((p_spec or {}).get("containers") or [])
            )
        ):
            raw_reqs = {}
            for rk, val in parsed.items():
                if rk == "cpu":
                    mcpu = int(round(val * 1000))
                    raw_reqs[rk] = str(mcpu // 1000) if mcpu % 1000 == 0 else f"{mcpu}m"
                elif rk == "memory":
                    gib = val
                    if abs(gib - round(gib)) < 1e-6:
                        raw_reqs[rk] = f"{int(round(gib))}Gi"
                    else:
                        raw_reqs[rk] = f"{int(round(gib * 1024))}Mi"
                else:
                    raw_reqs[rk] = str(int(round(val)))
        if not raw_reqs:
            raw_reqs = {"cpu": "1", "memory": "1Gi"}
        groups.append((ps_name, count, raw_reqs, is_tas))

    if kind == "JobSet":
        for rj in spec.get("replicatedJobs") or []:
            if not isinstance(rj, dict):
                continue
            rj_name = str(rj.get("name") or "main")
            replicas = max(1, int(rj.get("replicas") or 1))
            j_spec = ((rj.get("template") or {}).get("spec")) or {}
            parallelism = max(1, int(j_spec.get("parallelism") or j_spec.get("completions") or 1))
            _pod_tpl_to_entry(rj_name, replicas * parallelism, j_spec.get("template") or {})
    elif kind == "Job":
        parallelism = max(1, int(spec.get("parallelism") or spec.get("completions") or 1))
        _pod_tpl_to_entry("main", parallelism, spec.get("template") or {})
    elif kind in ("Deployment", "StatefulSet", "ReplicaSet"):
        raw_rep = spec.get("replicas")
        replicas = max(0, int(raw_rep)) if raw_rep is not None else 1
        _pod_tpl_to_entry("main", replicas, spec.get("template") or {})
    elif kind == "LeaderWorkerSet":
        replicas = max(1, int(spec.get("replicas") or 1))
        lwt = spec.get("leaderWorkerTemplate") or {}
        size = max(1, int(lwt.get("size") or 1))
        leader_tpl = lwt.get("leaderTemplate")
        worker_tpl = lwt.get("workerTemplate") or {}
        if isinstance(leader_tpl, dict) and leader_tpl:
            _pod_tpl_to_entry("leader", replicas, leader_tpl)
            _pod_tpl_to_entry("worker", replicas * max(0, size - 1), worker_tpl)
        else:
            _pod_tpl_to_entry("main", replicas * size, worker_tpl)
    elif kind in ("RayJob", "RayCluster"):
        rcs = (spec.get("rayClusterSpec") or {}) if kind == "RayJob" else spec
        if kind == "RayJob" and isinstance(spec.get("submitterPodTemplate"), dict):
            _pod_tpl_to_entry("submitter", 1, spec["submitterPodTemplate"])
        if isinstance(rcs.get("headGroupSpec"), dict):
            _pod_tpl_to_entry("head", 1, rcs["headGroupSpec"].get("template") or {})
        for idx_wg, wg in enumerate(rcs.get("workerGroupSpecs") or []):
            if isinstance(wg, dict):
                wg_name = str(wg.get("groupName") or f"worker-{idx_wg}")
                _pod_tpl_to_entry(wg_name, max(1, int(wg.get("replicas") or 1)), wg.get("template") or {})
    elif kind == "Pod":
        _pod_tpl_to_entry("main", 1, {"metadata": obj.get("metadata") or {}, "spec": spec})
    else:
        replicas = max(1, int(spec.get("replicas") or 1))
        _pod_tpl_to_entry("main", replicas, spec.get("template") or {})

    return groups


def _node_Matches_flavor(node_doc: dict[str, Any] | None, flavor_doc: dict[str, Any] | None) -> bool:
    if not node_doc or not flavor_doc:
        return True
    req_labels = ((flavor_doc.get("spec") or {}).get("nodeLabels")) or {}
    if not req_labels:
        return True
    n_labels = ((node_doc.get("metadata") or {}).get("labels")) or {}
    return all(str(n_labels.get(k)) == str(v) for k, v in req_labels.items())


def _resolve_cq_and_lq_for_candidate(
    cand: _AdmittedInitialCandidate,
    parsed_infra: _ParsedInfra,
    *,
    manage_jobs_without_queue_name: bool = False,
) -> tuple[str, str]:
    """Resolve `(local_queue_name, cluster_queue_name)` for an initial admitted workload candidate."""
    ns = cand.namespace
    raw_meta = (cand.raw_obj.get("metadata") or {}) if isinstance(cand.raw_obj, dict) else {}
    raw_labels = raw_meta.get("labels") or {}
    raw_anns = raw_meta.get("annotations") or {}

    lq_name = (
        cand.local_queue_name
        or (cand.exec_info.local_queue_name if cand.exec_info else "")
        or str(raw_labels.get("kueue.x-k8s.io/queue-name") or raw_anns.get("kueue.x-k8s.io/queue-name") or "")
    )
    cq_name = cand.exec_info.cluster_queue_name if cand.exec_info else ""

    # Never guess queues or synthesize `<cq>-init-lq` for system-namespace or unqueued non-Kueue
    # workflows from workflows.json unless manageJobsWithoutQueueName is active outside SYSTEM_NAMESPACES.
    has_kueue_signal = bool(
        lq_name
        or cq_name
        or cand.exec_info is not None
        or _has_kueue_queue_or_domain(cand.raw_obj, lq_name)
    )
    if cand.from_workflows_json and not has_kueue_signal:
        if cand.orig_namespace in SYSTEM_NAMESPACES or not manage_jobs_without_queue_name:
            return "", ""

    if lq_name and not cq_name:
        lq_doc = parsed_infra.lqs_by_key.get((ns, lq_name))
        if lq_doc:
            cq_name = str((lq_doc.get("spec") or {}).get("clusterQueue") or "")

    if not lq_name and cq_name:
        for (k_ns, k_lq), lq_doc in sorted(parsed_infra.lqs_by_key.items()):
            if k_ns == ns and str((lq_doc.get("spec") or {}).get("clusterQueue") or "") == cq_name:
                lq_name = k_lq
                break

    if not lq_name:
        ns_lqs = [(k_lq, doc) for (k_ns, k_lq), doc in sorted(parsed_infra.lqs_by_key.items()) if k_ns == ns]
        if len(ns_lqs) == 1:
            lq_name, lq_doc = ns_lqs[0]
            if not cq_name:
                cq_name = str((lq_doc.get("spec") or {}).get("clusterQueue") or "")

    if not cq_name and len(parsed_infra.cqs_by_name) == 1:
        cq_name = next(iter(parsed_infra.cqs_by_name.keys()))

    if cq_name and cq_name in parsed_infra.cqs_by_name and not lq_name:
        synth_lq = sanitize_k8s_name(f"{cq_name}-init-lq", max_len=MAX_K8S_NAME_LEN)
        if (ns, synth_lq) not in parsed_infra.lqs_by_key:
            parsed_infra.lqs_by_key[(ns, synth_lq)] = build_local_queue_manifest(
                name=synth_lq,
                namespace=ns,
                cluster_queue=cq_name,
            )
        lq_name = synth_lq

    if cq_name and cq_name not in parsed_infra.cqs_by_name:
        return "", ""
    if (
        lq_name
        and (ns, lq_name) not in parsed_infra.lqs_by_key
        and (ns, lq_name) not in parsed_infra.post_t0_lq_keys
        and cq_name
    ):
        parsed_infra.lqs_by_key[(ns, lq_name)] = build_local_queue_manifest(
            name=lq_name,
            namespace=ns,
            cluster_queue=cq_name,
        )

    return lq_name, cq_name


def _get_pod_meta_annotation_or_label(meta: dict[str, Any] | None, key: str) -> str:
    """Read a Kueue pod-group metadata key from annotations first, falling back to legacy labels."""
    if not isinstance(meta, dict):
        return ""
    anns = meta.get("annotations") or {}
    if isinstance(anns, dict):
        val = str(anns.get(key) or "").strip()
        if val:
            return val
    lbls = meta.get("labels") or {}
    if isinstance(lbls, dict):
        return str(lbls.get(key) or "").strip()
    return ""


def _resolve_podset_assignment_for_group(
    *,
    role_hash: str,
    count: int,
    reqs: dict[str, str],
    placements: list[str],
    cq_doc: dict[str, Any],
    parsed_infra: _ParsedInfra,
    scraped_psas: list[dict[str, Any]],
    ps_hint: str = "",
    group_index: int = 0,
    total_groups: int = 1,
) -> tuple[dict[str, Any], bool]:
    """Construct a valid Kueue `PodSetAssignment` and return `(psa_dict, uses_tas)`."""
    cq_rgs = ((cq_doc.get("spec") or {}).get("resourceGroups")) or []
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

    sample_node = parsed_infra.nodes_by_name.get(placements[0]) if placements else None

    def _pick_flavor(candidates: list[str]) -> str:
        for fn in candidates:
            if _node_Matches_flavor(sample_node, parsed_infra.flavors_by_name.get(fn)):
                return fn
        return candidates[0] if candidates else "default-flavor"

    target_resources = [rk for rk in reqs if rk in covered_to_flavors]
    if not target_resources:
        target_resources = [rk for rk in reqs if rk != "ephemeral-storage"]

    def _valid_psa_flavors(s_psa: dict[str, Any]) -> dict[str, str]:
        return {
            str(rk): str(fn)
            for rk, fn in (s_psa.get("flavors") or {}).items()
            if str(fn) in parsed_infra.flavors_by_name
        }

    def _psa_matches_node(s_psa: dict[str, Any]) -> bool:
        flvs = _valid_psa_flavors(s_psa)
        if not flvs:
            return False
        if sample_node is None:
            return True
        return all(_node_Matches_flavor(sample_node, parsed_infra.flavors_by_name.get(fn)) for fn in flvs.values())

    matched_psa: dict[str, Any] | None = None
    if scraped_psas:
        hint_names = {n for n in (ps_hint, role_hash) if n}
        for s_psa in scraped_psas:
            if str(s_psa.get("name") or "") in hint_names:
                matched_psa = s_psa
                break
        if matched_psa is None and len(scraped_psas) == 1:
            matched_psa = scraped_psas[0]
        if matched_psa is None:
            node_compatible = [s_psa for s_psa in scraped_psas if _psa_matches_node(s_psa)]
            if len(node_compatible) == 1:
                matched_psa = node_compatible[0]
            elif len(node_compatible) > 1:
                count_matches = [s_psa for s_psa in node_compatible if s_psa.get("count") == count]
                if len(count_matches) == 1:
                    matched_psa = count_matches[0]
        if (
            matched_psa is None
            and len(scraped_psas) == total_groups
            and 0 <= group_index < len(scraped_psas)
            and _psa_matches_node(scraped_psas[group_index])
        ):
            matched_psa = scraped_psas[group_index]

    if matched_psa is not None:
        scraped_flavors: dict[str, str] = _valid_psa_flavors(matched_psa)
    else:
        # No assignment could be tied to this pod group. Still trust Kueue's recorded flavor for every
        # resource on which all scraped assignments agree; only disputed resources fall back to the node.
        flavors_seen: dict[str, set[str]] = {}
        for s_psa in scraped_psas:
            for rk, fn in _valid_psa_flavors(s_psa).items():
                flavors_seen.setdefault(rk, set()).add(fn)
        scraped_flavors = {rk: next(iter(fns)) for rk, fns in flavors_seen.items() if len(fns) == 1}

    flavors_map: dict[str, str] = {}
    resource_usage: dict[str, str] = {}
    assigned_tas_topology: str = ""

    for rk in sorted(target_resources):
        flv_name = scraped_flavors.get(rk) or _pick_flavor(covered_to_flavors.get(rk) or all_cq_flavors)
        flavors_map[rk] = flv_name
        raw_qty = reqs.get(rk, "0")
        resource_usage[rk] = multiply_k8s_quantity_exact(raw_qty, count, rk)
        flv_doc = parsed_infra.flavors_by_name.get(flv_name) or {}
        topo_ref = str(((flv_doc.get("spec") or {}).get("topologyName")) or "")
        if topo_ref and not assigned_tas_topology:
            assigned_tas_topology = topo_ref

    psa: dict[str, Any] = {
        "name": role_hash,
        "count": count,
        "flavors": flavors_map,
        "resourceUsage": resource_usage,
    }

    uses_tas = False
    if assigned_tas_topology and assigned_tas_topology in parsed_infra.topologies_by_name:
        topo_doc = parsed_infra.topologies_by_name[assigned_tas_topology]
        topo_levels = [
            str(lv.get("nodeLabel") or "")
            for lv in ((topo_doc.get("spec") or {}).get("levels") or [])
            if isinstance(lv, dict) and lv.get("nodeLabel")
        ]
        ta = build_topology_assignment(placements, topo_levels, parsed_infra.nodes_by_name)
        if ta is not None:
            psa["topologyAssignment"] = ta
            uses_tas = True

    return psa, uses_tas


def _build_initial_workloads_and_pods(
    parsed_infra: _ParsedInfra,
    admitted_candidates: list[_AdmittedInitialCandidate],
    scheduled_workflows: list[ScheduledWorkflowItem],
    scheduled_deletions: list[ScheduledWorkflowDeletion],
    earliest_admit_by_ns: dict[str, float],
    exec_index: ExecutionTimeIndex,
    *,
    t0_epoch: float,
    effective_start_epoch: float,
    initial_window_sec: float,
    speedup: float,
    min_pod_duration_sec: float,
    infinite_pod_duration_sec: float,
    default_exec_sec: float,
    pause_image_override: str | None,
    override_workload_namespace: str | None,
    infra_pods_namespace: str,
    interrupted_duration_multiplier: float | None = None,
    interrupted_duration_override: InterruptedDurationOverride | None = None,
    manage_jobs_without_queue_name: bool = False,
) -> tuple[list[InitialWorkloadPodGroup], list[InitialPodSpec], list[ScheduledWorkflowItem], int]:
    """Partition t0 running state into pre-admitted Kueue `PodGroup` workloads and non-Kueue `infra-pods`."""
    not_admitted_wf_names = [
        (w.namespace, w.name) for w in scheduled_workflows if w.trigger_reason == "initial_not_admitted"
    ]
    qualifying_initial_pods, expired_initial_pods_count = filter_initial_running_pods(
        parsed_infra.pod_new_events,
        parsed_infra.pod_delete_epochs,
        t0_epoch=t0_epoch,
        effective_start_epoch=effective_start_epoch,
        initial_window_sec=initial_window_sec,
        not_admitted_workflow_names=not_admitted_wf_names,
        earliest_admit_by_ns=earliest_admit_by_ns,
        strict_timestamps=True,
    )

    pause_img = pause_image_override or DEFAULT_PAUSE_IMAGE

    # Filter workflow candidates to those genuinely active at `effective_start_epoch`.
    # Any Kueue workflow first seen Admitted strictly AFTER `effective_start_epoch + initial_window_sec`
    # (and not admitted before/at `effective_start_epoch`) is scheduled at its post-t0 arrival time
    # rather than teleported backwards to t0.
    active_candidates: list[_AdmittedInitialCandidate] = []
    known_wl_keys: set[tuple[str, str]] = set()
    known_owner_keys: set[tuple[str, str]] = set()
    next_post_t0_seq = (max((w.seq for w in scheduled_workflows), default=0) + 1)

    for cand in admitted_candidates:
        admitted_by_t0 = cand.first_seen_epoch <= (effective_start_epoch + initial_window_sec + 1e-6) or bool(
            cand.exec_info
            and cand.exec_info.admitted_epochs
            and min(cand.exec_info.admitted_epochs) <= (effective_start_epoch + initial_window_sec + 1e-6)
        )
        if not admitted_by_t0:
            if (
                cand.from_workflows_json
                and cand.raw_obj
                and cand.kind != "PodGroup"
                and cand.preempt_item is None
                and (
                    _has_kueue_queue_or_domain(cand.raw_obj, cand.local_queue_name)
                    or cand.exec_info is not None
                    or (manage_jobs_without_queue_name and cand.orig_namespace not in SYSTEM_NAMESPACES)
                )
            ):
                arr_ep = (
                    cand.creation_epoch
                    if (cand.creation_epoch is not None and cand.creation_epoch >= t0_epoch)
                    else cand.first_seen_epoch
                )
                existing_ann = _extract_existing_pod_exec_seconds(cand.raw_obj)
                has_explicit_ann = existing_ann is not None
                fb_dur = existing_ann or default_exec_sec
                wf_td, wf_mm = _extract_workflow_true_duration_and_multiplier(cand.raw_obj)
                raw_dur_late, found_late = exec_index.resolve_duration_sec(
                    namespace=cand.orig_namespace,
                    object_kind=cand.kind,
                    name=cand.name,
                    fallback_sec=fb_dur,
                    interrupted_duration_multiplier=interrupted_duration_multiplier,
                    interrupted_duration_override=interrupted_duration_override,
                    workflow_true_duration_sec=wf_td,
                    workflow_missing_multiplier=wf_mm,
                    creation_epoch=cand.creation_epoch,
                    incarnation_index=cand.incarnation_index,
                )
                comp_late = max(min_pod_duration_sec, raw_dur_late / speedup)
                exec_str_late = format_pod_execution_seconds(comp_late, min_seconds=min_pod_duration_sec)
                late_manifest, late_pods = _inject_pod_execution_time_and_sanitize_workflow(
                    raw_obj=cand.raw_obj,
                    exec_str=exec_str_late,
                    raw_duration_sec=raw_dur_late,
                    compressed_duration_sec=comp_late,
                    pause_image_override=pause_image_override,
                    override_namespace=override_workload_namespace,
                    speedup=speedup,
                    scale_by_waves=bool(found_late or not has_explicit_ann),
                    min_pod_duration_sec=min_pod_duration_sec,
                )
                raw_off_late = max(0.0, arr_ep - t0_epoch)
                wall_off_late = raw_off_late / speedup
                arr_iso = datetime.fromtimestamp(arr_ep, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                scheduled_workflows.append(
                    ScheduledWorkflowItem(
                        target_wall_offset_sec=wall_off_late,
                        seq=next_post_t0_seq,
                        raw_offset_sec=raw_off_late,
                        raw_offset_from_t0_sec=raw_off_late,
                        event_timestamp=arr_iso,
                        object_kind=cand.kind,
                        name=cand.name,
                        namespace=cand.namespace,
                        local_queue_name=cand.local_queue_name or (cand.exec_info.local_queue_name if cand.exec_info else ""),
                        workload_state="NotAdmitted",
                        trigger_reason="post_t0_fast_admitted",
                        execution_duration_sec=raw_dur_late,
                        compressed_duration_sec=comp_late,
                        indexed_from_execution_time=found_late,
                        pod_count=late_pods,
                        manifest=late_manifest,
                        creation_epoch=cand.creation_epoch if cand.creation_epoch is not None else arr_ep,
                        incarnation_index=cand.incarnation_index,
                        has_explicit_pod_exec_annotation=has_explicit_ann,
                    )
                )
                next_post_t0_seq += 1
            continue
        if cand.deleted_epoch is not None and cand.deleted_epoch <= effective_start_epoch + 1e-6:
            continue
        if cand.preempt_item is not None:
            preempt_abs_ep = t0_epoch + cand.preempt_item.raw_offset_from_t0_sec
            if preempt_abs_ep <= effective_start_epoch + 1e-6:
                # Already preempted before or at `effective_start_epoch`: leave in `scheduled_workflows`.
                continue
        if cand.exec_info and cand.exec_info.full_end_epoch is not None:
            if cand.exec_info.full_end_epoch <= effective_start_epoch + 1e-6:
                continue
        active_candidates.append(cand)
        known_owner_keys.add((cand.namespace, cand.name))
        if cand.exec_info and cand.exec_info.workload_name:
            known_wl_keys.add((cand.namespace, cand.exec_info.workload_name))

    # Also discover any t0-admitted Workloads in execution-time.json (or PodGroup pods in infra.json)
    # whose parent object was not recorded in workflows.json (e.g. PodGroup or custom CRDs).
    for (orig_ns, wl_name), w_info in sorted(exec_index.by_workload_name.items()):
        eff_ns = override_workload_namespace or orig_ns
        if (eff_ns, wl_name) in known_wl_keys or (w_info.owner_name and (eff_ns, w_info.owner_name) in known_owner_keys):
            continue
        if not w_info.admitted_epochs or min(w_info.admitted_epochs) > effective_start_epoch + initial_window_sec:
            continue
        if w_info.full_end_epoch is not None and w_info.full_end_epoch <= effective_start_epoch + 1e-6:
            continue
        if w_info.preempted_epochs and min(w_info.preempted_epochs) <= effective_start_epoch + 1e-6:
            continue
        active_candidates.append(
            _AdmittedInitialCandidate(
                namespace=eff_ns,
                orig_namespace=orig_ns,
                kind=w_info.owner_kind or "PodGroup",
                name=w_info.owner_name or wl_name,
                local_queue_name=w_info.local_queue_name,
                raw_obj={},
                exec_info=w_info,
                preempt_item=None,
                from_workflows_json=False,
            )
        )
        known_wl_keys.add((eff_ns, wl_name))

    has_enriched_pod_linkage = any(
        bool(pl.get("workloadName") or pl.get("ownerName"))
        for (_p_ns, _p_name, _ev_ep, pl, _del_ep) in qualifying_initial_pods
    )

    cand_by_wl: dict[tuple[str, str], int] = {}
    cand_by_owner_kind: dict[tuple[str, str, str], int] = {}
    cand_by_owner: dict[tuple[str, str], int] = {}
    for c_idx, cand in enumerate(active_candidates):
        if cand.exec_info and cand.exec_info.workload_name:
            cand_by_wl[(cand.namespace, cand.exec_info.workload_name)] = c_idx
        if cand.kind and cand.name:
            cand_by_owner_kind[(cand.namespace, cand.kind, cand.name)] = c_idx
        if cand.name:
            cand_by_owner.setdefault((cand.namespace, cand.name), c_idx)

    matched_pods_by_cand: dict[int, list[tuple[str, str, float, dict[str, Any], float | None]]] = {
        i: [] for i in range(len(active_candidates))
    }
    claimed_pod_indices: set[int] = set()

    # Tier 1: Exact enriched scraper linkage (`workloadName` / `ownerName`).
    for p_idx, pod_tuple in enumerate(qualifying_initial_pods):
        p_ns, _p_name, _ev_ep, pl, _del_ep = pod_tuple
        p_wl = str(pl.get("workloadName") or "")
        p_okind = str(pl.get("ownerKind") or "")
        p_oname = str(pl.get("ownerName") or "")
        if not p_wl and not p_oname:
            continue
        target_c_idx: int | None = None
        if p_wl and (p_ns, p_wl) in cand_by_wl:
            target_c_idx = cand_by_wl[(p_ns, p_wl)]
        elif p_okind and p_oname and (p_ns, p_okind, p_oname) in cand_by_owner_kind:
            target_c_idx = cand_by_owner_kind[(p_ns, p_okind, p_oname)]
        elif p_oname and (p_ns, p_oname) in cand_by_owner:
            target_c_idx = cand_by_owner[(p_ns, p_oname)]
        elif p_wl:
            # Create ad-hoc PodGroup candidate when pod carries explicit workloadName without workflows.json entry.
            new_c_idx = len(active_candidates)
            w_info = exec_index.by_workload_name.get((p_ns, p_wl))
            active_candidates.append(
                _AdmittedInitialCandidate(
                    namespace=p_ns,
                    orig_namespace=p_ns,
                    kind=p_okind or "PodGroup",
                    name=p_oname or p_wl,
                    local_queue_name=w_info.local_queue_name if w_info else "",
                    raw_obj={},
                    exec_info=w_info,
                    preempt_item=None,
                    from_workflows_json=False,
                )
            )
            cand_by_wl[(p_ns, p_wl)] = new_c_idx
            matched_pods_by_cand[new_c_idx] = []
            target_c_idx = new_c_idx

        if target_c_idx is not None:
            matched_pods_by_cand[target_c_idx].append(pod_tuple)
            claimed_pod_indices.add(p_idx)

    # Tiers 2 & 3: Legacy fallback matchers when trace predates scraper pod-workload linkage.
    if not has_enriched_pod_linkage and active_candidates and qualifying_initial_pods:
        # Tier 2: Plain-name prefix matching (`<wf_name>-*` or `== <wf_name>`).
        for c_idx, cand in enumerate(active_candidates):
            if matched_pods_by_cand[c_idx] or not cand.name:
                continue
            if cand.name.startswith("h-") and len(cand.name) == 34:
                continue
            ps_groups = _extract_workflow_podset_templates(cand.raw_obj, cand.kind) if cand.raw_obj else []
            max_pods = sum(cnt for (_n, cnt, _r, _t) in ps_groups) if ps_groups else 1000000
            prefix = f"{cand.name}-"
            for p_idx, pod_tuple in enumerate(qualifying_initial_pods):
                if p_idx in claimed_pod_indices:
                    continue
                p_ns, p_name, _ev_ep, _pl, _del_ep = pod_tuple
                if p_ns == cand.namespace and (p_name == cand.name or p_name.startswith(prefix)):
                    matched_pods_by_cand[c_idx].append(pod_tuple)
                    claimed_pod_indices.add(p_idx)
                    if len(matched_pods_by_cand[c_idx]) >= max_pods:
                        break

        # Build topology sort key for nodes so Tier 3 keeps pods on the same rack/block/host contiguous.
        all_topo_keys: list[str] = []
        for t_doc in parsed_infra.topologies_by_name.values():
            for lv in ((t_doc.get("spec") or {}).get("levels") or []):
                if isinstance(lv, dict) and lv.get("nodeLabel") and lv["nodeLabel"] not in all_topo_keys:
                    all_topo_keys.append(str(lv["nodeLabel"]))

        def _pod_topo_sort_key(p_idx: int) -> tuple[Any, ...]:
            _p_ns, p_name, _ev_ep, pl, _del_ep = qualifying_initial_pods[p_idx]
            node_name = str(pl.get("placement") or "")
            n_labels = (((parsed_infra.nodes_by_name.get(node_name) or {}).get("metadata") or {}).get("labels")) or {}
            return tuple(str(n_labels.get(k) or "") for k in all_topo_keys) + (node_name, p_name)

        unclaimed_sorted = sorted(
            (idx for idx in range(len(qualifying_initial_pods)) if idx not in claimed_pod_indices),
            key=_pod_topo_sort_key,
        )

        # Tier 3: Resource-profile + topology-contiguous matching for hashed/translated traces.
        for c_idx, cand in enumerate(active_candidates):
            if matched_pods_by_cand[c_idx] or not cand.raw_obj:
                continue
            lq_chk, cq_chk = _resolve_cq_and_lq_for_candidate(
                cand, parsed_infra, manage_jobs_without_queue_name=manage_jobs_without_queue_name
            )
            if not lq_chk or not cq_chk:
                continue
            ps_groups = _extract_workflow_podset_templates(cand.raw_obj, cand.kind)
            if not ps_groups:
                continue

            tentative_indices: list[int] = []
            all_groups_matched = True
            for _ps_name, want_cnt, want_reqs, _is_tas in ps_groups:
                want_full = _canonical_resource_profile(want_reqs)
                want_accel = _canonical_accel_profile(want_reqs)
                exact_matches = [
                    p_idx
                    for p_idx in unclaimed_sorted
                    if p_idx not in claimed_pod_indices
                    and p_idx not in tentative_indices
                    and qualifying_initial_pods[p_idx][0] == cand.namespace
                    and _canonical_resource_profile(qualifying_initial_pods[p_idx][3].get("resourceRequest") or {})
                    == want_full
                ]
                chosen = exact_matches[:want_cnt]
                if len(chosen) < want_cnt and want_accel:
                    accel_matches = [
                        p_idx
                        for p_idx in unclaimed_sorted
                        if p_idx not in claimed_pod_indices
                        and p_idx not in tentative_indices
                        and p_idx not in chosen
                        and qualifying_initial_pods[p_idx][0] == cand.namespace
                        and _canonical_accel_profile(qualifying_initial_pods[p_idx][3].get("resourceRequest") or {})
                        == want_accel
                    ]
                    chosen.extend(accel_matches[: want_cnt - len(chosen)])
                if not chosen:
                    all_groups_matched = False
                    break
                tentative_indices.extend(chosen)

            if all_groups_matched and tentative_indices:
                for p_idx in tentative_indices:
                    matched_pods_by_cand[c_idx].append(qualifying_initial_pods[p_idx])
                    claimed_pod_indices.add(p_idx)

    # Track remaining allocatable resources per node so synthetic missing-pod placement (Tier 4) fits cleanly.
    rem_node_alloc: dict[str, dict[str, float]] = {}
    for n_name, n_doc in parsed_infra.nodes_by_name.items():
        alloc = ((n_doc.get("status") or {}).get("allocatable")) or {}
        rem_node_alloc[n_name] = {rk: parse_k8s_quantity(rv, rk) for rk, rv in alloc.items()}
    for _p_ns, _p_name, _ev_ep, pl, _del_ep in qualifying_initial_pods:
        plc = str(pl.get("placement") or "")
        if plc in rem_node_alloc:
            for rk, rv in (pl.get("resourceRequest") or {}).items():
                rem_node_alloc[plc][rk] = rem_node_alloc[plc].get(rk, 0.0) - parse_k8s_quantity(rv, rk)

    # Materialize Class 1 `InitialWorkloadPodGroup`s and `InitialDeploymentBootstrap`s, and collect any preempted items superseded by dynamic resubmit.
    initial_workloads: list[InitialWorkloadPodGroup] = []
    initial_deployments: list[InitialDeploymentBootstrap] = []
    superseded_preempt_seqs: set[int] = set()
    used_wl_names_by_ns: dict[str, set[str]] = {}
    used_pod_names_by_ns: dict[str, set[str]] = {}
    admit_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for c_idx, cand in enumerate(active_candidates):
        lq_name, cq_name = _resolve_cq_and_lq_for_candidate(
            cand, parsed_infra, manage_jobs_without_queue_name=manage_jobs_without_queue_name
        )
        cand_pods = list(matched_pods_by_cand.get(c_idx) or [])

        # Bootstrap t0 apps/v1 Deployments natively as real Deployment objects (supporting full or partial t0 readiness,
        # mid-run ScaleUp/ScaleDown/Upgrade/Pause/Resume,ReplicaSet pod replacement on preemption, and Delete/Recreate).
        if cand.kind == "Deployment" and cand.from_workflows_json and cand.raw_obj and lq_name and cq_name:
            dep_spec_raw = cand.raw_obj.get("spec") or {}
            raw_rep_val = dep_spec_raw.get("replicas")
            desired_replicas = max(0, int(raw_rep_val)) if raw_rep_val is not None else 1
            if cand.ready_replicas is not None:
                k_admitted = min(desired_replicas, max(0, int(cand.ready_replicas)))
                if k_admitted == 0 and cand_pods:
                    k_admitted = min(desired_replicas, len(cand_pods))
            elif cand_pods:
                k_admitted = min(desired_replicas, len(cand_pods))
            else:
                k_admitted = desired_replicas

            ps_groups_dep = _extract_workflow_podset_templates(cand.raw_obj, "Deployment")
            dep_per_pod_reqs: dict[str, str] = (
                dict(ps_groups_dep[0][2]) if ps_groups_dep else {"cpu": "1", "memory": "1Gi"}
            )
            dep_is_tas = bool(ps_groups_dep[0][3]) if ps_groups_dep else False

            cq_doc_dep = parsed_infra.cqs_by_name[cq_name]
            cq_flvs_dep = [
                str(f.get("name") or "")
                for rg in ((cq_doc_dep.get("spec") or {}).get("resourceGroups") or [])
                for f in (rg.get("flavors") or [])
                if isinstance(f, dict) and f.get("name")
            ]
            flv_doc_dep = parsed_infra.flavors_by_name.get(cq_flvs_dep[0]) if cq_flvs_dep else None

            dep_placements = [str(pt[3].get("placement") or "") for pt in cand_pods[:k_admitted]]
            while len(dep_placements) < k_admitted:
                used_dep_nodes = {plc for plc in dep_placements if plc}
                chosen_node_dep = ""
                for prefer_unused in (True, False):
                    for n_name, n_doc in parsed_infra.nodes_by_name.items():
                        if prefer_unused and n_name in used_dep_nodes:
                            continue
                        if not _node_Matches_flavor(n_doc, flv_doc_dep):
                            continue
                        fits = all(
                            rem_node_alloc.get(n_name, {}).get(rk, 0.0) + 1e-6 >= parse_k8s_quantity(rv, rk)
                            for rk, rv in dep_per_pod_reqs.items()
                            if rk in rem_node_alloc.get(n_name, {})
                        )
                        if fits:
                            chosen_node_dep = n_name
                            for rk, rv in dep_per_pod_reqs.items():
                                if rk in rem_node_alloc[n_name]:
                                    rem_node_alloc[n_name][rk] -= parse_k8s_quantity(rv, rk)
                            break
                    if chosen_node_dep:
                        break
                if not chosen_node_dep and parsed_infra.nodes_by_name:
                    chosen_node_dep = next(iter(parsed_infra.nodes_by_name.keys()))
                dep_placements.append(chosen_node_dep)

            dep_manifest, _ = _inject_pod_execution_time_and_sanitize_workflow(
                raw_obj=cand.raw_obj,
                exec_str="1",
                raw_duration_sec=1.0,
                compressed_duration_sec=1.0,
                pause_image_override=pause_image_override,
                override_namespace=override_workload_namespace,
                speedup=speedup,
                min_pod_duration_sec=min_pod_duration_sec,
            )
            dep_meta = dep_manifest.setdefault("metadata", {})
            dep_labels = dict(dep_meta.get("labels") or {})
            if lq_name and not dep_labels.get("kueue.x-k8s.io/queue-name"):
                dep_labels["kueue.x-k8s.io/queue-name"] = lq_name
            dep_meta["labels"] = dep_labels
            applied_dep_name = str(dep_meta.get("name") or cand.name)

            raw_anns_dep = (cand.raw_obj.get("metadata") or {}).get("annotations") or {}
            pc_name_dep_raw = (
                str(dep_labels.get("kueue.x-k8s.io/priority-class") or "")
                or (cand.exec_info.priority_class_name if cand.exec_info else "")
            )
            pc_name_dep = validate_dns_label(pc_name_dep_raw, "kueue.x-k8s.io/priority-class") if pc_name_dep_raw else ""
            prio_val_dep: int | None = cand.exec_info.priority if cand.exec_info else None
            if prio_val_dep is None and raw_anns_dep.get("kueue-optimizer.gke.io/priority") is not None:
                try:
                    prio_val_dep = int(raw_anns_dep["kueue-optimizer.gke.io/priority"])
                except (TypeError, ValueError):
                    prio_val_dep = None
            if prio_val_dep is None and pc_name_dep and pc_name_dep in parsed_infra.priority_classes_by_name:
                try:
                    prio_val_dep = int(parsed_infra.priority_classes_by_name[pc_name_dep].get("value") or 0)
                except (TypeError, ValueError):
                    prio_val_dep = 0

            orig_ref_ep_dep: float | None = None
            if cand.exec_info and cand.exec_info.admitted_epochs:
                orig_ref_ep_dep = min(cand.exec_info.admitted_epochs)
            if cand.creation_epoch is not None and (
                orig_ref_ep_dep is None or orig_ref_ep_dep >= effective_start_epoch - 1e-6
            ):
                orig_ref_ep_dep = (
                    min(orig_ref_ep_dep, cand.creation_epoch) if orig_ref_ep_dep is not None else cand.creation_epoch
                )
            orig_age_dep = (
                max(0.0, (effective_start_epoch - orig_ref_ep_dep) / max(1e-6, speedup))
                if orig_ref_ep_dep is not None
                else 0.0
            )

            initial_deployments.append(
                InitialDeploymentBootstrap(
                    namespace=cand.namespace,
                    name=cand.name,
                    applied_name=applied_dep_name,
                    local_queue_name=lq_name,
                    cluster_queue_name=cq_name,
                    desired_replicas=desired_replicas,
                    admitted_replicas=k_admitted,
                    paused=bool((dep_manifest.get("spec") or {}).get("paused", False)),
                    original_age_sec=orig_age_dep,
                    placements=dep_placements,
                    per_pod_requests=dep_per_pod_reqs,
                    priority_class_name=pc_name_dep,
                    priority=prio_val_dep,
                    is_tas=dep_is_tas,
                    manifest=dep_manifest,
                    cq_doc=cq_doc_dep,
                    flavors_by_name=dict(parsed_infra.flavors_by_name),
                    topologies_by_name=dict(parsed_infra.topologies_by_name),
                    nodes_by_name=dict(parsed_infra.nodes_by_name),
                    scraped_pod_set_assignments=list(
                        cand.exec_info.pod_set_assignments if cand.exec_info else []
                    ),
                )
            )
            if cand.preempt_item is not None:
                superseded_preempt_seqs.add(cand.preempt_item.seq)
            continue

        # Tier 4: Synthesize missing pods when a scenario or vanilla-K8s t0 scrape specifies an Admitted
        # workflow in workflows.json with fewer bound Pod events in infra.json than expected (0 or partial binding).
        if cand.from_workflows_json and lq_name and cq_name:
            if cand.kind == "PodGroup":
                ps_groups_raw: dict[tuple[str, tuple[tuple[str, float], ...]], tuple[str, int, dict[str, str], bool]] = {}
                for m_obj in cand.member_objs:
                    for m_ps, m_cnt, m_reqs, m_tas in _extract_workflow_podset_templates(m_obj, "Pod"):
                        gk_m = (m_ps, _canonical_resource_profile(m_reqs))
                        if gk_m in ps_groups_raw:
                            prev_ps, prev_cnt, prev_reqs, prev_tas = ps_groups_raw[gk_m]
                            ps_groups_raw[gk_m] = (prev_ps, prev_cnt + m_cnt, prev_reqs, prev_tas or m_tas)
                        else:
                            ps_groups_raw[gk_m] = (m_ps, m_cnt, m_reqs, m_tas)
                ps_groups = list(ps_groups_raw.values())
                if len(ps_groups) == 1 and cand.member_objs:
                    pg_tot_str = _get_pod_meta_annotation_or_label(
                        cand.member_objs[0].get("metadata"),
                        "kueue.x-k8s.io/pod-group-total-count",
                    )
                    try:
                        pg_tot = int(pg_tot_str)
                    except ValueError:
                        pg_tot = 0
                    if pg_tot > ps_groups[0][1]:
                        ps_groups = [(ps_groups[0][0], pg_tot, ps_groups[0][2], ps_groups[0][3])]
            elif cand.raw_obj:
                ps_groups = _extract_workflow_podset_templates(cand.raw_obj, cand.kind)
            else:
                ps_groups = []
        else:
            ps_groups = []

        expected_total_pods = sum(cnt for (_n, cnt, _r, _t) in ps_groups)
        if ps_groups and len(cand_pods) < expected_total_pods:
            cq_doc_tmp = parsed_infra.cqs_by_name[cq_name]
            cq_flvs = [
                str(f.get("name") or "")
                for rg in ((cq_doc_tmp.get("spec") or {}).get("resourceGroups") or [])
                for f in (rg.get("flavors") or [])
                if isinstance(f, dict) and f.get("name")
            ]
            flv_doc_tmp = parsed_infra.flavors_by_name.get(cq_flvs[0]) if cq_flvs else None

            allowed_ta_domains: dict[str, set[str]] = {}
            for s_psa in (cand.exec_info.pod_set_assignments if cand.exec_info else []):
                ta_obj = s_psa.get("topologyAssignment") if isinstance(s_psa, dict) else None
                if not isinstance(ta_obj, dict):
                    continue
                ta_levels = [str(x) for x in (ta_obj.get("levels") or [])]
                for sl_obj in ta_obj.get("slices") or []:
                    if not isinstance(sl_obj, dict):
                        continue
                    for l_idx, lv_obj in enumerate(sl_obj.get("valuesPerLevel") or []):
                        if l_idx >= len(ta_levels) or not isinstance(lv_obj, dict):
                            continue
                        lvl_key = ta_levels[l_idx]
                        if lv_obj.get("universal") is not None:
                            allowed_ta_domains.setdefault(lvl_key, set()).add(str(lv_obj["universal"]))
                        indiv = lv_obj.get("individual")
                        if isinstance(indiv, dict):
                            pfx = str(indiv.get("prefix") or "")
                            sfx = str(indiv.get("suffix") or "")
                            for rt in indiv.get("roots") or []:
                                allowed_ta_domains.setdefault(lvl_key, set()).add(f"{pfx}{rt}{sfx}")

            raw_dur_cand = default_exec_sec
            has_finite_end = False
            wf_true_cand, wf_mult_cand = _extract_workflow_true_duration_and_multiplier(cand.raw_obj)
            if cand.exec_info:
                had_int_cand = bool(cand.exec_info.preempted_epochs or cand.exec_info.interrupted_epochs)
                if cand.exec_info.full_duration_sec is not None and cand.exec_info.full_duration_sec > 0 and had_int_cand:
                    adm_ep = min(cand.exec_info.admitted_epochs) if cand.exec_info.admitted_epochs else t0_epoch
                    rem = (adm_ep + cand.exec_info.full_duration_sec) - effective_start_epoch
                    raw_dur_cand = rem if rem > 0 else cand.exec_info.full_duration_sec
                    has_finite_end = True
                elif cand.exec_info.full_end_epoch is not None and cand.exec_info.full_end_epoch > effective_start_epoch:
                    raw_dur_cand = cand.exec_info.full_end_epoch - effective_start_epoch
                    has_finite_end = True
                elif cand.exec_info.full_duration_sec is not None and cand.exec_info.full_duration_sec > 0:
                    adm_ep = min(cand.exec_info.admitted_epochs) if cand.exec_info.admitted_epochs else t0_epoch
                    rem = (adm_ep + cand.exec_info.full_duration_sec) - effective_start_epoch
                    if rem > 0:
                        raw_dur_cand = rem
                        has_finite_end = True
                elif cand.exec_info.is_interrupted_only:
                    adm_ep = min(cand.exec_info.admitted_epochs) if cand.exec_info.admitted_epochs else t0_epoch
                    resolved_tot = cand.exec_info.resolved_duration_sec(
                        default_sec=default_exec_sec,
                        interrupted_duration_multiplier=interrupted_duration_multiplier,
                        interrupted_duration_override=interrupted_duration_override,
                        workflow_true_duration_sec=wf_true_cand,
                        workflow_missing_multiplier=wf_mult_cand,
                    )
                    rem = (adm_ep + resolved_tot) - effective_start_epoch
                    if rem > 0:
                        raw_dur_cand = rem
                        has_finite_end = True
            existing_ann_dur = _extract_existing_pod_exec_seconds(cand.raw_obj) if cand.raw_obj else None
            if not has_finite_end and existing_ann_dur is not None and existing_ann_dur > 0:
                raw_dur_cand = existing_ann_dur
                has_finite_end = True

            synth_del_ep = (effective_start_epoch + raw_dur_cand) if has_finite_end else None
            have_by_prof: dict[tuple[tuple[str, float], ...], int] = {}
            for pt_existing in cand_pods:
                prof_ex = _canonical_resource_profile(pt_existing[3].get("resourceRequest") or {})
                have_by_prof[prof_ex] = have_by_prof.get(prof_ex, 0) + 1

            synth_idx = len(cand_pods)
            for ps_name, cnt, reqs, is_tas in ps_groups:
                prof_grp = _canonical_resource_profile(reqs)
                already_have = min(cnt, have_by_prof.get(prof_grp, 0))
                have_by_prof[prof_grp] = max(0, have_by_prof.get(prof_grp, 0) - already_have)
                missing_cnt = cnt - already_have
                for _ in range(missing_cnt):
                    used_cand_nodes = {str(pt_c[3].get("placement") or "") for pt_c in cand_pods}
                    search_passes = (
                        [(True, True), (True, False), (False, False)]
                        if allowed_ta_domains
                        else [(False, True), (False, False)]
                    )
                    chosen_node = ""
                    for require_ta, prefer_unused in search_passes:
                        for n_name, n_doc in parsed_infra.nodes_by_name.items():
                            if prefer_unused and n_name in used_cand_nodes:
                                continue
                            if not _node_Matches_flavor(n_doc, flv_doc_tmp):
                                continue
                            if require_ta:
                                n_lbls = ((n_doc.get("metadata") or {}).get("labels")) or {}
                                if not all(
                                    str(n_lbls.get(lk) or "") in allowed_vals
                                    for lk, allowed_vals in allowed_ta_domains.items()
                                ):
                                    continue
                            fits = all(
                                rem_node_alloc.get(n_name, {}).get(rk, 0.0) + 1e-6 >= parse_k8s_quantity(rv, rk)
                                for rk, rv in reqs.items()
                                if rk in rem_node_alloc.get(n_name, {})
                            )
                            if fits:
                                chosen_node = n_name
                                for rk, rv in reqs.items():
                                    if rk in rem_node_alloc[n_name]:
                                        rem_node_alloc[n_name][rk] -= parse_k8s_quantity(rv, rk)
                                break
                        if chosen_node:
                            break
                    if not chosen_node and parsed_infra.nodes_by_name:
                        chosen_node = next(iter(parsed_infra.nodes_by_name.keys()))
                    synth_p_name = sanitize_k8s_name(
                        f"{cand.name}-{ps_name}-{synth_idx}", max_len=MAX_K8S_NAME_LEN
                    )
                    synth_idx += 1
                    cand_pods.append(
                        (
                            cand.namespace,
                            synth_p_name,
                            t0_epoch,
                            {
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "name": synth_p_name,
                                "namespace": cand.namespace,
                                "placement": chosen_node,
                                "resourceRequest": dict(reqs),
                                "podSetName": ps_name,
                                "isTAS": is_tas,
                            },
                            synth_del_ep,
                        )
                    )

        if not cand_pods or not lq_name or not cq_name:
            # Cannot form a valid ClusterQueue-backed PodGroup: release any claimed pods back to Class 2 infra-pods!
            if cand_pods:
                cand_pod_keys = {(p[0], p[1]) for p in cand_pods}
                for p_idx, pt in enumerate(qualifying_initial_pods):
                    if (pt[0], pt[1]) in cand_pod_keys:
                        claimed_pod_indices.discard(p_idx)
            continue

        ns = cand.namespace
        cq_doc = parsed_infra.cqs_by_name[cq_name]

        # Group pods by `(podSetName_hint, canonical_reqs)` so every PodSet has strictly homogeneous per-pod requests,
        # guaranteeing Kueue's `WorkloadWebhook` `resourceUsage % count == 0` invariant.
        grouped_pods: dict[tuple[str, tuple[tuple[str, float], ...]], list[tuple[str, str, float, dict[str, Any], float | None]]] = {}
        for pt in cand_pods:
            pl = pt[3]
            reqs_raw = dict(pl.get("resourceRequest") or {"cpu": "1", "memory": "1Gi"})
            ps_hint = str(pl.get("podSetName") or "main")
            g_key = (ps_hint, _canonical_resource_profile(reqs_raw))
            grouped_pods.setdefault(g_key, []).append(pt)

        # Kueue's Workload CRD limits `spec.podSets` to `MaxItems=8`; chunk into <=8-PodSet PodGroups if ever needed.
        group_entries = list(grouped_pods.items())
        chunks = [group_entries[i : i + 8] for i in range(0, len(group_entries), 8)]

        raw_meta = (cand.raw_obj.get("metadata") or {}) if isinstance(cand.raw_obj, dict) else {}
        raw_labels = raw_meta.get("labels") or {}
        raw_anns = raw_meta.get("annotations") or {}
        pc_name_raw = (
            str(raw_labels.get("kueue.x-k8s.io/priority-class") or "")
            or (cand.exec_info.priority_class_name if cand.exec_info else "")
        )
        pc_name = validate_dns_label(pc_name_raw, "kueue.x-k8s.io/priority-class") if pc_name_raw else ""

        prio_val: int | None = cand.exec_info.priority if cand.exec_info else None
        if prio_val is None and raw_anns.get("kueue-optimizer.gke.io/priority") is not None:
            try:
                prio_val = int(raw_anns["kueue-optimizer.gke.io/priority"])
            except (TypeError, ValueError):
                prio_val = None
        if prio_val is None and pc_name and pc_name in parsed_infra.priority_classes_by_name:
            try:
                prio_val = int(parsed_infra.priority_classes_by_name[pc_name].get("value") or 0)
            except (TypeError, ValueError):
                prio_val = 0
        if pc_name and prio_val is None:
            prio_val = 1000 - c_idx

        if cand.kind == "PodGroup":
            base_wl_raw = (cand.exec_info.workload_name if cand.exec_info and cand.exec_info.workload_name else "") or (
                cand.name if cand.name else f"pg-init-{c_idx}"
            )
        elif cand.from_workflows_json and cand.raw_obj:
            base_wl_raw = f"t0pg-{cand.name}" if cand.name else f"t0pg-init-{c_idx}"
        else:
            base_wl_raw = (cand.exec_info.workload_name if cand.exec_info and cand.exec_info.workload_name else "") or (
                f"pg-{cand.name}" if cand.name else f"pg-init-{c_idx}"
            )

        # Build dynamic resubmit item (`spec.suspend = True`) if parent workflow object is available from workflows.json.
        resubmit_item: ScheduledWorkflowItem | None = None
        cand_wf_true_dur, cand_wf_mult = _extract_workflow_true_duration_and_multiplier(cand.raw_obj)
        if cand.from_workflows_json and cand.raw_obj and cand.kind != "PodGroup":
            existing_resub_ann = _extract_existing_pod_exec_seconds(cand.raw_obj)
            has_explicit_resub_ann = existing_resub_ann is not None
            fallback_dur = existing_resub_ann or default_exec_sec
            raw_dur_full, found_idx = exec_index.resolve_duration_sec(
                namespace=cand.orig_namespace,
                object_kind=cand.kind,
                name=cand.name,
                fallback_sec=fallback_dur,
                interrupted_duration_multiplier=interrupted_duration_multiplier,
                interrupted_duration_override=interrupted_duration_override,
                workflow_true_duration_sec=cand_wf_true_dur,
                workflow_missing_multiplier=cand_wf_mult,
                creation_epoch=cand.creation_epoch,
                incarnation_index=cand.incarnation_index,
            )
            comp_dur_full = max(min_pod_duration_sec, raw_dur_full / speedup)
            exec_str_full = format_pod_execution_seconds(comp_dur_full, min_seconds=min_pod_duration_sec)
            resub_manifest, resub_pod_cnt = _inject_pod_execution_time_and_sanitize_workflow(
                raw_obj=cand.raw_obj,
                exec_str=exec_str_full,
                raw_duration_sec=raw_dur_full,
                compressed_duration_sec=comp_dur_full,
                pause_image_override=pause_image_override,
                override_namespace=override_workload_namespace,
                extra_pod_annotations={"kueue-optimizer.gke.io/preempted-t0-resubmit": "true"},
                speedup=speedup,
                scale_by_waves=bool(found_idx or not has_explicit_resub_ann),
                min_pod_duration_sec=min_pod_duration_sec,
            )
            resubmit_item = ScheduledWorkflowItem(
                target_wall_offset_sec=0.0,
                seq=100000 + c_idx,
                raw_offset_sec=0.0,
                raw_offset_from_t0_sec=0.0,
                event_timestamp=admit_iso,
                object_kind=cand.kind,
                name=cand.name,
                namespace=ns,
                local_queue_name=lq_name,
                workload_state="NotAdmitted",
                trigger_reason="preempted_after_initial_admission",
                execution_duration_sec=raw_dur_full,
                compressed_duration_sec=comp_dur_full,
                indexed_from_execution_time=found_idx,
                pod_count=resub_pod_cnt,
                manifest=resub_manifest,
                creation_epoch=cand.creation_epoch if cand.creation_epoch is not None else t0_epoch,
                incarnation_index=cand.incarnation_index,
                has_explicit_pod_exec_annotation=has_explicit_resub_ann,
            )

        for chunk_idx, chunk in enumerate(chunks):
            cand_wl_base = base_wl_raw if len(chunks) == 1 else f"{base_wl_raw}-{chunk_idx}"
            if not cand_wl_base[0].isalpha():
                cand_wl_base = f"pg-{cand_wl_base}"
            wl_k8s_name = sanitize_k8s_name(cand_wl_base, max_len=MAX_K8S_NAME_LEN)
            ns_used_wls = used_wl_names_by_ns.setdefault(ns, set())
            dedup = 1
            while wl_k8s_name in ns_used_wls:
                suffix = f"-{dedup}"
                wl_k8s_name = f"{sanitize_k8s_name(cand_wl_base, max_len=MAX_K8S_NAME_LEN - len(suffix))}{suffix}"
                dedup += 1
            ns_used_wls.add(wl_k8s_name)

            total_group_pods = sum(len(pts) for (_gk, pts) in chunk)
            pod_sets_spec: list[dict[str, Any]] = []
            pod_set_assignments: list[dict[str, Any]] = []
            group_pod_specs: list[InitialPodSpec] = []
            any_tas_in_wl = False
            ns_used_pods = used_pod_names_by_ns.setdefault(ns, set())

            for g_idx, ((ps_hint, _prof), pts) in enumerate(chunk):
                role_hash = hashlib.sha256(f"{g_idx}:{ps_hint}:{_prof!r}".encode("utf-8")).hexdigest()[:8]
                rep_reqs = {
                    str(rk): str(rv)
                    for rk, rv in (pts[0][3].get("resourceRequest") or {"cpu": "1", "memory": "1Gi"}).items()
                    if rk != "ephemeral-storage"
                }
                if not rep_reqs:
                    rep_reqs = {"cpu": "1", "memory": "1Gi"}
                placements = [str(pt[3].get("placement") or "") for pt in pts]
                scraped_psas = cand.exec_info.pod_set_assignments if cand.exec_info else []
                psa, psa_uses_tas = _resolve_podset_assignment_for_group(
                    role_hash=role_hash,
                    count=len(pts),
                    reqs=rep_reqs,
                    placements=placements,
                    cq_doc=cq_doc,
                    parsed_infra=parsed_infra,
                    scraped_psas=scraped_psas,
                    ps_hint=ps_hint,
                    group_index=g_idx,
                    total_groups=len(chunk),
                )
                if psa_uses_tas:
                    any_tas_in_wl = True
                pod_set_assignments.append(psa)

                ps_member_specs: list[InitialPodSpec] = []
                for p_ns, p_name, _ev_ep, pl, del_ep in pts:
                    # Fall back to execution-time.json full_end_epoch when individual pod lacked deletionTime,
                    # prevent preemption deletionTime from truncating a t0 workload that later finished FullExecution,
                    # and extend all interrupted-only t0 pods (including ScrapeEnded) using resolved_duration_sec.
                    eff_del_ep = del_ep
                    had_interruption = bool(
                        cand.exec_info and (cand.exec_info.preempted_epochs or cand.exec_info.interrupted_epochs)
                    )
                    if (
                        cand.exec_info
                        and cand.exec_info.full_duration_sec is not None
                        and cand.exec_info.full_duration_sec > 0
                        and had_interruption
                    ):
                        adm_ep = min(cand.exec_info.admitted_epochs) if cand.exec_info.admitted_epochs else t0_epoch
                        unint_full_end = max(
                            adm_ep + cand.exec_info.full_duration_sec,
                            effective_start_epoch + min_pod_duration_sec,
                        )
                        if eff_del_ep is None or unint_full_end > eff_del_ep:
                            eff_del_ep = unint_full_end
                    elif (
                        eff_del_ep is None
                        and cand.exec_info
                        and cand.exec_info.full_end_epoch is not None
                        and cand.exec_info.full_end_epoch > effective_start_epoch
                    ):
                        eff_del_ep = cand.exec_info.full_end_epoch
                    elif cand.exec_info and cand.exec_info.is_interrupted_only:
                        adm_ep = min(cand.exec_info.admitted_epochs) if cand.exec_info.admitted_epochs else t0_epoch
                        resolved_tot = cand.exec_info.resolved_duration_sec(
                            default_sec=default_exec_sec,
                            interrupted_duration_multiplier=interrupted_duration_multiplier,
                            interrupted_duration_override=interrupted_duration_override,
                            workflow_true_duration_sec=cand_wf_true_dur,
                            workflow_missing_multiplier=cand_wf_mult,
                        )
                        unint_end_ep = adm_ep + resolved_tot
                        if eff_del_ep is None or unint_end_ep > eff_del_ep:
                            eff_del_ep = unint_end_ep

                    if eff_del_ep is not None and eff_del_ep > effective_start_epoch:
                        rem_raw = eff_del_ep - effective_start_epoch
                        rem_compressed = max(min_pod_duration_sec, rem_raw / speedup)
                        is_inf = False
                    else:
                        rem_compressed = max(min_pod_duration_sec, infinite_pod_duration_sec / speedup)
                        is_inf = True

                    exec_str = format_pod_execution_seconds(rem_compressed, min_seconds=min_pod_duration_sec)
                    placement = str(pl.get("placement") or "")
                    base_p_name = p_name[2:] if p_name.startswith("p-") else p_name
                    k8s_pod_name = (
                        f"p-{sanitize_k8s_name(base_p_name, max_len=MAX_K8S_NAME_LEN - 2)}"
                        if (len(p_name) > MAX_K8S_NAME_LEN or not p_name[0].isalpha())
                        else p_name
                    )
                    p_dedup = 1
                    while k8s_pod_name in ns_used_pods:
                        suf = f"-{p_dedup}"
                        k8s_pod_name = f"{sanitize_k8s_name(k8s_pod_name, max_len=MAX_K8S_NAME_LEN - len(suf))}{suf}"
                        p_dedup += 1
                    ns_used_pods.add(k8s_pod_name)

                    p_labels: dict[str, str] = {
                        "kueue-optimizer.gke.io/initial-pod": "true",
                        "kueue-optimizer.gke.io/testrunner": "true",
                        "kueue.x-k8s.io/managed": "true",
                        "kueue.x-k8s.io/pod-group-name": wl_k8s_name,
                        "kueue.x-k8s.io/queue-name": lq_name,
                    }
                    if pc_name:
                        p_labels["kueue.x-k8s.io/priority-class"] = pc_name

                    p_anns: dict[str, str] = {
                        "kueue.x-k8s.io/pod-group-total-count": str(total_group_pods),
                        "kueue.x-k8s.io/role-hash": role_hash,
                        "kueue.x-k8s.io/retriable-in-group": "false",
                        "kueue.x-k8s.io/workload": wl_k8s_name,
                        "kueue-optimizer.gke.io/pod-execution-time-seconds": exec_str,
                        "kueue-optimizer.gke.io/original-name": p_name,
                        "kueue-optimizer.gke.io/original-namespace": p_ns,
                    }
                    if psa_uses_tas:
                        p_anns["kueue.x-k8s.io/podset-unconstrained-topology"] = "true"
                    if is_inf:
                        p_anns["kueue-optimizer.gke.io/infinite-duration"] = "true"

                    pod_manifest: dict[str, Any] = {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": k8s_pod_name,
                            "namespace": ns,
                            "labels": p_labels,
                            "annotations": p_anns,
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
                                        "requests": dict(rep_reqs),
                                        "limits": dict(rep_reqs),
                                    },
                                }
                            ],
                        },
                    }
                    if placement:
                        pod_manifest["spec"]["nodeName"] = placement

                    ps_member_specs.append(
                        InitialPodSpec(
                            name=k8s_pod_name,
                            namespace=ns,
                            placement=placement,
                            resource_request=dict(rep_reqs),
                            remaining_duration_sec=rem_compressed,
                            is_infinite=is_inf,
                            manifest=pod_manifest,
                        )
                    )

                rep_exec_str = format_pod_execution_seconds(
                    max(p.remaining_duration_sec for p in ps_member_specs),
                    min_seconds=min_pod_duration_sec,
                )
                tpl_labels = {
                    "kueue.x-k8s.io/managed": "true",
                    "kueue.x-k8s.io/pod-group-name": wl_k8s_name,
                    "kueue.x-k8s.io/queue-name": lq_name,
                }
                if pc_name:
                    tpl_labels["kueue.x-k8s.io/priority-class"] = pc_name
                tpl_anns = {
                    "kueue.x-k8s.io/pod-group-total-count": str(total_group_pods),
                    "kueue.x-k8s.io/role-hash": role_hash,
                    "kueue.x-k8s.io/retriable-in-group": "false",
                    "kueue-optimizer.gke.io/pod-execution-time-seconds": rep_exec_str,
                }
                if psa_uses_tas:
                    tpl_anns["kueue.x-k8s.io/podset-unconstrained-topology"] = "true"
                if all(p.is_infinite for p in ps_member_specs):
                    tpl_anns["kueue-optimizer.gke.io/infinite-duration"] = "true"

                ps_spec_entry: dict[str, Any] = {
                    "name": role_hash,
                    "count": len(pts),
                    "template": {
                        "metadata": {
                            "labels": tpl_labels,
                            "annotations": tpl_anns,
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
                                        "requests": dict(rep_reqs),
                                        "limits": dict(rep_reqs),
                                    },
                                }
                            ],
                        },
                    },
                }
                if psa_uses_tas:
                    ps_spec_entry["topologyRequest"] = {"unconstrained": True}
                pod_sets_spec.append(ps_spec_entry)
                group_pod_specs.extend(ps_member_specs)

            wl_labels: dict[str, str] = {
                "kueue-optimizer.gke.io/initial-workload": "true",
                "kueue-optimizer.gke.io/testrunner": "true",
            }
            if pc_name:
                wl_labels["kueue.x-k8s.io/priority-class"] = pc_name

            wl_spec: dict[str, Any] = {
                "active": True,
                "queueName": lq_name,
                "priority": int(prio_val if prio_val is not None else 0),
                "podSets": pod_sets_spec,
            }
            if pc_name:
                wl_spec["priorityClassRef"] = {
                    "group": "kueue.x-k8s.io",
                    "kind": "WorkloadPriorityClass",
                    "name": pc_name,
                }

            wl_manifest: dict[str, Any] = {
                "apiVersion": "kueue.x-k8s.io/v1beta2",
                "kind": "Workload",
                "metadata": {
                    "name": wl_k8s_name,
                    "namespace": ns,
                    "labels": wl_labels,
                    "annotations": {
                        "kueue.x-k8s.io/is-group-workload": "true",
                        "kueue-optimizer.gke.io/original-owner-kind": cand.kind,
                        "kueue-optimizer.gke.io/original-owner-name": cand.name,
                    },
                },
                "spec": wl_spec,
            }
            wl_status: dict[str, Any] = {
                "admission": {
                    "clusterQueue": cq_name,
                    "podSetAssignments": pod_set_assignments,
                },
                "conditions": [
                    {
                        "type": "QuotaReserved",
                        "status": "True",
                        "reason": "QuotaReserved",
                        "message": "Pre-admitted initial running workload at scenario start",
                        "lastTransitionTime": admit_iso,
                        "observedGeneration": 1,
                    },
                    {
                        "type": "Admitted",
                        "status": "True",
                        "reason": "Admitted",
                        "message": "Pre-admitted initial running workload at scenario start",
                        "lastTransitionTime": admit_iso,
                        "observedGeneration": 1,
                    },
                    {
                        "type": "PodsReady",
                        "status": "True",
                        "reason": "Started",
                        "message": "All pods reached readiness and the workload is running",
                        "lastTransitionTime": admit_iso,
                        "observedGeneration": 1,
                    },
                ],
            }

            orig_ref_ep: float | None = None
            if cand.exec_info and cand.exec_info.admitted_epochs:
                orig_ref_ep = min(cand.exec_info.admitted_epochs)
            if cand.creation_epoch is not None and (orig_ref_ep is None or orig_ref_ep >= effective_start_epoch - 1e-6):
                orig_ref_ep = min(orig_ref_ep, cand.creation_epoch) if orig_ref_ep is not None else cand.creation_epoch
            orig_age_sec = (
                max(0.0, (effective_start_epoch - orig_ref_ep) / max(1e-6, speedup))
                if orig_ref_ep is not None
                else 0.0
            )

            if cand.deleted_epoch is not None and cand.deleted_epoch > effective_start_epoch + 1e-6:
                for del_ev in scheduled_deletions:
                    if (
                        del_ev.namespace == ns
                        and del_ev.object_kind == cand.kind
                        and del_ev.name == cand.name
                        and del_ev.incarnation_index == cand.incarnation_index
                        and not del_ev.initial_workload_name
                    ):
                        del_ev.initial_workload_name = wl_k8s_name

            initial_workloads.append(
                InitialWorkloadPodGroup(
                    workload_name=wl_k8s_name,
                    namespace=ns,
                    local_queue_name=lq_name,
                    cluster_queue_name=cq_name,
                    owner_kind=cand.kind,
                    owner_name=cand.name,
                    priority_class_name=pc_name,
                    priority=prio_val,
                    is_tas=any_tas_in_wl,
                    pods=group_pod_specs,
                    workload_manifest=wl_manifest,
                    workload_status=wl_status,
                    resubmit_item=resubmit_item if chunk_idx == 0 else None,
                    original_age_sec=orig_age_sec,
                )
            )

        if cand.preempt_item is not None:
            superseded_preempt_seqs.add(cand.preempt_item.seq)

    # Order initial running workloads oldest-first so Kueue admission/creation order matches trace seniority.
    initial_workloads.sort(key=lambda iwl: (-iwl.original_age_sec, iwl.workload_name))
    initial_deployments.sort(key=lambda idep: (-idep.original_age_sec, idep.applied_name))

    # Remove any static historical preemption resubmissions now handled dynamically on Evicted=True,
    # and keep scheduled_workflows sorted by (target_wall_offset_sec, creation_epoch, raw_offset_from_t0_sec, seq).
    if superseded_preempt_seqs:
        scheduled_workflows = [w for w in scheduled_workflows if w.seq not in superseded_preempt_seqs]
    scheduled_workflows.sort(
        key=lambda item: (
            round(item.target_wall_offset_sec, 6),
            item.creation_epoch,
            item.raw_offset_from_t0_sec,
            item.seq,
        )
    )

    # Remaining unclaimed initial pods are Class 2 true non-Kueue infrastructure/system pods (`infra-pods`).
    initial_infra_pods: list[InitialPodSpec] = []
    used_infra_pod_names: set[str] = set()
    for p_idx, (p_ns, p_name, _ev_epoch, pl, del_ep) in enumerate(qualifying_initial_pods):
        if p_idx in claimed_pod_indices:
            continue
        if del_ep is not None and del_ep > effective_start_epoch:
            rem_raw = del_ep - effective_start_epoch
            rem_compressed = max(min_pod_duration_sec, rem_raw / speedup)
            is_inf = False
        else:
            rem_compressed = max(min_pod_duration_sec, infinite_pod_duration_sec / speedup)
            is_inf = True

        exec_str = format_pod_execution_seconds(rem_compressed, min_seconds=min_pod_duration_sec)
        placement = str(pl.get("placement") or "")
        reqs = {
            str(rk): str(rv)
            for rk, rv in (pl.get("resourceRequest") or {"cpu": "1", "memory": "1Gi"}).items()
            if rk != "ephemeral-storage"
        }
        if not reqs:
            reqs = {"cpu": "1", "memory": "1Gi"}
        base_p_name = p_name[2:] if p_name.startswith("p-") else p_name
        k8s_pod_name = (
            f"p-{sanitize_k8s_name(base_p_name, max_len=MAX_K8S_NAME_LEN - 2)}"
            if (len(p_name) > MAX_K8S_NAME_LEN or not p_name[0].isalpha())
            else p_name
        )
        i_dedup = 1
        while k8s_pod_name in used_infra_pod_names:
            suf = f"-{i_dedup}"
            k8s_pod_name = f"{sanitize_k8s_name(k8s_pod_name, max_len=MAX_K8S_NAME_LEN - len(suf))}{suf}"
            i_dedup += 1
        used_infra_pod_names.add(k8s_pod_name)

        pod_manifest = {
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
                    **({"kueue-optimizer.gke.io/infinite-duration": "true"} if is_inf else {}),
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

        initial_infra_pods.append(
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

    return (
        initial_workloads,
        initial_deployments,
        initial_infra_pods,
        scheduled_workflows,
        expired_initial_pods_count,
    )


def _schedule_post_t0_non_kueue_pods(
    parsed_infra: _ParsedInfra,
    scheduled_workflows: list[ScheduledWorkflowItem],
    initial_workloads: list[InitialWorkloadPodGroup],
    initial_deployments: list[InitialDeploymentBootstrap] | None = None,
    *,
    t0_epoch: float,
    effective_start_epoch: float,
    initial_window_sec: float,
    speedup: float,
    min_pod_duration_sec: float,
    infinite_pod_duration_sec: float,
    pause_image_override: str | None,
    infra_pods_namespace: str,
) -> None:
    """Schedule post-t0 non-Kueue pods into infra_pods_namespace without duplicating Kueue workflow pods."""
    has_any_enriched_linkage = any(
        bool(pl.get("workloadName") or pl.get("ownerName"))
        for (_ep, pl) in parsed_infra.pod_new_events.values()
    )
    kueue_wf_names_by_ns: dict[str, set[str]] = {}
    for sw in scheduled_workflows:
        kueue_wf_names_by_ns.setdefault(sw.namespace, set()).add(sw.name)
    for iwl in initial_workloads:
        if iwl.owner_name:
            kueue_wf_names_by_ns.setdefault(iwl.namespace, set()).add(iwl.owner_name)
    for idep in initial_deployments or []:
        if idep.name:
            kueue_wf_names_by_ns.setdefault(idep.namespace, set()).add(idep.name)
        if idep.applied_name:
            kueue_wf_names_by_ns.setdefault(idep.namespace, set()).add(idep.applied_name)

    kueue_prefixes_by_ns: dict[str, tuple[str, ...]] = {
        ns: tuple(
            f"{nm}-"
            for nm in sorted(names)
            if nm and not (nm.startswith("h-") and len(nm) == 34)
        )
        for ns, names in kueue_wf_names_by_ns.items()
    }

    pause_img = pause_image_override or DEFAULT_PAUSE_IMAGE
    next_seq = max((ie.seq for ie in parsed_infra.scheduled_infra_events), default=-1) + 1
    used_names: set[str] = {
        str((ie.manifest.get("metadata") or {}).get("name") or ie.name)
        for ie in parsed_infra.scheduled_infra_events
        if ie.object_kind == "Pod"
    }

    cutoff = max(t0_epoch + initial_window_sec, effective_start_epoch) + 1e-6
    for (p_ns, p_name), (ev_ep, pl) in sorted(
        parsed_infra.pod_new_events.items(), key=lambda x: (x[1][0], x[0])
    ):
        if ev_ep <= cutoff:
            continue
        p_meta = pl.get("metadata") or {}
        p_labels = p_meta.get("labels") or {}
        p_owner_kind = str(pl.get("ownerKind") or "")
        p_owner_name = str(pl.get("ownerName") or "")
        is_kueue_pod = bool(
            pl.get("workloadName")
            or p_labels.get("kueue.x-k8s.io/queue-name")
            or _get_pod_meta_annotation_or_label(p_meta, "kueue.x-k8s.io/pod-group-name")
            or p_labels.get("kueue.x-k8s.io/managed") == "true"
        )
        if not is_kueue_pod and p_owner_name and p_owner_kind not in ("DaemonSet", "StatefulSet", "Node"):
            if p_owner_name in kueue_wf_names_by_ns.get(p_ns, set()):
                is_kueue_pod = True
        if not is_kueue_pod:
            ns_names = kueue_wf_names_by_ns.get(p_ns, set())
            ns_pfxs = kueue_prefixes_by_ns.get(p_ns, ())
            if p_name in ns_names or (ns_pfxs and p_name.startswith(ns_pfxs)):
                is_kueue_pod = True
            elif ns_names and p_name.startswith("h-") and len(p_name) == 34 and not has_any_enriched_linkage:
                # Legacy unenriched --hash-names trace: cannot distinguish hashed Kueue pod from unmanaged pod in same ns.
                is_kueue_pod = True
        if is_kueue_pod:
            continue

        del_ep = parsed_infra.pod_delete_epochs.get((p_ns, p_name))
        if del_ep is not None and del_ep <= ev_ep:
            continue
        if del_ep is not None and del_ep > ev_ep:
            raw_pod_dur = del_ep - ev_ep
            comp_pod_dur = max(min_pod_duration_sec, raw_pod_dur / max(1e-6, speedup))
            is_inf = False
        else:
            comp_pod_dur = max(min_pod_duration_sec, infinite_pod_duration_sec / max(1e-6, speedup))
            is_inf = True
        exec_str = format_pod_execution_seconds(comp_pod_dur, min_seconds=min_pod_duration_sec)
        placement = str(pl.get("placement") or "")
        reqs = {
            str(rk): str(rv)
            for rk, rv in (pl.get("resourceRequest") or {"cpu": "1", "memory": "1Gi"}).items()
            if rk != "ephemeral-storage"
        }
        if not reqs:
            reqs = {"cpu": "1", "memory": "1Gi"}
        base_p_name = p_name[2:] if p_name.startswith("p-") else p_name
        k8s_pod_name = (
            f"p-{sanitize_k8s_name(base_p_name, max_len=MAX_K8S_NAME_LEN - 2)}"
            if (len(p_name) > MAX_K8S_NAME_LEN or not p_name[0].isalpha())
            else p_name
        )
        p_dedup = 1
        while k8s_pod_name in used_names:
            suf = f"-{p_dedup}"
            k8s_pod_name = f"{sanitize_k8s_name(k8s_pod_name, max_len=MAX_K8S_NAME_LEN - len(suf))}{suf}"
            p_dedup += 1
        used_names.add(k8s_pod_name)
        ev_ts_iso = datetime.fromtimestamp(ev_ep, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        pod_doc: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": k8s_pod_name,
                "namespace": infra_pods_namespace,
                "labels": {
                    "kueue-optimizer.gke.io/post-t0-infra-pod": "true",
                    "kueue-optimizer.gke.io/testrunner": "true",
                },
                "annotations": {
                    "kueue-optimizer.gke.io/pod-execution-time-seconds": exec_str,
                    "kueue-optimizer.gke.io/original-name": p_name,
                    "kueue-optimizer.gke.io/original-namespace": p_ns,
                    **({"kueue-optimizer.gke.io/infinite-duration": "true"} if is_inf else {}),
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
                        "resources": {"requests": reqs, "limits": dict(reqs)},
                    }
                ],
            },
        }
        if placement:
            pod_doc["spec"]["nodeName"] = placement
        raw_off = max(0.0, ev_ep - t0_epoch)
        wall_off = raw_off / max(1e-6, speedup)
        parsed_infra.scheduled_infra_events.append(
            ScheduledInfraEvent(
                target_wall_offset_sec=wall_off,
                seq=next_seq,
                raw_offset_sec=raw_off,
                raw_offset_from_t0_sec=raw_off,
                event_timestamp=ev_ts_iso,
                event_type="New",
                object_kind="Pod",
                name=k8s_pod_name,
                namespace=infra_pods_namespace,
                manifest=pod_doc,
            )
        )
        next_seq += 1

    parsed_infra.scheduled_infra_events.sort(key=lambda item: (item.target_wall_offset_sec, item.seq))


def _apply_missing_duration_policy(
    scheduled_workflows: list[ScheduledWorkflowItem],
    exec_index: ExecutionTimeIndex,
    *,
    policy: str,
    default_exec_sec: float,
    speedup: float,
    min_pod_duration_sec: float,
) -> tuple[list[str], int]:
    """Handle scheduled workflows that lack both an execution-time.json entry and a pod duration annotation.

    Policies:
      - 'impute': impute from same-(queue, shape) peer median, falling back to queue/global median, then default_exec_sec.
      - 'fallback': keep default_exec_sec and emit a warning.
      - 'error': raise ValueError listing all workflows missing recorded durations.
    """
    norm_policy = str(policy or "impute").strip().lower()
    if norm_policy not in ("impute", "fallback", "error"):
        raise ValueError(
            f"Unsupported missing_duration_policy {policy!r}; expected one of: 'impute', 'fallback', 'error'."
        )

    def _wf_shape_sig(sw: ScheduledWorkflowItem) -> tuple[tuple[str, float], ...]:
        m_kind = str(sw.manifest.get("kind") or sw.object_kind)
        groups = _extract_workflow_podset_templates(sw.manifest, m_kind)
        tot: dict[str, float] = {}
        for _ps_n, cnt, reqs, _tas in groups:
            for rk, rv in reqs.items():
                if rk not in ("pods", "ephemeral-storage"):
                    tot[rk] = round(tot.get(rk, 0.0) + cnt * parse_k8s_quantity(rv, rk), 6)
        return tuple(sorted((k, v) for k, v in tot.items() if v > 0))

    missing_items = [
        sw
        for sw in scheduled_workflows
        if sw.object_kind != "Deployment"
        and not sw.indexed_from_execution_time
        and not sw.has_explicit_pod_exec_annotation
    ]
    if not missing_items:
        return [], 0

    if norm_policy == "error":
        preview = ", ".join(f"{sw.namespace}/{sw.object_kind}/{sw.name}" for sw in missing_items[:10])
        more = f" (+{len(missing_items) - 10} more)" if len(missing_items) > 10 else ""
        raise ValueError(
            f"{len(missing_items)} workflow(s) have no recorded execution duration in execution-time.json "
            f"(missing_duration_policy='error'): {preview}{more}."
        )

    tier_exact: dict[tuple[str, str, str, int, tuple[tuple[str, float], ...]], list[float]] = {}
    tier_lq_pods: dict[tuple[str, str, int], list[float]] = {}
    tier_lq: dict[str, list[float]] = {}
    global_durs: list[float] = []

    for sw in scheduled_workflows:
        if sw.indexed_from_execution_time and sw.execution_duration_sec > 0:
            sig = _wf_shape_sig(sw)
            tier_exact.setdefault(
                (sw.namespace, sw.local_queue_name, sw.object_kind, sw.pod_count, sig), []
            ).append(sw.execution_duration_sec)
            tier_lq_pods.setdefault((sw.namespace, sw.local_queue_name, sw.pod_count), []).append(
                sw.execution_duration_sec
            )
            if sw.local_queue_name:
                tier_lq.setdefault(sw.local_queue_name, []).append(sw.execution_duration_sec)
            global_durs.append(sw.execution_duration_sec)

    for w_info in exec_index.all_workloads:
        if (w_info.full_duration_sec is not None and w_info.full_duration_sec > 0) or w_info.partial_durations_sec:
            d_val = w_info.resolved_duration_sec(default_sec=default_exec_sec)
            if d_val > 0:
                if w_info.local_queue_name and w_info.local_queue_name not in tier_lq:
                    tier_lq.setdefault(w_info.local_queue_name, []).append(d_val)
                global_durs.append(d_val)

    imputed_count = 0
    fallback_count = 0
    for sw in missing_items:
        chosen_dur: float | None = None
        if norm_policy == "impute":
            sig = _wf_shape_sig(sw)
            cands = (
                tier_exact.get((sw.namespace, sw.local_queue_name, sw.object_kind, sw.pod_count, sig))
                or tier_lq_pods.get((sw.namespace, sw.local_queue_name, sw.pod_count))
                or (tier_lq.get(sw.local_queue_name) if sw.local_queue_name else None)
                or global_durs
            )
            if cands:
                chosen_dur = float(statistics.median(cands))
        if chosen_dur is not None and chosen_dur > 0:
            imputed_count += 1
            sw.execution_duration_sec = chosen_dur
            sw.compressed_duration_sec = max(min_pod_duration_sec, chosen_dur / max(1e-6, speedup))
            new_exec_str = format_pod_execution_seconds(sw.compressed_duration_sec, min_seconds=min_pod_duration_sec)
            # Re-stamp pod-execution-time-seconds (respecting Job/JobSet completion waves) onto sw.manifest.
            m_kind = str(sw.manifest.get("kind") or sw.object_kind)
            m_ann = (sw.manifest.setdefault("metadata", {})).setdefault("annotations", {})
            m_ann["kueue-optimizer.gke.io/real-duration-sec"] = f"{sw.execution_duration_sec:.2f}"
            m_ann["kueue-optimizer.gke.io/compressed-duration-sec"] = f"{sw.compressed_duration_sec:.3f}"
            m_ann["kueue-optimizer.gke.io/imputed-duration"] = "true"
            m_spec = sw.manifest.get("spec") or {}
            if m_kind == "Job":
                par = max(1, int(m_spec.get("parallelism") or m_spec.get("completions") or 1))
                comp = max(par, int(m_spec.get("completions") or par))
                waves = (comp + par - 1) // par
                per_wave = (
                    format_pod_execution_seconds(
                        max(min_pod_duration_sec, sw.compressed_duration_sec / waves),
                        min_seconds=min_pod_duration_sec,
                    )
                    if waves > 1
                    else new_exec_str
                )
                m_ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = per_wave
                (((m_spec.setdefault("template", {})).setdefault("metadata", {})).setdefault("annotations", {}))[
                    "kueue-optimizer.gke.io/pod-execution-time-seconds"
                ] = per_wave
            elif m_kind == "JobSet":
                m_ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = new_exec_str
                for rj in m_spec.get("replicatedJobs") or []:
                    j_sp = (rj.setdefault("template", {})).setdefault("spec", {})
                    par = max(1, int(j_sp.get("parallelism") or j_sp.get("completions") or 1))
                    comp = max(par, int(j_sp.get("completions") or par))
                    waves = (comp + par - 1) // par
                    per_wave = (
                        format_pod_execution_seconds(
                            max(min_pod_duration_sec, sw.compressed_duration_sec / waves),
                            min_seconds=min_pod_duration_sec,
                        )
                        if waves > 1
                        else new_exec_str
                    )
                    (((j_sp.setdefault("template", {})).setdefault("metadata", {})).setdefault("annotations", {}))[
                        "kueue-optimizer.gke.io/pod-execution-time-seconds"
                    ] = per_wave
            elif m_kind == "Deployment":
                m_ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = new_exec_str
                (((m_spec.setdefault("template", {})).setdefault("metadata", {})).setdefault("annotations", {}))[
                    "kueue-optimizer.gke.io/pod-execution-time-seconds"
                ] = new_exec_str
        else:
            fallback_count += 1

    sample_names = ", ".join(f"{sw.namespace}/{sw.name}" for sw in missing_items[:5])
    more_s = f" (+{len(missing_items) - 5} more)" if len(missing_items) > 5 else ""
    warnings: list[str] = []
    if imputed_count > 0:
        warnings.append(
            f"{imputed_count} never-admitted/unindexed workflow(s) lacked execution-time.json entries and were "
            f"imputed from peer median duration ({sample_names}{more_s})."
        )
    if fallback_count > 0:
        warnings.append(
            f"{fallback_count} never-admitted/unindexed workflow(s) lacked execution-time.json entries and fell back "
            f"to default_exec_sec={default_exec_sec:g}s ({sample_names}{more_s})."
        )
    return warnings, imputed_count


def _synthesize_referenced_objects(
    parsed_infra: _ParsedInfra,
    initial_workloads: list[InitialWorkloadPodGroup],
    initial_deployments: list[InitialDeploymentBootstrap],
    initial_pods: list[InitialPodSpec],
    scheduled_workflows: list[ScheduledWorkflowItem],
    scheduled_updates: list[ScheduledWorkflowUpdate],
    infra_pods_namespace: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Synthesize missing Namespaces, Cohorts, WorkloadPriorityClasses, PriorityClasses, and RuntimeClasses."""
    referenced_namespaces: set[str] = set(parsed_infra.namespaces_by_name.keys())
    referenced_namespaces.add(infra_pods_namespace)
    for lq_ns, _ in parsed_infra.lqs_by_key:
        referenced_namespaces.add(lq_ns)
    for lq_ns, _ in parsed_infra.post_t0_lq_keys:
        referenced_namespaces.add(lq_ns)
    for ie in parsed_infra.scheduled_infra_events:
        if ie.namespace:
            referenced_namespaces.add(ie.namespace)
        elif ie.object_kind == "Namespace" and ie.name:
            referenced_namespaces.add(ie.name)
    for iwl in initial_workloads:
        referenced_namespaces.add(iwl.namespace)
    for idep in initial_deployments:
        referenced_namespaces.add(idep.namespace)
    for ipod in initial_pods:
        referenced_namespaces.add(ipod.namespace)
    for sw in scheduled_workflows:
        referenced_namespaces.add(sw.namespace)
    for su in scheduled_updates:
        referenced_namespaces.add(su.namespace)

    parsed_infra.namespaces_by_name[infra_pods_namespace] = build_namespace_manifest(
        infra_pods_namespace,
        part_of="kueue-optimizer-testrunner",
        exclude_from_kueue=True,
    )
    for ns_name in sorted(referenced_namespaces):
        if ns_name not in SYSTEM_NAMESPACES and ns_name not in parsed_infra.namespaces_by_name:
            parsed_infra.namespaces_by_name[ns_name] = build_namespace_manifest(
                ns_name,
                part_of="kueue-optimizer-testrunner",
                exclude_from_kueue=(ns_name == infra_pods_namespace),
            )

    # Synthesize any Cohort or WorkloadPriorityClass objects referenced by ClusterQueues or Workflows
    for cq_doc in parsed_infra.cqs_by_name.values():
        c_name = (cq_doc.get("spec") or {}).get("cohortName")
        if c_name:
            c_name_clean = validate_dns_label(c_name, "ClusterQueue spec.cohortName")
            if c_name_clean not in parsed_infra.cohorts_by_name:
                parsed_infra.cohorts_by_name[c_name_clean] = build_cohort_manifest(c_name_clean)
    for ie in parsed_infra.scheduled_infra_events:
        if ie.object_kind == "ClusterQueue" and ie.manifest:
            c_name = (ie.manifest.get("spec") or {}).get("cohortName")
            if c_name:
                c_name_clean = validate_dns_label(c_name, "ClusterQueue spec.cohortName")
                if c_name_clean not in parsed_infra.cohorts_by_name:
                    parsed_infra.cohorts_by_name[c_name_clean] = build_cohort_manifest(c_name_clean)

    for idx_iwl, iwl in enumerate(initial_workloads):
        if iwl.priority_class_name and iwl.priority_class_name not in parsed_infra.priority_classes_by_name:
            prio_val = int(iwl.priority if iwl.priority is not None else (1000 - idx_iwl))
            parsed_infra.priority_classes_by_name[iwl.priority_class_name] = build_priority_class_manifest(
                iwl.priority_class_name, value=prio_val
            )
        for p_init in iwl.pods:
            p_sp_init = (p_init.manifest.get("spec") or {}) if isinstance(p_init.manifest, dict) else {}
            pc_k8s_raw = str(p_sp_init.get("priorityClassName") or "").strip()
            if pc_k8s_raw and pc_k8s_raw not in ("system-cluster-critical", "system-node-critical"):
                pc_k8s = validate_dns_label(pc_k8s_raw, "PodSpec priorityClassName")
                if pc_k8s not in parsed_infra.k8s_priority_classes_by_name:
                    parsed_infra.k8s_priority_classes_by_name[pc_k8s] = {
                        "apiVersion": "scheduling.k8s.io/v1",
                        "kind": "PriorityClass",
                        "metadata": {"name": pc_k8s},
                        "value": 1000,
                        "globalDefault": False,
                    }

    for idx_idep, idep in enumerate(initial_deployments):
        if idep.priority_class_name and idep.priority_class_name not in parsed_infra.priority_classes_by_name:
            prio_val_dep = int(idep.priority if idep.priority is not None else (1000 - idx_idep))
            parsed_infra.priority_classes_by_name[idep.priority_class_name] = build_priority_class_manifest(
                idep.priority_class_name, value=prio_val_dep
            )

    all_wf_manifests: list[tuple[str, dict[str, Any]]] = [
        (sw.object_kind, sw.manifest) for sw in scheduled_workflows
    ] + [
        (iwl.resubmit_item.object_kind, iwl.resubmit_item.manifest)
        for iwl in initial_workloads
        if iwl.resubmit_item is not None
    ] + [
        ("Deployment", idep.manifest) for idep in initial_deployments
    ] + [
        (su.object_kind, su.manifest) for su in scheduled_updates
    ]
    for idx_pc, (wf_obj_kind, wf_manifest) in enumerate(all_wf_manifests):
        w_meta = wf_manifest.get("metadata") or {}
        w_labels = w_meta.get("labels") or {}
        w_ann = w_meta.get("annotations") or {}
        pc_name = w_labels.get("kueue.x-k8s.io/priority-class")
        if pc_name:
            pc_name_clean = validate_dns_label(pc_name, "kueue.x-k8s.io/priority-class")
            if pc_name_clean not in parsed_infra.priority_classes_by_name:
                prio_val = int(w_ann.get("kueue-optimizer.gke.io/priority") or (1000 - idx_pc))
                parsed_infra.priority_classes_by_name[pc_name_clean] = build_priority_class_manifest(
                    pc_name_clean, value=prio_val
                )
        # Synthesize RuntimeClass and scheduling.k8s.io/v1 PriorityClass referenced by pod templates
        w_spec = wf_manifest.get("spec") or {}
        m_kind = str(wf_manifest.get("kind") or wf_obj_kind)
        candidate_pod_specs: list[dict[str, Any]] = []
        if m_kind == "JobSet":
            for rj in w_spec.get("replicatedJobs") or []:
                candidate_pod_specs.append(
                    (((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get("spec") or {}
                )
        elif m_kind in ("Job", "Deployment"):
            candidate_pod_specs.append(((w_spec.get("template") or {}).get("spec")) or {})
        elif m_kind == "Pod":
            candidate_pod_specs.append(w_spec)
        for p_sp in candidate_pod_specs:
            if not isinstance(p_sp, dict):
                continue
            rc_raw = str(p_sp.get("runtimeClassName") or "").strip()
            ov_b = p_sp.get("overhead") if isinstance(p_sp.get("overhead"), dict) else None
            if rc_raw:
                rc_n = validate_dns_label(rc_raw, "PodSpec runtimeClassName")
                if rc_n not in parsed_infra.runtime_classes_by_name:
                    rc_doc: dict[str, Any] = {
                        "apiVersion": "node.k8s.io/v1",
                        "kind": "RuntimeClass",
                        "metadata": {"name": rc_n},
                        "handler": "kwok",
                    }
                    if ov_b:
                        rc_doc["overhead"] = {"podFixed": {str(k): str(v) for k, v in ov_b.items()}}
                    parsed_infra.runtime_classes_by_name[rc_n] = rc_doc
            pc_k8s_raw = str(p_sp.get("priorityClassName") or "").strip()
            if pc_k8s_raw and pc_k8s_raw not in ("system-cluster-critical", "system-node-critical"):
                pc_k8s = validate_dns_label(pc_k8s_raw, "PodSpec priorityClassName")
                if pc_k8s not in parsed_infra.k8s_priority_classes_by_name:
                    parsed_infra.k8s_priority_classes_by_name[pc_k8s] = {
                        "apiVersion": "scheduling.k8s.io/v1",
                        "kind": "PriorityClass",
                        "metadata": {"name": pc_k8s},
                        "value": 1000,
                        "globalDefault": False,
                    }

    prereq_and_queue_docs: list[dict[str, Any]] = []
    prereq_and_queue_docs.extend(parsed_infra.namespaces_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.runtime_classes_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.k8s_priority_classes_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.topologies_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.flavors_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.cohorts_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.priority_classes_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.cqs_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.lqs_by_key.values())

    workload_namespaces = sorted(referenced_namespaces) if referenced_namespaces else ["default"]
    return prereq_and_queue_docs, workload_namespaces


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
    interrupted_duration_multiplier: float | None = None,
    interrupted_duration_override: str | float | dict[str, Any] | InterruptedDurationOverride | None = None,
    missing_duration_policy: str = "impute",
) -> ScenarioBundle:
    """Index execution-time.json first, then load infra.json and workflows.json into a ScenarioBundle."""
    if interrupted_duration_multiplier is not None and interrupted_duration_multiplier <= 1.0:
        raise ValueError(
            f"--interrupted-duration-multiplier must be > 1.0 so interrupted workloads replay longer "
            f"than their interrupted recording (got {interrupted_duration_multiplier!r})."
        )
    parsed_override = parse_interrupted_duration_override(interrupted_duration_override)

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

    validate_workflow_events(workflow_events, wf_file)

    t0_epoch, t0_iso, t_end_epoch, t_end_iso = _compute_timeline_bounds(
        infra_event_epochs, workflow_event_epochs, exec_index
    )

    parsed_infra = _parse_infra_resources(
        infra_events,
        infra_event_epochs,
        inf_file,
        t0_epoch=t0_epoch,
        initial_window_sec=initial_window_sec,
        speedup=speedup,
        min_pod_duration_sec=min_pod_duration_sec,
        infinite_pod_duration_sec=infinite_pod_duration_sec,
        pause_image_override=pause_image_override,
        infra_pods_namespace=infra_pods_namespace,
        override_workload_namespace=override_workload_namespace,
        override_queueing_strategy=override_queueing_strategy,
        override_cq_fair_sharing_preemption=override_cq_fair_sharing_preemption,
    )

    kueue_cfg = parsed_infra.kueue_configuration
    if kueue_cfg is None:
        standalone_cfg_json = resolved_dir / "kueue-config.json"
        if standalone_cfg_json.is_file():
            import json as _json

            kueue_cfg = parse_kueue_configuration_payload(
                _json.loads(standalone_cfg_json.read_text(encoding="utf-8")),
                source_desc=str(standalone_cfg_json),
            )
    manage_without_queue = bool(kueue_cfg and kueue_cfg.manage_jobs_without_queue_name)

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

    (
        scheduled_workflows,
        scheduled_updates,
        scheduled_deletions,
        admitted_candidates,
        skipped_admitted,
        skipped_child_count,
    ) = _schedule_workflows(
        workflow_events,
        workflow_event_epochs,
        known_parents,
        exec_index,
        t0_epoch=t0_epoch,
        speedup=speedup,
        min_pod_duration_sec=min_pod_duration_sec,
        default_exec_sec=default_exec_sec,
        pause_image_override=pause_image_override,
        override_workload_namespace=override_workload_namespace,
        interrupted_duration_multiplier=interrupted_duration_multiplier,
        interrupted_duration_override=parsed_override,
        manage_jobs_without_queue_name=manage_without_queue,
    )

    # Compute jump target from genuine NotAdmitted arrivals and post-t0 infra/update/deletion events so dynamic
    # t0 preemption items don't distort t_start and we never jump past an early Node/Queue/Update/Delete event.
    initial_jump_raw_sec = 0.0
    initial_jump_wall_sec = 0.0
    if skip_initial_offset and (
        scheduled_workflows
        or parsed_infra.scheduled_infra_events
        or scheduled_updates
        or scheduled_deletions
    ):
        cand_raw_offsets: list[float] = []
        if scheduled_workflows:
            not_adm_items = [
                w
                for w in scheduled_workflows
                if w.trigger_reason in ("initial_not_admitted", "post_t0_fast_admitted")
            ]
            anchor = not_adm_items[0] if not_adm_items else scheduled_workflows[0]
            cand_raw_offsets.append(max(0.0, anchor.raw_offset_from_t0_sec))
        if parsed_infra.scheduled_infra_events:
            cand_raw_offsets.append(max(0.0, parsed_infra.scheduled_infra_events[0].raw_offset_from_t0_sec))
        if scheduled_updates:
            cand_raw_offsets.append(max(0.0, scheduled_updates[0].raw_offset_from_t0_sec))
        if scheduled_deletions:
            cand_raw_offsets.append(max(0.0, scheduled_deletions[0].raw_offset_from_t0_sec))
        initial_jump_raw_sec = min(cand_raw_offsets) if cand_raw_offsets else 0.0
        initial_jump_wall_sec = initial_jump_raw_sec / speedup

    effective_start_epoch = t0_epoch + initial_jump_raw_sec
    effective_start_iso = datetime.fromtimestamp(effective_start_epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    base_scheduled_workflows = [dc_replace(w) for w in scheduled_workflows]
    base_scheduled_deletions = [dc_replace(d) for d in scheduled_deletions]
    base_infra_events = list(parsed_infra.scheduled_infra_events)
    base_lqs_by_key = dict(parsed_infra.lqs_by_key)

    def _build_for_effective_start(eff_start_ep: float):
        # Every build starts from the same inputs. The build steps below append post-t0 non-Kueue Pod
        # events and synthesized LocalQueues to parsed_infra, so restore both before building again.
        parsed_infra.scheduled_infra_events[:] = base_infra_events
        parsed_infra.lqs_by_key.clear()
        parsed_infra.lqs_by_key.update(base_lqs_by_key)
        sched_wfs_in = [dc_replace(w) for w in base_scheduled_workflows]
        sched_dels_in = [dc_replace(d) for d in base_scheduled_deletions]
        init_wls, init_deps, init_pods, sched_wfs_out, expired_cnt = _build_initial_workloads_and_pods(
            parsed_infra,
            admitted_candidates,
            sched_wfs_in,
            sched_dels_in,
            earliest_admit_by_ns,
            exec_index,
            t0_epoch=t0_epoch,
            effective_start_epoch=eff_start_ep,
            initial_window_sec=initial_window_sec,
            speedup=speedup,
            min_pod_duration_sec=min_pod_duration_sec,
            infinite_pod_duration_sec=infinite_pod_duration_sec,
            default_exec_sec=default_exec_sec,
            pause_image_override=pause_image_override,
            override_workload_namespace=override_workload_namespace,
            infra_pods_namespace=infra_pods_namespace,
            interrupted_duration_multiplier=interrupted_duration_multiplier,
            interrupted_duration_override=parsed_override,
            manage_jobs_without_queue_name=manage_without_queue,
        )
        miss_warns, imp_cnt = _apply_missing_duration_policy(
            sched_wfs_out,
            exec_index,
            policy=missing_duration_policy,
            default_exec_sec=default_exec_sec,
            speedup=speedup,
            min_pod_duration_sec=min_pod_duration_sec,
        )
        _schedule_post_t0_non_kueue_pods(
            parsed_infra,
            sched_wfs_out,
            init_wls,
            init_deps,
            t0_epoch=t0_epoch,
            effective_start_epoch=eff_start_ep,
            initial_window_sec=initial_window_sec,
            speedup=speedup,
            min_pod_duration_sec=min_pod_duration_sec,
            infinite_pod_duration_sec=infinite_pod_duration_sec,
            pause_image_override=pause_image_override,
            infra_pods_namespace=infra_pods_namespace,
        )
        sched_wfs_out.sort(
            key=lambda item: (
                round(item.raw_offset_from_t0_sec, 6),
                item.creation_epoch,
                item.seq,
            )
        )
        return init_wls, init_deps, init_pods, sched_wfs_out, sched_dels_in, expired_cnt, miss_warns, imp_cnt

    (
        initial_workloads,
        initial_deployments,
        initial_pods,
        scheduled_workflows,
        scheduled_deletions,
        expired_initial_pods_count,
        missing_warnings,
        imputed_count,
    ) = _build_for_effective_start(effective_start_epoch)

    # Re-normalize scheduled workflow, update, deletion, and infra event offsets if jumping ahead.
    # If the effective start shifts (e.g., an unsuperseded early t0 PartialExecution occurs before the
    # first NotAdmitted arrival), rebuild initial workloads and pods at the converged effective_start_epoch
    # so t0 lifetimes and expired-pod filtering match the final timeline.
    if skip_initial_offset and (
        scheduled_workflows
        or parsed_infra.scheduled_infra_events
        or scheduled_updates
        or scheduled_deletions
    ):
        rebuild_count = 0
        while True:
            recalc_offsets: list[float] = []
            if scheduled_workflows:
                recalc_offsets.append(max(0.0, scheduled_workflows[0].raw_offset_from_t0_sec))
            if parsed_infra.scheduled_infra_events:
                recalc_offsets.append(max(0.0, parsed_infra.scheduled_infra_events[0].raw_offset_from_t0_sec))
            if scheduled_updates:
                recalc_offsets.append(max(0.0, scheduled_updates[0].raw_offset_from_t0_sec))
            if scheduled_deletions:
                recalc_offsets.append(max(0.0, scheduled_deletions[0].raw_offset_from_t0_sec))
            recalc_jump_raw_sec = min(recalc_offsets) if recalc_offsets else 0.0
            if abs(recalc_jump_raw_sec - initial_jump_raw_sec) <= 1e-6:
                break
            if rebuild_count >= 3:
                raise ValueError(
                    f"{resolved_dir}: initial time jump did not settle after {rebuild_count} rebuilds "
                    f"(last two candidates: +{initial_jump_raw_sec:.3f}s and +{recalc_jump_raw_sec:.3f}s); "
                    f"rerun with --no-skip-initial-offset"
                )
            rebuild_count += 1
            initial_jump_raw_sec = recalc_jump_raw_sec
            initial_jump_wall_sec = initial_jump_raw_sec / speedup
            effective_start_epoch = t0_epoch + initial_jump_raw_sec
            effective_start_iso = datetime.fromtimestamp(effective_start_epoch, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            (
                initial_workloads,
                initial_deployments,
                initial_pods,
                scheduled_workflows,
                scheduled_deletions,
                expired_initial_pods_count,
                missing_warnings,
                imputed_count,
            ) = _build_for_effective_start(effective_start_epoch)
        for item in scheduled_workflows:
            item.raw_offset_sec = max(0.0, item.raw_offset_from_t0_sec - initial_jump_raw_sec)
            item.target_wall_offset_sec = item.raw_offset_sec / speedup
        for ie in parsed_infra.scheduled_infra_events:
            ie.raw_offset_sec = max(0.0, ie.raw_offset_from_t0_sec - initial_jump_raw_sec)
            ie.target_wall_offset_sec = ie.raw_offset_sec / speedup
        for upd in scheduled_updates:
            upd.raw_offset_sec = max(0.0, upd.raw_offset_from_t0_sec - initial_jump_raw_sec)
            upd.target_wall_offset_sec = upd.raw_offset_sec / speedup
        for del_ev in scheduled_deletions:
            del_ev.raw_offset_sec = max(0.0, del_ev.raw_offset_from_t0_sec - initial_jump_raw_sec)
            del_ev.target_wall_offset_sec = del_ev.raw_offset_sec / speedup

    scheduled_workflows.sort(
        key=lambda item: (
            round(item.target_wall_offset_sec, 6),
            item.creation_epoch,
            item.raw_offset_from_t0_sec,
            item.seq,
        )
    )
    scheduled_updates.sort(key=lambda u: (u.target_wall_offset_sec, u.seq))
    scheduled_deletions.sort(key=lambda d: (d.target_wall_offset_sec, d.seq))

    prereq_and_queue_docs, workload_namespaces = _synthesize_referenced_objects(
        parsed_infra,
        initial_workloads,
        initial_deployments,
        initial_pods,
        scheduled_workflows,
        scheduled_updates,
        infra_pods_namespace,
    )

    primary_ns = (
        override_workload_namespace
        or (scheduled_workflows[0].namespace if scheduled_workflows else None)
        or (initial_workloads[0].namespace if initial_workloads else None)
        or (initial_deployments[0].namespace if initial_deployments else None)
        or workload_namespaces[0]
    )

    detected_strategy = override_queueing_strategy or "StrictFIFO"
    for cq_doc in parsed_infra.cqs_by_name.values():
        st_val = (cq_doc.get("spec") or {}).get("queueingStrategy")
        if st_val:
            detected_strategy = str(st_val)
            break

    all_cq_names = sorted(set(parsed_infra.cqs_by_name.keys()) | set(parsed_infra.post_t0_cq_names))

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
        nodes=list(parsed_infra.nodes_by_name.values()),
        namespaces=list(parsed_infra.namespaces_by_name.values()),
        prereq_and_queue_docs=prereq_and_queue_docs,
        initial_pods=initial_pods,
        scheduled_workflows=scheduled_workflows,
        skipped_already_admitted_workflows=skipped_admitted,
        skipped_child_workflows=skipped_child_count,
        workload_namespaces=workload_namespaces,
        primary_workload_namespace=primary_ns,
        infra_pods_namespace=infra_pods_namespace,
        cluster_queue_names=all_cq_names,
        queueing_strategy=detected_strategy,
        initial_workloads=initial_workloads,
        initial_deployments=initial_deployments,
        scheduled_infra_events=parsed_infra.scheduled_infra_events,
        scheduled_workflow_updates=scheduled_updates,
        scheduled_workflow_deletions=scheduled_deletions,
        interrupted_duration_multiplier=interrupted_duration_multiplier,
        interrupted_duration_override=parsed_override,
        kueue_configuration=kueue_cfg,
        missing_duration_warnings=missing_warnings,
        imputed_duration_count=imputed_count,
    )
