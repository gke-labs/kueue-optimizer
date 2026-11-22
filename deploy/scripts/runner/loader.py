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

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
from pathlib import Path
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
    build_cohort_manifest,
    build_local_queue_manifest,
    build_namespace_manifest,
    build_priority_class_manifest,
)
from chamber.parsers import (
    format_pod_execution_seconds,
    multiply_k8s_quantity_exact,
    parse_k8s_quantity,
    sanitize_k8s_name,
)
from chamber.scenario import (
    ExecutionTimeIndex,
    WorkloadExecutionInfo,
    build_topology_assignment,
    collect_known_parent_workflows,
    collect_not_admitted_workflow_admit_epochs,
    extract_pod_spec_resources,
    filter_initial_running_pods,
    index_execution_times,
    is_child_workflow_owned_by_tracked_parent,
)
from chamber.yaml_utils import load_jsonl_events
from runner.validation import (
    ALLOWED_INFRA_GVKS,
    MAX_K8S_NAME_LEN,
    SYSTEM_NAMESPACES,
    _extract_existing_pod_exec_seconds,
    _inject_pod_execution_time_and_sanitize_workflow,
    _parse_optional_event_epoch,
    _prepare_kueue_or_ns_doc,
    _prepare_node_manifest,
    _require_event_epoch,
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
    trigger_reason: str = field(compare=False)  # "initial_not_admitted" | "preempted_after_initial_admission"
    execution_duration_sec: float = field(compare=False)
    compressed_duration_sec: float = field(compare=False)
    indexed_from_execution_time: bool = field(compare=False)
    pod_count: int = field(compare=False)
    manifest: dict[str, Any] = field(compare=False)


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

    @property
    def initial_workload_pods_count(self) -> int:
        return sum(len(w.pods) for w in self.initial_workloads)


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

    t0_epoch = min(all_first_epochs) if all_first_epochs else time.time()
    t_end_epoch = max(all_last_epochs) if all_last_epochs else t0_epoch
    if t_end_epoch < t0_epoch:
        t_end_epoch = t0_epoch
    t0_iso = datetime.fromtimestamp(t0_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    t_end_iso = datetime.fromtimestamp(t_end_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return t0_epoch, t0_iso, t_end_epoch, t_end_iso


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
) -> tuple[
    list[ScheduledWorkflowItem],
    list[_AdmittedInitialCandidate],
    list[tuple[str, str, str]],
    int,
]:
    """Track workflow lifecycles chronologically and collect NotAdmitted and t0-Admitted workflows."""
    first_seen_state: dict[tuple[str, str, str], str] = {}
    first_seen_orig_ns: dict[tuple[str, str, str], str] = {}
    scheduled_by_key: dict[tuple[str, str, str], ScheduledWorkflowItem] = {}
    latest_obj_by_key: dict[tuple[str, str, str], tuple[dict[str, Any], str]] = {}
    podgroup_members_by_key: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = {}
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

        is_child, parent_js = is_child_workflow_owned_by_tracked_parent(raw_obj, kind, ns, known_parents)
        if is_child:
            if ev_type == "New":
                skipped_child_count += 1
            continue

        # Infer workloadState if reading an older trace without workloadState field
        if not wl_state:
            suspend_val = (raw_obj.get("spec") or {}).get("suspend")
            wl_state = "NotAdmitted" if suspend_val is True else "Admitted"

        pg_name = (
            str(((raw_meta.get("labels") or {}).get("kueue.x-k8s.io/pod-group-name")) or "").strip()
            if kind == "Pod"
            else ""
        )
        if pg_name:
            pg_key = (ns, "PodGroup", pg_name)
            if (pg_key in first_seen_state and first_seen_state[pg_key] != "NotAdmitted") or (
                pg_key not in first_seen_state and wl_state != "NotAdmitted"
            ):
                prev_pg_lq = latest_obj_by_key.get(pg_key, ({}, ""))[1]
                latest_obj_by_key[pg_key] = (raw_obj, lq_name or prev_pg_lq)
                podgroup_members_by_key.setdefault(pg_key, {})[name] = raw_obj
                if pg_key not in first_seen_state:
                    first_seen_state[pg_key] = wl_state
                    first_seen_orig_ns[pg_key] = orig_ns
                continue

        key = (ns, kind, name)
        prev_lq = latest_obj_by_key.get(key, ({}, ""))[1]
        latest_obj_by_key[key] = (raw_obj, lq_name or prev_lq)

        if key not in first_seen_state:
            first_seen_state[key] = wl_state
            first_seen_orig_ns[key] = orig_ns

        if key in scheduled_by_key:
            continue

        if wl_state == "NotAdmitted":
            trigger_reason = (
                "initial_not_admitted"
                if first_seen_state[key] == "NotAdmitted"
                else "preempted_after_initial_admission"
            )
            fallback_dur = _extract_existing_pod_exec_seconds(raw_obj) or default_exec_sec
            found_in_idx = False
            raw_dur = fallback_dur
            if pg_name:
                raw_dur, found_in_idx = exec_index.resolve_duration_sec(
                    namespace=orig_ns,
                    object_kind="PodGroup",
                    name=pg_name,
                    fallback_sec=fallback_dur,
                )
            if not found_in_idx:
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
                local_queue_name=lq_name or prev_lq,
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
        if kind == "PodGroup":
            continue
        orig_ns = first_seen_orig_ns.get(key, ns)
        info = exec_index.lookup(orig_ns, kind, name)
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
            preempt_iso = datetime.fromtimestamp(preempt_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
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

    admitted_candidates: list[_AdmittedInitialCandidate] = []
    for key, initial_st in first_seen_state.items():
        if initial_st == "NotAdmitted":
            continue
        ns, kind, name = key
        orig_ns = first_seen_orig_ns.get(key, ns)
        raw_obj, lq_name = latest_obj_by_key[key]
        info = exec_index.lookup(orig_ns, kind, name)
        admitted_candidates.append(
            _AdmittedInitialCandidate(
                namespace=ns,
                orig_namespace=orig_ns,
                kind=kind,
                name=name,
                local_queue_name=lq_name,
                raw_obj=raw_obj,
                exec_info=info,
                preempt_item=scheduled_by_key.get(key),
                from_workflows_json=True,
                member_objs=list((podgroup_members_by_key.get(key) or {}).values()),
            )
        )

    skipped_admitted = [key for key in first_seen_state if key not in scheduled_by_key]
    scheduled_workflows = sorted(
        scheduled_by_key.values(),
        key=lambda item: (item.target_wall_offset_sec, item.seq),
    )
    return scheduled_workflows, admitted_candidates, skipped_admitted, skipped_child_count


def _parse_infra_resources(
    infra_events: list[dict[str, Any]],
    infra_event_epochs: list[float],
    inf_file: Path,
    *,
    override_workload_namespace: str | None,
    override_queueing_strategy: str | None,
    override_cq_fair_sharing_preemption: bool,
) -> _ParsedInfra:
    """Validate and parse infra.json into Nodes, Kueue objects, Namespaces, and raw Pod events."""
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

    return _ParsedInfra(
        nodes_by_name=nodes_by_name,
        namespaces_by_name=namespaces_by_name,
        runtime_classes_by_name=runtime_classes_by_name,
        topologies_by_name=topologies_by_name,
        flavors_by_name=flavors_by_name,
        cohorts_by_name=cohorts_by_name,
        priority_classes_by_name=priority_classes_by_name,
        cqs_by_name=cqs_by_name,
        lqs_by_key=lqs_by_key,
        pod_new_events=pod_new_events,
        pod_delete_epochs=pod_delete_epochs,
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
        if len((p_spec or {}).get("containers") or []) > 1 or (p_spec or {}).get("initContainers") or (p_spec or {}).get("overhead"):
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
        replicas = max(1, int(spec.get("replicas") or 1))
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
    if lq_name and (ns, lq_name) not in parsed_infra.lqs_by_key and cq_name:
        parsed_infra.lqs_by_key[(ns, lq_name)] = build_local_queue_manifest(
            name=lq_name,
            namespace=ns,
            cluster_queue=cq_name,
        )

    return lq_name, cq_name


def _resolve_podset_assignment_for_group(
    *,
    role_hash: str,
    count: int,
    reqs: dict[str, str],
    placements: list[str],
    cq_doc: dict[str, Any],
    parsed_infra: _ParsedInfra,
    scraped_psas: list[dict[str, Any]],
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

    scraped_flavors: dict[str, str] = {}
    for s_psa in scraped_psas:
        for rk, fn in (s_psa.get("flavors") or {}).items():
            if str(fn) in parsed_infra.flavors_by_name:
                scraped_flavors[str(rk)] = str(fn)

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
    active_candidates: list[_AdmittedInitialCandidate] = []
    known_wl_keys: set[tuple[str, str]] = set()
    known_owner_keys: set[tuple[str, str]] = set()

    for cand in admitted_candidates:
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
            lq_chk, cq_chk = _resolve_cq_and_lq_for_candidate(cand, parsed_infra)
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

    # Materialize Class 1 `InitialWorkloadPodGroup`s and collect any preempted items superseded by dynamic resubmit.
    initial_workloads: list[InitialWorkloadPodGroup] = []
    superseded_preempt_seqs: set[int] = set()
    used_wl_names_by_ns: dict[str, set[str]] = {}
    used_pod_names_by_ns: dict[str, set[str]] = {}
    admit_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for c_idx, cand in enumerate(active_candidates):
        lq_name, cq_name = _resolve_cq_and_lq_for_candidate(cand, parsed_infra)
        cand_pods = list(matched_pods_by_cand.get(c_idx) or [])

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
                    pg_tot_str = str(
                        (((cand.member_objs[0].get("metadata") or {}).get("labels") or {}).get(
                            "kueue.x-k8s.io/pod-group-total-count"
                        ))
                        or ""
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
            if cand.exec_info:
                if cand.exec_info.full_end_epoch is not None and cand.exec_info.full_end_epoch > effective_start_epoch:
                    raw_dur_cand = cand.exec_info.full_end_epoch - effective_start_epoch
                    has_finite_end = True
                elif cand.exec_info.full_duration_sec is not None and cand.exec_info.full_duration_sec > 0:
                    adm_ep = min(cand.exec_info.admitted_epochs) if cand.exec_info.admitted_epochs else t0_epoch
                    rem = (adm_ep + cand.exec_info.full_duration_sec) - effective_start_epoch
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
        if cand.from_workflows_json and cand.raw_obj and cand.kind != "PodGroup":
            fallback_dur = _extract_existing_pod_exec_seconds(cand.raw_obj) or default_exec_sec
            raw_dur_full, found_idx = exec_index.resolve_duration_sec(
                namespace=cand.orig_namespace,
                object_kind=cand.kind,
                name=cand.name,
                fallback_sec=fallback_dur,
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
                )
                if psa_uses_tas:
                    any_tas_in_wl = True
                pod_set_assignments.append(psa)

                ps_member_specs: list[InitialPodSpec] = []
                for p_ns, p_name, _ev_ep, pl, del_ep in pts:
                    # Fall back to execution-time.json full_end_epoch when individual pod lacked deletionTime
                    eff_del_ep = del_ep
                    if (
                        eff_del_ep is None
                        and cand.exec_info
                        and cand.exec_info.full_end_epoch is not None
                        and cand.exec_info.full_end_epoch > effective_start_epoch
                    ):
                        eff_del_ep = cand.exec_info.full_end_epoch

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
                ],
            }

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
                )
            )

        if cand.preempt_item is not None:
            superseded_preempt_seqs.add(cand.preempt_item.seq)

    # Remove any static historical preemption resubmissions now handled dynamically on Evicted=True.
    if superseded_preempt_seqs:
        scheduled_workflows = [w for w in scheduled_workflows if w.seq not in superseded_preempt_seqs]

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

    return initial_workloads, initial_infra_pods, scheduled_workflows, expired_initial_pods_count


def _synthesize_referenced_objects(
    parsed_infra: _ParsedInfra,
    initial_workloads: list[InitialWorkloadPodGroup],
    initial_pods: list[InitialPodSpec],
    scheduled_workflows: list[ScheduledWorkflowItem],
    infra_pods_namespace: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Synthesize missing Namespaces, Cohorts, WorkloadPriorityClasses, and RuntimeClasses referenced by the scenario."""
    referenced_namespaces: set[str] = set(parsed_infra.namespaces_by_name.keys())
    referenced_namespaces.add(infra_pods_namespace)
    for lq_ns, _ in parsed_infra.lqs_by_key:
        referenced_namespaces.add(lq_ns)
    for iwl in initial_workloads:
        referenced_namespaces.add(iwl.namespace)
    for ipod in initial_pods:
        referenced_namespaces.add(ipod.namespace)
    for sw in scheduled_workflows:
        referenced_namespaces.add(sw.namespace)

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

    for idx_iwl, iwl in enumerate(initial_workloads):
        if iwl.priority_class_name and iwl.priority_class_name not in parsed_infra.priority_classes_by_name:
            prio_val = int(iwl.priority if iwl.priority is not None else (1000 - idx_iwl))
            parsed_infra.priority_classes_by_name[iwl.priority_class_name] = build_priority_class_manifest(
                iwl.priority_class_name, value=prio_val
            )

    all_wf_items: list[ScheduledWorkflowItem] = list(scheduled_workflows) + [
        iwl.resubmit_item for iwl in initial_workloads if iwl.resubmit_item is not None
    ]
    for idx_pc, sw in enumerate(all_wf_items):
        w_meta = sw.manifest.get("metadata") or {}
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
        # Synthesize RuntimeClass if any pod template specifies runtimeClassName / pod.spec.overhead
        w_spec = sw.manifest.get("spec") or {}
        candidate_pod_specs: list[dict[str, Any]] = []
        if sw.object_kind == "JobSet":
            for rj in w_spec.get("replicatedJobs") or []:
                candidate_pod_specs.append(
                    (((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get("spec") or {}
                )
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

    prereq_and_queue_docs: list[dict[str, Any]] = []
    prereq_and_queue_docs.extend(parsed_infra.namespaces_by_name.values())
    prereq_and_queue_docs.extend(parsed_infra.runtime_classes_by_name.values())
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

    validate_workflow_events(workflow_events, wf_file)

    t0_epoch, t0_iso, t_end_epoch, t_end_iso = _compute_timeline_bounds(
        infra_event_epochs, workflow_event_epochs, exec_index
    )

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

    scheduled_workflows, admitted_candidates, skipped_admitted, skipped_child_count = _schedule_workflows(
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
    )

    parsed_infra = _parse_infra_resources(
        infra_events,
        infra_event_epochs,
        inf_file,
        override_workload_namespace=override_workload_namespace,
        override_queueing_strategy=override_queueing_strategy,
        override_cq_fair_sharing_preemption=override_cq_fair_sharing_preemption,
    )

    # Compute jump target from genuine NotAdmitted arrivals first (falling back to scheduled_workflows
    # when no initial_not_admitted arrivals exist) so dynamic t0 preemption items don't distort t_start.
    initial_jump_raw_sec = 0.0
    initial_jump_wall_sec = 0.0
    if skip_initial_offset and scheduled_workflows:
        not_adm_items = [w for w in scheduled_workflows if w.trigger_reason == "initial_not_admitted"]
        anchor = not_adm_items[0] if not_adm_items else scheduled_workflows[0]
        initial_jump_raw_sec = max(0.0, anchor.raw_offset_from_t0_sec)
        initial_jump_wall_sec = max(0.0, anchor.target_wall_offset_sec)

    effective_start_epoch = t0_epoch + initial_jump_raw_sec
    effective_start_iso = datetime.fromtimestamp(effective_start_epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    initial_workloads, initial_pods, scheduled_workflows, expired_initial_pods_count = (
        _build_initial_workloads_and_pods(
            parsed_infra,
            admitted_candidates,
            scheduled_workflows,
            earliest_admit_by_ns,
            exec_index,
            t0_epoch=t0_epoch,
            effective_start_epoch=effective_start_epoch,
            initial_window_sec=initial_window_sec,
            speedup=speedup,
            min_pod_duration_sec=min_pod_duration_sec,
            infinite_pod_duration_sec=infinite_pod_duration_sec,
            default_exec_sec=default_exec_sec,
            pause_image_override=pause_image_override,
            override_workload_namespace=override_workload_namespace,
            infra_pods_namespace=infra_pods_namespace,
        )
    )

    # Re-normalize scheduled workflow offsets if jumping ahead (or if superseded preemption items were removed).
    if skip_initial_offset and scheduled_workflows:
        initial_jump_raw_sec = max(0.0, scheduled_workflows[0].raw_offset_from_t0_sec)
        initial_jump_wall_sec = initial_jump_raw_sec / speedup
        effective_start_epoch = t0_epoch + initial_jump_raw_sec
        effective_start_iso = datetime.fromtimestamp(effective_start_epoch, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        for item in scheduled_workflows:
            item.raw_offset_sec = max(0.0, item.raw_offset_from_t0_sec - initial_jump_raw_sec)
            item.target_wall_offset_sec = item.raw_offset_sec / speedup

    prereq_and_queue_docs, workload_namespaces = _synthesize_referenced_objects(
        parsed_infra,
        initial_workloads,
        initial_pods,
        scheduled_workflows,
        infra_pods_namespace,
    )

    primary_ns = (
        override_workload_namespace
        or (scheduled_workflows[0].namespace if scheduled_workflows else None)
        or (initial_workloads[0].namespace if initial_workloads else None)
        or workload_namespaces[0]
    )

    detected_strategy = override_queueing_strategy or "StrictFIFO"
    for cq_doc in parsed_infra.cqs_by_name.values():
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
        cluster_queue_names=sorted(parsed_infra.cqs_by_name.keys()),
        queueing_strategy=detected_strategy,
        initial_workloads=initial_workloads,
    )
