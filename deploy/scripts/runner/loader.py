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
    build_namespace_manifest,
    build_priority_class_manifest,
)
from chamber.parsers import (
    format_pod_execution_seconds,
    sanitize_k8s_name,
)
from chamber.scenario import (
    ExecutionTimeIndex,
    collect_known_parent_workflows,
    collect_not_admitted_workflow_admit_epochs,
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
) -> tuple[list[ScheduledWorkflowItem], list[tuple[str, str, str]], int]:
    """Track workflow lifecycles chronologically and schedule NotAdmitted/preempted top-level workflows."""
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

        is_child, parent_js = is_child_workflow_owned_by_tracked_parent(raw_obj, kind, ns, known_parents)
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

    skipped_admitted = [key for key in first_seen_state if key not in scheduled_by_key]
    scheduled_workflows = sorted(
        scheduled_by_key.values(),
        key=lambda item: (item.target_wall_offset_sec, item.seq),
    )
    return scheduled_workflows, skipped_admitted, skipped_child_count


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


def _build_initial_pod_specs(
    parsed_infra: _ParsedInfra,
    scheduled_workflows: list[ScheduledWorkflowItem],
    earliest_admit_by_ns: dict[str, float],
    *,
    t0_epoch: float,
    effective_start_epoch: float,
    initial_window_sec: float,
    speedup: float,
    min_pod_duration_sec: float,
    infinite_pod_duration_sec: float,
    pause_image_override: str | None,
    infra_pods_namespace: str,
) -> tuple[list[InitialPodSpec], int]:
    """Filter qualifying t0 running pods and build their Kueue-excluded KWOK Pod manifests."""
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

    return initial_pods, expired_initial_pods_count


def _synthesize_referenced_objects(
    parsed_infra: _ParsedInfra,
    initial_pods: list[InitialPodSpec],
    scheduled_workflows: list[ScheduledWorkflowItem],
    infra_pods_namespace: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Synthesize missing Namespaces, Cohorts, WorkloadPriorityClasses, and RuntimeClasses referenced by the scenario."""
    referenced_namespaces: set[str] = set(parsed_infra.namespaces_by_name.keys())
    referenced_namespaces.add(infra_pods_namespace)
    for lq_ns, _ in parsed_infra.lqs_by_key:
        referenced_namespaces.add(lq_ns)
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

    for idx_pc, sw in enumerate(scheduled_workflows):
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

    scheduled_workflows, skipped_admitted, skipped_child_count = _schedule_workflows(
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

    parsed_infra = _parse_infra_resources(
        infra_events,
        infra_event_epochs,
        inf_file,
        override_workload_namespace=override_workload_namespace,
        override_queueing_strategy=override_queueing_strategy,
        override_cq_fair_sharing_preemption=override_cq_fair_sharing_preemption,
    )

    initial_pods, expired_initial_pods_count = _build_initial_pod_specs(
        parsed_infra,
        scheduled_workflows,
        earliest_admit_by_ns,
        t0_epoch=t0_epoch,
        effective_start_epoch=effective_start_epoch,
        initial_window_sec=initial_window_sec,
        speedup=speedup,
        min_pod_duration_sec=min_pod_duration_sec,
        infinite_pod_duration_sec=infinite_pod_duration_sec,
        pause_image_override=pause_image_override,
        infra_pods_namespace=infra_pods_namespace,
    )

    prereq_and_queue_docs, workload_namespaces = _synthesize_referenced_objects(
        parsed_infra,
        initial_pods,
        scheduled_workflows,
        infra_pods_namespace,
    )

    primary_ns = (
        override_workload_namespace
        or (scheduled_workflows[0].namespace if scheduled_workflows else workload_namespaces[0])
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
    )
