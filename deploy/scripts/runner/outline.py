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

"""Terminal scenario outline printer for `testrunner`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from chamber.parsers import calc_summary_stats, format_duration_hms, parse_k8s_quantity
from chamber.scenario import extract_workflow_pods_and_requests
from runner.loader import InitialPodSpec, ScenarioBundle, ScheduledWorkflowItem


_IGNORE_NODE_LABEL_KEYS: frozenset[str] = frozenset(
    {
        "beta.kubernetes.io/arch",
        "beta.kubernetes.io/os",
        "kubernetes.io/arch",
        "kubernetes.io/hostname",
        "kubernetes.io/os",
        "kubernetes.io/role",
        "node-role.kubernetes.io/agent",
        "kueue-optimizer.gke.io/node-role",
        "kueue-optimizer.gke.io/trace-node",
        "cloud.google.com/gke-nodepool",
        "cloud.google.com/gce-topology-block",
        "cloud.google.com/gce-topology-subblock",
        "cloud.google.com/gke-tpu-partition-4x4x4-id",
        "cloud.google.com/gke-tpu-partition-2x4x4-id",
        "cloud.google.com/gke-tpu-partition-2x2x4-id",
        "cloud.google.com/gke-tpu-partition-2x2x2-id",
        "cloud.google.com/gke-tpu-partition-2x2x1-id",
    }
)


@dataclass
class _ClusterNodeSummary:
    node_groups: dict[tuple[str, str, str, str, str, str], dict[str, Any]]
    total_cpu: float
    total_mem_gi: float
    total_pods: int
    total_accels: dict[str, int]


@dataclass
class _InitialUsageSummary:
    cpu_used: float
    mem_used_gi: float
    accels_used: dict[str, int]
    pods_by_node: dict[str, list[InitialPodSpec]]
    finite_durs: list[float]
    infinite_count: int


@dataclass
class _QueueLoadSummary:
    lqs_for_cq: dict[str, list[str]]
    wf_stats_by_cq: dict[str, dict[str, float | int]]
    wf_counts_by_kind: dict[str, int]
    wf_counts_by_reason: dict[str, int]
    total_wf_pods: int
    total_wf_cpu: float
    total_wf_mem_gi: float


def _summarize_node_groups(nodes: list[dict[str, Any]]) -> _ClusterNodeSummary:
    """Group cluster nodes by role, accelerator/machine type, capacity, and selectors."""
    node_groups: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    total_cluster_cpu = 0.0
    total_cluster_mem_gi = 0.0
    total_cluster_pods = 0
    total_cluster_accels: dict[str, int] = {}

    for nd in nodes:
        meta = nd.get("metadata") or {}
        labels = meta.get("labels") or {}
        alloc = (nd.get("status") or {}).get("allocatable") or {}
        role = str(labels.get("kueue-optimizer.gke.io/node-role") or labels.get("kubernetes.io/role") or "agent")
        node_type = str(
            labels.get("cloud.google.com/gke-tpu-accelerator")
            or labels.get("cloud.google.com/gke-accelerator")
            or labels.get("cloud.google.com/machine-family")
            or labels.get("type")
            or f"{role}-node"
        )
        cpu_str = str(alloc.get("cpu", "0"))
        mem_str = str(alloc.get("memory", "0"))
        pods_str = str(alloc.get("pods", "110"))
        cpu_val = parse_k8s_quantity(cpu_str, "cpu")
        mem_val = parse_k8s_quantity(mem_str, "memory")
        pods_val = int(round(parse_k8s_quantity(pods_str, "pods")))

        total_cluster_cpu += cpu_val
        total_cluster_mem_gi += mem_val
        total_cluster_pods += pods_val

        accel_parts: list[str] = []
        for rk, rv in sorted(alloc.items()):
            if rk not in ("cpu", "memory", "pods", "ephemeral-storage") and not rk.startswith("hugepages-"):
                accel_parts.append(f"{rk}={rv}")
                total_cluster_accels[rk] = total_cluster_accels.get(rk, 0) + int(round(parse_k8s_quantity(rv, rk)))
        accel_str = ", ".join(accel_parts) if accel_parts else "-"

        sel_parts = [f"{k}={v}" for k, v in sorted(labels.items()) if k not in _IGNORE_NODE_LABEL_KEYS]
        sel_str = ", ".join(sel_parts) if sel_parts else "type=kwok"

        grp_key = (role, node_type, cpu_str, mem_str, accel_str, sel_str)
        grp = node_groups.get(grp_key)
        if grp is None:
            grp = {
                "role": role,
                "node_type": node_type,
                "count": 0,
                "cpu_val": cpu_val,
                "mem_val": mem_val,
                "pods_val": pods_val,
                "cpu_str": cpu_str,
                "mem_str": mem_str,
                "pods_str": pods_str,
                "accel_str": accel_str,
                "selectors": sel_str,
                "node_names": [],
            }
            node_groups[grp_key] = grp
        grp["count"] += 1
        grp["node_names"].append(str(meta.get("name") or ""))

    return _ClusterNodeSummary(
        node_groups=node_groups,
        total_cpu=total_cluster_cpu,
        total_mem_gi=total_cluster_mem_gi,
        total_pods=total_cluster_pods,
        total_accels=total_cluster_accels,
    )


def _summarize_initial_pod_usage(initial_pods: list[InitialPodSpec]) -> _InitialUsageSummary:
    """Aggregate CPU, memory, accelerators, placement, and lifetimes of initial running pods."""
    init_cpu_used = 0.0
    init_mem_used_gi = 0.0
    init_accels_used: dict[str, int] = {}
    init_pods_by_node: dict[str, list[InitialPodSpec]] = {}
    finite_init_durs: list[float] = []
    infinite_init_count = 0

    for ipod in initial_pods:
        init_cpu_used += parse_k8s_quantity(ipod.resource_request.get("cpu"), "cpu")
        init_mem_used_gi += parse_k8s_quantity(ipod.resource_request.get("memory"), "memory")
        for rk, rv in ipod.resource_request.items():
            if rk not in ("cpu", "memory", "pods", "ephemeral-storage") and not rk.startswith("hugepages-"):
                init_accels_used[rk] = init_accels_used.get(rk, 0) + int(round(parse_k8s_quantity(rv, rk)))
        init_pods_by_node.setdefault(ipod.placement or "<unassigned>", []).append(ipod)
        if ipod.is_infinite:
            infinite_init_count += 1
        else:
            finite_init_durs.append(ipod.remaining_duration_sec)

    return _InitialUsageSummary(
        cpu_used=init_cpu_used,
        mem_used_gi=init_mem_used_gi,
        accels_used=init_accels_used,
        pods_by_node=init_pods_by_node,
        finite_durs=finite_init_durs,
        infinite_count=infinite_init_count,
    )


def _summarize_queue_load(
    lq_docs: list[dict[str, Any]],
    scheduled_workflows: list[ScheduledWorkflowItem],
) -> _QueueLoadSummary:
    """Map LocalQueues to ClusterQueues and aggregate scheduled workflow counts and resource demands."""
    lq_to_cq: dict[tuple[str, str], str] = {}
    lqs_for_cq: dict[str, list[str]] = {}
    for lq in lq_docs:
        lq_meta = lq.get("metadata") or {}
        lq_ns = str(lq_meta.get("namespace") or "default")
        lq_name = str(lq_meta.get("name") or "")
        cq_target = str((lq.get("spec") or {}).get("clusterQueue") or "")
        lq_to_cq[(lq_ns, lq_name)] = cq_target
        lqs_for_cq.setdefault(cq_target, []).append(f"{lq_ns}/{lq_name}")

    wf_stats_by_cq: dict[str, dict[str, float | int]] = {}
    wf_counts_by_kind: dict[str, int] = {}
    wf_counts_by_reason: dict[str, int] = {}
    total_wf_pods = 0
    total_wf_cpu = 0.0
    total_wf_mem_gi = 0.0

    for sw in scheduled_workflows:
        wf_counts_by_kind[sw.object_kind] = wf_counts_by_kind.get(sw.object_kind, 0) + 1
        wf_counts_by_reason[sw.trigger_reason] = wf_counts_by_reason.get(sw.trigger_reason, 0) + 1
        _, wf_tot_reqs, _ = extract_workflow_pods_and_requests(sw.manifest, sw.object_kind)
        w_cpu = wf_tot_reqs.get("cpu", 0.0)
        w_mem = wf_tot_reqs.get("memory", 0.0)
        total_wf_pods += sw.pod_count
        total_wf_cpu += w_cpu
        total_wf_mem_gi += w_mem

        cq_name = lq_to_cq.get((sw.namespace, sw.local_queue_name), sw.local_queue_name or "<unmapped>")
        st = wf_stats_by_cq.setdefault(cq_name, {"workflows": 0, "pods": 0, "cpu": 0.0, "mem_gi": 0.0})
        st["workflows"] = int(st["workflows"]) + 1
        st["pods"] = int(st["pods"]) + sw.pod_count
        st["cpu"] = float(st["cpu"]) + w_cpu
        st["mem_gi"] = float(st["mem_gi"]) + w_mem

    return _QueueLoadSummary(
        lqs_for_cq=lqs_for_cq,
        wf_stats_by_cq=wf_stats_by_cq,
        wf_counts_by_kind=wf_counts_by_kind,
        wf_counts_by_reason=wf_counts_by_reason,
        total_wf_pods=total_wf_pods,
        total_wf_cpu=total_wf_cpu,
        total_wf_mem_gi=total_wf_mem_gi,
    )


def _print_timeline_and_index(
    bundle: ScenarioBundle,
    speedup: float,
    enable_fair_sharing: bool | None,
    reused_chamber: bool,
) -> None:
    idx = bundle.exec_index
    durations = [
        info.resolved_duration_sec()
        for info in idx.by_workload_name.values()
        if (info.full_duration_sec is not None or info.partial_durations_sec)
    ]
    dur_stats = calc_summary_stats(durations)
    min_d = dur_stats["min"]
    max_d = dur_stats["max"]
    avg_d = dur_stats["avg"]
    p50_d = dur_stats["p50"]
    p90_d = dur_stats["p90"]

    raw_scenario_span = max(0.0, bundle.t_end_epoch - bundle.t0_epoch)
    comp_scenario_span = raw_scenario_span / speedup

    first_raw_off = bundle.scheduled_workflows[0].raw_offset_sec if bundle.scheduled_workflows else 0.0
    last_raw_off = bundle.scheduled_workflows[-1].raw_offset_sec if bundle.scheduled_workflows else 0.0
    first_wall_off = bundle.scheduled_workflows[0].target_wall_offset_sec if bundle.scheduled_workflows else 0.0
    last_wall_off = bundle.scheduled_workflows[-1].target_wall_offset_sec if bundle.scheduled_workflows else 0.0
    arrival_window_raw = max(0.0, last_raw_off - first_raw_off)
    arrival_window_wall = max(0.0, last_wall_off - first_wall_off)

    est_min_finish_raw = max(
        (w.raw_offset_sec + w.execution_duration_sec for w in bundle.scheduled_workflows),
        default=0.0,
    )
    raw_exec_span = (
        max(0.0, idx.last_event_epoch - bundle.effective_start_epoch)
        if idx.last_event_epoch is not None
        else est_min_finish_raw
    )
    comp_exec_span = raw_exec_span / speedup

    print("")
    print("========================================================================================")
    print("  KUEUE-OPTIMIZER TESTRUNNER — SCENARIO & INFRASTRUCTURE DESCRIPTION")
    print("========================================================================================")
    print(f"  Scenario Directory        : {bundle.scenario_dir}")
    print(
        f"  Input Trace Files         : infra.json ({bundle.infra_event_count} events), "
        f"workflows.json ({bundle.workflow_event_count} events), "
        f"execution-time.json ({idx.total_events} events)"
    )
    print(f"  Time Compression (--speedup): {speedup:g}x")
    if enable_fair_sharing is None:
        fs_mode_desc = "inherited from running chamber (--skip-chamber-deploy)"
    else:
        base_fs_desc = (
            "enabled (LessThanOrEqualToFinalShare, LessThanInitialShare)"
            if enable_fair_sharing
            else "disabled (Classical Preemption)"
        )
        fs_mode_desc = f"{base_fs_desc} [detected from live chamber]" if reused_chamber else base_fs_desc
    print(f"  Kueue FairSharing Config  : {fs_mode_desc}")
    print("")
    print("  [1] TEST CASE LENGTH & TIMELINE")
    print(f"      Scenario Start (t0)   : {bundle.t0_iso}")
    if bundle.initial_jump_raw_sec > 0:
        print(
            f"      First Workload Start  : {bundle.effective_start_iso} "
            f"(immediately jumping ahead +{bundle.initial_jump_raw_sec:.2f}s raw / "
            f"+{bundle.initial_jump_wall_sec:.2f}s wall)"
        )
    print(f"      Scenario End (t_end)  : {bundle.t_end_iso}")
    print(
        f"      Total Scraped Span    : {raw_scenario_span:.2f}s ({format_duration_hms(raw_scenario_span)}) "
        f"-> {comp_scenario_span:.2f}s ({format_duration_hms(comp_scenario_span)}) at {speedup:g}x"
    )
    print(
        f"      Workflow Arrival Span : +{first_raw_off:.2f}s .. +{last_raw_off:.2f}s from test start "
        f"(window={arrival_window_raw:.2f}s raw / {arrival_window_wall:.2f}s wall)"
    )
    print(
        f"      Active Execution Span : {raw_exec_span:.2f}s ({format_duration_hms(raw_exec_span)}) "
        f"-> {comp_exec_span:.2f}s ({format_duration_hms(comp_exec_span)}) at {speedup:g}x "
        f"(test start to last workload completion)"
    )
    print("")
    print("  [2] WORKLOAD EXECUTION TIME INDEX (execution-time.json, indexed before cluster setup)")
    print(
        f"      Indexed Workloads     : {len(idx.by_workload_name)} workloads "
        f"(Admitted={idx.admitted_events}, PartialExecution={idx.partial_events}, FullExecution={idx.full_events})"
    )
    print(
        f"      Execution Durations   : min={min_d:.1f}s, p50={p50_d:.1f}s, avg={avg_d:.1f}s, "
        f"p90={p90_d:.1f}s, max={max_d:.1f}s "
        f"(at {speedup:g}x: min={min_d / speedup:.2f}s .. max={max_d / speedup:.2f}s)"
    )


def _print_nodes_and_initial_usage(
    bundle: ScenarioBundle,
    node_summary: _ClusterNodeSummary,
    init_usage: _InitialUsageSummary,
) -> None:
    cpu_util_pct = (100.0 * init_usage.cpu_used / node_summary.total_cpu) if node_summary.total_cpu > 0 else 0.0
    mem_util_pct = (
        (100.0 * init_usage.mem_used_gi / node_summary.total_mem_gi) if node_summary.total_mem_gi > 0 else 0.0
    )
    pod_util_pct = (
        (100.0 * len(bundle.initial_pods) / node_summary.total_pods) if node_summary.total_pods > 0 else 0.0
    )
    active_init_nodes = len([n for n in init_usage.pods_by_node if n != "<unassigned>"])

    print("")
    accel_tot_str = (
        ", " + ", ".join(f"{v} {k}" for k, v in sorted(node_summary.total_accels.items()))
        if node_summary.total_accels
        else ""
    )
    print(
        f"  [3] CLUSTER NODES & INITIAL USAGE AT START ({len(bundle.nodes)} Nodes: "
        f"{node_summary.total_cpu:g} CPU, {node_summary.total_mem_gi:g}Gi Memory, "
        f"{node_summary.total_pods} Pods{accel_tot_str})"
    )
    if node_summary.node_groups:
        print(
            f"      {'ROLE':<8}  {'NODE TYPE':<16}  {'NODES':>5}  "
            f"{'PER-NODE CAPACITY':<30}  {'POOL TOTAL CAPACITY':<28}  {'NODE LABELS / SELECTORS'}"
        )
        print(
            f"      {'-' * 8}  {'-' * 16}  {'-' * 5}  "
            f"{'-' * 30}  {'-' * 28}  {'-' * 26}"
        )
        for grp in node_summary.node_groups.values():
            cnt = grp["count"]
            per_node = f"{grp['cpu_str']} CPU, {grp['mem_str']} Mem, {grp['pods_str']} pods"
            if grp["accel_str"] != "-":
                per_node += f", {grp['accel_str']}"
            tot_pool = f"{cnt * grp['cpu_val']:g} CPU, {cnt * grp['mem_val']:g}Gi Mem, {cnt * grp['pods_val']} pods"
            names_preview = (
                f"{grp['node_names'][0]}..{grp['node_names'][-1]}"
                if cnt > 2
                else ", ".join(grp["node_names"])
            )
            print(
                f"      {grp['role']:<8}  {grp['node_type']:<16}  {cnt:>5}  "
                f"{per_node:<30}  {tot_pool:<28}  {grp['selectors']} ({names_preview})"
            )
    else:
        print("      No nodes recorded in infra.json")

    print("")
    expired_note = (
        f" (+ {bundle.expired_initial_pods_count} pod(s) from t0 already finished before +{bundle.initial_jump_raw_sec:.2f}s jump)"
        if bundle.expired_initial_pods_count > 0
        else ""
    )
    print(
        f"      Initial Usage at Start: {len(bundle.initial_pods)} running pod(s) in Kueue-excluded namespace "
        f"'{bundle.infra_pods_namespace}' (kueue.x-k8s.io/exclude=true) across "
        f"{active_init_nodes}/{len(bundle.nodes)} active node(s) "
        f"({len(bundle.nodes) - active_init_nodes} idle){expired_note}"
    )
    accel_load_parts: list[str] = []
    for rk, tot_acc in sorted(node_summary.total_accels.items()):
        used_acc = init_usage.accels_used.get(rk, 0)
        acc_pct = (100.0 * used_acc / tot_acc) if tot_acc > 0 else 0.0
        accel_load_parts.append(f"{rk}={used_acc}/{tot_acc} ({acc_pct:.1f}%)")
    accel_load_str = (", " + ", ".join(accel_load_parts)) if accel_load_parts else ""
    print(
        f"      Initial Resource Load : CPU={init_usage.cpu_used:g}/{node_summary.total_cpu:g} cores ({cpu_util_pct:.1f}%), "
        f"Memory={init_usage.mem_used_gi:g}/{node_summary.total_mem_gi:g}Gi ({mem_util_pct:.1f}%), "
        f"Pods={len(bundle.initial_pods)}/{node_summary.total_pods} ({pod_util_pct:.1f}%){accel_load_str}"
    )
    if bundle.initial_pods:
        if init_usage.finite_durs:
            fin_str = (
                f"{len(init_usage.finite_durs)} finite (remaining {min(init_usage.finite_durs):.1f}s .. "
                f"{max(init_usage.finite_durs):.1f}s)"
            )
        else:
            fin_str = "0 finite"
        inf_str = f"{init_usage.infinite_count} infinite/super-long"
        print(f"      Initial Pod Lifetimes : {fin_str}, {inf_str}")
        sorted_init_nodes = sorted(init_usage.pods_by_node.items())
        for n_name, npods in sorted_init_nodes[:8]:
            n_cpu = sum(parse_k8s_quantity(p.resource_request.get("cpu"), "cpu") for p in npods)
            n_mem = sum(parse_k8s_quantity(p.resource_request.get("memory"), "memory") for p in npods)
            print(f"        - {n_name}: {len(npods)} pod(s) requesting {n_cpu:g} CPU, {n_mem:g}Gi Memory")
        if len(sorted_init_nodes) > 8:
            print(f"        - ... and {len(sorted_init_nodes) - 8} more node(s) with initial pods")


def _print_queues_section(
    cq_docs: list[dict[str, Any]],
    lq_docs: list[dict[str, Any]],
    rf_docs: list[dict[str, Any]],
    topo_docs: list[dict[str, Any]],
    cohort_docs: list[dict[str, Any]],
    queue_load: _QueueLoadSummary,
) -> None:
    print("")
    print(
        f"  [4] KUEUE QUEUES — QUOTA, PREEMPTION, FAIR SHARING & RECLAIM SETTINGS "
        f"({len(cq_docs)} ClusterQueue(s), {len(lq_docs)} LocalQueue(s), {len(rf_docs)} ResourceFlavor(s))"
    )
    if rf_docs or topo_docs:
        rf_summaries: list[str] = []
        for rf in rf_docs:
            rf_name = (rf.get("metadata") or {}).get("name", "?")
            rf_labels = (rf.get("spec") or {}).get("nodeLabels") or {}
            rf_topo = (rf.get("spec") or {}).get("topologyName")
            lbl_s = ",".join(f"{k}={v}" for k, v in sorted(rf_labels.items())) or "any"
            if rf_topo:
                lbl_s += f", topology={rf_topo}"
            rf_summaries.append(f"{rf_name}[{lbl_s}]")
        print(f"      ResourceFlavors       : {', '.join(rf_summaries)}")
        if topo_docs:
            topo_summaries = [
                f"{(t.get('metadata') or {}).get('name', '?')}[levels="
                + "->".join(str(lv.get("nodeLabel", "")) for lv in ((t.get("spec") or {}).get("levels") or []))
                + "]"
                for t in topo_docs
            ]
            print(f"      Topologies            : {', '.join(topo_summaries)}")
        if cohort_docs:
            cohort_summaries: list[str] = []
            for ch in cohort_docs:
                ch_name = (ch.get("metadata") or {}).get("name", "?")
                ch_rgs = (ch.get("spec") or {}).get("resourceGroups") or []
                ch_q_parts: list[str] = []
                for rg in ch_rgs:
                    for flv in rg.get("flavors") or []:
                        for res in flv.get("resources") or []:
                            ch_q_parts.append(f"{res.get('name')}={res.get('nominalQuota')}")
                if ch_q_parts:
                    cohort_summaries.append(f"{ch_name}[nominalQuota: {', '.join(ch_q_parts)}]")
                else:
                    cohort_summaries.append(ch_name)
            print(f"      Cohorts               : {', '.join(cohort_summaries)}")

    if cq_docs:
        print(
            f"      {'CLUSTER QUEUE':<15}  {'COHORT':<14}  {'STRATEGY':<14}  {'FAIR SHARING':<14}  "
            f"{'PREEMPTION (WITHIN / RECLAIM / BORROW)':<52}  {'LOCAL QUEUES'}"
        )
        print(
            f"      {'-' * 15}  {'-' * 14}  {'-' * 14}  {'-' * 14}  "
            f"{'-' * 52}  {'-' * 20}"
        )
        for cq in cq_docs:
            cq_name = str((cq.get("metadata") or {}).get("name") or "?")
            spec = cq.get("spec") or {}
            cohort = str(spec.get("cohortName") or spec.get("cohort") or "-")
            strategy = str(spec.get("queueingStrategy") or "BestEffortFIFO")
            stop_pol = str(spec.get("stopPolicy") or "None")
            if stop_pol != "None":
                strategy = f"{strategy}(stop={stop_pol})"

            fs_obj = spec.get("fairSharing") or {}
            fs_weight = fs_obj.get("weight")
            fs_str = f"weight={fs_weight}" if fs_weight is not None else "none"

            preempt = spec.get("preemption") or {}
            within_cq = str(preempt.get("withinClusterQueue") or "Never")
            reclaim_cohort = str(preempt.get("reclaimWithinCohort") or "Never")
            borrow_obj = preempt.get("borrowWithinCohort") or {}
            borrow_pol = str(borrow_obj.get("policy") or "Never")
            max_prio = borrow_obj.get("maxPriorityThreshold")
            if max_prio is not None:
                borrow_pol += f"(maxPrio={max_prio})"
            preempt_str = f"within={within_cq}, reclaim={reclaim_cohort}, borrow={borrow_pol}"

            mapped_lqs = ", ".join(queue_load.lqs_for_cq.get(cq_name, [])) or "-"
            print(
                f"      {cq_name:<15}  {cohort:<14}  {strategy:<14}  {fs_str:<14}  "
                f"{preempt_str:<52}  {mapped_lqs}"
            )

            fung = spec.get("flavorFungibility") or {}
            fung_str = ""
            if fung:
                fung_str = (
                    f" | flavorFungibility(borrow={fung.get('whenCanBorrow', 'MayStopSearch')}, "
                    f"preempt={fung.get('whenCanPreempt', 'TryNextFlavor')})"
                )

            for rg_idx, rg in enumerate(spec.get("resourceGroups") or []):
                covered = ",".join(str(x) for x in (rg.get("coveredResources") or []))
                for flv in rg.get("flavors") or []:
                    flv_name = str(flv.get("name") or "default")
                    q_parts: list[str] = []
                    for res in flv.get("resources") or []:
                        r_name = str(res.get("name") or "")
                        nom = str(res.get("nominalQuota") or "0")
                        b_lim = res.get("borrowingLimit")
                        l_lim = res.get("lendingLimit")
                        extras: list[str] = []
                        if b_lim is not None:
                            extras.append(f"borrowLimit={b_lim}")
                        if l_lim is not None:
                            extras.append(f"lendLimit={l_lim}")
                        extra_s = f" [{', '.join(extras)}]" if extras else ""
                        q_parts.append(f"{r_name}: nominal={nom}{extra_s}")
                    print(
                        f"        -> flavor='{flv_name}' (covered=[{covered}]): "
                        + ", ".join(q_parts)
                        + (fung_str if rg_idx == 0 else "")
                    )

            w_st = queue_load.wf_stats_by_cq.get(cq_name)
            if w_st:
                print(
                    f"        -> scenario load: {w_st['workflows']} workflow(s), "
                    f"{w_st['pods']} pod(s), total demand={w_st['cpu']:g} CPU, {w_st['mem_gi']:g}Gi Memory"
                )
    else:
        print("      No ClusterQueues recorded in infra.json")


def _print_workflows_section(bundle: ScenarioBundle, queue_load: _QueueLoadSummary) -> None:
    print("")
    kinds_str = ", ".join(f"{k}={v}" for k, v in sorted(queue_load.wf_counts_by_kind.items())) or "none"
    reasons_str = ", ".join(f"{k}={v}" for k, v in sorted(queue_load.wf_counts_by_reason.items())) or "none"
    print(
        f"  [5] WORKFLOWS TO CREATE ({len(bundle.scheduled_workflows)} top-level NotAdmitted workflows, "
        f"{queue_load.total_wf_pods} total pods, {queue_load.total_wf_cpu:g} CPU, {queue_load.total_wf_mem_gi:g}Gi Memory)"
    )
    print(f"      By Resource Kind      : {kinds_str}")
    print(f"      By Trigger Reason     : {reasons_str}")
    print(
        f"      Skipped Workflows     : {len(bundle.skipped_already_admitted_workflows)} already-admitted at t0, "
        f"{bundle.skipped_child_workflows} controller-managed child object(s)"
    )
    print("========================================================================================", flush=True)


def print_scenario_outline(
    bundle: ScenarioBundle,
    speedup: float,
    enable_fair_sharing: bool | None = True,
    *,
    reused_chamber: bool = False,
) -> None:
    """Print a detailed description of the test case: length, execution-time index, nodes & initial usage, queues, and workflows."""
    cq_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "ClusterQueue"]
    lq_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "LocalQueue"]
    rf_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "ResourceFlavor"]
    topo_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "Topology"]
    cohort_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "Cohort"]

    node_summary = _summarize_node_groups(bundle.nodes)
    init_usage = _summarize_initial_pod_usage(bundle.initial_pods)
    queue_load = _summarize_queue_load(lq_docs, bundle.scheduled_workflows)

    _print_timeline_and_index(bundle, speedup, enable_fair_sharing, reused_chamber)
    _print_nodes_and_initial_usage(bundle, node_summary, init_usage)
    _print_queues_section(cq_docs, lq_docs, rf_docs, topo_docs, cohort_docs, queue_load)
    _print_workflows_section(bundle, queue_load)
