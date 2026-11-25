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

"""Markdown section renderers for auto-generated test scenario README.md summaries."""

from __future__ import annotations

from collections import Counter, defaultdict
import os
from pathlib import Path
from typing import Any

from chamber.constants import SCRIPT_DIR
from chamber.parsers import (
    calc_summary_stats,
    format_duration_human,
    format_duration_short,
    format_epoch_iso,
    format_resource_quantity,
)
from chamber.yaml_utils import to_clean_yaml
from scenario_readme.analysis import (
    COMMON_TOPOLOGY_LABEL_CANDIDATES,
    DEFAULT_SIX_HOUR_SEC,
    ParsedWorkload,
    ScenarioAnalysis,
    analyze_scenario,
    describe_node_group,
    extract_rg_totals,
)
from scenario_readme.histograms import render_duration_histogram, render_timeline_arrival_histogram


def fmt_quantity(val: float, resource_name: str = "") -> str:
    """Format a parsed resource value back into a clean human-readable string."""
    return format_resource_quantity(val, resource_name, show_tib_paren=True)


def _render_overview_section(
    a: ScenarioAnalysis,
    plan_5m: dict[str, Any] | None,
    plan_10m: dict[str, Any] | None,
) -> list[str]:
    md: list[str] = ["## 1. Scenario Overview", ""]

    total_wf_dur_stats = calc_summary_stats([w.duration_sec for w in a.unadmitted_wfs])
    total_wf_pod_stats = calc_summary_stats([float(w.total_pods) for w in a.unadmitted_wfs])
    headline_sat = a.saturation_pct(a.unadmitted_wfs, a.primary_sat_resource)
    cpu_sat_all = a.saturation_pct(a.unadmitted_wfs, "cpu")
    mem_sat_all = a.saturation_pct(a.unadmitted_wfs, "memory")

    cap_summary_parts = [
        f"**{a.total_node_count:,} nodes**",
        f"`{fmt_quantity(a.cluster_cap.get('cpu', 0.0), 'cpu')}`",
        f"`{fmt_quantity(a.cluster_cap.get('memory', 0.0), 'memory')}`",
    ]
    for ar in a.accel_resources:
        cap_summary_parts.append(f"`{fmt_quantity(a.cluster_cap[ar], ar)}` `{ar}`")

    md.append("| Dimension | Summary |")
    md.append("| :--- | :--- |")
    md.append(f"| **Scenario Directory** | `{a.rel_dir}` |")
    md.append(
        f"| **Raw Event Counts** | `infra.json`: **{a.infra_events_count:,}** events · "
        f"`workflows.json`: **{a.workflow_events_count:,}** events · "
        f"`execution-time.json`: **{a.exec_index.total_events:,}** events |"
    )
    post_t0_node_note = ""
    if a.post_t0_node_events:
        ev_counts = Counter(ev_t for ev_t, _, _ in a.post_t0_node_events)
        ev_desc = ", ".join(f"{cnt} `{ev_t}`" for ev_t, cnt in sorted(ev_counts.items()))
        post_t0_node_note = f"; +{len(a.post_t0_node_events)} post-$t_0$ `Node` event(s): {ev_desc}"
    md.append(
        f"| **Cluster Capacity** | {' · '.join(cap_summary_parts)} "
        f"({len(a.node_groups)} distinct node group{'s' if len(a.node_groups) != 1 else ''}{post_t0_node_note}) |"
    )
    md.append(
        f"| **Topology Configuration** | **{len(a.topologies_by_name)}** `Topology` CRD(s) "
        f"({', '.join(f'`{t}`' for t in sorted(a.topologies_by_name)) if a.topologies_by_name else 'none'}) |"
    )
    init_prim_pct = (
        (100.0 * a.init_pod_requests_sum.get(a.primary_sat_resource, 0.0) / a.cluster_alloc[a.primary_sat_resource])
        if a.cluster_alloc.get(a.primary_sat_resource, 0.0) > 0
        else 0.0
    )
    synth_init_note = (
        f"; includes {a.synthesized_initial_pods:,} synthesized from pre-admitted $t_0$ workloads"
        if a.synthesized_initial_pods > 0
        else ""
    )
    md.append(
        f"| **Initial In-Flight Pods at $t_0$** | **{a.total_initial_pods:,}** pods across "
        f"**{len(a.init_nodes_used):,}** / {a.total_node_count:,} nodes "
        f"(initial `{a.primary_sat_resource}` utilization: **{init_prim_pct:.2f}%**{synth_init_note}) |"
    )
    md.append(
        f"| **Kueue Objects** | **{len(a.flavors_by_name)}** `ResourceFlavor`(s) · "
        f"**{len(a.cohorts_by_name)}** `Cohort`(s) · "
        f"**{len(a.cqs_by_name)}** `ClusterQueue`(s) · "
        f"**{len(a.lqs_by_key)}** `LocalQueue`(s) · "
        f"**{len(a.wpc_by_name)}** `WorkloadPriorityClass`(es) |"
    )
    if a.kueue_configuration is not None:
        fs_blk = a.kueue_configuration.get("fairSharing")
        if isinstance(fs_blk, dict) and fs_blk.get("enable") is not False:
            fs_strats = fs_blk.get("preemptionStrategies") or [
                "LessThanOrEqualToFinalShare",
                "LessThanInitialShare",
            ]
            fs_summary = f"enabled (`[{', '.join(str(x) for x in fs_strats)}]`)"
        else:
            fs_summary = "**disabled globally** (`Classical Preemption`)"
        afs_blk = a.kueue_configuration.get("admissionFairSharing")
        if isinstance(afs_blk, dict):
            afs_summary = (
                f"enabled (`usageHalfLifeTime={afs_blk.get('usageHalfLifeTime', '0s')}`, "
                f"`usageSamplingInterval={afs_blk.get('usageSamplingInterval', '5m')}`)"
            )
        else:
            afs_summary = "disabled"
        md.append(
            f"| **Kueue Controller `Configuration`** | `fairSharing`: {fs_summary} · "
            f"`admissionFairSharing`: {afs_summary} |"
        )
    md.append(
        f"| **Test Arrival Window ($T_{{\\text{{test}}}}$)** | **{format_duration_human(a.test_length_sec)}** "
        f"(`{format_epoch_iso(a.t_first_unadmitted)}` $\\rightarrow$ `{format_epoch_iso(a.t_last_unadmitted)}`) |"
    )
    md.append(
        f"| **Unadmitted Workloads** | **{len(a.unadmitted_wfs):,}** `NotAdmitted` arrivals "
        f"({int(total_wf_pod_stats['sum']):,} total pods; gang size min={int(total_wf_pod_stats['min'])}, "
        f"p50={int(total_wf_pod_stats['p50'])}, p90={int(total_wf_pod_stats['p90'])}, max={int(total_wf_pod_stats['max'])}) "
        f"+ **{len(a.admitted_at_start_wfs):,}** pre-admitted at $t_0$ |"
    )
    md.append(
        f"| **Execution Duration Quantiles** | "
        f"`min={format_duration_short(total_wf_dur_stats['min'])}`, "
        f"`P50={format_duration_short(total_wf_dur_stats['p50'])}`, "
        f"`AVG={format_duration_short(total_wf_dur_stats['avg'])}`, "
        f"`P90={format_duration_short(total_wf_dur_stats['p90'])}`, "
        f"`P99={format_duration_short(total_wf_dur_stats['p99'])}`, "
        f"`max={format_duration_short(total_wf_dur_stats['max'])}` |"
    )

    sat_parts = []
    if headline_sat is not None:
        sat_parts.append(f"{a.primary_sat_label}: **{headline_sat:.2f}%**")
    if a.primary_sat_resource != "cpu" and cpu_sat_all is not None:
        sat_parts.append(f"`cpu`: **{cpu_sat_all:.2f}%**")
    if a.primary_sat_resource != "memory" and mem_sat_all is not None:
        sat_parts.append(f"`memory`: **{mem_sat_all:.2f}%**")
    if (
        a.primary_accel_resource
        and a.primary_accel_resource != a.primary_sat_resource
        and a.total_wf_accel > 0
    ):
        acc_sat_all = a.saturation_pct(a.unadmitted_wfs, a.primary_accel_resource)
        if acc_sat_all is not None:
            sat_parts.append(f"`{a.primary_accel_resource}`: **{acc_sat_all:.2f}%**")

    headline_sat_6h = a.saturation_over_window_pct(a.unadmitted_wfs, a.primary_sat_resource, DEFAULT_SIX_HOUR_SEC)
    cpu_sat_6h = a.saturation_over_window_pct(a.unadmitted_wfs, "cpu", DEFAULT_SIX_HOUR_SEC)
    mem_sat_6h = a.saturation_over_window_pct(a.unadmitted_wfs, "memory", DEFAULT_SIX_HOUR_SEC)
    sat_6h_parts = []
    if headline_sat_6h is not None:
        sat_6h_parts.append(f"{a.primary_sat_label}: **{headline_sat_6h:.2f}%**")
    if a.primary_sat_resource != "cpu" and cpu_sat_6h is not None:
        sat_6h_parts.append(f"`cpu`: **{cpu_sat_6h:.2f}%**")
    if a.primary_sat_resource != "memory" and mem_sat_6h is not None:
        sat_6h_parts.append(f"`memory`: **{mem_sat_6h:.2f}%**")
    if (
        a.primary_accel_resource
        and a.primary_accel_resource != a.primary_sat_resource
        and a.total_wf_accel > 0
    ):
        acc_sat_6h = a.saturation_over_window_pct(
            a.unadmitted_wfs, a.primary_accel_resource, DEFAULT_SIX_HOUR_SEC
        )
        if acc_sat_6h is not None:
            sat_6h_parts.append(f"`{a.primary_accel_resource}`: **{acc_sat_6h:.2f}%**")

    empty_sat_note = "n/a (instantaneous $t=0$ burst: $T_{\\text{test}} = 0$)"
    sat_window_summary = " · ".join(sat_parts) if sat_parts else empty_sat_note
    md.append(
        f"| **1. Saturation (Arrival Window $T_{{\\text{{test}}}}$)** | "
        f"{sat_window_summary} "
        f"*(over $T_{{\\text{{test}}}} = {format_duration_short(a.test_length_sec)}$)* |"
    )
    md.append(
        f"| **2. Saturation (Reference `6h` / `21,600s` Window)** | "
        f"{' · '.join(sat_6h_parts) if sat_6h_parts else 'n/a'} "
        f"*(over $T_{{6\\text{{h}}}} = 21{{,}}600\\text{{s}}$)* |"
    )
    if plan_5m and plan_10m:
        md.append(
            f"| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | "
            f"**5m (`300s`)**: `--speedup {plan_5m['speedup_str']} --duration {plan_5m['rec_duration_flag']}` · "
            f"**10m (`600s`)**: `--speedup {plan_10m['speedup_str']} --duration {plan_10m['rec_duration_flag']}` "
            f"*(bottleneck: {a.primary_sat_label}, $T_{{100\\%}} = {format_duration_human(plan_5m['t100_sec'])}$)* |"
        )
    else:
        md.append("| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | n/a |")
    md.append("")
    return md


def _render_nodes_section(a: ScenarioAnalysis) -> list[str]:
    md: list[str] = [
        "---",
        "",
        "## 2. Cluster Nodes & Distinct Node Groups",
        "",
        (
            f"The cluster contains **{a.total_node_count:,}** `v1/Node` objects partitioned into "
            f"**{len(a.node_groups)}** distinct hardware/label configuration group(s)."
        ),
        "",
        "### 2.1 Aggregate Physical Cluster Capacity & Allocatable Resources",
        "",
        "| Resource | Total Capacity | Total Allocatable | Per-Node Average (Capacity) |",
        "| :--- | ---: | ---: | ---: |",
        f"| `nodes` | **{a.total_node_count:,}** | **{a.total_node_count:,}** | `1` |",
    ]
    for rk in a.ordered_res_keys:
        if rk not in a.cluster_cap and rk not in a.cluster_alloc:
            continue
        c_val = a.cluster_cap.get(rk, 0.0)
        a_val = a.cluster_alloc.get(rk, 0.0)
        per_node = c_val / a.total_node_count if a.total_node_count > 0 else 0.0
        md.append(
            f"| `{rk}` | **{fmt_quantity(c_val, rk)}** | **{fmt_quantity(a_val, rk)}** | {fmt_quantity(per_node, rk)} |"
        )
    md.append("")

    md.append("### 2.2 Summary of Distinct Node Groups")
    md.append("")
    accel_col_hdr = " | ".join(f"`{ar}` / Node" for ar in a.accel_resources) if a.accel_resources else "Accelerators"
    accel_col_sep = " | ".join("---:" for _ in a.accel_resources) if a.accel_resources else ":---:"
    md.append(
        f"| Group | Node Count | Share | Sample Node Name | `cpu` / Node | `memory` / Node | `pods` / Node | {accel_col_hdr} | Key Identifying Labels |"
    )
    md.append(f"| :--- | ---: | ---: | :--- | ---: | ---: | ---: | {accel_col_sep} | :--- |")

    for g_idx, grp in enumerate(a.node_groups):
        sample_n = grp[0]
        g_cnt = len(grp)
        g_share = (100.0 * g_cnt / a.total_node_count) if a.total_node_count > 0 else 0.0
        n_meta = sample_n.get("metadata") or {}
        n_labels = n_meta.get("labels") or {}
        n_cap = (sample_n.get("status") or {}).get("capacity") or {}
        g_title = describe_node_group(sample_n, g_idx, grp)

        if a.accel_resources:
            accel_cells = " | ".join(str(n_cap.get(ar, "0")) for ar in a.accel_resources)
        else:
            accel_cells = "none"

        key_lbl_items = [
            f"`{k}={v}`"
            for k, v in sorted(n_labels.items())
            if k in a.non_unique_label_keys
            and k
            not in (
                "beta.kubernetes.io/arch",
                "beta.kubernetes.io/os",
                "kubernetes.io/arch",
                "kubernetes.io/os",
                "node-role.kubernetes.io/agent",
            )
            and str(v) != ""
        ]
        key_lbl_str = ", ".join(key_lbl_items[:5]) if key_lbl_items else "*(standard KWOK labels)*"

        md.append(
            f"| **{g_title}** | **{g_cnt:,}** | {g_share:.1f}% | `{n_meta.get('name', '?')}` | "
            f"`{n_cap.get('cpu', '?')}` | `{n_cap.get('memory', '?')}` | `{n_cap.get('pods', '?')}` | "
            f"{accel_cells} | {key_lbl_str} |"
        )
    md.append("")

    md.append("### 2.3 Sample Node Manifest (YAML) from Each Distinct Group")
    md.append("")
    for g_idx, grp in enumerate(a.node_groups):
        sample_n = grp[0]
        g_title = describe_node_group(sample_n, g_idx, grp)
        n_name = (sample_n.get("metadata") or {}).get("name", f"node-{g_idx}")
        md.append(f"#### {g_title} — Sample Node `{n_name}` ({len(grp):,} nodes in group)")
        md.append("")
        md.append("```yaml")
        md.append(to_clean_yaml(sample_n))
        md.append("```")
        md.append("")
    return md


def _render_topology_section(a: ScenarioAnalysis) -> list[str]:
    md: list[str] = ["---", "", "## 3. Cluster Topology & Node Distribution", ""]

    if a.topologies_by_name:
        md.append(
            f"This scenario defines **{len(a.topologies_by_name)}** Kueue `Topology` resource(s) for "
            f"Topology-Aware Scheduling (`TAS`):"
        )
        md.append("")
        for t_name, t_obj in sorted(a.topologies_by_name.items()):
            levels = [
                str(lv.get("nodeLabel") or "")
                for lv in ((t_obj.get("spec") or {}).get("levels") or [])
                if isinstance(lv, dict) and lv.get("nodeLabel")
            ]
            md.append(f"### 3.1 Topology `{t_name}` ({len(levels)} Hierarchy Level{'s' if len(levels) != 1 else ''})")
            md.append("")
            md.append("```yaml")
            md.append(to_clean_yaml(t_obj))
            md.append("```")
            md.append("")
            md.append(f"#### Node Distribution Across `{t_name}` Hierarchy Levels")
            md.append("")
            md.append(
                "| Level | Topology Label (`nodeLabel`) | Distinct Domains | Nodes w/ Label | Min Nodes/Domain | Median (P50) | Mean Nodes/Domain | Max Nodes/Domain | Domain Balance | Sample Domain Counts |"
            )
            md.append("| :---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- | :--- |")

            for lv_idx, lbl_key in enumerate(levels, start=1):
                dom_counts: Counter[str] = Counter()
                for n_obj in a.all_nodes:
                    val = ((n_obj.get("metadata") or {}).get("labels") or {}).get(lbl_key)
                    if val is not None and str(val) != "":
                        dom_counts[str(val)] += 1
                if not dom_counts:
                    md.append(
                        f"| `L{lv_idx}` | `{lbl_key}` | `0` | `0 / {a.total_node_count:,}` | — | — | — | — | Unused | *(label absent on nodes)* |"
                    )
                    continue
                cnt_vals = list(dom_counts.values())
                st_c = calc_summary_stats([float(c) for c in cnt_vals])
                covered = int(st_c["sum"])
                balanced = "Uniform" if st_c["min"] == st_c["max"] else f"Asymmetric (`{int(st_c['min'])}..{int(st_c['max'])}`)"
                dist_of_sizes = Counter(cnt_vals)
                if len(dist_of_sizes) == 1:
                    size_val = next(iter(dist_of_sizes.keys()))
                    sample_doms = ", ".join(f"`{d}` ({c})" for d, c in sorted(dom_counts.items())[:3])
                    if len(dom_counts) > 3:
                        sample_doms += f", … ({len(dom_counts)}x `{size_val}` nodes)"
                else:
                    sample_doms = ", ".join(
                        f"{num_d} domain{'s' if num_d != 1 else ''} × {sz} nodes"
                        for sz, num_d in sorted(dist_of_sizes.items(), reverse=True)
                    )
                md.append(
                    f"| `L{lv_idx}` | `{lbl_key}` | **{len(dom_counts):,}** | `{covered:,} / {a.total_node_count:,}` | "
                    f"`{int(st_c['min'])}` | `{int(st_c['p50'])}` | `{st_c['avg']:.1f}` | `{int(st_c['max'])}` | "
                    f"{balanced} | {sample_doms} |"
                )
            md.append("")

            if len(levels) >= 2:
                md.append(f"#### Hierarchical Tree Branching Summary (`{t_name}`)")
                md.append("")
                md.append("```text")
                md.append(f"Cluster Total: {a.total_node_count:,} nodes")
                for lv_idx, lbl_key in enumerate(levels, start=1):
                    dom_counts = Counter(
                        str(((n.get("metadata") or {}).get("labels") or {}).get(lbl_key))
                        for n in a.all_nodes
                        if ((n.get("metadata") or {}).get("labels") or {}).get(lbl_key) is not None
                    )
                    indent_str = "  " * lv_idx
                    if not dom_counts:
                        md.append(f"{indent_str}└── L{lv_idx} [{lbl_key}]: 0 domains (not labeled on nodes)")
                        continue
                    size_hist = Counter(dom_counts.values())
                    hist_desc = ", ".join(
                        f"{cnt} domain(s) of {sz} node(s)" for sz, cnt in sorted(size_hist.items(), reverse=True)
                    )
                    md.append(
                        f"{indent_str}└── L{lv_idx} [{lbl_key}]: {len(dom_counts):,} total domains ({hist_desc})"
                    )
                md.append("```")
                md.append("")
    else:
        md.append(
            "No `kueue.x-k8s.io/Topology` CRD is defined in `infra.json` (Topology-Aware Scheduling is not explicitly configured via a `Topology` object)."
        )
        md.append("")

    covered_topo_keys = {
        str(lv.get("nodeLabel") or "")
        for t_obj in a.topologies_by_name.values()
        for lv in ((t_obj.get("spec") or {}).get("levels") or [])
        if isinstance(lv, dict)
    }
    extra_topo_keys = [
        lk
        for lk in sorted(a.label_val_sets.keys())
        if lk not in covered_topo_keys
        and lk != "kubernetes.io/hostname"
        and (
            lk in COMMON_TOPOLOGY_LABEL_CANDIDATES
            or any(sub in lk.lower() for sub in ("topology", "partition", "rack", "island", "cube", "zone", "block", "switch"))
        )
        and 1 < len(a.label_val_sets[lk]) < a.total_node_count
    ]
    if extra_topo_keys:
        md.append("### Additional Node Placement / Topology Labels on Nodes")
        md.append("")
        md.append("| Node Label Key | Distinct Values | Min Nodes/Value | Median (P50) | Mean Nodes/Value | Max Nodes/Value | Distribution Summary |")
        md.append("| :--- | ---: | ---: | ---: | ---: | ---: | :--- |")
        for lk in extra_topo_keys:
            dom_counts = Counter(
                str(((n.get("metadata") or {}).get("labels") or {}).get(lk))
                for n in a.all_nodes
                if ((n.get("metadata") or {}).get("labels") or {}).get(lk) is not None
            )
            st_c = calc_summary_stats([float(c) for c in dom_counts.values()])
            size_hist = Counter(dom_counts.values())
            hist_desc = ", ".join(
                f"{num_d} × {sz} nodes" for sz, num_d in sorted(size_hist.items(), reverse=True)
            )
            md.append(
                f"| `{lk}` | **{len(dom_counts):,}** | `{int(st_c['min'])}` | `{int(st_c['p50'])}` | "
                f"`{st_c['avg']:.1f}` | `{int(st_c['max'])}` | {hist_desc} |"
            )
        md.append("")
    return md


def _render_initial_pods_section(a: ScenarioAnalysis) -> list[str]:
    md: list[str] = ["---", "", "## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$", ""]

    if a.total_initial_pods == 0:
        md.append(
            f"- **Initial Running Pods in `infra.json`**: **`0`** (the cluster starts completely empty at $t_0 = {format_epoch_iso(a.t0_epoch)}$)."
        )
        if a.admitted_at_start_wfs:
            md.append(
                f"- Note: `workflows.json` records **{len(a.admitted_at_start_wfs)}** workflow(s) marked with `workloadState: \"Admitted\"` at $t_0$."
            )
        md.append("- **Initial Cluster Utilization**: **`0.00%`** across all resources (`cpu`, `memory`, `pods`, accelerators).")
        md.append("")
        return md

    life_stats = calc_summary_stats(a.init_pod_lifetimes)
    md.append(
        f"At scenario start ($t_0 = {format_epoch_iso(a.t0_epoch)}$), **`{a.total_initial_pods:,}`** running `Pod` objects "
        f"are pre-placed across **`{len(a.init_nodes_used):,}`** of the **`{a.total_node_count:,}`** cluster nodes "
        f"(`{100.0 * len(a.init_nodes_used) / max(1, a.total_node_count):.1f}%` of nodes occupied; "
        f"`{a.total_node_count - len(a.init_nodes_used):,}` nodes completely free)."
    )
    if a.admitted_at_start_wfs:
        kind_counts = Counter(w.kind for w in a.admitted_at_start_wfs)
        if len(kind_counts) == 1:
            only_kind = next(iter(kind_counts))
            md.append(
                f"Additionally, `workflows.json` records **`{len(a.admitted_at_start_wfs):,}`** pre-existing `Admitted` "
                f"`{only_kind}` warmup workflow object(s) at $t_0$."
            )
        else:
            kind_parts = ", ".join(
                f"{cnt:,} `{k}`"
                for k, cnt in sorted(kind_counts.items(), key=lambda kv: (-kv[1], kv[0]))
            )
            md.append(
                f"Additionally, `workflows.json` records **`{len(a.admitted_at_start_wfs):,}`** pre-existing `Admitted` "
                f"warmup workflow object(s) at $t_0$ ({kind_parts})."
            )
    md.append("")

    md.append("### 4.1 Initial Cluster Utilization Breakdown")
    md.append("")
    md.append(
        "| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |"
    )
    md.append("| :--- | ---: | ---: | ---: | ---: | ---: |")
    for rk in a.ordered_res_keys:
        if rk not in a.cluster_alloc and rk not in a.init_pod_requests_sum:
            continue
        used = a.init_pod_requests_sum.get(rk, 0.0)
        alloc_v = a.cluster_alloc.get(rk, 0.0)
        cap_v = a.cluster_cap.get(rk, 0.0)
        free_v = max(0.0, alloc_v - used)
        pct_alloc = (100.0 * used / alloc_v) if alloc_v > 0 else 0.0
        pct_cap = (100.0 * used / cap_v) if cap_v > 0 else 0.0
        md.append(
            f"| `{rk}` | **{fmt_quantity(used, rk)}** | {fmt_quantity(alloc_v, rk)} | "
            f"{fmt_quantity(free_v, rk)} | **{pct_alloc:.2f}%** | {pct_cap:.2f}% |"
        )
    md.append("")

    md.append("### 4.2 Initial Pod Remaining Lifetimes & Request Profiles")
    md.append("")
    if a.init_pod_lifetimes:
        md.append(
            f"- **Pods with Finite Remaining Lifetime (`deletionTime - t0`)**: **{len(a.init_pod_lifetimes):,}** pods — "
            f"`min={format_duration_short(life_stats['min'])}`, "
            f"`P50={format_duration_short(life_stats['p50'])}`, "
            f"`AVG={format_duration_short(life_stats['avg'])}`, "
            f"`P90={format_duration_short(life_stats['p90'])}`, "
            f"`P99={format_duration_short(life_stats['p99'])}`, "
            f"`max={format_duration_short(life_stats['max'])}`"
        )
    if a.init_pod_infinite_count > 0:
        md.append(
            f"- **Pods with No Deletion Event (Run Entire Scenario / Infinite Duration)**: **{a.init_pod_infinite_count:,}** pods"
        )
    pods_per_occ_node = calc_summary_stats([float(c) for c in a.init_nodes_used.values()])
    if a.init_nodes_used:
        md.append(
            f"- **Pods per Occupied Node (`{len(a.init_nodes_used):,}` active nodes)**: "
            f"`min={int(pods_per_occ_node['min'])}`, `P50={int(pods_per_occ_node['p50'])}`, "
            f"`AVG={pods_per_occ_node['avg']:.2f}`, `P90={int(pods_per_occ_node['p90'])}`, "
            f"`max={int(pods_per_occ_node['max'])}`"
        )
    md.append("")
    md.append("| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |")
    md.append("| :---: | :--- | ---: | ---: |")
    for rank_idx, (prof, p_cnt) in enumerate(a.init_pod_profiles.most_common(10), start=1):
        prof_str = ", ".join(f"`{k}: {v}`" for k, v in prof) if prof else "*(unspecified)*"
        md.append(f"| `{rank_idx}` | {prof_str} | **{p_cnt:,}** | {100.0 * p_cnt / a.total_initial_pods:.1f}% |")
    md.append("")
    return md


def _render_kueue_setup_section(a: ScenarioAnalysis) -> list[str]:
    md: list[str] = [
        "---",
        "",
        "## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues",
        "",
    ]
    if a.kueue_configuration is not None:
        md.extend(
            [
                "### 5.0 Kueue Controller `Configuration` (`infra.json`)",
                "",
                "When replaying this scenario, `testrunner.py` applies the following declarative Kueue `Configuration` "
                "statement into `ConfigMap/kueue-manager-config` when deploying the `kueue-optimizer` chamber:",
                "",
                "```yaml",
                to_clean_yaml(a.kueue_configuration),
                "```",
                "",
            ]
        )
    md.extend(
        [
            "### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding",
            "",
        ]
    )

    if a.flavors_by_name:
        md.append("| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |")
        md.append("| :--- | :--- | :--- | :--- |")
        for rf_name, rf_obj in sorted(a.flavors_by_name.items()):
            rf_spec = rf_obj.get("spec") or {}
            topo_ref = rf_spec.get("topologyName") or "*(none)*"
            n_lbls = rf_spec.get("nodeLabels") or {}
            n_lbl_str = ", ".join(f"`{k}={v}`" for k, v in sorted(n_lbls.items())) if n_lbls else "*(matches all nodes)*"
            taints = rf_spec.get("nodeTaints") or []
            tols = rf_spec.get("tolerations") or []
            tt_parts = []
            if taints:
                tt_parts.append(f"{len(taints)} taint(s)")
            if tols:
                tt_parts.append(f"{len(tols)} toleration(s)")
            tt_str = ", ".join(tt_parts) if tt_parts else "none"
            md.append(f"| `{rf_name}` | `{topo_ref}` | {n_lbl_str} | {tt_str} |")
        md.append("")
    else:
        md.append("*(No `ResourceFlavor` objects defined in `infra.json`.)*")
        md.append("")

    md.append("### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)")
    md.append("")
    if a.wpc_by_name:
        md.append("| Priority Class Name | Priority `value` | API Version | Description |")
        md.append("| :--- | ---: | :--- | :--- |")
        sorted_wpcs = sorted(
            a.wpc_by_name.values(),
            key=lambda w: (-int(w.get("value") or 0), str((w.get("metadata") or {}).get("name") or "")),
        )
        for w_obj in sorted_wpcs:
            wm = w_obj.get("metadata") or {}
            w_desc = str(w_obj.get("description") or "—")
            md.append(
                f"| `{wm.get('name', '?')}` | **`{w_obj.get('value', 0)}`** | `{w_obj.get('apiVersion', '')}` | {w_desc} |"
            )
        md.append("")
    else:
        md.append(
            "*(No explicit `WorkloadPriorityClass` objects defined in `infra.json`; all workloads run at default priority unless annotated.)*"
        )
        md.append("")

    md.append("### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing")
    md.append("")

    all_quota_resources: list[str] = []
    for cq_obj in a.cqs_by_name.values():
        nom, _, _, _ = extract_rg_totals(cq_obj.get("spec") or {})
        for rk in nom:
            if rk not in all_quota_resources:
                all_quota_resources.append(rk)
    for ch_obj in a.cohorts_by_name.values():
        nom, _, _, _ = extract_rg_totals(ch_obj.get("spec") or {})
        for rk in nom:
            if rk not in all_quota_resources:
                all_quota_resources.append(rk)
    quota_res_ordered = [rk for rk in ("cpu", "memory", "pods") if rk in all_quota_resources] + [
        rk for rk in sorted(all_quota_resources) if rk not in ("cpu", "memory", "pods")
    ]

    referenced_cohorts: set[str] = set()
    for cq_obj in a.cqs_by_name.values():
        c_spec = cq_obj.get("spec") or {}
        c_ref = str(c_spec.get("cohortName") or c_spec.get("cohort") or "").strip()
        if c_ref:
            referenced_cohorts.add(c_ref)

    all_cohort_names = sorted(set(a.cohorts_by_name.keys()) | referenced_cohorts)
    if all_cohort_names:
        md.append("#### Cohort Configuration")
        md.append("")
        md.append(
            "| Cohort Name | Explicit CRD in `infra.json` | Parent Cohort | Fair-Sharing Weight | Member ClusterQueues | Direct Cohort Remainder Quota |"
        )
        md.append("| :--- | :---: | :--- | :---: | :---: | :--- |")
        for ch_name in all_cohort_names:
            ch_obj = a.cohorts_by_name.get(ch_name)
            ch_spec = (ch_obj or {}).get("spec") or {}
            has_crd = "Yes" if ch_obj is not None else "Implicit (via CQ `spec.cohort`)"
            parent_c = str(ch_spec.get("parentName") or ch_spec.get("parent") or "*(root)*")
            fs_w = str(((ch_spec.get("fairSharing") or {}).get("weight")) or "1 (default)")
            member_cqs = [
                cq_n
                for cq_n, cq_o in sorted(a.cqs_by_name.items())
                if str((cq_o.get("spec") or {}).get("cohortName") or (cq_o.get("spec") or {}).get("cohort") or "") == ch_name
            ]
            ch_nom, _, _, _ = extract_rg_totals(ch_spec)
            rem_str = (
                ", ".join(f"`{rk}`: {fmt_quantity(v, rk)}" for rk, v in ch_nom.items())
                if ch_nom
                else "none (`0`)"
            )
            md.append(
                f"| `{ch_name}` | {has_crd} | `{parent_c}` | `{fs_w}` | "
                f"**{len(member_cqs)}** ({', '.join(f'`{c}`' for c in member_cqs) if member_cqs else 'none'}) | {rem_str} |"
            )
        md.append("")

    md.append("#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration")
    md.append("")
    if a.cqs_by_name:
        res_hdr = " | ".join(f"Nominal `{rk}`" for rk in quota_res_ordered)
        res_sep = " | ".join("---:" for _ in quota_res_ordered)
        md.append(
            f"| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | {res_hdr} | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |"
        )
        md.append(f"| :--- | :--- | :--- | :---: | :--- | {res_sep} | :--- | :--- |")

        total_nom_across_all: dict[str, float] = defaultdict(float)
        for ch_obj in a.cohorts_by_name.values():
            ch_nom, _, _, _ = extract_rg_totals(ch_obj.get("spec") or {})
            for rk, rv in ch_nom.items():
                total_nom_across_all[rk] += rv

        for cq_name, cq_obj in sorted(a.cqs_by_name.items()):
            cq_spec = cq_obj.get("spec") or {}
            c_ref = str(cq_spec.get("cohortName") or cq_spec.get("cohort") or "*(standalone)*")
            q_strat = str(cq_spec.get("queueingStrategy") or "BestEffortFIFO")
            fs_w = str(((cq_spec.get("fairSharing") or {}).get("weight")) or "1")
            nom, b_lim, l_lim, flvs = extract_rg_totals(cq_spec)
            for rk, rv in nom.items():
                total_nom_across_all[rk] += rv

            nom_cells = " | ".join(
                f"**{fmt_quantity(nom.get(rk, 0.0), rk)}**" for rk in quota_res_ordered
            )
            bl_parts = []
            if b_lim:
                bl_parts.append("borrow: " + ", ".join(f"`{k}={v}`" for k, v in sorted(b_lim.items())))
            if l_lim:
                bl_parts.append("lend: " + ", ".join(f"`{k}={v}`" for k, v in sorted(l_lim.items())))
            bl_str = "; ".join(bl_parts) if bl_parts else "unbounded (default)"

            pre = cq_spec.get("preemption") or {}
            w_cq = str(pre.get("withinClusterQueue") or "Never")
            rec = str(pre.get("reclaimWithinCohort") or "Never")
            bwc_obj = pre.get("borrowWithinCohort") or {}
            bwc = str(bwc_obj.get("policy") or "Never") if isinstance(bwc_obj, dict) else str(bwc_obj)
            pre_str = f"`{w_cq}` / `{rec}` / `{bwc}`"

            md.append(
                f"| `{cq_name}` | `{c_ref}` | `{q_strat}` | `{fs_w}` | "
                f"{', '.join(f'`{f}`' for f in flvs) if flvs else '—'} | {nom_cells} | {bl_str} | {pre_str} |"
            )

        tot_cells = []
        for rk in quota_res_ordered:
            t_val = total_nom_across_all.get(rk, 0.0)
            c_cap = a.cluster_cap.get(rk, 0.0)
            pct_s = f" ({100.0 * t_val / c_cap:.1f}% of cluster)" if c_cap > 0 else ""
            tot_cells.append(f"**{fmt_quantity(t_val, rk)}**{pct_s}")
        md.append(
            f"| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | {' | '.join(tot_cells)} | — | — |"
        )
        md.append("")
    else:
        md.append("*(No `ClusterQueue` objects defined in `infra.json`.)*")
        md.append("")

    md.append("### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)")
    md.append("")
    all_ns_names = sorted(
        set(a.namespaces_by_name.keys())
        | {ns for ns, _ in a.lqs_by_key.keys()}
        | {w.namespace for w in a.unadmitted_wfs}
    )
    md.append(
        f"- **Namespaces (`{len(all_ns_names)}`)**: "
        + (", ".join(f"`{ns}`" for ns in all_ns_names) if all_ns_names else "*(default)*")
    )
    md.append("")

    if a.lqs_by_key:
        wf_count_by_lq: Counter[tuple[str, str]] = Counter(
            (w.namespace, w.local_queue) for w in a.unadmitted_wfs
        )
        md.append(
            "| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |"
        )
        md.append("| :--- | :--- | :--- | :---: | :--- | ---: |")
        for (lq_ns, lq_name), lq_obj in sorted(a.lqs_by_key.items()):
            target_cq = str(((lq_obj.get("spec") or {}).get("clusterQueue")) or "")
            cq_exists = target_cq in a.cqs_by_name
            target_cohort = (
                str(
                    ((a.cqs_by_name[target_cq].get("spec") or {}).get("cohortName"))
                    or ((a.cqs_by_name[target_cq].get("spec") or {}).get("cohort"))
                    or "*(standalone)*"
                )
                if cq_exists
                else "*(missing CQ)*"
            )
            w_cnt = wf_count_by_lq.get((lq_ns, lq_name), 0)
            md.append(
                f"| `{lq_ns}` | `{lq_name}` | `{target_cq}` | {'Yes' if cq_exists else '**NO**'} | "
                f"`{target_cohort}` | **{w_cnt:,}** |"
            )
        undeclared_lqs = sorted(
            (ns, lq, cnt)
            for (ns, lq), cnt in wf_count_by_lq.items()
            if (ns, lq) not in a.lqs_by_key and cnt > 0
        )
        for u_ns, u_lq, u_cnt in undeclared_lqs:
            md.append(
                f"| `{u_ns}` | `{u_lq}` *(undeclared in `infra.json`)* | *(none)* | **NO** | — | **{u_cnt:,}** |"
            )
        md.append("")
    else:
        md.append("*(No `LocalQueue` objects defined in `infra.json`.)*")
        md.append("")
    return md


def _render_timeline_section(a: ScenarioAnalysis) -> list[str]:
    md: list[str] = ["---", "", "## 6. Scenario Timeline & Arrival Dynamics", ""]

    inter_arrivals = [
        a.unadmitted_wfs[i].arrival_epoch - a.unadmitted_wfs[i - 1].arrival_epoch
        for i in range(1, len(a.unadmitted_wfs))
    ]
    ia_stats = calc_summary_stats(inter_arrivals)
    arrival_rate_per_min = (
        (len(a.unadmitted_wfs) / (a.test_length_sec / 60.0)) if a.test_length_sec > 0 else 0.0
    )
    arrival_rate_per_hr = (
        (len(a.unadmitted_wfs) / (a.test_length_sec / 3600.0)) if a.test_length_sec > 0 else 0.0
    )

    md.append("| Timeline Metric | Value |")
    md.append("| :--- | :--- |")
    md.append(f"| **Infrastructure Bootstrap Timestamp ($t_0$)** | `{format_epoch_iso(a.t0_epoch)}` |")
    md.append(
        f"| **First Unadmitted Workload Arrival ($t_{{\\text{{first}}}}$)** | `{format_epoch_iso(a.t_first_unadmitted)}` "
        f"(offset `+{max(0.0, (a.t_first_unadmitted or 0.0) - (a.t0_epoch or 0.0)):.2f}s` from $t_0$) |"
    )
    md.append(
        f"| **Last Unadmitted Workload Arrival ($t_{{\\text{{last}}}}$)** | `{format_epoch_iso(a.t_last_unadmitted)}` "
        f"(offset `+{max(0.0, (a.t_last_unadmitted or 0.0) - (a.t0_epoch or 0.0)):.2f}s` from $t_0$) |"
    )
    md.append(
        f"| **Active Test Arrival Duration ($T_{{\\text{{test}}}} = t_{{\\text{{last}}}} - t_{{\\text{{first}}}}$)** | "
        f"**{format_duration_human(a.test_length_sec)}** |"
    )
    md.append(
        f"| **Unadmitted Workload Submission Count** | **{len(a.unadmitted_wfs):,}** workloads "
        f"(plus **{len(a.admitted_at_start_wfs):,}** pre-admitted at $t_0$) |"
    )
    if a.test_length_sec > 0:
        md.append(
            f"| **Mean Arrival Rate ($\\lambda$)** | **{arrival_rate_per_min:.2f} workloads/min** "
            f"(**{arrival_rate_per_hr:.1f} workloads/hour**) |"
        )
    if inter_arrivals:
        md.append(
            f"| **Inter-Arrival Time ($\\Delta t_i$) Distribution** | "
            f"`min={ia_stats['min']:.2f}s`, `P50={ia_stats['p50']:.2f}s`, `AVG={ia_stats['avg']:.2f}s`, "
            f"`P90={ia_stats['p90']:.2f}s`, `P99={ia_stats['p99']:.2f}s`, `max={ia_stats['max']:.2f}s` |"
        )
    md.append("")

    if a.unadmitted_wfs:
        base_t = a.t_first_unadmitted or 0.0
        rel_arrivals = [max(0.0, w.arrival_epoch - base_t) for w in a.unadmitted_wfs]
        md.append("```text")
        md.append(render_timeline_arrival_histogram(rel_arrivals, a.test_length_sec))
        md.append("```")
        md.append("")
    return md


def _render_workload_analysis_section(
    a: ScenarioAnalysis,
    plan_5m: dict[str, Any] | None,
    plan_10m: dict[str, Any] | None,
) -> list[str]:
    md: list[str] = [
        "---",
        "",
        "## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms",
        "",
        "### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings",
        "",
        (
            "For any resource $r \\in \\{\\text{accelerators}, \\text{cpu}, \\text{memory}\\}$ with physical cluster capacity $C_{\\text{cluster}}(r)$ "
            "and total offered resource-seconds $W(r) = \\sum_{w} \\left(\\text{size}_w(r) \\times \\text{duration}_w\\right)$, "
            "let $T_{100\\%}(r) = W(r) / C_{\\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:"
        ),
        "",
        (
            "$$\\text{Sat}_{\\text{window}}(r) = \\frac{W(r)}{T_{\\text{window}} \\times C_{\\text{cluster}}(r)} \\times 100\\% = \\frac{T_{100\\%}(r)}{T_{\\text{window}}} \\times 100\\%, "
            "\\qquad N_{100\\%}(T_{\\text{target}}) = \\frac{T_{100\\%}(r^*)}{T_{\\text{target}}}$$"
        ),
        "",
        "#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\\text{test}}$) vs. Default `6h` (`21,600s`) Duration",
        "",
        (
            f"| Resource ($r$) | Physical Cluster Capacity $C_{{\\text{{cluster}}}}(r)$ | Total Offered Work ($1.0\\text{{x}}$) | Ideal `100%`-Util Service Time $T_{{100\\%}}(r)$ | (1) Saturation over Arrival Window ($T_{{\\text{{test}}}} = {format_duration_short(a.test_length_sec)}$) | (2) Saturation over Default `6h` ($21{{,}}600\\text{{s}}$) |"
        ),
        "| :--- | ---: | ---: | ---: | ---: | ---: |",
    ]

    sat_summary_res_keys: list[str] = []
    if a.primary_accel_resource and a.primary_accel_resource in a.cluster_cap:
        sat_summary_res_keys.append(a.primary_accel_resource)
    for extra_ar in a.accel_resources:
        if extra_ar in a.cluster_cap and extra_ar not in sat_summary_res_keys:
            sat_summary_res_keys.append(extra_ar)
    for extra_rk in ("cpu", "memory"):
        if extra_rk in a.cluster_cap and extra_rk not in sat_summary_res_keys:
            sat_summary_res_keys.append(extra_rk)

    for rk in sat_summary_res_keys:
        c_val = a.cluster_cap.get(rk, 0.0)
        w_sec = a.offered_resource_seconds(a.unadmitted_wfs, rk)
        w_hrs = w_sec / 3600.0
        t100_rk = a.ideal_full_capacity_sec(a.unadmitted_wfs, rk)
        s_arr = a.saturation_over_window_pct(a.unadmitted_wfs, rk, a.test_length_sec)
        s_6h = a.saturation_over_window_pct(a.unadmitted_wfs, rk, DEFAULT_SIX_HOUR_SEC)
        is_dom = rk == a.primary_sat_resource
        rk_disp = f"**`{rk}`** *(bottleneck)*" if is_dom else f"`{rk}`"
        unit_hr = f"`{rk}`-h" if rk not in ("cpu", "memory") else ("core-h" if rk == "cpu" else "GiB-h")
        s_arr_str = (
            f"**{s_arr:.2f}%**"
            if (s_arr is not None and is_dom)
            else (f"{s_arr:.2f}%" if s_arr is not None else "n/a ($T_{\\text{test}}=0$)")
        )
        s_6h_str = (
            f"**{s_6h:.2f}%**"
            if (s_6h is not None and is_dom)
            else (f"{s_6h:.2f}%" if s_6h is not None else "n/a")
        )
        t100_str = format_duration_human(t100_rk) if t100_rk is not None else "n/a"
        md.append(
            f"| {rk_disp} | {fmt_quantity(c_val, rk)} | `{w_hrs:,.2f}` {unit_hr} | `{t100_str}` | {s_arr_str} | {s_6h_str} |"
        )
    md.append("")

    md.append("#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)")
    md.append("")
    if plan_5m and plan_10m:
        md.append(
            "| Target Wall-Clock Window ($T_{\\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\\text{test}} / N_{100\\%}$) | Execution & Drain Behavior |"
        )
        md.append("| :--- | :---: | :---: | :---: | :---: | :--- |")
        for plan in (plan_5m, plan_10m):
            md.append(
                f"| **`{plan['target_label']}` (`{int(plan['target_sec'])}s`)** | `{a.primary_sat_resource}` | **`--speedup {plan['speedup_str']}`** | **`--duration {plan['rec_duration_flag']}`** | `{plan['comp_arrival_sec']:.1f}s` | {plan['note']} |"
            )
        md.append("")
        md.append("```bash")
        md.append(
            f"# Run at 100% offered {a.primary_sat_resource} saturation in a 5-minute (300s) wall-clock window:"
        )
        md.append(
            f"python3 deploy/scripts/testrunner.py --scenario {a.rel_dir} --speedup {plan_5m['speedup_str']} --duration {plan_5m['rec_duration_flag']}"
        )
        md.append("")
        md.append(
            f"# Run at 100% offered {a.primary_sat_resource} saturation in a 10-minute (600s) wall-clock window:"
        )
        md.append(
            f"python3 deploy/scripts/testrunner.py --scenario {a.rel_dir} --speedup {plan_10m['speedup_str']} --duration {plan_10m['rec_duration_flag']}"
        )
        md.append("```")
        md.append("")

    md.append("### 7.1 Workload Breakdown & Cluster Saturation by Priority Class")
    md.append("")

    wfs_by_prio: dict[str, list[ParsedWorkload]] = defaultdict(list)
    for w in a.unadmitted_wfs:
        wfs_by_prio[w.priority_class].append(w)

    ordered_priorities = sorted(
        wfs_by_prio.keys(),
        key=lambda p: (-a.priority_value_by_name.get(p, 0), p),
    )

    has_accel = bool(a.primary_accel_resource and a.total_wf_accel > 0)
    prim_sat = a.primary_sat_resource

    def _offered_col_hdr(rk: str, *, short: bool = False) -> str:
        if rk == "cpu":
            return "Offered Core-Hours"
        if rk == "memory":
            return "Offered GiB-h" if short else "Offered GiB-Hours"
        return f"Offered `{rk}`-h" if short else f"Offered `{rk}`-Hours"

    def _fmt_sat_cell(val: float | None, *, bold: bool) -> str:
        if val is None:
            return "n/a"
        return f"**{val:.2f}%**" if bold else f"{val:.2f}%"

    if has_accel:
        md.append(
            f"| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | {_offered_col_hdr(prim_sat)} | `{a.primary_accel_resource}` Saturation | `cpu` Saturation | `memory` Saturation |"
        )
        md.append("| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |")
    else:
        md.append(
            f"| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | {_offered_col_hdr(prim_sat)} | `cpu` Saturation | `memory` Saturation |"
        )
        md.append("| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: |")

    def _format_prio_row(label_str: str, prio_val_str: str, subset: list[ParsedWorkload], bold_total: bool = False) -> str:
        w_cnt = len(subset)
        w_share = (100.0 * w_cnt / len(a.unadmitted_wfs)) if a.unadmitted_wfs else 0.0
        p_st = calc_summary_stats([float(w.total_pods) for w in subset])
        d_st = calc_summary_stats([w.duration_sec for w in subset])
        gang_str = f"`{int(p_st['min'])} / {int(p_st['p50'])} / {int(p_st['p90'])} / {int(p_st['max'])}`"
        dur_str = f"`{d_st['p50']:.0f}s / {d_st['avg']:.0f}s / {d_st['p90']:.0f}s / {d_st['p99']:.0f}s`"
        prim_hrs = sum(w.total_requests.get(prim_sat, 0.0) * w.duration_sec for w in subset) / 3600.0
        c_sat = a.saturation_pct(subset, "cpu")
        m_sat = a.saturation_pct(subset, "memory")
        c_sat_s = _fmt_sat_cell(c_sat, bold=(prim_sat == "cpu" or bold_total))
        m_sat_s = _fmt_sat_cell(m_sat, bold=(prim_sat == "memory" or bold_total))

        if has_accel and a.primary_accel_resource is not None:
            a_sat = a.saturation_pct(subset, a.primary_accel_resource)
            a_sat_s = _fmt_sat_cell(a_sat, bold=(prim_sat == a.primary_accel_resource or bold_total))
            return (
                f"| {label_str} | {prio_val_str} | {'**' if bold_total else ''}{w_cnt:,}{'**' if bold_total else ''} | "
                f"{w_share:.1f}% | {int(p_st['sum']):,} | {gang_str} | {dur_str} | "
                f"{prim_hrs:,.1f} | {a_sat_s} | {c_sat_s} | {m_sat_s} |"
            )
        return (
            f"| {label_str} | {prio_val_str} | {'**' if bold_total else ''}{w_cnt:,}{'**' if bold_total else ''} | "
            f"{w_share:.1f}% | {int(p_st['sum']):,} | {gang_str} | {dur_str} | "
            f"{prim_hrs:,.1f} | {c_sat_s} | {m_sat_s} |"
        )

    for p_name in ordered_priorities:
        p_val = a.priority_value_by_name.get(p_name, 0)
        md.append(_format_prio_row(f"`{p_name}`", f"`{p_val}`", wfs_by_prio[p_name], bold_total=False))
    if a.unadmitted_wfs:
        md.append(_format_prio_row("**ALL (Total)**", "—", a.unadmitted_wfs, bold_total=True))
    md.append("")

    md.append("### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)")
    md.append("")

    wfs_by_lq: dict[tuple[str, str], list[ParsedWorkload]] = defaultdict(list)
    for w in a.unadmitted_wfs:
        wfs_by_lq[(w.namespace, w.local_queue)].append(w)

    ordered_lq_keys = sorted(
        set(a.lqs_by_key.keys()) | set(wfs_by_lq.keys()),
        key=lambda k: (-len(wfs_by_lq.get(k, [])), k[0], k[1]),
    )

    lq_cq_sat_hdr = (
        f"Target CQ Nominal `{prim_sat}` Saturation"
        if prim_sat in ("cpu", "memory")
        else "Target CQ Nominal Quota Saturation"
    )
    md.append(
        f"| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | {_offered_col_hdr(prim_sat, short=True)} | Cluster `{prim_sat}` Saturation | {lq_cq_sat_hdr} | Priority Breakdown (Count) |"
    )
    md.append("| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |")

    for lq_ns, lq_name in ordered_lq_keys:
        subset = wfs_by_lq.get((lq_ns, lq_name), [])
        lq_obj = a.lqs_by_key.get((lq_ns, lq_name))
        target_cq = str(((lq_obj or {}).get("spec") or {}).get("clusterQueue") or "*(undeclared)*")
        cq_obj = a.cqs_by_name.get(target_cq)
        cq_nom, _, _, _ = extract_rg_totals((cq_obj or {}).get("spec") or {})

        w_cnt = len(subset)
        w_share = (100.0 * w_cnt / len(a.unadmitted_wfs)) if a.unadmitted_wfs else 0.0
        p_st = calc_summary_stats([float(w.total_pods) for w in subset])
        d_st = calc_summary_stats([w.duration_sec for w in subset])
        dur_str = (
            f"`{d_st['p50']:.0f}s / {d_st['avg']:.0f}s / {d_st['p90']:.0f}s / {d_st['p99']:.0f}s`"
            if subset
            else "—"
        )
        prio_c = Counter(w.priority_class for w in subset)
        prio_brk = ", ".join(
            f"`{p}`:{prio_c[p]}" for p in ordered_priorities if prio_c.get(p, 0) > 0
        ) or "none"

        offered_sec = sum(w.total_requests.get(prim_sat, 0.0) * w.duration_sec for w in subset)
        cl_sat = a.saturation_pct(subset, prim_sat)
        cq_q_val = cq_nom.get(prim_sat, 0.0)
        cq_sat = (
            (100.0 * offered_sec / (a.test_length_sec * cq_q_val))
            if (a.test_length_sec > 0 and cq_q_val > 0)
            else None
        )
        md.append(
            f"| `{lq_ns}/{lq_name}` | `{target_cq}` | **{w_cnt:,}** | {w_share:.1f}% | "
            f"{int(p_st['sum']):,} | {dur_str} | {offered_sec / 3600.0:,.1f} | "
            f"{f'**{cl_sat:.2f}%**' if cl_sat is not None else 'n/a'} | "
            f"{f'**{cq_sat:.2f}%**' if cq_sat is not None else 'n/a'} | {prio_brk} |"
        )
    md.append("")

    if ordered_lq_keys and a.unadmitted_wfs:
        md.append("#### Detailed Per-`LocalQueue` × Priority Dissection")
        md.append("")
        if has_accel and a.primary_accel_resource is not None:
            if prim_sat == a.primary_accel_resource:
                det_col1, det_col2 = a.primary_accel_resource, "cpu"
            else:
                det_col1, det_col2 = prim_sat, a.primary_accel_resource
        else:
            det_col1, det_col2 = ("memory", "cpu") if prim_sat == "memory" else ("cpu", "memory")

        for lq_ns, lq_name in ordered_lq_keys:
            subset = wfs_by_lq.get((lq_ns, lq_name), [])
            if not subset:
                continue
            lq_obj = a.lqs_by_key.get((lq_ns, lq_name))
            target_cq = str(((lq_obj or {}).get("spec") or {}).get("clusterQueue") or "*(undeclared)*")
            md.append("<details>")
            md.append(
                f"<summary><b>LocalQueue <code>{lq_ns}/{lq_name}</code> → ClusterQueue <code>{target_cq}</code></b> ({len(subset):,} workloads)</summary>"
            )
            md.append("")
            md.append(
                f"| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `{det_col1}` Saturation | Cluster `{det_col2}` Saturation |"
            )
            md.append("| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |")

            for p_name in ordered_priorities:
                p_sub = [w for w in subset if w.priority_class == p_name]
                if not p_sub:
                    continue
                p_val = a.priority_value_by_name.get(p_name, 0)
                d_st = calc_summary_stats([w.duration_sec for w in p_sub])
                dur_s = f"`{d_st['p50']:.0f}s / {d_st['avg']:.0f}s / {d_st['p90']:.0f}s / {d_st['p99']:.0f}s`"
                sat1 = a.saturation_pct(p_sub, det_col1)
                sat2 = a.saturation_pct(p_sub, det_col2)
                md.append(
                    f"| `{p_name}` | `{p_val}` | **{len(p_sub):,}** | {100.0 * len(p_sub) / len(subset):.1f}% | "
                    f"{sum(w.total_pods for w in p_sub):,} | {dur_s} | "
                    f"{f'{sat1:.2f}%' if sat1 is not None else 'n/a'} | "
                    f"{f'{sat2:.2f}%' if sat2 is not None else 'n/a'} |"
                )
            md.append("")
            md.append("</details>")
            md.append("")

    md.append("### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)")
    md.append("")
    md.append("#### Aggregated Duration Histogram (All Unadmitted Workloads)")
    md.append("")
    md.append("```text")
    md.append(
        render_duration_histogram(
            "ALL PRIORITIES (Aggregated)",
            [w.duration_sec for w in a.unadmitted_wfs],
        )
    )
    md.append("```")
    md.append("")

    if ordered_priorities:
        md.append("#### Per-Priority Duration Histograms")
        md.append("")
        md.append("```text")
        hist_blocks: list[str] = []
        for p_name in ordered_priorities:
            p_val = a.priority_value_by_name.get(p_name, 0)
            p_durs = [w.duration_sec for w in wfs_by_prio[p_name]]
            hist_blocks.append(
                render_duration_histogram(
                    f"Priority: {p_name} (value={p_val})",
                    p_durs,
                )
            )
        md.append("\n\n".join(hist_blocks))
        md.append("```")
        md.append("")
    return md


def render_scenario_readme(analysis: ScenarioAnalysis) -> str:
    """Render a full scenario README.md document from a precomputed ScenarioAnalysis."""
    script_path = SCRIPT_DIR / "generate_scenario_readme.py"
    rel_script = os.path.relpath(script_path, start=analysis.scenario_dir)
    plan_5m = analysis.build_target_window_plan(300.0, "5m")
    plan_10m = analysis.build_target_window_plan(600.0, "10m")

    md: list[str] = [
        f"# Test Scenario Summary: `{analysis.scenario_title}`",
        "",
        (
            f"> **Auto-generated scenario analysis** from [`{analysis.rel_dir}/infra.json`](./infra.json), "
            f"[`{analysis.rel_dir}/workflows.json`](./workflows.json), and [`{analysis.rel_dir}/execution-time.json`](./execution-time.json) "
            f"via [`deploy/scripts/generate_scenario_readme.py`]({rel_script})."
        ),
        "",
    ]
    md.extend(_render_overview_section(analysis, plan_5m, plan_10m))
    md.extend(_render_nodes_section(analysis))
    md.extend(_render_topology_section(analysis))
    md.extend(_render_initial_pods_section(analysis))
    md.extend(_render_kueue_setup_section(analysis))
    md.extend(_render_timeline_section(analysis))
    md.extend(_render_workload_analysis_section(analysis, plan_5m, plan_10m))
    return "\n".join(md).rstrip() + "\n"


def generate_readme_for_scenario(scenario_dir: Path) -> str:
    """Analyze `infra.json`, `workflows.json`, and `execution-time.json` in `scenario_dir` and return Markdown."""
    return render_scenario_readme(analyze_scenario(scenario_dir))
