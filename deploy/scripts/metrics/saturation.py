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

"""Section 5.4 physical cluster saturation accounting and test-end snapshot loading."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
import subprocess

from chamber.constants import ROOT_DIR
from chamber.parsers import (
    format_compact_si,
    format_resource_quantity,
    parse_k8s_quantity,
    parse_positive_duration_annotation,
    parse_rfc3339_epoch,
)
from chamber.scenario import extract_pod_spec_resources


SAT_SYSTEM_NAMESPACES: frozenset[str] = frozenset(
    {
        "kube-system",
        "kube-public",
        "kube-node-lease",
        "kueue-system",
        "jobset-system",
        "infra-pods",
    }
)


def load_or_fetch_cluster_snapshot(
    snapshot_json_path: str,
    chamber_namespace: str,
    kubeconfig_path: str = "",
) -> dict | None:
    """Load pre-captured test-end snapshot JSON, or fall back to live chamber kubectl query if available."""
    if snapshot_json_path:
        try:
            with open(snapshot_json_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict) and ("nodes" in data or "workloads" in data):
                return data
        except Exception:
            pass

    if kubeconfig_path:
        kc_path = kubeconfig_path if os.path.isfile(kubeconfig_path) else ""
    elif (
        chamber_namespace
        and "/" not in chamber_namespace
        and "\\" not in chamber_namespace
        and ".." not in chamber_namespace
    ):
        default_kc = os.path.join(str(ROOT_DIR), f"kwok-{chamber_namespace}.kubeconfig")
        kc_path = default_kc if os.path.isfile(default_kc) else ""
    else:
        kc_path = ""

    if not kc_path:
        return None

    try:
        env = dict(os.environ, KUBECONFIG=kc_path)
        nodes_proc = subprocess.run(
            ["kubectl", "get", "nodes", "-o", "json"],
            env=env,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        wl_proc = subprocess.run(
            ["kubectl", "get", "workloads.kueue.x-k8s.io", "-A", "-o", "json"],
            env=env,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        pods_proc = subprocess.run(
            ["kubectl", "get", "pods", "-A", "-o", "json"],
            env=env,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        if nodes_proc.returncode != 0:
            return None
        return {
            "nodes": (json.loads(nodes_proc.stdout) or {}).get("items") or [],
            "workloads": (
                (json.loads(wl_proc.stdout) or {}).get("items") or []
                if wl_proc.returncode == 0 and wl_proc.stdout
                else []
            ),
            "pods": (
                (json.loads(pods_proc.stdout) or {}).get("items") or []
                if pods_proc.returncode == 0 and pods_proc.stdout
                else []
            ),
        }
    except Exception:
        return None


def extract_workload_resource_size_and_cfg_duration(
    wl: dict,
    assoc_pods: list[dict] | None = None,
) -> tuple[dict[str, float], float | None]:
    """Extract total workload resource requests (summing per-pod resources * pod count across all podSets)
    and configured execution duration from a Kueue Workload (supplemented by status.admission & assoc_pods)."""
    meta = wl.get("metadata") or {}
    spec = wl.get("spec") or {}
    pod_sets = spec.get("podSets") or []
    totals: dict[str, float] = {}
    cfg_dur_candidates: list[float] = []

    wl_dur = parse_positive_duration_annotation(
        meta.get("annotations"), "kueue-optimizer.gke.io/pod-execution-time-seconds"
    )
    if wl_dur is not None:
        cfg_dur_candidates.append(wl_dur)

    for ps in pod_sets:
        if not isinstance(ps, dict):
            continue
        ps_count = max(1, int(ps.get("count") if ps.get("count") is not None else 1))
        tpl = ps.get("template") or {}
        tpl_meta = tpl.get("metadata") or {}
        ps_dur = parse_positive_duration_annotation(
            tpl_meta.get("annotations"), "kueue-optimizer.gke.io/pod-execution-time-seconds"
        )
        if ps_dur is not None:
            cfg_dur_candidates.append(ps_dur)

        per_pod = extract_pod_spec_resources(tpl.get("spec"))
        for rk, rv in per_pod.items():
            totals[rk] = totals.get(rk, 0.0) + rv * ps_count
        totals["pods"] = totals.get("pods", 0.0) + float(ps_count)

    # Reconcile with Kueue's authoritative status.admission.podSetAssignments across ALL podSets
    psa_totals: dict[str, float] = {}
    psa_pod_count = 0.0
    admission = (wl.get("status") or {}).get("admission") or {}
    for psa in admission.get("podSetAssignments") or []:
        if not isinstance(psa, dict):
            continue
        if psa.get("count") is not None:
            try:
                psa_pod_count += max(1.0, float(psa["count"]))
            except (ValueError, TypeError):
                pass
        r_usage = psa.get("resourceUsage") or {}
        for rk, rv in r_usage.items():
            if rk == "ephemeral-storage":
                continue
            psa_totals[rk] = psa_totals.get(rk, 0.0) + parse_k8s_quantity(rv, rk)

    for rk, rv in psa_totals.items():
        if rv > totals.get(rk, 0.0):
            totals[rk] = rv
    if psa_pod_count > totals.get("pods", 0.0):
        totals["pods"] = psa_pod_count

    # Supplement from instantiated Kubernetes Pod objects if any resource key or duration was only on Pods
    wl_pod_count = max(1, int(round(totals.get("pods", float(len(assoc_pods or [])) or 1.0))))
    totals["pods"] = float(wl_pod_count)
    if assoc_pods:
        pod_res_list: list[dict[str, float]] = []
        for pod in assoc_pods:
            if not isinstance(pod, dict):
                continue
            p_meta = pod.get("metadata") or {}
            p_dur = parse_positive_duration_annotation(
                p_meta.get("annotations"), "kueue-optimizer.gke.io/pod-execution-time-seconds"
            )
            if p_dur is not None:
                cfg_dur_candidates.append(p_dur)
            pod_res_list.append(extract_pod_spec_resources(pod.get("spec")))

        if pod_res_list:
            all_pod_keys = {rk for pr in pod_res_list for rk in pr}
            for rk in all_pod_keys:
                if totals.get(rk, 0.0) > 0.0:
                    continue
                if len(pod_res_list) == wl_pod_count:
                    totals[rk] = sum(pr.get(rk, 0.0) for pr in pod_res_list)
                else:
                    totals[rk] = max(pr.get(rk, 0.0) for pr in pod_res_list) * wl_pod_count

    cfg_dur = max(cfg_dur_candidates) if cfg_dur_candidates else None
    return totals, cfg_dur


def completed_effective_duration(
    t_start: float,
    t_finish: float,
    start_epoch: float,
    end_epoch: float,
    cfg_dur: float | None,
) -> tuple[float, float]:
    """Return (d_eff, d_full) for a completed workload/pod within [start_epoch, end_epoch]."""
    start_f = float(start_epoch)
    end_f = float(end_epoch)
    eval_duration = max(0.0, end_f - start_f)
    has_cfg = cfg_dur is not None and cfg_dur > 0.0

    # No overlap with [start_epoch, end_epoch] -> zero effective credit.
    if t_start >= end_f or t_finish <= start_f:
        return 0.0, (float(cfg_dur) if has_cfg else 0.0)

    in_window_wall = max(0.0, min(t_finish, end_f) - max(t_start, start_f))
    if not has_cfg:
        return in_window_wall, in_window_wall

    d_full = float(cfg_dur)
    max_possible = min(d_full, eval_duration)
    if in_window_wall <= 0.0:
        # Same-second RFC3339 timestamps inside the window: actual runtime is < 1s.
        d_eff = min(max_possible, 1.0) if t_finish >= t_start else 0.0
    elif t_start >= start_f and t_finish <= end_f and abs(in_window_wall - max_possible) <= 1.5:
        # Fully inside the window: snap 1s RFC3339 rounding jitter to configured duration.
        d_eff = max_possible
    else:
        d_eff = min(max_possible, in_window_wall)
    return d_eff, d_full


@dataclass
class _SaturationAccumulator:
    eff_res_sec_completed: dict[str, float] = field(default_factory=dict)
    eff_res_sec_active: dict[str, float] = field(default_factory=dict)
    full_dur_res_sec_qual: dict[str, float] = field(default_factory=dict)
    cnt_completed: int = 0
    cnt_active_end: int = 0
    cnt_excluded_interrupted: int = 0
    cnt_pending: int = 0
    pods_completed: int = 0
    pods_active_end: int = 0
    pods_excluded_interrupted: int = 0
    pods_pending: int = 0
    cnt_infra_completed: int = 0
    cnt_infra_active: int = 0


def _aggregate_cluster_capacity(nodes: list[dict]) -> dict[str, float]:
    cluster_cap: dict[str, float] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        st = node.get("status") or {}
        alloc = st.get("allocatable") or st.get("capacity") or {}
        for rk, rv in alloc.items():
            if rk == "ephemeral-storage":
                continue
            cluster_cap[rk] = cluster_cap.get(rk, 0.0) + parse_k8s_quantity(rv, rk)
    return cluster_cap


def _index_pods_by_owner(pods: list[dict]) -> dict[tuple[str, str], list[dict]]:
    pods_by_wl_key: dict[tuple[str, str], list[dict]] = {}
    for pod in pods:
        if not isinstance(pod, dict):
            continue
        p_meta = pod.get("metadata") or {}
        p_ns = str(p_meta.get("namespace") or "default")
        if p_ns in SAT_SYSTEM_NAMESPACES:
            continue
        p_labels = p_meta.get("labels") or {}
        p_anns = p_meta.get("annotations") or {}
        candidate_keys: set[str] = set()
        for k_name in (
            p_labels.get("kueue.x-k8s.io/prebuilt-workload-name"),
            p_anns.get("kueue.x-k8s.io/workload"),
            p_labels.get("jobset.sigs.k8s.io/jobset-name"),
            p_labels.get("batch.kubernetes.io/job-name"),
            p_labels.get("job-name"),
        ):
            if k_name:
                candidate_keys.add(str(k_name))
        for k_name in candidate_keys:
            pods_by_wl_key.setdefault((p_ns, k_name), []).append(pod)
    return pods_by_wl_key


def _accumulate_kueue_workloads(
    acc: _SaturationAccumulator,
    workloads: list[dict],
    pods_by_wl_key: dict[tuple[str, str], list[dict]],
    start_epoch: float,
    end_epoch: float,
) -> None:
    eval_end_with_tol = float(end_epoch) + 2.0

    # Identify top-level JobSet owners so any duplicate child Job Workloads for the same JobSet are skipped
    jobset_wl_owners: set[tuple[str, str]] = set()
    for wl in workloads:
        if not isinstance(wl, dict):
            continue
        meta = wl.get("metadata") or {}
        wl_ns = str(meta.get("namespace") or "default")
        for oref in meta.get("ownerReferences") or []:
            if isinstance(oref, dict) and str(oref.get("kind") or "") == "JobSet" and oref.get("name"):
                jobset_wl_owners.add((wl_ns, str(oref.get("name"))))

    for wl in workloads:
        if not isinstance(wl, dict):
            continue
        meta = wl.get("metadata") or {}
        wl_ns = str(meta.get("namespace") or "default")
        if wl_ns in SAT_SYSTEM_NAMESPACES:
            continue
        wl_name = str(meta.get("name") or "")
        orefs = [oref for oref in (meta.get("ownerReferences") or []) if isinstance(oref, dict)]
        owner_names = [str(oref.get("name") or "") for oref in orefs if oref.get("name")]

        # If a parent JobSet Workload already exists in this namespace, skip duplicate child Job Workloads
        if jobset_wl_owners and not any(str(o.get("kind") or "") == "JobSet" for o in orefs):
            parent_js: str | None = (meta.get("labels") or {}).get("jobset.sigs.k8s.io/jobset-name")
            if not parent_js:
                for ps in ((wl.get("spec") or {}).get("podSets") or []):
                    if isinstance(ps, dict):
                        parent_js = (((ps.get("template") or {}).get("metadata") or {}).get("labels") or {}).get(
                            "jobset.sigs.k8s.io/jobset-name"
                        )
                        if parent_js:
                            break
            if parent_js and (wl_ns, str(parent_js)) in jobset_wl_owners:
                continue

        raw_assoc_pods: list[dict] = list(pods_by_wl_key.get((wl_ns, wl_name), []))
        if not raw_assoc_pods:
            for oname in owner_names:
                if (wl_ns, oname) in pods_by_wl_key:
                    raw_assoc_pods.extend(pods_by_wl_key[(wl_ns, oname)])
                    break
        seen_pod_ids: set[str] = set()
        assoc_pods: list[dict] = []
        for p in raw_assoc_pods:
            pm = p.get("metadata") or {}
            pid = str(pm.get("uid") or pm.get("name") or id(p))
            if pid not in seen_pod_ids:
                seen_pod_ids.add(pid)
                assoc_pods.append(p)

        wl_sizes, cfg_dur = extract_workload_resource_size_and_cfg_duration(wl, assoc_pods=assoc_pods)
        wl_pod_count = int(round(wl_sizes.get("pods", 1.0)))

        pod_phases = [str((p.get("status") or {}).get("phase") or "") for p in assoc_pods]
        pod_start_epochs = [
            ts
            for p in assoc_pods
            for ts in (parse_rfc3339_epoch((p.get("status") or {}).get("startTime")),)
            if ts is not None
        ]

        st = wl.get("status") or {}
        conds = {
            str(c.get("type")): c
            for c in (st.get("conditions") or [])
            if isinstance(c, dict) and c.get("type")
        }
        c_finished = conds.get("Finished")
        c_admitted = conds.get("Admitted")
        c_quota = conds.get("QuotaReserved")
        c_evicted = conds.get("Evicted")
        c_preempted = conds.get("Preempted")

        is_finished_true = bool(c_finished and str(c_finished.get("status")) == "True")
        fin_reason = str((c_finished or {}).get("reason") or "").lower()
        fin_msg = str((c_finished or {}).get("message") or "").lower()
        is_failed_finish = any(tok in fin_reason or tok in fin_msg for tok in ("fail", "error", "timeout"))
        has_failed_pods = any(ph == "Failed" for ph in pod_phases) and not all(
            ph == "Succeeded" for ph in pod_phases if ph
        )

        is_evicted_true = bool(c_evicted and str(c_evicted.get("status")) == "True")
        is_preempted_true = bool(c_preempted and str(c_preempted.get("status")) == "True")
        is_admitted_true = bool(
            (c_admitted and str(c_admitted.get("status")) == "True")
            or (c_quota and str(c_quota.get("status")) == "True")
        )
        requeue_cnt = int(((st.get("requeueState") or {}).get("count")) or 0)

        t_admit = (
            parse_rfc3339_epoch((c_admitted or {}).get("lastTransitionTime"))
            or parse_rfc3339_epoch((c_quota or {}).get("lastTransitionTime"))
            or (min(pod_start_epochs) if pod_start_epochs else None)
            or parse_rfc3339_epoch(meta.get("creationTimestamp"))
            or float(start_epoch)
        )
        t_evicted = parse_rfc3339_epoch((c_evicted or {}).get("lastTransitionTime"))
        if requeue_cnt > 0 and t_evicted is not None and t_evicted > t_admit:
            t_admit = t_evicted

        t_finish = parse_rfc3339_epoch((c_finished or {}).get("lastTransitionTime")) or float(end_epoch)

        # Ignore out-of-window completions (finished at/before start_epoch or admitted at/after end_epoch)
        if is_finished_true and (t_finish <= float(start_epoch) or t_admit >= float(end_epoch)):
            continue

        # Case 1: Fully completed (succeeded) during the evaluation window
        all_pods_succeeded = bool(pod_phases) and all(ph == "Succeeded" for ph in pod_phases)
        if (is_finished_true and not is_failed_finish and not has_failed_pods and t_finish <= eval_end_with_tol) or (
            all_pods_succeeded and not is_evicted_true and not is_failed_finish
        ):
            if t_finish <= float(start_epoch) or t_admit >= float(end_epoch):
                continue
            acc.cnt_completed += 1
            acc.pods_completed += wl_pod_count
            d_eff, d_full = completed_effective_duration(t_admit, t_finish, start_epoch, end_epoch, cfg_dur)
            for rk, r_amt in wl_sizes.items():
                acc.eff_res_sec_completed[rk] = acc.eff_res_sec_completed.get(rk, 0.0) + r_amt * d_eff
                acc.full_dur_res_sec_qual[rk] = acc.full_dur_res_sec_qual.get(rk, 0.0) + r_amt * d_full
            continue

        # Case 2: Actively running uninterrupted at the very end of the test (t_eval = end_epoch)
        has_active_pods = (not pod_phases) or any(ph in ("Running", "Succeeded") for ph in pod_phases)
        if (
            is_admitted_true
            and not is_evicted_true
            and not is_preempted_true
            and not is_failed_finish
            and not has_failed_pods
            and has_active_pods
            and t_admit < float(end_epoch)
        ):
            acc.cnt_active_end += 1
            acc.pods_active_end += wl_pod_count
            effective_start = max(float(start_epoch), min(float(t_admit), float(end_epoch)))
            elapsed_uninterrupted = max(0.0, float(end_epoch) - effective_start)
            if cfg_dur is not None and cfg_dur > 0.0:
                d_eff = min(float(cfg_dur), elapsed_uninterrupted)
                d_full = float(cfg_dur)
            else:
                d_eff = elapsed_uninterrupted
                d_full = elapsed_uninterrupted
            for rk, r_amt in wl_sizes.items():
                acc.eff_res_sec_active[rk] = acc.eff_res_sec_active.get(rk, 0.0) + r_amt * d_eff
                acc.full_dur_res_sec_qual[rk] = acc.full_dur_res_sec_qual.get(rk, 0.0) + r_amt * d_full
            continue

        # Case 3: Interrupted by preemption/reclaim/eviction/failure vs never-admitted (Pending)
        has_admission_record = bool((st.get("admission") or {}).get("clusterQueue"))
        has_started_pods = any(ph in ("Running", "Succeeded", "Failed") for ph in pod_phases)
        was_ever_admitted_or_evicted = (
            is_admitted_true
            or has_admission_record
            or has_started_pods
            or is_evicted_true
            or is_preempted_true
            or is_failed_finish
            or has_failed_pods
            or requeue_cnt > 0
        )
        if was_ever_admitted_or_evicted:
            acc.cnt_excluded_interrupted += 1
            acc.pods_excluded_interrupted += wl_pod_count
        else:
            acc.cnt_pending += 1
            acc.pods_pending += wl_pod_count


def _accumulate_infra_pods(
    acc: _SaturationAccumulator,
    pods: list[dict],
    start_epoch: float,
    end_epoch: float,
) -> None:
    """Account for pre-existing t0 running pods in namespace 'infra-pods'."""
    for pod in pods:
        if not isinstance(pod, dict):
            continue
        p_meta = pod.get("metadata") or {}
        p_ns = str(p_meta.get("namespace") or "default")
        if p_ns != "infra-pods":
            continue
        p_spec = pod.get("spec") or {}
        p_status = pod.get("status") or {}
        per_pod_res = extract_pod_spec_resources(p_spec)
        if not per_pod_res:
            continue
        per_pod_res["pods"] = 1.0
        cfg_dur = parse_positive_duration_annotation(
            p_meta.get("annotations"), "kueue-optimizer.gke.io/pod-execution-time-seconds"
        )
        phase = str(p_status.get("phase") or "")
        t_start = (
            parse_rfc3339_epoch(p_status.get("startTime"))
            or parse_rfc3339_epoch(p_meta.get("creationTimestamp"))
            or float(start_epoch)
        )
        term_fin_epochs: list[float] = []
        for cs in p_status.get("containerStatuses") or []:
            if isinstance(cs, dict):
                t_fin_cs = parse_rfc3339_epoch((((cs.get("state") or {}).get("terminated") or {}).get("finishedAt")))
                if t_fin_cs is not None:
                    term_fin_epochs.append(t_fin_cs)
        t_pod_fin = max(term_fin_epochs) if term_fin_epochs else parse_rfc3339_epoch(p_meta.get("deletionTimestamp"))

        if phase == "Succeeded":
            if (t_pod_fin is not None and t_pod_fin <= float(start_epoch)) or t_start >= float(end_epoch):
                continue
            acc.cnt_infra_completed += 1
            t_pfin = t_pod_fin if t_pod_fin is not None else float(end_epoch)
            d_eff, d_full = completed_effective_duration(t_start, t_pfin, start_epoch, end_epoch, cfg_dur)
            for rk, r_amt in per_pod_res.items():
                acc.eff_res_sec_completed[rk] = acc.eff_res_sec_completed.get(rk, 0.0) + r_amt * d_eff
                acc.full_dur_res_sec_qual[rk] = acc.full_dur_res_sec_qual.get(rk, 0.0) + r_amt * d_full
        elif phase == "Running" and t_start < float(end_epoch):
            acc.cnt_infra_active += 1
            effective_start = max(float(start_epoch), min(float(t_start), float(end_epoch)))
            elapsed_uninterrupted = max(0.0, float(end_epoch) - effective_start)
            if cfg_dur is not None and cfg_dur > 0.0:
                d_eff = min(float(cfg_dur), elapsed_uninterrupted)
                d_full = float(cfg_dur)
            else:
                d_eff = elapsed_uninterrupted
                d_full = elapsed_uninterrupted
            for rk, r_amt in per_pod_res.items():
                acc.eff_res_sec_active[rk] = acc.eff_res_sec_active.get(rk, 0.0) + r_amt * d_eff
                acc.full_dur_res_sec_qual[rk] = acc.full_dur_res_sec_qual.get(rk, 0.0) + r_amt * d_full


def _format_saturation_report(
    cluster_cap: dict[str, float],
    acc: _SaturationAccumulator,
    eval_duration: float,
) -> tuple[list[list[str]], str]:
    accel_keys = sorted(
        rk for rk in cluster_cap if rk not in ("cpu", "memory", "pods") and cluster_cap.get(rk, 0.0) > 0.0
    )
    ordered_res = accel_keys + [
        rk for rk in ("cpu", "memory", "pods") if cluster_cap.get(rk, 0.0) > 0.0
    ]

    rows: list[list[str]] = []
    for rk in ordered_res:
        c_val = cluster_cap.get(rk, 0.0)
        denom = c_val * eval_duration
        if denom <= 0.0:
            continue
        comp_s = acc.eff_res_sec_completed.get(rk, 0.0)
        act_s = acc.eff_res_sec_active.get(rk, 0.0)
        tot_eff_s = comp_s + act_s
        full_s = acc.full_dur_res_sec_qual.get(rk, 0.0)

        eff_pct = (tot_eff_s / denom) * 100.0
        comp_pct = (comp_s / denom) * 100.0
        act_pct = (act_s / denom) * 100.0
        full_pct = (full_s / denom) * 100.0

        unit_lbl = "core-s" if rk == "cpu" else ("GiB-s" if rk == "memory" else f"{rk.split('/')[-1]}-s")
        rows.append(
            [
                rk,
                format_resource_quantity(c_val, rk),
                f"{eff_pct:.2f}%",
                f"{comp_pct:.2f}%",
                f"{act_pct:.2f}%",
                f"{full_pct:.2f}%",
                f"{format_compact_si(tot_eff_s)} / {format_compact_si(denom)} {unit_lbl}",
            ]
        )

    infra_part = (
        f" | Initial Infra Pods: {acc.cnt_infra_completed + acc.cnt_infra_active} "
        f"(completed={acc.cnt_infra_completed}, active_at_end={acc.cnt_infra_active})"
        if (acc.cnt_infra_completed + acc.cnt_infra_active) > 0
        else ""
    )
    unadmitted_str = (
        f"{acc.cnt_pending} [{acc.pods_pending} pods]"
        if acc.cnt_pending > 0 and acc.pods_pending > 0
        else str(acc.cnt_pending)
    )
    subtitle = (
        f"    Eval Window: {eval_duration:.1f}s | "
        f"Qualifying Workloads: {acc.cnt_completed + acc.cnt_active_end} "
        f"(completed={acc.cnt_completed} [{acc.pods_completed} pods], "
        f"active_at_end={acc.cnt_active_end} [{acc.pods_active_end} pods]){infra_part} | "
        f"Excluded (preempted/reclaimed/failed): {acc.cnt_excluded_interrupted} [{acc.pods_excluded_interrupted} pods] | "
        f"Unadmitted: {unadmitted_str}"
    )
    return rows, subtitle


def report_effective_uninterrupted_saturation(
    snapshot: dict | None,
    start_epoch: float,
    end_epoch: float,
) -> tuple[list[list[str]], str]:
    """Compute effective uninterrupted cluster saturation across all cluster resources.

    Qualifying workloads:
      1. Fully completed (succeeded) during the evaluation window [start_epoch, end_epoch], OR
      2. Actively running uninterrupted at the very end of the evaluation window (end_epoch).
    Excluded workloads (0 contribution):
      - Workloads that ran partially or stopped due to preemptions, cohort reclaims, evictions, or failures
        without completing and not actively running at end_epoch.
      - Unadmitted / pending workloads.
    """
    eval_duration = max(0.0, float(end_epoch) - float(start_epoch))
    if not snapshot or eval_duration <= 0.0:
        return [], ""

    nodes = snapshot.get("nodes") or []
    workloads = snapshot.get("workloads") or []
    pods = snapshot.get("pods") or []
    if not nodes:
        return [], ""

    cluster_cap = _aggregate_cluster_capacity(nodes)
    if not cluster_cap:
        return [], ""

    pods_by_wl_key = _index_pods_by_owner(pods)
    acc = _SaturationAccumulator()
    _accumulate_kueue_workloads(acc, workloads, pods_by_wl_key, start_epoch, end_epoch)
    _accumulate_infra_pods(acc, pods, start_epoch, end_epoch)
    return _format_saturation_report(cluster_cap, acc, eval_duration)
