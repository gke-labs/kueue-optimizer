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


@dataclass(frozen=True)
class PodSetSlice:
    """Resource footprint and optional configured duration for one PodSet of a Workload."""

    name: str
    pod_count: int
    resources: dict[str, float]
    cfg_dur_sec: float | None = None
    is_infinite: bool = False


@dataclass(frozen=True)
class WorkloadSaturationClassification:
    """Pure Section 5.4 classification and resource-second contribution for a single Workload."""

    outcome: str  # "out_of_window" | "completed" | "active_at_end" | "excluded_interrupted" | "pending"
    pod_count: int
    eff_res_sec: dict[str, float] = field(default_factory=dict)
    full_res_sec: dict[str, float] = field(default_factory=dict)


def _pod_matches_podset(pod: dict, ps_name: str) -> bool:
    if not ps_name:
        return False
    p_meta = pod.get("metadata") or {}
    p_labels = p_meta.get("labels") or {}
    p_anns = p_meta.get("annotations") or {}
    return ps_name in (
        str(p_anns.get("kueue.x-k8s.io/role-hash") or ""),
        str(p_labels.get("jobset.sigs.k8s.io/replicatedjob-name") or ""),
        str(p_labels.get("kueue.x-k8s.io/podset") or ""),
    )


def extract_workload_podset_slices(
    wl: dict,
    assoc_pods: list[dict] | None = None,
) -> list[PodSetSlice]:
    """Extract per-PodSet resource footprints and configured durations for a Kueue Workload.

    Handles:
      - Per-PodSet `kueue-optimizer.gke.io/pod-execution-time-seconds` annotations (so short and long
        PodSets are credited independently rather than all using the maximum duration).
      - Authoritative partial-admission counts and `resourceUsage` from `status.admission.podSetAssignments`
        when assignments map 1-to-1 to `spec.podSets`.
      - Zero-count PodSets (`count: 0`), which contribute 0 pods and 0 resources.
      - Supplementation from instantiated Kubernetes Pods when resources or durations appear only on Pods.
    """
    meta = wl.get("metadata") or {}
    spec = wl.get("spec") or {}
    valid_pod_sets = [ps for ps in (spec.get("podSets") or []) if isinstance(ps, dict)]
    admission = (wl.get("status") or {}).get("admission") or {}
    psa_list = [psa for psa in (admission.get("podSetAssignments") or []) if isinstance(psa, dict)]

    wl_anns = meta.get("annotations") or {}
    wl_is_inf = str(wl_anns.get("kueue-optimizer.gke.io/infinite-duration") or "").lower() == "true"
    wl_dur = (
        None
        if wl_is_inf
        else parse_positive_duration_annotation(wl_anns, "kueue-optimizer.gke.io/pod-execution-time-seconds")
    )

    psa_by_name: dict[str, dict] = {
        str(psa["name"]): psa for psa in psa_list if psa.get("name")
    }
    ps_names = [str(ps.get("name") or "") for ps in valid_pod_sets]
    is_1to1_psa = bool(psa_list) and len(psa_list) == len(valid_pod_sets) and (
        (len(psa_by_name) == len(psa_list) and all(n in psa_by_name for n in ps_names))
        or len(valid_pod_sets) == 1
    )

    slices: list[PodSetSlice] = []
    for idx, ps in enumerate(valid_pod_sets):
        ps_name = ps_names[idx] or f"podset-{idx}"
        raw_ps_cnt = ps.get("count")
        ps_spec_count = max(0, int(raw_ps_cnt)) if raw_ps_cnt is not None else 1

        matched_psa: dict | None = None
        if is_1to1_psa:
            matched_psa = psa_by_name.get(ps_names[idx]) if ps_names[idx] in psa_by_name else psa_list[idx]

        eff_count = ps_spec_count
        if matched_psa is not None and matched_psa.get("count") is not None:
            try:
                eff_count = max(0, int(matched_psa["count"]))
            except (ValueError, TypeError):
                pass

        tpl = ps.get("template") or {}
        tpl_meta = tpl.get("metadata") or {}
        tpl_anns = tpl_meta.get("annotations") or {}
        is_ps_inf = (
            str(tpl_anns.get("kueue-optimizer.gke.io/infinite-duration") or "").lower() == "true"
            or (wl_is_inf and "kueue-optimizer.gke.io/pod-execution-time-seconds" not in tpl_anns)
        )
        ps_dur = (
            None
            if is_ps_inf
            else (
                parse_positive_duration_annotation(
                    tpl_anns, "kueue-optimizer.gke.io/pod-execution-time-seconds"
                )
                or wl_dur
            )
        )

        ps_res: dict[str, float] = {}
        if eff_count > 0:
            per_pod = extract_pod_spec_resources(tpl.get("spec"))
            for rk, rv in per_pod.items():
                ps_res[rk] = rv * eff_count
            if matched_psa is not None:
                for rk, rv in (matched_psa.get("resourceUsage") or {}).items():
                    if rk == "ephemeral-storage":
                        continue
                    ps_res[rk] = parse_k8s_quantity(rv, rk)
            ps_res["pods"] = float(eff_count)

        slices.append(
            PodSetSlice(
                name=ps_name,
                pod_count=eff_count,
                resources=ps_res,
                cfg_dur_sec=ps_dur,
                is_infinite=is_ps_inf,
            )
        )

    # When status.admission.podSetAssignments does not map 1-to-1 to spec.podSets (e.g., synthetic fixtures
    # or omitted spec.podSets), fold authoritative PSA resourceUsage / pod counts safely without dropping
    # any template resources.
    if psa_list and not is_1to1_psa:
        psa_totals: dict[str, float] = {}
        psa_pod_count = 0
        for psa in psa_list:
            if psa.get("count") is not None:
                try:
                    psa_pod_count += max(0, int(psa["count"]))
                except (ValueError, TypeError):
                    pass
            for rk, rv in (psa.get("resourceUsage") or {}).items():
                if rk == "ephemeral-storage":
                    continue
                psa_totals[rk] = psa_totals.get(rk, 0.0) + parse_k8s_quantity(rv, rk)

        if not slices:
            if psa_pod_count > 0:
                psa_totals["pods"] = float(psa_pod_count)
            slices.append(
                PodSetSlice(
                    name="admission",
                    pod_count=psa_pod_count,
                    resources=psa_totals,
                    cfg_dur_sec=wl_dur,
                    is_infinite=wl_is_inf,
                )
            )
        else:
            existing_totals: dict[str, float] = {}
            total_pods_cur = sum(s.pod_count for s in slices)
            for s in slices:
                for rk, rv in s.resources.items():
                    existing_totals[rk] = existing_totals.get(rk, 0.0) + rv
            extra_by_rk = {
                rk: rv - existing_totals.get(rk, 0.0)
                for rk, rv in psa_totals.items()
                if rv > existing_totals.get(rk, 0.0)
            }
            if extra_by_rk or psa_pod_count > total_pods_cur:
                updated_slices: list[PodSetSlice] = []
                weight_denom = float(total_pods_cur) if total_pods_cur > 0 else float(len(slices))
                for idx, s in enumerate(slices):
                    new_res = dict(s.resources)
                    weight = (float(s.pod_count) / weight_denom) if total_pods_cur > 0 else (1.0 / weight_denom)
                    for rk, delta in extra_by_rk.items():
                        new_res[rk] = new_res.get(rk, 0.0) + delta * weight
                    new_cnt = s.pod_count
                    if idx == 0 and psa_pod_count > total_pods_cur:
                        new_cnt += psa_pod_count - total_pods_cur
                        new_res["pods"] = float(new_cnt)
                    updated_slices.append(
                        PodSetSlice(
                            name=s.name,
                            pod_count=new_cnt,
                            resources=new_res,
                            cfg_dur_sec=s.cfg_dur_sec,
                            is_infinite=s.is_infinite,
                        )
                    )
                slices = updated_slices

    # Supplement from instantiated Kubernetes Pod objects if resources or durations were only on Pods.
    valid_pods = [p for p in (assoc_pods or []) if isinstance(p, dict)]
    if valid_pods:
        if not slices:
            pod_res_totals: dict[str, float] = {"pods": float(len(valid_pods))}
            pod_durs: list[float] = []
            for pod in valid_pods:
                p_anns = (pod.get("metadata") or {}).get("annotations") or {}
                if str(p_anns.get("kueue-optimizer.gke.io/infinite-duration") or "").lower() != "true":
                    p_dur = parse_positive_duration_annotation(
                        p_anns, "kueue-optimizer.gke.io/pod-execution-time-seconds"
                    )
                    if p_dur is not None:
                        pod_durs.append(p_dur)
                for rk, rv in extract_pod_spec_resources(pod.get("spec")).items():
                    pod_res_totals[rk] = pod_res_totals.get(rk, 0.0) + rv
            slices.append(
                PodSetSlice(
                    name="pods",
                    pod_count=len(valid_pods),
                    resources=pod_res_totals,
                    cfg_dur_sec=max(pod_durs) if pod_durs else wl_dur,
                    is_infinite=wl_is_inf and not pod_durs,
                )
            )
        else:
            existing_totals = {}
            total_pods = sum(s.pod_count for s in slices)
            for s in slices:
                for rk, rv in s.resources.items():
                    existing_totals[rk] = existing_totals.get(rk, 0.0) + rv

            pod_res_list = [extract_pod_spec_resources(p.get("spec")) for p in valid_pods]
            all_pod_keys = {rk for pr in pod_res_list for rk in pr}
            missing_key_totals: dict[str, float] = {}
            if total_pods > 0:
                for rk in all_pod_keys:
                    if existing_totals.get(rk, 0.0) > 0.0:
                        continue
                    if len(pod_res_list) == total_pods:
                        missing_key_totals[rk] = sum(pr.get(rk, 0.0) for pr in pod_res_list)
                    else:
                        missing_key_totals[rk] = max(pr.get(rk, 0.0) for pr in pod_res_list) * total_pods

            all_pod_durs = [
                d
                for p in valid_pods
                for p_anns in ((p.get("metadata") or {}).get("annotations") or {},)
                if str(p_anns.get("kueue-optimizer.gke.io/infinite-duration") or "").lower() != "true"
                for d in (
                    parse_positive_duration_annotation(p_anns, "kueue-optimizer.gke.io/pod-execution-time-seconds"),
                )
                if d is not None
            ]

            updated_slices = []
            for s in slices:
                new_res = dict(s.resources)
                if missing_key_totals and s.pod_count > 0 and total_pods > 0:
                    weight = float(s.pod_count) / float(total_pods)
                    for rk, total_v in missing_key_totals.items():
                        new_res[rk] = total_v * weight
                new_dur = s.cfg_dur_sec
                if new_dur is None and not s.is_infinite:
                    matched_durs = [
                        d
                        for p in valid_pods
                        if _pod_matches_podset(p, s.name)
                        for p_anns in ((p.get("metadata") or {}).get("annotations") or {},)
                        if str(p_anns.get("kueue-optimizer.gke.io/infinite-duration") or "").lower() != "true"
                        for d in (
                            parse_positive_duration_annotation(
                                p_anns, "kueue-optimizer.gke.io/pod-execution-time-seconds"
                            ),
                        )
                        if d is not None
                    ]
                    if matched_durs:
                        new_dur = max(matched_durs)
                    elif all_pod_durs and (len(slices) == 1 or len(set(all_pod_durs)) == 1):
                        new_dur = max(all_pod_durs)
                updated_slices.append(
                    PodSetSlice(
                        name=s.name,
                        pod_count=s.pod_count,
                        resources=new_res,
                        cfg_dur_sec=new_dur,
                        is_infinite=s.is_infinite,
                    )
                )
            slices = updated_slices

    return slices


def extract_workload_resource_size_and_cfg_duration(
    wl: dict,
    assoc_pods: list[dict] | None = None,
) -> tuple[dict[str, float], float | None]:
    """Extract total workload resource requests and maximum configured execution duration across PodSets."""
    slices = extract_workload_podset_slices(wl, assoc_pods=assoc_pods)
    totals: dict[str, float] = {}
    cfg_durs: list[float] = []
    for s in slices:
        for rk, rv in s.resources.items():
            totals[rk] = totals.get(rk, 0.0) + rv
        if s.cfg_dur_sec is not None:
            cfg_durs.append(s.cfg_dur_sec)
    if "pods" not in totals:
        totals["pods"] = float(sum(s.pod_count for s in slices))
    return totals, (max(cfg_durs) if cfg_durs else None)


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


def _parse_capacity_mapping(raw_cap: dict | None) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(raw_cap, dict):
        return out
    for rk, rv in raw_cap.items():
        if rk == "ephemeral-storage":
            continue
        if isinstance(rv, (int, float)) and not isinstance(rv, bool):
            val = float(rv)
        else:
            val = parse_k8s_quantity(rv, rk)
        if val > 0.0:
            out[str(rk)] = val
    return out


def _aggregate_cluster_capacity(nodes: list[dict]) -> dict[str, float]:
    cluster_cap: dict[str, float] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        st = node.get("status") or {}
        alloc = st.get("allocatable") or st.get("capacity") or {}
        for rk, rv in _parse_capacity_mapping(alloc).items():
            cluster_cap[rk] = cluster_cap.get(rk, 0.0) + rv
    return cluster_cap


def _integrate_cluster_capacity(
    nodes: list[dict],
    start_epoch: float,
    end_epoch: float,
    node_timeline: list[dict] | None = None,
) -> tuple[dict[str, float], dict[str, float]]:
    """Integrate physical cluster capacity (resource-seconds) over [start_epoch, end_epoch].

    Returns `(cap_res_sec, avg_cap)` where `avg_cap[rk] = cap_res_sec[rk] / eval_duration`.
    Uses piecewise-constant `node_timeline` when provided; otherwise integrates surviving `nodes`
    clipped to each node's `[creationTimestamp, deletionTimestamp]` overlap with `[start_epoch, end_epoch]`.
    """
    start_f = float(start_epoch)
    end_f = float(end_epoch)
    eval_duration = max(0.0, end_f - start_f)
    if eval_duration <= 0.0:
        return {}, {}

    cap_res_sec: dict[str, float] = {}
    parsed_timeline: list[tuple[float, dict[str, float]]] = []
    for entry in node_timeline or []:
        if not isinstance(entry, dict) or entry.get("epoch") is None:
            continue
        try:
            ep = float(entry["epoch"])
        except (TypeError, ValueError):
            continue
        raw_cap = entry.get("capacity") if entry.get("capacity") is not None else entry.get("allocatable")
        parsed_timeline.append((ep, _parse_capacity_mapping(raw_cap)))

    if parsed_timeline:
        parsed_timeline.sort(key=lambda item: item[0])
        cur_cap: dict[str, float] = {}
        idx = 0
        while idx < len(parsed_timeline) and parsed_timeline[idx][0] <= start_f:
            cur_cap = parsed_timeline[idx][1]
            idx += 1
        prev_t = start_f
        while idx < len(parsed_timeline) and parsed_timeline[idx][0] < end_f:
            next_t, next_cap = parsed_timeline[idx]
            dt = max(0.0, next_t - prev_t)
            if dt > 0.0:
                for rk, rv in cur_cap.items():
                    cap_res_sec[rk] = cap_res_sec.get(rk, 0.0) + rv * dt
            cur_cap = next_cap
            prev_t = next_t
            idx += 1
        tail_dt = max(0.0, end_f - prev_t)
        if tail_dt > 0.0:
            for rk, rv in cur_cap.items():
                cap_res_sec[rk] = cap_res_sec.get(rk, 0.0) + rv * tail_dt
    else:
        for node in nodes:
            if not isinstance(node, dict):
                continue
            meta = node.get("metadata") or {}
            t_created = parse_rfc3339_epoch(meta.get("creationTimestamp"))
            t_deleted = parse_rfc3339_epoch(meta.get("deletionTimestamp"))
            n_start = max(start_f, t_created) if t_created is not None else start_f
            n_end = min(end_f, t_deleted) if t_deleted is not None else end_f
            n_dur = max(0.0, n_end - n_start)
            if n_dur <= 0.0:
                continue
            st = node.get("status") or {}
            alloc = st.get("allocatable") or st.get("capacity") or {}
            for rk, rv in _parse_capacity_mapping(alloc).items():
                cap_res_sec[rk] = cap_res_sec.get(rk, 0.0) + rv * n_dur

    avg_cap = {rk: val / eval_duration for rk, val in cap_res_sec.items() if val > 0.0}
    return cap_res_sec, avg_cap


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
            p_labels.get("kueue.x-k8s.io/pod-group-name"),
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


def classify_workload_for_saturation(
    wl: dict,
    assoc_pods: list[dict] | None,
    start_epoch: float,
    end_epoch: float,
) -> WorkloadSaturationClassification:
    """Classify one Kueue Workload and compute its per-PodSet effective/full resource-seconds."""
    meta = wl.get("metadata") or {}
    pods = assoc_pods or []
    eval_end_with_tol = float(end_epoch) + 2.0

    slices = extract_workload_podset_slices(wl, assoc_pods=pods)
    wl_pod_count = sum(s.pod_count for s in slices)

    pod_phases = [str((p.get("status") or {}).get("phase") or "") for p in pods]
    pod_start_epochs = [
        ts
        for p in pods
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
    is_failed_finish = any(
        tok in fin_reason or tok in fin_msg
        for tok in ("fail", "error", "timeout", "ownernotfound", "outofsync")
    )
    has_failed_pods = any(ph == "Failed" for ph in pod_phases) and not all(
        ph == "Succeeded" for ph in pod_phases if ph
    )

    is_evicted_true = bool(c_evicted and str(c_evicted.get("status")) == "True")
    is_preempted_true = bool(c_preempted and str(c_preempted.get("status")) == "True")
    # QuotaReserved=True without Admitted=True means AdmissionChecks are still pending (nothing ran yet).
    is_admitted_true = bool(c_admitted and str(c_admitted.get("status")) == "True")
    requeue_cnt = int(((st.get("requeueState") or {}).get("count")) or 0)

    t_admit = (
        parse_rfc3339_epoch((c_admitted or {}).get("lastTransitionTime"))
        or (min(pod_start_epochs) if pod_start_epochs else None)
        or (parse_rfc3339_epoch((c_quota or {}).get("lastTransitionTime")) if is_admitted_true else None)
        or parse_rfc3339_epoch(meta.get("creationTimestamp"))
        or float(start_epoch)
    )
    t_evicted = parse_rfc3339_epoch((c_evicted or {}).get("lastTransitionTime"))
    if requeue_cnt > 0 and t_evicted is not None and t_evicted > t_admit:
        t_admit = t_evicted

    t_finish = parse_rfc3339_epoch((c_finished or {}).get("lastTransitionTime")) or float(end_epoch)

    # Ignore out-of-window completions (finished at/before start_epoch or admitted at/after end_epoch)
    if is_finished_true and (t_finish <= float(start_epoch) or t_admit >= float(end_epoch)):
        return WorkloadSaturationClassification(outcome="out_of_window", pod_count=wl_pod_count)

    # Case 1: Fully completed (succeeded) during the evaluation window
    all_pods_succeeded = bool(pod_phases) and all(ph == "Succeeded" for ph in pod_phases)
    if (
        is_finished_true
        and not is_evicted_true
        and not is_preempted_true
        and not is_failed_finish
        and not has_failed_pods
        and t_finish <= eval_end_with_tol
    ) or (
        all_pods_succeeded
        and not is_evicted_true
        and not is_preempted_true
        and not is_failed_finish
    ):
        if t_finish <= float(start_epoch) or t_admit >= float(end_epoch):
            return WorkloadSaturationClassification(outcome="out_of_window", pod_count=wl_pod_count)
        eff_res_sec: dict[str, float] = {}
        full_res_sec: dict[str, float] = {}
        for ps in slices:
            if not ps.resources:
                continue
            d_eff, d_full = completed_effective_duration(
                t_admit, t_finish, start_epoch, end_epoch, ps.cfg_dur_sec
            )
            for rk, r_amt in ps.resources.items():
                eff_res_sec[rk] = eff_res_sec.get(rk, 0.0) + r_amt * d_eff
                full_res_sec[rk] = full_res_sec.get(rk, 0.0) + r_amt * d_full
        return WorkloadSaturationClassification(
            outcome="completed",
            pod_count=wl_pod_count,
            eff_res_sec=eff_res_sec,
            full_res_sec=full_res_sec,
        )

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
        effective_start = max(float(start_epoch), min(float(t_admit), float(end_epoch)))
        elapsed_uninterrupted = max(0.0, float(end_epoch) - effective_start)
        eff_res_sec = {}
        full_res_sec = {}
        for ps in slices:
            if not ps.resources:
                continue
            if ps.cfg_dur_sec is not None and ps.cfg_dur_sec > 0.0:
                d_eff = min(float(ps.cfg_dur_sec), elapsed_uninterrupted)
                d_full = float(ps.cfg_dur_sec)
            else:
                d_eff = elapsed_uninterrupted
                d_full = elapsed_uninterrupted
            for rk, r_amt in ps.resources.items():
                eff_res_sec[rk] = eff_res_sec.get(rk, 0.0) + r_amt * d_eff
                full_res_sec[rk] = full_res_sec.get(rk, 0.0) + r_amt * d_full
        return WorkloadSaturationClassification(
            outcome="active_at_end",
            pod_count=wl_pod_count,
            eff_res_sec=eff_res_sec,
            full_res_sec=full_res_sec,
        )

    # Case 3: Interrupted by preemption/reclaim/eviction/failure vs unadmitted/pending
    is_resubmitted_t0_preemption = str(
        (meta.get("annotations") or {}).get("kueue-optimizer.gke.io/preempted-t0-resubmit") or ""
    ).lower() == "true" or any(
        isinstance(ps, dict)
        and str(
            (((ps.get("template") or {}).get("metadata") or {}).get("annotations") or {}).get(
                "kueue-optimizer.gke.io/preempted-t0-resubmit"
            )
            or ""
        ).lower()
        == "true"
        for ps in ((wl.get("spec") or {}).get("podSets") or [])
    )
    was_interrupted = (
        is_evicted_true
        or is_preempted_true
        or is_failed_finish
        or has_failed_pods
        or requeue_cnt > 0
        or is_resubmitted_t0_preemption
    )
    return WorkloadSaturationClassification(
        outcome="excluded_interrupted" if was_interrupted else "pending",
        pod_count=wl_pod_count,
    )


def _accumulate_kueue_workloads(
    acc: _SaturationAccumulator,
    workloads: list[dict],
    pods_by_wl_key: dict[tuple[str, str], list[dict]],
    start_epoch: float,
    end_epoch: float,
) -> None:
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

        result = classify_workload_for_saturation(wl, assoc_pods, start_epoch, end_epoch)
        if result.outcome == "out_of_window":
            continue
        if result.outcome == "completed":
            acc.cnt_completed += 1
            acc.pods_completed += result.pod_count
            for rk, val in result.eff_res_sec.items():
                acc.eff_res_sec_completed[rk] = acc.eff_res_sec_completed.get(rk, 0.0) + val
            for rk, val in result.full_res_sec.items():
                acc.full_dur_res_sec_qual[rk] = acc.full_dur_res_sec_qual.get(rk, 0.0) + val
        elif result.outcome == "active_at_end":
            acc.cnt_active_end += 1
            acc.pods_active_end += result.pod_count
            for rk, val in result.eff_res_sec.items():
                acc.eff_res_sec_active[rk] = acc.eff_res_sec_active.get(rk, 0.0) + val
            for rk, val in result.full_res_sec.items():
                acc.full_dur_res_sec_qual[rk] = acc.full_dur_res_sec_qual.get(rk, 0.0) + val
        elif result.outcome == "excluded_interrupted":
            acc.cnt_excluded_interrupted += 1
            acc.pods_excluded_interrupted += result.pod_count
        else:
            acc.cnt_pending += 1
            acc.pods_pending += result.pod_count


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
        p_anns = p_meta.get("annotations") or {}
        cfg_dur = (
            None
            if str(p_anns.get("kueue-optimizer.gke.io/infinite-duration") or "").lower() == "true"
            else parse_positive_duration_annotation(
                p_anns, "kueue-optimizer.gke.io/pod-execution-time-seconds"
            )
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
    cap_res_sec: dict[str, float],
    avg_cap: dict[str, float],
    acc: _SaturationAccumulator,
    eval_duration: float,
) -> tuple[list[list[str]], str]:
    accel_keys = sorted(
        rk for rk in cap_res_sec if rk not in ("cpu", "memory", "pods") and cap_res_sec.get(rk, 0.0) > 0.0
    )
    ordered_res = accel_keys + [
        rk for rk in ("cpu", "memory", "pods") if cap_res_sec.get(rk, 0.0) > 0.0
    ]

    rows: list[list[str]] = []
    for rk in ordered_res:
        denom = cap_res_sec.get(rk, 0.0)
        c_val = avg_cap.get(rk, 0.0)
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
    node_timeline = snapshot.get("nodeCapacityTimeline") or None
    workloads = snapshot.get("workloads") or []
    pods = snapshot.get("pods") or []
    if not nodes and not node_timeline:
        return [], ""

    cap_res_sec, avg_cap = _integrate_cluster_capacity(
        nodes, start_epoch, end_epoch, node_timeline=node_timeline
    )
    if not cap_res_sec:
        return [], ""

    pods_by_wl_key = _index_pods_by_owner(pods)
    acc = _SaturationAccumulator()
    _accumulate_kueue_workloads(acc, workloads, pods_by_wl_key, start_epoch, end_epoch)
    _accumulate_infra_pods(acc, pods, start_epoch, end_epoch)
    return _format_saturation_report(cap_res_sec, avg_cap, acc, eval_duration)
