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

from datetime import datetime, timezone
import heapq
import json
import signal
import time
from typing import Any

from chamber.constants import validate_dns_label
from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot, query_cluster_status
from chamber.parsers import format_duration_hms
from runner.loader import InitialPodSpec, InitialWorkloadPodGroup, ScenarioBundle, ScheduledWorkflowItem
from runner.validation import SYSTEM_NAMESPACES


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
        bootstrap_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        stamped_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for w in initial_workloads:
            st_copy = json.loads(json.dumps(w.workload_status))
            for cond in st_copy.get("conditions") or []:
                if isinstance(cond, dict):
                    cond["lastTransitionTime"] = bootstrap_iso
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

    initial_wl_by_key = {(w.namespace, w.workload_name): w for w in bundle.initial_workloads}
    initial_wl_keys = set(initial_wl_by_key.keys())
    finite_initial_wl_keys = {
        (w.namespace, w.workload_name)
        for w in bundle.initial_workloads
        if not all(p.is_infinite for p in w.pods)
    }
    resubmitted_initial_wls: set[tuple[str, str]] = set()

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
    poll_interval_sec = min(status_interval_sec, 0.5)
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
        if initial_wl_keys:
            finished_sched = len(snap.finished_workload_keys - initial_wl_keys)
            return max(finished_sched, snap.jobsets_completed + snap.jobsets_failed)
        return max(snap.workloads_finished, snap.jobsets_completed + snap.jobsets_failed)

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

    def _print_status_line(snap: ClusterStatusSnapshot, now_mono: float, note: str = "") -> None:
        wall_elapsed = now_mono - t0_mono
        sim_elapsed = wall_elapsed * speedup
        if event_heap:
            next_in = max(0.0, event_heap[0].target_wall_offset_sec - wall_elapsed)
            next_str = f"next_in={next_in:.2f}s"
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

            due_batch: list[ScheduledWorkflowItem] = []
            while event_heap and event_heap[0].target_wall_offset_sec <= (time.monotonic() - t0_mono):
                due_batch.append(heapq.heappop(event_heap))
                if len(due_batch) >= 100:
                    break

            if due_batch:
                mgr.apply_manifests_parallel([item.manifest for item in due_batch], max_workers=12)
                now_off = time.monotonic() - t0_mono
                for item in due_batch:
                    submitted_count += 1
                    total_pods_submitted += item.pod_count

                if len(due_batch) == 1:
                    it = due_batch[0]
                    src_tag = "exec-time.json" if it.indexed_from_execution_time else "fallback"
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
            need_fast_poll = (not event_heap) or bool(initial_wl_keys)
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

                if not event_heap:
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
                        and (finite_initial_wl_keys or (snap.pods_running == 0 and snap.workloads_admitted == 0))
                    ):
                        if not status_printed:
                            _print_status_line(snap, now_mono)
                        print("==> No pending workflows or running finite initial workloads remaining.", flush=True)
                        break

            now_mono = time.monotonic()
            wall_elapsed = now_mono - t0_mono
            wake_candidates = [next_status_mono - now_mono]
            if (not event_heap) or initial_wl_keys:
                wake_candidates.append(next_poll_mono - now_mono)
            if event_heap:
                wake_candidates.append(event_heap[0].target_wall_offset_sec - wall_elapsed)
            if max_run_seconds is not None and max_run_seconds > 0:
                wake_candidates.append(max_run_seconds - wall_elapsed)
            sleep_sec = max(0.01, min(wake_candidates))
            time.sleep(min(sleep_sec, 1.0))

    finally:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    end_epoch = time.time()
    return start_epoch, end_epoch, submitted_count, total_pods_submitted
