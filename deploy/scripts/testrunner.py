#!/usr/bin/env python3
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

"""kueue-optimizer Chamber Scraper Scenario Test Runner (`testrunner`).

Reads a scraped test case directory conforming to `SCRAPER-SCHEMA.md`
(`infra.json`, `workflows.json`, `execution-time.json` in JSON Lines format)
and replays the scenario inside a kueue-optimizer KWOK + JobSet + Kueue chamber:

1. Indexes the execution time of all workloads from `execution-time.json`
   BEFORE creating any workloads, ClusterQueues, LocalQueues, Nodes, or Pods.
2. Deploys (or reuses) a kueue-optimizer chamber and recreates the scraped cluster
   infrastructure from `infra.json` (`Node`s with exact capacity/allocatable,
   `Namespace`s, `Topology`, `ResourceFlavor`s, `Cohort`s, `ClusterQueue`s,
   and `LocalQueue`s).
3. Places any `Pod`s that were already running at the very beginning (`t0`)
   onto their recorded nodes (`spec.nodeName = payload.placement`) with their
   remaining execution time (`deletionTime - t0`, or a super-long / infinite
   duration if never deleted during the scrape).
4. Replays workflow objects (`JobSet`, `Job`, `LeaderWorkerSet`, `Deployment`,
   `RayJob`, `RayCluster`, standalone Kueue `Pod`) from `workflows.json` based
   on their timestamp offset from the beginning (`t0`):
   - Only creates top-level workflow objects that were not admitted yet
     (`workloadState == "NotAdmitted"`), or were admitted at `t0` and later
     preempted (`workloadState` transitioned to `"NotAdmitted"`).
   - Configures all pods of each created workload with the execution duration
     indexed from `execution-time.json` via the
     `kueue-optimizer.gke.io/pod-execution-time-seconds` annotation.
5. Watches workloads until completion, prints periodic status and optional
   Prometheus Kueue metrics report, and cleans up the chamber when requested.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys

from chamber_common import (
    ChamberManager,
    DEFAULT_FALLBACK_EXEC_SEC,
    DEFAULT_INFRA_PODS_NAMESPACE,
    DEFAULT_PAUSE_IMAGE,
    add_common_chamber_args,
    docs_to_pretty_yaml,
    env_bool,
    env_str,
    format_duration_hms,
    managed_chamber,
    normalize_queueing_strategy,
    parse_duration_seconds,
    query_cluster_status,
    resolve_fair_sharing_arg,
)
from runner.loader import (
    DEFAULT_INFINITE_POD_DURATION_SEC,
    DEFAULT_SCENARIO_DIR,
    InitialPodSpec,
    InitialWorkloadPodGroup,
    ScenarioBundle,
    ScheduledWorkflowItem,
    load_scenario_bundle,
)
from runner.outline import print_scenario_outline
from runner.replay import (
    apply_scenario_queues_and_wait,
    cleanup_scenario_workloads,
    run_scenario_replay,
)
from runner.validation import (
    ALLOWED_INFRA_GVKS,
    ALLOWED_WORKFLOW_GVKS,
    ALLOWED_WORKFLOW_KINDS,
    MAX_K8S_NAME_LEN,
    SERVER_METADATA_FIELDS,
    SYSTEM_NAMESPACES,
    validate_workflow_events,
)

__all__ = [
    "ALLOWED_INFRA_GVKS",
    "ALLOWED_WORKFLOW_GVKS",
    "ALLOWED_WORKFLOW_KINDS",
    "DEFAULT_INFINITE_POD_DURATION_SEC",
    "DEFAULT_SCENARIO_DIR",
    "InitialPodSpec",
    "InitialWorkloadPodGroup",
    "MAX_K8S_NAME_LEN",
    "SERVER_METADATA_FIELDS",
    "SYSTEM_NAMESPACES",
    "ScenarioBundle",
    "ScheduledWorkflowItem",
    "apply_scenario_queues_and_wait",
    "build_arg_parser",
    "cleanup_scenario_workloads",
    "load_scenario_bundle",
    "main",
    "print_scenario_outline",
    "run_scenario_replay",
    "validate_workflow_events",
]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "kueue-optimizer Scenario Test Runner (`testrunner`): replays a scraped test case "
            "(`infra.json`, `workflows.json`, `execution-time.json` per SCRAPER-SCHEMA.md) in a chamber."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "scenario_pos",
        nargs="?",
        default=None,
        help="Optional positional path to the scraped scenario directory (default: testscenarios/demo)",
    )
    parser.add_argument(
        "--scenario-dir",
        "--test-dir",
        "-d",
        dest="scenario_dir",
        type=Path,
        default=Path(env_str("SCENARIO_DIR", str(DEFAULT_SCENARIO_DIR))),
        help=f"Path to scraped test scenario directory (default: {DEFAULT_SCENARIO_DIR})",
    )
    parser.add_argument(
        "--infra-file",
        type=Path,
        default=None,
        help="Explicit path to infra.json (defaults to <scenario-dir>/infra.json)",
    )
    parser.add_argument(
        "--workflows-file",
        type=Path,
        default=None,
        help="Explicit path to workflows.json (defaults to <scenario-dir>/workflows.json)",
    )
    parser.add_argument(
        "--execution-time-file",
        type=Path,
        default=None,
        help="Explicit path to execution-time.json (defaults to <scenario-dir>/execution-time.json)",
    )

    add_common_chamber_args(
        parser,
        default_workload_namespace="",
        default_queueing_mode="",
        default_pause_image=DEFAULT_PAUSE_IMAGE,
    )

    parser.add_argument(
        "--speedup",
        "--time-compression",
        "-s",
        dest="speedup",
        type=float,
        default=1.0,
        help="Time compression factor N (> 0): divides timestamp offsets and pod execution times by N (default: 1.0)",
    )
    parser.add_argument(
        "--duration",
        "--run-duration",
        "--max-run-seconds",
        dest="duration",
        default=None,
        help="Maximum wall-clock duration to run the test (e.g. '60s', '5m'; default: until all workflows complete)",
    )
    parser.add_argument(
        "--status-interval",
        default="3s",
        help="Interval for printing periodic cluster & workflow status updates (default: 3s)",
    )
    parser.add_argument(
        "--default-exec-seconds",
        default=str(DEFAULT_FALLBACK_EXEC_SEC),
        help=(
            f"Fallback pod execution time in seconds for workloads without an entry in execution-time.json "
            f"(default: {DEFAULT_FALLBACK_EXEC_SEC})"
        ),
    )
    parser.add_argument(
        "--infinite-pod-duration",
        default=str(int(DEFAULT_INFINITE_POD_DURATION_SEC)),
        help=(
            "Execution duration for initial running pods present at t0 that have no deletionTime in infra.json "
            f"(default: {int(DEFAULT_INFINITE_POD_DURATION_SEC)}s = 10d)"
        ),
    )
    parser.add_argument(
        "--min-pod-duration",
        default="0.1s",
        help="Minimum KWOK pod execution duration after time compression (default: 0.1s)",
    )
    parser.add_argument(
        "--initial-window",
        default="1.0s",
        help="Window after t0 within which Pod 'New' events in infra.json are treated as initial pods (default: 1.0s)",
    )
    parser.add_argument(
        "--skip-initial-offset",
        "--zero-first-offset",
        dest="skip_initial_offset",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Immediately jump to the timestamp of the first workload to be added (default: true)",
    )
    parser.add_argument(
        "--wait-for-completion",
        "--watch",
        dest="wait_for_completion",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Wait for all submitted workflows to complete before finishing (default: true)",
    )
    parser.add_argument(
        "--metrics-report",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run the Prometheus Kueue metrics report at the end of the scenario run (default: true)",
    )
    parser.add_argument(
        "--no-cleanup",
        "--keep",
        dest="no_cleanup",
        action="store_true",
        default=env_bool("NO_CLEANUP", False),
        help="Keep the chamber running after the test run completes instead of tearing it down",
    )
    parser.add_argument(
        "--cleanup-after",
        dest="cleanup_after",
        action="store_true",
        default=False,
        help="Tear down the chamber after watching workflows to completion (default behavior)",
    )
    parser.add_argument(
        "--infra-pods-namespace",
        default=env_str("INFRA_PODS_NAMESPACE", DEFAULT_INFRA_PODS_NAMESPACE),
        help=(
            "Dedicated namespace (explicitly excluded from Kueue webhooks/management) for initial running "
            f"Pods from infra.json (default: {DEFAULT_INFRA_PODS_NAMESPACE})"
        ),
    )
    parser.add_argument(
        "--cq-fair-sharing-preemption",
        "--override-cq-fair-sharing-preemption",
        dest="cq_fair_sharing_preemption",
        action="store_true",
        default=False,
        help=(
            "Override scraped infra.json ClusterQueues to enable Preemption-Based Fair Sharing "
            "(sets fairSharing.weight=1 when unset, withinClusterQueue=LowerPriority, and reclaimWithinCohort=Any; "
            "default: False so infra.json ClusterQueue specs are replayed verbatim)"
        ),
    )
    parser.add_argument(
        "--dry-run-limit",
        type=int,
        default=5,
        help="Number of sample Nodes, Initial Pods, and Workflows to print in --dry-run mode (0 = print all; default: 5)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.scenario_pos:
        args.scenario_dir = Path(args.scenario_pos)

    if args.speedup <= 0:
        parser.error("--speedup / --time-compression must be > 0")

    enable_fair_sharing = resolve_fair_sharing_arg(
        args.fair_sharing,
        args.skip_chamber_deploy and not args.cleanup and not args.cleanup_workloads,
        parser,
    )

    if args.cleanup_after:
        args.no_cleanup = False
    elif args.skip_chamber_deploy:
        args.no_cleanup = True
    elif not args.wait_for_completion:
        args.no_cleanup = True

    override_q_strategy = normalize_queueing_strategy(args.queueing_mode) if args.queueing_mode else None
    override_wl_ns = args.workload_namespace.strip() if args.workload_namespace else None

    status_interval_sec = parse_duration_seconds(args.status_interval) or 3.0
    min_pod_duration_sec = parse_duration_seconds(args.min_pod_duration) or 0.1
    default_exec_sec = parse_duration_seconds(args.default_exec_seconds) or DEFAULT_FALLBACK_EXEC_SEC
    infinite_pod_sec = parse_duration_seconds(args.infinite_pod_duration) or DEFAULT_INFINITE_POD_DURATION_SEC
    initial_window_sec = parse_duration_seconds(args.initial_window) or 1.0
    max_run_seconds = parse_duration_seconds(args.duration)

    if args.cleanup:
        if args.dry_run:
            print(f"[dry-run] Would tear down chamber in host namespace '{args.namespace}'.")
            return 0
        mgr = ChamberManager(
            chamber_namespace=args.namespace,
            workload_namespace=override_wl_ns or "default",
            apiserver_port=args.port,
            prometheus_port=args.prometheus_port,
            scrape_interval=args.scrape_interval,
            registry_port=args.registry_port,
            registry_memory_limit=args.registry_memory_limit,
            kueue_image=args.kueue_image,
            kueue_build_dir=args.kueue_build_dir,
            enable_fair_sharing=enable_fair_sharing,
        )
        mgr.cleanup_chamber()
        return 0

    infra_pods_ns = args.infra_pods_namespace.strip() or DEFAULT_INFRA_PODS_NAMESPACE

    bundle = load_scenario_bundle(
        scenario_dir=args.scenario_dir,
        infra_path=args.infra_file,
        workflows_path=args.workflows_file,
        execution_time_path=args.execution_time_file,
        speedup=args.speedup,
        min_pod_duration_sec=min_pod_duration_sec,
        infinite_pod_duration_sec=infinite_pod_sec,
        default_exec_sec=default_exec_sec,
        initial_window_sec=initial_window_sec,
        skip_initial_offset=args.skip_initial_offset,
        pause_image_override=args.pause_image,
        override_workload_namespace=override_wl_ns,
        override_queueing_strategy=override_q_strategy,
        override_cq_fair_sharing_preemption=args.cq_fair_sharing_preemption,
        infra_pods_namespace=infra_pods_ns,
    )

    mgr = ChamberManager(
        chamber_namespace=args.namespace,
        workload_namespace=bundle.primary_workload_namespace,
        apiserver_port=args.port,
        prometheus_port=args.prometheus_port,
        scrape_interval=args.scrape_interval,
        registry_port=args.registry_port,
        registry_memory_limit=args.registry_memory_limit,
        kueue_image=args.kueue_image,
        kueue_build_dir=args.kueue_build_dir,
        enable_fair_sharing=enable_fair_sharing,
    )

    if args.cleanup_workloads:
        if args.dry_run:
            print(
                f"[dry-run] Would delete workloads and queues in chamber '{args.namespace}' "
                f"(namespaces: {', '.join(bundle.workload_namespaces)})."
            )
            return 0
        try:
            cleanup_scenario_workloads(mgr, bundle)
        finally:
            mgr.close()
        return 0

    if args.dry_run:
        mgr.close()
        outline_fs = None if args.skip_chamber_deploy else enable_fair_sharing
        print_scenario_outline(
            bundle,
            speedup=args.speedup,
            enable_fair_sharing=outline_fs,
            reused_chamber=args.skip_chamber_deploy,
        )
        limit = args.dry_run_limit
        sample_nodes = bundle.nodes[:limit] if limit > 0 else bundle.nodes
        sample_pods = bundle.initial_pods[:limit] if limit > 0 else bundle.initial_pods
        sample_wfs = bundle.scheduled_workflows[:limit] if limit > 0 else bundle.scheduled_workflows
        print(f"# === KWOK Nodes (showing {len(sample_nodes)} of {len(bundle.nodes)}) ===")
        print(docs_to_pretty_yaml(sample_nodes), end="")
        print("---\n# === Namespaces, ResourceFlavors, Topologies, ClusterQueues, LocalQueues ===")
        print(docs_to_pretty_yaml(bundle.prereq_and_queue_docs), end="")
        if bundle.initial_workloads:
            sample_iwls = bundle.initial_workloads[:limit] if limit > 0 else bundle.initial_workloads
            print(
                f"---\n# === Initial Pre-Admitted Kueue PodGroup Workloads at t0 "
                f"(showing {len(sample_iwls)} of {len(bundle.initial_workloads)}) ==="
            )
            print(
                docs_to_pretty_yaml(
                    [{**w.workload_manifest, "status": w.workload_status} for w in sample_iwls]
                ),
                end="",
            )
        if bundle.initial_pods:
            print(
                f"---\n# === Initial Non-Kueue Running Pods at t0 (showing {len(sample_pods)} of {len(bundle.initial_pods)}) ==="
            )
            print(docs_to_pretty_yaml([p.manifest for p in sample_pods]), end="")
        print(
            f"---\n# === Scheduled Workflows (showing {len(sample_wfs)} of {len(bundle.scheduled_workflows)}) ==="
        )
        print(docs_to_pretty_yaml([w.manifest for w in sample_wfs]), end="")
        return 0

    banner_fs = mgr.detect_live_fair_sharing_enabled() if args.skip_chamber_deploy else enable_fair_sharing
    print_scenario_outline(
        bundle,
        speedup=args.speedup,
        enable_fair_sharing=banner_fs,
        reused_chamber=args.skip_chamber_deploy,
    )

    should_cleanup_chamber = not args.no_cleanup
    with managed_chamber(
        mgr,
        skip_deploy=args.skip_chamber_deploy,
        should_cleanup=should_cleanup_chamber,
        initial_nodes=bundle.nodes,
    ):
        if args.skip_chamber_deploy and (args.kueue_build_dir or args.kueue_image):
            if args.kueue_build_dir:
                built_ref, built_dig = mgr.build_and_push_kueue_image(
                    args.kueue_build_dir,
                    tag="adhoc",
                    stage_if_needed=True,
                )
                mgr.update_kueue_image(
                    f"{built_ref}@{built_dig}" if built_dig else built_ref,
                    push_if_local=False,
                )
            elif args.kueue_image:
                mgr.update_kueue_image(args.kueue_image)

        print(
            f"==> Connecting to chamber KWOK kube-apiserver in '{args.namespace}' on localhost:{args.port}...",
            flush=True,
        )
        mgr.ensure_kwok_connection()
        mgr.wait_for_crds()
        mgr.apply_kwok_stages()
        mgr.ensure_kueue_webhook_namespace_exclusions(excluded_namespaces=(bundle.infra_pods_namespace,))

        for ns in bundle.workload_namespaces:
            mgr.workload_namespace = ns
            mgr.clear_workload_namespace(delete_sample_cq=True)
        mgr.workload_namespace = bundle.primary_workload_namespace

        if bundle.nodes:
            print(f"==> Syncing {len(bundle.nodes)} KWOK Node(s) from infra.json...", flush=True)
            mgr.sync_kwok_nodes(bundle.nodes, remove_stale=True)

        print(
            f"==> Applying Namespaces, ResourceFlavors, initial t0 state, ClusterQueues ({', '.join(bundle.cluster_queue_names)}), "
            f"and LocalQueues from infra.json...",
            flush=True,
        )
        apply_scenario_queues_and_wait(
            mgr,
            bundle.prereq_and_queue_docs,
            initial_workloads=bundle.initial_workloads,
            initial_pods=bundle.initial_pods,
        )

        start_epoch, end_epoch, submitted_count, total_pods = run_scenario_replay(
            mgr=mgr,
            bundle=bundle,
            speedup=args.speedup,
            max_run_seconds=max_run_seconds,
            status_interval_sec=status_interval_sec,
            wait_for_completion=args.wait_for_completion,
            initial_state_already_bootstrapped=True,
        )

        final_snap = query_cluster_status(
            mgr,
            workload_namespaces=bundle.workload_namespaces,
            include_kueue_workloads=True,
        )
        observed_pods = (
            final_snap.pods_running
            + final_snap.pods_pending
            + final_snap.pods_succeeded
            + final_snap.pods_failed
        )
        total_pods = max(total_pods, observed_pods)

        print("")
        print("================================================================================")
        print("  KUEUE-OPTIMIZER TESTRUNNER — EXECUTION SUMMARY")
        print("================================================================================")
        print(f"  Scenario Directory      : {bundle.scenario_dir}")
        print(
            f"  Chamber Registry        : localhost:{mgr.registry_port} (in-memory tmpfs {mgr.registry_memory_limit})"
        )
        if mgr._resolved_kueue_image or mgr.kueue_image:
            print(f"  Kueue Image             : {mgr._resolved_kueue_image or mgr.kueue_image}")
        print(
            f"  Wall-Clock Duration     : {end_epoch - start_epoch:.1f}s ({format_duration_hms(end_epoch - start_epoch)})"
        )
        if bundle.initial_workloads:
            print(
                f"  Initial Kueue Workloads : {len(bundle.initial_workloads)} PodGroup(s) "
                f"({bundle.initial_workload_pods_count} pod(s))"
            )
        print(f"  Initial Infra Pods      : {len(bundle.initial_pods)}")
        planned_denom = max(len(bundle.scheduled_workflows), submitted_count)
        resub_extra = max(0, submitted_count - len(bundle.scheduled_workflows))
        resub_note = f" [+{resub_extra} preempted-t0 resubmit]" if resub_extra > 0 else ""
        print(
            f"  Workflows Submitted     : {submitted_count}/{planned_denom}{resub_note} ({total_pods} total pods)"
        )
        print(
            f"  Workloads Status        : admitted={final_snap.workloads_admitted}, "
            f"pending={final_snap.workloads_pending}, finished={final_snap.workloads_finished}"
        )
        print(
            f"  KWOK Pods Status        : running={final_snap.pods_running}, "
            f"pending={final_snap.pods_pending}, succeeded={final_snap.pods_succeeded}, "
            f"failed={final_snap.pods_failed}"
        )
        print(f"  ClusterQueues Status    : {' '.join(final_snap.cq_summary)}")
        print("================================================================================", flush=True)

        if args.metrics_report:
            mgr.run_kueue_metrics_report(
                queueing_strategy=bundle.queueing_strategy,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                jobset_count=submitted_count,
                total_pods=total_pods,
                wait_for_scrape=True,
            )

    if should_cleanup_chamber:
        return 0

    print(f"\nChamber '{args.namespace}' left running (--no-cleanup).")
    print(f"  export KUBECONFIG={mgr.kubeconfig_path}")
    print(f"  kubectl -n {bundle.primary_workload_namespace} get clusterqueues,localqueues,jobsets,workloads,pods")
    print(
        f"To clean up workloads only: python3 deploy/scripts/testrunner.py --cleanup-workloads --namespace {args.namespace}"
    )
    print(f"To tear down the chamber  : python3 deploy/scripts/testrunner.py --cleanup --namespace {args.namespace}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        sys.exit(0)
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode or 1)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
