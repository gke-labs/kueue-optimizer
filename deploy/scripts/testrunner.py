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
import signal
import subprocess
import sys
import time

from chamber_common import (
    AdmissionFairSharingConfig,
    ChamberManager,
    ClusterStatusSnapshot,
    DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES,
    DEFAULT_FALLBACK_EXEC_SEC,
    DEFAULT_INFRA_PODS_NAMESPACE,
    DEFAULT_INTERRUPTED_DURATION_MULTIPLIER,
    DEFAULT_PAUSE_IMAGE,
    DEFAULT_WAIT_FOR_PODS_READY_MIN_WALL_SEC,
    InterruptedDurationOverride,
    KueueScenarioConfiguration,
    ROOT_DIR,
    WaitForPodsReadyConfig,
    add_common_chamber_args,
    docs_to_pretty_yaml,
    env_bool,
    env_str,
    format_duration_hms,
    managed_chamber,
    normalize_queueing_strategy,
    parse_duration_seconds,
    parse_fair_sharing_strategies,
    parse_feature_gates_arg,
    parse_interrupted_duration_override,
    query_cluster_status,
    reconcile_wait_for_pods_ready_feature_gates,
    resolve_admission_fair_sharing_config,
    resolve_fair_sharing_arg,
    resolve_wait_for_pods_ready_config,
)
from runner.loader import (
    DEFAULT_INFINITE_POD_DURATION_SEC,
    DEFAULT_SCENARIO_DIR,
    InitialPodSpec,
    InitialWorkloadPodGroup,
    ScenarioBundle,
    ScheduledInfraEvent,
    ScheduledWorkflowDeletion,
    ScheduledWorkflowItem,
    load_scenario_bundle,
)
from runner.outline import print_scenario_outline
from runner.replay import (
    ReplayResult,
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
    "AdmissionFairSharingConfig",
    "DEFAULT_INFINITE_POD_DURATION_SEC",
    "DEFAULT_SCENARIO_DIR",
    "InitialPodSpec",
    "InitialWorkloadPodGroup",
    "InterruptedDurationOverride",
    "KueueScenarioConfiguration",
    "MAX_K8S_NAME_LEN",
    "ReplayResult",
    "SERVER_METADATA_FIELDS",
    "SYSTEM_NAMESPACES",
    "ScenarioBundle",
    "ScheduledInfraEvent",
    "ScheduledWorkflowDeletion",
    "ScheduledWorkflowItem",
    "WaitForPodsReadyConfig",
    "apply_scenario_queues_and_wait",
    "build_arg_parser",
    "cleanup_scenario_workloads",
    "load_scenario_bundle",
    "main",
    "parse_interrupted_duration_override",
    "print_scenario_outline",
    "resolve_admission_fair_sharing_config",
    "resolve_wait_for_pods_ready_config",
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
        "--missing-duration-policy",
        choices=("impute", "fallback", "error"),
        default="impute",
        help=(
            "How to handle never-admitted/unindexed workflows missing from execution-time.json: "
            "'impute' (default: peer median from same queue/shape with warning), "
            "'fallback' (use --default-exec-seconds with warning), or 'error' (fail fast)"
        ),
    )
    parser.add_argument(
        "--interrupted-duration-multiplier",
        "--partial-duration-multiplier",
        dest="interrupted_duration_multiplier",
        type=float,
        default=None,
        help=(
            "Multiplier (> 1.0) applied to observed PartialExecution duration for workloads interrupted by "
            "preemption, node failure, or scrape cutoff that lack a FullExecution entry "
            f"(default when neither override nor schema multiplier is set: {DEFAULT_INTERRUPTED_DURATION_MULTIPLIER:g}x)"
        ),
    )
    parser.add_argument(
        "--interrupted-duration-override",
        "--true-duration-override",
        dest="interrupted_duration_override",
        default=None,
        help=(
            "Explicit true uninterrupted execution duration (> observed partial duration) for PartialExecution-only "
            "workloads: accepts a global duration (e.g. '45s'), 'name=duration' pairs ('job-a=40s,job-b=60s'), "
            "or a JSON map file path ('@overrides.json')"
        ),
    )
    parser.add_argument(
        "--wait-for-pods-ready",
        dest="wait_for_pods_ready",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Enable or disable Kueue waitForPodsReady in controller_manager_config.yaml "
            "(--no-wait-for-pods-ready sets featureGates.DisableWaitForPodsReady=true; auto-configured when any "
            "--wait-for-pods-ready-* timeout/backoff flag is passed; scaled to --speedup and --duration)"
        ),
    )
    parser.add_argument(
        "--wait-for-pods-ready-timeout",
        default=None,
        help="Raw trace waitForPodsReady.timeout (e.g. '30s', '30m'), scaled by 1/--speedup and clamped to --duration",
    )
    parser.add_argument(
        "--wait-for-pods-ready-recovery-timeout",
        default=None,
        help="Raw trace waitForPodsReady.recoveryTimeout after pod/node failure (e.g. '15s', '0s' to disable)",
    )
    parser.add_argument(
        "--wait-for-pods-ready-unscheduled-timeout",
        default=None,
        help=(
            "Raw trace waitForPodsReady.unscheduledTimeout for unscheduled pods (e.g. '15s', '0s' to disable; "
            "automatically enables featureGates.WaitForPodsReadyUnscheduledTimeout=true)"
        ),
    )
    parser.add_argument(
        "--wait-for-pods-ready-block-admission",
        dest="wait_for_pods_ready_block_admission",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Configure waitForPodsReady.blockAdmission in Kueue controller config",
    )
    parser.add_argument(
        "--wait-for-pods-ready-requeue-timestamp",
        default=None,
        choices=["Eviction", "Creation"],
        help="Configure waitForPodsReady.requeuingStrategy.timestamp ('Eviction' or 'Creation'; default: scenario value or Eviction)",
    )
    parser.add_argument(
        "--wait-for-pods-ready-backoff-limit",
        type=int,
        default=None,
        help="Configure waitForPodsReady.requeuingStrategy.backoffLimitCount before workload deactivation",
    )
    parser.add_argument(
        "--wait-for-pods-ready-backoff-base",
        default=None,
        help="Raw trace waitForPodsReady.requeuingStrategy.backoffBaseSeconds (scaled by 1/--speedup)",
    )
    parser.add_argument(
        "--wait-for-pods-ready-backoff-max",
        default=None,
        help="Raw trace waitForPodsReady.requeuingStrategy.backoffMaxSeconds (scaled by 1/--speedup)",
    )
    parser.add_argument(
        "--wait-for-pods-ready-min-timeout",
        default=f"{DEFAULT_WAIT_FOR_PODS_READY_MIN_WALL_SEC:g}s",
        help=(
            "Minimum wall-clock waitForPodsReady timeout floor after --speedup scaling "
            f"(default: {DEFAULT_WAIT_FOR_PODS_READY_MIN_WALL_SEC:g}s)"
        ),
    )
    parser.add_argument(
        "--wait-for-pods-ready-cap-to-duration",
        dest="wait_for_pods_ready_cap_to_duration",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Clamp scaled waitForPodsReady timeout to <= 50%% of --duration when --duration is set (default: true)",
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
    parser.add_argument(
        "--scrape-output-dir",
        default=env_str("SCRAPE_OUTPUT_DIR", ""),
        help=(
            "Optional directory where cluster-scraper captures live infra.json, workflows.json, and "
            "execution-time.json starting at exact chamber t0 through scenario replay completion"
        ),
    )
    parser.add_argument(
        "--scrape-interval-sec",
        default=env_str("CLUSTER_SCRAPER_INTERVAL", "30s"),
        help="Periodic reconciliation interval passed to cluster-scraper --interval when --scrape-output-dir is set (default: 30s)",
    )
    parser.add_argument(
        "--scrape-hash-names",
        action="store_true",
        default=False,
        help="Pass --hash-names to cluster-scraper when --scrape-output-dir is set",
    )
    parser.add_argument(
        "--scraper-bin",
        default=env_str("CLUSTER_SCRAPER_BIN", str(ROOT_DIR / "scraper" / "cluster-scraper")),
        help="Path to compiled cluster-scraper binary used when --scrape-output-dir is set",
    )
    return parser


def _wait_for_scraper_t0_barrier(
    scrape_dir: Path,
    proc: subprocess.Popen[str],
    *,
    expected_nodes: int,
    expected_cqs: int,
    timeout_sec: float = 45.0,
) -> None:
    """Wait until cluster-scraper completes its initial t0 List+ScrapeOnce sync before replay starts."""
    infra_path = scrape_dir / "infra.json"
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"cluster-scraper exited prematurely with code {proc.returncode}; inspect {scrape_dir / 'scraper.log'}"
            )
        if infra_path.is_file():
            try:
                text = infra_path.read_text(encoding="utf-8")
            except OSError:
                text = ""
            node_hits = text.count('"kind":"Node"')
            cq_hits = text.count('"kind":"ClusterQueue"')
            # ScrapeOnce writes Nodes first and RuntimeClasses/ClusterQueues/Pods before flushing logs at t0.
            if node_hits >= expected_nodes and cq_hits >= expected_cqs:
                # Give watch goroutines 0.25s to open Watch streams after ScrapeOnce finishes.
                time.sleep(0.25)
                return
        time.sleep(0.1)
    raise RuntimeError(
        f"Timed out after {timeout_sec:.0f}s waiting for cluster-scraper t0 barrier in {scrape_dir}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.scenario_pos:
        args.scenario_dir = Path(args.scenario_pos)

    if args.speedup <= 0:
        parser.error("--speedup / --time-compression must be > 0")

    if args.cleanup_after:
        args.no_cleanup = False
    elif args.skip_chamber_deploy:
        args.no_cleanup = True
    elif not args.wait_for_completion:
        args.no_cleanup = True

    override_q_strategy = normalize_queueing_strategy(args.queueing_mode) if args.queueing_mode else None
    override_wl_ns = args.workload_namespace.strip() if args.workload_namespace else None

    try:
        status_interval_sec = parse_duration_seconds(args.status_interval) or 3.0
        min_pod_duration_sec = parse_duration_seconds(args.min_pod_duration) or 0.1
        default_exec_sec = parse_duration_seconds(args.default_exec_seconds) or DEFAULT_FALLBACK_EXEC_SEC
        infinite_pod_sec = parse_duration_seconds(args.infinite_pod_duration) or DEFAULT_INFINITE_POD_DURATION_SEC
        initial_window_sec = parse_duration_seconds(args.initial_window) or 1.0
        max_run_seconds = parse_duration_seconds(args.duration)
    except ValueError as exc:
        parser.error(str(exc))

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
        )
        mgr.cleanup_chamber()
        return 0

    infra_pods_ns = args.infra_pods_namespace.strip() or DEFAULT_INFRA_PODS_NAMESPACE

    try:
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
            interrupted_duration_multiplier=args.interrupted_duration_multiplier,
            interrupted_duration_override=args.interrupted_duration_override,
            missing_duration_policy=args.missing_duration_policy,
        )
    except ValueError as exc:
        parser.error(str(exc))

    sc_cfg = bundle.kueue_configuration
    enable_fair_sharing = resolve_fair_sharing_arg(
        args.fair_sharing,
        args.skip_chamber_deploy and not args.cleanup_workloads,
        parser,
        scenario_default=sc_cfg.fair_sharing_enabled if sc_cfg is not None else None,
    )

    try:
        if args.fair_sharing_strategies is not None:
            fair_sharing_strategies = parse_fair_sharing_strategies(args.fair_sharing_strategies)
        elif sc_cfg is not None:
            fair_sharing_strategies = sc_cfg.fair_sharing_strategies
        else:
            fair_sharing_strategies = DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES

        afs_cfg = resolve_admission_fair_sharing_config(
            enabled=args.admission_fair_sharing,
            raw_usage_half_life=args.afs_usage_half_life,
            raw_usage_sampling_interval=args.afs_sampling_interval,
            resource_weights=args.afs_resource_weights,
            scenario_afs=sc_cfg.afs_raw if sc_cfg is not None else None,
            speedup=args.speedup,
            max_run_seconds=max_run_seconds,
        )

        sc_wfpr = sc_cfg.wfpr_raw if sc_cfg is not None else None
        sc_wfpr_req = (sc_wfpr.get("requeuingStrategy") or {}) if isinstance(sc_wfpr, dict) else {}
        eff_wfpr_enabled = (
            args.wait_for_pods_ready
            if args.wait_for_pods_ready is not None
            else (True if sc_wfpr is not None else None)
        )
        wfpr_cfg = resolve_wait_for_pods_ready_config(
            enabled=eff_wfpr_enabled,
            raw_timeout=(
                args.wait_for_pods_ready_timeout
                if args.wait_for_pods_ready_timeout is not None
                else (sc_wfpr.get("timeout") if sc_wfpr else None)
            ),
            raw_recovery_timeout=(
                args.wait_for_pods_ready_recovery_timeout
                if args.wait_for_pods_ready_recovery_timeout is not None
                else (sc_wfpr.get("recoveryTimeout") if sc_wfpr else None)
            ),
            raw_unscheduled_timeout=(
                args.wait_for_pods_ready_unscheduled_timeout
                if args.wait_for_pods_ready_unscheduled_timeout is not None
                else (sc_wfpr.get("unscheduledTimeout") if sc_wfpr else None)
            ),
            block_admission=(
                args.wait_for_pods_ready_block_admission
                if args.wait_for_pods_ready_block_admission is not None
                else (sc_wfpr.get("blockAdmission") if sc_wfpr else None)
            ),
            requeuing_timestamp=(
                args.wait_for_pods_ready_requeue_timestamp
                if args.wait_for_pods_ready_requeue_timestamp is not None
                else sc_wfpr_req.get("timestamp")
            ),
            backoff_limit_count=(
                args.wait_for_pods_ready_backoff_limit
                if args.wait_for_pods_ready_backoff_limit is not None
                else sc_wfpr_req.get("backoffLimitCount")
            ),
            raw_backoff_base=(
                args.wait_for_pods_ready_backoff_base
                if args.wait_for_pods_ready_backoff_base is not None
                else sc_wfpr_req.get("backoffBaseSeconds")
            ),
            raw_backoff_max=(
                args.wait_for_pods_ready_backoff_max
                if args.wait_for_pods_ready_backoff_max is not None
                else sc_wfpr_req.get("backoffMaxSeconds")
            ),
            speedup=args.speedup,
            max_run_seconds=max_run_seconds,
            min_wall_timeout=args.wait_for_pods_ready_min_timeout,
            cap_to_duration=args.wait_for_pods_ready_cap_to_duration,
        )
        cli_feature_gates = parse_feature_gates_arg(args.kueue_feature_gates)
        merged_feature_gates: dict[str, bool] = {}
        if sc_cfg is not None and sc_cfg.raw_feature_gates:
            merged_feature_gates.update(sc_cfg.raw_feature_gates)
        elif sc_cfg is not None and sc_cfg.feature_gates:
            merged_feature_gates.update(sc_cfg.feature_gates)
        merged_feature_gates.update(cli_feature_gates)
        wfpr_cfg, merged_feature_gates = reconcile_wait_for_pods_ready_feature_gates(
            wfpr_cfg,
            merged_feature_gates,
            cli_wfpr_override=args.wait_for_pods_ready,
            cli_feature_gates=cli_feature_gates,
        )
    except ValueError as exc:
        parser.error(str(exc))

    has_cli_controller_override = any(
        x is not None
        for x in (
            args.fair_sharing_strategies,
            args.admission_fair_sharing,
            args.afs_usage_half_life,
            args.afs_sampling_interval,
            args.afs_resource_weights,
            args.wait_for_pods_ready,
            args.wait_for_pods_ready_timeout,
            args.wait_for_pods_ready_recovery_timeout,
            args.wait_for_pods_ready_unscheduled_timeout,
            args.wait_for_pods_ready_block_admission,
            args.wait_for_pods_ready_requeue_timestamp,
            args.wait_for_pods_ready_backoff_limit,
            args.wait_for_pods_ready_backoff_base,
            args.wait_for_pods_ready_backoff_max,
            args.kueue_feature_gates,
        )
    )
    if args.skip_chamber_deploy and not args.cleanup_workloads and (has_cli_controller_override or sc_cfg is not None):
        parser.error(
            "Scenario Kueue Configuration statements and controller config CLI flags (--fair-sharing-strategies, "
            "--admission-fair-sharing*, --wait-for-pods-ready*, --kueue-feature-gates) cannot be combined with "
            "--skip-chamber-deploy because Kueue's controller configuration is only applied when deploying a chamber."
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
        fair_sharing_strategies=fair_sharing_strategies,
        admission_fair_sharing=afs_cfg,
        wait_for_pods_ready=wfpr_cfg,
        kueue_feature_gates=merged_feature_gates,
        kueue_resources_config=sc_cfg.resources_block if sc_cfg is not None else None,
        manage_jobs_without_queue_name=sc_cfg.manage_jobs_without_queue_name if sc_cfg is not None else None,
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
            wait_for_pods_ready=wfpr_cfg,
            fair_sharing_strategies=fair_sharing_strategies,
            admission_fair_sharing=afs_cfg,
            config_from_scenario=sc_cfg is not None,
        )
        limit = args.dry_run_limit
        sample_nodes = bundle.nodes[:limit] if limit > 0 else bundle.nodes
        sample_pods = bundle.initial_pods[:limit] if limit > 0 else bundle.initial_pods
        sample_wfs = bundle.scheduled_workflows[:limit] if limit > 0 else bundle.scheduled_workflows
        if sc_cfg is not None:
            print("# === Kueue Controller Configuration (from scenario infra.json) ===")
            print(docs_to_pretty_yaml([sc_cfg.raw_doc]), end="")
            print("---")
        print(f"# === KWOK Nodes (showing {len(sample_nodes)} of {len(bundle.nodes)}) ===")
        print(docs_to_pretty_yaml(sample_nodes), end="")
        print("---\n# === Namespaces, ResourceFlavors, Topologies, ClusterQueues, LocalQueues ===")
        print(docs_to_pretty_yaml(bundle.prereq_and_queue_docs), end="")
        if bundle.initial_deployments:
            sample_ideps = bundle.initial_deployments[:limit] if limit > 0 else bundle.initial_deployments
            print(
                f"---\n# === Initial Pre-Admitted Deployments at t0 "
                f"(showing {len(sample_ideps)} of {len(bundle.initial_deployments)}) ==="
            )
            print(docs_to_pretty_yaml([d.manifest for d in sample_ideps]), end="")
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
        wait_for_pods_ready=wfpr_cfg,
        fair_sharing_strategies=fair_sharing_strategies,
        admission_fair_sharing=afs_cfg,
        config_from_scenario=sc_cfg is not None,
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
        mgr.sync_kueue_configuration_to_kwok_apiserver()

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
        t0_pods_started_mono = apply_scenario_queues_and_wait(
            mgr,
            bundle.prereq_and_queue_docs,
            initial_workloads=bundle.initial_workloads,
            initial_deployments=bundle.initial_deployments,
            initial_pods=bundle.initial_pods,
            hold_finite_t0_pods=True,
        )

        scraper_proc: subprocess.Popen[str] | None = None
        scraper_log_fh = None
        scrape_out_dir: Path | None = None
        replay_started = False
        try:
            if args.scrape_output_dir.strip():
                scrape_out_dir = Path(args.scrape_output_dir.strip()).resolve()
                scrape_out_dir.mkdir(parents=True, exist_ok=True)
                for stale_name in ("infra.json", "workflows.json", "execution-time.json", "scraper.log"):
                    stale_file = scrape_out_dir / stale_name
                    if stale_file.exists():
                        stale_file.unlink()
                scraper_bin = Path(args.scraper_bin).resolve()
                if not scraper_bin.is_file():
                    raise RuntimeError(
                        f"cluster-scraper binary not found at {scraper_bin}; build it with 'make scraper-build'"
                    )
                scraper_cmd = [
                    str(scraper_bin),
                    "--kubeconfig",
                    str(mgr.kubeconfig_path),
                    "--output-dir",
                    str(scrape_out_dir),
                    "--interval",
                    str(args.scrape_interval_sec),
                    "--qps",
                    "500",
                    "--burst",
                    "1000",
                ]
                if args.scrape_hash_names:
                    scraper_cmd.append("--hash-names")
                scraper_log_fh = (scrape_out_dir / "scraper.log").open("w", encoding="utf-8")
                print(
                    f"==> Starting cluster-scraper at exact chamber t0 -> {scrape_out_dir} "
                    f"(hash_names={args.scrape_hash_names}, interval={args.scrape_interval_sec})...",
                    flush=True,
                )
                scraper_proc = subprocess.Popen(
                    scraper_cmd,
                    stdout=scraper_log_fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                _wait_for_scraper_t0_barrier(
                    scrape_out_dir,
                    scraper_proc,
                    expected_nodes=len(bundle.nodes),
                    expected_cqs=len(bundle.cluster_queue_names),
                )

            replay_started = True
            t0_started = (
                float(t0_pods_started_mono)
                if isinstance(t0_pods_started_mono, (int, float)) and not isinstance(t0_pods_started_mono, bool)
                else None
            )
            replay_result = run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=args.speedup,
                max_run_seconds=max_run_seconds,
                status_interval_sec=status_interval_sec,
                wait_for_completion=args.wait_for_completion,
                t0_pods_started_mono=t0_started,
                hold_finite_t0_pods=True,
                check_control_plane_health=True,
            )
            start_epoch, end_epoch, submitted_count, total_pods = replay_result
        finally:
            if scraper_proc is not None:
                if replay_started:
                    # Allow watch streams to deliver final Succeeded/Finished events before graceful SIGTERM flush.
                    time.sleep(1.5)
                if scraper_proc.poll() is None:
                    scraper_proc.send_signal(signal.SIGTERM)
                    try:
                        scraper_proc.wait(timeout=15.0)
                    except subprocess.TimeoutExpired:
                        scraper_proc.kill()
                        scraper_proc.wait(timeout=5.0)
                print(f"==> Flushed and stopped cluster-scraper ({scrape_out_dir}).", flush=True)
            if scraper_log_fh is not None:
                scraper_log_fh.close()

        if getattr(replay_result, "control_plane_unhealthy", False):
            final_snap = ClusterStatusSnapshot()
        else:
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
        if bundle.initial_deployments:
            adm_dep_pods = sum(d.admitted_replicas for d in bundle.initial_deployments)
            des_dep_pods = sum(d.desired_replicas for d in bundle.initial_deployments)
            print(
                f"  Initial Deployments     : {len(bundle.initial_deployments)} Deployment(s) "
                f"({adm_dep_pods}/{des_dep_pods} replica(s) ready at t0)"
            )
        if bundle.initial_workloads:
            pg_pods = sum(len(w.pods) for w in bundle.initial_workloads)
            print(
                f"  Initial Kueue Workloads : {len(bundle.initial_workloads)} PodGroup(s) "
                f"({pg_pods} pod(s))"
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
        if getattr(replay_result, "bootstrap_lag_sec", 0.0) >= 0.05:
            comp_cnt = int(getattr(replay_result, "compensated_t0_pods", 0) or 0)
            comp_note = f" (compensated on {comp_cnt} finite t0 pod(s))" if comp_cnt > 0 else ""
            print(f"  Bootstrap Setup Lag     : {replay_result.bootstrap_lag_sec:.2f}s{comp_note}")
        if replay_result.stop_reason != "completed":
            sig_note = f" (signal {replay_result.signal_num})" if replay_result.signal_num is not None else ""
            print(f"  Stop Reason             : {replay_result.stop_reason}{sig_note}")
        if getattr(replay_result, "health_issues", ()):
            print(f"  Control Plane Health    : {'; '.join(replay_result.health_issues)}")
        print("================================================================================", flush=True)

        if args.metrics_report and not replay_result.interrupted and not getattr(
            replay_result, "control_plane_unhealthy", False
        ):
            mgr.run_kueue_metrics_report(
                queueing_strategy=bundle.queueing_strategy,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                jobset_count=submitted_count,
                total_pods=total_pods,
                wait_for_scrape=True,
            )

    exit_code = replay_result.exit_code
    if should_cleanup_chamber:
        return exit_code

    print(f"\nChamber '{args.namespace}' left running (--no-cleanup).")
    print(f"  export KUBECONFIG={mgr.kubeconfig_path}")
    print(f"  kubectl -n {bundle.primary_workload_namespace} get clusterqueues,localqueues,jobsets,workloads,pods")
    print(
        f"To clean up workloads only: python3 deploy/scripts/testrunner.py --cleanup-workloads --namespace {args.namespace}"
    )
    print(f"To tear down the chamber  : python3 deploy/scripts/testrunner.py --cleanup --namespace {args.namespace}")
    return exit_code


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
