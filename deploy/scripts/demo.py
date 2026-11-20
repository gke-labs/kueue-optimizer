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

"""kueue-optimizer Synthetic Demo Runner.

1. Deploys a kueue-optimizer chamber (KWOK + JobSet + Kueue + Prometheus) in the host cluster
   (default namespace: kueue-optimizer-chamber-01).
2. Creates NODE_COUNT (default: 10) simulated KWOK nodes, each with NODE_CPU (default: 4)
   CPUs and NODE_MEMORY_GB (default: 16) GiB of RAM.
3. Creates the workload namespace (default: 'ai-lab'), a ResourceFlavor covering all KWOK
   nodes, a ClusterQueue in StrictFIFO (or BestEffortFIFO) mode with quota equal to total
   cluster resources, and a LocalQueue in the workload namespace.
4. Submits JOBSET_COUNT (default: 20) JobSets of varying sizes (5..10 pods, each requesting
   1..4 CPUs and 4..12 GiB RAM) using the pause container image.
5. Watches JobSet and Kueue Workload progress until completion, generates the Prometheus
   Kueue metrics report, and tears down the chamber (unless --no-cleanup is specified).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from typing import Any

from chamber_common import (
    ChamberManager,
    DEFAULT_FLAVOR_NAME,
    DEFAULT_PAUSE_IMAGE,
    add_common_chamber_args,
    build_cluster_queue_manifest,
    build_kwok_node_manifest,
    build_local_queue_manifest,
    build_namespace_manifest,
    build_resource_flavor_manifest,
    docs_to_pretty_yaml,
    env_bool,
    env_int,
    env_str,
    format_kwok_pod_duration,
    normalize_queueing_strategy,
    parse_duration_seconds,
    query_cluster_status,
    resolve_fair_sharing_arg,
    validate_dns_label,
)


DEFAULT_DEMO_WORKLOAD_NAMESPACE = "ai-lab"
DEFAULT_DEMO_CLUSTER_QUEUE = "ai-lab-cq"
DEFAULT_DEMO_LOCAL_QUEUE = "ai-lab-lq"

SHORTHAND_POSITIONAL_FLAGS: dict[str, str] = {
    "strict-fifo": "--strict-fifo",
    "best-effort": "--best-effort",
    "best-effort-fifo": "--best-effort-fifo",
    "no-watch": "--no-watch",
    "no-cleanup": "--no-cleanup",
    "keep": "--keep",
    "cleanup-after": "--cleanup-after",
    "cleanup": "--cleanup",
    "clean": "--clean",
    "cleanup-workloads": "--cleanup-workloads",
    "clean-workloads": "--clean-workloads",
}


def build_demo_nodes_manifests(
    node_count: int,
    node_cpu: int,
    node_memory_gb: int,
    node_pods: int = 110,
) -> list[dict[str, Any]]:
    """Build NODE_COUNT simulated KWOK Node manifests."""
    return [
        build_kwok_node_manifest(
            name=f"kwok-node-{i}",
            node_idx=i,
            cpu=node_cpu,
            mem_gi=node_memory_gb,
            pods=node_pods,
            role="agent",
            use_legacy_cidr=True,
        )
        for i in range(node_count)
    ]


def build_demo_queues_manifests(
    workload_namespace: str,
    resource_flavor_name: str,
    cluster_queue_name: str,
    local_queue_name: str,
    queueing_strategy: str,
    total_cpu: int,
    total_memory_gb: int,
    total_pods: int,
    enable_fair_sharing: bool = True,
) -> list[dict[str, Any]]:
    """Build Namespace, ResourceFlavor, ClusterQueue, and LocalQueue manifests for the demo."""
    docs: list[dict[str, Any]] = []
    if workload_namespace != "default":
        docs.append(build_namespace_manifest(workload_namespace, part_of="kueue-optimizer-demo"))

    docs.append(build_resource_flavor_manifest(name=resource_flavor_name))
    docs.append(
        build_cluster_queue_manifest(
            name=cluster_queue_name,
            flavor_name=resource_flavor_name,
            resources=[
                {"name": "cpu", "nominalQuota": str(total_cpu)},
                {"name": "memory", "nominalQuota": f"{total_memory_gb}Gi"},
                {"name": "pods", "nominalQuota": str(total_pods)},
            ],
            queueing_strategy=queueing_strategy,
            enable_preemption=True,
            enable_fair_sharing=enable_fair_sharing,
        )
    )
    docs.append(
        build_local_queue_manifest(
            name=local_queue_name,
            namespace=workload_namespace,
            cluster_queue=cluster_queue_name,
        )
    )
    return docs


def build_demo_jobsets_manifests(
    jobset_count: int,
    workload_namespace: str,
    local_queue_name: str,
    min_pods: int,
    max_pods: int,
    min_cpu: int,
    max_cpu: int,
    min_mem_gb: int,
    max_mem_gb: int,
    total_cpu: int,
    total_memory_gb: int,
    pod_execution_seconds: float,
    pause_image: str,
) -> tuple[list[dict[str, Any]], int]:
    """Build deterministic pseudo-random JobSets and return (manifests, total_pods)."""
    pods_span = max(1, max_pods - min_pods + 1)
    cpu_span = max(1, max_cpu - min_cpu + 1)
    mem_span = max(1, max_mem_gb - min_mem_gb + 1)
    exec_str = format_kwok_pod_duration(pod_execution_seconds, min_seconds=0.1)

    docs: list[dict[str, Any]] = []
    total_pods = 0

    for idx in range(1, jobset_count + 1):
        name = f"jobset-{idx:02d}"
        pods = min_pods + ((idx * 7 + 3) % pods_span)
        cpu = min_cpu + ((idx * 3 + 1) % cpu_span)
        mem_gb = min_mem_gb + ((idx * 5 + 2) % mem_span)

        # Clamp per-pod requests so a single JobSet never exceeds the total ClusterQueue quota
        while pods * cpu > total_cpu and cpu > min_cpu:
            cpu -= 1
        while pods * mem_gb > total_memory_gb and mem_gb > min_mem_gb:
            mem_gb -= 1

        total_pods += pods
        docs.append(
            {
                "apiVersion": "jobset.x-k8s.io/v1alpha2",
                "kind": "JobSet",
                "metadata": {
                    "name": name,
                    "namespace": workload_namespace,
                    "labels": {
                        "kueue.x-k8s.io/queue-name": local_queue_name,
                    },
                    "annotations": {
                        "kueue-optimizer.gke.io/pod-count": str(pods),
                        "kueue-optimizer.gke.io/pod-cpu": str(cpu),
                        "kueue-optimizer.gke.io/pod-memory": f"{mem_gb}Gi",
                    },
                },
                "spec": {
                    "replicatedJobs": [
                        {
                            "name": "workers",
                            "replicas": 1,
                            "template": {
                                "spec": {
                                    "parallelism": pods,
                                    "completions": pods,
                                    "backoffLimit": 0,
                                    "template": {
                                        "metadata": {
                                            "annotations": {
                                                "kueue-optimizer.gke.io/pod-execution-time-seconds": exec_str,
                                            }
                                        },
                                        "spec": {
                                            "restartPolicy": "Never",
                                            "containers": [
                                                {
                                                    "name": "pause",
                                                    "image": pause_image,
                                                    "resources": {
                                                        "requests": {
                                                            "cpu": str(cpu),
                                                            "memory": f"{mem_gb}Gi",
                                                        },
                                                        "limits": {
                                                            "cpu": str(cpu),
                                                            "memory": f"{mem_gb}Gi",
                                                        },
                                                    },
                                                }
                                            ],
                                        },
                                    },
                                }
                            },
                        }
                    ]
                },
            }
        )

    return docs, total_pods


def cleanup_demo_workloads(
    mgr: ChamberManager,
    resource_flavor_name: str,
    cluster_queue_name: str,
    local_queue_name: str,
) -> None:
    print(
        f"==> Cleaning up demo workloads and queues in KWOK cluster "
        f"(chamber: '{mgr.chamber_namespace}', namespace: '{mgr.workload_namespace}')...",
        flush=True,
    )
    mgr.ensure_kwok_connection()
    lq_valid = validate_dns_label(local_queue_name, "localQueueName")
    cq_valid = validate_dns_label(cluster_queue_name, "clusterQueueName")
    rf_valid = validate_dns_label(resource_flavor_name, "resourceFlavorName")
    mgr.kwok_kubectl(
        ["-n", mgr.workload_namespace, "delete", "jobsets", "--all", "--ignore-not-found"],
        check=False,
    )
    mgr.kwok_kubectl(
        ["-n", mgr.workload_namespace, "delete", "localqueue", "--ignore-not-found", "--", lq_valid],
        check=False,
    )
    mgr.kwok_kubectl(
        ["delete", "clusterqueue", "--ignore-not-found", "--", cq_valid],
        check=False,
    )
    mgr.kwok_kubectl(
        ["delete", "resourceflavor", "--ignore-not-found", "--", rf_valid],
        check=False,
    )
    if mgr.workload_namespace != "default":
        mgr.kwok_kubectl(
            ["delete", "namespace", "--ignore-not-found", "--", mgr.workload_namespace],
            check=False,
        )
    print(f"==> Workload cleanup in '{mgr.workload_namespace}' complete.", flush=True)


def print_cluster_summary(
    mgr: ChamberManager,
    cluster_queue_name: str,
) -> None:
    print(f"\n=== KWOK Cluster Summary (chamber: {mgr.chamber_namespace}) ===", flush=True)
    res_nodes = mgr.kwok_kubectl(
        [
            "get",
            "nodes",
            "-o",
            'custom-columns=NAME:.metadata.name,STATUS:.status.conditions[?(@.type=="Ready")].status,CPU:.status.allocatable.cpu,MEMORY:.status.allocatable.memory',
        ],
        check=False,
    )
    if res_nodes.stdout:
        print(res_nodes.stdout.rstrip())

    print("", flush=True)
    res_cq = mgr.kwok_kubectl(
        [
            "get",
            "clusterqueue",
            cluster_queue_name,
            "-o",
            "custom-columns=NAME:.metadata.name,STRATEGY:.spec.queueingStrategy,CPU_QUOTA:.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota,MEM_QUOTA:.spec.resourceGroups[0].flavors[0].resources[1].nominalQuota,ADMITTED:.status.admittedWorkloads,PENDING:.status.pendingWorkloads",
        ],
        check=False,
    )
    if res_cq.stdout:
        print(res_cq.stdout.rstrip())

    print("", flush=True)
    res_js = mgr.kwok_kubectl(
        [
            "-n",
            mgr.workload_namespace,
            "get",
            "jobsets",
            "-o",
            r"custom-columns=NAME:.metadata.name,PODS:.metadata.annotations.kueue-optimizer\.gke\.io/pod-count,CPU_PER_POD:.metadata.annotations.kueue-optimizer\.gke\.io/pod-cpu,MEM_PER_POD:.metadata.annotations.kueue-optimizer\.gke\.io/pod-memory,SUSPENDED:.spec.suspend,READY:.status.replicatedJobsStatus[0].ready,SUCCEEDED:.status.replicatedJobsStatus[0].succeeded",
        ],
        check=False,
    )
    if res_js.stdout:
        print(res_js.stdout.rstrip(), flush=True)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deploy a kueue-optimizer chamber and run a synthetic KWOK + JobSet + Kueue scheduling demo.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_common_chamber_args(
        parser,
        default_workload_namespace=DEFAULT_DEMO_WORKLOAD_NAMESPACE,
        default_queueing_mode="StrictFIFO",
        default_pause_image=DEFAULT_PAUSE_IMAGE,
    )

    default_nodes = env_int("NODE_COUNT", 10)
    default_node_cpu = env_int("NODE_CPU", 4)
    default_node_mem = env_int("NODE_MEMORY_GB", 16)
    default_node_pods = env_int("NODE_PODS", 110)
    default_flavor = env_str("RESOURCE_FLAVOR_NAME", DEFAULT_FLAVOR_NAME)
    default_cq = env_str("CLUSTER_QUEUE_NAME", DEFAULT_DEMO_CLUSTER_QUEUE)
    default_lq = env_str("LOCAL_QUEUE_NAME", DEFAULT_DEMO_LOCAL_QUEUE)
    default_jobsets = env_int("JOBSET_COUNT", 20)
    default_min_pods = env_int("MIN_PODS", 5)
    default_max_pods = env_int("MAX_PODS", 10)
    default_min_cpu = env_int("MIN_CPU", 1)
    default_max_cpu = env_int("MAX_CPU", 4)
    default_min_mem = env_int("MIN_MEM_GB", 4)
    default_max_mem = env_int("MAX_MEM_GB", 12)
    default_exec_sec = env_str("POD_EXECUTION_SECONDS", "10")
    default_watch = env_bool("WATCH", True)
    default_no_cleanup = env_bool("NO_CLEANUP", False)

    parser.add_argument(
        "--nodes",
        type=int,
        default=default_nodes,
        help=f"Number of simulated KWOK nodes (default: {default_nodes})",
    )
    parser.add_argument(
        "--node-cpu",
        type=int,
        default=default_node_cpu,
        help=f"CPUs per simulated KWOK node (default: {default_node_cpu})",
    )
    parser.add_argument(
        "--node-memory-gb",
        type=int,
        default=default_node_mem,
        help=f"Memory (GiB) per simulated KWOK node (default: {default_node_mem})",
    )
    parser.add_argument(
        "--node-pods",
        type=int,
        default=default_node_pods,
        help=f"Max pods per simulated KWOK node (default: {default_node_pods})",
    )
    parser.add_argument(
        "--resource-flavor",
        default=default_flavor,
        help=f"ResourceFlavor name (default: {default_flavor})",
    )
    parser.add_argument(
        "--cluster-queue",
        default=default_cq,
        help=f"ClusterQueue name (default: {default_cq})",
    )
    parser.add_argument(
        "--local-queue",
        default=default_lq,
        help=f"LocalQueue name (default: {default_lq})",
    )
    parser.add_argument(
        "--jobsets",
        type=int,
        default=default_jobsets,
        help=f"Number of JobSets to create (default: {default_jobsets})",
    )
    parser.add_argument(
        "--min-pods",
        type=int,
        default=default_min_pods,
        help=f"Minimum pods per JobSet (default: {default_min_pods})",
    )
    parser.add_argument(
        "--max-pods",
        type=int,
        default=default_max_pods,
        help=f"Maximum pods per JobSet (default: {default_max_pods})",
    )
    parser.add_argument(
        "--min-cpu",
        type=int,
        default=default_min_cpu,
        help=f"Minimum CPU request per pod (default: {default_min_cpu})",
    )
    parser.add_argument(
        "--max-cpu",
        type=int,
        default=default_max_cpu,
        help=f"Maximum CPU request per pod (default: {default_max_cpu})",
    )
    parser.add_argument(
        "--min-mem-gb",
        type=int,
        default=default_min_mem,
        help=f"Minimum memory request (GiB) per pod (default: {default_min_mem})",
    )
    parser.add_argument(
        "--max-mem-gb",
        type=int,
        default=default_max_mem,
        help=f"Maximum memory request (GiB) per pod (default: {default_max_mem})",
    )
    parser.add_argument(
        "--exec-seconds",
        default=default_exec_sec,
        help=f"Pod execution duration in seconds (default: {default_exec_sec})",
    )
    parser.add_argument(
        "--watch",
        dest="watch",
        action="store_true",
        default=default_watch,
        help="Watch JobSets and Kueue Workloads until all complete (enabled by default)",
    )
    parser.add_argument(
        "--no-watch",
        dest="watch",
        action="store_false",
        help="Do not wait for all JobSets to complete (implies --no-cleanup)",
    )
    parser.add_argument(
        "--no-cleanup",
        "--keep",
        dest="no_cleanup",
        action="store_true",
        default=default_no_cleanup,
        help="Keep the chamber running after the demo instead of tearing it down",
    )
    parser.add_argument(
        "--cleanup-after",
        dest="cleanup_after",
        action="store_true",
        default=False,
        help="Tear down the chamber after watching JobSets to completion (default behavior)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    normalized_args = [SHORTHAND_POSITIONAL_FLAGS.get(a, a) for a in raw_args]

    parser = build_arg_parser()
    args = parser.parse_args(normalized_args)

    enable_fair_sharing = resolve_fair_sharing_arg(
        args.fair_sharing,
        args.skip_chamber_deploy and not args.cleanup and not args.cleanup_workloads,
        parser,
    )

    if args.cleanup_after:
        args.no_cleanup = False
        args.watch = True
    elif args.skip_chamber_deploy:
        args.no_cleanup = True
    elif not args.watch:
        args.no_cleanup = True

    queueing_strategy = normalize_queueing_strategy(args.queueing_mode)
    exec_seconds = parse_duration_seconds(args.exec_seconds) or 10.0

    total_cpu = args.nodes * args.node_cpu
    total_memory_gb = args.nodes * args.node_memory_gb
    total_pods = args.nodes * args.node_pods

    mgr = ChamberManager(
        chamber_namespace=args.namespace,
        workload_namespace=args.workload_namespace,
        apiserver_port=args.port,
        prometheus_port=args.prometheus_port,
        scrape_interval=args.scrape_interval,
        registry_port=args.registry_port,
        registry_memory_limit=args.registry_memory_limit,
        kueue_image=args.kueue_image,
        kueue_build_dir=args.kueue_build_dir,
        enable_fair_sharing=enable_fair_sharing,
    )

    effective_cq_fs = enable_fair_sharing
    if args.skip_chamber_deploy and not args.dry_run and not args.cleanup and not args.cleanup_workloads:
        live_fs = mgr.detect_live_fair_sharing_enabled()
        if live_fs is not None:
            effective_cq_fs = live_fs

    node_docs = build_demo_nodes_manifests(
        node_count=args.nodes,
        node_cpu=args.node_cpu,
        node_memory_gb=args.node_memory_gb,
        node_pods=args.node_pods,
    )
    queue_docs = build_demo_queues_manifests(
        workload_namespace=args.workload_namespace,
        resource_flavor_name=args.resource_flavor,
        cluster_queue_name=args.cluster_queue,
        local_queue_name=args.local_queue,
        queueing_strategy=queueing_strategy,
        total_cpu=total_cpu,
        total_memory_gb=total_memory_gb,
        total_pods=total_pods,
        enable_fair_sharing=effective_cq_fs,
    )
    jobset_docs, total_workload_pods = build_demo_jobsets_manifests(
        jobset_count=args.jobsets,
        workload_namespace=args.workload_namespace,
        local_queue_name=args.local_queue,
        min_pods=args.min_pods,
        max_pods=args.max_pods,
        min_cpu=args.min_cpu,
        max_cpu=args.max_cpu,
        min_mem_gb=args.min_mem_gb,
        max_mem_gb=args.max_mem_gb,
        total_cpu=total_cpu,
        total_memory_gb=total_memory_gb,
        pod_execution_seconds=exec_seconds,
        pause_image=args.pause_image or DEFAULT_PAUSE_IMAGE,
    )

    if args.cleanup:
        if args.dry_run:
            print(f"[dry-run] Would tear down chamber in host namespace '{args.namespace}'.")
            return 0
        mgr.cleanup_chamber()
        return 0

    if args.cleanup_workloads:
        if args.dry_run:
            print(f"[dry-run] Would delete demo workloads and queues in chamber '{args.namespace}'.")
            return 0
        cleanup_demo_workloads(
            mgr=mgr,
            resource_flavor_name=args.resource_flavor,
            cluster_queue_name=args.cluster_queue,
            local_queue_name=args.local_queue,
        )
        return 0

    if args.dry_run:
        print(f"# === KWOK Nodes ({args.nodes} nodes: {args.node_cpu} CPU, {args.node_memory_gb}Gi RAM each) ===")
        print(docs_to_pretty_yaml(node_docs), end="")
        print("---")
        print(
            f"# === Namespace ({args.workload_namespace}), ResourceFlavor, "
            f"ClusterQueue ({queueing_strategy}, quota={total_cpu} CPU / {total_memory_gb}Gi RAM), LocalQueue ==="
        )
        print(docs_to_pretty_yaml(queue_docs), end="")
        print("---")
        print(
            f"# === {args.jobsets} JobSets ({args.min_pods}..{args.max_pods} pods, "
            f"{args.min_cpu}..{args.max_cpu} CPU, {args.min_mem_gb}..{args.max_mem_gb}Gi RAM) ==="
        )
        print(docs_to_pretty_yaml(jobset_docs), end="")
        return 0

    should_cleanup_chamber = not args.no_cleanup
    run_succeeded = False
    try:
        if not args.skip_chamber_deploy:
            mgr.deploy_chamber(initial_nodes=node_docs)

        print(
            f"==> Connecting to the chamber's KWOK kube-apiserver on localhost:{args.port}...",
            flush=True,
        )
        mgr.ensure_kwok_connection()
        print(
            "    Waiting for KWOK, JobSet, and Kueue CRDs to be Established in KWOK kube-apiserver...",
            flush=True,
        )
        mgr.wait_for_crds()
        mgr.apply_kwok_stages()

        print(
            f"==> Configuring {args.nodes} KWOK nodes ({args.node_cpu} CPU, {args.node_memory_gb}Gi RAM each) "
            f"and '{args.workload_namespace}' queues (quota: {total_cpu} CPU, {total_memory_gb}Gi RAM, mode: {queueing_strategy})...",
            flush=True,
        )
        mgr.clear_workload_namespace(delete_sample_cq=True)
        mgr.sync_kwok_nodes(node_docs, remove_stale=True)
        mgr.apply_and_wait_queues(queue_docs)

        print(
            f"==> Submitting {args.jobsets} JobSets ({args.min_pods}..{args.max_pods} pods, "
            f"{args.min_cpu}..{args.max_cpu} CPU, {args.min_mem_gb}..{args.max_mem_gb}Gi RAM) "
            f"to namespace '{args.workload_namespace}'...",
            flush=True,
        )
        demo_start_epoch = time.time()
        mgr.apply_manifests_parallel(jobset_docs, max_workers=16)

        print_cluster_summary(mgr=mgr, cluster_queue_name=args.cluster_queue)

        if args.watch:
            print(
                f"\n==> Watching JobSets and Workloads in '{args.workload_namespace}' "
                f"until all {args.jobsets} JobSets complete...",
                flush=True,
            )
            while True:
                snap = query_cluster_status(mgr)
                adm = snap.cq_admitted_by_name.get(args.cluster_queue, snap.workloads_admitted)
                pend = snap.cq_pending_by_name.get(args.cluster_queue, snap.workloads_pending)
                print(
                    f"  [Status] JobSets Completed: {snap.jobsets_completed:2d}/{args.jobsets:2d} | "
                    f"CQ Admitted: {adm:2d} | CQ Pending: {pend:2d} | "
                    f"Pods Running: {snap.pods_running:3d} | Pods Succeeded: {snap.pods_succeeded:3d}",
                    flush=True,
                )
                if snap.jobsets_completed >= args.jobsets:
                    print(f"==> All {args.jobsets} JobSets completed!", flush=True)
                    break
                time.sleep(3.0)

        demo_end_epoch = time.time()
        final_snap = query_cluster_status(mgr)
        observed_pods = (
            final_snap.pods_running
            + final_snap.pods_pending
            + final_snap.pods_succeeded
            + final_snap.pods_failed
        )
        total_workload_pods = max(total_workload_pods, observed_pods)

        mgr.run_kueue_metrics_report(
            queueing_strategy=queueing_strategy,
            start_epoch=demo_start_epoch,
            end_epoch=demo_end_epoch,
            jobset_count=args.jobsets,
            total_pods=total_workload_pods,
            wait_for_scrape=True,
        )
        run_succeeded = True
    finally:
        if should_cleanup_chamber and run_succeeded:
            print("", flush=True)
            mgr.cleanup_chamber()
        elif should_cleanup_chamber and mgr.created_namespace_in_this_run:
            print("", flush=True)
            try:
                mgr.cleanup_chamber()
            except Exception as cleanup_exc:
                print(
                    f"WARNING: Chamber cleanup after error failed for '{args.namespace}': {cleanup_exc}",
                    file=sys.stderr,
                    flush=True,
                )
        else:
            mgr.close()

    if should_cleanup_chamber:
        return 0

    print("")
    print(f"Chamber '{args.namespace}' left running (--no-cleanup).")
    print("To inspect the KWOK cluster directly:")
    print(f"  make port-forward NAMESPACE={args.namespace}")
    print(f"  export KUBECONFIG={mgr.kubeconfig_path}")
    print(f"  kubectl -n {args.workload_namespace} get jobsets,workloads,jobs,pods")
    print("")
    print(f"To query Prometheus Kueue metrics in namespace '{args.namespace}':")
    print(
        f'  kubectl get --raw "/api/v1/namespaces/{args.namespace}/services/http:prometheus:9090/proxy/api/v1/query?query=kueue_admitted_active_workloads"'
    )
    print(f"  make port-forward-prometheus NAMESPACE={args.namespace}")
    print("")
    print("To clean up when done:")
    print(f"  python3 deploy/scripts/demo.py --cleanup --namespace {args.namespace}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode or 1)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
