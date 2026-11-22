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

"""Deterministic generator for the 60-scenario kueue-optimizer Self-Test Suite (`testscenarios/self-tests/`).

Generates:
1. Sixty self-contained, live-runnable benchmark scenario directories (`st01-...` .. `st60-...`),
   each containing schema-compliant `infra.json`, `workflows.json`, and `execution-time.json` files
   designed with clean integer capacities and durations so that every Kueue Prometheus metric and
   every aggregated report section (`5.1`, `5.2`, `5.3`, `5.4`) has a closed-form mathematical ground truth.
   - `st01`–`st30`: Core 6-category verification matrix (saturation, queueing/backfill/TAS, intra-CQ
     preemption, cohort borrowing/reclaiming/limits, fair sharing, and resource accounting edge cases).
   - `st31`–`st45`: Dedicated 15-scenario `t0` initial-workload verification matrix covering pre-admitted
     `PodGroup` handling, quota occupancy, hybrid `infra-pods` separation, Tier 3/4 fallback matching,
     multi-role `PodSet` divisibility, infinite-duration `t0` workloads, intra-CQ preemption & dynamic
     parent workflow resubmission, equal-priority timestamp seniority, late-preemption badput accounting,
     TAS topology domain occupancy, cohort reclamation, borrowing limits, and Preemption-Based Fair Sharing.
   - `st46`–`st60`: Dedicated 15-scenario large-scale verification matrix (`128`–`256` nodes, `1,048`–`1,200`
     workloads, all with `t0` in-flight workloads; `st46`–`st50` quota-only without TAS and `st51`–`st60` with
     7-level TAS) validating chamber throughput, summary metrics, and live `cluster-scraper` fidelity across
     both vanilla Kubernetes deployments/APIs and `kueue-optimizer` chambers.
2. Per-scenario `AGENTS.md` execution and side-by-side paired cross-examination guides (`st01`–`st60`),
   plus the suite-level `testscenarios/self-tests/AGENTS.md` and `testscenarios/self-tests/README.md`.

Usage:
    python3 testscenarios/self-tests/generate_self_test_suite.py
    python3 deploy/scripts/generate_scenario_readme.py --all
    python3 testscenarios/self-tests/generate_self_test_suite.py --check
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

SUITE_DIR = Path(__file__).resolve().parent
if str(SUITE_DIR) not in sys.path:
    sys.path.insert(0, str(SUITE_DIR))

from failure_preemption_scenarios import FAILURE_PREEMPTION_PAIRINGS, build_failure_preemption_15_scenarios
from scale_scenarios import SCALE_SCENARIO_PAIRINGS, build_scale_15_scenarios
REPO_ROOT = SUITE_DIR.parent.parent
BASE_TIME = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

DEFAULT_FLAVOR = "st-tpu-flavor"
DEFAULT_TOPOLOGY = "st-slice-topology"
DEFAULT_NS = "st-workloads"
DEFAULT_ACCEL_LABEL = "tpu-v7x-st"

TOPOLOGY_LEVELS = [
    "cloud.google.com/gce-topology-block",
    "cloud.google.com/gke-tpu-partition-4x4x4-id",
    "cloud.google.com/gke-tpu-partition-2x4x4-id",
    "cloud.google.com/gke-tpu-partition-2x2x4-id",
    "cloud.google.com/gke-tpu-partition-2x2x2-id",
    "cloud.google.com/gke-tpu-partition-2x2x1-id",
    "kubernetes.io/hostname",
]


def fmt_ts(offset_sec: float) -> str:
    dt = BASE_TIME + timedelta(seconds=max(0.0, float(offset_sec)))
    if dt.microsecond:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def det_uid(seed_str: str) -> str:
    h = hashlib.sha256(seed_str.encode("utf-8")).hexdigest()[:32]
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def make_infra_event(obj: dict[str, Any], offset_sec: float = 0.0, event_type: str = "New") -> dict[str, Any]:
    return {
        "timestamp": fmt_ts(offset_sec),
        "eventType": event_type,
        "payload": obj,
    }


def make_node(
    idx: int,
    *,
    tpu_per_node: int = 4,
    cpu_per_node: int = 16,
    mem_gi_per_node: int = 64,
    pods_per_node: int = 110,
    with_topology: bool = False,
) -> dict[str, Any]:
    node_name = f"st-node-{idx:02d}"
    cube_idx = idx // 16
    local_idx = idx % 16
    block_idx = cube_idx // 8
    labels: dict[str, str] = {
        "type": "kwok",
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/os": "linux",
        "kubernetes.io/role": "agent",
        "node-role.kubernetes.io/agent": "",
        "kueue-optimizer.gke.io/node-role": "agent",
        "kubernetes.io/hostname": node_name,
        "cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL,
    }
    if with_topology:
        labels.update(
            {
                "cloud.google.com/gce-topology-block": f"st-block-{block_idx:02d}",
                "cloud.google.com/gke-tpu-partition-4x4x4-id": f"b{block_idx:02d}-c{cube_idx:02d}",
                "cloud.google.com/gke-tpu-partition-2x4x4-id": f"b{block_idx:02d}-c{cube_idx:02d}-p244-{local_idx // 8}",
                "cloud.google.com/gke-tpu-partition-2x2x4-id": f"b{block_idx:02d}-c{cube_idx:02d}-p224-{local_idx // 4}",
                "cloud.google.com/gke-tpu-partition-2x2x2-id": f"b{block_idx:02d}-c{cube_idx:02d}-p222-{local_idx // 2}",
                "cloud.google.com/gke-tpu-partition-2x2x1-id": f"b{block_idx:02d}-c{cube_idx:02d}-p221-{local_idx:02d}",
            }
        )
    cap: dict[str, str] = {
        "cpu": str(cpu_per_node),
        "memory": f"{mem_gi_per_node}Gi",
        "pods": str(pods_per_node),
    }
    annotations: dict[str, str] = {
        "kwok.x-k8s.io/node": "fake",
        "node.alpha.kubernetes.io/ttl": "0",
        "kueue-optimizer.gke.io/node-cpu": str(cpu_per_node),
        "kueue-optimizer.gke.io/node-memory": f"{mem_gi_per_node}Gi",
        "kueue-optimizer.gke.io/node-pods": str(pods_per_node),
    }
    if tpu_per_node > 0:
        cap["google.com/tpu"] = str(tpu_per_node)
        annotations["kueue-optimizer.gke.io/node-accel-resource"] = "google.com/tpu"
        annotations["kueue-optimizer.gke.io/node-accel-count"] = str(tpu_per_node)

    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "uid": det_uid(f"node:{node_name}"),
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {},
        "status": {
            "capacity": dict(cap),
            "allocatable": dict(cap),
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "kwok kubelet is posting ready status",
                }
            ],
        },
    }


def make_namespace(name: str = DEFAULT_NS) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "uid": det_uid(f"ns:{name}"),
        },
    }


def make_topology(name: str = DEFAULT_TOPOLOGY) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "Topology",
        "metadata": {
            "name": name,
            "uid": det_uid(f"topo:{name}"),
        },
        "spec": {
            "levels": [{"nodeLabel": lbl} for lbl in TOPOLOGY_LEVELS],
        },
    }


def make_flavor(name: str = DEFAULT_FLAVOR, topology_name: str | None = None) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "nodeLabels": {
            "cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL,
            "type": "kwok",
        }
    }
    if topology_name:
        spec["topologyName"] = topology_name
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ResourceFlavor",
        "metadata": {
            "name": name,
            "uid": det_uid(f"rf:{name}"),
        },
        "spec": spec,
    }


def make_priority_class(name: str, value: int) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "WorkloadPriorityClass",
        "metadata": {
            "name": name,
            "uid": det_uid(f"wpc:{name}"),
        },
        "value": int(value),
    }


def make_resource_entry(
    name: str,
    nominal: str,
    borrowing_limit: str | None = None,
    lending_limit: str | None = None,
) -> dict[str, Any]:
    d: dict[str, Any] = {"name": name, "nominalQuota": nominal}
    if borrowing_limit is not None:
        d["borrowingLimit"] = borrowing_limit
    if lending_limit is not None:
        d["lendingLimit"] = lending_limit
    return d


def make_resource_group(
    *,
    tpu_quota: int,
    cpu_quota: int = 128,
    mem_gi_quota: int = 512,
    flavor_name: str = DEFAULT_FLAVOR,
    tpu_borrowing_limit: int | None = None,
    tpu_lending_limit: int | None = None,
) -> dict[str, Any]:
    return {
        "coveredResources": ["cpu", "memory", "google.com/tpu"],
        "flavors": [
            {
                "name": flavor_name,
                "resources": [
                    make_resource_entry("cpu", str(cpu_quota)),
                    make_resource_entry("memory", f"{mem_gi_quota}Gi"),
                    make_resource_entry(
                        "google.com/tpu",
                        str(tpu_quota),
                        borrowing_limit=str(tpu_borrowing_limit) if tpu_borrowing_limit is not None else None,
                        lending_limit=str(tpu_lending_limit) if tpu_lending_limit is not None else None,
                    ),
                ],
            }
        ],
    }


def make_cohort(
    name: str,
    *,
    tpu_quota: int = 0,
    cpu_quota: int = 0,
    mem_gi_quota: int = 0,
    fair_sharing_weight: str | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "resourceGroups": [
            make_resource_group(
                tpu_quota=tpu_quota,
                cpu_quota=cpu_quota,
                mem_gi_quota=mem_gi_quota,
            )
        ]
    }
    if fair_sharing_weight is not None:
        spec["fairSharing"] = {"weight": str(fair_sharing_weight)}
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "Cohort",
        "metadata": {
            "name": name,
            "uid": det_uid(f"cohort:{name}"),
        },
        "spec": spec,
    }


def make_cluster_queue(
    name: str,
    *,
    tpu_quota: int,
    cpu_quota: int = 128,
    mem_gi_quota: int = 512,
    cohort_name: str | None = None,
    queueing_strategy: str = "BestEffortFIFO",
    within_cq: str = "Never",
    reclaim_within_cohort: str = "Never",
    borrow_policy: str | None = None,
    borrow_max_prio_threshold: int | None = None,
    fair_sharing_weight: str | None = None,
    tpu_borrowing_limit: int | None = None,
    tpu_lending_limit: int | None = None,
) -> dict[str, Any]:
    preemption: dict[str, Any] = {
        "withinClusterQueue": within_cq,
        "reclaimWithinCohort": reclaim_within_cohort,
    }
    if borrow_policy is not None:
        bwc: dict[str, Any] = {"policy": borrow_policy}
        if borrow_max_prio_threshold is not None:
            bwc["maxPriorityThreshold"] = int(borrow_max_prio_threshold)
        preemption["borrowWithinCohort"] = bwc

    spec: dict[str, Any] = {
        "namespaceSelector": {},
        "queueingStrategy": queueing_strategy,
        "preemption": preemption,
        "resourceGroups": [
            make_resource_group(
                tpu_quota=tpu_quota,
                cpu_quota=cpu_quota,
                mem_gi_quota=mem_gi_quota,
                tpu_borrowing_limit=tpu_borrowing_limit,
                tpu_lending_limit=tpu_lending_limit,
            )
        ],
    }
    if cohort_name:
        spec["cohortName"] = cohort_name
    if fair_sharing_weight is not None:
        spec["fairSharing"] = {"weight": str(fair_sharing_weight)}

    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ClusterQueue",
        "metadata": {
            "name": name,
            "uid": det_uid(f"cq:{name}"),
        },
        "spec": spec,
    }


def make_local_queue(name: str, cluster_queue: str, namespace: str = DEFAULT_NS) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "LocalQueue",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": det_uid(f"lq:{namespace}/{name}"),
        },
        "spec": {
            "clusterQueue": cluster_queue,
        },
    }


KWOK_FAKE_TOLERATION = {
    "key": "kwok.x-k8s.io/node",
    "operator": "Equal",
    "value": "fake",
    "effect": "NoSchedule",
}


def _build_jobset_object(
    *,
    name: str,
    ts_create: str,
    duration_sec: float,
    local_queue: str,
    num_pods: int = 1,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 4,
    mem_gi_per_pod: int = 16,
    priority_class: str | None = None,
    namespace: str = DEFAULT_NS,
    tas_required_level: str | None = None,
    init_container_tpu: int | None = None,
    pod_overhead_cpu: int | None = None,
    extra_coordinator_role: bool = False,
    suspend: bool = True,
) -> dict[str, Any]:
    labels: dict[str, str] = {
        "kueue.x-k8s.io/queue-name": local_queue,
    }
    if priority_class:
        labels["kueue.x-k8s.io/priority-class"] = priority_class

    pod_annotations: dict[str, str] = {
        "kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(round(duration_sec))),
    }
    if tas_required_level:
        pod_annotations["kueue.x-k8s.io/podset-required-topology"] = tas_required_level

    reqs: dict[str, str] = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
    }
    if tpu_per_pod > 0:
        reqs["google.com/tpu"] = str(tpu_per_pod)

    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "tolerations": [dict(KWOK_FAKE_TOLERATION)],
        "nodeSelector": {
            "cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL,
        },
        "containers": [
            {
                "name": "worker",
                "image": "registry.k8s.io/pause:3.10",
                "resources": {
                    "requests": dict(reqs),
                    "limits": dict(reqs),
                },
            }
        ],
    }

    if init_container_tpu is not None:
        init_reqs = {
            "cpu": str(cpu_per_pod),
            "memory": f"{mem_gi_per_pod}Gi",
            "google.com/tpu": str(init_container_tpu),
        }
        pod_spec["initContainers"] = [
            {
                "name": "tpu-init-warmup",
                "image": "registry.k8s.io/pause:3.10",
                "resources": {
                    "requests": dict(init_reqs),
                    "limits": dict(init_reqs),
                },
            }
        ]

    if pod_overhead_cpu is not None:
        pod_spec["runtimeClassName"] = "st-overhead-rc"
        pod_spec["overhead"] = {"cpu": str(pod_overhead_cpu)}

    replicated_jobs: list[dict[str, Any]] = []
    if extra_coordinator_role:
        coord_reqs = {"cpu": "4", "memory": "8Gi"}
        coord_anns = {"kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(round(duration_sec)))}
        replicated_jobs.append(
            {
                "name": "coord",
                "replicas": 1,
                "template": {
                    "metadata": {},
                    "spec": {
                        "parallelism": 1,
                        "completions": 1,
                        "backoffLimit": 0,
                        "template": {
                            "metadata": {"annotations": dict(coord_anns)},
                            "spec": {
                                "restartPolicy": "Never",
                                "tolerations": [dict(KWOK_FAKE_TOLERATION)],
                                "nodeSelector": {
                                    "cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL,
                                },
                                "containers": [
                                    {
                                        "name": "coordinator",
                                        "image": "registry.k8s.io/pause:3.10",
                                        "resources": {
                                            "requests": dict(coord_reqs),
                                            "limits": dict(coord_reqs),
                                        },
                                    }
                                ],
                            },
                        },
                    },
                },
            }
        )

    replicated_jobs.append(
        {
            "name": "w",
            "replicas": 1,
            "template": {
                "metadata": {"annotations": dict(pod_annotations)},
                "spec": {
                    "parallelism": int(num_pods),
                    "completions": int(num_pods),
                    "backoffLimit": 0,
                    "template": {
                        "metadata": {"annotations": dict(pod_annotations)},
                        "spec": pod_spec,
                    },
                },
            },
        }
    )

    return {
        "apiVersion": "jobset.x-k8s.io/v1alpha2",
        "kind": "JobSet",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": det_uid(f"js:{namespace}/{name}"),
            "creationTimestamp": ts_create,
            "labels": labels,
            "annotations": dict(pod_annotations),
        },
        "spec": {
            "suspend": bool(suspend),
            "replicatedJobs": replicated_jobs,
        },
    }


def make_jobset_and_exec_events(
    *,
    name: str,
    arrival_sec: float,
    duration_sec: float,
    local_queue: str,
    cluster_queue: str,
    num_pods: int = 1,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 4,
    mem_gi_per_pod: int = 16,
    priority_class: str | None = None,
    namespace: str = DEFAULT_NS,
    tas_required_level: str | None = None,
    init_container_tpu: int | None = None,
    pod_overhead_cpu: int | None = None,
    extra_coordinator_role: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Create matching `workflows.json` `NotAdmitted` event and `execution-time.json` lifecycle events."""
    ts_create = fmt_ts(arrival_sec)
    ts_end = fmt_ts(arrival_sec + duration_sec)
    wl_hash = hashlib.sha256(f"{namespace}/{name}".encode("utf-8")).hexdigest()[:5]
    wl_name = f"jobset-{name}-{wl_hash}"

    js_obj = _build_jobset_object(
        name=name,
        ts_create=ts_create,
        duration_sec=duration_sec,
        local_queue=local_queue,
        num_pods=num_pods,
        tpu_per_pod=tpu_per_pod,
        cpu_per_pod=cpu_per_pod,
        mem_gi_per_pod=mem_gi_per_pod,
        priority_class=priority_class,
        namespace=namespace,
        tas_required_level=tas_required_level,
        init_container_tpu=init_container_tpu,
        pod_overhead_cpu=pod_overhead_cpu,
        extra_coordinator_role=extra_coordinator_role,
        suspend=True,
    )

    wf_event = {
        "timestamp": ts_create,
        "eventType": "New",
        "workloadState": "NotAdmitted",
        "payload": {
            "objectKind": "JobSet",
            "name": name,
            "namespace": namespace,
            "creationTime": ts_create,
            "localQueueName": local_queue,
            "object": js_obj,
        },
    }

    exec_events = [
        {
            "timestamp": ts_create,
            "eventType": "Admitted",
            "workloadState": "Admitted",
            "payload": {
                "workloadName": wl_name,
                "namespace": namespace,
                "ownerKind": "JobSet",
                "ownerName": name,
                "ownerAPIVersion": "jobset.x-k8s.io/v1alpha2",
                "localQueueName": local_queue,
                "clusterQueueName": cluster_queue,
                "creationTime": ts_create,
                "admittedTime": ts_create,
                "preempted": False,
                "reason": "Admitted",
                "message": "The workload is admitted",
            },
        },
        {
            "timestamp": ts_end,
            "eventType": "FullExecution",
            "workloadState": "Admitted",
            "payload": {
                "executionType": "Full",
                "workloadName": wl_name,
                "namespace": namespace,
                "ownerKind": "JobSet",
                "ownerName": name,
                "ownerAPIVersion": "jobset.x-k8s.io/v1alpha2",
                "localQueueName": local_queue,
                "clusterQueueName": cluster_queue,
                "creationTime": ts_create,
                "admittedTime": ts_create,
                "endTime": ts_end,
                "executionDurationSeconds": float(duration_sec),
                "executionDuration": f"{int(round(duration_sec))}s",
                "preempted": False,
                "reason": "Succeeded",
                "message": "jobset completed successfully",
            },
        },
    ]
    return wf_event, exec_events


def make_t0_admitted_jobset_events(
    *,
    name: str,
    remaining_sec: float,
    local_queue: str,
    cluster_queue: str,
    node_names: list[str] | None = None,
    coord_node_name: str | None = None,
    num_pods: int = 1,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 4,
    mem_gi_per_pod: int = 16,
    priority_class: str | None = None,
    priority_value: int | None = None,
    namespace: str = DEFAULT_NS,
    tas_required_level: str | None = None,
    extra_coordinator_role: bool = False,
    is_infinite: bool = False,
    full_duration_sec: float | None = None,
    pod_mode: str = "enriched",  # "enriched" | "unlinked" | "none"
    pod_name_prefix: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Create `infra.json` Pod events, `workflows.json` `Admitted` event, and `execution-time.json` events for a `t0`-running Kueue JobSet."""
    t_start = fmt_ts(0.0)
    eff_full_dur = float(full_duration_sec if full_duration_sec is not None else remaining_sec)
    wl_hash = hashlib.sha256(f"{namespace}/{name}".encode("utf-8")).hexdigest()[:5]
    wl_name = f"jobset-{name[:45]}-{wl_hash}"

    js_obj = _build_jobset_object(
        name=name,
        ts_create=t_start,
        duration_sec=eff_full_dur,
        local_queue=local_queue,
        num_pods=num_pods,
        tpu_per_pod=tpu_per_pod,
        cpu_per_pod=cpu_per_pod,
        mem_gi_per_pod=mem_gi_per_pod,
        priority_class=priority_class,
        namespace=namespace,
        tas_required_level=tas_required_level,
        extra_coordinator_role=extra_coordinator_role,
        suspend=False,
    )

    wf_event = {
        "timestamp": t_start,
        "eventType": "New",
        "workloadState": "Admitted",
        "payload": {
            "objectKind": "JobSet",
            "name": name,
            "namespace": namespace,
            "creationTime": t_start,
            "localQueueName": local_queue,
            "object": js_obj,
        },
    }

    adm_payload: dict[str, Any] = {
        "workloadName": wl_name,
        "namespace": namespace,
        "ownerKind": "JobSet",
        "ownerName": name,
        "ownerAPIVersion": "jobset.x-k8s.io/v1alpha2",
        "localQueueName": local_queue,
        "clusterQueueName": cluster_queue,
        "creationTime": t_start,
        "admittedTime": t_start,
        "preempted": False,
        "reason": "Admitted",
        "message": "Already running at scrape start t0",
    }
    if priority_class:
        adm_payload["priorityClassName"] = priority_class
    if priority_value is not None:
        adm_payload["priority"] = int(priority_value)

    exec_events: list[dict[str, Any]] = [
        {
            "timestamp": t_start,
            "eventType": "Admitted",
            "workloadState": "Admitted",
            "payload": adm_payload,
        }
    ]
    if not is_infinite:
        t_end = fmt_ts(remaining_sec)
        full_payload = dict(adm_payload)
        full_payload.update(
            {
                "executionType": "Full",
                "endTime": t_end,
                "executionDurationSeconds": eff_full_dur,
                "executionDuration": f"{int(round(eff_full_dur))}s",
                "reason": "Succeeded",
                "message": "t0 workload completed",
            }
        )
        exec_events.append(
            {
                "timestamp": t_end,
                "eventType": "FullExecution",
                "workloadState": "Admitted",
                "payload": full_payload,
            }
        )

    infra_pod_events: list[dict[str, Any]] = []
    if pod_mode != "none":
        placements = list(node_names or [f"st-node-{i:02d}" for i in range(num_pods)])
        assert len(placements) == num_pods, f"{name}: expected {num_pods} placements, got {len(placements)}"
        p_prefix = pod_name_prefix or name

        def _append_pod(p_name: str, plc: str, reqs: dict[str, str], role_name: str, is_tas: bool) -> None:
            new_pl: dict[str, Any] = {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": p_name,
                "namespace": namespace,
                "placement": plc,
                "schedulingTime": t_start,
                "firstNoticeTime": t_start,
                "resourceRequest": dict(reqs),
            }
            if pod_mode == "enriched":
                new_pl["workloadName"] = wl_name
                new_pl["ownerKind"] = "JobSet"
                new_pl["ownerName"] = name
                new_pl["podSetName"] = role_name
                if is_tas:
                    new_pl["isTAS"] = True
            infra_pod_events.append(
                {
                    "timestamp": t_start,
                    "eventType": "New",
                    "payload": new_pl,
                }
            )
            if not is_infinite:
                t_del = fmt_ts(remaining_sec)
                infra_pod_events.append(
                    {
                        "timestamp": t_del,
                        "eventType": "Deleted",
                        "payload": {
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "name": p_name,
                            "namespace": namespace,
                            "deletionTime": t_del,
                        },
                    }
                )

        if extra_coordinator_role:
            c_node = coord_node_name or placements[0]
            _append_pod(
                f"{p_prefix}-coord-0",
                c_node,
                {"cpu": "4", "memory": "8Gi"},
                "coord",
                False,
            )

        w_reqs: dict[str, str] = {
            "cpu": str(cpu_per_pod),
            "memory": f"{mem_gi_per_pod}Gi",
        }
        if tpu_per_pod > 0:
            w_reqs["google.com/tpu"] = str(tpu_per_pod)
        for idx_p, plc in enumerate(placements):
            _append_pod(
                f"{p_prefix}-w-{idx_p}",
                plc,
                w_reqs,
                "w",
                bool(tas_required_level),
            )

    return infra_pod_events, wf_event, exec_events


def make_initial_pod_events(
    *,
    pod_name: str,
    node_name: str,
    remaining_sec: float | None,
    tpu_req: int = 4,
    cpu_req: int = 4,
    mem_gi_req: int = 16,
    namespace: str = "legacy-preexisting",
) -> list[dict[str, Any]]:
    """Create non-Kueue initial `Pod` events in `infra.json` (`Deleted` omitted when `remaining_sec is None`)."""
    t_start = fmt_ts(0.0)
    reqs: dict[str, str] = {
        "cpu": str(cpu_req),
        "memory": f"{mem_gi_req}Gi",
    }
    if tpu_req > 0:
        reqs["google.com/tpu"] = str(tpu_req)
    evs: list[dict[str, Any]] = [
        {
            "timestamp": t_start,
            "eventType": "New",
            "payload": {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": pod_name,
                "namespace": namespace,
                "placement": node_name,
                "schedulingTime": t_start,
                "firstNoticeTime": t_start,
                "resourceRequest": reqs,
            },
        }
    ]
    if remaining_sec is not None:
        t_del = fmt_ts(remaining_sec)
        evs.append(
            {
                "timestamp": t_del,
                "eventType": "Deleted",
                "payload": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": pod_name,
                    "namespace": namespace,
                    "deletionTime": t_del,
                },
            }
        )
    return evs


@dataclass
class ScenarioSpec:
    scenario_id: str
    slug: str
    category: str
    title: str
    feature_focus: str
    recommended_cmd: str
    setup_summary: str
    expected_behavior: str
    expected_metrics: list[str]
    must_not_happen: list[str]
    sec51_target: str
    sec52_target: str
    sec53_target: str
    sec54_target: str
    infra_events: list[dict[str, Any]] = field(default_factory=list)
    workflow_events: list[dict[str, Any]] = field(default_factory=list)
    exec_events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def dir_name(self) -> str:
        return f"{self.scenario_id}-{self.slug}"

    @property
    def scenario_num(self) -> int:
        return int(self.scenario_id[2:])

    @property
    def chamber_namespace(self) -> str:
        return f"kueue-optimizer-chamber-{self.scenario_id}"

    @property
    def apiserver_port(self) -> int:
        return 16400 + self.scenario_num

    @property
    def prometheus_port(self) -> int:
        return 19090 + (self.apiserver_port - 16443)

    @property
    def isolated_cmd(self) -> str:
        return f"{self.recommended_cmd} --namespace {self.chamber_namespace} --port {self.apiserver_port}"


def base_cluster_infra(
    *,
    num_nodes: int = 8,
    tpu_per_node: int = 4,
    cpu_per_node: int = 16,
    mem_gi_per_node: int = 64,
    with_topology: bool = False,
    priority_classes: list[tuple[str, int]] | None = None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for i in range(num_nodes):
        events.append(
            make_infra_event(
                make_node(
                    i,
                    tpu_per_node=tpu_per_node,
                    cpu_per_node=cpu_per_node,
                    mem_gi_per_node=mem_gi_per_node,
                    with_topology=with_topology,
                )
            )
        )
    events.append(make_infra_event(make_namespace(DEFAULT_NS)))
    if with_topology:
        events.append(make_infra_event(make_topology(DEFAULT_TOPOLOGY)))
    events.append(
        make_infra_event(
            make_flavor(DEFAULT_FLAVOR, topology_name=DEFAULT_TOPOLOGY if with_topology else None)
        )
    )
    for pc_name, pc_val in priority_classes or []:
        events.append(make_infra_event(make_priority_class(pc_name, pc_val)))
    return events


def build_all_45_scenarios() -> list[ScenarioSpec]:
    scenarios: list[ScenarioSpec] = []

    def add_job(
        wf_list: list[dict[str, Any]],
        ex_list: list[dict[str, Any]],
        *,
        name: str,
        arrival_sec: float,
        duration_sec: float,
        lq: str,
        cq: str,
        num_pods: int = 1,
        tpu_per_pod: int = 4,
        cpu_per_pod: int = 4,
        mem_gi_per_pod: int = 16,
        prio: str | None = None,
        tas_level: str | None = None,
        init_tpu: int | None = None,
        overhead_cpu: int | None = None,
        extra_coord: bool = False,
    ) -> None:
        wf_ev, ex_evs = make_jobset_and_exec_events(
            name=name,
            arrival_sec=arrival_sec,
            duration_sec=duration_sec,
            local_queue=lq,
            cluster_queue=cq,
            num_pods=num_pods,
            tpu_per_pod=tpu_per_pod,
            cpu_per_pod=cpu_per_pod,
            mem_gi_per_pod=mem_gi_per_pod,
            priority_class=prio,
            tas_required_level=tas_level,
            init_container_tpu=init_tpu,
            pod_overhead_cpu=overhead_cpu,
            extra_coordinator_role=extra_coord,
        )
        wf_list.append(wf_ev)
        ex_list.extend(ex_evs)

    def add_t0_job(
        inf_list: list[dict[str, Any]],
        wf_list: list[dict[str, Any]],
        ex_list: list[dict[str, Any]],
        *,
        name: str,
        remaining_sec: float,
        lq: str,
        cq: str,
        node_names: list[str] | None = None,
        coord_node_name: str | None = None,
        num_pods: int = 1,
        tpu_per_pod: int = 4,
        cpu_per_pod: int = 4,
        mem_gi_per_pod: int = 16,
        prio: str | None = None,
        prio_val: int | None = None,
        tas_level: str | None = None,
        extra_coord: bool = False,
        is_infinite: bool = False,
        full_duration_sec: float | None = None,
        pod_mode: str = "enriched",
        pod_name_prefix: str | None = None,
    ) -> None:
        p_evs, wf_ev, ex_evs = make_t0_admitted_jobset_events(
            name=name,
            remaining_sec=remaining_sec,
            local_queue=lq,
            cluster_queue=cq,
            node_names=node_names,
            coord_node_name=coord_node_name,
            num_pods=num_pods,
            tpu_per_pod=tpu_per_pod,
            cpu_per_pod=cpu_per_pod,
            mem_gi_per_pod=mem_gi_per_pod,
            priority_class=prio,
            priority_value=prio_val,
            tas_required_level=tas_level,
            extra_coordinator_role=extra_coord,
            is_infinite=is_infinite,
            full_duration_sec=full_duration_sec,
            pod_mode=pod_mode,
            pod_name_prefix=pod_name_prefix,
        )
        inf_list.extend(p_evs)
        wf_list.append(wf_ev)
        ex_list.extend(ex_evs)

    # =========================================================================
    # CATEGORY 1 (st01 - st05): Baseline Saturation, Quota Caps & Multi-Pod Math
    # =========================================================================

    # st01: 50% Cluster Saturation via Single ClusterQueue Quota Cap
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-half", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf.append(make_infra_event(make_local_queue("lq-half", "cq-half")))
    for i in range(8):
        add_job(wf, ex, name=f"js-half-{i:02d}", arrival_sec=0.0, duration_sec=20.0, lq="lq-half", cq="cq-half")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st01",
            slug="50pct-single-cq-quota-cap",
            category="1. Baseline Saturation & Accounting",
            title="50% Cluster Saturation Limited by Single ClusterQueue Quota",
            feature_focus="Single CQ quota ceiling (`16/32` TPUs = 50% of physical cluster) across 2 back-to-back waves",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap",
            setup_summary="8-node cluster (`32` TPUs total). Single `ClusterQueue` `cq-half` configured with `nominalQuota = 16` TPUs (`50%` of cluster) and no cohort. 8 single-node (`4`-TPU, `20s`) jobs arrive simultaneously at `t=0s`.",
            expected_behavior="Wave 1 (4 jobs = `16` TPUs) admits at `t=0s` and runs from `0s` to `20s` while 4 physical nodes sit completely idle due to the CQ quota ceiling. Wave 2 (remaining 4 jobs = `16` TPUs) waits `20s` in queue, admits at `t=20s`, and finishes at `t=40s`.",
            expected_metrics=[
                "`Execution Summary`: `Workflows Submitted: 8/8`, `Workloads Status: ... finished=8`, `KWOK Pods Status: ... succeeded=8, failed=0`.",
                "`Section 5.1 (ClusterQueue: cq-half / All)`: `P50 = P90 = P99 = 100.00%`, `AVG ≈ 95%–100%` (`16 / 16` TPUs) because the CQ's own `16`-TPU quota is 100% full across both waves.",
                "`Section 5.2 (Wait Time)`: Exact 4+4 bimodal wave split (`<=1s: 4` from Wave 1, `~T_wave` for Wave 2 where `T_wave = 20s / speedup`): Prometheus `P50 <= 1.00s` (exact `4/8 = 50%` cumulative count at `le=\"1.0\"`), while `AVG ≈ 0.5 * T_wave` (`~10s` at `1.0x`) and `P90 ≈ 0.9 * T_wave` (`~19s` at `1.0x`).",
                "`Section 5.3 (Evictions)`: `0` total evictions, `0` unique workloads evicted.",
                "`Section 5.4 (Effective Saturation)`: `8 jobs × 4 TPUs × T_wave` divided by `32 cluster TPUs × 2 * T_wave` → **`EFFECTIVE SAT ≈ 50.0%` (`46%–52%` across any `--speedup`)**, proving `Sec 5.1` (quota-relative = `100%`) and `Sec 5.4` (cluster-relative = `50%`) use their distinct denominators accurately.",
            ],
            must_not_happen=[
                "`Section 5.4 EFFECTIVE SAT` MUST NOT report `100%` (which would mean dividing by CQ quota `16` instead of physical cluster capacity `32`).",
                "`Section 5.1 AVG` MUST NOT report `50%` (which would mean dividing CQ usage `16` by cluster capacity `32` instead of `cq-half` nominal quota `16`).",
                "Zero workloads may be preempted or fail (`Evictions == 0`, `failed == 0`).",
            ],
            sec51_target="95%–100% (16/16 TPUs)",
            sec52_target="P50 <=1s, AVG ~0.5*T_wave (~10s @1x)",
            sec53_target="0 evictions",
            sec54_target="~50.0% (640 / 1,280 TPU-s)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st02: 100% Full Cluster Saturation (Zero Queueing)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-full", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-full", "cq-full")))
    for i in range(8):
        add_job(wf, ex, name=f"js-full-{i:02d}", arrival_sec=0.0, duration_sec=35.0, lq="lq-full", cq="cq-full")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st02",
            slug="100pct-full-cluster-saturation",
            category="1. Baseline Saturation & Accounting",
            title="100% Full Cluster Saturation in a Single Immediate Wave",
            feature_focus="Full `32/32` TPU saturation with zero queueing delay and zero evictions",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation",
            setup_summary="8-node cluster (`32` TPUs). Single `ClusterQueue` `cq-full` with full `32`-TPU quota. 8 single-node (`4`-TPU, `35s`) workloads submitted at `t=0s`.",
            expected_behavior="All 8 workloads admit immediately at `t=0s`, occupy all 8 nodes (`32/32` TPUs) continuously for `35s`, and finish together at `t=35s`.",
            expected_metrics=[
                "`Section 5.1 (cq-full)`: `AVG ≈ 95%–100%` (`32 / 32` TPUs).",
                "`Section 5.2 (Wait Time)`: `P50 < 0.5s`, `P99 < 1.5s` (pure controller admission latency).",
                "`Section 5.3 (Evictions)`: `0`.",
                "`Section 5.4 (Effective Saturation)`: `8 × 4 × 35 = 1,120 TPU-s` over `~35s` → **`EFFECTIVE SAT ≈ 93%–100%`** (`COMPLETED ONLY ≈ 93%–100%`, `ACTIVE AT END = 0.00%`).",
            ],
            must_not_happen=[
                "`EFFECTIVE SAT` exceeding `100.00%` (would indicate wall-clock evaluation window under-measurement or duplicate pod counting).",
                "Any workload waiting in `kueue_pending_workloads` after initial admission cycle.",
            ],
            sec51_target="95%–100% (32/32 TPUs)",
            sec52_target="< 1.0s (immediate)",
            sec53_target="0 evictions",
            sec54_target="93%–100%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st03: 25% Light Underloaded Cluster
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-light", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-light", "cq-light")))
    for i in range(2):
        add_job(wf, ex, name=f"js-light-{i:02d}", arrival_sec=0.0, duration_sec=35.0, lq="lq-light", cq="cq-light")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st03",
            slug="25pct-light-underloaded-cluster",
            category="1. Baseline Saturation & Accounting",
            title="25% Under-Saturated Cluster with Unrestricted Queue Quota",
            feature_focus="Demand-limited (not quota-limited) 25% cluster utilization (`8/32` TPUs)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster",
            setup_summary="8-node cluster (`32` TPUs) and unrestricted `cq-light` (`32`-TPU quota), but only 2 single-node (`4`-TPU, `35s`) workloads submitted at `t=0s`.",
            expected_behavior="Both jobs admit immediately (`8/32` TPUs used) while 6 nodes (`24` TPUs) remain idle throughout the `35s` run.",
            expected_metrics=[
                "`Section 5.1 (cq-light)`: `AVG ≈ 23%–26%` (`8 / 32` TPUs).",
                "`Section 5.2 (Wait Time)`: `< 1.0s`.",
                "`Section 5.3 (Evictions)`: `0`.",
                "`Section 5.4 (Effective Saturation)`: **`EFFECTIVE SAT ≈ 23%–25%`** (`280 / 1,120 TPU-s`), matching `Section 5.1` within `~2%`.",
            ],
            must_not_happen=[
                "Discrepancy `|Sec 5.1 AVG - Sec 5.4 EFFECTIVE SAT| > 3%` when CQ quota equals full cluster capacity and zero evictions occurred.",
            ],
            sec51_target="23%–26% (8/32 TPUs)",
            sec52_target="< 1.0s",
            sec53_target="0 evictions",
            sec54_target="23%–25%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st04: Multi-Pod Per-Pod Resource Multiplication (2 pods x 4 TPU + 4 pods x 4 TPU)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-mpod", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-mpod", "cq-mpod")))
    add_job(wf, ex, name="js-2pod-a", arrival_sec=0.0, duration_sec=35.0, lq="lq-mpod", cq="cq-mpod", num_pods=2, tpu_per_pod=4)
    add_job(wf, ex, name="js-2pod-b", arrival_sec=0.0, duration_sec=35.0, lq="lq-mpod", cq="cq-mpod", num_pods=2, tpu_per_pod=4)
    add_job(wf, ex, name="js-4pod-c", arrival_sec=0.0, duration_sec=35.0, lq="lq-mpod", cq="cq-mpod", num_pods=4, tpu_per_pod=4)
    scenarios.append(
        ScenarioSpec(
            scenario_id="st04",
            slug="multipod-4tpu-per-pod-saturation",
            category="1. Baseline Saturation & Accounting",
            title="Multi-Pod Per-Pod Resource Scaling Verification (2-Pod & 4-Pod JobSets)",
            feature_focus="Exact `num_pods × per_pod_tpu` accounting in `Section 5.4` + child Job/JobSet deduplication",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st04-multipod-4tpu-per-pod-saturation",
            setup_summary="8-node cluster (`32` TPUs = `8 × 4` TPUs). Only 3 `JobSet` objects submitted at `t=0s`: two 2-pod JobSets (`2 pods × 4 TPUs = 8 TPUs` each) and one 4-pod JobSet (`4 pods × 4 TPUs = 16 TPUs`), totaling `8 + 8 + 16 = 32 TPUs` (`8` pods across `3` workloads) for `35s`.",
            expected_behavior="All 3 multi-pod JobSets admit together at `t=0s`, launching `2 + 2 + 4 = 8` pods that fill all 8 nodes (`32/32` TPUs) at 100% saturation.",
            expected_metrics=[
                "`Execution Summary`: `Workflows Submitted: 3/3`, `Workloads Status: ... finished=3`, `KWOK Pods Status: ... succeeded=8`.",
                "`Section 5.1 (cq-mpod)`: `AVG ≈ 95%–100%` (`32 / 32` TPUs).",
                "`Section 5.4 (Effective Saturation)`: Subtitle reports `Qualifying Workloads: 3 (completed=3 [8 pods] ...)`. `EFFECTIVE SAT` for `google.com/tpu` MUST be **`93%–100%`** (`1,120 / 1,120 TPU-s`).",
            ],
            must_not_happen=[
                "`Section 5.4` reporting `37.5%` (`12/32` TPUs — classic bug of counting 1 pod per JobSet instead of `num_pods × per_pod_tpu`).",
                "`Section 5.4` reporting `>100%` / `200%` (classic bug of double-counting both parent `JobSet` Workload and child `batch/v1 Job` Workload).",
            ],
            sec51_target="95%–100% (32/32 TPUs)",
            sec52_target="< 1.0s",
            sec53_target="0 evictions",
            sec54_target="93%–100% (3 workloads, 8 pods)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st05: Mixed Completed Only + Active At End Split
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-split", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-split", "cq-split")))
    for i in range(4):
        add_job(wf, ex, name=f"js-short-w1-{i}", arrival_sec=0.0, duration_sec=20.0, lq="lq-split", cq="cq-split")
    for i in range(4):
        add_job(wf, ex, name=f"js-short-w2-{i}", arrival_sec=20.0, duration_sec=18.0, lq="lq-split", cq="cq-split")
    for i in range(4):
        add_job(wf, ex, name=f"js-long-active-{i}", arrival_sec=0.0, duration_sec=120.0, lq="lq-split", cq="cq-split")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st05",
            slug="mixed-completed-and-active-at-end",
            category="1. Baseline Saturation & Accounting",
            title="50% Completed Workloads + 50% Active-at-End Workloads Decomposition",
            feature_focus="Exact additive split `COMPLETED ONLY (~48%) + ACTIVE AT END (~50%) = EFFECTIVE SAT (~98%)` in Section 5.4",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st05-mixed-completed-and-active-at-end --duration 42s",
            setup_summary="8-node cluster (`32` TPUs). Half the cluster (`4` nodes = `16` TPUs) runs two waves of short completed jobs (`20s` + `18s`); the other half (`4` nodes = `16` TPUs) runs 4 long `120s` jobs that are still actively running when `--duration 42s` ends the evaluation window.",
            expected_behavior="8 short jobs complete cleanly before `t=42s` (`completed=8 [8 pods]`), while all 4 long jobs remain actively running at the exact end of the test (`active_at_end=4 [4 pods]`). Both groups contribute to `EFFECTIVE SAT`.",
            expected_metrics=[
                "`Section 5.4` columns for `google.com/tpu`: `COMPLETED ONLY ≈ 44%–50%`, `ACTIVE AT END ≈ 47%–50%`, and `EFFECTIVE SAT = COMPLETED ONLY + ACTIVE AT END ≈ 92%–100%`.",
                "`Section 5.4` subtitle: `Qualifying Workloads: 12 (completed=8 [8 pods], active_at_end=4 [4 pods]) | Excluded (preempted/reclaimed/failed): 0 [0 pods]`.",
            ],
            must_not_happen=[
                "Long jobs still actively running at `t_end` being misclassified as `Excluded` (`ACTIVE AT END == 0%`).",
                "Active jobs credited with their full `120s` declared duration in `EFFECTIVE SAT` instead of elapsed runtime `t_end - admit_time` (`42s`), which would falsely push `EFFECTIVE SAT` above `100%`.",
            ],
            sec51_target="95%–100%",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="COMPLETED ~47% + ACTIVE ~49% = ~96%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 2 (st06 - st10): Queueing Modes, Backfill, Backlog & Topology
    # =========================================================================

    # st06: StrictFIFO Head-of-Line Blocking
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-strict", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, queueing_strategy="StrictFIFO")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-strict", "cq-strict")))
    add_job(wf, ex, name="js-anchor-2n", arrival_sec=0.0, duration_sec=30.0, lq="lq-strict", cq="cq-strict", num_pods=2)
    add_job(wf, ex, name="js-blocker-7n", arrival_sec=1.0, duration_sec=15.0, lq="lq-strict", cq="cq-strict", num_pods=7)
    for i in range(6):
        add_job(wf, ex, name=f"js-small-{i}", arrival_sec=2.0, duration_sec=15.0, lq="lq-strict", cq="cq-strict", num_pods=1)
    scenarios.append(
        ScenarioSpec(
            scenario_id="st06",
            slug="strict-fifo-hol-blocking",
            category="2. Queueing, Backfill & Topology",
            title="StrictFIFO Head-of-Line Blocking Under Heterogeneous Job Sizes",
            feature_focus="`queueingStrategy: StrictFIFO` blocking 6 small 1-node jobs behind an inadmissible 7-node head-of-line job",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st06-strict-fifo-hol-blocking",
            setup_summary="8-node (`32`-TPU) cluster, `cq-strict` (`StrictFIFO`). At `t=0s`, a 2-node (`8`-TPU, `30s`) anchor job starts, leaving `24` TPUs (`75%` of cluster) free. At `t=1s`, a 7-node (`28`-TPU, `15s`) blocker arrives. At `t=2s`, six 1-node (`4`-TPU, `15s`) jobs arrive.",
            expected_behavior="Because `js-blocker-7n` needs `28` TPUs and only `24` are free, `StrictFIFO` keeps `js-blocker-7n` AND all 6 small jobs behind it pending for `29s` while 6 out of 8 cluster nodes sit idle. At `t=30s`, `js-anchor-2n` completes, `js-blocker-7n` + one 1-node job run (`30s–45s`), and the remaining 5 small jobs finish (`45s–60s`).",
            expected_metrics=[
                "`Section 5.1 (cq-strict)` & `Section 5.4`: Low utilization **`~49%–58%`** (`25%` during the first `30s` due to HoL blocking, then `100%` for `15s`, then `62.5%` for `15s`).",
                "`Section 5.2 (Wait Time)`: High wait time on the small jobs (`~28s–43s`), `P50 ≈ 35s`.",
                "`Section 1 Gauge (kueue_pending_workloads)`: Remains `7` for almost `30s`.",
            ],
            must_not_happen=[
                "Small jobs `js-small-*` admitting before `js-blocker-7n` while `queueingStrategy == StrictFIFO`.",
                "Cluster utilization exceeding `65%` in this trace (which would mean out-of-order backfilling occurred despite `StrictFIFO`).",
            ],
            sec51_target="~49%–58% (HoL blocked)",
            sec52_target="P50 ~30s–40s",
            sec53_target="0 evictions",
            sec54_target="~49%–58%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st07: BestEffortFIFO Backfill (Paired Twin of st06)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-be", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, queueing_strategy="BestEffortFIFO")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-be", "cq-be")))
    add_job(wf, ex, name="js-anchor-2n", arrival_sec=0.0, duration_sec=30.0, lq="lq-be", cq="cq-be", num_pods=2)
    add_job(wf, ex, name="js-blocker-7n", arrival_sec=1.0, duration_sec=15.0, lq="lq-be", cq="cq-be", num_pods=7)
    for i in range(6):
        add_job(wf, ex, name=f"js-small-{i}", arrival_sec=2.0, duration_sec=15.0, lq="lq-be", cq="cq-be", num_pods=1)
    scenarios.append(
        ScenarioSpec(
            scenario_id="st07",
            slug="best-effort-fifo-backfill",
            category="2. Queueing, Backfill & Topology",
            title="BestEffortFIFO Out-of-Order Backfill Eliminating Head-of-Line Blocking (Twin of st06)",
            feature_focus="Direct A/B twin of `st06` with `queueingStrategy: BestEffortFIFO` proving out-of-order backfill",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st07-best-effort-fifo-backfill",
            setup_summary="Identical 8-node cluster, identical job sizes, and identical arrival timestamps as `st06`, changing ONLY `queueingStrategy` from `StrictFIFO` to `BestEffortFIFO`.",
            expected_behavior="When `js-blocker-7n` cannot fit at `t=1s`, `BestEffortFIFO` immediately admits all 6 small 1-node jobs (`24` TPUs) at `t=2s` into the 6 free nodes (`100%` cluster fill from `t=2s` to `t=17s`), completes them all before `js-anchor-2n` finishes at `t=30s`, and then runs `js-blocker-7n` from `t=30s` to `t=45s` — cutting total makespan from `60s` (`st06`) down to `45s`.",
            expected_metrics=[
                "`Section 5.1` & `Section 5.4`: Higher utilization **`~66%–75%`** (vs `~52%` in `st06`) because total makespan shrinks by `25%` (`45s` vs `60s`) for the exact same total TPU-seconds (`1,020 TPU-s`).",
                "`Section 5.2 (Wait Time)`: Median wait time drops from `~35s` (`st06`) to **`< 2.0s`** because 7 of the 8 jobs (`js-anchor-2n` + all 6 `js-small-*`) start immediately upon arrival.",
            ],
            must_not_happen=[
                "`P50` wait time matching `st06` (`>20s`) — would mean `BestEffortFIFO` failed to backfill past `js-blocker-7n`.",
            ],
            sec51_target="~66%–75% (+16pp over st06)",
            sec52_target="P50 < 2.0s (vs ~35s in st06)",
            sec53_target="0 evictions",
            sec54_target="~66%–75%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st08: Single Priority Deep Backlog Staircase Waves
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_priority_class("p-standard", 100)))
    inf.append(make_infra_event(make_cluster_queue("cq-waves", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-waves", "cq-waves")))
    for i in range(16):
        add_job(
            wf,
            ex,
            name=f"js-wave-{i:02d}",
            arrival_sec=0.0,
            duration_sec=12.0,
            lq="lq-waves",
            cq="cq-waves",
            num_pods=2,
            prio="p-standard",
        )
    scenarios.append(
        ScenarioSpec(
            scenario_id="st08",
            slug="single-priority-deep-backlog-waves",
            category="2. Queueing, Backfill & Topology",
            title="Deterministic 4-Wave Backlog Wait-Time Staircase (`0s, 12s, 24s, 36s`)",
            feature_focus="Histogram percentile accuracy (`P50`, `P90`, `P99`, `AVG`) in Section 5.2 across 4 discrete admission waves",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st08-single-priority-deep-backlog-waves",
            setup_summary="8-node (`32`-TPU) cluster. 16 identical 2-node (`8`-TPU, `12s`) workloads with priority `p-standard (100)` submitted simultaneously at `t=0s` (4x cluster capacity).",
            expected_behavior="Exactly 4 jobs (`32` TPUs) execute per wave across 4 crisp waves: Wave 1 waits `0s`, Wave 2 waits `12s`, Wave 3 waits `24s`, Wave 4 waits `36s`. All 16 complete at `t≈48s`.",
            expected_metrics=[
                "`Section 5.1` & `Section 5.4`: `~92%–100%` continuous cluster utilization across all 4 waves.",
                "`Section 5.2 (Wait Time for p-standard)`: Mean wait time `AVG = (0 + 12 + 24 + 36) / 4 ≈ 18s`, `P50 ≈ 18s–24s`, `P90/P99 ≈ 36s`.",
                "`Section 5.3`: `0` evictions.",
            ],
            must_not_happen=[
                "More than 4 workloads (`>32` TPUs) running concurrently at any instant.",
                "`Section 5.2 AVG` deviating dramatically from `~18s` (within Prometheus histogram bucket interpolation bounds).",
            ],
            sec51_target="92%–100%",
            sec52_target="AVG ~18s, P90 ~36s",
            sec53_target="0 evictions",
            sec54_target="91%–99%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st09: Multi-Priority Non-Preemptive Queue Ordering
    inf, wf, ex = (
        base_cluster_infra(
            num_nodes=8,
            priority_classes=[("p-low", 50), ("p-normal", 200), ("p-high", 500), ("p-critical", 1000)],
        ),
        [],
        [],
    )
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-prio-order", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="Never")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-prio-order", "cq-prio-order")))
    for i in range(4):
        add_job(wf, ex, name=f"js-init-{i}", arrival_sec=0.0, duration_sec=15.0, lq="lq-prio-order", cq="cq-prio-order", num_pods=2, prio="p-low")
    for i in range(4):
        add_job(wf, ex, name=f"js-q-low-{i}", arrival_sec=2.0, duration_sec=12.0, lq="lq-prio-order", cq="cq-prio-order", num_pods=2, prio="p-low")
    for i in range(4):
        add_job(wf, ex, name=f"js-q-high-{i}", arrival_sec=3.0, duration_sec=12.0, lq="lq-prio-order", cq="cq-prio-order", num_pods=2, prio="p-high")
    for i in range(4):
        add_job(wf, ex, name=f"js-q-crit-{i}", arrival_sec=4.0, duration_sec=12.0, lq="lq-prio-order", cq="cq-prio-order", num_pods=2, prio="p-critical")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st09",
            slug="multi-priority-non-preemptive-ordering",
            category="2. Queueing, Backfill & Topology",
            title="Multi-Priority Queue Reordering Without Preemption (`withinClusterQueue: Never`)",
            feature_focus="Per-priority wait-time breakdown in `Section 5.2` when `withinClusterQueue: Never`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st09-multi-priority-non-preemptive-ordering",
            setup_summary="8-node (`32`-TPU) cluster, `withinClusterQueue: Never`. Initial wave of 4 `p-low (50)` jobs (`15s`) occupies `32/32` TPUs at `t=0s`. While full, 4 `p-low (50)` arrive at `t=2s`, 4 `p-high (500)` arrive at `t=3s`, and 4 `p-critical (1000)` arrive at `t=4s`.",
            expected_behavior="Zero running `t=0` jobs are preempted (`withinClusterQueue: Never`). When the initial wave finishes at `t=15s`, `p-critical (1000)` jumps ahead of `p-high` and `p-low` (`wait ≈ 11s`), followed by `p-high (500)` at `t=27s` (`wait ≈ 24s`), and finally the queued `p-low (50)` jobs at `t=39s` (`wait ≈ 37s`).",
            expected_metrics=[
                "`Section 5.2 (Wait Time per Priority)`: Strict monotonic ordering: **`Wait(p-critical) ≈ 11s < Wait(p-high) ≈ 24s`**, and queued `p-low` finishes last (`AVG(p-low) ≈ 18.5s` across 4 immediate + 4 `~37s` queued jobs).",
                "`Section 5.3 (Evictions)`: Strictly **`0`** (non-preemptive queue reordering only).",
                "`Section 5.4 (Effective Saturation)`: **`~92%–99%`** (100% uninterrupted execution across all 4 waves, zero wasted work).",
            ],
            must_not_happen=[
                "Any eviction (`Section 5.3 > 0`) when `withinClusterQueue == Never`.",
                "Queued `p-low` jobs admitting ahead of `p-critical` or `p-high` after `t=15s`.",
            ],
            sec51_target="93%–100%",
            sec52_target="Wait(p-critical)<Wait(p-high)<Wait(p-low,queued)",
            sec53_target="0 evictions",
            sec54_target="92%–99%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st10: TAS Sub-Slice & Cube Topology Packing
    inf = base_cluster_infra(num_nodes=16, tpu_per_node=4, cpu_per_node=32, mem_gi_per_node=128, with_topology=True)
    wf, ex = [], []
    inf.append(make_infra_event(make_cluster_queue("cq-tas", tpu_quota=64, cpu_quota=512, mem_gi_quota=2048)))
    inf.append(make_infra_event(make_local_queue("lq-tas", "cq-tas")))
    add_job(wf, ex, name="js-tas-8n", arrival_sec=0.0, duration_sec=35.0, lq="lq-tas", cq="cq-tas", num_pods=8, tas_level="cloud.google.com/gke-tpu-partition-2x4x4-id")
    add_job(wf, ex, name="js-tas-4n", arrival_sec=0.0, duration_sec=35.0, lq="lq-tas", cq="cq-tas", num_pods=4, tas_level="cloud.google.com/gke-tpu-partition-2x2x4-id")
    add_job(wf, ex, name="js-tas-2n", arrival_sec=0.0, duration_sec=35.0, lq="lq-tas", cq="cq-tas", num_pods=2, tas_level="cloud.google.com/gke-tpu-partition-2x2x2-id")
    add_job(wf, ex, name="js-tas-1n-a", arrival_sec=0.0, duration_sec=35.0, lq="lq-tas", cq="cq-tas", num_pods=1, tas_level="cloud.google.com/gke-tpu-partition-2x2x1-id")
    add_job(wf, ex, name="js-tas-1n-b", arrival_sec=0.0, duration_sec=35.0, lq="lq-tas", cq="cq-tas", num_pods=1, tas_level="cloud.google.com/gke-tpu-partition-2x2x1-id")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st10",
            slug="tas-subslice-and-cube-packing",
            category="2. Queueing, Backfill & Topology",
            title="7-Level Topology-Aware Scheduling (TAS) Exact Binary Tree Cube Packing",
            feature_focus="Kueue `Topology` + `ResourceFlavor.spec.topologyName` + `kueue.x-k8s.io/podset-required-topology` packing `8+4+2+1+1 = 16` nodes",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st10-tas-subslice-and-cube-packing",
            setup_summary="16-node (`64`-TPU) v7x cube with full 7-level hierarchy (`block -> 4x4x4 -> 2x4x4 -> 2x2x4 -> 2x2x2 -> 2x2x1 -> hostname`). 5 workloads submitted at `t=0s` whose binary partition sizes sum to `8 + 4 + 2 + 1 + 1 = 16` nodes (`64/64` TPUs).",
            expected_behavior="Kueue TAS assigns each JobSet to a non-overlapping aligned sub-tree (`p244-0`, `p224-2`, `p222-6`, `p221-14`, `p221-15`) on the very first scheduling cycle with zero fragmentation.",
            expected_metrics=[
                "`Execution Summary`: `Workflows Submitted: 5/5`, `Workloads Status: ... finished=5`, `KWOK Pods Status: ... succeeded=16, pending=0`.",
                "`Section 5.1` & `Section 5.4`: **`93%–100%`** (`64/64` TPUs packed across all 16 nodes).",
                "`Section 5.2`: `< 1.5s` (all 5 TAS shapes admit simultaneously).",
            ],
            must_not_happen=[
                "Misaligned 1-node or 2-node placement splintering the `2x4x4` or `2x2x4` domain and forcing `js-tas-8n` to wait (`Wait > 5s`).",
                "Pods stuck in `Pending` due to missing node topology labels.",
            ],
            sec51_target="93%–100% (64/64 TPUs)",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="92%–100%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 3 (st11 - st15): Intra-ClusterQueue Preemption Matrix
    # =========================================================================

    # st11: Preemption Disabled Control (withinClusterQueue: Never)
    inf, wf, ex = base_cluster_infra(num_nodes=8, priority_classes=[("p-low", 100), ("p-high", 1000)]), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-no-preempt", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="Never")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-no-preempt", "cq-no-preempt")))
    for i in range(8):
        add_job(wf, ex, name=f"js-low-{i}", arrival_sec=0.0, duration_sec=30.0, lq="lq-no-preempt", cq="cq-no-preempt", prio="p-low")
    for i in range(4):
        add_job(wf, ex, name=f"js-high-{i}", arrival_sec=8.0, duration_sec=20.0, lq="lq-no-preempt", cq="cq-no-preempt", prio="p-high")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st11",
            slug="preempt-within-cq-never",
            category="3. Intra-CQ Preemption Matrix",
            title="Intra-CQ Preemption Disabled Control (`withinClusterQueue: Never`)",
            feature_focus="Baseline control for `st12`: high-priority arrivals wait patiently without preempting running low-priority jobs",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st11-preempt-within-cq-never",
            setup_summary="8-node (`32`-TPU) cluster, `withinClusterQueue: Never`. 8 `p-low (100)` jobs (`30s`) fill all `32` TPUs at `t=0s`. 4 `p-high (1000)` jobs (`20s`) arrive at `t=8s`.",
            expected_behavior="All 8 `p-low` jobs run uninterrupted from `0s` to `30s`. All 4 `p-high` jobs wait `22s` (`t=8s -> t=30s`), then run from `30s` to `50s`. Zero work is lost.",
            expected_metrics=[
                "`Section 5.2 (Wait Time)`: `p-low` waits `~0s`, while `p-high` waits **`~22s`** (intentional non-preemptive wait).",
                "`Section 5.3 (Evictions)`: Strictly **`0`**.",
                "`Section 5.4 (Effective Saturation)`: All 12 jobs complete (`1,280 TPU-s` over `50s × 32 = 1,600 TPU-s`) → **`EFFECTIVE SAT ≈ 77%–82%`** with `Excluded (preempted/reclaimed/failed): 0 [0 pods]`.",
            ],
            must_not_happen=[
                "Any eviction (`Section 5.3 > 0`) when `withinClusterQueue: Never`.",
            ],
            sec51_target="~77%–82% (1,280/1,600 TPU-s)",
            sec52_target="p-high wait ~22s",
            sec53_target="0 evictions",
            sec54_target="~77%–82% (Excluded: 0)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st12: Intra-CQ Preemption Enabled (withinClusterQueue: LowerPriority — Twin of st11)
    inf, wf, ex = base_cluster_infra(num_nodes=8, priority_classes=[("p-low", 100), ("p-high", 1000)]), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-preempt", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="LowerPriority"
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-preempt", "cq-preempt")))
    for i in range(8):
        add_job(wf, ex, name=f"js-low-{i}", arrival_sec=0.0, duration_sec=30.0, lq="lq-preempt", cq="cq-preempt", prio="p-low")
    for i in range(4):
        add_job(wf, ex, name=f"js-high-{i}", arrival_sec=8.0, duration_sec=20.0, lq="lq-preempt", cq="cq-preempt", prio="p-high")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st12",
            slug="preempt-within-cq-lower-priority",
            category="3. Intra-CQ Preemption Matrix",
            title="Immediate Intra-CQ Preemption (`withinClusterQueue: LowerPriority` — Twin of st11)",
            feature_focus="Direct A/B twin of `st11` enabling `withinClusterQueue: LowerPriority`: verifies eviction counters, `p-high` near-zero wait, and requeued completion",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority",
            setup_summary="Identical 8-node cluster and identical 12 workloads as `st11`, changing ONLY `withinClusterQueue` from `Never` to `LowerPriority`.",
            expected_behavior="At `t=8s`, arrival of the 4 `p-high (1000)` jobs immediately preempts 4 of the 8 running `p-low (100)` jobs (`kueue_evicted_workloads_total` increments by `4`). The 4 `p-high` jobs start immediately (`wait < 2s`), finish at `t=28s`, and the 4 preempted `p-low` jobs are automatically re-admitted and run to completion.",
            expected_metrics=[
                "`Section 2 Counter (kueue_evicted_workloads_total)`: **`4`** evictions with `reason='Preempted'`.",
                "`Section 3 Histogram (kueue_pods_ready_to_evicted_time_seconds)`: `P50 ≈ 8s` (evicted `8s` after starting at `t=0s`).",
                "`Section 5.2 (Wait Time)`: `p-high` wait drops from `~22s` (`st11`) down to **`< 2.0s`**.",
                "`Section 5.3 (Total Evictions and Reclaims)`: `Total Evictions = 4`, `Unique workloads evicted once = 4` (no thrashing).",
            ],
            must_not_happen=[
                "`Section 5.3 Total Evictions == 0` (would mean intra-CQ preemption failed to trigger).",
                "`p-high` waiting `> 5s` while `p-low` jobs occupy the queue.",
                "`Total Evictions > 6` (would indicate spurious extra evictions beyond the 4 required slots).",
            ],
            sec51_target="80%–95%",
            sec52_target="p-high wait < 2.0s",
            sec53_target="4 evictions (4 unique)",
            sec54_target="~68%–88% (after requeue)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st13: Equal-Priority Preemption (withinClusterQueue: LowerOrNewerEqualPriority)
    inf, wf, ex = base_cluster_infra(num_nodes=8, priority_classes=[("p-equal", 500)]), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-equal-preempt",
                tpu_quota=32,
                cpu_quota=128,
                mem_gi_quota=512,
                within_cq="LowerOrNewerEqualPriority",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-equal-preempt", "cq-equal-preempt")))
    for i in range(5):
        add_job(wf, ex, name=f"js-eq-init-{i}", arrival_sec=0.0, duration_sec=30.0, lq="lq-equal-preempt", cq="cq-equal-preempt", prio="p-equal")
    add_job(wf, ex, name="js-eq-short-0", arrival_sec=0.0, duration_sec=12.0, lq="lq-equal-preempt", cq="cq-equal-preempt", prio="p-equal")
    add_job(wf, ex, name="js-eq-older-big-0", arrival_sec=2.0, duration_sec=20.0, lq="lq-equal-preempt", cq="cq-equal-preempt", num_pods=3, prio="p-equal")
    for i in range(2):
        add_job(wf, ex, name=f"js-eq-newer-{i}", arrival_sec=5.0, duration_sec=30.0, lq="lq-equal-preempt", cq="cq-equal-preempt", prio="p-equal")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st13",
            slug="preempt-within-cq-lower-or-newer-equal",
            category="3. Intra-CQ Preemption Matrix",
            title="Equal-Priority Timestamp Semantics (`withinClusterQueue: LowerOrNewerEqualPriority`)",
            feature_focus="Verifies `LowerOrNewerEqualPriority` protects older `t=0s` incumbents while allowing an older `t=2s` waiting job to preempt newer `t=5s` equal-priority backfills without ping-pong loops",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st13-preempt-within-cq-lower-or-newer-equal",
            setup_summary="8-node (`32`-TPU) cluster, `queueingStrategy: BestEffortFIFO`, `withinClusterQueue: LowerOrNewerEqualPriority`, single priority class `p-equal (500)`. 6 older `1-pod` jobs start at `t=0s` (`24/32` TPUs, one finishing at `t=12s`); 1 `3-pod` (`12`-TPU) job arrives at `t=2s` (blocked behind `t=0s` incumbents); 2 newer `1-pod` jobs arrive at `t=5s` and backfill to `32/32` TPUs until `t=12s`, when the older `t=2s` job preempts the newer `t=5s` jobs.",
            expected_behavior="`t=0s` older incumbents are protected at `t=2s`; `t=5s` newer jobs backfill the `8` free TPUs (`t=5s..12s`); at `t=12s` the older `t=2s` job preempts the 2 newer `t=5s` jobs (`InClusterQueue = 2` evictions) with zero counter-preemption ping-pong loop, and all `9/9` jobs complete cleanly.",
            expected_metrics=[
                "`Section 5.3 (Evictions)`: **`2`** (`InClusterQueue = 2` evictions of the newer `t=5s` jobs by the older `t=2s` job; `0` evictions of `t=0s` incumbents; `0` ping-pong re-evictions).",
                "`Section 5.4 (Effective Saturation)`: **`~65%–88%`** with `100%` (`9/9`) job completion.",
            ],
            must_not_happen=[
                "Ping-pong preemption loop (`Total Evictions >> 4`) between equal-priority workloads.",
                "Preemption of any older `t=0s` incumbent by a newer equal-priority arrival.",
            ],
            sec51_target="~75%–95%",
            sec52_target="Older 12-TPU job waits ~10s",
            sec53_target="2 evictions (InClusterQueue, newer-equal)",
            sec54_target="~65%–88%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st14: 4-Tier Cascading Multi-Priority Preemption Chain
    inf, wf, ex = (
        base_cluster_infra(
            num_nodes=8,
            priority_classes=[("tier-1", 100), ("tier-2", 300), ("tier-3", 600), ("tier-4", 1000)],
        ),
        [],
        [],
    )
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-cascade", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="LowerPriority")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-cascade", "cq-cascade")))
    for i in range(4):
        add_job(wf, ex, name=f"js-t1-{i}", arrival_sec=0.0, duration_sec=35.0, lq="lq-cascade", cq="cq-cascade", num_pods=2, prio="tier-1")
    for i in range(4):
        add_job(wf, ex, name=f"js-t2-{i}", arrival_sec=7.0, duration_sec=35.0, lq="lq-cascade", cq="cq-cascade", num_pods=2, prio="tier-2")
    for i in range(4):
        add_job(wf, ex, name=f"js-t3-{i}", arrival_sec=14.0, duration_sec=35.0, lq="lq-cascade", cq="cq-cascade", num_pods=2, prio="tier-3")
    for i in range(4):
        add_job(wf, ex, name=f"js-t4-{i}", arrival_sec=21.0, duration_sec=18.0, lq="lq-cascade", cq="cq-cascade", num_pods=2, prio="tier-4")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st14",
            slug="multi-tier-cascading-preemption",
            category="3. Intra-CQ Preemption Matrix",
            title="4-Tier Cascading Preemption Chain (`tier-1 -> tier-2 -> tier-3 -> tier-4`)",
            feature_focus="Multi-wave cascading preemptions (`12` total evictions across 3 distinct priority transitions) and time-to-eviction histogram accuracy",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st14-multi-tier-cascading-preemption --duration 42s",
            setup_summary="8-node (`32`-TPU) cluster, `withinClusterQueue: LowerPriority`. Four 4-job (`32`-TPU) waves arrive every `7s` with strictly ascending priority: `tier-1 (100)` at `t=0s`, `tier-2 (300)` at `t=7s`, `tier-3 (600)` at `t=14s`, `tier-4 (1000)` at `t=21s` (`18s` duration), evaluated over `--duration 42s`.",
            expected_behavior="`tier-1` runs `0s–7s` and gets evicted by `tier-2`; `tier-2` runs `7s–14s` and gets evicted by `tier-3`; `tier-3` runs `14s–21s` and gets evicted by `tier-4`; `tier-4` runs `21s–39s` to full completion.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions)`: **`~12` evictions** across `tier-1` (`4`), `tier-2` (`4`), and `tier-3` (`4`).",
                "`Section 3 Histogram (kueue_pods_ready_to_evicted_time_seconds)`: Tightly clustered around **`~7s`** (every preempted tier ran for `7s` before the next tier arrived).",
                "`Section 5.2`: `tier-4 (1000)` wait time `< 2.0s`.",
                "`Section 5.4`: Only `tier-4` (plus whichever `tier-3` jobs re-admit at `t=39s–42s` and remain active at `t=42s`) contributes to `EFFECTIVE SAT` (`~44%–54%`), while the `21s` (`0s–21s`) of preempted `tier-1/2/3` compute is accurately excluded as wasted badput.",
            ],
            must_not_happen=[
                "`Section 5.4 EFFECTIVE SAT` reporting `95%–100%` (which would mean crediting the aborted `0s–21s` runs of `tier-1`, `tier-2`, and `tier-3` that were killed before completing).",
            ],
            sec51_target="90%–99% (cluster busy all run)",
            sec52_target="tier-4 wait < 2.0s",
            sec53_target="~12 evictions (3 cascades x 4)",
            sec54_target="~44%–54% (only tier-4 + active t=42s)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st15: Adversarial Late-Preemption "Badput" Collapse Stress Test
    inf, wf, ex = base_cluster_infra(num_nodes=8, priority_classes=[("p-batch", 100), ("p-urgent", 1000)]), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-badput", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="LowerPriority")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-badput", "cq-badput")))
    for i in range(8):
        add_job(wf, ex, name=f"js-victim-{i}", arrival_sec=0.0, duration_sec=30.0, lq="cq-badput".replace("cq-", "lq-"), cq="cq-badput", prio="p-batch")
    for i in range(8):
        add_job(wf, ex, name=f"js-preemptor-{i}", arrival_sec=26.0, duration_sec=14.0, lq="lq-badput", cq="cq-badput", prio="p-urgent")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st15",
            slug="preemption-badput-collapse-stress",
            category="3. Intra-CQ Preemption Matrix",
            title="Adversarial Late-Preemption Goodput Collapse (`Sec 5.1 ≈ 98%` vs `Sec 5.4 ≈ 35%`)",
            feature_focus="Stress-tests the gap between raw quota occupancy (`Sec 5.1`) and uninterrupted effective saturation (`Sec 5.4`)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s",
            setup_summary="8-node (`32`-TPU) cluster, `withinClusterQueue: LowerPriority`. 8 `p-batch (100)` jobs (`30s` duration) occupy `100%` of the cluster from `t=0s` to `t=26s` (`87%` done), when 8 `p-urgent (1000)` jobs (`14s` duration) arrive at `t=26s`, preempt ALL 8 batch jobs, and finish at `t=40s` (window cut at `--duration 41s`).",
            expected_behavior="For `26s` out of `41s` (`63%` of wall-clock time), the cluster runs work that is killed right before completion. Only the 8 `p-urgent` jobs (`14s / 41s ≈ 34%` of wall-clock time) complete uninterrupted.",
            expected_metrics=[
                "`Section 5.1 (cq-badput)`: **`AVG ≈ 93%–99%`** (Kueue quota was allocated nearly 100% of the time).",
                "`Section 5.3 (Evictions)`: **`8`** evictions.",
                "`Section 5.4 (Effective Saturation)`: **`EFFECTIVE SAT ≈ 32%–40%`** (`448 / 1,312 TPU-s`), creating an intentional **`~55–60 percentage-point` gap** below `Section 5.1` that flags severe preemption badput.",
            ],
            must_not_happen=[
                "`Section 5.4` exceeding `45%` when evaluated with `--duration 41s` (would mean evicted workloads leaked into completed/active goodput).",
            ],
            sec51_target="93%–99% (raw occupancy)",
            sec52_target="p-urgent wait < 1.5s",
            sec53_target="8 evictions",
            sec54_target="~32%–40% (~60pp badput gap)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 4 (st16 - st20): Multi-CQ Isolation, Cohort Borrowing & Reclaim
    # =========================================================================

    # st16: Multi-CQ Strict Isolation (No Cohort)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-alpha", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf.append(make_infra_event(make_cluster_queue("cq-beta", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf.append(make_infra_event(make_local_queue("lq-alpha", "cq-alpha")))
    inf.append(make_infra_event(make_local_queue("lq-beta", "cq-beta")))
    for i in range(8):
        add_job(wf, ex, name=f"js-alpha-{i}", arrival_sec=0.0, duration_sec=20.0, lq="lq-alpha", cq="cq-alpha")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st16",
            slug="multi-cq-isolated-no-cohort",
            category="4. Multi-CQ & Cohort Borrowing/Reclaim",
            title="Multi-ClusterQueue Strict Quota Isolation Without a Cohort",
            feature_focus="Per-CQ breakdown in `Section 5.1` when `cq-alpha` is 200% overloaded and `cq-beta` is 0% idle with no shared Cohort",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort",
            setup_summary="8-node (`32`-TPU) cluster partitioned into two standalone `ClusterQueue`s (`cq-alpha`: `16` TPUs, `cq-beta`: `16` TPUs, **no `cohortName`**). 8 jobs (`32` TPUs total, `20s` each) arrive in `cq-alpha`; `0` jobs arrive in `cq-beta`.",
            expected_behavior="Because `cq-alpha` and `cq-beta` do not belong to a cohort, `cq-alpha` CANNOT borrow a single TPU from idle `cq-beta`. `cq-alpha` runs 2 waves of 4 jobs (`16` TPUs) over `40s` while `cq-beta` sits at `0%`.",
            expected_metrics=[
                "`Section 5.1`: Named rows `ClusterQueue: cq-alpha` = **`95%–100%`** (`16/16` TPUs) and `ClusterQueue: cq-beta` = **`0.00%`** (`0/16` TPUs), alongside `ClusterQueue Quotas (All)` = **`~50%`** (`16/32` TPUs).",
                "`Section 5.2`: Exact 4+4 bimodal wave split (`<=1s: 4` for Wave 1, `~T_wave` for Wave 2 where `T_wave = 20s / speedup`): `P50 <= 1.00s`, `AVG ≈ 0.5 * T_wave` (`~10s` at `1.0x`), `P90 ≈ 0.9 * T_wave` — matching `st01` identically because zero borrowing occurs.",
                "`Section 5.4`: **`EFFECTIVE SAT ≈ 50.0%`** (`46%–52%` across any `--speedup`).",
            ],
            must_not_happen=[
                "`cq-alpha` using `> 16` TPUs (`> 100%` of its nominal quota) without a Cohort.",
                "`cq-beta` showing non-zero resource usage.",
            ],
            sec51_target="ClusterQueue: cq-alpha ~100%, cq-beta 0%, All ~50%",
            sec52_target="P50 <=1s, AVG ~0.5*T_wave (~10s @1x)",
            sec53_target="0 evictions",
            sec54_target="~50.0%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st17: Seamless Cohort Borrowing From Idle Peer (Twin of st16)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-shared")))
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-alpha", tpu_quota=16, cpu_quota=64, mem_gi_quota=256, cohort_name="cohort-shared")
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-beta", tpu_quota=16, cpu_quota=64, mem_gi_quota=256, cohort_name="cohort-shared")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-alpha", "cq-alpha")))
    inf.append(make_infra_event(make_local_queue("lq-beta", "cq-beta")))
    for i in range(8):
        add_job(wf, ex, name=f"js-alpha-{i}", arrival_sec=0.0, duration_sec=30.0, lq="lq-alpha", cq="cq-alpha")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st17",
            slug="cohort-borrowing-idle-peer-no-reclaim",
            category="4. Multi-CQ & Cohort Borrowing/Reclaim",
            title="100% Cohort Borrowing From Idle Sister ClusterQueue (`200%` of Nominal Quota — Twin of st16)",
            feature_focus="Direct A/B twin of `st16` adding `cohortName: cohort-shared`: verifies `cq-alpha` reports `200%` of nominal quota while Cohort and Cluster report `100%`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st17-cohort-borrowing-idle-peer-no-reclaim",
            setup_summary="Identical two `16`-TPU CQs (`cq-alpha`, `cq-beta`) as `st16`, now joined in `cohort-shared`. 8 jobs (`32` TPUs total, `30s`) arrive in `cq-alpha` at `t=0s`; `0` jobs in `cq-beta`.",
            expected_behavior="`cq-alpha` immediately uses its own `16` nominal TPUs PLUS borrows all `16` idle TPUs from `cq-beta` (`32` TPUs total), admitting all 8 jobs at `t=0s` in a single `30s` wave with zero waiting and zero evictions.",
            expected_metrics=[
                "`Section 5.1`: `cq-alpha` = **`~190%–200%`** (`32 / 16` nominal TPUs — proves `Section 5.1` does NOT falsely clamp individual borrowing CQs at `100%`), `cq-beta` = **`0.00%`**, and `Cohort: cohort-shared` / `ClusterQueue Quotas (All)` = **`~95%–100%`** (`32 / 32` TPUs).",
                "`Section 5.2`: `< 1.0s` (all 8 jobs admit immediately).",
                "`Section 5.4`: **`EFFECTIVE SAT ≈ 93%–100%`** (double `st16`'s `50%`).",
            ],
            must_not_happen=[
                "`Section 5.1` clamping `cq-alpha` at `100%` when it is actively borrowing `32/16` TPUs (`200%`).",
                "`Section 5.4` exceeding `100%` (`200%` is per-CQ nominal ratio only; physical cluster saturation is `32/32 = 100%`).",
            ],
            sec51_target="cq-alpha ~200%, Cohort ~100%",
            sec52_target="< 1.0s",
            sec53_target="0 evictions",
            sec54_target="93%–100%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st18: Instant Cohort Quota Reclaim on Lender Arrival (reclaimWithinCohort: Any)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-reclaim")))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-borrower",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-reclaim",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-lender",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-reclaim",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-borrower", "cq-borrower")))
    inf.append(make_infra_event(make_local_queue("lq-lender", "cq-lender")))
    for i in range(8):
        add_job(wf, ex, name=f"js-bor-{i}", arrival_sec=0.0, duration_sec=35.0, lq="lq-borrower", cq="cq-borrower")
    for i in range(4):
        add_job(wf, ex, name=f"js-lend-{i}", arrival_sec=9.0, duration_sec=20.0, lq="lq-lender", cq="cq-lender")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st18",
            slug="cohort-reclaim-on-lender-arrival",
            category="4. Multi-CQ & Cohort Borrowing/Reclaim",
            title="Cohort Quota Reclaim When Lending Queue Submits Work (`reclaimWithinCohort: Any`)",
            feature_focus="Cross-CQ quota reclaim eviction (`4` borrowed jobs evicted at `t=9s` so `cq-lender` immediately reclaims its `16` nominal TPUs)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival",
            setup_summary="8-node (`32`-TPU) cluster, `cohort-reclaim` (`cq-borrower`: `16` TPUs, `cq-lender`: `16` TPUs, `reclaimWithinCohort: Any`). At `t=0s`, `cq-borrower` starts 8 jobs (`32` TPUs, `35s`). At `t=9s`, `cq-lender` submits 4 jobs (`16` TPUs, `20s`).",
            expected_behavior="At `t=9s`, Kueue sees `cq-borrower` is over its `16`-TPU nominal quota (`using 32`) while `cq-lender` is under its `16`-TPU nominal quota (`using 0`). Kueue immediately evicts **exactly 4** of `cq-borrower`'s jobs (`cq-borrower`'s own `16`-TPU nominal quota is untouchable), admits all 4 `cq-lender` jobs within `<2s`, and re-admits the 4 evicted `cq-borrower` jobs as soon as capacity frees at `t=29s`.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions and Reclaims)`: **`4`** evictions (`Unique workloads evicted once = 4`).",
                "`Section 5.1`: From `0s–9s`, `cq-borrower = 200%, cq-lender = 0%`; from `9s–29s`, `cq-borrower = 100% (16/16), cq-lender = 100% (16/16)`.",
                "`Section 5.2`: `cq-lender` jobs wait **`< 2.0s`**.",
            ],
            must_not_happen=[
                "More than 4 `cq-borrower` workloads evicted (`cq-lender` may only reclaim its own `16` nominal TPUs, never invade `cq-borrower`'s `16` nominal TPUs).",
                "`cq-lender` jobs waiting until `t=35s` when `reclaimWithinCohort: Any`.",
            ],
            sec51_target="Cohort 85%–100% throughout",
            sec52_target="cq-lender wait < 2.0s",
            sec53_target="4 reclaims (4 unique)",
            sec54_target="~72%–90%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st19: Cohort Reclaim Disabled Control (reclaimWithinCohort: Never — Twin of st18)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-no-reclaim")))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-borrower",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-no-reclaim",
                reclaim_within_cohort="Never",
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-lender",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-no-reclaim",
                reclaim_within_cohort="Never",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-borrower", "cq-borrower")))
    inf.append(make_infra_event(make_local_queue("lq-lender", "cq-lender")))
    for i in range(8):
        add_job(wf, ex, name=f"js-bor-{i}", arrival_sec=0.0, duration_sec=30.0, lq="lq-borrower", cq="cq-borrower")
    for i in range(4):
        add_job(wf, ex, name=f"js-lend-{i}", arrival_sec=9.0, duration_sec=20.0, lq="lq-lender", cq="cq-lender")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st19",
            slug="cohort-reclaim-disabled-lender-waits",
            category="4. Multi-CQ & Cohort Borrowing/Reclaim",
            title="Non-Preemptive Cohort Borrowing (`reclaimWithinCohort: Never` — Twin of st18)",
            feature_focus="Direct A/B twin of `st18` with `reclaimWithinCohort: Never`: zero evictions, lender waits for borrowed leases to finish naturally",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st19-cohort-reclaim-disabled-lender-waits",
            setup_summary="Identical cohort and arrivals as `st18`, changing `reclaimWithinCohort` from `Any` to `Never`.",
            expected_behavior="`cq-borrower` borrows all `32` TPUs at `t=0s`. When `cq-lender` submits 4 jobs at `t=9s`, `reclaimWithinCohort: Never` forbids killing running borrowers, so all 8 `cq-borrower` jobs finish cleanly at `t=30s` (`0` evictions), and `cq-lender`'s 4 jobs run from `t=30s` to `t=50s` (`wait ≈ 21s`).",
            expected_metrics=[
                "`Section 5.3 (Evictions)`: Strictly **`0`** (vs `4` in `st18`).",
                "`Section 5.2`: `cq-lender` wait increases from `<2s` (`st18`) to **`~21s`**.",
                "`Section 5.4`: Zero wasted compute (`Excluded (preempted/reclaimed/failed): 0 [0 pods]`), `EFFECTIVE SAT ≈ 77%–82%` (`1,280 / 1,600 TPU-s`).",
            ],
            must_not_happen=[
                "Any reclaim eviction (`Section 5.3 > 0`) when `reclaimWithinCohort: Never`.",
            ],
            sec51_target="~77%–82% across 50s makespan",
            sec52_target="cq-lender wait ~21s",
            sec53_target="0 evictions",
            sec54_target="~77%–82% (Excluded: 0)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st20: Explicit borrowingLimit and lendingLimit Enforcement
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-limits")))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-capped",
                tpu_quota=16,
                cpu_quota=96,
                mem_gi_quota=384,
                cohort_name="cohort-limits",
                tpu_borrowing_limit=8,
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-donor",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-limits",
                tpu_lending_limit=8,
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-capped", "cq-capped")))
    inf.append(make_infra_event(make_local_queue("lq-donor", "cq-donor")))
    for i in range(8):
        add_job(wf, ex, name=f"js-cap-{i}", arrival_sec=0.0, duration_sec=22.0, lq="lq-capped", cq="cq-capped")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st20",
            slug="borrowing-and-lending-limits-enforced",
            category="4. Multi-CQ & Cohort Borrowing/Reclaim",
            title="Explicit `borrowingLimit` (`+8` TPUs) & `lendingLimit` (`8` TPUs) Cap Verification",
            feature_focus="Verifies `cq-capped` stops at exactly `16 + 8 = 24` TPUs (`150%` of nominal, `75%` of cluster) even when `8` TPUs sit free",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced",
            setup_summary="8-node (`32`-TPU) cluster, `cohort-limits`. `cq-capped` has `nominalQuota=16, borrowingLimit=8`; `cq-donor` has `nominalQuota=16, lendingLimit=8`. 8 single-node (`4`-TPU, `22s`) jobs submitted to `cq-capped` at `t=0s`; `0` jobs in `cq-donor`.",
            expected_behavior="Wave 1 admits **6 jobs** (`24` TPUs = `16` nominal + `8` borrowed = `150%` of nominal quota), leaving 2 physical nodes (`8` TPUs) reserved/unborrowable and keeping 2 jobs queued for `22s`. Wave 2 runs those last 2 jobs (`8` TPUs) from `22s` to `44s`.",
            expected_metrics=[
                "`Section 5.1 (cq-capped)`: Peak utilization hits **exact `150.0%`** (`24 / 16` TPUs) during Wave 1 and never exceeds `150%`.",
                "`Section 5.4 (Effective Saturation)`: `8 × 4 × 22 = 704 TPU-s` over `44s × 32 = 1,408 TPU-s` → **`EFFECTIVE SAT ≈ 47%–52%`** (Wave 1 = `75%`, Wave 2 = `25%`, average = `50%`).",
            ],
            must_not_happen=[
                "`cq-capped` utilization exceeding `150%` (`> 24` TPUs) in `Section 5.1` (would mean `borrowingLimit` / `lendingLimit` was violated).",
            ],
            sec51_target="cq-capped P90/P99 = 150.0% (24/16 TPUs)",
            sec52_target="6 jobs ~0s, 2 jobs ~22s",
            sec53_target="0 evictions",
            sec54_target="~50.0% (Wave 1: 75%, Wave 2: 25%)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 5 (st21 - st25): Fair Sharing vs. No Fair Sharing
    # =========================================================================

    # st21: No Fair Sharing — First-Come Cohort Pool Starvation
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-nofs", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_cluster_queue("cq-hog", tpu_quota=0, cpu_quota=0, mem_gi_quota=0, cohort_name="cohort-nofs")))
    inf.append(make_infra_event(make_cluster_queue("cq-victim", tpu_quota=0, cpu_quota=0, mem_gi_quota=0, cohort_name="cohort-nofs")))
    inf.append(make_infra_event(make_local_queue("lq-hog", "cq-hog")))
    inf.append(make_infra_event(make_local_queue("lq-victim", "cq-victim")))
    for i in range(8):
        add_job(wf, ex, name=f"js-hog-{i}", arrival_sec=0.0, duration_sec=28.0, lq="lq-hog", cq="cq-hog")
    for i in range(4):
        add_job(wf, ex, name=f"js-vic-{i}", arrival_sec=4.0, duration_sec=18.0, lq="lq-victim", cq="cq-victim")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st21",
            slug="no-fair-sharing-fcfs-hogging",
            category="5. Fair Sharing & Priority Thresholds",
            title="Zero-Nominal Shared Pool Without Fair Sharing (First-Mover Monopolization)",
            feature_focus="Baseline control for `st22`: shared `32`-TPU Cohort pool (`0` CQ nominal quotas, `reclaimWithinCohort: Never`)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st21-no-fair-sharing-fcfs-hogging",
            setup_summary="8-node (`32`-TPU) cluster. All `32` TPUs live on `Cohort: cohort-nofs`; both `cq-hog` and `cq-victim` have `nominalQuota: 0` and `reclaimWithinCohort: Never`. `cq-hog` submits 8 jobs (`32` TPUs, `28s`) at `t=0s`; `cq-victim` submits 4 jobs (`16` TPUs, `18s`) at `t=4s`.",
            expected_behavior="`cq-hog` grabs 100% of the shared `32`-TPU cohort pool at `t=0s`. When `cq-victim` arrives at `t=4s`, `reclaimWithinCohort: Never` prevents cohort preemption, so `cq-victim` waits (`0` TPUs) for `24s` until `cq-hog` finishes at `t=28s`.",
            expected_metrics=[
                "`Section 5.1 (Cohort: cohort-nofs)`: `AVG ≈ 80%–95%` (`32/32` from `0s–28s`, `16/32` from `28s–46s`).",
                "`Section 5.2`: `cq-victim` jobs wait **`~24s`** while `cq-hog` jobs wait `~0s`.",
                "`Section 5.3`: **`0`** evictions.",
            ],
            must_not_happen=[
                "`cq-victim` preempting `cq-hog` when both have `0` nominal quota and `reclaimWithinCohort: Never`.",
            ],
            sec51_target="Cohort ~80%–92%",
            sec52_target="cq-victim waits ~24s",
            sec53_target="0 evictions",
            sec54_target="~78%–88%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st22: Equal Fair Sharing 50/50 Split (1:1 Weights — Twin of st21)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-fs-eq", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-team-a",
                tpu_quota=0,
                cpu_quota=0,
                mem_gi_quota=0,
                cohort_name="cohort-fs-eq",
                fair_sharing_weight="1",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-team-b",
                tpu_quota=0,
                cpu_quota=0,
                mem_gi_quota=0,
                cohort_name="cohort-fs-eq",
                fair_sharing_weight="1",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-team-a", "cq-team-a")))
    inf.append(make_infra_event(make_local_queue("lq-team-b", "cq-team-b")))
    for i in range(12):
        add_job(wf, ex, name=f"js-ta-{i:02d}", arrival_sec=0.0, duration_sec=16.0, lq="lq-team-a", cq="cq-team-a")
        add_job(wf, ex, name=f"js-tb-{i:02d}", arrival_sec=0.0, duration_sec=16.0, lq="lq-team-b", cq="cq-team-b")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st22",
            slug="equal-fair-sharing-50-50-split",
            category="5. Fair Sharing & Priority Thresholds",
            title="Equal-Weight (`1 : 1`) Cohort Fair Sharing Convergence Under Heavy Dual Backlog",
            feature_focus="Equal `fairSharing.weight: '1'` across `cq-team-a` and `cq-team-b` splitting `32` Cohort TPUs symmetrically (`16` vs `16`)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split",
            setup_summary="8-node (`32`-TPU) cluster, `cohort-fs-eq` (`32` Cohort TPUs). `cq-team-a` (`weight: 1`) and `cq-team-b` (`weight: 1`) each submit 12 single-node (`4`-TPU, `16s`) jobs simultaneously at `t=0s` (24 jobs = `96` TPUs = 3 full cluster waves).",
            expected_behavior="Across all 3 waves (`0s–48s`), Kueue DRF fair sharing allocates `16` TPUs (`4` nodes) to `cq-team-a` and `16` TPUs (`4` nodes) to `cq-team-b` in every wave, producing balanced wait-time distributions and `kueue_cluster_queue_weighted_share` values.",
            expected_metrics=[
                "`Section 1 Gauge (kueue_cluster_queue_weighted_share)`: `cq-team-a` and `cq-team-b` report balanced `AVG` weighted share.",
                "`Section 5.1 (Cohort: cohort-fs-eq)`: **`92%–100%`** (`32/32` TPUs full across all 3 waves).",
                "`Section 5.4 (Effective Saturation)`: **`90%–99%`** (all 24 jobs complete in `~48s`).",
            ],
            must_not_happen=[
                "Severe starvation of either team (one team taking `>75%` of cluster capacity across the run despite identical weights and simultaneous arrivals).",
            ],
            sec51_target="Cohort 92%–100% (50/50 split)",
            sec52_target="Symmetric across both CQs (AVG ~16s)",
            sec53_target="0 evictions",
            sec54_target="90%–99%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st23: Heavy Asymmetric Fair Sharing (7:1 Weight Ratio)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-fs-skew", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-gold",
                tpu_quota=0,
                cpu_quota=0,
                mem_gi_quota=0,
                cohort_name="cohort-fs-skew",
                fair_sharing_weight="7",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-bronze",
                tpu_quota=0,
                cpu_quota=0,
                mem_gi_quota=0,
                cohort_name="cohort-fs-skew",
                fair_sharing_weight="1",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-gold", "cq-gold")))
    inf.append(make_infra_event(make_local_queue("lq-bronze", "cq-bronze")))
    for i in range(14):
        add_job(wf, ex, name=f"js-gold-{i:02d}", arrival_sec=0.0, duration_sec=16.0, lq="lq-gold", cq="cq-gold")
    for i in range(6):
        add_job(wf, ex, name=f"js-bronze-{i:02d}", arrival_sec=0.0, duration_sec=16.0, lq="lq-bronze", cq="cq-bronze")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st23",
            slug="heavy-asymmetric-fair-sharing-7-to-1",
            category="5. Fair Sharing & Priority Thresholds",
            title="Heavy Asymmetric `7 : 1` Fair Sharing (`cq-gold = 28 TPUs [87.5%]` vs `cq-bronze = 4 TPUs [12.5%]`)",
            feature_focus="Exact `7 : 1` weight ratio mapping onto 8 physical nodes (`7` nodes for `cq-gold`, `1` node for `cq-bronze`)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1",
            setup_summary="8-node (`32`-TPU) cluster, `cohort-fs-skew` (`32` Cohort TPUs). `cq-gold` has `fairSharing.weight: 7` (exact integer `7/8 = 28` TPUs = `7` nodes); `cq-bronze` has `fairSharing.weight: 1` (`1/8 = 4` TPUs = `1` node). At `t=0s`, `cq-gold` submits 14 jobs (`2` waves of `7`) and `cq-bronze` submits 6 jobs (`6` waves; `1` active during `0s–32s`, then `4` concurrent in `32s–48s`).",
            expected_behavior="Because `7 + 1 = 8` matches the 8-node cluster with zero rounding error, Kueue converges immediately (`< 1s`, using at most a few sub-second `InCohortFairSharing` convergence preemptions if parallel `t=0s` `JobSet` creation reconciles a `cq-bronze` job ahead of `cq-gold`) so `cq-gold` holds `7` nodes (`28` TPUs, `87.5%`) and finishes all 14 of its jobs in two clean waves (`~32s`). Meanwhile, `cq-bronze` holds `1` node (`4` TPUs, `12.5%`) during `0s–32s` and then inherits the idle cohort from `t=32s` onward to drain its remaining 4 jobs concurrently (`32s–48s`).",
            expected_metrics=[
                "`Section 1 Gauge (kueue_admitted_active_workloads)` & `Section 5.1`: During `0s–32s`, `cq-gold` holds `P50 = 87.50% (cohort)` (`7` jobs) while `cq-bronze` holds `P50 = 12.50% (cohort)` (`1` job).",
                "`Section 5.1 (Cohort: cohort-fs-skew)`: `P50 = 100.00%`, `AVG ≈ 80%–92%`.",
                "`Section 5.4 (Effective Saturation)`: `20 × 4 × 16 = 1,280 TPU-s` over `~49s–52s` → **`EFFECTIVE SAT ≈ 76%–85%`**.",
            ],
            must_not_happen=[
                "`cq-bronze` holding `4` nodes (`50%`) throughout `0s–32s` despite having `1/7th` the weight of `cq-gold`.",
            ],
            sec51_target="Cohort ~80%–92% (87.5% vs 12.5% in 0s–32s)",
            sec52_target="cq-gold finishes all 14 in 2 waves (~32s)",
            sec53_target="0 steady-state (<= 6 t=0s convergence FairSharing)",
            sec54_target="~76%–85%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st24: Heavy 6-ClusterQueue Geometric Fair-Sharing Hierarchy
    inf, wf, ex = base_cluster_infra(num_nodes=16), [], []
    inf.append(make_infra_event(make_cohort("cohort-geo6", tpu_quota=64, cpu_quota=256, mem_gi_quota=1024)))
    weights = [1, 2, 4, 8, 16, 32]
    for idx_q, w in enumerate(weights):
        cq_n = f"cq-geo-{idx_q}"
        lq_n = f"lq-geo-{idx_q}"
        inf.append(
            make_infra_event(
                make_cluster_queue(
                    cq_n,
                    tpu_quota=0,
                    cpu_quota=0,
                    mem_gi_quota=0,
                    cohort_name="cohort-geo6",
                    fair_sharing_weight=str(w),
                    reclaim_within_cohort="Any",
                )
            )
        )
        inf.append(make_infra_event(make_local_queue(lq_n, cq_n)))
    # Emit t=0s arrivals in descending weight order so parallel worker submission sends
    # cq-geo-5 (weight 32) and cq-geo-4 (weight 16) first rather than filling empty capacity
    # with weight-1 jobs before weight-32 jobs hit kube-apiserver.
    for idx_q in reversed(range(len(weights))):
        cq_n = f"cq-geo-{idx_q}"
        lq_n = f"lq-geo-{idx_q}"
        for j in range(6):
            add_job(wf, ex, name=f"js-g{idx_q}-{j}", arrival_sec=0.0, duration_sec=15.0, lq=lq_n, cq=cq_n)
    scenarios.append(
        ScenarioSpec(
            scenario_id="st24",
            slug="geometric-6cq-fair-sharing-hierarchy",
            category="5. Fair Sharing & Priority Thresholds",
            title="Heavy 6-ClusterQueue Geometric Fair-Sharing Hierarchy (`Weights 1, 2, 4, 8, 16, 32`)",
            feature_focus="Multi-queue DRF stress test across 6 simultaneously backlogged ClusterQueues spanning a 32x weight range",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st24-geometric-6cq-fair-sharing-hierarchy",
            setup_summary="16-node (`64`-TPU) cluster, `cohort-geo6` (`64` Cohort TPUs). Six ClusterQueues `cq-geo-0` .. `cq-geo-5` with geometric weights `1, 2, 4, 8, 16, 32` each submit 6 single-node (`4`-TPU, `15s`) jobs simultaneously at `t=0s` (`36` jobs = `144` TPUs = `2.25` cluster waves).",
            expected_behavior="Highest-weight queues (`cq-geo-5` weight `32`: `6` jobs, `cq-geo-4` weight `16`: `6` jobs, `cq-geo-3` weight `8`: `3` jobs, `cq-geo-2` weight `4`: `1` job = `16` nodes) saturate Wave 1 (`0s–15s`), medium/lower weights (`cq-geo-3`, `cq-geo-2`, `cq-geo-1`, `cq-geo-0`) fill all 16 nodes in Wave 2 (`15s–30s`), and the remaining 4 lowest-weight jobs (`cq-geo-0`, `cq-geo-1`) drain in Wave 3 (`30s–45s`).",
            expected_metrics=[
                "`Section 1 Gauge`: All 6 `cq-geo-*` queues appear in `kueue_cluster_queue_status{status='active'} = 1` and `kueue_cluster_queue_weighted_share`.",
                "`Section 5.1 (Cohort: cohort-geo6)`: `P50 = 100.00%`, `AVG ≈ 70%–86%` across the 3-wave drain (`100%` in Waves 1 & 2, `25%` in final 4-job tail Wave 3).",
                "`Section 5.4 (Effective Saturation)`: **`~68%–80%`** (`36 × 4 × 15 = 2,160 TPU-s` over `~46s–49s × 64 TPUs`; `75.0%` ideal zero-overhead theoretical).",
            ],
            must_not_happen=[
                "`cq-geo-0` (`weight 1`) completing all 6 jobs ahead of `cq-geo-5` (`weight 32`).",
                "Any of the 6 CQs missing from `Section 1` or `Section 5.1` tables.",
            ],
            sec51_target="Cohort P50=100%, AVG ~70%–86%",
            sec52_target="Wait inversely proportional to weight",
            sec53_target="0 steady-state (<= 6 t=0s convergence FairSharing)",
            sec54_target="~68%–80% (2,160 TPU-s work)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st25: Cohort Borrow Priority Threshold Gating (borrowWithinCohort.maxPriorityThreshold under Classical Preemption)
    inf, wf, ex = (
        base_cluster_infra(
            num_nodes=8,
            priority_classes=[("p-low", 100), ("p-mid", 600), ("p-high", 800), ("p-vip", 900)],
        ),
        [],
        [],
    )
    inf.append(make_infra_event(make_cohort("cohort-thresh", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-main",
                tpu_quota=8,
                cpu_quota=32,
                mem_gi_quota=128,
                cohort_name="cohort-thresh",
                within_cq="LowerPriority",
                reclaim_within_cohort="LowerPriority",
                borrow_policy="LowerPriority",
                borrow_max_prio_threshold=500,
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-peer",
                tpu_quota=8,
                cpu_quota=32,
                mem_gi_quota=128,
                cohort_name="cohort-thresh",
                within_cq="LowerPriority",
                reclaim_within_cohort="LowerPriority",
                borrow_policy="LowerPriority",
                borrow_max_prio_threshold=500,
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-main", "cq-main")))
    inf.append(make_infra_event(make_local_queue("lq-peer", "cq-peer")))
    for i in range(4):
        add_job(wf, ex, name=f"js-peer-mid-{i}", arrival_sec=0.0, duration_sec=28.0, lq="lq-peer", cq="cq-peer", prio="p-mid")
    for i in range(4):
        add_job(wf, ex, name=f"js-peer-low-{i}", arrival_sec=0.0, duration_sec=28.0, lq="lq-peer", cq="cq-peer", prio="p-low")
    for i in range(4):
        add_job(wf, ex, name=f"js-main-vip-{i}", arrival_sec=8.0, duration_sec=18.0, lq="lq-main", cq="cq-main", prio="p-vip")
    for i in range(2):
        add_job(wf, ex, name=f"js-main-high-{i}", arrival_sec=10.0, duration_sec=18.0, lq="lq-main", cq="cq-main", prio="p-high")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st25",
            slug="borrow-within-cohort-priority-threshold",
            category="5. Fair Sharing & Priority Thresholds",
            title="Classical Cohort Borrowing Preemption With `maxPriorityThreshold: 500` (`--no-fair-sharing`)",
            feature_focus="Tests Classical Preemption (`--no-fair-sharing`) `InCohortReclaimWhileBorrowing` + `maxPriorityThreshold: 500` ceiling across peer ClusterQueues",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st25-borrow-within-cohort-priority-threshold --no-fair-sharing",
            setup_summary="8-node (`32`-TPU) cluster, `cohort-thresh` (`16` shared Cohort TPUs + `cq-main`: `8` TPUs + `cq-peer`: `8` TPUs = `32` TPUs) configured with `borrowWithinCohort: {policy: LowerPriority, maxPriorityThreshold: 500}` under `--no-fair-sharing`. At `t=0s`, `cq-peer` starts 4 `p-mid (600 > 500)` + 4 `p-low (100 <= 500)` jobs (`32` TPUs = `400%` of `cq-peer` nominal, `28s`). At `t=8s`, `cq-main` submits 4 `p-vip (900)` jobs (`16` TPUs = `200%` of `cq-main` nominal, `18s`), followed at `t=10s` by 2 `p-high (800)` jobs (`8` TPUs, `18s`).",
            expected_behavior="At `t=8s`, `cq-main`'s first 2 `p-vip` jobs reclaim `cq-main`'s `8` nominal TPUs (`InCohortReclamation = 2`), AND its next 2 `p-vip` jobs borrow `8` shared Cohort TPUs (`cq-main = 200%`) by preempting `cq-peer`'s remaining 2 `p-low (100 <= 500)` jobs via **`InCohortReclaimWhileBorrowing = 2`**. At `t=10s`, `cq-main`'s 2 `p-high (800)` jobs attempt to borrow further (`24/8 = 300%`) against `cq-peer` (`16/8 = 200%`), but because `p-mid (600) > maxPriorityThreshold (500)`, `p-mid` is protected from preemption while `cq-main` borrows — forcing `p-high (800)` to wait `~16s` until `t=26s`.",
            expected_metrics=[
                "`Section 5.1 (ClusterQueue: cq-main)`: Reaches **`P90 / P99 = 200.00%`** (`16/8` TPUs = `8` nominal + `8` borrowed via preemption).",
                "`Section 5.2`: `p-vip (900)` wait `< 3.0s` across all 4 jobs, whereas `p-high (800)` waits `~15s–19s` behind threshold-protected `p-mid (600)`.",
                "`Section 5.3 (Evictions)`: **`4`** evictions of `p-low` (`2` `InCohortReclamation` + `2` `InCohortReclaimWhileBorrowing`) and **`0`** evictions of `p-mid`.",
            ],
            must_not_happen=[
                "`cq-main` capped at `100%` (`8` TPUs) failing to emit `InCohortReclaimWhileBorrowing` against `p-low (100)`.",
                "Any `p-mid (600 > 500)` workload being evicted while `cq-main` is borrowing.",
            ],
            sec51_target="cq-main P90/P99 = 200%, Cohort ~85%–100%",
            sec52_target="p-vip < 3.0s vs p-high ~16s",
            sec53_target="4 p-low evictions (2 Reclaim + 2 ReclaimWhileBorrowing), 0 p-mid",
            sec54_target="~75%–92%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 6 (st26 - st30): Tricky Accounting, Secondary Bottlenecks & Edge Cases
    # =========================================================================

    # st26: Pre-existing t0 Running Pods in infra-pods (kueue.x-k8s.io/exclude=true)
    inf = base_cluster_infra(num_nodes=8)
    wf, ex = [], []
    inf.append(make_infra_event(make_cluster_queue("cq-coexist", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf.append(make_infra_event(make_local_queue("lq-coexist", "cq-coexist")))
    for i in range(4):
        inf.extend(
            make_initial_pod_events(
                pod_name=f"legacy-pod-{i}",
                node_name=f"st-node-{i:02d}",
                remaining_sec=28.0,
                tpu_req=4,
            )
        )
    for i in range(4):
        add_job(wf, ex, name=f"js-kueue-{i}", arrival_sec=0.0, duration_sec=32.0, lq="lq-coexist", cq="cq-coexist")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st26",
            slug="t0-initial-running-pods-baseline",
            category="6. Tricky Accounting & Edge Cases",
            title="Coexistence With Pre-Existing `t0` Unmanaged Pods (`infra-pods` Namespace)",
            feature_focus="Verifies `infra-pods` (`kueue.x-k8s.io/exclude=true`) placement on `st-node-00..03` alongside Kueue jobs on `st-node-04..07`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st26-t0-initial-running-pods-baseline",
            setup_summary="8-node (`32`-TPU) cluster. At `t0`, 4 pre-existing unmanaged pods (`legacy-pod-0..3`, `4` TPUs each = `16` TPUs, `28s` remaining runtime) occupy `st-node-00..03`. `cq-coexist` (`16` TPUs) receives 4 Kueue jobs (`16` TPUs, `32s`) at `t=0s`.",
            expected_behavior="`testrunner.py` creates namespace `infra-pods` with label `kueue.x-k8s.io/exclude: 'true'`, places all 4 `legacy-pod-*` onto `st-node-00..03` (`Initial Infra Pods: 4`), and schedules all 4 Kueue `JobSet` pods onto `st-node-04..07` with zero node resource collisions. In `Section 5.4`, both the 4 Kueue-managed workloads (`Qualifying Workloads: 4`) and the 4 pre-existing `infra-pods` (`Initial Infra Pods: 4`) are accounted for (`32/32` TPUs = **`~88%–96%`** effective uninterrupted cluster saturation).",
            expected_metrics=[
                "`Execution Summary`: `Initial Infra Pods: 4` and `KWOK Pods Status: ... succeeded=8` (`4` initial + `4` JobSet pods).",
                "`Section 5.1 (cq-coexist)`: **`95%–100%`** (`16/16` TPUs).",
                "`Section 5.4 (Effective Saturation)`: **`~88%–96%`** (`8` total pods × `4` TPUs / `32` cluster TPUs = `4` Kueue workloads + `4` initial `infra-pods`), with `Qualifying Workloads: 4` and `Initial Infra Pods: 4` confirming zero rogue Kueue `Workload` objects were created for `infra-pods` while full physical cluster occupancy is captured.",
            ],
            must_not_happen=[
                "`Initial Infra Pods: 0` or Kueue pods colliding onto `st-node-00..03` while `legacy-pod-*` hold all `4` TPUs on those nodes.",
                "Kueue creating rogue `Workload` objects for pods in `infra-pods`.",
            ],
            sec51_target="95%–100% (16/16 CQ TPUs)",
            sec52_target="< 1.0s",
            sec53_target="0 evictions",
            sec54_target="~88%–96% (4 Kueue + 4 infra-pods = 32/32 TPUs)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st27: Memory/CPU Secondary Resource Bottleneck (100% Memory, 25% TPU)
    inf = base_cluster_infra(num_nodes=8, tpu_per_node=8, cpu_per_node=16, mem_gi_per_node=64)
    wf, ex = [], []
    inf.append(make_infra_event(make_cluster_queue("cq-membnd", tpu_quota=64, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-membnd", "cq-membnd")))
    for i in range(8):
        add_job(
            wf,
            ex,
            name=f"js-mem-{i}",
            arrival_sec=0.0,
            duration_sec=35.0,
            lq="lq-membnd",
            cq="cq-membnd",
            tpu_per_pod=2,
            cpu_per_pod=16,
            mem_gi_per_pod=64,
        )
    scenarios.append(
        ScenarioSpec(
            scenario_id="st27",
            slug="cpu-memory-secondary-bottleneck",
            category="6. Tricky Accounting & Edge Cases",
            title="Multi-Resource Bottleneck Inversion (`cpu` & `memory` at `100%`, `google.com/tpu` at `25%`)",
            feature_focus="Per-resource reporting across `cpu`, `memory`, `pods`, and `google.com/tpu` in Sections `5.1` and `5.4` when non-TPU host memory/CPU saturates the cluster",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st27-cpu-memory-secondary-bottleneck",
            setup_summary="8-node cluster where each node has `8` TPUs (`64` total), `16` CPUs (`128` total), and `64Gi` RAM (`512Gi` total). 8 jobs arrive at `t=0s` requesting `16` CPU + `64Gi` RAM (`100%` of host CPU/RAM) but only `2` TPUs (`25%` of host TPUs) for `35s`.",
            expected_behavior="All 8 jobs admit at `t=0s` (1 pod per node), saturating **100%** of cluster `cpu` (`128/128`) and `memory` (`512Gi/512Gi`) while utilizing **exact 25.0%** (`16/64`) of `google.com/tpu`.",
            expected_metrics=[
                "`Section 5.1` & `Section 5.4` per-resource rows: `cpu` = **`93%–100%`**, `memory` = **`93%–100%`**, `google.com/tpu` = **`23%–26%`** (`16/64` TPUs), and `pods` = **`~0.9%`** (`8/880` pods).",
            ],
            must_not_happen=[
                "`memory` (binary `Gi` IEC units: `512Gi = 549,755,813,888` bytes) or `cpu` mis-scaled by `1000x` or `1024/1000` unit conversion bugs.",
                "All resource rows reporting identical percentages when `cpu/memory = 100%` and `tpu = 25%`.",
            ],
            sec51_target="cpu/mem ~100%, tpu ~25%",
            sec52_target="< 1.0s",
            sec53_target="0 evictions",
            sec54_target="cpu/mem ~96%, tpu ~24%–25%",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st28: InitContainer Max-Request & Pod Overhead Accounting
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(
        make_infra_event(
            {
                "apiVersion": "node.k8s.io/v1",
                "kind": "RuntimeClass",
                "metadata": {"name": "st-overhead-rc"},
                "handler": "kwok",
                "overhead": {"podFixed": {"cpu": "2"}},
            }
        )
    )
    inf.append(make_infra_event(make_cluster_queue("cq-init", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-init", "cq-init")))
    for i in range(8):
        add_job(
            wf,
            ex,
            name=f"js-init-{i}",
            arrival_sec=0.0,
            duration_sec=35.0,
            lq="lq-init",
            cq="cq-init",
            tpu_per_pod=2,
            cpu_per_pod=6,
            init_tpu=4,
            overhead_cpu=2,
        )
    scenarios.append(
        ScenarioSpec(
            scenario_id="st28",
            slug="init-container-and-pod-overhead",
            category="6. Tricky Accounting & Edge Cases",
            title="Kubernetes `initContainers` High-Water Mark & `spec.overhead` Resource Formula",
            feature_focus="Verifies `effective_req = max(max(initContainers), sum(containers)) + overhead` in `Section 5.4` matches Kueue `Section 5.1`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st28-init-container-and-pod-overhead",
            setup_summary="8-node (`32`-TPU, `128`-CPU) cluster. Each of 8 jobs (`35s`) defines an `initContainer` requesting `4` TPUs + a main `container` requesting only `2` TPUs (`effective = max(4, 2) = 4` TPUs), plus `6` container CPUs + `2` `spec.overhead.cpu` (`effective = 6 + 2 = 8` CPUs = `50%` of node's `16` CPUs).",
            expected_behavior="Both Kueue (`Section 5.1`) and `kueue_metrics_report.py` (`Section 5.4`) apply the exact Kubernetes pod resource formula (`max(initContainers, sum(containers)) + overhead`), reporting **`100%` TPU saturation** (`8 × 4 = 32` TPUs, NOT `8 × 2 = 16` = `50%`) and **`50%` CPU saturation** (`8 × (6+2) = 64 / 128` CPUs, NOT `48/128 = 37.5%`).",
            expected_metrics=[
                "`Section 5.1` & `Section 5.4`: `google.com/tpu` = **`93%–100%`** (`32/32` TPUs), `cpu` = **`46%–50%`** (`64/128` CPUs).",
            ],
            must_not_happen=[
                "`Section 5.4` ignoring `initContainers` and reporting only `50%` TPU saturation (`16/32` from main container alone) while `Section 5.1` reports `100%`.",
                "`Section 5.4` summing `initContainer + container` (`4 + 2 = 6` TPUs = `150%`) instead of taking `max(4, 2) = 4`.",
            ],
            sec51_target="tpu ~100%, cpu ~50%",
            sec52_target="< 1.0s",
            sec53_target="0 evictions",
            sec54_target="tpu ~96% (32/32), cpu ~48% (64/128)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st29: Multi-Role Heterogeneous JobSet (CPU Coordinator + Multi-TPU Worker PodSets)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-mrole", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-mrole", "cq-mrole")))
    for i in range(2):
        add_job(
            wf,
            ex,
            name=f"js-dist-train-{i}",
            arrival_sec=0.0,
            duration_sec=35.0,
            lq="lq-mrole",
            cq="cq-mrole",
            num_pods=4,
            tpu_per_pod=4,
            cpu_per_pod=4,
            extra_coord=True,
        )
    scenarios.append(
        ScenarioSpec(
            scenario_id="st29",
            slug="multi-role-heterogeneous-jobset",
            category="6. Tricky Accounting & Edge Cases",
            title="Multi-Role Heterogeneous `JobSet` (`1` CPU Coordinator + `4` TPU Workers per JobSet)",
            feature_focus="Accumulation across multiple `spec.podSets` (`coord` with `0` TPUs + `w` with `4×4=16` TPUs) inside a single Kueue `Workload`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset",
            setup_summary="8-node (`32`-TPU, `128`-CPU) cluster. 2 multi-role `JobSet`s (`js-dist-train-0`, `js-dist-train-1`, `35s` duration), each containing two `replicatedJobs`: `coord` (`1` pod × `0` TPUs, `4` CPUs) and `w` (`4` pods × `4` TPUs, `4` CPUs). Total across both JobSets: `2` workloads, `10` pods (`2` coordinators + `8` workers), `32/32` TPUs (`100%`), `40/128` CPUs (`31.25%`).",
            expected_behavior="Both JobSets admit immediately at `t=0s`, launching all `10` pods across the 8 nodes and running to completion at `t=35s`.",
            expected_metrics=[
                "`Execution Summary`: `Workflows Submitted: 2/2`, `Workloads Status: ... finished=2`, `KWOK Pods Status: ... succeeded=10`.",
                "`Section 5.4`: Subtitle shows `Qualifying Workloads: 2 (completed=2 [10 pods] ...)`. `google.com/tpu` = **`93%–100%`** (`32/32` TPUs), `cpu` = **`~29%–32%`** (`40/128` CPUs).",
            ],
            must_not_happen=[
                "`Section 5.4` reading only `podSets[0]` (`coord` = `0` TPUs) and reporting **`0.00%` TPU saturation**.",
                "`Section 5.4` multiplying `coord` resources by `10` pods (`0` TPUs) or `w` resources by `10` pods (`40` TPUs > `32` cluster cap = `125%`).",
            ],
            sec51_target="tpu ~100%, cpu ~31.25%",
            sec52_target="< 1.0s",
            sec53_target="0 evictions",
            sec54_target="tpu ~96% (2 workloads, 10 pods)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st30: Permanently Inadmissible Oversized Workload Rejection & Non-Inflation
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-guard", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-guard", "cq-guard")))
    for i in range(4):
        add_job(wf, ex, name=f"js-valid-{i}", arrival_sec=0.0, duration_sec=35.0, lq="lq-guard", cq="cq-guard", num_pods=1, tpu_per_pod=4)
    for i in range(2):
        add_job(wf, ex, name=f"js-oversized-{i}", arrival_sec=1.0, duration_sec=35.0, lq="lq-guard", cq="cq-guard", num_pods=16, tpu_per_pod=4)
    scenarios.append(
        ScenarioSpec(
            scenario_id="st30",
            slug="inadmissible-oversized-workload-rejection",
            category="6. Tricky Accounting & Edge Cases",
            title="Permanently Inadmissible Oversized Workloads (`64` TPUs Requested on a `32`-TPU Cluster)",
            feature_focus="Graceful handling of permanently `inadmissible` jobs (`Unadmitted: 2 [32 pods]`) without hang, crash, or saturation inflation",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection --duration 42s",
            setup_summary="8-node (`32`-TPU) cluster, `cq-guard` (`32` TPUs). 4 valid 1-node (`4`-TPU, `35s`) jobs arrive at `t=0s` (`16` TPUs = `50%` of cluster), plus 2 impossible 16-node (`64`-TPU) jobs at `t=1s` that exceed both CQ quota and physical cluster size (bounded by `--duration 42s`).",
            expected_behavior="All 4 valid jobs admit at `t=0s` and complete cleanly at `t=35s`. Both `js-oversized-*` workloads remain permanently `Pending` (`status=inadmissible` in `kueue_pending_workloads`) until `--duration 42s` concludes the test.",
            expected_metrics=[
                "`Execution Summary`: `Workloads Status: admitted=0, pending=2, finished=4` (surfacing the 2 unrunnable workloads).",
                "`Section 1 Gauge (kueue_pending_workloads{status='inadmissible'})`: Constant `2.00` throughout the run.",
                "`Section 5.4 (Effective Saturation)`: Subtitle shows `Qualifying Workloads: 4 (completed=4 [4 pods], active_at_end=0 [0 pods]) | Excluded (preempted/reclaimed/failed): 0 [0 pods] | Unadmitted: 2 [32 pods]`. `EFFECTIVE SAT` reflects ONLY the 4 completed valid jobs (`560 / 1,344 TPU-s ≈ 40%–45%`).",
            ],
            must_not_happen=[
                "Unadmitted `64`-TPU workloads being counted as `Active at End` (which would push `EFFECTIVE SAT` above `400%`).",
                "`testrunner.py` or `kueue_metrics_report.py` crashing when workloads have `status.admission == None`.",
            ],
            sec51_target="~40%–45% (16/32 TPUs for 35s of 42s)",
            sec52_target="Valid jobs < 1.0s",
            sec53_target="0 evictions",
            sec54_target="~40%–45% (Unadmitted: 2 [32 pods])",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 7 (st31 - st35): Initial t0 Workloads — Handling & Accounting
    # =========================================================================

    # st31: Pre-Admitted t0 Kueue Workloads Blocking New Arrivals + Offset Jump
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-t0-cap", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-t0-cap", "cq-t0-cap")))
    # 8 t0 pre-admitted JobSets started at t=0s with deletionTime=26s; first NotAdmitted arrival at t=6s ->
    # skip_initial_offset jumps +6s so remaining runtime is exact 20s!
    for i in range(8):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-js-{i:02d}",
            remaining_sec=26.0,
            lq="lq-t0-cap",
            cq="cq-t0-cap",
            node_names=[f"st-node-{i:02d}"],
        )
    for i in range(4):
        add_job(wf, ex, name=f"arr-js-{i:02d}", arrival_sec=6.0, duration_sec=20.0, lq="lq-t0-cap", cq="cq-t0-cap")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st31",
            slug="t0-admitted-quota-blocking-arrivals",
            category="7. Initial t0 Workloads — Handling & Accounting",
            title="Pre-Admitted `t0` Kueue `PodGroup` Workloads Holding Quota & Blocking New Arrivals",
            feature_focus="Bootstraps 8 `t0`-admitted `PodGroup` `Workload`s (`32/32` TPUs) before CQ activation, jumps `+6s` to first arrival (`remaining = 20s`), and blocks 4 arriving jobs in Kueue until `t0` quota frees",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st31-t0-admitted-quota-blocking-arrivals",
            setup_summary="8-node (`32`-TPU) cluster, `cq-t0-cap` (`32` TPUs). At `t=0s`, 8 `Admitted` `JobSet`s (`init-js-00..07`, `deletionTime = 26s`) occupy all 8 nodes (`32/32` TPUs). At `t=6s`, 4 `NotAdmitted` `JobSet`s (`arr-js-00..03`, `16` TPUs, `20s`) arrive.",
            expected_behavior="`testrunner.py` jumps `+6s` to `t=6s` (adjusting `init-js-*` remaining runtime to `26 - 6 = 20s`), bootstraps all 8 as pre-admitted `PodGroup` `Workload`s (`Initial Kueue Workloads: 8 PodGroup(s) (8 pod(s))`, `Initial Infra Pods: 0`), and activates `cq-t0-cap` at `32/32` TPUs. All 4 `arr-js-*` arrivals are held `Pending` in Kueue for `~20s` (never admitted prematurely onto full nodes), then run from `wall=20s` to `wall=40s`.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 8 PodGroup(s) (8 pod(s))`, `Initial Infra Pods: 0`, `Workflows Submitted: 4/4`, `Workloads Status: ... finished=12`, `KWOK Pods Status: ... succeeded=12, pending=0, failed=0`.",
                "`Section 5.1 (cq-t0-cap)`: `AVG ≈ 70%–78%` (`32/32 = 100%` during `0s–20s`, `16/32 = 50%` during `20s–40s`, theoretical mean `75.0%`).",
                "`Section 5.2 (Wait Time)`: `AVG ≈ 17s–21s` across the 4 arriving workloads (proving `t0` `PodGroup`s genuinely held `32/32` CQ quota).",
                "`Section 5.3 (Evictions)`: `0`.",
                "`Section 5.4 (Effective Saturation)`: `Qualifying Workloads: 12 (completed=12 [12 pods])` with `Initial Infra Pods` absent (`0`) and **`EFFECTIVE SAT ≈ 70%–77%`** (`960 / 1,280 TPU-s = 75.0%`).",
            ],
            must_not_happen=[
                "`Section 5.2 AVG < 3s` (the pre-Option-1 bug where `t0` pods went to `infra-pods` with zero `Workload`s, letting Kueue admit `arr-js-*` at `wall=0s` into node scheduling collisions).",
                "`Initial Infra Pods > 0` when all 8 `t0` pods belong to Kueue-admitted workloads.",
            ],
            sec51_target="~70%–78% (100% w1, 50% w2)",
            sec52_target="AVG ~18s–20s (blocked by t0 quota)",
            sec53_target="0 evictions",
            sec54_target="~70%–77% (12 completed workloads)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st32: Mixed t0 Kueue PodGroup Workloads + t0 Non-Kueue infra-pods Separation
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-hybrid", tpu_quota=24, cpu_quota=96, mem_gi_quota=384)))
    inf.append(make_infra_event(make_local_queue("lq-hybrid", "cq-hybrid")))
    # 2 non-Kueue daemon/system pods on st-node-00..01 (8 TPUs in infra-pods)
    for i in range(2):
        inf.extend(
            make_initial_pod_events(
                pod_name=f"sys-daemon-{i}",
                node_name=f"st-node-{i:02d}",
                remaining_sec=28.0,
                tpu_req=4,
            )
        )
    # 4 Kueue-admitted t0 JobSets on st-node-02..05 (16 TPUs in cq-hybrid)
    for i in range(4):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-kueue-{i}",
            remaining_sec=28.0,
            lq="lq-hybrid",
            cq="cq-hybrid",
            node_names=[f"st-node-{i + 2:02d}"],
        )
    # 2 arriving NotAdmitted Kueue JobSets at t=0s (8 TPUs -> 16+8 = 24/24 CQ quota, placed on st-node-06..07)
    for i in range(2):
        add_job(wf, ex, name=f"arr-kueue-{i}", arrival_sec=0.0, duration_sec=30.0, lq="lq-hybrid", cq="cq-hybrid")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st32",
            slug="t0-mixed-kueue-workloads-and-infra-pods",
            category="7. Initial t0 Workloads — Handling & Accounting",
            title="Hybrid `t0` Partitioning: 4 Pre-Admitted Kueue `PodGroup`s + 2 Non-Kueue `infra-pods` + 2 New Arrivals",
            feature_focus="Simultaneous separation of Class 1 `t0` Kueue `PodGroup`s (`16` TPUs in `cq-hybrid`) and Class 2 `infra-pods` (`8` TPUs excluded from Kueue) alongside `8` TPUs of `t=0s` arrivals",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods",
            setup_summary="8-node (`32`-TPU) cluster. At `t0`: `st-node-00..01` run 2 unmanaged `sys-daemon-*` pods (`8` TPUs, no `workloadName`); `st-node-02..05` run 4 `Admitted` `init-kueue-*` `JobSet`s (`16` TPUs in `cq-hybrid`, `nominalQuota = 24` TPUs). At `t=0s`, 2 `NotAdmitted` `arr-kueue-*` `JobSet`s (`8` TPUs, `30s`) arrive.",
            expected_behavior="`sys-daemon-0..1` are isolated in `infra-pods` (`0` CQ quota impact); `init-kueue-0..3` become pre-admitted `PodGroup` `Workload`s consuming `16/24` TPUs of `cq-hybrid`; `arr-kueue-0..1` admit immediately (`16 + 8 = 24/24` CQ TPUs) onto free `st-node-06..07` (`32/32` physical TPUs packed cleanly).",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 4 PodGroup(s) (4 pod(s))`, `Initial Infra Pods: 2`, `Workflows Submitted: 2/2`, `KWOK Pods Status: ... succeeded=8`.",
                "`Section 5.1 (cq-hybrid)`: **`~92%–100%`** (`24 / 24` CQ TPUs).",
                "`Section 5.4 (Effective Saturation)`: `Qualifying Workloads: 6 (completed=6 [6 pods])` + `Initial Infra Pods: 2 (completed=2, active_at_end=0)` → **`EFFECTIVE SAT ≈ 90%–98%`** (`32 / 32` cluster TPUs).",
            ],
            must_not_happen=[
                "`sys-daemon-*` consuming `cq-hybrid` quota (which would push demand to `32 > 24` and block `arr-kueue-*`).",
                "`init-kueue-*` leaking into `infra-pods` (which would drop `cq-hybrid` `Sec 5.1` from `100%` to `33.3%`).",
            ],
            sec51_target="~92%–100% (24/24 CQ TPUs)",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="~90%–98% (6 Kueue + 2 infra = 32/32 TPUs)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st33: Legacy Unlinked Hashed Pods (Tier 3) + Missing-Pod Synthesis (Tier 4)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-legacy", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-legacy", "cq-legacy")))
    # 4 hashed Admitted JobSets matched via Tier 3 resource-profile matcher to unlinked hashed pods on st-node-00..03
    for i in range(4):
        h_js_name = f"h-{i + 1:032x}"
        h_pod_prefix = f"h-pod{i + 1:028x}"
        add_t0_job(
            inf,
            wf,
            ex,
            name=h_js_name,
            remaining_sec=28.0,
            lq="lq-legacy",
            cq="cq-legacy",
            node_names=[f"st-node-{i:02d}"],
            pod_mode="unlinked",
            pod_name_prefix=h_pod_prefix,
        )
    # 2 Admitted JobSets with NO Pod events in infra.json -> Tier 4 synthesizes pods onto free nodes (st-node-04..05)
    for i in range(2):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-synth-{i}",
            remaining_sec=28.0,
            lq="lq-legacy",
            cq="cq-legacy",
            pod_mode="none",
        )
    # 2 NotAdmitted arrivals at t=0s -> land on remaining 2 free nodes (st-node-06..07)
    for i in range(2):
        add_job(wf, ex, name=f"arr-legacy-{i}", arrival_sec=0.0, duration_sec=30.0, lq="lq-legacy", cq="cq-legacy")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st33",
            slug="t0-legacy-unlinked-and-synthesized-pods",
            category="7. Initial t0 Workloads — Handling & Accounting",
            title="Legacy Unlinked Hashed `t0` Pods (Tier 3) & Synthetic Missing-Pod Placement (Tier 4)",
            feature_focus="Backward-compatible Tier 3 resource-profile matching for `--hash-names` traces without `workloadName` + Tier 4 automatic pod synthesis when `infra.json` omits `t0` pod events",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st33-t0-legacy-unlinked-and-synthesized-pods",
            setup_summary="8-node (`32`-TPU) cluster, `cq-legacy` (`32` TPUs). At `t0`: 4 hashed `Admitted` `JobSet`s (`h-000...01..04`) accompany 4 unlinked hashed `Pod` events on `st-node-00..03` (`pod_mode='unlinked'`), plus 2 `Admitted` `JobSet`s (`init-synth-0..1`) with zero `Pod` events in `infra.json` (`pod_mode='none'`). 2 `NotAdmitted` arrivals (`arr-legacy-0..1`, `30s`) submit at `t=0s`.",
            expected_behavior="Tier 3 claims all 4 unlinked hashed pods into `PodGroup` `Workload`s (`Initial Infra Pods: 0`), Tier 4 synthesizes 2 non-colliding pods onto `st-node-04..05`, and `arr-legacy-0..1` schedule onto `st-node-06..07` (`32/32` TPUs).",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 6 PodGroup(s) (6 pod(s))`, `Initial Infra Pods: 0`, `Workflows Submitted: 2/2`, `KWOK Pods Status: ... succeeded=8, failed=0`.",
                "`Section 5.1 (cq-legacy)`: **`~90%–100%`** (`32/32` TPUs).",
                "`Section 5.4 (Effective Saturation)`: `Qualifying Workloads: 8 (completed=8 [8 pods])`, **`EFFECTIVE SAT ≈ 88%–98%`**.",
            ],
            must_not_happen=[
                "Unlinked hashed pods falling through to `Initial Infra Pods: 4` when matching `Admitted` `workflows.json` candidates exist.",
                "Tier 4 synthetic pods colliding onto `st-node-00..03` (already full with `4` TPUs each).",
            ],
            sec51_target="~90%–100% (32/32 TPUs)",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="~88%–98% (8 Kueue workloads, 0 infra)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st34: Multi-Role Heterogeneous t0 PodGroup (CPU Coordinator + 4 TPU Workers)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-t0-mrole", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-t0-mrole", "cq-t0-mrole")))
    for i in range(2):
        w_nodes = [f"st-node-{4 * i + k:02d}" for k in range(4)]
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-mrole-{i}",
            remaining_sec=24.0,
            lq="lq-t0-mrole",
            cq="cq-t0-mrole",
            node_names=w_nodes,
            coord_node_name=w_nodes[0],
            num_pods=4,
            tpu_per_pod=4,
            cpu_per_pod=4,
            extra_coord=True,
        )
    add_job(
        wf,
        ex,
        name="arr-mrole-after",
        arrival_sec=0.0,
        duration_sec=16.0,
        lq="lq-t0-mrole",
        cq="cq-t0-mrole",
        num_pods=4,
        tpu_per_pod=4,
        cpu_per_pod=4,
        extra_coord=True,
    )
    scenarios.append(
        ScenarioSpec(
            scenario_id="st34",
            slug="t0-multi-role-heterogeneous-podgroup",
            category="7. Initial t0 Workloads — Handling & Accounting",
            title="Heterogeneous Multi-PodSet `t0` `PodGroup` (`1` CPU Coordinator + `4` TPU Workers per Workload)",
            feature_focus="Homogeneous-per-PodSet grouping (`resourceUsage % count == 0` Kueue `WorkloadWebhook` invariant) and multi-role `t0` quota/saturation math",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st34-t0-multi-role-heterogeneous-podgroup",
            setup_summary="8-node (`32`-TPU, `128`-CPU) cluster, `cq-t0-mrole` (`32` TPUs, `128` CPUs). At `t0`, 2 multi-role `Admitted` `JobSet`s (`init-mrole-0..1`, `24s` remaining) run `1` CPU `coord` pod (`0` TPUs, `4` CPUs) + `4` TPU `w` pods (`16` TPUs, `16` CPUs) each = `10` initial pods (`32/32` TPUs, `40/128` CPUs). At `t=0s`, 1 `NotAdmitted` multi-role `JobSet` (`arr-mrole-after`, `5` pods = `16` TPUs, `16s`) arrives.",
            expected_behavior="Both `init-mrole-*` `PodGroup` `Workload`s pass Kueue's `WorkloadWebhook` with 2 distinct `podSets` (`count=1` and `count=4`), hold `32/32` TPUs + `40/128` CPUs, block `arr-mrole-after` for `~24s`, and finish cleanly before `arr-mrole-after` runs `24s–40s`.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 2 PodGroup(s) (10 pod(s))`, `Workflows Submitted: 1/1`, `KWOK Pods Status: ... succeeded=15, failed=0`.",
                "`Section 5.2`: `arr-mrole-after` waits **`~22s–26s`** behind the `32/32` TPU `t0` multi-role workloads.",
                "`Section 5.4`: `Qualifying Workloads: 3 (completed=3 [15 pods])`, `google.com/tpu` **`EFFECTIVE SAT ≈ 72%–82%`** (`(32×24 + 16×16) / (32×40) = 1,024 / 1,280 = 80.0%`).",
            ],
            must_not_happen=[
                "Kueue `WorkloadWebhook` rejecting `init-mrole-*` with `resourceUsage is not a multiple of count` (would happen if `0`-TPU `coord` and `4`-TPU `w` pods were merged into one `count=5` PodSet with `16` TPUs).",
            ],
            sec51_target="tpu ~75%–82% (100% w1, 50% w2)",
            sec52_target="arr-mrole-after waits ~24s",
            sec53_target="0 evictions",
            sec54_target="tpu ~75%–82% (3 workloads, 15 pods)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st35: Infinite-Duration t0 Kueue Workloads + Bounded --duration Window
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cluster_queue("cq-t0-inf", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(make_infra_event(make_local_queue("lq-t0-inf", "cq-t0-inf")))
    for i in range(4):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-inf-{i}",
            remaining_sec=864000.0,
            lq="lq-t0-inf",
            cq="cq-t0-inf",
            node_names=[f"st-node-{i:02d}"],
            is_infinite=True,
        )
    for i in range(4):
        add_job(wf, ex, name=f"arr-short-w1-{i}", arrival_sec=0.0, duration_sec=18.0, lq="lq-t0-inf", cq="cq-t0-inf")
    for i in range(4):
        add_job(wf, ex, name=f"arr-short-w2-{i}", arrival_sec=18.0, duration_sec=18.0, lq="lq-t0-inf", cq="cq-t0-inf")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st35",
            slug="t0-infinite-workload-and-active-at-end",
            category="7. Initial t0 Workloads — Handling & Accounting",
            title="Long-Running / Infinite `t0` Kueue Workloads (`is_infinite=True`) With `ACTIVE AT END` & `FULL-DUR SAT` Guard",
            feature_focus="Verifies `t0` `PodGroup`s with no `deletionTime` (`864000s` placeholder) report exact `~50%` `ACTIVE AT END` without inflating `FULL-DUR SAT`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st35-t0-infinite-workload-and-active-at-end --duration 40s",
            setup_summary="8-node (`32`-TPU) cluster, `cq-t0-inf` (`32` TPUs). At `t0`, 4 `Admitted` `JobSet`s (`init-inf-0..3`, no pod `Deleted` event -> `is_infinite=True`) hold `st-node-00..03` (`16` TPUs = `50%`). Two waves of 4 short `18s` jobs run on `st-node-04..07` (`16` TPUs = `50%`) inside `--duration 40s`.",
            expected_behavior="All 4 `init-inf-*` workloads remain actively running at `t=40s` (`active_at_end=4 [4 pods]`), while all 8 short jobs complete (`completed=8 [8 pods]`). `kueue-optimizer.gke.io/infinite-duration: 'true'` prevents the `864000s` KWOK placeholder from inflating `FULL-DUR SAT`.",
            expected_metrics=[
                "`Section 5.1 (cq-t0-inf)`: **`~92%–100%`** (`32/32` TPUs for `36s`, `16/32` for `4s`).",
                "`Section 5.4`: `COMPLETED ONLY ≈ 42%–48%`, `ACTIVE AT END ≈ 48%–50%`, `EFFECTIVE SAT ≈ 90%–98%`, AND **`FULL-DUR SAT ≈ 90%–98%`** (matching `EFFECTIVE SAT` rather than exploding to `>1,000,000%`).",
            ],
            must_not_happen=[
                "`FULL-DUR SAT` exceeding `110%` due to the `864000s` (`10d`) infinite-pod timer annotation.",
                "`ACTIVE AT END == 0.00%` when 4 `init-inf-*` `PodGroup`s ran uninterrupted across the entire `40s` window.",
            ],
            sec51_target="~92%–100%",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="COMPLETED ~45% + ACTIVE ~50% = ~95% (FULL-DUR ~95%)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 8 (st36 - st40): Initial t0 Workloads — Preemption & TAS
    # =========================================================================

    # st36: t0 Intra-CQ Preemption Disabled Control (withinClusterQueue: Never)
    inf, wf, ex = base_cluster_infra(num_nodes=8, priority_classes=[("p-low", 100), ("p-high", 1000)]), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-t0-nopre", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="Never")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-t0-nopre", "cq-t0-nopre")))
    for i in range(8):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-low-{i}",
            remaining_sec=30.0,
            lq="lq-t0-nopre",
            cq="cq-t0-nopre",
            node_names=[f"st-node-{i:02d}"],
            prio="p-low",
            prio_val=100,
        )
    for i in range(4):
        add_job(wf, ex, name=f"arr-high-{i}", arrival_sec=11.0, duration_sec=18.0, lq="lq-t0-nopre", cq="cq-t0-nopre", prio="p-high")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st36",
            slug="t0-preempt-within-cq-never",
            category="8. Initial t0 Workloads — Preemption & TAS",
            title="Non-Preemptive Protection of `t0` `PodGroup` Incumbents (`withinClusterQueue: Never`)",
            feature_focus="Baseline control twin of `st37`: high-priority arrivals wait for `t0` low-priority `PodGroup`s to finish without eviction",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset",
            setup_summary="8-node (`32`-TPU) cluster, `withinClusterQueue: Never`. At `t0`, 8 `p-low (100)` `Admitted` `JobSet`s (`init-low-0..7`, `30s` remaining) fill all `32` TPUs. At `t=11s` (`--no-skip-initial-offset`), 4 `p-high (1000)` `JobSet`s (`18s`) arrive.",
            expected_behavior="All 8 `t0` `p-low` `PodGroup`s run uninterrupted until `t≈30s` (`0` evictions). All 4 `p-high` arrivals wait `~19s` (`t=11s -> t=30s`) and finish at `t≈48s`.",
            expected_metrics=[
                "`Section 5.2`: `p-high` waits **`~16s–21s`**.",
                "`Section 5.3`: **`0`** evictions (`0` preempted-t0 resubmits).",
                "`Section 5.4`: `Qualifying Workloads: 12 (completed=12 [12 pods])`, `Excluded: 0`, **`EFFECTIVE SAT ≈ 74%–82%`**.",
            ],
            must_not_happen=[
                "Any eviction (`Section 5.3 > 0`) of a `t0` `PodGroup` when `withinClusterQueue: Never`.",
            ],
            sec51_target="~75%–82%",
            sec52_target="p-high wait ~18s–20s",
            sec53_target="0 evictions",
            sec54_target="~74%–82% (12 completed, Excluded: 0)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st37: t0 Intra-CQ Preemption Enabled + Dynamic Parent JobSet Resubmit (Twin of st36)
    inf, wf, ex = base_cluster_infra(num_nodes=8, priority_classes=[("p-low", 100), ("p-high", 1000)]), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-t0-pre", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="LowerPriority")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-t0-pre", "cq-t0-pre")))
    for i in range(8):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-low-{i}",
            remaining_sec=30.0,
            full_duration_sec=24.0,
            lq="lq-t0-pre",
            cq="cq-t0-pre",
            node_names=[f"st-node-{i:02d}"],
            prio="p-low",
            prio_val=100,
        )
    for i in range(4):
        add_job(wf, ex, name=f"arr-high-{i}", arrival_sec=11.0, duration_sec=18.0, lq="lq-t0-pre", cq="cq-t0-pre", prio="p-high")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st37",
            slug="t0-preempt-within-cq-lower-priority-resubmit",
            category="8. Initial t0 Workloads — Preemption & TAS",
            title="Active Intra-CQ Preemption of `t0` `PodGroup`s & Dynamic Parent `JobSet` Resubmission (Twin of st36)",
            feature_focus="Direct A/B twin of `st36` with `withinClusterQueue: LowerPriority`: verifies Kueue preempts 4 `t0` `PodGroup`s, `testrunner.py` dynamically resubmits their 4 parent `JobSet`s without name collisions, and all 12 complete",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset",
            setup_summary="Identical 8 `t0` `p-low (100)` incumbents (`30s` remaining, `24s` full restart duration) and 4 `p-high (1000)` arrivals at `t=11s` (`18s`) as `st36`, changing ONLY `withinClusterQueue` from `Never` to `LowerPriority`.",
            expected_behavior="At `t=11s`, Kueue preempts **exact 4** of the 8 `t0` `p-low` `PodGroup` `Workload`s (`InClusterQueue = 4`). `arr-high-0..3` admit in `< 2.5s` and finish at `t=29s`. `testrunner.py` detects all 4 evicted/GC'd `t0pg-init-low-*` `PodGroup`s, dynamically submits `JobSet/init-low-*` (`[+4 preempted-t0 resubmit]`), and Kueue re-admits all 4 to full completion.",
            expected_metrics=[
                "`Execution Summary`: `Workflows Submitted: 8/8 [+4 preempted-t0 resubmit]`, `KWOK Pods Status: ... failed=0`.",
                "`Section 5.2`: `p-high` wait drops from `~19s` (`st36`) down to **`< 2.5s`**.",
                "`Section 5.3`: `Total Evictions = 4`, `InClusterQueue = 4` (`priority_class=p-low`).",
                "`Section 5.4`: `Qualifying Workloads: 12 (completed=12 [12 pods])`, `Excluded: 0` (the 4 aborted `0s–11s` `PodGroup` runs are GC'd with zero credit while their 4 uninterrupted resubmitted `JobSet` runs complete).",
            ],
            must_not_happen=[
                "`Section 5.3 Total Evictions == 0` (would mean `t0` `PodGroup`s were immune to preemption or missing `priority: 100`).",
                "Workload name collision or hang when resubmitting `JobSet/init-low-*` after `PodGroup` eviction.",
            ],
            sec51_target="~82%–96%",
            sec52_target="p-high wait < 2.5s",
            sec53_target="4 evictions (InClusterQueue, p-low)",
            sec54_target="~70%–86% (12 completed after resubmit)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st38: t0 Equal-Priority Timestamp Seniority Protection (LowerOrNewerEqualPriority)
    inf, wf, ex = base_cluster_infra(num_nodes=8, priority_classes=[("p-equal", 500)]), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-t0-eq",
                tpu_quota=32,
                cpu_quota=128,
                mem_gi_quota=512,
                within_cq="LowerOrNewerEqualPriority",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-t0-eq", "cq-t0-eq")))
    for i in range(5):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-eq-{i}",
            remaining_sec=32.0,
            lq="lq-t0-eq",
            cq="cq-t0-eq",
            node_names=[f"st-node-{i:02d}"],
            prio="p-equal",
            prio_val=500,
        )
    add_t0_job(
        inf,
        wf,
        ex,
        name="init-eq-short",
        remaining_sec=14.0,
        lq="lq-t0-eq",
        cq="cq-t0-eq",
        node_names=["st-node-05"],
        prio="p-equal",
        prio_val=500,
    )
    add_job(
        wf,
        ex,
        name="arr-eq-older-3n",
        arrival_sec=3.0,
        duration_sec=18.0,
        lq="lq-t0-eq",
        cq="cq-t0-eq",
        num_pods=3,
        prio="p-equal",
    )
    for i in range(2):
        add_job(wf, ex, name=f"arr-eq-newer-{i}", arrival_sec=6.0, duration_sec=28.0, lq="lq-t0-eq", cq="cq-t0-eq", prio="p-equal")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st38",
            slug="t0-preempt-lower-or-newer-equal-protection",
            category="8. Initial t0 Workloads — Preemption & TAS",
            title="Seniority Protection of `t0` `PodGroup`s Under `withinClusterQueue: LowerOrNewerEqualPriority`",
            feature_focus="Verifies `t0` `PodGroup`s carry older `creationTimestamp` than `t > 0` arrivals so equal-priority newcomers never preempt `t0` incumbents",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st38-t0-preempt-lower-or-newer-equal-protection --no-skip-initial-offset",
            setup_summary="8-node (`32`-TPU) cluster, `withinClusterQueue: LowerOrNewerEqualPriority`, single priority `p-equal (500)`. At `t0`, 6 `Admitted` `1-pod` jobs (`24/32` TPUs: 5 running `32s`, 1 running `14s`) hold `st-node-00..05`. At `t=3s`, `arr-eq-older-3n` (`12` TPUs, `18s`) arrives; at `t=6s`, 2 `1-pod` `arr-eq-newer-*` jobs (`8` TPUs, `28s`) backfill `st-node-06..07`.",
            expected_behavior="`arr-eq-older-3n` (`t=3s`) CANNOT preempt any `t0` `PodGroup` incumbent (`creationTimestamp < t0`). When `init-eq-short` finishes at `t≈14s`, `arr-eq-older-3n` preempts ONLY the 2 newer `t=6s` backfills (`InClusterQueue = 2`), leaving all 5 remaining `t0` `PodGroup`s untouched (`0` `preempted-t0` resubmits).",
            expected_metrics=[
                "`Execution Summary`: `Workflows Submitted: 3/3` (zero `[+N preempted-t0 resubmit]`).",
                "`Section 5.3`: **`2`** evictions (`InClusterQueue = 2` against `arr-eq-newer-*`; `0` evictions of `t0` incumbents).",
                "`Section 5.4`: `Qualifying Workloads: 9 (completed=9 [11 pods])`, **`EFFECTIVE SAT ≈ 65%–88%`**.",
            ],
            must_not_happen=[
                "Any `t0` `PodGroup` (`init-eq-*`) being preempted by an equal-priority `t > 0` arrival.",
            ],
            sec51_target="~75%–95%",
            sec52_target="arr-eq-older-3n waits ~10s–12s",
            sec53_target="2 evictions (0 t0 incumbents evicted)",
            sec54_target="~65%–88% (9 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st39: Adversarial Late Preemption of t0 Workloads ("Badput" Collapse)
    inf, wf, ex = base_cluster_infra(num_nodes=8, priority_classes=[("p-batch", 100), ("p-urgent", 1000)]), [], []
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-t0-badput", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="LowerPriority")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-t0-badput", "cq-t0-badput")))
    for i in range(8):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-victim-{i}",
            remaining_sec=32.0,
            full_duration_sec=32.0,
            lq="lq-t0-badput",
            cq="cq-t0-badput",
            node_names=[f"st-node-{i:02d}"],
            prio="p-batch",
            prio_val=100,
        )
    for i in range(8):
        add_job(wf, ex, name=f"arr-urgent-{i}", arrival_sec=24.0, duration_sec=18.0, lq="lq-t0-badput", cq="cq-t0-badput", prio="p-urgent")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st39",
            slug="t0-late-preemption-badput-accounting",
            category="8. Initial t0 Workloads — Preemption & TAS",
            title="Adversarial Late Preemption of `t0` `PodGroup`s (`Sec 5.1 ≈ 97%` vs `Sec 5.4 ≈ 36%`, `Excluded: 8`)",
            feature_focus="Verifies aborted `t0` `PodGroup` compute (`0s–24s`) is excluded from `Sec 5.4` goodput and pending resubmitted workflows classify under `Excluded (preempted/reclaimed/failed): 8`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st39-t0-late-preemption-badput-accounting --no-skip-initial-offset --duration 38s",
            setup_summary="8-node (`32`-TPU) cluster, `withinClusterQueue: LowerPriority`. At `t0`, 8 `p-batch (100)` `Admitted` `JobSet`s (`32s` remaining) occupy `32/32` TPUs. At `t=24s`, 8 `p-urgent (1000)` `JobSet`s (`18s` duration) arrive and preempt ALL 8 `t0` `PodGroup`s; window ends at `--duration 38s` while `arr-urgent-*` are actively running (`14s` elapsed) and all 8 resubmitted `init-victim-*` sit queued.",
            expected_behavior="All 8 `t0` `PodGroup`s are killed at `t=24s` (`8` evictions) and GC'd; `testrunner.py` resubmits all 8 `init-victim-*` `JobSet`s (`kueue-optimizer.gke.io/preempted-t0-resubmit: 'true'`). At `t=38s`, `Section 5.4` credits ONLY the `~14s` of uninterrupted `arr-urgent-*` execution (`14/38 ≈ 36.8%`) and reports `Excluded (preempted/reclaimed/failed): 8 [8 pods] | Unadmitted: 0`.",
            expected_metrics=[
                "`Section 5.1 (cq-t0-badput)`: **`AVG ≈ 92%–99%`**.",
                "`Section 5.3`: **`8`** evictions (`InClusterQueue = 8`, `priority_class=p-batch`).",
                "`Section 5.4`: **`EFFECTIVE SAT ≈ 31%–41%`** (`ACTIVE AT END ≈ 31%–41%`, `COMPLETED ONLY = 0.00%`), `Excluded (preempted/reclaimed/failed): 8 [8 pods]`, `Unadmitted: 0`.",
            ],
            must_not_happen=[
                "`Section 5.4` exceeding `46%` (would mean preempted `t0` `PodGroup` runtime leaked into goodput).",
                "Resubmitted `init-victim-*` workloads misreported as `Unadmitted: 8` (`Excluded: 0`) despite having been preempted at `t=24s`.",
            ],
            sec51_target="92%–99% (raw occupancy)",
            sec52_target="p-urgent wait < 2.0s",
            sec53_target="8 evictions",
            sec54_target="~31%–41% (Excluded: 8, Unadmitted: 0)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st40: t0 TAS Topology Domain Occupancy & Disjoint Placement
    inf = base_cluster_infra(num_nodes=16, tpu_per_node=4, cpu_per_node=32, mem_gi_per_node=128, with_topology=True)
    wf, ex = [], []
    inf.append(make_infra_event(make_cluster_queue("cq-t0-tas", tpu_quota=64, cpu_quota=512, mem_gi_quota=2048)))
    inf.append(make_infra_event(make_local_queue("lq-t0-tas", "cq-t0-tas")))
    # Occupy p244-0 (st-node-00..07 = 8 nodes) and p224-2 (st-node-08..11 = 4 nodes) at t0 -> leaves p224-3 (st-node-12..15 = 4 nodes) free!
    add_t0_job(
        inf,
        wf,
        ex,
        name="init-tas-8n",
        remaining_sec=26.0,
        lq="lq-t0-tas",
        cq="cq-t0-tas",
        node_names=[f"st-node-{i:02d}" for i in range(8)],
        num_pods=8,
        tas_level="cloud.google.com/gke-tpu-partition-2x4x4-id",
    )
    add_t0_job(
        inf,
        wf,
        ex,
        name="init-tas-4n",
        remaining_sec=26.0,
        lq="lq-t0-tas",
        cq="cq-t0-tas",
        node_names=[f"st-node-{i:02d}" for i in range(8, 12)],
        num_pods=4,
        tas_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
    )
    # Arriving 4-node 2x2x4 job fits IMMEDIATELY on free p224-3 (st-node-12..15)!
    add_job(
        wf,
        ex,
        name="arr-tas-4n-fit",
        arrival_sec=0.0,
        duration_sec=20.0,
        lq="lq-t0-tas",
        cq="cq-t0-tas",
        num_pods=4,
        tas_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
    )
    # Arriving 8-node 2x4x4 job MUST wait ~26s until init-tas-8n frees p244-0!
    add_job(
        wf,
        ex,
        name="arr-tas-8n-wait",
        arrival_sec=0.0,
        duration_sec=18.0,
        lq="lq-t0-tas",
        cq="cq-t0-tas",
        num_pods=8,
        tas_level="cloud.google.com/gke-tpu-partition-2x4x4-id",
    )
    scenarios.append(
        ScenarioSpec(
            scenario_id="st40",
            slug="t0-tas-topology-domain-occupancy",
            category="8. Initial t0 Workloads — Preemption & TAS",
            title="7-Level TAS `t0` Domain Occupancy (`p244-0` + `p224-2` Occupied at `t0`, Disjoint `p224-3` Fit + `p244` Queueing)",
            feature_focus="Verifies `build_topology_assignment` populates Kueue's TAS cache at `t0` so `arr-tas-4n-fit` lands on disjoint `st-node-12..15` (`<2s`) while `arr-tas-8n-wait` waits `~26s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy",
            setup_summary="16-node (`64`-TPU) v7x cube with 7-level `Topology`. At `t0`, `init-tas-8n` occupies `p244-0` (`st-node-00..07`, `26s`) and `init-tas-4n` occupies `p224-2` (`st-node-08..11`, `26s`), leaving ONLY `p224-3` (`st-node-12..15`, `16` TPUs) free. At `t=0s`, `arr-tas-4n-fit` (`2x2x4`, `20s`) and `arr-tas-8n-wait` (`2x4x4`, `18s`) arrive.",
            expected_behavior="`arr-tas-4n-fit` admits immediately (`< 2s`) onto `st-node-12..15` (`64/64` TPUs = `100%` cube pack during `0s–20s`) with zero TAS collision against `st-node-00..11`. `arr-tas-8n-wait` waits `~26s` until `init-tas-8n` finishes, then runs `26s–44s`.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 2 PodGroup(s) (12 pod(s))` (`[TAS]`), `KWOK Pods Status: ... succeeded=24, pending=0, failed=0`.",
                "`Section 5.2`: Bimodal 2-workload split (`arr-tas-4n-fit <= 1.5s`, `arr-tas-8n-wait ≈ 26s`, `AVG ≈ 12s–15s`).",
                "`Section 5.4`: `Qualifying Workloads: 4 (completed=4 [24 pods])`, **`EFFECTIVE SAT ≈ 70%–82%`** (`(48×26 + 16×20 + 32×18) / (64×44) = 2,144 / 2,816 = 76.1%`).",
            ],
            must_not_happen=[
                "Kueue TAS assigning `arr-tas-4n-fit` onto `p224-0/1/2` (`st-node-00..11`, causing `Pending` pod collisions) because `topologyAssignment` was missing on `t0` `PodGroup`s.",
                "`arr-tas-8n-wait` admitting before `init-tas-8n` finishes.",
            ],
            sec51_target="~72%–82% (100% during 0s–20s)",
            sec52_target="4n < 1.5s, 8n ~26s (AVG ~13s)",
            sec53_target="0 evictions",
            sec54_target="~70%–82% (4 TAS workloads, 24 pods)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 9 (st41 - st45): Initial t0 Workloads — Reclaims, Limits & Fair Sharing
    # =========================================================================

    # st41: t0 Cohort Borrowing (200%) + Instant Quota Reclaim on Lender Arrival (reclaimWithinCohort: Any)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-t0-rec")))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-borrower",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-t0-rec",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-lender",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-t0-rec",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-borrower", "cq-borrower")))
    inf.append(make_infra_event(make_local_queue("lq-lender", "cq-lender")))
    for i in range(8):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-bor-{i}",
            remaining_sec=34.0,
            full_duration_sec=22.0,
            lq="lq-borrower",
            cq="cq-borrower",
            node_names=[f"st-node-{i:02d}"],
        )
    for i in range(4):
        add_job(wf, ex, name=f"arr-lend-{i}", arrival_sec=11.0, duration_sec=18.0, lq="lq-lender", cq="cq-lender")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st41",
            slug="t0-cohort-borrowing-reclaim-any",
            category="9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing",
            title="Cohort Reclamation (`reclaimWithinCohort: Any`) Against `t0` Borrowing `PodGroup`s (`200% -> 100%`)",
            feature_focus="Verifies `Cache.AddClusterQueue` registers `t0` borrowed Cohort usage (`32/16 = 200%`) so `cq-lender` immediately reclaims **exact 4** `t0` `PodGroup`s at `t=11s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset",
            setup_summary="8-node (`32`-TPU) cluster, `cohort-t0-rec` (`cq-borrower`: `16` TPUs, `cq-lender`: `16` TPUs, `reclaimWithinCohort: Any`). At `t0`, `cq-borrower` runs 8 `Admitted` `JobSet`s (`32` TPUs = `16` nominal + `16` borrowed, `34s` remaining, `22s` restart). At `t=11s`, `cq-lender` submits 4 `JobSet`s (`16` TPUs, `18s`).",
            expected_behavior="During `0s–11s`, `cq-borrower` reports `200%` (`32/16` TPUs). At `t=11s`, Kueue evicts **exact 4** `init-bor-*` `t0` `PodGroup`s (`cq-borrower`'s own `16` nominal TPUs remain protected), admits all 4 `arr-lend-*` jobs in `< 2.5s`, dynamically resubmits the 4 reclaimed `init-bor-*` `JobSet`s (`[+4 preempted-t0 resubmit]`), and completes all 12 workloads.",
            expected_metrics=[
                "`Section 5.1`: `cq-borrower` hits **`200.0%`** during `0s–11s`, then `100.0%` alongside `cq-lender = 100.0%` during `11s–29s`.",
                "`Section 5.2`: `arr-lend-*` wait **`< 2.5s`**.",
                "`Section 5.3`: **`4`** evictions (`4` unique) reclaiming `cq-lender`'s nominal quota.",
                "`Section 5.4`: `Qualifying Workloads: 12 (completed=12 [12 pods])`, **`EFFECTIVE SAT ≈ 70%–88%`**.",
            ],
            must_not_happen=[
                "More than 4 `init-bor-*` `t0` `PodGroup`s evicted (`cq-borrower`'s 4 nominal slots are untouchable).",
                "`arr-lend-*` waiting `23s` until `t=34s` when `reclaimWithinCohort: Any`.",
            ],
            sec51_target="cq-borrower P90/P99=200%, Cohort ~85%–98%",
            sec52_target="cq-lender wait < 2.5s",
            sec53_target="4 reclaims (4 unique)",
            sec54_target="~70%–88% (12 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st42: t0 Cohort Borrowing + Reclaim Disabled Control (reclaimWithinCohort: Never — Twin of st41)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-t0-norec")))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-borrower",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-t0-norec",
                reclaim_within_cohort="Never",
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-lender",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-t0-norec",
                reclaim_within_cohort="Never",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-borrower", "cq-borrower")))
    inf.append(make_infra_event(make_local_queue("lq-lender", "cq-lender")))
    for i in range(8):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-bor-{i}",
            remaining_sec=30.0,
            lq="lq-borrower",
            cq="cq-borrower",
            node_names=[f"st-node-{i:02d}"],
        )
    for i in range(4):
        add_job(wf, ex, name=f"arr-lend-{i}", arrival_sec=11.0, duration_sec=18.0, lq="lq-lender", cq="cq-lender")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st42",
            slug="t0-cohort-borrowing-reclaim-never",
            category="9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing",
            title="Non-Preemptive `t0` Cohort Borrowing (`reclaimWithinCohort: Never` — Twin of st41)",
            feature_focus="Direct A/B twin of `st41` with `reclaimWithinCohort: Never`: `t0` borrower holds `200%` quota uninterrupted while `cq-lender` waits `~19s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset",
            setup_summary="Identical cohort quotas (`16` + `16` TPUs), 8 `t0` borrower jobs (`30s`), and 4 `cq-lender` arrivals at `t=11s` (`18s`) as `st41`, changing ONLY `reclaimWithinCohort` from `Any` to `Never`.",
            expected_behavior="Because `cq-borrower`'s `32`-TPU `t0` usage consumes the entire Cohort and `reclaimWithinCohort: Never` forbids eviction, all 8 `init-bor-*` jobs finish naturally at `t=30s` (`0` evictions), and `arr-lend-*` wait `~19s` (`t=11s -> t=30s`) before running `30s–48s`.",
            expected_metrics=[
                "`Section 5.2`: `arr-lend-*` wait **`~16s–21s`** (vs `< 2.5s` in `st41`).",
                "`Section 5.3`: Strictly **`0`** evictions (vs `4` in `st41`).",
                "`Section 5.4`: `Qualifying Workloads: 12 (completed=12 [12 pods])`, `Excluded: 0`, **`EFFECTIVE SAT ≈ 74%–82%`**.",
            ],
            must_not_happen=[
                "Any eviction (`Section 5.3 > 0`) when `reclaimWithinCohort: Never`.",
                "`arr-lend-*` admitting at `t=11s` (which would mean `cq-borrower`'s `t0` borrowed quota was not tracked in the Cohort).",
            ],
            sec51_target="cq-borrower 200% in 0s–30s, Cohort ~78%",
            sec52_target="cq-lender wait ~18s–20s",
            sec53_target="0 evictions",
            sec54_target="~74%–82% (Excluded: 0)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st43: t0 Nominal Occupancy + Explicit borrowingLimit / lendingLimit Cap (150% Ceiling)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-t0-lim")))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-capped",
                tpu_quota=16,
                cpu_quota=96,
                mem_gi_quota=384,
                cohort_name="cohort-t0-lim",
                tpu_borrowing_limit=8,
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-donor",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                cohort_name="cohort-t0-lim",
                tpu_lending_limit=8,
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-capped", "cq-capped")))
    inf.append(make_infra_event(make_local_queue("lq-donor", "cq-donor")))
    # 4 t0 jobs already consume 100% of cq-capped's 16 nominal TPUs!
    for i in range(4):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-cap-{i}",
            remaining_sec=22.0,
            lq="lq-capped",
            cq="cq-capped",
            node_names=[f"st-node-{i:02d}"],
        )
    # 4 arriving jobs want 16 more TPUs, but borrowingLimit=8 allows ONLY 2 jobs (+8 TPUs -> 24 TPUs = 150% max)!
    for i in range(4):
        add_job(wf, ex, name=f"arr-cap-{i}", arrival_sec=0.0, duration_sec=22.0, lq="lq-capped", cq="cq-capped")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st43",
            slug="t0-borrowing-limit-enforced-with-initial-load",
            category="9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing",
            title="`borrowingLimit: 8` & `lendingLimit: 8` Enforced on Top of `t0` Nominal Quota Load (`150%` Ceiling)",
            feature_focus="Verifies `16` TPUs of `t0` nominal usage + `8` TPUs of `t=0s` arrivals hit the exact `24`-TPU (`150.0%`) borrowing ceiling while keeping 2 nodes idle and 2 arrivals queued",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st43-t0-borrowing-limit-enforced-with-initial-load",
            setup_summary="8-node (`32`-TPU) cluster, `cohort-t0-lim`. `cq-capped` (`nominalQuota=16, borrowingLimit=8`); `cq-donor` (`nominalQuota=16, lendingLimit=8`). At `t0`, 4 `Admitted` `init-cap-*` jobs (`16` TPUs, `22s`) already fill `cq-capped`'s nominal quota. At `t=0s`, 4 `NotAdmitted` `arr-cap-*` jobs (`16` TPUs, `22s`) arrive.",
            expected_behavior="Kueue accounts for `init-cap-0..3`'s `16` TPUs, admits **only 2** of the 4 `arr-cap-*` arrivals at `t=0s` (`16 + 8 = 24` TPUs = `150.0%` of nominal, leaving 2 nodes idle), queues the other 2 `arr-cap-*` jobs for `~22s`, and drains them in `22s–44s`.",
            expected_metrics=[
                "`Section 5.1 (cq-capped)`: Peak utilization hits **exact `150.0%`** (`24 / 16` TPUs) and NEVER reaches `200%` (`32 / 16`).",
                "`Section 5.2`: 2 immediate arrivals (`<= 1.0s`) + 2 queued arrivals (`~22s`), `AVG ≈ 10s–12s`.",
                "`Section 5.4`: `Qualifying Workloads: 8 (completed=8 [8 pods])`, **`EFFECTIVE SAT ≈ 46%–52%`** (`704 / 1,408 TPU-s = 50.0%`).",
            ],
            must_not_happen=[
                "`cq-capped` admitting all 4 `arr-cap-*` jobs at `t=0s` (`32` TPUs = `200%`, which would happen if `init-cap-*`'s `16` TPUs were missing from `cq-capped` quota accounting).",
            ],
            sec51_target="cq-capped P90/P99 = 150.0% (24/16 TPUs)",
            sec52_target="2 jobs ~0s, 2 jobs ~22s (AVG ~11s)",
            sec53_target="0 evictions",
            sec54_target="~46%–52% (Wave 1: 75%, Wave 2: 25%)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st44: t0 Cohort Fair Sharing Preemption Against Monopolizer (1:1 Weights — Twin of st45)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-t0-fs", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-team-a",
                tpu_quota=0,
                cpu_quota=0,
                mem_gi_quota=0,
                cohort_name="cohort-t0-fs",
                fair_sharing_weight="1",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue(
                "cq-team-b",
                tpu_quota=0,
                cpu_quota=0,
                mem_gi_quota=0,
                cohort_name="cohort-t0-fs",
                fair_sharing_weight="1",
                reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-team-a", "cq-team-a")))
    inf.append(make_infra_event(make_local_queue("lq-team-b", "cq-team-b")))
    # cq-team-a monopolizes 100% of the 32-TPU cohort at t0 (8 jobs)
    for i in range(8):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-ta-{i}",
            remaining_sec=34.0,
            full_duration_sec=20.0,
            lq="lq-team-a",
            cq="cq-team-a",
            node_names=[f"st-node-{i:02d}"],
        )
    # At t=11s, cq-team-b (equal weight 1) demands 6 jobs (24 TPUs = 75% of cohort):
    # Fair Sharing MUST evict EXACT 4 of cq-team-a's t0 jobs (16 TPUs = 50% fair share, NOT 6!)
    for i in range(6):
        add_job(wf, ex, name=f"arr-tb-{i}", arrival_sec=11.0, duration_sec=18.0, lq="lq-team-b", cq="cq-team-b")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st44",
            slug="t0-fair-sharing-preemption-over-monopolizer",
            category="9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing",
            title="Cohort Fair-Sharing Preemption (`1 : 1` Weights) Evicting **Exact 4** of 8 `t0` Monopolizer `PodGroup`s",
            feature_focus="Proves both (1) `t0` `PodGroup`s are preemptible via Fair Sharing and (2) `cq-team-a`'s remaining `16` `t0` TPUs anchor its DRS at `0.5` so `cq-team-b` (`6`-job demand) stops at **exact 4** evictions (`50/50` split)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset",
            setup_summary="8-node (`32`-TPU) cluster, `cohort-t0-fs` (`32` Cohort TPUs). `cq-team-a` (`nominalQuota=0, weight=1, reclaimWithinCohort=Any`) starts at `t0` with 8 `Admitted` `JobSet`s (`32/32` TPUs, `34s` remaining, `20s` restart). At `t=11s`, `cq-team-b` (`nominalQuota=0, weight=1, reclaimWithinCohort=Any`) submits **6** `JobSet`s (`24` TPUs = `75%` demand, `18s`).",
            expected_behavior="At `t=11s`, Preemption-Based Fair Sharing (`LessThanOrEqualToFinalShare`) evicts **exact 4** of `cq-team-a`'s `t0` `PodGroup`s (`16` TPUs = `50%` fair share — refusing to evict a 5th or 6th `t0` job because `cq-team-a` and `cq-team-b` have equal `1 : 1` weights). 4 of `cq-team-b`'s jobs admit immediately while its 5th & 6th jobs wait; the 4 evicted `init-ta-*` `JobSet`s are dynamically resubmitted and all 14 workloads complete.",
            expected_metrics=[
                "`Execution Summary`: `Workflows Submitted: 10/10 [+4 preempted-t0 resubmit]`.",
                "`Section 5.3`: **`4`** evictions (`4` unique, `FairSharing` / cohort preemption) — strictly `4`, NEVER `6`.",
                "`Section 5.2`: 4 of `cq-team-b`'s jobs admit in `< 2.5s` while 2 wait `~18s`.",
                "`Section 5.4`: `Qualifying Workloads: 14 (completed=14 [14 pods])`, **`EFFECTIVE SAT ≈ 72%–90%`**.",
            ],
            must_not_happen=[
                "`Section 5.3 Total Evictions == 0` (would mean Fair Sharing failed to preempt `t0` `PodGroup` monopolizers).",
                "`Section 5.3 Total Evictions == 6` (would mean `cq-team-a`'s surviving `t0` `PodGroup`s were missing from `DominantResourceShare` calculation).",
            ],
            sec51_target="Cohort ~85%–98% (100% -> 50/50 split)",
            sec52_target="4 arr-tb < 2.5s, 2 arr-tb ~18s",
            sec53_target="4 FairSharing evictions (never 6)",
            sec54_target="~72%–90% (14 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st45: t0 Cohort Monopolizer Control Without Fair Sharing Preemption (Twin of st44)
    inf, wf, ex = base_cluster_infra(num_nodes=8), [], []
    inf.append(make_infra_event(make_cohort("cohort-t0-nofs", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-hog", tpu_quota=0, cpu_quota=0, mem_gi_quota=0, cohort_name="cohort-t0-nofs", reclaim_within_cohort="Never")
        )
    )
    inf.append(
        make_infra_event(
            make_cluster_queue("cq-victim", tpu_quota=0, cpu_quota=0, mem_gi_quota=0, cohort_name="cohort-t0-nofs", reclaim_within_cohort="Never")
        )
    )
    inf.append(make_infra_event(make_local_queue("lq-hog", "cq-hog")))
    inf.append(make_infra_event(make_local_queue("lq-victim", "cq-victim")))
    for i in range(8):
        add_t0_job(
            inf,
            wf,
            ex,
            name=f"init-hog-{i}",
            remaining_sec=30.0,
            lq="lq-hog",
            cq="cq-hog",
            node_names=[f"st-node-{i:02d}"],
        )
    for i in range(6):
        add_job(wf, ex, name=f"arr-vic-{i}", arrival_sec=11.0, duration_sec=18.0, lq="lq-victim", cq="cq-victim")
    scenarios.append(
        ScenarioSpec(
            scenario_id="st45",
            slug="t0-no-fair-sharing-monopolizer-control",
            category="9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing",
            title="Zero-Nominal Cohort Monopolized at `t0` Without Fair-Sharing Preemption (`reclaimWithinCohort: Never` — Twin of st44)",
            feature_focus="Direct A/B control twin of `st44` with `reclaimWithinCohort: Never`: `cq-hog`'s 8 `t0` `PodGroup`s hold `32/32` Cohort TPUs uninterrupted while all 6 `cq-victim` arrivals wait `~19s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st45-t0-no-fair-sharing-monopolizer-control --no-skip-initial-offset",
            setup_summary="Identical `32`-TPU zero-nominal Cohort, 8 `t0` `Admitted` `JobSet`s (`30s`), and 6 peer arrivals at `t=11s` (`18s`) as `st44`, changing `reclaimWithinCohort` from `Any` to `Never` (and omitting explicit weights).",
            expected_behavior="Zero `t0` `PodGroup`s are evicted (`Sec 5.3 = 0` vs `4` in `st44`). All 6 `arr-vic-*` jobs wait until `init-hog-*` finish at `t=30s` (`wait ≈ 19s`), then execute in `30s–48s`.",
            expected_metrics=[
                "`Section 5.2`: `arr-vic-*` wait **`~16s–21s`** (vs `< 2.5s` for the first 4 jobs in `st44`).",
                "`Section 5.3`: Strictly **`0`** evictions (vs `4` in `st44`).",
                "`Section 5.4`: `Qualifying Workloads: 14 (completed=14 [14 pods])`, `Excluded: 0`, **`EFFECTIVE SAT ≈ 82%–92%`** (`(32×30 + 24×18) / (32×48) = 1,392 / 1,536 = 90.6%`).",
            ],
            must_not_happen=[
                "Any eviction (`Section 5.3 > 0`) when `reclaimWithinCohort: Never`.",
                "`arr-vic-*` admitting before `t=30s` while `init-hog-*` occupy `32/32` Cohort TPUs.",
            ],
            sec51_target="Cohort ~85%–93%",
            sec52_target="cq-victim waits ~18s–20s",
            sec53_target="0 evictions",
            sec54_target="~82%–92% (14 completed, Excluded: 0)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    scenarios.extend(
        build_scale_15_scenarios(
            scenario_spec_cls=ScenarioSpec,
            fmt_ts_fn=fmt_ts,
            det_uid_fn=det_uid,
            make_infra_event_fn=make_infra_event,
            make_namespace_fn=make_namespace,
            make_topology_fn=make_topology,
            make_flavor_fn=make_flavor,
            make_priority_class_fn=make_priority_class,
            make_cohort_fn=make_cohort,
            make_cluster_queue_fn=make_cluster_queue,
            make_local_queue_fn=make_local_queue,
            make_jobset_and_exec_events_fn=make_jobset_and_exec_events,
            make_t0_admitted_jobset_events_fn=make_t0_admitted_jobset_events,
            make_initial_pod_events_fn=make_initial_pod_events,
        )
    )
    scenarios.extend(
        build_failure_preemption_15_scenarios(
            scenario_spec_cls=ScenarioSpec,
            fmt_ts_fn=fmt_ts,
            det_uid_fn=det_uid,
            make_infra_event_fn=make_infra_event,
            make_namespace_fn=make_namespace,
            make_topology_fn=make_topology,
            make_flavor_fn=make_flavor,
            make_priority_class_fn=make_priority_class,
            make_cohort_fn=make_cohort,
            make_cluster_queue_fn=make_cluster_queue,
            make_local_queue_fn=make_local_queue,
            make_jobset_and_exec_events_fn=make_jobset_and_exec_events,
            make_t0_admitted_jobset_events_fn=make_t0_admitted_jobset_events,
            make_initial_pod_events_fn=make_initial_pod_events,
        )
    )
    assert len(scenarios) == 75, f"Expected exactly 75 scenarios, got {len(scenarios)}"
    return scenarios


build_all_60_scenarios = build_all_45_scenarios
build_all_75_scenarios = build_all_45_scenarios


SCENARIO_PAIRINGS: dict[str, list[dict[str, str]]] = {
    "st01": [
        {
            "peer_id": "st02",
            "relationship": "Quota Ceiling vs. Full Cluster Capacity Pair",
            "knob_changed": "`cq-half` nominal quota (`16` TPUs = `50%` of cluster) in `st01` vs. `cq-full` (`32` TPUs = `100%` of cluster) in `st02`.",
            "cross_check": "`Sec 5.1 AVG` MUST be `~100%` in BOTH `st01` (`16/16`) and `st02` (`32/32`), whereas `Sec 5.4 EFFECTIVE SAT` MUST drop by half from `~96%–100%` (`st02`) to `~50.0%` (`st01`), proving `Sec 5.1` normalizes by CQ nominal quota while `Sec 5.4` normalizes by physical cluster capacity.",
        },
        {
            "peer_id": "st16",
            "relationship": "Single Quota-Capped CQ vs. Two Isolated CQs Equivalence Pair",
            "knob_changed": "Single `16`-TPU CQ (`st01`) vs. two isolated `16`-TPU CQs (`cq-alpha`, `cq-beta`, no cohort) where only `cq-alpha` receives load (`st16`).",
            "cross_check": "Both runs execute the exact same 2 waves of 4 jobs (`40s` total) and MUST yield virtually identical `Sec 5.4 EFFECTIVE SAT ≈ 50.0%` and `Sec 5.2 AVG ≈ 10s`.",
        },
    ],
    "st02": [
        {
            "peer_id": "st01",
            "relationship": "Linear Saturation Scaling Ladder (`100%` vs. `50%`)",
            "knob_changed": "`32`-TPU full-cluster quota (`st02`) vs. `16`-TPU half-cluster quota (`st01`).",
            "cross_check": "`Sec 5.4(st02) / Sec 5.4(st01) ≈ 2.0x` (`~96%` vs. `~50%`) with `0` evictions in both.",
        },
        {
            "peer_id": "st03",
            "relationship": "Linear Saturation Scaling Ladder (`100%` vs. `25%`)",
            "knob_changed": "`8` single-node jobs (`32` TPUs, `st02`) vs. `2` single-node jobs (`8` TPUs, `st03`) under the same unrestricted `32`-TPU quota.",
            "cross_check": "`Sec 5.4(st02) / Sec 5.4(st03) ≈ 4.0x` (`~96%` vs. `~24%`) and `Sec 5.1(st02) / Sec 5.1(st03) ≈ 4.0x`.",
        },
    ],
    "st03": [
        {
            "peer_id": "st02",
            "relationship": "Demand-Limited `25%` vs. Full `100%` Baseline Pair",
            "knob_changed": "Submitted job count (`2` vs. `8` single-node `35s` jobs) with identical `32`-TPU cluster and CQ quota.",
            "cross_check": "In both `st03` and `st02`, `|Sec 5.1 AVG - Sec 5.4 EFFECTIVE SAT| <= 4%`, confirming agreement between Prometheus quota gauges and snapshot-based saturation when `quota == cluster_capacity` and `evictions == 0`.",
        },
        {
            "peer_id": "st01",
            "relationship": "Demand-Limited (`st03`) vs. Quota-Limited (`st01`) Contrast Pair",
            "knob_changed": "`st03` has `32`-TPU quota with `8`-TPU demand (`Sec 5.1 ≈ Sec 5.4 ≈ 25%`); `st01` has `16`-TPU quota with `32`-TPU demand (`Sec 5.1 ≈ 100%`, `Sec 5.4 ≈ 50%`).",
            "cross_check": "Cross-examining `st03` vs. `st01` verifies whether an idle cluster is starved of incoming demand (`Sec 5.1` low) or bottlenecked by CQ quota configuration (`Sec 5.1 ≈ 100%` while `Sec 5.4` low).",
        },
    ],
    "st04": [
        {
            "peer_id": "st02",
            "relationship": "Multi-Pod (`2x4` + `4x4` TPUs) vs. Single-Pod (`8x4` TPUs) Iso-Capacity Pair",
            "knob_changed": "`3` multi-pod JobSets totaling `8` pods (`32` TPUs, `st04`) vs. `8` single-pod JobSets totaling `8` pods (`32` TPUs, `st02`).",
            "cross_check": "`Sec 5.4 EFFECTIVE SAT` for `google.com/tpu` MUST be `93%–100%` in BOTH `st04` and `st02`, proving per-pod TPU requests are multiplied by `num_pods` and parent/child JobSet workloads are never double-counted.",
        },
        {
            "peer_id": "st29",
            "relationship": "Homogeneous Multi-Pod (`st04`) vs. Heterogeneous Multi-Role (`st29`) JobSet Pair",
            "knob_changed": "Single `replicatedJob` (`w`) per JobSet in `st04` vs. dual `replicatedJobs` (`coord` with `0` TPUs + `w` with `4` TPUs/pod) in `st29`.",
            "cross_check": "Both scenarios pack `32/32` cluster TPUs (`Sec 5.4 ≈ 93%–100%`), while `st29` reports `10` total succeeded pods (`2` CPU coordinators + `8` TPU workers) vs. `8` pods in `st04`.",
        },
    ],
    "st05": [
        {
            "peer_id": "st35",
            "relationship": "Scheduled Active-at-End (`st05`) vs. Infinite `t0` `PodGroup` Active-at-End (`st35`) Pair",
            "knob_changed": "`st05` (`--duration 42s`) runs 4 long `120s` scheduled JobSets active at `t_end`, whereas `st35` (`--duration 40s`) runs 4 infinite `t0` `PodGroup`s (`is_infinite=True`) active at `t_end`.",
            "cross_check": "Both scenarios split `Sec 5.4` into `COMPLETED ONLY ~45%–48% + ACTIVE AT END ~48%–50% ≈ 95%–98%`, while `st35` additionally keeps `FULL-DUR SAT ≈ 95%` without `864000s` inflation.",
        },
        {
            "peer_id": "st15",
            "relationship": "Active-at-End Uninterrupted Work (`st05`) vs. Evicted Incomplete Work (`st15`) Pair",
            "knob_changed": "Incomplete jobs in `st05` are still actively running at `t_end` (counted in `ACTIVE AT END`), whereas incomplete jobs in `st15` were preempted at `t=26s` (excluded as badput).",
            "cross_check": "`Sec 5.4(st05) ≈ 93%–100%` vs. `Sec 5.4(st15) ≈ 32%–40%` (`>50pp` delta), proving `Sec 5.4` cleanly separates healthy in-flight work at test end from aborted/preempted work.",
        },
    ],
    "st06": [
        {
            "peer_id": "st07",
            "relationship": "Exact A/B Twin (`StrictFIFO` vs. `BestEffortFIFO`)",
            "knob_changed": "ONLY `ClusterQueue.spec.queueingStrategy` changes (`StrictFIFO` in `st06` vs. `BestEffortFIFO` in `st07`); nodes, job sizes, durations, and arrival times are 100% identical.",
            "cross_check": "`st06` MUST show severe head-of-line blocking behind `js-blocker-7n`: total wall duration `~60s` (vs. `~45s` in `st07`), `Sec 5.4 EFFECTIVE SAT ≈ 49%–58%` (vs. `~66%–75%` in `st07`), and `Sec 5.2 P50 Wait Time ≈ 30s–40s` (vs. `< 2.0s` in `st07`).",
        },
    ],
    "st07": [
        {
            "peer_id": "st06",
            "relationship": "Exact A/B Twin (`BestEffortFIFO` Backfill vs. `StrictFIFO` HoL Blocking)",
            "knob_changed": "ONLY `ClusterQueue.spec.queueingStrategy` changes (`BestEffortFIFO` in `st07` vs. `StrictFIFO` in `st06`).",
            "cross_check": "`st07` MUST backfill all 6 small 1-node jobs ahead of `js-blocker-7n` at `t=2s`, reducing wall-clock makespan by `~15s` (`~45s` vs. `~60s`), boosting `Sec 5.4 EFFECTIVE SAT` by `+15pp..+20pp`, and collapsing `Sec 5.2 P50 Wait Time` from `~35s` to `< 2.0s`.",
        },
    ],
    "st08": [
        {
            "peer_id": "st09",
            "relationship": "Single-Priority FIFO Waves (`st08`) vs. Multi-Priority Reordered Waves (`st09`) Pair",
            "knob_changed": "All 16 jobs share `p-standard (100)` in `st08`, whereas queued waves in `st09` carry ascending priorities `p-low (50)`, `p-high (500)`, `p-critical (1000)` in reverse arrival order.",
            "cross_check": "Both scenarios maintain `Sec 5.4 ≈ 91%–99%` and `Sec 5.3 Evictions == 0`, while `Sec 5.2` displays a single row (`AVG ~18s`, `P90 ~36s`) in `st08` vs. 3 priority rows ordered `Wait(p-critical) < Wait(p-high)` in `st09`.",
        },
    ],
    "st09": [
        {
            "peer_id": "st08",
            "relationship": "Multi-Priority Non-Preemptive Reordering (`st09`) vs. Single-Priority Staircase (`st08`) Pair",
            "knob_changed": "Introduces `WorkloadPriorityClass` tiers arriving in reverse priority order while keeping `withinClusterQueue: Never`.",
            "cross_check": "`Sec 5.3 Evictions` MUST remain `0` (matching `st08`), while `Sec 5.2` proves `p-critical (1000)` (`wait ~11s`) overtakes both `p-high (500)` (`wait ~24s`) and queued `p-low (50)` (`wait ~37s`).",
        },
        {
            "peer_id": "st12",
            "relationship": "Non-Preemptive Queue Reordering (`st09`) vs. Active Intra-CQ Preemption (`st12`) Pair",
            "knob_changed": "`withinClusterQueue: Never` (`st09`) vs. `withinClusterQueue: LowerPriority` (`st12`).",
            "cross_check": "In `st09`, high-priority jobs wait for the running `t=0` wave to finish naturally (`wait ~11s`, `0` evictions); in `st12`, high-priority jobs evict running low-priority incumbents immediately (`wait < 2s`, `4` evictions).",
        },
    ],
    "st10": [
        {
            "peer_id": "st40",
            "relationship": "Greenfield TAS Cube Packing (`st10`) vs. Pre-Occupied `t0` TAS Cube Packing (`st40`) Pair",
            "knob_changed": "`st10` packs `8+4+2+1+1 = 16` fresh `t=0s` nodes, whereas `st40` starts with `p244-0` (`8` nodes) + `p224-2` (`4` nodes) already occupied by `t0` `PodGroup`s and schedules new `2x2x4` and `2x4x4` arrivals against `t0` TAS state.",
            "cross_check": "Both runs achieve `0` pod collisions; in `st40`, `arr-tas-4n-fit` lands immediately (`< 2s`) on disjoint `p224-3` while `arr-tas-8n-wait` queues `~26s` until `init-tas-8n` frees `p244-0`.",
        },
    ],
    "st11": [
        {
            "peer_id": "st12",
            "relationship": "Exact A/B Twin (`withinClusterQueue: Never` vs. `LowerPriority`)",
            "knob_changed": "ONLY `ClusterQueue.spec.preemption.withinClusterQueue` changes (`Never` in `st11` vs. `LowerPriority` in `st12`); all 8 nodes, 8 `p-low` jobs (`t=0s`), and 4 `p-high` jobs (`t=8s`) are 100% identical.",
            "cross_check": "`Sec 5.3 Evictions` flips from `0` (`st11`) to `4` (`st12`), and `Sec 5.2` wait time for `p-high (1000)` drops from `~22s` (`st11`) to `< 2.0s` (`st12`).",
        },
        {
            "peer_id": "st36",
            "relationship": "Fresh `t=0s` Non-Preemptive Control (`st11`) vs. Bootstrapped `t0` `PodGroup` Control (`st36`) Pair",
            "knob_changed": "`st11` submits 8 `NotAdmitted` `p-low` JobSets at `t=0s`, whereas `st36` bootstraps 8 pre-admitted `t0` `p-low` `PodGroup`s.",
            "cross_check": "Both scenarios report `0` evictions and hold incoming `p-high` workloads in queue until the low-priority incumbents finish.",
        },
    ],
    "st12": [
        {
            "peer_id": "st11",
            "relationship": "Exact A/B Twin (`withinClusterQueue: LowerPriority` vs. `Never`)",
            "knob_changed": "ONLY `withinClusterQueue` changes (`LowerPriority` in `st12` vs. `Never` in `st11`).",
            "cross_check": "`st12` MUST emit exact `4` evictions (`Unique workloads evicted once = 4`) and `< 2.0s` wait for `p-high` vs. `0` evictions and `~22s` wait in `st11`.",
        },
        {
            "peer_id": "st37",
            "relationship": "Scheduled `JobSet` Preemption (`st12`) vs. Bootstrapped `t0` `PodGroup` Preemption + Resubmit (`st37`) Pair",
            "knob_changed": "`st12` preempts controller-owned `JobSet` Workloads (in-place requeue), whereas `st37` preempts bootstrapped `t0` `PodGroup` Workloads (GC + dynamic parent `JobSet` resubmission).",
            "cross_check": "Both `st12` and `st37` record exact `4` `InClusterQueue` evictions, `< 2.5s` `p-high` wait, and `12` completed workloads (`Excluded: 0`).",
        },
    ],
    "st13": [
        {
            "peer_id": "st38",
            "relationship": "Fresh `t=0s` Seniority (`st13`) vs. Pre-Existing `t0` `PodGroup` Seniority (`st38`) Under `LowerOrNewerEqualPriority`",
            "knob_changed": "`st13` creates older incumbents as `NotAdmitted` arrivals at `t=0s`, whereas `st38` bootstraps them as pre-admitted `t0` `PodGroup`s before queue activation.",
            "cross_check": "Both runs protect all `t0` / `t=0s` incumbents (`0` evictions of initial jobs) and evict ONLY the `2` newer equal-priority backfill jobs (`InClusterQueue = 2`).",
        },
    ],
    "st14": [
        {
            "peer_id": "st12",
            "relationship": "Multi-Tier 3-Cascade Preemption Chain (`st14`) vs. Single-Step Preemption (`st12`) Pair",
            "knob_changed": "4 ascending priority tiers arriving every `7s` (`tier-1 -> tier-2 -> tier-3 -> tier-4`, `st14`) vs. a single `p-low -> p-high` step at `t=8s` (`st12`).",
            "cross_check": "`Sec 5.3 Total Evictions` scales `3x` from `4` (`st12`) to `~12` (`3` cascades × `4` jobs in `st14`), while `Sec 5.4 EFFECTIVE SAT` drops to `~48%` (`st14`) as aborted intermediate tiers are excluded.",
        },
        {
            "peer_id": "st15",
            "relationship": "Cascading Mid-Life Preemptions (`st14`) vs. Late 87%-Complete Preemption Collapse (`st15`) Pair",
            "knob_changed": "Preemptions at `7s` intervals (`st14`) vs. simultaneous preemption of all 8 jobs at `t=26s` out of `30s` (`st15`).",
            "cross_check": "Both scenarios keep raw `Sec 5.1` occupancy `>90%` while `Sec 5.4` accurately tracks uninterrupted goodput (`~48%` in `st14`, `~35%` in `st15`).",
        },
    ],
    "st15": [
        {
            "peer_id": "st39",
            "relationship": "Scheduled Late-Preemption Badput (`st15`) vs. `t0` `PodGroup` Late-Preemption Badput (`st39`) Pair",
            "knob_changed": "`st15` kills 8 scheduled `t=0s` batch JobSets at `t=26s`, whereas `st39` kills 8 bootstrapped `t0` `PodGroup`s at `t=24s` (triggering GC + resubmission).",
            "cross_check": "Both runs expose `~55–60pp` badput gap (`Sec 5.1 ≈ 93%–99%` vs. `Sec 5.4 ≈ 32%–41%`), `8` evictions, and `Excluded: 8` (`Unadmitted: 0`).",
        },
    ],
    "st16": [
        {
            "peer_id": "st17",
            "relationship": "Exact A/B Twin (Isolated Multi-CQ vs. Shared Cohort Borrowing)",
            "knob_changed": "ONLY `cohortName: cohort-shared` is added on `cq-alpha` and `cq-beta` in `st17`; both CQs keep `16`-TPU nominal quota and identical 8 jobs in `cq-alpha` (`0` in `cq-beta`).",
            "cross_check": "Adding the cohort flips `cq-alpha` `Sec 5.1` usage from `~100%` (`16/16` TPUs in `st16`) to `~200%` (`32/16` TPUs in `st17`), cuts total makespan in half, and doubles `Sec 5.4 EFFECTIVE SAT` from `~50%` (`st16`) to `~96%` (`st17`).",
        },
    ],
    "st17": [
        {
            "peer_id": "st16",
            "relationship": "Exact A/B Twin (Full Cohort Borrowing `200%` vs. Strict No-Cohort Isolation `100%`)",
            "knob_changed": "Shared `cohort-shared` (`st17`) vs. standalone CQs with no cohort (`st16`).",
            "cross_check": "`cq-alpha` in `Sec 5.1` MUST report `~200%` (`32/16` nominal TPUs — never clamped to `100%`), while `Sec 5.4` reports `~96%` (`32/32` physical cluster TPUs — never `200%`).",
        },
        {
            "peer_id": "st20",
            "relationship": "Unrestricted Borrowing (`200%` in `st17`) vs. Capped `borrowingLimit` (`150%` in `st20`) Pair",
            "knob_changed": "No `borrowingLimit` (`st17` borrows all `+16` TPUs -> `32/16 = 200%`) vs. `borrowingLimit: 8` (`st20` borrows max `+8` TPUs -> `24/16 = 150%`).",
            "cross_check": "`cq` peak utilization in `Sec 5.1` steps down from `200.0%` (`st17`) to exact `150.0%` (`st20`) to `100.0%` (`st16`).",
        },
    ],
    "st18": [
        {
            "peer_id": "st19",
            "relationship": "Exact A/B Twin (`reclaimWithinCohort: Any` vs. `reclaimWithinCohort: Never`)",
            "knob_changed": "ONLY `reclaimWithinCohort` changes (`Any` in `st18` vs. `Never` in `st19`); all 8 borrower jobs (`t=0s`) and 4 lender jobs (`t=9s`) are 100% identical.",
            "cross_check": "`Sec 5.3 Evictions` flips from `4` (`st18`, evicting only the 4 borrowed slots while protecting `cq-borrower`'s own 4 nominal slots) to `0` (`st19`), and `cq-lender` wait time jumps from `< 2.0s` (`st18`) to `~21s` (`st19`).",
        },
        {
            "peer_id": "st41",
            "relationship": "Scheduled Borrower Reclaim (`st18`) vs. Bootstrapped `t0` Borrower Reclaim (`st41`) Pair",
            "knob_changed": "`st18` admits borrower jobs at `t=0s`, whereas `st41` bootstraps all 8 borrower jobs as pre-admitted `t0` `PodGroup`s (`200%` quota at startup).",
            "cross_check": "Both scenarios evict **exact 4** borrower jobs when `cq-lender` arrives, admit `cq-lender` in `< 2.5s`, and complete all 12 workloads.",
        },
    ],
    "st19": [
        {
            "peer_id": "st18",
            "relationship": "Exact A/B Twin (Non-Preemptive Cohort Lending `st19` vs. Instant Quota Reclaim `st18`)",
            "knob_changed": "ONLY `reclaimWithinCohort` changes (`Never` in `st19` vs. `Any` in `st18`).",
            "cross_check": "`Sec 5.3 Evictions` MUST be strictly `0` in `st19` vs. `4` in `st18`.",
        },
        {
            "peer_id": "st42",
            "relationship": "Scheduled Non-Preemptive Borrowing (`st19`) vs. `t0` Non-Preemptive Borrowing (`st42`) Pair",
            "knob_changed": "`st19` starts 8 `NotAdmitted` borrower jobs at `t=0s`, whereas `st42` bootstraps 8 `Admitted` `t0` `PodGroup` borrower jobs.",
            "cross_check": "Both runs record `0` evictions and hold `cq-lender` in queue (`~19s–21s`) until the borrower finishes.",
        },
    ],
    "st20": [
        {
            "peer_id": "st43",
            "relationship": "Greenfield `borrowingLimit: 8` (`st20`) vs. `t0`-Occupied `borrowingLimit: 8` (`st43`) Pair",
            "knob_changed": "`st20` submits 8 `NotAdmitted` jobs at `t=0s`, whereas `st43` starts with 4 `t0` `Admitted` jobs (`16` nominal TPUs) and submits 4 `NotAdmitted` jobs at `t=0s`.",
            "cross_check": "Both runs cap `cq-capped` at **exact `150.0%` (`24/16` TPUs)** in Wave 1 (`6` concurrent jobs = `4` `t0` + `2` arriving in `st43`), leaving 2 nodes idle and 2 jobs queued.",
        },
    ],
    "st21": [
        {
            "peer_id": "st22",
            "relationship": "No Fair Sharing (`st21`) vs. Equal `1:1` Fair Sharing (`st22`) Pair",
            "knob_changed": "Unweighted shared Cohort pool (`st21`) vs. `fairSharing: {weight: '1'}` on both CQs (`st22`).",
            "cross_check": "In `st21`, first-mover `cq-hog` monopolizes `32/32` TPUs while `cq-victim` waits `~24s`; in `st22`, both teams split the `32`-TPU pool `50/50` (`16` vs. `16` TPUs) across every wave.",
        },
    ],
    "st22": [
        {
            "peer_id": "st23",
            "relationship": "Symmetric `1:1` (`st22`) vs. Heavy Asymmetric `7:1` (`st23`) Fair-Sharing Pair",
            "knob_changed": "Weight ratio changes from `1 : 1` (`4` nodes vs. `4` nodes in `st22`) to `7 : 1` (`7` nodes vs. `1` node in `st23`).",
            "cross_check": "Active workload split across the two CQs shifts from `4 : 4` (`st22`) to `7 : 1` (`28` TPUs vs. `4` TPUs in `st23`) while keeping total Cohort utilization `~85%–100%`.",
        },
        {
            "peer_id": "st44",
            "relationship": "Simultaneous `1:1` Fair Sharing (`st22`) vs. Preemptive `1:1` Fair Sharing Over `t0` Monopolizer (`st44`) Pair",
            "knob_changed": "`st22` submits dual backlogs at `t=0s` (`0` evictions), whereas `st44` starts `cq-team-a` at `32/32` TPUs at `t0` and arrives `cq-team-b` at `t=11s` (`4` FairSharing evictions).",
            "cross_check": "Both converge to the exact `50/50` (`16` vs. `16` TPUs) Cohort split.",
        },
    ],
    "st23": [
        {
            "peer_id": "st22",
            "relationship": "Skewed `7:1` (`st23`) vs. Equal `1:1` (`st22`) Fair-Sharing Pair",
            "knob_changed": "CQ weights `7 : 1` (`st23`) vs. `1 : 1` (`st22`).",
            "cross_check": "`cq-gold` (`weight 7`) runs `7x` as many concurrent jobs as `cq-bronze` (`weight 1`) during `0s–32s`, then `cq-bronze` work-conserves all 8 nodes from `32s–48s`.",
        },
        {
            "peer_id": "st24",
            "relationship": "2-CQ `7:1` Skew (`st23`) vs. 6-CQ Geometric `1..32` Hierarchy (`st24`) Pair",
            "knob_changed": "Scales from 2 weighted CQs on 8 nodes (`32` TPUs, `st23`) to 6 geometrically weighted CQs (`1, 2, 4, 8, 16, 32`) on 16 nodes (`64` TPUs, `st24`).",
            "cross_check": "Both scenarios preserve inverse correlation between `fairSharing.weight` and mean queue wait time without starving the cluster.",
        },
    ],
    "st24": [
        {
            "peer_id": "st23",
            "relationship": "6-CQ Geometric Hierarchy (`st24`) vs. 2-CQ `7:1` Baseline (`st23`) Pair",
            "knob_changed": "6 CQs (`cq-geo-0..5`, weights `1, 2, 4, 8, 16, 32`, `36` jobs on `16` nodes) vs. 2 CQs (`weights 7, 1`, `20` jobs on `8` nodes).",
            "cross_check": "All 6 `cq-geo-*` queues appear in `Sec 1` & `Sec 5.1`, completion order follows descending weight (`32 -> 16 -> 8 -> 4 -> 2 -> 1`), and `Sec 5.4 EFFECTIVE SAT` lands at `~75%` (`2,160 / 2,880 TPU-s`).",
        },
    ],
    "st25": [
        {
            "peer_id": "st18",
            "relationship": "Classical Priority-Threshold Cohort Preemption (`st25`) vs. Nominal Quota Reclaim (`st18`) Pair",
            "knob_changed": "In `st18`, `cq-lender` reclaims only its own `16` nominal TPUs (`InCohortReclamation = 4`); in `st25` (`--no-fair-sharing`), `cq-main` (`8` nominal TPUs + `16` shared Cohort TPUs) uses `borrowWithinCohort: {policy: LowerPriority, maxPriorityThreshold: 500}` to reclaim `8` nominal TPUs (`InCohortReclamation = 2`) AND preempt 2 `p-low (100 <= 500)` jobs while borrowing (`InCohortReclaimWhileBorrowing = 2`), while `maxPriorityThreshold: 500` protects `cq-peer`'s `p-mid (600 > 500)` jobs from borrowing preemption by `p-high (800)`.",
            "cross_check": "`cq-main` reaches `P90/P99 = 200.00%` (`16/8` TPUs), `p-vip (900)` achieves `< 3.0s` wait (`4` jobs) while `p-high (800)` waits `~16s` (`2` jobs), and `p-mid (600)` suffers `0` evictions.",
        },
    ],
    "st26": [
        {
            "peer_id": "st32",
            "relationship": "Pure Non-Kueue `infra-pods` (`st26`) vs. Hybrid `t0` Kueue `PodGroup`s + `infra-pods` (`st32`) Pair",
            "knob_changed": "`st26` places 4 unmanaged `infra-pods` (`16` TPUs) alongside 4 `t=0s` Kueue jobs (`16` TPUs), whereas `st32` places 2 unmanaged `infra-pods` (`8` TPUs) + 4 `t0` Kueue `PodGroup`s (`16` TPUs) + 2 `t=0s` Kueue jobs (`8` TPUs).",
            "cross_check": "Both scenarios pack `32/32` physical TPUs (`Sec 5.4 ≈ 88%–98%`) with zero node collisions, while `Sec 5.1` reflects `16/16` CQ TPUs in `st26` vs. `24/24` CQ TPUs (`16` `t0` + `8` arriving) in `st32`.",
        },
        {
            "peer_id": "st01",
            "relationship": "Unmanaged `t0` `infra-pods` Coexistence (`st26`) vs. Half-Cluster CQ Cap (`st01`) Pair",
            "knob_changed": "`st26` occupies `st-node-00..03` with 4 `infra-pods` (`16` TPUs) while `cq-coexist` (`16` TPUs) runs 4 jobs on `st-node-04..07`; `st01` leaves 4 nodes idle.",
            "cross_check": "`Sec 5.1 AVG` is `~100%` (`16/16`) in both, while `Sec 5.4 EFFECTIVE SAT` jumps from `~50%` (`st01`) to `~88%–96%` (`st26`) because `saturation.py` accounts for `Initial Infra Pods: 4`.",
        },
    ],
    "st27": [
        {
            "peer_id": "st03",
            "relationship": "Host CPU/Memory `100%` Bottleneck (`st27`) vs. Unconstrained `25%` TPU Load (`st03`) Pair",
            "knob_changed": "Both `st27` (`16/64` TPUs) and `st03` (`8/32` TPUs) use exact `25.0%` of cluster TPUs, but `st27` consumes `100%` of node `cpu` (`128/128`) and `memory` (`512Gi/512Gi`).",
            "cross_check": "`google.com/tpu` is `~24%–26%` in BOTH `st27` and `st03`, while `cpu` and `memory` jump from `~25%` (`st03`) to `~95%–100%` (`st27`), proving independent per-resource normalization.",
        },
    ],
    "st28": [
        {
            "peer_id": "st02",
            "relationship": "`initContainers` High-Water Mark + `spec.overhead` (`st28`) vs. Standard Container Spec (`st02`) Pair",
            "knob_changed": "`st28` splits the `4`-TPU request between `initContainer` (`4` TPUs) and main `container` (`2` TPUs) and adds `spec.overhead.cpu: 2`, whereas `st02` puts `4` TPUs directly on `container`.",
            "cross_check": "`Sec 5.4` TPU saturation MUST be `93%–100%` in BOTH `st28` and `st02` (`max(4, 2) = 4` TPUs, never `50%` and never `150%`), and `cpu` in `st28` MUST be `~46%–50%` (`64 / 128` CPUs).",
        },
    ],
    "st29": [
        {
            "peer_id": "st34",
            "relationship": "Scheduled Multi-Role `JobSet` (`st29`) vs. Pre-Admitted `t0` Multi-Role `PodGroup` (`st34`) Pair",
            "knob_changed": "`st29` submits 2 multi-role JobSets (`coord` + `w`) as `NotAdmitted` at `t=0s`, whereas `st34` bootstraps them as pre-admitted `t0` `PodGroup`s and queues a 3rd multi-role arrival behind them.",
            "cross_check": "Both scenarios preserve exact multi-PodSet resource totals (`16` TPUs + `20` CPUs across `5` pods per JobSet) without `WorkloadWebhook` errors.",
        },
    ],
    "st30": [
        {
            "peer_id": "st01",
            "relationship": "Oversized `inadmissible` Monster Guard (`st30`) vs. Clean Half-Cluster Load (`st01`) Pair",
            "knob_changed": "`st30` injects two impossible `16`-node (`64`-TPU > `32`-TPU cluster) JobSets alongside 4 valid `1`-node (`4`-TPU) JobSets (`--duration 42s`).",
            "cross_check": "The 2 oversized `64`-TPU jobs MUST remain in `Unadmitted: 2 [32 pods]` (`pending=2`), contributing `0.00%` to `Sec 5.4` (`EFFECTIVE SAT ≈ 40%–45%` from the 4 valid jobs only).",
        },
    ],
    "st31": [
        {
            "peer_id": "st32",
            "relationship": "Full-Quota `t0` Kueue `PodGroup`s (`st31`) vs. Hybrid `t0` `PodGroup`s + `infra-pods` (`st32`) Pair",
            "knob_changed": "`st31` fills `32/32` CQ TPUs with 8 `t0` Kueue `PodGroup`s (blocking 4 arrivals for `~20s`), whereas `st32` puts 2 pods in `infra-pods` (`8` TPUs) and 4 in `cq-hybrid` (`16/24` TPUs, admitting 2 arrivals immediately).",
            "cross_check": "`Sec 5.2 AVG` drops from `~18s–20s` (`st31`, blocked behind full `t0` CQ quota) to `< 1.5s` (`st32`, `8` free CQ TPUs).",
        },
        {
            "peer_id": "st01",
            "relationship": "Bootstrapped `t0` Wave-1 Occupancy (`st31`) vs. Quota-Capped Two-Wave Run (`st01`) Pair",
            "knob_changed": "`st31` blocks Wave 2 behind 8 pre-admitted `t0` `PodGroup`s (`32/32` TPUs), whereas `st01` blocks Wave 2 behind a `16`-TPU CQ ceiling.",
            "cross_check": "In `st31`, all 4 queued arrivals wait the full `~20s` Wave-1 remainder (`Sec 5.4 ≈ 75%` across 12 total workloads vs. `~50%` in `st01`).",
        },
    ],
    "st32": [
        {
            "peer_id": "st26",
            "relationship": "Hybrid `t0` Kueue + `infra-pods` (`st32`) vs. Pure `infra-pods` (`st26`) Pair",
            "knob_changed": "Replaces 2 of `st26`'s unmanaged `infra-pods` and 2 `t=0s` arrivals with 4 pre-admitted `t0` Kueue `PodGroup`s (`Initial Kueue Workloads: 4`, `Initial Infra Pods: 2`).",
            "cross_check": "Both pack `32/32` physical TPUs (`Sec 5.4 ≈ 90%–98%`) with `< 1.5s` wait and `0` collisions, while `cq-hybrid` `Sec 5.1` accounts for all `24` Kueue TPUs (`16` `t0` + `8` arriving).",
        },
        {
            "peer_id": "st33",
            "relationship": "Tier-1 Enriched Pod Linkage (`st32`) vs. Tier-3/4 Legacy Unlinked & Synthesized Pods (`st33`) Pair",
            "knob_changed": "`st32` uses explicit `workloadName`/`ownerName` pod linkage plus true `infra-pods`, whereas `st33` exercises Tier-3 hashed unlinked pod matching and Tier-4 missing-pod synthesis.",
            "cross_check": "`Initial Infra Pods` is `2` in `st32` vs. `0` in `st33` (confirming Tier 3 claimed all 4 unlinked hashed pods).",
        },
    ],
    "st33": [
        {
            "peer_id": "st32",
            "relationship": "Tier-3/4 Fallback Matching (`st33`) vs. Tier-1 Enriched Linkage (`st32`) Pair",
            "knob_changed": "4 unlinked `--hash-names` pods + 2 podless `Admitted` workflows (`st33`) vs. enriched scraper pod linkage (`st32`).",
            "cross_check": "`st33` bootstraps `6` `PodGroup`s (`6` pods on `st-node-00..05`), reports `Initial Infra Pods: 0`, and runs 2 arrivals on `st-node-06..07` (`32/32` TPUs, `failed=0`).",
        },
    ],
    "st34": [
        {
            "peer_id": "st29",
            "relationship": "Bootstrapped `t0` Multi-Role `PodGroup` (`st34`) vs. Scheduled Multi-Role `JobSet` (`st29`) Pair",
            "knob_changed": "`st34` bootstraps the two `1 coord + 4 worker` JobSets at `t0` (`24s`) and queues `arr-mrole-after` (`16s`) behind them.",
            "cross_check": "`st34` passes `WorkloadWebhook` validation (`2` PodSets per `t0` `PodGroup`), holds `arr-mrole-after` for `~24s`, and finishes `15` total pods (`3` workloads) cleanly.",
        },
    ],
    "st35": [
        {
            "peer_id": "st05",
            "relationship": "Infinite `t0` `PodGroup` Active-at-End (`st35`) vs. Finite `120s` Active-at-End (`st05`) Pair",
            "knob_changed": "4 `t0` `PodGroup`s with omitted `Deleted`/`FullExecution` events (`is_infinite=True`, `864000s` stage timer) in `st35` vs. 4 scheduled `120s` JobSets in `st05`.",
            "cross_check": "`Sec 5.4` in `st35` reports `ACTIVE AT END ≈ 48%–50%`, `EFFECTIVE SAT ≈ 90%–98%`, AND `FULL-DUR SAT ≈ 90%–98%` (never inflated by `864000s`).",
        },
    ],
    "st36": [
        {
            "peer_id": "st37",
            "relationship": "Exact A/B Twin (`t0` `withinClusterQueue: Never` vs. `LowerPriority`)",
            "knob_changed": "ONLY `withinClusterQueue` changes (`Never` in `st36` vs. `LowerPriority` in `st37`); all 8 `t0` `p-low` `PodGroup`s and 4 `t=11s` `p-high` arrivals are identical.",
            "cross_check": "`Sec 5.3 Evictions` flips from `0` (`st36`) to `4` (`st37`, `[+4 preempted-t0 resubmit]`), and `p-high` wait drops from `~19s` (`st36`) to `< 2.5s` (`st37`).",
        },
    ],
    "st37": [
        {
            "peer_id": "st36",
            "relationship": "Exact A/B Twin (`t0` `withinClusterQueue: LowerPriority` + Dynamic Resubmit vs. `Never`)",
            "knob_changed": "ONLY `withinClusterQueue` changes (`LowerPriority` in `st37` vs. `Never` in `st36`).",
            "cross_check": "`st37` preempts exact `4` `t0` `PodGroup`s at `t=11s`, dynamically resubmits all 4 parent `JobSet`s with zero `Workload` name collisions, and completes all `12` workloads.",
        },
        {
            "peer_id": "st12",
            "relationship": "`t0` `PodGroup` Preemption (`st37`) vs. Scheduled `JobSet` Preemption (`st12`) Pair",
            "knob_changed": "Bootstrapped `t0` `PodGroup` victims (`st37`) vs. `t=0s` `JobSet` victims (`st12`).",
            "cross_check": "Both emit `4` `InClusterQueue` evictions, `< 2.5s` `p-high` wait, and `12` completed workloads.",
        },
    ],
    "st38": [
        {
            "peer_id": "st13",
            "relationship": "`t0` `PodGroup` Seniority (`st38`) vs. `t=0s` `JobSet` Seniority (`st13`) Under `LowerOrNewerEqualPriority`",
            "knob_changed": "Pre-admitted `t0` `PodGroup` incumbents (`st38`) vs. `t=0s` `NotAdmitted` incumbents (`st13`).",
            "cross_check": "Both emit exact `2` evictions (evicting only the 2 newer equal-priority backfills) and `0` evictions of `t0` incumbents.",
        },
        {
            "peer_id": "st37",
            "relationship": "Equal-Priority Seniority Protection (`st38`) vs. Higher-Priority Preemption (`st37`) on `t0` `PodGroup`s",
            "knob_changed": "Equal priority `500` with `LowerOrNewerEqualPriority` (`st38`) vs. `p-high (1000) > p-low (100)` with `LowerPriority` (`st37`).",
            "cross_check": "`t0` `PodGroup` evictions drop from `4` (`st37`) to `0` (`st38`).",
        },
    ],
    "st39": [
        {
            "peer_id": "st15",
            "relationship": "`t0` Late-Preemption Badput Collapse (`st39`) vs. Scheduled Late-Preemption Badput Collapse (`st15`) Pair",
            "knob_changed": "8 `t0` `PodGroup` victims killed at `t=24s` (`--duration 38s`, `st39`) vs. 8 scheduled `JobSet` victims killed at `t=26s` (`--duration 41s`, `st15`).",
            "cross_check": "Both report `Sec 5.1 ≈ 93%–99%`, `Sec 5.3 = 8` evictions, `Sec 5.4 EFFECTIVE SAT ≈ 31%–41%` (`~55–60pp` badput gap), and `Excluded: 8` (`Unadmitted: 0`).",
        },
    ],
    "st40": [
        {
            "peer_id": "st10",
            "relationship": "`t0`-Occupied 7-Level TAS Cube (`st40`) vs. Greenfield 7-Level TAS Cube (`st10`) Pair",
            "knob_changed": "`st40` pre-occupies `p244-0` (`8` nodes) and `p224-2` (`4` nodes) with `t0` `PodGroup`s (`topologyAssignment` populated), leaving only `p224-3` (`st-node-12..15`) free at `t=0s`.",
            "cross_check": "`arr-tas-4n-fit` schedules immediately (`< 2s`) onto `st-node-12..15` while `arr-tas-8n-wait` waits `~26s` for `p244-0`, with `0` node collisions.",
        },
    ],
    "st41": [
        {
            "peer_id": "st42",
            "relationship": "Exact A/B Twin (`t0` Cohort Borrowing with `reclaimWithinCohort: Any` vs. `Never`)",
            "knob_changed": "ONLY `reclaimWithinCohort` changes (`Any` in `st41` vs. `Never` in `st42`); all 8 `t0` borrower `PodGroup`s (`32/16 = 200%`) and 4 `t=11s` lender arrivals are identical.",
            "cross_check": "`Sec 5.3 Evictions` flips from `4` (`st41`, reclaiming `cq-lender`'s `16` TPUs while protecting `cq-borrower`'s `16` nominal TPUs) to `0` (`st42`), and `cq-lender` wait jumps from `< 2.5s` (`st41`) to `~19s` (`st42`).",
        },
        {
            "peer_id": "st18",
            "relationship": "`t0` Borrower Reclaim (`st41`) vs. Scheduled Borrower Reclaim (`st18`) Pair",
            "knob_changed": "Bootstrapped `t0` `PodGroup` borrowers (`st41`) vs. `t=0s` scheduled borrowers (`st18`).",
            "cross_check": "Both hit `200%` borrower nominal utilization before reclaim, emit `4` reclaims, and complete all 12 workloads.",
        },
    ],
    "st42": [
        {
            "peer_id": "st41",
            "relationship": "Exact A/B Twin (`t0` Non-Preemptive Borrowing `st42` vs. Instant Cohort Reclaim `st41`)",
            "knob_changed": "ONLY `reclaimWithinCohort` changes (`Never` in `st42` vs. `Any` in `st41`).",
            "cross_check": "`Sec 5.3` is strictly `0` in `st42` vs. `4` in `st41`, and `cq-lender` waits `~19s` (`st42`) vs. `< 2.5s` (`st41`).",
        },
    ],
    "st43": [
        {
            "peer_id": "st20",
            "relationship": "`t0`-Loaded `borrowingLimit: 8` (`st43`) vs. Fresh `borrowingLimit: 8` (`st20`) Pair",
            "knob_changed": "`st43` pre-fills `cq-capped`'s `16` nominal TPUs with 4 `t0` `PodGroup`s before 4 arrivals submit at `t=0s`, whereas `st20` submits all 8 as `NotAdmitted` at `t=0s`.",
            "cross_check": "Both enforce the exact `24`-TPU (`150.0%`) ceiling in `Sec 5.1` and yield `Sec 5.4 EFFECTIVE SAT ≈ 50.0%` (`2` arrivals admitted immediately + `2` queued `~22s` in `st43`).",
        },
    ],
    "st44": [
        {
            "peer_id": "st45",
            "relationship": "Exact A/B Twin (`t0` Cohort Monopolizer With `1:1` Fair-Sharing Preemption `st44` vs. Without Preemption `st45`)",
            "knob_changed": "`fairSharing.weight: '1'` + `reclaimWithinCohort: Any` (`st44`) vs. unweighted `reclaimWithinCohort: Never` (`st45`) against identical 8 `t0` monopolizer `PodGroup`s and 6 peer arrivals at `t=11s`.",
            "cross_check": "In `st44`, Fair Sharing evicts **exact 4** (never `0` and never `6`) of `cq-team-a`'s `t0` `PodGroup`s so `cq-team-b` immediately gets its `16`-TPU (`50%`) share while 2 of `cq-team-b`'s 6 jobs wait; in `st45`, `0` evictions occur and all 6 peer jobs wait `~19s`.",
        },
    ],
    "st45": [
        {
            "peer_id": "st44",
            "relationship": "Exact A/B Twin (`t0` Monopolizer Control `st45` vs. `1:1` Fair-Sharing Preemption `st44`)",
            "knob_changed": "`reclaimWithinCohort: Never` without weights (`st45`) vs. `1:1` Fair Sharing with `reclaimWithinCohort: Any` (`st44`).",
            "cross_check": "`Sec 5.3 Evictions` flips from `0` (`st45`) to `4` (`st44`), proving `t0` `PodGroup`s participate in both Cohort quota blocking (`st45`) and DRS Fair-Sharing preemption (`st44`).",
        },
    ],
}
SCENARIO_PAIRINGS.update(SCALE_SCENARIO_PAIRINGS)
SCENARIO_PAIRINGS.update(FAILURE_PREEMPTION_PAIRINGS)


def generate_scenario_agents_md(sc: ScenarioSpec, by_id: dict[str, ScenarioSpec]) -> str:
    """Auto-generate the per-scenario `AGENTS.md` inside `testscenarios/self-tests/<sc.dir_name>/`."""
    pairs = SCENARIO_PAIRINGS.get(sc.scenario_id, [])
    pair_dirs = [by_id[p["peer_id"]].dir_name for p in pairs if p["peer_id"] in by_id]
    pair_badges = ", ".join(f"[`{d}`](../{d}/AGENTS.md)" for d in pair_dirs) if pair_dirs else "None"

    lines: list[str] = [
        f"# Agent Execution & Cross-Examination Guide — `{sc.dir_name}`",
        "",
        "> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**",
        "> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).",
        "> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:",
        "> ```bash",
        "> python3 testscenarios/self-tests/generate_self_test_suite.py",
        "> python3 deploy/scripts/generate_scenario_readme.py --all",
        "> python3 testscenarios/self-tests/generate_self_test_suite.py --check",
        "> ```",
        "",
        "## 1. Scenario Identity & Dedicated Chamber Isolation",
        "",
        f"- **Scenario ID / Directory**: `{sc.dir_name}`",
        f"- **Category**: {sc.category}",
        f"- **Title**: {sc.title}",
        f"- **Primary Verification Target**: {sc.feature_focus}",
        f"- **Dedicated `kueue-optimizer` Chamber Namespace**: `{sc.chamber_namespace}` (`--namespace {sc.chamber_namespace}`)",
        f"- **Dedicated Local Port Allocation**: KWOK API `--port {sc.apiserver_port}` | Prometheus `{sc.prometheus_port}` (collision-free across all 45 scenarios `st01`–`st45`)",
        f"- **Mandatory Paired Counterpart(s)**: {pair_badges}",
        f"- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).",
        "",
        "---",
        "",
        "## 2. How to Fire This Test (Strict Separate-Chamber Isolation)",
        "",
        "**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.",
        "",
        "### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers",
        "",
        "Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:",
        "",
        "```bash",
        f"# Launch {sc.scenario_id.upper()} in dedicated kueue-optimizer chamber '{sc.chamber_namespace}' (API port {sc.apiserver_port}, Prom port {sc.prometheus_port}):",
        f"{sc.isolated_cmd} > /tmp/{sc.scenario_id}.log 2>&1 &",
        f"PID_{sc.scenario_id.upper()}=$!",
        "",
    ]

    wait_vars = [f'"$PID_{sc.scenario_id.upper()}"']
    for p in pairs:
        peer = by_id[p["peer_id"]]
        wait_vars.append(f'"$PID_{peer.scenario_id.upper()}"')
        lines.extend(
            [
                f"# Simultaneously launch paired {peer.scenario_id.upper()} in its own separate kueue-optimizer chamber '{peer.chamber_namespace}' (API port {peer.apiserver_port}, Prom port {peer.prometheus_port}):",
                f"{peer.isolated_cmd} > /tmp/{peer.scenario_id}.log 2>&1 &",
                f"PID_{peer.scenario_id.upper()}=$!",
                "",
            ]
        )

    lines.extend(
        [
            "# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:",
            f"wait {' '.join(wait_vars)}",
            "```",
            "",
            "### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers",
            "",
            "Run from the repository root:",
            "",
            "```bash",
            f"# 1. Run {sc.scenario_id.upper()} in its own isolated kueue-optimizer chamber '{sc.chamber_namespace}' (auto-deployed & auto-cleaned):",
            f"{sc.isolated_cmd} | tee /tmp/{sc.scenario_id}.log",
            "",
        ]
    )

    for idx, p in enumerate(pairs, start=2):
        peer = by_id[p["peer_id"]]
        lines.extend(
            [
                f"# {idx}. Run paired {peer.scenario_id.upper()} in its own separate fresh kueue-optimizer chamber '{peer.chamber_namespace}':",
                f"{peer.isolated_cmd} | tee /tmp/{peer.scenario_id}.log",
                "",
            ]
        )

    lines.extend(
        [
            "# Fast offline manifest & timeline sanity check (<1s, no cluster required):",
            f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc.dir_name} --dry-run",
            "```",
            "",
            "---",
            "",
            "## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol",
            "",
            "Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`"
            + sc.chamber_namespace
            + "`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:",
            "",
        ]
    )

    for p in pairs:
        peer = by_id[p["peer_id"]]
        lines.extend(
            [
                f"### Pair: `{sc.dir_name}` (`{sc.chamber_namespace}`) ↔ [`{peer.dir_name}`](../{peer.dir_name}/AGENTS.md) (`{peer.chamber_namespace}`) — {p['relationship']}",
                "",
                f"- **Controlled Variable Changed**: {p['knob_changed']}",
                f"- **Isolated Peer Command**: `{peer.isolated_cmd}`",
                f"- **Cross-Examination Assertion**: {p['cross_check']}",
                f"- **Expected Target Shift**:",
                f"  - `Sec 5.1 Quota Util`: `{sc.sec51_target}` (`{sc.scenario_id}`) vs. `{peer.sec51_target}` (`{peer.scenario_id}`)",
                f"  - `Sec 5.2 Wait Time`: `{sc.sec52_target}` (`{sc.scenario_id}`) vs. `{peer.sec52_target}` (`{peer.scenario_id}`)",
                f"  - `Sec 5.3 Evictions`: `{sc.sec53_target}` (`{sc.scenario_id}`) vs. `{peer.sec53_target}` (`{peer.scenario_id}`)",
                f"  - `Sec 5.4 Effective Sat`: `{sc.sec54_target}` (`{sc.scenario_id}`) vs. `{peer.sec54_target}` (`{peer.scenario_id}`)",
                "",
            ]
        )

    lines.extend(
        [
            "---",
            "",
            "## 4. Scenario Mechanics & Expected Live Metrics",
            "",
            f"- **Setup**: {sc.setup_summary}",
            f"- **Expected Runtime Behavior**: {sc.expected_behavior}",
            "",
            "### Target Metric Scorecard",
            "",
            "| Report Section | Expected Healthy Band |",
            "| :--- | :--- |",
            f"| **Section `5.1` Quota Utilization** | `{sc.sec51_target}` |",
            f"| **Section `5.2` Queue Wait Time** | `{sc.sec52_target}` |",
            f"| **Section `5.3` Evictions & Reclaims** | `{sc.sec53_target}` |",
            f"| **Section `5.4` Effective Saturation** | `{sc.sec54_target}` |",
            "",
            "### Detailed Metric Checkpoints",
            "",
        ]
    )
    for m in sc.expected_metrics:
        lines.append(f"- {m}")

    lines.extend(
        [
            "",
            "### Hard Failure Invariants (What Definitely MUST NOT Happen)",
            "",
        ]
    )
    for bad in sc.must_not_happen:
        lines.append(f"- ❌ {bad}")

    lines.extend(
        [
            "",
            "---",
            "",
            "## 5. Synchronization & Anti-Drift Rule",
            "",
            "If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:",
            "",
            "```bash",
            "python3 testscenarios/self-tests/generate_self_test_suite.py",
            "python3 deploy/scripts/generate_scenario_readme.py --all",
            "python3 testscenarios/self-tests/generate_self_test_suite.py --check",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def generate_suite_agents_md(scenarios: list[ScenarioSpec], by_id: dict[str, ScenarioSpec]) -> str:
    """Auto-generate the top-level `testscenarios/self-tests/AGENTS.md` governance file."""
    lines: list[str] = [
        "# Agent Instructions — `kueue-optimizer` 75-Scenario Self-Test Suite (`testscenarios/self-tests/`)",
        "",
        "> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**",
        "> Generated by [`generate_self_test_suite.py`](generate_self_test_suite.py). Always update `generate_self_test_suite.py`, `scale_scenarios.py`, or `failure_preemption_scenarios.py` first and re-run together with `python3 deploy/scripts/generate_scenario_readme.py --all` so all 75 scenarios, per-scenario `AGENTS.md` guides, per-scenario `README.md` summaries, and top-level docs stay 100% synchronized.",
        "",
        "---",
        "",
        "## Mandatory Rule 1: Fire Paired Scenarios in Separate Isolated `kueue-optimizer` Chambers (Zero Metric Cross-Talk)",
        "",
        "Every scenario in `testscenarios/self-tests/` (`st01`–`st75`) is paired with one or more controlled A/B counterpart scenarios (including `st31`–`st45` dedicated to `t0` initial workloads, `st46`–`st60` dedicated to `>100`-node, `>1,000`-workload scale, and `st61`–`st75` dedicated to node failures, TAS placement hot-swap, `waitForPodsReady` scaling, and interrupted-workload replay).",
        "",
        "Whenever an agent runs, debugs, or evaluates ANY test scenario in this directory (or modifies `deploy/scripts/testrunner.py`, `deploy/scripts/runner/`, `deploy/scripts/chamber/`, `deploy/scripts/kueue_metrics_report.py`, or `scraper/`):",
        "",
        "1. **NEVER Share or Reuse a Chamber Across Scenarios (`--skip-chamber-deploy` Forbidden Across Pairs)**:",
        "   - Inside a live `kueue-optimizer` chamber, `kueue-controller-manager` accumulates monotonic counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`) and cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), while the chamber's `prometheus` instance retains historical TSDB series and stale queue/cohort labels.",
        "   - Reusing the same chamber across two runs contaminates Run B's Sections `1–4` and `5.1–5.3` with Run A's metric state.",
        "   - **Every scenario `stXX` MUST run in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX`) with its own non-overlapping local port (`--port 164XX`, which automatically binds Prometheus to `19047 + XX` and `kwok-registry` NodePort to `30457 + XX`)** so every run starts from a completely clean `etcd`, `kwok-kube-apiserver`, `kueue-controller-manager`, and `prometheus` at exact zero baseline.",
        "2. **Read the Scenario's Local `AGENTS.md` & Launch Up to 7 Isolated Chambers in Parallel**:",
        "   - Open `testscenarios/self-tests/<scenario>/AGENTS.md` for copy-paste parallel launch commands (`--namespace kueue-optimizer-chamber-stXX --port 164XX > /tmp/stXX.log 2>&1 &`) and exact CLI flags (`--duration`, `--no-skip-initial-offset`, `--no-fair-sharing`, `--wait-for-pods-ready-*`, `--interrupted-duration-*`, `--scrape-output-dir`).",
        "3. **Cross-Examine the Paired Outputs Explicitly**:",
        "   - Never declare a scenario healthy from a single metric in isolation. Compare `Execution Summary` (including `Initial Kueue Workloads` and `Initial Infra Pods`), `Section 5.1` (Quota Utilization), `Section 5.2` (Wait Time by Priority), `Section 5.3` (Total Evictions & Unique Evicted), and `Section 5.4` (Effective Uninterrupted Cluster Saturation) side-by-side across the isolated `kueue-optimizer` chambers.",
        "",
        "---",
        "",
        "## Mandatory Rule 2: Complete 75-Scenario Isolated-Chamber Pairing & Launch Matrix",
        "",
        "| ID | Scenario (`AGENTS.md`) | Dedicated Chamber (`API/Prom Port`) | Mandatory Pair(s) | Isolated Launch Command | Core Cross-Examination Check |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    for sc in scenarios:
        pairs = SCENARIO_PAIRINGS.get(sc.scenario_id, [])
        peer_links = ", ".join(
            f"[`{p['peer_id']}`]({by_id[p['peer_id']].dir_name}/AGENTS.md)"
            for p in pairs
            if p["peer_id"] in by_id
        )
        primary_check = pairs[0]["cross_check"] if pairs else sc.feature_focus
        lines.append(
            f"| **{sc.scenario_id.upper()}** | [`{sc.dir_name}`]({sc.dir_name}/AGENTS.md) | `{sc.chamber_namespace}` (`{sc.apiserver_port}/{sc.prometheus_port}`) | {peer_links} | `{sc.isolated_cmd}` | {primary_check} |"
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Mandatory Rule 3: Zero-Drift Synchronization & Full Auto-Regeneration",
            "",
            "All **377 generated artifacts** inside `testscenarios/self-tests/` derive from [`testscenarios/self-tests/generate_self_test_suite.py`](generate_self_test_suite.py) (with `st46`–`st60` in [`scale_scenarios.py`](scale_scenarios.py) and `st61`–`st75` in [`failure_preemption_scenarios.py`](failure_preemption_scenarios.py)) plus [`deploy/scripts/generate_scenario_readme.py`](../../deploy/scripts/generate_scenario_readme.py):",
            "",
            "- `75 × 3 = 225` scenario trace files (`stXX-.../{infra,workflows,execution-time}.json`)",
            "- `75` per-scenario agent guides (`stXX-.../AGENTS.md`)",
            "- `75` per-scenario statistical summaries (`stXX-.../README.md`)",
            "- `1` top-level master reference (`testscenarios/self-tests/README.md`)",
            "- `1` top-level agent governance file (`testscenarios/self-tests/AGENTS.md`)",
            "",
            "### Whenever ANY Definition, Metric Formula, Pairing Rule, or Doc Changes (or Drift Is Suspected)",
            "",
            "Execute the complete 4-step synchronization sequence from the repository root and commit all updated files together:",
            "",
            "```bash",
            "# Step 1: Regenerate all 75 scenario JSON triplets, all 75 stXX/AGENTS.md files,",
            "#         top-level testscenarios/self-tests/AGENTS.md, and master README.md:",
            "python3 testscenarios/self-tests/generate_self_test_suite.py",
            "",
            "# Step 2: Regenerate all per-scenario README.md summaries across the repo:",
            "python3 deploy/scripts/generate_scenario_readme.py --all",
            "",
            "# Step 3: Verify byte-for-byte zero drift across all generated suite files:",
            "python3 testscenarios/self-tests/generate_self_test_suite.py --check",
            "",
            "# Step 4: Run the unit test suites:",
            "python3 deploy/scripts/test_testrunner_and_readme.py",
            "python3 deploy/scripts/test_kueue_metrics_report.py",
            "```",
            "",
            "**Never** hand-edit an individual `stXX-.../infra.json`, `stXX-.../workflows.json`, `stXX-.../execution-time.json`, `stXX-.../AGENTS.md`, `stXX-.../README.md`, `testscenarios/self-tests/README.md`, or `testscenarios/self-tests/AGENTS.md` without updating the generator and re-running the commands above.",
            "",
        ]
    )
    return "\n".join(lines)


def generate_master_readme(scenarios: list[ScenarioSpec]) -> str:
    by_id = {sc.scenario_id: sc for sc in scenarios}
    lines: list[str] = [
        "# `kueue-optimizer` Live Self-Test Suite (`testscenarios/self-tests/`)",
        "",
        "This directory contains **75 purpose-built, mathematically closed-form live test scenarios** (`st01`–`st75`) generated by [`generate_self_test_suite.py`](generate_self_test_suite.py), [`scale_scenarios.py`](scale_scenarios.py), and [`failure_preemption_scenarios.py`](failure_preemption_scenarios.py) (with agent pairing & separate-chamber isolation rules in [`AGENTS.md`](AGENTS.md)) to verify end-to-end correctness of:",
        "",
        "1. **Kueue live scheduling, `t0` initial-workload bootstrapping (`PodGroup` + `infra-pods`), queueing, preemption, cohort borrowing/reclaiming, fair sharing, Topology-Aware Scheduling (TAS), failed-node hot-swap, `waitForPodsReady` timeout scaling, and interrupted-workload replay** inside isolated **`kueue-optimizer` evaluation chambers** (from 4–16 node micro-benchmarks up to **256-node, 1,200-workload** scale).",
        "2. **Live `cluster-scraper` fidelity** across both vanilla Kubernetes deployments/APIs (`JobSet`, `Job`, native `PodGroup`, `Deployment`, standalone `Pod`, unmanaged `infra-pods`), real-time `Node` failure/recovery/deletion watching, and interrupted/cutoff `PartialExecution` recording (`Preempted`, `NodeFailure`, `PodsReadyTimeout`, `ScrapeEnded`).",
        "3. **Prometheus metric scraping & catalog coverage** in [`deploy/scripts/kueue_metrics_report.py`](../../deploy/scripts/kueue_metrics_report.py) (Sections `1`–`4`).",
        "4. **All four high-level aggregations** in `AGGREGATED KUEUE METRICS SUMMARY`:",
        "   - **Section `5.1`**: ClusterQueue & Cohort Quota Utilization per Resource Type (`P50`, `P90`, `P99`, `AVG`, `AVG USED / QUOTA`)",
        "   - **Section `5.2`**: Wait Time Across All ClusterQueues per Workload Priority (`P50`, `P90`, `P99`, `AVG`, `WORKLOADS`)",
        "   - **Section `5.3`**: Total Evictions and Reclaims (`Total Evictions`, `Unique workloads evicted once`, breakdown by reason & priority)",
        "   - **Section `5.4`**: Effective Uninterrupted Cluster Saturation (`COMPLETED ONLY`, `ACTIVE AT END`, `EFFECTIVE SAT`, `FULL-DUR SAT`, `Initial Infra Pods`, and exclusion of preempted/unadmitted work)",
        "",
        "---",
        "",
        "## 1. How the 75 Test Cases Were Designed (15 Categories × 5 Scenarios)",
        "",
        "Every scenario in `testscenarios/self-tests/` uses power-of-two/multiple-of-16 integer capacities (`4`–`16` simulated KWOK nodes for `st01`–`st45` & `st61`–`st75`; **`128`–`256` nodes and `1,048`–`1,200` workloads** for `st46`–`st60`), fast native wall-clock execution, and collision-free per-scenario chamber namespaces (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st75`, ports `16401` .. `16475`):",
        "",
        "- **Category 1 (`st01`–`st05`)**: Baseline saturation levels (`25%`, `50%` quota-capped, `100%`), multi-pod per-pod resource scaling (`2×4` + `4×4` TPUs), and additive `COMPLETED ONLY + ACTIVE AT END` decomposition.",
        "- **Category 2 (`st06`–`st10`)**: Queueing discipline (`StrictFIFO` HoL blocking vs. `BestEffortFIFO` backfill), 4-wave backlog wait-time staircase, non-preemptive priority ordering, and 7-level TAS binary-tree cube packing.",
        "- **Category 3 (`st11`–`st15`)**: Intra-ClusterQueue preemption policies (`Never`, `LowerPriority`, `LowerOrNewerEqualPriority`), 4-tier cascading preemptions, and adversarial late-preemption badput collapse.",
        "- **Category 4 (`st16`–`st20`)**: Multi-CQ quota isolation vs. Cohort borrowing (`200%` of nominal quota), instant Cohort reclaim (`reclaimWithinCohort: Any` vs. `Never`), and explicit `borrowingLimit` / `lendingLimit` ceilings (`150%`).",
        "- **Category 5 (`st21`–`st25`)**: Fair-sharing spectrum from unweighted monopolization (`st21`) to symmetric `1:1` DRF (`st22`), asymmetric `7:1` DRF (`st23`), 6-CQ geometric hierarchy `1..32` (`st24`), and Classical `borrowWithinCohort.maxPriorityThreshold` gating (`st25`).",
        "- **Category 6 (`st26`–`st30`)**: Real-world Kubernetes/Kueue accounting edge cases — pre-existing `t0` unmanaged pods in `infra-pods` (`st26`), host CPU/RAM bottleneck inversion (`st27`), `initContainers` high-water mark + `spec.overhead` (`st28`), multi-role heterogeneous `JobSet` PodSets (`st29`), and permanently `inadmissible` oversized jobs (`st30`).",
        "- **Category 7 (`st31`–`st35`) — Initial `t0` Workloads: Handling & Accounting**: Pre-admitted `t0` Kueue `PodGroup` quota occupancy & offset-jump remaining duration (`st31`), hybrid `t0` Kueue `PodGroup`s + non-Kueue `infra-pods` separation (`st32`), legacy `--hash-names` Tier-3 matching + Tier-4 missing-pod synthesis (`st33`), multi-role heterogeneous `t0` `PodGroup` divisibility (`st34`), and infinite-duration `t0` `PodGroup` `ACTIVE AT END` / `FULL-DUR SAT` non-inflation (`st35`).",
        "- **Category 8 (`st36`–`st40`) — Initial `t0` Workloads: Preemption & TAS**: Non-preemptive `t0` protection (`st36`) vs. active `LowerPriority` preemption + dynamic parent `JobSet` resubmission without `Workload` name collisions (`st37`), `LowerOrNewerEqualPriority` `t0` seniority protection (`st38`), adversarial `t0` late-preemption badput exclusion (`st39`), and `t0` 7-level TAS topology domain occupancy (`st40`).",
        "- **Category 9 (`st41`–`st45`) — Initial `t0` Workloads: Reclaims, Limits & Fair Sharing**: Cohort reclamation against `200%` `t0` borrowers (`st41` `Any` vs. `st42` `Never`), `borrowingLimit: 8` / `lendingLimit: 8` enforced on top of `t0` nominal load (`st43`), and `1:1` Cohort Fair-Sharing preemption evicting exact `4` of `8` `t0` monopolizer `PodGroup`s (`st44`) vs. `reclaimWithinCohort: Never` control (`st45`).",
        "- **Category 10 (`st46`–`st50`) — Large-Scale Quota-Only Admission (`>100` Nodes, `>1,000` Workloads, No TAS)**: `128`-node `1,056`-workload full saturation (`st46`), `128`-node `1,080`-workload intra-CQ preemption & `64`-job `t0` resubmit (`st47`), `144`-node `1,116`-workload Cohort `200%` borrowing & `72`-job reclaim (`st48`), `160`-node `1,160`-workload 4-CQ Fair Sharing (`st49`), and `128`-node `1,064`-workload multi-API (`JobSet`, `Job`, `PodGroup`, infinite `Deployment`) + `16` `infra-pods` parity (`st50`).",
        "- **Category 11 (`st51`–`st55`) — Large-Scale TAS Packing, Slices & Multi-Pool (`>100` Nodes, `>1,000` Workloads)**: `128`-node `1,088`-workload 7-level TAS packing (`st51`), `160`-node `1,120`-workload `4`/`2`/`1`-node mixed TAS slices (`st52`), `128`-node `1,056`-workload rack-fragmentation blocking & `BestEffortFIFO` backfill (`st53`), `144`-node `1,104`-workload partial vanilla-K8s `t0` TAS binding + `required`/`preferred`/`unconstrained` modes (`st54`), and `256`-node (`2,048`-TPU) `1,200`-workload dual-`ResourceFlavor` TAS stress (`st55`).",
        "- **Category 12 (`st56`–`st60`) — Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (`>100` Nodes, `>1,000` Workloads)**: `128`-node `1,060`-workload `2x2x2-id` TAS preemption (`st56`), `128`-node `1,088`-workload TAS Cohort `200%` borrowing & `64`-job reclaim (`st57`), `160`-node `1,140`-workload 4-CQ TAS Fair Sharing (`st58`), `128`-node `1,048`-workload TAS `borrowingLimit: 256` (`150%`) cap (`st59`), and `144`-node `1,080`-workload hybrid multi-API TAS + `16` `infra-pods` + `16` infinite `Deployment`s scraper stress (`st60`).",
        "- **Category 13 (`st61`–`st65`) — Mid-Scenario Node Failures & Placement Hot-Swap**: TAS single-node `NotReady` in-place hot-swap (`st61`), TAS `Node` deletion hot-swap + post-`t0` `Node` `New` scale-up (`st62`), TAS `0`-spare `FailFast` `NodeFailures` eviction & recovery (`st63`), sequential dual-node failure (`st64`), and non-TAS `NoExecute` pod rescheduling + `LowerPriority` preemption + recovery (`st65`).",
        "- **Category 14 (`st66`–`st70`) — `waitForPodsReady` Timeout Scaling (`--speedup` & `--duration`)**: Native `1.0x` `8s` timeout eviction (`st66`), exact `5.0x` `--speedup` scaled `40s -> 8s` twin (`st67`), `10m` timeout auto-clamped to `50%` of `--duration 24s` (`12s`, `st68`), post-node-failure `recoveryTimeout` eviction (`st69`), and `blockAdmission` startup gating (`st70`).",
        "- **Category 15 (`st71`–`st75`) — Interrupted-Workload Recording, Multiplier & True-Length Replay**: Preemption-interrupted `PartialExecution` replay with `--interrupted-duration-multiplier 2.5` (`8s -> 20s`, `st71`), schema `trueDurationSeconds` + CLI `--interrupted-duration-override` (`st72`), `NodeFailure` `missingDurationMultiplier: 2.5` alongside untouched `FullExecution` peers (`st73`), interrupted `t0` `PodGroup` extension + `ScrapeEnded` shutdown flush (`st74`), and combined multi-fault TAS hot-swap + `t0` preemption + `waitForPodsReady` stress (`st75`).",
        "",
        "---",
        "",
        "## 2. Master Overview Matrix of All 75 Scenarios (`st01`–`st75`)",
        "",
        "| ID | Scenario Directory (`README` / `AGENTS`) | Chamber (`Port`) | Paired With | Sec `5.1` Quota Util | Sec `5.2` Wait Time | Sec `5.3` Evictions | Sec `5.4` Effective Sat |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    for sc in scenarios:
        pairs = SCENARIO_PAIRINGS.get(sc.scenario_id, [])
        peer_str = ", ".join(
            f"[`{p['peer_id']}`]({by_id[p['peer_id']].dir_name}/AGENTS.md)"
            for p in pairs
            if p["peer_id"] in by_id
        )
        lines.append(
            f"| **{sc.scenario_id.upper()}** | [`{sc.dir_name}`]({sc.dir_name}/README.md) ([`AGENTS`]({sc.dir_name}/AGENTS.md)) | `{sc.chamber_namespace}` (`{sc.apiserver_port}`) | {peer_str} | {sc.sec51_target} | {sc.sec52_target} | {sc.sec53_target} | {sc.sec54_target} |"
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## 3. Detailed Per-Scenario Reference (`st01` through `st75`)",
            "",
        ]
    )

    current_cat = ""
    for sc in scenarios:
        if sc.category != current_cat:
            current_cat = sc.category
            lines.append(f"### Category {current_cat}")
            lines.append("")
        pairs = SCENARIO_PAIRINGS.get(sc.scenario_id, [])
        peer_links = ", ".join(
            f"[`{by_id[p['peer_id']].dir_name}`]({by_id[p['peer_id']].dir_name}/AGENTS.md)"
            for p in pairs
            for _ in [0]
            if p["peer_id"] in by_id
        )
        lines.append(f"#### `{sc.dir_name}` — {sc.title}")
        lines.append("")
        lines.append(f"- **Feature Focus**: {sc.feature_focus}")
        lines.append(
            f"- **Isolated Run Command**: `{sc.isolated_cmd}` (Guides: [`{sc.dir_name}/AGENTS.md`]({sc.dir_name}/AGENTS.md) | [`{sc.dir_name}/README.md`]({sc.dir_name}/README.md))"
        )
        lines.append(f"- **Side-by-Side Pair(s)**: {peer_links}")
        lines.append(f"- **Scenario Setup**: {sc.setup_summary}")
        lines.append(f"- **Expected Live Behavior**: {sc.expected_behavior}")
        lines.append("- **What MUST Be Visible in the Metrics**:")
        for m in sc.expected_metrics:
            lines.append(f"  - {m}")
        lines.append("- **What Definitely MUST NOT Happen (Red Flags / Regression Bugs)**:")
        for bad in sc.must_not_happen:
            lines.append(f"  - ❌ {bad}")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## 4. How to Run, Validate & Regenerate the Suite",
            "",
            "### Regenerate All 75 Scenarios, All `AGENTS.md` Guides & All `README.md` Summaries",
            "",
            "```bash",
            "python3 testscenarios/self-tests/generate_self_test_suite.py",
            "python3 deploy/scripts/generate_scenario_readme.py --all",
            "python3 testscenarios/self-tests/generate_self_test_suite.py --check",
            "```",
            "",
            "### Dry-Run Structural Validation of All 75 Scenarios (Offline, `< 10s`)",
            "",
            "```bash",
            "for d in testscenarios/self-tests/st*/; do",
            '  python3 deploy/scripts/testrunner.py "$d" --dry-run > /dev/null || exit 1',
            "done",
            'echo "All 75 scenarios passed dry-run validation."',
            "```",
            "",
            "### Live Parallel Execution Across Separate `kueue-optimizer` Chambers (Up to 7 Concurrent Chambers)",
            "",
            "Always fire scenarios in **separate `kueue-optimizer` chambers** (`--namespace kueue-optimizer-chamber-stXX --port 164XX`):",
            "",
            "```bash",
            "# Example: Fire t0 Intra-CQ Preemption Never (st36) vs. LowerPriority (st37) concurrently:",
            "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never \\",
            "  --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436 > /tmp/st36.log 2>&1 &",
            "PID_ST36=$!",
            "",
            "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit \\",
            "  --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 > /tmp/st37.log 2>&1 &",
            "PID_ST37=$!",
            "",
            'wait "$PID_ST36" "$PID_ST37"',
            "```",
            "",
            "---",
            "",
            "## 5. Scope Boundaries & Limitations",
            "",
            "1. **Simulated Pod Execution Physics**: KWOK simulates pod state transitions (`Pending -> Running -> Succeeded`) via timer annotations (`kueue-optimizer.gke.io/pod-execution-time-seconds` and `kueue-optimizer.gke.io/pod-startup-delay-seconds`) rather than real XLA compilation or HBM/ICI hardware faults.",
            "2. **Deterministic Trace-Driven Node Events**: Scenarios `st01`–`st60` use a static `t0` node inventory, while `st61`–`st75` replay deterministic post-`t0` `Node` `NotReady`/`NoExecute`/`Deleted`/`New` events without live cloud-provider `ProvisioningRequest` autoscaler loops.",
            "3. **Fixed Pod Resource Requests**: Elastic in-place vertical pod resizing mid-execution is not exercised.",
            "4. **Single-Cluster Cohorts**: MultiKueue multi-cluster federation and deep multi-level `Cohort` trees are outside the scope of these single-chamber scenarios.",
            "",
        ]
    )
    return "\n".join(lines)


def _format_jsonl(records: list[dict[str, Any]]) -> str:
    lines = [json.dumps(r, separators=(",", ":"), sort_keys=False) for r in records]
    return "\n".join(lines) + ("\n" if lines else "")


def build_expected_suite_files() -> dict[Path, str]:
    """Return exact `{path: expected_utf8_content}` for all 302 generator-owned suite artifacts."""
    scenarios = build_all_75_scenarios()
    by_id = {sc.scenario_id: sc for sc in scenarios}
    assert set(SCENARIO_PAIRINGS.keys()) == set(by_id.keys()), (
        f"SCENARIO_PAIRINGS keys mismatch: {set(SCENARIO_PAIRINGS.keys()) ^ set(by_id.keys())}"
    )

    expected: dict[Path, str] = {}
    for sc in scenarios:
        sc_dir = SUITE_DIR / sc.dir_name
        expected[sc_dir / "infra.json"] = _format_jsonl(sc.infra_events)
        expected[sc_dir / "workflows.json"] = _format_jsonl(sc.workflow_events)
        expected[sc_dir / "execution-time.json"] = _format_jsonl(sc.exec_events)
        expected[sc_dir / "AGENTS.md"] = generate_scenario_agents_md(sc, by_id)

    expected[SUITE_DIR / "AGENTS.md"] = generate_suite_agents_md(scenarios, by_id)
    expected[SUITE_DIR / "README.md"] = generate_master_readme(scenarios)
    return expected


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate or verify all 75 kueue-optimizer self-test scenarios, READMEs, and AGENTS.md files."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify that all generated JSONL, AGENTS.md, and README.md files match the generator with zero drift.",
    )
    args = parser.parse_args()

    expected = build_expected_suite_files()
    rel_suite = SUITE_DIR.relative_to(REPO_ROOT)

    if args.check:
        drifted: list[str] = []
        for path, exp_text in sorted(expected.items()):
            if not path.is_file():
                drifted.append(f"MISSING: {path.relative_to(REPO_ROOT)}")
            elif path.read_text(encoding="utf-8") != exp_text:
                drifted.append(f"DRIFTED: {path.relative_to(REPO_ROOT)}")
        for sc_dir in sorted(p for p in SUITE_DIR.iterdir() if p.is_dir() and p.name.startswith("st")):
            rm = sc_dir / "README.md"
            if not rm.is_file():
                drifted.append(f"MISSING SCENARIO README: {rm.relative_to(REPO_ROOT)}")
        if drifted:
            print("Self-test suite synchronization check FAILED:", file=sys.stderr)
            for item in drifted:
                print(f"  - {item}", file=sys.stderr)
            print(
                "\nFix by running:\n"
                "  python3 testscenarios/self-tests/generate_self_test_suite.py\n"
                "  python3 deploy/scripts/generate_scenario_readme.py --all",
                file=sys.stderr,
            )
            sys.exit(1)
        print(
            f"OK: All {len(expected)} generated files + 75 per-scenario README.md files in "
            f"{rel_suite} are 100% in sync."
        )
        return

    for path, content in expected.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    print(
        f"Generated {len(expected)} suite artifacts across 75 scenarios in {rel_suite}:\n"
        f"  - 225 trace files (infra.json, workflows.json, execution-time.json)\n"
        f"  - 75 per-scenario AGENTS.md guides\n"
        f"  - 1 top-level AGENTS.md ({rel_suite / 'AGENTS.md'})\n"
        f"  - 1 top-level README.md ({rel_suite / 'README.md'})"
    )


if __name__ == "__main__":
    main()
