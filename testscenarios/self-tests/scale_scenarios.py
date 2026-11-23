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

"""Large-scale (>1,000 workloads, >100 nodes) scenarios `st46`–`st60` for `testscenarios/self-tests/`.

Covers:
- Category 10 (`st46`–`st50`, 5 scenarios = 33.3%): Quota-only admission (no TAS) at 128–160 nodes and
  1,056–1,160 workloads with in-flight `t0` workloads (saturation, intra-CQ preemption + dynamic resubmit,
  cohort borrowing/reclaim, 4-CQ fair sharing, and multi-API vanilla K8s + `infra-pods` + infinite Deployments).
- Category 11 (`st51`–`st55`, 5 scenarios = 33.3%): Topology-Aware Scheduling (TAS) packing, multi-slice
  `JobSet`s, `BestEffortFIFO` rack backfilling, preferred/unconstrained + partial-pod vanilla K8s `t0` binding,
  and dual-flavor 256-node / 1,200-workload scale.
- Category 12 (`st56`–`st60`, 5 scenarios = 33.3%): Large-scale TAS intra-CQ preemption, TAS cohort reclaim,
  4-CQ TAS fair sharing, TAS `borrowingLimit` / `lendingLimit` enforcement, and hybrid multi-API TAS +
  `infra-pods` + infinite Deployment scraper stress.
"""

from __future__ import annotations

import hashlib
from typing import Any

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

KWOK_FAKE_TOLERATION = {
    "key": "kwok.x-k8s.io/node",
    "operator": "Equal",
    "value": "fake",
    "effect": "NoSchedule",
}


def _scale_node_labels(idx: int, *, with_topology: bool, accel_label: str = DEFAULT_ACCEL_LABEL) -> dict[str, str]:
    node_name = f"st-node-{idx:03d}"
    cube_idx = idx // 16
    local_idx = idx % 16
    block_idx = cube_idx // 4
    labels: dict[str, str] = {
        "type": "kwok",
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/os": "linux",
        "kubernetes.io/role": "agent",
        "node-role.kubernetes.io/agent": "",
        "kueue-optimizer.gke.io/node-role": "agent",
        "kubernetes.io/hostname": node_name,
        "cloud.google.com/gke-tpu-accelerator": accel_label,
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
    return labels


def _make_scale_node(
    idx: int,
    *,
    det_uid_fn: Any,
    tpu_per_node: int = 8,
    cpu_per_node: int = 32,
    mem_gi_per_node: int = 128,
    pods_per_node: int = 110,
    with_topology: bool = False,
    accel_label: str = DEFAULT_ACCEL_LABEL,
) -> dict[str, Any]:
    node_name = f"st-node-{idx:03d}"
    labels = _scale_node_labels(idx, with_topology=with_topology, accel_label=accel_label)
    cap: dict[str, str] = {
        "cpu": str(cpu_per_node),
        "memory": f"{mem_gi_per_node}Gi",
        "pods": str(pods_per_node),
        "google.com/tpu": str(tpu_per_node),
    }
    annotations: dict[str, str] = {
        "kwok.x-k8s.io/node": "fake",
        "node.alpha.kubernetes.io/ttl": "0",
        "kueue-optimizer.gke.io/node-cpu": str(cpu_per_node),
        "kueue-optimizer.gke.io/node-memory": f"{mem_gi_per_node}Gi",
        "kueue-optimizer.gke.io/node-pods": str(pods_per_node),
        "kueue-optimizer.gke.io/node-accel-resource": "google.com/tpu",
        "kueue-optimizer.gke.io/node-accel-count": str(tpu_per_node),
    }
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "uid": det_uid_fn(f"node:{node_name}"),
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


def _build_tas_podset_assignment(
    *,
    ps_name: str,
    node_indices: list[int],
    tpu_per_pod: int,
    cpu_per_pod: int,
    mem_gi_per_pod: int,
    flavor_name: str = DEFAULT_FLAVOR,
    accel_label: str = DEFAULT_ACCEL_LABEL,
) -> dict[str, Any]:
    count = len(node_indices)
    lbls_per_node = [_scale_node_labels(idx, with_topology=True, accel_label=accel_label) for idx in node_indices]
    values_per_level: list[dict[str, Any]] = []
    for lk in TOPOLOGY_LEVELS:
        vals = [nl[lk] for nl in lbls_per_node]
        if len(set(vals)) == 1:
            values_per_level.append({"universal": vals[0]})
        else:
            values_per_level.append({"individual": {"roots": vals}})
    return {
        "name": ps_name,
        "count": count,
        "flavors": {
            "cpu": flavor_name,
            "memory": flavor_name,
            "google.com/tpu": flavor_name,
        },
        "resourceUsage": {
            "cpu": str(cpu_per_pod * count),
            "memory": f"{mem_gi_per_pod * count}Gi",
            "google.com/tpu": str(tpu_per_pod * count),
        },
        "topologyAssignment": {
            "levels": list(TOPOLOGY_LEVELS),
            "slices": [
                {
                    "podCount": count,
                    "valuesPerLevel": values_per_level,
                }
            ],
        },
    }


def _build_batch_job_obj(
    *,
    name: str,
    ts_create: str,
    duration_sec: float,
    local_queue: str,
    det_uid_fn: Any,
    num_pods: int = 1,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 4,
    mem_gi_per_pod: int = 16,
    priority_class: str | None = None,
    namespace: str = DEFAULT_NS,
    tas_required_level: str | None = None,
    tas_preferred_level: str | None = None,
    tas_unconstrained: bool = False,
    accel_label: str | None = DEFAULT_ACCEL_LABEL,
    suspend: bool = True,
) -> dict[str, Any]:
    labels: dict[str, str] = {"kueue.x-k8s.io/queue-name": local_queue}
    if priority_class:
        labels["kueue.x-k8s.io/priority-class"] = priority_class

    pod_anns: dict[str, str] = {
        "kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(round(duration_sec))),
    }
    if tas_required_level:
        pod_anns["kueue.x-k8s.io/podset-required-topology"] = tas_required_level
    elif tas_preferred_level:
        pod_anns["kueue.x-k8s.io/podset-preferred-topology"] = tas_preferred_level
    elif tas_unconstrained:
        pod_anns["kueue.x-k8s.io/podset-unconstrained-topology"] = "true"

    reqs = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
        "google.com/tpu": str(tpu_per_pod),
    }
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "tolerations": [dict(KWOK_FAKE_TOLERATION)],
        "containers": [
            {
                "name": "worker",
                "image": "registry.k8s.io/pause:3.10",
                "resources": {"requests": dict(reqs), "limits": dict(reqs)},
            }
        ],
    }
    if accel_label:
        pod_spec["nodeSelector"] = {"cloud.google.com/gke-tpu-accelerator": accel_label}

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": det_uid_fn(f"job:{namespace}/{name}"),
            "creationTimestamp": ts_create,
            "labels": labels,
            "annotations": dict(pod_anns),
        },
        "spec": {
            "suspend": bool(suspend),
            "parallelism": int(num_pods),
            "completions": int(num_pods),
            "backoffLimit": 0,
            "template": {
                "metadata": {"annotations": dict(pod_anns)},
                "spec": pod_spec,
            },
        },
    }


def _add_arriving_batch_job(
    wf_list: list[dict[str, Any]],
    ex_list: list[dict[str, Any]],
    *,
    fmt_ts_fn: Any,
    det_uid_fn: Any,
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
    tas_required: str | None = None,
    tas_preferred: str | None = None,
    tas_unconstrained: bool = False,
    accel_label: str | None = DEFAULT_ACCEL_LABEL,
) -> None:
    ts_create = fmt_ts_fn(arrival_sec)
    ts_end = fmt_ts_fn(arrival_sec + duration_sec)
    wl_hash = hashlib.sha256(f"{DEFAULT_NS}/{name}".encode("utf-8")).hexdigest()[:5]
    wl_name = f"job-{name}-{wl_hash}"
    job_obj = _build_batch_job_obj(
        name=name,
        ts_create=ts_create,
        duration_sec=duration_sec,
        local_queue=lq,
        det_uid_fn=det_uid_fn,
        num_pods=num_pods,
        tpu_per_pod=tpu_per_pod,
        cpu_per_pod=cpu_per_pod,
        mem_gi_per_pod=mem_gi_per_pod,
        priority_class=prio,
        tas_required_level=tas_required,
        tas_preferred_level=tas_preferred,
        tas_unconstrained=tas_unconstrained,
        accel_label=accel_label,
        suspend=True,
    )
    wf_list.append(
        {
            "timestamp": ts_create,
            "eventType": "New",
            "workloadState": "NotAdmitted",
            "payload": {
                "objectKind": "Job",
                "name": name,
                "namespace": DEFAULT_NS,
                "creationTime": ts_create,
                "localQueueName": lq,
                "object": job_obj,
            },
        }
    )
    ex_list.extend(
        [
            {
                "timestamp": ts_create,
                "eventType": "Admitted",
                "workloadState": "Admitted",
                "payload": {
                    "workloadName": wl_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "Job",
                    "ownerName": name,
                    "ownerAPIVersion": "batch/v1",
                    "localQueueName": lq,
                    "clusterQueueName": cq,
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
                    "namespace": DEFAULT_NS,
                    "ownerKind": "Job",
                    "ownerName": name,
                    "ownerAPIVersion": "batch/v1",
                    "localQueueName": lq,
                    "clusterQueueName": cq,
                    "creationTime": ts_create,
                    "admittedTime": ts_create,
                    "endTime": ts_end,
                    "executionDurationSeconds": float(duration_sec),
                    "executionDuration": f"{int(round(duration_sec))}s",
                    "preempted": False,
                    "reason": "Succeeded",
                    "message": "job completed successfully",
                },
            },
        ]
    )


def _add_t0_batch_job(
    inf_list: list[dict[str, Any]],
    wf_list: list[dict[str, Any]],
    ex_list: list[dict[str, Any]],
    *,
    fmt_ts_fn: Any,
    det_uid_fn: Any,
    name: str,
    remaining_sec: float,
    full_duration_sec: float,
    lq: str,
    cq: str,
    node_indices: list[int],
    bound_pod_count: int | None = None,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 4,
    mem_gi_per_pod: int = 16,
    prio: str | None = None,
    prio_val: int | None = None,
    tas_required: str | None = None,
    tas_preferred: str | None = None,
    with_tas_psa: bool = False,
    flavor_name: str = DEFAULT_FLAVOR,
    accel_label: str = DEFAULT_ACCEL_LABEL,
) -> None:
    t0_str = fmt_ts_fn(0.0)
    t_end = fmt_ts_fn(remaining_sec)
    num_pods = len(node_indices)
    actual_bound = num_pods if bound_pod_count is None else min(num_pods, bound_pod_count)
    wl_hash = hashlib.sha256(f"{DEFAULT_NS}/{name}".encode("utf-8")).hexdigest()[:5]
    wl_name = f"job-{name}-{wl_hash}"

    job_obj = _build_batch_job_obj(
        name=name,
        ts_create=t0_str,
        duration_sec=full_duration_sec,
        local_queue=lq,
        det_uid_fn=det_uid_fn,
        num_pods=num_pods,
        tpu_per_pod=tpu_per_pod,
        cpu_per_pod=cpu_per_pod,
        mem_gi_per_pod=mem_gi_per_pod,
        priority_class=prio,
        tas_required_level=tas_required,
        tas_preferred_level=tas_preferred,
        accel_label=accel_label,
        suspend=False,
    )
    wf_list.append(
        {
            "timestamp": t0_str,
            "eventType": "New",
            "workloadState": "Admitted",
            "payload": {
                "objectKind": "Job",
                "name": name,
                "namespace": DEFAULT_NS,
                "creationTime": t0_str,
                "localQueueName": lq,
                "object": job_obj,
            },
        }
    )

    adm_pl: dict[str, Any] = {
        "workloadName": wl_name,
        "namespace": DEFAULT_NS,
        "ownerKind": "Job",
        "ownerName": name,
        "ownerAPIVersion": "batch/v1",
        "localQueueName": lq,
        "clusterQueueName": cq,
        "creationTime": t0_str,
        "admittedTime": t0_str,
        "preempted": False,
        "reason": "Admitted",
        "message": "The workload was already admitted at t0",
    }
    if prio:
        adm_pl["priorityClassName"] = prio
    if prio_val is not None:
        adm_pl["priority"] = int(prio_val)
    if with_tas_psa:
        adm_pl["podSetAssignments"] = [
            _build_tas_podset_assignment(
                ps_name="main",
                node_indices=node_indices,
                tpu_per_pod=tpu_per_pod,
                cpu_per_pod=cpu_per_pod,
                mem_gi_per_pod=mem_gi_per_pod,
                flavor_name=flavor_name,
                accel_label=accel_label,
            )
        ]

    ex_list.extend(
        [
            {
                "timestamp": t0_str,
                "eventType": "Admitted",
                "workloadState": "Admitted",
                "payload": adm_pl,
            },
            {
                "timestamp": t_end,
                "eventType": "FullExecution",
                "workloadState": "Admitted",
                "payload": {
                    "executionType": "Full",
                    "workloadName": wl_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "Job",
                    "ownerName": name,
                    "ownerAPIVersion": "batch/v1",
                    "localQueueName": lq,
                    "clusterQueueName": cq,
                    "creationTime": t0_str,
                    "admittedTime": t0_str,
                    "endTime": t_end,
                    "executionDurationSeconds": float(full_duration_sec),
                    "executionDuration": f"{int(round(full_duration_sec))}s",
                    "preempted": False,
                    "reason": "Succeeded",
                    "message": "job completed successfully",
                },
            },
        ]
    )

    reqs = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
        "google.com/tpu": str(tpu_per_pod),
    }
    for p_i in range(actual_bound):
        n_idx = node_indices[p_i]
        p_name = f"{name}-{p_i}"
        inf_list.append(
            {
                "timestamp": t0_str,
                "eventType": "New",
                "payload": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": p_name,
                    "namespace": DEFAULT_NS,
                    "placement": f"st-node-{n_idx:03d}",
                    "schedulingTime": t0_str,
                    "firstNoticeTime": t0_str,
                    "resourceRequest": dict(reqs),
                    "workloadName": wl_name,
                    "ownerKind": "Job",
                    "ownerName": name,
                    "podSetName": "main",
                    "isTAS": bool(with_tas_psa or tas_required or tas_preferred),
                },
            }
        )
        inf_list.append(
            {
                "timestamp": t_end,
                "eventType": "Deleted",
                "payload": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": p_name,
                    "namespace": DEFAULT_NS,
                    "deletionTime": t_end,
                },
            }
        )


def _add_t0_vanilla_podgroup(
    inf_list: list[dict[str, Any]],
    wf_list: list[dict[str, Any]],
    ex_list: list[dict[str, Any]],
    *,
    fmt_ts_fn: Any,
    det_uid_fn: Any,
    pg_name: str,
    remaining_sec: float,
    lq: str,
    cq: str,
    node_indices: list[int],
    tpu_per_pod: int = 2,
    cpu_per_pod: int = 2,
    mem_gi_per_pod: int = 8,
    tas_required: str | None = None,
) -> None:
    """Create a real vanilla-Kubernetes Kueue PodGroup (`N` member `Pod`s with `pod-group-name`) running at `t0`."""
    t0_str = fmt_ts_fn(0.0)
    t_end = fmt_ts_fn(remaining_sec)
    total = len(node_indices)
    reqs = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
        "google.com/tpu": str(tpu_per_pod),
    }
    anns: dict[str, str] = {
        "kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(round(remaining_sec))),
    }
    if tas_required:
        anns["kueue.x-k8s.io/podset-required-topology"] = tas_required

    for idx_m, n_idx in enumerate(node_indices):
        p_name = f"{pg_name}-m{idx_m}"
        p_obj = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": p_name,
                "namespace": DEFAULT_NS,
                "uid": det_uid_fn(f"pod:{DEFAULT_NS}/{p_name}"),
                "creationTimestamp": t0_str,
                "labels": {
                    "kueue.x-k8s.io/queue-name": lq,
                    "kueue.x-k8s.io/managed": "true",
                    "kueue.x-k8s.io/pod-group-name": pg_name,
                    "kueue.x-k8s.io/pod-group-total-count": str(total),
                },
                "annotations": dict(anns),
            },
            "spec": {
                "restartPolicy": "Never",
                "tolerations": [dict(KWOK_FAKE_TOLERATION)],
                "nodeSelector": {"cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL},
                "containers": [
                    {
                        "name": "worker",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": dict(reqs), "limits": dict(reqs)},
                    }
                ],
            },
        }
        wf_list.append(
            {
                "timestamp": t0_str,
                "eventType": "New",
                "workloadState": "Admitted",
                "payload": {
                    "objectKind": "Pod",
                    "name": p_name,
                    "namespace": DEFAULT_NS,
                    "creationTime": t0_str,
                    "localQueueName": lq,
                    "object": p_obj,
                },
            }
        )
        inf_list.append(
            {
                "timestamp": t0_str,
                "eventType": "New",
                "payload": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": p_name,
                    "namespace": DEFAULT_NS,
                    "placement": f"st-node-{n_idx:03d}",
                    "schedulingTime": t0_str,
                    "firstNoticeTime": t0_str,
                    "resourceRequest": dict(reqs),
                    "workloadName": pg_name,
                    "ownerKind": "PodGroup",
                    "ownerName": pg_name,
                    "podSetName": "main",
                    "isTAS": bool(tas_required),
                },
            }
        )
        inf_list.append(
            {
                "timestamp": t_end,
                "eventType": "Deleted",
                "payload": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": p_name,
                    "namespace": DEFAULT_NS,
                    "deletionTime": t_end,
                },
            }
        )

    adm_pl: dict[str, Any] = {
        "workloadName": pg_name,
        "namespace": DEFAULT_NS,
        "ownerKind": "PodGroup",
        "ownerName": pg_name,
        "ownerAPIVersion": "v1",
        "localQueueName": lq,
        "clusterQueueName": cq,
        "creationTime": t0_str,
        "admittedTime": t0_str,
        "preempted": False,
        "reason": "Admitted",
        "message": "PodGroup already admitted at t0",
    }
    if tas_required:
        adm_pl["podSetAssignments"] = [
            _build_tas_podset_assignment(
                ps_name="main",
                node_indices=node_indices,
                tpu_per_pod=tpu_per_pod,
                cpu_per_pod=cpu_per_pod,
                mem_gi_per_pod=mem_gi_per_pod,
            )
        ]
    ex_list.extend(
        [
            {"timestamp": t0_str, "eventType": "Admitted", "workloadState": "Admitted", "payload": adm_pl},
            {
                "timestamp": t_end,
                "eventType": "FullExecution",
                "workloadState": "Admitted",
                "payload": {
                    "executionType": "Full",
                    "workloadName": pg_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "PodGroup",
                    "ownerName": pg_name,
                    "ownerAPIVersion": "v1",
                    "localQueueName": lq,
                    "clusterQueueName": cq,
                    "creationTime": t0_str,
                    "admittedTime": t0_str,
                    "endTime": t_end,
                    "executionDurationSeconds": float(remaining_sec),
                    "executionDuration": f"{int(round(remaining_sec))}s",
                    "preempted": False,
                    "reason": "Succeeded",
                    "message": "PodGroup completed successfully",
                },
            },
        ]
    )


def _add_t0_infinite_deployment(
    inf_list: list[dict[str, Any]],
    wf_list: list[dict[str, Any]],
    ex_list: list[dict[str, Any]],
    *,
    fmt_ts_fn: Any,
    det_uid_fn: Any,
    name: str,
    lq: str,
    cq: str,
    node_idx: int,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 4,
    mem_gi_per_pod: int = 16,
    tas_required: str | None = None,
) -> None:
    t0_str = fmt_ts_fn(0.0)
    wl_hash = hashlib.sha256(f"{DEFAULT_NS}/deploy/{name}".encode("utf-8")).hexdigest()[:5]
    wl_name = f"pod-{name}-{wl_hash}"
    reqs = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
        "google.com/tpu": str(tpu_per_pod),
    }
    pod_anns: dict[str, str] = {}
    if tas_required:
        pod_anns["kueue.x-k8s.io/podset-required-topology"] = tas_required

    dep_obj = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": DEFAULT_NS,
            "uid": det_uid_fn(f"deploy:{DEFAULT_NS}/{name}"),
            "creationTimestamp": t0_str,
            "labels": {"kueue.x-k8s.io/queue-name": lq},
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {
                    "labels": {"app": name, "kueue.x-k8s.io/queue-name": lq},
                    "annotations": dict(pod_anns),
                },
                "spec": {
                    "tolerations": [dict(KWOK_FAKE_TOLERATION)],
                    "nodeSelector": {"cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL},
                    "containers": [
                        {
                            "name": "serving",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": dict(reqs), "limits": dict(reqs)},
                        }
                    ],
                },
            },
        },
    }
    wf_list.append(
        {
            "timestamp": t0_str,
            "eventType": "New",
            "workloadState": "Admitted",
            "payload": {
                "objectKind": "Deployment",
                "name": name,
                "namespace": DEFAULT_NS,
                "creationTime": t0_str,
                "localQueueName": lq,
                "object": dep_obj,
            },
        }
    )
    adm_pl: dict[str, Any] = {
        "workloadName": wl_name,
        "namespace": DEFAULT_NS,
        "ownerKind": "Deployment",
        "ownerName": name,
        "ownerAPIVersion": "apps/v1",
        "localQueueName": lq,
        "clusterQueueName": cq,
        "creationTime": t0_str,
        "admittedTime": t0_str,
        "preempted": False,
        "reason": "Admitted",
        "message": "Serving Deployment admitted at t0",
    }
    if tas_required:
        adm_pl["podSetAssignments"] = [
            _build_tas_podset_assignment(
                ps_name="main",
                node_indices=[node_idx],
                tpu_per_pod=tpu_per_pod,
                cpu_per_pod=cpu_per_pod,
                mem_gi_per_pod=mem_gi_per_pod,
            )
        ]
    ex_list.append({"timestamp": t0_str, "eventType": "Admitted", "workloadState": "Admitted", "payload": adm_pl})
    inf_list.append(
        {
            "timestamp": t0_str,
            "eventType": "New",
            "payload": {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": f"{name}-rs-0",
                "namespace": DEFAULT_NS,
                "placement": f"st-node-{node_idx:03d}",
                "schedulingTime": t0_str,
                "firstNoticeTime": t0_str,
                "resourceRequest": dict(reqs),
                "workloadName": wl_name,
                "ownerKind": "Deployment",
                "ownerName": name,
                "podSetName": "main",
                "isTAS": bool(tas_required),
            },
        }
    )


def _add_arriving_standalone_pod(
    wf_list: list[dict[str, Any]],
    ex_list: list[dict[str, Any]],
    *,
    fmt_ts_fn: Any,
    det_uid_fn: Any,
    name: str,
    arrival_sec: float,
    duration_sec: float,
    lq: str,
    cq: str,
    tpu_req: int = 4,
    tas_required: str | None = None,
) -> None:
    ts_create = fmt_ts_fn(arrival_sec)
    ts_end = fmt_ts_fn(arrival_sec + duration_sec)
    wl_hash = hashlib.sha256(f"{DEFAULT_NS}/pod/{name}".encode("utf-8")).hexdigest()[:5]
    wl_name = f"pod-{name}-{wl_hash}"
    reqs = {"cpu": "4", "memory": "16Gi", "google.com/tpu": str(tpu_req)}
    anns: dict[str, str] = {"kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(round(duration_sec)))}
    if tas_required:
        anns["kueue.x-k8s.io/podset-required-topology"] = tas_required
    p_obj = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": DEFAULT_NS,
            "uid": det_uid_fn(f"pod:{DEFAULT_NS}/{name}"),
            "creationTimestamp": ts_create,
            "labels": {"kueue.x-k8s.io/queue-name": lq, "kueue.x-k8s.io/managed": "true"},
            "annotations": anns,
        },
        "spec": {
            "restartPolicy": "Never",
            "tolerations": [dict(KWOK_FAKE_TOLERATION)],
            "nodeSelector": {"cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL},
            "containers": [
                {
                    "name": "worker",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": dict(reqs), "limits": dict(reqs)},
                }
            ],
        },
    }
    wf_list.append(
        {
            "timestamp": ts_create,
            "eventType": "New",
            "workloadState": "NotAdmitted",
            "payload": {
                "objectKind": "Pod",
                "name": name,
                "namespace": DEFAULT_NS,
                "creationTime": ts_create,
                "localQueueName": lq,
                "object": p_obj,
            },
        }
    )
    ex_list.extend(
        [
            {
                "timestamp": ts_create,
                "eventType": "Admitted",
                "workloadState": "Admitted",
                "payload": {
                    "workloadName": wl_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "Pod",
                    "ownerName": name,
                    "ownerAPIVersion": "v1",
                    "localQueueName": lq,
                    "clusterQueueName": cq,
                    "creationTime": ts_create,
                    "admittedTime": ts_create,
                    "preempted": False,
                    "reason": "Admitted",
                    "message": "Standalone Pod admitted",
                },
            },
            {
                "timestamp": ts_end,
                "eventType": "FullExecution",
                "workloadState": "Admitted",
                "payload": {
                    "executionType": "Full",
                    "workloadName": wl_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "Pod",
                    "ownerName": name,
                    "ownerAPIVersion": "v1",
                    "localQueueName": lq,
                    "clusterQueueName": cq,
                    "creationTime": ts_create,
                    "admittedTime": ts_create,
                    "endTime": ts_end,
                    "executionDurationSeconds": float(duration_sec),
                    "executionDuration": f"{int(round(duration_sec))}s",
                    "preempted": False,
                    "reason": "Succeeded",
                    "message": "Pod completed successfully",
                },
            },
        ]
    )


def build_scale_15_scenarios(
    *,
    scenario_spec_cls: Any,
    fmt_ts_fn: Any,
    det_uid_fn: Any,
    make_infra_event_fn: Any,
    make_namespace_fn: Any,
    make_topology_fn: Any,
    make_flavor_fn: Any,
    make_priority_class_fn: Any,
    make_cohort_fn: Any,
    make_cluster_queue_fn: Any,
    make_local_queue_fn: Any,
    make_jobset_and_exec_events_fn: Any,
    make_t0_admitted_jobset_events_fn: Any,
    make_initial_pod_events_fn: Any,
) -> list[Any]:
    """Build the 15 large-scale (>1,000 workloads, >100 nodes) self-test scenarios `st46`–`st60`."""
    scenarios: list[Any] = []

    def scale_infra(
        num_nodes: int,
        *,
        with_topology: bool,
        priority_classes: list[tuple[str, int]] | None = None,
    ) -> list[dict[str, Any]]:
        evs: list[dict[str, Any]] = []
        for idx in range(num_nodes):
            evs.append(
                make_infra_event_fn(
                    _make_scale_node(idx, det_uid_fn=det_uid_fn, with_topology=with_topology)
                )
            )
        evs.append(make_infra_event_fn(make_namespace_fn(DEFAULT_NS)))
        if with_topology:
            evs.append(make_infra_event_fn(make_topology_fn(DEFAULT_TOPOLOGY)))
        evs.append(
            make_infra_event_fn(
                make_flavor_fn(DEFAULT_FLAVOR, topology_name=DEFAULT_TOPOLOGY if with_topology else None)
            )
        )
        for pc_n, pc_v in priority_classes or []:
            evs.append(make_infra_event_fn(make_priority_class_fn(pc_n, pc_v)))
        return evs

    # =========================================================================
    # CATEGORY 10: Large-Scale Quota-Only Admission (No TAS — 5 scenarios = 33.3%)
    # =========================================================================

    # st46: 128 Nodes (1,024 TPUs), 1,056 Workloads — Quota-Only Full Saturation with 128 t0 Jobs
    inf, wf, ex = scale_infra(128, with_topology=False), [], []
    inf.append(make_infra_event_fn(make_cluster_queue_fn("cq-scale-full", tpu_quota=1024, cpu_quota=4096, mem_gi_quota=16384)))
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-scale-full", "cq-scale-full")))
    for i in range(128):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-qsat-{i:03d}", remaining_sec=12.0, full_duration_sec=12.0,
            lq="lq-scale-full", cq="cq-scale-full", node_indices=[i], tpu_per_pod=4,
        )
    wave_schedule_46 = [(1.0, 128), (3.0, 256), (6.0, 256), (9.0, 288)]
    j_idx = 0
    for arr_t, cnt in wave_schedule_46:
        for _ in range(cnt):
            _add_arriving_batch_job(
                wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                name=f"arr-qsat-{j_idx:04d}", arrival_sec=arr_t, duration_sec=9.0,
                lq="lq-scale-full", cq="cq-scale-full", tpu_per_pod=4,
            )
            j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st46",
            slug="scale-quota-saturation-128n-1056w",
            category="10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)",
            title="128-Node, 1,056-Workload Quota-Only Full Saturation with 128 In-Flight `t0` Jobs",
            feature_focus="Validates chamber throughput, `t0` quota accounting (`512/1,024` TPUs), and live `cluster-scraper` parity at >100 nodes and >1,000 workloads without TAS",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset",
            setup_summary="128 simulated KWOK nodes (`8` TPUs/node = `1,024` TPUs, no TAS), single `cq-scale-full` (`1,024` TPUs). At `t0`, `128` `Admitted` `batch/v1` `Job`s (`4` TPUs each = `512` TPUs, `12s` remaining) run across `st-node-000..127`. `928` `NotAdmitted` `Job`s (`4` TPUs, `9s`) arrive across 4 waves (`128` at `t=1s`, `256` at `t=3s`, `256` at `t=6s`, `288` at `t=9s`) for **`1,056` total workloads**.",
            expected_behavior="`128` `t0` `PodGroup`s hold `512/1,024` TPUs immediately at `t0`; the first `128` arrivals at `t=1s` fill the remaining `512` TPUs (`1,024/1,024 = 100%`), while the remaining `800` arrivals drain across waves with `0` evictions and `1,056/1,056` completed workloads.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 128 PodGroup(s) (128 pod(s))`, `Workflows Submitted: 928/928`.",
                "`Section 5.1`: `cq-scale-full` peaks at **`100.0%`** (`1,024 / 1,024` TPUs).",
                "`Section 5.3`: **`0`** evictions.",
                "`Section 5.4`: `Qualifying Workloads: 1056 (completed=1056 [1056 pods])` with high effective saturation (`~65%–92%`).",
            ],
            must_not_happen=[
                "`cq-scale-full` admitting >128 new `4`-TPU jobs at `t=1s` while `128` `t0` `PodGroup`s occupy `512` TPUs.",
                "Any dropped/failed pods or missing terminal `Pod` `Deleted` events when captured by `cluster-scraper`.",
            ],
            sec51_target="Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%",
            sec52_target="Staircase waves (AVG ~12s–26s)",
            sec53_target="0 evictions",
            sec54_target="~65%–92% (1,056 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st47: 128 Nodes (1,024 TPUs), 1,080 Workloads — Quota-Only Intra-CQ Preemption & Dynamic t0 Resubmit
    inf, wf, ex = scale_infra(128, with_topology=False, priority_classes=[("p-low", 100), ("p-high", 1000)]), [], []
    inf.append(
        make_infra_event_fn(
            make_cluster_queue_fn("cq-scale-preempt", tpu_quota=1024, cpu_quota=4096, mem_gi_quota=16384, within_cq="LowerPriority")
        )
    )
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-scale-preempt", "cq-scale-preempt")))
    for i in range(64):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-qlow-{i:03d}", remaining_sec=34.0, full_duration_sec=9.0,
            lq="lq-scale-preempt", cq="cq-scale-preempt", node_indices=[i],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32, prio="p-low", prio_val=100,
        )
    for i in range(128):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-qhigh-{i:03d}", remaining_sec=20.0, full_duration_sec=20.0,
            lq="lq-scale-preempt", cq="cq-scale-preempt", node_indices=[64 + (i // 2)],
            tpu_per_pod=4, prio="p-high", prio_val=1000,
        )
    for i in range(64):
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-qburst-hi-{i:03d}", arrival_sec=3.0, duration_sec=9.0,
            lq="lq-scale-preempt", cq="cq-scale-preempt",
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32, prio="p-high",
        )
    j_idx = 0
    for arr_t, cnt, pr in [(8.0, 280, "p-high"), (13.0, 280, "p-low"), (18.0, 264, "p-low")]:
        for _ in range(cnt):
            _add_arriving_batch_job(
                wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                name=f"arr-qmix-{j_idx:04d}", arrival_sec=arr_t, duration_sec=8.0,
                lq="lq-scale-preempt", cq="cq-scale-preempt", tpu_per_pod=4, prio=pr,
            )
            j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st47",
            slug="scale-quota-preemption-128n-1080w",
            category="10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)",
            title="128-Node, 1,080-Workload Quota-Only Intra-CQ Preemption & `t0` Resubmission",
            feature_focus="Stress-tests `withinClusterQueue: LowerPriority` evicting `64` `t0` `p-low` `PodGroup`s (`512` TPUs) at `t=3s` and dynamically resubmitting all `64` parent `Job`s alongside `888` arrivals",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset",
            setup_summary="128 nodes (`1,024` TPUs, no TAS), `cq-scale-preempt` (`withinClusterQueue: LowerPriority`). At `t0`, `64` `p-low (100)` `8`-TPU `Job`s (`512` TPUs, `34s` remaining) + `128` `p-high (1000)` `4`-TPU `Job`s (`512` TPUs, `20s` remaining) fill `100%` of quota (`192` `t0` workloads). At `t=3s`, `64` `p-high` `8`-TPU `Job`s arrive, followed by `824` `4`-TPU `p-high`/`p-low` `Job`s across `t=8s..18s` (**`1,080` configured workloads**).",
            expected_behavior="At `t=3s`, Kueue evicts `64` `t0` `p-low` `PodGroup`s (`InClusterQueue`), admits the `64` `p-high` `8`-TPU burst jobs immediately, dynamically resubmits the `64` preempted `t0` `Job`s (`[+64 preempted-t0 resubmit]`), and drains all `1,080` workloads to completion.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 192 PodGroup(s)`, `[+64 preempted-t0 resubmit]`.",
                "`Section 5.3`: **`>= 64`** evictions (`InClusterQueue` on `p-low`).",
                "`Section 5.4`: `Qualifying Workloads: 1080 (completed=1080)`, `Excluded (Preempted/Unadmitted): 64` (aborted `t0` `PodGroup` runs excluded as badput).",
            ],
            must_not_happen=[
                "Any `t0-qhigh-*` (`p-high`) workload evicted.",
                "`Workload` name collision when dynamically resubmitting `64` `t0-qlow-*` `Job`s.",
            ],
            sec51_target="Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%",
            sec52_target="p-high < p-low wait",
            sec53_target=">= 64 InClusterQueue evictions",
            sec54_target="~60%–90% (1,080 completed, 64 excluded)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st48: 144 Nodes (1,152 TPUs), 1,116 Workloads — Quota-Only Cohort Borrowing & Reclaim
    inf, wf, ex = scale_infra(144, with_topology=False), [], []
    inf.append(make_infra_event_fn(make_cohort_fn("cohort-scale-quota")))
    inf.append(
        make_infra_event_fn(
            make_cluster_queue_fn(
                "cq-scale-borrower", tpu_quota=576, cpu_quota=2304, mem_gi_quota=9216,
                cohort_name="cohort-scale-quota", reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(
        make_infra_event_fn(
            make_cluster_queue_fn(
                "cq-scale-lender", tpu_quota=576, cpu_quota=2304, mem_gi_quota=9216,
                cohort_name="cohort-scale-quota", reclaim_within_cohort="Any",
            )
        )
    )
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-scale-borrower", "cq-scale-borrower")))
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-scale-lender", "cq-scale-lender")))
    for i in range(144):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-qbor-{i:03d}", remaining_sec=28.0, full_duration_sec=9.0,
            lq="lq-scale-borrower", cq="cq-scale-borrower", node_indices=[i],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
        )
    for i in range(72):
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-qlend-rec-{i:03d}", arrival_sec=3.0, duration_sec=10.0,
            lq="lq-scale-lender", cq="cq-scale-lender",
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
        )
    j_idx = 0
    for arr_t, cnt_per_cq in [(10.0, 150), (15.0, 150), (20.0, 150)]:
        for cq_tag, lq_n, cq_n in [("bor", "lq-scale-borrower", "cq-scale-borrower"), ("lnd", "lq-scale-lender", "cq-scale-lender")]:
            for _ in range(cnt_per_cq):
                _add_arriving_batch_job(
                    wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                    name=f"arr-qcoh-{cq_tag}-{j_idx:04d}", arrival_sec=arr_t, duration_sec=8.0,
                    lq=lq_n, cq=cq_n, tpu_per_pod=4,
                )
                j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st48",
            slug="scale-quota-cohort-reclaim-144n-1116w",
            category="10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)",
            title="144-Node, 1,116-Workload Quota-Only Cohort Borrowing (`200%`) & Instant Reclamation",
            feature_focus="Verifies Cohort borrowing (`1,152/576 = 200%`) at `t0` across `144` nodes and exact `72`-workload (`576`-TPU) reclamation when `cq-scale-lender` arrives at `t=3s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --no-skip-initial-offset",
            setup_summary="144 nodes (`1,152` TPUs, no TAS), `cohort-scale-quota` (`cq-scale-borrower`: `576` TPUs, `cq-scale-lender`: `576` TPUs, `reclaimWithinCohort: Any`). At `t0`, `cq-scale-borrower` runs `144` `8`-TPU `Job`s (`1,152` TPUs = `200%` of nominal). At `t=3s`, `cq-scale-lender` submits `72` `8`-TPU `Job`s (`576` TPUs), followed by `900` `4`-TPU `Job`s (`450` per CQ) across `t=10s..20s` (**`1,116` workloads**).",
            expected_behavior="`cq-scale-borrower` starts at `200%` (`1,152/576`); at `t=3s` Kueue reclaims **exact `72`** `t0` `PodGroup`s so `cq-scale-borrower` and `cq-scale-lender` run side-by-side at `576` TPUs (`100%` each), dynamically resubmitting the `72` reclaimed `Job`s and completing all `1,116` workloads.",
            expected_metrics=[
                "`Section 5.1`: `cq-scale-borrower` hits **`200.0%`** before `t=3s`, then settles to `~100%` alongside `cq-scale-lender`.",
                "`Section 5.3`: **`72`** Cohort reclamation evictions (`72` unique).",
                "`Section 5.4`: `Qualifying Workloads: 1116 (completed=1116)`, `Excluded: 72`.",
            ],
            must_not_happen=[
                "More than `72` `t0` borrower `PodGroup`s reclaimed (`cq-scale-borrower`'s `576` nominal TPUs are untouchable).",
                "`cq-scale-lender` waiting `25s` until `t=28s` for `t0` borrowers to drain naturally.",
            ],
            sec51_target="cq-scale-borrower P90/P99=200%, Cohort ~75%–95%",
            sec52_target="t=3s lender burst waits < 5s",
            sec53_target="72 Cohort reclaims (72 unique)",
            sec54_target="~62%–90% (1,116 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st49: 160 Nodes (1,280 TPUs), 1,160 Workloads — Quota-Only 4-CQ Preemption-Based Fair Sharing
    inf, wf, ex = scale_infra(160, with_topology=False), [], []
    inf.append(make_infra_event_fn(make_cohort_fn("cohort-scale-fs", tpu_quota=1280, cpu_quota=5120, mem_gi_quota=20480)))
    for k in range(1, 5):
        cq_n = f"cq-fs-q{k}"
        lq_n = f"lq-fs-q{k}"
        inf.append(
            make_infra_event_fn(
                make_cluster_queue_fn(
                    cq_n, tpu_quota=0, cpu_quota=0, mem_gi_quota=0,
                    cohort_name="cohort-scale-fs", fair_sharing_weight="1", reclaim_within_cohort="Any",
                )
            )
        )
        inf.append(make_infra_event_fn(make_local_queue_fn(lq_n, cq_n)))
    for i in range(160):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-fshog-{i:03d}", remaining_sec=28.0, full_duration_sec=9.0,
            lq="lq-fs-q1", cq="cq-fs-q1", node_indices=[i],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
        )
    for k in (2, 3, 4):
        for i in range(40):
            _add_arriving_batch_job(
                wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                name=f"arr-fsburst-q{k}-{i:02d}", arrival_sec=3.0, duration_sec=10.0,
                lq=f"lq-fs-q{k}", cq=f"cq-fs-q{k}",
                tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
            )
    j_idx = 0
    for arr_t, cnt_per_cq in [(11.0, 80), (16.0, 70), (21.0, 70)]:
        for k in (1, 2, 3, 4):
            for _ in range(cnt_per_cq):
                _add_arriving_batch_job(
                    wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                    name=f"arr-fswave-q{k}-{j_idx:04d}", arrival_sec=arr_t, duration_sec=8.0,
                    lq=f"lq-fs-q{k}", cq=f"cq-fs-q{k}", tpu_per_pod=4,
                )
                j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st49",
            slug="scale-quota-fair-sharing-160n-1160w",
            category="10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)",
            title="160-Node, 1,160-Workload 4-CQ Preemption-Based Fair Sharing Against `t0` Monopolizer",
            feature_focus="Verifies `InCohortFairSharing` across `160` nodes (`1,280` TPUs) when `cq-fs-q1` holds `100%` of the cohort at `t0` (`160` jobs) and 3 equal-weight peer CQs arrive at `t=3s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset",
            setup_summary="160 nodes (`1,280` TPUs, no TAS), `cohort-scale-fs` (`1,280` TPUs) shared by `cq-fs-q1..q4` (`nominalQuota=0`, `fairSharing.weight=1`, `reclaimWithinCohort: Any`). At `t0`, `cq-fs-q1` monopolizes all `1,280` TPUs (`160` `8`-TPU `Job`s, `28s` remaining). At `t=3s`, `cq-fs-q2..q4` each submit `40` `8`-TPU `Job`s (`320` TPUs each), followed by `880` `4`-TPU `Job`s (`220` per CQ) across `t=11s..21s` (**`1,160` workloads**).",
            expected_behavior="At `t=3s`, Fair Sharing preempts `120` of `cq-fs-q1`'s `160` `t0` `PodGroup`s (`InCohortFairSharing`), equalizing all 4 CQs at `320` TPUs (`25%` each), dynamically resubmits the `120` preempted `Job`s, and completes all `1,160` workloads.",
            expected_metrics=[
                "`Section 5.1`: Cohort `cohort-scale-fs` maintains high saturation while `cq-fs-q1..q4` equalize around `320` TPUs each during contention.",
                "`Section 5.3`: **`~120`** `InCohortFairSharing` evictions against `cq-fs-q1`.",
                "`Section 5.4`: `Qualifying Workloads: 1160 (completed=1160)`.",
            ],
            must_not_happen=[
                "`0` evictions at `t=3s` while `cq-fs-q1` monopolizes `100%` of the `1,280`-TPU cohort.",
                "Evicting more than `120` `t0` `PodGroup`s below `cq-fs-q1`'s `320`-TPU (`25%`) fair share.",
            ],
            sec51_target="Cohort ~75%–95%, 4 CQs equalized ~320 TPUs",
            sec52_target="q2..q4 burst waits < 6s",
            sec53_target="~120 InCohortFairSharing evictions",
            sec54_target="~60%–88% (1,160 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st50: 128 Nodes (1,024 TPUs), 1,064 Kueue Workloads + 16 Infra Pods — Multi-API Vanilla K8s & Chamber Parity
    inf, wf, ex = scale_infra(128, with_topology=False), [], []
    inf.append(make_infra_event_fn(make_cluster_queue_fn("cq-scale-mixed", tpu_quota=1024, cpu_quota=4096, mem_gi_quota=16384)))
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-scale-mixed", "cq-scale-mixed")))
    # 16 unmanaged t0 infra-pods on nodes 0..15 (64 TPUs, 14s remaining)
    for i in range(16):
        inf.extend(
            make_initial_pod_events_fn(
                pod_name=f"sys-daemon-{i:02d}", node_name=f"st-node-{i:03d}", remaining_sec=14.0, tpu_req=4
            )
        )
    # 16 t0 JobSets on nodes 16..31
    for i in range(16):
        p_evs, wf_ev, ex_evs = make_t0_admitted_jobset_events_fn(
            name=f"t0-mjs-{i:02d}", remaining_sec=12.0, full_duration_sec=12.0,
            local_queue="lq-scale-mixed", cluster_queue="cq-scale-mixed",
            node_names=[f"st-node-{16 + i:03d}"], tpu_per_pod=4,
        )
        inf.extend(p_evs)
        wf.append(wf_ev)
        ex.extend(ex_evs)
    # 16 t0 batch/v1 Jobs on nodes 32..47
    for i in range(16):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-mjob-{i:02d}", remaining_sec=12.0, full_duration_sec=12.0,
            lq="lq-scale-mixed", cq="cq-scale-mixed", node_indices=[32 + i], tpu_per_pod=4,
        )
    # 16 t0 vanilla K8s PodGroups (2 pods x 2 TPUs = 4 TPUs each) on nodes 48..63
    for i in range(16):
        _add_t0_vanilla_podgroup(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            pg_name=f"t0-mpg-{i:02d}", remaining_sec=12.0,
            lq="lq-scale-mixed", cq="cq-scale-mixed", node_indices=[48 + i, 48 + i], tpu_per_pod=2,
        )
    # 16 t0 infinite Deployments (4 TPUs each = 64 TPUs) on nodes 64..79
    for i in range(16):
        _add_t0_infinite_deployment(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-mdep-{i:02d}", lq="lq-scale-mixed", cq="cq-scale-mixed", node_idx=64 + i, tpu_per_pod=4,
        )
    # 1,000 arriving workloads: 800 Jobs + 100 JobSets + 100 standalone Pods
    for i in range(800):
        arr_t = 1.0 + 4.0 * (i // 200)
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-mjob-{i:03d}", arrival_sec=arr_t, duration_sec=8.0,
            lq="lq-scale-mixed", cq="cq-scale-mixed", tpu_per_pod=4,
        )
    for i in range(100):
        wf_ev, ex_evs = make_jobset_and_exec_events_fn(
            name=f"arr-mjs-{i:03d}", arrival_sec=3.0 + 5.0 * (i // 50), duration_sec=8.0,
            local_queue="lq-scale-mixed", cluster_queue="cq-scale-mixed", tpu_per_pod=4,
        )
        wf.append(wf_ev)
        ex.extend(ex_evs)
    for i in range(100):
        _add_arriving_standalone_pod(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-mpod-{i:03d}", arrival_sec=6.0 + 5.0 * (i // 50), duration_sec=8.0,
            lq="lq-scale-mixed", cq="cq-scale-mixed", tpu_req=4,
        )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st50",
            slug="scale-quota-mixed-apis-infra-128n-1064w",
            category="10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)",
            title="128-Node, 1,064-Workload Multi-API Quota-Only (`JobSet` + `Job` + `PodGroup` + `Deployment` + `infra-pods`)",
            feature_focus="Validates vanilla K8s & chamber `t0` parity across 4 `t0` API kinds (`JobSet`, `Job`, native `PodGroup`, infinite `Deployment`) + `16` unmanaged `infra-pods` + `1,000` multi-API arrivals",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --no-skip-initial-offset --duration 85s",
            setup_summary="128 nodes (`1,024` TPUs, no TAS), `cq-scale-mixed` (`1,024` TPUs), `--duration 85s`. At `t0`: `16` unmanaged `infra-pods` (`64` TPUs, `14s`) + `64` Kueue `t0` workloads (`16` `JobSet`s, `16` `Job`s, `16` `2`-pod vanilla `PodGroup`s, and `16` infinite `Deployment`s = `256` TPUs, `80` Kueue `t0` pods). Arrivals: `800` `Job`s + `100` `JobSet`s + `100` standalone `Pod`s (**`1,064` Kueue workloads** + `16` `infra-pods`).",
            expected_behavior="Loader coalesces the `16` `2`-pod vanilla `PodGroup`s without phantom pod duplication (`64` `InitialWorkloadPodGroup`s = `80` Kueue `t0` pods + `16` `infra-pods`), drains all `1,048` finite Kueue workloads well before `85s`, and keeps the `16` infinite `t0` `Deployment` `PodGroup`s active at `85s` (`ACTIVE AT END` without `864,000s` `FULL-DUR SAT` inflation).",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 64 PodGroup(s) (80 pod(s))`, `Initial Infra Pods: 16`, `Workflows Submitted: 1000/1000`.",
                "`Section 5.4`: `Qualifying Workloads: 1064 (completed=1048, active_at_end=16)` and `FULL-DUR SAT <= 100%`.",
            ],
            must_not_happen=[
                "Doubling of the `16` `2`-pod `PodGroup`s into `16 + 32` candidates or phantom Tier 4 pods.",
                "`FULL-DUR SAT` exploding `> 1,000%` from the `16` infinite `t0` `Deployment`s.",
            ],
            sec51_target="Peak 100% (1,024/1,024 TPUs), steady floor 6.25% (64/1,024)",
            sec52_target="Multi-wave drain < 70s",
            sec53_target="0 evictions",
            sec54_target="1,048 completed + 16 active_at_end (FULL-DUR <= 100%)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 11: Large-Scale TAS Packing, Slices & Multi-Pool (5 scenarios = 33.3%)
    # =========================================================================

    # st51: 128 Nodes (32 Racks, 1,024 TPUs), 1,088 Workloads — TAS Pair/Host Packing
    inf, wf, ex = scale_infra(128, with_topology=True), [], []
    inf.append(make_infra_event_fn(make_cluster_queue_fn("cq-tas-pack", tpu_quota=1024, cpu_quota=4096, mem_gi_quota=16384)))
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-pack", "cq-tas-pack")))
    for i in range(64):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-taspack-{i:03d}", remaining_sec=12.0, full_duration_sec=12.0,
            lq="lq-tas-pack", cq="cq-tas-pack", node_indices=[2 * i, 2 * i + 1],
            tpu_per_pod=4, tas_required="cloud.google.com/gke-tpu-partition-2x2x2-id", with_tas_psa=True,
        )
    j_idx = 0
    for arr_t, cnt in [(1.0, 256), (4.0, 256), (8.0, 256), (12.0, 256)]:
        for _ in range(cnt):
            _add_arriving_batch_job(
                wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                name=f"arr-taspack-{j_idx:04d}", arrival_sec=arr_t, duration_sec=9.0,
                lq="lq-tas-pack", cq="cq-tas-pack", tpu_per_pod=4,
                tas_required="kubernetes.io/hostname",
            )
            j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st51",
            slug="scale-tas-rack-packing-128n-1088w",
            category="11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)",
            title="128-Node, 1,088-Workload 7-Level TAS Pair & Host Packing with `64` `t0` TAS Jobs",
            feature_focus="Validates 7-level TAS `topologyAssignment` seeding across `64` `2`-node `t0` TAS `PodGroup`s (`512` TPUs) and `1,024` arriving TAS jobs on `128` nodes",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset",
            setup_summary="128 nodes (`8` cubes / `32` racks, `8` TPUs/node = `1,024` TPUs) with 7-level `st-slice-topology`. At `t0`, `64` `Admitted` `2`-pod (`8`-TPU) TAS `Job`s (`podset-required-topology: 2x2x2-id`, `12s` remaining) occupy `4` TPUs on every node (`512/1,024` TPUs). `1,024` `1`-pod (`4`-TPU) TAS `Job`s (`hostname`, `9s`) arrive in 4 waves of `256` (`t=1s, 4s, 8s, 12s`) for **`1,088` total TAS workloads**.",
            expected_behavior="Kueue's TAS cache ingests all `64` `t0` `topologyAssignment`s (`4/8` TPUs used per node), packs the first `128` arrivals onto the remaining `4` TPUs of each node (`1,024/1,024` TPUs), and completes all `1,088` TAS workloads with `0` evictions.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 64 PodGroup(s) (128 pod(s))`, `Workflows Submitted: 1024/1024`.",
                "`Section 5.1`: `cq-tas-pack` peaks at **`100.0%`** (`1,024 / 1,024` TPUs).",
                "`Section 5.4`: `Qualifying Workloads: 1088 (completed=1088 [1152 pods])`.",
            ],
            must_not_happen=[
                "TAS double-booking any node above `8` TPUs (`pods[pend]` stuck due to missing `t0` `topologyAssignment`).",
                "`cluster-scraper` omitting `podSetAssignments[].topologyAssignment` in `execution-time.json`.",
            ],
            sec51_target="Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%",
            sec52_target="4-wave TAS drain",
            sec53_target="0 evictions",
            sec54_target="~65%–92% (1,088 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st52: 160 Nodes (40 Racks, 1,280 TPUs), 1,120 Workloads — Multi-Node TAS Rack & Pair Slices
    inf, wf, ex = scale_infra(160, with_topology=True), [], []
    inf.append(make_infra_event_fn(make_cluster_queue_fn("cq-tas-slices", tpu_quota=1280, cpu_quota=5120, mem_gi_quota=20480)))
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-slices", "cq-tas-slices")))
    for r in range(40):
        p_evs, wf_ev, ex_evs = make_t0_admitted_jobset_events_fn(
            name=f"t0-rack-js-{r:02d}", remaining_sec=12.0, full_duration_sec=12.0,
            local_queue="lq-tas-slices", cluster_queue="cq-tas-slices",
            node_names=[f"st-node-{4 * r + k:03d}" for k in range(4)],
            num_pods=4, tpu_per_pod=4, tas_required_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
        )
        inf.extend(p_evs)
        wf.append(wf_ev)
        ex.extend(ex_evs)
    for i in range(80):
        wf_ev, ex_evs = make_jobset_and_exec_events_fn(
            name=f"arr-pair-js-{i:03d}", arrival_sec=2.0 + 4.0 * (i // 40), duration_sec=9.0,
            local_queue="lq-tas-slices", cluster_queue="cq-tas-slices",
            num_pods=2, tpu_per_pod=4, tas_required_level="cloud.google.com/gke-tpu-partition-2x2x2-id",
        )
        wf.append(wf_ev)
        ex.extend(ex_evs)
    for i in range(1000):
        arr_t = 1.0 + 4.0 * (i // 250)
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-host-job-{i:04d}", arrival_sec=arr_t, duration_sec=8.0,
            lq="lq-tas-slices", cq="cq-tas-slices", tpu_per_pod=4,
            tas_required="kubernetes.io/hostname",
        )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st52",
            slug="scale-tas-mixed-slices-160n-1120w",
            category="11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)",
            title="160-Node, 1,120-Workload Mixed `4`-Node Rack, `2`-Node Pair & `1`-Node TAS Slices",
            feature_focus="Co-schedules `40` `4`-pod `t0` rack `JobSet`s (`160` pods), `80` `2`-pod pair `JobSet`s (`160` pods), and `1,000` `1`-pod TAS `Job`s across `40` racks (`160` nodes)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w --no-skip-initial-offset",
            setup_summary="160 nodes (`10` cubes / `40` racks, `8` TPUs/node = `1,280` TPUs) with TAS. At `t0`, `40` `4`-pod (`16`-TPU) `JobSet`s (`2x2x4-id`, `12s` remaining) span all `40` racks (`640` TPUs). Arrivals: `80` `2`-pod (`8`-TPU) TAS `JobSet`s (`2x2x2-id`, `9s`) + `1,000` `1`-pod (`4`-TPU) TAS `Job`s (`hostname`, `8s`) (**`1,120` workloads**, `1,320` pods).",
            expected_behavior="All three TAS slice granularities (`4`-node rack, `2`-node pair, `1`-node host) pack cleanly across `160` nodes without topology fragmentation deadlocks, completing all `1,120` workloads.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 40 PodGroup(s) (160 pod(s))`, `Workflows Submitted: 1080/1080`.",
                "`Section 5.4`: `Qualifying Workloads: 1120 (completed=1120 [1320 pods])`.",
            ],
            must_not_happen=[
                "Child `Job`s owned by the `120` `JobSet`s duplicated as standalone workflows in `workflows.json` or replay.",
            ],
            sec51_target="Peak 100% (1,280/1,280 TPUs), AVG ~68%–90%",
            sec52_target="Multi-slice TAS packing",
            sec53_target="0 evictions",
            sec54_target="~62%–90% (1,120 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st53: 128 Nodes (32 Racks, 1,024 TPUs), 1,056 Workloads — TAS Rack Fragmentation & BestEffortFIFO Backfill
    inf, wf, ex = scale_infra(128, with_topology=True), [], []
    inf.append(
        make_infra_event_fn(
            make_cluster_queue_fn("cq-tas-backfill", tpu_quota=1024, cpu_quota=4096, mem_gi_quota=16384, queueing_strategy="BestEffortFIFO")
        )
    )
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-backfill", "cq-tas-backfill")))
    # 1 full-node (8-TPU) blocker in every single one of the 32 4-node racks!
    for r in range(32):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-rack-pin-{r:02d}", remaining_sec=16.0, full_duration_sec=16.0,
            lq="lq-tas-backfill", cq="cq-tas-backfill", node_indices=[4 * r],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
            tas_required="kubernetes.io/hostname", with_tas_psa=True,
        )
    # 8 full-rack (4 pods x 8 TPUs = 32 TPUs) jobs arrive at t=1s -> blocked until racks clear!
    for i in range(8):
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-fullrack-{i:02d}", arrival_sec=1.0, duration_sec=9.0,
            lq="lq-tas-backfill", cq="cq-tas-backfill",
            num_pods=4, tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
            tas_required="cloud.google.com/gke-tpu-partition-2x2x4-id",
        )
    # 1,016 1-pod (4-TPU) host jobs backfill around the blocked full-rack jobs under BestEffortFIFO!
    for i in range(1016):
        arr_t = 2.0 + 4.0 * (i // 254)
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-bf-small-{i:04d}", arrival_sec=arr_t, duration_sec=8.0,
            lq="lq-tas-backfill", cq="cq-tas-backfill", tpu_per_pod=4,
            tas_required="kubernetes.io/hostname",
        )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st53",
            slug="scale-tas-backfill-best-effort-128n-1056w",
            category="11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)",
            title="128-Node, 1,056-Workload TAS Rack-Fragmentation Blocking & `BestEffortFIFO` Backfill",
            feature_focus="Pins 1 full node (`8` TPUs) in every one of the `32` racks at `t0` (`256/1,024` TPUs), proving `4`-node full-rack TAS jobs block on topology while `1,016` `4`-TPU jobs backfill `768` free TPUs",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st53-scale-tas-backfill-best-effort-128n-1056w --no-skip-initial-offset",
            setup_summary="128 nodes (`32` `4`-node racks, `8` TPUs/node = `1,024` TPUs), `BestEffortFIFO`. At `t0`, `32` `8`-TPU TAS jobs pin `1` node in each of the `32` racks (`16s` remaining -> `0` completely empty `32`-TPU racks, `768` TPUs free). At `t=1s`, `8` full-rack `32`-TPU (`4×8`) TAS jobs (`2x2x4-id`) arrive, followed by `1,016` `4`-TPU `hostname` TAS jobs across `t=2s..14s` (**`1,056` workloads**).",
            expected_behavior="`arr-fullrack-00..07` cannot fit any single `2x2x4-id` rack at `t=1s` despite `768` free TPUs (`32 × 8 = 256` needed), proving TAS enforces rack-level `t0` occupancy; meanwhile `BestEffortFIFO` backfills the `768` free TPUs with `arr-bf-small-*` jobs and drains all `1,056` workloads once racks clear.",
            expected_metrics=[
                "`Section 5.1`: `cq-tas-backfill` climbs above `90%` during `2s–16s` via `BestEffortFIFO` backfill instead of stalling at `25%` (`256/1,024`).",
                "`Section 5.4`: `Qualifying Workloads: 1056 (completed=1056 [1080 pods])`.",
            ],
            must_not_happen=[
                "`arr-fullrack-*` admitting at `t=1s` onto racks whose 4th node is already full at `t0`.",
                "Head-of-line blocking holding cluster utilization at `25%` under `BestEffortFIFO`.",
            ],
            sec51_target="Backfill > 90% during rack block, AVG ~68%–90%",
            sec52_target="Full-rack jobs wait >= 15s; small jobs backfill",
            sec53_target="0 evictions",
            sec54_target="~64%–90% (1,056 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st54: 144 Nodes (36 Racks, 1,152 TPUs), 1,104 Workloads — Partial Vanilla-K8s t0 TAS Binding + Preferred/Unconstrained
    inf, wf, ex = scale_infra(144, with_topology=True), [], []
    inf.append(make_infra_event_fn(make_cluster_queue_fn("cq-tas-modes", tpu_quota=1152, cpu_quota=4608, mem_gi_quota=18432)))
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-modes", "cq-tas-modes")))
    # 24 fully-bound 2-pod t0 TAS jobs + 24 partially-bound (1 of 2 pods bound in infra.json, 1 synthesized via Tier 4!)
    for i in range(48):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-taspart-{i:02d}", remaining_sec=12.0, full_duration_sec=12.0,
            lq="lq-tas-modes", cq="cq-tas-modes", node_indices=[2 * i, 2 * i + 1],
            bound_pod_count=2 if i < 24 else 1,
            tpu_per_pod=4, tas_required="cloud.google.com/gke-tpu-partition-2x2x2-id", with_tas_psa=True,
        )
    j_idx = 0
    for arr_t in (1.0, 5.0, 9.0, 13.0):
        for mode in ("req", "pref", "uncon"):
            for _ in range(88):
                _add_arriving_batch_job(
                    wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                    name=f"arr-tas-{mode}-{j_idx:04d}", arrival_sec=arr_t, duration_sec=8.0,
                    lq="lq-tas-modes", cq="cq-tas-modes", tpu_per_pod=4,
                    tas_required="cloud.google.com/gke-tpu-partition-2x2x4-id" if mode == "req" else None,
                    tas_preferred="cloud.google.com/gce-topology-block" if mode == "pref" else None,
                    tas_unconstrained=(mode == "uncon"),
                )
                j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st54",
            slug="scale-tas-preferred-and-partial-t0-144n-1104w",
            category="11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)",
            title="144-Node, 1,104-Workload Partial Vanilla-K8s `t0` TAS Binding + Required/Preferred/Unconstrained Modes",
            feature_focus="Tests vanilla K8s `t0` snapshots where `24` of `48` `2`-pod TAS jobs have only `1/2` pods bound in `infra.json` (Tier 4 synthesizes missing pods onto `topologyAssignment` domains) plus all 3 TAS constraint annotations",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st54-scale-tas-preferred-and-partial-t0-144n-1104w --no-skip-initial-offset",
            setup_summary="144 nodes (`36` racks, `1,152` TPUs) with TAS. At `t0`, `48` `Admitted` `2`-pod (`8`-TPU) TAS `Job`s (`384` TPUs): `24` have both pods bound in `infra.json` (`48` pods) and `24` have `1` bound pod + `podSetAssignments[].topologyAssignment` (`24` bound + `24` Tier-4-synthesized = `96` `t0` pods total!). Arrivals: `1,056` `4`-TPU TAS `Job`s evenly split (`352` each) across `podset-required-topology`, `podset-preferred-topology`, and `podset-unconstrained-topology` (**`1,104` workloads**).",
            expected_behavior="Loader reconstructs all `48` `t0` `PodGroup`s with exact **`96` `t0` pods** (`384` TPUs) placed on their exact `2x2x2-id` node pairs, and Kueue schedules all `1,056` required/preferred/unconstrained arrivals across `144` nodes with `0` failures.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 48 PodGroup(s) (96 pod(s))` (not `72` pods!), `Workflows Submitted: 1056/1056`.",
                "`Section 5.4`: `Qualifying Workloads: 1104 (completed=1104 [1152 pods])`.",
            ],
            must_not_happen=[
                "Under-counting `t0` quota at `288` TPUs (`72` pods) when `24` vanilla-K8s `t0` pods were still unscheduled at scrape time.",
                "Placing synthesized `t0` TAS pods outside their `topologyAssignment` node-pair domain.",
            ],
            sec51_target="Peak 100% (1,152/1,152 TPUs), AVG ~68%–90%",
            sec52_target="3 TAS modes co-scheduled cleanly",
            sec53_target="0 evictions",
            sec54_target="~62%–90% (1,104 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st55: 256 Nodes (64 Racks, 2,048 TPUs), 1,200 Workloads — Dual-ResourceFlavor TAS Scale Stress
    inf, wf, ex = [], [], []
    for idx in range(256):
        pool_lbl = "tpu-v7x-pool-a" if idx < 128 else "tpu-v7x-pool-b"
        inf.append(
            make_infra_event_fn(
                _make_scale_node(idx, det_uid_fn=det_uid_fn, with_topology=True, accel_label=pool_lbl)
            )
        )
    inf.append(make_infra_event_fn(make_namespace_fn(DEFAULT_NS)))
    inf.append(make_infra_event_fn(make_topology_fn(DEFAULT_TOPOLOGY)))
    for flv_n, pool_lbl in [("st-tpu-pool-a", "tpu-v7x-pool-a"), ("st-tpu-pool-b", "tpu-v7x-pool-b")]:
        inf.append(
            make_infra_event_fn(
                {
                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                    "kind": "ResourceFlavor",
                    "metadata": {"name": flv_n, "uid": det_uid_fn(f"rf:{flv_n}")},
                    "spec": {
                        "nodeLabels": {"cloud.google.com/gke-tpu-accelerator": pool_lbl},
                        "topologyName": DEFAULT_TOPOLOGY,
                    },
                }
            )
        )
    inf.append(
        make_infra_event_fn(
            {
                "apiVersion": "kueue.x-k8s.io/v1beta2",
                "kind": "ClusterQueue",
                "metadata": {"name": "cq-tas-dual-256n", "uid": det_uid_fn("cq:cq-tas-dual-256n")},
                "spec": {
                    "namespaceSelector": {},
                    "queueingStrategy": "BestEffortFIFO",
                    "preemption": {"withinClusterQueue": "Never", "reclaimWithinCohort": "Never"},
                    "resourceGroups": [
                        {
                            "coveredResources": ["cpu", "memory", "google.com/tpu"],
                            "flavors": [
                                {
                                    "name": flv_n,
                                    "resources": [
                                        {"name": "cpu", "nominalQuota": "4096"},
                                        {"name": "memory", "nominalQuota": "16384Gi"},
                                        {"name": "google.com/tpu", "nominalQuota": "1024"},
                                    ],
                                }
                                for flv_n in ("st-tpu-pool-a", "st-tpu-pool-b")
                            ],
                        }
                    ],
                },
            }
        )
    )
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-dual-256n", "cq-tas-dual-256n")))
    for i in range(128):
        n_idx = i * 2
        flv_n = "st-tpu-pool-a" if n_idx < 128 else "st-tpu-pool-b"
        pool_lbl = "tpu-v7x-pool-a" if n_idx < 128 else "tpu-v7x-pool-b"
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-dual-{i:03d}", remaining_sec=12.0, full_duration_sec=12.0,
            lq="lq-tas-dual-256n", cq="cq-tas-dual-256n", node_indices=[n_idx, n_idx + 1],
            tpu_per_pod=4, tas_required="cloud.google.com/gke-tpu-partition-2x2x2-id",
            with_tas_psa=True, flavor_name=flv_n, accel_label=pool_lbl,
        )
    j_idx = 0
    for arr_t, cnt in [(1.0, 384), (5.0, 384), (9.0, 304)]:
        for _ in range(cnt):
            _add_arriving_batch_job(
                wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                name=f"arr-dual-{j_idx:04d}", arrival_sec=arr_t, duration_sec=9.0,
                lq="lq-tas-dual-256n", cq="cq-tas-dual-256n", tpu_per_pod=4,
                tas_required="kubernetes.io/hostname", accel_label=None,
            )
            j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st55",
            slug="scale-tas-dual-flavor-256n-1200w",
            category="11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)",
            title="256-Node (`2,048`-TPU), 1,200-Workload Dual-`ResourceFlavor` TAS Fleet Stress Test",
            feature_focus="Largest node & workload scale test: `256` KWOK nodes (`64` racks, `2,048` TPUs) across 2 TAS `ResourceFlavor`s (`st-tpu-pool-a`, `st-tpu-pool-b`) with `128` `t0` TAS jobs (`256` pods) and `1,072` arrivals",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st55-scale-tas-dual-flavor-256n-1200w --no-skip-initial-offset",
            setup_summary="**256 nodes** (`16` cubes / `64` racks, `8` TPUs/node = **`2,048` TPUs**) split across `st-tpu-pool-a` (`128` nodes, `1,024` TPUs) and `st-tpu-pool-b` (`128` nodes, `1,024` TPUs) inside `cq-tas-dual-256n` (`2,048` TPUs). At `t0`, `128` `2`-pod (`8`-TPU) TAS `Job`s (`1,024` TPUs across both pools, `12s` remaining) run alongside `1,072` arriving `4`-TPU TAS `Job`s (`9s`) for **`1,200` total workloads** (`1,328` pods).",
            expected_behavior="Both `st-tpu-pool-a` and `st-tpu-pool-b` saturate up to `1,024` TPUs each (`512` concurrent pods across `256` nodes!), `cluster-scraper` captures all `256` nodes, both `ResourceFlavor`s, and all `1,200` workflows cleanly, and all `1,200` workloads finish.",
            expected_metrics=[
                "`Execution Summary`: `256` synced KWOK Nodes, `Initial Kueue Workloads: 128 PodGroup(s) (256 pod(s))`, `Workflows Submitted: 1072/1072`.",
                "`Section 5.1`: `cq-tas-dual-256n` peaks near **`100.0%`** (`2,048 / 2,048` TPUs).",
                "`Section 5.4`: `Qualifying Workloads: 1200 (completed=1200 [1328 pods])`.",
            ],
            must_not_happen=[
                "ConfigMap `1MiB` overflow when syncing `256` nodes (`lifecycle.py` dynamically omits `kwok-nodes.yaml` >50 nodes).",
                "Flavor mismatch assigning `st-tpu-pool-a` `t0` pods to `st-tpu-pool-b`.",
            ],
            sec51_target="Peak ~100% (2,048/2,048 TPUs), AVG ~60%–88%",
            sec52_target="Dual-flavor 512-pod concurrency",
            sec53_target="0 evictions",
            sec54_target="~58%–88% (1,200 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # =========================================================================
    # CATEGORY 12: Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (5 scenarios = 33.3%)
    # =========================================================================

    # st56: 128 Nodes (32 Racks, 1,024 TPUs), 1,060 Workloads — TAS Intra-CQ Preemption & Resubmit
    inf, wf, ex = scale_infra(128, with_topology=True, priority_classes=[("p-low", 100), ("p-high", 1000)]), [], []
    inf.append(
        make_infra_event_fn(
            make_cluster_queue_fn("cq-tas-preempt", tpu_quota=1024, cpu_quota=4096, mem_gi_quota=16384, within_cq="LowerPriority")
        )
    )
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-preempt", "cq-tas-preempt")))
    for i in range(64):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-taslow-{i:02d}", remaining_sec=34.0, full_duration_sec=9.0,
            lq="lq-tas-preempt", cq="cq-tas-preempt", node_indices=[i],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32, prio="p-low", prio_val=100,
            tas_required="kubernetes.io/hostname", with_tas_psa=True,
        )
    for i in range(64):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-tashi-{i:02d}", remaining_sec=20.0, full_duration_sec=20.0,
            lq="lq-tas-preempt", cq="cq-tas-preempt", node_indices=[64 + i],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32, prio="p-high", prio_val=1000,
            tas_required="kubernetes.io/hostname", with_tas_psa=True,
        )
    for i in range(32):
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-tasburst-hi-{i:02d}", arrival_sec=3.0, duration_sec=9.0,
            lq="lq-tas-preempt", cq="cq-tas-preempt",
            num_pods=2, tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32, prio="p-high",
            tas_required="cloud.google.com/gke-tpu-partition-2x2x2-id",
        )
    j_idx = 0
    for arr_t, cnt, pr in [(8.0, 300, "p-high"), (13.0, 300, "p-low"), (18.0, 300, "p-low")]:
        for _ in range(cnt):
            _add_arriving_batch_job(
                wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                name=f"arr-tasmix-{j_idx:04d}", arrival_sec=arr_t, duration_sec=8.0,
                lq="lq-tas-preempt", cq="cq-tas-preempt", tpu_per_pod=4, prio=pr,
                tas_required="kubernetes.io/hostname",
            )
            j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st56",
            slug="scale-tas-preempt-within-cq-128n-1060w",
            category="12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)",
            title="128-Node, 1,060-Workload TAS Intra-CQ Preemption (`2x2x2-id` Evicting `64` `t0` TAS Jobs)",
            feature_focus="Verifies TAS topology-aware preemption evicts `64` `t0` `p-low` TAS `PodGroup`s (`32` node-pairs = `512` TPUs) for `32` arriving `2`-node `p-high` TAS jobs and dynamically resubmits all `64` victims",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st56-scale-tas-preempt-within-cq-128n-1060w --no-skip-initial-offset",
            setup_summary="128 nodes (`32` racks, `1,024` TPUs) with TAS, `withinClusterQueue: LowerPriority`. At `t0`, `64` `p-low` `8`-TPU TAS jobs (`st-node-000..063`, `34s` remaining) + `64` `p-high` `8`-TPU TAS jobs (`st-node-064..127`, `20s` remaining) occupy `100%` of the cluster (`128` `t0` workloads). At `t=3s`, `32` `2`-node (`16`-TPU) `p-high` TAS jobs (`2x2x2-id`) arrive, followed by `900` `4`-TPU TAS jobs across `t=8s..18s` (**`1,060` workloads**).",
            expected_behavior="At `t=3s`, Kueue's TAS preemptor evicts `64` `t0` `p-low` TAS `PodGroup`s to free `32` contiguous `2x2x2-id` node pairs, admits all `32` `p-high` `2`-node TAS jobs immediately, dynamically resubmits the `64` `p-low` TAS `Job`s (`[+64 preempted-t0 resubmit]`), and completes all `1,060` workloads.",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 128 PodGroup(s)`, `[+64 preempted-t0 resubmit]`.",
                "`Section 5.3`: **`>= 64`** `InClusterQueue` TAS evictions.",
                "`Section 5.4`: `Qualifying Workloads: 1060 (completed=1060)`, `Excluded: 64`.",
            ],
            must_not_happen=[
                "TAS preemption failing to free contiguous `2x2x2-id` node pairs occupied by `t0` `PodGroup`s.",
            ],
            sec51_target="Peak 100% (1,024/1,024 TPUs), AVG ~68%–90%",
            sec52_target="p-high 2-node TAS burst waits < 5s",
            sec53_target=">= 64 InClusterQueue TAS evictions",
            sec54_target="~60%–88% (1,060 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st57: 128 Nodes (32 Racks, 1,024 TPUs), 1,088 Workloads — TAS Cohort Borrowing & Reclaim
    inf, wf, ex = scale_infra(128, with_topology=True), [], []
    inf.append(make_infra_event_fn(make_cohort_fn("cohort-tas-reclaim")))
    for cq_n, lq_n in [("cq-tas-borrower", "lq-tas-borrower"), ("cq-tas-lender", "lq-tas-lender")]:
        inf.append(
            make_infra_event_fn(
                make_cluster_queue_fn(
                    cq_n, tpu_quota=512, cpu_quota=2048, mem_gi_quota=8192,
                    cohort_name="cohort-tas-reclaim", reclaim_within_cohort="Any",
                )
            )
        )
        inf.append(make_infra_event_fn(make_local_queue_fn(lq_n, cq_n)))
    for i in range(128):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-tasbor-{i:03d}", remaining_sec=28.0, full_duration_sec=9.0,
            lq="lq-tas-borrower", cq="cq-tas-borrower", node_indices=[i],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
            tas_required="kubernetes.io/hostname", with_tas_psa=True,
        )
    for i in range(64):
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-taslnd-rec-{i:02d}", arrival_sec=3.0, duration_sec=10.0,
            lq="lq-tas-lender", cq="cq-tas-lender",
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
            tas_required="kubernetes.io/hostname",
        )
    j_idx = 0
    for arr_t, cnt_per_cq in [(10.0, 150), (15.0, 150), (20.0, 148)]:
        for tag, lq_n, cq_n in [("bor", "lq-tas-borrower", "cq-tas-borrower"), ("lnd", "lq-tas-lender", "cq-tas-lender")]:
            for _ in range(cnt_per_cq):
                _add_arriving_batch_job(
                    wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                    name=f"arr-tascoh-{tag}-{j_idx:04d}", arrival_sec=arr_t, duration_sec=8.0,
                    lq=lq_n, cq=cq_n, tpu_per_pod=4, tas_required="kubernetes.io/hostname",
                )
                j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st57",
            slug="scale-tas-cohort-borrow-reclaim-128n-1088w",
            category="12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)",
            title="128-Node, 1,088-Workload TAS Cohort Borrowing (`200%`) & Reclamation (`reclaimWithinCohort: Any`)",
            feature_focus="TAS counterpart to `st48`: verifies `cq-tas-lender` reclaims **exact `64`** `t0` TAS `PodGroup`s (`512` TPUs) from `200%`-borrowing `cq-tas-borrower` at `t=3s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset",
            setup_summary="128 nodes (`32` racks, `1,024` TPUs) with TAS, `cohort-tas-reclaim` (`512` + `512` TPUs, `reclaimWithinCohort: Any`). At `t0`, `cq-tas-borrower` runs `128` `8`-TPU TAS jobs (`1,024/512 = 200%`, `28s` remaining). At `t=3s`, `cq-tas-lender` submits `64` `8`-TPU TAS jobs (`512` TPUs), followed by `896` `4`-TPU TAS jobs (`448` per CQ) across `t=10s..20s` (**`1,088` workloads**).",
            expected_behavior="At `t=3s`, Kueue reclaims **exact `64`** `t0` TAS `PodGroup`s (`512` TPUs), admits `cq-tas-lender`'s `64` TAS jobs onto the freed nodes, dynamically resubmits the `64` reclaimed TAS jobs (`[+64 preempted-t0 resubmit]`), and completes all `1,088` workloads.",
            expected_metrics=[
                "`Section 5.1`: `cq-tas-borrower` hits **`200.0%`** during `0s–3s`, then `100.0%` alongside `cq-tas-lender = 100.0%`.",
                "`Section 5.3`: **`64`** Cohort TAS reclaims (`64` unique).",
                "`Section 5.4`: `Qualifying Workloads: 1088 (completed=1088)`, `Excluded: 64`.",
            ],
            must_not_happen=[
                "More than `64` `t0` TAS borrower `PodGroup`s reclaimed.",
            ],
            sec51_target="cq-tas-borrower P90/P99=200%, Cohort ~75%–95%",
            sec52_target="t=3s lender TAS burst waits < 5s",
            sec53_target="64 TAS Cohort reclaims (64 unique)",
            sec54_target="~62%–90% (1,088 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st58: 160 Nodes (40 Racks, 1,280 TPUs), 1,140 Workloads — 4-CQ TAS Fair Sharing
    inf, wf, ex = scale_infra(160, with_topology=True), [], []
    inf.append(make_infra_event_fn(make_cohort_fn("cohort-tas-fs", tpu_quota=1280, cpu_quota=5120, mem_gi_quota=20480)))
    for k in range(1, 5):
        cq_n = f"cq-tas-fs{k}"
        lq_n = f"lq-tas-fs{k}"
        inf.append(
            make_infra_event_fn(
                make_cluster_queue_fn(
                    cq_n, tpu_quota=0, cpu_quota=0, mem_gi_quota=0,
                    cohort_name="cohort-tas-fs", fair_sharing_weight="1", reclaim_within_cohort="Any",
                )
            )
        )
        inf.append(make_infra_event_fn(make_local_queue_fn(lq_n, cq_n)))
    for i in range(160):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-tasfshog-{i:03d}", remaining_sec=28.0, full_duration_sec=9.0,
            lq="lq-tas-fs1", cq="cq-tas-fs1", node_indices=[i],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
            tas_required="kubernetes.io/hostname", with_tas_psa=True,
        )
    for k in (2, 3, 4):
        for i in range(40):
            _add_arriving_batch_job(
                wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                name=f"arr-tasfs-b{k}-{i:02d}", arrival_sec=3.0, duration_sec=10.0,
                lq=f"lq-tas-fs{k}", cq=f"cq-tas-fs{k}",
                tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
                tas_required="kubernetes.io/hostname",
            )
    j_idx = 0
    for arr_t, cnt_per_cq in [(11.0, 75), (16.0, 70), (21.0, 70)]:
        for k in (1, 2, 3, 4):
            for _ in range(cnt_per_cq):
                _add_arriving_batch_job(
                    wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
                    name=f"arr-tasfsw-q{k}-{j_idx:04d}", arrival_sec=arr_t, duration_sec=8.0,
                    lq=f"lq-tas-fs{k}", cq=f"cq-tas-fs{k}", tpu_per_pod=4,
                    tas_required="kubernetes.io/hostname",
                )
                j_idx += 1
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st58",
            slug="scale-tas-fair-sharing-160n-1140w",
            category="12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)",
            title="160-Node, 1,140-Workload 4-CQ TAS Preemption-Based Fair Sharing",
            feature_focus="TAS counterpart to `st49`: verifies `InCohortFairSharing` + TAS domain eviction & reassignment across `160` nodes (`40` racks, `1,280` TPUs) and `1,140` TAS workloads",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st58-scale-tas-fair-sharing-160n-1140w --no-skip-initial-offset",
            setup_summary="160 nodes (`40` racks, `1,280` TPUs) with TAS, `cohort-tas-fs` (`1,280` TPUs) shared by `cq-tas-fs1..4` (`weight=1`, `reclaimWithinCohort: Any`). At `t0`, `cq-tas-fs1` occupies `100%` of the cohort (`160` `8`-TPU TAS jobs, `28s` remaining). At `t=3s`, `cq-tas-fs2..4` each submit `40` `8`-TPU TAS jobs (`320` TPUs each), followed by `860` `4`-TPU TAS jobs across `t=11s..21s` (**`1,140` workloads**).",
            expected_behavior="Fair Sharing preempts `120` of `cq-tas-fs1`'s `160` `t0` TAS `PodGroup`s at `t=3s`, frees their TAS node domains for `cq-tas-fs2..4` (`320` TPUs each), dynamically resubmits the `120` victims, and drains all `1,140` TAS workloads.",
            expected_metrics=[
                "`Section 5.3`: **`~120`** `InCohortFairSharing` TAS evictions.",
                "`Section 5.4`: `Qualifying Workloads: 1140 (completed=1140)`.",
            ],
            must_not_happen=[
                "TAS topology cache retaining stale `t0` victim domain reservations after Fair-Sharing eviction.",
            ],
            sec51_target="Cohort ~75%–95%, 4 TAS CQs equalized ~320 TPUs",
            sec52_target="fs2..fs4 burst waits < 6s",
            sec53_target="~120 InCohortFairSharing TAS evictions",
            sec54_target="~60%–88% (1,140 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st59: 128 Nodes (32 Racks, 1,024 TPUs), 1,048 Workloads — TAS borrowingLimit & lendingLimit Ceiling (150%)
    inf, wf, ex = scale_infra(128, with_topology=True), [], []
    inf.append(make_infra_event_fn(make_cohort_fn("cohort-tas-limit")))
    inf.append(
        make_infra_event_fn(
            make_cluster_queue_fn(
                "cq-tas-capped", tpu_quota=512, cpu_quota=3072, mem_gi_quota=12288,
                cohort_name="cohort-tas-limit", tpu_borrowing_limit=256,
            )
        )
    )
    inf.append(
        make_infra_event_fn(
            make_cluster_queue_fn(
                "cq-tas-donor", tpu_quota=512, cpu_quota=3072, mem_gi_quota=12288,
                cohort_name="cohort-tas-limit", tpu_lending_limit=256,
            )
        )
    )
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-capped", "cq-tas-capped")))
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-donor", "cq-tas-donor")))
    for i in range(64):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-tascap-{i:02d}", remaining_sec=14.0, full_duration_sec=14.0,
            lq="lq-tas-capped", cq="cq-tas-capped", node_indices=[i],
            tpu_per_pod=8, cpu_per_pod=8, mem_gi_per_pod=32,
            tas_required="kubernetes.io/hostname", with_tas_psa=True,
        )
    for i in range(984):
        arr_t = 1.0 + 4.0 * (i // 197)
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-tascap-{i:04d}", arrival_sec=arr_t, duration_sec=8.0,
            lq="lq-tas-capped", cq="cq-tas-capped", tpu_per_pod=4,
            tas_required="kubernetes.io/hostname",
        )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st59",
            slug="scale-tas-borrowing-limit-128n-1048w",
            category="12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)",
            title="128-Node, 1,048-Workload TAS `borrowingLimit: 256` & `lendingLimit: 256` (`150.0%` Ceiling)",
            feature_focus="Verifies `cq-tas-capped` (`512` nominal + `256` `borrowingLimit`) never exceeds **`768` TPUs (`150.0%`)** across `128` TAS nodes even with `64` `t0` TAS jobs (`512` TPUs) + `984` queued TAS arrivals",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st59-scale-tas-borrowing-limit-128n-1048w --no-skip-initial-offset",
            setup_summary="128 nodes (`32` racks, `1,024` TPUs) with TAS, `cohort-tas-limit`: `cq-tas-capped` (`nominalQuota=512, borrowingLimit=256`) + `cq-tas-donor` (`nominalQuota=512, lendingLimit=256`, idle). At `t0`, `cq-tas-capped` runs `64` `8`-TPU TAS jobs (`512` TPUs = `100%` nominal, `14s` remaining). `984` `4`-TPU TAS jobs (`8s`) arrive on `cq-tas-capped` across `t=1s..17s` (**`1,048` workloads**).",
            expected_behavior="`cq-tas-capped` borrows **at most `256` TPUs** on top of its `512`-TPU `t0` load (`768 / 512 = 150.0%`, leaving `256` TPUs = `32` full nodes idle throughout the run!) and drains all `1,048` workloads cleanly.",
            expected_metrics=[
                "`Section 5.1 (cq-tas-capped)`: `P90 / P99 = 150.0%` (`768 / 512` TPUs) and NEVER `> 150.0%`.",
                "`Section 5.4`: Cluster `EFFECTIVE SAT <= 75.0%` (`768 / 1,024` physical TPUs ceiling), `1,048` completed.",
            ],
            must_not_happen=[
                "`cq-tas-capped` exceeding `150.0%` (`768` TPUs) at `t=1s` (which would occur if `t0` TAS `PodGroup`s were missing from CQ quota accounting).",
            ],
            sec51_target="cq-tas-capped P90/P99 = 150.0% (768/512 TPUs)",
            sec52_target="Capped at 768 TPUs (32 nodes idle)",
            sec53_target="0 evictions",
            sec54_target="~50%–75% (<= 75% physical cap, 1,048 completed)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    # st60: 144 Nodes (36 Racks, 1,152 TPUs), 1,080 Kueue Workloads + 16 Infra Pods — Hybrid Multi-API TAS & Scraper Stress
    inf, wf, ex = scale_infra(144, with_topology=True), [], []
    inf.append(make_infra_event_fn(make_cluster_queue_fn("cq-tas-hybrid", tpu_quota=1152, cpu_quota=4608, mem_gi_quota=18432)))
    inf.append(make_infra_event_fn(make_local_queue_fn("lq-tas-hybrid", "cq-tas-hybrid")))
    for i in range(16):
        inf.extend(
            make_initial_pod_events_fn(
                pod_name=f"tas-sys-daemon-{i:02d}", node_name=f"st-node-{i:03d}", remaining_sec=14.0, tpu_req=4
            )
        )
    for i in range(16):
        p_evs, wf_ev, ex_evs = make_t0_admitted_jobset_events_fn(
            name=f"t0-tas-mjs-{i:02d}", remaining_sec=12.0, full_duration_sec=12.0,
            local_queue="lq-tas-hybrid", cluster_queue="cq-tas-hybrid",
            node_names=[f"st-node-{16 + i:03d}"], tpu_per_pod=4,
            tas_required_level="kubernetes.io/hostname",
        )
        inf.extend(p_evs)
        wf.append(wf_ev)
        ex.extend(ex_evs)
    for i in range(16):
        _add_t0_batch_job(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-tas-mjob-{i:02d}", remaining_sec=12.0, full_duration_sec=12.0,
            lq="lq-tas-hybrid", cq="cq-tas-hybrid", node_indices=[32 + i],
            tpu_per_pod=4, tas_required="kubernetes.io/hostname", with_tas_psa=True,
        )
    for i in range(16):
        _add_t0_vanilla_podgroup(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            pg_name=f"t0-tas-mpg-{i:02d}", remaining_sec=12.0,
            lq="lq-tas-hybrid", cq="cq-tas-hybrid", node_indices=[48 + i, 48 + i],
            tpu_per_pod=2, tas_required="kubernetes.io/hostname",
        )
    for i in range(16):
        _add_t0_infinite_deployment(
            inf, wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"t0-tas-mdep-{i:02d}", lq="lq-tas-hybrid", cq="cq-tas-hybrid",
            node_idx=64 + i, tpu_per_pod=4, tas_required="kubernetes.io/hostname",
        )
    for i in range(816):
        arr_t = 1.0 + 4.0 * (i // 204)
        _add_arriving_batch_job(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-tas-mjob-{i:03d}", arrival_sec=arr_t, duration_sec=8.0,
            lq="lq-tas-hybrid", cq="cq-tas-hybrid", tpu_per_pod=4,
            tas_required="kubernetes.io/hostname",
        )
    for i in range(100):
        wf_ev, ex_evs = make_jobset_and_exec_events_fn(
            name=f"arr-tas-mjs-{i:03d}", arrival_sec=3.0 + 5.0 * (i // 50), duration_sec=8.0,
            local_queue="lq-tas-hybrid", cluster_queue="cq-tas-hybrid", tpu_per_pod=4,
            tas_required_level="kubernetes.io/hostname",
        )
        wf.append(wf_ev)
        ex.extend(ex_evs)
    for i in range(100):
        _add_arriving_standalone_pod(
            wf, ex, fmt_ts_fn=fmt_ts_fn, det_uid_fn=det_uid_fn,
            name=f"arr-tas-mpod-{i:03d}", arrival_sec=6.0 + 5.0 * (i // 50), duration_sec=8.0,
            lq="lq-tas-hybrid", cq="cq-tas-hybrid", tpu_req=4,
            tas_required="kubernetes.io/hostname",
        )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st60",
            slug="scale-tas-hybrid-infra-infinite-144n-1080w",
            category="12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)",
            title="144-Node, 1,080-Workload Hybrid Multi-API TAS + `infra-pods` + Infinite `Deployment` Scraper Stress",
            feature_focus="End-to-end TAS + `cluster-scraper` (`--hash-names` & plain) capstone combining `144` TAS nodes, `16` `infra-pods`, `64` multi-API `t0` TAS workloads (`JobSet`, `Job`, `PodGroup`, infinite `Deployment`), and `1,016` multi-API TAS arrivals",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st60-scale-tas-hybrid-infra-infinite-144n-1080w --no-skip-initial-offset --duration 85s",
            setup_summary="144 nodes (`36` racks, `1,152` TPUs) with TAS, `cq-tas-hybrid` (`1,152` TPUs), `--duration 85s`. At `t0`: `16` unmanaged `infra-pods` (`64` TPUs, `14s`) + `64` TAS `t0` workloads (`16` `JobSet`s, `16` `Job`s, `16` `2`-pod vanilla `PodGroup`s, `16` infinite `Deployment`s = `256` TPUs, `80` `t0` Kueue pods). Arrivals: `816` TAS `Job`s + `100` TAS `JobSet`s + `100` standalone TAS `Pod`s (**`1,080` Kueue workloads** + `16` `infra-pods`).",
            expected_behavior="Separates `16` `infra-pods` (`infra-pods` namespace) from `64` TAS `InitialWorkloadPodGroup`s (`80` Kueue `t0` pods with TAS `topologyAssignment`), completes all `1,064` finite TAS workloads within `85s`, retains `16` infinite `t0` TAS `PodGroup`s as `ACTIVE AT END`, and round-trips cleanly through `cluster-scraper` (`--hash-names` & plain).",
            expected_metrics=[
                "`Execution Summary`: `Initial Kueue Workloads: 64 PodGroup(s) (80 pod(s))`, `Initial Infra Pods: 16`, `Workflows Submitted: 1016/1016`.",
                "`Section 5.4`: `Qualifying Workloads: 1080 (completed=1064, active_at_end=16)` and `FULL-DUR SAT <= 100%`.",
            ],
            must_not_happen=[
                "Any `infra-pod` assigned a Kueue `PodGroup` or any `PodGroup` doubled on `cluster-scraper` round-trip.",
                "`FULL-DUR SAT` exceeding `100%` due to the `16` infinite `t0` TAS `Deployment`s.",
            ],
            sec51_target="Peak ~100% (1,152/1,152 TPUs), floor 5.56% (64/1,152)",
            sec52_target="Multi-API TAS drain < 70s",
            sec53_target="0 evictions",
            sec54_target="1,064 completed + 16 active_at_end (FULL-DUR <= 100%)",
            infra_events=inf,
            workflow_events=wf,
            exec_events=ex,
        )
    )

    assert len(scenarios) == 15, f"Expected 15 scale scenarios, got {len(scenarios)}"
    return scenarios


SCALE_SCENARIO_PAIRINGS: dict[str, list[dict[str, str]]] = {
    "st46": [
        {
            "peer_id": "st51",
            "relationship": "128-Node Quota-Only vs. 7-Level TAS Packing Scale Pair",
            "knob_changed": "Quota-only `ResourceFlavor` without `Topology` (`st46`, `1,056` workloads) vs. 7-level `st-slice-topology` TAS packing (`st51`, `1,088` workloads) on `128` nodes (`1,024` TPUs).",
            "cross_check": "Both hit `100.0%` peak quota utilization (`1,024/1,024` TPUs) with `0` evictions and `100%` completion; `cluster-scraper` records `Topology` + `podSetAssignments[].topologyAssignment` in `st51` and omits them in `st46`.",
        },
        {
            "peer_id": "st47",
            "relationship": "128-Node Non-Preemptive Saturation vs. Intra-CQ `LowerPriority` Preemption Pair",
            "knob_changed": "Single priority `withinClusterQueue: Never` (`st46`) vs. `p-high` burst preempting `64` `t0` `p-low` `PodGroup`s (`withinClusterQueue: LowerPriority`, `st47`).",
            "cross_check": "`Sec 5.3` shifts from `0` (`st46`) to `>= 64` `InClusterQueue` evictions + `[+64 preempted-t0 resubmit]` (`st47`).",
        },
    ],
    "st47": [
        {
            "peer_id": "st46",
            "relationship": "128-Node Intra-CQ Preemption vs. Non-Preemptive Baseline Pair",
            "knob_changed": "`withinClusterQueue: LowerPriority` with `64` `t0` `p-low` victims (`st47`) vs. `withinClusterQueue: Never` (`st46`).",
            "cross_check": "`st47` records `>= 64` `InClusterQueue` evictions and excludes the `64` aborted `t0` `PodGroup` runs from `Sec 5.4`, whereas `st46` has `0` evictions and `0` excluded.",
        },
        {
            "peer_id": "st56",
            "relationship": "Quota-Only vs. TAS 128-Node Intra-CQ Preemption Pair",
            "knob_changed": "Quota-only preemption (`st47`) vs. 7-level TAS `2x2x2-id` node-pair preemption (`st56`).",
            "cross_check": "Both evict `>= 64` `t0` `p-low` `PodGroup`s (`512` TPUs) and dynamically resubmit all `64` parent `Job`s to full completion.",
        },
    ],
    "st48": [
        {
            "peer_id": "st57",
            "relationship": "Quota-Only (`144` Nodes) vs. TAS (`128` Nodes) Cohort Borrowing & Reclamation Pair",
            "knob_changed": "Quota-only Cohort borrowing (`1,152/576 = 200%`, `72` reclaims in `st48`) vs. TAS Cohort borrowing (`1,024/512 = 200%`, `64` reclaims in `st57`).",
            "cross_check": "Both hit `200.0%` borrower `Sec 5.1` at `t0` and reclaim exact `50%` of cluster TPUs (`72` vs. `64` `8`-TPU `t0` `PodGroup`s) within `< 5s` of lender arrival.",
        },
        {
            "peer_id": "st49",
            "relationship": "Nominal Cohort Reclamation (`st48`) vs. Zero-Nominal 4-CQ Fair Sharing (`st49`)",
            "knob_changed": "2 CQs with `576` nominal TPUs each (`st48`) vs. 4 CQs with `0` nominal TPUs and `1:1:1:1` `fairSharing.weight` (`st49`).",
            "cross_check": "`st48` reclaims `72` `t0` `PodGroup`s (`50%` of `144`), whereas `st49` preempts `~120` `t0` `PodGroup`s (`75%` of `160`) via `InCohortFairSharing`.",
        },
    ],
    "st49": [
        {
            "peer_id": "st58",
            "relationship": "160-Node Quota-Only vs. TAS 4-CQ Fair Sharing Pair",
            "knob_changed": "Quota-only 4-CQ Fair Sharing (`st49`, `1,160` workloads) vs. 7-level TAS 4-CQ Fair Sharing (`st58`, `1,140` workloads) on `160` nodes (`1,280` TPUs).",
            "cross_check": "Both preempt `~120` of `160` `t0` monopolizer `PodGroup`s (`InCohortFairSharing`) to equalize all 4 CQs around `320` TPUs (`25%` each).",
        },
    ],
    "st50": [
        {
            "peer_id": "st60",
            "relationship": "Multi-API + `infra-pods` + Infinite `Deployment` Quota-Only (`st50`) vs. TAS (`st60`) Pair",
            "knob_changed": "128-node quota-only (`st50`, `1,064` Kueue workloads) vs. 144-node TAS (`st60`, `1,080` Kueue workloads) with identical `64` `t0` multi-API workloads (`JobSet`, `Job`, `PodGroup`, infinite `Deployment`) + `16` `infra-pods`.",
            "cross_check": "Both report exact `Initial Kueue Workloads: 64 PodGroup(s) (80 pod(s))`, `Initial Infra Pods: 16`, `active_at_end=16`, and `FULL-DUR SAT <= 100%`.",
        },
    ],
    "st51": [
        {
            "peer_id": "st46",
            "relationship": "128-Node TAS Packing vs. Quota-Only Baseline Pair",
            "knob_changed": "7-level TAS `st-slice-topology` (`st51`) vs. quota-only (`st46`).",
            "cross_check": "Both achieve `100.0%` peak CQ utilization (`1,024/1,024` TPUs) and `0` evictions across >1,050 workloads.",
        },
        {
            "peer_id": "st55",
            "relationship": "128-Node Single-Pool TAS vs. 256-Node Dual-`ResourceFlavor` TAS Scale Pair",
            "knob_changed": "`128` nodes / `1,024` TPUs / 1 flavor (`st51`) vs. `256` nodes / `2,048` TPUs / 2 flavors (`st55`, `1,200` workloads).",
            "cross_check": "Both complete `100%` of >1,080 TAS workloads with `0` evictions while `st55` doubles concurrent pod capacity to `512` pods (`2,048` TPUs).",
        },
    ],
    "st52": [
        {
            "peer_id": "st51",
            "relationship": "160-Node Multi-Slice (`4`/`2`/`1`-Node) TAS `JobSet`s vs. 128-Node `2`/`1`-Node TAS Jobs Pair",
            "knob_changed": "`40` `4`-node rack `t0` `JobSet`s + `80` `2`-node `JobSet`s + `1,000` `1`-node `Job`s (`st52`) vs. `64` `2`-node `t0` `Job`s + `1,024` `1`-node `Job`s (`st51`).",
            "cross_check": "Both complete `100%` of workloads with `0` evictions and `0` duplicate child `Job` submissions.",
        },
    ],
    "st53": [
        {
            "peer_id": "st54",
            "relationship": "TAS Rack-Blocked Backfill (`st53`) vs. Mixed Required/Preferred/Unconstrained TAS (`st54`) Pair",
            "knob_changed": "`32` rack-pinning `t0` blockers delaying `8` full-rack `32`-TPU jobs while `1,016` host jobs backfill (`st53`) vs. `48` `t0` `2`-pod TAS jobs (half partially bound at `t0`) + `1,056` required/preferred/unconstrained arrivals (`st54`).",
            "cross_check": "`st53` delays `arr-fullrack-*` until `t >= 16s` while maintaining `> 90%` backfill utilization; `st54` reconstructs all `96` `t0` pods (`48` bound + `24`+`24` Tier-4 synthesized) and completes `1,104` workloads.",
        },
    ],
    "st54": [
        {
            "peer_id": "st53",
            "relationship": "Partial Vanilla-K8s `t0` TAS Synthesis + 3 TAS Modes (`st54`) vs. TAS Rack Backfill (`st53`)",
            "knob_changed": "`24` partially bound `t0` TAS jobs (`1/2` pods in `infra.json`) + `preferred`/`unconstrained` TAS modes (`st54`) vs. `required`-only rack backfill (`st53`).",
            "cross_check": "`st54` reports `Initial Kueue Workloads: 48 PodGroup(s) (96 pod(s))` (verifying partial-pod Tier 4 synthesis) and `1,104` completed workloads.",
        },
    ],
    "st55": [
        {
            "peer_id": "st51",
            "relationship": "256-Node Dual-Flavor TAS Fleet (`st55`) vs. 128-Node Single-Flavor TAS (`st51`)",
            "knob_changed": "`256` nodes (`2,048` TPUs, `st-tpu-pool-a` + `st-tpu-pool-b`, `1,200` workloads) vs. `128` nodes (`1,024` TPUs, `1,088` workloads).",
            "cross_check": "`st55` syncs `256` nodes without ConfigMap overflow and saturates both `1,024`-TPU flavors concurrently (`2,048` TPUs total).",
        },
    ],
    "st56": [
        {
            "peer_id": "st47",
            "relationship": "128-Node TAS vs. Quota-Only Intra-CQ Preemption Pair",
            "knob_changed": "7-level TAS `2x2x2-id` preemption (`st56`, `1,060` workloads) vs. quota-only preemption (`st47`, `1,080` workloads).",
            "cross_check": "Both evict `>= 64` `t0` `p-low` `PodGroup`s (`512` TPUs) at `t=3s` and dynamically resubmit all `64` victims.",
        },
    ],
    "st57": [
        {
            "peer_id": "st59",
            "relationship": "Unbounded TAS Cohort Borrowing (`200%`, `st57`) vs. Capped TAS `borrowingLimit: 256` (`150%`, `st59`)",
            "knob_changed": "Unbounded Cohort borrowing + lender reclaim (`st57`) vs. `borrowingLimit: 256` / `lendingLimit: 256` with idle donor (`st59`) on `128` TAS nodes.",
            "cross_check": "Borrower `Sec 5.1` hits `200.0%` (`1,024/512`) in `st57` with `64` reclaims, whereas `cq-tas-capped` in `st59` is strictly capped at `150.0%` (`768/512`) with `0` evictions and `32` idle nodes.",
        },
        {
            "peer_id": "st48",
            "relationship": "TAS (`st57`) vs. Quota-Only (`st48`) Cohort Borrowing & Reclamation Pair",
            "knob_changed": "7-level TAS (`st57`) vs. quota-only (`st48`).",
            "cross_check": "Both reclaim exact `50%` of cluster TPUs from `200%`-borrowing `t0` `PodGroup`s upon lender arrival.",
        },
    ],
    "st58": [
        {
            "peer_id": "st49",
            "relationship": "160-Node TAS (`st58`) vs. Quota-Only (`st49`) 4-CQ Fair Sharing Pair",
            "knob_changed": "7-level TAS (`st58`, `1,140` workloads) vs. quota-only (`st49`, `1,160` workloads) on `160` nodes (`1,280` TPUs).",
            "cross_check": "Both trigger `~120` `InCohortFairSharing` evictions against `t0` monopolizer `PodGroup`s so all 4 CQs converge to `320` TPUs (`25%` each).",
        },
    ],
    "st59": [
        {
            "peer_id": "st57",
            "relationship": "TAS `borrowingLimit` `150%` Ceiling (`st59`) vs. `200%` Full Cohort Borrowing (`st57`)",
            "knob_changed": "`borrowingLimit: 256` (`st59`) vs. unlimited Cohort borrowing (`st57`).",
            "cross_check": "`cq-tas-capped` `Sec 5.1 P90/P99` stays `<= 150.0%` (`768/512`) and `Sec 5.4 <= 75.0%` in `st59`, vs. `200.0%` peak in `st57`.",
        },
    ],
    "st60": [
        {
            "peer_id": "st50",
            "relationship": "144-Node TAS Hybrid Multi-API (`st60`) vs. 128-Node Quota-Only Hybrid Multi-API (`st50`) Pair",
            "knob_changed": "7-level TAS on `144` nodes (`st60`, `1,080` Kueue workloads) vs. quota-only on `128` nodes (`st50`, `1,064` Kueue workloads).",
            "cross_check": "Both cleanly separate `16` `infra-pods` from `64` multi-API `t0` `PodGroup`s (`80` pods), complete all finite arrivals, keep `16` infinite `Deployment`s `active_at_end`, and round-trip through `cluster-scraper` without phantom pod duplication.",
        },
    ],
}
