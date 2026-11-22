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

"""Node failure, TAS hot-swap, waitForPodsReady scaling, and interrupted-duration scenarios (`st61`–`st75`).

Covers:
- Category 13 (`st61`–`st65`): Mid-scenario node failures (`NotReady`, `Deleted`, `NoExecute` taints),
  Kueue TAS placement hot-swap (`0` Workload evictions when spare topology nodes exist) vs.
  `TASFailedNodeReplacementFailFast` / multi-node `NodeFailures` eviction, non-TAS pod replacement,
  and dynamic post-`t0` node recovery / scale-up.
- Category 14 (`st66`–`st70`): Adjusting Kueue `waitForPodsReady` (`timeout`, `recoveryTimeout`,
  `blockAdmission`, `requeuingStrategy`) to `--speedup` and `--duration`, including post-node-failure
  `recoveryTimeout` eviction and head-of-line `blockAdmission` gating.
- Category 15 (`st71`–`st75`): Recording `PartialExecution` (`Preempted`, `NodeFailure`,
  `PodsReadyTimeout`, `ScrapeEnded`) and replaying interrupted-only workloads (`no FullExecution`)
  strictly longer than their interrupted recording via `--interrupted-duration-multiplier`,
  schema `missingDurationMultiplier`, schema `trueDurationSeconds`, or CLI
  `--interrupted-duration-override`, culminating in a combined multi-fault + preemption stress scenario.
"""

from __future__ import annotations

import json
from typing import Any

DEFAULT_FLAVOR = "st-tpu-flavor"
DEFAULT_TOPOLOGY = "st-slice-topology"
DEFAULT_NS = "st-workloads"
DEFAULT_ACCEL_LABEL = "tpu-v7x-st"

KWOK_FAKE_TOLERATION = {
    "key": "kwok.x-k8s.io/node",
    "operator": "Equal",
    "value": "fake",
    "effect": "NoSchedule",
}


def _make_tas_or_plain_node(
    idx: int,
    *,
    det_uid_fn: Any,
    tpu_per_node: int = 4,
    cpu_per_node: int = 16,
    mem_gi_per_node: int = 64,
    pods_per_node: int = 110,
    with_topology: bool = False,
    rack_override: int | None = None,
    ready_status: str = "True",
    unschedulable: bool = False,
    extra_taints: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    node_name = f"st-node-{idx:02d}"
    cube_idx = 0
    local_idx = idx % 16
    block_idx = 0
    rack_id = rack_override if rack_override is not None else (local_idx // 4)
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
                "cloud.google.com/gke-tpu-partition-2x4x4-id": f"b{block_idx:02d}-c{cube_idx:02d}-p244-{rack_id // 2}",
                "cloud.google.com/gke-tpu-partition-2x2x4-id": f"b{block_idx:02d}-c{cube_idx:02d}-p224-{rack_id}",
                "cloud.google.com/gke-tpu-partition-2x2x2-id": f"b{block_idx:02d}-c{cube_idx:02d}-p222-{local_idx // 2}",
                "cloud.google.com/gke-tpu-partition-2x2x1-id": f"b{block_idx:02d}-c{cube_idx:02d}-p221-{idx:02d}",
            }
        )
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
    if ready_status != "True":
        annotations["kueue-optimizer.gke.io/node-failed"] = "true"
    spec: dict[str, Any] = {}
    if unschedulable:
        spec["unschedulable"] = True
    if extra_taints:
        spec["taints"] = [dict(t) for t in extra_taints]
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "uid": det_uid_fn(f"node:{node_name}"),
            "labels": labels,
            "annotations": annotations,
        },
        "spec": spec,
        "status": {
            "capacity": dict(cap),
            "allocatable": dict(cap),
            "conditions": [
                {
                    "type": "Ready",
                    "status": ready_status,
                    "reason": "KubeletReady" if ready_status == "True" else "KubeletNotReady",
                    "message": (
                        "kwok kubelet is posting ready status"
                        if ready_status == "True"
                        else "simulated node failure"
                    ),
                }
            ],
        },
    }


def _make_batch_job_and_exec_events(
    *,
    name: str,
    fmt_ts_fn: Any,
    det_uid_fn: Any,
    arrival_sec: float,
    admitted_sec: float | None,
    duration_sec: float,
    lq_name: str,
    cq_name: str,
    num_pods: int = 1,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 8,
    mem_gi_per_pod: int = 32,
    priority_class: str | None = None,
    tas_required_level: str | None = None,
    backoff_limit: int = 4,
    indexed: bool = True,
    startup_delay_sec: float | None = None,
    never_ready: bool = False,
    record_full: bool = True,
    partial_slices: list[tuple[float, float, str, bool]] | None = None,
    true_duration_sec: float | None = None,
    missing_multiplier: float | None = None,
    unhealthy_nodes: list[str] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    create_ts = fmt_ts_fn(arrival_sec)
    adm_sec = arrival_sec if admitted_sec is None else admitted_sec
    adm_ts = fmt_ts_fn(adm_sec)
    wl_name = f"job-{name}-wl"

    labels: dict[str, str] = {"kueue.x-k8s.io/queue-name": lq_name}
    if priority_class:
        labels["kueue.x-k8s.io/priority-class"] = priority_class

    top_anns: dict[str, str] = {
        "kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(round(duration_sec))),
    }
    pod_anns: dict[str, str] = {
        "kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(round(duration_sec))),
    }
    if tas_required_level:
        pod_anns["kueue.x-k8s.io/podset-required-topology"] = tas_required_level
    if startup_delay_sec is not None and startup_delay_sec > 0:
        s_str = str(int(round(startup_delay_sec)))
        top_anns["kueue-optimizer.gke.io/pod-startup-delay-seconds"] = s_str
        pod_anns["kueue-optimizer.gke.io/pod-startup-delay-seconds"] = s_str
    if never_ready:
        top_anns["kueue-optimizer.gke.io/pod-never-ready"] = "true"
        pod_anns["kueue-optimizer.gke.io/pod-never-ready"] = "true"
    if true_duration_sec is not None:
        top_anns["kueue-optimizer.gke.io/true-execution-time-seconds"] = str(true_duration_sec)
    if missing_multiplier is not None:
        top_anns["kueue-optimizer.gke.io/missing-duration-multiplier"] = str(missing_multiplier)

    reqs = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
        "google.com/tpu": str(tpu_per_pod),
    }
    job_spec: dict[str, Any] = {
        "suspend": True,
        "parallelism": num_pods,
        "completions": num_pods,
        "backoffLimit": backoff_limit,
        "completionMode": "Indexed" if indexed else "NonIndexed",
        "template": {
            "metadata": {"annotations": pod_anns},
            "spec": {
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
            },
        },
    }
    job_obj = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": DEFAULT_NS,
            "uid": det_uid_fn(f"job:{DEFAULT_NS}/{name}"),
            "creationTimestamp": create_ts,
            "labels": labels,
            "annotations": top_anns,
        },
        "spec": job_spec,
    }
    wf_ev = {
        "timestamp": create_ts,
        "eventType": "New",
        "workloadState": "NotAdmitted",
        "payload": {
            "objectKind": "Job",
            "name": name,
            "namespace": DEFAULT_NS,
            "creationTime": create_ts,
            "localQueueName": lq_name,
            "object": job_obj,
        },
    }

    base_pl: dict[str, Any] = {
        "workloadName": wl_name,
        "namespace": DEFAULT_NS,
        "ownerKind": "Job",
        "ownerName": name,
        "ownerAPIVersion": "batch/v1",
        "localQueueName": lq_name,
        "clusterQueueName": cq_name,
        "creationTime": create_ts,
    }
    if priority_class:
        base_pl["priorityClassName"] = priority_class
    if unhealthy_nodes:
        base_pl["unhealthyNodes"] = list(unhealthy_nodes)
    if true_duration_sec is not None:
        base_pl["trueDurationSeconds"] = float(true_duration_sec)
    if missing_multiplier is not None:
        base_pl["missingDurationMultiplier"] = float(missing_multiplier)

    exec_evs: list[dict[str, Any]] = []
    preempt_cnt = 0
    int_cnt = 0
    if partial_slices:
        for sl_adm, sl_end, int_reason, is_preempt in partial_slices:
            exec_evs.append(
                {
                    "timestamp": fmt_ts_fn(sl_adm),
                    "eventType": "Admitted",
                    "workloadState": "Admitted",
                    "payload": {
                        **base_pl,
                        "admittedTime": fmt_ts_fn(sl_adm),
                        "preempted": False,
                        "preemptionCount": preempt_cnt,
                        "interruptionCount": int_cnt,
                        "reason": "Admitted",
                    },
                }
            )
            int_cnt += 1
            if is_preempt:
                preempt_cnt += 1
            p_dur = max(0.0, float(sl_end - sl_adm))
            exec_evs.append(
                {
                    "timestamp": fmt_ts_fn(sl_end),
                    "eventType": "PartialExecution",
                    "workloadState": "Admitted" if int_reason == "ScrapeEnded" else "NotAdmitted",
                    "payload": {
                        **base_pl,
                        "executionType": "Partial",
                        "admittedTime": fmt_ts_fn(sl_adm),
                        "endTime": fmt_ts_fn(sl_end),
                        "executionDurationSeconds": p_dur,
                        "executionDuration": f"{p_dur:g}s",
                        "preempted": is_preempt,
                        "preemptionCount": preempt_cnt,
                        "interruptionReason": int_reason,
                        "interruptionCount": int_cnt,
                        "reason": int_reason,
                    },
                }
            )

    if record_full:
        end_ts = fmt_ts_fn(adm_sec + duration_sec)
        exec_evs.append(
            {
                "timestamp": adm_ts,
                "eventType": "Admitted",
                "workloadState": "Admitted",
                "payload": {
                    **base_pl,
                    "admittedTime": adm_ts,
                    "preempted": False,
                    "preemptionCount": preempt_cnt,
                    "interruptionCount": int_cnt,
                    "reason": "Admitted",
                },
            }
        )
        exec_evs.append(
            {
                "timestamp": end_ts,
                "eventType": "FullExecution",
                "workloadState": "Admitted",
                "payload": {
                    **base_pl,
                    "executionType": "Full",
                    "admittedTime": adm_ts,
                    "endTime": end_ts,
                    "executionDurationSeconds": float(duration_sec),
                    "executionDuration": f"{duration_sec:g}s",
                    "preempted": False,
                    "preemptionCount": preempt_cnt,
                    "interruptionCount": int_cnt,
                    "reason": "Succeeded",
                },
            }
        )
    return wf_ev, exec_evs


def _make_jobset_partial_or_full_events(
    *,
    name: str,
    fmt_ts_fn: Any,
    det_uid_fn: Any,
    arrival_sec: float,
    admitted_sec: float | None,
    duration_sec: float,
    lq_name: str,
    cq_name: str,
    num_pods: int = 1,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 8,
    mem_gi_per_pod: int = 32,
    priority_class: str | None = None,
    tas_required_level: str | None = None,
    backoff_limit: int = 0,
    startup_delay_sec: float | None = None,
    never_ready: bool = False,
    record_full: bool = True,
    partial_slices: list[tuple[float, float, str, bool]] | None = None,
    true_duration_sec: float | None = None,
    missing_multiplier: float | None = None,
    unhealthy_nodes: list[str] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    create_ts = fmt_ts_fn(arrival_sec)
    adm_sec = arrival_sec if admitted_sec is None else admitted_sec
    adm_ts = fmt_ts_fn(adm_sec)
    wl_name = f"jobset-{name}-wl"

    labels: dict[str, str] = {"kueue.x-k8s.io/queue-name": lq_name}
    if priority_class:
        labels["kueue.x-k8s.io/priority-class"] = priority_class

    top_anns: dict[str, str] = {}
    pod_anns: dict[str, str] = {}
    if record_full:
        top_anns["kueue-optimizer.gke.io/pod-execution-time-seconds"] = str(int(round(duration_sec)))
        pod_anns["kueue-optimizer.gke.io/pod-execution-time-seconds"] = str(int(round(duration_sec)))
    if tas_required_level:
        pod_anns["kueue.x-k8s.io/podset-required-topology"] = tas_required_level
    if startup_delay_sec is not None and startup_delay_sec > 0:
        s_str = str(int(round(startup_delay_sec)))
        top_anns["kueue-optimizer.gke.io/pod-startup-delay-seconds"] = s_str
        pod_anns["kueue-optimizer.gke.io/pod-startup-delay-seconds"] = s_str
    if never_ready:
        top_anns["kueue-optimizer.gke.io/pod-never-ready"] = "true"
        pod_anns["kueue-optimizer.gke.io/pod-never-ready"] = "true"
    if true_duration_sec is not None:
        top_anns["kueue-optimizer.gke.io/true-execution-time-seconds"] = str(true_duration_sec)
    if missing_multiplier is not None:
        top_anns["kueue-optimizer.gke.io/missing-duration-multiplier"] = str(missing_multiplier)

    reqs = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
        "google.com/tpu": str(tpu_per_pod),
    }
    js_obj = {
        "apiVersion": "jobset.x-k8s.io/v1alpha2",
        "kind": "JobSet",
        "metadata": {
            "name": name,
            "namespace": DEFAULT_NS,
            "uid": det_uid_fn(f"js:{DEFAULT_NS}/{name}"),
            "creationTimestamp": create_ts,
            "labels": labels,
            "annotations": top_anns,
        },
        "spec": {
            "suspend": True,
            "replicatedJobs": [
                {
                    "name": "workers",
                    "replicas": 1,
                    "template": {
                        "metadata": {},
                        "spec": {
                            "parallelism": num_pods,
                            "completions": num_pods,
                            "backoffLimit": backoff_limit,
                            "template": {
                                "metadata": {"annotations": pod_anns},
                                "spec": {
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
                                },
                            },
                        },
                    },
                }
            ],
        },
    }
    wf_ev = {
        "timestamp": create_ts,
        "eventType": "New",
        "workloadState": "NotAdmitted",
        "payload": {
            "objectKind": "JobSet",
            "name": name,
            "namespace": DEFAULT_NS,
            "creationTime": create_ts,
            "localQueueName": lq_name,
            "object": js_obj,
        },
    }

    base_pl: dict[str, Any] = {
        "workloadName": wl_name,
        "namespace": DEFAULT_NS,
        "ownerKind": "JobSet",
        "ownerName": name,
        "ownerAPIVersion": "jobset.x-k8s.io/v1alpha2",
        "localQueueName": lq_name,
        "clusterQueueName": cq_name,
        "creationTime": create_ts,
    }
    if priority_class:
        base_pl["priorityClassName"] = priority_class
    if unhealthy_nodes:
        base_pl["unhealthyNodes"] = list(unhealthy_nodes)
    if true_duration_sec is not None:
        base_pl["trueDurationSeconds"] = float(true_duration_sec)
    if missing_multiplier is not None:
        base_pl["missingDurationMultiplier"] = float(missing_multiplier)

    exec_evs: list[dict[str, Any]] = []
    preempt_cnt = 0
    int_cnt = 0
    if partial_slices:
        for sl_adm, sl_end, int_reason, is_preempt in partial_slices:
            exec_evs.append(
                {
                    "timestamp": fmt_ts_fn(sl_adm),
                    "eventType": "Admitted",
                    "workloadState": "Admitted",
                    "payload": {
                        **base_pl,
                        "admittedTime": fmt_ts_fn(sl_adm),
                        "preempted": False,
                        "preemptionCount": preempt_cnt,
                        "interruptionCount": int_cnt,
                        "reason": "Admitted",
                    },
                }
            )
            int_cnt += 1
            if is_preempt:
                preempt_cnt += 1
            p_dur = max(0.0, float(sl_end - sl_adm))
            exec_evs.append(
                {
                    "timestamp": fmt_ts_fn(sl_end),
                    "eventType": "PartialExecution",
                    "workloadState": "Admitted" if int_reason == "ScrapeEnded" else "NotAdmitted",
                    "payload": {
                        **base_pl,
                        "executionType": "Partial",
                        "admittedTime": fmt_ts_fn(sl_adm),
                        "endTime": fmt_ts_fn(sl_end),
                        "executionDurationSeconds": p_dur,
                        "executionDuration": f"{p_dur:g}s",
                        "preempted": is_preempt,
                        "preemptionCount": preempt_cnt,
                        "interruptionReason": int_reason,
                        "interruptionCount": int_cnt,
                        "reason": int_reason,
                    },
                }
            )

    if record_full:
        end_ts = fmt_ts_fn(adm_sec + duration_sec)
        exec_evs.append(
            {
                "timestamp": adm_ts,
                "eventType": "Admitted",
                "workloadState": "Admitted",
                "payload": {
                    **base_pl,
                    "admittedTime": adm_ts,
                    "preempted": False,
                    "preemptionCount": preempt_cnt,
                    "interruptionCount": int_cnt,
                    "reason": "Admitted",
                },
            }
        )
        exec_evs.append(
            {
                "timestamp": end_ts,
                "eventType": "FullExecution",
                "workloadState": "Admitted",
                "payload": {
                    **base_pl,
                    "executionType": "Full",
                    "admittedTime": adm_ts,
                    "endTime": end_ts,
                    "executionDurationSeconds": float(duration_sec),
                    "executionDuration": f"{duration_sec:g}s",
                    "preempted": False,
                    "preemptionCount": preempt_cnt,
                    "interruptionCount": int_cnt,
                    "reason": "Succeeded",
                },
            }
        )
    return wf_ev, exec_evs


def build_failure_preemption_15_scenarios(
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
    """Build scenarios `st61`–`st75` covering node failures, hot-swap, waitForPodsReady, and interrupted replay."""
    _ = (make_cohort_fn, make_jobset_and_exec_events_fn, make_initial_pod_events_fn)
    scenarios: list[Any] = []

    def _base_infra(
        num_nodes: int,
        *,
        with_topology: bool = False,
        priority_classes: list[tuple[str, int]] | None = None,
    ) -> list[dict[str, Any]]:
        evs: list[dict[str, Any]] = []
        for idx in range(num_nodes):
            evs.append(
                make_infra_event_fn(
                    _make_tas_or_plain_node(idx, det_uid_fn=det_uid_fn, with_topology=with_topology),
                    0.0,
                    "New",
                )
            )
        evs.append(make_infra_event_fn(make_namespace_fn(DEFAULT_NS), 0.0, "New"))
        if with_topology:
            evs.append(make_infra_event_fn(make_topology_fn(DEFAULT_TOPOLOGY), 0.0, "New"))
            evs.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR, topology_name=DEFAULT_TOPOLOGY), 0.0, "New"))
        else:
            evs.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR, topology_name=None), 0.0, "New"))
        for pc_name, pc_val in priority_classes or [("p-standard", 100)]:
            evs.append(make_infra_event_fn(make_priority_class_fn(pc_name, pc_val), 0.0, "New"))
        return evs

    # =========================================================================
    # CATEGORY 13 (st61 - st65): Mid-Scenario Node Failures & Placement Hot-Swap
    # =========================================================================

    # st61: Single-Node Failure TAS Hot-Swap Within Same Rack (0 Evictions)
    inf61 = _base_infra(8, with_topology=True)
    inf61.append(make_infra_event_fn(make_cluster_queue_fn("cq-tas-hs", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf61.append(make_infra_event_fn(make_local_queue_fn("lq-tas-hs", "cq-tas-hs")))
    # At t=6s, st-node-00 in rack p224-0 transitions to Ready=False while st-node-03 is spare in p224-0!
    inf61.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(
                0,
                det_uid_fn=det_uid_fn,
                with_topology=True,
                ready_status="False",
                unschedulable=True,
                extra_taints=[{"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"}],
            ),
            6.0,
            "Update",
        )
    )
    wf61, ex61 = [], []
    for r_i in range(2):
        w_ev, e_evs = _make_batch_job_and_exec_events(
            name=f"job-tas-rack-{r_i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=0.0,
            duration_sec=16.0,
            lq_name="lq-tas-hs",
            cq_name="cq-tas-hs",
            num_pods=3,
            priority_class="p-standard",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
            backoff_limit=4,
            unhealthy_nodes=["st-node-00"] if r_i == 0 else None,
        )
        wf61.append(w_ev)
        ex61.extend(e_evs)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st61",
            slug="tas-single-node-failure-hotswap-same-rack",
            category="13. Mid-Scenario Node Failures & Placement Hot-Swap",
            title="TAS Single-Node Failure In-Place Placement Hot-Swap (`0` Workload Evictions)",
            feature_focus="Kueue `TASFailedNodeReplacement` + `TASReplaceNodeOnPodTermination` hot-swaps a failed rack node (`st-node-00`) to a healthy spare node (`st-node-03`) in the same `2x2x4-id` rack without evicting the Workload",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack",
            setup_summary="8-node (`32`-TPU, `2` racks of `4` nodes: `p224-0` = `st-node-00..03`, `p224-1` = `st-node-04..07`) 7-level TAS cluster. Two `3`-pod (`12`-TPU) `Indexed` `Job`s (`backoffLimit: 4`, `podset-required-topology: 2x2x4-id`, `16s`) admit at `t=0s`, leaving `1` spare node per rack. At `t=6s`, `st-node-00` fails (`Ready=False`).",
            expected_behavior="Pod on `st-node-00` fails immediately (`NodeFailure`); `tas-node-reconciler` sets `unhealthyNodes=[st-node-00]`, Kueue hot-swaps `TopologyAssignment` in-place to spare `st-node-03` in `p224-0` with **zero Workload evictions**, `Job` controller creates 1 replacement pod ungated onto `st-node-03`, and both `Job`s complete cleanly.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions)`: Strictly **`0`** (in-place TAS failed-node hot-swap, NOT a Workload eviction).",
                "`Execution Summary`: `workloads finished = 2`, `pods failed = 1`, `pods succeeded = 6` (`3 + 3`).",
                "`Section 5.4`: Both `Job`s (`24` TPUs total) complete uninterrupted from Kueue's Workload perspective (`Excluded: 0`).",
                "`cluster-scraper`: Captures `Node` `Update` (`Ready=False`) + failed pod `Deleted` + replacement pod `New` on `st-node-03` in `infra.json`.",
            ],
            must_not_happen=[
                "`Section 5.3` reporting `>= 1` Workload eviction when a healthy spare node (`st-node-03`) exists in the same `2x2x4-id` rack.",
                "Replacement pod getting stuck `Pending` on `st-node-00` or failing to ungate onto `st-node-03`.",
            ],
            sec51_target="~65%–78% (24/32 TPUs)",
            sec52_target="< 1.5s",
            sec53_target="0 evictions (in-place hot-swap)",
            sec54_target="~62%–76% (2 completed, 0 evicted)",
            infra_events=inf61,
            workflow_events=wf61,
            exec_events=ex61,
        )
    )

    # st62: Node Deletion Hot-Swap + Dynamic Replacement Node Addition
    inf62 = _base_infra(8, with_topology=True)
    inf62.append(make_infra_event_fn(make_cluster_queue_fn("cq-tas-del", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf62.append(make_infra_event_fn(make_local_queue_fn("lq-tas-del", "cq-tas-del")))
    # At t=5s, st-node-00 is Deleted!
    inf62.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(0, det_uid_fn=det_uid_fn, with_topology=True),
            5.0,
            "Deleted",
        )
    )
    # At t=11s, replacement node st-node-08 joins rack 0 (p224-0) so full-rack Wave 2 fits!
    inf62.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(8, det_uid_fn=det_uid_fn, with_topology=True, rack_override=0),
            11.0,
            "New",
        )
    )
    wf62, ex62 = [], []
    for r_i in range(2):
        w_ev, e_evs = _make_jobset_partial_or_full_events(
            name=f"js-wave1-hs-{r_i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=0.0,
            duration_sec=14.0,
            lq_name="lq-tas-del",
            cq_name="cq-tas-del",
            num_pods=3,
            priority_class="p-standard",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
            backoff_limit=4,
            unhealthy_nodes=["st-node-00"] if r_i == 0 else None,
        )
        wf62.append(w_ev)
        ex62.extend(e_evs)
    for r_i in range(2):
        w_ev, e_evs = _make_jobset_partial_or_full_events(
            name=f"js-wave2-rack4-{r_i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=9.0,
            admitted_sec=20.0,
            duration_sec=10.0,
            lq_name="lq-tas-del",
            cq_name="cq-tas-del",
            num_pods=4,
            priority_class="p-standard",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
            backoff_limit=2,
        )
        wf62.append(w_ev)
        ex62.extend(e_evs)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st62",
            slug="tas-node-deleted-hotswap-and-scaleup",
            category="13. Mid-Scenario Node Failures & Placement Hot-Swap",
            title="TAS Mid-Run Node Deletion Hot-Swap + Post-`t0` Replacement Node Scale-Up",
            feature_focus="`Node` `Deleted` hot-swap (`!nodeExists` immediate replacement onto spare `st-node-03`) followed by post-`t0` `Node` `New` (`st-node-08` joining rack `p224-0`) unblocking full-rack `4`-node Wave-2 `JobSet`s",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st62-tas-node-deleted-hotswap-and-scaleup",
            setup_summary="8-node (`32`-TPU) TAS cluster (`2` racks of `4` nodes). Wave 1 (`t=0s`): two `3`-pod TAS `JobSet`s (`backoffLimit: 4`, `14s`). At `t=5s`, `st-node-00` is **Deleted** (`EventDeleted`), triggering hot-swap to `st-node-03`. Wave 2 (`t=9s`): two `4`-pod full-rack TAS `JobSet`s (`10s`) arrive; at `t=11s`, replacement `st-node-08` joins `p224-0` (`EventNew`).",
            expected_behavior="Wave 1 survives `st-node-00` deletion with `0` Workload evictions via hot-swap to `st-node-03`. Adding `st-node-08` restores `p224-0` to `4` healthy nodes (`st-node-01,02,03,08`), allowing both `4`-pod full-rack Wave-2 `JobSet`s to admit and succeed once Wave 1 finishes.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions)`: Strictly **`0`**.",
                "`Execution Summary`: All **`4/4` `JobSet`s** complete (`workloads finished = 4`).",
                "`cluster-scraper`: `infra.json` records `Node` `Deleted` (`st-node-00`) at `t≈5s` and `Node` `New` (`st-node-08`) at `t≈11s` via real-time `watchNodesLoop`.",
            ],
            must_not_happen=[
                "Wave-2 `4`-node full-rack `JobSet` deadlocking in `Pending` (would happen if post-`t0` `Node` `New` `st-node-08` were ignored).",
                "Wave-1 `JobSet` getting evicted when `st-node-00` is deleted while `st-node-03` is idle in `p224-0`.",
            ],
            sec51_target="~75%–88%",
            sec52_target="Wave 1 < 1.5s, Wave 2 ~10s–12s",
            sec53_target="0 evictions",
            sec54_target="~72%–86% (4 completed)",
            infra_events=inf62,
            workflow_events=wf62,
            exec_events=ex62,
        )
    )

    # st63: TAS Node Failure With Zero Spare Nodes -> FailFast Eviction + Node Recovery (Twin of st61)
    inf63 = _base_infra(8, with_topology=True)
    inf63.append(make_infra_event_fn(make_cluster_queue_fn("cq-tas-ff", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf63.append(make_infra_event_fn(make_local_queue_fn("lq-tas-ff", "cq-tas-ff")))
    # At t=6s, st-node-00 fails (0 spare nodes in p224-0 because jobs use all 4 nodes per rack!).
    inf63.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(
                0,
                det_uid_fn=det_uid_fn,
                with_topology=True,
                ready_status="False",
                unschedulable=True,
                extra_taints=[{"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"}],
            ),
            6.0,
            "Update",
        )
    )
    # At t=13s, st-node-00 recovers (Ready=True)!
    inf63.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(0, det_uid_fn=det_uid_fn, with_topology=True, ready_status="True"),
            13.0,
            "Update",
        )
    )
    wf63, ex63 = [], []
    for r_i in range(2):
        w_ev, e_evs = _make_batch_job_and_exec_events(
            name=f"job-tas-fullrack-{r_i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=13.0 if r_i == 0 else 0.0,
            duration_sec=14.0,
            lq_name="lq-tas-ff",
            cq_name="cq-tas-ff",
            num_pods=4,
            priority_class="p-standard",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
            backoff_limit=6,
            partial_slices=[(0.0, 6.0, "NodeFailure", False)] if r_i == 0 else None,
            unhealthy_nodes=["st-node-00"] if r_i == 0 else None,
        )
        wf63.append(w_ev)
        ex63.extend(e_evs)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st63",
            slug="tas-node-failure-no-spare-failfast-evict",
            category="13. Mid-Scenario Node Failures & Placement Hot-Swap",
            title="TAS Node Failure With Zero Spare Nodes — `FailFast` `NodeFailures` Eviction & Recovery (Twin of st61)",
            feature_focus="Direct A/B twin of `st61` using `4`-pod full-rack `Job`s (`0` spare nodes in `p224-0`): `TASFailedNodeReplacementFailFast` immediately evicts with `NodeFailures`, then re-admits once `st-node-00` recovers at `t=13s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st63-tas-node-failure-no-spare-failfast-evict",
            setup_summary="Identical 8-node (`32`-TPU, `2` racks of `4` nodes) TAS cluster and `t=6s` `st-node-00` failure as `st61`, except both `Job`s request **all `4` nodes (`16` TPUs) per rack** (`0` spare nodes). At `t=13s`, `st-node-00` recovers (`Ready=True`).",
            expected_behavior="Healthy rack `p224-1`'s `Job` finishes uninterrupted at `t≈14s`. Affected rack `p224-0`'s `Job` cannot hot-swap (`0` spare nodes in `p224-0`), so Kueue immediately evicts its Workload (`NodeFailures`) at `t≈6s`, holds it suspended while `st-node-00` is down (`6s–13s`), and re-admits it at `t≈13s` to run `14s` to completion.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions)`: **`1` eviction** (`NodeFailures` — contrasting with `0` in `st61`).",
                "`Execution Summary`: Both `2/2` `Job`s finish (`workloads finished = 2`).",
                "`cluster-scraper`: Emits `PartialExecution` (`interruptionReason: 'NodeFailure'`, `preempted: false`) at `t≈6s`, followed by re-`Admitted` at `t≈13s` and `FullExecution` (`~14s`).",
            ],
            must_not_happen=[
                "`Section 5.3` reporting `0` evictions when `0` spare nodes exist in the required `2x2x4-id` rack.",
                "`cluster-scraper` mislabeling a `NodeFailures` eviction as `preempted: true`.",
            ],
            sec51_target="~70%–86%",
            sec52_target="< 2.0s initial, re-admit at ~13s",
            sec53_target="1 eviction (NodeFailures)",
            sec54_target="~62%–76% (6s aborted slice excluded)",
            infra_events=inf63,
            workflow_events=wf63,
            exec_events=ex63,
        )
    )

    # st64: Multi-Node Failure on Same TAS Workload (1st Hot-Swaps, 2nd Evicts Under Default Gate)
    inf64 = _base_infra(8, with_topology=True)
    inf64.append(make_infra_event_fn(make_cluster_queue_fn("cq-tas-multi", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf64.append(make_infra_event_fn(make_local_queue_fn("lq-tas-multi", "cq-tas-multi")))
    # At t=4s, st-node-00 fails (1st failure -> hot-swaps onto spare node in block!).
    # At t=8s, st-node-01 ALSO fails (2nd failure -> triggers multi-node eviction on default Kueue!).
    for fail_idx, fail_t in [(0, 4.0), (1, 8.0)]:
        inf64.append(
            make_infra_event_fn(
                _make_tas_or_plain_node(
                    fail_idx,
                    det_uid_fn=det_uid_fn,
                    with_topology=True,
                    ready_status="False",
                    unschedulable=True,
                    extra_taints=[{"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"}],
                ),
                fail_t,
                "Update",
            )
        )
    wf64, ex64 = [], []
    w_ev64, e_evs64 = _make_batch_job_and_exec_events(
        name="job-tas-block-4n",
        fmt_ts_fn=fmt_ts_fn,
        det_uid_fn=det_uid_fn,
        arrival_sec=0.0,
        admitted_sec=8.5,
        duration_sec=13.0,
        lq_name="lq-tas-multi",
        cq_name="cq-tas-multi",
        num_pods=4,
        priority_class="p-standard",
        tas_required_level="cloud.google.com/gce-topology-block",
        backoff_limit=8,
        partial_slices=[(0.0, 8.0, "NodeFailure", False)],
        unhealthy_nodes=["st-node-00", "st-node-01"],
    )
    wf64.append(w_ev64)
    ex64.extend(e_evs64)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st64",
            slug="tas-multi-node-failure-eviction-and-readmit",
            category="13. Mid-Scenario Node Failures & Placement Hot-Swap",
            title="TAS Sequential Dual-Node Failure — 1st Node Hot-Swaps, 2nd Node Evicts & Re-Admits on Healthy Spares",
            feature_focus="Sequential two-node failure (`st-node-00` at `t=4s`, `st-node-01` at `t=8s`) on a `4`-pod block-level TAS `Job` with `8` block nodes (`6` remain healthy): verifies multi-node `NodeFailures` eviction and immediate re-admission onto the `6` remaining healthy nodes",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit",
            setup_summary="8-node (`32`-TPU) single-block (`st-block-00`) TAS cluster. One `4`-pod (`16`-TPU) `Job` (`backoffLimit: 8`, `podset-required-topology: gce-topology-block`, `13s`) admits at `t=0s`. `st-node-00` fails at `t=4s`; `st-node-01` fails at `t=8s` (`6` healthy nodes = `24` TPUs remain available in `st-block-00`).",
            expected_behavior="At `t=4s`, `st-node-00` hot-swaps in-place to a spare node. Wait: if Kueue clears `unhealthyNodes` before `t=8s`, `st-node-01` ALSO hot-swaps in-place (`0` evictions), OR if both overlap in `unhealthyNodes`, Kueue evicts (`1` `NodeFailures` eviction) and immediately re-admits onto 4 of the 6 healthy nodes (`st-node-02..07`) — in both cases completing cleanly while `st-node-00/01` stay `NotReady`!",
            expected_metrics=[
                "`Execution Summary`: `workloads finished = 1` despite `2` permanently failed nodes (`st-node-00`, `st-node-01`).",
                "`KWOK Nodes`: `st-node-00` and `st-node-01` remain `Ready=False` (`node-failed=true`), proving `Stage/node-initialize` never resurrects failed nodes prematurely.",
                "`cluster-scraper`: Records both `Node` `Update` (`Ready=False`) events in `infra.json` and pod replacements onto `st-node-02..07`.",
            ],
            must_not_happen=[
                "KWOK `Stage/node-initialize` flipping `st-node-00` or `st-node-01` back to `Ready=True` while `kueue-optimizer.gke.io/node-failed=true` is set.",
                "`job-tas-block-4n` failing to finish when `6` healthy nodes (`24` TPUs >= `16` TPUs) remain in `st-block-00`.",
            ],
            sec51_target="~45%–55% (16/32 TPUs)",
            sec52_target="< 1.5s",
            sec53_target="0–1 evictions (timing of 1st swap vs 2nd fault)",
            sec54_target="~32%–52%",
            infra_events=inf64,
            workflow_events=wf64,
            exec_events=ex64,
        )
    )

    # st65: Non-TAS Quota Cluster Node Failure + Pod Rescheduling + High-Priority Preemption + Node Recovery
    inf65 = _base_infra(8, with_topology=False, priority_classes=[("p-low", 100), ("p-high", 1000)])
    inf65.append(
        make_infra_event_fn(
            make_cluster_queue_fn("cq-nontas-nf", tpu_quota=32, cpu_quota=128, mem_gi_quota=512, within_cq="LowerPriority")
        )
    )
    inf65.append(make_infra_event_fn(make_local_queue_fn("lq-nontas-nf", "cq-nontas-nf")))
    # At t=5s, st-node-00 gets Ready=False + unreachable:NoExecute taint.
    inf65.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(
                0,
                det_uid_fn=det_uid_fn,
                with_topology=False,
                ready_status="False",
                unschedulable=True,
                extra_taints=[{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}],
            ),
            5.0,
            "Update",
        )
    )
    # At t=16s, st-node-00 recovers!
    inf65.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(0, det_uid_fn=det_uid_fn, with_topology=False, ready_status="True"),
            16.0,
            "Update",
        )
    )
    wf65, ex65 = [], []
    for i in range(6):
        w_ev, e_evs = _make_batch_job_and_exec_events(
            name=f"job-low-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=21.0 if i >= 4 else 0.0,
            duration_sec=16.0,
            lq_name="lq-nontas-nf",
            cq_name="cq-nontas-nf",
            num_pods=1,
            priority_class="p-low",
            backoff_limit=4,
            partial_slices=[(0.0, 9.0, "Preempted", True)] if i >= 4 else None,
        )
        wf65.append(w_ev)
        ex65.extend(e_evs)
    for i in range(4):
        w_ev, e_evs = _make_batch_job_and_exec_events(
            name=f"job-high-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=9.0,
            admitted_sec=9.0,
            duration_sec=11.0,
            lq_name="lq-nontas-nf",
            cq_name="cq-nontas-nf",
            num_pods=1,
            priority_class="p-high",
            backoff_limit=2,
        )
        wf65.append(w_ev)
        ex65.extend(e_evs)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st65",
            slug="nontas-node-failure-reschedule-preemption-and-recovery",
            category="13. Mid-Scenario Node Failures & Placement Hot-Swap",
            title="Non-TAS `NoExecute` Node Failure Pod Rescheduling + `LowerPriority` Preemption + Node Recovery",
            feature_focus="Quota-only (non-TAS) `NoExecute` node failure reschedules replacement pod onto spare node (`0` node-failure evictions), followed by `p-high` arrival preempting `2` `p-low` jobs (`40 > 32` TPUs) and `st-node-00` recovery",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st65-nontas-node-failure-reschedule-preemption-and-recovery",
            setup_summary="8-node (`32`-TPU) non-TAS cluster, `withinClusterQueue: LowerPriority`. Six `1`-node `p-low (100)` `Job`s (`24` TPUs, `16s`) start at `t=0s` (`2` spare nodes). At `t=5s`, `st-node-00` fails with `NoExecute` taint (pod reschedules to spare node). At `t=9s`, four `1`-node `p-high (1000)` `Job`s (`16` TPUs, `11s`) arrive (`24 + 16 = 40 > 32` quota). At `t=16s`, `st-node-00` recovers.",
            expected_behavior="The `t=5s` node failure kills `1` pod, which `Job` controller reschedules onto an idle healthy node without Workload eviction. At `t=9s`, `4` `p-high` jobs preempt exact **`2` `p-low` jobs** (`InClusterQueue`). At `t=16s`, `st-node-00` recovers and all `10` jobs complete.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions)`: **`2` evictions** (`InClusterQueue` preemption of `2` `p-low` jobs at `t=9s`).",
                "`Section 5.2`: `p-high (1000)` wait `< 2.0s`.",
                "`Execution Summary`: All **`10/10` `Job`s** finish (`workloads finished = 10`).",
            ],
            must_not_happen=[
                "`p-high` jobs stalling behind `NotReady` `st-node-00` or failing to preempt `2` `p-low` jobs.",
                "`st-node-00` failing to recover at `t=16s`.",
            ],
            sec51_target="~75%–92%",
            sec52_target="p-high wait < 2.0s",
            sec53_target="2 evictions (InClusterQueue)",
            sec54_target="~65%–82% (10 completed)",
            infra_events=inf65,
            workflow_events=wf65,
            exec_events=ex65,
        )
    )

    # =========================================================================
    # CATEGORY 14 (st66 - st70): Adjusting waitForPodsReady to Speedup & Duration
    # =========================================================================

    # st66: waitForPodsReady Timeout Evicts Stuck Pods at Native 1.0x Speedup
    inf66 = _base_infra(4, with_topology=False)
    inf66.append(make_infra_event_fn(make_cluster_queue_fn("cq-wfpr-1x", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf66.append(make_infra_event_fn(make_local_queue_fn("lq-wfpr-1x", "cq-wfpr-1x")))
    wf66, ex66 = [], []
    w_stuck66, e_stuck66 = _make_jobset_partial_or_full_events(
        name="js-stuck-1x",
        fmt_ts_fn=fmt_ts_fn,
        det_uid_fn=det_uid_fn,
        arrival_sec=0.0,
        admitted_sec=0.0,
        duration_sec=12.0,
        lq_name="lq-wfpr-1x",
        cq_name="cq-wfpr-1x",
        num_pods=4,
        priority_class="p-standard",
        startup_delay_sec=60.0,
        record_full=False,
        partial_slices=[(0.0, 8.0, "PodsReadyTimeout", False)],
        true_duration_sec=20.0,
    )
    wf66.append(w_stuck66)
    ex66.extend(e_stuck66)
    for i in range(2):
        w_h, e_h = _make_jobset_partial_or_full_events(
            name=f"js-healthy-1x-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=1.0,
            admitted_sec=8.5,
            duration_sec=10.0,
            lq_name="lq-wfpr-1x",
            cq_name="cq-wfpr-1x",
            num_pods=2,
            priority_class="p-standard",
        )
        wf66.append(w_h)
        ex66.extend(e_h)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st66",
            slug="wfpr-timeout-evicts-stuck-pods-native",
            category="14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)",
            title="`waitForPodsReady` Timeout Evicts Unready Stuck Workload at Native `1.0x` Speedup",
            feature_focus="Baseline `waitForPodsReady.timeout = 8s` (`--wait-for-pods-ready-backoff-limit 0`) evicting & deactivating a slow-starting `60s`-delay `JobSet` at `t≈8s` so healthy queued `JobSet`s admit and complete",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native "
                "--wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s"
            ),
            setup_summary="4-node (`16`-TPU) cluster with `--wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0`. `js-stuck-1x` (`4` pods = `16` TPUs, `pod-startup-delay-seconds: 60`) arrives at `t=0s`; two healthy `2`-pod (`8`-TPU, `10s`) `JobSet`s arrive at `t=1s`.",
            expected_behavior="`js-stuck-1x` reserves `16/16` TPUs at `t=0s` but stays `PodsReady=False` past `8s`. At `t≈8s`, Kueue evicts and deactivates `js-stuck-1x` (`RequeuingLimitExceeded` / `PodsReadyTimeout`), freeing all `16` TPUs so `js-healthy-1x-0` and `js-healthy-1x-1` admit at `t≈8s` (`wait ≈ 7s–9s`) and finish at `t≈18s`.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions)`: **`1` eviction** (`Deactivated` / `PodsReadyTimeout`).",
                "`Section 5.2`: Healthy jobs wait `~7s–9s` (`AVG` across all 3 admissions `~5s–6.5s`).",
                "`cluster-scraper`: Records `PartialExecution` (`interruptionReason: 'PodsReadyTimeout'`, `preempted: false`) for `js-stuck-1x` and `FullExecution` (`~10s`) for both healthy jobs.",
            ],
            must_not_happen=[
                "`js-stuck-1x` holding `16/16` TPUs for `60s` instead of getting evicted at `~8s`.",
                "`run_scenario_replay` hanging forever waiting for deactivated `js-stuck-1x` (`spec.active=false`).",
            ],
            sec51_target="~85%–98% (quota reserved during stuck + healthy)",
            sec52_target="AVG ~5s–7s (healthy wait ~7.5s)",
            sec53_target="1 eviction (PodsReadyTimeout/Deactivated)",
            sec54_target="~48%–62% (only healthy 10s wave counts)",
            infra_events=inf66,
            workflow_events=wf66,
            exec_events=ex66,
        )
    )

    # st67: waitForPodsReady Timeout Scales Proportional to --speedup 5 (Exact Wall-Clock Twin of st66)
    inf67 = _base_infra(4, with_topology=False)
    inf67.append(make_infra_event_fn(make_cluster_queue_fn("cq-wfpr-5x", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf67.append(make_infra_event_fn(make_local_queue_fn("lq-wfpr-5x", "cq-wfpr-5x")))
    wf67, ex67 = [], []
    w_stuck67, e_stuck67 = _make_jobset_partial_or_full_events(
        name="js-stuck-5x",
        fmt_ts_fn=fmt_ts_fn,
        det_uid_fn=det_uid_fn,
        arrival_sec=0.0,
        admitted_sec=0.0,
        duration_sec=60.0,
        lq_name="lq-wfpr-5x",
        cq_name="cq-wfpr-5x",
        num_pods=4,
        priority_class="p-standard",
        startup_delay_sec=300.0,
        record_full=False,
        partial_slices=[(0.0, 40.0, "PodsReadyTimeout", False)],
        true_duration_sec=100.0,
    )
    wf67.append(w_stuck67)
    ex67.extend(e_stuck67)
    for i in range(2):
        w_h, e_h = _make_jobset_partial_or_full_events(
            name=f"js-healthy-5x-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=5.0,
            admitted_sec=42.5,
            duration_sec=50.0,
            lq_name="lq-wfpr-5x",
            cq_name="cq-wfpr-5x",
            num_pods=2,
            priority_class="p-standard",
        )
        wf67.append(w_h)
        ex67.extend(e_h)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st67",
            slug="wfpr-timeout-scales-with-speedup",
            category="14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)",
            title="`waitForPodsReady` Timeout Scaled by `--speedup 5` (`40s` Raw -> `8s` Wall — Exact Twin of st66)",
            feature_focus="Direct `5x` time-compressed twin of `st66`: `--speedup 5 --wait-for-pods-ready-timeout 40s` automatically compresses `waitForPodsReady.timeout` (`40s -> 8s`), `backoffBaseSeconds` (`10s -> 2s`), and `pod-startup-delay-seconds` (`300s -> 60s`) to match `st66` wall-for-wall",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st67-wfpr-timeout-scales-with-speedup "
                "--speedup 5 --wait-for-pods-ready-timeout 40s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 10s"
            ),
            setup_summary="Identical 4-node (`16`-TPU) cluster as `st66` with all raw trace timings multiplied by `5x` (`timeout=40s`, `startup-delay=300s`, healthy arrival `5s`, healthy duration `50s`), replayed at `--speedup 5`.",
            expected_behavior="`resolve_wait_for_pods_ready_config` divides `40s / 5 = 8s` wall timeout (`10s / 5 = 2s` backoff) and `validation.py` divides `300s / 5 = 60s` pod startup delay and `50s / 5 = 10s` execution — reproducing `st66`'s `~8s` wall eviction and `~18s` total wall runtime instead of stalling `40s`!",
            expected_metrics=[
                "`Chamber Banner`: Prints `WaitForPodsReady: [timeout=8s (raw 40s), backoffBase=2s, backoffLimit=0]`.",
                "`Section 5.3 (Total Evictions)`: **`1` eviction**, matching `st66`.",
                "`Wall-Clock Duration`: **`~18s–22s`** (matching `st66`, NOT `50s+` as would happen if `40s` timeout were left unscaled).",
            ],
            must_not_happen=[
                "Wall-clock runtime exceeding `35s` (which would indicate `waitForPodsReady.timeout=40s` was NOT divided by `--speedup 5`).",
            ],
            sec51_target="~85%–98% (matches st66)",
            sec52_target="AVG ~5s–7s wall (matches st66)",
            sec53_target="1 eviction (matches st66)",
            sec54_target="~48%–62% (matches st66)",
            infra_events=inf67,
            workflow_events=wf67,
            exec_events=ex67,
        )
    )

    # st68: waitForPodsReady Timeout Clamped to --duration Budget
    inf68 = _base_infra(4, with_topology=False)
    inf68.append(make_infra_event_fn(make_cluster_queue_fn("cq-wfpr-cap", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf68.append(make_infra_event_fn(make_local_queue_fn("lq-wfpr-cap", "cq-wfpr-cap")))
    wf68, ex68 = [], []
    w_stuck68, e_stuck68 = _make_jobset_partial_or_full_events(
        name="js-stuck-unready",
        fmt_ts_fn=fmt_ts_fn,
        det_uid_fn=det_uid_fn,
        arrival_sec=0.0,
        admitted_sec=0.0,
        duration_sec=10.0,
        lq_name="lq-wfpr-cap",
        cq_name="cq-wfpr-cap",
        num_pods=4,
        priority_class="p-standard",
        never_ready=True,
        record_full=False,
        partial_slices=[(0.0, 12.0, "PodsReadyTimeout", False)],
        true_duration_sec=25.0,
    )
    wf68.append(w_stuck68)
    ex68.extend(e_stuck68)
    for i in range(2):
        w_h, e_h = _make_jobset_partial_or_full_events(
            name=f"js-healthy-cap-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=1.0,
            admitted_sec=12.5,
            duration_sec=8.0,
            lq_name="lq-wfpr-cap",
            cq_name="cq-wfpr-cap",
            num_pods=2,
            priority_class="p-standard",
        )
        wf68.append(w_h)
        ex68.extend(e_h)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st68",
            slug="wfpr-timeout-clamped-to-duration",
            category="14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)",
            title="`waitForPodsReady` Long `10m` Timeout Clamped to `--duration 24s` (`effective = 12s`)",
            feature_focus="Automatic clamping of `--wait-for-pods-ready-timeout 10m` to `50%` of `--duration 24s` (`12s`) so `PodsReadyTimeout` fires inside bounded chamber runs and unblocks healthy peers before test cutoff",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st68-wfpr-timeout-clamped-to-duration "
                "--duration 24s --wait-for-pods-ready-timeout 10m --wait-for-pods-ready-backoff-limit 0"
            ),
            setup_summary="4-node (`16`-TPU) cluster run with `--duration 24s --wait-for-pods-ready-timeout 10m --wait-for-pods-ready-backoff-limit 0`. `js-stuck-unready` (`pod-never-ready: true`, `16` TPUs) arrives at `t=0s`; two healthy `2`-pod (`8`-TPU, `8s`) `JobSet`s arrive at `t=1s`.",
            expected_behavior="`resolve_wait_for_pods_ready_config` clamps `600s` (`10m`) down to `0.5 * 24s = 12s` (`clamped-to-duration`). At `t≈12s`, Kueue evicts and deactivates `js-stuck-unready`, and both `8s` healthy `JobSet`s admit at `t≈12s` and finish at `t≈20s` (well inside the `24s` test budget).",
            expected_metrics=[
                "`Chamber Banner`: Prints `WaitForPodsReady: [timeout=12s (raw 600s), clamped-to-duration, backoffLimit=0]`.",
                "`Section 5.3 (Total Evictions)`: **`1` eviction** (would be `0` if `10m` were left unclamped over `24s`).",
                "`Execution Summary`: Both healthy `JobSet`s finish (`workloads finished = 3` including deactivated stuck workload).",
            ],
            must_not_happen=[
                "`Section 5.3` reporting `0` evictions or `Section 5.4` reporting `0.0%` goodput over the `24s` window.",
            ],
            sec51_target="~85%–98%",
            sec52_target="healthy wait ~11s–12s",
            sec53_target="1 eviction (clamped 12s timeout)",
            sec54_target="~35%–50% (2 healthy 8s jobs)",
            infra_events=inf68,
            workflow_events=wf68,
            exec_events=ex68,
        )
    )

    # st69: waitForPodsReady recoveryTimeout After Mid-Run Node Failure
    inf69 = _base_infra(4, with_topology=False)
    inf69.append(make_infra_event_fn(make_cluster_queue_fn("cq-wfpr-rec", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf69.append(make_infra_event_fn(make_local_queue_fn("lq-wfpr-rec", "cq-wfpr-rec")))
    # At t=4s, st-node-00 fails (only 3 healthy nodes remain, so 4-pod job's replacement pod stays Pending!).
    inf69.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(
                0,
                det_uid_fn=det_uid_fn,
                with_topology=False,
                ready_status="False",
                unschedulable=True,
                extra_taints=[{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}],
            ),
            4.0,
            "Update",
        )
    )
    # At t=18s, st-node-00 recovers so the 4-pod job can re-admit and complete!
    inf69.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(0, det_uid_fn=det_uid_fn, with_topology=False, ready_status="True"),
            18.0,
            "Update",
        )
    )
    wf69, ex69 = [], []
    w_rec, e_rec = _make_batch_job_and_exec_events(
        name="job-4n-recover",
        fmt_ts_fn=fmt_ts_fn,
        det_uid_fn=det_uid_fn,
        arrival_sec=0.0,
        admitted_sec=18.5,
        duration_sec=12.0,
        lq_name="lq-wfpr-rec",
        cq_name="cq-wfpr-rec",
        num_pods=4,
        priority_class="p-standard",
        backoff_limit=6,
        partial_slices=[(0.0, 10.0, "PodsReadyTimeout", False)],
    )
    wf69.append(w_rec)
    ex69.extend(e_rec)
    for i in range(2):
        w_p, e_p = _make_batch_job_and_exec_events(
            name=f"job-1n-peer-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=1.0,
            admitted_sec=10.5,
            duration_sec=7.0,
            lq_name="lq-wfpr-rec",
            cq_name="cq-wfpr-rec",
            num_pods=1,
            priority_class="p-standard",
            backoff_limit=2,
        )
        wf69.append(w_p)
        ex69.extend(e_p)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st69",
            slug="wfpr-recovery-timeout-after-node-failure",
            category="14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)",
            title="`waitForPodsReady.recoveryTimeout` Eviction After Mid-Run Node Failure Frees Healthy Nodes for Peers",
            feature_focus="`waitForPodsReady.recoveryTimeout = 6s` evicting a running `4`-node `Job` whose replacement pod stalls `Pending` after `st-node-00` fails at `t=4s`, freeing the `3` healthy nodes for `1`-node peer jobs before `st-node-00` recovers at `t=18s`",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st69-wfpr-recovery-timeout-after-node-failure "
                "--wait-for-pods-ready-timeout 20s --wait-for-pods-ready-recovery-timeout 6s "
                "--wait-for-pods-ready-backoff-limit 2 --wait-for-pods-ready-backoff-base 2s"
            ),
            setup_summary="4-node (`16`-TPU) non-TAS cluster with `timeout=20s`, `recoveryTimeout=6s`, `backoffLimit=2`, `backoffBase=2s`. `job-4n-recover` (`4` pods = `16` TPUs, `12s`) reaches `PodsReady=True` at `t=0s`; two `1`-node (`4`-TPU, `7s`) peer `Job`s arrive at `t=1s`. `st-node-00` fails at `t=4s` and recovers at `t=18s`.",
            expected_behavior="At `t=4s`, `st-node-00` fails, killing 1 pod of `job-4n-recover`; its replacement pod stays `Pending` (`3` healthy nodes < `4` needed), flipping `PodsReady` to `False`. At `t≈10s` (`4s + 6s recoveryTimeout`), Kueue evicts `job-4n-recover` (`PodsReadyTimeout`), allowing both `1`-node peer `Job`s to run (`10s–17s`) on healthy nodes! Once `st-node-00` recovers at `t=18s`, `job-4n-recover` re-admits across all 4 nodes and succeeds.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions)`: **`>= 1` eviction** (`PodsReadyTimeout` triggered by `recoveryTimeout`).",
                "`Execution Summary`: All **`3/3` `Job`s** succeed (`workloads finished = 3`).",
                "`cluster-scraper`: Captures `st-node-00` failure (`t=4s`), `PodsReadyTimeout` `PartialExecution` (`t≈10s`), `st-node-00` recovery (`t=18s`), and `3` `FullExecution` records.",
            ],
            must_not_happen=[
                "`job-4n-recover` holding `3` healthy nodes idle from `t=4s` to `t=18s` without `recoveryTimeout` firing.",
            ],
            sec51_target="~65%–88%",
            sec52_target="peers admit right after ~10s recoveryTimeout",
            sec53_target="1–2 evictions (PodsReadyTimeout)",
            sec54_target="~40%–70% (all 3 jobs succeed)",
            infra_events=inf69,
            workflow_events=wf69,
            exec_events=ex69,
        )
    )

    # st70: waitForPodsReady blockAdmission Head-of-Line Startup Gate
    inf70 = _base_infra(8, with_topology=False)
    inf70.append(make_infra_event_fn(make_cluster_queue_fn("cq-wfpr-blk", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf70.append(make_infra_event_fn(make_local_queue_fn("lq-wfpr-blk", "cq-wfpr-blk")))
    wf70, ex70 = [], []
    w_head, e_head = _make_jobset_partial_or_full_events(
        name="js-slow-startup-head",
        fmt_ts_fn=fmt_ts_fn,
        det_uid_fn=det_uid_fn,
        arrival_sec=0.0,
        admitted_sec=0.0,
        duration_sec=10.0,
        lq_name="lq-wfpr-blk",
        cq_name="cq-wfpr-blk",
        num_pods=2,
        priority_class="p-standard",
        startup_delay_sec=8.0,
    )
    wf70.append(w_head)
    ex70.extend(e_head)
    for i in range(2):
        w_f, e_f = _make_jobset_partial_or_full_events(
            name=f"js-follower-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=1.0,
            admitted_sec=8.5,
            duration_sec=10.0,
            lq_name="lq-wfpr-blk",
            cq_name="cq-wfpr-blk",
            num_pods=2,
            priority_class="p-standard",
        )
        wf70.append(w_f)
        ex70.extend(e_f)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st70",
            slug="wfpr-block-admission-startup-gate",
            category="14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)",
            title="`waitForPodsReady.blockAdmission` Serializes Follower Admissions Behind `8s` Pod Startup Delay",
            feature_focus="`--wait-for-pods-ready-block-admission` holding follower `JobSet` admissions for `~7s` until `js-slow-startup-head` (`pod-startup-delay-seconds: 8`) reaches `PodsReady=True` despite `75%` free cluster quota",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st70-wfpr-block-admission-startup-gate "
                "--wait-for-pods-ready-timeout 25s --wait-for-pods-ready-block-admission"
            ),
            setup_summary="8-node (`32`-TPU) cluster with `--wait-for-pods-ready-timeout 25s --wait-for-pods-ready-block-admission`. `js-slow-startup-head` (`2` pods = `8` TPUs, `pod-startup-delay-seconds: 8`, `10s` run) arrives at `t=0s`; two `2`-pod (`8`-TPU, `0s` delay, `10s` run) followers arrive at `t=1s` (`24` TPUs of quota sit free).",
            expected_behavior="`js-slow-startup-head` admits at `t=0s` (`8/32` TPUs) and stays `PodsReady=False` for `8s` (`< 25s` timeout, so `0` evictions). Because `blockAdmission=true`, Kueue blocks `js-follower-0` and `js-follower-1` until `t≈8s` when `js-slow-startup-head` transitions to `PodsReady=True`, after which both followers admit (`wait ≈ 7s`) and all `3` jobs succeed.",
            expected_metrics=[
                "`Section 5.2 (Wait Time)`: Follower jobs wait **`~6.5s–8.5s`** (`P50/P90 >= 5s` despite `24/32` idle TPUs!).",
                "`Section 5.3 (Total Evictions)`: Strictly **`0`** (`8s` startup delay `< 25s` timeout).",
                "`Execution Summary`: All **`3/3` `JobSet`s** succeed.",
            ],
            must_not_happen=[
                "Follower jobs admitting at `t=1s` (`wait < 2s`) while `js-slow-startup-head` is still `PodsReady=False` under `blockAdmission=true`.",
            ],
            sec51_target="~55%–75% (24/32 TPUs peak)",
            sec52_target="P50/P90 ~6.5s–8.5s (followers gated)",
            sec53_target="0 evictions",
            sec54_target="~42%–58% (3 completed)",
            infra_events=inf70,
            workflow_events=wf70,
            exec_events=ex70,
        )
    )

    # =========================================================================
    # CATEGORY 15 (st71 - st75): Interrupted-Workload Recording, Multiplier & Override
    # =========================================================================

    # st71: Interrupted Preempted Workloads Replayed With CLI Multiplier (--interrupted-duration-multiplier 2.5)
    inf71 = _base_infra(4, with_topology=False)
    inf71.append(make_infra_event_fn(make_cluster_queue_fn("cq-int-mult", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf71.append(make_infra_event_fn(make_local_queue_fn("lq-int-mult", "cq-int-mult")))
    wf71, ex71 = [], []
    for i in range(4):
        w_ev, e_evs = _make_jobset_partial_or_full_events(
            name=f"js-preempted-rec-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=0.0,
            duration_sec=8.0,
            lq_name="lq-int-mult",
            cq_name="cq-int-mult",
            num_pods=1,
            priority_class="p-standard",
            record_full=False,
            partial_slices=[(0.0, 8.0, "Preempted", True)],
        )
        wf71.append(w_ev)
        ex71.extend(e_evs)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st71",
            slug="partial-exec-preempted-multiplier-replay",
            category="15. Interrupted-Workload Recording, Multiplier & True-Length Replay",
            title="Preemption-Interrupted Workloads (`PartialExecution = 8s` Only) Replayed With `--interrupted-duration-multiplier 2.5` (`20s`)",
            feature_focus="Workloads scraped from admission to preemption (`PartialExecution = 8.0s`, no `FullExecution`) replayed without preemption using `--interrupted-duration-multiplier 2.5` so each runs for `20.0s` (`> 8.0s` interrupted recording)",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay "
                "--interrupted-duration-multiplier 2.5"
            ),
            setup_summary="4-node (`16`-TPU) cluster where 4 `1`-node (`4`-TPU) `JobSet`s (`js-preempted-rec-0..3`) have ONLY `Admitted` (`t=0s`) + `PartialExecution` (`endTime=8s`, `executionDurationSeconds=8.0`, `interruptionReason='Preempted'`, no `FullExecution`, no pod annotation fallback). Replayed with `--interrupted-duration-multiplier 2.5`.",
            expected_behavior="Because `FullExecution` is absent and no preemption occurs during replay, `testrunner.py` multiplies each job's observed `8.0s` partial duration by `2.5x` -> **`20.0s`** (`> 8.0s`). All 4 `JobSet`s run uninterrupted for `20s` across all `16` TPUs.",
            expected_metrics=[
                "`Pre-Run Outline`: Reports `4 PartialExecution-only [Preempted=4] -> replay policy: CLI multiplier 2.5x` and `Execution Durations: min=20.0s, max=20.0s`.",
                "`Wall-Clock Duration`: **`~20s–23s`** (strictly longer than the `8s` interrupted recording!).",
                "`Section 5.4`: **`EFFECTIVE SAT ≈ 90%–100%`** over the `~20s` run (`4` completed).",
                "`cluster-scraper`: Records `FullExecution ≈ 20s` (`> 8s`) for all 4 jobs.",
            ],
            must_not_happen=[
                "Workloads terminating after only `8s` (`<= interrupted recording`) when replayed without preemption.",
                "Workloads falling back to `30s` default instead of `8.0s * 2.5 = 20.0s`.",
            ],
            sec51_target="~92%–100% (16/16 TPUs for 20s)",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="~90%–100% (20s > 8s partial)",
            infra_events=inf71,
            workflow_events=wf71,
            exec_events=ex71,
        )
    )

    # st72: Interrupted Preempted Workloads Replayed With True-Length Schema & CLI Override (Twin of st71)
    inf72 = _base_infra(4, with_topology=False)
    inf72.append(make_infra_event_fn(make_cluster_queue_fn("cq-int-ov", tpu_quota=16, cpu_quota=64, mem_gi_quota=256)))
    inf72.append(make_infra_event_fn(make_local_queue_fn("lq-int-ov", "cq-int-ov")))
    wf72, ex72 = [], []
    for i in range(4):
        w_ev, e_evs = _make_jobset_partial_or_full_events(
            name=f"js-preempted-rec-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=0.0,
            duration_sec=8.0,
            lq_name="lq-int-ov",
            cq_name="cq-int-ov",
            num_pods=1,
            priority_class="p-standard",
            record_full=False,
            partial_slices=[(0.0, 8.0, "Preempted", True)],
            true_duration_sec=16.0 if i < 2 else 22.0,
        )
        wf72.append(w_ev)
        ex72.extend(e_evs)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st72",
            slug="partial-exec-preempted-true-length-override",
            category="15. Interrupted-Workload Recording, Multiplier & True-Length Replay",
            title="Preemption-Interrupted Workloads Replayed With Schema `trueDurationSeconds` & CLI `--interrupted-duration-override` (Twin of st71)",
            feature_focus="Direct A/B twin of `st71`: explicit true lengths via schema `trueDurationSeconds` (`16s` on `js-0..1`) and CLI `--interrupted-duration-override js-preempted-rec-2=24s,js-preempted-rec-3=24s` (`> 8s` interrupted recording)",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st72-partial-exec-preempted-true-length-override "
                "--interrupted-duration-override js-preempted-rec-2=24s,js-preempted-rec-3=24s"
            ),
            setup_summary="Identical 4-node (`16`-TPU) cluster and `8.0s` `PartialExecution`-only `JobSet`s as `st71`. `execution-time.json` carries `trueDurationSeconds: 16.0` on `js-0..1` and `22.0` on `js-2..3`, while CLI `--interrupted-duration-override` overrides `js-2..3` to `24s`.",
            expected_behavior="`js-preempted-rec-0..1` run for exact **`16.0s`** (from schema `trueDurationSeconds`), while `js-preempted-rec-2..3` run for exact **`24.0s`** (CLI override taking highest precedence over schema `22.0s`), creating a clean two-step (`16s` -> `24s`) completion profile (`avg = 20s`, `~83%` cluster saturation over `24s`).",
            expected_metrics=[
                "`Pre-Run Outline`: `Execution Durations: min=16.0s, max=24.0s` (`avg=20.0s`, strictly `> 8.0s` partial).",
                "`Section 5.4`: **`EFFECTIVE SAT ≈ 78%–88%`** (`(2*16 + 2*24) / (4*24) = 80 / 96 = 83.3%`, vs `~95%` uniform `20s` in `st71`).",
                "`cluster-scraper`: Records `FullExecution ≈ 16s` for `js-0..1` and `FullExecution ≈ 24s` for `js-2..3`.",
            ],
            must_not_happen=[
                "CLI `--interrupted-duration-override` failing to override `js-2..3` or affecting `js-0..1`.",
                "Any workload replaying for `<= 8.0s`.",
            ],
            sec51_target="~80%–90% (16 TPUs 0-16s, 8 TPUs 16-24s)",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="~78%–88% (83.3% theoretical)",
            infra_events=inf72,
            workflow_events=wf72,
            exec_events=ex72,
        )
    )

    # st73: Node-Failure Interrupted TAS Workloads Replayed With Schema missingDurationMultiplier Alongside FullExecution Peers
    inf73 = _base_infra(8, with_topology=True)
    inf73.append(make_infra_event_fn(make_cluster_queue_fn("cq-nf-mult", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf73.append(make_infra_event_fn(make_local_queue_fn("lq-nf-mult", "cq-nf-mult")))
    wf73, ex73 = [], []
    # 2 workloads that were interrupted by NodeFailure at 7s (missingDurationMultiplier = 2.5 -> 17.5s)
    for i in range(2):
        w_ev, e_evs = _make_jobset_partial_or_full_events(
            name=f"js-nf-interrupted-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=0.0,
            duration_sec=7.0,
            lq_name="lq-nf-mult",
            cq_name="cq-nf-mult",
            num_pods=2,
            priority_class="p-standard",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x2-id",
            record_full=False,
            partial_slices=[(0.0, 7.0, "NodeFailure", False)],
            missing_multiplier=2.5,
            unhealthy_nodes=[f"st-node-0{i * 2}"],
        )
        wf73.append(w_ev)
        ex73.extend(e_evs)
    # 2 peer workloads that completed cleanly in the original trace (FullExecution = 14.0s -> must stay 14.0s!)
    for i in range(2):
        w_ev, e_evs = _make_jobset_partial_or_full_events(
            name=f"js-full-peer-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=0.0,
            duration_sec=14.0,
            lq_name="lq-nf-mult",
            cq_name="cq-nf-mult",
            num_pods=2,
            priority_class="p-standard",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x2-id",
            record_full=True,
        )
        wf73.append(w_ev)
        ex73.extend(e_evs)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st73",
            slug="partial-exec-node-failure-multiplier-replay",
            category="15. Interrupted-Workload Recording, Multiplier & True-Length Replay",
            title="Node-Failure Interrupted TAS Workloads (`PartialExecution = 7s`, `missingDurationMultiplier = 2.5`) Replayed Alongside `FullExecution` Peers",
            feature_focus="Schema `interruptionReason: 'NodeFailure'` + `missingDurationMultiplier: 2.5` scaling interrupted TAS `JobSet`s from `7.0s -> 17.5s` while leaving `FullExecution = 14.0s` peer `JobSet`s untouched",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st73-partial-exec-node-failure-multiplier-replay",
            setup_summary="8-node (`32`-TPU) 7-level TAS cluster with 4 `2`-node (`8`-TPU, `2x2x2-id`) `JobSet`s at `t=0s`: `js-nf-interrupted-0..1` have ONLY `PartialExecution` (`7.0s`, `interruptionReason='NodeFailure'`, `missingDurationMultiplier=2.5`), while `js-full-peer-0..1` have `FullExecution` (`14.0s`).",
            expected_behavior="On the healthy replay cluster, `js-nf-interrupted-0..1` run for **`7.0s * 2.5 = 17.5s`** (`> 7.0s`), while `js-full-peer-0..1` run for their exact **`14.0s`** `FullExecution` duration.",
            expected_metrics=[
                "`Pre-Run Outline`: `Execution Durations: min=14.0s, max=17.5s` (`2 PartialExecution-only [NodeFailure=2]`).",
                "`Section 5.3`: `0` evictions on healthy replay cluster.",
                "`Section 5.4`: **`EFFECTIVE SAT ≈ 84%–95%`** (`(2*14 + 2*17.5) / (4*17.5) = 63 / 70 = 90.0%`).",
            ],
            must_not_happen=[
                "`missingDurationMultiplier` altering `FullExecution` workloads (`js-full-peer-0..1` must stay `14.0s`).",
                "`js-nf-interrupted-0..1` terminating at `7.0s`.",
            ],
            sec51_target="~86%–96%",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="~84%–95% (90.0% theoretical)",
            infra_events=inf73,
            workflow_events=wf73,
            exec_events=ex73,
        )
    )

    # st74: Interrupted t0 Pre-Admitted Workloads + Scrape-End Cutoff PartialExecution Flush
    inf74 = _base_infra(8, with_topology=False)
    inf74.append(make_infra_event_fn(make_cluster_queue_fn("cq-t0-cut", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)))
    inf74.append(make_infra_event_fn(make_local_queue_fn("lq-t0-cut", "cq-t0-cut")))
    wf74, ex74 = [], []
    # 4 t0 pre-admitted workloads that were preempted at t=6s in the original trace (PartialExecution=6s, NO FullExecution!)
    for i in range(4):
        t0_pods, t0_wf, t0_ex_orig = make_t0_admitted_jobset_events_fn(
            name=f"js-t0-interrupted-{i}",
            remaining_sec=6.0,
            local_queue="lq-t0-cut",
            cluster_queue="cq-t0-cut",
            node_names=[f"st-node-0{i}"],
            num_pods=1,
            priority_class="p-standard",
        )
        t0_wl_name = t0_ex_orig[0]["payload"]["workloadName"]
        t0_obj = json.loads(json.dumps(t0_wf["payload"]["object"]))
        (t0_obj.get("metadata") or {}).get("annotations", {}).pop("kueue-optimizer.gke.io/pod-execution-time-seconds", None)
        for rj in (t0_obj.get("spec") or {}).get("replicatedJobs") or []:
            (((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get("metadata", {}).get(
                "annotations", {}
            ).pop("kueue-optimizer.gke.io/pod-execution-time-seconds", None)
        t0_wf["payload"]["object"] = t0_obj
        wf74.append(t0_wf)
        inf74.extend(t0_pods)
        ex74.append(t0_ex_orig[0])
        ex74.append(
            {
                "timestamp": fmt_ts_fn(6.0),
                "eventType": "PartialExecution",
                "workloadState": "NotAdmitted",
                "payload": {
                    "executionType": "Partial",
                    "workloadName": t0_wl_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "JobSet",
                    "ownerName": f"js-t0-interrupted-{i}",
                    "ownerAPIVersion": "jobset.x-k8s.io/v1alpha2",
                    "localQueueName": "lq-t0-cut",
                    "clusterQueueName": "cq-t0-cut",
                    "priorityClassName": "p-standard",
                    "creationTime": fmt_ts_fn(0.0),
                    "admittedTime": fmt_ts_fn(0.0),
                    "endTime": fmt_ts_fn(6.0),
                    "executionDurationSeconds": 6.0,
                    "executionDuration": "6s",
                    "preempted": True,
                    "preemptionCount": 1,
                    "interruptionReason": "Preempted",
                    "interruptionCount": 1,
                    "reason": "Preempted",
                },
            }
        )
    # Plus 4 arriving long JobSets at t=3s (duration 32s -> still in flight when --duration 22s stops!)
    for i in range(4):
        w_ev, e_evs = _make_jobset_partial_or_full_events(
            name=f"js-inflight-cutoff-{i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=3.0,
            admitted_sec=3.0,
            duration_sec=32.0,
            lq_name="lq-t0-cut",
            cq_name="cq-t0-cut",
            num_pods=1,
            priority_class="p-standard",
        )
        wf74.append(w_ev)
        ex74.extend(e_evs)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st74",
            slug="partial-exec-t0-interrupted-and-scrape-cutoff",
            category="15. Interrupted-Workload Recording, Multiplier & True-Length Replay",
            title="Interrupted `t0` `PodGroup` Multiplier Extension (`6s -> 15s`) + Mid-Flight `ScrapeEnded` Cutoff Recording",
            feature_focus="Extends `t0` pre-admitted pods whose `deletionTime=6s` came from preemption (`PartialExecution` only) to `15.0s` (`2.5x`) when replayed without preemption, and verifies `cluster-scraper` flushes `PartialExecution (ScrapeEnded)` at `--duration 22s`",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st74-partial-exec-t0-interrupted-and-scrape-cutoff "
                "--no-skip-initial-offset --duration 22s --interrupted-duration-multiplier 2.5"
            ),
            setup_summary="8-node (`32`-TPU) cluster: 4 `t0` `PodGroup`s (`16` TPUs) recorded `PartialExecution = 6.0s` (`Preempted`, `deletionTime = t0+6s`, no `FullExecution`); 4 arriving `JobSet`s (`16` TPUs, `32s`) enter at `t=3s`. Run with `--no-skip-initial-offset --duration 22s --interrupted-duration-multiplier 2.5`.",
            expected_behavior="Because the `4` `t0` workloads are NOT preempted during replay, `loader.py` extends their `t0` pod lifetimes from `6s` to **`6.0s * 2.5 = 15.0s`**, so they complete at `t≈15s` (`COMPLETED ONLY = 4`). Meanwhile, the `4` `t=3s` `JobSet`s (`32s`) are still running at `t=22s` (`ACTIVE AT END = 4`), and `cluster-scraper` flushes `PartialExecution` (`interruptionReason: 'ScrapeEnded'`, `~19s`) for all 4 in-flight jobs on shutdown!",
            expected_metrics=[
                "`Section 5.4`: Additive split of `4` completed `t0` `PodGroup`s (`15s` each) + `4` active-at-end `JobSet`s (`~19s` each) -> **`EFFECTIVE SAT ≈ 72%–86%`** (`(4*4*15 + 4*4*19) / (32*22) = 544 / 704 = 77.3%`, vs `59%` if `t0` pods had died early at `6s`!).",
                "`cluster-scraper`: Emits `4` `FullExecution` (`~15s`) + `4` `PartialExecution` (`interruptionReason: 'ScrapeEnded'`) records.",
            ],
            must_not_happen=[
                "`t0` interrupted-only pods terminating at `t=6s` instead of `t≈15s` (`2.5x`).",
                "`cluster-scraper` dropping in-flight `Admitted` workloads without a `ScrapeEnded` `PartialExecution` at shutdown.",
            ],
            sec51_target="~75%–88%",
            sec52_target="< 1.5s",
            sec53_target="0 evictions",
            sec54_target="~72%–86% (4 completed @15s + 4 active)",
            infra_events=inf74,
            workflow_events=wf74,
            exec_events=ex74,
        )
    )

    # st75: Combined Multi-Fault Stress (TAS Hot-Swap + High-Priority t0 Preemption + waitForPodsReady Timeout + Partial Multiplier)
    inf75 = _base_infra(16, with_topology=True, priority_classes=[("p-low", 100), ("p-med", 500), ("p-high", 1000)])
    inf75.append(
        make_infra_event_fn(
            make_cluster_queue_fn("cq-combo", tpu_quota=64, cpu_quota=256, mem_gi_quota=1024, within_cq="LowerPriority")
        )
    )
    inf75.append(make_infra_event_fn(make_local_queue_fn("lq-combo", "cq-combo")))
    # At t=4s, st-node-08 (in rack 2 = st-node-08..11) fails -> p-med 3-node TAS Job hot-swaps onto spare st-node-11!
    inf75.append(
        make_infra_event_fn(
            _make_tas_or_plain_node(
                8,
                det_uid_fn=det_uid_fn,
                with_topology=True,
                ready_status="False",
                unschedulable=True,
                extra_taints=[{"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"}],
            ),
            4.0,
            "Update",
        )
    )
    wf75, ex75 = [], []
    # 2 t0 pre-admitted p-low 4-node TAS PodGroups on racks 0 & 1 (st-node-00..07, PartialExecution=8s -> 2.0x = 16s on resubmit!)
    for r_i in range(2):
        rack_nodes = [f"st-node-{r_i * 4 + k:02d}" for k in range(4)]
        t0_pods, t0_wf, t0_ex_orig = make_t0_admitted_jobset_events_fn(
            name=f"js-t0-low-rack-{r_i}",
            remaining_sec=8.0,
            local_queue="lq-combo",
            cluster_queue="cq-combo",
            node_names=rack_nodes,
            num_pods=4,
            priority_class="p-low",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
        )
        t0_wl_name = t0_ex_orig[0]["payload"]["workloadName"]
        # Strip pod-execution-time-seconds from raw workflow so replay duration comes strictly from PartialExecution * 2.0 = 16s!
        t0_obj = json.loads(json.dumps(t0_wf["payload"]["object"]))
        (t0_obj.get("metadata") or {}).get("annotations", {}).pop("kueue-optimizer.gke.io/pod-execution-time-seconds", None)
        for rj in (t0_obj.get("spec") or {}).get("replicatedJobs") or []:
            (((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get("metadata", {}).get(
                "annotations", {}
            ).pop("kueue-optimizer.gke.io/pod-execution-time-seconds", None)
        t0_wf["payload"]["object"] = t0_obj
        wf75.append(t0_wf)
        inf75.extend(t0_pods)
        ex75.append(t0_ex_orig[0])
        ex75.append(
            {
                "timestamp": fmt_ts_fn(8.0),
                "eventType": "PartialExecution",
                "workloadState": "NotAdmitted",
                "payload": {
                    "executionType": "Partial",
                    "workloadName": t0_wl_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "JobSet",
                    "ownerName": f"js-t0-low-rack-{r_i}",
                    "ownerAPIVersion": "jobset.x-k8s.io/v1alpha2",
                    "localQueueName": "lq-combo",
                    "clusterQueueName": "cq-combo",
                    "priorityClassName": "p-low",
                    "creationTime": fmt_ts_fn(0.0),
                    "admittedTime": fmt_ts_fn(0.0),
                    "endTime": fmt_ts_fn(8.0),
                    "executionDurationSeconds": 8.0,
                    "executionDuration": "8s",
                    "preempted": True,
                    "preemptionCount": 1,
                    "interruptionReason": "Preempted",
                    "interruptionCount": 1,
                    "reason": "Preempted",
                },
            }
        )
    # 2 t=0s p-med 3-node TAS Jobs on racks 2 & 3 (1 spare node per rack so st-node-08 failure hot-swaps!)
    for r_i in range(2):
        w_ev, e_evs = _make_batch_job_and_exec_events(
            name=f"job-med-hotswap-{r_i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=0.0,
            admitted_sec=0.0,
            duration_sec=14.0,
            lq_name="lq-combo",
            cq_name="cq-combo",
            num_pods=3,
            priority_class="p-med",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
            backoff_limit=4,
            unhealthy_nodes=["st-node-08"] if r_i == 0 else None,
        )
        wf75.append(w_ev)
        ex75.extend(e_evs)
    # 2 t=5s p-high 4-node TAS JobSets -> preempt the 2 p-low t0 PodGroups on racks 0 & 1!
    for r_i in range(2):
        w_ev, e_evs = _make_jobset_partial_or_full_events(
            name=f"js-high-preempt-{r_i}",
            fmt_ts_fn=fmt_ts_fn,
            det_uid_fn=det_uid_fn,
            arrival_sec=5.0,
            admitted_sec=5.5,
            duration_sec=10.0,
            lq_name="lq-combo",
            cq_name="cq-combo",
            num_pods=4,
            priority_class="p-high",
            tas_required_level="cloud.google.com/gke-tpu-partition-2x2x4-id",
        )
        wf75.append(w_ev)
        ex75.extend(e_evs)
    # 1 t=6s stuck unready p-low JobSet -> once admitted, gets evicted & deactivated by waitForPodsReady (8s)!
    w_st75, e_st75 = _make_jobset_partial_or_full_events(
        name="js-low-stuck-wfpr",
        fmt_ts_fn=fmt_ts_fn,
        det_uid_fn=det_uid_fn,
        arrival_sec=6.0,
        admitted_sec=15.5,
        duration_sec=10.0,
        lq_name="lq-combo",
        cq_name="cq-combo",
        num_pods=2,
        priority_class="p-low",
        tas_required_level="cloud.google.com/gke-tpu-partition-2x2x2-id",
        never_ready=True,
        record_full=False,
        partial_slices=[(15.5, 23.5, "PodsReadyTimeout", False)],
    )
    wf75.append(w_st75)
    ex75.extend(e_st75)
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st75",
            slug="combined-tas-failure-hotswap-preemption-wfpr-stress",
            category="15. Interrupted-Workload Recording, Multiplier & True-Length Replay",
            title="Combined Multi-Fault TAS Hot-Swap + `t0` Preemption Resubmit (`2.0x` Partial Multiplier) + `waitForPodsReady` Timeout",
            feature_focus="End-to-end integration of all 4 capabilities: TAS failed-node hot-swap (`st-node-08` -> `st-node-11` with `0` node-failure evictions), `p-high` intra-CQ preemption of `2` `t0` `PodGroup`s, `2.0x` `--interrupted-duration-multiplier` (`8s -> 16s`) on resubmitted `t0` jobs, and `8s` `waitForPodsReady` deactivation of a stuck `JobSet`",
            recommended_cmd=(
                "python3 deploy/scripts/testrunner.py testscenarios/self-tests/st75-combined-tas-failure-hotswap-preemption-wfpr-stress "
                "--no-skip-initial-offset --interrupted-duration-multiplier 2.0 "
                "--wait-for-pods-ready-timeout 8s --wait-for-pods-ready-recovery-timeout 18s --wait-for-pods-ready-backoff-limit 0"
            ),
            setup_summary="16-node (`64`-TPU, `4` racks of `4` nodes) TAS cluster (`timeout=8s`, `recoveryTimeout=18s` exceeding `batch/v1` `Job`'s `10s` pod-recreation backoff, `backoffLimit=0`): `2` `t0` `p-low` `4`-node `PodGroup`s (`PartialExecution=8s`, scaled `2.0x -> 16s`), `2` `t=0s` `p-med` `3`-node `Job`s (`14s`), `st-node-08` fails at `t=4s`, `2` `t=5s` `p-high` `4`-node `JobSet`s (`10s`) preempt `t0` `p-low`, and `1` `t=6s` unready `p-low` `JobSet` hits `waitForPodsReady` (`8s`).",
            expected_behavior="`st-node-08` failure at `t=4s` hot-swaps `p-med` onto `st-node-11` within `recoveryTimeout=18s` (`0` node-failure or recovery evictions); `p-high` preempts both `t0` `p-low` `PodGroup`s (`2` `InClusterQueue` evictions) which dynamically resubmit with **`16.0s`** (`8s * 2.0`) duration and complete on racks `0` & `1` after `p-high` finishes; `js-low-stuck-wfpr` never reaches `PodsReady=True` and gets evicted & deactivated after `timeout=8s` (`1` `PodsReadyTimeout`/`Deactivated` eviction).",
            expected_metrics=[
                "`Section 5.3 (Total Evictions)`: **`3` evictions** (`2` `InClusterQueue` preemptions + `1` `PodsReadyTimeout`/`Deactivated` eviction, and **`0` `NodeFailures` evictions** thanks to TAS hot-swap!).",
                "`Execution Summary`: All `6` healthy workflows (`2` `p-med` + `2` `p-high` + `2` resubmitted `t0` `p-low`) succeed and `1` stuck workflow deactivates cleanly.",
                "`cluster-scraper`: Captures `Node` `Update` (`st-node-08`), hot-swapped pod on `st-node-11`, `Preempted` + `PodsReadyTimeout` `PartialExecution`s, and `6` `FullExecution`s (`~16s` on resubmitted `p-low`).",
            ],
            must_not_happen=[
                "`p-med` suffering a `NodeFailures` eviction when `st-node-11` is available in rack `2`.",
                "Resubmitted `t0` `p-low` jobs running only `8s` instead of `16s` (`2.0x`).",
            ],
            sec51_target="~65%–85%",
            sec52_target="p-high < 2.5s",
            sec53_target="3 evictions (2 Preempt + 1 WFPR, 0 NodeFailure)",
            sec54_target="~55%–75% (6 healthy completed)",
            infra_events=inf75,
            workflow_events=wf75,
            exec_events=ex75,
        )
    )

    return scenarios


FAILURE_PREEMPTION_PAIRINGS: dict[str, list[dict[str, str]]] = {
    "st61": [
        {
            "peer_id": "st63",
            "relationship": "Exact TAS Rack Node-Failure A/B Twin (`1` Spare Node Hot-Swap `st61` vs. `0` Spare Nodes FailFast Eviction `st63`)",
            "knob_changed": "`3`-pod (`12`-TPU) rack `Job`s leaving `st-node-03` spare in `p224-0` (`st61`) vs. `4`-pod (`16`-TPU) full-rack `Job`s leaving `0` spare nodes (`st63`) when `st-node-00` fails at `t=6s`.",
            "cross_check": "`Sec 5.3 Evictions` flips from **`0` (`st61`, in-place TAS hot-swap)** to **`1` (`st63`, `NodeFailures` FailFast eviction)**, proving Kueue replaces failed TAS nodes without eviction iff a healthy spare exists in the required topology domain.",
        },
        {
            "peer_id": "st62",
            "relationship": "`Node` `NotReady` Hot-Swap (`st61`) vs. `Node` `Deleted` Hot-Swap + Scale-Up (`st62`)",
            "knob_changed": "`Node` `Update` (`Ready=False`) in `st61` vs. `Node` `Deleted` (`!nodeExists`) + post-`t0` `Node` `New` (`st-node-08`) in `st62`.",
            "cross_check": "Both `st61` and `st62` complete Wave 1 with **`0` Workload evictions** via TAS hot-swap, while `st62` additionally unblocks `4`-node full-rack Wave-2 `JobSet`s once `st-node-08` joins.",
        },
    ],
    "st62": [
        {
            "peer_id": "st61",
            "relationship": "`Node` `Deleted` + Scale-Up (`st62`) vs. `Node` `NotReady` (`st61`) Hot-Swap Pair",
            "knob_changed": "Mid-run `Node` deletion + replacement node addition (`st62`) vs. mid-run `Ready=False` transition (`st61`).",
            "cross_check": "Both achieve `0` Workload evictions; `st62` completes `4/4` `JobSet`s including `2` full-rack Wave-2 jobs requiring the newly added `st-node-08`.",
        },
    ],
    "st63": [
        {
            "peer_id": "st61",
            "relationship": "Exact TAS Rack Node-Failure A/B Twin (`0` Spare Nodes FailFast Eviction `st63` vs. `1` Spare Node Hot-Swap `st61`)",
            "knob_changed": "`4`-pod full-rack `Job` (`0` spares, `st63`) vs. `3`-pod rack `Job` (`1` spare, `st61`).",
            "cross_check": "`st63` records `1` `NodeFailures` eviction + `PartialExecution (interruptionReason='NodeFailure')` before re-admitting on `st-node-00` recovery at `t=13s`, whereas `st61` records `0` evictions.",
        },
    ],
    "st64": [
        {
            "peer_id": "st61",
            "relationship": "Sequential Dual-Node Failure (`st64`) vs. Single-Node Failure (`st61`) Pair",
            "knob_changed": "Two nodes (`st-node-00` at `t=4s`, `st-node-01` at `t=8s`) failing on the same `4`-pod block `Job` (`st64`) vs. single node failure (`st61`).",
            "cross_check": "Both `st-node-00` and `st-node-01` remain `Ready=False` throughout `st64` while the `4`-pod `Job` recovers onto healthy spares `st-node-02..07` and finishes cleanly.",
        },
    ],
    "st65": [
        {
            "peer_id": "st61",
            "relationship": "Non-TAS `NoExecute` Pod Rescheduling + Preemption (`st65`) vs. TAS In-Place Hot-Swap (`st61`)",
            "knob_changed": "Quota-only non-TAS cluster with `NoExecute` node taint + `p-high` preemption (`st65`) vs. 7-level TAS rack hot-swap (`st61`).",
            "cross_check": "In `st65`, the `t=5s` node failure causes `0` Kueue evictions (pod reschedules to spare node), while the `t=9s` `p-high` wave causes exact `2` `InClusterQueue` preemption evictions.",
        },
    ],
    "st66": [
        {
            "peer_id": "st67",
            "relationship": "Exact Time-Compression A/B Twin (`1.0x` Native `timeout=8s` `st66` vs. `5.0x` Speedup `timeout=40s -> 8s` `st67`)",
            "knob_changed": "All raw trace timings and `--wait-for-pods-ready-timeout` scaled by `5x` (`8s -> 40s`) and replayed with `--speedup 5` in `st67`.",
            "cross_check": "Both `st66` and `st67` evict the stuck head `JobSet` at **`~8s` wall clock**, admit both healthy peers at `~8s` wall, and finish in **`~18s–22s` wall clock** with `1` eviction.",
        },
        {
            "peer_id": "st68",
            "relationship": "Explicit Short Timeout (`st66`) vs. `--duration`-Clamped `10m -> 12s` Timeout (`st68`)",
            "knob_changed": "Raw `--wait-for-pods-ready-timeout 8s` (`st66`) vs. `--wait-for-pods-ready-timeout 10m --duration 24s` auto-clamped to `12s` (`st68`).",
            "cross_check": "Both evict the stuck head job (`~8s` in `st66` vs. `~12s` in `st68`) and complete both healthy peer jobs within the run window.",
        },
    ],
    "st67": [
        {
            "peer_id": "st66",
            "relationship": "Exact Time-Compression A/B Twin (`5.0x` Speedup `st67` vs. `1.0x` Native `st66`)",
            "knob_changed": "`--speedup 5 --wait-for-pods-ready-timeout 40s` (`st67`) vs. `--speedup 1 --wait-for-pods-ready-timeout 8s` (`st66`).",
            "cross_check": "Wall-clock duration (`~18s–22s`), `Sec 5.3 = 1` eviction, and `Sec 5.4 ≈ 48%–62%` match `st66` within timing jitter.",
        },
    ],
    "st68": [
        {
            "peer_id": "st66",
            "relationship": "`--duration`-Clamped `waitForPodsReady` Timeout (`st68`) vs. Native `8s` Timeout (`st66`)",
            "knob_changed": "`--wait-for-pods-ready-timeout 10m` clamped by `--duration 24s` to `12s` (`st68`) vs. native `8s` (`st66`).",
            "cross_check": "`st68` banner shows `timeout=12s (raw 600s), clamped-to-duration`, healthy peers wait `~11s–12s` (`+4s` vs. `st66`), and `Sec 5.3 = 1`.",
        },
    ],
    "st69": [
        {
            "peer_id": "st66",
            "relationship": "Post-Running `recoveryTimeout` Eviction (`st69`) vs. Initial-Startup `timeout` Eviction (`st66`)",
            "knob_changed": "Workload reaches `PodsReady=True` first and loses 1 node at `t=4s` (`recoveryTimeout=6s` in `st69`) vs. never reaching `PodsReady=True` (`timeout=8s` in `st66`).",
            "cross_check": "In `st69`, `recoveryTimeout` evicts `job-4n-recover` at `t≈10s` (`4s + 6s`), unblocking `2` `1`-node peers before `st-node-00` recovers at `t=18s` so all `3/3` jobs succeed.",
        },
    ],
    "st70": [
        {
            "peer_id": "st66",
            "relationship": "Sub-Timeout `blockAdmission` Startup Gate (`st70`) vs. Over-Timeout Eviction (`st66`)",
            "knob_changed": "`8s` pod startup delay `< 25s` timeout with `--wait-for-pods-ready-block-admission` (`st70`) vs. `60s` startup delay `> 8s` timeout (`st66`).",
            "cross_check": "`st70` records **`0` evictions** while holding followers for `~7s` (`Sec 5.2 P50 >= 5s` despite `75%` free quota), whereas `st66` records `1` eviction.",
        },
    ],
    "st71": [
        {
            "peer_id": "st72",
            "relationship": "Exact Interrupted-Workload Replay A/B Twin (`--interrupted-duration-multiplier 2.5` `st71` vs. Explicit True-Length Override `st72`)",
            "knob_changed": "Uniform `2.5x` multiplier (`8s -> 20s` in `st71`) vs. schema `trueDurationSeconds: 16s` + CLI `--interrupted-duration-override ...=24s` (`st72`) on the exact same `8.0s` preempted `PartialExecution`-only trace.",
            "cross_check": "Both replay strictly longer than the `8.0s` interrupted recording (`20s` uniform in `st71`, `Sec 5.4 ≈ 95%`; `16s` + `24s` two-tier in `st72`, `Sec 5.4 ≈ 83%`).",
        },
    ],
    "st72": [
        {
            "peer_id": "st71",
            "relationship": "Exact Interrupted-Workload Replay A/B Twin (True-Length Override `st72` vs. Multiplier `st71`)",
            "knob_changed": "Per-workload `trueDurationSeconds` (`16s`) + CLI `--interrupted-duration-override` (`24s`) in `st72` vs. `--interrupted-duration-multiplier 2.5` (`20s`) in `st71`.",
            "cross_check": "Outline & live `cluster-scraper` confirm `min=16s, max=24s` in `st72` vs. `min=20s, max=20s` in `st71`.",
        },
    ],
    "st73": [
        {
            "peer_id": "st71",
            "relationship": "Schema `NodeFailure` `missingDurationMultiplier` + `FullExecution` Mix (`st73`) vs. CLI `Preempted` Multiplier (`st71`)",
            "knob_changed": "`NodeFailure` `PartialExecution` (`7s * 2.5 = 17.5s`) mixed with `FullExecution` (`14s`) peers (`st73`) vs. all-`Preempted` `PartialExecution` (`8s * 2.5 = 20s`, `st71`).",
            "cross_check": "In `st73`, `FullExecution` peers stay at exact `14.0s` while `NodeFailure` interrupted jobs expand to `17.5s` (`> 7.0s`).",
        },
    ],
    "st74": [
        {
            "peer_id": "st71",
            "relationship": "Interrupted `t0` `PodGroup` Extension + `ScrapeEnded` Flush (`st74`) vs. Arriving `PartialExecution` Multiplier (`st71`)",
            "knob_changed": "Pre-admitted `t0` `PodGroup`s (`6s -> 15s` at `2.5x`) + mid-flight `--duration 22s` cutoff (`st74`) vs. `NotAdmitted` arriving jobs (`st71`).",
            "cross_check": "`st74` proves `t0` interrupted-only pods extend past `6s` to `15s` when unpreempted AND `cluster-scraper` flushes `ScrapeEnded` `PartialExecution` for all `4` jobs still running at `t=22s`.",
        },
    ],
    "st75": [
        {
            "peer_id": "st61",
            "relationship": "Combined Multi-Fault Stress (`st75`) vs. Isolated TAS Hot-Swap (`st61`)",
            "knob_changed": "`st75` combines `st61`'s TAS rack hot-swap (`st-node-08 -> st-node-11`) with `p-high` preemption of `2` `t0` `PodGroup`s (`2.0x` partial multiplier `8s -> 16s`) and `8s` `waitForPodsReady` deactivation.",
            "cross_check": "`st75` records `3` total evictions (`2` `InClusterQueue` + `1` `PodsReadyTimeout`, **`0` `NodeFailures`**) and completes all `6` healthy workflows.",
        },
    ],
}
